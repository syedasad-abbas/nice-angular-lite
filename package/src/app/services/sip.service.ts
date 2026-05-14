import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { AuthService, SipConfig } from '../auth/auth.service';
import { DialerWsService } from './dialer-ws.service';
import * as JsSIP from 'jssip';

export type CallState = "idle" | "ringing" | "in-call";

@Injectable({
  providedIn: 'root'
})
export class SipService {
  private ua: any = null;
  private activeSipKey = "";

  public sipRegistered$ = new BehaviorSubject<boolean>(false);
  public callState$ = new BehaviorSubject<CallState>("idle");
  public incomingSession$ = new BehaviorSubject<any>(null);
  public callerNumber$ = new BehaviorSubject<string>("");
  public available$ = new BehaviorSubject<boolean>(true);

  constructor(
    private authService: AuthService,
    private dialerWs: DialerWsService
  ) {
    // Whenever availability changes, send new presence
    this.available$.subscribe(available => {
      this.dialerWs.sendPresence(this.sipRegistered$.value, available);
    });
  }

  private normalizeWsUrl(rawUrl: string): string {
    try {
      const parsed = new URL(rawUrl);
      const pageIsHttps = window.location.protocol === "https:";
      const socketProtocol =
        parsed.protocol === "wss:" || pageIsHttps ? "wss:" : "ws:";

      const host = parsed.hostname || "";
      const isLocalHost = ["127.0.0.1", "localhost", "0.0.0.0"].includes(host);
      const isIpv4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(host);
      const isIpv6 = host.includes(":");
      const hasDot = host.includes(".");
      // Docker service names like "auth-api" are not browser-resolvable.
      const isInternalServiceHost = !isLocalHost && !isIpv4 && !isIpv6 && !hasDot;

      let resolvedHost = host;
      let resolvedPort = parsed.port || "5066";
      let resolvedPath = parsed.pathname || "/";

      if (isLocalHost || isInternalServiceHost) {
        resolvedHost = window.location.hostname || "127.0.0.1";
      }

      // When auth API accidentally returns ws://auth-api:3100/sip-ws,
      // route SIP WS directly to FreeSWITCH WS port for local/dev use.
      const isSipWsPath = resolvedPath.startsWith("/sip-ws");
      if (isSipWsPath && (isInternalServiceHost || parsed.port === "3100")) {
        resolvedPort = "5066";
        resolvedPath = "/";
      }

      return `${socketProtocol}//${resolvedHost}:${resolvedPort}${resolvedPath}`;
    } catch {
      return rawUrl;
    }
  }

  startSIP(config: SipConfig) {
    const rawWsServers = config.wsServers || config.ws;
    const wsServers = rawWsServers ? this.normalizeWsUrl(rawWsServers) : rawWsServers;
    if (!wsServers) {
      console.error("[SIP] No WS server provided");
      return;
    }
    // Keep portal WS online even when SIP registration fails.
    this.dialerWs.connectDialerWS();

    console.log("[SIP] startSIP config", {
      uri: config.uri,
      wsServers,
    });

    const sipKey = `${config.uri}|${wsServers}`;
    
    if (this.ua && this.activeSipKey === sipKey) {
      return;
    }

    if (this.ua && this.activeSipKey !== sipKey) {
      try { this.ua.stop(); } catch (e) {}
      this.ua = null;
    }

    const socket = new JsSIP.WebSocketInterface(wsServers);
    const ua = new JsSIP.UA({
      sockets: [socket],
      uri: config.uri,
      password: config.password,
    });
    this.ua = ua;

    this.activeSipKey = sipKey;

    ua.on("registered", () => {
      if (this.ua !== ua) return;
      this.sipRegistered$.next(true);
      this.dialerWs.connectDialerWS();
      // Delay to ensure WS connects before sending presence
      setTimeout(() => this.dialerWs.sendPresence(true, this.available$.value), 1000);
    });

    ua.on("unregistered", () => {
      if (this.ua !== ua) return;
      this.sipRegistered$.next(false);
      this.dialerWs.sendPresence(false, this.available$.value);
    });

    ua.on("registrationFailed", (event: any) => {
      if (this.ua !== ua) return;
      console.error("[SIP] registrationFailed", {
        cause: event?.cause,
        response: event?.response?.status_code,
      });
      this.sipRegistered$.next(false);
      this.dialerWs.sendPresence(false, this.available$.value);
    });

    ua.on("disconnected", () => {
      if (this.ua !== ua) return;
      this.sipRegistered$.next(false);
      this.dialerWs.sendPresence(false, this.available$.value);
      this.ua = null;
      this.activeSipKey = "";
    });

    ua.on("newRTCSession", (data: any) => {
      if (this.ua !== ua) return;
      if (data.originator !== "remote") return;

      const session = data.session;
      const caller = session.remote_identity?.uri?.user || data.request?.from?.uri?.user || "Unknown";

      this.incomingSession$.next(session);
      this.callerNumber$.next(String(caller));
      this.callState$.next("ringing");

      const endCall = () => {
        this.clearCurrentCall();
      };

      session.on("accepted", () => {
        this.callState$.next("in-call");
      });

      session.on("ended", endCall);
      session.on("failed", endCall);
    });

    ua.start();
  }

  answerIncomingCall() {
    const session = this.incomingSession$.value;
    if (session) {
      session.answer({
        mediaConstraints: { audio: true, video: false }
      });
    }
  }

  rejectIncomingCall() {
    const session = this.incomingSession$.value;
    if (session) {
      session.terminate();
      this.clearCurrentCall();
    }
  }

  hangupCurrentCall() {
    const session = this.incomingSession$.value;
    if (session) {
      session.terminate();
      this.clearCurrentCall();
    }
  }

  sendDTMF(tone: string) {
    const session = this.incomingSession$.value;
    if (session && this.callState$.value === 'in-call') {
      session.sendDTMF(tone);
    }
  }

  stopSIP() {
    try {
      if (this.ua) {
        this.ua.stop();
        this.ua = null;
      }
      this.activeSipKey = "";
    } finally {
      this.sipRegistered$.next(false);
      this.clearCurrentCall();
      this.dialerWs.disconnectDialerWS();
    }
  }

  private clearCurrentCall() {
    this.callState$.next("idle");
    this.incomingSession$.next(null);
    this.callerNumber$.next("");
  }
}
