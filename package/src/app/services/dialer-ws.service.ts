import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { AuthService } from '../auth/auth.service';

@Injectable({
  providedIn: 'root'
})
export class DialerWsService {
  private ws: WebSocket | null = null;
  private candidateUrls: string[] = [];
  private candidateIndex = 0;
  
  public wsConnected$ = new BehaviorSubject<boolean>(false);
  public activeCall$ = new BehaviorSubject<any | null>(null);

  constructor(private authService: AuthService) {}

  connectDialerWS() {
    const pageProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const host = window.location.hostname || '127.0.0.1';
    const hostWithPort = window.location.host || host;
    const user = this.authService.currentUser;
    
    if (!user) return;

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    // Allow override first, then try both known ports used across deployments.
    const explicitUrl = localStorage.getItem('dialer_ws_url')?.trim();
    const defaults = [
      `${pageProtocol}://${hostWithPort}/agent-ws`,
      `${pageProtocol}://${host}:18081`,
    ];
    this.candidateUrls = explicitUrl ? [explicitUrl, ...defaults.filter(u => u !== explicitUrl)] : defaults;
    this.candidateIndex = 0;
    this.connectNextCandidate();
  }

  private connectNextCandidate() {
    const user = this.authService.currentUser;
    if (!user) return;
    if (this.candidateIndex >= this.candidateUrls.length) {
      this.wsConnected$.next(false);
      return;
    }

    const url = this.candidateUrls[this.candidateIndex++];
    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      this.wsConnected$.next(true);
    };

    this.ws.onclose = () => {
      this.wsConnected$.next(false);
      if (this.candidateIndex < this.candidateUrls.length) {
        this.connectNextCandidate();
      }
    };

    this.ws.onerror = () => {
      this.wsConnected$.next(false);
    };

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === "CALL_ASSIGNED") {
          this.activeCall$.next(data.call);
        }
        if (data.type === "CALL_ENDED") {
          this.activeCall$.next(null);
        }
      } catch (e) {
        // ignore parse errors
      }
    };
  }

  sendPresence(sipRegistered: boolean, available: boolean) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    const user = this.authService.currentUser;
    if (!user) return;

    this.ws.send(JSON.stringify({
      type: "agent_presence",
      agentId: user.id,
      extension: user.extension,
      sipRegistered,
      availableInbound: available,
      availableOutbound: available,
    }));
  }

  disconnectDialerWS() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.wsConnected$.next(false);
  }
}
