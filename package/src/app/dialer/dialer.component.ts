import { Component, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { SipService, CallState } from '../services/sip.service';
import { DialerWsService } from '../services/dialer-ws.service';

@Component({
  selector: 'app-dialer',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './dialer.component.html',
  styleUrls: ['./dialer.component.css']
})
export class DialerComponent implements OnDestroy {
  sipRegistered = false;
  wsConnected = false;
  available = true;
  callState: CallState = 'idle';
  callerNumber = '';

  private callInterval: any;
  elapsedSec = 0;
  manualNumber = '';
  manualCallLoading = false;
  manualCallError = '';
  manualCallSuccess = '';
  
  constructor(
    public sipService: SipService,
    public dialerWs: DialerWsService,
    private http: HttpClient
  ) {
    this.sipService.sipRegistered$.subscribe(val => this.sipRegistered = val);
    this.dialerWs.wsConnected$.subscribe(val => this.wsConnected = val);
    this.sipService.available$.subscribe(val => this.available = val);
    this.sipService.callerNumber$.subscribe(val => this.callerNumber = val);
    
    this.sipService.callState$.subscribe(val => {
      this.callState = val;
      if (val === 'in-call') {
        this.startTimer();
      } else {
        this.stopTimer();
      }
    });
  }

  toggleAvailability() {
    this.sipService.available$.next(!this.available);
  }

  answerCall() {
    this.sipService.answerIncomingCall();
  }

  rejectCall() {
    this.sipService.rejectIncomingCall();
  }

  hangupCall() {
    this.sipService.hangupCurrentCall();
  }

  sendDTMF(tone: string) {
    this.sipService.sendDTMF(tone);
  }

  placeManualCall() {
    this.manualCallError = '';
    this.manualCallSuccess = '';
    const numberTo = String(this.manualNumber || '').trim();

    if (!/^\+?\d{8,15}$/.test(numberTo)) {
      this.manualCallError = 'Enter a valid number (8-15 digits, optional +).';
      return;
    }

    this.manualCallLoading = true;
    this.http.post<{ transactionId: string }>('/api/agent/manual-call', { numberTo }).subscribe({
      next: (res) => {
        this.manualCallSuccess = `Call accepted. Transaction ID: ${res?.transactionId || 'N/A'}`;
        this.manualNumber = '';
        this.manualCallLoading = false;
      },
      error: (err) => {
        const code = err?.error?.error || '';
        if (code === 'rate-limit-exceeded') {
          this.manualCallError = 'Rate limit reached. Please try again shortly.';
        } else if (code === 'agent-already-has-active-call') {
          this.manualCallError = 'You already have an active manual call.';
        } else if (code === 'invalid-numberTo') {
          this.manualCallError = 'Invalid destination number.';
        } else {
          this.manualCallError = 'Manual call failed. Please try again.';
        }
        this.manualCallLoading = false;
      }
    });
  }

  private startTimer() {
    this.elapsedSec = 0;
    this.callInterval = setInterval(() => {
      this.elapsedSec++;
    }, 1000);
  }

  private stopTimer() {
    if (this.callInterval) clearInterval(this.callInterval);
    this.elapsedSec = 0;
  }

  get formattedDuration() {
    const mm = String(Math.floor(this.elapsedSec / 60)).padStart(2, '0');
    const ss = String(this.elapsedSec % 60).padStart(2, '0');
    return `${mm}:${ss}`;
  }

  ngOnDestroy() {
    this.stopTimer();
  }
}
