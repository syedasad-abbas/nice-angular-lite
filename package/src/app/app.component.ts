import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { AuthService } from './auth/auth.service';
import { SipService } from './services/sip.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit, OnDestroy {
  title = 'app';

  private authSub: Subscription | null = null;

  constructor(
    private authService: AuthService,
    private sipService: SipService
  ) { }

  ngOnInit(): void {
    // On page reload: if a token exists, restore session from server
    // then start SIP with the recovered config
    if (this.authService.isAuthenticated()) {
      this.authService.restoreSession().subscribe({
        next: () => {
          const sip = this.authService.sipConfig;
          if (sip) {
            this.sipService.startSIP(sip);
          }
        },
        error: () => {
          // Token expired or invalid — force logout
          this.authService.logout();
        }
      });
    }

    // On login: start SIP whenever the user becomes authenticated
    this.authSub = this.authService.loggedIn$.subscribe(loggedIn => {
      if (loggedIn) {
        const sip = this.authService.sipConfig;
        if (sip) {
          this.sipService.startSIP(sip);
        }
      } else {
        this.sipService.stopSIP();
      }
    });
  }

  ngOnDestroy(): void {
    this.authSub?.unsubscribe();
  }
}