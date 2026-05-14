import { Injectable } from '@angular/core';
import { Router } from '@angular/router';
import { HttpClient } from '@angular/common/http';
import { BehaviorSubject, Observable, tap } from 'rxjs';

export interface User {
  id: string;
  name?: string;
  email?: string;
  role: "admin" | "agent" | "user";
  extension?: string;
}

export interface SipConfig {
  uri: string;
  password: string;
  ws: string;
  wsServers?: string;
}

export interface LoginResponse {
  user: User;
  token: string;
  sip?: SipConfig;
}

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private loggedInSubject = new BehaviorSubject<boolean>(false);
  public loggedIn$ = this.loggedInSubject.asObservable();
  public currentUser: User | null = null;
  public sipConfig: SipConfig | null = null;

  constructor(private router: Router, private http: HttpClient) {
    const token = localStorage.getItem('auth_token');
    if (token) {
      this.loggedInSubject.next(true);
      this._hydrateFromStorage();
    }
  }

  private _hydrateFromStorage(): void {
    try {
      const userRaw = localStorage.getItem('auth_user');
      const sipRaw = localStorage.getItem('auth_sip');
      if (userRaw) this.currentUser = JSON.parse(userRaw);
      if (sipRaw) this.sipConfig = JSON.parse(sipRaw);
    } catch {
      // Corrupted storage — clear it
      localStorage.removeItem('auth_user');
      localStorage.removeItem('auth_sip');
    }
  }

  login(username: string, password: string): Observable<LoginResponse> {
    return this.http.post<LoginResponse>('/api/auth/login', { username, password }).pipe(
      tap(res => {
        if (res && res.token) {
          this.loggedInSubject.next(true);
          this.currentUser = res.user;
          this.sipConfig = res.sip || null;

          if (this.sipConfig && this.sipConfig.ws && !this.sipConfig.wsServers) {
            this.sipConfig.wsServers = this.sipConfig.ws;
          }

          localStorage.setItem('auth_token', res.token);
          localStorage.setItem('auth_user', JSON.stringify(res.user));
          if (res.sip) {
            localStorage.setItem('auth_sip', JSON.stringify(this.sipConfig));
          }
        }
      })
    );
  }

  restoreSession(): Observable<LoginResponse> {
    return this.http.get<LoginResponse>('/api/auth/me').pipe(
      tap(res => {
        if (res?.user) {
          this.currentUser = res.user;
          localStorage.setItem('auth_user', JSON.stringify(res.user));
        }
        if (res?.sip) {
          this.sipConfig = res.sip;
          if (this.sipConfig.ws && !this.sipConfig.wsServers) {
            this.sipConfig.wsServers = this.sipConfig.ws;
          }
          localStorage.setItem('auth_sip', JSON.stringify(this.sipConfig));
        }
      })
    );
  }

  logout(): void {
    this.loggedInSubject.next(false);
    this.currentUser = null;
    this.sipConfig = null;
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_user');
    localStorage.removeItem('auth_sip');
    this.router.navigate(['/login']);
  }

  isAuthenticated(): boolean {
    return this.loggedInSubject.value;
  }
}