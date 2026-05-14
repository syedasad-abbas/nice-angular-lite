import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface CreateUserRequest {
  name: string;
  username: string;
  extension: string;
  email?: string | null;
  password: string;
  role: "admin" | "agent" | "user";
  sipPassword?: string;
  status?: "enabled" | "disabled";
}

export interface CreateUserResponse {
  user: {
    id: string;
    name?: string;
    username: string;
    email?: string;
    extension: string;
    role: "admin" | "agent" | "user";
    status?: "enabled" | "disabled";
  };
}

export interface User {
  id: string;
  name?: string;
  username: string;
  email?: string;
  extension: string;
  role: "admin" | "agent" | "user";
  status?: "enabled" | "disabled";
}

export interface DashboardStats {
  totalUsers: number;
  loggedInUsers: number;
  answeredAgentCalls: number;
}

export interface AnsweredCallsSeries {
  labels: string[];
  series: number[];
}

export type AnsweredCallsRange = '24h' | '7d' | '1m' | '1y';

@Injectable({ providedIn: 'root' })
export class UserService {
  private apiUrl = '/api/admin/users';
  private dashboardUrl = '/api/admin/dashboard';

  constructor(private http: HttpClient) {}

  createUserApi(data: CreateUserRequest): Observable<CreateUserResponse> {
    return this.http.post<CreateUserResponse>(this.apiUrl, data);
  }

  getUsers(): Observable<User[]> {
    return this.http.get<User[]>(this.apiUrl);
  }

  deleteUser(id: string): Observable<any> {
    return this.http.delete(`${this.apiUrl}/${id}`);
  }

  updateUser(id: string, data: Partial<CreateUserRequest>): Observable<any> {
    return this.http.put(`${this.apiUrl}/${id}`, data);
  }

  getDashboardStats(): Observable<DashboardStats> {
    return this.http.get<DashboardStats>(this.dashboardUrl);
  }

  getAnsweredCallsSeries(range: AnsweredCallsRange = '7d', userId: string = 'all'): Observable<AnsweredCallsSeries> {
    return this.http.get<AnsweredCallsSeries>(
      `${this.dashboardUrl}/answered-calls-series?range=${encodeURIComponent(range)}&userId=${encodeURIComponent(userId)}`
    );
  }
}
