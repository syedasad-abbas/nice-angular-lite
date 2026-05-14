import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface Permission {
  id: string;
  name: string;
  description: string;
  createdAt?: string;
}

@Injectable({ providedIn: 'root' })
export class PermissionService {
  private apiUrl = '/api/admin/permissions';

  constructor(private http: HttpClient) {}

  getPermissions(): Observable<Permission[]> {
    return this.http.get<Permission[]>(this.apiUrl);
  }

  createPermission(data: Partial<Permission>): Observable<Permission> {
    return this.http.post<Permission>(this.apiUrl, data);
  }
}
