import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Permission, PermissionService } from './permission.service';

@Component({
  selector: 'app-permissions',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './permissions.component.html',
})
export class PermissionsComponent implements OnInit {
  permissions: Permission[] = [];
  name = '';
  description = '';
  loading = false;
  message = '';

  constructor(private permissionService: PermissionService) {}

  ngOnInit(): void {
    this.loadPermissions();
  }

  loadPermissions(): void {
    this.permissionService.getPermissions().subscribe({
      next: (rows) => {
        this.permissions = rows || [];
      },
      error: () => {
        this.message = 'Failed to load permissions';
      },
    });
  }

  createPermission(): void {
    if (!this.name.trim()) {
      this.message = 'Permission name is required';
      return;
    }

    this.loading = true;
    this.message = '';

    this.permissionService
      .createPermission({
        name: this.name.trim(),
        description: this.description.trim(),
      })
      .subscribe({
        next: () => {
          this.name = '';
          this.description = '';
          this.loading = false;
          this.loadPermissions();
        },
        error: (err) => {
          this.loading = false;
          this.message = err?.error?.error || 'Failed to create permission';
        },
      });
  }
}
