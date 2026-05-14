import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { RoleService, Role } from '../role.service';
import { Permission, PermissionService } from '../../permissions/permission.service';

@Component({
  selector: 'app-role-form',
  standalone: true,
  imports: [RouterModule, FormsModule, CommonModule],
  templateUrl: './role-form.component.html'
})
export class RoleFormComponent implements OnInit {
  isEditMode = false;
  editRoleId: string | null = null;

  name = '';
  description = '';
  selectedPermissions: string[] = [];
  permissions: Permission[] = [];

  message = '';
  loading = false;

  constructor(
    private roleService: RoleService,
    private permissionService: PermissionService,
    private route: ActivatedRoute,
    private router: Router
  ) { }

  ngOnInit() {
    this.permissionService.getPermissions().subscribe({
      next: (rows) => {
        this.permissions = rows || [];
      },
      error: () => {
        this.message = 'Failed to load permissions';
      }
    });

    this.editRoleId = this.route.snapshot.paramMap.get('id');
    this.isEditMode = !!this.editRoleId;

    if (this.isEditMode && this.editRoleId) {
      this.roleService.getRoles().subscribe({
        next: (roles) => {
          const role = roles.find(r => r.id === this.editRoleId);
          if (role) {
            this.name = role.name;
            this.description = role.description;
            this.selectedPermissions = Array.isArray(role.permissions) ? [...role.permissions] : [];
          }
        },
        error: () => this.message = 'Failed to load role'
      });
    }
  }

  togglePermission(permissionId: string, checked: boolean): void {
    if (checked) {
      if (!this.selectedPermissions.includes(permissionId)) {
        this.selectedPermissions = [...this.selectedPermissions, permissionId];
      }
      return;
    }
    this.selectedPermissions = this.selectedPermissions.filter((p) => p !== permissionId);
  }

  onSubmit() {
    if (!this.name) {
      this.message = 'Name is required';
      return;
    }

    this.loading = true;
    this.message = '';

    const payload: Partial<Role> = {
      name: this.name,
      description: this.description,
      permissions: this.selectedPermissions
    };

    if (this.isEditMode && this.editRoleId) {
      this.roleService.updateRole(this.editRoleId, payload).subscribe({
        next: () => {
          this.loading = false;
          this.router.navigate(['/roles']);
        },
        error: (err) => {
          this.loading = false;
          this.message = err?.error?.error || 'Failed to update role';
        }
      });
    } else {
      this.roleService.createRole(payload).subscribe({
        next: () => {
          this.loading = false;
          this.router.navigate(['/roles']);
        },
        error: (err) => {
          this.loading = false;
          this.message = err?.error?.error || 'Failed to create role';
        }
      });
    }
  }
}
