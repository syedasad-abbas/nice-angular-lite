import { Component, OnInit } from '@angular/core';
import { RouterModule } from '@angular/router';
import { CommonModule } from '@angular/common';
import { RoleService, Role } from '../role.service';

@Component({
  selector: 'app-role-list',
  standalone: true,
  imports: [RouterModule, CommonModule],
  templateUrl: './role-list.component.html'
})
export class RoleListComponent implements OnInit {
  roles: Role[] = [];
  message = '';

  constructor(private roleService: RoleService) { }

  ngOnInit() {
    this.loadRoles();
  }

  loadRoles() {
    this.roleService.getRoles().subscribe({
      next: (data) => this.roles = data,
      error: () => this.message = 'Failed to load roles'
    });
  }

  deleteRole(id: string) {
    if (['admin', 'agent', 'user'].includes(id)) {
      alert('Cannot delete system roles');
      return;
    }
    if (!confirm('Delete this role?')) return;
    this.roleService.deleteRole(id).subscribe({
      next: () => this.roles = this.roles.filter(r => r.id !== id),
      error: (err) => {
        this.message = err?.error?.error || 'Failed to delete role';
      }
    });
  }
}
