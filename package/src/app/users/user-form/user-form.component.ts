import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { UserService, CreateUserRequest } from '../user.service';

@Component({
  selector: 'app-user-form',
  standalone: true,
  imports: [RouterModule, FormsModule, CommonModule],
  templateUrl: './user-form.component.html',
  styleUrl: './user-form.component.scss'
})
export class UserFormComponent implements OnInit {
  isEditMode = false;
  editUserId: string | null = null;

  name = '';
  username = '';
  extension = '';
  email = '';
  role = 'agent';
  status = 'enabled';
  password = '';
  confirmPassword = '';
  sipPassword = '';

  message = '';
  loading = false;

  constructor(
    private userService: UserService,
    private route: ActivatedRoute,
    private router: Router
  ) { }

  ngOnInit() {
    this.editUserId = this.route.snapshot.paramMap.get('id');
    this.isEditMode = !!this.editUserId;

    if (this.isEditMode && this.editUserId) {
      this.userService.getUsers().subscribe({
        next: (users: any[]) => {
          const user = users.find(u => String(u.id) === String(this.editUserId));

          if (user) {
            this.name = user.name || '';
            this.username = user.username || '';
            this.extension = user.extension || '';
            this.email = user.email || '';
            this.role = user.role || 'agent';
            this.status = user.status || 'enabled';

            this.password = '';
            this.confirmPassword = '';
            this.sipPassword = '';
          }
        },
        error: (err: any) => {
          console.error("[LOAD USER ERROR]", err);
          this.message = "Failed to load user";
        }
      });
    }
  }

  validate(): string | null {
    if (!this.name || !this.username || !this.extension) {
      return "All required fields must be filled";
    }

    if (!this.isEditMode && (!this.password || !this.confirmPassword)) {
      return "Password and confirm password are required";
    }

    if (!/^[a-z0-9_]+$/.test(this.username)) {
      return "Username must be lowercase alphanumeric with underscore only";
    }

    if (!/^\d{3,6}$/.test(this.extension)) {
      return "Extension must be 3–6 digits only";
    }

    if (this.password && this.password.length < 6) {
      return "Password must be at least 6 characters";
    }

    if (this.password !== this.confirmPassword) {
      return "Passwords do not match";
    }

    if (!["admin", "agent", "user"].includes(this.role)) {
      return "Invalid role selected";
    }

    if (!["enabled", "disabled"].includes(this.status)) {
      return "Invalid status selected";
    }

    return null;
  }

  mapBackendError(err: any): string {
    const code = String(err?.error?.error || "").trim();

    switch (code) {
      case "username-exists":
        return "Username already exists";
      case "email-exists":
        return "Email already exists";
      case "missing-required-fields":
        return "Missing required fields (backend validation failed)";
      case "invalid-extension":
        return "Extension must be 3-6 digits only";
      case "invalid-role":
        return "Invalid role sent to server";
      case "forbidden":
        return "You are not allowed to create users";
      case "missing-token":
      case "invalid-token":
        return "Your session expired. Please login again";
      default:
        if (err?.status >= 500) {
          return "Server error while saving user";
        }
        return code || err?.message || "Failed to save user";
    }
  }

  onSubmit() {
    this.message = '';

    const error = this.validate();
    if (error) {
      this.message = error;
      return;
    }

    this.loading = true;

    const payload: CreateUserRequest = {
      name: this.name,
      username: this.username,
      extension: this.extension,
      email: this.email || "",
      password: this.password,
      role: this.role as "admin" | "agent" | "user",
      status: this.status as "enabled" | "disabled",
      sipPassword: this.sipPassword || this.password,
    };

    if (this.isEditMode && this.editUserId) {
      this.userService.updateUser(this.editUserId, payload).subscribe({
        next: () => {
          this.message = "User updated successfully";
          this.loading = false;
          this.router.navigate(['/users']);
        },
        error: (err: any) => {
          console.error("[UPDATE USER ERROR]", err);
          this.message = this.mapBackendError(err);
          this.loading = false;
        }
      });

      return;
    }

    this.userService.createUserApi(payload).subscribe({
      next: (res: any) => {
        console.log("[CREATE USER SUCCESS]", res);
        this.message = "User created successfully";

        this.name = '';
        this.username = '';
        this.extension = '';
        this.email = '';
        this.password = '';
        this.confirmPassword = '';
        this.sipPassword = '';
        this.role = 'agent';
        this.status = 'enabled';
        this.loading = false;
      },
      error: (err: any) => {
        console.error("[CREATE USER ERROR]", err);
        this.message = this.mapBackendError(err);
        this.loading = false;
      }
    });
  }
}
