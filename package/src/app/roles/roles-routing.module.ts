import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { RoleListComponent } from './role-list/role-list.component';
import { RoleFormComponent } from './role-form/role-form.component';

const routes: Routes = [
  {
    path: '',
    children: [
      {
        path: '',
        component: RoleListComponent,
        data: {
          title: 'Roles',
          urls: [
            { title: 'Dashboard', url: '/dashboard' },
            { title: 'Roles' }
          ]
        }
      },
      {
        path: 'create',
        component: RoleFormComponent,
        data: {
          title: 'Create Role',
          urls: [
            { title: 'Dashboard', url: '/dashboard' },
            { title: 'Roles', url: '/roles' },
            { title: 'Create Role' }
          ]
        }
      },
      {
        path: 'edit/:id',
        component: RoleFormComponent,
        data: {
          title: 'Edit Role',
          urls: [
            { title: 'Dashboard', url: '/dashboard' },
            { title: 'Roles', url: '/roles' },
            { title: 'Edit Role' }
          ]
        }
      }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class RolesRoutingModule { }
