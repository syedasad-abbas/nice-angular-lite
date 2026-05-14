import { RouteInfo } from './sidebar.metadata';

export const ROUTES: RouteInfo[] = [

  {
    path: '/dashboard',
    title: 'Dashboard',
    icon: 'bi bi-speedometer2',
    class: '',
    extralink: false,
    submenu: []
  },
  {
    path: '/users',          // The URL route it should navigate to
    title: 'Users',          // The text that will appear in the sidebar
    icon: 'bi bi-person',    // The Bootstrap icon class for the users icon
    class: '',
    extralink: false,
    submenu: []
  },
  {
    path: '/dialer',
    title: 'Dialer',
    icon: 'bi bi-telephone',
    class: '',
    extralink: false,
    submenu: []
  },
  {
    path: '/roles',
    title: 'Roles',
    icon: 'bi bi-shield-lock',
    class: '',
    extralink: false,
    submenu: []
  },
  {
    path: '/permissions',
    title: 'Permissions',
    icon: 'bi bi-key',
    class: '',
    extralink: false,
    submenu: []
  }

];
