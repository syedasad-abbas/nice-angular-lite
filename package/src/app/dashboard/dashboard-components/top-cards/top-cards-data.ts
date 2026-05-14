export interface topcard {
  bgcolor: string,
  icon: string,
  title: string,
  subtitle: string
}

export const topcards: topcard[] = [
  {
    bgcolor: 'success',
    icon: 'bi bi-people',
    title: '0',
    subtitle: 'Total Users'
  },
  {
    bgcolor: 'success',
    icon: 'bi bi-person-check',
    title: '0',
    subtitle: 'Logged-in Users'
  },
  {
    bgcolor: 'warning',
    icon: 'bi bi-telephone-inbound',
    title: '0',
    subtitle: 'Answered By Agents'
  },
  {
    bgcolor: 'info',
    icon: 'bi bi-bag',
    title: '210',
    subtitle: 'Weekly Sales'
  },
]
