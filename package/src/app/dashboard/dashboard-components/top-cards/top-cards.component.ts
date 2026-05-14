import { Component, OnInit } from '@angular/core';
import { topcard, topcards } from './top-cards-data';
import { UserService } from 'src/app/users/user.service';

@Component({
  selector: 'app-top-cards',
  templateUrl: './top-cards.component.html'
})
export class TopCardsComponent implements OnInit {
  topcards: topcard[];

  constructor(private userService: UserService) {
    this.topcards = topcards;
  }

  ngOnInit(): void {
    this.userService.getDashboardStats().subscribe({
      next: (stats) => {
        this.topcards[0].title = String(stats.totalUsers);
        this.topcards[0].subtitle = 'Total Users';
        this.topcards[0].icon = 'bi bi-people';
        this.topcards[0].bgcolor = 'primary';

        this.topcards[1].title = String(stats.loggedInUsers);
        this.topcards[1].subtitle = 'Logged-in Users';
        this.topcards[1].icon = 'bi bi-person-check';
        this.topcards[1].bgcolor = 'success';

        this.topcards[2].title = String(stats.answeredAgentCalls);
        this.topcards[2].subtitle = 'Answered By Agents';
        this.topcards[2].icon = 'bi bi-telephone-inbound';
        this.topcards[2].bgcolor = 'warning';
      },
      error: () => {
        this.topcards[0].title = '0';
        this.topcards[0].subtitle = 'Total Users';

        this.topcards[1].title = '0';
        this.topcards[1].subtitle = 'Logged-in Users';

        this.topcards[2].title = '0';
        this.topcards[2].subtitle = 'Answered By Agents';
      }
    });
  }
}
