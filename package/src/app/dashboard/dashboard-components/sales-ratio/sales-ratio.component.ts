import { Component, OnInit, ViewChild } from '@angular/core';
import {
  ApexAxisChartSeries,
  ApexChart,
  ChartComponent,
  ApexDataLabels,
  ApexYAxis,
  ApexLegend,
  ApexXAxis,
  ApexTooltip,
  ApexTheme,
  ApexGrid
} from 'ng-apexcharts';
import { UserService, User, AnsweredCallsRange } from 'src/app/users/user.service';

export type salesChartOptions = {
  series: ApexAxisChartSeries | any;
  chart: ApexChart | any;
  xaxis: ApexXAxis | any;
  yaxis: ApexYAxis | any;
  stroke: any;
  theme: ApexTheme | any;
  tooltip: ApexTooltip | any;
  dataLabels: ApexDataLabels | any;
  legend: ApexLegend | any;
  colors: string[] | any;
  markers: any;
  grid: ApexGrid | any;
};

@Component({
  selector: 'app-sales-ratio',
  templateUrl: './sales-ratio.component.html'
})
export class SalesRatioComponent implements OnInit {

  @ViewChild("chart") chart: ChartComponent = Object.create(null);
  public salesChartOptions: Partial<salesChartOptions>;
  users: User[] = [];
  selectedUserId = 'all';
  selectedRange: AnsweredCallsRange = '7d';
  constructor(private userService: UserService) {
    this.salesChartOptions = {
      series: [
        {
          name: "Answered Calls",
          data: [0, 0, 0, 0, 0, 0, 0],
        },
      ],
      chart: {
        fontFamily: 'Rubik,sans-serif',
        height: 250,
        type: 'line',
        toolbar: {
          show: false
        }
      },
      dataLabels: {
        enabled: false
      },
      colors: ["#137eff", "#6c757d"],
      stroke: {
        curve: 'smooth',
        width: '2',
      },
      grid: {
        strokeDashArray: 3,
      },
      markers: {
        size: 3
      },
      xaxis: {
        categories: [],
      },
      tooltip: {
        theme: 'dark'
      }
    };
  }

  ngOnInit(): void {
    this.userService.getUsers().subscribe({
      next: (users) => {
        this.users = users || [];
      },
      error: () => {
        this.users = [];
      }
    });
    this.loadSeries();
  }

  onFiltersChanged(): void {
    this.loadSeries();
  }

  private loadSeries(): void {
    this.userService.getAnsweredCallsSeries(this.selectedRange, this.selectedUserId).subscribe({
      next: (data) => {
        this.salesChartOptions.series = [
          { name: 'Answered Calls', data: data.series || [] }
        ];
        this.salesChartOptions.xaxis = {
          ...(this.salesChartOptions.xaxis || {}),
          categories: data.labels || [],
        };
      },
      error: () => {
        this.salesChartOptions.series = [{ name: 'Answered Calls', data: [0, 0, 0, 0, 0, 0, 0] }];
        this.salesChartOptions.xaxis = {
          ...(this.salesChartOptions.xaxis || {}),
          categories: ['-', '-', '-', '-', '-', '-', '-'],
        };
      }
    });
  }

}
