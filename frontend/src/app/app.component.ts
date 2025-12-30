import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { KafkaService } from './kafka.service';

interface LogEntry {
  timestamp: Date;
  message: string;
  type: 'success' | 'error' | 'info';
}

interface ConsumedMessage {
  key: string;
  eventId: string;
  data: string;
  timestamp: number;
  partition: number;
  offset: number;
  consumedAt: number;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnInit, OnDestroy {
  selectedTopicId: string = 'demo';
  topics: string[] = ['demo', 'orders', 'payments'];

  // Producer fields
  producerKey: string = '';
  producerData: string = '';

  // Control fields
  controlKey: string = '';

  // Logs
  logs: LogEntry[] = [];

  // Dashboard Data
  inputMessages: any[] = [];
  outputMessages: any[] = [];
  bufferedEvents: any[] = [];
  keyStatus: string = 'UNKNOWN';

  isLoading: boolean = false;
  autoRefresh: boolean = false;
  refreshInterval: any;

  constructor(private kafkaService: KafkaService) { }

  ngOnInit() {
    this.refreshDashboard();
  }

  ngOnDestroy() {
    this.stopAutoRefresh();
  }

  refreshDashboard() {
    this.isLoading = true;

    // 1. Fetch Input Messages
    this.kafkaService.getInputMessages(this.selectedTopicId, 20).subscribe({
      next: (msgs) => this.inputMessages = msgs,
      error: (err) => this.addLog(`Error fetching inputs: ${err.message}`, 'error')
    });

    // 2. Fetch Output Messages
    this.kafkaService.getOutputMessages(this.selectedTopicId, 20).subscribe({
      next: (msgs) => this.outputMessages = msgs,
      error: (err) => this.addLog(`Error fetching outputs: ${err.message}`, 'error')
    });

    // 3. Fetch Key Specific Data (Status & Buffer) if key is entered
    if (this.controlKey) {
      this.kafkaService.getKeyStatus(this.selectedTopicId, this.controlKey).subscribe({
        next: (res) => this.keyStatus = res.status,
        error: () => this.keyStatus = 'UNKNOWN'
      });

      this.kafkaService.getBufferedEvents(this.selectedTopicId, this.controlKey).subscribe({
        next: (events) => this.bufferedEvents = events,
        error: () => this.bufferedEvents = []
      });
    } else {
      this.keyStatus = 'UNKNOWN';
      this.bufferedEvents = [];
    }

    this.isLoading = false;
  }

  toggleAutoRefresh() {
    if (this.autoRefresh) {
      this.refreshInterval = setInterval(() => this.refreshDashboard(), 2000);
    } else {
      this.stopAutoRefresh();
    }
  }

  stopAutoRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
  }

  sendEvent() {
    if (!this.producerKey || !this.producerData) {
      this.addLog('Please enter both key and data', 'error');
      return;
    }
    this.kafkaService.sendEvent(this.selectedTopicId, this.producerKey, this.producerData)
      .subscribe({
        next: (res) => {
          this.addLog(`Event sent: ${res.eventId}`, 'success');
          this.refreshDashboard();
        },
        error: (err) => this.addLog(`Error sending: ${err.message}`, 'error')
      });
  }

  pause() {
    if (!this.controlKey) return;
    this.kafkaService.pause(this.selectedTopicId, this.controlKey).subscribe({
      next: () => {
        this.addLog(`Paused key: ${this.controlKey}`, 'info');
        setTimeout(() => this.refreshDashboard(), 500);
      },
      error: (err) => this.addLog(`Error pausing: ${err.message}`, 'error')
    });
  }

  resume() {
    if (!this.controlKey) return;
    this.kafkaService.resume(this.selectedTopicId, this.controlKey).subscribe({
      next: () => {
        this.addLog(`Resumed key: ${this.controlKey}`, 'success');
        setTimeout(() => this.refreshDashboard(), 500); // Wait for processing
      },
      error: (err) => this.addLog(`Error resuming: ${err.message}`, 'error')
    });
  }

  onTopicChange() {
    this.refreshDashboard();
  }

  private addLog(message: string, type: 'success' | 'error' | 'info') {
    this.logs.unshift({
      timestamp: new Date(),
      message: `[${this.selectedTopicId}] ${message}`,
      type
    });
    if (this.logs.length > 50) this.logs = this.logs.slice(0, 50);
  }

  clearLogs() {
    this.logs = [];
  }
}

