import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class KafkaService {
  private apiUrl = 'http://localhost:8080/control';

  constructor(private http: HttpClient) {}

  pause(topicId: string, key: string): Observable<void> {
    return this.http.post<void>(`${this.apiUrl}/${topicId}/pause/${key}`, {});
  }

  resume(topicId: string, key: string): Observable<void> {
    return this.http.post<void>(`${this.apiUrl}/${topicId}/resume/${key}`, {});
  }

  sendEvent(topicId: string, key: string, data: string): Observable<{message: string, eventId: string}> {
    const params = new HttpParams()
      .set('key', key)
      .set('data', data);
    return this.http.post<{message: string, eventId: string}>(`${this.apiUrl}/${topicId}/send`, {}, { params });
  }

  getOutputMessages(topicId: string, limit: number = 50): Observable<any[]> {
    const params = new HttpParams().set('limit', limit.toString());
    return this.http.get<any[]>(`${this.apiUrl}/${topicId}/output-messages`, { params });
  }

  getInputMessages(topicId: string, limit: number = 50): Observable<any[]> {
    const params = new HttpParams().set('limit', limit.toString());
    return this.http.get<any[]>(`${this.apiUrl}/${topicId}/input-messages`, { params });
  }

  getKeyStatus(topicId: string, key: string): Observable<{key: string, status: string}> {
    return this.http.get<{key: string, status: string}>(`${this.apiUrl}/${topicId}/status/${key}`);
  }

  getBufferedEvents(topicId: string, key: string): Observable<any[]> {
    return this.http.get<any[]>(`${this.apiUrl}/${topicId}/buffer/${key}`);
  }
}

