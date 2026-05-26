'use client';

import { useEffect, useState } from 'react';
import { format } from 'date-fns';

interface Decision {
  id: string;
  matched_incident: string;
  symptom: string;
  recommended_action: string;
  confidence_score: number;
  citations: string[];
  latency_ms: number;
  generated_at: string;
  tenant_id?: string;
  service_name?: string;
}

interface Anomaly {
  type: string;
  tenant_id: string;
  service_name: string;
  error_type: string;
  spike_percentage: number;
  metric_name: string;
  timestamp: string;
}

export function useIncidentStream() {
  const [decisions, setDecisions] = useState<Decision[]>([]);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [connectionCount, setConnectionCount] = useState(0);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/incidents');

    ws.onopen = () => {
      setIsConnected(true);
    };

    ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);

        if (message.type === 'decision') {
          setDecisions((prev) => [message.data, ...prev].slice(0, 50));
          setLastUpdate(new Date());
        } else if (message.type === 'anomaly') {
          setAnomalies((prev) => [message, ...prev].slice(0, 50));
        } else if (message.type === 'connected') {
          console.log('Connected to incident stream:', message.client_id);
        }
      } catch (e) {
        console.error('Failed to parse message:', e);
      }
    };

    ws.onerror = () => {
      setIsConnected(false);
    };

    ws.onclose = () => {
      setIsConnected(false);
    };

    return () => {
      ws.close();
    };
  }, []);

  useEffect(() => {
    const timer = setInterval(async () => {
      try {
        const response = await fetch('http://localhost:8000/ws/stats');
        const data = await response.json();
        setConnectionCount(data.active_connections);
      } catch (e) {
        console.error('Failed to fetch stats:', e);
      }
    }, 5000);

    return () => clearInterval(timer);
  }, []);

  return {
    decisions,
    anomalies,
    isConnected,
    connectionCount,
    lastUpdate,
  };
}

export function IncidentStreamDisplay({ decisions }: { decisions: Decision[] }) {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold">Recent Decisions</h2>
      <div className="space-y-2">
        {decisions.length === 0 ? (
          <p className="text-muted-foreground">No decisions yet. Waiting for anomalies...</p>
        ) : (
          decisions.map((decision) => (
            <div
              key={decision.id}
              className="border rounded-lg p-4 bg-card hover:bg-accent transition-colors"
            >
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <span className="font-semibold text-lg">{decision.matched_incident}</span>
                    <span className="text-xs bg-green-100 text-green-800 px-2 py-1 rounded">
                      {(decision.confidence_score * 100).toFixed(0)}% confident
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground mt-1">{decision.symptom}</p>
                  <p className="text-base font-medium mt-2 text-blue-600">
                    Action: {decision.recommended_action}
                  </p>
                  {decision.citations && decision.citations.length > 0 && (
                    <div className="mt-2 text-xs text-muted-foreground bg-muted p-2 rounded">
                      <strong>Evidence:</strong> {decision.citations[0]}
                    </div>
                  )}
                </div>
                <div className="text-right">
                  <div className="text-xs text-muted-foreground">
                    {format(new Date(decision.generated_at), 'HH:mm:ss')}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {decision.latency_ms}ms
                  </div>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export function AnomalyStreamDisplay({ anomalies }: { anomalies: Anomaly[] }) {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold">Detected Anomalies</h2>
      <div className="space-y-2">
        {anomalies.length === 0 ? (
          <p className="text-muted-foreground">No anomalies detected yet.</p>
        ) : (
          anomalies.map((anomaly, idx) => (
            <div
              key={idx}
              className="border border-yellow-200 rounded-lg p-4 bg-yellow-50 dark:bg-yellow-950"
            >
              <div className="flex items-center justify-between">
                <div>
                  <p className="font-semibold text-yellow-900 dark:text-yellow-200">
                    {anomaly.error_type}
                  </p>
                  <p className="text-sm text-yellow-700 dark:text-yellow-300">
                    {anomaly.tenant_id} / {anomaly.service_name}
                  </p>
                  <p className="text-base font-bold text-red-600 mt-1">
                    Spike: +{anomaly.spike_percentage.toFixed(1)}%
                  </p>
                </div>
                <div className="text-right text-xs text-yellow-600 dark:text-yellow-400">
                  {format(new Date(anomaly.timestamp), 'HH:mm:ss')}
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
