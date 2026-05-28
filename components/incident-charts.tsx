"use client";

import { useIncidentStore } from '@/lib/store/useStore';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

export function IncidentCharts() {
  const incidents = useIncidentStore((state) => state.activeIncidents);
  
  // Transform data for the chart (just a simple example)
  const chartData = incidents.map((inc, index) => ({
    time: new Date(inc.timestamp || Date.now()).toLocaleTimeString(),
    spike: inc.spike_percentage || 0,
    name: inc.metric_name || `Incident ${index}`
  })).reverse(); // Oldest first

  if (incidents.length === 0) {
    return (
      <Card>
        <CardHeader><CardTitle>Real-time Anomalies</CardTitle></CardHeader>
        <CardContent className="flex items-center justify-center h-[300px] text-muted-foreground">
          Waiting for telemetry anomalies...
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader><CardTitle>Real-time Anomalies</CardTitle></CardHeader>
      <CardContent className="h-[300px]">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" />
            <YAxis label={{ value: 'Spike %', angle: -90, position: 'insideLeft' }} />
            <Tooltip />
            <Legend />
            <Line 
              type="monotone" 
              dataKey="spike" 
              stroke="#ef4444" 
              activeDot={{ r: 8 }} 
              name="Anomaly Spike %" 
            />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}
