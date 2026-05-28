"use client";

import { useWebSockets } from '@/lib/hooks/useWebSockets';
import { useIncidentStore } from '@/lib/store/useStore';
import { IncidentCharts } from '@/components/incident-charts';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';

export default function DashboardPage() {
  // Connect to the real websocket endpoint
  const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/api/v1/ws';
  const { isConnected } = useWebSockets(`${wsUrl}/incidents`);
  const incidents = useIncidentStore((state) => state.activeIncidents);

  return (
    <div className="container mx-auto py-8 space-y-8">
      <div className="flex justify-between items-center">
        <h1 className="text-4xl font-bold tracking-tight text-primary">PulseGuard AI Dashboard</h1>
        <Badge variant={isConnected ? "default" : "destructive"}>
          {isConnected ? "Live Connected" : "Disconnected"}
        </Badge>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
        <div className="col-span-1 md:col-span-2">
          <IncidentCharts />
        </div>
        
        <div className="col-span-1">
          <Card className="h-[400px] flex flex-col">
            <CardHeader>
              <CardTitle>Recent Alerts</CardTitle>
              <CardDescription>Live streaming decisions from backend.</CardDescription>
            </CardHeader>
            <CardContent className="flex-1 overflow-hidden p-0">
              <ScrollArea className="h-full px-6">
                <div className="space-y-4 pb-6">
                  {incidents.length === 0 ? (
                     <div className="text-muted-foreground text-center py-4">No recent alerts.</div>
                  ) : (
                    incidents.map((incident, i) => (
                      <div key={i} className="border p-4 rounded-lg bg-card/50 shadow-sm border-l-4 border-l-red-500">
                        <div className="flex justify-between items-start mb-2">
                          <h4 className="font-semibold">{incident.error_type || "Unknown Error"}</h4>
                          <Badge variant="outline">{incident.tenant_id}</Badge>
                        </div>
                        <p className="text-sm text-muted-foreground mb-2">
                          Spike: {incident.spike_percentage}% on {incident.metric_name}
                        </p>
                        {incident.recommended_action && (
                          <div className="bg-muted p-2 rounded text-sm mt-2">
                            <span className="font-semibold text-primary">AI Action: </span>
                            {incident.recommended_action}
                          </div>
                        )}
                      </div>
                    ))
                  )}
                </div>
              </ScrollArea>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
