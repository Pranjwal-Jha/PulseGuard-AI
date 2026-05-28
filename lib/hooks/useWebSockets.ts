import { useEffect, useRef, useState } from 'react';
import { useIncidentStore } from '../store/useStore';

export function useWebSockets(url: string) {
  const [isConnected, setIsConnected] = useState(false);
  const ws = useRef<WebSocket | null>(null);
  const addIncident = useIncidentStore((state) => state.addIncident);

  useEffect(() => {
    let reconnectInterval: NodeJS.Timeout;

    const connect = () => {
      ws.current = new WebSocket(url);

      ws.current.onopen = () => {
        setIsConnected(true);
        console.log('WebSocket connected');
      };

      ws.current.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'incident' || data.type === 'anomaly') {
            addIncident(data.payload);
          }
        } catch (e) {
          console.error('Error parsing WS message', e);
        }
      };

      ws.current.onclose = () => {
        setIsConnected(false);
        console.log('WebSocket disconnected. Reconnecting in 5s...');
        reconnectInterval = setTimeout(connect, 5000);
      };

      ws.current.onerror = (err) => {
        console.error('WebSocket error:', err);
        ws.current?.close();
      };
    };

    connect();

    return () => {
      clearTimeout(reconnectInterval);
      ws.current?.close();
    };
  }, [url, addIncident]);

  return { isConnected };
}
