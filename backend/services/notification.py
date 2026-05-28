"""
Notification service for real-time alerts via WebSocket and Slack.
Handles decision broadcasting and multi-channel delivery.
"""

import asyncio
import json
import uuid
import aiohttp
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum

from backend.config import get_settings
from backend.utils.logging import get_logger

logger = get_logger("notification")
settings = get_settings()


class NotificationType(str, Enum):
    """Types of notifications."""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    WEBSOCKET = "websocket"
    IN_APP = "in_app"


@dataclass
class Decision:
    """Decision to broadcast."""
    id: str
    matched_incident: str
    symptom: str
    recommended_action: str
    confidence_score: float
    citations: List[str]
    latency_ms: int
    generated_at: str
    tenant_id: Optional[str] = None
    service_name: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


class WebSocketManager:
    """Manages WebSocket connections for real-time notifications."""
    
    def __init__(self):
        """Initialize WebSocket manager."""
        self.active_connections: Dict[str, List[asyncio.Queue]] = {}
        self.logger = logger
        self.logger.info("WebSocketManager initialized")
    
    async def connect(self, client_id: str) -> asyncio.Queue:
        """
        Register a new WebSocket client.
        
        Args:
            client_id: Unique client identifier
        
        Returns:
            Queue for sending messages to client
        """
        message_queue = asyncio.Queue()
        
        if client_id not in self.active_connections:
            self.active_connections[client_id] = []
        
        self.active_connections[client_id].append(message_queue)
        self.logger.info(f"WebSocket connected: {client_id} (total: {self.get_active_clients()})")
        
        return message_queue
    
    async def disconnect(self, client_id: str, queue: asyncio.Queue) -> None:
        """
        Unregister a WebSocket client.
        
        Args:
            client_id: Client identifier
            queue: Client's message queue
        """
        if client_id in self.active_connections:
            try:
                self.active_connections[client_id].remove(queue)
            except ValueError:
                pass
            
            if not self.active_connections[client_id]:
                del self.active_connections[client_id]
            
            self.logger.info(f"WebSocket disconnected: {client_id} (remaining: {self.get_active_clients()})")
    
    async def broadcast(self, message: Dict, exclude_client: Optional[str] = None) -> int:
        """
        Broadcast message to all connected clients.
        
        Args:
            message: Message to broadcast
            exclude_client: Optional client to exclude
        
        Returns:
            Number of clients that received message
        """
        count = 0
        message_json = json.dumps(message, default=str)
        
        for client_id, queues in list(self.active_connections.items()):
            if exclude_client and client_id == exclude_client:
                continue
            
            for queue in queues[:]:  # Copy to avoid modification during iteration
                try:
                    await queue.put(message_json)
                    count += 1
                except Exception as e:
                    self.logger.error(f"Error broadcasting to {client_id}: {e}")
        
        self.logger.debug(f"Broadcast message to {count} connections")
        return count
    
    async def broadcast_to_tenant(self, tenant_id: str, message: Dict) -> int:
        """
        Broadcast to all clients of a specific tenant.
        
        Args:
            tenant_id: Tenant identifier
            message: Message to send
        
        Returns:
            Number of clients notified
        """
        count = 0
        message_json = json.dumps(message, default=str)
        
        for client_id, queues in list(self.active_connections.items()):
            # Parse tenant from client_id (format: tenant_xxxxx)
            if client_id.startswith(f"{tenant_id}_") or not tenant_id:
                for queue in queues[:]:
                    try:
                        await queue.put(message_json)
                        count += 1
                    except Exception as e:
                        self.logger.error(f"Error broadcasting to {client_id}: {e}")
        
        self.logger.info(f"Broadcast to tenant {tenant_id}: {count} clients")
        return count
    
    def get_active_clients(self) -> int:
        """Get count of active connections."""
        return len(self.active_connections)
    
    def get_client_list(self) -> List[str]:
        """Get list of connected client IDs."""
        return list(self.active_connections.keys())


class SlackNotifier:
    """Sends notifications to Slack channels."""
    
    def __init__(self, webhook_url: str = ""):
        """
        Initialize Slack notifier.
        
        Args:
            webhook_url: Slack incoming webhook URL
        """
        self.webhook_url = webhook_url or settings.slack_webhook_url
        self.logger = logger
        self.enabled = bool(self.webhook_url)
        
        if self.enabled:
            self.logger.info("Slack notifier initialized")
        else:
            self.logger.debug("Slack webhook not configured")
    
    async def send_decision_alert(self, decision: Decision) -> bool:
        """
        Send decision alert to Slack.
        
        Args:
            decision: Decision to notify about
        
        Returns:
            True if successful
        """
        if not self.enabled:
            self.logger.debug("Slack notifications disabled")
            return False
        
        try:
            # Build Slack message
            payload = {
                "text": "🚨 Incident Mitigation Decision Generated",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"Incident Decision: {decision.matched_incident}",
                            "emoji": True
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Symptom:*\n{decision.symptom}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Confidence:*\n{decision.confidence_score:.2%}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Recommended Action:*\n```{decision.recommended_action}```"
                        }
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"⏱️ {decision.latency_ms}ms | ID: {decision.id}"
                            }
                        ]
                    }
                ]
            }
            
            # Send to Slack
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        self.logger.info(f"Slack alert sent: {decision.id}")
                        return True
                    else:
                        self.logger.error(f"Slack error {resp.status}")
                        return False
        
        except Exception as e:
            self.logger.error(f"Error sending Slack alert: {e}", exc_info=True)
            return False


class NotificationService:
    """Service for sending notifications through multiple channels."""
    
    def __init__(self):
        """Initialize notification service."""
        self.ws_manager = WebSocketManager()
        self.slack_notifier = SlackNotifier()
        self.logger = logger
        self.is_running = False
        self.notifications: Dict[str, Dict[str, Any]] = {}
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def notify_decision(
        self,
        decision: Decision,
        channels: List[str] = ["websocket", "slack"]
    ) -> bool:
        """
        Send decision notification through specified channels.
        
        Args:
            decision: Decision to notify about
            channels: List of channels (websocket, slack, email)
        
        Returns:
            True if at least one channel succeeded
        """
        success = False
        
        # WebSocket broadcast
        if "websocket" in channels:
            try:
                ws_count = await self.ws_manager.broadcast({
                    "type": "decision",
                    "data": decision.to_dict()
                })
                
                if ws_count > 0:
                    self.logger.info(f"Decision broadcasted to {ws_count} WebSocket clients")
                    success = True
            except Exception as e:
                self.logger.error(f"Error broadcasting via WebSocket: {e}")
        
        # Slack notification
        if "slack" in channels:
            try:
                if await self.slack_notifier.send_decision_alert(decision):
                    success = True
            except Exception as e:
                self.logger.error(f"Error sending Slack notification: {e}")
        
        return success
    
    async def notify_anomaly(
        self,
        tenant_id: str,
        service_name: str,
        error_type: str,
        spike_percentage: float,
        metric_name: str,
        channels: List[str] = ["websocket"]
    ) -> bool:
        """
        Send anomaly detected notification.
        
        Args:
            tenant_id: Affected tenant
            service_name: Affected service
            error_type: Error type
            spike_percentage: Spike percentage
            metric_name: Metric name
            channels: Notification channels
        
        Returns:
            True if successful
        """
        success = False
        
        # WebSocket broadcast
        if "websocket" in channels:
            try:
                await self.ws_manager.broadcast({
                    "type": "anomaly",
                    "tenant_id": tenant_id,
                    "service_name": service_name,
                    "error_type": error_type,
                    "spike_percentage": spike_percentage,
                    "metric_name": metric_name,
                    "timestamp": datetime.utcnow().isoformat()
                })
                success = True
            except Exception as e:
                self.logger.error(f"Error broadcasting anomaly: {e}")
        
        return success

    async def send_notification(
        self,
        notification_type: NotificationType,
        target: str,
        title: str,
        message: str,
        decision_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Send notification through specified channel."""
        notification_id = str(uuid.uuid4())[:8]
        
        try:
            if notification_type == NotificationType.EMAIL:
                success = await self._send_email(target, title, message)
            elif notification_type == NotificationType.SLACK:
                success = await self._send_slack(target, title, message)
            elif notification_type == NotificationType.WEBHOOK:
                success = await self._send_webhook(target, title, message)
            elif notification_type == NotificationType.IN_APP:
                success = True  # In-app notifications are always stored
            else:
                success = False
            
            # Store notification record
            notification = {
                "id": notification_id,
                "type": notification_type.value,
                "target": target,
                "title": title,
                "message": message,
                "decision_id": decision_id,
                "status": "sent" if success else "failed",
                "sent_at": datetime.utcnow().isoformat(),
                "metadata": metadata or {}
            }
            
            self.notifications[notification_id] = notification
            logger.info(f"Notification {notification_id} sent via {notification_type.value}")
            
            return notification_id
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
            raise
    
    async def _send_email(self, target: str, title: str, message: str) -> bool:
        """Send email notification."""
        try:
            # In production, use SMTP or email service
            logger.debug(f"Email notification to {target}: {title}")
            # For now, just log it
            return True
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False
    
    async def _send_slack(self, webhook_url: str, title: str, message: str) -> bool:
        """Send Slack notification via webhook."""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            payload = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": title
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message
                        }
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"Sent at {datetime.utcnow().isoformat()}"
                            }
                        ]
                    }
                ]
            }
            
            async with self.session.post(webhook_url, json=payload) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")
            return False
    
    async def _send_webhook(self, webhook_url: str, title: str, message: str) -> bool:
        """Send custom webhook notification."""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            payload = {
                "title": title,
                "message": message,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            async with self.session.post(webhook_url, json=payload) as resp:
                return resp.status in [200, 201, 202]
        except Exception as e:
            logger.error(f"Error sending webhook notification: {e}")
            return False
    
    def get_notification(self, notification_id: str) -> Optional[Dict[str, Any]]:
        """Get notification by ID."""
        return self.notifications.get(notification_id)
    
    def get_notifications_by_decision(self, decision_id: str) -> List[Dict[str, Any]]:
        """Get all notifications for a decision."""
        return [
            n for n in self.notifications.values()
            if n.get('decision_id') == decision_id
        ]
    
    def get_recent_notifications(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent notifications."""
        notifications = list(self.notifications.values())
        notifications.sort(key=lambda n: n['sent_at'], reverse=True)
        return notifications[:limit]
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()


# Global notification service instance
_notification_service: Optional[NotificationService] = None


def get_notification_service() -> NotificationService:
    """Get or create global notification service instance."""
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service
