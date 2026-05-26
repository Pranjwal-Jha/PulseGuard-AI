"""Notification management API routes."""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from backend.services.notification import get_notification_service, NotificationType
from backend.utils.logging import get_logger

router = APIRouter(prefix="/notifications", tags=["Notifications"])
logger = get_logger("notifications_route")


class NotificationSendRequest(BaseModel):
    """Request model for sending notification."""
    type: str  # email, slack, webhook, in_app
    target: str  # email address, Slack webhook URL, etc.
    title: str
    message: str
    decision_id: Optional[str] = None
    metadata: Optional[dict] = None


@router.post("", response_model=dict)
async def send_notification(request: NotificationSendRequest):
    """Send a notification."""
    try:
        # Validate notification type
        try:
            notif_type = NotificationType(request.type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid notification type: {request.type}")
        
        service = get_notification_service()
        notification_id = await service.send_notification(
            notification_type=notif_type,
            target=request.target,
            title=request.title,
            message=request.message,
            decision_id=request.decision_id,
            metadata=request.metadata
        )
        
        return {
            "notification_id": notification_id,
            "type": request.type,
            "status": "sent"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{notification_id}", response_model=dict)
async def get_notification(notification_id: str):
    """Get notification by ID."""
    try:
        service = get_notification_service()
        notification = service.get_notification(notification_id)
        
        if not notification:
            raise HTTPException(status_code=404, detail=f"Notification {notification_id} not found")
        
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=list)
async def get_recent_notifications(limit: int = Query(20, ge=1, le=100)):
    """Get recent notifications."""
    try:
        service = get_notification_service()
        notifications = service.get_recent_notifications(limit=limit)
        return notifications
    except Exception as e:
        logger.error(f"Error retrieving notifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/decision/{decision_id}", response_model=list)
async def get_decision_notifications(decision_id: str):
    """Get notifications for a specific decision."""
    try:
        service = get_notification_service()
        notifications = service.get_notifications_by_decision(decision_id)
        return notifications
    except Exception as e:
        logger.error(f"Error retrieving decision notifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))
