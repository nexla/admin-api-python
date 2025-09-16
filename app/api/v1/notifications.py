"""
Notifications and Alerts API endpoints - Real-time messaging and alert system.
Handles user notifications, system alerts, and communication preferences.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import (
    RBACService, SystemPermissions, check_admin_permission
)
from ...models.user import User
from ...services.audit_service import AuditService, AuditActions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/notifications", tags=["Notifications & Alerts"])


# Pydantic models for notifications
class NotificationCreate(BaseModel):
    recipient_ids: List[int]
    title: str = Field(..., min_length=1, max_length=255)
    message: str = Field(..., min_length=1, max_length=2000)
    notification_type: str = Field(default="info", pattern="^(info|success|warning|error|critical)$")
    category: str = Field(default="general", max_length=50)
    action_url: Optional[str] = None
    expires_at: Optional[datetime] = None
    send_email: bool = False
    send_push: bool = False

class NotificationResponse(BaseModel):
    id: int
    recipient_id: int
    sender_id: Optional[int]
    title: str
    message: str
    notification_type: str
    category: str
    action_url: Optional[str]
    is_read: bool
    is_archived: bool
    created_at: datetime
    read_at: Optional[datetime]
    expires_at: Optional[datetime]

class AlertRule(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    condition_type: str = Field(..., pattern="^(metric_threshold|event_pattern|schedule)$")
    condition_config: Dict[str, Any]
    severity: str = Field(..., pattern="^(info|warning|error|critical)$")
    notification_channels: List[str] = Field(default=["email"])
    recipients: List[int] = Field(default=[])
    is_active: bool = True
    cooldown_minutes: int = Field(default=60, ge=0, le=1440)

class AlertRuleResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    condition_type: str
    condition_config: Dict[str, Any]
    severity: str
    notification_channels: List[str]
    recipients: List[int]
    is_active: bool
    cooldown_minutes: int
    last_triggered: Optional[datetime]
    trigger_count: int
    created_by: int
    created_at: datetime
    updated_at: datetime

class AlertInstance(BaseModel):
    id: int
    rule_id: int
    rule_name: str
    title: str
    message: str
    severity: str
    status: str
    triggered_at: datetime
    resolved_at: Optional[datetime]
    acknowledged_by: Optional[int]
    acknowledged_at: Optional[datetime]
    details: Optional[Dict[str, Any]]

class NotificationPreferences(BaseModel):
    email_enabled: bool = True
    push_enabled: bool = True
    sms_enabled: bool = False
    categories: Dict[str, bool] = Field(default_factory=dict)
    quiet_hours_start: Optional[str] = None  # HH:MM format
    quiet_hours_end: Optional[str] = None
    timezone: str = "UTC"

class NotificationStats(BaseModel):
    total_notifications: int
    unread_notifications: int
    notifications_today: int
    notifications_by_type: Dict[str, int]
    notifications_by_category: Dict[str, int]


# Mock notification storage (would be replaced with database models)
notifications_store: List[Dict[str, Any]] = []
alert_rules_store: List[Dict[str, Any]] = []
alert_instances_store: List[Dict[str, Any]] = []
user_preferences_store: Dict[int, Dict[str, Any]] = {}


# Notification management
@router.get("/", response_model=List[NotificationResponse], summary="Get User Notifications")
async def get_notifications(
    unread_only: bool = Query(False, description="Show only unread notifications"),
    category: Optional[str] = Query(None, description="Filter by category"),
    notification_type: Optional[str] = Query(None, description="Filter by type"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get notifications for the current user.
    """
    try:
        # Filter notifications for current user
        user_notifications = [
            n for n in notifications_store 
            if n.get("recipient_id") == current_user.id
        ]
        
        # Apply filters
        if unread_only:
            user_notifications = [n for n in user_notifications if not n.get("is_read", False)]
        
        if category:
            user_notifications = [n for n in user_notifications if n.get("category") == category]
        
        if notification_type:
            user_notifications = [n for n in user_notifications if n.get("notification_type") == notification_type]
        
        # Sort by creation date (newest first)
        user_notifications.sort(key=lambda x: x.get("created_at", datetime.min), reverse=True)
        
        # Apply pagination
        paginated_notifications = user_notifications[offset:offset + limit]
        
        # Convert to response format
        response_notifications = []
        for n in paginated_notifications:
            response_notifications.append(NotificationResponse(
                id=n["id"],
                recipient_id=n["recipient_id"],
                sender_id=n.get("sender_id"),
                title=n["title"],
                message=n["message"],
                notification_type=n["notification_type"],
                category=n["category"],
                action_url=n.get("action_url"),
                is_read=n.get("is_read", False),
                is_archived=n.get("is_archived", False),
                created_at=n["created_at"],
                read_at=n.get("read_at"),
                expires_at=n.get("expires_at")
            ))
        
        return response_notifications
        
    except Exception as e:
        logger.error(f"Failed to get notifications: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve notifications"
        )


@router.post("/", summary="Create Notification")
async def create_notification(
    notification: NotificationCreate,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create and send notifications to specified recipients.
    """
    try:
        # Validate recipients exist
        recipients = db.query(User).filter(User.id.in_(notification.recipient_ids)).all()
        if len(recipients) != len(notification.recipient_ids):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Some recipients not found"
            )
        
        created_notifications = []
        
        # Create notification for each recipient
        for recipient in recipients:
            notification_id = len(notifications_store) + 1
            
            notification_data = {
                "id": notification_id,
                "recipient_id": recipient.id,
                "sender_id": current_user.id,
                "title": notification.title,
                "message": notification.message,
                "notification_type": notification.notification_type,
                "category": notification.category,
                "action_url": notification.action_url,
                "is_read": False,
                "is_archived": False,
                "created_at": datetime.utcnow(),
                "read_at": None,
                "expires_at": notification.expires_at
            }
            
            notifications_store.append(notification_data)
            created_notifications.append(notification_id)
            
            # Send email if requested
            if notification.send_email:
                background_tasks.add_task(
                    _send_email_notification,
                    recipient.email,
                    notification.title,
                    notification.message
                )
            
            # Send push notification if requested
            if notification.send_push:
                background_tasks.add_task(
                    _send_push_notification,
                    recipient.id,
                    notification.title,
                    notification.message
                )
        
        # Log audit action
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action=AuditActions.CREATE,
            resource_type="notification",
            details={
                "recipients": notification.recipient_ids,
                "category": notification.category,
                "type": notification.notification_type
            }
        )
        
        logger.info(f"Created {len(created_notifications)} notifications by user {current_user.id}")
        
        return {
            "message": f"Created {len(created_notifications)} notifications",
            "notification_ids": created_notifications,
            "recipients": len(recipients)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create notification: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create notification"
        )


@router.put("/{notification_id}/read", summary="Mark Notification as Read")
async def mark_notification_read(
    notification_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Mark a notification as read.
    """
    try:
        # Find notification
        notification = None
        for n in notifications_store:
            if n["id"] == notification_id and n["recipient_id"] == current_user.id:
                notification = n
                break
        
        if not notification:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Notification not found"
            )
        
        # Mark as read
        notification["is_read"] = True
        notification["read_at"] = datetime.utcnow()
        
        return {"message": "Notification marked as read"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to mark notification as read: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to mark notification as read"
        )


@router.put("/mark-all-read", summary="Mark All Notifications as Read")
async def mark_all_notifications_read(
    category: Optional[str] = Query(None, description="Mark only specific category"),
    current_user: User = Depends(get_current_active_user)
):
    """
    Mark all user notifications as read.
    """
    try:
        marked_count = 0
        
        for notification in notifications_store:
            if (notification["recipient_id"] == current_user.id and 
                not notification.get("is_read", False)):
                
                # Check category filter
                if category and notification.get("category") != category:
                    continue
                
                notification["is_read"] = True
                notification["read_at"] = datetime.utcnow()
                marked_count += 1
        
        return {
            "message": f"Marked {marked_count} notifications as read",
            "marked_count": marked_count
        }
        
    except Exception as e:
        logger.error(f"Failed to mark all notifications as read: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to mark notifications as read"
        )


@router.delete("/{notification_id}", summary="Delete Notification")
async def delete_notification(
    notification_id: int,
    current_user: User = Depends(get_current_active_user)
):
    """
    Delete a notification.
    """
    try:
        # Find and remove notification
        for i, notification in enumerate(notifications_store):
            if (notification["id"] == notification_id and 
                notification["recipient_id"] == current_user.id):
                notifications_store.pop(i)
                return {"message": "Notification deleted"}
        
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Notification not found"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete notification: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete notification"
        )


# Alert management
@router.get("/alerts/rules", response_model=List[AlertRuleResponse], summary="Get Alert Rules")
async def get_alert_rules(
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get alert rules (admin only).
    """
    try:
        filtered_rules = alert_rules_store.copy()
        
        if is_active is not None:
            filtered_rules = [r for r in filtered_rules if r.get("is_active") == is_active]
        
        if severity:
            filtered_rules = [r for r in filtered_rules if r.get("severity") == severity]
        
        # Convert to response format
        response_rules = []
        for rule in filtered_rules:
            response_rules.append(AlertRuleResponse(
                id=rule["id"],
                name=rule["name"],
                description=rule.get("description"),
                condition_type=rule["condition_type"],
                condition_config=rule["condition_config"],
                severity=rule["severity"],
                notification_channels=rule["notification_channels"],
                recipients=rule["recipients"],
                is_active=rule["is_active"],
                cooldown_minutes=rule["cooldown_minutes"],
                last_triggered=rule.get("last_triggered"),
                trigger_count=rule.get("trigger_count", 0),
                created_by=rule["created_by"],
                created_at=rule["created_at"],
                updated_at=rule["updated_at"]
            ))
        
        return response_rules
        
    except Exception as e:
        logger.error(f"Failed to get alert rules: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve alert rules"
        )


@router.post("/alerts/rules", summary="Create Alert Rule")
async def create_alert_rule(
    rule: AlertRule,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_admin_permission())
):
    """
    Create a new alert rule (admin only).
    """
    try:
        rule_id = len(alert_rules_store) + 1
        
        rule_data = {
            "id": rule_id,
            "name": rule.name,
            "description": rule.description,
            "condition_type": rule.condition_type,
            "condition_config": rule.condition_config,
            "severity": rule.severity,
            "notification_channels": rule.notification_channels,
            "recipients": rule.recipients,
            "is_active": rule.is_active,
            "cooldown_minutes": rule.cooldown_minutes,
            "last_triggered": None,
            "trigger_count": 0,
            "created_by": current_user.id,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        alert_rules_store.append(rule_data)
        
        # Log audit action
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action=AuditActions.CREATE,
            resource_type="alert_rule",
            resource_id=rule_id,
            resource_name=rule.name,
            details={"severity": rule.severity, "condition_type": rule.condition_type}
        )
        
        logger.info(f"Alert rule created: {rule.name} by user {current_user.id}")
        
        return {
            "message": "Alert rule created successfully",
            "rule_id": rule_id,
            "name": rule.name
        }
        
    except Exception as e:
        logger.error(f"Failed to create alert rule: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create alert rule"
        )


@router.get("/alerts/instances", response_model=List[AlertInstance], summary="Get Alert Instances")
async def get_alert_instances(
    severity: Optional[str] = Query(None, description="Filter by severity"),
    status: Optional[str] = Query(None, description="Filter by status"),
    rule_id: Optional[int] = Query(None, description="Filter by rule ID"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get alert instances (admin only).
    """
    try:
        filtered_alerts = alert_instances_store.copy()
        
        if severity:
            filtered_alerts = [a for a in filtered_alerts if a.get("severity") == severity]
        
        if status:
            filtered_alerts = [a for a in filtered_alerts if a.get("status") == status]
        
        if rule_id:
            filtered_alerts = [a for a in filtered_alerts if a.get("rule_id") == rule_id]
        
        # Sort by triggered date (newest first)
        filtered_alerts.sort(key=lambda x: x.get("triggered_at", datetime.min), reverse=True)
        
        # Apply pagination
        paginated_alerts = filtered_alerts[offset:offset + limit]
        
        # Convert to response format
        response_alerts = []
        for alert in paginated_alerts:
            response_alerts.append(AlertInstance(
                id=alert["id"],
                rule_id=alert["rule_id"],
                rule_name=alert["rule_name"],
                title=alert["title"],
                message=alert["message"],
                severity=alert["severity"],
                status=alert["status"],
                triggered_at=alert["triggered_at"],
                resolved_at=alert.get("resolved_at"),
                acknowledged_by=alert.get("acknowledged_by"),
                acknowledged_at=alert.get("acknowledged_at"),
                details=alert.get("details")
            ))
        
        return response_alerts
        
    except Exception as e:
        logger.error(f"Failed to get alert instances: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve alert instances"
        )


# Notification preferences
@router.get("/preferences", response_model=NotificationPreferences, summary="Get Notification Preferences")
async def get_notification_preferences(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get user notification preferences.
    """
    user_prefs = user_preferences_store.get(current_user.id, {})
    
    return NotificationPreferences(
        email_enabled=user_prefs.get("email_enabled", True),
        push_enabled=user_prefs.get("push_enabled", True),
        sms_enabled=user_prefs.get("sms_enabled", False),
        categories=user_prefs.get("categories", {}),
        quiet_hours_start=user_prefs.get("quiet_hours_start"),
        quiet_hours_end=user_prefs.get("quiet_hours_end"),
        timezone=user_prefs.get("timezone", "UTC")
    )


@router.put("/preferences", summary="Update Notification Preferences")
async def update_notification_preferences(
    preferences: NotificationPreferences,
    current_user: User = Depends(get_current_active_user)
):
    """
    Update user notification preferences.
    """
    user_preferences_store[current_user.id] = {
        "email_enabled": preferences.email_enabled,
        "push_enabled": preferences.push_enabled,
        "sms_enabled": preferences.sms_enabled,
        "categories": preferences.categories,
        "quiet_hours_start": preferences.quiet_hours_start,
        "quiet_hours_end": preferences.quiet_hours_end,
        "timezone": preferences.timezone,
        "updated_at": datetime.utcnow()
    }
    
    return {"message": "Notification preferences updated successfully"}


@router.get("/stats", response_model=NotificationStats, summary="Get Notification Statistics")
async def get_notification_stats(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get notification statistics for the current user.
    """
    user_notifications = [
        n for n in notifications_store 
        if n.get("recipient_id") == current_user.id
    ]
    
    # Calculate stats
    total_notifications = len(user_notifications)
    unread_notifications = len([n for n in user_notifications if not n.get("is_read", False)])
    
    today = datetime.utcnow().date()
    notifications_today = len([
        n for n in user_notifications 
        if n.get("created_at", datetime.min).date() == today
    ])
    
    # Group by type and category
    notifications_by_type = {}
    notifications_by_category = {}
    
    for notification in user_notifications:
        notif_type = notification.get("notification_type", "unknown")
        category = notification.get("category", "unknown")
        
        notifications_by_type[notif_type] = notifications_by_type.get(notif_type, 0) + 1
        notifications_by_category[category] = notifications_by_category.get(category, 0) + 1
    
    return NotificationStats(
        total_notifications=total_notifications,
        unread_notifications=unread_notifications,
        notifications_today=notifications_today,
        notifications_by_type=notifications_by_type,
        notifications_by_category=notifications_by_category
    )


# Background task functions
async def _send_email_notification(email: str, title: str, message: str):
    """Send email notification (placeholder)"""
    logger.info(f"Email notification sent to {email}: {title}")
    # TODO: Implement actual email sending

async def _send_push_notification(user_id: int, title: str, message: str):
    """Send push notification (placeholder)"""
    logger.info(f"Push notification sent to user {user_id}: {title}")
    # TODO: Implement actual push notification sending