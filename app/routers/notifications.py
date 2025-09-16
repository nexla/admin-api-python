from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from app.database import get_db
from app.models.notification import Notification
from app.models.user import User
from app.auth.dependencies import get_current_user, require_permissions
from app.services.audit_service import AuditService
from pydantic import BaseModel

router = APIRouter(prefix="/notifications", tags=["notifications"])

class NotificationCreate(BaseModel):
    title: str
    message: str
    notification_type: str
    target_user_id: Optional[int] = None
    target_org_id: Optional[int] = None
    priority: str = "medium"
    expires_at: Optional[datetime] = None

class NotificationUpdate(BaseModel):
    title: Optional[str] = None
    message: Optional[str] = None
    priority: Optional[str] = None
    expires_at: Optional[datetime] = None

class NotificationResponse(BaseModel):
    id: int
    title: str
    message: str
    notification_type: str
    priority: str
    is_read: bool
    created_at: datetime
    expires_at: Optional[datetime]
    target_user_id: Optional[int]
    target_org_id: Optional[int]

@router.get("/", response_model=List[NotificationResponse])
async def get_notifications(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    unread_only: bool = Query(False),
    notification_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get user notifications with filtering"""
    query = db.query(Notification).filter(
        Notification.target_user_id == current_user.id
    )
    
    if unread_only:
        query = query.filter(Notification.is_read == False)
    
    if notification_type:
        query = query.filter(Notification.notification_type == notification_type)
    
    # Filter out expired notifications
    query = query.filter(
        (Notification.expires_at.is_(None)) | 
        (Notification.expires_at > datetime.utcnow())
    )
    
    notifications = query.order_by(Notification.created_at.desc()).offset(skip).limit(limit).all()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="notification.list",
        resource_type="notification",
        details={"count": len(notifications), "unread_only": unread_only}
    )
    
    return notifications

@router.get("/unread-count")
async def get_unread_count(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get count of unread notifications"""
    count = db.query(Notification).filter(
        Notification.target_user_id == current_user.id,
        Notification.is_read == False,
        (Notification.expires_at.is_(None)) | 
        (Notification.expires_at > datetime.utcnow())
    ).count()
    
    return {"unread_count": count}

@router.post("/", response_model=NotificationResponse)
async def create_notification(
    notification_data: NotificationCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["notification.create"]))
):
    """Create a new notification"""
    notification = Notification(
        title=notification_data.title,
        message=notification_data.message,
        notification_type=notification_data.notification_type,
        target_user_id=notification_data.target_user_id,
        target_org_id=notification_data.target_org_id,
        priority=notification_data.priority,
        expires_at=notification_data.expires_at,
        created_by_id=current_user.id
    )
    
    db.add(notification)
    db.commit()
    db.refresh(notification)
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="notification.create",
        resource_type="notification",
        resource_id=notification.id,
        details={
            "title": notification.title,
            "type": notification.notification_type,
            "target_user_id": notification.target_user_id,
            "target_org_id": notification.target_org_id
        }
    )
    
    return notification

@router.put("/{notification_id}/read")
async def mark_as_read(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Mark notification as read"""
    notification = db.query(Notification).filter(
        Notification.id == notification_id,
        Notification.target_user_id == current_user.id
    ).first()
    
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    
    notification.is_read = True
    notification.read_at = datetime.utcnow()
    db.commit()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="notification.read",
        resource_type="notification",
        resource_id=notification.id
    )
    
    return {"message": "Notification marked as read"}

@router.put("/read-all")
async def mark_all_as_read(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Mark all notifications as read"""
    updated_count = db.query(Notification).filter(
        Notification.target_user_id == current_user.id,
        Notification.is_read == False
    ).update({
        "is_read": True,
        "read_at": datetime.utcnow()
    })
    
    db.commit()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="notification.read_all",
        resource_type="notification",
        details={"count": updated_count}
    )
    
    return {"message": f"Marked {updated_count} notifications as read"}

@router.delete("/{notification_id}")
async def delete_notification(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["notification.delete"]))
):
    """Delete a notification"""
    notification = db.query(Notification).filter(
        Notification.id == notification_id
    ).first()
    
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    
    # Users can only delete their own notifications, admins can delete any
    if notification.target_user_id != current_user.id and not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    db.delete(notification)
    db.commit()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="notification.delete",
        resource_type="notification",
        resource_id=notification.id,
        details={"title": notification.title}
    )
    
    return {"message": "Notification deleted"}

@router.get("/system", response_model=List[NotificationResponse])
async def get_system_notifications(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["notification.read_system"]))
):
    """Get system-wide notifications (admin only)"""
    notifications = db.query(Notification).filter(
        Notification.notification_type == "system"
    ).order_by(Notification.created_at.desc()).offset(skip).limit(limit).all()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="notification.list_system",
        resource_type="notification",
        details={"count": len(notifications)}
    )
    
    return notifications

@router.post("/broadcast")
async def broadcast_notification(
    notification_data: NotificationCreate,
    target_org_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["notification.broadcast"]))
):
    """Broadcast notification to all users or org members"""
    from app.models.org_membership import OrgMembership
    
    if target_org_id:
        # Broadcast to org members
        members = db.query(OrgMembership).filter(
            OrgMembership.org_id == target_org_id
        ).all()
        target_users = [member.user_id for member in members]
    else:
        # Broadcast to all users
        users = db.query(User).filter(User.is_active == True).all()
        target_users = [user.id for user in users]
    
    notifications_created = 0
    for user_id in target_users:
        notification = Notification(
            title=notification_data.title,
            message=notification_data.message,
            notification_type=notification_data.notification_type,
            target_user_id=user_id,
            target_org_id=target_org_id,
            priority=notification_data.priority,
            expires_at=notification_data.expires_at,
            created_by_id=current_user.id
        )
        db.add(notification)
        notifications_created += 1
    
    db.commit()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="notification.broadcast",
        resource_type="notification",
        details={
            "title": notification_data.title,
            "type": notification_data.notification_type,
            "target_org_id": target_org_id,
            "recipients_count": notifications_created
        }
    )
    
    return {"message": f"Broadcast notification sent to {notifications_created} users"}