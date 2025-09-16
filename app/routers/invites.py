from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from datetime import datetime, timedelta

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.invite import Invite, InviteStatuses, InviteRoles

router = APIRouter()

# Pydantic models for request/response validation
class InviteBase(BaseModel):
    email: str = Field(..., min_length=1, max_length=254)
    full_name: Optional[str] = Field(None, max_length=255)
    message: Optional[str] = None
    role: Optional[InviteRoles] = InviteRoles.MEMBER
    invite_type: str = Field("user_invite", max_length=50)
    max_uses: int = Field(1, ge=1)
    expires_at: Optional[datetime] = None
    permissions: Optional[Dict[str, Any]] = None
    team_id: Optional[int] = None

class InviteCreate(InviteBase):
    pass

class InviteUpdate(BaseModel):
    full_name: Optional[str] = Field(None, max_length=255)
    message: Optional[str] = None
    role: Optional[InviteRoles] = None
    expires_at: Optional[datetime] = None
    permissions: Optional[Dict[str, Any]] = None
    max_uses: Optional[int] = Field(None, ge=1)

class InviteResponse(InviteBase):
    id: int
    uid: str
    status: InviteStatuses
    uses_count: int
    sent_at: Optional[datetime]
    accepted_at: Optional[datetime]
    expired_at: Optional[datetime]
    revoked_at: Optional[datetime]
    bounced_at: Optional[datetime]
    email_sent_count: int
    last_email_sent_at: Optional[datetime]
    created_by_user_id: int
    org_id: int
    accepted_by_user_id: Optional[int]
    created_at: datetime
    updated_at: Optional[datetime]
    
    # Computed properties
    pending: bool
    accepted: bool
    expired: bool
    revoked: bool
    bounced: bool
    active: bool
    inactive: bool
    is_expired: bool
    sent: bool
    not_sent: bool
    can_resend: bool
    has_uses_remaining: bool
    single_use: bool
    multi_use: bool
    unlimited_uses: bool
    has_message: bool
    has_permissions: bool
    is_admin_invite: bool
    is_team_invite: bool
    is_org_invite: bool
    automatically_expirable: bool
    needs_cleanup: bool
    uses_remaining: float
    days_until_expiration: Optional[int]
    hours_until_expiration: Optional[float]
    days_since_created: int
    status_display: str
    role_display: str
    invite_type_display: str
    expiration_display: str
    usage_display: str
    activity_summary: str
    invite_summary: str
    invite_url: str
    
    class Config:
        from_attributes = True

# Core CRUD operations
@router.get("/", response_model=List[InviteResponse])
async def list_invites(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    status: Optional[InviteStatuses] = Query(None),
    role: Optional[InviteRoles] = Query(None),
    invite_type: Optional[str] = Query(None),
    active_only: bool = Query(False),
    pending_only: bool = Query(False),
    team_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of invites for current user's organization."""
    # Base query for user's org
    query = db.query(Invite).filter(Invite.org_id == current_user.org_id)
    
    # Apply filters
    if status:
        query = query.filter(Invite.status == status)
    if role:
        query = query.filter(Invite.role == role)
    if invite_type:
        query = query.filter(Invite.invite_type == invite_type)
    if team_id:
        query = query.filter(Invite.team_id == team_id)
    
    if active_only:
        query = query.filter(
            Invite.status == InviteStatuses.PENDING,
            Invite.expires_at > datetime.utcnow()
        )
    
    if pending_only:
        query = query.filter(Invite.status == InviteStatuses.PENDING)
    
    invites = query.order_by(Invite.created_at.desc()).offset(offset).limit(limit).all()
    
    # Add computed properties
    for invite in invites:
        invite.pending = invite.pending_()
        invite.accepted = invite.accepted_()
        invite.expired = invite.expired_()
        invite.revoked = invite.revoked_()
        invite.bounced = invite.bounced_()
        invite.active = invite.active_()
        invite.inactive = invite.inactive_()
        invite.is_expired = invite.is_expired_()
        invite.sent = invite.sent_()
        invite.not_sent = invite.not_sent_()
        invite.can_resend = invite.can_resend_()
        invite.has_uses_remaining = invite.has_uses_remaining_()
        invite.single_use = invite.single_use_()
        invite.multi_use = invite.multi_use_()
        invite.unlimited_uses = invite.unlimited_uses_()
        invite.has_message = invite.has_message_()
        invite.has_permissions = invite.has_permissions_()
        invite.is_admin_invite = invite.is_admin_invite_()
        invite.is_team_invite = invite.is_team_invite_()
        invite.is_org_invite = invite.is_org_invite_()
        invite.automatically_expirable = invite.automatically_expirable_()
        invite.needs_cleanup = invite.needs_cleanup_()
        invite.uses_remaining = invite.uses_remaining_()
        invite.days_until_expiration = invite.days_until_expiration()
        invite.hours_until_expiration = invite.hours_until_expiration()
        invite.days_since_created = invite.days_since_created()
        invite.status_display = invite.status_display()
        invite.role_display = invite.role_display()
        invite.invite_type_display = invite.invite_type_display()
        invite.expiration_display = invite.expiration_display()
        invite.usage_display = invite.usage_display()
        invite.activity_summary = invite.activity_summary()
        invite.invite_summary = invite.invite_summary()
        invite.invite_url = invite.invite_url()
    
    return invites

@router.post("/", response_model=InviteResponse, status_code=status.HTTP_201_CREATED)
async def create_invite(
    invite_data: InviteCreate,
    send_immediately: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create new invite."""
    # Check if user already exists with this email
    existing_user = db.query(User).filter(User.email == invite_data.email.lower()).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email already exists"
        )
    
    # Check if active invite already exists
    existing_invite = db.query(Invite).filter(
        Invite.email == invite_data.email.lower(),
        Invite.org_id == current_user.org_id,
        Invite.status == InviteStatuses.PENDING
    ).first()
    
    if existing_invite:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Active invite already exists for this email"
        )
    
    # Create invite using factory method
    try:
        invite = Invite.build_from_input(
            created_by_user=current_user,
            org=current_user.org,  # Assuming org relationship exists
            input_data=invite_data.dict()
        )
        
        db.add(invite)
        db.commit()
        db.refresh(invite)
        
        # Send immediately if requested
        if send_immediately:
            try:
                invite.send_()
                db.commit()
            except ValueError as e:
                # Invite created but couldn't send - still return success
                pass
        
        # Add computed properties
        invite.pending = invite.pending_()
        invite.accepted = invite.accepted_()
        invite.expired = invite.expired_()
        invite.revoked = invite.revoked_()
        invite.bounced = invite.bounced_()
        invite.active = invite.active_()
        invite.inactive = invite.inactive_()
        invite.is_expired = invite.is_expired_()
        invite.sent = invite.sent_()
        invite.not_sent = invite.not_sent_()
        invite.can_resend = invite.can_resend_()
        invite.has_uses_remaining = invite.has_uses_remaining_()
        invite.single_use = invite.single_use_()
        invite.multi_use = invite.multi_use_()
        invite.unlimited_uses = invite.unlimited_uses_()
        invite.has_message = invite.has_message_()
        invite.has_permissions = invite.has_permissions_()
        invite.is_admin_invite = invite.is_admin_invite_()
        invite.is_team_invite = invite.is_team_invite_()
        invite.is_org_invite = invite.is_org_invite_()
        invite.automatically_expirable = invite.automatically_expirable_()
        invite.needs_cleanup = invite.needs_cleanup_()
        invite.uses_remaining = invite.uses_remaining_()
        invite.days_until_expiration = invite.days_until_expiration()
        invite.hours_until_expiration = invite.hours_until_expiration()
        invite.days_since_created = invite.days_since_created()
        invite.status_display = invite.status_display()
        invite.role_display = invite.role_display()
        invite.invite_type_display = invite.invite_type_display()
        invite.expiration_display = invite.expiration_display()
        invite.usage_display = invite.usage_display()
        invite.activity_summary = invite.activity_summary()
        invite.invite_summary = invite.invite_summary()
        invite.invite_url = invite.invite_url()
        
        return invite
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/{invite_id}", response_model=InviteResponse)
async def get_invite(
    invite_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get specific invite by ID."""
    invite = db.query(Invite).filter(Invite.id == invite_id).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Check access permissions
    if not invite.accessible_by_user_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this invite"
        )
    
    # Add computed properties (same as in list_invites)
    _add_computed_properties(invite)
    
    return invite

@router.put("/{invite_id}", response_model=InviteResponse)
async def update_invite(
    invite_id: int,
    invite_data: InviteUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update invite."""
    invite = db.query(Invite).filter(Invite.id == invite_id).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Check permissions
    if not invite.manageable_by_user_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this invite"
        )
    
    # Update using model method
    invite.update_mutable_(invite_data.dict(exclude_unset=True))
    
    db.commit()
    db.refresh(invite)
    
    _add_computed_properties(invite)
    
    return invite

@router.delete("/{invite_id}")
async def delete_invite(
    invite_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete/revoke invite."""
    invite = db.query(Invite).filter(Invite.id == invite_id).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Check permissions
    if not invite.manageable_by_user_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this invite"
        )
    
    if invite.accepted_():
        # Don't actually delete accepted invites, just revoke
        invite.revoke_(current_user)
        db.commit()
        return {"message": "Invite revoked successfully"}
    else:
        # Can delete pending invites
        db.delete(invite)
        db.commit()
        return {"message": "Invite deleted successfully"}

# Invite actions
@router.post("/{invite_id}/send")
async def send_invite(
    invite_id: int,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Send or resend invite email."""
    invite = db.query(Invite).filter(Invite.id == invite_id).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Check permissions
    if not invite.manageable_by_user_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to send this invite"
        )
    
    try:
        invite.send_(force=force)
        db.commit()
        return {"message": "Invite sent successfully"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{invite_id}/resend")
async def resend_invite(
    invite_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Resend invite email."""
    invite = db.query(Invite).filter(Invite.id == invite_id).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Check permissions
    if not invite.manageable_by_user_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to resend this invite"
        )
    
    try:
        invite.resend_()
        db.commit()
        return {"message": "Invite resent successfully"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{invite_id}/revoke")
async def revoke_invite(
    invite_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Revoke invite."""
    invite = db.query(Invite).filter(Invite.id == invite_id).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Check permissions
    if not invite.manageable_by_user_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to revoke this invite"
        )
    
    try:
        invite.revoke_(current_user)
        db.commit()
        return {"message": "Invite revoked successfully"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{invite_id}/extend")
async def extend_invite_expiration(
    invite_id: int,
    days: int = Query(..., ge=1, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Extend invite expiration."""
    invite = db.query(Invite).filter(Invite.id == invite_id).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Check permissions
    if not invite.manageable_by_user_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to extend this invite"
        )
    
    invite.extend_expiration_(days)
    db.commit()
    
    return {"message": f"Invite expiration extended by {days} days"}

# Public invite acceptance (no auth required)
@router.get("/accept/{uid}")
async def get_invite_for_acceptance(
    uid: str,
    db: Session = Depends(get_db)
):
    """Get invite details for acceptance (public endpoint)."""
    invite = db.query(Invite).filter(Invite.uid == uid).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    # Return limited public information
    return {
        "id": invite.id,
        "uid": invite.uid,
        "org_name": invite.org.name if invite.org else "Unknown Organization",
        "role": invite.role_display(),
        "invite_type": invite.invite_type_display(),
        "active": invite.active_(),
        "expired": invite.is_expired_(),
        "message": invite.message,
        "expiration_display": invite.expiration_display(),
        "created_by": invite.created_by_user.full_name if invite.created_by_user else "Unknown"
    }

@router.post("/accept/{uid}")
async def accept_invite(
    uid: str,
    accepting_user_data: Dict[str, Any],  # User registration data
    db: Session = Depends(get_db)
):
    """Accept invite and create user account."""
    invite = db.query(Invite).filter(Invite.uid == uid).first()
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invite not found"
        )
    
    if not invite.active_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invite is not active or has expired"
        )
    
    # This would typically integrate with user creation logic
    # For now, just mark as accepted
    try:
        # Create user account here (simplified)
        # user = User.create_from_invite(invite, accepting_user_data)
        # invite.accept_(user)
        
        # Simplified acceptance without user creation
        invite.status = InviteStatuses.ACCEPTED
        invite.accepted_at = datetime.utcnow()
        invite.uses_count += 1
        
        db.commit()
        
        return {"message": "Invite accepted successfully"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# Bulk operations
@router.post("/bulk/expire-overdue")
async def bulk_expire_overdue(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Bulk expire overdue invites (admin only)."""
    # Check if user is admin
    if not hasattr(current_user, 'is_admin') or not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    count = Invite.bulk_expire_overdue(db)
    db.commit()
    
    return {"message": f"Expired {count} overdue invites"}

@router.post("/bulk/cleanup-old")
async def bulk_cleanup_old(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Clean up old processed invites (admin only)."""
    # Check if user is admin
    if not hasattr(current_user, 'is_admin') or not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    count = Invite.cleanup_old_invites(db, days)
    db.commit()
    
    return {"message": f"Cleaned up {count} old invites"}

# Analytics
@router.get("/analytics/statistics")
async def get_invite_statistics(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get invite statistics for organization."""
    stats = Invite.invite_statistics(db, current_user.org_id)
    return stats

@router.get("/analytics/recent")
async def get_recent_invites(
    hours: int = Query(24, ge=1, le=168),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get recent invites for organization."""
    recent_invites = db.query(Invite).filter(
        Invite.org_id == current_user.org_id,
        Invite.created_at >= datetime.utcnow() - timedelta(hours=hours)
    ).order_by(Invite.created_at.desc()).all()
    
    for invite in recent_invites:
        _add_computed_properties(invite)
    
    return recent_invites

def _add_computed_properties(invite: Invite) -> None:
    """Helper function to add computed properties to invite."""
    invite.pending = invite.pending_()
    invite.accepted = invite.accepted_()
    invite.expired = invite.expired_()
    invite.revoked = invite.revoked_()
    invite.bounced = invite.bounced_()
    invite.active = invite.active_()
    invite.inactive = invite.inactive_()
    invite.is_expired = invite.is_expired_()
    invite.sent = invite.sent_()
    invite.not_sent = invite.not_sent_()
    invite.can_resend = invite.can_resend_()
    invite.has_uses_remaining = invite.has_uses_remaining_()
    invite.single_use = invite.single_use_()
    invite.multi_use = invite.multi_use_()
    invite.unlimited_uses = invite.unlimited_uses_()
    invite.has_message = invite.has_message_()
    invite.has_permissions = invite.has_permissions_()
    invite.is_admin_invite = invite.is_admin_invite_()
    invite.is_team_invite = invite.is_team_invite_()
    invite.is_org_invite = invite.is_org_invite_()
    invite.automatically_expirable = invite.automatically_expirable_()
    invite.needs_cleanup = invite.needs_cleanup_()
    invite.uses_remaining = invite.uses_remaining_()
    invite.days_until_expiration = invite.days_until_expiration()
    invite.hours_until_expiration = invite.hours_until_expiration()
    invite.days_since_created = invite.days_since_created()
    invite.status_display = invite.status_display()
    invite.role_display = invite.role_display()
    invite.invite_type_display = invite.invite_type_display()
    invite.expiration_display = invite.expiration_display()
    invite.usage_display = invite.usage_display()
    invite.activity_summary = invite.activity_summary()
    invite.invite_summary = invite.invite_summary()
    invite.invite_url = invite.invite_url()