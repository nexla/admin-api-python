from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, JSON, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship, validates
from datetime import datetime, timedelta
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import json
import secrets
import uuid

from app.database import Base

class InviteStatuses(PyEnum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    EXPIRED = "expired"
    REVOKED = "revoked"
    BOUNCED = "bounced"

class InviteRoles(PyEnum):
    MEMBER = "member"
    ADMIN = "admin"
    VIEWER = "viewer"
    COLLABORATOR = "collaborator"

class Invite(Base):
    __tablename__ = "invites"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Core invite attributes
    uid = Column(String(24), unique=True, nullable=False)
    email = Column(String(254), nullable=False)
    full_name = Column(String(255))
    message = Column(Text)
    
    # Invite configuration
    role = Column(SQLEnum(InviteRoles), default=InviteRoles.MEMBER)
    status = Column(SQLEnum(InviteStatuses), default=InviteStatuses.PENDING)
    
    # Expiration and limits
    expires_at = Column(DateTime)
    max_uses = Column(Integer, default=1)
    uses_count = Column(Integer, default=0)
    
    # Tracking
    sent_at = Column(DateTime)
    accepted_at = Column(DateTime)
    expired_at = Column(DateTime)
    revoked_at = Column(DateTime)
    bounced_at = Column(DateTime)
    
    # Email tracking
    email_sent_count = Column(Integer, default=0)
    last_email_sent_at = Column(DateTime)
    
    # Invite metadata
    invite_type = Column(String(50), default="user_invite")  # user_invite, team_invite, org_invite
    permissions = Column(JSON)  # Additional permissions/metadata
    
    # Relationships
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    accepted_by_user_id = Column(Integer, ForeignKey("users.id"))
    team_id = Column(Integer, ForeignKey("teams.id"))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    created_by_user = relationship("User", foreign_keys=[created_by_user_id], back_populates="created_invites")
    org = relationship("Org", foreign_keys=[org_id], back_populates="invites")
    accepted_by_user = relationship("User", foreign_keys=[accepted_by_user_id], back_populates="accepted_invites")
    team = relationship("Team", foreign_keys=[team_id], back_populates="invites")
    
    # Class constants
    DEFAULT_EXPIRATION_DAYS = 7
    MAX_EMAIL_ATTEMPTS = 3
    EMAIL_RETRY_INTERVAL_HOURS = 24
    MAX_USES_UNLIMITED = -1
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.ensure_uid_()
        if not self.expires_at:
            self.expires_at = datetime.utcnow() + timedelta(days=self.DEFAULT_EXPIRATION_DAYS)
    
    @validates('email')
    def validate_email(self, key, email):
        if not email or not email.strip():
            raise ValueError("Email is required")
        if len(email) > 254:
            raise ValueError("Email is too long")
        # Basic email validation
        if '@' not in email or '.' not in email:
            raise ValueError("Invalid email format")
        return email.strip().lower()
    
    @validates('max_uses')
    def validate_max_uses(self, key, max_uses):
        if max_uses is not None and max_uses < -1:
            raise ValueError("Max uses must be -1 (unlimited) or positive")
        return max_uses
    
    def ensure_uid_(self) -> None:
        """Ensure unique UID is set (Rails before_save pattern)"""
        if self.uid:
            return
        
        # Generate unique UID
        max_attempts = 10
        for _ in range(max_attempts):
            uid = secrets.token_hex(12)  # 24 character hex string
            # Check if UID already exists
            if not self.__class__.query.filter_by(uid=uid).first():
                self.uid = uid
                return
        
        raise ValueError("Failed to generate unique invite UID")
    
    @classmethod
    def build_from_input(cls, created_by_user, org, input_data: Dict[str, Any]) -> 'Invite':
        """Factory method to create invite from input data"""
        if not created_by_user or not org:
            raise ValueError("Created by user and organization are required")
        
        invite = cls(
            email=input_data['email'],
            full_name=input_data.get('full_name'),
            message=input_data.get('message'),
            role=input_data.get('role', cls.InviteRoles.MEMBER),
            invite_type=input_data.get('invite_type', 'user_invite'),
            max_uses=input_data.get('max_uses', 1),
            expires_at=input_data.get('expires_at'),
            permissions=input_data.get('permissions'),
            created_by_user_id=created_by_user.id,
            org_id=org.id,
            team_id=input_data.get('team_id')
        )
        
        return invite
    
    def update_mutable_(self, input_data: Dict[str, Any]) -> None:
        """Update mutable attributes"""
        if 'full_name' in input_data:
            self.full_name = input_data['full_name']
        if 'message' in input_data:
            self.message = input_data['message']
        if 'role' in input_data:
            self.role = input_data['role']
        if 'expires_at' in input_data:
            self.expires_at = input_data['expires_at']
        if 'permissions' in input_data:
            self.permissions = input_data['permissions']
        if 'max_uses' in input_data:
            self.max_uses = input_data['max_uses']
    
    # Predicate methods (Rails pattern)
    def pending_(self) -> bool:
        """Check if invite is pending"""
        return self.status == InviteStatuses.PENDING
    
    def accepted_(self) -> bool:
        """Check if invite has been accepted"""
        return self.status == InviteStatuses.ACCEPTED
    
    def expired_(self) -> bool:
        """Check if invite has expired"""
        return self.status == InviteStatuses.EXPIRED
    
    def revoked_(self) -> bool:
        """Check if invite has been revoked"""
        return self.status == InviteStatuses.REVOKED
    
    def bounced_(self) -> bool:
        """Check if invite email bounced"""
        return self.status == InviteStatuses.BOUNCED
    
    def active_(self) -> bool:
        """Check if invite is active (pending and not expired)"""
        return self.pending_() and not self.is_expired_()
    
    def inactive_(self) -> bool:
        """Check if invite is inactive"""
        return not self.active_()
    
    def is_expired_(self) -> bool:
        """Check if invite is past expiration date"""
        if not self.expires_at:
            return False
        return datetime.utcnow() > self.expires_at
    
    def is_within_expiration_(self, hours: int = 24) -> bool:
        """Check if invite expires within given hours"""
        if not self.expires_at:
            return False
        cutoff = datetime.utcnow() + timedelta(hours=hours)
        return self.expires_at <= cutoff
    
    def sent_(self) -> bool:
        """Check if invite has been sent"""
        return self.sent_at is not None
    
    def not_sent_(self) -> bool:
        """Check if invite has not been sent"""
        return not self.sent_()
    
    def recently_sent_(self, hours: int = 1) -> bool:
        """Check if invite was recently sent"""
        if not self.sent_at:
            return False
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return self.sent_at > cutoff
    
    def can_resend_(self) -> bool:
        """Check if invite can be resent"""
        if not self.active_():
            return False
        
        if self.email_sent_count >= self.MAX_EMAIL_ATTEMPTS:
            return False
        
        if self.last_email_sent_at:
            retry_cutoff = self.last_email_sent_at + timedelta(hours=self.EMAIL_RETRY_INTERVAL_HOURS)
            if datetime.utcnow() < retry_cutoff:
                return False
        
        return True
    
    def uses_remaining_(self) -> int:
        """Get remaining uses for invite"""
        if self.max_uses == self.MAX_USES_UNLIMITED:
            return float('inf')
        return max(0, self.max_uses - self.uses_count)
    
    def has_uses_remaining_(self) -> bool:
        """Check if invite has uses remaining"""
        return self.uses_remaining_() > 0
    
    def single_use_(self) -> bool:
        """Check if invite is single use"""
        return self.max_uses == 1
    
    def multi_use_(self) -> bool:
        """Check if invite allows multiple uses"""
        return self.max_uses != 1
    
    def unlimited_uses_(self) -> bool:
        """Check if invite has unlimited uses"""
        return self.max_uses == self.MAX_USES_UNLIMITED
    
    def has_message_(self) -> bool:
        """Check if invite has custom message"""
        return bool(self.message and self.message.strip())
    
    def has_permissions_(self) -> bool:
        """Check if invite has custom permissions"""
        return bool(self.permissions)
    
    def is_admin_invite_(self) -> bool:
        """Check if invite is for admin role"""
        return self.role == InviteRoles.ADMIN
    
    def is_member_invite_(self) -> bool:
        """Check if invite is for member role"""
        return self.role == InviteRoles.MEMBER
    
    def is_team_invite_(self) -> bool:
        """Check if invite is for team membership"""
        return self.invite_type == "team_invite" or self.team_id is not None
    
    def is_org_invite_(self) -> bool:
        """Check if invite is for organization membership"""
        return self.invite_type == "org_invite"
    
    def created_by_current_user_(self, user) -> bool:
        """Check if invite was created by current user"""
        return self.created_by_user_id == user.id if user else False
    
    def for_user_(self, user) -> bool:
        """Check if invite is for specific user email"""
        return user and user.email == self.email
    
    def can_be_accepted_by_(self, user) -> bool:
        """Check if invite can be accepted by user"""
        if not self.active_():
            return False
        
        if not self.has_uses_remaining_():
            return False
        
        # Check if email matches
        if user and user.email != self.email:
            return False
        
        return True
    
    def accessible_by_user_(self, user) -> bool:
        """Check if invite is accessible by user"""
        if not user:
            return False
        
        # Created by user
        if self.created_by_current_user_(user):
            return True
        
        # Org admin
        if self.org and self.org.has_admin_access_(user):
            return True
        
        # Invitee themselves
        if self.for_user_(user):
            return True
        
        return False
    
    def manageable_by_user_(self, user) -> bool:
        """Check if invite can be managed by user"""
        if not user:
            return False
        
        # Created by user
        if self.created_by_current_user_(user):
            return True
        
        # Org admin
        if self.org and self.org.has_admin_access_(user):
            return True
        
        return False
    
    def automatically_expirable_(self) -> bool:
        """Check if invite should be automatically expired"""
        return self.pending_() and self.is_expired_()
    
    def needs_cleanup_(self, days: int = 30) -> bool:
        """Check if old invite needs cleanup"""
        if self.accepted_() and self.accepted_at:
            cutoff = datetime.utcnow() - timedelta(days=days)
            return self.accepted_at < cutoff
        
        if self.expired_() and self.expired_at:
            cutoff = datetime.utcnow() - timedelta(days=days)
            return self.expired_at < cutoff
        
        return False
    
    # State management methods (Rails pattern)
    def send_(self, force: bool = False) -> None:
        """Send invite email"""
        if not force and not self.can_resend_():
            raise ValueError("Cannot send invite at this time")
        
        if not self.active_():
            raise ValueError("Cannot send inactive invite")
        
        # Mark as sent
        self.sent_at = datetime.utcnow()
        self.last_email_sent_at = datetime.utcnow()
        self.email_sent_count += 1
        
        # In production, send actual email
        # EmailService.send_invite(self)
    
    def resend_(self) -> None:
        """Resend invite email"""
        self.send_(force=False)
    
    def accept_(self, accepting_user) -> None:
        """Accept the invite"""
        if not self.can_be_accepted_by_(accepting_user):
            raise ValueError("Invite cannot be accepted by this user")
        
        self.status = InviteStatuses.ACCEPTED
        self.accepted_at = datetime.utcnow()
        self.accepted_by_user_id = accepting_user.id
        self.uses_count += 1
        
        # If single use, mark as fully used
        if self.single_use_():
            pass  # Already handled by status change
    
    def revoke_(self, revoking_user=None) -> None:
        """Revoke the invite"""
        if not self.active_():
            raise ValueError("Cannot revoke inactive invite")
        
        self.status = InviteStatuses.REVOKED
        self.revoked_at = datetime.utcnow()
    
    def expire_(self) -> None:
        """Mark invite as expired"""
        if not self.pending_():
            raise ValueError("Cannot expire non-pending invite")
        
        self.status = InviteStatuses.EXPIRED
        self.expired_at = datetime.utcnow()
    
    def bounce_(self) -> None:
        """Mark invite email as bounced"""
        self.status = InviteStatuses.BOUNCED
        self.bounced_at = datetime.utcnow()
    
    def extend_expiration_(self, days: int) -> None:
        """Extend invite expiration"""
        if not self.expires_at:
            self.expires_at = datetime.utcnow() + timedelta(days=days)
        else:
            self.expires_at += timedelta(days=days)
    
    def reset_expiration_(self, days: int = None) -> None:
        """Reset expiration to new date"""
        days = days or self.DEFAULT_EXPIRATION_DAYS
        self.expires_at = datetime.utcnow() + timedelta(days=days)
    
    def increment_use_(self) -> None:
        """Increment usage count"""
        self.uses_count += 1
        
        # Check if invite should be deactivated
        if not self.has_uses_remaining_() and not self.unlimited_uses_():
            # Don't change status - let it remain accepted for tracking
            pass
    
    def reset_uses_(self) -> None:
        """Reset usage count"""
        self.uses_count = 0
    
    def regenerate_uid_(self) -> None:
        """Regenerate invite UID"""
        old_uid = self.uid
        self.uid = None
        self.ensure_uid_()
    
    # Calculation methods
    def days_until_expiration(self) -> Optional[int]:
        """Get days until expiration"""
        if not self.expires_at:
            return None
        
        delta = self.expires_at - datetime.utcnow()
        return max(0, delta.days)
    
    def hours_until_expiration(self) -> Optional[float]:
        """Get hours until expiration"""
        if not self.expires_at:
            return None
        
        delta = self.expires_at - datetime.utcnow()
        return max(0, delta.total_seconds() / 3600)
    
    def days_since_created(self) -> int:
        """Get days since invite was created"""
        delta = datetime.utcnow() - self.created_at
        return delta.days
    
    def days_since_sent(self) -> Optional[int]:
        """Get days since invite was sent"""
        if not self.sent_at:
            return None
        
        delta = datetime.utcnow() - self.sent_at
        return delta.days
    
    def days_since_accepted(self) -> Optional[int]:
        """Get days since invite was accepted"""
        if not self.accepted_at:
            return None
        
        delta = datetime.utcnow() - self.accepted_at
        return delta.days
    
    def acceptance_rate_for_org(self) -> float:
        """Get acceptance rate for organization (class method simulation)"""
        # This would calculate org-wide acceptance rate
        return 0.75  # Mock value
    
    # Display methods
    def status_display(self) -> str:
        """Get human-readable status"""
        status_map = {
            InviteStatuses.PENDING: "Pending",
            InviteStatuses.ACCEPTED: "Accepted",
            InviteStatuses.EXPIRED: "Expired",
            InviteStatuses.REVOKED: "Revoked",
            InviteStatuses.BOUNCED: "Bounced"
        }
        return status_map.get(self.status, self.status.value)
    
    def role_display(self) -> str:
        """Get human-readable role"""
        role_map = {
            InviteRoles.MEMBER: "Member",
            InviteRoles.ADMIN: "Administrator",
            InviteRoles.VIEWER: "Viewer",
            InviteRoles.COLLABORATOR: "Collaborator"
        }
        return role_map.get(self.role, self.role.value)
    
    def invite_type_display(self) -> str:
        """Get human-readable invite type"""
        type_map = {
            "user_invite": "User Invitation",
            "team_invite": "Team Invitation", 
            "org_invite": "Organization Invitation"
        }
        return type_map.get(self.invite_type, self.invite_type)
    
    def expiration_display(self) -> str:
        """Get expiration status display"""
        if not self.expires_at:
            return "No expiration"
        
        days = self.days_until_expiration()
        if days == 0:
            hours = self.hours_until_expiration()
            if hours <= 0:
                return "Expired"
            elif hours < 24:
                return f"Expires in {hours:.1f} hours"
            else:
                return "Expires today"
        elif days == 1:
            return "Expires tomorrow"
        elif days <= 7:
            return f"Expires in {days} days"
        else:
            return f"Expires {self.expires_at.strftime('%Y-%m-%d')}"
    
    def usage_display(self) -> str:
        """Get usage status display"""
        if self.unlimited_uses_():
            return f"{self.uses_count} uses (unlimited)"
        else:
            remaining = self.uses_remaining_()
            return f"{self.uses_count}/{self.max_uses} uses ({remaining} remaining)"
    
    def activity_summary(self) -> str:
        """Get activity summary"""
        parts = []
        
        if self.sent_():
            parts.append(f"Sent {self.days_since_sent()} days ago")
        else:
            parts.append("Not sent")
        
        if self.accepted_():
            parts.append(f"Accepted {self.days_since_accepted()} days ago")
        
        return " | ".join(parts) if parts else "No activity"
    
    def invite_summary(self) -> str:
        """Get complete invite summary"""
        parts = [
            self.status_display(),
            self.role_display(),
            self.expiration_display()
        ]
        
        if self.is_team_invite_():
            parts.append("Team invite")
        
        if not self.single_use_():
            parts.append(self.usage_display())
        
        return " | ".join(parts)
    
    def invite_url(self, base_url: str = "https://app.example.com") -> str:
        """Generate invite acceptance URL"""
        return f"{base_url}/invites/{self.uid}/accept"
    
    # === Rails-style Class Methods and Scopes ===
    
    @classmethod
    def active_invites(cls, session):
        """Get all active invites (Rails scope pattern)"""
        return session.query(cls).filter(
            cls.status == InviteStatuses.PENDING,
            cls.expires_at > datetime.utcnow()
        )
    
    @classmethod
    def pending_invites(cls, session):
        """Get all pending invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.status == InviteStatuses.PENDING)
    
    @classmethod
    def accepted_invites(cls, session):
        """Get all accepted invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.status == InviteStatuses.ACCEPTED)
    
    @classmethod
    def expired_invites(cls, session):
        """Get all expired invites (Rails scope pattern)"""
        return session.query(cls).filter(
            (cls.status == InviteStatuses.EXPIRED) |
            (cls.expires_at <= datetime.utcnow())
        )
    
    @classmethod
    def revoked_invites(cls, session):
        """Get all revoked invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.status == InviteStatuses.REVOKED)
    
    @classmethod
    def bounced_invites(cls, session):
        """Get all bounced invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.status == InviteStatuses.BOUNCED)
    
    @classmethod
    def by_org(cls, session, org_id: int):
        """Get invites by organization (Rails scope pattern)"""
        return session.query(cls).filter(cls.org_id == org_id)
    
    @classmethod
    def by_creator(cls, session, user_id: int):
        """Get invites created by user (Rails scope pattern)"""
        return session.query(cls).filter(cls.created_by_user_id == user_id)
    
    @classmethod
    def by_role(cls, session, role: InviteRoles):
        """Get invites by role (Rails scope pattern)"""
        return session.query(cls).filter(cls.role == role)
    
    @classmethod
    def by_team(cls, session, team_id: int):
        """Get invites for team (Rails scope pattern)"""
        return session.query(cls).filter(cls.team_id == team_id)
    
    @classmethod
    def by_email(cls, session, email: str):
        """Get invites by email (Rails scope pattern)"""
        return session.query(cls).filter(cls.email == email.lower().strip())
    
    @classmethod
    def admin_invites(cls, session):
        """Get all admin invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.role == InviteRoles.ADMIN)
    
    @classmethod
    def team_invites(cls, session):
        """Get all team invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.invite_type == "team_invite")
    
    @classmethod
    def org_invites(cls, session):
        """Get all org invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.invite_type == "org_invite")
    
    @classmethod
    def unsent_invites(cls, session):
        """Get all unsent invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.sent_at.is_(None))
    
    @classmethod
    def sent_invites(cls, session):
        """Get all sent invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.sent_at.isnot(None))
    
    @classmethod
    def multi_use_invites(cls, session):
        """Get all multi-use invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.max_uses != 1)
    
    @classmethod
    def unlimited_invites(cls, session):
        """Get all unlimited use invites (Rails scope pattern)"""
        return session.query(cls).filter(cls.max_uses == cls.MAX_USES_UNLIMITED)
    
    @classmethod
    def expiring_soon(cls, session, hours: int = 24):
        """Get invites expiring soon (Rails scope pattern)"""
        cutoff = datetime.utcnow() + timedelta(hours=hours)
        return cls.active_invites(session).filter(cls.expires_at <= cutoff)
    
    @classmethod
    def needing_cleanup(cls, session, days: int = 30):
        """Get invites needing cleanup (Rails scope pattern)"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        return session.query(cls).filter(
            ((cls.status == InviteStatuses.ACCEPTED) & (cls.accepted_at <= cutoff)) |
            ((cls.status == InviteStatuses.EXPIRED) & (cls.expired_at <= cutoff))
        )
    
    @classmethod
    def find_by_uid(cls, session, uid: str):
        """Find invite by UID (Rails finder pattern)"""
        return session.query(cls).filter(cls.uid == uid).first()
    
    @classmethod
    def find_by_uid_(cls, session, uid: str):
        """Find invite by UID or raise exception (Rails bang finder pattern)"""
        invite = cls.find_by_uid(session, uid)
        if not invite:
            raise ValueError(f"Invite with UID '{uid}' not found")
        return invite
    
    @classmethod
    def recent_invites(cls, session, hours: int = 24):
        """Get recent invites (Rails scope pattern)"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return session.query(cls).filter(cls.created_at >= cutoff)
    
    @classmethod
    def accessible_to_user(cls, session, user):
        """Get invites accessible to user (Rails scope pattern)"""
        if not user:
            return session.query(cls).filter(False)  # Empty query
        
        return session.query(cls).filter(
            (cls.created_by_user_id == user.id) |
            (cls.email == user.email)
            # Would add org admin check if org relationship available
        )
    
    @classmethod
    def manageable_by_user(cls, session, user):
        """Get invites manageable by user (Rails scope pattern)"""
        if not user:
            return session.query(cls).filter(False)  # Empty query
        
        return session.query(cls).filter(
            cls.created_by_user_id == user.id
            # Would add org admin check if org relationship available
        )
    
    @classmethod
    def bulk_expire_overdue(cls, session) -> int:
        """Bulk expire overdue invites (Rails class method pattern)"""
        count = cls.expired_invites(session).filter(
            cls.status == InviteStatuses.PENDING
        ).update({
            'status': InviteStatuses.EXPIRED,
            'expired_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        })
        return count
    
    @classmethod
    def cleanup_old_invites(cls, session, days: int = 30) -> int:
        """Clean up old processed invites (Rails class method pattern)"""
        count = cls.needing_cleanup(session, days).delete()
        return count
    
    @classmethod
    def invite_statistics(cls, session, org_id: int = None) -> Dict[str, int]:
        """Get invite statistics (Rails class method pattern)"""
        query = session.query(cls)
        if org_id:
            query = query.filter(cls.org_id == org_id)
        
        total = query.count()
        pending = query.filter(cls.status == InviteStatuses.PENDING).count()
        accepted = query.filter(cls.status == InviteStatuses.ACCEPTED).count()
        expired = query.filter(cls.status == InviteStatuses.EXPIRED).count()
        revoked = query.filter(cls.status == InviteStatuses.REVOKED).count()
        bounced = query.filter(cls.status == InviteStatuses.BOUNCED).count()
        
        acceptance_rate = (accepted / total * 100) if total > 0 else 0
        
        return {
            'total': total,
            'pending': pending,
            'accepted': accepted,
            'expired': expired,
            'revoked': revoked,
            'bounced': bounced,
            'acceptance_rate': round(acceptance_rate, 1)
        }
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<Invite(id={self.id}, uid='{self.uid}', email='{self.email}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return f"Invite to {self.email} ({self.status_display()})"
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        data = {
            'id': self.id,
            'uid': self.uid,
            'email': self.email,
            'full_name': self.full_name,
            'message': self.message,
            'role': self.role.value if self.role else None,
            'status': self.status.value if self.status else None,
            'invite_type': self.invite_type,
            'max_uses': self.max_uses,
            'uses_count': self.uses_count,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'email_sent_count': self.email_sent_count,
            'permissions': self.permissions,
            'created_by_user_id': self.created_by_user_id,
            'org_id': self.org_id,
            'team_id': self.team_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Predicate methods
            'pending': self.pending_(),
            'accepted': self.accepted_(),
            'expired': self.expired_(),
            'revoked': self.revoked_(),
            'bounced': self.bounced_(),
            'active': self.active_(),
            'inactive': self.inactive_(),
            'is_expired': self.is_expired_(),
            'sent': self.sent_(),
            'not_sent': self.not_sent_(),
            'can_resend': self.can_resend_(),
            'has_uses_remaining': self.has_uses_remaining_(),
            'single_use': self.single_use_(),
            'multi_use': self.multi_use_(),
            'unlimited_uses': self.unlimited_uses_(),
            'has_message': self.has_message_(),
            'has_permissions': self.has_permissions_(),
            'is_admin_invite': self.is_admin_invite_(),
            'is_team_invite': self.is_team_invite_(),
            'is_org_invite': self.is_org_invite_(),
            'automatically_expirable': self.automatically_expirable_(),
            'needs_cleanup': self.needs_cleanup_(),
            
            # Calculations
            'uses_remaining': self.uses_remaining_(),
            'days_until_expiration': self.days_until_expiration(),
            'hours_until_expiration': self.hours_until_expiration(),
            'days_since_created': self.days_since_created(),
            
            # Display values
            'status_display': self.status_display(),
            'role_display': self.role_display(),
            'invite_type_display': self.invite_type_display(),
            'expiration_display': self.expiration_display(),
            'usage_display': self.usage_display(),
            'activity_summary': self.activity_summary(),
            'invite_summary': self.invite_summary(),
            'invite_url': self.invite_url()
        }
        
        if include_sensitive:
            data.update({
                'sent_at': self.sent_at.isoformat() if self.sent_at else None,
                'accepted_at': self.accepted_at.isoformat() if self.accepted_at else None,
                'expired_at': self.expired_at.isoformat() if self.expired_at else None,
                'revoked_at': self.revoked_at.isoformat() if self.revoked_at else None,
                'bounced_at': self.bounced_at.isoformat() if self.bounced_at else None,
                'last_email_sent_at': self.last_email_sent_at.isoformat() if self.last_email_sent_at else None,
                'accepted_by_user_id': self.accepted_by_user_id,
                'recently_sent': self.recently_sent_(),
                'days_since_sent': self.days_since_sent(),
                'days_since_accepted': self.days_since_accepted()
            })
        
        return data
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'email': self.email,
            'full_name': self.full_name,
            'role': self.role.value if self.role else None,
            'status': self.status.value if self.status else None,
            'active': self.active_(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'status_display': self.status_display(),
            'role_display': self.role_display(),
            'expiration_display': self.expiration_display()
        }