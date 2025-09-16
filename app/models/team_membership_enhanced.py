from datetime import datetime, timedelta
from enum import Enum as PyEnum
import json
from typing import Dict, List, Optional, Any, Union
import uuid

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class MembershipRole(PyEnum):
    OWNER = "OWNER"
    ADMIN = "ADMIN"
    MODERATOR = "MODERATOR"
    MEMBER = "MEMBER"
    GUEST = "GUEST"
    OBSERVER = "OBSERVER"
    
    @property
    def display_name(self) -> str:
        return {
            self.OWNER: "Owner",
            self.ADMIN: "Administrator",
            self.MODERATOR: "Moderator",
            self.MEMBER: "Member",
            self.GUEST: "Guest",
            self.OBSERVER: "Observer"
        }.get(self, self.value)
    
    @property
    def permission_level(self) -> int:
        return {
            self.OWNER: 100,
            self.ADMIN: 80,
            self.MODERATOR: 60,
            self.MEMBER: 40,
            self.GUEST: 20,
            self.OBSERVER: 10
        }.get(self, 0)
    
    @property
    def can_manage_members(self) -> bool:
        return self in [self.OWNER, self.ADMIN]
    
    @property
    def can_moderate(self) -> bool:
        return self in [self.OWNER, self.ADMIN, self.MODERATOR]


class MembershipStatus(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUSPENDED = "SUSPENDED"
    PENDING = "PENDING"
    INVITED = "INVITED"
    DECLINED = "DECLINED"
    REMOVED = "REMOVED"
    BANNED = "BANNED"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.SUSPENDED: "Suspended",
            self.PENDING: "Pending Approval",
            self.INVITED: "Invited",
            self.DECLINED: "Declined",
            self.REMOVED: "Removed",
            self.BANNED: "Banned"
        }.get(self, self.value)
    
    @property
    def is_operational(self) -> bool:
        return self in [self.ACTIVE, self.PENDING]


class MembershipType(PyEnum):
    DIRECT = "DIRECT"
    INHERITED = "INHERITED"
    TEMPORARY = "TEMPORARY"
    CONDITIONAL = "CONDITIONAL"
    
    @property
    def display_name(self) -> str:
        return {
            self.DIRECT: "Direct Membership",
            self.INHERITED: "Inherited Membership",
            self.TEMPORARY: "Temporary Membership",
            self.CONDITIONAL: "Conditional Membership"
        }.get(self, self.value)


class TeamMembershipEnhanced(Base):
    __tablename__ = 'team_memberships_enhanced'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    team_id = Column(Integer, ForeignKey('teams_enhanced.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    
    role = Column(SQLEnum(MembershipRole), nullable=False, default=MembershipRole.MEMBER)
    status = Column(SQLEnum(MembershipStatus), nullable=False, default=MembershipStatus.ACTIVE)
    membership_type = Column(SQLEnum(MembershipType), nullable=False, default=MembershipType.DIRECT)
    
    permissions = Column(JSON, default=list)
    custom_permissions = Column(JSON, default=dict)
    
    joined_at = Column(DateTime, default=datetime.now, nullable=False)
    invited_at = Column(DateTime)
    accepted_at = Column(DateTime)
    last_active_at = Column(DateTime)
    expires_at = Column(DateTime)
    
    invited_by = Column(Integer, ForeignKey('users.id'))
    added_by = Column(Integer, ForeignKey('users.id'))
    approved_by = Column(Integer, ForeignKey('users.id'))
    
    active = Column(Boolean, default=True, nullable=False)
    
    invitation_message = Column(Text)
    notes = Column(Text)
    
    activity_score = Column(Float, default=0.0)
    contribution_score = Column(Float, default=0.0)
    collaboration_score = Column(Float, default=0.0)
    
    notification_preferences = Column(JSON, default=dict)
    settings = Column(JSON, default=dict)
    extra_metadata = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    deactivated_at = Column(DateTime)
    suspended_at = Column(DateTime)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    team = relationship("TeamEnhanced", back_populates="memberships")
    user = relationship("User", foreign_keys=[user_id])
    inviter = relationship("User", foreign_keys=[invited_by])
    adder = relationship("User", foreign_keys=[added_by])
    approver = relationship("User", foreign_keys=[approved_by])
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    
    __table_args__ = (
        Index('idx_team_membership_enhanced_team_id', 'team_id'),
        Index('idx_team_membership_enhanced_user_id', 'user_id'),
        Index('idx_team_membership_enhanced_role', 'role'),
        Index('idx_team_membership_enhanced_status', 'status'),
        Index('idx_team_membership_enhanced_type', 'membership_type'),
        Index('idx_team_membership_enhanced_active', 'active'),
        Index('idx_team_membership_enhanced_joined_at', 'joined_at'),
        Index('idx_team_membership_enhanced_last_active', 'last_active_at'),
        Index('idx_team_membership_enhanced_expires_at', 'expires_at'),
        UniqueConstraint('team_id', 'user_id', name='uq_team_membership_enhanced_team_user'),
        CheckConstraint('activity_score >= 0 AND activity_score <= 10', name='ck_membership_enhanced_activity_score_range'),
        CheckConstraint('contribution_score >= 0 AND contribution_score <= 10', name='ck_membership_enhanced_contribution_score_range'),
        CheckConstraint('collaboration_score >= 0 AND collaboration_score <= 10', name='ck_membership_enhanced_collaboration_score_range'),
    )
    
    HIGH_ACTIVITY_THRESHOLD = 8.0
    LOW_ACTIVITY_THRESHOLD = 3.0
    INACTIVE_DAYS_THRESHOLD = 30
    
    def __repr__(self):
        return f"<TeamMembershipEnhanced(id={self.id}, team_id={self.team_id}, user_id={self.user_id}, role='{self.role.value}', status='{self.status.value}')>"
    
    def active_(self) -> bool:
        """Check if membership is active (Rails pattern)"""
        return (self.active and 
                self.status == MembershipStatus.ACTIVE and
                not self.expired_() and
                not self.suspended_())
    
    def operational_(self) -> bool:
        """Check if membership is operational (Rails pattern)"""
        return self.status.is_operational and not self.expired_()
    
    def pending_(self) -> bool:
        """Check if membership is pending (Rails pattern)"""
        return self.status == MembershipStatus.PENDING
    
    def invited_(self) -> bool:
        """Check if membership is invited (Rails pattern)"""
        return self.status == MembershipStatus.INVITED
    
    def suspended_(self) -> bool:
        """Check if membership is suspended (Rails pattern)"""
        return self.status == MembershipStatus.SUSPENDED
    
    def banned_(self) -> bool:
        """Check if membership is banned (Rails pattern)"""
        return self.status == MembershipStatus.BANNED
    
    def declined_(self) -> bool:
        """Check if membership is declined (Rails pattern)"""
        return self.status == MembershipStatus.DECLINED
    
    def removed_(self) -> bool:
        """Check if membership is removed (Rails pattern)"""
        return self.status == MembershipStatus.REMOVED
    
    def expired_(self) -> bool:
        """Check if membership has expired (Rails pattern)"""
        return self.expires_at and self.expires_at < datetime.now()
    
    def temporary_(self) -> bool:
        """Check if membership is temporary (Rails pattern)"""
        return self.membership_type == MembershipType.TEMPORARY
    
    def inherited_(self) -> bool:
        """Check if membership is inherited (Rails pattern)"""
        return self.membership_type == MembershipType.INHERITED
    
    def direct_(self) -> bool:
        """Check if membership is direct (Rails pattern)"""
        return self.membership_type == MembershipType.DIRECT
    
    def is_owner_(self) -> bool:
        """Check if member is owner (Rails pattern)"""
        return self.role == MembershipRole.OWNER
    
    def is_admin_(self) -> bool:
        """Check if member is admin (Rails pattern)"""
        return self.role == MembershipRole.ADMIN
    
    def is_moderator_(self) -> bool:
        """Check if member is moderator (Rails pattern)"""
        return self.role == MembershipRole.MODERATOR
    
    def is_member_(self) -> bool:
        """Check if member is regular member (Rails pattern)"""
        return self.role == MembershipRole.MEMBER
    
    def is_guest_(self) -> bool:
        """Check if member is guest (Rails pattern)"""
        return self.role == MembershipRole.GUEST
    
    def is_observer_(self) -> bool:
        """Check if member is observer (Rails pattern)"""
        return self.role == MembershipRole.OBSERVER
    
    def can_manage_members_(self) -> bool:
        """Check if member can manage other members (Rails pattern)"""
        return self.active_() and self.role.can_manage_members
    
    def can_moderate_(self) -> bool:
        """Check if member can moderate (Rails pattern)"""
        return self.active_() and self.role.can_moderate
    
    def has_permission_(self, permission: str) -> bool:
        """Check if member has specific permission (Rails pattern)"""
        if not self.active_():
            return False
        
        return (permission in (self.permissions or []) or 
                permission in (self.custom_permissions or {}))
    
    def high_activity_(self) -> bool:
        """Check if member has high activity (Rails pattern)"""
        return self.activity_score >= self.HIGH_ACTIVITY_THRESHOLD
    
    def low_activity_(self) -> bool:
        """Check if member has low activity (Rails pattern)"""
        return self.activity_score <= self.LOW_ACTIVITY_THRESHOLD
    
    def inactive_member_(self) -> bool:
        """Check if member has been inactive (Rails pattern)"""
        if not self.last_active_at:
            return True
        cutoff = datetime.now() - timedelta(days=self.INACTIVE_DAYS_THRESHOLD)
        return self.last_active_at < cutoff
    
    def needs_attention_(self) -> bool:
        """Check if membership needs attention (Rails pattern)"""
        return (self.low_activity_() or 
                self.inactive_member_() or
                self.suspended_() or
                self.expired_())
    
    def activate_(self) -> None:
        """Activate membership (Rails bang method pattern)"""
        self.active = True
        self.status = MembershipStatus.ACTIVE
        self.accepted_at = datetime.now()
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate membership (Rails bang method pattern)"""
        self.active = False
        self.status = MembershipStatus.INACTIVE
        self.deactivated_at = datetime.now()
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deactivation_reason'] = reason
    
    def suspend_(self, reason: str, suspended_until: datetime = None) -> None:
        """Suspend membership (Rails bang method pattern)"""
        self.status = MembershipStatus.SUSPENDED
        self.suspended_at = datetime.now()
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['suspension_reason'] = reason
        if suspended_until:
            self.extra_metadata['suspended_until'] = suspended_until.isoformat()
    
    def unsuspend_(self) -> None:
        """Unsuspend membership (Rails bang method pattern)"""
        if self.status == MembershipStatus.SUSPENDED:
            self.status = MembershipStatus.ACTIVE
            self.suspended_at = None
            self.updated_at = datetime.now()
    
    def ban_(self, reason: str) -> None:
        """Ban member from team (Rails bang method pattern)"""
        self.status = MembershipStatus.BANNED
        self.active = False
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['ban_reason'] = reason
        self.extra_metadata['banned_at'] = datetime.now().isoformat()
    
    def remove_(self, reason: str = None) -> None:
        """Remove member from team (Rails bang method pattern)"""
        self.status = MembershipStatus.REMOVED
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['removal_reason'] = reason
    
    def accept_invitation_(self) -> None:
        """Accept team invitation (Rails bang method pattern)"""
        if self.status == MembershipStatus.INVITED:
            self.status = MembershipStatus.ACTIVE
            self.accepted_at = datetime.now()
            self.active = True
            self.updated_at = datetime.now()
    
    def decline_invitation_(self, reason: str = None) -> None:
        """Decline team invitation (Rails bang method pattern)"""
        if self.status == MembershipStatus.INVITED:
            self.status = MembershipStatus.DECLINED
            self.updated_at = datetime.now()
            
            if reason:
                self.extra_metadata = self.extra_metadata or {}
                self.extra_metadata['decline_reason'] = reason
    
    def promote_(self, new_role: MembershipRole, promoted_by: int = None) -> None:
        """Promote member to higher role (Rails bang method pattern)"""
        if new_role.permission_level > self.role.permission_level:
            old_role = self.role
            self.role = new_role
            self.updated_at = datetime.now()
            
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata.setdefault('role_changes', []).append({
                'from': old_role.value,
                'to': new_role.value,
                'promoted_by': promoted_by,
                'timestamp': datetime.now().isoformat()
            })
    
    def demote_(self, new_role: MembershipRole, demoted_by: int = None) -> None:
        """Demote member to lower role (Rails bang method pattern)"""
        if new_role.permission_level < self.role.permission_level:
            old_role = self.role
            self.role = new_role
            self.updated_at = datetime.now()
            
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata.setdefault('role_changes', []).append({
                'from': old_role.value,
                'to': new_role.value,
                'demoted_by': demoted_by,
                'timestamp': datetime.now().isoformat()
            })
    
    def change_role_(self, new_role: MembershipRole, changed_by: int = None) -> None:
        """Change member role (Rails bang method pattern)"""
        if new_role != self.role:
            old_role = self.role
            self.role = new_role
            self.updated_at = datetime.now()
            
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata.setdefault('role_changes', []).append({
                'from': old_role.value,
                'to': new_role.value,
                'changed_by': changed_by,
                'timestamp': datetime.now().isoformat()
            })
    
    def extend_membership_(self, new_expiry: datetime) -> None:
        """Extend membership expiry (Rails bang method pattern)"""
        if not self.expires_at or new_expiry > self.expires_at:
            self.expires_at = new_expiry
            self.updated_at = datetime.now()
    
    def grant_permission_(self, permission: str) -> None:
        """Grant specific permission (Rails bang method pattern)"""
        permissions = list(self.permissions or [])
        if permission not in permissions:
            permissions.append(permission)
            self.permissions = permissions
            self.updated_at = datetime.now()
    
    def revoke_permission_(self, permission: str) -> None:
        """Revoke specific permission (Rails bang method pattern)"""
        permissions = list(self.permissions or [])
        if permission in permissions:
            permissions.remove(permission)
            self.permissions = permissions
            self.updated_at = datetime.now()
    
    def set_custom_permission_(self, permission: str, value: Any) -> None:
        """Set custom permission value (Rails bang method pattern)"""
        custom_perms = dict(self.custom_permissions or {})
        custom_perms[permission] = value
        self.custom_permissions = custom_perms
        self.updated_at = datetime.now()
    
    def update_activity_score_(self, score: float) -> None:
        """Update activity score (Rails bang method pattern)"""
        if 0 <= score <= 10:
            self.activity_score = score
            self.updated_at = datetime.now()
    
    def update_contribution_score_(self, score: float) -> None:
        """Update contribution score (Rails bang method pattern)"""
        if 0 <= score <= 10:
            self.contribution_score = score
            self.updated_at = datetime.now()
    
    def record_activity_(self, activity_type: str = None) -> None:
        """Record member activity (Rails bang method pattern)"""
        self.last_active_at = datetime.now()
        self.updated_at = datetime.now()
        
        if activity_type:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata.setdefault('recent_activities', []).append({
                'type': activity_type,
                'timestamp': datetime.now().isoformat()
            })
            
            activities = self.extra_metadata['recent_activities']
            if len(activities) > 100:
                self.extra_metadata['recent_activities'] = activities[-100:]
    
    def set_notification_preference_(self, preference: str, value: bool) -> None:
        """Set notification preference (Rails bang method pattern)"""
        prefs = dict(self.notification_preferences or {})
        prefs[preference] = value
        self.notification_preferences = prefs
        self.updated_at = datetime.now()
    
    def calculate_collaboration_score(self) -> float:
        """Calculate collaboration score based on interactions (Rails pattern)"""
        base_score = self.activity_score or 0
        
        if self.can_manage_members_():
            base_score += 2.0
        elif self.can_moderate_():
            base_score += 1.5
        
        if not self.inactive_member_():
            base_score += 1.0
        
        return min(10.0, base_score)
    
    def update_all_scores_(self) -> None:
        """Update all performance scores (Rails bang method pattern)"""
        self.collaboration_score = self.calculate_collaboration_score()
        self.updated_at = datetime.now()
    
    def days_since_joined(self) -> int:
        """Calculate days since joining (Rails pattern)"""
        return (datetime.now() - self.joined_at).days
    
    def days_since_active(self) -> int:
        """Calculate days since last activity (Rails pattern)"""
        if not self.last_active_at:
            return self.days_since_joined()
        return (datetime.now() - self.last_active_at).days
    
    def membership_duration(self) -> int:
        """Calculate membership duration in days (Rails pattern)"""
        if self.status in [MembershipStatus.REMOVED, MembershipStatus.BANNED]:
            return (self.updated_at - self.joined_at).days
        return self.days_since_joined()
    
    def time_until_expiry(self) -> Optional[timedelta]:
        """Calculate time until membership expires (Rails pattern)"""
        if not self.expires_at:
            return None
        return self.expires_at - datetime.now()
    
    def role_history(self) -> List[Dict[str, Any]]:
        """Get role change history (Rails pattern)"""
        return self.extra_metadata.get('role_changes', []) if self.extra_metadata else []
    
    def recent_activities(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent activities (Rails pattern)"""
        activities = self.extra_metadata.get('recent_activities', []) if self.extra_metadata else []
        return activities[-limit:] if activities else []
    
    def permission_summary(self) -> Dict[str, Any]:
        """Get permission summary (Rails pattern)"""
        return {
            'role': self.role.value,
            'permission_level': self.role.permission_level,
            'can_manage_members': self.can_manage_members_(),
            'can_moderate': self.can_moderate_(),
            'specific_permissions': self.permissions or [],
            'custom_permissions': self.custom_permissions or {}
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate membership health report (Rails pattern)"""
        return {
            'membership_id': self.membership_id,
            'user_id': self.user_id,
            'team_id': self.team_id,
            'active': self.active_(),
            'status': self.status.value,
            'role': self.role.value,
            'activity_score': self.activity_score,
            'contribution_score': self.contribution_score,
            'collaboration_score': self.collaboration_score,
            'high_activity': self.high_activity_(),
            'low_activity': self.low_activity_(),
            'inactive': self.inactive_member_(),
            'expired': self.expired_(),
            'needs_attention': self.needs_attention_(),
            'days_since_joined': self.days_since_joined(),
            'days_since_active': self.days_since_active(),
            'membership_duration': self.membership_duration()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'membership_id': self.membership_id,
            'team_id': self.team_id,
            'user_id': self.user_id,
            'role': self.role.value,
            'status': self.status.value,
            'membership_type': self.membership_type.value,
            'active': self.active,
            'activity_score': self.activity_score,
            'contribution_score': self.contribution_score,
            'collaboration_score': self.collaboration_score,
            'joined_at': self.joined_at.isoformat(),
            'last_active_at': self.last_active_at.isoformat() if self.last_active_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'permissions': self.permissions,
                'custom_permissions': self.custom_permissions,
                'notification_preferences': self.notification_preferences,
                'settings': self.settings,
                'metadata': self.extra_metadata,
                'notes': self.notes
            })
        
        return result