"""
OrgMembership Model - Organization membership and access control entity.
Manages user membership in organizations with comprehensive Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import uuid
from ..database import Base


class MembershipStatuses(PyEnum):
    """Membership status enumeration"""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DEACTIVATED = "DEACTIVATED"
    SUSPENDED = "SUSPENDED"
    PENDING = "PENDING"
    INVITED = "INVITED"
    EXPIRED = "EXPIRED"
    BLOCKED = "BLOCKED"


class MembershipRoles(PyEnum):
    """Membership role enumeration"""
    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"
    COLLABORATOR = "collaborator"
    MANAGER = "manager"
    EDITOR = "editor"
    OWNER = "owner"
    GUEST = "guest"


class AccessLevels(PyEnum):
    """Access level enumeration"""
    FULL = "full"
    LIMITED = "limited"
    READ_ONLY = "read_only"
    NO_ACCESS = "no_access"
    CUSTOM = "custom"


class MembershipTypes(PyEnum):
    """Membership type enumeration"""
    DIRECT = "direct"
    INHERITED = "inherited"
    TEMPORARY = "temporary"
    SERVICE = "service"
    API_ONLY = "api_only"


class OrgMembership(Base):
    __tablename__ = "org_memberships_old"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    status = Column(SQLEnum(MembershipStatuses), default=MembershipStatuses.ACTIVE, nullable=False, index=True)
    role = Column(SQLEnum(MembershipRoles), default=MembershipRoles.USER, nullable=False, index=True)
    access_level = Column(SQLEnum(AccessLevels), default=AccessLevels.FULL, index=True)
    membership_type = Column(SQLEnum(MembershipTypes), default=MembershipTypes.DIRECT, index=True)
    
    # Access and authentication
    api_key = Column(String(255), index=True)
    permissions = Column(JSON)  # Specific permissions
    restrictions = Column(JSON)  # Access restrictions
    
    # Activity and usage tracking
    last_activity_at = Column(DateTime, index=True)
    last_login_at = Column(DateTime)
    login_count = Column(Integer, default=0)
    resource_usage_count = Column(Integer, default=0)
    
    # Membership lifecycle
    invited_at = Column(DateTime)
    joined_at = Column(DateTime)
    activated_at = Column(DateTime)
    deactivated_at = Column(DateTime)
    expires_at = Column(DateTime, index=True)
    
    # Notes and metadata
    notes = Column(Text)
    extra_metadata = Column(JSON)
    tags = Column(JSON)
    
    # State flags
    is_default = Column(Boolean, default=False)
    is_primary = Column(Boolean, default=False)
    is_system = Column(Boolean, default=False)
    auto_approve = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Foreign keys
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    invited_by_id = Column(Integer, ForeignKey("users.id"), index=True)
    approved_by_id = Column(Integer, ForeignKey("users.id"), index=True)
    parent_membership_id = Column(Integer, ForeignKey("org_memberships.id"), index=True)
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="org_memberships")
    org = relationship("Org", foreign_keys=[org_id], back_populates="org_memberships")
    invited_by = relationship("User", foreign_keys=[invited_by_id])
    approved_by = relationship("User", foreign_keys=[approved_by_id])
    parent_membership = relationship("OrgMembership", remote_side="OrgMembership.id", foreign_keys=[parent_membership_id])
    child_memberships = relationship("OrgMembership", remote_side="OrgMembership.parent_membership_id")
    
    # Rails business logic constants
    DEFAULT_EXPIRY_DAYS = 365
    API_KEY_LENGTH = 32
    MAX_LOGIN_ATTEMPTS = 5
    ACTIVITY_THRESHOLD_DAYS = 30
    CACHE_TTL_SECONDS = 300
    BULK_OPERATION_BATCH_SIZE = 1000
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self._permission_cache = {}
        self._access_cache = {}
        self._resource_counts = {}
    
    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if membership is active (Rails pattern)"""
        return (self.status == MembershipStatuses.ACTIVE and 
                not self.expired_() and 
                not self.blocked_())
    
    def inactive_(self) -> bool:
        """Check if membership is inactive (Rails pattern)"""
        return self.status == MembershipStatuses.INACTIVE
    
    def deactivated_(self) -> bool:
        """Check if membership is deactivated (Rails pattern)"""
        return self.status == MembershipStatuses.DEACTIVATED
    
    def suspended_(self) -> bool:
        """Check if membership is suspended (Rails pattern)"""
        return self.status == MembershipStatuses.SUSPENDED
    
    def pending_(self) -> bool:
        """Check if membership is pending (Rails pattern)"""
        return self.status == MembershipStatuses.PENDING
    
    def invited_(self) -> bool:
        """Check if membership is in invited state (Rails pattern)"""
        return self.status == MembershipStatuses.INVITED
    
    def expired_(self) -> bool:
        """Check if membership is expired (Rails pattern)"""
        return (self.expires_at is not None and self.expires_at < datetime.now()) or \
               self.status == MembershipStatuses.EXPIRED
    
    def blocked_(self) -> bool:
        """Check if membership is blocked (Rails pattern)"""
        return self.status == MembershipStatuses.BLOCKED
    
    def admin_(self) -> bool:
        """Check if member has admin role (Rails pattern)"""
        return self.role == MembershipRoles.ADMIN
    
    def owner_(self) -> bool:
        """Check if member is org owner (Rails pattern)"""
        return self.role == MembershipRoles.OWNER
    
    def user_role_(self) -> bool:
        """Check if member has user role (Rails pattern)"""
        return self.role == MembershipRoles.USER
    
    def viewer_(self) -> bool:
        """Check if member has viewer role (Rails pattern)"""
        return self.role == MembershipRoles.VIEWER
    
    def collaborator_(self) -> bool:
        """Check if member is collaborator (Rails pattern)"""
        return self.role == MembershipRoles.COLLABORATOR
    
    def manager_(self) -> bool:
        """Check if member is manager (Rails pattern)"""
        return self.role == MembershipRoles.MANAGER
    
    def editor_(self) -> bool:
        """Check if member is editor (Rails pattern)"""
        return self.role == MembershipRoles.EDITOR
    
    def guest_(self) -> bool:
        """Check if member is guest (Rails pattern)"""
        return self.role == MembershipRoles.GUEST
    
    def full_access_(self) -> bool:
        """Check if member has full access (Rails pattern)"""
        return self.access_level == AccessLevels.FULL
    
    def limited_access_(self) -> bool:
        """Check if member has limited access (Rails pattern)"""
        return self.access_level == AccessLevels.LIMITED
    
    def read_only_(self) -> bool:
        """Check if member has read-only access (Rails pattern)"""
        return self.access_level == AccessLevels.READ_ONLY
    
    def no_access_(self) -> bool:
        """Check if member has no access (Rails pattern)"""
        return self.access_level == AccessLevels.NO_ACCESS
    
    def custom_access_(self) -> bool:
        """Check if member has custom access (Rails pattern)"""
        return self.access_level == AccessLevels.CUSTOM
    
    def direct_(self) -> bool:
        """Check if membership is direct (Rails pattern)"""
        return self.membership_type == MembershipTypes.DIRECT
    
    def inherited_(self) -> bool:
        """Check if membership is inherited (Rails pattern)"""
        return self.membership_type == MembershipTypes.INHERITED
    
    def temporary_(self) -> bool:
        """Check if membership is temporary (Rails pattern)"""
        return self.membership_type == MembershipTypes.TEMPORARY
    
    def service_(self) -> bool:
        """Check if membership is service account (Rails pattern)"""
        return self.membership_type == MembershipTypes.SERVICE
    
    def api_only_(self) -> bool:
        """Check if membership is API-only (Rails pattern)"""
        return self.membership_type == MembershipTypes.API_ONLY
    
    def default_(self) -> bool:
        """Check if membership is default (Rails pattern)"""
        return self.is_default is True
    
    def primary_(self) -> bool:
        """Check if membership is primary (Rails pattern)"""
        return self.is_primary is True
    
    def system_(self) -> bool:
        """Check if membership is system-managed (Rails pattern)"""
        return self.is_system is True
    
    def has_api_key_(self) -> bool:
        """Check if membership has API key (Rails pattern)"""
        return self.api_key is not None and len(self.api_key.strip()) > 0
    
    def recently_active_(self, days: int = None) -> bool:
        """Check if member was recently active (Rails pattern)"""
        if not self.last_activity_at:
            return False
        
        threshold_days = days or self.ACTIVITY_THRESHOLD_DAYS
        threshold = datetime.now() - timedelta(days=threshold_days)
        return self.last_activity_at > threshold
    
    def can_login_(self) -> bool:
        """Check if member can log in (Rails pattern)"""
        return (self.active_() and 
                not self.api_only_() and 
                self.access_level != AccessLevels.NO_ACCESS)
    
    def can_access_api_(self) -> bool:
        """Check if member can access API (Rails pattern)"""
        return (self.active_() and 
                self.has_api_key_() and 
                self.access_level != AccessLevels.NO_ACCESS)
    
    def can_manage_org_(self) -> bool:
        """Check if member can manage org (Rails pattern)"""
        return self.role in [MembershipRoles.OWNER, MembershipRoles.ADMIN, MembershipRoles.MANAGER]
    
    def can_invite_users_(self) -> bool:
        """Check if member can invite users (Rails pattern)"""
        return self.role in [MembershipRoles.OWNER, MembershipRoles.ADMIN, MembershipRoles.MANAGER]
    
    def can_read_resources_(self) -> bool:
        """Check if member can read resources (Rails pattern)"""
        return self.access_level not in [AccessLevels.NO_ACCESS]
    
    def can_write_resources_(self) -> bool:
        """Check if member can write resources (Rails pattern)"""
        return self.access_level in [AccessLevels.FULL, AccessLevels.LIMITED] and \
               self.role not in [MembershipRoles.VIEWER, MembershipRoles.GUEST]
    
    def can_delete_resources_(self) -> bool:
        """Check if member can delete resources (Rails pattern)"""
        return self.access_level == AccessLevels.FULL and \
               self.role in [MembershipRoles.OWNER, MembershipRoles.ADMIN, MembershipRoles.MANAGER]
    
    def privileged_(self) -> bool:
        """Check if member has privileged access (Rails pattern)"""
        return self.role in [MembershipRoles.OWNER, MembershipRoles.ADMIN]
    
    def has_children_(self) -> bool:
        """Check if membership has child memberships (Rails pattern)"""
        return len(self.child_memberships or []) > 0
    
    def has_parent_(self) -> bool:
        """Check if membership has parent (Rails pattern)"""
        return self.parent_membership_id is not None
    
    def needs_approval_(self) -> bool:
        """Check if membership needs approval (Rails pattern)"""
        return self.pending_() and not self.auto_approve
    
    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self) -> None:
        """Activate membership (Rails bang method pattern)"""
        if self.active_():
            return
        
        self.status = MembershipStatuses.ACTIVE
        self.activated_at = datetime.now()
        self.updated_at = datetime.now()
        
        if not self.joined_at:
            self.joined_at = datetime.now()
        
        self._clear_cache()
    
    def deactivate_(self, reason: str = None, delegate_owner_id: int = None, 
                   pause_data_flows: bool = False) -> None:
        """Deactivate membership with resource delegation (Rails bang method pattern)"""
        if self.deactivated_():
            return
        
        try:
            # Update status and timestamp
            self.status = MembershipStatuses.DEACTIVATED
            self.deactivated_at = datetime.now()
            self.updated_at = datetime.now()
            
            # Add deactivation reason to metadata
            if reason:
                self.extra_metadata = self.extra_metadata or {}
                self.extra_metadata['deactivation_reason'] = reason
                self.extra_metadata['deactivated_at'] = datetime.now().isoformat()
            
            # Pause user flows if requested
            if pause_data_flows and self.user:
                self._pause_user_flows()
            
            # Handle resource delegation if specified
            if delegate_owner_id:
                self._transfer_resources_to_delegate(delegate_owner_id)
            
            self._clear_cache()
            
        except Exception as e:
            raise ValueError(f"Failed to deactivate membership: {e}")
    
    def suspend_(self, reason: str = None, duration_days: int = None) -> None:
        """Suspend membership (Rails bang method pattern)"""
        if self.suspended_():
            return
        
        self.status = MembershipStatuses.SUSPENDED
        self.updated_at = datetime.now()
        
        if reason or duration_days:
            self.extra_metadata = self.extra_metadata or {}
            if reason:
                self.extra_metadata['suspension_reason'] = reason
            if duration_days:
                self.extra_metadata['suspension_duration_days'] = duration_days
                suspension_end = datetime.now() + timedelta(days=duration_days)
                self.extra_metadata['suspended_until'] = suspension_end.isoformat()
            self.extra_metadata['suspended_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def unsuspend_(self) -> None:
        """Unsuspend membership (Rails bang method pattern)"""
        if not self.suspended_():
            return
        
        self.status = MembershipStatuses.ACTIVE
        self.updated_at = datetime.now()
        
        # Clear suspension metadata
        if self.extra_metadata:
            suspension_keys = ['suspension_reason', 'suspension_duration_days', 
                             'suspended_until', 'suspended_at']
            for key in suspension_keys:
                self.extra_metadata.pop(key, None)
            
            self.extra_metadata['unsuspended_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def block_(self, reason: str = None) -> None:
        """Block membership (Rails bang method pattern)"""
        if self.blocked_():
            return
        
        self.status = MembershipStatuses.BLOCKED
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['block_reason'] = reason
            self.extra_metadata['blocked_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def unblock_(self) -> None:
        """Unblock membership (Rails bang method pattern)"""
        if not self.blocked_():
            return
        
        self.status = MembershipStatuses.ACTIVE
        self.updated_at = datetime.now()
        
        # Clear block metadata
        if self.extra_metadata:
            self.extra_metadata.pop('block_reason', None)
            self.extra_metadata.pop('blocked_at', None)
            self.extra_metadata['unblocked_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def expire_(self) -> None:
        """Expire membership (Rails bang method pattern)"""
        if self.expired_():
            return
        
        self.status = MembershipStatuses.EXPIRED
        self.expires_at = datetime.now()
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def approve_(self, approver_id: int = None) -> None:
        """Approve pending membership (Rails bang method pattern)"""
        if not self.pending_():
            return
        
        self.status = MembershipStatuses.ACTIVE
        self.activated_at = datetime.now()
        self.joined_at = datetime.now()
        self.approved_by_id = approver_id
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def set_role_(self, role: MembershipRoles) -> None:
        """Set membership role (Rails bang method pattern)"""
        if self.role == role:
            return
        
        old_role = self.role
        self.role = role
        self.updated_at = datetime.now()
        
        # Log role change in metadata
        self.extra_metadata = self.extra_metadata or {}
        role_changes = self.extra_metadata.get('role_changes', [])
        role_changes.append({
            'from': old_role.value if old_role else None,
            'to': role.value,
            'changed_at': datetime.now().isoformat()
        })
        self.extra_metadata['role_changes'] = role_changes[-10:]  # Keep last 10 changes
        
        self._clear_cache()
    
    def set_access_level_(self, access_level: AccessLevels) -> None:
        """Set access level (Rails bang method pattern)"""
        if self.access_level == access_level:
            return
        
        old_access = self.access_level
        self.access_level = access_level
        self.updated_at = datetime.now()
        
        # Log access change in metadata
        self.extra_metadata = self.extra_metadata or {}
        access_changes = self.extra_metadata.get('access_changes', [])
        access_changes.append({
            'from': old_access.value if old_access else None,
            'to': access_level.value,
            'changed_at': datetime.now().isoformat()
        })
        self.extra_metadata['access_changes'] = access_changes[-10:]
        
        self._clear_cache()
    
    def generate_api_key_(self) -> str:
        """Generate new API key (Rails bang method pattern)"""
        import secrets
        import string
        
        alphabet = string.ascii_letters + string.digits
        api_key = ''.join(secrets.choice(alphabet) for _ in range(self.API_KEY_LENGTH))
        
        self.api_key = api_key
        self.updated_at = datetime.now()
        
        # Log API key generation
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['api_key_generated_at'] = datetime.now().isoformat()
        
        return api_key
    
    def revoke_api_key_(self) -> None:
        """Revoke API key (Rails bang method pattern)"""
        if not self.has_api_key_():
            return
        
        self.api_key = None
        self.updated_at = datetime.now()
        
        # Log API key revocation
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['api_key_revoked_at'] = datetime.now().isoformat()
    
    def extend_expiry_(self, days: int) -> None:
        """Extend membership expiry (Rails bang method pattern)"""
        extension = timedelta(days=days)
        
        if self.expires_at:
            self.expires_at += extension
        else:
            self.expires_at = datetime.now() + extension
        
        self.updated_at = datetime.now()
        
        # Log expiry extension
        self.extra_metadata = self.extra_metadata or {}
        extensions = self.extra_metadata.get('expiry_extensions', [])
        extensions.append({
            'days': days,
            'extended_at': datetime.now().isoformat(),
            'new_expiry': self.expires_at.isoformat()
        })
        self.extra_metadata['expiry_extensions'] = extensions[-10:]
    
    def record_login_(self) -> None:
        """Record user login (Rails bang method pattern)"""
        self.last_login_at = datetime.now()
        self.last_activity_at = datetime.now()
        self.login_count += 1
        self.updated_at = datetime.now()
    
    def record_activity_(self) -> None:
        """Record user activity (Rails bang method pattern)"""
        self.last_activity_at = datetime.now()
        self.updated_at = datetime.now()
    
    def increment_resource_usage_(self, count: int = 1) -> None:
        """Increment resource usage count (Rails bang method pattern)"""
        self.resource_usage_count += count
        self.last_activity_at = datetime.now()
        self.updated_at = datetime.now()
    
    def add_permission_(self, permission: str) -> None:
        """Add specific permission (Rails bang method pattern)"""
        if not self.permissions:
            self.permissions = []
        
        if permission not in self.permissions:
            self.permissions.append(permission)
            self.updated_at = datetime.now()
            self._clear_cache()
    
    def remove_permission_(self, permission: str) -> None:
        """Remove specific permission (Rails bang method pattern)"""
        if self.permissions and permission in self.permissions:
            self.permissions.remove(permission)
            self.updated_at = datetime.now()
            self._clear_cache()
    
    def add_tag_(self, tag_name: str) -> None:
        """Add tag to membership (Rails bang method pattern)"""
        if not self.tags:
            self.tags = []
        if tag_name not in self.tags:
            self.tags.append(tag_name)
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag_name: str) -> None:
        """Remove tag from membership (Rails bang method pattern)"""
        if self.tags and tag_name in self.tags:
            self.tags.remove(tag_name)
            self.updated_at = datetime.now()
    
    # ========================================
    # Rails Scopes
    # ========================================
    
    @classmethod
    def active(cls, db: Session):
        """Rails scope: Get active memberships"""
        return db.query(cls).filter(
            cls.status == MembershipStatuses.ACTIVE,
            (cls.expires_at.is_(None)) | (cls.expires_at > datetime.utcnow())
        )
    
    @classmethod
    def inactive(cls, db: Session):
        """Rails scope: Get inactive memberships"""
        return db.query(cls).filter(cls.status == MembershipStatuses.INACTIVE)
    
    @classmethod
    def pending(cls, db: Session):
        """Rails scope: Get pending memberships"""
        return db.query(cls).filter(cls.status == MembershipStatuses.PENDING)
    
    @classmethod
    def expired(cls, db: Session):
        """Rails scope: Get expired memberships"""
        return db.query(cls).filter(
            (cls.expires_at.isnot(None) & (cls.expires_at < datetime.utcnow())) |
            (cls.status == MembershipStatuses.EXPIRED)
        )
    
    @classmethod
    def by_user(cls, db: Session, user_id: int):
        """Rails scope: Get memberships by user"""
        return db.query(cls).filter(cls.user_id == user_id)
    
    @classmethod
    def by_org(cls, db: Session, org_id: int):
        """Rails scope: Get memberships by organization"""
        return db.query(cls).filter(cls.org_id == org_id)
    
    @classmethod
    def by_role(cls, db: Session, role: MembershipRoles):
        """Rails scope: Get memberships by role"""
        return db.query(cls).filter(cls.role == role)
    
    @classmethod
    def admins(cls, db: Session):
        """Rails scope: Get admin memberships"""
        return db.query(cls).filter(
            cls.role.in_([MembershipRoles.ADMIN, MembershipRoles.OWNER])
        )
    
    @classmethod
    def users(cls, db: Session):
        """Rails scope: Get user memberships"""
        return db.query(cls).filter(cls.role == MembershipRoles.USER)
    
    @classmethod
    def viewers(cls, db: Session):
        """Rails scope: Get viewer memberships"""
        return db.query(cls).filter(cls.role == MembershipRoles.VIEWER)
    
    @classmethod
    def privileged(cls, db: Session):
        """Rails scope: Get privileged memberships"""
        return db.query(cls).filter(
            cls.role.in_([MembershipRoles.OWNER, MembershipRoles.ADMIN, MembershipRoles.MANAGER])
        )
    
    @classmethod
    def recently_active(cls, db: Session, days: int = 30):
        """Rails scope: Get recently active memberships"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(cls.last_activity_at >= cutoff)
    
    @classmethod
    def with_api_access(cls, db: Session):
        """Rails scope: Get memberships with API access"""
        return db.query(cls).filter(
            cls.api_key.isnot(None),
            cls.access_level != AccessLevels.NO_ACCESS,
            cls.status == MembershipStatuses.ACTIVE
        )
    
    @classmethod
    def direct_memberships(cls, db: Session):
        """Rails scope: Get direct memberships"""
        return db.query(cls).filter(cls.membership_type == MembershipTypes.DIRECT)
    
    @classmethod
    def inherited_memberships(cls, db: Session):
        """Rails scope: Get inherited memberships"""
        return db.query(cls).filter(cls.membership_type == MembershipTypes.INHERITED)
    
    @classmethod
    def service_accounts(cls, db: Session):
        """Rails scope: Get service account memberships"""
        return db.query(cls).filter(cls.membership_type == MembershipTypes.SERVICE)
    
    @classmethod
    def needs_approval(cls, db: Session):
        """Rails scope: Get memberships needing approval"""
        return db.query(cls).filter(
            cls.status == MembershipStatuses.PENDING,
            cls.auto_approve.is_(False)
        )
    
    @classmethod
    def deactivated(cls, db: Session):
        """Rails scope: Get deactivated memberships"""
        return db.query(cls).filter(cls.status == MembershipStatuses.DEACTIVATED)
    
    @classmethod
    def suspended(cls, db: Session):
        """Rails scope: Get suspended memberships"""
        return db.query(cls).filter(cls.status == MembershipStatuses.SUSPENDED)
    
    @classmethod
    def blocked(cls, db: Session):
        """Rails scope: Get blocked memberships"""
        return db.query(cls).filter(cls.status == MembershipStatuses.BLOCKED)
    
    @classmethod
    def invited(cls, db: Session):
        """Rails scope: Get invited memberships"""
        return db.query(cls).filter(cls.status == MembershipStatuses.INVITED)
    
    @classmethod
    def by_access_level(cls, db: Session, access_level: AccessLevels):
        """Rails scope: Get memberships by access level"""
        return db.query(cls).filter(cls.access_level == access_level)
    
    @classmethod
    def full_access(cls, db: Session):
        """Rails scope: Get memberships with full access"""
        return db.query(cls).filter(cls.access_level == AccessLevels.FULL)
    
    @classmethod
    def read_only(cls, db: Session):
        """Rails scope: Get read-only memberships"""
        return db.query(cls).filter(cls.access_level == AccessLevels.READ_ONLY)
    
    @classmethod
    def with_tags(cls, db: Session, tags: List[str]):
        """Rails scope: Get memberships with specific tags"""
        from sqlalchemy import func
        return db.query(cls).filter(
            func.json_contains(cls.tags, json.dumps(tags))
        )
    
    @classmethod
    def expiring_soon(cls, db: Session, days: int = 30):
        """Rails scope: Get memberships expiring soon"""
        cutoff = datetime.utcnow() + timedelta(days=days)
        return db.query(cls).filter(
            cls.expires_at.isnot(None),
            cls.expires_at <= cutoff,
            cls.status == MembershipStatuses.ACTIVE
        )
    
    @classmethod
    def by_membership_type(cls, db: Session, membership_type: MembershipTypes):
        """Rails scope: Get memberships by type"""
        return db.query(cls).filter(cls.membership_type == membership_type)
    
    @classmethod
    def default_memberships(cls, db: Session):
        """Rails scope: Get default memberships"""
        return db.query(cls).filter(cls.is_default == True)
    
    @classmethod
    def primary_memberships(cls, db: Session):
        """Rails scope: Get primary memberships"""
        return db.query(cls).filter(cls.is_primary == True)
    
    @classmethod
    def system_memberships(cls, db: Session):
        """Rails scope: Get system memberships"""
        return db.query(cls).filter(cls.is_system == True)
    
    @classmethod
    def recent(cls, db: Session, days: int = 30):
        """Rails scope: Get recently created memberships"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(cls.created_at >= cutoff)
    
    @classmethod
    def create_with_defaults(cls, user, org, role: MembershipRoles = None, **kwargs):
        """Factory method to create membership with defaults (Rails pattern)"""
        membership_data = {
            'user': user,
            'org': org,
            'role': role or MembershipRoles.USER,
            'status': MembershipStatuses.ACTIVE,
            'membership_type': MembershipTypes.DIRECT,
            'access_level': AccessLevels.FULL,
            'joined_at': datetime.now(),
            'activated_at': datetime.now(),
            **kwargs
        }
        
        return cls(**membership_data)
    
    @classmethod
    def create_invitation(cls, user, org, invited_by, role: MembershipRoles = None, **kwargs):
        """Factory method to create membership invitation (Rails pattern)"""
        membership_data = {
            'user': user,
            'org': org,
            'invited_by': invited_by,
            'role': role or MembershipRoles.USER,
            'status': MembershipStatuses.INVITED,
            'membership_type': MembershipTypes.DIRECT,
            'invited_at': datetime.now(),
            **kwargs
        }
        
        return cls(**membership_data)
    
    @classmethod
    def bulk_activate(cls, membership_ids: List[int]):
        """Bulk activate memberships (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_deactivate(cls, membership_ids: List[int], reason: str = None):
        """Bulk deactivate memberships (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def cleanup_expired(cls) -> int:
        """Clean up expired memberships (Rails pattern)"""
        # Implementation would handle expired memberships
        return 0
    
    # ========================================
    # Rails Instance Methods
    # ========================================
    
    def has_permission(self, permission: str) -> bool:
        """Check if membership has specific permission (Rails pattern)"""
        # Check cached permissions first
        if permission in self._permission_cache:
            return self._permission_cache[permission]
        
        # Check explicit permissions
        if self.permissions and permission in self.permissions:
            self._permission_cache[permission] = True
            return True
        
        # Check role-based permissions
        role_permissions = self._get_role_permissions()
        has_perm = permission in role_permissions
        
        self._permission_cache[permission] = has_perm
        return has_perm
    
    def get_effective_permissions(self) -> List[str]:
        """Get all effective permissions for membership (Rails pattern)"""
        permissions = set()
        
        # Add explicit permissions
        if self.permissions:
            permissions.update(self.permissions)
        
        # Add role-based permissions
        permissions.update(self._get_role_permissions())
        
        return list(permissions)
    
    def create_child_membership(self, user, role: MembershipRoles = None, **kwargs):
        """Create child membership (Rails pattern)"""
        child_data = {
            'user': user,
            'org': self.org,
            'role': role or MembershipRoles.USER,
            'parent_membership': self,
            'membership_type': MembershipTypes.INHERITED,
            'status': MembershipStatuses.ACTIVE,
            **kwargs
        }
        
        return self.__class__(**child_data)
    
    def transfer_to_org(self, target_org, role: MembershipRoles = None):
        """Transfer membership to another org (Rails pattern)"""
        if not self.can_be_transferred_():
            raise ValueError("Membership cannot be transferred")
        
        transfer_data = {
            'user': self.user,
            'org': target_org,
            'role': role or self.role,
            'status': MembershipStatuses.PENDING,
            'membership_type': self.membership_type,
            'access_level': self.access_level,
            'permissions': self.permissions.copy() if self.permissions else None,
            'notes': f"Transferred from org {self.org_id}"
        }
        
        return self.__class__(**transfer_data)
    
    def get_activity_summary(self) -> Dict[str, Any]:
        """Get activity summary (Rails pattern)"""
        return {
            'login_count': self.login_count,
            'resource_usage_count': self.resource_usage_count,
            'last_login_at': self.last_login_at.isoformat() if self.last_login_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'recently_active': self.recently_active_(),
            'days_since_last_activity': self._days_since_last_activity()
        }
    
    def get_membership_history(self) -> List[Dict[str, Any]]:
        """Get membership change history (Rails pattern)"""
        history = []
        
        if self.extra_metadata:
            # Role changes
            if 'role_changes' in self.extra_metadata:
                for change in self.extra_metadata['role_changes']:
                    history.append({
                        'type': 'role_change',
                        'from': change['from'],
                        'to': change['to'],
                        'timestamp': change['changed_at']
                    })
            
            # Access changes
            if 'access_changes' in self.extra_metadata:
                for change in self.extra_metadata['access_changes']:
                    history.append({
                        'type': 'access_change',
                        'from': change['from'],
                        'to': change['to'],
                        'timestamp': change['changed_at']
                    })
            
            # Status changes from metadata
            status_events = ['suspended_at', 'unsuspended_at', 'blocked_at', 'unblocked_at', 'deactivated_at']
            for event in status_events:
                if event in self.extra_metadata:
                    history.append({
                        'type': event.replace('_at', ''),
                        'timestamp': self.extra_metadata[event]
                    })
        
        # Sort by timestamp
        history.sort(key=lambda x: x['timestamp'], reverse=True)
        return history
    
    def can_be_transferred_(self) -> bool:
        """Check if membership can be transferred (Rails pattern)"""
        return not self.system_() and not self.owner_()
    
    def has_tag(self, tag_name: str) -> bool:
        """Check if membership has specific tag (Rails pattern)"""
        return bool(self.tags and tag_name in self.tags)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        return self.tags or []
    
    def permissions_list(self) -> List[str]:
        """Get list of explicit permissions (Rails pattern)"""
        return self.permissions or []
    
    def _get_role_permissions(self) -> List[str]:
        """Get permissions for current role (Rails private pattern)"""
        role_permissions = {
            MembershipRoles.OWNER: [
                'org.manage', 'org.delete', 'users.manage', 'resources.manage',
                'resources.create', 'resources.read', 'resources.update', 'resources.delete',
                'api.access', 'admin.access'
            ],
            MembershipRoles.ADMIN: [
                'org.manage', 'users.manage', 'resources.manage',
                'resources.create', 'resources.read', 'resources.update', 'resources.delete',
                'api.access', 'admin.access'
            ],
            MembershipRoles.MANAGER: [
                'users.invite', 'resources.manage',
                'resources.create', 'resources.read', 'resources.update',
                'api.access'
            ],
            MembershipRoles.USER: [
                'resources.create', 'resources.read', 'resources.update',
                'api.access'
            ],
            MembershipRoles.EDITOR: [
                'resources.read', 'resources.update', 'api.access'
            ],
            MembershipRoles.COLLABORATOR: [
                'resources.read', 'resources.update', 'api.access'
            ],
            MembershipRoles.VIEWER: [
                'resources.read', 'api.access'
            ],
            MembershipRoles.GUEST: [
                'resources.read'
            ]
        }
        
        return role_permissions.get(self.role, [])
    
    def _days_since_last_activity(self) -> Optional[int]:
        """Get days since last activity (Rails private pattern)"""
        if not self.last_activity_at:
            return None
        
        delta = datetime.now() - self.last_activity_at
        return delta.days
    
    def _pause_user_flows(self) -> None:
        """Pause user's data flows (Rails private pattern)"""
        # Implementation would pause user's data flows in the organization
        pass
    
    def _transfer_resources_to_delegate(self, delegate_owner_id: int) -> None:
        """Transfer resources to delegate owner (Rails private pattern)"""
        # Implementation would transfer user's resources to the delegate
        pass
    
    def _clear_cache(self) -> None:
        """Clear internal caches (Rails private pattern)"""
        self._permission_cache.clear()
        self._access_cache.clear()
        self._resource_counts.clear()
    
    # ========================================
    # Rails Validation and Display Methods
    # ========================================
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        user_name = self.user.name if self.user else f"User #{self.user_id}"
        org_name = self.org.name if self.org else f"Org #{self.org_id}"
        return f"{user_name} in {org_name}"
    
    def display_role(self) -> str:
        """Get formatted role for display (Rails pattern)"""
        return self.role.value.replace('_', ' ').title()
    
    def display_status(self) -> str:
        """Get formatted status for display (Rails pattern)"""
        return self.status.value.replace('_', ' ').title()
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            MembershipStatuses.ACTIVE: 'green',
            MembershipStatuses.PENDING: 'yellow',
            MembershipStatuses.INVITED: 'blue',
            MembershipStatuses.SUSPENDED: 'orange',
            MembershipStatuses.DEACTIVATED: 'gray',
            MembershipStatuses.EXPIRED: 'red',
            MembershipStatuses.BLOCKED: 'red'
        }
        return status_colors.get(self.status, 'gray')
    
    def role_color(self) -> str:
        """Get role color for UI (Rails pattern)"""
        role_colors = {
            MembershipRoles.OWNER: 'purple',
            MembershipRoles.ADMIN: 'red',
            MembershipRoles.MANAGER: 'orange',
            MembershipRoles.USER: 'blue',
            MembershipRoles.EDITOR: 'green',
            MembershipRoles.COLLABORATOR: 'teal',
            MembershipRoles.VIEWER: 'gray',
            MembershipRoles.GUEST: 'gray'
        }
        return role_colors.get(self.role, 'blue')
    
    def validate_for_activation(self) -> Tuple[bool, List[str]]:
        """Validate membership can be activated (Rails pattern)"""
        errors = []
        
        if self.active_():
            errors.append("Membership is already active")
        
        if self.expired_():
            errors.append("Membership has expired")
        
        if not self.user:
            errors.append("User is required")
        
        if not self.org:
            errors.append("Organization is required")
        
        return len(errors) == 0, errors
    
    # ========================================
    # Rails API and Serialization Methods
    # ========================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for basic API responses (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'status': self.status.value,
            'display_status': self.display_status(),
            'status_color': self.status_color(),
            'role': self.role.value,
            'display_role': self.display_role(),
            'role_color': self.role_color(),
            'access_level': self.access_level.value,
            'membership_type': self.membership_type.value,
            'active': self.active_(),
            'admin': self.admin_(),
            'privileged': self.privileged_(),
            'can_login': self.can_login_(),
            'can_access_api': self.can_access_api_(),
            'has_api_key': self.has_api_key_(),
            'recently_active': self.recently_active_(),
            'default': self.default_(),
            'primary': self.primary_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'user_id': self.user_id,
            'org_id': self.org_id,
            'tags': self.tags_list()
        }
    
    def to_detailed_dict(self) -> Dict[str, Any]:
        """Convert to detailed dictionary for full API responses (Rails pattern)"""
        base_dict = self.to_dict()
        
        detailed_info = {
            'invited_by_id': self.invited_by_id,
            'approved_by_id': self.approved_by_id,
            'parent_membership_id': self.parent_membership_id,
            'permissions': self.permissions_list(),
            'effective_permissions': self.get_effective_permissions(),
            'notes': self.notes,
            'metadata': self.extra_metadata,
            'activity_summary': self.get_activity_summary(),
            'membership_history': self.get_membership_history(),
            'invited_at': self.invited_at.isoformat() if self.invited_at else None,
            'joined_at': self.joined_at.isoformat() if self.joined_at else None,
            'activated_at': self.activated_at.isoformat() if self.activated_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'relationships': {
                'user_name': self.user.name if self.user else None,
                'org_name': self.org.name if self.org else None,
                'invited_by_name': self.invited_by.name if self.invited_by else None,
                'approved_by_name': self.approved_by.name if self.approved_by else None,
                'child_count': len(self.child_memberships or [])
            }
        }
        
        base_dict.update(detailed_info)
        return base_dict
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for audit logging (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'status': self.status.value,
            'role': self.role.value,
            'access_level': self.access_level.value,
            'user_id': self.user_id,
            'org_id': self.org_id,
            'active': self.active_(),
            'has_api_key': self.has_api_key_(),
            'login_count': self.login_count,
            'resource_usage_count': self.resource_usage_count,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self) -> str:
        return f"<OrgMembership(id={self.id}, user_id={self.user_id}, org_id={self.org_id}, role='{self.role.value}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        return f"OrgMembership: {self.display_name()} ({self.display_role()}) - {self.display_status()}"