"""
OrgMembership Enhanced Model - Advanced organization membership and access control entity.
Manages user membership in organizations with comprehensive Rails business logic patterns,
enterprise features, and advanced security controls.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, Index
from sqlalchemy.orm import relationship, sessionmaker, Session, validates
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple, Set
from enum import Enum as PyEnum
import json
import uuid
import secrets
import string
import hashlib
import logging
from ..database import Base

logger = logging.getLogger(__name__)

class MembershipStatuses(PyEnum):
    """Enhanced membership status enumeration"""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DEACTIVATED = "DEACTIVATED"
    SUSPENDED = "SUSPENDED"
    PENDING = "PENDING"
    INVITED = "INVITED"
    EXPIRED = "EXPIRED"
    BLOCKED = "BLOCKED"
    LOCKED = "LOCKED"
    TERMINATED = "TERMINATED"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    REJECTED = "REJECTED"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.DEACTIVATED: "Deactivated",
            self.SUSPENDED: "Suspended",
            self.PENDING: "Pending",
            self.INVITED: "Invited",
            self.EXPIRED: "Expired",
            self.BLOCKED: "Blocked",
            self.LOCKED: "Account Locked",
            self.TERMINATED: "Terminated",
            self.PENDING_APPROVAL: "Pending Approval",
            self.REJECTED: "Rejected"
        }.get(self, "Unknown Status")

class MembershipRoles(PyEnum):
    """Enhanced membership role enumeration"""
    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"
    COLLABORATOR = "collaborator"
    MANAGER = "manager"
    EDITOR = "editor"
    OWNER = "owner"
    GUEST = "guest"
    AUDITOR = "auditor"
    DEVELOPER = "developer"
    ANALYST = "analyst"
    
    @property
    def display_name(self) -> str:
        return {
            self.ADMIN: "Administrator",
            self.USER: "User",
            self.VIEWER: "Viewer",
            self.COLLABORATOR: "Collaborator", 
            self.MANAGER: "Manager",
            self.EDITOR: "Editor",
            self.OWNER: "Owner",
            self.GUEST: "Guest",
            self.AUDITOR: "Auditor",
            self.DEVELOPER: "Developer",
            self.ANALYST: "Analyst"
        }.get(self, "Unknown Role")

class AccessLevels(PyEnum):
    """Enhanced access level enumeration"""
    FULL = "full"
    LIMITED = "limited"
    READ_ONLY = "read_only"
    NO_ACCESS = "no_access"
    CUSTOM = "custom"
    RESTRICTED = "restricted"
    AUDIT_ONLY = "audit_only"
    
    @property
    def display_name(self) -> str:
        return {
            self.FULL: "Full Access",
            self.LIMITED: "Limited Access",
            self.READ_ONLY: "Read Only",
            self.NO_ACCESS: "No Access",
            self.CUSTOM: "Custom Access",
            self.RESTRICTED: "Restricted Access",
            self.AUDIT_ONLY: "Audit Only"
        }.get(self, "Unknown Access Level")

class MembershipTypes(PyEnum):
    """Enhanced membership type enumeration"""
    DIRECT = "direct"
    INHERITED = "inherited"
    TEMPORARY = "temporary"
    SERVICE = "service"
    API_ONLY = "api_only"
    SSO = "sso"
    FEDERATED = "federated"
    
    @property
    def display_name(self) -> str:
        return {
            self.DIRECT: "Direct Membership",
            self.INHERITED: "Inherited Membership",
            self.TEMPORARY: "Temporary Access",
            self.SERVICE: "Service Account",
            self.API_ONLY: "API Only Access",
            self.SSO: "SSO Integration",
            self.FEDERATED: "Federated Access"
        }.get(self, "Unknown Type")

class MembershipPriority(PyEnum):
    """Membership priority levels"""
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    
    @property
    def display_name(self) -> str:
        return {
            self.LOW: "Low Priority",
            self.NORMAL: "Normal Priority",
            self.HIGH: "High Priority",
            self.CRITICAL: "Critical Priority"
        }.get(self, "Unknown Priority")

class SecurityLevel(PyEnum):
    """Security clearance levels"""
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"
    SECRET = "SECRET"
    
    @property
    def display_name(self) -> str:
        return {
            self.PUBLIC: "Public",
            self.INTERNAL: "Internal Use",
            self.CONFIDENTIAL: "Confidential",
            self.RESTRICTED: "Restricted",
            self.SECRET: "Secret"
        }.get(self, "Unknown Security Level")

class OrgMembershipEnhanced(Base):
    __tablename__ = "org_memberships"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    status = Column(SQLEnum(MembershipStatuses), default=MembershipStatuses.ACTIVE, nullable=False, index=True)
    role = Column(SQLEnum(MembershipRoles), default=MembershipRoles.USER, nullable=False, index=True)
    access_level = Column(SQLEnum(AccessLevels), default=AccessLevels.FULL, index=True)
    membership_type = Column(SQLEnum(MembershipTypes), default=MembershipTypes.DIRECT, index=True)
    priority = Column(SQLEnum(MembershipPriority), default=MembershipPriority.NORMAL, index=True)
    security_level = Column(SQLEnum(SecurityLevel), default=SecurityLevel.INTERNAL, index=True)
    
    # Access and authentication
    api_key = Column(String(255), index=True)
    api_key_hash = Column(String(128), index=True)
    permissions = Column(JSON)
    restrictions = Column(JSON)
    access_patterns = Column(JSON)
    
    # Enhanced security fields
    mfa_enabled = Column(Boolean, default=False, index=True)
    mfa_secret = Column(String(32))
    backup_codes = Column(JSON)
    failed_login_attempts = Column(Integer, default=0)
    last_failed_login_at = Column(DateTime)
    account_locked_until = Column(DateTime)
    password_last_changed_at = Column(DateTime)
    
    # Activity and usage tracking
    last_activity_at = Column(DateTime, index=True)
    last_login_at = Column(DateTime, index=True)
    last_api_access_at = Column(DateTime, index=True)
    login_count = Column(Integer, default=0)
    api_request_count = Column(Integer, default=0)
    resource_usage_count = Column(Integer, default=0)
    data_transfer_bytes = Column(Integer, default=0)
    
    # Usage quotas and limits
    monthly_api_quota = Column(Integer, default=10000)
    monthly_api_usage = Column(Integer, default=0)
    storage_quota_mb = Column(Integer, default=1000)
    storage_usage_mb = Column(Integer, default=0)
    bandwidth_quota_mbps = Column(Integer, default=100)
    concurrent_sessions_limit = Column(Integer, default=5)
    active_sessions_count = Column(Integer, default=0)
    
    # Membership lifecycle
    invited_at = Column(DateTime, index=True)
    joined_at = Column(DateTime, index=True)
    activated_at = Column(DateTime, index=True)
    deactivated_at = Column(DateTime, index=True)
    suspended_at = Column(DateTime, index=True)
    expires_at = Column(DateTime, index=True)
    last_reminder_sent_at = Column(DateTime)
    grace_period_end_at = Column(DateTime)
    
    # Compliance and audit
    compliance_status = Column(String(50), default="compliant", index=True)
    last_compliance_check_at = Column(DateTime)
    audit_log_retention_days = Column(Integer, default=90)
    data_classification = Column(String(50), default="internal")
    
    # Notes and metadata
    notes = Column(Text)
    extra_metadata = Column(JSON)
    tags = Column(JSON)
    custom_fields = Column(JSON)
    
    # State flags
    is_default = Column(Boolean, default=False, index=True)
    is_primary = Column(Boolean, default=False, index=True)
    is_system = Column(Boolean, default=False, index=True)
    is_service_account = Column(Boolean, default=False, index=True)
    auto_approve = Column(Boolean, default=False)
    email_notifications_enabled = Column(Boolean, default=True)
    audit_exempt = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, index=True)
    
    # Foreign keys
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    invited_by_id = Column(Integer, ForeignKey("users.id"), index=True)
    approved_by_id = Column(Integer, ForeignKey("users.id"), index=True)
    parent_membership_id = Column(Integer, ForeignKey("org_memberships.id"), index=True)
    billing_contact_id = Column(Integer, ForeignKey("users.id"), index=True)
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="org_memberships")
    org = relationship("Org", foreign_keys=[org_id], back_populates="org_memberships")
    invited_by = relationship("User", foreign_keys=[invited_by_id])
    approved_by = relationship("User", foreign_keys=[approved_by_id])
    billing_contact = relationship("User", foreign_keys=[billing_contact_id])
    parent_membership = relationship("OrgMembershipEnhanced", remote_side="OrgMembershipEnhanced.id", foreign_keys=[parent_membership_id])
    child_memberships = relationship("OrgMembershipEnhanced", remote_side="OrgMembershipEnhanced.parent_membership_id")
    
    # Enhanced database indexes
    __table_args__ = (
        Index('idx_org_memberships_status_active', 'status', 'is_default'),
        Index('idx_org_memberships_user_org', 'user_id', 'org_id'),
        Index('idx_org_memberships_role_access', 'role', 'access_level'),
        Index('idx_org_memberships_type_priority', 'membership_type', 'priority'),
        Index('idx_org_memberships_security_level', 'security_level', 'status'),
        Index('idx_org_memberships_activity', 'last_activity_at', 'status'),
        Index('idx_org_memberships_expiry', 'expires_at', 'status'),
        Index('idx_org_memberships_compliance', 'compliance_status', 'last_compliance_check_at'),
        Index('idx_org_memberships_api_access', 'api_key_hash', 'last_api_access_at'),
        Index('idx_org_memberships_quota_usage', 'monthly_api_usage', 'monthly_api_quota'),
    )
    
    # Rails business logic constants
    DEFAULT_EXPIRY_DAYS = 365
    API_KEY_LENGTH = 32
    MAX_LOGIN_ATTEMPTS = 5
    LOCKOUT_DURATION_MINUTES = 30
    ACTIVITY_THRESHOLD_DAYS = 30
    CACHE_TTL_SECONDS = 300
    BULK_OPERATION_BATCH_SIZE = 1000
    MFA_BACKUP_CODES_COUNT = 10
    PASSWORD_EXPIRY_DAYS = 90
    COMPLIANCE_CHECK_DAYS = 30
    REMINDER_DAYS_BEFORE_EXPIRY = [30, 14, 7, 1]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._permission_cache = {}
        self._access_cache = {}
        self._resource_counts = {}
        self._compliance_cache = {}
        self._quota_cache = {}
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if membership is active (Rails pattern)"""
        return (self.status == MembershipStatuses.ACTIVE and 
                not self.expired_() and 
                not self.blocked_() and
                not self.account_locked_() and
                not self.suspended_())
    
    def inactive_(self) -> bool:
        """Check if membership is inactive (Rails pattern)"""
        return self.status == MembershipStatuses.INACTIVE
    
    def suspended_(self) -> bool:
        """Check if membership is suspended (Rails pattern)"""
        return self.status == MembershipStatuses.SUSPENDED
    
    def pending_(self) -> bool:
        """Check if membership is pending (Rails pattern)"""
        return self.status in [MembershipStatuses.PENDING, MembershipStatuses.PENDING_APPROVAL]
    
    def expired_(self) -> bool:
        """Check if membership is expired (Rails pattern)"""
        if self.status == MembershipStatuses.EXPIRED:
            return True
        if self.expires_at and self.expires_at < datetime.now():
            return True
        return False
    
    def blocked_(self) -> bool:
        """Check if membership is blocked (Rails pattern)"""
        return self.status in [MembershipStatuses.BLOCKED, MembershipStatuses.TERMINATED]
    
    def account_locked_(self) -> bool:
        """Check if account is locked due to failed logins (Rails pattern)"""
        if not self.account_locked_until:
            return False
        return self.account_locked_until > datetime.now()
    
    def mfa_enabled_(self) -> bool:
        """Check if MFA is enabled (Rails pattern)"""
        return self.mfa_enabled and self.mfa_secret is not None
    
    def quota_exceeded_(self, quota_type: str = 'api') -> bool:
        """Check if quota is exceeded (Rails pattern)"""
        if quota_type == 'api':
            return self.monthly_api_usage >= self.monthly_api_quota
        elif quota_type == 'storage':
            return self.storage_usage_mb >= self.storage_quota_mb
        return False
    
    def privileged_(self) -> bool:
        """Check if member has privileged access (Rails pattern)"""
        return self.role in [MembershipRoles.OWNER, MembershipRoles.ADMIN]
    
    def can_login_(self) -> bool:
        """Check if member can log in (Rails pattern)"""
        return (self.active_() and 
                not self.api_only_() and 
                not self.account_locked_() and
                self.access_level != AccessLevels.NO_ACCESS)
    
    def api_only_(self) -> bool:
        """Check if membership is API-only (Rails pattern)"""
        return self.membership_type == MembershipTypes.API_ONLY
    
    # Rails bang methods
    def activate_(self) -> None:
        """Activate membership (Rails bang method pattern)"""
        if self.active_():
            return
        
        self.status = MembershipStatuses.ACTIVE
        self.activated_at = datetime.now()
        self.updated_at = datetime.now()
        self.failed_login_attempts = 0
        self.account_locked_until = None
        self._clear_cache()
    
    def suspend_(self, reason: str = None) -> None:
        """Suspend membership (Rails bang method pattern)"""
        if self.suspended_():
            return
        
        self.status = MembershipStatuses.SUSPENDED
        self.suspended_at = datetime.now()
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('suspension_reason', reason)
        
        self._clear_cache()
    
    def generate_api_key_(self) -> str:
        """Generate new API key (Rails bang method pattern)"""
        alphabet = string.ascii_letters + string.digits
        api_key = ''.join(secrets.choice(alphabet) for _ in range(self.API_KEY_LENGTH))
        
        self.api_key = api_key
        self.api_key_hash = self._hash_api_key(api_key)
        self.updated_at = datetime.now()
        
        return api_key
    
    def record_login_(self, ip_address: str = None) -> None:
        """Record user login (Rails bang method pattern)"""
        self.last_login_at = datetime.now()
        self.last_activity_at = datetime.now()
        self.login_count += 1
        self.failed_login_attempts = 0
        self.updated_at = datetime.now()
    
    # Helper methods
    def _hash_api_key(self, api_key: str) -> str:
        """Hash API key for storage"""
        return hashlib.sha256(api_key.encode()).hexdigest()
    
    def _update_metadata(self, key: str, value: Any) -> None:
        """Update metadata field"""
        if not self.extra_metadata:
            self.extra_metadata = {}
        self.extra_metadata[key] = value
    
    def _clear_cache(self) -> None:
        """Clear internal caches"""
        self._permission_cache.clear()
        self._access_cache.clear()
        self._resource_counts.clear()
        self._compliance_cache.clear()
        self._quota_cache.clear()
    
    # Display methods
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        user_name = self.user.name if self.user else f"User #{self.user_id}"
        org_name = self.org.name if self.org else f"Org #{self.org_id}"
        return f"{user_name} in {org_name}"
    
    def display_status(self) -> str:
        """Get formatted status for display (Rails pattern)"""
        return self.status.display_name if self.status else "Unknown Status"
    
    def display_role(self) -> str:
        """Get formatted role for display (Rails pattern)"""
        return self.role.display_name if self.role else "Unknown Role"
    
    # API methods
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'status': self.status.value,
            'display_status': self.display_status(),
            'role': self.role.value,
            'display_role': self.display_role(),
            'access_level': self.access_level.value,
            'membership_type': self.membership_type.value,
            'active': self.active_(),
            'privileged': self.privileged_(),
            'can_login': self.can_login_(),
            'mfa_enabled': self.mfa_enabled_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'user_id': self.user_id,
            'org_id': self.org_id
        }
    
    def __repr__(self) -> str:
        return f"<OrgMembershipEnhanced(id={self.id}, user_id={self.user_id}, org_id={self.org_id}, role='{self.role.value}', status='{self.status.value}')>"

# Backwards compatibility alias
OrgMembership = OrgMembershipEnhanced