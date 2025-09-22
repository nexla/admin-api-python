"""
User Model - Enhanced with comprehensive Rails business logic patterns.
Core user authentication, authorization, and lifecycle management with Rails-style patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum as PyEnum
import bcrypt
import hashlib
import re
import secrets
import string
import time
import json
import logging
from ..database import Base

logger = logging.getLogger(__name__)


class UserStatuses(PyEnum):
    """User status enumeration with Rails-style constants"""
    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    SUSPENDED = "SUSPENDED"
    LOCKED = "LOCKED"
    PENDING = "PENDING"
    TRIAL = "TRIAL"
    EXPIRED = "EXPIRED"
    SOURCE_COUNT_CAPPED = "SOURCE_COUNT_CAPPED"
    SOURCE_DATA_CAPPED = "SOURCE_DATA_CAPPED"
    TRIAL_EXPIRED = "TRIAL_EXPIRED"
    DISABLED = "DISABLED"

    @property
    def display_name(self) -> str:
        """Human readable status name"""
        return self.value.replace('_', ' ').title()


class UserRoles(PyEnum):
    """User role enumeration with Rails-style hierarchy"""
    SUPER_USER = "SUPER_USER"
    ORG_OWNER = "ORG_OWNER"
    ORG_ADMIN = "ORG_ADMIN"
    ORG_MEMBER = "ORG_MEMBER"
    PROJECT_ADMIN = "PROJECT_ADMIN"
    PROJECT_MEMBER = "PROJECT_MEMBER"
    USER = "USER"
    GUEST = "GUEST"
    
    @property
    def hierarchy_level(self) -> int:
        """Role hierarchy level (lower = higher privilege)"""
        hierarchy = {
            'SUPER_USER': 1,
            'ORG_OWNER': 2,
            'ORG_ADMIN': 3,
            'ORG_MEMBER': 4,
            'PROJECT_ADMIN': 5,
            'PROJECT_MEMBER': 6,
            'USER': 7,
            'GUEST': 8
        }
        return hierarchy.get(self.value, 9)


class AuthenticationTypes(PyEnum):
    """Authentication type enumeration"""
    PASSWORD = "password"
    SSO = "sso"
    API_KEY = "api_key"
    TOKEN = "token"
    OAUTH = "oauth"
    SAML = "saml"
    LDAP = "ldap"


class User(Base):
    __tablename__ = "users_v2"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(254), unique=True, index=True, nullable=False)
    full_name = Column(String(255))
    first_name = Column(String(100))
    last_name = Column(String(100))
    display_name = Column(String(200))
    
    # Authentication
    password_digest = Column(String(255), nullable=False)
    password_digest_1 = Column(String(255))
    password_digest_2 = Column(String(255))
    password_digest_3 = Column(String(255))
    password_digest_4 = Column(String(255))
    
    # Status and state
    status = Column(SQLEnum(UserStatuses), default=UserStatuses.ACTIVE, nullable=False, index=True)
    
    # Password management
    password_retry_count = Column(Integer, default=0)
    password_change_required_at = Column(DateTime)
    password_reset_token = Column(String(255))
    password_reset_token_at = Column(DateTime)
    password_reset_token_count = Column(Integer, default=0)
    account_locked_at = Column(DateTime)
    account_locked_reason = Column(Text)
    failed_login_count = Column(Integer, default=0)
    last_failed_login_at = Column(DateTime)
    last_login_at = Column(DateTime)
    login_count = Column(Integer, default=0)
    last_password_change_at = Column(DateTime)
    
    # User lifecycle timestamps
    activated_at = Column(DateTime)
    deactivated_at = Column(DateTime)
    suspended_at = Column(DateTime)
    suspension_reason = Column(Text)
    trial_expires_at = Column(DateTime)
    
    # Profile and preferences
    avatar_url = Column(String(500))
    timezone = Column(String(50), default='UTC')
    locale = Column(String(10), default='en')
    
    # Security settings
    two_factor_enabled = Column(Boolean, default=False)
    two_factor_secret = Column(String(255))
    backup_codes = Column(Text)  # JSON array of backup codes
    session_timeout_minutes = Column(Integer, default=480)  # 8 hours
    require_password_change = Column(Boolean, default=False)
    
    # Activity tracking
    last_activity_at = Column(DateTime)
    last_api_access_at = Column(DateTime)
    api_access_count = Column(Integer, default=0)
    
    # Feature flags and metadata
    is_beta_user = Column(Boolean, default=False)
    feature_flags = Column(Text)  # JSON object
    preferences = Column(Text)  # JSON object
    extra_metadata = Column(Text)  # JSON object for additional data
    
    # Verification timestamps
    email_verified_at = Column(DateTime)
    tos_signed_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # Foreign keys
    default_org_id = Column(Integer, ForeignKey("orgs.id", use_alter=True), nullable=True)
    user_tier_id = Column(Integer, ForeignKey("user_tiers.id"), nullable=True)
    
    # Relationships
    default_org = relationship("Org", foreign_keys=[default_org_id], back_populates="users")
    user_tier = relationship("UserTier")  
    org_memberships = relationship("OrgMembership", back_populates="user")
    flows = relationship("Flow", back_populates="owner")
    projects = relationship("Project", back_populates="owner")
    
    # Rails business logic constants
    BACKEND_ADMIN_EMAIL = "admin@nexla.com"
    MAX_PASSWORD_RETRY_COUNT = 5
    PASSWORD_CHANGE_REQUIRED_AFTER_DAYS = 90
    MAX_RESET_PASSWORD_TRIES = 5
    RESET_PASSWORD_INTERVAL_MINUTES = 1
    MIN_PASSWORD_ENTROPY = 16
    
    PASSWORD_CONSTRAINTS = {"minimum": 8, "maximum": 72}
    EMAIL_CONSTRAINTS = {"minimum": 3, "maximum": 254}
    EXTRA_WORDS = ["nexla", "Nexla", "NEXLA", "test", "Test", "TEST"]
    
    PASSWORD_REGEX = re.compile(
        r"^(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^\w\d]).*$"
    )
    
    STATUSES = {
        "active": UserStatuses.ACTIVE.value,
        "deactivated": UserStatuses.DEACTIVATED.value,
        "suspended": UserStatuses.SUSPENDED.value,
        "locked": UserStatuses.LOCKED.value,
        "pending": UserStatuses.PENDING.value,
        "trial": UserStatuses.TRIAL.value,
        "expired": UserStatuses.EXPIRED.value,
        "source_count_capped": UserStatuses.SOURCE_COUNT_CAPPED.value,
        "source_data_capped": UserStatuses.SOURCE_DATA_CAPPED.value,
        "trial_expired": UserStatuses.TRIAL_EXPIRED.value,
        "disabled": UserStatuses.DISABLED.value
    }
    
    AUTHENTICATION_TYPES = {
        "login": "login",
        "logout": "logout"
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self._cache = {}
        self._performance_metrics = {}

    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if user is active (Rails pattern)"""
        return (self.status == UserStatuses.ACTIVE and 
                not self.account_locked_() and 
                not self.suspended_() and
                not self.trial_expired_())
    
    def deactivated_(self) -> bool:
        """Check if user is deactivated (Rails pattern)"""
        return self.status == UserStatuses.DEACTIVATED
    
    def suspended_(self) -> bool:
        """Check if user is suspended (Rails pattern)"""
        return self.status == UserStatuses.SUSPENDED or self.suspended_at is not None
    
    def locked_(self) -> bool:
        """Check if user account is locked (Rails pattern)"""
        return self.status == UserStatuses.LOCKED or self.account_locked_at is not None
    
    def account_locked_(self) -> bool:
        """Check if user account is locked (Rails pattern)"""
        return self.locked_()
    
    def pending_(self) -> bool:
        """Check if user is pending activation (Rails pattern)"""
        return self.status == UserStatuses.PENDING
    
    def trial_(self) -> bool:
        """Check if user is on trial (Rails pattern)"""
        return self.status == UserStatuses.TRIAL
    
    def expired_(self) -> bool:
        """Check if user is expired (Rails pattern)"""
        return self.status == UserStatuses.EXPIRED
    
    def trial_expired_(self) -> bool:
        """Check if user trial has expired (Rails pattern)"""
        return (self.status == UserStatuses.TRIAL_EXPIRED or
                (self.trial_expires_at and self.trial_expires_at < datetime.now()))
    
    def source_count_capped_(self) -> bool:
        """Check if user is source count capped (Rails pattern)"""
        return self.status == UserStatuses.SOURCE_COUNT_CAPPED
    
    def source_data_capped_(self) -> bool:
        """Check if user is source data capped (Rails pattern)"""
        return self.status == UserStatuses.SOURCE_DATA_CAPPED
    
    def disabled_(self) -> bool:
        """Check if user is disabled (Rails pattern)"""
        return self.status == UserStatuses.DISABLED
    
    def email_verified_(self) -> bool:
        """Check if user email is verified (Rails pattern)"""
        return self.email_verified_at is not None
    
    def tos_accepted_(self) -> bool:
        """Check if user has accepted terms of service (Rails pattern)"""
        return self.tos_signed_at is not None
    
    def two_factor_enabled_(self) -> bool:
        """Check if two-factor authentication is enabled (Rails pattern)"""
        return self.two_factor_enabled and bool(self.two_factor_secret)
    
    def password_expired_(self) -> bool:
        """Check if user password has expired (Rails pattern)"""
        if not self.password_change_required_at:
            return False
        return self.password_change_required_at < datetime.now()
    
    def password_change_required_(self) -> bool:
        """Check if password change is required (Rails pattern)"""
        return self.require_password_change or self.password_expired_()
    
    def recently_active_(self, minutes: int = 30) -> bool:
        """Check if user was recently active (Rails pattern)"""
        if not self.last_activity_at:
            return False
        return self.last_activity_at >= datetime.now() - timedelta(minutes=minutes)
    
    def beta_user_(self) -> bool:
        """Check if user is beta user (Rails pattern)"""
        return self.is_beta_user is True
    
    def can_login_(self) -> bool:
        """Check if user can login (Rails pattern)"""
        return (self.active_() and 
                self.email_verified_() and 
                not self.password_change_required_())
    
    def login_attempts_exceeded_(self) -> bool:
        """Check if user has exceeded login attempts (Rails pattern)"""
        return self.password_retry_count >= self.MAX_PASSWORD_RETRY_COUNT

    def admin_(self) -> bool:
        """Check if user has admin privileges (Rails pattern)"""
        return self.super_user_() or self.org_owner_() or self.org_admin_()
    
    def super_user_(self) -> bool:
        """Rails super_user? business logic implementation"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            from .org_membership import OrgMembership
            
            # Get database session
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            # Get Nexla Admin org (nexla_admin_org = 1)
            nexla_admin_org = db.query(Org).filter(Org.nexla_admin_org == 1).first()
            if not nexla_admin_org:
                return False
            
            # Check if user is owner of Nexla Admin org
            if nexla_admin_org.owner_id == self.id:
                return True
            
            # Check if user is active member of Nexla Admin org
            membership = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.org_id == nexla_admin_org.id,
                OrgMembership.status == 'ACTIVE'
            ).first()
            
            # If active member of Nexla Admin org, they are super user
            return membership is not None
            
        except Exception:
            # Fallback: Check if email is Nexla admin email
            return self.email == self.BACKEND_ADMIN_EMAIL
        finally:
            if 'db' in locals():
                db.close()
    
    def org_owner_(self) -> bool:
        """Check if user owns any organization"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            # Check if user owns any org
            owned_org = db.query(Org).filter(Org.owner_id == self.id).first()
            return owned_org is not None
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def org_admin_(self, org_id: int = None) -> bool:
        """Check if user is organization admin (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            query = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.status == 'ACTIVE'
            )
            
            if org_id:
                query = query.filter(OrgMembership.org_id == org_id)
            
            # This would check for admin role when role system is implemented
            membership = query.first()
            return membership is not None and hasattr(membership, 'is_admin') and membership.is_admin
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def org_member_(self, org_id: int = None) -> bool:
        """Check if user is active member of specific org or any org (Rails active_org_member?)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            query = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.status == 'ACTIVE'
            )
            
            if org_id:
                query = query.filter(OrgMembership.org_id == org_id)
            
            membership = query.first()
            return membership is not None
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()

    def infrastructure_user_(self) -> bool:
        """Check if user is infrastructure user (Rails pattern)"""
        return self.nexla_backend_admin_()
    
    def infrastructure_or_super_user_(self) -> bool:
        """Check if user is infrastructure user or super user (Rails pattern)"""
        return self.super_user_() or self.infrastructure_user_()
    
    def nexla_backend_admin_(self) -> bool:
        """Check if user is Nexla backend admin (Rails pattern)"""
        return self.email == self.BACKEND_ADMIN_EMAIL

    @property
    def role(self) -> UserRoles:
        """Get user primary role (Rails pattern)"""
        if self.super_user_():
            return UserRoles.SUPER_USER
        elif self.org_owner_():
            return UserRoles.ORG_OWNER
        elif self.org_admin_():
            return UserRoles.ORG_ADMIN
        elif self.org_member_():
            return UserRoles.ORG_MEMBER
        else:
            return UserRoles.USER
    
    @property
    def role_display(self) -> str:
        """Get human-readable role display name (Rails pattern)"""
        role_map = {
            UserRoles.SUPER_USER: "Super User",
            UserRoles.ORG_OWNER: "Organization Owner", 
            UserRoles.ORG_ADMIN: "Organization Admin",
            UserRoles.ORG_MEMBER: "Organization Member",
            UserRoles.PROJECT_ADMIN: "Project Admin",
            UserRoles.PROJECT_MEMBER: "Project Member",
            UserRoles.USER: "User",
            UserRoles.GUEST: "Guest"
        }
        return role_map.get(self.role, "Unknown")

    def account_status(self, api_org=None) -> UserStatuses:
        """Get account status considering org context (Rails pattern)"""
        org = api_org if api_org else self.default_org
        if org and hasattr(org, 'org_tier') and org.org_tier and hasattr(org, 'status'):
            # Return org status if it's more restrictive
            if org.status in ['DEACTIVATED', 'SUSPENDED', 'TRIAL_EXPIRED']:
                return UserStatuses(org.status)
        return self.status

    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self, org=None) -> None:
        """Activate user account (Rails bang method pattern)"""
        if self.active_():
            return
        
        self.status = UserStatuses.ACTIVE
        self.activated_at = datetime.now()
        self.deactivated_at = None
        self.suspended_at = None
        self.suspension_reason = None
        self.account_locked_at = None
        self.account_locked_reason = None
        self.password_retry_count = 0
        self.failed_login_count = 0
        self.updated_at = datetime.now()
        
        if org:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            try:
                membership = db.query(OrgMembership).filter(
                    OrgMembership.user_id == self.id,
                    OrgMembership.org_id == org.id
                ).first()
                
                if membership and hasattr(membership, 'activate_'):
                    membership.activate_()
                    db.commit()
            finally:
                db.close()
        
        logger.info(f"User activated: {self.email}")
    
    def deactivate_(self, org=None, pause_data_flows: bool = False, reason: str = None) -> None:
        """Deactivate user account (Rails bang method pattern)"""
        if org is None:
            # Global deactivation - check for owned orgs
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            try:
                owned_orgs = db.query(Org).filter(
                    Org.owner_id == self.id,
                    Org.status != UserStatuses.DEACTIVATED.value
                ).all()
                
                # Check if any owned org has multiple members
                for owned_org in owned_orgs:
                    if len(getattr(owned_org, 'members', [])) > 1:
                        raise ValueError(f"User cannot be deactivated while owning active multi-user org: {owned_org.id}")
                
                # Deactivate owned orgs
                for owned_org in owned_orgs:
                    if hasattr(owned_org, 'deactivate_'):
                        owned_org.deactivate_()
                    else:
                        owned_org.deactivate()
                
                self.status = UserStatuses.DEACTIVATED
                self.deactivated_at = datetime.now()
                if reason:
                    self._update_metadata('deactivation_reason', reason)
                self.updated_at = datetime.now()
                db.commit()
            finally:
                db.close()
        else:
            # Org-specific deactivation
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            try:
                membership = db.query(OrgMembership).filter(
                    OrgMembership.user_id == self.id,
                    OrgMembership.org_id == org.id
                ).first()
                
                if not membership:
                    raise ValueError("Org membership not found")
                
                is_org_owner = org.owner_id == self.id if hasattr(org, 'owner_id') else False
                org_has_members = len(getattr(org, 'members', [])) > 1
                org_is_active = not getattr(org, 'deactivated_', lambda: False)()
                
                if is_org_owner and org_has_members and org_is_active:
                    raise ValueError(f"User cannot be deactivated while owning active multi-user org: {org.id}")
                
                if is_org_owner:
                    if hasattr(org, 'deactivate_'):
                        org.deactivate_()
                    else:
                        org.deactivate()
                
                if hasattr(membership, 'deactivate_'):
                    membership.deactivate_()
                else:
                    membership.deactivate()
                db.commit()
            finally:
                db.close()
        
        logger.info(f"User deactivated: {self.email}")
    
    def suspend_(self, reason: str = None, suspended_by=None) -> None:
        """Suspend user account (Rails bang method pattern)"""
        if self.suspended_():
            return
        
        self.status = UserStatuses.SUSPENDED
        self.suspended_at = datetime.now()
        self.suspension_reason = reason
        self.updated_at = datetime.now()
        
        if suspended_by:
            self._update_metadata('suspended_by', suspended_by.id)
        
        logger.info(f"User suspended: {self.email}, reason: {reason}")
    
    def unsuspend_(self) -> None:
        """Remove suspension from user account (Rails bang method pattern)"""
        if not self.suspended_():
            return
        
        self.status = UserStatuses.ACTIVE
        self.suspended_at = None
        self.suspension_reason = None
        self.updated_at = datetime.now()
        
        logger.info(f"User suspension removed: {self.email}")
    
    def lock_account_(self, reason: str = None) -> None:
        """Lock user account (Rails bang method pattern)"""
        if self.infrastructure_user_():
            raise ValueError("Cannot lock infrastructure user account")
        
        if self.locked_():
            return
        
        self.status = UserStatuses.LOCKED
        self.account_locked_at = datetime.now()
        self.account_locked_reason = reason
        self.updated_at = datetime.now()
        
        logger.warning(f"User account locked: {self.email}, reason: {reason}")
    
    def unlock_account_(self) -> None:
        """Unlock user account (Rails bang method pattern)"""
        if not self.locked_():
            return
        
        self.status = UserStatuses.ACTIVE
        self.account_locked_at = None
        self.account_locked_reason = None
        self.password_retry_count = 0
        self.failed_login_count = 0
        self.updated_at = datetime.now()
        
        logger.info(f"User account unlocked: {self.email}")
    
    def disable_(self, reason: str = None) -> None:
        """Disable user account (Rails bang method pattern)"""
        self.status = UserStatuses.DISABLED
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('disabled_reason', reason)
        
        logger.info(f"User disabled: {self.email}")
    
    def enable_(self) -> None:
        """Enable user account (Rails bang method pattern)"""
        self.status = UserStatuses.ACTIVE
        self.updated_at = datetime.now()
        
        logger.info(f"User enabled: {self.email}")
    
    def verify_email_(self) -> None:
        """Verify user email address (Rails bang method pattern)"""
        if self.email_verified_():
            return
        
        self.email_verified_at = datetime.now()
        self.updated_at = datetime.now()
        
        # Auto-activate if pending email verification
        if self.pending_():
            self.activate_()
        
        logger.info(f"Email verified: {self.email}")
    
    def accept_terms_(self) -> None:
        """Accept terms of service (Rails bang method pattern)"""
        if self.tos_accepted_():
            return
        
        self.tos_signed_at = datetime.now()
        self.updated_at = datetime.now()
        
        logger.info(f"Terms accepted: {self.email}")
    
    def enable_two_factor_(self, secret: str, backup_codes: List[str] = None) -> None:
        """Enable two-factor authentication (Rails bang method pattern)"""
        self.two_factor_enabled = True
        self.two_factor_secret = secret
        
        if backup_codes:
            self.backup_codes = json.dumps(backup_codes)
        
        self.updated_at = datetime.now()
        
        logger.info(f"Two-factor authentication enabled: {self.email}")
    
    def disable_two_factor_(self) -> None:
        """Disable two-factor authentication (Rails bang method pattern)"""
        self.two_factor_enabled = False
        self.two_factor_secret = None
        self.backup_codes = None
        self.updated_at = datetime.now()
        
        logger.info(f"Two-factor authentication disabled: {self.email}")

    def change_password_(self, new_password: str, new_password_confirm: str) -> None:
        """Change password with history tracking (Rails bang method pattern)"""
        if new_password != new_password_confirm:
            raise ValueError("Password confirmation does not match")
            
        if self.authenticate_with_previous(new_password):
            raise ValueError("Cannot reuse a recent password")
            
        # Validate password strength
        if not self.PASSWORD_REGEX.match(new_password):
            raise ValueError("Password does not meet security requirements")
            
        # Save current password to history and set new one
        self.password_digest_4 = self.password_digest_3
        self.password_digest_3 = self.password_digest_2  
        self.password_digest_2 = self.password_digest_1
        self.password_digest_1 = self.password_digest
        
        # Hash new password
        salt = bcrypt.gensalt()
        self.password_digest = bcrypt.hashpw(new_password.encode('utf-8'), salt).decode('utf-8')
        
        # Set password change required date
        self.password_change_required_at = datetime.now() + timedelta(days=self.PASSWORD_CHANGE_REQUIRED_AFTER_DAYS)
        self.last_password_change_at = datetime.now()
        self.require_password_change = False
        
        # Clear reset token
        self.password_reset_token = None
        self.password_reset_token_at = None
        self.password_reset_token_count = 0
        
        # Update timestamp
        self.updated_at = datetime.now()
        
        logger.info(f"Password changed for user: {self.email}")

    def increment_login_count_(self) -> None:
        """Increment successful login count (Rails bang method pattern)"""
        self.login_count = (self.login_count or 0) + 1
        self.last_login_at = datetime.now()
        self.last_activity_at = datetime.now()
        self.failed_login_count = 0  # Reset failed attempts on success
        self.password_retry_count = 0  # Reset password retry count
        self.updated_at = datetime.now()
    
    def increment_failed_login_(self) -> None:
        """Increment failed login count (Rails bang method pattern)"""
        self.failed_login_count = (self.failed_login_count or 0) + 1
        self.last_failed_login_at = datetime.now()
        self.updated_at = datetime.now()
        
        # Auto-lock after too many failures
        if self.failed_login_count >= self.MAX_PASSWORD_RETRY_COUNT:
            self.lock_account_("Too many failed login attempts")
    
    def reset_password_retry_count_(self) -> None:
        """Reset password retry count (Rails bang method pattern)"""
        if self.password_retry_count != 0:
            self.password_retry_count = 0
            self.failed_login_count = 0
            self.updated_at = datetime.now()
    
    def increment_password_retry_count_(self) -> None:
        """Increment password retry count and lock account if exceeded (Rails bang method pattern)"""
        self.password_retry_count += 1
        self.updated_at = datetime.now()
        
        if self.password_retry_count_exceeded():
            self.lock_account_("Too many password retry attempts")

    def update_activity_(self) -> None:
        """Update last activity timestamp (Rails bang method pattern)"""
        self.last_activity_at = datetime.now()
        # Don't update updated_at for activity updates to avoid noise
    
    def update_api_activity_(self) -> None:
        """Update API access activity (Rails bang method pattern)"""
        self.last_api_access_at = datetime.now()
        self.api_access_count = (self.api_access_count or 0) + 1
        self.last_activity_at = datetime.now()
    
    def extend_trial_(self, days: int = 30) -> None:
        """Extend user trial period (Rails bang method pattern)"""
        if not self.trial_():
            raise ValueError("User is not on trial")
        
        current_expiry = self.trial_expires_at or datetime.now()
        self.trial_expires_at = current_expiry + timedelta(days=days)
        self.updated_at = datetime.now()
        
        logger.info(f"Trial extended for {self.email} by {days} days")
    
    def convert_from_trial_(self) -> None:
        """Convert user from trial to active (Rails bang method pattern)"""
        if not self.trial_():
            raise ValueError("User is not on trial")
        
        self.status = UserStatuses.ACTIVE
        self.trial_expires_at = None
        self.updated_at = datetime.now()
        
        logger.info(f"User converted from trial: {self.email}")

    # ========================================
    # Rails Helper and Utility Methods
    # ========================================
    
    def _update_metadata(self, key: str, value: Any) -> None:
        """Update metadata field (Rails helper pattern)"""
        try:
            current_meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
        except (json.JSONDecodeError, TypeError):
            current_meta = {}
            
        current_meta[key] = value
        self.extra_metadata = json.dumps(current_meta)
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Get metadata value (Rails helper pattern)"""
        try:
            meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
            return meta.get(key, default)
        except (json.JSONDecodeError, TypeError):
            return default
    
    def get_feature_flags(self) -> Dict[str, Any]:
        """Get user feature flags (Rails pattern)"""
        try:
            return json.loads(self.feature_flags) if self.feature_flags else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_feature_flag_(self, flag_name: str, enabled: bool) -> None:
        """Set feature flag for user (Rails bang method pattern)"""
        flags = self.get_feature_flags()
        flags[flag_name] = enabled
        self.feature_flags = json.dumps(flags)
        self.updated_at = datetime.now()
    
    def has_feature_flag_(self, flag_name: str) -> bool:
        """Check if user has feature flag enabled (Rails pattern)"""
        flags = self.get_feature_flags()
        return flags.get(flag_name, False) is True
    
    def get_preferences(self) -> Dict[str, Any]:
        """Get user preferences (Rails pattern)"""
        try:
            return json.loads(self.preferences) if self.preferences else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_preference_(self, key: str, value: Any) -> None:
        """Set user preference (Rails bang method pattern)"""
        prefs = self.get_preferences()
        prefs[key] = value
        self.preferences = json.dumps(prefs)
        self.updated_at = datetime.now()
    
    def get_preference(self, key: str, default=None) -> Any:
        """Get user preference value (Rails pattern)"""
        prefs = self.get_preferences()
        return prefs.get(key, default)
    
    def display_name_or_email(self) -> str:
        """Get display name or fallback to email (Rails pattern)"""
        if self.display_name:
            return self.display_name
        elif self.full_name:
            return self.full_name
        elif self.first_name or self.last_name:
            return f"{self.first_name or ''} {self.last_name or ''}".strip()
        else:
            return self.email
    
    def initials(self) -> str:
        """Get user initials (Rails pattern)"""
        if self.first_name and self.last_name:
            return f"{self.first_name[0]}{self.last_name[0]}".upper()
        elif self.full_name:
            parts = self.full_name.split()
            if len(parts) >= 2:
                return f"{parts[0][0]}{parts[-1][0]}".upper()
            else:
                return parts[0][0].upper() if parts[0] else ''
        elif self.email:
            return self.email[0].upper()
        else:
            return '??'
    
    def session_expired_(self) -> bool:
        """Check if user session should be expired (Rails pattern)"""
        if not self.last_activity_at:
            return True
        
        timeout_minutes = self.session_timeout_minutes or 480  # 8 hours default
        expiry_time = self.last_activity_at + timedelta(minutes=timeout_minutes)
        return datetime.now() > expiry_time
    
    def time_until_trial_expiry(self) -> Optional[timedelta]:
        """Get time until trial expires (Rails pattern)"""
        if not self.trial_() or not self.trial_expires_at:
            return None
        
        now = datetime.now()
        if self.trial_expires_at > now:
            return self.trial_expires_at - now
        else:
            return timedelta(0)  # Already expired

    # ========================================
    # Rails Validation Methods
    # ========================================
    
    def validate_email_format(self) -> bool:
        """Validate email format (Rails pattern)"""
        if not self.email:
            return False
        
        email_regex = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        return email_regex.match(self.email) is not None
    
    def validate_for_activation(self) -> List[str]:
        """Validate user can be activated (Rails pattern)"""
        errors = []
        
        if not self.validate_email_format():
            errors.append("Invalid email format")
        
        if not self.email_verified_():
            errors.append("Email must be verified")
        
        if not self.tos_accepted_():
            errors.append("Terms of service must be accepted")
        
        return errors
    
    def can_be_activated_(self) -> bool:
        """Check if user can be activated (Rails pattern)"""
        return len(self.validate_for_activation()) == 0

    # ========================================
    # Rails Display and Formatting Methods
    # ========================================
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        return self.status.display_name if hasattr(self.status, 'display_name') else str(self.status)
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            UserStatuses.ACTIVE: 'green',
            UserStatuses.PENDING: 'yellow',
            UserStatuses.TRIAL: 'blue',
            UserStatuses.SUSPENDED: 'orange',
            UserStatuses.LOCKED: 'red',
            UserStatuses.DEACTIVATED: 'gray',
            UserStatuses.DISABLED: 'gray',
            UserStatuses.EXPIRED: 'red',
            UserStatuses.TRIAL_EXPIRED: 'red',
            UserStatuses.SOURCE_COUNT_CAPPED: 'orange',
            UserStatuses.SOURCE_DATA_CAPPED: 'orange'
        }
        return status_colors.get(self.status, 'gray')
    
    def activity_summary(self) -> Dict[str, Any]:
        """Get user activity summary (Rails pattern)"""
        return {
            'login_count': self.login_count or 0,
            'last_login_at': self.last_login_at.isoformat() if self.last_login_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'api_access_count': self.api_access_count or 0,
            'last_api_access_at': self.last_api_access_at.isoformat() if self.last_api_access_at else None,
            'recently_active': self.recently_active_(),
            'session_expired': self.session_expired_()
        }
    
    def security_summary(self) -> Dict[str, Any]:
        """Get user security summary (Rails pattern)"""
        return {
            'two_factor_enabled': self.two_factor_enabled_(),
            'email_verified': self.email_verified_(),
            'password_change_required': self.password_change_required_(),
            'account_locked': self.locked_(),
            'failed_login_count': self.failed_login_count or 0,
            'last_password_change_at': self.last_password_change_at.isoformat() if self.last_password_change_at else None
        }
    
    def trial_summary(self) -> Dict[str, Any]:
        """Get trial information summary (Rails pattern)"""
        return {
            'is_trial': self.trial_(),
            'trial_expires_at': self.trial_expires_at.isoformat() if self.trial_expires_at else None,
            'trial_expired': self.trial_expired_(),
            'time_until_expiry': str(self.time_until_trial_expiry()) if self.time_until_trial_expiry() else None
        }

    # ========================================
    # Legacy Rails Methods (all existing methods preserved)
    # ========================================
    
    def sso_options(self) -> List[Dict]:
        """Get SSO options for user (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            memberships = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id
            ).all()
            
            if not memberships:
                return []
                
            # Use default org or first org  
            org = self.default_org or memberships[0].org
            
            # This would return org.api_auth_configs.map(&:public_attributes) in Rails
            return []
            
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    @classmethod
    def create_temporary_password(cls) -> str:
        """Generate a secure temporary password following Rails pattern"""
        import random
        
        # Generate base password
        chars = string.ascii_lowercase + string.digits
        tmp = ''.join(random.choices(chars, k=24))
        
        # Insert required character types at random positions
        uppers = string.ascii_uppercase
        specials = ['!', '&', '-', '#', '$', '@', '+', '*']
        nums = string.digits
        
        # Insert uppercase letters
        for _ in range(2):
            pos = random.randint(0, len(tmp))
            tmp = tmp[:pos] + random.choice(uppers) + tmp[pos:]
            
        # Insert special characters 
        for _ in range(2):
            pos = random.randint(0, len(tmp))
            tmp = tmp[:pos] + random.choice(specials) + tmp[pos:]
            
        # Insert a number
        pos = random.randint(0, len(tmp))
        tmp = tmp[:pos] + random.choice(nums) + tmp[pos:]
        
        return tmp
    
    @classmethod
    def email_verified(cls, email: str) -> bool:
        """Check if email should be auto-verified (Rails pattern)"""
        if not email:
            return False
        if "nexla" in email and "test" in email:
            return False
        return True
    
    def authenticate_with_previous(self, password: str) -> bool:
        """Check password against current and previous 4 passwords (Rails pattern)"""
        # Check current password
        if self.password_digest and bcrypt.checkpw(password.encode('utf-8'), self.password_digest.encode('utf-8')):
            return True
            
        # Check previous passwords
        for digest_field in [self.password_digest_1, self.password_digest_2, 
                           self.password_digest_3, self.password_digest_4]:
            if digest_field and bcrypt.checkpw(password.encode('utf-8'), digest_field.encode('utf-8')):
                return True
                
        return False
    
    def password_retry_count_exceeded(self) -> bool:
        """Check if password retry count exceeded (Rails pattern)"""
        return self.password_retry_count >= self.MAX_PASSWORD_RETRY_COUNT
    
    def password_signature(self) -> str:
        """Generate password signature for JWT invalidation (Rails pattern)"""
        digest_str = self.password_digest or ""
        return hashlib.md5(digest_str.encode('utf-8')).hexdigest()
    
    def password_changed(self) -> bool:
        """Check if password field has been changed (Rails pattern)"""
        # This would be tracked by SQLAlchemy session in real implementation
        return hasattr(self, '_password_changed') and self._password_changed

    def has_admin_access(self, user) -> bool:
        """Check if given user has admin access to this user (Rails pattern)"""
        if not user:
            return False
        if user.id == self.id or user.super_user_():
            return True
        
        # Check if user is admin of any org this user is member of
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            # Get orgs where given user has admin access
            admin_orgs = db.query(Org).join(OrgMembership).filter(
                OrgMembership.user_id == user.id,
                OrgMembership.status == "ACTIVE"
                # TODO: Add admin role checking when role system is implemented
            ).all()
            
            # Check if this user is member of any of those orgs
            for org in admin_orgs:
                if self.org_member(org):
                    return True
                    
            return False
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def org_member(self, org) -> bool:
        """Check if user is member of specific org (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            membership = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.org_id == org.id,
                OrgMembership.status == "ACTIVE"
            ).first()
            
            return membership is not None
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def impersonated(self) -> bool:
        """Check if user is being impersonated (Rails pattern)"""
        # This would be set by authentication middleware
        return getattr(self, '_impersonator', None) is not None
    
    def super_user_read_only(self) -> bool:
        """Check if user has read-only super user access (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            nexla_admin_org = db.query(Org).filter(Org.nexla_admin_org == 1).first()
            if not nexla_admin_org:
                return False
                
            # Would need role system implementation for full check
            return False
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def orgs(self, access_role: str = "member") -> List:
        """Get user's accessible orgs by role (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            if access_role == "all":
                if self.super_user_():
                    return db.query(Org).all()
                else:
                    # Return member orgs + accessible orgs
                    member_orgs = db.query(Org).join(OrgMembership).filter(
                        OrgMembership.user_id == self.id,
                        OrgMembership.status == "ACTIVE"
                    ).all()
                    return member_orgs
            elif access_role == "member":
                return db.query(Org).join(OrgMembership).filter(
                    OrgMembership.user_id == self.id,
                    OrgMembership.status == "ACTIVE"
                ).all()
            else:
                # Other access roles would be implemented with proper role system
                return []
                
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    def active_member_orgs(self) -> List:
        """Get user's active member orgs (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            return db.query(Org).join(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.status == "ACTIVE",
                Org.status == "ACTIVE"
            ).all()
            
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    def team_member(self, team) -> bool:
        """Check if user is member of specific team (Rails pattern)"""
        # This would be implemented when team system is added
        return False
    
    def users(self, access_role: str = "all", org=None) -> List:
        """Get accessible users by role (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            if self.super_user_() and access_role == "all":
                return db.query(User).all()
            elif org and org.has_admin_access(self) and access_role == "all":
                # Return org members
                from .org_membership import OrgMembership
                return db.query(User).join(OrgMembership).filter(
                    OrgMembership.org_id == org.id,
                    OrgMembership.status == "ACTIVE"
                ).all()
            else:
                # Return just self
                return db.query(User).filter(User.id == self.id).all()
                
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    def domain_custodian(self, domain_id=None) -> bool:
        """Check if user is domain custodian (Rails pattern)"""
        # This would be implemented when domain custodian system is added
        return False
    
    def org_custodian(self, org_id: int) -> bool:
        """Check if user is org custodian (Rails pattern)"""
        # This would be implemented when org custodian system is added  
        return False
    
    def get_api_key(self, org=None):
        """Get user's API key for org (Rails pattern)"""
        # This would be implemented when API key system is added
        return None
    
    def login_audits(self) -> List:
        """Get user login audit records (Rails pattern)"""
        # This would be implemented when audit system is added
        return []
    
    def logout_audits(self) -> List:
        """Get user logout audit records (Rails pattern)"""
        # This would be implemented when audit system is added
        return []
    
    def pause_flows(self, org=None) -> None:
        """Pause user's data flows (Rails bang method pattern)"""
        # This would pause all origin nodes owned by user
        # Implementation would depend on flow system
        pass
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any], user, org):
        """Build user from input data (Rails pattern)"""
        # This would be a complex factory method for creating users
        # Implementation would depend on complete org/tier system
        return None
    
    def update_mutable(self, request, user, org, input_data: Dict[str, Any]) -> None:
        """Update mutable user fields (Rails bang method pattern)"""
        # This would update allowed fields based on permissions
        # Implementation would depend on complete authorization system
        pass
    
    def update_admin_status(self, user, org, input_data: Dict[str, Any]) -> None:
        """Update user admin status (Rails pattern)"""
        # This would manage admin role assignments
        # Implementation would depend on complete role system
        pass
    
    @classmethod
    def find_external_idp_user(cls, api_auth_config, email: str, full_name: str):
        """Find or create external IDP user (Rails pattern)"""
        # This would handle external identity provider user creation
        return [None, None]
    
    def account_summary(self, access_role: str = "all", org=None) -> Dict[str, Any]:
        """Get user account summary (Rails pattern)"""
        # This would return summary of user's resources
        return {}
    
    def transferable(self, org=None) -> List:
        """Get transferable resources (Rails pattern)"""
        # This would return resources that can be transferred to another user
        return []
    
    def transfer(self, org, delegate_owner, delegate_org=None) -> None:
        """Transfer user resources (Rails bang method pattern)"""
        # This would transfer ownership of user's resources
        pass
    
    def authenticate(self, password: str) -> bool:
        """Authenticate user with password (Rails has_secure_password pattern)"""
        if not self.password_digest or not password:
            return False
        try:
            return bcrypt.checkpw(password.encode('utf-8'), self.password_digest.encode('utf-8'))
        except Exception:
            return False
    
    def create_password_reset_token(self, org=None, origin: str = None, force: bool = False, send_email: bool = True) -> Optional[str]:
        """Create password reset token (Rails pattern)"""
        now = datetime.now()
        
        if force or (not self.password_reset_token_at or 
                    self.password_reset_token_at < (now - timedelta(minutes=self.RESET_PASSWORD_INTERVAL_MINUTES))):
            self.password_reset_token_at = now
            self.password_reset_token_count = 0
        
        if force:
            self.password_reset_token_count = 0
        
        self.password_reset_token_count += 1
        
        if self.password_reset_token_count > self.MAX_RESET_PASSWORD_TRIES:
            self.account_locked_at = now
            return None
        
        # Generate secure token
        token = secrets.token_urlsafe(32)
        self.password_reset_token = token
        self.password_reset_token_at = now
        
        # In a real implementation, you'd send email here
        if send_email and origin:
            # NotificationService.publish_reset_password(self, org, origin)
            pass
        
        return token
    
    @classmethod
    def verify_password_reset_token(cls, token: str):
        """Verify and return user for password reset token (Rails pattern)"""
        if not token:
            return None
        
        try:
            from sqlalchemy.orm import sessionmaker
            
            # This would need to be implemented with proper session handling
            # For now, return None to indicate not implemented
            return None
        except Exception:
            return None
    
    @classmethod
    def find_user_and_org_by_api_key(cls, api_key: str, scopes: Optional[List[str]] = None):
        """Find user and org by API key (Rails pattern)"""
        # This would integrate with API key system when implemented
        return [None, None, None]
    
    @classmethod
    def find_user_and_org_by_service_key(cls, api_key: str):
        """Find user and org by service key (Rails pattern)"""
        # This would integrate with service key system when implemented
        return [None, None, None]
    
    def generate_password_reset_token(self, new_user: bool = False) -> Optional[str]:
        """Generate password reset token (Rails pattern)"""
        token = secrets.token_urlsafe(32)
        self.password_reset_token = token
        self.password_reset_token_at = datetime.now()
        return token
    
    @classmethod
    def validate_password(cls, email: str, full_name: str, password: str) -> Dict[str, Any]:
        """Validate password strength (Rails pattern)"""
        errors = []
        
        # Check format requirements
        if not cls.PASSWORD_REGEX.match(password):
            errors.append("must contain at least 8 characters, one digit, one lower case character, one upper case character, and one symbol")
        
        # Check length
        if len(password) < cls.PASSWORD_CONSTRAINTS["minimum"]:
            errors.append(f"is too short (minimum is {cls.PASSWORD_CONSTRAINTS['minimum']} characters)")
        
        if len(password) > cls.PASSWORD_CONSTRAINTS["maximum"]:
            errors.append(f"is too long (maximum is {cls.PASSWORD_CONSTRAINTS['maximum']} characters)")
        
        # Simple entropy calculation (would use proper library in production)
        entropy = len(set(password)) * 2  # Simplified calculation
        
        return {
            'entropy': entropy,
            'min_entropy': cls.MIN_PASSWORD_ENTROPY,
            'errors': errors
        }

    # ========================================
    # Rails API Serialization Methods
    # ========================================
    
    def to_dict(self, include_sensitive: bool = False, include_activity: bool = False) -> Dict[str, Any]:
        """Convert user to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'email': self.email,
            'full_name': self.full_name,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'display_name': self.display_name,
            'display_name_or_email': self.display_name_or_email(),
            'initials': self.initials(),
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'status_display': self.status_display(),
            'status_color': self.status_color(),
            'role': self.role.value if hasattr(self.role, 'value') else str(self.role),
            'role_display': self.role_display,
            'active': self.active_(),
            'deactivated': self.deactivated_(),
            'suspended': self.suspended_(),
            'locked': self.locked_(),
            'email_verified': self.email_verified_(),
            'tos_accepted': self.tos_accepted_(),
            'two_factor_enabled': self.two_factor_enabled_(),
            'beta_user': self.beta_user_(),
            'super_user': self.super_user_(),
            'org_owner': self.org_owner_(),
            'infrastructure_user': self.infrastructure_user_(),
            'timezone': self.timezone,
            'locale': self.locale,
            'avatar_url': self.avatar_url,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'default_org_id': self.default_org_id,
            'user_tier_id': self.user_tier_id
        }
        
        if self.trial_():
            result.update(self.trial_summary())
        
        if include_activity:
            result.update({
                'activity_summary': self.activity_summary(),
                'security_summary': self.security_summary()
            })
        
        if include_sensitive and self.can_be_accessed_by_(self):
            result.update({
                'password_change_required': self.password_change_required_(),
                'session_timeout_minutes': self.session_timeout_minutes,
                'feature_flags': self.get_feature_flags(),
                'preferences': self.get_preferences()
            })
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert user to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'email': self.email,
            'display_name_or_email': self.display_name_or_email(),
            'initials': self.initials(),
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'role': self.role.value if hasattr(self.role, 'value') else str(self.role),
            'active': self.active_(),
            'avatar_url': self.avatar_url
        }
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Convert user to dictionary for audit logging (Rails pattern)"""
        return {
            'id': self.id,
            'email': self.email,
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'role': self.role.value if hasattr(self.role, 'value') else str(self.role),
            'last_login_at': self.last_login_at.isoformat() if self.last_login_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'org_id': self.default_org_id
        }
    
    def can_be_accessed_by_(self, accessing_user) -> bool:
        """Check if user data can be accessed by another user (Rails pattern)"""
        if not accessing_user:
            return False
        
        # Users can always access their own data
        if self.id == accessing_user.id:
            return True
        
        # Super users can access any user
        if accessing_user.super_user_():
            return True
        
        # Org owners/admins can access members of their orgs
        if accessing_user.org_owner_() or accessing_user.org_admin_():
            # Check if both users are in same org
            return self.default_org_id == accessing_user.default_org_id
        
        return False

    # ========================================
    # Legacy Methods for Backwards Compatibility
    # ========================================
    
    # Keep all existing methods for backwards compatibility
    def is_active(self) -> bool:
        return self.active_()
    
    def is_deactivated(self) -> bool:
        return self.deactivated_()
    
    def account_locked(self) -> bool:
        return self.account_locked_()
    
    def is_admin(self) -> bool:
        return self.admin_()
    
    def is_super_user(self) -> bool:
        return self.super_user_()
    
    def is_org_owner(self) -> bool:
        return self.org_owner_()
    
    def active_org_member(self, org_id: int = None) -> bool:
        return self.org_member_(org_id)
    
    def infrastructure_user(self) -> bool:
        return self.infrastructure_user_()
    
    def infrastructure_or_super_user(self) -> bool:
        return self.infrastructure_or_super_user_()
    
    def nexla_backend_admin(self) -> bool:
        return self.nexla_backend_admin_()
    
    def activate(self, org=None) -> None:
        self.activate_(org)
    
    def deactivate(self, org=None, pause_data_flows: bool = False) -> None:
        self.deactivate_(org, pause_data_flows)
    
    def deactivated(self) -> bool:
        return self.deactivated_()
    
    def lock_account(self) -> None:
        self.lock_account_()
    
    def unlock_account(self) -> None:
        self.unlock_account_()
    
    def change_password(self, new_password: str, new_password_confirm: str) -> None:
        self.change_password_(new_password, new_password_confirm)
    
    def reset_password_retry_count(self) -> None:
        self.reset_password_retry_count_()
    
    def increment_password_retry_count(self) -> None:
        self.increment_password_retry_count_()
    
    def password_change_required(self) -> bool:
        return self.password_change_required_()
    
    def __repr__(self) -> str:
        return f"<User(id={self.id}, email='{self.email}', status='{self.status}', role='{self.role}')>"
    
    def __str__(self) -> str:
        return self.display_name_or_email()