"""
Enhanced User model with complete Rails authentication logic ported to Python.
Includes password management, validation, security features, and comprehensive business logic.
"""

from enum import Enum as PyEnum
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, event, Index
from sqlalchemy.orm import relationship, validates, Session
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func
from passlib.context import CryptContext
from email_validator import validate_email, EmailNotValidError
import bcrypt
import re
import secrets
import string
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
from dataclasses import dataclass
import json

from ..database import Base
from ..config import settings

logger = logging.getLogger(__name__)

# Password configuration (from Rails User model)
MAX_PASSWORD_RETRY_COUNT = 5
PASSWORD_CHANGE_REQUIRED_AFTER_DAYS = 90
MAX_RESET_PASSWORD_TRIES = 5
RESET_PASSWORD_INTERVAL_MINS = 1

PASSWORD_CONSTRAINTS = {"minimum": 8, "maximum": 72}
EMAIL_CONSTRAINTS = {"minimum": 3, "maximum": 254}
EXTRA_WORDS = ["nexla", "Nexla", "NEXLA", "test", "Test", "TEST"]

# Password regex patterns from Rails
STRONG_REGEX = re.compile(r"^(?=.*[a-zA-Z])(?=.*[0-9])(?=.*[\W]).{8,}$")
MEDIUM_REGEX = re.compile(r"^(?=.*[a-zA-Z])(?=.*[0-9]).{8,}$")
MIN_PASSWORD_ENTROPY = 16

# Password format: 8+ chars, digit, lower, upper, symbol
PASSWORD_FORMAT = re.compile(r"^(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^\w\s]).*$")

# User status enums
class UserStatuses(PyEnum):
    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED" 
    SOURCE_COUNT_CAPPED = "SOURCE_COUNT_CAPPED"
    PENDING_ACTIVATION = "PENDING_ACTIVATION"
    SUSPENDED = "SUSPENDED"
    ARCHIVED = "ARCHIVED"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class UserRoles(PyEnum):
    USER = "USER"
    ADMIN = "ADMIN"
    SUPER_ADMIN = "SUPER_ADMIN"
    INFRASTRUCTURE = "INFRASTRUCTURE"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class LoginProviders(PyEnum):
    LOCAL = "LOCAL"
    GOOGLE = "GOOGLE"
    MICROSOFT = "MICROSOFT"
    OKTA = "OKTA"
    SAML = "SAML"


class SecurityLevels(PyEnum):
    STANDARD = "STANDARD"
    ENHANCED = "ENHANCED"
    HIGH = "HIGH"
    MAXIMUM = "MAXIMUM"


@dataclass
class UserSecurityInfo:
    """User security information"""
    has_mfa_enabled: bool = False
    last_login_at: Optional[datetime] = None
    last_login_ip: Optional[str] = None
    failed_login_attempts: int = 0
    security_level: SecurityLevels = SecurityLevels.STANDARD
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'has_mfa_enabled': self.has_mfa_enabled,
            'last_login_at': self.last_login_at.isoformat() if self.last_login_at else None,
            'last_login_ip': self.last_login_ip,
            'failed_login_attempts': self.failed_login_attempts,
            'security_level': self.security_level.value
        }


@dataclass
class UserPreferences:
    """User preferences and settings"""
    timezone: str = "UTC"
    language: str = "en"
    theme: str = "light"
    email_notifications: bool = True
    desktop_notifications: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timezone': self.timezone,
            'language': self.language,
            'theme': self.theme,
            'email_notifications': self.email_notifications,
            'desktop_notifications': self.desktop_notifications
        }


# Backwards compatibility
class UserStatus:
    ACTIVE = UserStatuses.ACTIVE.value
    DEACTIVATED = UserStatuses.DEACTIVATED.value
    SOURCE_COUNT_CAPPED = UserStatuses.SOURCE_COUNT_CAPPED.value
    PENDING_ACTIVATION = UserStatuses.PENDING_ACTIVATION.value

# Password context for hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class User(Base):
    __tablename__ = "users"
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_user_email', 'email'),
        Index('idx_user_status', 'status'),
        Index('idx_user_created_at', 'created_at'),
        Index('idx_user_org', 'default_org_id', 'status'),
        Index('idx_user_tier', 'user_tier_id', 'status'),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(254), unique=True, index=True, nullable=False)
    full_name = Column(String(255))
    first_name = Column(String(100))
    last_name = Column(String(100))
    display_name = Column(String(255))  # Computed or custom display name
    
    # Password storage - Rails stores 5 previous passwords for reuse prevention
    password_digest = Column(String(255), nullable=False)
    password_digest_1 = Column(String(255))
    password_digest_2 = Column(String(255))
    password_digest_3 = Column(String(255))
    password_digest_4 = Column(String(255))
    
    # Enhanced status and role management
    status = Column(String(50), default=UserStatuses.ACTIVE.value)
    role = Column(String(50), default=UserRoles.USER.value)
    
    # Authentication and security
    login_provider = Column(String(50), default=LoginProviders.LOCAL.value)
    provider_user_id = Column(String(255))  # External provider user ID
    security_level = Column(String(50), default=SecurityLevels.STANDARD.value)
    
    # Password management fields
    password_retry_count = Column(Integer, default=0)
    password_change_required_at = Column(DateTime)
    password_reset_token = Column(String(255))
    password_reset_token_at = Column(DateTime)
    password_reset_token_count = Column(Integer, default=0)
    account_locked_at = Column(DateTime)
    last_password_change_at = Column(DateTime)
    
    # Multi-factor authentication
    mfa_enabled = Column(Boolean, default=False)
    mfa_secret = Column(String(255))  # TOTP secret
    mfa_backup_codes = Column(Text)  # JSON array of backup codes
    mfa_setup_at = Column(DateTime)
    
    # Login tracking
    last_login_at = Column(DateTime)
    last_login_ip = Column(String(45))  # IPv6 support
    login_count = Column(Integer, default=0)
    failed_login_count = Column(Integer, default=0)
    last_failed_login_at = Column(DateTime)
    
    # Verification and compliance
    email_verified_at = Column(DateTime)
    email_verification_token = Column(String(255))
    email_verification_token_at = Column(DateTime)
    tos_signed_at = Column(DateTime)
    privacy_policy_signed_at = Column(DateTime)
    
    # User preferences (JSON storage)
    preferences = Column(Text)  # JSON object
    extra_metadata = Column(Text)  # Additional user metadata as JSON
    
    # Geographic and localization
    timezone = Column(String(100), default="UTC")
    language = Column(String(10), default="en")
    country_code = Column(String(3))
    
    # Account lifecycle
    activated_at = Column(DateTime)
    deactivated_at = Column(DateTime)
    archived_at = Column(DateTime)
    suspended_at = Column(DateTime)
    suspension_reason = Column(String(500))
    
    # API and integration
    api_key_enabled = Column(Boolean, default=True)
    webhook_notifications_enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Foreign keys
    default_org_id = Column(Integer, ForeignKey("orgs.id"))
    user_tier_id = Column(Integer, ForeignKey("user_tiers.id"))
    
    # Relationships temporarily commented out for authentication testing
    # default_org = relationship("Org", back_populates="users")
    # user_tier = relationship("UserTier", back_populates="users")
    # org_memberships = relationship("OrgMembership", back_populates="user")
    # api_keys = relationship("UsersApiKey", back_populates="user")
    # user_login_audits = relationship("UserLoginAudit", back_populates="user")
    
    # Authentication state (not persisted)
    _current_password = None
    _is_super_user = None
    _infrastructure_user = False
    _cached_preferences = None
    _cached_security_info = None
    
    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}', status='{self.status}', role='{self.role}')>"
    
    def __str__(self) -> str:
        display_name = self.display_name or self.full_name or self.email.split('@')[0]
        return f"{display_name} ({self.email})"
    
    # === Rails-style Predicate Methods ===
    
    def active_(self) -> bool:
        """Check if user is active and not locked"""
        return self.status == UserStatuses.ACTIVE.value and not self.is_account_locked()
    
    def inactive_(self) -> bool:
        """Check if user is inactive"""
        return not self.active_()
    
    def deactivated_(self) -> bool:
        """Check if user is deactivated"""
        return self.status == UserStatuses.DEACTIVATED.value
    
    def suspended_(self) -> bool:
        """Check if user is suspended"""
        return self.status == UserStatuses.SUSPENDED.value or self.suspended_at is not None
    
    def archived_(self) -> bool:
        """Check if user is archived"""
        return self.status == UserStatuses.ARCHIVED.value or self.archived_at is not None
    
    def pending_activation_(self) -> bool:
        """Check if user is pending activation"""
        return self.status == UserStatuses.PENDING_ACTIVATION.value
    
    def source_count_capped_(self) -> bool:
        """Check if user is source count capped"""
        return self.status == UserStatuses.SOURCE_COUNT_CAPPED.value
    
    def admin_(self) -> bool:
        """Check if user is an admin"""
        return self.role in [UserRoles.ADMIN.value, UserRoles.SUPER_ADMIN.value]
    
    def super_admin_(self) -> bool:
        """Check if user is a super admin"""
        return self.role == UserRoles.SUPER_ADMIN.value
    
    def infrastructure_user_(self) -> bool:
        """Check if user is an infrastructure user"""
        return self.role == UserRoles.INFRASTRUCTURE.value or self._infrastructure_user
    
    def mfa_enabled_(self) -> bool:
        """Check if MFA is enabled"""
        return self.mfa_enabled and self.mfa_secret is not None
    
    def mfa_setup_complete_(self) -> bool:
        """Check if MFA setup is complete"""
        return self.mfa_enabled_() and self.mfa_setup_at is not None
    
    def has_backup_codes_(self) -> bool:
        """Check if user has MFA backup codes"""
        if not self.mfa_backup_codes:
            return False
        try:
            codes = json.loads(self.mfa_backup_codes)
            return isinstance(codes, list) and len(codes) > 0
        except (json.JSONDecodeError, TypeError):
            return False
    
    def email_verified_(self) -> bool:
        """Check if email is verified"""
        return self.email_verified_at is not None
    
    def tos_signed_(self) -> bool:
        """Check if terms of service is signed"""
        return self.tos_signed_at is not None
    
    def privacy_policy_signed_(self) -> bool:
        """Check if privacy policy is signed"""
        return self.privacy_policy_signed_at is not None
    
    def compliant_(self) -> bool:
        """Check if user is compliant (verified email and signed ToS)"""
        return self.email_verified_() and self.tos_signed_()
    
    def local_authentication_(self) -> bool:
        """Check if user uses local authentication"""
        return self.login_provider == LoginProviders.LOCAL.value
    
    def sso_authentication_(self) -> bool:
        """Check if user uses SSO authentication"""
        return self.login_provider != LoginProviders.LOCAL.value
    
    def api_access_enabled_(self) -> bool:
        """Check if API access is enabled"""
        return self.api_key_enabled and self.active_()
    
    def webhook_notifications_enabled_(self) -> bool:
        """Check if webhook notifications are enabled"""
        return self.webhook_notifications_enabled and self.active_()
    
    def password_expired_(self) -> bool:
        """Check if password has expired"""
        if not self.password_change_required_at:
            return False
        return datetime.utcnow() > self.password_change_required_at
    
    def password_recently_changed_(self, days: int = 1) -> bool:
        """Check if password was changed recently"""
        if not self.last_password_change_at:
            return False
        threshold = datetime.utcnow() - timedelta(days=days)
        return self.last_password_change_at > threshold
    
    def recently_logged_in_(self, hours: int = 24) -> bool:
        """Check if user logged in recently"""
        if not self.last_login_at:
            return False
        threshold = datetime.utcnow() - timedelta(hours=hours)
        return self.last_login_at > threshold
    
    def frequent_failed_logins_(self, threshold: int = 3) -> bool:
        """Check if user has frequent failed login attempts"""
        return self.failed_login_count >= threshold
    
    def requires_password_change_(self) -> bool:
        """Check if user requires password change"""
        return self.password_expired_() or self.password_change_required_at is not None
    
    def high_security_(self) -> bool:
        """Check if user has high security level"""
        return self.security_level in [SecurityLevels.HIGH.value, SecurityLevels.MAXIMUM.value]
    
    def maximum_security_(self) -> bool:
        """Check if user has maximum security level"""
        return self.security_level == SecurityLevels.MAXIMUM.value
    
    def can_login_(self) -> bool:
        """Check if user can login"""
        return (self.active_() and 
                self.email_verified_() and 
                self.tos_signed_() and 
                not self.is_account_locked())
    
    def new_user_(self, days: int = 30) -> bool:
        """Check if user is new (created within specified days)"""
        if not self.created_at:
            return False
        threshold = datetime.utcnow() - timedelta(days=days)
        return self.created_at > threshold
    
    def stale_user_(self, days: int = 90) -> bool:
        """Check if user is stale (not logged in for specified days)"""
        if not self.last_login_at:
            return True  # Never logged in
        threshold = datetime.utcnow() - timedelta(days=days)
        return self.last_login_at < threshold
    
    # === Rails-style Bang Methods ===
    
    def activate_(self) -> None:
        """Activate user account"""
        if self.active_():
            return
        
        self.status = UserStatuses.ACTIVE.value
        self.activated_at = datetime.utcnow()
        self.deactivated_at = None
        self.suspended_at = None
        self.suspension_reason = None
        self.account_locked_at = None
        self.password_retry_count = 0
        self.updated_at = datetime.utcnow()
        
        logger.info(f"User activated: {self.email}")
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate user account"""
        if self.deactivated_():
            return
        
        self.status = UserStatuses.DEACTIVATED.value
        self.deactivated_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        logger.info(f"User deactivated: {self.email} (reason: {reason or 'Not specified'})")
    
    def suspend_(self, reason: str = None) -> None:
        """Suspend user account"""
        if self.suspended_():
            return
        
        self.status = UserStatuses.SUSPENDED.value
        self.suspended_at = datetime.utcnow()
        self.suspension_reason = reason
        self.updated_at = datetime.utcnow()
        
        logger.warning(f"User suspended: {self.email} (reason: {reason or 'Not specified'})")
    
    def unsuspend_(self) -> None:
        """Remove suspension from user account"""
        if not self.suspended_():
            return
        
        self.status = UserStatuses.ACTIVE.value
        self.suspended_at = None
        self.suspension_reason = None
        self.updated_at = datetime.utcnow()
        
        logger.info(f"User suspension removed: {self.email}")
    
    def archive_(self) -> None:
        """Archive user account"""
        if self.archived_():
            return
        
        self.status = UserStatuses.ARCHIVED.value
        self.archived_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Clear sensitive data
        self.password_reset_token = None
        self.email_verification_token = None
        self.mfa_secret = None
        self.mfa_backup_codes = None
        
        logger.info(f"User archived: {self.email}")
    
    def promote_to_admin_(self) -> None:
        """Promote user to admin role"""
        if self.admin_():
            return
        
        self.role = UserRoles.ADMIN.value
        self.updated_at = datetime.utcnow()
        
        logger.info(f"User promoted to admin: {self.email}")
    
    def promote_to_super_admin_(self) -> None:
        """Promote user to super admin role"""
        if self.super_admin_():
            return
        
        self.role = UserRoles.SUPER_ADMIN.value
        self.updated_at = datetime.utcnow()
        
        logger.info(f"User promoted to super admin: {self.email}")
    
    def demote_to_user_(self) -> None:
        """Demote user to regular user role"""
        if self.role == UserRoles.USER.value:
            return
        
        self.role = UserRoles.USER.value
        self.updated_at = datetime.utcnow()
        
        logger.info(f"User demoted to regular user: {self.email}")
    
    def enable_mfa_(self, secret: str, backup_codes: List[str] = None) -> None:
        """Enable multi-factor authentication"""
        if self.mfa_enabled_():
            return
        
        self.mfa_enabled = True
        self.mfa_secret = secret
        self.mfa_setup_at = datetime.utcnow()
        
        if backup_codes:
            self.mfa_backup_codes = json.dumps(backup_codes)
        
        self.updated_at = datetime.utcnow()
        
        logger.info(f"MFA enabled for user: {self.email}")
    
    def disable_mfa_(self) -> None:
        """Disable multi-factor authentication"""
        if not self.mfa_enabled_():
            return
        
        self.mfa_enabled = False
        self.mfa_secret = None
        self.mfa_backup_codes = None
        self.mfa_setup_at = None
        self.updated_at = datetime.utcnow()
        
        logger.info(f"MFA disabled for user: {self.email}")
    
    def verify_email_(self) -> None:
        """Mark email as verified"""
        if self.email_verified_():
            return
        
        self.email_verified_at = datetime.utcnow()
        self.email_verification_token = None
        self.email_verification_token_at = None
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Email verified for user: {self.email}")
    
    def sign_tos_(self) -> None:
        """Mark terms of service as signed"""
        if self.tos_signed_():
            return
        
        self.tos_signed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        logger.info(f"ToS signed for user: {self.email}")
    
    def sign_privacy_policy_(self) -> None:
        """Mark privacy policy as signed"""
        if self.privacy_policy_signed_():
            return
        
        self.privacy_policy_signed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Privacy policy signed for user: {self.email}")
    
    def increase_security_level_(self, level: SecurityLevels = None) -> None:
        """Increase user security level"""
        if level is None:
            # Auto-increment to next level
            current_levels = list(SecurityLevels)
            try:
                current_index = current_levels.index(SecurityLevels(self.security_level))
                if current_index < len(current_levels) - 1:
                    level = current_levels[current_index + 1]
                else:
                    return  # Already at maximum
            except ValueError:
                level = SecurityLevels.ENHANCED
        
        self.security_level = level.value
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Security level increased for user {self.email}: {level.value}")
    
    def reset_security_level_(self) -> None:
        """Reset security level to standard"""
        if self.security_level == SecurityLevels.STANDARD.value:
            return
        
        self.security_level = SecurityLevels.STANDARD.value
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Security level reset for user: {self.email}")
    
    def enable_api_access_(self) -> None:
        """Enable API access"""
        if self.api_key_enabled:
            return
        
        self.api_key_enabled = True
        self.updated_at = datetime.utcnow()
        
        logger.info(f"API access enabled for user: {self.email}")
    
    def disable_api_access_(self) -> None:
        """Disable API access"""
        if not self.api_key_enabled:
            return
        
        self.api_key_enabled = False
        self.updated_at = datetime.utcnow()
        
        logger.info(f"API access disabled for user: {self.email}")
    
    def update_preferences_(self, preferences_dict: Dict[str, Any]) -> None:
        """Update user preferences"""
        current_prefs = self.get_preferences()
        
        # Merge with current preferences
        for key, value in preferences_dict.items():
            if hasattr(current_prefs, key):
                setattr(current_prefs, key, value)
        
        self.preferences = json.dumps(current_prefs.to_dict())
        self.updated_at = datetime.utcnow()
        
        # Clear cache
        self._cached_preferences = None
    
    def record_successful_login_(self, ip_address: str = None) -> None:
        """Record successful login"""
        self.last_login_at = datetime.utcnow()
        self.login_count = (self.login_count or 0) + 1
        self.failed_login_count = 0  # Reset failed count
        
        if ip_address:
            self.last_login_ip = ip_address
        
        self.updated_at = datetime.utcnow()
    
    def record_failed_login_(self) -> None:
        """Record failed login attempt"""
        self.failed_login_count = (self.failed_login_count or 0) + 1
        self.last_failed_login_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    @validates('email')
    def validate_email_field(self, key, address):
        """Email validation and normalization"""
        if not address:
            raise ValueError("Email is required")
            
        # Normalize email to lowercase
        address = address.lower().strip()
        
        # Length validation
        if len(address) < EMAIL_CONSTRAINTS["minimum"]:
            raise ValueError(f"Email must be at least {EMAIL_CONSTRAINTS['minimum']} characters")
        if len(address) > EMAIL_CONSTRAINTS["maximum"]:
            raise ValueError(f"Email must not exceed {EMAIL_CONSTRAINTS['maximum']} characters")
        
        # Email format validation
        try:
            validate_email(address)
        except EmailNotValidError as e:
            raise ValueError(f"Invalid email format: {str(e)}")
            
        return address
    
    @hybrid_property
    def password(self):
        """Password getter - always raises error for security"""
        raise AttributeError("Password is not readable")
    
    @password.setter
    def password(self, plaintext_password):
        """Password setter with full Rails validation logic"""
        if not plaintext_password:
            raise ValueError("Password cannot be empty")
            
        # Store for validation
        self._current_password = plaintext_password
        
        # Validate password
        self._validate_password(plaintext_password)
        
        # Check password reuse
        if self._password_previously_used(plaintext_password):
            raise ValueError("Password has been used recently. Please choose a different password.")
        
        # Rotate password digests (store previous 4 for reuse checking)
        self.password_digest_4 = self.password_digest_3
        self.password_digest_3 = self.password_digest_2  
        self.password_digest_2 = self.password_digest_1
        self.password_digest_1 = self.password_digest
        
        # Hash and store new password
        self.password_digest = pwd_context.hash(plaintext_password)
        
        # Reset password management fields
        self.password_retry_count = 0
        self.account_locked_at = None
        self.password_change_required_at = datetime.utcnow() + timedelta(days=PASSWORD_CHANGE_REQUIRED_AFTER_DAYS)
        
        logger.info(f"Password updated for user {self.id}")
    
    def _validate_password(self, password: str):
        """Complete password validation from Rails model"""
        # Length validation
        if len(password) < PASSWORD_CONSTRAINTS["minimum"]:
            raise ValueError(f"Password must be at least {PASSWORD_CONSTRAINTS['minimum']} characters long")
        if len(password) > PASSWORD_CONSTRAINTS["maximum"]:
            raise ValueError(f"Password must not exceed {PASSWORD_CONSTRAINTS['maximum']} characters")
        
        # Pattern validation (digit, lower, upper, symbol)
        if not PASSWORD_FORMAT.match(password):
            raise ValueError("Password must contain at least 8 characters, one digit, one lower case character, one upper case character, and one symbol")
        
        # Weakness validation
        if self._password_weak(password):
            raise ValueError("Password is too weak. Please choose a stronger password.")
    
    def _password_weak(self, password: str) -> bool:
        """Port of Rails password_weak? validation"""
        if not password:
            return True
            
        # Check if password contains extra words that make it weak
        password_lower = password.lower()
        for word in EXTRA_WORDS:
            if word.lower() in password_lower:
                return True
                
        # Check if password contains email parts
        if self.email:
            email_parts = self.email.split('@')
            for part in email_parts:
                if len(part) > 3 and part.lower() in password_lower:
                    return True
        
        # Entropy check - simplified version
        if self._calculate_password_entropy(password) < MIN_PASSWORD_ENTROPY:
            return True
            
        return False
    
    def _calculate_password_entropy(self, password: str) -> float:
        """Simplified password entropy calculation"""
        if not password:
            return 0
            
        # Character set size estimation
        charset_size = 0
        if any(c.islower() for c in password):
            charset_size += 26
        if any(c.isupper() for c in password):
            charset_size += 26  
        if any(c.isdigit() for c in password):
            charset_size += 10
        if any(not c.isalnum() for c in password):
            charset_size += 32  # Approximate special characters
            
        # Entropy = log2(charset_size^length)
        import math
        return math.log2(charset_size ** len(password)) if charset_size > 0 else 0
    
    def _password_previously_used(self, password: str) -> bool:
        """Check if password was used in the last 4 passwords"""
        previous_hashes = [
            self.password_digest,
            self.password_digest_1,
            self.password_digest_2,
            self.password_digest_3,
            self.password_digest_4
        ]
        
        for previous_hash in previous_hashes:
            if previous_hash and pwd_context.verify(password, previous_hash):
                return True
        return False
    
    def verify_password(self, password: str) -> bool:
        """Verify password against current hash"""
        if not password or not self.password_digest:
            return False
        return pwd_context.verify(password, self.password_digest)
    
    def authenticate(self, password: str) -> bool:
        """Authenticate user with password (includes lockout logic)"""
        if self.is_account_locked():
            logger.warning(f"Authentication attempt for locked account: {self.email}")
            return False
            
        if self.verify_password(password):
            # Reset retry count on successful authentication
            self.password_retry_count = 0
            logger.info(f"Successful authentication for user: {self.email}")
            return True
        else:
            # Increment retry count
            self.password_retry_count = (self.password_retry_count or 0) + 1
            
            # Lock account if max retries exceeded
            if self.password_retry_count >= MAX_PASSWORD_RETRY_COUNT:
                self.account_locked_at = datetime.utcnow()
                logger.warning(f"Account locked due to too many failed attempts: {self.email}")
            
            logger.warning(f"Failed authentication for user: {self.email} (attempt {self.password_retry_count})")
            return False
    
    # Status checking methods
    def is_active(self) -> bool:
        return self.status == UserStatus.ACTIVE and not self.is_account_locked()
    
    def is_deactivated(self) -> bool:
        return self.status == UserStatus.DEACTIVATED
        
    def is_account_locked(self) -> bool:
        return self.account_locked_at is not None
    
    def is_password_change_required(self) -> bool:
        """Check if password change is required"""
        if not self.password_change_required_at:
            return False
        return datetime.utcnow() > self.password_change_required_at
    
    def is_email_verified(self) -> bool:
        return self.email_verified_at is not None
    
    def is_tos_signed(self) -> bool:
        return self.tos_signed_at is not None
    
    # Password reset functionality
    def generate_password_reset_token(self) -> str:
        """Generate secure password reset token"""
        # Check rate limiting
        if self.password_reset_token_at:
            time_since_last = datetime.utcnow() - self.password_reset_token_at
            if time_since_last.total_seconds() < RESET_PASSWORD_INTERVAL_MINS * 60:
                raise ValueError("Password reset requested too recently. Please wait before requesting again.")
        
        # Check max tries
        if (self.password_reset_token_count or 0) >= MAX_RESET_PASSWORD_TRIES:
            raise ValueError("Maximum password reset attempts exceeded. Please contact support.")
        
        # Generate secure token
        token = secrets.token_urlsafe(32)
        
        # Store token (hash it for security)
        self.password_reset_token = pwd_context.hash(token)
        self.password_reset_token_at = datetime.utcnow()
        self.password_reset_token_count = (self.password_reset_token_count or 0) + 1
        
        return token
    
    def verify_password_reset_token(self, token: str) -> bool:
        """Verify password reset token"""
        if not token or not self.password_reset_token:
            return False
            
        # Check if token has expired (24 hours)
        if self.password_reset_token_at:
            time_since_token = datetime.utcnow() - self.password_reset_token_at
            if time_since_token.total_seconds() > 24 * 60 * 60:  # 24 hours
                return False
        
        return pwd_context.verify(token, self.password_reset_token)
    
    def reset_password_with_token(self, token: str, new_password: str):
        """Reset password using valid token"""
        if not self.verify_password_reset_token(token):
            raise ValueError("Invalid or expired password reset token")
        
        # Set new password (this validates it automatically)
        self.password = new_password
        
        # Clear reset token
        self.password_reset_token = None
        self.password_reset_token_at = None
        self.password_reset_token_count = 0
        
        logger.info(f"Password reset completed for user: {self.email}")
    
    # Account management
    def activate(self):
        """Activate user account"""
        self.status = UserStatus.ACTIVE
        self.account_locked_at = None
        self.password_retry_count = 0
        logger.info(f"User account activated: {self.email}")
    
    def deactivate(self):
        """Deactivate user account"""  
        self.status = UserStatus.DEACTIVATED
        logger.info(f"User account deactivated: {self.email}")
    
    def unlock_account(self):
        """Unlock locked account"""
        self.account_locked_at = None
        self.password_retry_count = 0
        logger.info(f"User account unlocked: {self.email}")
    
    def verify_email(self):
        """Mark email as verified"""
        self.email_verified_at = datetime.utcnow()
        logger.info(f"Email verified for user: {self.email}")
    
    def sign_tos(self):
        """Mark terms of service as signed"""
        self.tos_signed_at = datetime.utcnow()
        logger.info(f"ToS signed for user: {self.email}")
    
    # Organization and role management
    def get_org_membership(self, org_id: int):
        """Get membership for specific organization"""
        for membership in self.org_memberships:
            if membership.org_id == org_id:
                return membership
        return None
    
    def has_org_role(self, org_id: int, role: str) -> bool:
        """Check if user has specific role in organization"""
        membership = self.get_org_membership(org_id)
        return membership and membership.role == role
    
    def is_org_admin(self, org_id: int) -> bool:
        """Check if user is admin of organization"""
        return self.has_org_role(org_id, 'admin')
    
    def get_member_org_ids(self) -> List[int]:
        """Get list of organization IDs user is a member of"""
        return [membership.org_id for membership in self.org_memberships]
    
    # Super user functionality (from Rails model)
    def is_super_user(self) -> bool:
        """Check if user has super user privileges"""
        if self._is_super_user is not None:
            return self._is_super_user
        
        # Cache and return super user status
        # This would need to be implemented based on the actual super user logic
        self._is_super_user = False  # Default implementation
        return self._is_super_user
    
    def is_infrastructure_user(self) -> bool:
        """Check if user is an infrastructure user"""
        return self._infrastructure_user
    
    @classmethod
    def find_by_email(cls, session: Session, email: str):
        """Find user by email (case insensitive)"""
        return session.query(cls).filter(
            func.lower(cls.email) == func.lower(email)
        ).first()
    
    @classmethod
    def active_users(cls):
        """Query scope for active users"""
        return cls.query.filter(cls.status == UserStatus.ACTIVE)
    
    # === Helper Methods ===
    
    def get_preferences(self) -> UserPreferences:
        """Get user preferences with caching"""
        if self._cached_preferences is None:
            if self.preferences:
                try:
                    prefs_dict = json.loads(self.preferences)
                    self._cached_preferences = UserPreferences(**prefs_dict)
                except (json.JSONDecodeError, TypeError):
                    self._cached_preferences = UserPreferences()
            else:
                self._cached_preferences = UserPreferences()
        return self._cached_preferences
    
    def get_security_info(self) -> UserSecurityInfo:
        """Get user security information"""
        if self._cached_security_info is None:
            self._cached_security_info = UserSecurityInfo(
                has_mfa_enabled=self.mfa_enabled_(),
                last_login_at=self.last_login_at,
                last_login_ip=self.last_login_ip,
                failed_login_attempts=self.failed_login_count or 0,
                security_level=SecurityLevels(self.security_level)
            )
        return self._cached_security_info
    
    def get_display_name(self) -> str:
        """Get user display name with fallback logic"""
        if self.display_name:
            return self.display_name
        if self.full_name:
            return self.full_name
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        if self.first_name:
            return self.first_name
        return self.email.split('@')[0]
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get user metadata as dictionary"""
        if not self.extra_metadata:
            return {}
        try:
            return json.loads(self.extra_metadata)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_metadata(self, metadata_dict: Dict[str, Any]) -> None:
        """Set user metadata from dictionary"""
        self.extra_metadata = json.dumps(metadata_dict)
        self.updated_at = datetime.utcnow()
    
    def update_metadata(self, key: str, value: Any) -> None:
        """Update single metadata field"""
        current_metadata = self.get_metadata()
        current_metadata[key] = value
        self.set_metadata(current_metadata)
    
    def generate_email_verification_token(self) -> str:
        """Generate secure email verification token"""
        token = secrets.token_urlsafe(32)
        self.email_verification_token = pwd_context.hash(token)
        self.email_verification_token_at = datetime.utcnow()
        return token
    
    def verify_email_verification_token(self, token: str) -> bool:
        """Verify email verification token"""
        if not token or not self.email_verification_token:
            return False
        
        # Check if token has expired (24 hours)
        if self.email_verification_token_at:
            time_since_token = datetime.utcnow() - self.email_verification_token_at
            if time_since_token.total_seconds() > 24 * 60 * 60:
                return False
        
        return pwd_context.verify(token, self.email_verification_token)
    
    def get_login_history_summary(self, days: int = 30) -> Dict[str, Any]:
        """Get login history summary"""
        return {
            'total_logins': self.login_count or 0,
            'failed_logins': self.failed_login_count or 0,
            'last_login': self.last_login_at.isoformat() if self.last_login_at else None,
            'last_login_ip': self.last_login_ip,
            'last_failed_login': self.last_failed_login_at.isoformat() if self.last_failed_login_at else None,
            'recent_login': self.recently_logged_in_(hours=24),
            'frequent_failures': self.frequent_failed_logins_()
        }
    
    def get_security_summary(self) -> Dict[str, Any]:
        """Get comprehensive security summary"""
        return {
            'security_level': self.security_level,
            'mfa_enabled': self.mfa_enabled_(),
            'has_backup_codes': self.has_backup_codes_(),
            'password_expired': self.password_expired_(),
            'password_recently_changed': self.password_recently_changed_(),
            'account_locked': self.is_account_locked(),
            'login_provider': self.login_provider,
            'high_security': self.high_security_(),
            'api_access': self.api_access_enabled_()
        }
    
    # === Class Methods (Rails-style Scopes) ===
    
    @classmethod
    def active_users(cls, session: Session):
        """Get all active users"""
        return session.query(cls).filter(cls.status == UserStatuses.ACTIVE.value)
    
    @classmethod
    def inactive_users(cls, session: Session):
        """Get all inactive users"""
        return session.query(cls).filter(cls.status != UserStatuses.ACTIVE.value)
    
    @classmethod
    def verified_users(cls, session: Session):
        """Get all email verified users"""
        return session.query(cls).filter(cls.email_verified_at.isnot(None))
    
    @classmethod
    def unverified_users(cls, session: Session):
        """Get all unverified users"""
        return session.query(cls).filter(cls.email_verified_at.is_(None))
    
    @classmethod
    def admins(cls, session: Session):
        """Get all admin users"""
        return session.query(cls).filter(
            cls.role.in_([UserRoles.ADMIN.value, UserRoles.SUPER_ADMIN.value])
        )
    
    @classmethod
    def super_admins(cls, session: Session):
        """Get all super admin users"""
        return session.query(cls).filter(cls.role == UserRoles.SUPER_ADMIN.value)
    
    @classmethod
    def mfa_enabled_users(cls, session: Session):
        """Get all users with MFA enabled"""
        return session.query(cls).filter(cls.mfa_enabled == True)
    
    @classmethod
    def suspended_users(cls, session: Session):
        """Get all suspended users"""
        return session.query(cls).filter(cls.status == UserStatuses.SUSPENDED.value)
    
    @classmethod
    def locked_accounts(cls, session: Session):
        """Get all locked accounts"""
        return session.query(cls).filter(cls.account_locked_at.isnot(None))
    
    @classmethod
    def recent_logins(cls, session: Session, hours: int = 24):
        """Get users with recent logins"""
        threshold = datetime.utcnow() - timedelta(hours=hours)
        return session.query(cls).filter(cls.last_login_at > threshold)
    
    @classmethod
    def stale_users(cls, session: Session, days: int = 90):
        """Get stale users (not logged in recently)"""
        threshold = datetime.utcnow() - timedelta(days=days)
        return session.query(cls).filter(
            (cls.last_login_at < threshold) | (cls.last_login_at.is_(None))
        )
    
    @classmethod
    def new_users(cls, session: Session, days: int = 30):
        """Get recently created users"""
        threshold = datetime.utcnow() - timedelta(days=days)
        return session.query(cls).filter(cls.created_at > threshold)
    
    @classmethod
    def by_security_level(cls, session: Session, level: SecurityLevels):
        """Get users by security level"""
        return session.query(cls).filter(cls.security_level == level.value)
    
    @classmethod
    def high_security_users(cls, session: Session):
        """Get high security users"""
        return session.query(cls).filter(
            cls.security_level.in_([SecurityLevels.HIGH.value, SecurityLevels.MAXIMUM.value])
        )
    
    @classmethod
    def by_login_provider(cls, session: Session, provider: LoginProviders):
        """Get users by login provider"""
        return session.query(cls).filter(cls.login_provider == provider.value)
    
    @classmethod
    def sso_users(cls, session: Session):
        """Get all SSO users"""
        return session.query(cls).filter(cls.login_provider != LoginProviders.LOCAL.value)
    
    @classmethod
    def find_by_email(cls, session: Session, email: str):
        """Find user by email (case insensitive)"""
        return session.query(cls).filter(
            func.lower(cls.email) == func.lower(email)
        ).first()
    
    @classmethod
    def find_by_provider_user_id(cls, session: Session, provider: LoginProviders, provider_user_id: str):
        """Find user by external provider user ID"""
        return session.query(cls).filter(
            cls.login_provider == provider.value,
            cls.provider_user_id == provider_user_id
        ).first()
    
    @classmethod
    def search_by_name_or_email(cls, session: Session, query: str):
        """Search users by name or email"""
        search_term = f"%{query.lower()}%"
        return session.query(cls).filter(
            (func.lower(cls.email).like(search_term)) |
            (func.lower(cls.full_name).like(search_term)) |
            (func.lower(cls.first_name).like(search_term)) |
            (func.lower(cls.last_name).like(search_term)) |
            (func.lower(cls.display_name).like(search_term))
        )
    
    @classmethod
    def build_from_input(cls, input_data: dict):
        """Create new User from input data (Rails pattern)"""
        if not input_data.get('email'):
            raise ValueError("Email is required")
        if not input_data.get('password'):
            raise ValueError("Password is required")
        
        # Set defaults
        defaults = {
            'status': UserStatuses.PENDING_ACTIVATION.value,
            'role': UserRoles.USER.value,
            'login_provider': LoginProviders.LOCAL.value,
            'security_level': SecurityLevels.STANDARD.value,
            'mfa_enabled': False,
            'api_key_enabled': True,
            'webhook_notifications_enabled': True,
            'timezone': 'UTC',
            'language': 'en'
        }
        
        # Merge with input data
        user_data = {**defaults, **input_data}
        
        # Handle enum conversions
        if isinstance(user_data.get('status'), str):
            try:
                user_data['status'] = UserStatuses(user_data['status']).value
            except ValueError:
                pass  # Keep original value if invalid
        
        if isinstance(user_data.get('role'), str):
            try:
                user_data['role'] = UserRoles(user_data['role']).value
            except ValueError:
                pass
        
        if isinstance(user_data.get('login_provider'), str):
            try:
                user_data['login_provider'] = LoginProviders(user_data['login_provider']).value
            except ValueError:
                pass
        
        # Extract password before creating user
        password = user_data.pop('password')
        
        # Create user instance
        user = cls(**user_data)
        
        # Set password (this will trigger validation and hashing)
        user.password = password
        
        return user
    
    def to_dict(self, include_sensitive: bool = False) -> dict:
        """Convert user to dictionary (for API responses)"""
        data = {
            'id': self.id,
            'email': self.email,
            'full_name': self.full_name,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'display_name': self.get_display_name(),
            'status': self.status,
            'role': self.role,
            'login_provider': self.login_provider,
            'security_level': self.security_level,
            'timezone': self.timezone,
            'language': self.language,
            'country_code': self.country_code,
            'is_active': self.active_(),
            'is_admin': self.admin_(),
            'is_email_verified': self.email_verified_(),
            'is_tos_signed': self.tos_signed_(),
            'is_compliant': self.compliant_(),
            'mfa_enabled': self.mfa_enabled_(),
            'api_access_enabled': self.api_access_enabled_(),
            'webhook_notifications_enabled': self.webhook_notifications_enabled_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'last_login_at': self.last_login_at.isoformat() if self.last_login_at else None,
            'default_org_id': self.default_org_id,
            'user_tier_id': self.user_tier_id,
            'preferences': self.get_preferences().to_dict(),
            'metadata': self.get_metadata()
        }
        
        if include_sensitive:
            data.update({
                'suspended': self.suspended_(),
                'archived': self.archived_(),
                'password_expired': self.password_expired_(),
                'password_recently_changed': self.password_recently_changed_(),
                'requires_password_change': self.requires_password_change_(),
                'is_account_locked': self.is_account_locked(),
                'password_retry_count': self.password_retry_count,
                'failed_login_count': self.failed_login_count,
                'login_count': self.login_count,
                'security_info': self.get_security_info().to_dict(),
                'login_history': self.get_login_history_summary(),
                'security_summary': self.get_security_summary(),
                'email_verified_at': self.email_verified_at.isoformat() if self.email_verified_at else None,
                'tos_signed_at': self.tos_signed_at.isoformat() if self.tos_signed_at else None,
                'privacy_policy_signed_at': self.privacy_policy_signed_at.isoformat() if self.privacy_policy_signed_at else None,
                'activated_at': self.activated_at.isoformat() if self.activated_at else None,
                'deactivated_at': self.deactivated_at.isoformat() if self.deactivated_at else None,
                'suspended_at': self.suspended_at.isoformat() if self.suspended_at else None,
                'suspension_reason': self.suspension_reason,
                'archived_at': self.archived_at.isoformat() if self.archived_at else None
            })
        
        return data
    
    def to_json(self, include_sensitive: bool = False) -> str:
        """Convert user to JSON string"""
        return json.dumps(self.to_dict(include_sensitive=include_sensitive), indent=2)


# SQLAlchemy event listeners for Rails-like callbacks
@event.listens_for(User, 'before_insert')
def user_before_insert(mapper, connection, target):
    """Before create callback"""
    # Ensure email is lowercase
    if target.email:
        target.email = target.email.lower()
    
    # Set password change requirement
    if not target.password_change_required_at:
        target.password_change_required_at = datetime.utcnow() + timedelta(days=PASSWORD_CHANGE_REQUIRED_AFTER_DAYS)

@event.listens_for(User, 'before_update') 
def user_before_update(mapper, connection, target):
    """Before update callback"""
    # Ensure email is lowercase
    if target.email:
        target.email = target.email.lower()