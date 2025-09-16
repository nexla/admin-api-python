from datetime import datetime, timedelta
from enum import Enum as PyEnum
import hashlib
import hmac
import json
import secrets
from typing import Dict, List, Optional, Any, Union, Set
import uuid

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint, BigInteger
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class ApiKeyStatus(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    REVOKED = "REVOKED"
    EXPIRED = "EXPIRED"
    SUSPENDED = "SUSPENDED"
    COMPROMISED = "COMPROMISED"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.REVOKED: "Revoked",
            self.EXPIRED: "Expired",
            self.SUSPENDED: "Suspended",
            self.COMPROMISED: "Compromised"
        }.get(self, self.value)
    
    @property
    def is_usable(self) -> bool:
        return self == self.ACTIVE


class ApiKeyType(PyEnum):
    FULL_ACCESS = "FULL_ACCESS"
    READ_ONLY = "READ_ONLY"
    WRITE_ONLY = "WRITE_ONLY"
    SERVICE_ACCOUNT = "SERVICE_ACCOUNT"
    WEBHOOK = "WEBHOOK"
    INTEGRATION = "INTEGRATION"
    TEMPORARY = "TEMPORARY"
    RESTRICTED = "RESTRICTED"
    
    @property
    def display_name(self) -> str:
        return {
            self.FULL_ACCESS: "Full Access",
            self.READ_ONLY: "Read Only",
            self.WRITE_ONLY: "Write Only", 
            self.SERVICE_ACCOUNT: "Service Account",
            self.WEBHOOK: "Webhook",
            self.INTEGRATION: "Integration",
            self.TEMPORARY: "Temporary",
            self.RESTRICTED: "Restricted"
        }.get(self, self.value)


class ApiKeyScope(PyEnum):
    GLOBAL = "GLOBAL"
    ORG = "ORG"
    PROJECT = "PROJECT"
    USER = "USER"
    RESOURCE = "RESOURCE"
    
    @property
    def display_name(self) -> str:
        return {
            self.GLOBAL: "Global Access",
            self.ORG: "Organization",
            self.PROJECT: "Project",
            self.USER: "User",
            self.RESOURCE: "Resource"
        }.get(self, self.value)


class ApiKeyEnvironment(PyEnum):
    PRODUCTION = "PRODUCTION"
    STAGING = "STAGING"
    DEVELOPMENT = "DEVELOPMENT"
    TESTING = "TESTING"
    
    @property
    def display_name(self) -> str:
        return {
            self.PRODUCTION: "Production",
            self.STAGING: "Staging",
            self.DEVELOPMENT: "Development",
            self.TESTING: "Testing"
        }.get(self, self.value)


class ApiKey(Base):
    __tablename__ = 'api_keys'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    key_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    key_prefix = Column(String(20), nullable=False)
    key_hash = Column(String(128), nullable=False, index=True)
    key_suffix = Column(String(10), nullable=False)
    
    api_key_type = Column(SQLEnum(ApiKeyType), nullable=False, default=ApiKeyType.READ_ONLY)
    status = Column(SQLEnum(ApiKeyStatus), nullable=False, default=ApiKeyStatus.ACTIVE)
    scope = Column(SQLEnum(ApiKeyScope), nullable=False, default=ApiKeyScope.ORG)
    environment = Column(SQLEnum(ApiKeyEnvironment), nullable=False, default=ApiKeyEnvironment.DEVELOPMENT)
    
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    project_id = Column(Integer, ForeignKey('projects.id'))
    
    permissions = Column(JSON, default=list)
    allowed_ips = Column(JSON, default=list)
    allowed_domains = Column(JSON, default=list)
    allowed_endpoints = Column(JSON, default=list)
    
    rate_limit_requests = Column(Integer, default=1000)
    rate_limit_window_seconds = Column(Integer, default=3600)
    current_rate_count = Column(Integer, default=0)
    rate_limit_reset_at = Column(DateTime)
    
    usage_count = Column(BigInteger, default=0)
    last_used_at = Column(DateTime)
    last_used_ip = Column(String(45))
    last_used_user_agent = Column(String(500))
    
    expires_at = Column(DateTime)
    revoked_at = Column(DateTime)
    revoked_by = Column(Integer, ForeignKey('users.id'))
    revocation_reason = Column(String(500))
    
    rotation_schedule_days = Column(Integer)
    last_rotated_at = Column(DateTime)
    next_rotation_at = Column(DateTime)
    
    active = Column(Boolean, default=True, nullable=False)
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    usage_logs = Column(JSON, default=list)
    security_events = Column(JSON, default=list)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    user = relationship("User", foreign_keys=[user_id], back_populates="api_keys")
    org = relationship("Org", back_populates="api_keys")
    project = relationship("Project", back_populates="api_keys")
    api_key_events = relationship("ApiKeyEvent", back_populates="api_key")
    revoker = relationship("User", foreign_keys=[revoked_by])
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    
    __table_args__ = (
        Index('idx_api_key_status', 'status'),
        Index('idx_api_key_type', 'api_key_type'),
        Index('idx_api_key_user_id', 'user_id'),
        Index('idx_api_key_org_id', 'org_id'),
        Index('idx_api_key_project_id', 'project_id'),
        Index('idx_api_key_scope', 'scope'),
        Index('idx_api_key_environment', 'environment'),
        Index('idx_api_key_expires_at', 'expires_at'),
        Index('idx_api_key_last_used', 'last_used_at'),
        Index('idx_api_key_active', 'active'),
        Index('idx_api_key_rate_limit_reset', 'rate_limit_reset_at'),
        UniqueConstraint('org_id', 'name', name='uq_api_key_org_name'),
        CheckConstraint('rate_limit_requests > 0', name='ck_api_key_rate_limit_positive'),
        CheckConstraint('rate_limit_window_seconds > 0', name='ck_api_key_rate_window_positive'),
        CheckConstraint('usage_count >= 0', name='ck_api_key_usage_count_non_negative'),
        CheckConstraint('current_rate_count >= 0', name='ck_api_key_current_rate_non_negative'),
    )
    
    KEY_LENGTH = 32
    PREFIX_LENGTH = 8
    SUFFIX_LENGTH = 4
    DEFAULT_RATE_LIMIT = 1000
    HIGH_USAGE_THRESHOLD = 10000
    SUSPICIOUS_USAGE_THRESHOLD = 1000  # Per hour
    MAX_LOG_ENTRIES = 1000
    
    def __repr__(self):
        return f"<ApiKey(id={self.id}, name='{self.name}', status='{self.status.value}')>"
    
    def active_(self) -> bool:
        """Check if API key is active (Rails pattern)"""
        return (self.active and 
                self.status == ApiKeyStatus.ACTIVE and
                not self.expired_() and
                not self.revoked_())
    
    def usable_(self) -> bool:
        """Check if API key is usable (Rails pattern)"""
        return self.status.is_usable and not self.expired_()
    
    def revoked_(self) -> bool:
        """Check if API key is revoked (Rails pattern)"""
        return self.status == ApiKeyStatus.REVOKED or self.revoked_at is not None
    
    def expired_(self) -> bool:
        """Check if API key is expired (Rails pattern)"""
        return (self.status == ApiKeyStatus.EXPIRED or 
                (self.expires_at and self.expires_at < datetime.now()))
    
    def suspended_(self) -> bool:
        """Check if API key is suspended (Rails pattern)"""
        return self.status == ApiKeyStatus.SUSPENDED
    
    def compromised_(self) -> bool:
        """Check if API key is compromised (Rails pattern)"""
        return self.status == ApiKeyStatus.COMPROMISED
    
    def temporary_(self) -> bool:
        """Check if API key is temporary (Rails pattern)"""
        return self.api_key_type == ApiKeyType.TEMPORARY
    
    def read_only_(self) -> bool:
        """Check if API key is read-only (Rails pattern)"""
        return self.api_key_type == ApiKeyType.READ_ONLY
    
    def write_only_(self) -> bool:
        """Check if API key is write-only (Rails pattern)"""
        return self.api_key_type == ApiKeyType.WRITE_ONLY
    
    def full_access_(self) -> bool:
        """Check if API key has full access (Rails pattern)"""
        return self.api_key_type == ApiKeyType.FULL_ACCESS
    
    def service_account_(self) -> bool:
        """Check if API key is for service account (Rails pattern)"""
        return self.api_key_type == ApiKeyType.SERVICE_ACCOUNT
    
    def rate_limited_(self) -> bool:
        """Check if API key is currently rate limited (Rails pattern)"""
        if not self.rate_limit_reset_at:
            return False
        return (self.rate_limit_reset_at > datetime.now() and 
                self.current_rate_count >= self.rate_limit_requests)
    
    def high_usage_(self) -> bool:
        """Check if API key has high usage (Rails pattern)"""
        return self.usage_count >= self.HIGH_USAGE_THRESHOLD
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if API key was recently used (Rails pattern)"""
        if not self.last_used_at:
            return False
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.last_used_at > cutoff
    
    def needs_rotation_(self) -> bool:
        """Check if API key needs rotation (Rails pattern)"""
        if not self.rotation_schedule_days:
            return False
        return (self.next_rotation_at and 
                self.next_rotation_at <= datetime.now())
    
    def overdue_rotation_(self) -> bool:
        """Check if API key is overdue for rotation (Rails pattern)"""
        if not self.rotation_schedule_days or not self.next_rotation_at:
            return False
        grace_period = timedelta(days=7)
        return self.next_rotation_at < (datetime.now() - grace_period)
    
    def suspicious_usage_(self) -> bool:
        """Check if API key has suspicious usage patterns (Rails pattern)"""
        if not self.last_used_at:
            return False
        
        # Check for high usage in last hour
        recent_logs = [log for log in (self.usage_logs or [])
                      if 'timestamp' in log and 
                      datetime.fromisoformat(log['timestamp']) > (datetime.now() - timedelta(hours=1))]
        
        return len(recent_logs) > self.SUSPICIOUS_USAGE_THRESHOLD
    
    def has_ip_restrictions_(self) -> bool:
        """Check if API key has IP restrictions (Rails pattern)"""
        return bool(self.allowed_ips)
    
    def has_domain_restrictions_(self) -> bool:
        """Check if API key has domain restrictions (Rails pattern)"""
        return bool(self.allowed_domains)
    
    def has_endpoint_restrictions_(self) -> bool:
        """Check if API key has endpoint restrictions (Rails pattern)"""
        return bool(self.allowed_endpoints)
    
    def needs_attention_(self) -> bool:
        """Check if API key needs attention (Rails pattern)"""
        return (self.compromised_() or 
                self.overdue_rotation_() or
                self.suspicious_usage_() or
                self.expired_())
    
    def activate_(self) -> None:
        """Activate API key (Rails bang method pattern)"""
        self.active = True
        self.status = ApiKeyStatus.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate API key (Rails bang method pattern)"""
        self.active = False
        self.status = ApiKeyStatus.INACTIVE
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deactivation_reason'] = reason
    
    def revoke_(self, reason: str, revoked_by_user_id: int = None) -> None:
        """Revoke API key (Rails bang method pattern)"""
        self.status = ApiKeyStatus.REVOKED
        self.revoked_at = datetime.now()
        self.revocation_reason = reason
        self.revoked_by = revoked_by_user_id
        self.active = False
        self.updated_at = datetime.now()
        
        self._log_security_event('revoked', {'reason': reason})
    
    def suspend_(self, reason: str) -> None:
        """Suspend API key (Rails bang method pattern)"""
        self.status = ApiKeyStatus.SUSPENDED
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['suspension_reason'] = reason
        self.extra_metadata['suspended_at'] = datetime.now().isoformat()
        
        self._log_security_event('suspended', {'reason': reason})
    
    def unsuspend_(self) -> None:
        """Unsuspend API key (Rails bang method pattern)"""
        if self.status == ApiKeyStatus.SUSPENDED:
            self.status = ApiKeyStatus.ACTIVE
            self.updated_at = datetime.now()
    
    def mark_compromised_(self, reason: str) -> None:
        """Mark API key as compromised (Rails bang method pattern)"""
        self.status = ApiKeyStatus.COMPROMISED
        self.active = False
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['compromise_reason'] = reason
        self.extra_metadata['compromised_at'] = datetime.now().isoformat()
        
        self._log_security_event('compromised', {'reason': reason})
    
    def set_expiry_(self, expires_at: datetime) -> None:
        """Set API key expiry (Rails bang method pattern)"""
        self.expires_at = expires_at
        self.updated_at = datetime.now()
    
    def extend_expiry_(self, days: int) -> None:
        """Extend API key expiry (Rails bang method pattern)"""
        if self.expires_at:
            self.expires_at = self.expires_at + timedelta(days=days)
        else:
            self.expires_at = datetime.now() + timedelta(days=days)
        self.updated_at = datetime.now()
    
    def record_usage_(self, ip_address: str = None, user_agent: str = None, 
                     endpoint: str = None, method: str = None) -> bool:
        """Record API key usage (Rails bang method pattern)"""
        # Check rate limiting first
        if not self._check_rate_limit():
            return False
        
        self.usage_count += 1
        self.last_used_at = datetime.now()
        
        if ip_address:
            self.last_used_ip = ip_address
        if user_agent:
            self.last_used_user_agent = user_agent[:500]
        
        # Log usage
        usage_log = {
            'timestamp': datetime.now().isoformat(),
            'ip_address': ip_address,
            'user_agent': user_agent,
            'endpoint': endpoint,
            'method': method
        }
        
        self.usage_logs = self.usage_logs or []
        self.usage_logs.append(usage_log)
        
        # Keep only recent logs
        if len(self.usage_logs) > self.MAX_LOG_ENTRIES:
            self.usage_logs = self.usage_logs[-self.MAX_LOG_ENTRIES:]
        
        self.updated_at = datetime.now()
        return True
    
    def rotate_(self, new_key: str = None) -> str:
        """Rotate API key (Rails bang method pattern)"""
        if not new_key:
            new_key = self._generate_api_key()
        
        old_key_hash = self.key_hash
        self.key_hash = self._hash_key(new_key)
        self.key_suffix = new_key[-self.SUFFIX_LENGTH:]
        
        self.last_rotated_at = datetime.now()
        if self.rotation_schedule_days:
            self.next_rotation_at = datetime.now() + timedelta(days=self.rotation_schedule_days)
        
        self._log_security_event('rotated', {
            'old_key_hash': old_key_hash,
            'new_key_hash': self.key_hash
        })
        
        self.updated_at = datetime.now()
        return new_key
    
    def set_rotation_schedule_(self, days: int) -> None:
        """Set rotation schedule (Rails bang method pattern)"""
        self.rotation_schedule_days = days
        if days:
            self.next_rotation_at = datetime.now() + timedelta(days=days)
        else:
            self.next_rotation_at = None
        self.updated_at = datetime.now()
    
    def update_rate_limit_(self, requests: int, window_seconds: int) -> None:
        """Update rate limit settings (Rails bang method pattern)"""
        self.rate_limit_requests = requests
        self.rate_limit_window_seconds = window_seconds
        self.current_rate_count = 0
        self.rate_limit_reset_at = None
        self.updated_at = datetime.now()
    
    def add_permission_(self, permission: str) -> None:
        """Add permission to API key (Rails bang method pattern)"""
        permissions = list(self.permissions or [])
        if permission not in permissions:
            permissions.append(permission)
            self.permissions = permissions
            self.updated_at = datetime.now()
    
    def remove_permission_(self, permission: str) -> None:
        """Remove permission from API key (Rails bang method pattern)"""
        permissions = list(self.permissions or [])
        if permission in permissions:
            permissions.remove(permission)
            self.permissions = permissions
            self.updated_at = datetime.now()
    
    def add_allowed_ip_(self, ip_address: str) -> None:
        """Add allowed IP address (Rails bang method pattern)"""
        allowed_ips = list(self.allowed_ips or [])
        if ip_address not in allowed_ips:
            allowed_ips.append(ip_address)
            self.allowed_ips = allowed_ips
            self.updated_at = datetime.now()
    
    def remove_allowed_ip_(self, ip_address: str) -> None:
        """Remove allowed IP address (Rails bang method pattern)"""
        allowed_ips = list(self.allowed_ips or [])
        if ip_address in allowed_ips:
            allowed_ips.remove(ip_address)
            self.allowed_ips = allowed_ips
            self.updated_at = datetime.now()
    
    def add_allowed_endpoint_(self, endpoint: str) -> None:
        """Add allowed endpoint (Rails bang method pattern)"""
        endpoints = list(self.allowed_endpoints or [])
        if endpoint not in endpoints:
            endpoints.append(endpoint)
            self.allowed_endpoints = endpoints
            self.updated_at = datetime.now()
    
    def remove_allowed_endpoint_(self, endpoint: str) -> None:
        """Remove allowed endpoint (Rails bang method pattern)"""
        endpoints = list(self.allowed_endpoints or [])
        if endpoint in endpoints:
            endpoints.remove(endpoint)
            self.allowed_endpoints = endpoints
            self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to API key (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from API key (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def _check_rate_limit(self) -> bool:
        """Check if request is within rate limit (private helper)"""
        now = datetime.now()
        
        # Reset rate limit window if needed
        if not self.rate_limit_reset_at or self.rate_limit_reset_at <= now:
            self.current_rate_count = 0
            self.rate_limit_reset_at = now + timedelta(seconds=self.rate_limit_window_seconds)
        
        # Check if within limit
        if self.current_rate_count >= self.rate_limit_requests:
            return False
        
        self.current_rate_count += 1
        return True
    
    def _log_security_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Log security event (private helper)"""
        event = {
            'type': event_type,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        self.security_events = self.security_events or []
        self.security_events.append(event)
        
        # Keep only recent events
        if len(self.security_events) > 100:
            self.security_events = self.security_events[-100:]
    
    @classmethod
    def _generate_api_key(cls) -> str:
        """Generate secure API key (Rails pattern)"""
        return secrets.token_urlsafe(cls.KEY_LENGTH)
    
    @classmethod
    def _hash_key(cls, api_key: str) -> str:
        """Hash API key for storage (Rails pattern)"""
        return hashlib.sha256(api_key.encode()).hexdigest()
    
    @classmethod
    def create_with_key(cls, name: str, user_id: int, org_id: int, 
                       api_key_type: ApiKeyType = ApiKeyType.READ_ONLY, **kwargs) -> tuple['ApiKey', str]:
        """Create API key with generated key (Rails pattern)"""
        api_key_value = cls._generate_api_key()
        prefix = f"ak_{secrets.token_hex(cls.PREFIX_LENGTH//2)}"
        
        api_key_data = {
            'name': name,
            'user_id': user_id,
            'org_id': org_id,
            'api_key_type': api_key_type,
            'key_prefix': prefix,
            'key_hash': cls._hash_key(api_key_value),
            'key_suffix': api_key_value[-cls.SUFFIX_LENGTH:],
            **kwargs
        }
        
        api_key = cls(**api_key_data)
        return api_key, api_key_value
    
    def validate_key(self, provided_key: str) -> bool:
        """Validate provided API key (Rails pattern)"""
        if not self.usable_():
            return False
        
        provided_hash = self._hash_key(provided_key)
        return hmac.compare_digest(self.key_hash, provided_hash)
    
    def can_access_endpoint(self, endpoint: str) -> bool:
        """Check if API key can access endpoint (Rails pattern)"""
        if not self.has_endpoint_restrictions_():
            return True
        
        # Check exact matches and wildcards
        for allowed in self.allowed_endpoints:
            if endpoint == allowed or endpoint.startswith(allowed.rstrip('*')):
                return True
        
        return False
    
    def can_access_from_ip(self, ip_address: str) -> bool:
        """Check if API key can access from IP (Rails pattern)"""
        if not self.has_ip_restrictions_():
            return True
        
        return ip_address in self.allowed_ips
    
    def has_permission(self, permission: str) -> bool:
        """Check if API key has specific permission (Rails pattern)"""
        return permission in (self.permissions or [])
    
    def masked_key(self) -> str:
        """Get masked API key for display (Rails pattern)"""
        return f"{self.key_prefix}****{self.key_suffix}"
    
    def usage_in_period(self, hours: int = 24) -> int:
        """Get usage count in time period (Rails pattern)"""
        if not self.usage_logs:
            return 0
        
        cutoff = datetime.now() - timedelta(hours=hours)
        return len([log for log in self.usage_logs 
                   if 'timestamp' in log and 
                   datetime.fromisoformat(log['timestamp']) > cutoff])
    
    def days_since_creation(self) -> int:
        """Calculate days since API key creation (Rails pattern)"""
        return (datetime.now() - self.created_at).days
    
    def days_since_last_use(self) -> Optional[int]:
        """Calculate days since last use (Rails pattern)"""
        if not self.last_used_at:
            return None
        return (datetime.now() - self.last_used_at).days
    
    def days_until_expiry(self) -> Optional[int]:
        """Calculate days until expiry (Rails pattern)"""
        if not self.expires_at:
            return None
        delta = self.expires_at - datetime.now()
        return max(0, delta.days)
    
    def days_until_rotation(self) -> Optional[int]:
        """Calculate days until next rotation (Rails pattern)"""
        if not self.next_rotation_at:
            return None
        delta = self.next_rotation_at - datetime.now()
        return max(0, delta.days)
    
    def rate_limit_status(self) -> Dict[str, Any]:
        """Get rate limit status (Rails pattern)"""
        return {
            'requests_limit': self.rate_limit_requests,
            'window_seconds': self.rate_limit_window_seconds,
            'current_count': self.current_rate_count,
            'remaining': max(0, self.rate_limit_requests - self.current_rate_count),
            'reset_at': self.rate_limit_reset_at.isoformat() if self.rate_limit_reset_at else None,
            'rate_limited': self.rate_limited_()
        }
    
    def security_summary(self) -> Dict[str, Any]:
        """Get security summary (Rails pattern)"""
        return {
            'key_id': self.key_id,
            'status': self.status.value,
            'compromised': self.compromised_(),
            'suspicious_usage': self.suspicious_usage_(),
            'needs_rotation': self.needs_rotation_(),
            'overdue_rotation': self.overdue_rotation_(),
            'has_restrictions': {
                'ip': self.has_ip_restrictions_(),
                'domain': self.has_domain_restrictions_(),
                'endpoint': self.has_endpoint_restrictions_()
            },
            'usage_count': self.usage_count,
            'high_usage': self.high_usage_(),
            'last_used_days_ago': self.days_since_last_use(),
            'security_events_count': len(self.security_events or [])
        }
    
    def usage_statistics(self) -> Dict[str, Any]:
        """Get usage statistics (Rails pattern)"""
        return {
            'key_id': self.key_id,
            'total_usage': self.usage_count,
            'usage_last_24h': self.usage_in_period(24),
            'usage_last_7d': self.usage_in_period(24 * 7),
            'usage_last_30d': self.usage_in_period(24 * 30),
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'days_since_last_use': self.days_since_last_use(),
            'days_since_creation': self.days_since_creation(),
            'rate_limit_status': self.rate_limit_status(),
            'high_usage': self.high_usage_(),
            'recently_used': self.recently_used_()
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate API key health report (Rails pattern)"""
        return {
            'key_id': self.key_id,
            'healthy': not self.needs_attention_(),
            'active': self.active_(),
            'usable': self.usable_(),
            'status': self.status.value,
            'expired': self.expired_(),
            'needs_rotation': self.needs_rotation_(),
            'overdue_rotation': self.overdue_rotation_(),
            'compromised': self.compromised_(),
            'suspicious_usage': self.suspicious_usage_(),
            'needs_attention': self.needs_attention_(),
            'security_summary': self.security_summary(),
            'usage_statistics': self.usage_statistics()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'key_id': self.key_id,
            'name': self.name,
            'description': self.description,
            'masked_key': self.masked_key(),
            'api_key_type': self.api_key_type.value,
            'status': self.status.value,
            'scope': self.scope.value,
            'environment': self.environment.value,
            'active': self.active,
            'usage_count': self.usage_count,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'tags': self.tags,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'permissions': self.permissions,
                'allowed_ips': self.allowed_ips,
                'allowed_domains': self.allowed_domains,
                'allowed_endpoints': self.allowed_endpoints,
                'rate_limit_status': self.rate_limit_status(),
                'metadata': self.extra_metadata,
                'revocation_reason': self.revocation_reason,
                'security_events': self.security_events[-10:] if self.security_events else []  # Last 10 events
            })
        
        return result