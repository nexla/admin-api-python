from datetime import datetime, timedelta
from enum import Enum as PyEnum
import hashlib
import json
import secrets
from typing import Dict, List, Optional, Any, Union
import uuid
import user_agents

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint, BigInteger
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class SessionStatus(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    EXPIRED = "EXPIRED"
    TERMINATED = "TERMINATED"
    SUSPICIOUS = "SUSPICIOUS"
    LOCKED = "LOCKED"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.EXPIRED: "Expired",
            self.TERMINATED: "Terminated",
            self.SUSPICIOUS: "Suspicious",
            self.LOCKED: "Locked"
        }.get(self, self.value)
    
    @property
    def is_usable(self) -> bool:
        return self == self.ACTIVE


class SessionType(PyEnum):
    WEB = "WEB"
    API = "API"
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    CLI = "CLI"
    SERVICE = "SERVICE"
    
    @property
    def display_name(self) -> str:
        return {
            self.WEB: "Web Browser",
            self.API: "API Client",
            self.MOBILE: "Mobile App",
            self.DESKTOP: "Desktop App",
            self.CLI: "Command Line",
            self.SERVICE: "Service Account"
        }.get(self, self.value)


class DeviceType(PyEnum):
    DESKTOP = "DESKTOP"
    MOBILE = "MOBILE"
    TABLET = "TABLET"
    BOT = "BOT"
    UNKNOWN = "UNKNOWN"
    
    @property
    def display_name(self) -> str:
        return {
            self.DESKTOP: "Desktop",
            self.MOBILE: "Mobile",
            self.TABLET: "Tablet",
            self.BOT: "Bot",
            self.UNKNOWN: "Unknown"
        }.get(self, self.value)


class SecurityLevel(PyEnum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    
    @property
    def display_name(self) -> str:
        return {
            self.LOW: "Low Security",
            self.NORMAL: "Normal Security",
            self.HIGH: "High Security",
            self.CRITICAL: "Critical Security"
        }.get(self, self.value)


class Session(Base):
    __tablename__ = 'sessions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    session_token = Column(String(128), unique=True, nullable=False, index=True)
    
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'))
    
    session_type = Column(SQLEnum(SessionType), nullable=False, default=SessionType.WEB)
    status = Column(SQLEnum(SessionStatus), nullable=False, default=SessionStatus.ACTIVE)
    security_level = Column(SQLEnum(SecurityLevel), nullable=False, default=SecurityLevel.NORMAL)
    
    # Device and browser information
    ip_address = Column(String(45), nullable=False, index=True)
    user_agent = Column(String(1000))
    device_type = Column(SQLEnum(DeviceType), default=DeviceType.UNKNOWN)
    device_fingerprint = Column(String(128))
    
    # Browser/client details
    browser_name = Column(String(100))
    browser_version = Column(String(50))
    os_name = Column(String(100))
    os_version = Column(String(50))
    
    # Geographic information
    country = Column(String(100))
    region = Column(String(100))
    city = Column(String(100))
    timezone = Column(String(50))
    
    # Session lifecycle
    started_at = Column(DateTime, default=datetime.now, nullable=False)
    last_activity_at = Column(DateTime, default=datetime.now, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    terminated_at = Column(DateTime)
    
    # Activity tracking
    page_views = Column(BigInteger, default=0)
    api_calls = Column(BigInteger, default=0)
    idle_time_seconds = Column(BigInteger, default=0)
    active_time_seconds = Column(BigInteger, default=0)
    
    # Security features
    mfa_verified = Column(Boolean, default=False)
    mfa_verified_at = Column(DateTime)
    remember_me = Column(Boolean, default=False)
    auto_logout_enabled = Column(Boolean, default=True)
    
    # Risk assessment
    risk_score = Column(Float, default=0.0)
    anomaly_score = Column(Float, default=0.0)
    trust_score = Column(Float, default=5.0)
    
    # Concurrent sessions
    concurrent_session_count = Column(Integer, default=1)
    max_concurrent_sessions = Column(Integer, default=5)
    
    # Session data
    session_data = Column(JSON, default=dict)
    preferences = Column(JSON, default=dict)
    activity_log = Column(JSON, default=list)
    security_events = Column(JSON, default=list)
    
    # Metadata
    client_version = Column(String(50))
    referrer = Column(String(500))
    landing_page = Column(String(500))
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    user = relationship("User", back_populates="sessions")
    org = relationship("Org", back_populates="sessions")
    
    __table_args__ = (
        Index('idx_session_user_id', 'user_id'),
        Index('idx_session_org_id', 'org_id'),
        Index('idx_session_status', 'status'),
        Index('idx_session_type', 'session_type'),
        Index('idx_session_ip_address', 'ip_address'),
        Index('idx_session_expires_at', 'expires_at'),
        Index('idx_session_last_activity', 'last_activity_at'),
        Index('idx_session_device_type', 'device_type'),
        Index('idx_session_risk_score', 'risk_score'),
        Index('idx_session_mfa_verified', 'mfa_verified'),
        CheckConstraint('page_views >= 0', name='ck_session_page_views_non_negative'),
        CheckConstraint('api_calls >= 0', name='ck_session_api_calls_non_negative'),
        CheckConstraint('risk_score >= 0 AND risk_score <= 10', name='ck_session_risk_score_range'),
        CheckConstraint('trust_score >= 0 AND trust_score <= 10', name='ck_session_trust_score_range'),
        CheckConstraint('concurrent_session_count >= 0', name='ck_session_concurrent_count_non_negative'),
    )
    
    DEFAULT_EXPIRY_HOURS = 24
    REMEMBER_ME_EXPIRY_DAYS = 30
    IDLE_TIMEOUT_MINUTES = 30
    HIGH_RISK_THRESHOLD = 7.0
    SUSPICIOUS_ACTIVITY_THRESHOLD = 5
    MAX_ACTIVITY_LOG_ENTRIES = 500
    
    def __init__(self, **kwargs):
        if 'session_token' not in kwargs:
            kwargs['session_token'] = self._generate_session_token()
        if 'expires_at' not in kwargs:
            hours = self.REMEMBER_ME_EXPIRY_DAYS * 24 if kwargs.get('remember_me') else self.DEFAULT_EXPIRY_HOURS
            kwargs['expires_at'] = datetime.now() + timedelta(hours=hours)
        super().__init__(**kwargs)
    
    def __repr__(self):
        return f"<Session(id={self.id}, user_id={self.user_id}, status='{self.status.value}')>"
    
    def active_(self) -> bool:
        """Check if session is active (Rails pattern)"""
        return (self.status == SessionStatus.ACTIVE and 
                not self.expired_() and 
                not self.terminated_())
    
    def usable_(self) -> bool:
        """Check if session is usable (Rails pattern)"""
        return self.status.is_usable and not self.expired_()
    
    def expired_(self) -> bool:
        """Check if session is expired (Rails pattern)"""
        return (self.status == SessionStatus.EXPIRED or 
                self.expires_at < datetime.now())
    
    def terminated_(self) -> bool:
        """Check if session is terminated (Rails pattern)"""
        return self.status == SessionStatus.TERMINATED or self.terminated_at is not None
    
    def suspicious_(self) -> bool:
        """Check if session is suspicious (Rails pattern)"""
        return self.status == SessionStatus.SUSPICIOUS
    
    def locked_(self) -> bool:
        """Check if session is locked (Rails pattern)"""
        return self.status == SessionStatus.LOCKED
    
    def idle_(self) -> bool:
        """Check if session is idle (Rails pattern)"""
        idle_cutoff = datetime.now() - timedelta(minutes=self.IDLE_TIMEOUT_MINUTES)
        return self.last_activity_at < idle_cutoff
    
    def mfa_verified_(self) -> bool:
        """Check if session has MFA verification (Rails pattern)"""
        return self.mfa_verified and self.mfa_verified_at is not None
    
    def remember_me_(self) -> bool:
        """Check if session has remember me enabled (Rails pattern)"""
        return self.remember_me
    
    def high_risk_(self) -> bool:
        """Check if session is high risk (Rails pattern)"""
        return self.risk_score >= self.HIGH_RISK_THRESHOLD
    
    def trusted_(self) -> bool:
        """Check if session is trusted (Rails pattern)"""
        return self.trust_score >= 7.0
    
    def mobile_(self) -> bool:
        """Check if session is from mobile device (Rails pattern)"""
        return self.device_type == DeviceType.MOBILE
    
    def desktop_(self) -> bool:
        """Check if session is from desktop (Rails pattern)"""
        return self.device_type == DeviceType.DESKTOP
    
    def bot_(self) -> bool:
        """Check if session is from bot (Rails pattern)"""
        return self.device_type == DeviceType.BOT
    
    def concurrent_limit_exceeded_(self) -> bool:
        """Check if concurrent session limit is exceeded (Rails pattern)"""
        return self.concurrent_session_count > self.max_concurrent_sessions
    
    def needs_attention_(self) -> bool:
        """Check if session needs attention (Rails pattern)"""
        return (self.high_risk_() or 
                self.suspicious_() or
                self.concurrent_limit_exceeded_() or
                self.locked_())
    
    def activate_(self) -> None:
        """Activate session (Rails bang method pattern)"""
        self.status = SessionStatus.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self) -> None:
        """Deactivate session (Rails bang method pattern)"""
        self.status = SessionStatus.INACTIVE
        self.updated_at = datetime.now()
    
    def terminate_(self, reason: str = None) -> None:
        """Terminate session (Rails bang method pattern)"""
        self.status = SessionStatus.TERMINATED
        self.terminated_at = datetime.now()
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['termination_reason'] = reason
        
        self._log_security_event('terminated', {'reason': reason})
    
    def mark_suspicious_(self, reason: str) -> None:
        """Mark session as suspicious (Rails bang method pattern)"""
        self.status = SessionStatus.SUSPICIOUS
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['suspicious_reason'] = reason
        
        self._log_security_event('marked_suspicious', {'reason': reason})
    
    def lock_(self, reason: str) -> None:
        """Lock session (Rails bang method pattern)"""
        self.status = SessionStatus.LOCKED
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['lock_reason'] = reason
        
        self._log_security_event('locked', {'reason': reason})
    
    def unlock_(self) -> None:
        """Unlock session (Rails bang method pattern)"""
        if self.status == SessionStatus.LOCKED:
            self.status = SessionStatus.ACTIVE
            self.updated_at = datetime.now()
    
    def expire_(self) -> None:
        """Expire session (Rails bang method pattern)"""
        self.status = SessionStatus.EXPIRED
        self.updated_at = datetime.now()
    
    def extend_expiry_(self, hours: int = None) -> None:
        """Extend session expiry (Rails bang method pattern)"""
        if hours is None:
            hours = self.DEFAULT_EXPIRY_HOURS
        
        self.expires_at = datetime.now() + timedelta(hours=hours)
        self.updated_at = datetime.now()
    
    def record_activity_(self, activity_type: str, details: Dict[str, Any] = None) -> None:
        """Record session activity (Rails bang method pattern)"""
        self.last_activity_at = datetime.now()
        
        if activity_type == 'page_view':
            self.page_views += 1
        elif activity_type == 'api_call':
            self.api_calls += 1
        
        # Log activity
        activity_entry = {
            'type': activity_type,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        
        self.activity_log = self.activity_log or []
        self.activity_log.append(activity_entry)
        
        # Keep only recent entries
        if len(self.activity_log) > self.MAX_ACTIVITY_LOG_ENTRIES:
            self.activity_log = self.activity_log[-self.MAX_ACTIVITY_LOG_ENTRIES:]
        
        self.updated_at = datetime.now()
    
    def verify_mfa_(self) -> None:
        """Verify MFA for session (Rails bang method pattern)"""
        self.mfa_verified = True
        self.mfa_verified_at = datetime.now()
        self.trust_score = min(10.0, self.trust_score + 1.0)
        self.updated_at = datetime.now()
        
        self._log_security_event('mfa_verified', {})
    
    def update_risk_score_(self, new_score: float, reason: str = None) -> None:
        """Update risk score (Rails bang method pattern)"""
        old_score = self.risk_score
        self.risk_score = max(0.0, min(10.0, new_score))
        
        if self.high_risk_():
            self.mark_suspicious_(f"High risk score: {self.risk_score}")
        
        self._log_security_event('risk_score_updated', {
            'old_score': old_score,
            'new_score': self.risk_score,
            'reason': reason
        })
        
        self.updated_at = datetime.now()
    
    def update_device_info_(self, user_agent: str) -> None:
        """Update device information from user agent (Rails bang method pattern)"""
        self.user_agent = user_agent
        
        try:
            ua = user_agents.parse(user_agent)
            self.browser_name = ua.browser.family
            self.browser_version = ua.browser.version_string
            self.os_name = ua.os.family
            self.os_version = ua.os.version_string
            
            if ua.is_mobile:
                self.device_type = DeviceType.MOBILE
            elif ua.is_tablet:
                self.device_type = DeviceType.TABLET
            elif ua.is_bot:
                self.device_type = DeviceType.BOT
            else:
                self.device_type = DeviceType.DESKTOP
                
        except Exception:
            self.device_type = DeviceType.UNKNOWN
        
        self.updated_at = datetime.now()
    
    def set_geographic_info_(self, country: str = None, region: str = None, 
                            city: str = None, timezone: str = None) -> None:
        """Set geographic information (Rails bang method pattern)"""
        if country:
            self.country = country
        if region:
            self.region = region
        if city:
            self.city = city
        if timezone:
            self.timezone = timezone
        
        self.updated_at = datetime.now()
    
    def add_security_event_(self, event_type: str, data: Dict[str, Any] = None) -> None:
        """Add security event (Rails bang method pattern)"""
        self._log_security_event(event_type, data or {})
        self.updated_at = datetime.now()
    
    def set_preference_(self, key: str, value: Any) -> None:
        """Set session preference (Rails bang method pattern)"""
        self.preferences = self.preferences or {}
        self.preferences[key] = value
        self.updated_at = datetime.now()
    
    def remove_preference_(self, key: str) -> None:
        """Remove session preference (Rails bang method pattern)"""
        if self.preferences and key in self.preferences:
            del self.preferences[key]
            self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to session (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from session (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def _log_security_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Log security event (private helper)"""
        event = {
            'type': event_type,
            'timestamp': datetime.now().isoformat(),
            'ip_address': self.ip_address,
            'data': data
        }
        
        self.security_events = self.security_events or []
        self.security_events.append(event)
        
        # Keep only recent events
        if len(self.security_events) > 100:
            self.security_events = self.security_events[-100:]
    
    @classmethod
    def _generate_session_token(cls) -> str:
        """Generate secure session token (Rails pattern)"""
        return hashlib.sha256(secrets.token_bytes(32)).hexdigest()
    
    def validate_token(self, provided_token: str) -> bool:
        """Validate provided session token (Rails pattern)"""
        if not self.usable_():
            return False
        
        return secrets.compare_digest(self.session_token, provided_token)
    
    def session_duration(self) -> timedelta:
        """Calculate session duration (Rails pattern)"""
        end_time = self.terminated_at or datetime.now()
        return end_time - self.started_at
    
    def idle_duration(self) -> timedelta:
        """Calculate idle duration (Rails pattern)"""
        return datetime.now() - self.last_activity_at
    
    def time_until_expiry(self) -> timedelta:
        """Calculate time until expiry (Rails pattern)"""
        return self.expires_at - datetime.now()
    
    def activity_in_period(self, hours: int = 24) -> int:
        """Get activity count in time period (Rails pattern)"""
        if not self.activity_log:
            return 0
        
        cutoff = datetime.now() - timedelta(hours=hours)
        return len([log for log in self.activity_log 
                   if 'timestamp' in log and 
                   datetime.fromisoformat(log['timestamp']) > cutoff])
    
    def get_preference(self, key: str, default: Any = None) -> Any:
        """Get session preference (Rails pattern)"""
        return (self.preferences or {}).get(key, default)
    
    def device_summary(self) -> Dict[str, Any]:
        """Get device summary (Rails pattern)"""
        return {
            'device_type': self.device_type.value,
            'browser_name': self.browser_name,
            'browser_version': self.browser_version,
            'os_name': self.os_name,
            'os_version': self.os_version,
            'mobile': self.mobile_(),
            'desktop': self.desktop_(),
            'bot': self.bot_()
        }
    
    def location_summary(self) -> Dict[str, Any]:
        """Get location summary (Rails pattern)"""
        return {
            'ip_address': self.ip_address,
            'country': self.country,
            'region': self.region,
            'city': self.city,
            'timezone': self.timezone
        }
    
    def security_summary(self) -> Dict[str, Any]:
        """Get security summary (Rails pattern)"""
        return {
            'session_id': self.session_id,
            'security_level': self.security_level.value,
            'risk_score': self.risk_score,
            'trust_score': self.trust_score,
            'mfa_verified': self.mfa_verified_(),
            'high_risk': self.high_risk_(),
            'suspicious': self.suspicious_(),
            'locked': self.locked_(),
            'concurrent_sessions': self.concurrent_session_count,
            'security_events_count': len(self.security_events or [])
        }
    
    def activity_summary(self) -> Dict[str, Any]:
        """Get activity summary (Rails pattern)"""
        return {
            'session_id': self.session_id,
            'page_views': self.page_views,
            'api_calls': self.api_calls,
            'total_activities': self.page_views + self.api_calls,
            'session_duration_minutes': self.session_duration().total_seconds() / 60,
            'idle_duration_minutes': self.idle_duration().total_seconds() / 60,
            'activity_last_24h': self.activity_in_period(24),
            'last_activity_at': self.last_activity_at.isoformat(),
            'idle': self.idle_()
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate session health report (Rails pattern)"""
        return {
            'session_id': self.session_id,
            'healthy': not self.needs_attention_(),
            'active': self.active_(),
            'usable': self.usable_(),
            'status': self.status.value,
            'expired': self.expired_(),
            'terminated': self.terminated_(),
            'idle': self.idle_(),
            'needs_attention': self.needs_attention_(),
            'security_summary': self.security_summary(),
            'activity_summary': self.activity_summary(),
            'device_summary': self.device_summary(),
            'location_summary': self.location_summary()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'session_id': self.session_id,
            'session_type': self.session_type.value,
            'status': self.status.value,
            'security_level': self.security_level.value,
            'device_type': self.device_type.value,
            'ip_address': self.ip_address,
            'browser_name': self.browser_name,
            'os_name': self.os_name,
            'country': self.country,
            'mfa_verified': self.mfa_verified,
            'risk_score': self.risk_score,
            'trust_score': self.trust_score,
            'page_views': self.page_views,
            'api_calls': self.api_calls,
            'started_at': self.started_at.isoformat(),
            'last_activity_at': self.last_activity_at.isoformat(),
            'expires_at': self.expires_at.isoformat(),
            'tags': self.tags,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'session_token': self.session_token,
                'user_agent': self.user_agent,
                'device_fingerprint': self.device_fingerprint,
                'session_data': self.session_data,
                'preferences': self.preferences,
                'metadata': self.extra_metadata,
                'activity_log': self.activity_log[-10:] if self.activity_log else [],  # Last 10 activities
                'security_events': self.security_events[-5:] if self.security_events else []  # Last 5 events
            })
        
        return result