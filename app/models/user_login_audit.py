"""
UserLoginAudit Model - User authentication and login tracking entity.
Manages login attempts, session tracking, and security auditing with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum as PyEnum
import json
import uuid
from ..database import Base


class LoginAttemptType(PyEnum):
    """Login attempt type enumeration"""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    LOCKOUT = "LOCKOUT"
    PASSWORD_RESET = "PASSWORD_RESET"
    MFA_REQUIRED = "MFA_REQUIRED"
    MFA_SUCCESS = "MFA_SUCCESS"
    MFA_FAILURE = "MFA_FAILURE"
    LOGOUT = "LOGOUT"
    SESSION_EXPIRED = "SESSION_EXPIRED"


class LoginMethod(PyEnum):
    """Login method enumeration"""
    PASSWORD = "PASSWORD"
    API_KEY = "API_KEY"
    SSO = "SSO"
    OAUTH = "OAUTH"
    MFA = "MFA"
    SERVICE_ACCOUNT = "SERVICE_ACCOUNT"
    IMPERSONATION = "IMPERSONATION"


class UserLoginAudit(Base):
    __tablename__ = "user_login_audits"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    attempt_type = Column(SQLEnum(LoginAttemptType), nullable=False, index=True)
    login_method = Column(SQLEnum(LoginMethod), default=LoginMethod.PASSWORD, index=True)
    
    # Authentication details
    email_attempted = Column(String(255), index=True)
    ip_address = Column(String(45), index=True)  # IPv6 compatible
    user_agent = Column(Text)
    session_id = Column(String(255), index=True)
    
    # Request details
    request_path = Column(String(500))
    request_method = Column(String(10))
    request_headers = Column(JSON)
    
    # Failure details
    failure_reason = Column(String(255))
    error_message = Column(Text)
    
    # Security context
    is_suspicious = Column(Boolean, default=False, index=True)
    risk_score = Column(Integer, default=0, index=True)
    geolocation = Column(JSON)
    device_fingerprint = Column(String(255))
    
    # Additional metadata
    extra_metadata = Column(JSON)
    notes = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    attempted_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Foreign keys
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    
    # Relationships
    user = relationship("User", back_populates="user_login_audits")
    org = relationship("Org", foreign_keys=[org_id])
    
    # Rails business logic constants
    RETENTION_DAYS = 365
    MAX_FAILED_ATTEMPTS = 5
    LOCKOUT_DURATION_MINUTES = 30
    SUSPICIOUS_THRESHOLD = 3
    
    # Rails predicate methods
    def success_(self) -> bool:
        """Rails predicate: Check if login was successful"""
        return self.attempt_type == LoginAttemptType.SUCCESS
    
    def failure_(self) -> bool:
        """Rails predicate: Check if login failed"""
        return self.attempt_type == LoginAttemptType.FAILURE
    
    def lockout_(self) -> bool:
        """Rails predicate: Check if this caused account lockout"""
        return self.attempt_type == LoginAttemptType.LOCKOUT
    
    def suspicious_(self) -> bool:
        """Rails predicate: Check if login attempt was suspicious"""
        return self.is_suspicious or self.risk_score >= self.SUSPICIOUS_THRESHOLD
    
    def recent_(self, hours: int = 24) -> bool:
        """Rails predicate: Check if attempt is recent"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return self.attempted_at >= cutoff
    
    def same_session_(self, other_session_id: str) -> bool:
        """Rails predicate: Check if same session"""
        return self.session_id == other_session_id
    
    # Rails business logic methods
    def calculate_risk_score(self) -> int:
        """Calculate risk score based on various factors (Rails pattern)"""
        score = 0
        
        # Multiple failures from same IP
        if self.attempt_type == LoginAttemptType.FAILURE:
            score += 1
        
        # Unusual location
        if self.geolocation and 'unusual' in str(self.geolocation):
            score += 2
            
        # Suspicious user agent
        if self.user_agent and any(keyword in self.user_agent.lower() 
                                  for keyword in ['bot', 'crawler', 'script']):
            score += 1
            
        # Off-hours access
        if self.attempted_at.hour < 6 or self.attempted_at.hour > 22:
            score += 1
            
        return score
    
    def should_alert_(self) -> bool:
        """Check if this login attempt should trigger an alert (Rails pattern)"""
        return (self.failure_() or self.lockout_() or 
                self.suspicious_() or self.risk_score >= 3)
    
    def to_dict(self) -> dict:
        """Convert audit record to dictionary for API responses"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'attempt_type': self.attempt_type.value if self.attempt_type else None,
            'login_method': self.login_method.value if self.login_method else None,
            'email_attempted': self.email_attempted,
            'ip_address': self.ip_address,
            'success': self.success_(),
            'failure_reason': self.failure_reason,
            'is_suspicious': self.is_suspicious,
            'risk_score': self.risk_score,
            'attempted_at': self.attempted_at.isoformat() if self.attempted_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'user_id': self.user_id,
            'org_id': self.org_id
        }
    
    @classmethod
    def create_success_audit(cls, user, ip_address: str = None, user_agent: str = None,
                           session_id: str = None, login_method: LoginMethod = LoginMethod.PASSWORD):
        """Create successful login audit record (Rails pattern)"""
        return cls(
            attempt_type=LoginAttemptType.SUCCESS,
            login_method=login_method,
            email_attempted=user.email,
            ip_address=ip_address,
            user_agent=user_agent,
            session_id=session_id,
            user_id=user.id,
            org_id=user.default_org_id,
            attempted_at=datetime.utcnow()
        )
    
    @classmethod
    def create_failure_audit(cls, email: str, ip_address: str = None, user_agent: str = None,
                           failure_reason: str = None, user_id: int = None):
        """Create failed login audit record (Rails pattern)"""
        return cls(
            attempt_type=LoginAttemptType.FAILURE,
            login_method=LoginMethod.PASSWORD,
            email_attempted=email,
            ip_address=ip_address,
            user_agent=user_agent,
            failure_reason=failure_reason,
            user_id=user_id,
            attempted_at=datetime.utcnow()
        )
    
    @classmethod
    def create_lockout_audit(cls, user, ip_address: str = None, user_agent: str = None):
        """Create account lockout audit record (Rails pattern)"""
        return cls(
            attempt_type=LoginAttemptType.LOCKOUT,
            login_method=LoginMethod.PASSWORD,
            email_attempted=user.email,
            ip_address=ip_address,
            user_agent=user_agent,
            failure_reason="Account locked due to multiple failed attempts",
            user_id=user.id,
            org_id=user.default_org_id,
            attempted_at=datetime.utcnow()
        )