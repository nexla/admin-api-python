"""
ApiKeyEvent Model - API key usage tracking and audit trail entity.
Manages API key events, usage logging, and access auditing with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum as PyEnum
import json
import uuid
from ..database import Base


class ApiKeyEventType(PyEnum):
    """API key event type enumeration"""
    CREATED = "CREATED"
    USED = "USED"
    REVOKED = "REVOKED"
    EXPIRED = "EXPIRED"
    FAILED_AUTH = "FAILED_AUTH"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    RATE_LIMITED = "RATE_LIMITED"
    SUSPENDED = "SUSPENDED"
    REACTIVATED = "REACTIVATED"


class ApiKeyEventStatus(PyEnum):
    """API key event status enumeration"""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    WARNING = "WARNING"
    INFO = "INFO"
    ERROR = "ERROR"


class ApiKeyEvent(Base):
    __tablename__ = "api_key_events"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    event_type = Column(SQLEnum(ApiKeyEventType), nullable=False, index=True)
    status = Column(SQLEnum(ApiKeyEventStatus), default=ApiKeyEventStatus.SUCCESS, index=True)
    
    # Event details
    endpoint = Column(String(255), index=True)
    method = Column(String(10), index=True)
    ip_address = Column(String(45), index=True)  # IPv6 compatible
    user_agent = Column(Text)
    request_id = Column(String(36), index=True)
    
    # Response details
    status_code = Column(Integer, index=True)
    response_time_ms = Column(Float)
    bytes_processed = Column(Integer)
    
    # Additional metadata
    extra_metadata = Column(JSON)
    error_message = Column(Text)
    warning_message = Column(Text)
    notes = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    occurred_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Foreign keys
    api_key_id = Column(Integer, ForeignKey("api_keys.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)  # Rails owner_id
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    
    # Relationships
    api_key = relationship("ApiKey", back_populates="api_key_events")
    user = relationship("User", foreign_keys=[user_id], back_populates="api_key_events")
    org = relationship("Org", foreign_keys=[org_id])
    
    # Rails business logic constants
    RETENTION_DAYS = 90
    MAX_EVENTS_PER_KEY = 10000
    
    # Rails predicate methods
    def success_(self) -> bool:
        """Rails predicate: Check if event was successful"""
        return self.status == ApiKeyEventStatus.SUCCESS
    
    def failure_(self) -> bool:
        """Rails predicate: Check if event was a failure"""
        return self.status == ApiKeyEventStatus.FAILURE
    
    def error_(self) -> bool:
        """Rails predicate: Check if event was an error"""
        return self.status == ApiKeyEventStatus.ERROR
    
    def recent_(self, hours: int = 24) -> bool:
        """Rails predicate: Check if event is recent"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return self.occurred_at >= cutoff
    
    # Rails business logic methods
    def should_alert_(self) -> bool:
        """Check if this event should trigger an alert (Rails pattern)"""
        return (self.event_type in [ApiKeyEventType.FAILED_AUTH, ApiKeyEventType.PERMISSION_DENIED] 
                or self.status == ApiKeyEventStatus.ERROR)
    
    def to_dict(self) -> dict:
        """Convert event to dictionary for API responses"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'event_type': self.event_type.value if self.event_type else None,
            'status': self.status.value if self.status else None,
            'endpoint': self.endpoint,
            'method': self.method,
            'ip_address': self.ip_address,
            'status_code': self.status_code,
            'response_time_ms': self.response_time_ms,
            'occurred_at': self.occurred_at.isoformat() if self.occurred_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'api_key_id': self.api_key_id,
            'user_id': self.user_id,
            'org_id': self.org_id
        }
    
    @classmethod
    def create_usage_event(cls, api_key, endpoint: str, method: str, 
                          status_code: int, response_time_ms: float = None,
                          ip_address: str = None, user_agent: str = None):
        """Create a usage event for an API key (Rails pattern)"""
        event_type = ApiKeyEventType.USED
        status = ApiKeyEventStatus.SUCCESS if 200 <= status_code < 400 else ApiKeyEventStatus.FAILURE
        
        return cls(
            event_type=event_type,
            status=status,
            endpoint=endpoint,
            method=method,
            ip_address=ip_address,
            user_agent=user_agent,
            status_code=status_code,
            response_time_ms=response_time_ms,
            api_key_id=api_key.id,
            user_id=api_key.user_id,
            org_id=api_key.org_id,
            occurred_at=datetime.utcnow()
        )
    
    @classmethod
    def create_auth_failure_event(cls, api_key, endpoint: str, ip_address: str = None,
                                 error_message: str = None):
        """Create authentication failure event (Rails pattern)"""
        return cls(
            event_type=ApiKeyEventType.FAILED_AUTH,
            status=ApiKeyEventStatus.FAILURE,
            endpoint=endpoint,
            method="N/A",
            ip_address=ip_address,
            error_message=error_message,
            api_key_id=api_key.id if api_key else None,
            user_id=api_key.user_id if api_key else None,
            org_id=api_key.org_id if api_key else None,
            occurred_at=datetime.utcnow()
        )