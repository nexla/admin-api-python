"""
Audit Log Models - Track system activity and security events.
Provides comprehensive audit logging capabilities.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base


class AuditLog(Base):
    __tablename__ = "audit_logs_old"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # User and request context
    user_id = Column(Integer, ForeignKey("users.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"))
    ip_address = Column(String(45))  # IPv6 compatible
    user_agent = Column(Text)
    
    # Action details
    action = Column(String(100), nullable=False)
    resource_type = Column(String(100))
    resource_id = Column(Integer)
    resource_name = Column(String(500))
    
    # HTTP details
    method = Column(String(10))
    endpoint = Column(String(500))
    
    # Change tracking
    old_values = Column(JSON)
    new_values = Column(JSON)
    changes = Column(JSON)
    details = Column(JSON)
    
    # Risk assessment
    risk_level = Column(String(20), default="low")
    
    # Timestamp
    timestamp = Column(DateTime, server_default=func.now())
    
    # Relationships
    user = relationship("User")
    org = relationship("Org")


class SecurityEvent(Base):
    __tablename__ = "security_events"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Event classification
    event_type = Column(String(100), nullable=False)
    severity = Column(String(20), nullable=False)  # info, warning, error, critical
    category = Column(String(50), nullable=False)
    status = Column(String(20), default="open")
    
    # Event details
    title = Column(String(500), nullable=False)
    description = Column(Text)
    details = Column(JSON)
    
    # Context
    user_id = Column(Integer, ForeignKey("users.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"))
    ip_address = Column(String(45))
    resource_type = Column(String(100))
    resource_id = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    resolved_at = Column(DateTime)
    
    # Relationships
    user = relationship("User")
    org = relationship("Org")


class SystemLog(Base):
    __tablename__ = "system_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Log level and source
    level = Column(String(20), nullable=False)  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    component = Column(String(100), nullable=False)
    message = Column(Text, nullable=False)
    
    # Context
    user_id = Column(Integer, ForeignKey("users.id"))
    session_id = Column(String(255))
    request_id = Column(String(255))
    
    # Additional data
    details = Column(JSON)
    exception_type = Column(String(255))
    stack_trace = Column(Text)
    
    # Performance metrics
    execution_time_ms = Column(Integer)
    memory_usage_mb = Column(Integer)
    
    # Timestamp
    timestamp = Column(DateTime, server_default=func.now())
    
    # Relationships
    user = relationship("User")


# Legacy alias for Rails compatibility  
AuditEntry = AuditLog