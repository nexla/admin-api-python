from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Enum as SQLEnum
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class ResourceType(str, Enum):
    USER = "user"
    ORG = "org"
    PROJECT = "project"
    DATA_SOURCE = "data_source"
    DATA_SET = "data_set"
    DATA_SINK = "data_sink"
    FLOW = "flow"
    API_KEY = "api_key"
    BILLING = "billing"

class ActionType(str, Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"
    MANAGE = "manage"
    ADMIN = "admin"

class AccessDecision(str, Enum):
    ALLOW = "allow"
    DENY = "deny"

class RoleType(str, Enum):
    SYSTEM = "system"
    ORG = "org"
    PROJECT = "project"
    CUSTOM = "custom"

class SecurityRole(Base):
    __tablename__ = "security_roles"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    display_name = Column(String(255))
    description = Column(Text)
    
    # Role configuration
    role_type = Column(String(20), nullable=False, default=RoleType.CUSTOM)
    is_system_role = Column(Boolean, default=False)
    is_default = Column(Boolean, default=False)
    
    # Permissions
    permissions = Column(JSON, default=list)
    restrictions = Column(JSON, default=dict)
    
    # Hierarchy
    parent_role_id = Column(Integer, ForeignKey("security_roles.id"))
    level = Column(Integer, default=0)
    
    # Status
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    parent_role = relationship("SecurityRole", remote_side=[id])
    child_roles = relationship("SecurityRole")
    org = relationship("Org")
    creator = relationship("User")
    role_assignments = relationship("RoleAssignment", back_populates="role")
    policy_bindings = relationship("PolicyBinding", back_populates="role")

class RoleAssignment(Base):
    __tablename__ = "role_assignments"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Assignment scope
    resource_type = Column(String(50))
    resource_id = Column(Integer)
    
    # Assignment details
    granted_at = Column(DateTime, nullable=False, default=func.now())
    expires_at = Column(DateTime)
    
    # Conditions
    conditions = Column(JSON, default=dict)
    
    # Foreign keys
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    role_id = Column(Integer, ForeignKey("security_roles.id"), nullable=False)
    granted_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id])
    role = relationship("SecurityRole", back_populates="role_assignments")
    granter = relationship("User", foreign_keys=[granted_by])

class SecurityPolicy(Base):
    __tablename__ = "security_policies"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Policy content
    policy_document = Column(JSON, nullable=False)
    version = Column(String(10), default="1.0")
    
    # Policy configuration
    effect = Column(String(10), nullable=False, default=AccessDecision.ALLOW)
    priority = Column(Integer, default=100)
    
    # Status
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    policy_bindings = relationship("PolicyBinding", back_populates="policy")

class PolicyBinding(Base):
    __tablename__ = "policy_bindings"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Binding configuration
    binding_type = Column(String(50), default="role")  # role, user, group
    conditions = Column(JSON, default=dict)
    
    # Status
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    policy_id = Column(Integer, ForeignKey("security_policies.id"), nullable=False)
    role_id = Column(Integer, ForeignKey("security_roles.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    policy = relationship("SecurityPolicy", back_populates="policy_bindings")
    role = relationship("SecurityRole", back_populates="policy_bindings")
    user = relationship("User")

class AccessControlEntry(Base):
    __tablename__ = "access_control_entries"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Principal (who)
    principal_type = Column(String(20), nullable=False)  # user, role, group
    principal_id = Column(Integer, nullable=False)
    
    # Resource (what)
    resource_type = Column(String(50), nullable=False)
    resource_id = Column(Integer)
    resource_path = Column(String(500))
    
    # Permission (action)
    action = Column(String(50), nullable=False)
    effect = Column(String(10), nullable=False, default=AccessDecision.ALLOW)
    
    # Conditions
    conditions = Column(JSON, default=dict)
    
    # Metadata
    granted_at = Column(DateTime, nullable=False, default=func.now())
    expires_at = Column(DateTime)
    
    # Foreign keys
    granted_by = Column(Integer, ForeignKey("users.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"))
    
    # Relationships
    granter = relationship("User")
    org = relationship("Org")

class SecurityAuditLog(Base):
    __tablename__ = "security_audit_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Event details
    event_type = Column(String(50), nullable=False)
    action = Column(String(100), nullable=False)
    result = Column(String(20), nullable=False)  # success, failure, denied
    
    # Principal information
    user_id = Column(Integer, ForeignKey("users.id"))
    user_email = Column(String(255))
    user_ip = Column(String(45))
    user_agent = Column(String(500))
    
    # Resource information
    resource_type = Column(String(50))
    resource_id = Column(Integer)
    resource_path = Column(String(500))
    
    # Request details
    request_id = Column(String(100))
    session_id = Column(String(100))
    method = Column(String(10))
    endpoint = Column(String(500))
    
    # Additional context
    details = Column(JSON)
    risk_score = Column(Integer, default=0)
    
    # Timestamps
    timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    
    # Relationships
    user = relationship("User")
    org = relationship("Org")

class SecurityRule(Base):
    __tablename__ = "security_rules"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Rule configuration
    rule_type = Column(String(50), nullable=False)  # rate_limit, ip_whitelist, geo_restriction
    conditions = Column(JSON, nullable=False)
    actions = Column(JSON, nullable=False)
    
    # Thresholds and limits
    threshold_value = Column(Integer)
    time_window = Column(Integer)  # seconds
    
    # Status
    enabled = Column(Boolean, default=True)
    priority = Column(Integer, default=100)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    last_triggered = Column(DateTime)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    rule_violations = relationship("SecurityRuleViolation", back_populates="rule")

class SecurityRuleViolation(Base):
    __tablename__ = "security_rule_violations"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Violation details
    violation_type = Column(String(50), nullable=False)
    severity = Column(String(20), default="medium")
    description = Column(Text)
    
    # Context
    user_id = Column(Integer, ForeignKey("users.id"))
    user_ip = Column(String(45))
    user_agent = Column(String(500))
    request_path = Column(String(500))
    request_data = Column(JSON)
    
    # Actions taken
    action_taken = Column(String(100))
    blocked = Column(Boolean, default=False)
    
    # Timestamps
    occurred_at = Column(DateTime, nullable=False, default=func.now())
    resolved_at = Column(DateTime)
    
    # Foreign keys
    rule_id = Column(Integer, ForeignKey("security_rules.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"))
    
    # Relationships
    rule = relationship("SecurityRule", back_populates="rule_violations")
    user = relationship("User")
    org = relationship("Org")

class ThreatIntelligence(Base):
    __tablename__ = "threat_intelligence"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Threat information
    threat_type = Column(String(50), nullable=False)
    indicator_type = Column(String(50), nullable=False)  # ip, domain, hash, email
    indicator_value = Column(String(500), nullable=False, index=True)
    
    # Threat details
    severity = Column(String(20), default="medium")
    confidence = Column(Integer, default=50)  # 0-100
    description = Column(Text)
    
    # Source information
    source = Column(String(100))
    source_confidence = Column(Integer, default=50)
    
    # Status
    active = Column(Boolean, default=True)
    
    # Timestamps
    first_seen = Column(DateTime, nullable=False, default=func.now())
    last_seen = Column(DateTime, nullable=False, default=func.now())
    expires_at = Column(DateTime)
    
    # Additional metadata
    tags = Column(JSON, default=list)
    security_metadata = Column(JSON, default=dict)

class SecurityIncident(Base):
    __tablename__ = "security_incidents"
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Incident classification
    incident_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)
    status = Column(String(20), default="open")
    
    # Affected resources
    affected_users = Column(JSON, default=list)
    affected_resources = Column(JSON, default=list)
    
    # Investigation details
    assigned_to = Column(Integer, ForeignKey("users.id"))
    investigation_notes = Column(Text)
    remediation_actions = Column(JSON, default=list)
    
    # Timeline
    detected_at = Column(DateTime, nullable=False, default=func.now())
    reported_at = Column(DateTime)
    resolved_at = Column(DateTime)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    reported_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    reporter = relationship("User", foreign_keys=[reported_by])
    assignee = relationship("User", foreign_keys=[assigned_to])

class DataClassification(Base):
    __tablename__ = "data_classifications"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    level = Column(Integer, nullable=False)  # 1=public, 2=internal, 3=confidential, 4=restricted
    
    # Classification rules
    detection_rules = Column(JSON, default=list)
    handling_requirements = Column(JSON, default=dict)
    retention_policy = Column(JSON, default=dict)
    
    # Access controls
    default_permissions = Column(JSON, default=dict)
    encryption_required = Column(Boolean, default=False)
    
    # Compliance
    compliance_frameworks = Column(JSON, default=list)
    regulatory_requirements = Column(JSON, default=dict)
    
    # Status
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")