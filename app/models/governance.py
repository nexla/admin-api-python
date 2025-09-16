from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, BigInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class ComplianceFramework(str, Enum):
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    SOC2 = "soc2"
    ISO27001 = "iso27001"
    PCI_DSS = "pci_dss"
    CUSTOM = "custom"

class PolicyType(str, Enum):
    DATA_RETENTION = "data_retention"
    DATA_ACCESS = "data_access"
    DATA_CLASSIFICATION = "data_classification"
    DATA_LINEAGE = "data_lineage"
    PRIVACY = "privacy"
    SECURITY = "security"
    QUALITY = "quality"

class AuditType(str, Enum):
    COMPLIANCE = "compliance"
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    POLICY_VIOLATION = "policy_violation"
    SYSTEM_EVENT = "system_event"

class ViolationType(str, Enum):
    DATA_RETENTION = "data_retention"
    UNAUTHORIZED_ACCESS = "unauthorized_access"
    DATA_QUALITY = "data_quality"
    POLICY_BREACH = "policy_breach"
    SECURITY = "security"

class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class DataGovernancePolicy(Base):
    __tablename__ = "data_governance_policies"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Policy configuration
    policy_type = Column(String(50), nullable=False)
    compliance_frameworks = Column(JSON, default=list)
    policy_document = Column(JSON, nullable=False)
    
    # Scope and applicability
    scope_config = Column(JSON, nullable=False)
    applies_to_orgs = Column(JSON, default=list)
    applies_to_projects = Column(JSON, default=list)
    applies_to_data_types = Column(JSON, default=list)
    
    # Enforcement settings
    enforcement_level = Column(String(20), default="warning")  # warning, blocking, audit
    auto_remediation = Column(Boolean, default=False)
    remediation_config = Column(JSON)
    
    # Monitoring and alerts
    monitoring_enabled = Column(Boolean, default=True)
    alert_config = Column(JSON)
    
    # Metadata
    version = Column(String(20), default="1.0")
    effective_date = Column(DateTime, nullable=False)
    expiry_date = Column(DateTime)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    approved_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User", foreign_keys=[created_by])
    approver = relationship("User", foreign_keys=[approved_by])
    violations = relationship("PolicyViolation", back_populates="policy")

class DataClassification(Base):
    __tablename__ = "data_classifications"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, index=True)
    level = Column(Integer, nullable=False)  # 1=public, 2=internal, 3=confidential, 4=restricted
    description = Column(Text)
    
    # Classification rules
    detection_rules = Column(JSON, default=list)
    field_patterns = Column(JSON, default=list)
    content_patterns = Column(JSON, default=list)
    
    # Handling requirements
    retention_requirements = Column(JSON)
    access_requirements = Column(JSON)
    processing_requirements = Column(JSON)
    
    # Security controls
    encryption_required = Column(Boolean, default=False)
    masking_required = Column(Boolean, default=False)
    audit_required = Column(Boolean, default=True)
    
    # Compliance mapping
    compliance_frameworks = Column(JSON, default=list)
    regulatory_requirements = Column(JSON)
    
    # Display configuration
    label_color = Column(String(7))  # Hex color
    icon = Column(String(50))
    
    # Status
    active = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    classified_data = relationship("DataClassificationResult", back_populates="classification")

class DataClassificationResult(Base):
    __tablename__ = "data_classification_results"
    
    id = Column(BigInteger, primary_key=True, index=True)
    
    # Resource information
    resource_type = Column(String(50), nullable=False)  # table, file, field, dataset
    resource_identifier = Column(String(500), nullable=False, index=True)
    resource_metadata = Column(JSON)
    
    # Classification results
    confidence_score = Column(Float, nullable=False)
    detection_method = Column(String(50))  # pattern, ml, manual
    detected_patterns = Column(JSON, default=list)
    
    # Classification context
    sample_data = Column(JSON)
    field_names = Column(JSON, default=list)
    data_statistics = Column(JSON)
    
    # Review and approval
    reviewed = Column(Boolean, default=False)
    reviewed_by = Column(Integer, ForeignKey("users.id"))
    reviewed_at = Column(DateTime)
    override_classification_id = Column(Integer, ForeignKey("data_classifications.id"))
    
    # Timestamps
    classified_at = Column(DateTime, nullable=False, default=func.now(), index=True)
    last_scanned_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    classification_id = Column(Integer, ForeignKey("data_classifications.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"))
    
    # Relationships
    classification = relationship("DataClassification", back_populates="classified_data")
    org = relationship("Org")
    reviewer = relationship("User")
    override_classification = relationship("DataClassification", foreign_keys=[override_classification_id])
    data_source = relationship("DataSource")

class DataLineage(Base):
    __tablename__ = "data_lineage"
    
    id = Column(Integer, primary_key=True, index=True)
    lineage_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Source information
    source_type = Column(String(50), nullable=False)
    source_identifier = Column(String(500), nullable=False)
    source_schema = Column(JSON)
    source_metadata = Column(JSON)
    
    # Target information
    target_type = Column(String(50), nullable=False)
    target_identifier = Column(String(500), nullable=False)
    target_schema = Column(JSON)
    target_metadata = Column(JSON)
    
    # Transformation details
    transformation_type = Column(String(50))
    transformation_logic = Column(Text)
    transformation_config = Column(JSON)
    
    # Lineage metadata
    lineage_level = Column(Integer, default=1)  # 1=direct, 2=indirect
    confidence_score = Column(Float, default=1.0)
    detection_method = Column(String(50))  # automated, manual, inferred
    
    # Impact analysis
    downstream_count = Column(Integer, default=0)
    upstream_count = Column(Integer, default=0)
    criticality_score = Column(Float)
    
    # Validation
    validated = Column(Boolean, default=False)
    validated_by = Column(Integer, ForeignKey("users.id"))
    validated_at = Column(DateTime)
    
    # Timestamps
    first_observed = Column(DateTime, nullable=False, default=func.now())
    last_observed = Column(DateTime, nullable=False, default=func.now())
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    pipeline = relationship("Pipeline")
    creator = relationship("User", foreign_keys=[created_by])
    validator = relationship("User", foreign_keys=[validated_by])

class PolicyViolation(Base):
    __tablename__ = "policy_violations"
    
    id = Column(Integer, primary_key=True, index=True)
    violation_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Violation details
    violation_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)
    description = Column(Text, nullable=False)
    
    # Resource information
    resource_type = Column(String(50), nullable=False)
    resource_identifier = Column(String(500), nullable=False)
    resource_metadata = Column(JSON)
    
    # Context
    user_id = Column(Integer, ForeignKey("users.id"))
    user_action = Column(String(100))
    violation_context = Column(JSON)
    
    # Detection
    detection_method = Column(String(50))  # automated, manual, audit
    detection_timestamp = Column(DateTime, nullable=False, default=func.now())
    
    # Resolution
    status = Column(String(20), default="open")  # open, investigating, resolved, dismissed
    resolution_action = Column(String(100))
    resolution_details = Column(Text)
    resolved_at = Column(DateTime)
    resolved_by = Column(Integer, ForeignKey("users.id"))
    
    # Risk assessment
    risk_level = Column(String(20), nullable=False)
    business_impact = Column(Text)
    compliance_impact = Column(Text)
    
    # Remediation
    auto_remediation_attempted = Column(Boolean, default=False)
    remediation_status = Column(String(20))
    remediation_details = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    policy_id = Column(Integer, ForeignKey("data_governance_policies.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    policy = relationship("DataGovernancePolicy", back_populates="violations")
    org = relationship("Org")
    user = relationship("User", foreign_keys=[user_id])
    resolver = relationship("User", foreign_keys=[resolved_by])

class ComplianceReport(Base):
    __tablename__ = "compliance_reports"
    
    id = Column(Integer, primary_key=True, index=True)
    report_name = Column(String(255), nullable=False)
    
    # Report configuration
    compliance_framework = Column(String(50), nullable=False)
    report_type = Column(String(50), default="periodic")  # periodic, on_demand, incident
    scope_config = Column(JSON, nullable=False)
    
    # Report period
    period_start = Column(DateTime, nullable=False)
    period_end = Column(DateTime, nullable=False)
    
    # Report execution
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    # Report results
    findings = Column(JSON)
    compliance_score = Column(Float)
    violations_count = Column(Integer, default=0)
    recommendations = Column(JSON)
    
    # Report artifacts
    report_data = Column(JSON)
    report_file_path = Column(String(500))
    
    # Metadata
    version = Column(String(20), default="1.0")
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    generated_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    generator = relationship("User")

class DataRetentionRule(Base):
    __tablename__ = "data_retention_rules"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Rule configuration
    data_type = Column(String(50), nullable=False)
    retention_period_days = Column(Integer, nullable=False)
    retention_criteria = Column(JSON)
    
    # Scope
    applies_to_sources = Column(JSON, default=list)
    applies_to_datasets = Column(JSON, default=list)
    data_filters = Column(JSON)
    
    # Actions
    retention_action = Column(String(50), default="delete")  # delete, archive, anonymize
    archive_location = Column(String(500))
    anonymization_config = Column(JSON)
    
    # Legal and compliance
    legal_basis = Column(String(500))
    compliance_frameworks = Column(JSON, default=list)
    
    # Execution
    auto_execute = Column(Boolean, default=False)
    execution_schedule = Column(JSON)
    last_executed_at = Column(DateTime)
    next_execution_at = Column(DateTime)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    executions = relationship("RetentionExecution", back_populates="retention_rule")

class RetentionExecution(Base):
    __tablename__ = "retention_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Execution details
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    trigger_type = Column(String(50))  # scheduled, manual, policy
    
    # Execution scope
    data_scope = Column(JSON)
    execution_criteria = Column(JSON)
    
    # Results
    records_processed = Column(BigInteger, default=0)
    records_deleted = Column(BigInteger, default=0)
    records_archived = Column(BigInteger, default=0)
    records_anonymized = Column(BigInteger, default=0)
    
    # Performance
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Error handling
    error_message = Column(Text)
    errors_count = Column(Integer, default=0)
    
    # Audit trail
    execution_log = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    retention_rule_id = Column(Integer, ForeignKey("data_retention_rules.id"), nullable=False)
    triggered_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    retention_rule = relationship("DataRetentionRule", back_populates="executions")
    trigger_user = relationship("User")

class DataPrivacyRequest(Base):
    __tablename__ = "data_privacy_requests"
    
    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Request details
    request_type = Column(String(50), nullable=False)  # access, deletion, portability, rectification
    subject_type = Column(String(50), default="data_subject")  # data_subject, customer, user
    
    # Subject information
    subject_identifier = Column(String(255), nullable=False)
    subject_email = Column(String(255))
    subject_metadata = Column(JSON)
    
    # Request specifics
    data_categories = Column(JSON, default=list)
    specific_data = Column(JSON)
    reason = Column(Text)
    
    # Legal basis
    legal_basis = Column(String(500))
    compliance_framework = Column(String(50))
    
    # Processing
    status = Column(String(20), default="received")  # received, processing, completed, rejected
    priority = Column(String(20), default="normal")  # low, normal, high, urgent
    
    # Verification
    verification_required = Column(Boolean, default=True)
    verification_status = Column(String(20), default="pending")
    verification_method = Column(String(50))
    verified_at = Column(DateTime)
    verified_by = Column(Integer, ForeignKey("users.id"))
    
    # Deadlines
    received_at = Column(DateTime, nullable=False, default=func.now())
    due_date = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    
    # Processing details
    assigned_to = Column(Integer, ForeignKey("users.id"))
    processing_notes = Column(Text)
    data_found = Column(JSON)
    actions_taken = Column(JSON)
    
    # Response
    response_method = Column(String(50))  # email, postal, portal
    response_data = Column(JSON)
    response_sent_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    verifier = relationship("User", foreign_keys=[verified_by])
    assignee = relationship("User", foreign_keys=[assigned_to])

class DataQualityRule(Base):
    __tablename__ = "data_quality_rules"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Rule configuration
    rule_type = Column(String(50), nullable=False)  # completeness, accuracy, consistency, validity
    rule_definition = Column(JSON, nullable=False)
    
    # Scope
    data_source_id = Column(Integer, ForeignKey("data_sources.id"))
    dataset_id = Column(Integer, ForeignKey("data_sets.id"))
    table_name = Column(String(255))
    column_name = Column(String(255))
    
    # Thresholds
    warning_threshold = Column(Float)
    error_threshold = Column(Float)
    
    # Execution
    execution_schedule = Column(JSON)
    auto_remediation = Column(Boolean, default=False)
    remediation_config = Column(JSON)
    
    # Monitoring
    enabled = Column(Boolean, default=True)
    last_executed_at = Column(DateTime)
    next_execution_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    data_source = relationship("DataSource")
    dataset = relationship("DataSet")
    quality_results = relationship("DataQualityResult", back_populates="quality_rule")

class DataQualityResult(Base):
    __tablename__ = "data_quality_results"
    
    id = Column(BigInteger, primary_key=True, index=True)
    
    # Execution details
    execution_timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    execution_id = Column(String(100), nullable=False, index=True)
    
    # Results
    status = Column(String(20), nullable=False)  # passed, warning, failed
    score = Column(Float, nullable=False)  # 0.0 to 1.0
    
    # Metrics
    total_records = Column(BigInteger)
    valid_records = Column(BigInteger)
    invalid_records = Column(BigInteger)
    
    # Details
    violations = Column(JSON, default=list)
    sample_violations = Column(JSON)
    statistics = Column(JSON)
    
    # Remediation
    remediation_attempted = Column(Boolean, default=False)
    remediation_success = Column(Boolean)
    remediation_details = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    quality_rule_id = Column(Integer, ForeignKey("data_quality_rules.id"), nullable=False)
    
    # Relationships
    quality_rule = relationship("DataQualityRule", back_populates="quality_results")

class GovernanceAuditLog(Base):
    __tablename__ = "governance_audit_logs"
    
    id = Column(BigInteger, primary_key=True, index=True)
    
    # Event details
    event_type = Column(String(50), nullable=False, index=True)
    action = Column(String(100), nullable=False)
    result = Column(String(20), nullable=False)
    
    # Actor information
    user_id = Column(Integer, ForeignKey("users.id"))
    user_email = Column(String(255))
    user_role = Column(String(100))
    
    # Resource information
    resource_type = Column(String(50))
    resource_id = Column(String(255))
    resource_name = Column(String(500))
    
    # Context
    policy_id = Column(Integer, ForeignKey("data_governance_policies.id"))
    compliance_framework = Column(String(50))
    
    # Request details
    request_id = Column(String(100))
    session_id = Column(String(100))
    client_ip = Column(String(45))
    user_agent = Column(String(500))
    
    # Data details
    data_classification = Column(String(50))
    data_sensitivity = Column(String(20))
    data_volume = Column(BigInteger)
    
    # Additional context
    details = Column(JSON)
    before_state = Column(JSON)
    after_state = Column(JSON)
    
    # Risk assessment
    risk_score = Column(Float)
    compliance_impact = Column(String(20))
    
    # Timestamps
    timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    user = relationship("User")
    policy = relationship("DataGovernancePolicy")