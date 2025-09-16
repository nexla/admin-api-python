from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, BigInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class MetricType(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    TIMER = "timer"

class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertStatus(str, Enum):
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"

class MetricDefinition(Base):
    __tablename__ = "metric_definitions"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)
    metric_type = Column(String(50), nullable=False)
    unit = Column(String(50))
    labels = Column(JSON)
    
    # Configuration
    collection_interval = Column(Integer, default=60)
    retention_days = Column(Integer, default=90)
    aggregation_rules = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    
    # Relationships
    org = relationship("Org")
    metric_values = relationship("MetricValue", back_populates="definition")
    alert_rules = relationship("AlertRule", back_populates="metric_definition")

class MetricValue(Base):
    __tablename__ = "metric_values"
    
    id = Column(BigInteger, primary_key=True, index=True)
    value = Column(Float, nullable=False)
    labels = Column(JSON)
    timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    
    # Additional fields for different metric types
    sample_count = Column(Integer)
    sample_sum = Column(Float)
    quantiles = Column(JSON)
    
    # Foreign keys
    metric_definition_id = Column(Integer, ForeignKey("metric_definitions.id"))
    
    # Relationships
    definition = relationship("MetricDefinition", back_populates="metric_values")

class AlertRule(Base):
    __tablename__ = "alert_rules"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Rule configuration
    condition = Column(String(1000), nullable=False)
    threshold = Column(Float)
    comparison_operator = Column(String(10))
    evaluation_window = Column(Integer, default=300)
    
    # Alert configuration
    severity = Column(String(20), nullable=False)
    notification_channels = Column(JSON)
    escalation_rules = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    last_evaluation = Column(DateTime)
    last_triggered = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    metric_definition_id = Column(Integer, ForeignKey("metric_definitions.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"))
    
    # Relationships
    metric_definition = relationship("MetricDefinition", back_populates="alert_rules")
    org = relationship("Org")
    alert_instances = relationship("AlertInstance", back_populates="alert_rule")

class AlertInstance(Base):
    __tablename__ = "alert_instances"
    
    id = Column(Integer, primary_key=True, index=True)
    status = Column(String(20), nullable=False, default=AlertStatus.ACTIVE)
    severity = Column(String(20), nullable=False)
    
    # Alert details
    triggered_value = Column(Float)
    triggered_labels = Column(JSON)
    message = Column(Text)
    
    # Timing
    triggered_at = Column(DateTime, nullable=False, default=func.now())
    acknowledged_at = Column(DateTime)
    resolved_at = Column(DateTime)
    
    # Resolution
    resolution_reason = Column(String(500))
    auto_resolved = Column(Boolean, default=False)
    
    # Foreign keys
    alert_rule_id = Column(Integer, ForeignKey("alert_rules.id"))
    acknowledged_by = Column(Integer, ForeignKey("users.id"))
    resolved_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    alert_rule = relationship("AlertRule", back_populates="alert_instances")
    acknowledger = relationship("User", foreign_keys=[acknowledged_by])
    resolver = relationship("User", foreign_keys=[resolved_by])
    notifications = relationship("AlertNotification", back_populates="alert_instance")

class AlertNotification(Base):
    __tablename__ = "alert_notifications"
    
    id = Column(Integer, primary_key=True, index=True)
    channel = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False)
    
    # Notification details
    recipient = Column(String(255))
    subject = Column(String(500))
    message = Column(Text)
    
    # Delivery tracking
    sent_at = Column(DateTime)
    delivered_at = Column(DateTime)
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    
    # Foreign keys
    alert_instance_id = Column(Integer, ForeignKey("alert_instances.id"))
    
    # Relationships
    alert_instance = relationship("AlertInstance", back_populates="notifications")

class Dashboard(Base):
    __tablename__ = "dashboards"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Dashboard configuration
    layout = Column(JSON)
    widgets = Column(JSON)
    filters = Column(JSON)
    refresh_interval = Column(Integer, default=300)
    
    # Access control
    is_public = Column(Boolean, default=False)
    tags = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    dashboard_shares = relationship("DashboardShare", back_populates="dashboard")

class DashboardShare(Base):
    __tablename__ = "dashboard_shares"
    
    id = Column(Integer, primary_key=True, index=True)
    permission_level = Column(String(20), nullable=False)
    
    # Share configuration
    can_edit = Column(Boolean, default=False)
    can_share = Column(Boolean, default=False)
    expires_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    dashboard_id = Column(Integer, ForeignKey("dashboards.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    shared_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    dashboard = relationship("Dashboard", back_populates="dashboard_shares")
    user = relationship("User", foreign_keys=[user_id])
    sharer = relationship("User", foreign_keys=[shared_by])

class AnalyticsReport(Base):
    __tablename__ = "analytics_reports"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    report_type = Column(String(50), nullable=False)
    
    # Report configuration
    query = Column(Text)
    parameters = Column(JSON)
    schedule = Column(JSON)
    
    # Output configuration
    format = Column(String(20), default="json")
    delivery_channels = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    last_run = Column(DateTime)
    next_run = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    report_runs = relationship("AnalyticsReportRun", back_populates="report")

class AnalyticsReportRun(Base):
    __tablename__ = "analytics_report_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    status = Column(String(20), nullable=False)
    
    # Execution details
    started_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Results
    output_data = Column(JSON)
    output_file_path = Column(String(500))
    row_count = Column(Integer)
    error_message = Column(Text)
    
    # Foreign keys
    report_id = Column(Integer, ForeignKey("analytics_reports.id"))
    triggered_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    report = relationship("AnalyticsReport", back_populates="report_runs")
    trigger_user = relationship("User")