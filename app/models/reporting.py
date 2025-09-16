from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, BigInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class ReportType(str, Enum):
    OPERATIONAL = "operational"
    FINANCIAL = "financial"
    PERFORMANCE = "performance"
    COMPLIANCE = "compliance"
    CUSTOM = "custom"

class ReportStatus(str, Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class VisualizationType(str, Enum):
    BAR_CHART = "bar_chart"
    LINE_CHART = "line_chart"
    PIE_CHART = "pie_chart"
    SCATTER_PLOT = "scatter_plot"
    HEATMAP = "heatmap"
    TABLE = "table"
    METRIC = "metric"
    GAUGE = "gauge"

class DashboardType(str, Enum):
    EXECUTIVE = "executive"
    OPERATIONAL = "operational"
    ANALYTICAL = "analytical"
    MONITORING = "monitoring"
    CUSTOM = "custom"

class Report(Base):
    __tablename__ = "reports"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Report configuration
    report_type = Column(String(50), nullable=False)
    data_sources = Column(JSON, nullable=False)
    query_config = Column(JSON, nullable=False)
    
    # Visualization settings
    visualization_config = Column(JSON)
    layout_config = Column(JSON)
    
    # Parameters and filters
    parameters = Column(JSON, default=dict)
    default_filters = Column(JSON, default=dict)
    
    # Scheduling
    schedule_config = Column(JSON)
    auto_refresh_interval = Column(Integer)  # minutes
    
    # Output configuration
    output_formats = Column(JSON, default=list)  # pdf, excel, csv, json
    email_config = Column(JSON)
    
    # Access control
    is_public = Column(Boolean, default=False)
    allowed_roles = Column(JSON, default=list)
    
    # Status
    status = Column(String(20), default=ReportStatus.DRAFT)
    last_run_at = Column(DateTime)
    next_run_at = Column(DateTime)
    
    # Cache settings
    cache_ttl_minutes = Column(Integer, default=60)
    cached_result = Column(JSON)
    cached_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    project = relationship("Project")
    creator = relationship("User")
    executions = relationship("ReportExecution", back_populates="report")
    subscriptions = relationship("ReportSubscription", back_populates="report")

class ReportExecution(Base):
    __tablename__ = "report_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Execution details
    status = Column(String(20), nullable=False, default=ReportStatus.RUNNING)
    trigger_type = Column(String(50))  # manual, scheduled, api
    
    # Parameters used
    parameters = Column(JSON)
    filters = Column(JSON)
    
    # Execution timing
    started_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Results
    result_data = Column(JSON)
    result_metadata = Column(JSON)
    output_files = Column(JSON)  # List of generated files
    
    # Performance metrics
    rows_processed = Column(BigInteger)
    data_size_bytes = Column(BigInteger)
    
    # Error handling
    error_message = Column(Text)
    error_details = Column(JSON)
    
    # Foreign keys
    report_id = Column(Integer, ForeignKey("reports.id"), nullable=False)
    triggered_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    report = relationship("Report", back_populates="executions")
    trigger_user = relationship("User")

class Dashboard(Base):
    __tablename__ = "dashboards"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Dashboard configuration
    dashboard_type = Column(String(50), nullable=False)
    layout_config = Column(JSON, nullable=False)
    theme_config = Column(JSON)
    
    # Widgets and components
    widgets = Column(JSON, nullable=False)
    filters = Column(JSON, default=dict)
    
    # Display settings
    auto_refresh_interval = Column(Integer, default=300)  # seconds
    time_range_config = Column(JSON)
    
    # Access control
    is_public = Column(Boolean, default=False)
    allowed_roles = Column(JSON, default=list)
    
    # Dashboard metadata
    tags = Column(JSON, default=list)
    category = Column(String(100))
    
    # Usage analytics
    view_count = Column(Integer, default=0)
    last_viewed_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    project = relationship("Project")
    creator = relationship("User")
    widgets_rel = relationship("Widget", back_populates="dashboard")
    shares = relationship("DashboardShare", back_populates="dashboard")

class Widget(Base):
    __tablename__ = "widgets"
    
    id = Column(Integer, primary_key=True, index=True)
    widget_id = Column(String(100), nullable=False, index=True)  # Unique within dashboard
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Widget configuration
    widget_type = Column(String(50), nullable=False)
    visualization_type = Column(String(50), nullable=False)
    data_source_config = Column(JSON, nullable=False)
    
    # Query and data
    query_config = Column(JSON, nullable=False)
    transformation_config = Column(JSON)
    
    # Visual configuration
    visual_config = Column(JSON, nullable=False)
    position_config = Column(JSON, nullable=False)  # x, y, width, height
    
    # Behavior settings
    refresh_interval = Column(Integer)  # seconds
    auto_refresh = Column(Boolean, default=False)
    
    # Data caching
    cache_ttl_minutes = Column(Integer, default=30)
    cached_data = Column(JSON)
    cached_at = Column(DateTime)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    dashboard_id = Column(Integer, ForeignKey("dashboards.id"), nullable=False)
    report_id = Column(Integer, ForeignKey("reports.id"))
    
    # Relationships
    dashboard = relationship("Dashboard", back_populates="widgets_rel")
    report = relationship("Report")

class DashboardShare(Base):
    __tablename__ = "dashboard_shares"
    
    id = Column(Integer, primary_key=True, index=True)
    share_token = Column(String(100), nullable=False, unique=True, index=True)
    
    # Share configuration
    permission_level = Column(String(20), nullable=False)  # view, edit
    expires_at = Column(DateTime)
    password_protected = Column(Boolean, default=False)
    password_hash = Column(String(255))
    
    # Access restrictions
    allowed_domains = Column(JSON)
    max_views = Column(Integer)
    current_views = Column(Integer, default=0)
    
    # Tracking
    last_accessed_at = Column(DateTime)
    access_count = Column(Integer, default=0)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    dashboard_id = Column(Integer, ForeignKey("dashboards.id"), nullable=False)
    shared_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    shared_with = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    dashboard = relationship("Dashboard", back_populates="shares")
    sharer = relationship("User", foreign_keys=[shared_by])
    recipient = relationship("User", foreign_keys=[shared_with])

class ReportSubscription(Base):
    __tablename__ = "report_subscriptions"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Subscription configuration
    delivery_method = Column(String(50), nullable=False)  # email, webhook, file
    schedule_config = Column(JSON, nullable=False)
    
    # Delivery settings
    recipients = Column(JSON, nullable=False)
    subject_template = Column(String(500))
    message_template = Column(Text)
    
    # Output configuration
    output_format = Column(String(20), default="pdf")
    include_data = Column(Boolean, default=True)
    
    # Filters and parameters
    default_parameters = Column(JSON, default=dict)
    default_filters = Column(JSON, default=dict)
    
    # Status and tracking
    enabled = Column(Boolean, default=True)
    last_sent_at = Column(DateTime)
    next_send_at = Column(DateTime)
    send_count = Column(Integer, default=0)
    
    # Error tracking
    last_error = Column(Text)
    error_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    report_id = Column(Integer, ForeignKey("reports.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    report = relationship("Report", back_populates="subscriptions")
    creator = relationship("User")

class ReportTemplate(Base):
    __tablename__ = "report_templates"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    category = Column(String(100))
    
    # Template configuration
    template_config = Column(JSON, nullable=False)
    sample_data = Column(JSON)
    
    # Template metadata
    tags = Column(JSON, default=list)
    industry = Column(String(100))
    use_case = Column(String(200))
    
    # Usage tracking
    usage_count = Column(Integer, default=0)
    rating = Column(Float)
    
    # Template status
    is_public = Column(Boolean, default=False)
    is_featured = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")

class DataVisualization(Base):
    __tablename__ = "data_visualizations"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Visualization configuration
    visualization_type = Column(String(50), nullable=False)
    data_config = Column(JSON, nullable=False)
    visual_config = Column(JSON, nullable=False)
    
    # Data source
    data_source_type = Column(String(50), nullable=False)  # query, dataset, api
    data_source_config = Column(JSON, nullable=False)
    
    # Interactivity
    interactive_config = Column(JSON)
    drill_down_config = Column(JSON)
    filter_config = Column(JSON)
    
    # Export settings
    exportable = Column(Boolean, default=True)
    export_formats = Column(JSON, default=list)
    
    # Status
    published = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")

class ReportingMetric(Base):
    __tablename__ = "reporting_metrics"
    
    id = Column(BigInteger, primary_key=True, index=True)
    metric_name = Column(String(100), nullable=False, index=True)
    metric_value = Column(Float, nullable=False)
    
    # Metric metadata
    metric_type = Column(String(50))  # kpi, performance, usage
    unit = Column(String(20))
    category = Column(String(100))
    
    # Dimensions
    dimensions = Column(JSON, default=dict)
    
    # Time information
    timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    time_period = Column(String(20))  # hour, day, week, month
    
    # Aggregation info
    aggregation_type = Column(String(20))  # sum, avg, count, max, min
    source_count = Column(Integer)
    
    # Context
    context = Column(JSON, default=dict)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    report_id = Column(Integer, ForeignKey("reports.id"))
    dashboard_id = Column(Integer, ForeignKey("dashboards.id"))
    
    # Relationships
    org = relationship("Org")
    report = relationship("Report")
    dashboard = relationship("Dashboard")

class AlertRule(Base):
    __tablename__ = "alert_rules"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Rule configuration
    rule_type = Column(String(50), nullable=False)  # threshold, anomaly, trend
    condition_config = Column(JSON, nullable=False)
    
    # Data source
    data_source_config = Column(JSON, nullable=False)
    query_config = Column(JSON, nullable=False)
    
    # Threshold settings
    threshold_value = Column(Float)
    comparison_operator = Column(String(10))  # >, <, >=, <=, ==, !=
    
    # Alert settings
    severity = Column(String(20), default="medium")
    notification_config = Column(JSON, nullable=False)
    
    # Evaluation settings
    evaluation_interval = Column(Integer, default=300)  # seconds
    evaluation_window = Column(Integer, default=900)  # seconds
    
    # Status
    enabled = Column(Boolean, default=True)
    last_evaluated_at = Column(DateTime)
    last_triggered_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    dashboard_id = Column(Integer, ForeignKey("dashboards.id"))
    report_id = Column(Integer, ForeignKey("reports.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    dashboard = relationship("Dashboard")
    report = relationship("Report")
    creator = relationship("User")
    alert_instances = relationship("AlertInstance", back_populates="alert_rule")

class AlertInstance(Base):
    __tablename__ = "alert_instances"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Alert details
    severity = Column(String(20), nullable=False)
    status = Column(String(20), default="active")  # active, acknowledged, resolved
    
    # Trigger information
    triggered_value = Column(Float)
    trigger_data = Column(JSON)
    message = Column(Text)
    
    # Timing
    triggered_at = Column(DateTime, nullable=False, default=func.now())
    acknowledged_at = Column(DateTime)
    resolved_at = Column(DateTime)
    
    # Resolution
    resolution_reason = Column(String(500))
    auto_resolved = Column(Boolean, default=False)
    
    # Foreign keys
    alert_rule_id = Column(Integer, ForeignKey("alert_rules.id"), nullable=False)
    acknowledged_by = Column(Integer, ForeignKey("users.id"))
    resolved_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    alert_rule = relationship("AlertRule", back_populates="alert_instances")
    acknowledger = relationship("User", foreign_keys=[acknowledged_by])
    resolver = relationship("User", foreign_keys=[resolved_by])