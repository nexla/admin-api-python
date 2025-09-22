from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, BigInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class PipelineStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    FAILED = "failed"

class ExecutionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"

class TriggerType(str, Enum):
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT = "event"
    WEBHOOK = "webhook"
    DATA_CHANGE = "data_change"

class NodeType(str, Enum):
    EXTRACTOR = "extractor"
    TRANSFORMER = "transformer"
    LOADER = "loader"
    VALIDATOR = "validator"
    AGGREGATOR = "aggregator"
    SPLITTER = "splitter"
    JOINER = "joiner"
    CONDITIONAL = "conditional"

class Pipeline(Base):
    __tablename__ = "pipelines"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Pipeline configuration
    pipeline_config = Column(JSON, nullable=False)
    version = Column(String(20), default="1.0")
    status = Column(String(20), nullable=False, default=PipelineStatus.DRAFT)
    
    # Scheduling
    schedule_config = Column(JSON)
    trigger_type = Column(String(20), default=TriggerType.MANUAL)
    
    # Execution settings
    max_concurrent_executions = Column(Integer, default=1)
    timeout_seconds = Column(Integer, default=3600)
    retry_config = Column(JSON)
    
    # Performance settings
    parallelism = Column(Integer, default=1)
    resource_requirements = Column(JSON)
    
    # Monitoring
    sla_config = Column(JSON)
    alert_config = Column(JSON)
    
    # Status tracking
    enabled = Column(Boolean, default=True)
    last_execution_id = Column(Integer)
    last_executed_at = Column(DateTime)
    next_execution_at = Column(DateTime)
    
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
    nodes = relationship("PipelineNode", back_populates="pipeline", cascade="all, delete-orphan")
    edges = relationship("PipelineEdge", back_populates="pipeline", cascade="all, delete-orphan")
    executions = relationship("PipelineExecution", back_populates="pipeline")
    dependencies = relationship("PipelineDependency", foreign_keys="PipelineDependency.pipeline_id")

class PipelineNode(Base):
    __tablename__ = "pipeline_nodes"
    
    id = Column(Integer, primary_key=True, index=True)
    node_id = Column(String(100), nullable=False, index=True)  # Unique within pipeline
    name = Column(String(255), nullable=False)
    node_type = Column(String(50), nullable=False)
    
    # Node configuration
    config = Column(JSON, nullable=False)
    input_schema = Column(JSON)
    output_schema = Column(JSON)
    
    # Execution settings
    timeout_seconds = Column(Integer, default=300)
    retry_attempts = Column(Integer, default=3)
    retry_delay_seconds = Column(Integer, default=60)
    
    # Resource requirements
    cpu_request = Column(Float)
    memory_request = Column(Integer)  # MB
    cpu_limit = Column(Float)
    memory_limit = Column(Integer)  # MB
    
    # Position in UI
    position_x = Column(Float)
    position_y = Column(Float)
    
    # Status
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    
    # Relationships
    pipeline = relationship("Pipeline", back_populates="nodes")
    input_edges = relationship("PipelineEdge", foreign_keys="PipelineEdge.target_node_id")
    output_edges = relationship("PipelineEdge", foreign_keys="PipelineEdge.source_node_id")
    executions = relationship("NodeExecution", back_populates="node")

class PipelineEdge(Base):
    __tablename__ = "pipeline_edges"
    
    id = Column(Integer, primary_key=True, index=True)
    edge_id = Column(String(100), nullable=False, index=True)  # Unique within pipeline
    
    # Connection configuration
    source_port = Column(String(100), default="output")
    target_port = Column(String(100), default="input")
    
    # Data transformation
    transformation_config = Column(JSON)
    condition_config = Column(JSON)  # Conditional routing
    
    # Status
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    source_node_id = Column(Integer, ForeignKey("pipeline_nodes.id"), nullable=False)
    target_node_id = Column(Integer, ForeignKey("pipeline_nodes.id"), nullable=False)
    
    # Relationships
    pipeline = relationship("Pipeline", back_populates="edges")
    source_node = relationship("PipelineNode", foreign_keys=[source_node_id])
    target_node = relationship("PipelineNode", foreign_keys=[target_node_id])

class PipelineExecution(Base):
    __tablename__ = "pipeline_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Execution details
    status = Column(String(20), nullable=False, default=ExecutionStatus.PENDING)
    trigger_type = Column(String(20), nullable=False)
    trigger_data = Column(JSON)
    
    # Timing
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Resource usage
    cpu_usage = Column(JSON)
    memory_usage = Column(JSON)
    
    # Results
    input_data = Column(JSON)
    output_data = Column(JSON)
    metrics = Column(JSON)
    logs = Column(Text)
    error_message = Column(Text)
    
    # Configuration at execution time
    pipeline_config_snapshot = Column(JSON)
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    triggered_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    pipeline = relationship("Pipeline", back_populates="executions")
    trigger_user = relationship("User")
    node_executions = relationship("NodeExecution", back_populates="execution")

class NodeExecution(Base):
    __tablename__ = "node_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(100), nullable=False, index=True)
    
    # Execution details
    status = Column(String(20), nullable=False, default=ExecutionStatus.PENDING)
    attempt_number = Column(Integer, default=1)
    
    # Timing
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Resource usage
    cpu_usage = Column(Float)
    memory_usage = Column(Integer)  # MB
    
    # Data processing
    input_data = Column(JSON)
    output_data = Column(JSON)
    rows_processed = Column(BigInteger)
    bytes_processed = Column(BigInteger)
    
    # Results
    metrics = Column(JSON)
    logs = Column(Text)
    error_message = Column(Text)
    
    # Foreign keys
    pipeline_execution_id = Column(Integer, ForeignKey("pipeline_executions.id"), nullable=False)
    node_id = Column(Integer, ForeignKey("pipeline_nodes.id"), nullable=False)
    
    # Relationships
    execution = relationship("PipelineExecution", back_populates="node_executions")
    node = relationship("PipelineNode", back_populates="executions")

class PipelineDependency(Base):
    __tablename__ = "pipeline_dependencies"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Dependency configuration
    dependency_type = Column(String(50), nullable=False)  # pipeline, data, external
    condition_config = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    depends_on_pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    depends_on_data_source_id = Column(Integer, ForeignKey("data_sources.id"))
    
    # Relationships
    pipeline = relationship("Pipeline", foreign_keys=[pipeline_id])
    depends_on_pipeline = relationship("Pipeline", foreign_keys=[depends_on_pipeline_id])
    depends_on_data_source = relationship("DataSource")

class PipelineTemplate(Base):
    __tablename__ = "pipeline_templates"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    category = Column(String(100))
    
    # Template configuration
    template_config = Column(JSON, nullable=False)
    parameters = Column(JSON)
    
    # Metadata
    tags = Column(JSON, default=list)
    use_count = Column(Integer, default=0)
    rating = Column(Float)
    
    # Status
    is_public = Column(Boolean, default=False)
    is_featured = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")

class PipelineSchedule(Base):
    __tablename__ = "pipeline_schedules"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    
    # Schedule configuration
    cron_expression = Column(String(100))
    timezone = Column(String(50), default="UTC")
    
    # Execution window
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    
    # Schedule parameters
    parameters = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    last_triggered_at = Column(DateTime)
    next_trigger_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    pipeline = relationship("Pipeline")
    creator = relationship("User")

class PipelineAlert(Base):
    __tablename__ = "pipeline_alerts"
    
    id = Column(Integer, primary_key=True, index=True)
    alert_type = Column(String(50), nullable=False)
    
    # Alert configuration
    condition_config = Column(JSON, nullable=False)
    notification_config = Column(JSON, nullable=False)
    
    # Alert details
    message_template = Column(Text)
    severity = Column(String(20), default="medium")
    
    # Status
    enabled = Column(Boolean, default=True)
    last_triggered_at = Column(DateTime)
    trigger_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    pipeline = relationship("Pipeline")
    creator = relationship("User")

class DataLineage(Base):
    __tablename__ = "orchestration_data_lineage"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Source information
    source_type = Column(String(50), nullable=False)  # table, file, api
    source_identifier = Column(String(500), nullable=False)
    source_schema = Column(JSON)
    
    # Target information
    target_type = Column(String(50), nullable=False)
    target_identifier = Column(String(500), nullable=False)
    target_schema = Column(JSON)
    
    # Transformation details
    transformation_type = Column(String(50))
    transformation_config = Column(JSON)
    
    # Lineage metadata
    lineage_level = Column(Integer, default=1)  # Direct = 1, Indirect = 2+
    confidence_score = Column(Float, default=1.0)
    
    # Timestamps
    first_observed = Column(DateTime, nullable=False, default=func.now())
    last_observed = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    pipeline = relationship("Pipeline")
    org = relationship("Org")

class PipelineMetric(Base):
    __tablename__ = "pipeline_metrics"
    
    id = Column(BigInteger, primary_key=True, index=True)
    metric_name = Column(String(100), nullable=False, index=True)
    metric_value = Column(Float, nullable=False)
    
    # Metric metadata
    metric_type = Column(String(50))  # counter, gauge, histogram
    tags = Column(JSON, default=dict)
    
    # Aggregation window
    aggregation_window = Column(String(20))  # 1m, 5m, 1h, 1d
    
    # Timestamps
    timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    
    # Foreign keys
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    pipeline_execution_id = Column(Integer, ForeignKey("pipeline_executions.id"))
    node_id = Column(Integer, ForeignKey("pipeline_nodes.id"))
    
    # Relationships
    pipeline = relationship("Pipeline")
    execution = relationship("PipelineExecution")
    node = relationship("PipelineNode")