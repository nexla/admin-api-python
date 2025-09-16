"""
DataSource Model - Data ingestion and source management entity.
Handles data ingestion from external sources with comprehensive Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import uuid
import secrets
from ..database import Base


class DataSourceStatuses(PyEnum):
    """DataSource status enumeration"""
    INIT = "INIT"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"
    SUSPENDED = "SUSPENDED"
    ARCHIVED = "ARCHIVED"
    RATE_LIMITED = "RATE_LIMITED"
    MAINTENANCE = "MAINTENANCE"


class RuntimeStatuses(PyEnum):
    """Runtime status enumeration"""
    IDLE = "IDLE"
    PROCESSING = "PROCESSING"
    WAITING = "WAITING"
    THROTTLED = "THROTTLED"
    ERROR = "ERROR"


class IngestMethods(PyEnum):
    """Ingest method enumeration"""
    POLL = "POLL"
    API = "API"
    WEBHOOK = "WEBHOOK"
    STREAM = "STREAM"
    PUSH = "PUSH"
    MANUAL = "MANUAL"


class SourceFormats(PyEnum):
    """Source format enumeration"""
    JSON = "JSON"
    CSV = "CSV"
    TSV = "TSV"
    XML = "XML"
    PARQUET = "PARQUET"
    AVRO = "AVRO"
    YAML = "YAML"
    BINARY = "BINARY"
    UNKNOWN = "UNKNOWN"


class ConnectorTypes(PyEnum):
    """Connector type enumeration"""
    S3 = "s3"
    DATABASE = "database"
    API = "api"
    WEBHOOK = "webhook"
    FILE_UPLOAD = "file_upload"
    FTP = "ftp"
    SFTP = "sftp"
    KAFKA = "kafka"
    REDIS = "redis"
    AI_WEB_SERVER = "ai_web_server"
    API_WEB_SERVER = "api_web_server"
    REST_API = "rest_api"
    NEXSET_API = "nexset_api"
    CUSTOM = "custom"


class PipelineTypes(PyEnum):
    """Pipeline type enumeration"""
    BATCH = "batch"
    STREAMING = "streaming"
    REALTIME = "realtime"
    SCHEDULED = "scheduled"
    EVENT_DRIVEN = "event_driven"


class DataSource(Base):
    __tablename__ = "data_sources"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    status = Column(SQLEnum(DataSourceStatuses), default=DataSourceStatuses.INIT, nullable=False, index=True)
    runtime_status = Column(SQLEnum(RuntimeStatuses), default=RuntimeStatuses.IDLE, index=True)
    
    # Flow information
    flow_name = Column(String(255))
    flow_description = Column(Text)
    pipeline_type = Column(SQLEnum(PipelineTypes), default=PipelineTypes.BATCH)
    
    # Configuration
    source_config = Column(JSON)
    template_config = Column(JSON)
    runtime_config = Column(JSON)
    processing_config = Column(JSON)
    
    # Connection and ingestion
    connector_type = Column(SQLEnum(ConnectorTypes), default=ConnectorTypes.S3, nullable=False, index=True)
    ingest_method = Column(SQLEnum(IngestMethods), default=IngestMethods.POLL, index=True)
    source_format = Column(SQLEnum(SourceFormats), default=SourceFormats.JSON, index=True)
    poll_schedule = Column(String(255))
    
    # Execution and performance
    last_run_id = Column(String(255))
    last_execution_at = Column(DateTime, index=True)
    next_execution_at = Column(DateTime, index=True)
    execution_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    avg_execution_time_ms = Column(Float, default=0.0)
    total_execution_time_ms = Column(Float, default=0.0)
    
    # Run now functionality
    run_now_at = Column(DateTime)
    run_now_status = Column(String(255))
    run_now_count = Column(Integer, default=0)
    
    # Re-ingestion functionality
    reingest_at = Column(DateTime)
    reingest_status = Column(String(255))
    reingest_count = Column(Integer, default=0)
    
    # Data tracking
    records_ingested = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    last_record_count = Column(Integer, default=0)
    data_size_bytes = Column(Integer, default=0)
    
    # Rate limiting and throttling
    rate_limit_per_second = Column(Integer, default=100)
    throttle_delay_ms = Column(Integer, default=0)
    backoff_multiplier = Column(Float, default=1.5)
    max_retries = Column(Integer, default=3)
    
    # Flow settings
    managed = Column(Boolean, default=False)
    adaptive_flow = Column(Boolean, default=False)
    referenced_resources_enabled = Column(Boolean, default=True)
    auto_schema_detection = Column(Boolean, default=True)
    single_schema_detection = Column(Boolean, default=False)
    
    # Metadata and tags
    tags = Column(JSON)
    extra_metadata = Column(JSON)
    execution_history = Column(JSON)
    error_details = Column(JSON)
    
    # State flags
    is_system = Column(Boolean, default=False)
    is_template = Column(Boolean, default=False)
    is_auto_generated = Column(Boolean, default=False)
    force_refresh = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    last_activity_at = Column(DateTime)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    flow_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    data_credentials_group_id = Column(Integer, index=True)
    data_sink_id = Column(Integer, ForeignKey("data_sinks.id"), index=True)
    vendor_endpoint_id = Column(Integer, index=True)
    code_container_id = Column(Integer, index=True)
    copied_from_id = Column(Integer, ForeignKey("data_sources.id"), index=True)
    connector_id = Column(Integer, ForeignKey("connectors.id"), index=True)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    org = relationship("Org", foreign_keys=[org_id], back_populates="data_sources")
    flow_node = relationship("FlowNode", foreign_keys=[flow_node_id])
    origin_node = relationship("FlowNode", foreign_keys=[origin_node_id])
    data_credentials = relationship("DataCredentials", foreign_keys=[data_credentials_id])
    data_sink = relationship("DataSink", foreign_keys=[data_sink_id])
    copied_from = relationship("DataSource", remote_side="DataSource.id", foreign_keys=[copied_from_id])
    copied_data_sources = relationship("DataSource", remote_side="DataSource.copied_from_id")
    data_sets = relationship("DataSet", back_populates="data_source")
    connector = relationship("Connector", foreign_keys=[connector_id], back_populates="data_sources")
    
    # Rails business logic constants
    SCRIPT_DATA_CREDENTIALS_ID = 1
    DEFAULT_RUN_ID_COUNT = 5
    SINGLE_SCHEMA_KEY = "schema.detection.once"
    PIPELINE_TYPE_KEY = "pipeline.type"
    MAX_ERROR_COUNT = 100
    PERFORMANCE_THRESHOLD_MS = 5000
    RATE_LIMIT_THRESHOLD = 1000
    CACHE_TTL_SECONDS = 300
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self._execution_metrics = {}
        self._config_cache = {}
        self._validation_cache = {}
    
    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if data source is active (Rails pattern)"""
        return self.status == DataSourceStatuses.ACTIVE and not self.is_system
    
    def paused_(self) -> bool:
        """Check if data source is paused (Rails pattern)"""
        return self.status == DataSourceStatuses.PAUSED
    
    def init_(self) -> bool:
        """Check if data source is in init state (Rails pattern)"""
        return self.status == DataSourceStatuses.INIT
    
    def processing_(self) -> bool:
        """Check if data source is processing (Rails pattern)"""
        return (self.status == DataSourceStatuses.PROCESSING or 
                self.runtime_status == RuntimeStatuses.PROCESSING)
    
    def completed_(self) -> bool:
        """Check if data source processing is completed (Rails pattern)"""
        return self.status == DataSourceStatuses.COMPLETED
    
    def failed_(self) -> bool:
        """Check if data source has failed (Rails pattern)"""
        return self.status in [DataSourceStatuses.FAILED, DataSourceStatuses.ERROR]
    
    def stopped_(self) -> bool:
        """Check if data source is stopped (Rails pattern)"""
        return self.status == DataSourceStatuses.STOPPED
    
    def suspended_(self) -> bool:
        """Check if data source is suspended (Rails pattern)"""
        return self.status == DataSourceStatuses.SUSPENDED
    
    def archived_(self) -> bool:
        """Check if data source is archived (Rails pattern)"""
        return self.status == DataSourceStatuses.ARCHIVED
    
    def rate_limited_(self) -> bool:
        """Check if data source is rate limited (Rails pattern)"""
        return self.status == DataSourceStatuses.RATE_LIMITED
    
    def maintenance_(self) -> bool:
        """Check if data source is under maintenance (Rails pattern)"""
        return self.status == DataSourceStatuses.MAINTENANCE
    
    def healthy_(self) -> bool:
        """Check if data source is healthy (Rails pattern)"""
        return (self.active_() and 
                not self.rate_limited_() and 
                self.error_count < self.MAX_ERROR_COUNT and
                not self.maintenance_())
    
    def idle_(self) -> bool:
        """Check if data source is idle (Rails pattern)"""
        return self.runtime_status == RuntimeStatuses.IDLE
    
    def waiting_(self) -> bool:
        """Check if data source is waiting (Rails pattern)"""
        return self.runtime_status == RuntimeStatuses.WAITING
    
    def throttled_(self) -> bool:
        """Check if data source is throttled (Rails pattern)"""
        return self.runtime_status == RuntimeStatuses.THROTTLED
    
    def webhook_(self) -> bool:
        """Check if data source uses webhook connector (Rails pattern)"""
        return self.connector_type == ConnectorTypes.WEBHOOK
    
    def file_upload_(self) -> bool:
        """Check if data source uses file upload (Rails pattern)"""
        return self.connector_type == ConnectorTypes.FILE_UPLOAD
    
    def database_(self) -> bool:
        """Check if data source uses database connector (Rails pattern)"""
        return self.connector_type == ConnectorTypes.DATABASE
    
    def api_(self) -> bool:
        """Check if data source uses API connector (Rails pattern)"""
        return self.connector_type in [ConnectorTypes.API, ConnectorTypes.REST_API, ConnectorTypes.NEXSET_API]
    
    def streaming_(self) -> bool:
        """Check if data source uses streaming connector (Rails pattern)"""
        return self.connector_type in [ConnectorTypes.KAFKA, ConnectorTypes.REDIS] or \
               self.pipeline_type == PipelineTypes.STREAMING
    
    def ai_web_server_(self) -> bool:
        """Check if data source is AI web server (Rails pattern)"""
        return self.connector_type == ConnectorTypes.AI_WEB_SERVER
    
    def api_web_server_(self) -> bool:
        """Check if data source is API web server (Rails pattern)"""
        return self.connector_type == ConnectorTypes.API_WEB_SERVER
    
    def has_credentials_(self) -> bool:
        """Check if data source has credentials (Rails pattern)"""
        return self.data_credentials_id is not None
    
    def nexset_api_compatible_(self) -> bool:
        """Check if data source is nexset API compatible (Rails pattern)"""
        return self.connector_type in [ConnectorTypes.NEXSET_API, ConnectorTypes.API, ConnectorTypes.REST_API]
    
    def source_records_count_capped_(self) -> bool:
        """Check if source records count is capped (Rails pattern)"""
        return self.rate_limited_()
    
    def auto_generated_(self) -> bool:
        """Check if data source is auto generated (Rails pattern)"""
        return self.is_auto_generated is True
    
    def system_(self) -> bool:
        """Check if data source is system-managed (Rails pattern)"""
        return self.is_system is True
    
    def template_(self) -> bool:
        """Check if data source is a template (Rails pattern)"""
        return self.is_template is True
    
    def managed_(self) -> bool:
        """Check if data source is managed (Rails pattern)"""
        return self.managed is True
    
    def adaptive_(self) -> bool:
        """Check if data source uses adaptive flow (Rails pattern)"""
        return self.adaptive_flow is True
    
    def has_errors_(self) -> bool:
        """Check if data source has errors (Rails pattern)"""
        return self.error_count > 0
    
    def performance_issues_(self) -> bool:
        """Check if data source has performance issues (Rails pattern)"""
        return self.avg_execution_time_ms > self.PERFORMANCE_THRESHOLD_MS
    
    def needs_reingest_(self) -> bool:
        """Check if data source needs re-ingestion (Rails pattern)"""
        return self.reingest_at is not None and self.reingest_at <= datetime.now()
    
    def scheduled_for_run_(self) -> bool:
        """Check if data source is scheduled for immediate run (Rails pattern)"""
        return self.run_now_at is not None and self.run_now_at <= datetime.now()
    
    def can_execute_(self) -> bool:
        """Check if data source can execute (Rails pattern)"""
        return (self.active_() and 
                not self.maintenance_() and 
                not self.archived_() and
                self.has_credentials_())
    
    def can_be_paused_(self) -> bool:
        """Check if data source can be paused (Rails pattern)"""
        return self.active_() or self.processing_()
    
    def can_be_deleted_(self) -> bool:
        """Check if data source can be deleted (Rails pattern)"""
        return not self.system_() and not self.processing_()
    
    def recently_executed_(self, hours: int = 24) -> bool:
        """Check if data source was recently executed (Rails pattern)"""
        if not self.last_execution_at:
            return False
        
        threshold = datetime.now() - timedelta(hours=hours)
        return self.last_execution_at > threshold
    
    def batch_mode_(self) -> bool:
        """Check if data source uses batch pipeline (Rails pattern)"""
        return self.pipeline_type == PipelineTypes.BATCH
    
    def streaming_mode_(self) -> bool:
        """Check if data source uses streaming pipeline (Rails pattern)"""
        return self.pipeline_type == PipelineTypes.STREAMING
    
    def realtime_mode_(self) -> bool:
        """Check if data source uses realtime pipeline (Rails pattern)"""
        return self.pipeline_type == PipelineTypes.REALTIME
    
    def scheduled_mode_(self) -> bool:
        """Check if data source uses scheduled pipeline (Rails pattern)"""
        return self.pipeline_type == PipelineTypes.SCHEDULED
    
    def event_driven_mode_(self) -> bool:
        """Check if data source uses event-driven pipeline (Rails pattern)"""
        return self.pipeline_type == PipelineTypes.EVENT_DRIVEN
    
    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self, check_tier_limits: bool = True, force: bool = False, run_now: bool = False) -> None:
        """Activate data source (Rails bang method pattern)"""
        if self.active_():
            return
        
        try:
            # Check tier limits if requested
            if check_tier_limits and not force:
                if not self._check_account_limit():
                    raise ValueError("Account limit exceeded - cannot activate data source")
            
            # Update status
            old_status = self.status
            self.status = DataSourceStatuses.ACTIVE
            self.runtime_status = RuntimeStatuses.IDLE
            self.updated_at = datetime.now()
            self.last_activity_at = datetime.now()
            
            # Handle run now flag
            if run_now:
                self.run_now_at = datetime.now()
                self.run_now_count += 1
            
            # Log status transition
            self._log_status_transition(old_status, self.status)
            
            # Trigger execution if needed
            if run_now:
                self._trigger_execution()
            
            self._clear_cache()
            
        except Exception as e:
            raise ValueError(f"Failed to activate data source: {e}")
    
    def pause_(self, suppress_notifications: bool = False) -> None:
        """Pause data source (Rails bang method pattern)"""
        if self.paused_():
            return
        
        old_status = self.status
        self.status = DataSourceStatuses.PAUSED
        self.runtime_status = RuntimeStatuses.IDLE
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        
        # Log status transition
        self._log_status_transition(old_status, self.status)
        
        # Send notifications unless suppressed
        if not suppress_notifications:
            self._send_pause_notification()
        
        self._clear_cache()
    
    def stop_(self) -> None:
        """Stop data source (Rails bang method pattern)"""
        if self.stopped_():
            return
        
        old_status = self.status
        self.status = DataSourceStatuses.STOPPED
        self.runtime_status = RuntimeStatuses.IDLE
        self.updated_at = datetime.now()
        
        # Log status transition
        self._log_status_transition(old_status, self.status)
        self._clear_cache()
    
    def fail_(self, error_message: str = None) -> None:
        """Mark data source as failed (Rails bang method pattern)"""
        self.status = DataSourceStatuses.FAILED
        self.runtime_status = RuntimeStatuses.ERROR
        self.error_count += 1
        self.updated_at = datetime.now()
        
        if error_message:
            if not self.error_details:
                self.error_details = []
            error_entry = {
                'message': error_message,
                'timestamp': datetime.now().isoformat(),
                'error_type': 'execution_failure'
            }
            self.error_details.append(error_entry)
            # Keep only recent errors
            if len(self.error_details) > self.MAX_ERROR_COUNT:
                self.error_details = self.error_details[-self.MAX_ERROR_COUNT:]
        
        self._clear_cache()
    
    def suspend_(self, reason: str = None) -> None:
        """Suspend data source (Rails bang method pattern)"""
        if self.suspended_():
            return
        
        self.status = DataSourceStatuses.SUSPENDED
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['suspension_reason'] = reason
            self.extra_metadata['suspended_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def archive_(self) -> None:
        """Archive data source (Rails bang method pattern)"""
        if self.archived_():
            return
        
        self.status = DataSourceStatuses.ARCHIVED
        self.runtime_status = RuntimeStatuses.IDLE
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def rate_limit_(self) -> None:
        """Apply rate limiting to data source (Rails bang method pattern)"""
        if self.rate_limited_():
            return
        
        old_status = self.status
        self.status = DataSourceStatuses.RATE_LIMITED
        self.runtime_status = RuntimeStatuses.THROTTLED
        self.updated_at = datetime.now()
        
        # Log emergency rate limiting
        self._log_status_transition(old_status, self.status, severity="CRITICAL")
        
        # Alert administrators
        self._alert_rate_limit()
        self._clear_cache()
    
    def enter_maintenance_(self, reason: str = None) -> None:
        """Enter maintenance mode (Rails bang method pattern)"""
        if self.maintenance_():
            return
        
        self.status = DataSourceStatuses.MAINTENANCE
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['maintenance_reason'] = reason
            self.extra_metadata['maintenance_started_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def exit_maintenance_(self) -> None:
        """Exit maintenance mode (Rails bang method pattern)"""
        if not self.maintenance_():
            return
        
        self.status = DataSourceStatuses.ACTIVE
        self.updated_at = datetime.now()
        
        if self.extra_metadata:
            self.extra_metadata.pop('maintenance_reason', None)
            self.extra_metadata.pop('maintenance_started_at', None)
            self.extra_metadata['maintenance_ended_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def run_now_(self) -> None:
        """Trigger immediate execution (Rails bang method pattern)"""
        if not self.can_execute_():
            raise ValueError(f"Cannot execute data source in {self.status} status")
        
        self.run_now_at = datetime.now()
        self.run_now_count += 1
        self.updated_at = datetime.now()
        
        # Trigger execution
        self._trigger_execution()
    
    def reingest_(self, reason: str = None) -> None:
        """Schedule re-ingestion (Rails bang method pattern)"""
        self.reingest_at = datetime.now()
        self.reingest_count += 1
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['reingest_reason'] = reason
            self.extra_metadata['reingest_scheduled_at'] = datetime.now().isoformat()
        
        # Schedule re-ingestion execution
        self._schedule_reingest()
    
    def start_processing_(self) -> None:
        """Start processing data source (Rails bang method pattern)"""
        if not self.can_execute_():
            raise ValueError(f"Data source cannot process in {self.status} status")
        
        self.status = DataSourceStatuses.PROCESSING
        self.runtime_status = RuntimeStatuses.PROCESSING
        self.last_execution_at = datetime.now()
        self.execution_count += 1
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
    
    def complete_processing_(self, records_processed: int = 0, execution_time_ms: float = 0) -> None:
        """Complete processing data source (Rails bang method pattern)"""
        self.status = DataSourceStatuses.COMPLETED
        self.runtime_status = RuntimeStatuses.IDLE
        self.success_count += 1
        
        if records_processed > 0:
            self.records_ingested += records_processed
            self.last_record_count = records_processed
        
        if execution_time_ms > 0:
            self._update_performance_metrics(execution_time_ms)
        
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self._clear_cache()
    
    def increment_processed_records_(self, count: int = 1) -> None:
        """Increment processed records count (Rails bang method pattern)"""
        self.records_ingested += count
        self.last_activity_at = datetime.now()
        self.updated_at = datetime.now()
    
    def increment_failed_records_(self, count: int = 1) -> None:
        """Increment failed records count (Rails bang method pattern)"""
        self.records_failed += count
        self.updated_at = datetime.now()
    
    def clear_errors_(self) -> None:
        """Clear error state and count (Rails bang method pattern)"""
        self.error_count = 0
        self.error_details = []
        
        if self.failed_():
            self.status = DataSourceStatuses.ACTIVE
            self.runtime_status = RuntimeStatuses.IDLE
        
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def update_mutable_(self, input_data: Dict[str, Any], api_user_info: Dict[str, Any] = None, 
                       force: bool = False, run_now: bool = False) -> None:
        """Update mutable fields (Rails bang method pattern)"""
        # Validate user permissions
        if api_user_info and not self._validate_update_permissions(api_user_info):
            raise ValueError("Insufficient permissions to update data source")
        
        # Update basic fields
        updatable_fields = ['name', 'description', 'flow_name', 'flow_description', 'poll_schedule']
        for field in updatable_fields:
            if field in input_data:
                setattr(self, field, input_data[field])
        
        # Update configuration
        if 'source_config' in input_data:
            self._validate_config(input_data['source_config'])
            self.source_config = input_data['source_config']
        
        # Update JSON fields
        json_fields = ['template_config', 'runtime_config', 'processing_config', 'tags', 'metadata']
        for field in json_fields:
            if field in input_data:
                setattr(self, field, input_data[field])
        
        # Handle status changes
        if 'status' in input_data and input_data['status'] != self.status.value:
            new_status = DataSourceStatuses(input_data['status'])
            if new_status == DataSourceStatuses.ACTIVE:
                self.activate_(not force, force, run_now)
                return
            elif new_status == DataSourceStatuses.PAUSED:
                self.pause_()
                return
            else:
                self.status = new_status
        
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def add_tag_(self, tag_name: str) -> None:
        """Add tag to data source (Rails bang method pattern)"""
        if not self.tags:
            self.tags = []
        if tag_name not in self.tags:
            self.tags.append(tag_name)
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag_name: str) -> None:
        """Remove tag from data source (Rails bang method pattern)"""
        if self.tags and tag_name in self.tags:
            self.tags.remove(tag_name)
            self.updated_at = datetime.now()
    
    # ========================================
    # Rails Class Methods and Scopes
    # ========================================
    
    @classmethod
    def active(cls):
        """Scope for active data sources (Rails scope pattern)"""
        return cls.status == DataSourceStatuses.ACTIVE
    
    @classmethod
    def paused(cls):
        """Scope for paused data sources (Rails scope pattern)"""
        return cls.status == DataSourceStatuses.PAUSED
    
    @classmethod
    def processing(cls):
        """Scope for processing data sources (Rails scope pattern)"""
        return cls.status == DataSourceStatuses.PROCESSING
    
    @classmethod
    def failed(cls):
        """Scope for failed data sources (Rails scope pattern)"""
        return cls.status.in_([DataSourceStatuses.FAILED, DataSourceStatuses.ERROR])
    
    @classmethod
    def rate_limited(cls):
        """Scope for rate limited data sources (Rails scope pattern)"""
        return cls.status == DataSourceStatuses.RATE_LIMITED
    
    @classmethod
    def by_connector_type(cls, connector_type: ConnectorTypes):
        """Scope for data sources by connector type (Rails scope pattern)"""
        return cls.connector_type == connector_type
    
    @classmethod
    def by_owner(cls, owner_id: int):
        """Scope for data sources by owner (Rails scope pattern)"""
        return cls.owner_id == owner_id
    
    @classmethod
    def by_org(cls, org_id: int):
        """Scope for data sources by organization (Rails scope pattern)"""
        return cls.org_id == org_id
    
    @classmethod
    def webhooks(cls):
        """Scope for webhook data sources (Rails scope pattern)"""
        return cls.connector_type == ConnectorTypes.WEBHOOK
    
    @classmethod
    def apis(cls):
        """Scope for API data sources (Rails scope pattern)"""
        return cls.connector_type.in_([ConnectorTypes.API, ConnectorTypes.REST_API, ConnectorTypes.NEXSET_API])
    
    @classmethod
    def streaming(cls):
        """Scope for streaming data sources (Rails scope pattern)"""
        from sqlalchemy import or_
        return or_(
            cls.connector_type.in_([ConnectorTypes.KAFKA, ConnectorTypes.REDIS]),
            cls.pipeline_type == PipelineTypes.STREAMING
        )
    
    @classmethod
    def recently_executed(cls, hours: int = 24):
        """Scope for recently executed data sources (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return cls.last_execution_at >= cutoff
    
    @classmethod
    def with_errors(cls):
        """Scope for data sources with errors (Rails scope pattern)"""
        return cls.error_count > 0
    
    @classmethod
    def performance_issues(cls):
        """Scope for data sources with performance issues (Rails scope pattern)"""
        return cls.avg_execution_time_ms > cls.PERFORMANCE_THRESHOLD_MS
    
    @classmethod
    def managed_sources(cls):
        """Scope for managed data sources (Rails scope pattern)"""
        return cls.managed.is_(True)
    
    @classmethod
    def for_search_index(cls):
        """Scope for search indexing (Rails scope pattern)"""
        return cls.status.in_([DataSourceStatuses.ACTIVE, DataSourceStatuses.PAUSED])
    
    @classmethod
    def script_data_credentials_id(cls) -> int:
        """Get script data credentials ID (Rails constant pattern)"""
        return cls.SCRIPT_DATA_CREDENTIALS_ID
    
    @classmethod
    def default_run_id_count(cls) -> int:
        """Get default run ID count (Rails constant pattern)"""
        return cls.DEFAULT_RUN_ID_COUNT
    
    @classmethod
    def single_schema_key(cls) -> str:
        """Get single schema detection key (Rails constant pattern)"""
        return cls.SINGLE_SCHEMA_KEY
    
    @classmethod
    def pipeline_type_key(cls) -> str:
        """Get pipeline type key (Rails constant pattern)"""
        return cls.PIPELINE_TYPE_KEY
    
    @classmethod
    def create_with_defaults(cls, owner, org, name: str, connector_type: ConnectorTypes = None, **kwargs):
        """Factory method to create data source with defaults (Rails pattern)"""
        source_data = {
            'owner': owner,
            'org': org,
            'name': name,
            'connector_type': connector_type or ConnectorTypes.S3,
            'status': DataSourceStatuses.INIT,
            'runtime_status': RuntimeStatuses.IDLE,
            'ingest_method': IngestMethods.POLL,
            'source_format': SourceFormats.JSON,
            'pipeline_type': PipelineTypes.BATCH,
            'managed': False,
            'adaptive_flow': False,
            'referenced_resources_enabled': True,
            'auto_schema_detection': True,
            **kwargs
        }
        
        return cls(**source_data)
    
    @classmethod
    def create_from_template(cls, template, owner, org, name: str, **overrides):
        """Factory method to create data source from template (Rails pattern)"""
        if not template or not template.template_():
            raise ValueError("Invalid template provided")
        
        source_data = {
            'owner': owner,
            'org': org,
            'name': name,
            'description': template.description,
            'connector_type': template.connector_type,
            'ingest_method': template.ingest_method,
            'source_format': template.source_format,
            'pipeline_type': template.pipeline_type,
            'source_config': template.source_config,
            'template_config': template.template_config,
            'managed': template.managed,
            'adaptive_flow': template.adaptive_flow,
            'copied_from': template,
            'status': DataSourceStatuses.INIT,
            **overrides
        }
        
        return cls(**source_data)
    
    @classmethod
    def bulk_activate(cls, source_ids: List[int], check_limits: bool = True):
        """Bulk activate data sources (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_pause(cls, source_ids: List[int]):
        """Bulk pause data sources (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    # ========================================
    # Rails Instance Methods
    # ========================================
    
    def build_from_input(self, input_data: Dict[str, Any], api_user_info: Dict[str, Any], 
                        request_info: Dict[str, Any] = None):
        """Build data source from input data (Rails pattern)"""
        if not input_data:
            raise ValueError("Data source input missing")
        
        # Validate required fields
        if 'name' not in input_data:
            raise ValueError("Name is required")
        if 'org_id' not in input_data:
            raise ValueError("Organization ID is required")
        
        # Set defaults and build from input
        self.set_defaults(api_user_info, input_data)
        self.update_mutable_(input_data, api_user_info)
        
        return self
    
    def set_defaults(self, api_user_info: Dict[str, Any], input_data: Dict[str, Any] = None):
        """Set default values (Rails pattern)"""
        self.owner_id = api_user_info.get('user_id')
        self.org_id = input_data.get('org_id') if input_data else None
        self.status = DataSourceStatuses.INIT
        self.runtime_status = RuntimeStatuses.IDLE
        self.connector_type = ConnectorTypes.S3
        self.ingest_method = IngestMethods.POLL
        self.source_format = SourceFormats.JSON
        self.pipeline_type = PipelineTypes.BATCH
        self.managed = False
        self.adaptive_flow = False
        self.referenced_resources_enabled = True
        self.auto_schema_detection = True
        self.rate_limit_per_second = 100
        self.max_retries = 3
        self.backoff_multiplier = 1.5
    
    def generate_script_email_token(self) -> str:
        """Generate token for script email (Rails pattern)"""
        return secrets.token_urlsafe(32)
    
    def script_source_config(self, request_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Build script configuration (Rails pattern)"""
        return {
            'source_id': self.id,
            'source_name': self.name,
            'org_id': self.org_id,
            'connector_type': self.connector_type.value,
            'source_config': self.source_config,
            'request_host': request_info.get('host', 'localhost') if request_info else 'localhost'
        }
    
    def webhook_url(self, api_key: str) -> str:
        """Generate webhook URL (Rails pattern)"""
        if not self.webhook_():
            raise ValueError("Data source is not configured for webhooks")
        
        return f"/api/v1/webhooks/datasource/{self.id}?api_key={api_key}"
    
    def nexset_api_config(self) -> Optional[Dict[str, Any]]:
        """Extract nexset API configuration (Rails pattern)"""
        if not self.nexset_api_compatible_():
            return None
        
        config = self.source_config or {}
        
        return {
            'endpoint': config.get('endpoint'),
            'method': config.get('method', 'GET'),
            'headers': config.get('headers', {}),
            'auth': config.get('auth', {}),
            'timeout': config.get('timeout', 30000),
            'retry_policy': config.get('retry_policy', {})
        }
    
    def copy_to_org(self, target_org, owner=None, name: str = None):
        """Copy data source to another organization (Rails pattern)"""
        copy_data = {
            'org': target_org,
            'owner': owner or target_org.admin_users[0],
            'name': name or f"{self.name} (Copy)",
            'description': self.description,
            'connector_type': self.connector_type,
            'ingest_method': self.ingest_method,
            'source_format': self.source_format,
            'pipeline_type': self.pipeline_type,
            'source_config': self.source_config.copy() if self.source_config else None,
            'template_config': self.template_config.copy() if self.template_config else None,
            'managed': self.managed,
            'adaptive_flow': self.adaptive_flow,
            'copied_from': self,
            'status': DataSourceStatuses.INIT
        }
        
        return self.__class__(**copy_data)
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get execution performance summary (Rails pattern)"""
        total_executions = self.execution_count
        success_rate = (self.success_count / max(total_executions, 1)) * 100
        
        return {
            'execution_count': self.execution_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate_percent': round(success_rate, 2),
            'records_ingested': self.records_ingested,
            'records_failed': self.records_failed,
            'last_record_count': self.last_record_count,
            'avg_execution_time_ms': self.avg_execution_time_ms,
            'total_execution_time_ms': self.total_execution_time_ms,
            'has_performance_issues': self.performance_issues_(),
            'last_execution_at': self.last_execution_at.isoformat() if self.last_execution_at else None,
            'recently_executed': self.recently_executed_()
        }
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary (Rails pattern)"""
        error_count = len(self.error_details) if self.error_details else 0
        recent_errors = []
        
        if self.error_details:
            # Get recent errors (last 5)
            recent_errors = sorted(
                self.error_details, 
                key=lambda x: x.get('timestamp', ''), 
                reverse=True
            )[:5]
        
        return {
            'has_errors': self.has_errors_(),
            'error_count': self.error_count,
            'error_details_count': error_count,
            'recent_errors': recent_errors,
            'error_types': list(set(e.get('error_type', 'unknown') for e in (self.error_details or [])))
        }
    
    def has_tag(self, tag_name: str) -> bool:
        """Check if data source has specific tag (Rails pattern)"""
        return bool(self.tags and tag_name in self.tags)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        return self.tags or []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    def flow_type(self) -> Optional[str]:
        """Get flow type from origin node (Rails delegate pattern)"""
        if self.origin_node and hasattr(self.origin_node, 'flow_type'):
            return self.origin_node.flow_type
        return self.pipeline_type.value if self.pipeline_type else None
    
    def ingestion_mode(self) -> Optional[str]:
        """Get ingestion mode (Rails delegate pattern)"""
        if self.origin_node and hasattr(self.origin_node, 'ingestion_mode'):
            return self.origin_node.ingestion_mode
        return self.ingest_method.value if self.ingest_method else None
    
    def backend_resource_name(self) -> str:
        """Get backend resource name (Rails pattern)"""
        return "datasource"
    
    def send_control_event(self, event_type: str) -> Dict[str, Any]:
        """Send control event (Rails pattern)"""
        event_data = {
            'event': event_type,
            'source_id': self.id,
            'source_uuid': self.uuid,
            'run_id': f"run_{self.id}_{event_type}_{int(datetime.now().timestamp())}",
            'timestamp': datetime.now().isoformat()
        }
        
        # In real implementation, this would publish to message queue
        return event_data
    
    def _check_account_limit(self) -> bool:
        """Check if organization can activate more data sources (Rails private pattern)"""
        # Implementation would check tier limits
        # For now, return True as placeholder
        return True
    
    def _trigger_execution(self) -> None:
        """Trigger data source execution (Rails private pattern)"""
        # Implementation would trigger actual execution
        self.start_processing_()
    
    def _schedule_reingest(self) -> None:
        """Schedule re-ingestion execution (Rails private pattern)"""
        # Implementation would schedule re-ingestion
        pass
    
    def _send_pause_notification(self) -> None:
        """Send notification when data source is paused (Rails private pattern)"""
        # Implementation would send notification
        pass
    
    def _alert_rate_limit(self) -> None:
        """Alert administrators of rate limiting (Rails private pattern)"""
        # Implementation would send alert
        pass
    
    def _log_status_transition(self, old_status: DataSourceStatuses, new_status: DataSourceStatuses, 
                              severity: str = "INFO") -> None:
        """Log status transition (Rails private pattern)"""
        # Implementation would log to system logs
        pass
    
    def _validate_update_permissions(self, api_user_info: Dict[str, Any]) -> bool:
        """Validate user permissions for updates (Rails private pattern)"""
        return api_user_info.get('user_id') == self.owner_id
    
    def _validate_config(self, config: Any) -> None:
        """Validate configuration data (Rails private pattern)"""
        if config is not None and not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")
    
    def _update_performance_metrics(self, execution_time_ms: float) -> None:
        """Update performance metrics (Rails private pattern)"""
        self.total_execution_time_ms += execution_time_ms
        
        # Calculate running average
        if self.execution_count > 0:
            self.avg_execution_time_ms = self.total_execution_time_ms / self.execution_count
    
    def _clear_cache(self) -> None:
        """Clear internal cache (Rails private pattern)"""
        self._execution_metrics.clear()
        self._config_cache.clear()
        self._validation_cache.clear()
    
    # ========================================
    # Rails Validation and Display Methods
    # ========================================
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        return self.name or f"DataSource #{self.id}"
    
    def display_status(self) -> str:
        """Get formatted status for display (Rails pattern)"""
        return self.status.value.replace('_', ' ').title()
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            DataSourceStatuses.ACTIVE: 'green',
            DataSourceStatuses.PROCESSING: 'blue',
            DataSourceStatuses.COMPLETED: 'green',
            DataSourceStatuses.PAUSED: 'yellow',
            DataSourceStatuses.FAILED: 'red',
            DataSourceStatuses.ERROR: 'red',
            DataSourceStatuses.SUSPENDED: 'orange',
            DataSourceStatuses.ARCHIVED: 'gray',
            DataSourceStatuses.RATE_LIMITED: 'red',
            DataSourceStatuses.MAINTENANCE: 'purple'
        }
        return status_colors.get(self.status, 'gray')
    
    def connector_type_display(self) -> str:
        """Get formatted connector type for display (Rails pattern)"""
        return self.connector_type.value.replace('_', ' ').title()
    
    def validate_for_activation(self) -> Tuple[bool, List[str]]:
        """Validate data source can be activated (Rails pattern)"""
        errors = []
        
        if self.active_():
            errors.append("Data source is already active")
        
        if not self.name:
            errors.append("Name is required")
        
        if not self.owner_id:
            errors.append("Owner is required")
        
        if not self.org_id:
            errors.append("Organization is required")
        
        if not self.has_credentials_() and self.connector_type not in [ConnectorTypes.WEBHOOK, ConnectorTypes.FILE_UPLOAD]:
            errors.append("Data credentials are required")
        
        return len(errors) == 0, errors
    
    # ========================================
    # Rails API and Serialization Methods
    # ========================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for basic API responses (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'name': self.name,
            'description': self.description,
            'status': self.status.value,
            'display_status': self.display_status(),
            'status_color': self.status_color(),
            'runtime_status': self.runtime_status.value,
            'connector_type': self.connector_type.value,
            'connector_type_display': self.connector_type_display(),
            'ingest_method': self.ingest_method.value,
            'source_format': self.source_format.value,
            'pipeline_type': self.pipeline_type.value,
            'active': self.active_(),
            'paused': self.paused_(),
            'processing': self.processing_(),
            'failed': self.failed_(),
            'healthy': self.healthy_(),
            'rate_limited': self.rate_limited_(),
            'webhook': self.webhook_(),
            'file_upload': self.file_upload_(),
            'api': self.api_(),
            'streaming': self.streaming_(),
            'has_credentials': self.has_credentials_(),
            'nexset_api_compatible': self.nexset_api_compatible_(),
            'managed': self.managed_(),
            'adaptive': self.adaptive_(),
            'recently_executed': self.recently_executed_(),
            'has_errors': self.has_errors_(),
            'performance_issues': self.performance_issues_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'tags': self.tags_list()
        }
    
    def to_detailed_dict(self) -> Dict[str, Any]:
        """Convert to detailed dictionary for full API responses (Rails pattern)"""
        base_dict = self.to_dict()
        
        detailed_info = {
            'flow_name': self.flow_name,
            'flow_description': self.flow_description,
            'poll_schedule': self.poll_schedule,
            'last_run_id': self.last_run_id,
            'execution_summary': self.get_execution_summary(),
            'error_summary': self.get_error_summary(),
            'source_config': self.source_config,
            'metadata': self.extra_metadata,
            'data_credentials_id': self.data_credentials_id,
            'flow_node_id': self.flow_node_id,
            'origin_node_id': self.origin_node_id,
            'copied_from_id': self.copied_from_id,
            'last_execution_at': self.last_execution_at.isoformat() if self.last_execution_at else None,
            'next_execution_at': self.next_execution_at.isoformat() if self.next_execution_at else None,
            'relationships': {
                'owner_name': self.owner.name if self.owner else None,
                'org_name': self.org.name if self.org else None,
                'connector_name': self.connector.name if self.connector else None,
                'data_sets_count': len(self.data_sets or [])
            }
        }
        
        base_dict.update(detailed_info)
        return base_dict
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for audit logging (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'name': self.name,
            'status': self.status.value,
            'connector_type': self.connector_type.value,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'execution_count': self.execution_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'records_ingested': self.records_ingested,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self) -> str:
        return f"<DataSource(id={self.id}, name='{self.name}', status='{self.status.value}', connector_type='{self.connector_type.value}', org_id={self.org_id})>"
    
    def __str__(self) -> str:
        return f"DataSource: {self.display_name()} ({self.connector_type_display()}) - {self.display_status()}"