"""
DataSink Model - Data destination and output management entity.
Handles data egress to external systems with comprehensive Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import uuid
from ..database import Base


class DataSinkStatuses(PyEnum):
    """DataSink status enumeration"""
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
    CONFIGURING = "CONFIGURING"
    TESTING = "TESTING"


class ConnectorTypes(PyEnum):
    """Connector type enumeration"""
    S3 = "s3"
    REST = "rest"
    WEBHOOK = "webhook"
    DATABASE = "database"
    FILE = "file"
    KAFKA = "kafka"
    REDIS = "redis"
    ELASTICSEARCH = "elasticsearch"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    CUSTOM = "custom"
    SCRIPT = "script"
    FTP = "ftp"
    SFTP = "sftp"


class DeliveryModes(PyEnum):
    """Delivery mode enumeration"""
    STREAMING = "streaming"
    BATCH = "batch"
    REALTIME = "realtime"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    EVENT_DRIVEN = "event_driven"


class CompressionTypes(PyEnum):
    """Compression type enumeration"""
    NONE = "none"
    GZIP = "gzip"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    ZSTD = "zstd"


class DataSink(Base):
    __tablename__ = "data_sinks"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    status = Column(SQLEnum(DataSinkStatuses), default=DataSinkStatuses.INIT, nullable=False, index=True)
    runtime_status = Column(String(100), index=True)
    
    # Connection and configuration
    connector_type = Column(SQLEnum(ConnectorTypes), nullable=False, index=True)
    connection_type = Column(String(255), index=True)  # Legacy field for compatibility
    delivery_mode = Column(SQLEnum(DeliveryModes), default=DeliveryModes.BATCH)
    compression_type = Column(SQLEnum(CompressionTypes), default=CompressionTypes.NONE)
    
    # Configuration
    config = Column(JSON)
    runtime_config = Column(JSON)
    sink_config = Column(JSON)
    template_config = Column(JSON)
    connection_config = Column(JSON)
    delivery_config = Column(JSON)
    
    # Performance and monitoring
    records_delivered = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    last_delivery_at = Column(DateTime)
    avg_delivery_time_ms = Column(Float, default=0.0)
    total_delivery_time_ms = Column(Float, default=0.0)
    delivery_errors = Column(JSON)
    
    # Health and reliability
    success_rate_percent = Column(Float, default=100.0)
    last_health_check_at = Column(DateTime)
    health_check_status = Column(String(50), default="unknown")
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    
    # Metadata and tags
    tags = Column(JSON)
    extra_metadata = Column(JSON)
    external_id = Column(String(255), index=True)
    
    # State flags
    is_disabled = Column(Boolean, default=False)
    is_template = Column(Boolean, default=False)
    is_system = Column(Boolean, default=False)
    auto_create_resources = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    last_activity_at = Column(DateTime, default=func.now())
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    connector_id = Column(Integer, ForeignKey("connectors.id"), index=True)
    flow_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    data_map_id = Column(Integer, ForeignKey("data_maps.id"), index=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), index=True)
    vendor_endpoint_id = Column(Integer, ForeignKey("vendor_endpoints.id"), index=True)
    code_container_id = Column(Integer, ForeignKey("code_containers.id"), index=True)
    copied_from_id = Column(Integer, ForeignKey("data_sinks.id"), index=True)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    org = relationship("Org", back_populates="data_sinks")
    data_set = relationship("DataSet", back_populates="data_sinks")
    data_credentials = relationship("DataCredentials", foreign_keys=[data_credentials_id])
    connector = relationship("Connector")
    flow_node = relationship("FlowNode", foreign_keys=[flow_node_id])
    origin_node = relationship("FlowNode", foreign_keys=[origin_node_id])
    data_map = relationship("DataMap", foreign_keys=[data_map_id])
    data_source = relationship("DataSource", foreign_keys=[data_source_id])
    vendor_endpoint = relationship("VendorEndpoint", foreign_keys=[vendor_endpoint_id])
    code_container = relationship("CodeContainer", foreign_keys=[code_container_id])
    copied_from = relationship("DataSink", remote_side="DataSink.id", foreign_keys=[copied_from_id])
    copied_data_sinks = relationship("DataSink", remote_side="DataSink.copied_from_id")
    
    # Rails business logic constants
    SCRIPT_DATA_CREDENTIALS_ID = 1
    CREATE_DATA_SOURCE_KEY = "create.datasource"
    INGEST_DATA_SOURCE_KEY = "ingest.datasource.id"
    MAX_DELIVERY_ERRORS = 100
    PERFORMANCE_THRESHOLD_MS = 3000
    HEALTH_CHECK_INTERVAL_MINUTES = 15
    CACHE_TTL_SECONDS = 300
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self.control_messages_enabled = True
        self.auto_retry_enabled = True
        self._cache = {}
        self._performance_metrics = {}
        self._health_status = {}
    
    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if data sink is active and not disabled (Rails pattern)"""
        return self.status == DataSinkStatuses.ACTIVE and not self.is_disabled
    
    def paused_(self) -> bool:
        """Check if data sink is paused (Rails pattern)"""
        return self.status == DataSinkStatuses.PAUSED
    
    def init_(self) -> bool:
        """Check if data sink is in init state (Rails pattern)"""
        return self.status == DataSinkStatuses.INIT
    
    def processing_(self) -> bool:
        """Check if data sink is currently processing (Rails pattern)"""
        return self.status == DataSinkStatuses.PROCESSING
    
    def completed_(self) -> bool:
        """Check if data sink processing is completed (Rails pattern)"""
        return self.status == DataSinkStatuses.COMPLETED
    
    def failed_(self) -> bool:
        """Check if data sink has failed (Rails pattern)"""
        return self.status in [DataSinkStatuses.FAILED, DataSinkStatuses.ERROR]
    
    def stopped_(self) -> bool:
        """Check if data sink is stopped (Rails pattern)"""
        return self.status == DataSinkStatuses.STOPPED
    
    def suspended_(self) -> bool:
        """Check if data sink is suspended (Rails pattern)"""
        return self.status == DataSinkStatuses.SUSPENDED
    
    def archived_(self) -> bool:
        """Check if data sink is archived (Rails pattern)"""
        return self.status == DataSinkStatuses.ARCHIVED
    
    def configuring_(self) -> bool:
        """Check if data sink is being configured (Rails pattern)"""
        return self.status == DataSinkStatuses.CONFIGURING
    
    def testing_(self) -> bool:
        """Check if data sink is being tested (Rails pattern)"""
        return self.status == DataSinkStatuses.TESTING
    
    def healthy_(self) -> bool:
        """Check if data sink is in healthy state (Rails pattern)"""
        return (self.status in [DataSinkStatuses.ACTIVE, DataSinkStatuses.COMPLETED] and 
                not self.is_disabled and
                self.health_check_status == "healthy")
    
    def disabled_(self) -> bool:
        """Check if data sink is disabled (Rails pattern)"""
        return self.is_disabled is True
    
    def template_(self) -> bool:
        """Check if data sink is a template (Rails pattern)"""
        return self.is_template is True
    
    def system_(self) -> bool:
        """Check if data sink is system-managed (Rails pattern)"""
        return self.is_system is True
    
    def streaming_(self) -> bool:
        """Check if data sink uses streaming delivery (Rails pattern)"""
        return self.delivery_mode == DeliveryModes.STREAMING
    
    def batch_(self) -> bool:
        """Check if data sink uses batch delivery (Rails pattern)"""
        return self.delivery_mode == DeliveryModes.BATCH
    
    def realtime_(self) -> bool:
        """Check if data sink uses realtime delivery (Rails pattern)"""
        return self.delivery_mode == DeliveryModes.REALTIME
    
    def has_credentials_(self) -> bool:
        """Check if data sink has credentials configured (Rails pattern)"""
        return self.data_credentials_id is not None
    
    def has_errors_(self) -> bool:
        """Check if data sink has delivery errors (Rails pattern)"""
        return bool(self.delivery_errors and len(self.delivery_errors) > 0)
    
    def performance_issues_(self) -> bool:
        """Check if data sink has performance issues (Rails pattern)"""
        return self.avg_delivery_time_ms > self.PERFORMANCE_THRESHOLD_MS
    
    def needs_health_check_(self) -> bool:
        """Check if data sink needs health check (Rails pattern)"""
        if not self.last_health_check_at:
            return True
        
        threshold = datetime.now() - timedelta(minutes=self.HEALTH_CHECK_INTERVAL_MINUTES)
        return self.last_health_check_at < threshold
    
    def can_deliver_(self) -> bool:
        """Check if data sink can deliver data (Rails pattern)"""
        return (self.status in [DataSinkStatuses.ACTIVE, DataSinkStatuses.PROCESSING] and 
                not self.is_disabled and 
                self.has_credentials_() and
                not self.archived_())
    
    def can_be_tested_(self) -> bool:
        """Check if data sink can be tested (Rails pattern)"""
        return (self.has_credentials_() and 
                not self.is_disabled and 
                self.status != DataSinkStatuses.ARCHIVED)
    
    def can_be_copied_(self) -> bool:
        """Check if data sink can be copied (Rails pattern)"""
        return not self.is_disabled and not self.archived_()
    
    def can_be_deleted_(self) -> bool:
        """Check if data sink can be deleted (Rails pattern)"""
        return not self.is_system or self.force_delete
    
    def requires_retry_(self) -> bool:
        """Check if data sink requires retry (Rails pattern)"""
        return self.failed_() and self.retry_count < self.max_retries
    
    def rest_connector_(self) -> bool:
        """Check if data sink uses REST connector (Rails pattern)"""
        return self.connector_type == ConnectorTypes.REST
    
    def database_connector_(self) -> bool:
        """Check if data sink uses database connector (Rails pattern)"""
        return self.connector_type == ConnectorTypes.DATABASE
    
    def file_connector_(self) -> bool:
        """Check if data sink uses file connector (Rails pattern)"""
        return self.connector_type in [ConnectorTypes.FILE, ConnectorTypes.FTP, ConnectorTypes.SFTP]
    
    def cloud_connector_(self) -> bool:
        """Check if data sink uses cloud connector (Rails pattern)"""
        return self.connector_type in [ConnectorTypes.S3, ConnectorTypes.BIGQUERY, ConnectorTypes.SNOWFLAKE]
    
    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self) -> None:
        """Activate data sink with dependencies (Rails bang method pattern)"""
        if self.active_():
            return
        
        try:
            # Update dependent data source if REST connector
            if self.rest_connector_():
                self.update_dependent_data_source_()
            
            # Activate associated data source if present
            if self.data_source and hasattr(self.data_source, 'activate_'):
                self.data_source.activate_()
            
            # Update self and flow node
            self.status = DataSinkStatuses.ACTIVE
            self.updated_at = datetime.now()
            self.last_activity_at = datetime.now()
            if self.flow_node and hasattr(self.flow_node, 'status'):
                self.flow_node.status = self.status
            
            # Send control events and notifications if active
            if self.active_():
                self.send_control_event_("activate")
                self.update_flow_ingestion_mode_()
            
            self._clear_cache()
            
        except Exception as e:
            raise ValueError(f"Failed to activate data sink: {e}")
    
    def pause_(self) -> None:
        """Pause data sink with dependencies (Rails bang method pattern)"""
        if self.paused_():
            return
        
        try:
            # Pause associated data source if present
            if self.data_source and hasattr(self.data_source, 'pause_'):
                self.data_source.pause_()
            
            # Update self and flow node
            self.status = DataSinkStatuses.PAUSED
            self.updated_at = datetime.now()
            self.last_activity_at = datetime.now()
            if self.flow_node and hasattr(self.flow_node, 'status'):
                self.flow_node.status = self.status
            
            # Send control events if paused
            if self.paused_():
                self.send_control_event_("pause")
            
            self._clear_cache()
            
        except Exception as e:
            raise ValueError(f"Failed to pause data sink: {e}")
    
    def stop_(self) -> None:
        """Stop data sink processing (Rails bang method pattern)"""
        if self.stopped_():
            return
        
        self.status = DataSinkStatuses.STOPPED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        if self.flow_node and hasattr(self.flow_node, 'status'):
            self.flow_node.status = self.status
        self._clear_cache()
    
    def fail_(self, error_message: str = None) -> None:
        """Mark data sink as failed (Rails bang method pattern)"""
        self.status = DataSinkStatuses.FAILED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        
        if error_message:
            if not self.delivery_errors:
                self.delivery_errors = []
            error_entry = {
                'message': error_message,
                'timestamp': datetime.now().isoformat(),
                'error_type': 'delivery_failure'
            }
            self.delivery_errors.append(error_entry)
            # Keep only recent errors
            if len(self.delivery_errors) > self.MAX_DELIVERY_ERRORS:
                self.delivery_errors = self.delivery_errors[-self.MAX_DELIVERY_ERRORS:]
        
        if self.flow_node and hasattr(self.flow_node, 'status'):
            self.flow_node.status = self.status
        self._clear_cache()
    
    def start_processing_(self) -> None:
        """Start data sink processing (Rails bang method pattern)"""
        if not self.can_deliver_():
            raise ValueError(f"Data sink cannot deliver. Status: {self.status}")
        
        self.status = DataSinkStatuses.PROCESSING
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self._clear_cache()
    
    def complete_processing_(self, records_delivered: int = 0) -> None:
        """Complete data sink processing (Rails bang method pattern)"""
        self.status = DataSinkStatuses.COMPLETED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self.last_delivery_at = datetime.now()
        
        if records_delivered > 0:
            self.records_delivered += records_delivered
        
        self._update_performance_metrics()
        self._update_success_rate()
        self._clear_cache()
    
    def suspend_(self, reason: str = None) -> None:
        """Suspend data sink (Rails bang method pattern)"""
        self.status = DataSinkStatuses.SUSPENDED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['suspension_reason'] = reason
            self.extra_metadata['suspended_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def archive_(self) -> None:
        """Archive data sink (Rails bang method pattern)"""
        self.status = DataSinkStatuses.ARCHIVED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self.is_disabled = True
        self._clear_cache()
    
    def enable_(self) -> None:
        """Enable data sink (Rails bang method pattern)"""
        self.is_disabled = False
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def disable_(self) -> None:
        """Disable data sink (Rails bang method pattern)"""
        self.is_disabled = True
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def configure_(self, config_data: Dict[str, Any]) -> None:
        """Configure data sink settings (Rails bang method pattern)"""
        self.status = DataSinkStatuses.CONFIGURING
        self.config = {**(self.config or {}), **config_data}
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def test_connection_(self) -> bool:
        """Test data sink connection (Rails bang method pattern)"""
        if not self.can_be_tested_():
            raise ValueError("Data sink cannot be tested")
        
        self.status = DataSinkStatuses.TESTING
        self.updated_at = datetime.now()
        
        # Simulate connection test
        # In real implementation, this would test actual connection
        test_successful = True
        
        if test_successful:
            self.health_check_status = "healthy"
            self.last_health_check_at = datetime.now()
        else:
            self.health_check_status = "unhealthy"
        
        self._clear_cache()
        return test_successful
    
    def retry_delivery_(self) -> None:
        """Retry failed delivery (Rails bang method pattern)"""
        if not self.requires_retry_():
            raise ValueError("Retry not required or max retries exceeded")
        
        self.retry_count += 1
        self.status = DataSinkStatuses.PROCESSING
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self._clear_cache()
    
    def reset_retries_(self) -> None:
        """Reset retry counter (Rails bang method pattern)"""
        self.retry_count = 0
        self.updated_at = datetime.now()
    
    def increment_delivered_records_(self, count: int = 1) -> None:
        """Increment delivered records count (Rails bang method pattern)"""
        self.records_delivered += count
        self.last_delivery_at = datetime.now()
        self.updated_at = datetime.now()
    
    def increment_failed_records_(self, count: int = 1) -> None:
        """Increment failed records count (Rails bang method pattern)"""
        self.records_failed += count
        self.updated_at = datetime.now()
        self._update_success_rate()
    
    # ========================================
    # Rails Class Methods and Scopes
    # ========================================
    
    @classmethod
    def active(cls):
        """Scope for active data sinks (Rails scope pattern)"""
        from sqlalchemy import and_
        return and_(cls.status == DataSinkStatuses.ACTIVE, cls.is_disabled.is_(False))
    
    @classmethod
    def paused(cls):
        """Scope for paused data sinks (Rails scope pattern)"""
        return cls.status == DataSinkStatuses.PAUSED
    
    @classmethod
    def failed(cls):
        """Scope for failed data sinks (Rails scope pattern)"""
        return cls.status.in_([DataSinkStatuses.FAILED, DataSinkStatuses.ERROR])
    
    @classmethod
    def by_connector_type(cls, connector_type: ConnectorTypes):
        """Scope for data sinks by connector type (Rails scope pattern)"""
        return cls.connector_type == connector_type
    
    @classmethod
    def by_delivery_mode(cls, delivery_mode: DeliveryModes):
        """Scope for data sinks by delivery mode (Rails scope pattern)"""
        return cls.delivery_mode == delivery_mode
    
    @classmethod
    def by_owner(cls, owner_id: int):
        """Scope for data sinks by owner (Rails scope pattern)"""
        return cls.owner_id == owner_id
    
    @classmethod
    def by_org(cls, org_id: int):
        """Scope for data sinks by organization (Rails scope pattern)"""
        return cls.org_id == org_id
    
    @classmethod
    def healthy(cls):
        """Scope for healthy data sinks (Rails scope pattern)"""
        from sqlalchemy import and_
        return and_(
            cls.status.in_([DataSinkStatuses.ACTIVE, DataSinkStatuses.COMPLETED]),
            cls.is_disabled.is_(False),
            cls.health_check_status == "healthy"
        )
    
    @classmethod
    def with_errors(cls):
        """Scope for data sinks with delivery errors (Rails scope pattern)"""
        return cls.delivery_errors.isnot(None)
    
    @classmethod
    def performance_issues(cls):
        """Scope for data sinks with performance issues (Rails scope pattern)"""
        return cls.avg_delivery_time_ms > cls.PERFORMANCE_THRESHOLD_MS
    
    @classmethod
    def needs_health_check(cls):
        """Scope for data sinks needing health check (Rails scope pattern)"""
        from sqlalchemy import or_
        cutoff_time = datetime.now() - timedelta(minutes=cls.HEALTH_CHECK_INTERVAL_MINUTES)
        return or_(
            cls.last_health_check_at.is_(None),
            cls.last_health_check_at < cutoff_time
        )
    
    @classmethod
    def recent(cls, days: int = 7):
        """Scope for recent data sinks (Rails scope pattern)"""
        cutoff_date = datetime.now() - timedelta(days=days)
        return cls.created_at >= cutoff_date
    
    @classmethod
    def backend_resource_name(cls) -> str:
        """Get backend resource name (Rails pattern)"""
        return "sink"
    
    @classmethod
    def connector_types(cls) -> List[str]:
        """Get available connector types (Rails pattern)"""
        return [ct.value for ct in ConnectorTypes]
    
    @classmethod
    def delivery_modes(cls) -> List[str]:
        """Get available delivery modes (Rails pattern)"""
        return [dm.value for dm in DeliveryModes]
    
    @classmethod
    def all_condensed(cls, filter_opts: Dict[str, Any] = None, sort_opts: Dict[str, Any] = None):
        """Get condensed data sink list (Rails pattern)"""
        fields = [
            "id", "uuid", "name", "status", "connector_type", "delivery_mode",
            "owner_id", "org_id", "data_set_id", "flow_node_id", "origin_node_id",
            "records_delivered", "records_failed", "success_rate_percent",
            "updated_at", "created_at"
        ]
        # Implementation would depend on query builder
        return []
    
    @classmethod
    def for_search_index(cls):
        """Get data sinks for search indexing (Rails scope pattern)"""
        # Implementation would return sinks for search indexing with eager loading
        return []
    
    @classmethod
    def create_with_defaults(cls, owner, org, **kwargs):
        """Factory method to create data sink with defaults (Rails pattern)"""
        data_sink = cls(
            owner=owner,
            org=org,
            status=DataSinkStatuses.INIT,
            **kwargs
        )
        return data_sink
    
    @classmethod
    def create_from_template(cls, template, owner, org, **overrides):
        """Factory method to create data sink from template (Rails pattern)"""
        if not template or not template.template_():
            raise ValueError("Invalid template provided")
        
        sink_data = template.to_template_dict()
        sink_data.update(overrides)
        sink_data.update({
            'owner': owner,
            'org': org,
            'copied_from': template,
            'status': DataSinkStatuses.INIT
        })
        
        return cls(**sink_data)
    
    @classmethod
    def bulk_activate(cls, data_sink_ids: List[int]):
        """Bulk activate multiple data sinks (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_pause(cls, data_sink_ids: List[int]):
        """Bulk pause multiple data sinks (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_health_check(cls, data_sink_ids: List[int]):
        """Bulk health check for multiple data sinks (Rails pattern)"""
        # Implementation would perform health checks efficiently
        pass
    
    # ========================================
    # Rails Instance Methods
    # ========================================
    
    def copy_to_org(self, target_org, owner=None):
        """Copy data sink to another organization (Rails pattern)"""
        if not self.can_be_copied_():
            raise ValueError("Data sink cannot be copied")
        
        copy_data = self.to_copy_dict()
        copy_data.update({
            'org': target_org,
            'owner': owner or target_org.admin_users[0],
            'copied_from': self,
            'status': DataSinkStatuses.INIT,
            'name': f"{self.name} (Copy)"
        })
        
        return self.__class__(**copy_data)
    
    def flow_type(self) -> Optional[str]:
        """Get flow type from origin node (Rails delegate pattern)"""
        if self.origin_node and hasattr(self.origin_node, 'flow_type'):
            return self.origin_node.flow_type
        return None
    
    def ingestion_mode(self) -> Optional[str]:
        """Get ingestion mode from origin node (Rails delegate pattern)"""
        if self.origin_node and hasattr(self.origin_node, 'ingestion_mode'):
            return self.origin_node.ingestion_mode
        return None
    
    def add_tag(self, tag_name: str) -> None:
        """Add tag to data sink (Rails pattern)"""
        if not self.tags:
            self.tags = []
        if tag_name not in self.tags:
            self.tags.append(tag_name)
            self.updated_at = datetime.now()
    
    def remove_tag(self, tag_name: str) -> None:
        """Remove tag from data sink (Rails pattern)"""
        if self.tags and tag_name in self.tags:
            self.tags.remove(tag_name)
            self.updated_at = datetime.now()
    
    def has_tag(self, tag_name: str) -> bool:
        """Check if data sink has specific tag (Rails pattern)"""
        return bool(self.tags and tag_name in self.tags)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        return self.tags or []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    def update_performance_metrics(self, delivery_time_ms: float) -> None:
        """Update performance metrics (Rails pattern)"""
        self.total_delivery_time_ms += delivery_time_ms
        
        # Calculate running average
        if self.records_delivered > 0:
            self.avg_delivery_time_ms = self.total_delivery_time_ms / self.records_delivered
        
        self.last_delivery_at = datetime.now()
        self.updated_at = datetime.now()
    
    def get_delivery_summary(self) -> Dict[str, Any]:
        """Get delivery performance summary (Rails pattern)"""
        total_records = self.records_delivered + self.records_failed
        
        return {
            'records_delivered': self.records_delivered,
            'records_failed': self.records_failed,
            'total_records': total_records,
            'success_rate_percent': self.success_rate_percent,
            'avg_delivery_time_ms': self.avg_delivery_time_ms,
            'total_delivery_time_ms': self.total_delivery_time_ms,
            'has_performance_issues': self.performance_issues_(),
            'last_delivery_at': self.last_delivery_at.isoformat() if self.last_delivery_at else None,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries
        }
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary (Rails pattern)"""
        error_count = len(self.delivery_errors) if self.delivery_errors else 0
        recent_errors = []
        
        if self.delivery_errors:
            # Get recent errors (last 5)
            recent_errors = sorted(
                self.delivery_errors, 
                key=lambda x: x.get('timestamp', ''), 
                reverse=True
            )[:5]
        
        return {
            'has_errors': self.has_errors_(),
            'error_count': error_count,
            'recent_errors': recent_errors,
            'error_types': list(set(e.get('error_type', 'unknown') for e in (self.delivery_errors or [])))
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status summary (Rails pattern)"""
        return {
            'is_healthy': self.healthy_(),
            'health_check_status': self.health_check_status,
            'last_health_check_at': self.last_health_check_at.isoformat() if self.last_health_check_at else None,
            'needs_health_check': self.needs_health_check_(),
            'can_deliver': self.can_deliver_(),
            'has_credentials': self.has_credentials_()
        }
    
    def update_dependent_data_source_(self) -> None:
        """Update dependent data source (Rails private pattern)"""
        if self.data_source:
            # Implementation would update associated data source configurations
            self.data_source.updated_at = datetime.now()
    
    def update_flow_ingestion_mode_(self) -> None:
        """Update flow ingestion mode (Rails private pattern)"""
        if self.origin_node and hasattr(self.origin_node, 'update_ingestion_mode_'):
            self.origin_node.update_ingestion_mode_()
    
    def send_control_event_(self, event_type: str) -> Dict[str, Any]:
        """Send control event (Rails private pattern)"""
        if not self.control_messages_enabled:
            return {}
        
        # Implementation would send control events to the data plane
        event_data = {
            'event': event_type,
            'sink_id': self.id,
            'sink_uuid': self.uuid,
            'connector_type': self.connector_type.value,
            'timestamp': datetime.now().isoformat()
        }
        
        # In real implementation, this would publish to message queue
        return event_data
    
    def set_flow_node_sink_id_(self) -> None:
        """Set flow node sink ID after creation (Rails private pattern)"""
        if self.flow_node:
            # Implementation would link the flow node to this sink
            pass
    
    def build_flow_node_(self) -> None:
        """Build flow node before creation (Rails private pattern)"""
        # Implementation would create associated flow node
        pass
    
    # ========================================
    # Rails Validation and Display Methods
    # ========================================
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        return self.name or f"DataSink #{self.id}"
    
    def display_status(self) -> str:
        """Get formatted status for display (Rails pattern)"""
        return self.status.value.replace('_', ' ').title()
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            DataSinkStatuses.ACTIVE: 'green',
            DataSinkStatuses.PROCESSING: 'blue',
            DataSinkStatuses.COMPLETED: 'green',
            DataSinkStatuses.PAUSED: 'yellow',
            DataSinkStatuses.FAILED: 'red',
            DataSinkStatuses.ERROR: 'red',
            DataSinkStatuses.SUSPENDED: 'orange',
            DataSinkStatuses.ARCHIVED: 'gray',
            DataSinkStatuses.CONFIGURING: 'purple',
            DataSinkStatuses.TESTING: 'cyan'
        }
        return status_colors.get(self.status, 'gray')
    
    def validate_for_delivery(self) -> Tuple[bool, List[str]]:
        """Validate data sink can deliver data (Rails pattern)"""
        errors = []
        
        if not self.can_deliver_():
            errors.append(f"Data sink cannot deliver in {self.status} status")
        
        if not self.owner:
            errors.append("Data sink must have an owner")
        
        if not self.org:
            errors.append("Data sink must belong to an organization")
        
        if not self.has_credentials_():
            errors.append("Data credentials are required")
        
        if not self.config:
            errors.append("Configuration is required")
        
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
            'connector_type': self.connector_type.value,
            'delivery_mode': self.delivery_mode.value,
            'runtime_status': self.runtime_status,
            'active': self.active_(),
            'paused': self.paused_(),
            'processing': self.processing_(),
            'failed': self.failed_(),
            'healthy': self.healthy_(),
            'has_errors': self.has_errors_(),
            'performance_issues': self.performance_issues_(),
            'flow_type': self.flow_type(),
            'ingestion_mode': self.ingestion_mode(),
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
            'data_set_id': self.data_set_id,
            'flow_node_id': self.flow_node_id,
            'origin_node_id': self.origin_node_id,
            'data_credentials_id': self.data_credentials_id,
            'connector_id': self.connector_id,
            'compression_type': self.compression_type.value,
            'external_id': self.external_id,
            'metadata': self.extra_metadata,
            'delivery_summary': self.get_delivery_summary(),
            'error_summary': self.get_error_summary(),
            'health_status': self.get_health_status(),
            'relationships': {
                'data_set_name': self.data_set.name if self.data_set else None,
                'data_source_name': self.data_source.name if self.data_source else None,
                'connector_name': self.connector.name if self.connector else None,
                'owner_name': self.owner.name if self.owner else None,
                'org_name': self.org.name if self.org else None
            }
        }
        
        base_dict.update(detailed_info)
        return base_dict
    
    def to_copy_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for copying (Rails pattern)"""
        return {
            'name': f"{self.name} (Copy)",
            'description': self.description,
            'connector_type': self.connector_type,
            'delivery_mode': self.delivery_mode,
            'compression_type': self.compression_type,
            'config': self.config,
            'sink_config': self.sink_config,
            'template_config': self.template_config,
            'tags': self.tags_list(),
            'metadata': self.extra_metadata or {},
            'max_retries': self.max_retries
        }
    
    def to_template_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for template creation (Rails pattern)"""
        template_dict = self.to_copy_dict()
        template_dict.update({
            'is_template': False,  # New instances are not templates
            'copied_from_id': None,  # Clear the copy reference
            'uuid': None  # New UUID will be generated
        })
        return template_dict
    
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
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'records_delivered': self.records_delivered,
            'records_failed': self.records_failed,
            'success_rate_percent': self.success_rate_percent
        }
    
    # ========================================
    # Rails Private Methods
    # ========================================
    
    def _clear_cache(self) -> None:
        """Clear internal cache (Rails private method pattern)"""
        self._cache.clear()
    
    def _update_performance_metrics(self) -> None:
        """Update performance metrics (Rails private method pattern)"""
        if not self._performance_metrics:
            return
        
        # Calculate averages and update metrics
        if 'delivery_times' in self._performance_metrics:
            times = self._performance_metrics['delivery_times']
            if times:
                self.avg_delivery_time_ms = sum(times) / len(times)
                self.total_delivery_time_ms += sum(times)
    
    def _update_success_rate(self) -> None:
        """Update success rate (Rails private method pattern)"""
        total_records = self.records_delivered + self.records_failed
        if total_records > 0:
            self.success_rate_percent = (self.records_delivered / total_records) * 100
        else:
            self.success_rate_percent = 100.0
    
    def __repr__(self) -> str:
        return f"<DataSink(id={self.id}, name='{self.name}', status='{self.status.value}', connector_type='{self.connector_type.value}', org_id={self.org_id})>"
    
    def __str__(self) -> str:
        return f"DataSink: {self.display_name()} ({self.display_status()}) - {self.connector_type.value}"