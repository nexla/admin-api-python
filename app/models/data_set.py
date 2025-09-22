"""
DataSet Model - Core data processing pipeline entity.
Represents processed data entities within flows with comprehensive Rails business logic patterns.
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


class DataSetStatuses(PyEnum):
    """DataSet status enumeration"""
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
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DEPRECATED = "DEPRECATED"


class DataSetTypes(PyEnum):
    """DataSet type enumeration"""
    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"
    SPLITTER = "splitter"
    AGGREGATOR = "aggregator"
    FILTER = "filter"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    STREAMING = "streaming"
    BATCH = "batch"
    REALTIME = "realtime"


class SchemaValidationModes(PyEnum):
    """Schema validation mode enumeration"""
    STRICT = "strict"
    LOOSE = "loose"
    NONE = "none"
    AUTO = "auto"


class OutputFormats(PyEnum):
    """Output format enumeration"""
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    AVRO = "avro"
    XML = "xml"
    YAML = "yaml"
    BINARY = "binary"


class DataSet(Base):
    __tablename__ = "data_sets"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    status = Column(SQLEnum(DataSetStatuses), default=DataSetStatuses.INIT, nullable=False, index=True)
    runtime_status = Column(String(100), index=True)
    data_set_type = Column(SQLEnum(DataSetTypes), default=DataSetTypes.TRANSFORM, nullable=False)
    
    # Schema and validation
    schema_validation_mode = Column(SQLEnum(SchemaValidationModes), default=SchemaValidationModes.AUTO)
    output_format = Column(SQLEnum(OutputFormats), default=OutputFormats.JSON)
    output_schema_locked = Column(Boolean, default=False)
    schema_version = Column(String(50), default="1.0")
    
    # Data samples and schemas
    data_sample = Column(JSON)
    schema_sample = Column(JSON)
    source_path = Column(JSON)
    source_schema = Column(JSON)
    output_schema = Column(JSON)
    output_schema_annotations = Column(JSON)
    output_validation_schema = Column(JSON)
    
    # Configuration
    transform_config = Column(JSON)
    custom_config = Column(JSON)
    runtime_config = Column(JSON)
    processing_config = Column(JSON)
    
    # Performance and monitoring
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    last_processed_at = Column(DateTime)
    avg_processing_time_ms = Column(Float, default=0.0)
    total_processing_time_ms = Column(Float, default=0.0)
    processing_errors = Column(JSON)
    
    # Access control and sharing
    public = Column(Boolean, default=False, index=True)
    shared_with_org = Column(Boolean, default=False)
    access_level = Column(String(50), default="private")
    
    # Metadata and tags
    tags = Column(JSON)
    extra_metadata = Column(JSON)
    external_id = Column(String(255), index=True)
    
    # State flags
    is_disabled = Column(Boolean, default=False)
    is_template = Column(Boolean, default=False)
    is_system = Column(Boolean, default=False)
    force_refresh = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    last_activity_at = Column(DateTime, default=func.now())
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), index=True)
    flow_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    data_schema_id = Column(Integer, ForeignKey("data_schemas.id"), index=True)
    parent_data_set_id = Column(Integer, ForeignKey("data_sets.id"), index=True)
    origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    code_container_id = Column(Integer, ForeignKey("code_containers.id"), index=True)
    output_validator_id = Column(Integer, index=True)  # TODO: Add validators table
    copied_from_id = Column(Integer, ForeignKey("data_sets.id"), index=True)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    org = relationship("Org", back_populates="data_sets")
    data_source = relationship("DataSource", back_populates="data_sets")
    flow_node = relationship("FlowNode", foreign_keys=[flow_node_id])
    data_schema = relationship("DataSchema")
    data_sinks = relationship("DataSink", back_populates="data_set")
    parent_data_set = relationship("DataSet", remote_side="DataSet.id", foreign_keys=[parent_data_set_id])
    child_data_sets = relationship("DataSet", remote_side="DataSet.parent_data_set_id")
    origin_node = relationship("FlowNode", foreign_keys=[origin_node_id])
    data_credentials = relationship("DataCredentials", foreign_keys=[data_credentials_id])
    code_container = relationship("CodeContainer", foreign_keys=[code_container_id])
    output_validator = relationship("Validator", foreign_keys=[output_validator_id])
    copied_from = relationship("DataSet", remote_side="DataSet.id", foreign_keys=[copied_from_id])
    copied_data_sets = relationship("DataSet", remote_side="DataSet.copied_from_id")
    
    # Rails business logic constants
    SPLITTER_OPERATION = "nexla.splitter"
    UPSTREAM_SPLITTER_LOOKUP_LIMIT = 10
    DEFAULT_SPLITTER_RULES_LIMIT = 5
    MAX_PROCESSING_ERRORS = 100
    PERFORMANCE_THRESHOLD_MS = 5000
    CACHE_TTL_SECONDS = 300
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self.nexset_api_config = None
        self.cascading_saves = True
        self.control_messages_enabled = True
        self.in_copy = False
        self.skip_schema_detection = False
        self.force_delete = False
        self.parent_data_sink_id = None
        self._cache = {}
        self._performance_metrics = {}
    
    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if data set is active and not disabled (Rails pattern)"""
        return self.status == DataSetStatuses.ACTIVE and not self.is_disabled
    
    def paused_(self) -> bool:
        """Check if data set is paused (Rails pattern)"""
        return self.status == DataSetStatuses.PAUSED
    
    def init_(self) -> bool:
        """Check if data set is in init state (Rails pattern)"""
        return self.status == DataSetStatuses.INIT
    
    def processing_(self) -> bool:
        """Check if data set is currently processing (Rails pattern)"""
        return self.status == DataSetStatuses.PROCESSING
    
    def completed_(self) -> bool:
        """Check if data set processing is completed (Rails pattern)"""
        return self.status == DataSetStatuses.COMPLETED
    
    def failed_(self) -> bool:
        """Check if data set has failed (Rails pattern)"""
        return self.status in [DataSetStatuses.FAILED, DataSetStatuses.ERROR]
    
    def stopped_(self) -> bool:
        """Check if data set is stopped (Rails pattern)"""
        return self.status == DataSetStatuses.STOPPED
    
    def suspended_(self) -> bool:
        """Check if data set is suspended (Rails pattern)"""
        return self.status == DataSetStatuses.SUSPENDED
    
    def archived_(self) -> bool:
        """Check if data set is archived (Rails pattern)"""
        return self.status == DataSetStatuses.ARCHIVED
    
    def draft_(self) -> bool:
        """Check if data set is in draft state (Rails pattern)"""
        return self.status == DataSetStatuses.DRAFT
    
    def published_(self) -> bool:
        """Check if data set is published (Rails pattern)"""
        return self.status == DataSetStatuses.PUBLISHED
    
    def deprecated_(self) -> bool:
        """Check if data set is deprecated (Rails pattern)"""
        return self.status == DataSetStatuses.DEPRECATED
    
    def healthy_(self) -> bool:
        """Check if data set is in healthy state (Rails pattern)"""
        return self.status in [DataSetStatuses.ACTIVE, DataSetStatuses.COMPLETED] and not self.is_disabled
    
    def splitter_(self) -> bool:
        """Check if data set is a splitter (Rails pattern)"""
        return (self.data_set_type == DataSetTypes.SPLITTER or
                (self.transform_config and 
                 self.transform_config.get('operation') == self.SPLITTER_OPERATION))
    
    def source_(self) -> bool:
        """Check if data set is a source type (Rails pattern)"""
        return self.data_set_type == DataSetTypes.SOURCE
    
    def sink_(self) -> bool:
        """Check if data set is a sink type (Rails pattern)"""
        return self.data_set_type == DataSetTypes.SINK
    
    def streaming_(self) -> bool:
        """Check if data set is streaming type (Rails pattern)"""
        return self.data_set_type == DataSetTypes.STREAMING
    
    def batch_(self) -> bool:
        """Check if data set is batch type (Rails pattern)"""
        return self.data_set_type == DataSetTypes.BATCH
    
    def public_(self) -> bool:
        """Check if data set is public (Rails pattern)"""
        return self.public is True
    
    def private_(self) -> bool:
        """Check if data set is private (Rails pattern)"""
        return not self.public_() and self.access_level == "private"
    
    def shared_(self) -> bool:
        """Check if data set is shared (Rails pattern)"""
        return self.shared_with_org or self.access_level != "private"
    
    def template_(self) -> bool:
        """Check if data set is a template (Rails pattern)"""
        return self.is_template is True
    
    def system_(self) -> bool:
        """Check if data set is system-managed (Rails pattern)"""
        return self.is_system is True
    
    def disabled_(self) -> bool:
        """Check if data set is disabled (Rails pattern)"""
        return self.is_disabled is True
    
    def has_schema_(self) -> bool:
        """Check if data set has schema defined (Rails pattern)"""
        return bool(self.output_schema or self.source_schema)
    
    def schema_locked_(self) -> bool:
        """Check if output schema is locked (Rails pattern)"""
        return self.output_schema_locked is True
    
    def has_parent_(self) -> bool:
        """Check if data set has parent (Rails pattern)"""
        return self.parent_data_set_id is not None
    
    def has_children_(self) -> bool:
        """Check if data set has children (Rails pattern)"""
        return len(self.child_data_sets or []) > 0
    
    def has_errors_(self) -> bool:
        """Check if data set has processing errors (Rails pattern)"""
        return bool(self.processing_errors and len(self.processing_errors) > 0)
    
    def performance_issues_(self) -> bool:
        """Check if data set has performance issues (Rails pattern)"""
        return self.avg_processing_time_ms > self.PERFORMANCE_THRESHOLD_MS
    
    def upstream_has_splitter_(self) -> bool:
        """Check if upstream has splitter (Rails pattern)"""
        if not self.parent_data_set:
            return False
        
        current = self.parent_data_set
        count = 0
        while current and count < self.UPSTREAM_SPLITTER_LOOKUP_LIMIT:
            if current.splitter_():
                return True
            current = current.parent_data_set
            count += 1
        
        return False
    
    def can_be_processed_(self) -> bool:
        """Check if data set can be processed (Rails pattern)"""
        return (self.status in [DataSetStatuses.ACTIVE, DataSetStatuses.INIT] and 
                not self.is_disabled and 
                not self.archived_())
    
    def can_be_copied_(self) -> bool:
        """Check if data set can be copied (Rails pattern)"""
        return not self.is_disabled and not self.archived_()
    
    def can_be_deleted_(self) -> bool:
        """Check if data set can be deleted (Rails pattern)"""
        return not self.is_system and (self.force_delete or not self.has_children_())
    
    def requires_validation_(self) -> bool:
        """Check if data set requires schema validation (Rails pattern)"""
        return self.schema_validation_mode in [SchemaValidationModes.STRICT, SchemaValidationModes.AUTO]
    
    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self) -> None:
        """Activate data set with cascading logic (Rails bang method pattern)"""
        if self.active_():
            return
        
        try:
            # Handle splitter parent logic
            if self.parent_data_set and self.parent_data_set.splitter_():
                for child in self.parent_data_set.child_data_sets:
                    child.status = DataSetStatuses.ACTIVE
                    child.updated_at = datetime.now()
                    if child.flow_node:
                        child.flow_node.status = child.status
            
            # Update self
            self.status = DataSetStatuses.ACTIVE
            self.updated_at = datetime.now()
            self.last_activity_at = datetime.now()
            if self.flow_node:
                self.flow_node.status = self.status
            
            # Handle splitter children logic
            if self.splitter_():
                for child in self.child_data_sets:
                    child.status = DataSetStatuses.ACTIVE
                    child.updated_at = datetime.now()
                    if child.flow_node:
                        child.flow_node.status = child.status
            
            self._clear_cache()
            
        except Exception as e:
            raise ValueError(f"Failed to activate data set: {e}")
    
    def pause_(self) -> None:
        """Pause data set with cascading logic (Rails bang method pattern)"""
        if self.paused_():
            return
        
        try:
            # Handle splitter parent logic
            if self.parent_data_set and self.parent_data_set.splitter_():
                for child in self.parent_data_set.child_data_sets:
                    child.status = DataSetStatuses.PAUSED
                    child.updated_at = datetime.now()
                    if child.flow_node:
                        child.flow_node.status = child.status
            
            # Update self
            self.status = DataSetStatuses.PAUSED
            self.updated_at = datetime.now()
            self.last_activity_at = datetime.now()
            if self.flow_node:
                self.flow_node.status = self.status
            
            # Handle splitter children logic
            if self.splitter_():
                for child in self.child_data_sets:
                    child.status = DataSetStatuses.PAUSED
                    child.updated_at = datetime.now()
                    if child.flow_node:
                        child.flow_node.status = child.status
            
            self._clear_cache()
            
        except Exception as e:
            raise ValueError(f"Failed to pause data set: {e}")
    
    def stop_(self) -> None:
        """Stop data set processing (Rails bang method pattern)"""
        if self.stopped_():
            return
        
        self.status = DataSetStatuses.STOPPED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        if self.flow_node:
            self.flow_node.status = self.status
        self._clear_cache()
    
    def fail_(self, error_message: str = None) -> None:
        """Mark data set as failed (Rails bang method pattern)"""
        self.status = DataSetStatuses.FAILED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        
        if error_message:
            if not self.processing_errors:
                self.processing_errors = []
            error_entry = {
                'message': error_message,
                'timestamp': datetime.now().isoformat(),
                'error_type': 'processing_failure'
            }
            self.processing_errors.append(error_entry)
            # Keep only recent errors
            if len(self.processing_errors) > self.MAX_PROCESSING_ERRORS:
                self.processing_errors = self.processing_errors[-self.MAX_PROCESSING_ERRORS:]
        
        if self.flow_node:
            self.flow_node.status = self.status
        self._clear_cache()
    
    def start_processing_(self) -> None:
        """Start data set processing (Rails bang method pattern)"""
        if not self.can_be_processed_():
            raise ValueError(f"Data set cannot be processed. Status: {self.status}")
        
        self.status = DataSetStatuses.PROCESSING
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self.last_processed_at = datetime.now()
        self._clear_cache()
    
    def complete_processing_(self, records_processed: int = 0) -> None:
        """Complete data set processing (Rails bang method pattern)"""
        self.status = DataSetStatuses.COMPLETED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        
        if records_processed > 0:
            self.records_processed += records_processed
        
        self._update_performance_metrics()
        self._clear_cache()
    
    def suspend_(self, reason: str = None) -> None:
        """Suspend data set (Rails bang method pattern)"""
        self.status = DataSetStatuses.SUSPENDED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['suspension_reason'] = reason
            self.extra_metadata['suspended_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def archive_(self) -> None:
        """Archive data set (Rails bang method pattern)"""
        self.status = DataSetStatuses.ARCHIVED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self.is_disabled = True
        self._clear_cache()
    
    def publish_(self) -> None:
        """Publish data set (Rails bang method pattern)"""
        if not self.completed_():
            raise ValueError("Cannot publish incomplete data set")
        
        self.status = DataSetStatuses.PUBLISHED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        self._clear_cache()
    
    def deprecate_(self, reason: str = None) -> None:
        """Deprecate data set (Rails bang method pattern)"""
        self.status = DataSetStatuses.DEPRECATED
        self.updated_at = datetime.now()
        self.last_activity_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deprecation_reason'] = reason
            self.extra_metadata['deprecated_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def enable_(self) -> None:
        """Enable data set (Rails bang method pattern)"""
        self.is_disabled = False
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def disable_(self) -> None:
        """Disable data set (Rails bang method pattern)"""
        self.is_disabled = True
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def make_public_(self) -> None:
        """Make data set public (Rails bang method pattern)"""
        self.public = True
        self.access_level = "public"
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def make_private_(self) -> None:
        """Make data set private (Rails bang method pattern)"""
        self.public = False
        self.access_level = "private"
        self.shared_with_org = False
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def lock_schema_(self) -> None:
        """Lock output schema (Rails bang method pattern)"""
        self.output_schema_locked = True
        self.updated_at = datetime.now()
    
    def unlock_schema_(self) -> None:
        """Unlock output schema (Rails bang method pattern)"""
        self.output_schema_locked = False
        self.updated_at = datetime.now()
    
    def increment_processed_records_(self, count: int = 1) -> None:
        """Increment processed records count (Rails bang method pattern)"""
        self.records_processed += count
        self.last_processed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def increment_failed_records_(self, count: int = 1) -> None:
        """Increment failed records count (Rails bang method pattern)"""
        self.records_failed += count
        self.updated_at = datetime.now()
    
    # ========================================
    # Rails Class Methods and Scopes
    # ========================================
    
    @classmethod
    def active(cls):
        """Scope for active data sets (Rails scope pattern)"""
        from sqlalchemy import and_
        return and_(cls.status == DataSetStatuses.ACTIVE, cls.is_disabled.is_(False))
    
    @classmethod
    def paused(cls):
        """Scope for paused data sets (Rails scope pattern)"""
        return cls.status == DataSetStatuses.PAUSED
    
    @classmethod
    def failed(cls):
        """Scope for failed data sets (Rails scope pattern)"""
        return cls.status.in_([DataSetStatuses.FAILED, DataSetStatuses.ERROR])
    
    @classmethod
    def public(cls):
        """Scope for public data sets (Rails scope pattern)"""
        return cls.public.is_(True)
    
    @classmethod
    def private(cls):
        """Scope for private data sets (Rails scope pattern)"""
        from sqlalchemy import and_
        return and_(cls.public.is_(False), cls.access_level == "private")
    
    @classmethod
    def splitters(cls):
        """Scope for splitter data sets (Rails scope pattern)"""
        return cls.data_set_type == DataSetTypes.SPLITTER
    
    @classmethod
    def by_owner(cls, owner_id: int):
        """Scope for data sets by owner (Rails scope pattern)"""
        return cls.owner_id == owner_id
    
    @classmethod
    def by_org(cls, org_id: int):
        """Scope for data sets by organization (Rails scope pattern)"""
        return cls.org_id == org_id
    
    @classmethod
    def by_type(cls, data_set_type: DataSetTypes):
        """Scope for data sets by type (Rails scope pattern)"""
        return cls.data_set_type == data_set_type
    
    @classmethod
    def recent(cls, days: int = 7):
        """Scope for recent data sets (Rails scope pattern)"""
        from sqlalchemy import and_
        cutoff_date = datetime.now() - timedelta(days=days)
        return cls.created_at >= cutoff_date
    
    @classmethod
    def with_errors(cls):
        """Scope for data sets with processing errors (Rails scope pattern)"""
        return cls.processing_errors.isnot(None)
    
    @classmethod
    def performance_issues(cls):
        """Scope for data sets with performance issues (Rails scope pattern)"""
        return cls.avg_processing_time_ms > cls.PERFORMANCE_THRESHOLD_MS
    
    @classmethod
    def backend_resource_name(cls) -> str:
        """Get backend resource name (Rails pattern)"""
        return "dataset"
    
    @classmethod
    def shared_with_any_in_org(cls, org_id: int):
        """Get data sets shared with any user in org (Rails pattern)"""
        if not org_id:
            return []
        # Implementation would query access control when available
        return []
    
    @classmethod
    def shared_with_user(cls, user_id: int, org_id: int = None):
        """Get data sets shared with specific user (Rails pattern)"""
        if not user_id:
            return []
        # Implementation would query access control when available
        return []
    
    @classmethod
    def derived_from_shared_or_public(cls, user_id: int = None, org_id: int = None):
        """Get data sets derived from shared or public data sets (Rails pattern)"""
        # Implementation would query for derived data sets when access control is available
        return []
    
    @classmethod
    def create_with_defaults(cls, owner, org, **kwargs):
        """Factory method to create data set with defaults (Rails pattern)"""
        data_set = cls(
            owner=owner,
            org=org,
            status=DataSetStatuses.INIT,
            **kwargs
        )
        return data_set
    
    @classmethod
    def create_from_template(cls, template, owner, org, **overrides):
        """Factory method to create data set from template (Rails pattern)"""
        if not template or not template.template_():
            raise ValueError("Invalid template provided")
        
        data_set_data = template.to_template_dict()
        data_set_data.update(overrides)
        data_set_data.update({
            'owner': owner,
            'org': org,
            'copied_from': template,
            'status': DataSetStatuses.INIT
        })
        
        return cls(**data_set_data)
    
    @classmethod
    def bulk_activate(cls, data_set_ids: List[int]):
        """Bulk activate multiple data sets (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_pause(cls, data_set_ids: List[int]):
        """Bulk pause multiple data sets (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    # ========================================
    # Rails Instance Methods
    # ========================================
    
    def copy_to_org(self, target_org, owner=None):
        """Copy data set to another organization (Rails pattern)"""
        if not self.can_be_copied_():
            raise ValueError("Data set cannot be copied")
        
        copy_data = self.to_copy_dict()
        copy_data.update({
            'org': target_org,
            'owner': owner or target_org.admin_users[0],
            'copied_from': self,
            'status': DataSetStatuses.INIT,
            'name': f"{self.name} (Copy)"
        })
        
        return self.__class__(**copy_data)
    
    def create_child(self, **child_attributes):
        """Create child data set (Rails pattern)"""
        child_data = {
            'parent_data_set': self,
            'org': self.org,
            'owner': self.owner,
            'data_source': self.data_source,
            'status': DataSetStatuses.INIT,
            **child_attributes
        }
        
        return self.__class__(**child_data)
    
    def get_root_parent(self):
        """Get root parent data set (Rails pattern)"""
        current = self
        while current.parent_data_set:
            current = current.parent_data_set
        return current
    
    def get_all_descendants(self) -> List['DataSet']:
        """Get all descendant data sets recursively (Rails pattern)"""
        descendants = []
        for child in self.child_data_sets:
            descendants.append(child)
            descendants.extend(child.get_all_descendants())
        return descendants
    
    def get_sibling_data_sets(self) -> List['DataSet']:
        """Get sibling data sets (Rails pattern)"""
        if not self.parent_data_set:
            return []
        return [child for child in self.parent_data_set.child_data_sets if child.id != self.id]
    
    def calculate_depth(self) -> int:
        """Calculate depth in data set hierarchy (Rails pattern)"""
        depth = 0
        current = self
        while current.parent_data_set:
            depth += 1
            current = current.parent_data_set
        return depth
    
    def add_tag(self, tag_name: str) -> None:
        """Add tag to data set (Rails pattern)"""
        if not self.tags:
            self.tags = []
        if tag_name not in self.tags:
            self.tags.append(tag_name)
            self.updated_at = datetime.now()
    
    def remove_tag(self, tag_name: str) -> None:
        """Remove tag from data set (Rails pattern)"""
        if self.tags and tag_name in self.tags:
            self.tags.remove(tag_name)
            self.updated_at = datetime.now()
    
    def has_tag(self, tag_name: str) -> bool:
        """Check if data set has specific tag (Rails pattern)"""
        return bool(self.tags and tag_name in self.tags)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        return self.tags or []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    def get_nexset_api_config(self) -> Optional[Dict[str, Any]]:
        """Get Nexset API configuration with caching (Rails pattern)"""
        cache_key = 'nexset_api_config'
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        if self.splitter_() or self.upstream_has_splitter_():
            return None
        
        config = None
        if self.origin_node:
            parent_source = self.parent_source()
        else:
            parent_source = self.flow_origin()
        
        if hasattr(parent_source, 'nexset_api_config'):
            config = parent_source.nexset_api_config
        
        self._cache[cache_key] = config
        return config
    
    def parent_source(self):
        """Get parent data source by traversing flow (Rails pattern)"""
        # Implementation would traverse flow nodes to find parent source
        if self.origin_node:
            return self.origin_node.get_parent_source()
        return None
    
    def flow_origin(self):
        """Get flow origin (Rails pattern)"""
        # Implementation would find the origin of the flow
        if self.flow_node:
            return self.flow_node.get_flow_origin()
        return None
    
    def requires_unique_source_schema_validation(self) -> bool:
        """Check if unique source schema validation is required (Rails pattern)"""
        return (self.requires_validation_() and 
                self.schema_validation_mode == SchemaValidationModes.STRICT)
    
    def update_performance_metrics(self, processing_time_ms: float) -> None:
        """Update performance metrics (Rails pattern)"""
        self.total_processing_time_ms += processing_time_ms
        
        # Calculate running average
        if self.records_processed > 0:
            self.avg_processing_time_ms = self.total_processing_time_ms / self.records_processed
        
        self.last_processed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get processing performance summary (Rails pattern)"""
        total_records = self.records_processed + self.records_failed
        success_rate = (self.records_processed / total_records * 100) if total_records > 0 else 0
        
        return {
            'records_processed': self.records_processed,
            'records_failed': self.records_failed,
            'total_records': total_records,
            'success_rate_percent': round(success_rate, 2),
            'avg_processing_time_ms': self.avg_processing_time_ms,
            'total_processing_time_ms': self.total_processing_time_ms,
            'has_performance_issues': self.performance_issues_(),
            'last_processed_at': self.last_processed_at.isoformat() if self.last_processed_at else None
        }
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary (Rails pattern)"""
        error_count = len(self.processing_errors) if self.processing_errors else 0
        recent_errors = []
        
        if self.processing_errors:
            # Get recent errors (last 5)
            recent_errors = sorted(
                self.processing_errors, 
                key=lambda x: x.get('timestamp', ''), 
                reverse=True
            )[:5]
        
        return {
            'has_errors': self.has_errors_(),
            'error_count': error_count,
            'recent_errors': recent_errors,
            'error_types': list(set(e.get('error_type', 'unknown') for e in (self.processing_errors or [])))
        }
    
    # ========================================
    # Rails Validation and Display Methods
    # ========================================
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        return self.name or f"DataSet #{self.id}"
    
    def display_status(self) -> str:
        """Get formatted status for display (Rails pattern)"""
        return self.status.value.replace('_', ' ').title()
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            DataSetStatuses.ACTIVE: 'green',
            DataSetStatuses.PROCESSING: 'blue',
            DataSetStatuses.COMPLETED: 'green',
            DataSetStatuses.PAUSED: 'yellow',
            DataSetStatuses.FAILED: 'red',
            DataSetStatuses.ERROR: 'red',
            DataSetStatuses.SUSPENDED: 'orange',
            DataSetStatuses.ARCHIVED: 'gray',
            DataSetStatuses.DRAFT: 'purple',
            DataSetStatuses.PUBLISHED: 'teal'
        }
        return status_colors.get(self.status, 'gray')
    
    def validate_for_processing(self) -> Tuple[bool, List[str]]:
        """Validate data set can be processed (Rails pattern)"""
        errors = []
        
        if not self.can_be_processed_():
            errors.append(f"Data set cannot be processed in {self.status} status")
        
        if not self.owner:
            errors.append("Data set must have an owner")
        
        if not self.org:
            errors.append("Data set must belong to an organization")
        
        if self.requires_validation_() and not self.has_schema_():
            errors.append("Schema is required for validation mode")
        
        if self.schema_locked_() and not self.output_schema:
            errors.append("Output schema is required when schema is locked")
        
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
            'data_set_type': self.data_set_type.value,
            'runtime_status': self.runtime_status,
            'active': self.active_(),
            'paused': self.paused_(),
            'processing': self.processing_(),
            'completed': self.completed_(),
            'failed': self.failed_(),
            'healthy': self.healthy_(),
            'public': self.public_(),
            'shared': self.shared_(),
            'template': self.template_(),
            'has_children': self.has_children_(),
            'has_parent': self.has_parent_(),
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
            'data_source_id': self.data_source_id,
            'flow_node_id': self.flow_node_id,
            'parent_data_set_id': self.parent_data_set_id,
            'schema_validation_mode': self.schema_validation_mode.value,
            'output_format': self.output_format.value,
            'output_schema_locked': self.output_schema_locked,
            'schema_version': self.schema_version,
            'access_level': self.access_level,
            'external_id': self.external_id,
            'metadata': self.extra_metadata,
            'processing_summary': self.get_processing_summary(),
            'error_summary': self.get_error_summary(),
            'relationships': {
                'parent_name': self.parent_data_set.name if self.parent_data_set else None,
                'child_count': len(self.child_data_sets or []),
                'data_source_name': self.data_source.name if self.data_source else None,
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
            'data_set_type': self.data_set_type,
            'schema_validation_mode': self.schema_validation_mode,
            'output_format': self.output_format,
            'transform_config': self.transform_config,
            'custom_config': self.custom_config,
            'source_schema': self.source_schema,
            'output_schema': self.output_schema,
            'tags': self.tags_list(),
            'metadata': self.extra_metadata or {}
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
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'public': self.public,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'records_processed': self.records_processed,
            'records_failed': self.records_failed
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
        if 'processing_times' in self._performance_metrics:
            times = self._performance_metrics['processing_times']
            if times:
                self.avg_processing_time_ms = sum(times) / len(times)
                self.total_processing_time_ms += sum(times)
    
    def __repr__(self) -> str:
        return f"<DataSet(id={self.id}, name='{self.name}', status='{self.status.value}', org_id={self.org_id})>"
    
    def __str__(self) -> str:
        return f"DataSet: {self.display_name()} ({self.display_status()})"