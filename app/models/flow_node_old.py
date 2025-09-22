from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Enum as SQLEnum
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Set
from enum import Enum as PyEnum
from sqlalchemy.orm import Session
import secrets
import re
import os
import json
from ..database import Base

# Flow Type Enums (Rails API_FLOW_TYPES)
class FlowType(PyEnum):
    STREAMING = "streaming"
    BATCH = "batch"
    API_SERVER = "api_server"
    IN_MEMORY = "in_memory"
    ELT = "elt"
    REAL_TIME = "real_time"
    MICRO_BATCH = "micro_batch"

# Ingestion Mode Enums (Rails API_INGESTION_MODES)
class IngestionMode(PyEnum):
    BATCH = "BATCH"
    STREAMING = "STREAMING"
    INCREMENTAL = "INCREMENTAL"
    WEBHOOK = "WEBHOOK"
    CDC = "CDC"  # Change Data Capture
    TRIGGER = "TRIGGER"

# Node Status Enums
class NodeStatuses(PyEnum):
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

# Node Types
class NodeTypes(PyEnum):
    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"
    FILTER = "filter"
    JOIN = "join"
    AGGREGATE = "aggregate"
    SPLIT = "split"
    MERGE = "merge"
    CUSTOM = "custom"

class FlowNode(Base):
    __tablename__ = "flow_nodes_old"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255))
    flow_type = Column(SQLEnum(FlowType), nullable=False, default=FlowType.STREAMING)
    ingestion_mode = Column(SQLEnum(IngestionMode), nullable=False, default=IngestionMode.BATCH)
    status = Column(SQLEnum(NodeStatuses), nullable=False, default=NodeStatuses.INIT)
    node_type = Column(SQLEnum(NodeTypes), nullable=False, default=NodeTypes.TRANSFORM)
    
    # Node metadata
    uid = Column(String(24), unique=True, index=True)
    description = Column(Text)
    version = Column(String(50), default="1.0")
    
    # Processing configuration
    parallel_processing = Column(Boolean, default=False)
    max_parallel_tasks = Column(Integer, default=1)
    batch_size = Column(Integer, default=1000)
    processing_timeout = Column(Integer, default=3600)  # seconds
    retry_count = Column(Integer, default=3)
    
    # Performance metrics
    records_processed = Column(Integer, default=0)
    bytes_processed = Column(Integer, default=0)
    processing_time_ms = Column(Integer)
    avg_processing_time_ms = Column(Integer)
    last_processed_at = Column(DateTime)
    
    # Error handling
    error_count = Column(Integer, default=0)
    last_error = Column(Text)
    last_error_at = Column(DateTime)
    
    # Scheduling and dependencies
    schedule_config = Column(JSON)
    depends_on_node_ids = Column(JSON)  # List of node IDs this depends on
    
    # Configuration and metadata
    config = Column(JSON)  # Node-specific configuration
    extra_metadata = Column(Text)  # JSON string of additional metadata
    tags = Column(Text)  # JSON string of tags
    
    # State flags
    is_disabled = Column(Boolean, default=False)
    is_template = Column(Boolean, default=False)
    skip_on_error = Column(Boolean, default=False)
    auto_retry = Column(Boolean, default=True)
    
    # Node relationships
    origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"))
    parent_node_id = Column(Integer, ForeignKey("flow_nodes.id"))
    shared_origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"))
    copied_from_id = Column(Integer, ForeignKey("flow_nodes.id"))
    
    # Resource references
    data_source_id = Column(Integer, ForeignKey("data_sources.id"))
    data_set_id = Column(Integer, ForeignKey("data_sets.id")) 
    data_sink_id = Column(Integer, ForeignKey("data_sinks.id"))
    
    # Additional Rails fields
    cluster_id = Column(Integer, ForeignKey("clusters.id"))
    project_id = Column(Integer, ForeignKey("projects.id"))
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Foreign keys
    flow_id = Column(Integer, ForeignKey("flows.id"), nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    org = relationship("Org")
    flow = relationship("Flow", back_populates="flow_nodes")
    project = relationship("Project", back_populates="flow_nodes")
    cluster = relationship("Cluster", foreign_keys=[cluster_id])
    
    # Node relationships
    origin_node = relationship("FlowNode", remote_side=[id], foreign_keys=[origin_node_id])
    parent_node = relationship("FlowNode", remote_side=[id], foreign_keys=[parent_node_id])
    shared_origin_node = relationship("FlowNode", remote_side=[id], foreign_keys=[shared_origin_node_id])
    copied_from = relationship("FlowNode", remote_side=[id], foreign_keys=[copied_from_id])
    child_nodes = relationship("FlowNode", remote_side=[parent_node_id], viewonly=True)
    
    # Resource relationships
    data_source = relationship("DataSource", foreign_keys=[data_source_id])
    data_set = relationship("DataSet", foreign_keys=[data_set_id])
    data_sink = relationship("DataSink", foreign_keys=[data_sink_id])

    # Rails business logic attributes
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.ensure_uid_()
        self.in_copy = False
        self._runtime_status = None
        self._processing_stats = None
    
    # Rails constants
    FLOW_TYPES = {
        "streaming": "streaming",
        "batch": "batch", 
        "api_server": "api_server",
        "in_memory": "in_memory",
        "elt": "elt",
        "real_time": "real_time",
        "micro_batch": "micro_batch"
    }
    
    INGESTION_MODES = {
        "batch": "BATCH",
        "streaming": "STREAMING",
        "incremental": "INCREMENTAL",
        "webhook": "WEBHOOK",
        "cdc": "CDC",
        "trigger": "TRIGGER"
    }
    
    CORE_MODELS = ["DataSet", "DataSink", "DataSource"]
    ACCESSIBLE_MODELS = ["CodeContainer", "DataCredentials"] + CORE_MODELS
    
    CONDENSED_SELECT_FIELDS = [
        "id", "origin_node_id", "parent_node_id", "shared_origin_node_id",
        "owner_id", "org_id", "cluster_id", "status",
        "ingestion_mode", "flow_type",
        "data_source_id", "data_set_id", "data_sink_id"
    ]
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if flow node is active (Rails pattern)"""
        return self.status == NodeStatuses.ACTIVE and not self.is_disabled
    
    def inactive_(self) -> bool:
        """Check if flow node is inactive (Rails pattern)"""
        return not self.active_()
    
    def paused_(self) -> bool:
        """Check if flow node is paused (Rails pattern)"""
        return self.status == NodeStatuses.PAUSED
        
    def stopped_(self) -> bool:
        """Check if flow node is stopped (Rails pattern)"""
        return self.status == NodeStatuses.STOPPED
        
    def failed_(self) -> bool:
        """Check if flow node has failed (Rails pattern)"""
        return self.status == NodeStatuses.FAILED
        
    def processing_(self) -> bool:
        """Check if flow node is processing (Rails pattern)"""
        return self.status == NodeStatuses.PROCESSING
        
    def completed_(self) -> bool:
        """Check if flow node is completed (Rails pattern)"""
        return self.status == NodeStatuses.COMPLETED
        
    def error_(self) -> bool:
        """Check if flow node has error (Rails pattern)"""
        return self.status == NodeStatuses.ERROR
        
    def suspended_(self) -> bool:
        """Check if flow node is suspended (Rails pattern)"""
        return self.status == NodeStatuses.SUSPENDED
        
    def archived_(self) -> bool:
        """Check if flow node is archived (Rails pattern)"""
        return self.status == NodeStatuses.ARCHIVED
    
    def disabled_(self) -> bool:
        """Check if flow node is disabled (Rails pattern)"""
        return self.is_disabled
        
    def enabled_(self) -> bool:
        """Check if flow node is enabled (Rails pattern)"""
        return not self.is_disabled
        
    def template_(self) -> bool:
        """Check if flow node is a template (Rails pattern)"""
        return self.is_template
    
    def same_origin_(self, other_node) -> bool:
        """Check if two nodes have the same origin (Rails pattern)"""
        if not other_node:
            return False
        return self.origin_node_id == other_node.origin_node_id
    
    def is_origin_(self) -> bool:
        """Check if this is an origin node (Rails pattern)"""
        return self.origin_node_id == self.id
        
    def is_child_node_(self) -> bool:
        """Check if this is a child node (Rails pattern)"""
        return self.parent_node_id is not None
        
    def has_children_(self) -> bool:
        """Check if node has children (Rails pattern)"""
        return bool(self.child_nodes)
        
    def has_dependencies_(self) -> bool:
        """Check if node has dependencies (Rails pattern)"""
        return bool(self.depends_on_node_ids)
        
    def dependencies_satisfied_(self) -> bool:
        """Check if all dependencies are satisfied (Rails pattern)"""
        if not self.has_dependencies_():
            return True
        # This would check that all dependent nodes are completed
        return True  # Placeholder
        
    def can_be_processed_(self) -> bool:
        """Check if node can be processed (Rails pattern)"""
        return (self.enabled_() and 
                self.dependencies_satisfied_() and
                self.status in [NodeStatuses.INIT, NodeStatuses.PAUSED] and
                not self.failed_())
        
    def can_be_paused_(self) -> bool:
        """Check if node can be paused (Rails pattern)"""
        return self.status in [NodeStatuses.ACTIVE, NodeStatuses.PROCESSING]
        
    def can_be_resumed_(self) -> bool:
        """Check if node can be resumed (Rails pattern)"""
        return self.paused_()
        
    def can_be_stopped_(self) -> bool:
        """Check if node can be stopped (Rails pattern)"""
        return self.status in [NodeStatuses.ACTIVE, NodeStatuses.PROCESSING, NodeStatuses.PAUSED]
    
    def has_errors_(self) -> bool:
        """Check if node has errors (Rails pattern)"""
        return self.error_count > 0 or bool(self.last_error)
        
    def healthy_(self) -> bool:
        """Check if node is healthy (Rails pattern)"""
        return (self.active_() and 
                not self.has_errors_() and
                not self.stale_())
        
    def stale_(self, hours: int = 24) -> bool:
        """Check if node is stale (Rails pattern)"""
        if not self.last_processed_at:
            return True
        return self.last_processed_at < datetime.now() - timedelta(hours=hours)
        
    def recently_processed_(self, hours: int = 1) -> bool:
        """Check if node was recently processed (Rails pattern)"""
        if not self.last_processed_at:
            return False
        return self.last_processed_at >= datetime.now() - timedelta(hours=hours)
        
    def performance_issues_(self) -> bool:
        """Check if node has performance issues (Rails pattern)"""
        if not self.avg_processing_time_ms or not self.processing_time_ms:
            return False
        # Consider performance issue if current time > 2x average
        return self.processing_time_ms > (self.avg_processing_time_ms * 2)
        
    def streaming_(self) -> bool:
        """Check if node uses streaming flow type (Rails pattern)"""
        return self.flow_type in [FlowType.STREAMING, FlowType.REAL_TIME]
        
    def batch_(self) -> bool:
        """Check if node uses batch flow type (Rails pattern)"""
        return self.flow_type in [FlowType.BATCH, FlowType.MICRO_BATCH]
        
    def source_node_(self) -> bool:
        """Check if this is a source node (Rails pattern)"""
        return self.node_type == NodeTypes.SOURCE or bool(self.data_source_id)
        
    def transform_node_(self) -> bool:
        """Check if this is a transform node (Rails pattern)"""
        return self.node_type == NodeTypes.TRANSFORM
        
    def sink_node_(self) -> bool:
        """Check if this is a sink node (Rails pattern)"""
        return self.node_type == NodeTypes.SINK or bool(self.data_sink_id)
    
    def parallel_processing_enabled_(self) -> bool:
        """Check if parallel processing is enabled (Rails pattern)"""
        return self.parallel_processing and self.max_parallel_tasks > 1
    
    def accessible_by_(self, user, access_level: str = 'read') -> bool:
        """Check if user can access node (Rails pattern)"""
        if not user:
            return False
            
        # Owner always has access
        if self.owner_id == user.id:
            return True
            
        # Flow members have access based on flow permissions
        if self.flow and hasattr(self.flow, 'accessible_by_'):
            return self.flow.accessible_by_(user, access_level)
            
        # Project members have access based on project permissions
        if self.project and hasattr(self.project, 'accessible_by_'):
            return self.project.accessible_by_(user, access_level)
            
        # Org members have read access
        if access_level == 'read' and user.org_id == self.org_id:
            return True
            
        return False
    
    def editable_by_(self, user) -> bool:
        """Check if user can edit node (Rails pattern)"""
        return self.accessible_by_(user, 'write')
    
    def deletable_by_(self, user) -> bool:
        """Check if user can delete node (Rails pattern)"""
        return (self.accessible_by_(user, 'admin') and 
                self.can_be_stopped_() and
                not self.has_children_())

    # Rails-style bang methods (state changes)
    def activate_(self) -> None:
        """Activate node (Rails bang method pattern)"""
        if self.disabled_():
            self.is_disabled = False
            
        self.status = NodeStatuses.ACTIVE
        self.updated_at = datetime.now()
    
    def pause_(self, reason: Optional[str] = None) -> None:
        """Pause node (Rails bang method pattern)"""
        if not self.can_be_paused_():
            raise ValueError(f"Node cannot be paused. Status: {self.status}")
            
        self.status = NodeStatuses.PAUSED
        if reason:
            self._update_metadata('pause_reason', reason)
        self.updated_at = datetime.now()
    
    def resume_(self) -> None:
        """Resume paused node (Rails bang method pattern)"""
        if not self.can_be_resumed_():
            raise ValueError(f"Node cannot be resumed. Status: {self.status}")
            
        self.status = NodeStatuses.ACTIVE
        self.updated_at = datetime.now()
    
    def stop_(self, reason: Optional[str] = None) -> None:
        """Stop node (Rails bang method pattern)"""
        if not self.can_be_stopped_():
            raise ValueError(f"Node cannot be stopped. Status: {self.status}")
            
        self.status = NodeStatuses.STOPPED
        if reason:
            self._update_metadata('stop_reason', reason)
        self.updated_at = datetime.now()
    
    def fail_(self, error: str, error_details: Optional[Dict] = None) -> None:
        """Mark node as failed (Rails bang method pattern)"""
        self.status = NodeStatuses.FAILED
        self.error_count = (self.error_count or 0) + 1
        self.last_error = error
        self.last_error_at = datetime.now()
        
        if error_details:
            self._update_metadata('error_details', error_details)
        
        self.updated_at = datetime.now()
    
    def complete_(self) -> None:
        """Mark node as completed (Rails bang method pattern)"""
        self.status = NodeStatuses.COMPLETED
        self.updated_at = datetime.now()
    
    def start_processing_(self) -> None:
        """Start node processing (Rails bang method pattern)"""
        if not self.can_be_processed_():
            raise ValueError(f"Node cannot be processed. Status: {self.status}")
            
        self.status = NodeStatuses.PROCESSING
        self.last_processed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def suspend_(self, reason: Optional[str] = None) -> None:
        """Suspend node (Rails bang method pattern)"""
        self.status = NodeStatuses.SUSPENDED
        if reason:
            self._update_metadata('suspension_reason', reason)
        self.updated_at = datetime.now()
    
    def archive_(self, reason: Optional[str] = None) -> None:
        """Archive node (Rails bang method pattern)"""
        self.status = NodeStatuses.ARCHIVED
        if reason:
            self._update_metadata('archive_reason', reason)
        self.updated_at = datetime.now()
    
    def disable_(self, reason: Optional[str] = None) -> None:
        """Disable node (Rails bang method pattern)"""
        self.is_disabled = True
        if reason:
            self._update_metadata('disable_reason', reason)
        self.updated_at = datetime.now()
    
    def enable_(self) -> None:
        """Enable node (Rails bang method pattern)"""
        self.is_disabled = False
        self.updated_at = datetime.now()
    
    def reset_errors_(self) -> None:
        """Reset error state (Rails bang method pattern)"""
        self.error_count = 0
        self.last_error = None
        self.last_error_at = None
        if self.failed_() or self.error_():
            self.status = NodeStatuses.INIT
        self.updated_at = datetime.now()
    
    def track_processing_(self, records: int, bytes_processed: int, processing_time_ms: int) -> None:
        """Track processing metrics (Rails bang method pattern)"""
        self.records_processed = (self.records_processed or 0) + records
        self.bytes_processed = (self.bytes_processed or 0) + bytes_processed
        self.processing_time_ms = processing_time_ms
        
        # Update running average
        if not self.avg_processing_time_ms:
            self.avg_processing_time_ms = processing_time_ms
        else:
            # Simple moving average
            self.avg_processing_time_ms = int((self.avg_processing_time_ms + processing_time_ms) / 2)
        
        self.last_processed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def flow_pause_(self, opts: Dict[str, Any] = None) -> None:
        """Pause flow (Rails bang method pattern)"""
        if opts is None:
            opts = {"all": False}
        
        target_node = self.origin_node if opts.get("all") else self
        if target_node:
            target_node.flow_activate_traverse_(False)
    
    def flow_activate_traverse_(self, activate: bool = True) -> None:
        """Traverse and activate/deactivate flow nodes (Rails bang method pattern)"""
        new_status = NodeStatuses.ACTIVE if activate else NodeStatuses.PAUSED
        
        # Update this node
        self.status = new_status
        
        # Traverse child nodes with same origin
        for child in self.child_nodes or []:
            if child.same_origin_(self):
                child.flow_activate_traverse_(activate)
    
    def make_template_(self, template_name: Optional[str] = None) -> None:
        """Convert node to template (Rails bang method pattern)"""
        self.is_template = True
        if template_name:
            self.name = template_name
        self.updated_at = datetime.now()
    
    # Rails helper and utility methods
    def ensure_uid_(self) -> None:
        """Ensure unique UID is set (Rails before_save pattern)"""
        if self.uid:
            return
        
        max_attempts = 10
        for _ in range(max_attempts):
            uid = secrets.token_hex(12)  # 24 character hex string
            if not self.__class__.query.filter_by(uid=uid).first():
                self.uid = uid
                return
        
        raise ValueError("Failed to generate unique node UID")
    
    def _update_metadata(self, key: str, value: Any) -> None:
        """Update metadata field (Rails helper pattern)"""
        try:
            current_meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
        except (json.JSONDecodeError, TypeError):
            current_meta = {}
            
        current_meta[key] = value
        self.extra_metadata = json.dumps(current_meta)
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Get metadata value (Rails helper pattern)"""
        try:
            meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
            return meta.get(key, default)
        except (json.JSONDecodeError, TypeError):
            return default
    
    def get_dependency_ids(self) -> List[int]:
        """Get list of dependency node IDs (Rails helper pattern)"""
        if not self.depends_on_node_ids:
            return []
        try:
            return json.loads(self.depends_on_node_ids) if isinstance(self.depends_on_node_ids, str) else self.depends_on_node_ids
        except (json.JSONDecodeError, TypeError):
            return []
    
    def set_dependency_ids_(self, node_ids: List[int]) -> None:
        """Set dependency node IDs (Rails bang helper pattern)"""
        self.depends_on_node_ids = node_ids
        self.updated_at = datetime.now()
    
    def add_dependency_(self, node_id: int) -> None:
        """Add node dependency (Rails bang helper pattern)"""
        current_deps = self.get_dependency_ids()
        if node_id not in current_deps:
            current_deps.append(node_id)
            self.set_dependency_ids_(current_deps)
    
    def remove_dependency_(self, node_id: int) -> None:
        """Remove node dependency (Rails bang helper pattern)"""
        current_deps = self.get_dependency_ids()
        if node_id in current_deps:
            current_deps.remove(node_id)
            self.set_dependency_ids_(current_deps)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        if not self.tags:
            return []
        try:
            return json.loads(self.tags)
        except (json.JSONDecodeError, TypeError):
            return []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    def set_tags_(self, tags: Union[List[str], Set[str], str]) -> None:
        """Set node tags (Rails bang method pattern)"""
        if isinstance(tags, str):
            # Handle comma-separated string
            tag_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
        else:
            tag_list = list(set(tags)) if tags else []
        
        self.tags = json.dumps(tag_list) if tag_list else None
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add a tag to node (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.add(tag.strip())
        self.set_tags_(current_tags)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove a tag from node (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.discard(tag.strip())
        self.set_tags_(current_tags)
    
    def has_tag_(self, tag: str) -> bool:
        """Check if node has specific tag (Rails pattern)"""
        return tag.strip() in self.tags_list()
    
    def processing_rate_per_second(self) -> float:
        """Calculate processing rate (Rails pattern)"""
        if not self.processing_time_ms or not self.records_processed:
            return 0.0
        return (self.records_processed * 1000.0) / self.processing_time_ms
    
    def estimated_completion_time(self, remaining_records: int) -> Optional[datetime]:
        """Estimate completion time (Rails pattern)"""
        rate = self.processing_rate_per_second()
        if rate <= 0:
            return None
        
        seconds_remaining = remaining_records / rate
        return datetime.now() + timedelta(seconds=seconds_remaining)
    
    def performance_summary(self) -> Dict[str, Any]:
        """Get performance summary (Rails pattern)"""
        return {
            'records_processed': self.records_processed or 0,
            'bytes_processed': self.bytes_processed or 0,
            'processing_time_ms': self.processing_time_ms,
            'avg_processing_time_ms': self.avg_processing_time_ms,
            'processing_rate_per_second': self.processing_rate_per_second(),
            'last_processed_at': self.last_processed_at.isoformat() if self.last_processed_at else None,
            'error_count': self.error_count or 0,
            'has_performance_issues': self.performance_issues_()
        }
    
    @property
    def resource(self):
        """Get the associated resource (Rails pattern)"""
        if self.data_source:
            return self.data_source
        elif self.data_set:
            return self.data_set
        elif self.data_sink:
            return self.data_sink
        return None
    
    # Rails class methods and validation
    @classmethod
    def flow_types_enum(cls) -> str:
        """Get SQL ENUM string for flow types (Rails pattern)"""
        values = [f"'{v}'" for v in cls.FLOW_TYPES.values()]
        return f"ENUM({','.join(values)})"
    
    @classmethod
    def default_flow_type(cls) -> str:
        """Get default flow type (Rails pattern)"""
        return cls.FLOW_TYPES["streaming"]
    
    @classmethod
    def validate_flow_type(cls, flow_type: str) -> Optional[str]:
        """Validate flow type with ELT checking (Rails pattern)"""
        if not isinstance(flow_type, str):
            return None
        
        # Convert dot form to underscore form
        ft_key = flow_type.lower().replace(".", "_")
        
        # Check if ELT flows are enabled (Rails ENV check)
        enable_elt = os.environ.get("API_ENABLE_ELT_FLOWS", "true").lower() == "true"
        
        if not enable_elt and ft_key == "elt":
            return None
        
        return cls.FLOW_TYPES.get(ft_key)
    
    @classmethod
    def build_flow_from_data_source(cls, data_source):
        """Build flow from data source (Rails pattern)"""
        if not hasattr(data_source, '__class__') or data_source.__class__.__name__ != 'DataSource':
            raise ValueError(f"Invalid resource for data source node: {data_source.__class__.__name__}")
        
        if data_source.flow_node:
            return data_source.flow_node
        
        # Create flow node for data source
        flow_node = cls(
            owner=data_source.owner,
            org=data_source.org,
            data_source=data_source,
            flow_type=cls.default_flow_type(),
            ingestion_mode=IngestionMode.BATCH,
            node_type=NodeTypes.SOURCE
        )
        
        flow_node.origin_node_id = flow_node.id
        return flow_node
    
    # ====================
    # Rails Scopes
    # ====================
    
    @classmethod
    def active(cls, db: Session):
        """Rails scope: Get active flow nodes"""
        return db.query(cls).filter(cls.status == NodeStatuses.ACTIVE, cls.is_disabled == False)
    
    @classmethod
    def inactive(cls, db: Session):
        """Rails scope: Get inactive flow nodes"""
        return db.query(cls).filter(
            (cls.status != NodeStatuses.ACTIVE) | (cls.is_disabled == True)
        )
    
    @classmethod
    def paused(cls, db: Session):
        """Rails scope: Get paused nodes"""
        return db.query(cls).filter(cls.status == NodeStatuses.PAUSED)
    
    @classmethod
    def failed(cls, db: Session):
        """Rails scope: Get failed nodes"""
        return db.query(cls).filter(cls.status == NodeStatuses.FAILED)
    
    @classmethod
    def processing(cls, db: Session):
        """Rails scope: Get processing nodes"""
        return db.query(cls).filter(cls.status == NodeStatuses.PROCESSING)
    
    @classmethod
    def completed(cls, db: Session):
        """Rails scope: Get completed nodes"""
        return db.query(cls).filter(cls.status == NodeStatuses.COMPLETED)
    
    @classmethod
    def suspended(cls, db: Session):
        """Rails scope: Get suspended nodes"""
        return db.query(cls).filter(cls.status == NodeStatuses.SUSPENDED)
    
    @classmethod
    def archived(cls, db: Session):
        """Rails scope: Get archived nodes"""
        return db.query(cls).filter(cls.status == NodeStatuses.ARCHIVED)
    
    @classmethod
    def enabled(cls, db: Session):
        """Rails scope: Get enabled nodes"""
        return db.query(cls).filter(cls.is_disabled == False)
    
    @classmethod
    def disabled(cls, db: Session):
        """Rails scope: Get disabled nodes"""
        return db.query(cls).filter(cls.is_disabled == True)
    
    @classmethod
    def templates(cls, db: Session):
        """Rails scope: Get template nodes"""
        return db.query(cls).filter(cls.is_template == True)
    
    @classmethod
    def origins(cls, db: Session):
        """Rails scope: Get origin nodes"""
        return db.query(cls).filter(cls.origin_node_id == cls.id)
    
    @classmethod
    def sources(cls, db: Session):
        """Rails scope: Get source nodes"""
        return db.query(cls).filter(
            (cls.node_type == NodeTypes.SOURCE) | (cls.data_source_id.isnot(None))
        )
    
    @classmethod
    def transforms(cls, db: Session):
        """Rails scope: Get transform nodes"""
        return db.query(cls).filter(cls.node_type == NodeTypes.TRANSFORM)
    
    @classmethod
    def sinks(cls, db: Session):
        """Rails scope: Get sink nodes"""
        return db.query(cls).filter(
            (cls.node_type == NodeTypes.SINK) | (cls.data_sink_id.isnot(None))
        )
    
    @classmethod
    def streaming(cls, db: Session):
        """Rails scope: Get streaming nodes"""
        return db.query(cls).filter(
            cls.flow_type.in_([FlowType.STREAMING, FlowType.REAL_TIME])
        )
    
    @classmethod
    def batch(cls, db: Session):
        """Rails scope: Get batch nodes"""
        return db.query(cls).filter(
            cls.flow_type.in_([FlowType.BATCH, FlowType.MICRO_BATCH])
        )
    
    @classmethod
    def by_flow_type(cls, db: Session, flow_type: FlowType):
        """Rails scope: Get nodes by flow type"""
        return db.query(cls).filter(cls.flow_type == flow_type)
    
    @classmethod
    def by_node_type(cls, db: Session, node_type: NodeTypes):
        """Rails scope: Get nodes by node type"""
        return db.query(cls).filter(cls.node_type == node_type)
    
    @classmethod
    def by_status(cls, db: Session, status: NodeStatuses):
        """Rails scope: Get nodes by status"""
        return db.query(cls).filter(cls.status == status)
    
    @classmethod
    def by_owner(cls, db: Session, user_id: int):
        """Rails scope: Get nodes by owner"""
        return db.query(cls).filter(cls.owner_id == user_id)
    
    @classmethod
    def by_org(cls, db: Session, org_id: int):
        """Rails scope: Get nodes by organization"""
        return db.query(cls).filter(cls.org_id == org_id)
    
    @classmethod
    def by_flow(cls, db: Session, flow_id: int):
        """Rails scope: Get nodes by flow"""
        return db.query(cls).filter(cls.flow_id == flow_id)
    
    @classmethod
    def by_project(cls, db: Session, project_id: int):
        """Rails scope: Get nodes by project"""
        return db.query(cls).filter(cls.project_id == project_id)
    
    @classmethod
    def by_cluster(cls, db: Session, cluster_id: int):
        """Rails scope: Get nodes by cluster"""
        return db.query(cls).filter(cls.cluster_id == cluster_id)
    
    @classmethod
    def with_errors(cls, db: Session):
        """Rails scope: Get nodes with errors"""
        return db.query(cls).filter(
            (cls.error_count > 0) | (cls.last_error.isnot(None))
        )
    
    @classmethod
    def healthy(cls, db: Session):
        """Rails scope: Get healthy nodes"""
        return db.query(cls).filter(
            cls.status == NodeStatuses.ACTIVE,
            cls.is_disabled == False,
            (cls.error_count == 0) | (cls.error_count.is_(None)),
            cls.last_error.is_(None)
        )
    
    @classmethod
    def stale(cls, db: Session, hours: int = 24):
        """Rails scope: Get stale nodes"""
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return db.query(cls).filter(
            (cls.last_processed_at < cutoff) | (cls.last_processed_at.is_(None)),
            cls.status == NodeStatuses.ACTIVE
        )
    
    @classmethod
    def recently_processed(cls, db: Session, hours: int = 1):
        """Rails scope: Get recently processed nodes"""
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return db.query(cls).filter(cls.last_processed_at >= cutoff)
    
    @classmethod
    def with_dependencies(cls, db: Session):
        """Rails scope: Get nodes with dependencies"""
        return db.query(cls).filter(cls.depends_on_node_ids.isnot(None))
    
    @classmethod
    def without_dependencies(cls, db: Session):
        """Rails scope: Get nodes without dependencies"""
        return db.query(cls).filter(cls.depends_on_node_ids.is_(None))
    
    @classmethod
    def child_nodes_scope(cls, db: Session):
        """Rails scope: Get child nodes"""
        return db.query(cls).filter(cls.parent_node_id.isnot(None))
    
    @classmethod
    def root_nodes(cls, db: Session):
        """Rails scope: Get root nodes (no parent)"""
        return db.query(cls).filter(cls.parent_node_id.is_(None))
    
    @classmethod
    def by_uid(cls, db: Session, uid: str):
        """Rails scope: Find node by UID"""
        return db.query(cls).filter(cls.uid == uid)
    
    @classmethod
    def recent(cls, db: Session, days: int = 30):
        """Rails scope: Get recently created nodes"""
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(cls.created_at >= cutoff)
    
    # Rails class methods and scopes
    @classmethod
    def find_by_uid(cls, uid: str):
        """Find node by UID (Rails finder pattern)"""
        return cls.query.filter_by(uid=uid).first()
    
    @classmethod
    def find_by_uid_(cls, uid: str):
        """Find node by UID or raise exception (Rails bang finder pattern)"""
        node = cls.find_by_uid(uid)
        if not node:
            raise ValueError(f"FlowNode with UID '{uid}' not found")
        return node
    
    @classmethod
    def active_nodes(cls, flow=None, project=None):
        """Get active nodes (Rails scope pattern)"""
        query = cls.query.filter_by(status=NodeStatuses.ACTIVE, is_disabled=False)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if project:
            query = query.filter_by(project_id=project.id if hasattr(project, 'id') else project)
        return query
    
    @classmethod
    def failed_nodes(cls, flow=None, hours=24):
        """Get recently failed nodes (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            cls.status == NodeStatuses.FAILED,
            cls.last_error_at >= cutoff
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def processing_nodes(cls, flow=None):
        """Get currently processing nodes (Rails scope pattern)"""
        query = cls.query.filter_by(status=NodeStatuses.PROCESSING)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def source_nodes(cls, flow=None):
        """Get source nodes (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.node_type == NodeTypes.SOURCE) | (cls.data_source_id.isnot(None))
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def sink_nodes(cls, flow=None):
        """Get sink nodes (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.node_type == NodeTypes.SINK) | (cls.data_sink_id.isnot(None))
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def transform_nodes(cls, flow=None):
        """Get transform nodes (Rails scope pattern)"""
        query = cls.query.filter_by(node_type=NodeTypes.TRANSFORM)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def origin_nodes(cls, flow=None):
        """Get origin nodes only (Rails scope pattern)"""
        query = cls.query.filter(cls.origin_node_id == cls.id)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def child_nodes_of(cls, parent_node):
        """Get child nodes of parent (Rails scope pattern)"""
        parent_id = parent_node.id if hasattr(parent_node, 'id') else parent_node
        return cls.query.filter_by(parent_node_id=parent_id)
    
    @classmethod
    def stale_nodes(cls, hours=24, flow=None):
        """Get stale nodes (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            (cls.last_processed_at < cutoff) | (cls.last_processed_at.is_(None)),
            cls.status == NodeStatuses.ACTIVE
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def by_flow_type(cls, flow_type: FlowType, flow=None):
        """Get nodes by flow type (Rails scope pattern)"""
        query = cls.query.filter_by(flow_type=flow_type)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def streaming_nodes(cls, flow=None):
        """Get streaming nodes (Rails scope pattern)"""
        query = cls.query.filter(
            cls.flow_type.in_([FlowType.STREAMING, FlowType.REAL_TIME])
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def batch_nodes(cls, flow=None):
        """Get batch nodes (Rails scope pattern)"""
        query = cls.query.filter(
            cls.flow_type.in_([FlowType.BATCH, FlowType.MICRO_BATCH])
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        return query
    
    @classmethod
    def templates(cls, node_type=None):
        """Get template nodes (Rails scope pattern)"""
        query = cls.query.filter_by(is_template=True)
        if node_type:
            query = query.filter_by(node_type=node_type)
        return query
    
    @classmethod
    def accessible_to(cls, user, access_level: str = 'read'):
        """Get nodes accessible to user (Rails scope pattern)"""
        if not user:
            return cls.query.filter(False)  # Empty query
        
        # Start with user's own nodes
        query = cls.query.filter_by(owner_id=user.id)
        
        # Add org nodes if user has access
        if hasattr(user, 'org_id'):
            org_nodes = cls.query.filter_by(org_id=user.org_id)
            if access_level == 'read':
                query = query.union(org_nodes)
        
        return query.distinct()
    
    @classmethod
    def for_search_index(cls):
        """Get nodes for search index (Rails scope pattern)"""
        return cls.origin_nodes()
    
    @classmethod
    def search_ignored(cls):
        """Get search ignored nodes (Rails scope pattern)"""
        return cls.query.filter(cls.origin_node_id != cls.id)
    
    @classmethod
    def condensed_origins(cls):
        """Get condensed origin nodes (Rails scope pattern)"""
        return cls.origin_nodes().with_entities(
            *[getattr(cls, field) for field in cls.CONDENSED_SELECT_FIELDS]
        )
    
    def flow_copy(self, api_user_info: Dict[str, Any], options: Dict[str, Any] = None, 
                  fn=None, pfn=None):
        """Copy flow (Rails flow_copy pattern)"""
        if options is None:
            options = {}
        
        is_origin = fn is None
        fn = self.origin_node if is_origin else fn
        
        if (pfn and fn.resource and fn.resource.__class__.__name__ == 'DataSource' and
            not options.get('copy_dependent_data_flows')):
            if hasattr(pfn, 'unlink_dependent_source'):
                pfn.unlink_dependent_source()
            return None
        
        # Copy the resource (DataSource, DataSet, DataSink have copy methods)
        if hasattr(fn.resource, 'copy'):
            ds = fn.resource.copy(api_user_info, options)
        else:
            return None
        
        # Set up flow node relationships
        if is_origin:
            ds.flow_node.origin_node_id = ds.flow_node.id
            if pfn and hasattr(ds.flow_node, 'shared_origin_node_id'):
                ds.flow_node.shared_origin_node_id = pfn.shared_origin_node_id
        else:
            ds.flow_node.origin_node_id = pfn.origin_node_id
        
        ds.flow_node.parent_node_id = pfn.id if pfn else None
        
        # Recursively copy child nodes
        for child_node in fn.child_nodes or []:
            if child_node.same_origin_(fn):
                child_node.flow_copy(api_user_info, options, child_node, ds.flow_node)
        
        return ds
    
    def handle_after_save(self) -> None:
        """Handle after save callback (Rails pattern)"""
        # This would handle post-save logic
        pass
    
    def set_defaults(self, user, org) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        self.org = org
        self.flow_type = FlowType.STREAMING
        self.ingestion_mode = IngestionMode.BATCH
        self.status = NodeStatuses.INIT
        self.node_type = NodeTypes.TRANSFORM
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        if self.name:
            return self.name
        elif self.resource:
            return f"{self.resource.__class__.__name__} Node"
        else:
            return f"{self.node_type.value.title()} Node #{self.id}"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        status_map = {
            NodeStatuses.INIT: "Initialized",
            NodeStatuses.ACTIVE: "Active",
            NodeStatuses.PAUSED: "Paused",
            NodeStatuses.STOPPED: "Stopped",
            NodeStatuses.FAILED: "Failed",
            NodeStatuses.PROCESSING: "Processing",
            NodeStatuses.COMPLETED: "Completed",
            NodeStatuses.ERROR: "Error",
            NodeStatuses.SUSPENDED: "Suspended",
            NodeStatuses.ARCHIVED: "Archived"
        }
        return status_map.get(self.status, "Unknown")
    
    def flow_type_display(self) -> str:
        """Get human-readable flow type (Rails pattern)"""
        type_map = {
            FlowType.STREAMING: "Streaming",
            FlowType.BATCH: "Batch",
            FlowType.API_SERVER: "API Server",
            FlowType.IN_MEMORY: "In-Memory",
            FlowType.ELT: "ELT",
            FlowType.REAL_TIME: "Real-time",
            FlowType.MICRO_BATCH: "Micro-batch"
        }
        return type_map.get(self.flow_type, "Unknown")
    
    def node_type_display(self) -> str:
        """Get human-readable node type (Rails pattern)"""
        type_map = {
            NodeTypes.SOURCE: "Source",
            NodeTypes.TRANSFORM: "Transform",
            NodeTypes.SINK: "Sink",
            NodeTypes.FILTER: "Filter",
            NodeTypes.JOIN: "Join",
            NodeTypes.AGGREGATE: "Aggregate",
            NodeTypes.SPLIT: "Split",
            NodeTypes.MERGE: "Merge",
            NodeTypes.CUSTOM: "Custom"
        }
        return type_map.get(self.node_type, "Unknown")
    
    def health_status(self) -> str:
        """Get health status (Rails pattern)"""
        if self.healthy_():
            return "Healthy"
        elif self.has_errors_():
            return "Unhealthy"
        elif self.stale_():
            return "Stale"
        else:
            return "Unknown"
    
    def validate_(self) -> List[str]:
        """Validate node data (Rails validation pattern)"""
        errors = []
        
        if not self.owner_id:
            errors.append("Owner is required")
        
        if not self.org_id:
            errors.append("Organization is required")
        
        if self.batch_size and self.batch_size <= 0:
            errors.append("Batch size must be positive")
        
        if self.processing_timeout and self.processing_timeout <= 0:
            errors.append("Processing timeout must be positive")
        
        if self.max_parallel_tasks and self.max_parallel_tasks <= 0:
            errors.append("Max parallel tasks must be positive")
        
        if self.retry_count and self.retry_count < 0:
            errors.append("Retry count cannot be negative")
        
        # Validate that node has at least one resource reference
        if not any([self.data_source_id, self.data_set_id, self.data_sink_id]):
            if self.node_type not in [NodeTypes.TRANSFORM, NodeTypes.CUSTOM]:
                errors.append("Node must reference at least one resource")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if node is valid (Rails validation pattern)"""
        return len(self.validate_()) == 0
    
    def to_dict(self, include_performance: bool = False, include_metadata: bool = False,
               include_relationships: bool = False) -> Dict[str, Any]:
        """Convert flow node to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'description': self.description,
            'flow_type': self.flow_type.value if self.flow_type else None,
            'flow_type_display': self.flow_type_display(),
            'ingestion_mode': self.ingestion_mode.value if self.ingestion_mode else None,
            'node_type': self.node_type.value if self.node_type else None,
            'node_type_display': self.node_type_display(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'version': self.version,
            'is_origin': self.is_origin_(),
            'is_child_node': self.is_child_node_(),
            'is_template': self.is_template,
            'is_disabled': self.is_disabled,
            'active': self.active_(),
            'paused': self.paused_(),
            'healthy': self.healthy_(),
            'health_status': self.health_status(),
            'parallel_processing': self.parallel_processing,
            'max_parallel_tasks': self.max_parallel_tasks,
            'batch_size': self.batch_size,
            'origin_node_id': self.origin_node_id,
            'parent_node_id': self.parent_node_id,
            'shared_origin_node_id': self.shared_origin_node_id,
            'data_source_id': self.data_source_id,
            'data_set_id': self.data_set_id,
            'data_sink_id': self.data_sink_id,
            'cluster_id': self.cluster_id,
            'project_id': self.project_id,
            'flow_id': self.flow_id,
            'tags': self.tags_list(),
            'has_dependencies': self.has_dependencies_(),
            'has_children': self.has_children_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'last_processed_at': self.last_processed_at.isoformat() if self.last_processed_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id
        }
        
        if include_performance:
            result['performance_summary'] = self.performance_summary()
        
        if self.has_errors_():
            result.update({
                'error_count': self.error_count,
                'last_error': self.last_error,
                'last_error_at': self.last_error_at.isoformat() if self.last_error_at else None
            })
        
        if include_metadata and self.extra_metadata:
            try:
                result['metadata'] = json.loads(self.extra_metadata)
            except (json.JSONDecodeError, TypeError):
                pass
        
        if include_relationships:
            result.update({
                'owner': self.owner.to_dict() if self.owner else None,
                'org': self.org.to_dict() if self.org else None,
                'flow': self.flow.to_dict() if self.flow else None,
                'project': self.project.to_dict() if self.project else None,
                'origin_node': self.origin_node.to_dict() if self.origin_node else None,
                'parent_node': self.parent_node.to_dict() if self.parent_node else None,
                'resource': self.resource.to_dict() if self.resource else None
            })
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert node to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'node_type': self.node_type.value if self.node_type else None,
            'node_type_display': self.node_type_display(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'flow_type': self.flow_type.value if self.flow_type else None,
            'is_origin': self.is_origin_(),
            'healthy': self.healthy_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_processed_at': self.last_processed_at.isoformat() if self.last_processed_at else None
        }
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<FlowNode(id={self.id}, uid='{self.uid}', type='{self.node_type}', status='{self.status}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return self.display_name()