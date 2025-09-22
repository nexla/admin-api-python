from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Enum as SQLEnum, Index
from sqlalchemy.orm import relationship, sessionmaker, validates
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Set, Tuple
from enum import Enum as PyEnum
import secrets
import re
import os
import json
import time
import logging
from ..database import Base

logger = logging.getLogger(__name__)

class FlowType(PyEnum):
    STREAMING = "streaming"
    BATCH = "batch"
    API_SERVER = "api_server"
    IN_MEMORY = "in_memory"
    ELT = "elt"
    REAL_TIME = "real_time"
    MICRO_BATCH = "micro_batch"
    
    @property
    def display_name(self) -> str:
        return {
            self.STREAMING: "Streaming",
            self.BATCH: "Batch Processing",
            self.API_SERVER: "API Server",
            self.IN_MEMORY: "In-Memory Processing",
            self.ELT: "Extract-Load-Transform",
            self.REAL_TIME: "Real-time Processing",
            self.MICRO_BATCH: "Micro-batch Processing"
        }.get(self, "Unknown Flow Type")

class IngestionMode(PyEnum):
    BATCH = "BATCH"
    STREAMING = "STREAMING"
    INCREMENTAL = "INCREMENTAL"
    WEBHOOK = "WEBHOOK"
    CDC = "CDC"
    TRIGGER = "TRIGGER"
    
    @property
    def display_name(self) -> str:
        return {
            self.BATCH: "Batch Ingestion",
            self.STREAMING: "Streaming Ingestion",
            self.INCREMENTAL: "Incremental Loading",
            self.WEBHOOK: "Webhook Triggered",
            self.CDC: "Change Data Capture",
            self.TRIGGER: "Event Triggered"
        }.get(self, "Unknown Ingestion Mode")

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
    TERMINATED = "TERMINATED"
    THROTTLED = "THROTTLED"
    
    @property
    def display_name(self) -> str:
        return {
            self.INIT: "Initialized",
            self.ACTIVE: "Active",
            self.PAUSED: "Paused",
            self.STOPPED: "Stopped",
            self.FAILED: "Failed",
            self.PROCESSING: "Processing",
            self.COMPLETED: "Completed",
            self.ERROR: "Error State",
            self.SUSPENDED: "Suspended",
            self.ARCHIVED: "Archived",
            self.TERMINATED: "Terminated",
            self.THROTTLED: "Throttled"
        }.get(self, "Unknown Status")
    
    @property
    def is_active_state(self) -> bool:
        return self in [self.ACTIVE, self.PROCESSING]
    
    @property
    def is_stopped_state(self) -> bool:
        return self in [self.STOPPED, self.FAILED, self.ERROR, self.TERMINATED]
    
    @property
    def is_paused_state(self) -> bool:
        return self in [self.PAUSED, self.SUSPENDED, self.THROTTLED]

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
    ROUTER = "router"
    VALIDATOR = "validator"
    
    @property
    def display_name(self) -> str:
        return {
            self.SOURCE: "Data Source",
            self.TRANSFORM: "Transform",
            self.SINK: "Data Sink",
            self.FILTER: "Filter",
            self.JOIN: "Join Operation",
            self.AGGREGATE: "Aggregation",
            self.SPLIT: "Split Operation",
            self.MERGE: "Merge Operation",
            self.CUSTOM: "Custom Node",
            self.ROUTER: "Data Router",
            self.VALIDATOR: "Data Validator"
        }.get(self, "Unknown Node Type")

class FlowNodePriority(PyEnum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    
    @property
    def display_name(self) -> str:
        return {
            self.LOW: "Low Priority",
            self.NORMAL: "Normal Priority",
            self.HIGH: "High Priority",
            self.CRITICAL: "Critical Priority"
        }.get(self, "Unknown Priority")

class FlowNode(Base):
    __tablename__ = "flow_nodes"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255))
    flow_type = Column(SQLEnum(FlowType), nullable=False, default=FlowType.STREAMING, index=True)
    ingestion_mode = Column(SQLEnum(IngestionMode), nullable=False, default=IngestionMode.BATCH, index=True)
    status = Column(SQLEnum(NodeStatuses), nullable=False, default=NodeStatuses.INIT, index=True)
    node_type = Column(SQLEnum(NodeTypes), nullable=False, default=NodeTypes.TRANSFORM, index=True)
    priority = Column(SQLEnum(FlowNodePriority), default=FlowNodePriority.NORMAL, index=True)
    
    uid = Column(String(24), unique=True, index=True)
    description = Column(Text)
    version = Column(String(50), default="1.0")
    
    parallel_processing = Column(Boolean, default=False, index=True)
    max_parallel_tasks = Column(Integer, default=1)
    batch_size = Column(Integer, default=1000)
    processing_timeout = Column(Integer, default=3600)
    retry_count = Column(Integer, default=3)
    max_retry_count = Column(Integer, default=5)
    
    records_processed = Column(Integer, default=0)
    bytes_processed = Column(Integer, default=0)
    processing_time_ms = Column(Integer)
    avg_processing_time_ms = Column(Integer)
    last_processed_at = Column(DateTime, index=True)
    
    error_count = Column(Integer, default=0)
    last_error = Column(Text)
    last_error_at = Column(DateTime, index=True)
    consecutive_failures = Column(Integer, default=0)
    
    schedule_config = Column(JSON)
    depends_on_node_ids = Column(JSON)
    
    config = Column(JSON)
    extra_metadata = Column(Text)
    tags = Column(Text)
    
    is_disabled = Column(Boolean, default=False, index=True)
    is_template = Column(Boolean, default=False, index=True)
    skip_on_error = Column(Boolean, default=False)
    auto_retry = Column(Boolean, default=True)
    is_deprecated = Column(Boolean, default=False, index=True)
    
    origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    parent_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    shared_origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    copied_from_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), index=True)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), index=True)
    data_sink_id = Column(Integer, ForeignKey("data_sinks.id"), index=True)
    
    cluster_id = Column(Integer, ForeignKey("clusters.id"), index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), index=True)
    flow_id = Column(Integer, ForeignKey("flows.id"), index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    
    created_at = Column(DateTime, server_default=func.now(), index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), index=True)
    
    # Enhanced timestamps
    activated_at = Column(DateTime, index=True)
    paused_at = Column(DateTime, index=True)
    completed_at = Column(DateTime, index=True)
    failed_at = Column(DateTime, index=True)
    archived_at = Column(DateTime, index=True)
    last_health_check_at = Column(DateTime, index=True)
    
    # Performance tracking
    throughput_records_per_second = Column(Integer, default=0)
    peak_memory_usage_mb = Column(Integer, default=0)
    cpu_utilization_percent = Column(Integer, default=0)
    
    # Monitoring and alerting
    alert_threshold_errors = Column(Integer, default=10)
    alert_threshold_latency_ms = Column(Integer, default=30000)
    last_alert_sent_at = Column(DateTime)
    alert_count = Column(Integer, default=0)
    
    # Resource management
    allocated_memory_mb = Column(Integer, default=512)
    allocated_cpu_cores = Column(Integer, default=1)
    disk_usage_mb = Column(Integer, default=0)
    network_bandwidth_mbps = Column(Integer, default=100)
    
    owner = relationship("User", foreign_keys=[owner_id])
    org = relationship("Org")
    flow = relationship("Flow", back_populates="flow_nodes")
    project = relationship("Project", back_populates="flow_nodes")
    cluster = relationship("Cluster", foreign_keys=[cluster_id])
    
    origin_node = relationship("FlowNode", remote_side=[id], foreign_keys=[origin_node_id])
    parent_node = relationship("FlowNode", remote_side=[id], foreign_keys=[parent_node_id])
    shared_origin_node = relationship("FlowNode", remote_side=[id], foreign_keys=[shared_origin_node_id])
    copied_from = relationship("FlowNode", remote_side=[id], foreign_keys=[copied_from_id])
    child_nodes = relationship("FlowNode", remote_side=[parent_node_id], viewonly=True)
    
    data_source = relationship("DataSource", foreign_keys=[data_source_id])
    data_set = relationship("DataSet", foreign_keys=[data_set_id])
    data_sink = relationship("DataSink", foreign_keys=[data_sink_id])
    
    __table_args__ = (
        Index('idx_flow_nodes_status_active', 'status', 'is_disabled'),
        Index('idx_flow_nodes_org_status', 'org_id', 'status'),
        Index('idx_flow_nodes_project_status', 'project_id', 'status'),
        Index('idx_flow_nodes_flow_status', 'flow_id', 'status'),
        Index('idx_flow_nodes_owner_active', 'owner_id', 'status', 'is_disabled'),
        Index('idx_flow_nodes_type_status', 'node_type', 'status'),
        Index('idx_flow_nodes_flow_type_status', 'flow_type', 'status'),
        Index('idx_flow_nodes_priority_status', 'priority', 'status'),
        Index('idx_flow_nodes_last_processed', 'last_processed_at', 'status'),
        Index('idx_flow_nodes_performance', 'avg_processing_time_ms', 'status'),
        Index('idx_flow_nodes_errors', 'error_count', 'last_error_at'),
        Index('idx_flow_nodes_health', 'last_health_check_at', 'status'),
    )
    
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
        "ingestion_mode", "flow_type", "priority",
        "data_source_id", "data_set_id", "data_sink_id"
    ]
    
    # Performance thresholds
    PERFORMANCE_ISSUE_THRESHOLD_MS = 30000
    STALE_HOURS_THRESHOLD = 24
    HIGH_ERROR_RATE_THRESHOLD = 10
    CRITICAL_ERROR_RATE_THRESHOLD = 25
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.ensure_uid_()
        self.in_copy = False
        self._runtime_status = None
        self._processing_stats = None
        self._performance_cache = {}
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if flow node is active (Rails pattern)"""
        return (self.status == NodeStatuses.ACTIVE and 
                not self.is_disabled and 
                not self.is_deprecated and
                not self.suspended_())
    
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
    
    def terminated_(self) -> bool:
        """Check if flow node is terminated (Rails pattern)"""
        return self.status == NodeStatuses.TERMINATED
    
    def throttled_(self) -> bool:
        """Check if flow node is throttled (Rails pattern)"""
        return self.status == NodeStatuses.THROTTLED
    
    def disabled_(self) -> bool:
        """Check if flow node is disabled (Rails pattern)"""
        return self.is_disabled
        
    def enabled_(self) -> bool:
        """Check if flow node is enabled (Rails pattern)"""
        return not self.is_disabled
        
    def template_(self) -> bool:
        """Check if flow node is a template (Rails pattern)"""
        return self.is_template
    
    def deprecated_(self) -> bool:
        """Check if flow node is deprecated (Rails pattern)"""
        return self.is_deprecated
    
    def runnable_(self) -> bool:
        """Check if flow node can be run (Rails pattern)"""
        return (self.enabled_() and 
                not self.deprecated_() and
                not self.template_() and
                self.dependencies_satisfied_() and
                self.status in [NodeStatuses.INIT, NodeStatuses.PAUSED])
    
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
        return bool(self.get_dependency_ids())
        
    def dependencies_satisfied_(self) -> bool:
        """Check if all dependencies are satisfied (Rails pattern)"""
        if not self.has_dependencies_():
            return True
        
        dependency_ids = self.get_dependency_ids()
        if not dependency_ids:
            return True
        
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()()
        try:
            completed_deps = session.query(FlowNode).filter(
                FlowNode.id.in_(dependency_ids),
                FlowNode.status == NodeStatuses.COMPLETED
            ).count()
            return completed_deps == len(dependency_ids)
        finally:
            session.close()
        
    def can_be_processed_(self) -> bool:
        """Check if node can be processed (Rails pattern)"""
        return (self.runnable_() and
                not self.processing_() and
                not self.failed_() and
                not self.error_())
        
    def can_be_paused_(self) -> bool:
        """Check if node can be paused (Rails pattern)"""
        return self.status in [NodeStatuses.ACTIVE, NodeStatuses.PROCESSING]
        
    def can_be_resumed_(self) -> bool:
        """Check if node can be resumed (Rails pattern)"""
        return self.paused_() or self.throttled_()
        
    def can_be_stopped_(self) -> bool:
        """Check if node can be stopped (Rails pattern)"""
        return self.status in [NodeStatuses.ACTIVE, NodeStatuses.PROCESSING, 
                              NodeStatuses.PAUSED, NodeStatuses.THROTTLED]
    
    def can_be_terminated_(self) -> bool:
        """Check if node can be terminated (Rails pattern)"""
        return not self.terminated_() and not self.archived_()
    
    def can_be_archived_(self) -> bool:
        """Check if node can be archived (Rails pattern)"""
        return (self.stopped_() or self.failed_() or 
                self.completed_() or self.terminated_())
    
    def has_errors_(self) -> bool:
        """Check if node has errors (Rails pattern)"""
        return self.error_count > 0 or bool(self.last_error)
        
    def healthy_(self) -> bool:
        """Check if node is healthy (Rails pattern)"""
        return (self.active_() and 
                not self.has_errors_() and
                not self.stale_() and
                not self.performance_issues_() and
                not self.throttled_())
        
    def stale_(self, hours: int = None) -> bool:
        """Check if node is stale (Rails pattern)"""
        hours = hours or self.STALE_HOURS_THRESHOLD
        if not self.last_processed_at:
            return self.active_()
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
        return (self.processing_time_ms > self.PERFORMANCE_ISSUE_THRESHOLD_MS or
                self.processing_time_ms > (self.avg_processing_time_ms * 2))
        
    def high_error_rate_(self) -> bool:
        """Check if node has high error rate (Rails pattern)"""
        return self.error_count >= self.HIGH_ERROR_RATE_THRESHOLD
    
    def critical_error_rate_(self) -> bool:
        """Check if node has critical error rate (Rails pattern)"""
        return self.error_count >= self.CRITICAL_ERROR_RATE_THRESHOLD
    
    def needs_attention_(self) -> bool:
        """Check if node needs attention (Rails pattern)"""
        return (self.failed_() or self.error_() or 
                self.high_error_rate_() or 
                self.performance_issues_() or
                self.stale_(6))  # 6 hours for attention threshold
    
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
    
    def high_priority_(self) -> bool:
        """Check if node has high priority (Rails pattern)"""
        return self.priority in [FlowNodePriority.HIGH, FlowNodePriority.CRITICAL]
    
    def critical_priority_(self) -> bool:
        """Check if node has critical priority (Rails pattern)"""
        return self.priority == FlowNodePriority.CRITICAL
    
    def low_priority_(self) -> bool:
        """Check if node has low priority (Rails pattern)"""
        return self.priority == FlowNodePriority.LOW
    
    def resource_intensive_(self) -> bool:
        """Check if node is resource intensive (Rails pattern)"""
        return (self.allocated_memory_mb > 2048 or 
                self.allocated_cpu_cores > 4 or
                self.parallel_processing_enabled_())
    
    def over_allocated_(self) -> bool:
        """Check if node is over allocated resources (Rails pattern)"""
        return (self.peak_memory_usage_mb > self.allocated_memory_mb * 0.9 or
                self.cpu_utilization_percent > 90)
    
    def accessible_by_(self, user, access_level: str = 'read') -> bool:
        """Check if user can access node (Rails pattern)"""
        if not user:
            return False
            
        if self.owner_id == user.id:
            return True
            
        if self.flow and hasattr(self.flow, 'accessible_by_'):
            return self.flow.accessible_by_(user, access_level)
            
        if self.project and hasattr(self.project, 'accessible_by_'):
            return self.project.accessible_by_(user, access_level)
            
        if access_level == 'read' and user.org_id == self.org_id:
            return True
            
        return False
    
    def editable_by_(self, user) -> bool:
        """Check if user can edit node (Rails pattern)"""
        return self.accessible_by_(user, 'write')
    
    def deletable_by_(self, user) -> bool:
        """Check if user can delete node (Rails pattern)"""
        return (self.accessible_by_(user, 'admin') and 
                self.can_be_terminated_() and
                not self.has_children_() and
                not self.template_())

    # Rails-style bang methods (state changes)
    def activate_(self) -> None:
        """Activate node (Rails bang method pattern)"""
        if self.disabled_():
            self.is_disabled = False
            
        self.status = NodeStatuses.ACTIVE
        self.activated_at = datetime.now()
        self.updated_at = datetime.now()
        self._clear_error_state()
    
    def pause_(self, reason: Optional[str] = None) -> None:
        """Pause node (Rails bang method pattern)"""
        if not self.can_be_paused_():
            raise ValueError(f"Node cannot be paused. Status: {self.status}")
            
        self.status = NodeStatuses.PAUSED
        self.paused_at = datetime.now()
        if reason:
            self._update_metadata('pause_reason', reason)
        self.updated_at = datetime.now()
    
    def resume_(self) -> None:
        """Resume paused node (Rails bang method pattern)"""
        if not self.can_be_resumed_():
            raise ValueError(f"Node cannot be resumed. Status: {self.status}")
            
        self.status = NodeStatuses.ACTIVE
        self.activated_at = datetime.now()
        self._remove_metadata('pause_reason')
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
        self.consecutive_failures = (self.consecutive_failures or 0) + 1
        self.last_error = error
        self.last_error_at = datetime.now()
        self.failed_at = datetime.now()
        
        if error_details:
            self._update_metadata('error_details', error_details)
        
        self._check_alert_thresholds()
        self.updated_at = datetime.now()
    
    def complete_(self) -> None:
        """Mark node as completed (Rails bang method pattern)"""
        self.status = NodeStatuses.COMPLETED
        self.completed_at = datetime.now()
        self.consecutive_failures = 0
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
    
    def unsuspend_(self) -> None:
        """Unsuspend node (Rails bang method pattern)"""
        if not self.suspended_():
            return
        
        self.status = NodeStatuses.ACTIVE
        self.activated_at = datetime.now()
        self._remove_metadata('suspension_reason')
        self.updated_at = datetime.now()
    
    def archive_(self, reason: Optional[str] = None) -> None:
        """Archive node (Rails bang method pattern)"""
        if not self.can_be_archived_():
            raise ValueError(f"Node cannot be archived. Status: {self.status}")
            
        self.status = NodeStatuses.ARCHIVED
        self.archived_at = datetime.now()
        if reason:
            self._update_metadata('archive_reason', reason)
        self.updated_at = datetime.now()
    
    def terminate_(self, reason: Optional[str] = None) -> None:
        """Terminate node (Rails bang method pattern)"""
        if not self.can_be_terminated_():
            raise ValueError(f"Node cannot be terminated. Status: {self.status}")
            
        self.status = NodeStatuses.TERMINATED
        if reason:
            self._update_metadata('termination_reason', reason)
        self.updated_at = datetime.now()
    
    def throttle_(self, reason: Optional[str] = None) -> None:
        """Throttle node (Rails bang method pattern)"""
        self.status = NodeStatuses.THROTTLED
        if reason:
            self._update_metadata('throttle_reason', reason)
        self.updated_at = datetime.now()
    
    def unthrottle_(self) -> None:
        """Remove throttling from node (Rails bang method pattern)"""
        if not self.throttled_():
            return
        
        self.status = NodeStatuses.ACTIVE
        self.activated_at = datetime.now()
        self._remove_metadata('throttle_reason')
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
        self._remove_metadata('disable_reason')
        self.updated_at = datetime.now()
    
    def deprecate_(self, reason: Optional[str] = None) -> None:
        """Mark node as deprecated (Rails bang method pattern)"""
        self.is_deprecated = True
        if reason:
            self._update_metadata('deprecation_reason', reason)
        self.updated_at = datetime.now()
    
    def undeprecate_(self) -> None:
        """Remove deprecated status (Rails bang method pattern)"""
        self.is_deprecated = False
        self._remove_metadata('deprecation_reason')
        self.updated_at = datetime.now()
    
    def reset_errors_(self) -> None:
        """Reset error state (Rails bang method pattern)"""
        self.error_count = 0
        self.consecutive_failures = 0
        self.last_error = None
        self.last_error_at = None
        if self.failed_() or self.error_():
            self.status = NodeStatuses.INIT
        self.updated_at = datetime.now()
    
    def track_processing_(self, records: int, bytes_processed: int, 
                         processing_time_ms: int, memory_usage_mb: int = None,
                         cpu_percent: int = None) -> None:
        """Track processing metrics (Rails bang method pattern)"""
        self.records_processed = (self.records_processed or 0) + records
        self.bytes_processed = (self.bytes_processed or 0) + bytes_processed
        self.processing_time_ms = processing_time_ms
        
        # Update running averages
        if not self.avg_processing_time_ms:
            self.avg_processing_time_ms = processing_time_ms
        else:
            self.avg_processing_time_ms = int((self.avg_processing_time_ms + processing_time_ms) / 2)
        
        # Update performance metrics
        if records > 0 and processing_time_ms > 0:
            self.throughput_records_per_second = int((records * 1000) / processing_time_ms)
        
        if memory_usage_mb:
            self.peak_memory_usage_mb = max(self.peak_memory_usage_mb or 0, memory_usage_mb)
        
        if cpu_percent:
            self.cpu_utilization_percent = cpu_percent
        
        self.last_processed_at = datetime.now()
        self.consecutive_failures = 0  # Reset on successful processing
        self.updated_at = datetime.now()
    
    def increment_priority_(self) -> None:
        """Increase node priority (Rails bang method pattern)"""
        priority_order = [FlowNodePriority.LOW, FlowNodePriority.NORMAL, 
                         FlowNodePriority.HIGH, FlowNodePriority.CRITICAL]
        
        current_index = priority_order.index(self.priority)
        if current_index < len(priority_order) - 1:
            self.priority = priority_order[current_index + 1]
            self.updated_at = datetime.now()
    
    def decrement_priority_(self) -> None:
        """Decrease node priority (Rails bang method pattern)"""
        priority_order = [FlowNodePriority.LOW, FlowNodePriority.NORMAL, 
                         FlowNodePriority.HIGH, FlowNodePriority.CRITICAL]
        
        current_index = priority_order.index(self.priority)
        if current_index > 0:
            self.priority = priority_order[current_index - 1]
            self.updated_at = datetime.now()
    
    def scale_resources_(self, memory_mb: int = None, cpu_cores: int = None) -> None:
        """Scale node resources (Rails bang method pattern)"""
        if memory_mb:
            self.allocated_memory_mb = memory_mb
        if cpu_cores:
            self.allocated_cpu_cores = cpu_cores
        self.updated_at = datetime.now()
    
    def health_check_(self, force: bool = False) -> Dict[str, Any]:
        """Perform health check (Rails bang method pattern)"""
        now = datetime.now()
        
        # Skip if recently checked (unless forced)
        if (not force and self.last_health_check_at and 
            self.last_health_check_at > now - timedelta(minutes=5)):
            return self._get_cached_health_status()
        
        health_status = {
            'healthy': self.healthy_(),
            'status': self.status.value,
            'last_processed': self.last_processed_at.isoformat() if self.last_processed_at else None,
            'error_count': self.error_count,
            'consecutive_failures': self.consecutive_failures,
            'stale': self.stale_(),
            'performance_issues': self.performance_issues_(),
            'needs_attention': self.needs_attention_(),
            'resource_utilization': {
                'memory_usage_percent': round((self.peak_memory_usage_mb / self.allocated_memory_mb) * 100, 2) if self.allocated_memory_mb else 0,
                'cpu_utilization_percent': self.cpu_utilization_percent,
                'over_allocated': self.over_allocated_()
            },
            'throughput': {
                'records_per_second': self.throughput_records_per_second,
                'avg_processing_time_ms': self.avg_processing_time_ms
            }
        }
        
        self.last_health_check_at = now
        self.updated_at = now
        
        # Cache the result
        self._performance_cache['health_status'] = health_status
        self._performance_cache['health_check_time'] = now
        
        return health_status
    
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
        
        self.status = new_status
        
        for child in self.child_nodes or []:
            if child.same_origin_(self):
                child.flow_activate_traverse_(activate)
    
    def make_template_(self, template_name: Optional[str] = None) -> None:
        """Convert node to template (Rails bang method pattern)"""
        self.is_template = True
        if template_name:
            self.name = template_name
        self.updated_at = datetime.now()
    
    def send_alert_(self, alert_type: str, message: str) -> None:
        """Send alert for node (Rails bang method pattern)"""
        self.alert_count = (self.alert_count or 0) + 1
        self.last_alert_sent_at = datetime.now()
        
        alert_data = {
            'type': alert_type,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'node_id': self.id,
            'node_name': self.name or f"Node {self.id}",
            'status': self.status.value
        }
        
        self._update_metadata('last_alert', alert_data)
        logger.warning(f"Alert sent for node {self.id}: {message}")
    
    # Rails helper and utility methods
    def ensure_uid_(self) -> None:
        """Ensure unique UID is set (Rails before_save pattern)"""
        if self.uid:
            return
        
        max_attempts = 10
        for _ in range(max_attempts):
            uid = secrets.token_hex(12)
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
    
    def _remove_metadata(self, key: str) -> None:
        """Remove metadata field (Rails helper pattern)"""
        try:
            current_meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
            if key in current_meta:
                del current_meta[key]
                self.extra_metadata = json.dumps(current_meta) if current_meta else None
        except (json.JSONDecodeError, TypeError):
            pass
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Get metadata value (Rails helper pattern)"""
        try:
            meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
            return meta.get(key, default)
        except (json.JSONDecodeError, TypeError):
            return default
    
    def _clear_error_state(self) -> None:
        """Clear error state when node recovers (Rails helper pattern)"""
        if self.has_errors_():
            self._update_metadata('error_cleared_at', datetime.now().isoformat())
    
    def _check_alert_thresholds(self) -> None:
        """Check if alert thresholds are exceeded (Rails helper pattern)"""
        if self.error_count >= self.alert_threshold_errors:
            self.send_alert_('high_error_rate', 
                           f"Node has {self.error_count} errors, exceeding threshold of {self.alert_threshold_errors}")
        
        if self.processing_time_ms and self.processing_time_ms >= self.alert_threshold_latency_ms:
            self.send_alert_('high_latency', 
                           f"Processing time {self.processing_time_ms}ms exceeds threshold of {self.alert_threshold_latency_ms}ms")
    
    def _get_cached_health_status(self) -> Dict[str, Any]:
        """Get cached health status (Rails helper pattern)"""
        if 'health_status' in self._performance_cache:
            return self._performance_cache['health_status']
        return {'healthy': False, 'status': 'unknown', 'cache_miss': True}
    
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
            'throughput_records_per_second': self.throughput_records_per_second,
            'processing_rate_per_second': self.processing_rate_per_second(),
            'last_processed_at': self.last_processed_at.isoformat() if self.last_processed_at else None,
            'error_count': self.error_count or 0,
            'consecutive_failures': self.consecutive_failures or 0,
            'has_performance_issues': self.performance_issues_(),
            'resource_utilization': {
                'memory_usage_mb': self.peak_memory_usage_mb,
                'memory_allocated_mb': self.allocated_memory_mb,
                'cpu_utilization_percent': self.cpu_utilization_percent,
                'cpu_cores_allocated': self.allocated_cpu_cores,
                'over_allocated': self.over_allocated_()
            }
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
        
        ft_key = flow_type.lower().replace(".", "_")
        
        enable_elt = os.environ.get("API_ENABLE_ELT_FLOWS", "true").lower() == "true"
        
        if not enable_elt and ft_key == "elt":
            return None
        
        return cls.FLOW_TYPES.get(ft_key)
    
    @classmethod
    def build_flow_from_data_source(cls, data_source, **kwargs):
        """Build flow from data source (Rails pattern)"""
        if not hasattr(data_source, '__class__') or data_source.__class__.__name__ != 'DataSource':
            raise ValueError(f"Invalid resource for data source node: {data_source.__class__.__name__}")
        
        if data_source.flow_node:
            return data_source.flow_node
        
        flow_node = cls(
            owner=data_source.owner,
            org=data_source.org,
            data_source=data_source,
            flow_type=cls.default_flow_type(),
            ingestion_mode=IngestionMode.BATCH,
            node_type=NodeTypes.SOURCE,
            **kwargs
        )
        
        flow_node.origin_node_id = flow_node.id
        return flow_node
    
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
    def active_nodes(cls, flow=None, project=None, org=None):
        """Get active nodes (Rails scope pattern)"""
        query = cls.query.filter_by(status=NodeStatuses.ACTIVE, is_disabled=False, is_deprecated=False)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if project:
            query = query.filter_by(project_id=project.id if hasattr(project, 'id') else project)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def failed_nodes(cls, flow=None, hours=24, org=None):
        """Get recently failed nodes (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            cls.status == NodeStatuses.FAILED,
            cls.last_error_at >= cutoff
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def processing_nodes(cls, flow=None, org=None):
        """Get currently processing nodes (Rails scope pattern)"""
        query = cls.query.filter_by(status=NodeStatuses.PROCESSING)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def unhealthy_nodes(cls, org=None, hours=6):
        """Get unhealthy nodes (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            (cls.status.in_([NodeStatuses.FAILED, NodeStatuses.ERROR])) |
            (cls.error_count >= cls.HIGH_ERROR_RATE_THRESHOLD) |
            (cls.last_processed_at < cutoff)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def needs_attention(cls, org=None):
        """Get nodes that need attention (Rails scope pattern)"""
        critical_cutoff = datetime.now() - timedelta(hours=6)
        query = cls.query.filter(
            (cls.status.in_([NodeStatuses.FAILED, NodeStatuses.ERROR, NodeStatuses.SUSPENDED])) |
            (cls.error_count >= cls.HIGH_ERROR_RATE_THRESHOLD) |
            (cls.consecutive_failures >= 3) |
            ((cls.last_processed_at < critical_cutoff) & (cls.status == NodeStatuses.ACTIVE))
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def high_priority_nodes(cls, org=None):
        """Get high priority nodes (Rails scope pattern)"""
        query = cls.query.filter(cls.priority.in_([FlowNodePriority.HIGH, FlowNodePriority.CRITICAL]))
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def source_nodes(cls, flow=None, org=None):
        """Get source nodes (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.node_type == NodeTypes.SOURCE) | (cls.data_source_id.isnot(None))
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def sink_nodes(cls, flow=None, org=None):
        """Get sink nodes (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.node_type == NodeTypes.SINK) | (cls.data_sink_id.isnot(None))
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def transform_nodes(cls, flow=None, org=None):
        """Get transform nodes (Rails scope pattern)"""
        query = cls.query.filter_by(node_type=NodeTypes.TRANSFORM)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def origin_nodes(cls, flow=None, org=None):
        """Get origin nodes only (Rails scope pattern)"""
        query = cls.query.filter(cls.origin_node_id == cls.id)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def child_nodes_of(cls, parent_node):
        """Get child nodes of parent (Rails scope pattern)"""
        parent_id = parent_node.id if hasattr(parent_node, 'id') else parent_node
        return cls.query.filter_by(parent_node_id=parent_id)
    
    @classmethod
    def stale_nodes(cls, hours=24, flow=None, org=None):
        """Get stale nodes (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            (cls.last_processed_at < cutoff) | (cls.last_processed_at.is_(None)),
            cls.status == NodeStatuses.ACTIVE,
            cls.is_disabled == False
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def by_flow_type(cls, flow_type: FlowType, flow=None, org=None):
        """Get nodes by flow type (Rails scope pattern)"""
        query = cls.query.filter_by(flow_type=flow_type)
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def by_priority(cls, priority: FlowNodePriority, org=None):
        """Get nodes by priority (Rails scope pattern)"""
        query = cls.query.filter_by(priority=priority)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def streaming_nodes(cls, flow=None, org=None):
        """Get streaming nodes (Rails scope pattern)"""
        query = cls.query.filter(
            cls.flow_type.in_([FlowType.STREAMING, FlowType.REAL_TIME])
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def batch_nodes(cls, flow=None, org=None):
        """Get batch nodes (Rails scope pattern)"""
        query = cls.query.filter(
            cls.flow_type.in_([FlowType.BATCH, FlowType.MICRO_BATCH])
        )
        if flow:
            query = query.filter_by(flow_id=flow.id if hasattr(flow, 'id') else flow)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def templates(cls, node_type=None, org=None):
        """Get template nodes (Rails scope pattern)"""
        query = cls.query.filter_by(is_template=True)
        if node_type:
            query = query.filter_by(node_type=node_type)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def deprecated_nodes(cls, org=None):
        """Get deprecated nodes (Rails scope pattern)"""
        query = cls.query.filter_by(is_deprecated=True)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def resource_intensive_nodes(cls, org=None):
        """Get resource intensive nodes (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.allocated_memory_mb > 2048) |
            (cls.allocated_cpu_cores > 4) |
            (cls.parallel_processing == True)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def over_allocated_nodes(cls, org=None):
        """Get over-allocated nodes (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.peak_memory_usage_mb > cls.allocated_memory_mb * 0.9) |
            (cls.cpu_utilization_percent > 90)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def accessible_to(cls, user, access_level: str = 'read'):
        """Get nodes accessible to user (Rails scope pattern)"""
        if not user:
            return cls.query.filter(False)
        
        query = cls.query.filter_by(owner_id=user.id)
        
        if hasattr(user, 'org_id'):
            org_nodes = cls.query.filter_by(org_id=user.org_id)
            if access_level == 'read':
                query = query.union(org_nodes)
        
        return query.distinct()
    
    @classmethod
    def for_search_index(cls):
        """Get nodes for search index (Rails scope pattern)"""
        return cls.origin_nodes().filter_by(is_deprecated=False)
    
    @classmethod
    def search_ignored(cls):
        """Get search ignored nodes (Rails scope pattern)"""
        return cls.query.filter(
            (cls.origin_node_id != cls.id) |
            (cls.is_deprecated == True) |
            (cls.is_template == True)
        )
    
    @classmethod
    def condensed_origins(cls):
        """Get condensed origin nodes (Rails scope pattern)"""
        return cls.origin_nodes().with_entities(
            *[getattr(cls, field) for field in cls.CONDENSED_SELECT_FIELDS]
        )
    
    @classmethod
    def performance_statistics(cls, org=None, hours=24) -> Dict[str, Any]:
        """Get performance statistics (Rails class method pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        base_query = cls.query
        
        if org:
            base_query = base_query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        
        total_nodes = base_query.count()
        active_nodes = base_query.filter_by(status=NodeStatuses.ACTIVE, is_disabled=False).count()
        failed_nodes = base_query.filter(cls.status == NodeStatuses.FAILED, cls.failed_at >= cutoff).count()
        stale_nodes = base_query.filter(
            (cls.last_processed_at < cutoff) | (cls.last_processed_at.is_(None)),
            cls.status == NodeStatuses.ACTIVE
        ).count()
        
        return {
            'total_nodes': total_nodes,
            'active_nodes': active_nodes,
            'failed_nodes': failed_nodes,
            'stale_nodes': stale_nodes,
            'health_percentage': round((active_nodes / total_nodes * 100), 2) if total_nodes > 0 else 0,
            'failure_rate': round((failed_nodes / total_nodes * 100), 2) if total_nodes > 0 else 0,
            'stale_rate': round((stale_nodes / total_nodes * 100), 2) if total_nodes > 0 else 0
        }
    
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
        
        if hasattr(fn.resource, 'copy'):
            ds = fn.resource.copy(api_user_info, options)
        else:
            return None
        
        if is_origin:
            ds.flow_node.origin_node_id = ds.flow_node.id
            if pfn and hasattr(ds.flow_node, 'shared_origin_node_id'):
                ds.flow_node.shared_origin_node_id = pfn.shared_origin_node_id
        else:
            ds.flow_node.origin_node_id = pfn.origin_node_id
        
        ds.flow_node.parent_node_id = pfn.id if pfn else None
        
        for child_node in fn.child_nodes or []:
            if child_node.same_origin_(fn):
                child_node.flow_copy(api_user_info, options, child_node, ds.flow_node)
        
        return ds
    
    def handle_after_save(self) -> None:
        """Handle after save callback (Rails pattern)"""
        pass
    
    def set_defaults(self, user, org) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        self.org = org
        self.flow_type = FlowType.STREAMING
        self.ingestion_mode = IngestionMode.BATCH
        self.status = NodeStatuses.INIT
        self.node_type = NodeTypes.TRANSFORM
        self.priority = FlowNodePriority.NORMAL
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        if self.name:
            return self.name
        elif self.resource:
            return f"{self.resource.__class__.__name__} Node"
        else:
            return f"{self.node_type.display_name} #{self.id}"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        return self.status.display_name if self.status else "Unknown"
    
    def flow_type_display(self) -> str:
        """Get human-readable flow type (Rails pattern)"""
        return self.flow_type.display_name if self.flow_type else "Unknown"
    
    def node_type_display(self) -> str:
        """Get human-readable node type (Rails pattern)"""
        return self.node_type.display_name if self.node_type else "Unknown"
    
    def priority_display(self) -> str:
        """Get human-readable priority (Rails pattern)"""
        return self.priority.display_name if self.priority else "Unknown"
    
    def ingestion_mode_display(self) -> str:
        """Get human-readable ingestion mode (Rails pattern)"""
        return self.ingestion_mode.display_name if self.ingestion_mode else "Unknown"
    
    def health_status(self) -> str:
        """Get health status (Rails pattern)"""
        if self.healthy_():
            return "Healthy"
        elif self.needs_attention_():
            return "Needs Attention"
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
        
        if self.max_retry_count and self.max_retry_count < self.retry_count:
            errors.append("Max retry count must be greater than or equal to retry count")
        
        if self.allocated_memory_mb and self.allocated_memory_mb <= 0:
            errors.append("Allocated memory must be positive")
        
        if self.allocated_cpu_cores and self.allocated_cpu_cores <= 0:
            errors.append("Allocated CPU cores must be positive")
        
        if not any([self.data_source_id, self.data_set_id, self.data_sink_id]):
            if self.node_type not in [NodeTypes.TRANSFORM, NodeTypes.CUSTOM, NodeTypes.ROUTER]:
                errors.append("Node must reference at least one resource")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if node is valid (Rails validation pattern)"""
        return len(self.validate_()) == 0
    
    def to_dict(self, include_performance: bool = False, include_metadata: bool = False,
               include_relationships: bool = False, include_health: bool = False) -> Dict[str, Any]:
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
            'ingestion_mode_display': self.ingestion_mode_display(),
            'node_type': self.node_type.value if self.node_type else None,
            'node_type_display': self.node_type_display(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'priority': self.priority.value if self.priority else None,
            'priority_display': self.priority_display(),
            'version': self.version,
            'is_origin': self.is_origin_(),
            'is_child_node': self.is_child_node_(),
            'is_template': self.is_template,
            'is_disabled': self.is_disabled,
            'is_deprecated': self.is_deprecated,
            'active': self.active_(),
            'paused': self.paused_(),
            'healthy': self.healthy_(),
            'health_status': self.health_status(),
            'needs_attention': self.needs_attention_(),
            'parallel_processing': self.parallel_processing,
            'max_parallel_tasks': self.max_parallel_tasks,
            'batch_size': self.batch_size,
            'processing_timeout': self.processing_timeout,
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
        
        if include_health:
            result['health_check'] = self.health_check_()
        
        if self.has_errors_():
            result.update({
                'error_count': self.error_count,
                'consecutive_failures': self.consecutive_failures,
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
                'origin_node': self.origin_node.to_dict() if self.origin_node and self.origin_node.id != self.id else None,
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
            'priority': self.priority.value if self.priority else None,
            'priority_display': self.priority_display(),
            'flow_type': self.flow_type.value if self.flow_type else None,
            'is_origin': self.is_origin_(),
            'healthy': self.healthy_(),
            'needs_attention': self.needs_attention_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_processed_at': self.last_processed_at.isoformat() if self.last_processed_at else None
        }
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<FlowNode(id={self.id}, uid='{self.uid}', type='{self.node_type}', status='{self.status}', priority='{self.priority}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return self.display_name()

# Backwards compatibility alias
FlowNode = FlowNode