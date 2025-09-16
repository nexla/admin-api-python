"""
Flow Model - Data processing pipeline management entity.
Manages flow execution, status tracking, and deployment with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Set
from enum import Enum as PyEnum
import json
import secrets
import re
from ..database import Base

class FlowStatuses(PyEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    FAILED = "failed"
    ARCHIVED = "archived"
    SUSPENDED = "suspended"
    TEMPLATE = "template"

class FlowTypes(PyEnum):
    DATA_PIPELINE = "data_pipeline"
    ANALYTICS = "analytics"
    ETL = "etl"
    ML_PIPELINE = "ml_pipeline"
    STREAMING = "streaming"
    BATCH = "batch"
    REAL_TIME = "real_time"
    WORKFLOW = "workflow"
    NOTIFICATION = "notification"

class ScheduleTypes(PyEnum):
    MANUAL = "manual"
    CRON = "cron"
    EVENT_DRIVEN = "event_driven"
    INTERVAL = "interval"
    ONCE = "once"

class RunStatuses(PyEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    NEVER_RUN = "never_run"
    SKIPPED = "skipped"

class FlowPriorities(PyEnum):
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4
    LOWEST = 5

class Flow(Base):
    __tablename__ = "flows"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Flow configuration
    flow_type = Column(SQLEnum(FlowTypes), nullable=False, default=FlowTypes.DATA_PIPELINE)
    status = Column(SQLEnum(FlowStatuses), nullable=False, default=FlowStatuses.DRAFT)
    schedule_type = Column(SQLEnum(ScheduleTypes), nullable=False, default=ScheduleTypes.MANUAL)
    schedule_config = Column(JSON)  # Cron expressions, event configurations
    
    # Flow metadata
    uid = Column(String(24), unique=True, index=True)
    version = Column(String(50), default="1.0")
    tags = Column(Text)  # JSON string of tags
    priority = Column(SQLEnum(FlowPriorities), nullable=False, default=FlowPriorities.MEDIUM)
    extra_metadata = Column(Text)  # JSON string of additional metadata
    
    # Flow settings
    is_active = Column(Boolean, default=True)
    is_template = Column(Boolean, default=False)  # Template flows for reuse
    auto_start = Column(Boolean, default=False)
    retry_count = Column(Integer, default=3)
    timeout_minutes = Column(Integer, default=60)
    
    # Data lineage and dependencies
    parent_flow_id = Column(Integer, ForeignKey("flows.id"), nullable=True)
    template_flow_id = Column(Integer, ForeignKey("flows.id"), nullable=True)
    
    # Execution tracking
    last_run_at = Column(DateTime)
    last_run_status = Column(SQLEnum(RunStatuses), default=RunStatuses.NEVER_RUN)
    next_run_at = Column(DateTime)
    run_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    
    # Performance metrics
    avg_runtime_seconds = Column(Integer)
    max_runtime_seconds = Column(Integer)
    min_runtime_seconds = Column(Integer)
    avg_records_processed = Column(Integer)
    
    # Resource usage
    estimated_memory_mb = Column(Integer)
    estimated_cpu_percent = Column(Integer)
    max_parallel_runs = Column(Integer, default=1)
    
    # Flow lifecycle
    deployed_at = Column(DateTime)
    deployed_by_id = Column(Integer, ForeignKey("users.id"))
    last_modified_by_id = Column(Integer, ForeignKey("users.id"))
    archived_at = Column(DateTime)
    archived_by_id = Column(Integer, ForeignKey("users.id"))
    
    # Error handling
    max_retries = Column(Integer, default=3)
    retry_delay_seconds = Column(Integer, default=60)
    error_notification_enabled = Column(Boolean, default=True)
    success_notification_enabled = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    
    # Relationships
    owner = relationship("User", back_populates="flows", foreign_keys=[owner_id])
    org = relationship("Org", back_populates="flows")
    project = relationship("Project", back_populates="flows")
    # team = relationship("Team", back_populates="flows")  # TODO: Import Team model
    parent_flow = relationship("Flow", remote_side=[id], foreign_keys=[parent_flow_id])
    template_flow = relationship("Flow", remote_side=[id], foreign_keys=[template_flow_id])
    child_flows = relationship("Flow", remote_side=[parent_flow_id], foreign_keys=[parent_flow_id])
    flow_nodes = relationship("FlowNode", back_populates="flow")
    flow_runs = relationship("FlowRun", back_populates="flow")
    flow_permissions = relationship("FlowPermission", back_populates="flow")
    deployed_by = relationship("User", foreign_keys=[deployed_by_id])
    last_modified_by = relationship("User", foreign_keys=[last_modified_by_id])
    archived_by = relationship("User", foreign_keys=[archived_by_id])
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.ensure_uid_()
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if flow is active (Rails pattern)"""
        return self.status == FlowStatuses.ACTIVE and self.is_active
    
    def inactive_(self) -> bool:
        """Check if flow is inactive (Rails pattern)"""
        return not self.is_active or self.status in [FlowStatuses.STOPPED, FlowStatuses.SUSPENDED]
        
    def paused_(self) -> bool:
        """Check if flow is paused (Rails pattern)"""
        return self.status == FlowStatuses.PAUSED
        
    def draft_(self) -> bool:
        """Check if flow is in draft status (Rails pattern)"""
        return self.status == FlowStatuses.DRAFT
        
    def stopped_(self) -> bool:
        """Check if flow is stopped (Rails pattern)"""
        return self.status == FlowStatuses.STOPPED
        
    def failed_(self) -> bool:
        """Check if flow has failed (Rails pattern)"""
        return self.status == FlowStatuses.FAILED
        
    def archived_(self) -> bool:
        """Check if flow is archived (Rails pattern)"""
        return self.status == FlowStatuses.ARCHIVED
        
    def suspended_(self) -> bool:
        """Check if flow is suspended (Rails pattern)"""
        return self.status == FlowStatuses.SUSPENDED
        
    def template_(self) -> bool:
        """Check if flow is a template (Rails pattern)"""
        return self.is_template or self.status == FlowStatuses.TEMPLATE
    
    def scheduled_(self) -> bool:
        """Check if flow is scheduled (Rails pattern)"""
        return self.schedule_type != ScheduleTypes.MANUAL
        
    def manual_(self) -> bool:
        """Check if flow is manual execution (Rails pattern)"""
        return self.schedule_type == ScheduleTypes.MANUAL
        
    def cron_scheduled_(self) -> bool:
        """Check if flow is cron scheduled (Rails pattern)"""
        return self.schedule_type == ScheduleTypes.CRON
        
    def event_driven_(self) -> bool:
        """Check if flow is event driven (Rails pattern)"""
        return self.schedule_type == ScheduleTypes.EVENT_DRIVEN
    
    def running_(self) -> bool:
        """Check if flow is currently running (Rails pattern)"""
        return self.last_run_status == RunStatuses.RUNNING
        
    def successful_(self) -> bool:
        """Check if last run was successful (Rails pattern)"""
        return self.last_run_status == RunStatuses.SUCCESS
        
    def never_run_(self) -> bool:
        """Check if flow has never been run (Rails pattern)"""
        return self.last_run_status == RunStatuses.NEVER_RUN or not self.last_run_at
    
    def deployed_(self) -> bool:
        """Check if flow is deployed (Rails pattern)"""
        return self.deployed_at is not None
        
    def has_child_flows_(self) -> bool:
        """Check if flow has child flows (Rails pattern)"""
        return bool(self.child_flows)
        
    def is_child_flow_(self) -> bool:
        """Check if flow is a child flow (Rails pattern)"""
        return self.parent_flow_id is not None
        
    def from_template_(self) -> bool:
        """Check if flow was created from template (Rails pattern)"""
        return self.template_flow_id is not None
    
    def high_priority_(self) -> bool:
        """Check if flow has high priority (Rails pattern)"""
        return self.priority in [FlowPriorities.CRITICAL, FlowPriorities.HIGH]
        
    def can_be_started_(self) -> bool:
        """Check if flow can be started (Rails pattern)"""
        return (self.is_active and 
                not self.running_() and 
                self.status in [FlowStatuses.ACTIVE, FlowStatuses.DRAFT] and
                not self.archived_())
        
    def can_be_paused_(self) -> bool:
        """Check if flow can be paused (Rails pattern)"""
        return self.status == FlowStatuses.ACTIVE and not self.running_()
        
    def can_be_resumed_(self) -> bool:
        """Check if flow can be resumed (Rails pattern)"""
        return self.status == FlowStatuses.PAUSED
        
    def can_be_stopped_(self) -> bool:
        """Check if flow can be stopped (Rails pattern)"""
        return self.status in [FlowStatuses.ACTIVE, FlowStatuses.PAUSED]
        
    def can_be_deployed_(self) -> bool:
        """Check if flow can be deployed (Rails pattern)"""
        return (self.status == FlowStatuses.DRAFT and 
                self.get_node_count() > 0 and
                self.valid_())
        
    def can_be_deleted_(self) -> bool:
        """Check if flow can be deleted (Rails pattern)"""
        return not self.running_() and not self.has_child_flows_()
    
    def healthy_(self) -> bool:
        """Check if flow is healthy (Rails pattern)"""
        if self.never_run_():
            return True  # New flows are considered healthy
        return (self.success_rate() >= 80.0 and  # At least 80% success rate
                not self.failed_() and
                not self.suspended_())
    
    def performance_issues_(self) -> bool:
        """Check if flow has performance issues (Rails pattern)"""
        if not self.avg_runtime_seconds or self.run_count < 5:
            return False
        # Consider performance issue if runtime is > 2x average or > 1 hour
        return (self.max_runtime_seconds > self.avg_runtime_seconds * 2 or
                self.avg_runtime_seconds > 3600)
    
    def overdue_(self) -> bool:
        """Check if scheduled flow is overdue (Rails pattern)"""
        if not self.scheduled_() or not self.next_run_at:
            return False
        return self.next_run_at < datetime.now()
        
    def recently_run_(self, hours: int = 24) -> bool:
        """Check if flow was recently run (Rails pattern)"""
        if not self.last_run_at:
            return False
        return self.last_run_at >= datetime.now() - timedelta(hours=hours)
        
    def stale_(self, days: int = 30) -> bool:
        """Check if flow is stale (Rails pattern)"""
        if not self.last_run_at:
            return True
        return self.last_run_at < datetime.now() - timedelta(days=days)
    
    def accessible_by_(self, user, access_level: str = 'read') -> bool:
        """Check if user can access flow (Rails pattern)"""
        if not user:
            return False
            
        # Owner always has access
        if self.owner_id == user.id:
            return True
            
        # Project members have access based on project permissions
        if self.project and hasattr(self.project, 'accessible_by_'):
            return self.project.accessible_by_(user, access_level)
            
        # Org members have read access to non-private flows
        if access_level == 'read' and user.org_id == self.org_id:
            return True
            
        # Check explicit permissions
        # This would integrate with FlowPermission model
        return False
    
    def editable_by_(self, user) -> bool:
        """Check if user can edit flow (Rails pattern)"""
        return self.accessible_by_(user, 'write')
    
    def executable_by_(self, user) -> bool:
        """Check if user can execute flow (Rails pattern)"""
        return self.accessible_by_(user, 'execute')
    
    def deletable_by_(self, user) -> bool:
        """Check if user can delete flow (Rails pattern)"""
        return (self.accessible_by_(user, 'admin') and 
                self.can_be_deleted_())
    
    # Rails-style bang methods (state changes)
    def activate_(self) -> None:
        """Activate flow (Rails bang method pattern)"""
        if self.archived_():
            self.archived_at = None
            self.archived_by_id = None
            
        self.status = FlowStatuses.ACTIVE
        self.is_active = True
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: Optional[str] = None) -> None:
        """Deactivate flow (Rails bang method pattern)"""
        self.is_active = False
        self.updated_at = datetime.now()
        if reason:
            self._update_metadata('deactivation_reason', reason)
    
    def pause_(self) -> None:
        """Pause flow (Rails bang method pattern)"""
        if not self.can_be_paused_():
            raise ValueError(f"Flow cannot be paused. Status: {self.status}")
        
        self.status = FlowStatuses.PAUSED
        self.updated_at = datetime.now()
    
    def resume_(self) -> None:
        """Resume flow (Rails bang method pattern)"""
        if not self.can_be_resumed_():
            raise ValueError(f"Flow cannot be resumed. Status: {self.status}")
        
        self.status = FlowStatuses.ACTIVE
        self.updated_at = datetime.now()
    
    def stop_(self, reason: Optional[str] = None) -> None:
        """Stop flow (Rails bang method pattern)"""
        if not self.can_be_stopped_():
            raise ValueError(f"Flow cannot be stopped. Status: {self.status}")
        
        self.status = FlowStatuses.STOPPED
        self.is_active = False
        self.updated_at = datetime.now()
        if reason:
            self._update_metadata('stop_reason', reason)
    
    def fail_(self, error: Optional[str] = None) -> None:
        """Mark flow as failed (Rails bang method pattern)"""
        self.status = FlowStatuses.FAILED
        self.last_run_status = RunStatuses.FAILED
        if error:
            self._update_metadata('failure_reason', error)
        self.updated_at = datetime.now()
    
    def archive_(self, archived_by_user=None, reason: Optional[str] = None) -> None:
        """Archive flow (Rails bang method pattern)"""
        self.status = FlowStatuses.ARCHIVED
        self.archived_at = datetime.now()
        self.archived_by_id = archived_by_user.id if archived_by_user else None
        self.is_active = False
        if reason:
            self._update_metadata('archive_reason', reason)
        self.updated_at = datetime.now()
    
    def suspend_(self, reason: Optional[str] = None) -> None:
        """Suspend flow (Rails bang method pattern)"""
        self.status = FlowStatuses.SUSPENDED
        self.is_active = False
        if reason:
            self._update_metadata('suspension_reason', reason)
        self.updated_at = datetime.now()
    
    def deploy_(self, deployed_by_user=None) -> None:
        """Deploy flow (Rails bang method pattern)"""
        if not self.can_be_deployed_():
            raise ValueError("Flow cannot be deployed")
        
        self.status = FlowStatuses.ACTIVE
        self.deployed_at = datetime.now()
        self.deployed_by_id = deployed_by_user.id if deployed_by_user else None
        self.is_active = True
        self.updated_at = datetime.now()
    
    def make_template_(self, template_name: Optional[str] = None) -> None:
        """Convert flow to template (Rails bang method pattern)"""
        self.is_template = True
        self.status = FlowStatuses.TEMPLATE
        if template_name:
            self.name = template_name
        self.updated_at = datetime.now()
    
    def track_run_start_(self, trigger_type: str = 'manual', triggered_by_user=None) -> None:
        """Track flow run start (Rails bang method pattern)"""
        self.last_run_status = RunStatuses.RUNNING
        self.last_run_at = datetime.now()
        self.run_count = (self.run_count or 0) + 1
        
        # Calculate next run time for scheduled flows
        if self.scheduled_() and self.schedule_config:
            self._calculate_next_run_time()
        
        self.updated_at = datetime.now()
    
    def track_run_success_(self, duration_seconds: Optional[int] = None, 
                          records_processed: Optional[int] = None) -> None:
        """Track successful run completion (Rails bang method pattern)"""
        self.last_run_status = RunStatuses.SUCCESS
        self.success_count = (self.success_count or 0) + 1
        
        if duration_seconds:
            self._update_performance_metrics(duration_seconds)
        
        if records_processed:
            self._update_processing_metrics(records_processed)
        
        self.updated_at = datetime.now()
    
    def track_run_failure_(self, error: Optional[str] = None, 
                          duration_seconds: Optional[int] = None) -> None:
        """Track failed run completion (Rails bang method pattern)"""
        self.last_run_status = RunStatuses.FAILED
        self.failure_count = (self.failure_count or 0) + 1
        
        if duration_seconds:
            self._update_performance_metrics(duration_seconds)
        
        if error:
            self._update_metadata('last_error', error)
            self._update_metadata('last_error_at', datetime.now().isoformat())
        
        # Auto-suspend if failure rate is too high
        if self.failure_rate() > 90.0 and self.run_count >= 10:
            self.suspend_("High failure rate detected")
        
        self.updated_at = datetime.now()
    
    def increment_priority_(self) -> None:
        """Increase flow priority (Rails bang method pattern)"""
        if self.priority.value > 1:
            new_priority_value = self.priority.value - 1
            self.priority = FlowPriorities(new_priority_value)
            self.updated_at = datetime.now()
    
    def decrement_priority_(self) -> None:
        """Decrease flow priority (Rails bang method pattern)"""
        if self.priority.value < 5:
            new_priority_value = self.priority.value + 1
            self.priority = FlowPriorities(new_priority_value)
            self.updated_at = datetime.now()
    
    def reset_statistics_(self) -> None:
        """Reset run statistics (Rails bang method pattern)"""
        self.run_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.avg_runtime_seconds = None
        self.max_runtime_seconds = None
        self.min_runtime_seconds = None
        self.avg_records_processed = None
        self.updated_at = datetime.now()
    
    def copy_from_(self, source_flow, new_name: Optional[str] = None, 
                  copy_as_template: bool = False) -> None:
        """Create flow as copy of another (Rails bang method pattern)"""
        if not source_flow:
            raise ValueError("Source flow is required")
            
        self.name = new_name or f"Copy of {source_flow.name}"
        self.description = source_flow.description
        self.flow_type = source_flow.flow_type
        self.priority = source_flow.priority
        self.schedule_type = ScheduleTypes.MANUAL  # Always start as manual
        self.retry_count = source_flow.retry_count
        self.timeout_minutes = source_flow.timeout_minutes
        self.max_retries = source_flow.max_retries
        self.retry_delay_seconds = source_flow.retry_delay_seconds
        
        # Copy metadata
        if source_flow.metadata:
            try:
                import json
                source_meta = json.loads(source_flow.metadata)
                filtered_meta = {k: v for k, v in source_meta.items() 
                               if not k.startswith('_') and k not in ['api_keys', 'secrets']}
                self.extra_metadata = json.dumps(filtered_meta) if filtered_meta else None
            except (json.JSONDecodeError, TypeError):
                pass
                
        # Copy tags
        self.set_tags_(source_flow.tags_list())
        
        if copy_as_template:
            self.make_template_()
        else:
            self.status = FlowStatuses.DRAFT
            self.is_active = False
            
        self.updated_at = datetime.now()
        
        # Note: Flow nodes would be copied separately
    
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
        
        raise ValueError("Failed to generate unique flow UID")
    
    def _update_metadata(self, key: str, value: Any) -> None:
        """Update metadata field (Rails helper pattern)"""
        import json
        try:
            current_meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
        except (json.JSONDecodeError, TypeError):
            current_meta = {}
            
        current_meta[key] = value
        self.extra_metadata = json.dumps(current_meta)
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Get metadata value (Rails helper pattern)"""
        import json
        try:
            meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
            return meta.get(key, default)
        except (json.JSONDecodeError, TypeError):
            return default
    
    def _update_performance_metrics(self, duration_seconds: int) -> None:
        """Update performance metrics (Rails helper pattern)"""
        if not self.avg_runtime_seconds:
            self.avg_runtime_seconds = duration_seconds
            self.max_runtime_seconds = duration_seconds
            self.min_runtime_seconds = duration_seconds
        else:
            # Update running average
            total_runtime = self.avg_runtime_seconds * (self.run_count - 1)
            self.avg_runtime_seconds = int((total_runtime + duration_seconds) / self.run_count)
            self.max_runtime_seconds = max(self.max_runtime_seconds or 0, duration_seconds)
            self.min_runtime_seconds = min(self.min_runtime_seconds or duration_seconds, duration_seconds)
    
    def _update_processing_metrics(self, records_processed: int) -> None:
        """Update processing metrics (Rails helper pattern)"""
        if not self.avg_records_processed:
            self.avg_records_processed = records_processed
        else:
            # Update running average
            total_records = self.avg_records_processed * (self.success_count - 1)
            self.avg_records_processed = int((total_records + records_processed) / self.success_count)
    
    def _calculate_next_run_time(self) -> None:
        """Calculate next run time for scheduled flows (Rails helper pattern)"""
        if not self.scheduled_() or not self.schedule_config:
            return
            
        # This would implement cron parsing and next run calculation
        # For now, set a default next run time
        if self.schedule_type == ScheduleTypes.INTERVAL:
            interval_minutes = self.schedule_config.get('interval_minutes', 60)
            self.next_run_at = datetime.now() + timedelta(minutes=interval_minutes)
        # TODO: Implement cron parsing for CRON schedule type
    
    def get_node_count(self) -> int:
        """Get the number of nodes in this flow (Rails pattern)"""
        if hasattr(self, 'flow_nodes') and self.flow_nodes:
            return len(self.flow_nodes)
        # This would query FlowNode.where(flow_id: self.id).count
        return 0
    
    def get_latest_run_status(self) -> RunStatuses:
        """Get the latest run status (Rails pattern)"""
        return self.last_run_status or RunStatuses.NEVER_RUN
    
    def success_rate(self) -> float:
        """Calculate success rate (Rails pattern)"""
        if not self.run_count or self.run_count == 0:
            return 0.0
        return (self.success_count / self.run_count) * 100.0
    
    def failure_rate(self) -> float:
        """Calculate failure rate (Rails pattern)"""
        if not self.run_count or self.run_count == 0:
            return 0.0
        return (self.failure_count / self.run_count) * 100.0
    
    def health_score(self) -> float:
        """Calculate overall health score (Rails pattern)"""
        if self.never_run_():
            return 100.0  # New flows are considered healthy
        
        base_score = self.success_rate()
        
        # Adjust for performance issues
        if self.performance_issues_():
            base_score -= 20
        
        # Adjust for being overdue
        if self.overdue_():
            base_score -= 15
        
        # Adjust for recent failures
        if self.last_run_status == RunStatuses.FAILED:
            base_score -= 25
        
        return max(0.0, base_score)
    
    def estimated_next_runtime(self) -> Optional[int]:
        """Get estimated runtime for next execution (Rails pattern)"""
        if not self.avg_runtime_seconds:
            return None
        
        # Add 20% buffer to average runtime
        return int(self.avg_runtime_seconds * 1.2)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        if not self.tags:
            return []
        try:
            import json
            return json.loads(self.tags)
        except (json.JSONDecodeError, TypeError):
            return []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    def set_tags_(self, tags: Union[List[str], Set[str], str]) -> None:
        """Set flow tags (Rails bang method pattern)"""
        import json
        if isinstance(tags, str):
            # Handle comma-separated string
            tag_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
        else:
            tag_list = list(set(tags)) if tags else []
        
        self.tags = json.dumps(tag_list) if tag_list else None
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add a tag to flow (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.add(tag.strip())
        self.set_tags_(current_tags)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove a tag from flow (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.discard(tag.strip())
        self.set_tags_(current_tags)
    
    def has_tag_(self, tag: str) -> bool:
        """Check if flow has specific tag (Rails pattern)"""
        return tag.strip() in self.tags_list()
    
# Rails class methods and scopes
    @classmethod
    def find_by_uid(cls, uid: str):
        """Find flow by UID (Rails finder pattern)"""
        return cls.query.filter_by(uid=uid).first()
    
    @classmethod
    def find_by_uid_(cls, uid: str):
        """Find flow by UID or raise exception (Rails bang finder pattern)"""
        flow = cls.find_by_uid(uid)
        if not flow:
            raise ValueError(f"Flow with UID '{uid}' not found")
        return flow
    
    @classmethod
    def active_flows(cls, org=None, project=None):
        """Get active flows (Rails scope pattern)"""
        query = cls.query.filter_by(status=FlowStatuses.ACTIVE, is_active=True)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        if project:
            query = query.filter_by(project_id=project.id if hasattr(project, 'id') else project)
        return query
    
    @classmethod
    def failed_flows(cls, org=None):
        """Get failed flows (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.status == FlowStatuses.FAILED) |
            (cls.last_run_status == RunStatuses.FAILED)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def scheduled_flows(cls, org=None):
        """Get scheduled flows (Rails scope pattern)"""
        query = cls.query.filter(cls.schedule_type != ScheduleTypes.MANUAL)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def overdue_flows(cls, org=None):
        """Get overdue flows (Rails scope pattern)"""
        now = datetime.now()
        query = cls.query.filter(
            cls.next_run_at < now,
            cls.schedule_type != ScheduleTypes.MANUAL,
            cls.status == FlowStatuses.ACTIVE
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def running_flows(cls, org=None):
        """Get currently running flows (Rails scope pattern)"""
        query = cls.query.filter_by(last_run_status=RunStatuses.RUNNING)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def templates(cls, org=None):
        """Get template flows (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.is_template == True) | (cls.status == FlowStatuses.TEMPLATE)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def by_priority(cls, priority: FlowPriorities, org=None):
        """Get flows by priority (Rails scope pattern)"""
        query = cls.query.filter_by(priority=priority)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def high_priority_flows(cls, org=None):
        """Get high priority flows (Rails scope pattern)"""
        query = cls.query.filter(
            cls.priority.in_([FlowPriorities.CRITICAL, FlowPriorities.HIGH])
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def stale_flows(cls, days: int = 30, org=None):
        """Get stale flows (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        query = cls.query.filter(
            (cls.last_run_at < cutoff) | (cls.last_run_at.is_(None))
        ).filter_by(status=FlowStatuses.ACTIVE)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def accessible_to(cls, user, access_level: str = 'read'):
        """Get flows accessible to user (Rails scope pattern)"""
        if not user:
            return cls.query.filter(False)  # Empty query
        
        # Start with user's own flows
        query = cls.query.filter_by(owner_id=user.id)
        
        # Add flows from accessible projects
        # This would integrate with project permissions
        if hasattr(user, 'org_id'):
            org_flows = cls.query.filter_by(
                org_id=user.org_id,
                is_template=True  # Public templates
            )
            query = query.union(org_flows)
        
        return query.distinct()
    
    @classmethod
    def build_from_input(cls, api_user_info: Dict[str, Any], input_data: Dict[str, Any]):
        """Build flow from input data (Rails pattern)"""
        if not input_data or not api_user_info:
            raise ValueError("Flow input missing")
        
        flow = cls()
        flow.set_defaults(api_user_info.get('input_owner'), api_user_info.get('input_org'))
        flow.update_mutable(api_user_info, input_data)
        return flow
    
    @classmethod
    def build_from_template(cls, template_flow, api_user_info: Dict[str, Any], 
                           new_name: Optional[str] = None) -> 'Flow':
        """Build flow from template (Rails factory pattern)"""
        if not template_flow or not template_flow.template_():
            raise ValueError("Invalid template flow")
            
        flow = cls()
        flow.owner = api_user_info['input_owner']
        flow.org = api_user_info['input_org']
        flow.project_id = api_user_info.get('input_project_id')
        
        # Copy template attributes
        flow.copy_from_(template_flow, new_name)
        flow.template_flow_id = template_flow.id
        flow._update_metadata('created_from_template', True)
        flow._update_metadata('template_id', template_flow.id)
        
        return flow
    
    def update_mutable(self, api_user_info: Dict[str, Any], input_data: Dict[str, Any]) -> None:
        """Update mutable fields (Rails update_mutable! pattern)"""
        if not input_data or not api_user_info:
            return
        
        # Track who made the change
        self.last_modified_by_id = api_user_info.get('input_owner', {}).id if api_user_info.get('input_owner') else None
        
        # Update basic fields
        if 'name' in input_data:
            if not input_data['name'] or input_data['name'].strip() == '':
                raise ValueError("Flow name is required")
            self.name = input_data['name']
        
        if 'description' in input_data:
            self.description = input_data['description']
        
        if 'flow_type' in input_data:
            try:
                self.flow_type = FlowTypes(input_data['flow_type'])
            except ValueError:
                raise ValueError(f"Invalid flow type: {input_data['flow_type']}")
        
        if 'priority' in input_data:
            try:
                if isinstance(input_data['priority'], int):
                    self.priority = FlowPriorities(input_data['priority'])
                else:
                    self.priority = FlowPriorities(input_data['priority'])
            except ValueError:
                raise ValueError(f"Invalid priority: {input_data['priority']}")
        
        if 'schedule_type' in input_data:
            try:
                self.schedule_type = ScheduleTypes(input_data['schedule_type'])
            except ValueError:
                raise ValueError(f"Invalid schedule type: {input_data['schedule_type']}")
        
        if 'schedule_config' in input_data:
            self.schedule_config = input_data['schedule_config']
            if self.scheduled_():
                self._calculate_next_run_time()
        
        # Update settings
        for field in ['is_active', 'auto_start', 'retry_count', 'timeout_minutes', 
                     'max_retries', 'retry_delay_seconds', 'error_notification_enabled',
                     'success_notification_enabled', 'max_parallel_runs']:
            if field in input_data:
                setattr(self, field, input_data[field])
        
        # Update ownership if different
        if self.owner != api_user_info.get('input_owner'):
            self.owner = api_user_info['input_owner']
        
        if self.org != api_user_info.get('input_org'):
            self.org = api_user_info['input_org']
        
        if 'project_id' in input_data:
            self.project_id = input_data['project_id']
        
        # Handle tags
        if 'tags' in input_data:
            self.set_tags_(input_data['tags'])
        
        self.updated_at = datetime.now()
    
# Rails execution control methods
    def start_(self, triggered_by=None, trigger_type: str = 'manual') -> Dict[str, Any]:
        """Start flow execution (Rails bang method pattern)"""
        if not self.can_be_started_():
            raise ValueError(f"Flow cannot be started. Status: {self.status}")
        
        self.track_run_start_(trigger_type, triggered_by)
        return {'status': 'started', 'message': f"Flow {self.name} started"}
    
    def cancel_(self, reason: Optional[str] = None) -> Dict[str, Any]:
        """Cancel running flow (Rails bang method pattern)"""
        if not self.running_():
            raise ValueError(f"Flow is not running. Status: {self.last_run_status}")
        
        self.last_run_status = RunStatuses.CANCELLED
        if reason:
            self._update_metadata('cancellation_reason', reason)
        self.updated_at = datetime.now()
        
        return {'status': 'cancelled', 'message': f"Flow {self.name} cancelled"}
    
    def set_defaults(self, user, org) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        self.org = org
        self.status = FlowStatuses.DRAFT
        self.flow_type = FlowTypes.DATA_PIPELINE
        self.schedule_type = ScheduleTypes.MANUAL
        self.priority = FlowPriorities.MEDIUM
        self.retry_count = 3
        self.timeout_minutes = 60
        self.max_retries = 3
        self.retry_delay_seconds = 60
        self.is_active = False
        self.is_template = False
        self.auto_start = False
        self.error_notification_enabled = True
        self.success_notification_enabled = False
        self.max_parallel_runs = 1
    
    def validate_(self) -> List[str]:
        """Validate flow data (Rails validation pattern)"""
        errors = []
        
        if not self.name or not self.name.strip():
            errors.append("Name cannot be blank")
        elif len(self.name) > 255:
            errors.append("Name is too long (maximum 255 characters)")
        
        if self.name and not re.match(r'^[\w\s\-\.\(\)]+$', self.name):
            errors.append("Name contains invalid characters")
        
        if not self.owner_id:
            errors.append("Owner is required")
        
        if not self.org_id:
            errors.append("Organization is required")
        
        if self.description and len(self.description) > 10000:
            errors.append("Description is too long (maximum 10,000 characters)")
        
        if self.timeout_minutes and self.timeout_minutes <= 0:
            errors.append("Timeout must be positive")
        
        if self.retry_count and self.retry_count < 0:
            errors.append("Retry count cannot be negative")
        
        if self.max_parallel_runs and self.max_parallel_runs <= 0:
            errors.append("Max parallel runs must be positive")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if flow is valid (Rails validation pattern)"""
        return len(self.validate_()) == 0
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        if self.template_():
            return f"{self.name} (Template)"
        elif self.from_template_():
            return f"{self.name} (From Template)"
        elif self.is_child_flow_():
            return f"{self.name} (Child Flow)"
        return self.name or "Unnamed Flow"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        status_map = {
            FlowStatuses.DRAFT: "Draft",
            FlowStatuses.ACTIVE: "Active",
            FlowStatuses.PAUSED: "Paused",
            FlowStatuses.STOPPED: "Stopped",
            FlowStatuses.FAILED: "Failed",
            FlowStatuses.ARCHIVED: "Archived",
            FlowStatuses.SUSPENDED: "Suspended",
            FlowStatuses.TEMPLATE: "Template"
        }
        return status_map.get(self.status, "Unknown")
    
    def flow_type_display(self) -> str:
        """Get human-readable flow type (Rails pattern)"""
        type_map = {
            FlowTypes.DATA_PIPELINE: "Data Pipeline",
            FlowTypes.ANALYTICS: "Analytics",
            FlowTypes.ETL: "ETL",
            FlowTypes.ML_PIPELINE: "ML Pipeline",
            FlowTypes.STREAMING: "Streaming",
            FlowTypes.BATCH: "Batch",
            FlowTypes.REAL_TIME: "Real-time",
            FlowTypes.WORKFLOW: "Workflow",
            FlowTypes.NOTIFICATION: "Notification"
        }
        return type_map.get(self.flow_type, "Unknown")
    
    def priority_display(self) -> str:
        """Get human-readable priority (Rails pattern)"""
        priority_map = {
            FlowPriorities.CRITICAL: "Critical",
            FlowPriorities.HIGH: "High",
            FlowPriorities.MEDIUM: "Medium",
            FlowPriorities.LOW: "Low",
            FlowPriorities.LOWEST: "Lowest"
        }
        return priority_map.get(self.priority, "Medium")
    
    def schedule_display(self) -> str:
        """Get human-readable schedule (Rails pattern)"""
        if self.manual_():
            return "Manual"
        elif self.cron_scheduled_():
            return f"Cron: {self.schedule_config.get('cron_expression', 'N/A') if self.schedule_config else 'N/A'}"
        elif self.event_driven_():
            return "Event-driven"
        elif self.schedule_type == ScheduleTypes.INTERVAL:
            interval = self.schedule_config.get('interval_minutes', 'N/A') if self.schedule_config else 'N/A'
            return f"Every {interval} minutes"
        return "Unknown"
    
    def performance_summary(self) -> Dict[str, Any]:
        """Get performance summary (Rails pattern)"""
        return {
            'run_count': self.run_count or 0,
            'success_rate': self.success_rate(),
            'failure_rate': self.failure_rate(),
            'health_score': self.health_score(),
            'avg_runtime_seconds': self.avg_runtime_seconds,
            'avg_runtime_display': f"{self.avg_runtime_seconds}s" if self.avg_runtime_seconds else "N/A",
            'avg_records_processed': self.avg_records_processed,
            'estimated_next_runtime': self.estimated_next_runtime(),
            'has_performance_issues': self.performance_issues_()
        }
    
    def execution_summary(self) -> Dict[str, Any]:
        """Get execution summary (Rails pattern)"""
        return {
            'last_run_at': self.last_run_at.isoformat() if self.last_run_at else None,
            'last_run_status': self.last_run_status.value if self.last_run_status else None,
            'next_run_at': self.next_run_at.isoformat() if self.next_run_at else None,
            'is_running': self.running_(),
            'is_overdue': self.overdue_(),
            'can_be_started': self.can_be_started_(),
            'can_be_paused': self.can_be_paused_(),
            'can_be_resumed': self.can_be_resumed_(),
            'can_be_stopped': self.can_be_stopped_()
        }
    
    def to_dict(self, include_metadata: bool = False, include_performance: bool = False,
               include_relationships: bool = False) -> Dict[str, Any]:
        """Convert flow to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'description': self.description,
            'flow_type': self.flow_type.value if self.flow_type else None,
            'flow_type_display': self.flow_type_display(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'priority': self.priority.value if self.priority else None,
            'priority_display': self.priority_display(),
            'schedule_type': self.schedule_type.value if self.schedule_type else None,
            'schedule_display': self.schedule_display(),
            'version': self.version,
            'is_active': self.is_active,
            'is_template': self.is_template,
            'auto_start': self.auto_start,
            'retry_count': self.retry_count,
            'timeout_minutes': self.timeout_minutes,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'deployed_at': self.deployed_at.isoformat() if self.deployed_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'project_id': self.project_id,
            'parent_flow_id': self.parent_flow_id,
            'template_flow_id': self.template_flow_id,
            'tags': self.tags_list(),
            'node_count': self.get_node_count(),
            'healthy': self.healthy_(),
            'execution_summary': self.execution_summary()
        }
        
        if include_performance:
            result['performance_summary'] = self.performance_summary()
        
        if include_metadata and self.extra_metadata:
            try:
                import json
                result['metadata'] = json.loads(self.extra_metadata)
            except (json.JSONDecodeError, TypeError):
                pass
        
        if include_relationships:
            result.update({
                'owner': self.owner.to_dict() if self.owner else None,
                'org': self.org.to_dict() if self.org else None,
                'project': self.project.to_dict() if self.project else None,
                'parent_flow': self.parent_flow.to_dict() if self.parent_flow else None,
                'template_flow': self.template_flow.to_dict() if self.template_flow else None
            })
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert flow to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'flow_type': self.flow_type.value if self.flow_type else None,
            'priority': self.priority.value if self.priority else None,
            'is_active': self.is_active,
            'is_template': self.is_template,
            'success_rate': self.success_rate(),
            'last_run_at': self.last_run_at.isoformat() if self.last_run_at else None,
            'healthy': self.healthy_()
        }
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<Flow(id={self.id}, uid='{self.uid}', name='{self.name}', status='{self.status}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return self.display_name()

class FlowRun(Base):
    __tablename__ = "flow_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    flow_id = Column(Integer, ForeignKey("flows.id"), nullable=False)
    
    # Run metadata
    run_number = Column(Integer, nullable=False)  # Sequential run number for this flow
    status = Column(SQLEnum(RunStatuses), default=RunStatuses.QUEUED)
    trigger_type = Column(String(50))  # manual, scheduled, api, webhook
    triggered_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    
    # Execution details
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    error_message = Column(Text)
    log_data = Column(JSON)
    
    # Resource usage
    memory_usage_mb = Column(Integer)
    cpu_usage_percent = Column(Integer)
    
    # Data processed
    records_processed = Column(Integer, default=0)
    records_success = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    bytes_processed = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    flow = relationship("Flow", back_populates="flow_runs")
    triggered_by_user = relationship("User")
    # node_runs = relationship("FlowNodeRun", back_populates="flow_run")  # FlowNodeRun model not yet implemented
    
    def get_duration_display(self) -> str:
        """Get human-readable duration"""
        if not self.duration_seconds:
            return "N/A"
        
        hours = self.duration_seconds // 3600
        minutes = (self.duration_seconds % 3600) // 60
        seconds = self.duration_seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

class FlowPermission(Base):
    __tablename__ = "flow_permissions"
    
    id = Column(Integer, primary_key=True, index=True)
    flow_id = Column(Integer, ForeignKey("flows.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    
    # Permission types
    permission_type = Column(String(50), nullable=False)  # view, edit, execute, admin
    granted_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Timestamps
    granted_at = Column(DateTime, server_default=func.now(), nullable=False)
    expires_at = Column(DateTime, nullable=True)
    
    # Relationships
    flow = relationship("Flow", back_populates="flow_permissions")
    user = relationship("User", foreign_keys=[user_id])
    # team = relationship("Team")  # TODO: Import Team model
    granted_by_user = relationship("User", foreign_keys=[granted_by])

class FlowTemplate(Base):
    __tablename__ = "flow_templates"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    category = Column(String(100))  # ETL, Analytics, ML, Data Quality, etc.
    
    # Template configuration
    template_config = Column(JSON, nullable=False)  # Complete flow configuration
    parameter_schema = Column(JSON)  # Schema for template parameters
    is_public = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)  # Verified by platform team
    
    # Usage tracking
    usage_count = Column(Integer, default=0)
    rating = Column(Integer, default=0)  # 1-5 star rating
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Foreign keys
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=True)
    
    # Relationships
    created_by_user = relationship("User")
    org = relationship("Org")