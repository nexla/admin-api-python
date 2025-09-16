"""
Async Task Model - Background task management with comprehensive Rails business logic patterns.
Tracks async task execution, status, and results with sophisticated state management.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, ForeignKey, Boolean, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Set
from enum import Enum as PyEnum
import json
import secrets
import re
from ..database import Base


class TaskStatuses(PyEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RETRY = "retry"
    PAUSED = "paused"
    SUSPENDED = "suspended"

class TaskTypes(PyEnum):
    DATA_SYNC = "data_sync"
    DATA_EXPORT = "data_export"
    DATA_IMPORT = "data_import"
    FLOW_EXECUTION = "flow_execution"
    REPORT_GENERATION = "report_generation"
    EMAIL_NOTIFICATION = "email_notification"
    WEBHOOK_DELIVERY = "webhook_delivery"
    FILE_PROCESSING = "file_processing"
    DATA_VALIDATION = "data_validation"
    BACKUP = "backup"
    CLEANUP = "cleanup"
    SYSTEM_MAINTENANCE = "system_maintenance"
    USER_OPERATION = "user_operation"
    API_REQUEST = "api_request"
    CUSTOM = "custom"

class TaskPriorities(PyEnum):
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4
    LOWEST = 5


class AsyncTask(Base):
    __tablename__ = "async_tasks"
    
    id = Column(Integer, primary_key=True, index=True)
    task_type = Column(SQLEnum(TaskTypes), nullable=False, default=TaskTypes.CUSTOM)
    status = Column(SQLEnum(TaskStatuses), nullable=False, default=TaskStatuses.PENDING)
    
    # Task metadata
    uid = Column(String(24), unique=True, index=True)
    name = Column(String(255))
    description = Column(Text)
    priority = Column(SQLEnum(TaskPriorities), nullable=False, default=TaskPriorities.MEDIUM)
    
    # Task data
    arguments = Column(JSON)
    result = Column(JSON)
    result_url = Column(Text)
    error_message = Column(Text)
    error_code = Column(String(50))
    error_details = Column(JSON)
    
    # Progress tracking
    progress = Column(Integer, default=0)
    progress_message = Column(Text)
    progress_details = Column(JSON)
    total_steps = Column(Integer)
    current_step = Column(Integer, default=0)
    
    # Execution details
    execution_time_seconds = Column(Integer)
    memory_usage_mb = Column(Integer)
    cpu_usage_percent = Column(Integer)
    
    # Retry and timeout configuration
    max_retries = Column(Integer, default=3)
    retry_count = Column(Integer, default=0)
    retry_delay_seconds = Column(Integer, default=60)
    timeout_seconds = Column(Integer, default=3600)  # 1 hour default
    
    # Dependencies and scheduling
    depends_on_task_ids = Column(JSON)  # List of task IDs this task depends on
    schedule_at = Column(DateTime)  # When to execute the task
    expires_at = Column(DateTime)  # Task expiration time
    
    # Result management
    result_expires_at = Column(DateTime)
    result_size_bytes = Column(Integer)
    result_mime_type = Column(String(100))
    
    # Metadata and tags
    extra_metadata = Column(Text)  # JSON string of additional metadata
    tags = Column(Text)  # JSON string of tags
    
    # State flags
    is_internal = Column(Boolean, default=False)  # Internal system task
    is_critical = Column(Boolean, default=False)  # Critical task that shouldn't be cancelled
    allow_parallel = Column(Boolean, default=True)  # Allow parallel execution
    
    # Monitoring and alerts
    notify_on_completion = Column(Boolean, default=False)
    notify_on_failure = Column(Boolean, default=True)
    webhook_url = Column(Text)  # Webhook to call on completion
    
    # Worker information
    worker_id = Column(String(100))  # ID of worker processing the task
    worker_name = Column(String(255))  # Name of worker
    queue_name = Column(String(100))  # Queue the task was assigned to
    
    # Pause/resume functionality
    paused_at = Column(DateTime)
    paused_by_id = Column(Integer, ForeignKey("users.id"))
    pause_reason = Column(Text)
    
    # Request metadata
    request_data = Column(JSON)
    request_id = Column(String(100))  # Original request ID
    user_agent = Column(String(500))
    ip_address = Column(String(50))
    
    # Result management
    result_purged = Column(Boolean, default=False)
    result_purged_at = Column(DateTime)
    auto_purge_after_days = Column(Integer, default=30)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    started_at = Column(DateTime)
    stopped_at = Column(DateTime)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"))
    flow_id = Column(Integer, ForeignKey("flows.id"))
    parent_task_id = Column(Integer, ForeignKey("async_tasks.id"))
    root_task_id = Column(Integer, ForeignKey("async_tasks.id"))
    
    # Relationships
    owner = relationship("User", back_populates="async_tasks", foreign_keys=[owner_id])
    org = relationship("Org", back_populates="async_tasks")
    project = relationship("Project", foreign_keys=[project_id])
    flow = relationship("Flow", foreign_keys=[flow_id])
    parent_task = relationship("AsyncTask", remote_side=[id], foreign_keys=[parent_task_id])
    root_task = relationship("AsyncTask", remote_side=[id], foreign_keys=[root_task_id])
    child_tasks = relationship("AsyncTask", remote_side=[parent_task_id], foreign_keys=[parent_task_id])
    paused_by = relationship("User", foreign_keys=[paused_by_id])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.ensure_uid_()
        if not self.name and self.task_type:
            self.name = f"{self.task_type.value.replace('_', ' ').title()} Task"
    
    # Rails-style predicate methods
    def pending_(self) -> bool:
        """Check if task is pending (Rails pattern)"""
        return self.status == TaskStatuses.PENDING
    
    def queued_(self) -> bool:
        """Check if task is queued (Rails pattern)"""
        return self.status == TaskStatuses.QUEUED
        
    def running_(self) -> bool:
        """Check if task is running (Rails pattern)"""
        return self.status == TaskStatuses.RUNNING
        
    def completed_(self) -> bool:
        """Check if task is completed (Rails pattern)"""
        return self.status == TaskStatuses.COMPLETED
        
    def failed_(self) -> bool:
        """Check if task has failed (Rails pattern)"""
        return self.status == TaskStatuses.FAILED
        
    def cancelled_(self) -> bool:
        """Check if task is cancelled (Rails pattern)"""
        return self.status == TaskStatuses.CANCELLED
        
    def timeout_(self) -> bool:
        """Check if task timed out (Rails pattern)"""
        return self.status == TaskStatuses.TIMEOUT
        
    def paused_(self) -> bool:
        """Check if task is paused (Rails pattern)"""
        return self.status == TaskStatuses.PAUSED
        
    def suspended_(self) -> bool:
        """Check if task is suspended (Rails pattern)"""
        return self.status == TaskStatuses.SUSPENDED
        
    def retrying_(self) -> bool:
        """Check if task is retrying (Rails pattern)"""
        return self.status == TaskStatuses.RETRY
    
    def successful_(self) -> bool:
        """Check if task completed successfully (Rails pattern)"""
        return self.completed_() and not self.has_errors_()
        
    def in_progress_(self) -> bool:
        """Check if task is in progress (Rails pattern)"""
        return self.status in [TaskStatuses.QUEUED, TaskStatuses.RUNNING, TaskStatuses.RETRY]
        
    def finished_(self) -> bool:
        """Check if task is finished (Rails pattern)"""
        return self.status in [TaskStatuses.COMPLETED, TaskStatuses.FAILED, TaskStatuses.CANCELLED, TaskStatuses.TIMEOUT]
        
    def can_be_cancelled_(self) -> bool:
        """Check if task can be cancelled (Rails pattern)"""
        return (self.status in [TaskStatuses.PENDING, TaskStatuses.QUEUED, TaskStatuses.RUNNING, TaskStatuses.PAUSED] and
                not self.is_critical)
        
    def can_be_retried_(self) -> bool:
        """Check if task can be retried (Rails pattern)"""
        return (self.failed_() or self.timeout_()) and self.retry_count < self.max_retries
        
    def can_be_paused_(self) -> bool:
        """Check if task can be paused (Rails pattern)"""
        return self.running_() and not self.is_critical
        
    def can_be_resumed_(self) -> bool:
        """Check if task can be resumed (Rails pattern)"""
        return self.paused_()
    
    def expired_(self) -> bool:
        """Check if task has expired (Rails pattern)"""
        return self.expires_at and self.expires_at <= datetime.now()
        
    def overdue_(self) -> bool:
        """Check if scheduled task is overdue (Rails pattern)"""
        return (self.schedule_at and 
                self.schedule_at <= datetime.now() and 
                self.status == TaskStatuses.PENDING)
        
    def has_result_(self) -> bool:
        """Check if task has result (Rails pattern)"""
        return bool(self.result or self.result_url)
        
    def has_errors_(self) -> bool:
        """Check if task has errors (Rails pattern)"""
        return bool(self.error_message or self.error_code)
        
    def result_available_(self) -> bool:
        """Check if result is available (Rails pattern)"""
        return (self.has_result_() and 
                not self.result_purged and
                (not self.result_expires_at or self.result_expires_at > datetime.now()))
        
    def result_expired_(self) -> bool:
        """Check if result has expired (Rails pattern)"""
        return (self.result_expires_at and 
                self.result_expires_at <= datetime.now() and
                not self.result_purged)
        
    def high_priority_(self) -> bool:
        """Check if task has high priority (Rails pattern)"""
        return self.priority in [TaskPriorities.CRITICAL, TaskPriorities.HIGH]
        
    def critical_(self) -> bool:
        """Check if task is critical (Rails pattern)"""
        return self.is_critical or self.priority == TaskPriorities.CRITICAL
        
    def internal_(self) -> bool:
        """Check if task is internal (Rails pattern)"""
        return self.is_internal
        
    def has_dependencies_(self) -> bool:
        """Check if task has dependencies (Rails pattern)"""
        return bool(self.depends_on_task_ids)
        
    def has_child_tasks_(self) -> bool:
        """Check if task has child tasks (Rails pattern)"""
        return bool(self.child_tasks)
        
    def is_child_task_(self) -> bool:
        """Check if task is a child task (Rails pattern)"""
        return self.parent_task_id is not None
        
    def dependencies_completed_(self) -> bool:
        """Check if all dependencies are completed (Rails pattern)"""
        if not self.has_dependencies_():
            return True
        
        # This would query the database to check dependency statuses
        # For now, return True as placeholder
        return True
        
    def stale_(self, hours: int = 24) -> bool:
        """Check if task is stale (Rails pattern)"""
        if not self.created_at:
            return False
        return (self.created_at < datetime.now() - timedelta(hours=hours) and
                self.status in [TaskStatuses.PENDING, TaskStatuses.QUEUED])
        
    def long_running_(self, hours: int = 4) -> bool:
        """Check if task is long running (Rails pattern)"""
        if not self.started_at or not self.running_():
            return False
        return self.started_at < datetime.now() - timedelta(hours=hours)
        
    def recently_completed_(self, hours: int = 1) -> bool:
        """Check if task was recently completed (Rails pattern)"""
        if not self.stopped_at or not self.finished_():
            return False
        return self.stopped_at >= datetime.now() - timedelta(hours=hours)
    
    def accessible_by_(self, user, access_level: str = 'read') -> bool:
        """Check if user can access task (Rails pattern)"""
        if not user:
            return False
            
        # Owner always has access
        if self.owner_id == user.id:
            return True
            
        # Org admins have access to org tasks
        if user.org_id == self.org_id and hasattr(user, 'is_admin') and user.is_admin:
            return True
            
        # Project members have access to project tasks
        if self.project and hasattr(self.project, 'accessible_by_'):
            return self.project.accessible_by_(user, access_level)
            
        return False
    
    def cancellable_by_(self, user) -> bool:
        """Check if user can cancel task (Rails pattern)"""
        return (self.can_be_cancelled_() and 
                (self.accessible_by_(user, 'admin') or self.owner_id == user.id))
    
    # Rails-style bang methods (state changes)
    def queue_(self) -> None:
        """Queue task for execution (Rails bang method pattern)"""
        if self.status != TaskStatuses.PENDING:
            raise ValueError(f"Task cannot be queued. Status: {self.status}")
        
        if not self.dependencies_completed_():
            raise ValueError("Task dependencies not completed")
            
        self.status = TaskStatuses.QUEUED
        self.updated_at = datetime.now()
    
    def start_(self, worker_id: Optional[str] = None, worker_name: Optional[str] = None) -> None:
        """Start task execution (Rails bang method pattern)"""
        if self.status not in [TaskStatuses.QUEUED, TaskStatuses.RETRY]:
            raise ValueError(f"Task cannot be started. Status: {self.status}")
        
        self.status = TaskStatuses.RUNNING
        self.started_at = datetime.now()
        self.worker_id = worker_id
        self.worker_name = worker_name
        self.progress = 0
        self.current_step = 0
        self.updated_at = datetime.now()
    
    def complete_(self, result: Optional[Dict] = None, result_url: Optional[str] = None) -> None:
        """Mark task as completed (Rails bang method pattern)"""
        if not self.running_():
            raise ValueError(f"Task is not running. Status: {self.status}")
        
        self.status = TaskStatuses.COMPLETED
        self.stopped_at = datetime.now()
        self.progress = 100
        self.current_step = self.total_steps or 1
        
        if result:
            self.result = result
            self.result_size_bytes = len(json.dumps(result).encode('utf-8'))
        
        if result_url:
            self.result_url = result_url
        
        # Set result expiration
        if self.auto_purge_after_days:
            self.result_expires_at = datetime.now() + timedelta(days=self.auto_purge_after_days)
        
        # Calculate execution time
        if self.started_at:
            self.execution_time_seconds = int((self.stopped_at - self.started_at).total_seconds())
        
        self.updated_at = datetime.now()
    
    def fail_(self, error: str, error_code: Optional[str] = None, error_details: Optional[Dict] = None) -> None:
        """Mark task as failed (Rails bang method pattern)"""
        self.status = TaskStatuses.FAILED
        self.stopped_at = datetime.now()
        self.error_message = error
        self.error_code = error_code
        
        if error_details:
            self.error_details = error_details
        
        # Calculate execution time if started
        if self.started_at:
            self.execution_time_seconds = int((self.stopped_at - self.started_at).total_seconds())
        
        self.updated_at = datetime.now()
        
        # Auto-retry if possible
        if self.can_be_retried_():
            self.schedule_retry_()
    
    def cancel_(self, reason: Optional[str] = None) -> None:
        """Cancel task (Rails bang method pattern)"""
        if not self.can_be_cancelled_():
            raise ValueError(f"Task cannot be cancelled. Status: {self.status}")
        
        self.status = TaskStatuses.CANCELLED
        self.stopped_at = datetime.now()
        
        if reason:
            self._update_metadata('cancellation_reason', reason)
        
        # Calculate execution time if started
        if self.started_at:
            self.execution_time_seconds = int((self.stopped_at - self.started_at).total_seconds())
        
        self.updated_at = datetime.now()
    
    def timeout_(self) -> None:
        """Mark task as timed out (Rails bang method pattern)"""
        self.status = TaskStatuses.TIMEOUT
        self.stopped_at = datetime.now()
        self.error_message = f"Task timed out after {self.timeout_seconds} seconds"
        self.error_code = "TIMEOUT"
        
        # Calculate execution time
        if self.started_at:
            self.execution_time_seconds = int((self.stopped_at - self.started_at).total_seconds())
        
        self.updated_at = datetime.now()
        
        # Auto-retry if possible
        if self.can_be_retried_():
            self.schedule_retry_()
    
    def pause_(self, paused_by_user=None, reason: Optional[str] = None) -> None:
        """Pause task execution (Rails bang method pattern)"""
        if not self.can_be_paused_():
            raise ValueError(f"Task cannot be paused. Status: {self.status}")
        
        self.status = TaskStatuses.PAUSED
        self.paused_at = datetime.now()
        self.paused_by_id = paused_by_user.id if paused_by_user else None
        self.pause_reason = reason
        self.updated_at = datetime.now()
    
    def resume_(self) -> None:
        """Resume paused task (Rails bang method pattern)"""
        if not self.can_be_resumed_():
            raise ValueError(f"Task cannot be resumed. Status: {self.status}")
        
        self.status = TaskStatuses.QUEUED  # Back to queue for re-execution
        self.paused_at = None
        self.paused_by_id = None
        self.pause_reason = None
        self.updated_at = datetime.now()
    
    def suspend_(self, reason: Optional[str] = None) -> None:
        """Suspend task (Rails bang method pattern)"""
        self.status = TaskStatuses.SUSPENDED
        self.stopped_at = datetime.now()
        if reason:
            self._update_metadata('suspension_reason', reason)
        self.updated_at = datetime.now()
    
    def schedule_retry_(self) -> None:
        """Schedule task for retry (Rails bang method pattern)"""
        if not self.can_be_retried_():
            raise ValueError("Task cannot be retried")
        
        self.retry_count += 1
        self.status = TaskStatuses.RETRY
        
        # Schedule next retry with exponential backoff
        delay = self.retry_delay_seconds * (2 ** (self.retry_count - 1))
        self.schedule_at = datetime.now() + timedelta(seconds=delay)
        
        # Clear previous run state
        self.started_at = None
        self.stopped_at = None
        self.worker_id = None
        self.worker_name = None
        self.progress = 0
        self.current_step = 0
        
        self.updated_at = datetime.now()
    
    def update_progress_(self, progress: int, message: Optional[str] = None, 
                        details: Optional[Dict] = None, step: Optional[int] = None) -> None:
        """Update task progress (Rails bang method pattern)"""
        if not self.running_():
            return
        
        self.progress = max(0, min(100, progress))
        
        if message:
            self.progress_message = message
        
        if details:
            self.progress_details = details
        
        if step is not None:
            self.current_step = step
        
        self.updated_at = datetime.now()
    
    def purge_result_(self) -> None:
        """Purge task result (Rails bang method pattern)"""
        self.result = None
        self.result_url = None
        self.result_purged = True
        self.result_purged_at = datetime.now()
        self.updated_at = datetime.now()
    
    def extend_result_expiration_(self, days: int = 30) -> None:
        """Extend result expiration (Rails bang method pattern)"""
        if self.has_result_() and not self.result_purged:
            self.result_expires_at = datetime.now() + timedelta(days=days)
            self.updated_at = datetime.now()
    
    def increment_priority_(self) -> None:
        """Increase task priority (Rails bang method pattern)"""
        if self.priority.value > 1:
            new_priority_value = self.priority.value - 1
            self.priority = TaskPriorities(new_priority_value)
            self.updated_at = datetime.now()
    
    def decrement_priority_(self) -> None:
        """Decrease task priority (Rails bang method pattern)"""
        if self.priority.value < 5:
            new_priority_value = self.priority.value + 1
            self.priority = TaskPriorities(new_priority_value)
            self.updated_at = datetime.now()
    
    def mark_critical_(self) -> None:
        """Mark task as critical (Rails bang method pattern)"""
        self.is_critical = True
        self.priority = TaskPriorities.CRITICAL
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
        
        raise ValueError("Failed to generate unique task UID")
    
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
        """Get list of dependency task IDs (Rails helper pattern)"""
        if not self.depends_on_task_ids:
            return []
        try:
            return json.loads(self.depends_on_task_ids) if isinstance(self.depends_on_task_ids, str) else self.depends_on_task_ids
        except (json.JSONDecodeError, TypeError):
            return []
    
    def set_dependency_ids_(self, task_ids: List[int]) -> None:
        """Set dependency task IDs (Rails bang helper pattern)"""
        self.depends_on_task_ids = task_ids
        self.updated_at = datetime.now()
    
    def add_dependency_(self, task_id: int) -> None:
        """Add task dependency (Rails bang helper pattern)"""
        current_deps = self.get_dependency_ids()
        if task_id not in current_deps:
            current_deps.append(task_id)
            self.set_dependency_ids_(current_deps)
    
    def remove_dependency_(self, task_id: int) -> None:
        """Remove task dependency (Rails bang helper pattern)"""
        current_deps = self.get_dependency_ids()
        if task_id in current_deps:
            current_deps.remove(task_id)
            self.set_dependency_ids_(current_deps)
    
    def estimated_duration(self) -> Optional[int]:
        """Get estimated duration in seconds (Rails pattern)"""
        # This would use historical data to estimate duration
        # For now, return timeout as rough estimate
        return self.timeout_seconds
    
    def progress_percentage(self) -> int:
        """Get progress as percentage (Rails pattern)"""
        return max(0, min(100, self.progress or 0))
    
    def step_progress(self) -> str:
        """Get step progress as string (Rails pattern)"""
        if not self.total_steps:
            return "N/A"
        current = self.current_step or 0
        return f"{current}/{self.total_steps}"
    
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
        """Set task tags (Rails bang method pattern)"""
        if isinstance(tags, str):
            # Handle comma-separated string
            tag_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
        else:
            tag_list = list(set(tags)) if tags else []
        
        self.tags = json.dumps(tag_list) if tag_list else None
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add a tag to task (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.add(tag.strip())
        self.set_tags_(current_tags)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove a tag from task (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.discard(tag.strip())
        self.set_tags_(current_tags)
    
    def has_tag_(self, tag: str) -> bool:
        """Check if task has specific tag (Rails pattern)"""
        return tag.strip() in self.tags_list()
    
    # Rails class methods and scopes
    @classmethod
    def find_by_uid(cls, uid: str):
        """Find task by UID (Rails finder pattern)"""
        return cls.query.filter_by(uid=uid).first()
    
    @classmethod
    def find_by_uid_(cls, uid: str):
        """Find task by UID or raise exception (Rails bang finder pattern)"""
        task = cls.find_by_uid(uid)
        if not task:
            raise ValueError(f"AsyncTask with UID '{uid}' not found")
        return task
    
    @classmethod
    def pending_tasks(cls, org=None, task_type=None):
        """Get pending tasks (Rails scope pattern)"""
        query = cls.query.filter_by(status=TaskStatuses.PENDING)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        if task_type:
            query = query.filter_by(task_type=task_type)
        return query.order_by(cls.priority, cls.created_at)
    
    @classmethod
    def running_tasks(cls, org=None, worker_id=None):
        """Get running tasks (Rails scope pattern)"""
        query = cls.query.filter_by(status=TaskStatuses.RUNNING)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        if worker_id:
            query = query.filter_by(worker_id=worker_id)
        return query
    
    @classmethod
    def failed_tasks(cls, org=None, hours=24):
        """Get recently failed tasks (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            cls.status == TaskStatuses.FAILED,
            cls.stopped_at >= cutoff
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query.order_by(cls.stopped_at.desc())
    
    @classmethod
    def completed_tasks(cls, org=None, days=7):
        """Get recently completed tasks (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        query = cls.query.filter(
            cls.status == TaskStatuses.COMPLETED,
            cls.stopped_at >= cutoff
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query.order_by(cls.stopped_at.desc())
    
    @classmethod
    def high_priority_tasks(cls, org=None):
        """Get high priority tasks (Rails scope pattern)"""
        query = cls.query.filter(
            cls.priority.in_([TaskPriorities.CRITICAL, TaskPriorities.HIGH])
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query.order_by(cls.priority, cls.created_at)
    
    @classmethod
    def critical_tasks(cls, org=None):
        """Get critical tasks (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.is_critical == True) | (cls.priority == TaskPriorities.CRITICAL)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query.order_by(cls.created_at)
    
    @classmethod
    def stale_tasks(cls, hours=24, org=None):
        """Get stale tasks (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            cls.created_at < cutoff,
            cls.status.in_([TaskStatuses.PENDING, TaskStatuses.QUEUED])
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def long_running_tasks(cls, hours=4, org=None):
        """Get long running tasks (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        query = cls.query.filter(
            cls.status == TaskStatuses.RUNNING,
            cls.started_at < cutoff
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def overdue_tasks(cls, org=None):
        """Get overdue scheduled tasks (Rails scope pattern)"""
        now = datetime.now()
        query = cls.query.filter(
            cls.schedule_at < now,
            cls.status == TaskStatuses.PENDING
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def by_task_type(cls, task_type: TaskTypes, org=None):
        """Get tasks by type (Rails scope pattern)"""
        query = cls.query.filter_by(task_type=task_type)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def accessible_to(cls, user, access_level: str = 'read'):
        """Get tasks accessible to user (Rails scope pattern)"""
        if not user:
            return cls.query.filter(False)  # Empty query
        
        # Start with user's own tasks
        query = cls.query.filter_by(owner_id=user.id)
        
        # Add org tasks if user is admin
        if hasattr(user, 'is_admin') and user.is_admin:
            org_tasks = cls.query.filter_by(org_id=user.org_id if hasattr(user, 'org_id') else None)
            query = query.union(org_tasks)
        
        return query.distinct()
    
    @classmethod
    def with_results_expiring(cls, days=7, org=None):
        """Get tasks with results expiring soon (Rails scope pattern)"""
        cutoff = datetime.now() + timedelta(days=days)
        query = cls.query.filter(
            cls.result_expires_at <= cutoff,
            cls.result_purged == False,
            cls.status == TaskStatuses.COMPLETED
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        if self.name:
            return self.name
        return f"{self.task_type.value.replace('_', ' ').title()} Task #{self.id}"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        status_map = {
            TaskStatuses.PENDING: "Pending",
            TaskStatuses.QUEUED: "Queued",
            TaskStatuses.RUNNING: "Running",
            TaskStatuses.COMPLETED: "Completed",
            TaskStatuses.FAILED: "Failed",
            TaskStatuses.CANCELLED: "Cancelled",
            TaskStatuses.TIMEOUT: "Timed Out",
            TaskStatuses.RETRY: "Retrying",
            TaskStatuses.PAUSED: "Paused",
            TaskStatuses.SUSPENDED: "Suspended"
        }
        return status_map.get(self.status, "Unknown")
    
    def task_type_display(self) -> str:
        """Get human-readable task type (Rails pattern)"""
        type_map = {
            TaskTypes.DATA_SYNC: "Data Synchronization",
            TaskTypes.DATA_EXPORT: "Data Export",
            TaskTypes.DATA_IMPORT: "Data Import",
            TaskTypes.FLOW_EXECUTION: "Flow Execution",
            TaskTypes.REPORT_GENERATION: "Report Generation",
            TaskTypes.EMAIL_NOTIFICATION: "Email Notification",
            TaskTypes.WEBHOOK_DELIVERY: "Webhook Delivery",
            TaskTypes.FILE_PROCESSING: "File Processing",
            TaskTypes.DATA_VALIDATION: "Data Validation",
            TaskTypes.BACKUP: "Backup",
            TaskTypes.CLEANUP: "Cleanup",
            TaskTypes.SYSTEM_MAINTENANCE: "System Maintenance",
            TaskTypes.USER_OPERATION: "User Operation",
            TaskTypes.API_REQUEST: "API Request",
            TaskTypes.CUSTOM: "Custom Task"
        }
        return type_map.get(self.task_type, "Unknown")
    
    def priority_display(self) -> str:
        """Get human-readable priority (Rails pattern)"""
        priority_map = {
            TaskPriorities.CRITICAL: "Critical",
            TaskPriorities.HIGH: "High",
            TaskPriorities.MEDIUM: "Medium",
            TaskPriorities.LOW: "Low",
            TaskPriorities.LOWEST: "Lowest"
        }
        return priority_map.get(self.priority, "Medium")
    
    def duration_display(self) -> str:
        """Get human-readable duration (Rails pattern)"""
        if not self.execution_time_seconds:
            return "N/A"
        
        seconds = self.execution_time_seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"
    
    def execution_summary(self) -> Dict[str, Any]:
        """Get execution summary (Rails pattern)"""
        return {
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'progress': self.progress_percentage(),
            'step_progress': self.step_progress(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'stopped_at': self.stopped_at.isoformat() if self.stopped_at else None,
            'duration_seconds': self.execution_time_seconds,
            'duration_display': self.duration_display(),
            'worker_id': self.worker_id,
            'worker_name': self.worker_name,
            'retry_count': self.retry_count,
            'has_result': self.has_result_(),
            'has_errors': self.has_errors_()
        }
    
    def resource_summary(self) -> Dict[str, Any]:
        """Get resource usage summary (Rails pattern)"""
        return {
            'memory_usage_mb': self.memory_usage_mb,
            'cpu_usage_percent': self.cpu_usage_percent,
            'result_size_bytes': self.result_size_bytes,
            'execution_time_seconds': self.execution_time_seconds,
            'timeout_seconds': self.timeout_seconds,
            'estimated_duration': self.estimated_duration()
        }
    
    def validate_(self) -> List[str]:
        """Validate task data (Rails validation pattern)"""
        errors = []
        
        if not self.task_type:
            errors.append("Task type is required")
        
        if not self.owner_id:
            errors.append("Owner is required")
        
        if not self.org_id:
            errors.append("Organization is required")
        
        if self.timeout_seconds and self.timeout_seconds <= 0:
            errors.append("Timeout must be positive")
        
        if self.max_retries and self.max_retries < 0:
            errors.append("Max retries cannot be negative")
        
        if self.retry_delay_seconds and self.retry_delay_seconds <= 0:
            errors.append("Retry delay must be positive")
        
        if self.progress and (self.progress < 0 or self.progress > 100):
            errors.append("Progress must be between 0 and 100")
        
        if self.expires_at and self.expires_at <= datetime.now():
            errors.append("Expiration time must be in the future")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if task is valid (Rails validation pattern)"""
        return len(self.validate_()) == 0
    
    def to_dict(self, include_result: bool = False, include_metadata: bool = False,
               include_relationships: bool = False) -> Dict[str, Any]:
        """Convert task to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'description': self.description,
            'task_type': self.task_type.value if self.task_type else None,
            'task_type_display': self.task_type_display(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'priority': self.priority.value if self.priority else None,
            'priority_display': self.priority_display(),
            'is_critical': self.is_critical,
            'is_internal': self.is_internal,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'schedule_at': self.schedule_at.isoformat() if self.schedule_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'project_id': self.project_id,
            'flow_id': self.flow_id,
            'parent_task_id': self.parent_task_id,
            'tags': self.tags_list(),
            'has_dependencies': self.has_dependencies_(),
            'has_child_tasks': self.has_child_tasks_(),
            'execution_summary': self.execution_summary(),
            'resource_summary': self.resource_summary()
        }
        
        if include_result and self.result_available_():
            result.update({
                'result': self.result,
                'result_url': self.result_url,
                'result_expires_at': self.result_expires_at.isoformat() if self.result_expires_at else None
            })
        
        if self.has_errors_():
            result.update({
                'error_message': self.error_message,
                'error_code': self.error_code,
                'error_details': self.error_details
            })
        
        if self.paused_():
            result.update({
                'paused_at': self.paused_at.isoformat() if self.paused_at else None,
                'paused_by_id': self.paused_by_id,
                'pause_reason': self.pause_reason
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
                'project': self.project.to_dict() if self.project else None,
                'flow': self.flow.to_dict() if self.flow else None,
                'parent_task': self.parent_task.to_dict() if self.parent_task else None
            })
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert task to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'task_type': self.task_type.value if self.task_type else None,
            'task_type_display': self.task_type_display(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'priority': self.priority.value if self.priority else None,
            'progress': self.progress_percentage(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'stopped_at': self.stopped_at.isoformat() if self.stopped_at else None
        }
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<AsyncTask(id={self.id}, uid='{self.uid}', type='{self.task_type}', status='{self.status}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return self.display_name()