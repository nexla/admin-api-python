from datetime import datetime, timedelta
from enum import Enum as PyEnum
import json
import uuid
from typing import Dict, List, Optional, Any, Union

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class JobStatus(PyEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRYING = "RETRYING"
    SCHEDULED = "SCHEDULED"
    PAUSED = "PAUSED"
    EXPIRED = "EXPIRED"
    
    @property
    def display_name(self) -> str:
        return {
            self.PENDING: "Pending",
            self.RUNNING: "Running",
            self.COMPLETED: "Completed",
            self.FAILED: "Failed",
            self.CANCELLED: "Cancelled",
            self.RETRYING: "Retrying",
            self.SCHEDULED: "Scheduled",
            self.PAUSED: "Paused",
            self.EXPIRED: "Expired"
        }.get(self, self.value)
    
    @property
    def is_active(self) -> bool:
        return self in [self.PENDING, self.RUNNING, self.RETRYING]
    
    @property
    def is_completed(self) -> bool:
        return self in [self.COMPLETED, self.FAILED, self.CANCELLED, self.EXPIRED]


class JobPriority(PyEnum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"
    BACKGROUND = "BACKGROUND"
    
    @property
    def display_name(self) -> str:
        return {
            self.CRITICAL: "Critical",
            self.HIGH: "High Priority",
            self.NORMAL: "Normal Priority",
            self.LOW: "Low Priority",
            self.BACKGROUND: "Background"
        }.get(self, self.value)
    
    @property
    def priority_value(self) -> int:
        return {
            self.CRITICAL: 100,
            self.HIGH: 80,
            self.NORMAL: 50,
            self.LOW: 20,
            self.BACKGROUND: 10
        }.get(self, 0)


class JobType(PyEnum):
    DATA_PROCESSING = "DATA_PROCESSING"
    FILE_PROCESSING = "FILE_PROCESSING"
    EMAIL_SENDING = "EMAIL_SENDING"
    REPORT_GENERATION = "REPORT_GENERATION"
    BACKUP = "BACKUP"
    CLEANUP = "CLEANUP"
    SYNC = "SYNC"
    WEBHOOK_DELIVERY = "WEBHOOK_DELIVERY"
    NOTIFICATION = "NOTIFICATION"
    IMPORT = "IMPORT"
    EXPORT = "EXPORT"
    MAINTENANCE = "MAINTENANCE"
    CUSTOM = "CUSTOM"
    
    @property
    def display_name(self) -> str:
        return {
            self.DATA_PROCESSING: "Data Processing",
            self.FILE_PROCESSING: "File Processing",
            self.EMAIL_SENDING: "Email Sending",
            self.REPORT_GENERATION: "Report Generation",
            self.BACKUP: "Backup",
            self.CLEANUP: "Cleanup",
            self.SYNC: "Synchronization",
            self.WEBHOOK_DELIVERY: "Webhook Delivery",
            self.NOTIFICATION: "Notification",
            self.IMPORT: "Data Import",
            self.EXPORT: "Data Export",
            self.MAINTENANCE: "Maintenance",
            self.CUSTOM: "Custom Job"
        }.get(self, self.value)


class BackgroundJob(Base):
    __tablename__ = 'background_jobs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    job_type = Column(SQLEnum(JobType), nullable=False)
    
    status = Column(SQLEnum(JobStatus), nullable=False, default=JobStatus.PENDING)
    priority = Column(SQLEnum(JobPriority), nullable=False, default=JobPriority.NORMAL)
    
    queue_name = Column(String(100), default='default', nullable=False)
    worker_id = Column(String(100))
    
    org_id = Column(Integer, ForeignKey('orgs.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    project_id = Column(Integer, ForeignKey('projects.id'))
    
    job_class = Column(String(255), nullable=False)
    job_method = Column(String(100), default='perform')
    job_args = Column(JSON, default=list)
    job_kwargs = Column(JSON, default=dict)
    
    result = Column(JSON)
    error_message = Column(Text)
    error_traceback = Column(Text)
    
    progress_percentage = Column(Float, default=0.0)
    progress_message = Column(String(500))
    progress_data = Column(JSON, default=dict)
    
    max_retries = Column(Integer, default=3)
    retry_count = Column(Integer, default=0)
    retry_delay_seconds = Column(Integer, default=60)
    
    timeout_seconds = Column(Integer, default=3600)
    
    scheduled_at = Column(DateTime, nullable=False, default=datetime.now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    failed_at = Column(DateTime)
    
    duration_seconds = Column(Float)
    cpu_usage_percent = Column(Float)
    memory_usage_mb = Column(Float)
    
    cron_expression = Column(String(100))
    recurring = Column(Boolean, default=False)
    next_run_at = Column(DateTime)
    last_run_at = Column(DateTime)
    
    active = Column(Boolean, default=True, nullable=False)
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    context = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    org = relationship("Org", back_populates="background_jobs")
    user = relationship("User", foreign_keys=[user_id])
    project = relationship("Project", back_populates="background_jobs")
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    
    dependencies = relationship("JobDependency", foreign_keys="JobDependency.job_id", back_populates="job")
    dependents = relationship("JobDependency", foreign_keys="JobDependency.depends_on_job_id", back_populates="depends_on_job")
    
    __table_args__ = (
        Index('idx_background_job_status', 'status'),
        Index('idx_background_job_priority', 'priority'),
        Index('idx_background_job_type', 'job_type'),
        Index('idx_background_job_queue', 'queue_name'),
        Index('idx_background_job_worker', 'worker_id'),
        Index('idx_background_job_org_id', 'org_id'),
        Index('idx_background_job_user_id', 'user_id'),
        Index('idx_background_job_project_id', 'project_id'),
        Index('idx_background_job_scheduled_at', 'scheduled_at'),
        Index('idx_background_job_next_run_at', 'next_run_at'),
        Index('idx_background_job_active', 'active'),
        Index('idx_background_job_recurring', 'recurring'),
        CheckConstraint('progress_percentage >= 0 AND progress_percentage <= 100', name='ck_job_progress_range'),
        CheckConstraint('retry_count >= 0', name='ck_job_retry_count_non_negative'),
        CheckConstraint('max_retries >= 0', name='ck_job_max_retries_non_negative'),
        CheckConstraint('retry_delay_seconds > 0', name='ck_job_retry_delay_positive'),
        CheckConstraint('timeout_seconds > 0', name='ck_job_timeout_positive'),
    )
    
    DEFAULT_TIMEOUT = 3600
    MAX_RETRY_DELAY = 3600
    HIGH_PRIORITY_THRESHOLD = 80
    
    def __repr__(self):
        return f"<BackgroundJob(id={self.id}, job_id='{self.job_id}', name='{self.name}', status='{self.status.value}')>"
    
    def pending_(self) -> bool:
        """Check if job is pending (Rails pattern)"""
        return self.status == JobStatus.PENDING
    
    def running_(self) -> bool:
        """Check if job is running (Rails pattern)"""
        return self.status == JobStatus.RUNNING
    
    def completed_(self) -> bool:
        """Check if job is completed (Rails pattern)"""
        return self.status == JobStatus.COMPLETED
    
    def failed_(self) -> bool:
        """Check if job is failed (Rails pattern)"""
        return self.status == JobStatus.FAILED
    
    def cancelled_(self) -> bool:
        """Check if job is cancelled (Rails pattern)"""
        return self.status == JobStatus.CANCELLED
    
    def retrying_(self) -> bool:
        """Check if job is retrying (Rails pattern)"""
        return self.status == JobStatus.RETRYING
    
    def scheduled_(self) -> bool:
        """Check if job is scheduled (Rails pattern)"""
        return self.status == JobStatus.SCHEDULED
    
    def paused_(self) -> bool:
        """Check if job is paused (Rails pattern)"""
        return self.status == JobStatus.PAUSED
    
    def expired_(self) -> bool:
        """Check if job is expired (Rails pattern)"""
        return self.status == JobStatus.EXPIRED
    
    def active_(self) -> bool:
        """Check if job is active (Rails pattern)"""
        return self.active and self.status.is_active
    
    def finished_(self) -> bool:
        """Check if job is finished (Rails pattern)"""
        return self.status.is_completed
    
    def successful_(self) -> bool:
        """Check if job completed successfully (Rails pattern)"""
        return self.status == JobStatus.COMPLETED
    
    def recurring_(self) -> bool:
        """Check if job is recurring (Rails pattern)"""
        return self.recurring and self.cron_expression is not None
    
    def high_priority_(self) -> bool:
        """Check if job is high priority (Rails pattern)"""
        return self.priority.priority_value >= self.HIGH_PRIORITY_THRESHOLD
    
    def can_retry_(self) -> bool:
        """Check if job can be retried (Rails pattern)"""
        return (self.failed_() and 
                self.retry_count < self.max_retries and
                self.active)
    
    def should_run_(self) -> bool:
        """Check if job should run now (Rails pattern)"""
        return (self.active and 
                self.scheduled_at <= datetime.now() and
                self.status in [JobStatus.PENDING, JobStatus.SCHEDULED])
    
    def overdue_(self) -> bool:
        """Check if job is overdue (Rails pattern)"""
        return (self.scheduled_at < datetime.now() and 
                self.status in [JobStatus.PENDING, JobStatus.SCHEDULED])
    
    def timed_out_(self) -> bool:
        """Check if job has timed out (Rails pattern)"""
        if not self.started_at or not self.running_():
            return False
        elapsed = (datetime.now() - self.started_at).total_seconds()
        return elapsed > self.timeout_seconds
    
    def stuck_(self) -> bool:
        """Check if job appears stuck (Rails pattern)"""
        if not self.running_() or not self.started_at:
            return False
        elapsed = (datetime.now() - self.started_at).total_seconds()
        return elapsed > (self.timeout_seconds * 2)
    
    def needs_attention_(self) -> bool:
        """Check if job needs attention (Rails pattern)"""
        return (self.failed_() or 
                self.timed_out_() or
                self.stuck_() or
                (self.overdue_() and self.high_priority_()))
    
    def start_(self, worker_id: str = None) -> None:
        """Start job execution (Rails bang method pattern)"""
        self.status = JobStatus.RUNNING
        self.started_at = datetime.now()
        self.worker_id = worker_id
        self.updated_at = datetime.now()
    
    def complete_(self, result: Any = None) -> None:
        """Complete job successfully (Rails bang method pattern)"""
        self.status = JobStatus.COMPLETED
        self.completed_at = datetime.now()
        self.progress_percentage = 100.0
        
        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()
        
        if result is not None:
            self.result = result if isinstance(result, (dict, list)) else {'result': str(result)}
        
        if self.recurring_():
            self._schedule_next_run()
        
        self.updated_at = datetime.now()
    
    def fail_(self, error_message: str, error_traceback: str = None) -> None:
        """Fail job with error (Rails bang method pattern)"""
        self.status = JobStatus.FAILED
        self.failed_at = datetime.now()
        self.error_message = error_message
        self.error_traceback = error_traceback
        
        if self.started_at:
            self.duration_seconds = (self.failed_at - self.started_at).total_seconds()
        
        if self.can_retry_():
            self._schedule_retry()
        
        self.updated_at = datetime.now()
    
    def cancel_(self, reason: str = None) -> None:
        """Cancel job (Rails bang method pattern)"""
        self.status = JobStatus.CANCELLED
        self.completed_at = datetime.now()
        
        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['cancellation_reason'] = reason
        
        self.updated_at = datetime.now()
    
    def pause_(self, reason: str = None) -> None:
        """Pause job execution (Rails bang method pattern)"""
        self.status = JobStatus.PAUSED
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['pause_reason'] = reason
    
    def resume_(self) -> None:
        """Resume paused job (Rails bang method pattern)"""
        if self.status == JobStatus.PAUSED:
            self.status = JobStatus.PENDING
            self.updated_at = datetime.now()
    
    def retry_(self, delay_seconds: int = None) -> None:
        """Retry failed job (Rails bang method pattern)"""
        if self.can_retry_():
            self.status = JobStatus.RETRYING
            self.retry_count += 1
            
            if delay_seconds is None:
                delay_seconds = min(self.retry_delay_seconds * (2 ** (self.retry_count - 1)), self.MAX_RETRY_DELAY)
            
            self.scheduled_at = datetime.now() + timedelta(seconds=delay_seconds)
            self.error_message = None
            self.error_traceback = None
            self.updated_at = datetime.now()
    
    def reset_(self) -> None:
        """Reset job to pending state (Rails bang method pattern)"""
        self.status = JobStatus.PENDING
        self.started_at = None
        self.completed_at = None
        self.failed_at = None
        self.duration_seconds = None
        self.progress_percentage = 0.0
        self.progress_message = None
        self.error_message = None
        self.error_traceback = None
        self.worker_id = None
        self.result = None
        self.retry_count = 0
        self.scheduled_at = datetime.now()
        self.updated_at = datetime.now()
    
    def update_progress_(self, percentage: float, message: str = None, data: Dict[str, Any] = None) -> None:
        """Update job progress (Rails bang method pattern)"""
        if 0 <= percentage <= 100:
            self.progress_percentage = percentage
            self.progress_message = message
            if data:
                self.progress_data = data
            self.updated_at = datetime.now()
    
    def extend_timeout_(self, additional_seconds: int) -> None:
        """Extend job timeout (Rails bang method pattern)"""
        self.timeout_seconds += additional_seconds
        self.updated_at = datetime.now()
    
    def change_priority_(self, new_priority: JobPriority) -> None:
        """Change job priority (Rails bang method pattern)"""
        self.priority = new_priority
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to job (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from job (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def set_metadata_(self, key: str, value: Any) -> None:
        """Set metadata value (Rails bang method pattern)"""
        metadata = dict(self.extra_metadata or {})
        metadata[key] = value
        self.extra_metadata = metadata
        self.updated_at = datetime.now()
    
    def record_resource_usage_(self, cpu_percent: float = None, memory_mb: float = None) -> None:
        """Record resource usage (Rails bang method pattern)"""
        if cpu_percent is not None:
            self.cpu_usage_percent = cpu_percent
        if memory_mb is not None:
            self.memory_usage_mb = memory_mb
        self.updated_at = datetime.now()
    
    def _schedule_retry(self) -> None:
        """Schedule retry attempt (private helper)"""
        delay = min(self.retry_delay_seconds * (2 ** (self.retry_count - 1)), self.MAX_RETRY_DELAY)
        self.scheduled_at = datetime.now() + timedelta(seconds=delay)
        self.status = JobStatus.RETRYING
    
    def _schedule_next_run(self) -> None:
        """Schedule next recurring run (private helper)"""
        if self.recurring_() and self.cron_expression:
            # This would use a cron parser library to calculate next run time
            # For now, schedule for 1 hour later as placeholder
            self.next_run_at = datetime.now() + timedelta(hours=1)
            self.last_run_at = datetime.now()
    
    def elapsed_time(self) -> Optional[float]:
        """Calculate elapsed execution time (Rails pattern)"""
        if not self.started_at:
            return None
        
        end_time = self.completed_at or self.failed_at or datetime.now()
        return (end_time - self.started_at).total_seconds()
    
    def wait_time(self) -> float:
        """Calculate time waited before starting (Rails pattern)"""
        if not self.started_at:
            return (datetime.now() - self.scheduled_at).total_seconds()
        return (self.started_at - self.scheduled_at).total_seconds()
    
    def estimated_completion(self) -> Optional[datetime]:
        """Estimate completion time based on progress (Rails pattern)"""
        if not self.started_at or self.progress_percentage <= 0:
            return None
        
        elapsed = self.elapsed_time()
        if not elapsed:
            return None
        
        estimated_total = elapsed / (self.progress_percentage / 100)
        return self.started_at + timedelta(seconds=estimated_total)
    
    def runtime_efficiency(self) -> Optional[float]:
        """Calculate runtime efficiency score (Rails pattern)"""
        if not self.duration_seconds:
            return None
        
        # Higher score for jobs that complete quickly relative to their timeout
        efficiency = 1.0 - (self.duration_seconds / self.timeout_seconds)
        return max(0.0, min(1.0, efficiency))
    
    def success_rate(self) -> float:
        """Calculate success rate for this job type (Rails pattern)"""
        # This would query the database for historical success rates
        # For now, return a placeholder based on retry count
        if self.retry_count == 0:
            return 1.0
        return max(0.0, 1.0 - (self.retry_count / self.max_retries))
    
    def resource_usage_summary(self) -> Dict[str, Any]:
        """Get resource usage summary (Rails pattern)"""
        return {
            'cpu_usage_percent': self.cpu_usage_percent,
            'memory_usage_mb': self.memory_usage_mb,
            'duration_seconds': self.duration_seconds,
            'runtime_efficiency': self.runtime_efficiency(),
            'timeout_seconds': self.timeout_seconds
        }
    
    def execution_summary(self) -> Dict[str, Any]:
        """Get execution summary (Rails pattern)"""
        return {
            'job_id': self.job_id,
            'name': self.name,
            'status': self.status.value,
            'priority': self.priority.value,
            'progress_percentage': self.progress_percentage,
            'retry_count': self.retry_count,
            'duration_seconds': self.duration_seconds,
            'wait_time_seconds': self.wait_time(),
            'success_rate': self.success_rate(),
            'scheduled_at': self.scheduled_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'estimated_completion': self.estimated_completion().isoformat() if self.estimated_completion() else None
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate job health report (Rails pattern)"""
        return {
            'job_id': self.job_id,
            'healthy': not self.needs_attention_(),
            'active': self.active_(),
            'status': self.status.value,
            'overdue': self.overdue_(),
            'timed_out': self.timed_out_(),
            'stuck': self.stuck_(),
            'can_retry': self.can_retry_(),
            'needs_attention': self.needs_attention_(),
            'resource_usage': self.resource_usage_summary(),
            'execution_summary': self.execution_summary()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'job_id': self.job_id,
            'name': self.name,
            'description': self.description,
            'job_type': self.job_type.value,
            'status': self.status.value,
            'priority': self.priority.value,
            'queue_name': self.queue_name,
            'progress_percentage': self.progress_percentage,
            'progress_message': self.progress_message,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'recurring': self.recurring,
            'active': self.active,
            'tags': self.tags,
            'scheduled_at': self.scheduled_at.isoformat(),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'job_class': self.job_class,
                'job_method': self.job_method,
                'job_args': self.job_args,
                'job_kwargs': self.job_kwargs,
                'result': self.result,
                'error_message': self.error_message,
                'metadata': self.extra_metadata,
                'context': self.context
            })
        
        return result


class JobDependency(Base):
    __tablename__ = 'job_dependencies'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('background_jobs.id'), nullable=False)
    depends_on_job_id = Column(Integer, ForeignKey('background_jobs.id'), nullable=False)
    
    dependency_type = Column(String(50), default='completion', nullable=False)
    required = Column(Boolean, default=True, nullable=False)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    
    job = relationship("BackgroundJob", foreign_keys=[job_id], back_populates="dependencies")
    depends_on_job = relationship("BackgroundJob", foreign_keys=[depends_on_job_id], back_populates="dependents")
    
    __table_args__ = (
        Index('idx_job_dependency_job_id', 'job_id'),
        Index('idx_job_dependency_depends_on', 'depends_on_job_id'),
        UniqueConstraint('job_id', 'depends_on_job_id', name='uq_job_dependency_unique'),
    )
    
    def __repr__(self):
        return f"<JobDependency(job_id={self.job_id}, depends_on={self.depends_on_job_id})>"