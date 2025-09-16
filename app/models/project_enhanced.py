"""
Project Model - Enhanced with comprehensive Rails business logic patterns.
Core project management with Rails-style patterns for collaborative development workflows.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Set
from enum import Enum as PyEnum
import json
import secrets
import re
import logging
from ..database import Base

logger = logging.getLogger(__name__)


class ProjectStatuses(PyEnum):
    """Project status enumeration with Rails-style constants"""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE" 
    ARCHIVED = "ARCHIVED"
    TEMPLATE = "TEMPLATE"
    SUSPENDED = "SUSPENDED"
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DEPRECATED = "DEPRECATED"
    MAINTENANCE = "MAINTENANCE"
    DELETED = "DELETED"

    @property
    def display_name(self) -> str:
        """Human readable status name"""
        return self.value.replace('_', ' ').title()


class ProjectVisibilities(PyEnum):
    """Project visibility enumeration"""
    PRIVATE = "PRIVATE"
    ORG_WIDE = "ORG_WIDE"
    PUBLIC = "PUBLIC"
    TEAM_ONLY = "TEAM_ONLY"
    RESTRICTED = "RESTRICTED"

    @property
    def display_name(self) -> str:
        """Human readable visibility name"""
        return self.value.replace('_', ' ').title()


class ProjectPriorities(PyEnum):
    """Project priority enumeration"""
    LOWEST = "LOWEST"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    URGENT = "URGENT"

    @property
    def hierarchy_level(self) -> int:
        """Priority hierarchy level (lower = higher priority)"""
        hierarchy = {
            'URGENT': 1,
            'CRITICAL': 2,
            'HIGH': 3,
            'MEDIUM': 4,
            'LOW': 5,
            'LOWEST': 6
        }
        return hierarchy.get(self.value, 7)


class ProjectTypes(PyEnum):
    """Project type enumeration"""
    DATA_PIPELINE = "DATA_PIPELINE"
    ANALYTICS = "ANALYTICS"
    ML_PROJECT = "ML_PROJECT"
    ETL_WORKFLOW = "ETL_WORKFLOW"
    DASHBOARD = "DASHBOARD"
    API_INTEGRATION = "API_INTEGRATION"
    STREAMING = "STREAMING"
    BATCH_PROCESSING = "BATCH_PROCESSING"
    PROTOTYPE = "PROTOTYPE"
    PRODUCTION = "PRODUCTION"


class AccessLevels(PyEnum):
    """Access level enumeration"""
    READ = "READ"
    WRITE = "WRITE"
    ADMIN = "ADMIN"
    OWNER = "OWNER"


class Project(Base):
    __tablename__ = "projects"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uid = Column(String(24), unique=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Project classification
    status = Column(SQLEnum(ProjectStatuses), nullable=False, default=ProjectStatuses.DRAFT, index=True)
    visibility = Column(SQLEnum(ProjectVisibilities), nullable=False, default=ProjectVisibilities.PRIVATE)
    priority = Column(SQLEnum(ProjectPriorities), nullable=False, default=ProjectPriorities.MEDIUM)
    project_type = Column(SQLEnum(ProjectTypes), default=ProjectTypes.DATA_PIPELINE)
    
    # Metadata
    version = Column(String(50), default="1.0")
    tags = Column(JSON)  # JSON array of tags
    extra_metadata = Column(JSON)  # JSON object of additional metadata
    settings = Column(JSON)  # JSON object for project-specific settings
    
    # Tracking fields
    last_activity_at = Column(DateTime, default=func.now())
    flow_count = Column(Integer, default=0)
    active_flow_count = Column(Integer, default=0)
    completed_flow_count = Column(Integer, default=0)
    failed_flow_count = Column(Integer, default=0)
    
    # Performance metrics
    total_records_processed = Column(Integer, default=0)
    total_data_processed_gb = Column(Float, default=0.0)
    avg_execution_time_minutes = Column(Float)
    success_rate_percent = Column(Float, default=100.0)
    
    # Resource limits and quotas
    max_flows = Column(Integer)
    max_data_gb = Column(Float)
    max_execution_time_minutes = Column(Integer, default=1440)  # 24 hours
    
    # Collaboration settings
    allow_member_access = Column(Boolean, default=True)
    require_approval = Column(Boolean, default=False)
    auto_share_with_team = Column(Boolean, default=False)
    
    # Lifecycle timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    archived_at = Column(DateTime)
    published_at = Column(DateTime)
    deprecated_at = Column(DateTime)
    last_deployed_at = Column(DateTime)
    
    # Template fields
    is_template = Column(Boolean, default=False)
    template_description = Column(Text)
    template_category = Column(String(100))
    template_usage_count = Column(Integer, default=0)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    copied_from_id = Column(Integer, ForeignKey("projects.id"), index=True)
    archived_by_id = Column(Integer, ForeignKey("users.id"), index=True)
    
    # External integration
    client_identifier = Column(String(255))
    client_url = Column(String(500))
    external_id = Column(String(255), index=True)
    git_repository_url = Column(String(500))
    documentation_url = Column(String(500))
    
    # Relationships
    owner = relationship("User", back_populates="projects", foreign_keys=[owner_id])
    org = relationship("Org", back_populates="projects")
    team = relationship("Team", back_populates="projects")
    flows = relationship("Flow", back_populates="project")
    flow_nodes = relationship("FlowNode", back_populates="project")
    copied_from = relationship("Project", remote_side="Project.id", foreign_keys=[copied_from_id])
    project_copies = relationship("Project", remote_side="Project.copied_from_id")
    archived_by = relationship("User", foreign_keys=[archived_by_id])
    # project_permissions = relationship("ProjectPermission", back_populates="project")
    # project_collaborators = relationship("ProjectCollaborator", back_populates="project")
    
    # Rails business logic constants
    STATUSES = {
        "active": ProjectStatuses.ACTIVE.value,
        "inactive": ProjectStatuses.INACTIVE.value,
        "archived": ProjectStatuses.ARCHIVED.value,
        "template": ProjectStatuses.TEMPLATE.value,
        "suspended": ProjectStatuses.SUSPENDED.value,
        "draft": ProjectStatuses.DRAFT.value,
        "published": ProjectStatuses.PUBLISHED.value,
        "deprecated": ProjectStatuses.DEPRECATED.value
    }
    
    DEFAULT_MAX_FLOWS = 100
    DEFAULT_MAX_DATA_GB = 100.0
    DEFAULT_MAX_EXECUTION_TIME = 1440  # 24 hours
    STALE_THRESHOLD_DAYS = 30
    INACTIVE_THRESHOLD_DAYS = 90
    SUCCESS_RATE_THRESHOLD = 80.0
    PERFORMANCE_ISSUE_THRESHOLD = 60  # minutes

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.uid:
            self.ensure_uid_()
        # Rails-style instance variables
        self._cache = {}
        self._collaborator_cache = {}
        self._metrics_cache = {}

    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if project is active (Rails pattern)"""
        return (self.status == ProjectStatuses.ACTIVE and 
                not self.archived_() and 
                not self.suspended_())
    
    def inactive_(self) -> bool:
        """Check if project is inactive (Rails pattern)"""
        return self.status == ProjectStatuses.INACTIVE
        
    def archived_(self) -> bool:
        """Check if project is archived (Rails pattern)"""
        return self.status == ProjectStatuses.ARCHIVED
        
    def template_(self) -> bool:
        """Check if project is a template (Rails pattern)"""
        return self.status == ProjectStatuses.TEMPLATE or self.is_template
        
    def suspended_(self) -> bool:
        """Check if project is suspended (Rails pattern)"""
        return self.status == ProjectStatuses.SUSPENDED
    
    def draft_(self) -> bool:
        """Check if project is in draft status (Rails pattern)"""
        return self.status == ProjectStatuses.DRAFT
    
    def published_(self) -> bool:
        """Check if project is published (Rails pattern)"""
        return self.status == ProjectStatuses.PUBLISHED
    
    def deprecated_(self) -> bool:
        """Check if project is deprecated (Rails pattern)"""
        return self.status == ProjectStatuses.DEPRECATED
    
    def maintenance_(self) -> bool:
        """Check if project is in maintenance mode (Rails pattern)"""
        return self.status == ProjectStatuses.MAINTENANCE
    
    def deleted_(self) -> bool:
        """Check if project is deleted (Rails pattern)"""
        return self.status == ProjectStatuses.DELETED

    def public_(self) -> bool:
        """Check if project is public (Rails pattern)"""
        return self.visibility == ProjectVisibilities.PUBLIC
        
    def private_(self) -> bool:
        """Check if project is private (Rails pattern)"""
        return self.visibility == ProjectVisibilities.PRIVATE
        
    def org_wide_(self) -> bool:
        """Check if project is org-wide visible (Rails pattern)"""
        return self.visibility == ProjectVisibilities.ORG_WIDE
    
    def team_only_(self) -> bool:
        """Check if project is team-only visible (Rails pattern)"""
        return self.visibility == ProjectVisibilities.TEAM_ONLY
    
    def restricted_(self) -> bool:
        """Check if project has restricted access (Rails pattern)"""
        return self.visibility == ProjectVisibilities.RESTRICTED

    def copy_(self) -> bool:
        """Check if project is a copy (Rails pattern)"""
        return self.copied_from_id is not None
        
    def original_(self) -> bool:
        """Check if project is original (not a copy) (Rails pattern)"""
        return self.copied_from_id is None
        
    def has_flows_(self) -> bool:
        """Check if project has any flows (Rails pattern)"""
        return (self.flow_count or 0) > 0
        
    def has_active_flows_(self) -> bool:
        """Check if project has active flows (Rails pattern)"""
        return (self.active_flow_count or 0) > 0
    
    def has_team_(self) -> bool:
        """Check if project is assigned to a team (Rails pattern)"""
        return self.team_id is not None
    
    def high_priority_(self) -> bool:
        """Check if project has high priority (Rails pattern)"""
        return self.priority in [ProjectPriorities.HIGH, ProjectPriorities.CRITICAL, ProjectPriorities.URGENT]
        
    def recently_active_(self, days: int = 7) -> bool:
        """Check if project was recently active (Rails pattern)"""
        if not self.last_activity_at:
            return False
        return self.last_activity_at >= datetime.now() - timedelta(days=days)
    
    def stale_(self, days: int = None) -> bool:
        """Check if project is stale (Rails pattern)"""
        threshold_days = days or self.STALE_THRESHOLD_DAYS
        if not self.last_activity_at:
            return True
        return self.last_activity_at < datetime.now() - timedelta(days=threshold_days)
    
    def long_inactive_(self, days: int = None) -> bool:
        """Check if project has been inactive for a long time (Rails pattern)"""
        threshold_days = days or self.INACTIVE_THRESHOLD_DAYS
        if not self.last_activity_at:
            return True
        return self.last_activity_at < datetime.now() - timedelta(days=threshold_days)
    
    def performance_issues_(self) -> bool:
        """Check if project has performance issues (Rails pattern)"""
        return (self.avg_execution_time_minutes and 
                self.avg_execution_time_minutes > self.PERFORMANCE_ISSUE_THRESHOLD)
    
    def low_success_rate_(self) -> bool:
        """Check if project has low success rate (Rails pattern)"""
        return (self.success_rate_percent and 
                self.success_rate_percent < self.SUCCESS_RATE_THRESHOLD)
    
    def healthy_(self) -> bool:
        """Check if project is healthy (Rails pattern)"""
        return (self.active_() and 
                not self.performance_issues_() and
                not self.low_success_rate_() and
                not self.stale_())
    
    def at_flow_limit_(self) -> bool:
        """Check if project is at flow limit (Rails pattern)"""
        return (self.max_flows and 
                (self.flow_count or 0) >= self.max_flows)
    
    def at_data_limit_(self) -> bool:
        """Check if project is at data limit (Rails pattern)"""
        return (self.max_data_gb and 
                (self.total_data_processed_gb or 0.0) >= self.max_data_gb)
    
    def has_external_integration_(self) -> bool:
        """Check if project has external integrations (Rails pattern)"""
        return bool(self.git_repository_url or self.client_url or self.external_id)
    
    def documented_(self) -> bool:
        """Check if project is documented (Rails pattern)"""
        return bool(self.documentation_url or (self.description and len(self.description) > 100))
    
    def accessible_by_(self, user, access_level: str = 'read') -> bool:
        """Check if user can access project (Rails pattern)"""
        if not user:
            return False
            
        # Owner always has access
        if self.owner_id == user.id:
            return True
            
        # Super users have access to all
        if user.super_user_():
            return True
            
        # Public projects are readable by anyone in same org
        if self.public_() and access_level == 'read':
            return user.default_org_id == self.org_id
            
        # Org-wide projects are accessible to org members
        if self.org_wide_() and user.default_org_id == self.org_id:
            if not self.allow_member_access and access_level != 'read':
                return False
            return True
        
        # Team projects are accessible to team members
        if self.team_only_() and self.team_id:
            return user.team_member_(self.team_id) if hasattr(user, 'team_member_') else False
            
        # Check explicit permissions (would integrate with permission system)
        return False
    
    def editable_by_(self, user) -> bool:
        """Check if user can edit project (Rails pattern)"""
        return self.accessible_by_(user, 'write')
    
    def deletable_by_(self, user) -> bool:
        """Check if user can delete project (Rails pattern)"""
        return (self.accessible_by_(user, 'admin') or 
                self.owner_id == user.id) and not self.has_active_flows_()
    
    def can_be_copied_(self) -> bool:
        """Check if project can be copied (Rails pattern)"""
        return not self.deleted_() and (self.published_() or self.template_())
    
    def can_be_archived_(self) -> bool:
        """Check if project can be archived (Rails pattern)"""
        return not self.archived_() and not self.has_active_flows_()
    
    def can_be_published_(self) -> bool:
        """Check if project can be published (Rails pattern)"""
        return (self.draft_() or self.active_()) and self.has_flows_() and self.documented_()

    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self) -> None:
        """Activate project (Rails bang method pattern)"""
        if self.active_():
            return
        
        if self.archived_():
            self.archived_at = None
            self.archived_by_id = None
            
        self.status = ProjectStatuses.ACTIVE
        self.updated_at = datetime.now()
        self.update_activity_()
        
        logger.info(f"Project activated: {self.name} (ID: {self.id})")
    
    def deactivate_(self, reason: Optional[str] = None) -> None:
        """Deactivate project (Rails bang method pattern)"""
        if self.inactive_():
            return
        
        self.status = ProjectStatuses.INACTIVE
        self.updated_at = datetime.now()
        self.update_activity_()
        
        if reason:
            self._update_metadata('deactivation_reason', reason)
        
        logger.info(f"Project deactivated: {self.name} (ID: {self.id})")
    
    def archive_(self, archived_by_user=None, reason: Optional[str] = None) -> None:
        """Archive project (Rails bang method pattern)"""
        if self.archived_():
            return
        
        if self.has_active_flows_():
            raise ValueError("Cannot archive project with active flows")
        
        self.status = ProjectStatuses.ARCHIVED
        self.archived_at = datetime.now()
        self.archived_by_id = archived_by_user.id if archived_by_user else None
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('archive_reason', reason)
        
        logger.info(f"Project archived: {self.name} (ID: {self.id})")
    
    def unarchive_(self) -> None:
        """Unarchive project (Rails bang method pattern)"""
        if not self.archived_():
            return
        
        self.status = ProjectStatuses.ACTIVE
        self.archived_at = None
        self.archived_by_id = None
        self.updated_at = datetime.now()
        self.update_activity_()
        
        logger.info(f"Project unarchived: {self.name} (ID: {self.id})")
    
    def suspend_(self, reason: Optional[str] = None) -> None:
        """Suspend project (Rails bang method pattern)"""
        if self.suspended_():
            return
        
        self.status = ProjectStatuses.SUSPENDED
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('suspension_reason', reason)
        
        # Pause all active flows
        self._pause_all_flows()
        
        logger.warning(f"Project suspended: {self.name} (ID: {self.id}), reason: {reason}")
    
    def unsuspend_(self) -> None:
        """Remove suspension from project (Rails bang method pattern)"""
        if not self.suspended_():
            return
        
        self.status = ProjectStatuses.ACTIVE
        self.updated_at = datetime.now()
        self.update_activity_()
        
        logger.info(f"Project suspension removed: {self.name} (ID: {self.id})")
    
    def publish_(self, publish_notes: str = None) -> None:
        """Publish project (Rails bang method pattern)"""
        if not self.can_be_published_():
            raise ValueError("Project cannot be published - missing requirements")
        
        self.status = ProjectStatuses.PUBLISHED
        self.published_at = datetime.now()
        self.updated_at = datetime.now()
        
        if publish_notes:
            self._update_metadata('publish_notes', publish_notes)
        
        logger.info(f"Project published: {self.name} (ID: {self.id})")
    
    def deprecate_(self, reason: Optional[str] = None) -> None:
        """Deprecate project (Rails bang method pattern)"""
        self.status = ProjectStatuses.DEPRECATED
        self.deprecated_at = datetime.now()
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('deprecation_reason', reason)
        
        logger.info(f"Project deprecated: {self.name} (ID: {self.id})")
    
    def make_template_(self, template_description: Optional[str] = None, 
                      category: Optional[str] = None) -> None:
        """Convert project to template (Rails bang method pattern)"""
        self.status = ProjectStatuses.TEMPLATE
        self.is_template = True
        self.template_description = template_description
        self.template_category = category
        self.updated_at = datetime.now()
        
        logger.info(f"Project converted to template: {self.name} (ID: {self.id})")
    
    def make_public_(self) -> None:
        """Make project public (Rails bang method pattern)"""
        self.visibility = ProjectVisibilities.PUBLIC
        self.updated_at = datetime.now()
        
        logger.info(f"Project made public: {self.name} (ID: {self.id})")
    
    def make_private_(self) -> None:
        """Make project private (Rails bang method pattern)"""
        self.visibility = ProjectVisibilities.PRIVATE
        self.updated_at = datetime.now()
        
        logger.info(f"Project made private: {self.name} (ID: {self.id})")
    
    def make_org_wide_(self) -> None:
        """Make project org-wide visible (Rails bang method pattern)"""
        self.visibility = ProjectVisibilities.ORG_WIDE
        self.updated_at = datetime.now()
        
        logger.info(f"Project made org-wide: {self.name} (ID: {self.id})")
    
    def restrict_access_(self) -> None:
        """Restrict project access (Rails bang method pattern)"""
        self.visibility = ProjectVisibilities.RESTRICTED
        self.allow_member_access = False
        self.require_approval = True
        self.updated_at = datetime.now()
        
        logger.info(f"Project access restricted: {self.name} (ID: {self.id})")
    
    def update_activity_(self) -> None:
        """Update last activity timestamp (Rails bang method pattern)"""
        self.last_activity_at = datetime.now()
        # Don't update updated_at for activity tracking
    
    def increment_flow_count_(self, active: bool = True) -> None:
        """Increment flow count (Rails bang method pattern)"""
        self.flow_count = (self.flow_count or 0) + 1
        if active:
            self.active_flow_count = (self.active_flow_count or 0) + 1
        self.update_activity_()
        self.updated_at = datetime.now()
    
    def decrement_flow_count_(self, was_active: bool = True) -> None:
        """Decrement flow count (Rails bang method pattern)"""
        if self.flow_count and self.flow_count > 0:
            self.flow_count -= 1
        if was_active and self.active_flow_count and self.active_flow_count > 0:
            self.active_flow_count -= 1
        self.update_activity_()
        self.updated_at = datetime.now()
    
    def increment_completed_flows_(self) -> None:
        """Increment completed flow count (Rails bang method pattern)"""
        self.completed_flow_count = (self.completed_flow_count or 0) + 1
        self.update_activity_()
        self._recalculate_success_rate()
    
    def increment_failed_flows_(self) -> None:
        """Increment failed flow count (Rails bang method pattern)"""
        self.failed_flow_count = (self.failed_flow_count or 0) + 1
        self.update_activity_()
        self._recalculate_success_rate()
    
    def increment_template_usage_(self) -> None:
        """Increment template usage count (Rails bang method pattern)"""
        if not self.template_():
            return
        
        self.template_usage_count = (self.template_usage_count or 0) + 1
        self.updated_at = datetime.now()
    
    def update_data_processed_(self, data_gb: float) -> None:
        """Update data processed metrics (Rails bang method pattern)"""
        self.total_data_processed_gb = (self.total_data_processed_gb or 0.0) + data_gb
        self.total_records_processed = (self.total_records_processed or 0) + 1
        self.update_activity_()
        self.updated_at = datetime.now()
        
        # Check if approaching data limit
        if self.at_data_limit_():
            logger.warning(f"Project {self.name} at data limit: {self.total_data_processed_gb}/{self.max_data_gb} GB")
    
    def refresh_counts_(self) -> None:
        """Refresh flow counts from database (Rails bang method pattern)"""
        # This would query the Flow model to get accurate counts
        if hasattr(self, 'flows') and self.flows:
            self.flow_count = len(self.flows)
            self.active_flow_count = sum(1 for flow in self.flows 
                                       if hasattr(flow, 'active_') and flow.active_())
            self.completed_flow_count = sum(1 for flow in self.flows 
                                          if hasattr(flow, 'completed_') and flow.completed_())
            self.failed_flow_count = sum(1 for flow in self.flows 
                                       if hasattr(flow, 'failed_') and flow.failed_())
        else:
            self.flow_count = 0
            self.active_flow_count = 0
            self.completed_flow_count = 0
            self.failed_flow_count = 0
        
        self._recalculate_success_rate()
        self.updated_at = datetime.now()

    # ========================================
    # Rails Helper and Utility Methods
    # ========================================
    
    def ensure_uid_(self) -> None:
        """Ensure unique UID is set (Rails before_save pattern)"""
        if self.uid:
            return
        
        max_attempts = 10
        for _ in range(max_attempts):
            uid = secrets.token_hex(12)  # 24 character hex string
            # This would check uniqueness against database
            self.uid = uid
            return
        
        raise ValueError("Failed to generate unique project UID")
    
    def _update_metadata(self, key: str, value: Any) -> None:
        """Update metadata field (Rails helper pattern)"""
        try:
            current_meta = self.extra_metadata or {}
        except (TypeError):
            current_meta = {}
            
        current_meta[key] = value
        self.extra_metadata = current_meta
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Get metadata value (Rails helper pattern)"""
        try:
            meta = self.extra_metadata or {}
            return meta.get(key, default)
        except (TypeError):
            return default
    
    def get_settings(self) -> Dict[str, Any]:
        """Get project settings (Rails pattern)"""
        try:
            return self.settings or {}
        except (TypeError):
            return {}
    
    def set_setting_(self, key: str, value: Any) -> None:
        """Set project setting (Rails bang method pattern)"""
        settings = self.get_settings()
        settings[key] = value
        self.settings = settings
        self.updated_at = datetime.now()
    
    def get_setting(self, key: str, default=None) -> Any:
        """Get project setting value (Rails pattern)"""
        settings = self.get_settings()
        return settings.get(key, default)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        try:
            return self.tags or []
        except (TypeError):
            return []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    def set_tags_(self, tags: Union[List[str], Set[str], str]) -> None:
        """Set project tags (Rails bang method pattern)"""
        if isinstance(tags, str):
            # Handle comma-separated string
            tag_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
        else:
            tag_list = list(set(tags)) if tags else []
        
        self.tags = tag_list
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add a tag to project (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.add(tag.strip())
        self.set_tags_(current_tags)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove a tag from project (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.discard(tag.strip())
        self.set_tags_(current_tags)
    
    def has_tag_(self, tag: str) -> bool:
        """Check if project has specific tag (Rails pattern)"""
        return tag.strip() in self.tags_list()
    
    def _pause_all_flows(self) -> None:
        """Pause all flows in project (Rails helper pattern)"""
        # This would pause all flows - implementation depends on flow system
        logger.info(f"Pausing all flows for project: {self.name}")
    
    def _recalculate_success_rate(self) -> None:
        """Recalculate success rate (Rails helper pattern)"""
        total_completed = (self.completed_flow_count or 0) + (self.failed_flow_count or 0)
        if total_completed > 0:
            self.success_rate_percent = ((self.completed_flow_count or 0) / total_completed) * 100
        else:
            self.success_rate_percent = 100.0
    
    def copy_from_(self, source_project, new_name: Optional[str] = None, 
                  copy_flows: bool = True, copy_settings: bool = True) -> None:
        """Create project as copy of another (Rails bang method pattern)"""
        if not source_project:
            raise ValueError("Source project is required")
        
        if not source_project.can_be_copied_():
            raise ValueError("Source project cannot be copied")
            
        self.copied_from_id = source_project.id
        self.name = new_name or f"Copy of {source_project.name}"
        self.description = source_project.description
        self.priority = source_project.priority
        self.project_type = source_project.project_type
        self.visibility = ProjectVisibilities.PRIVATE  # Always start as private
        
        # Copy tags and metadata
        self.set_tags_(source_project.tags_list())
        
        if copy_settings:
            self.settings = source_project.settings
            self.max_flows = source_project.max_flows
            self.max_data_gb = source_project.max_data_gb
            self.max_execution_time_minutes = source_project.max_execution_time_minutes
        
        # Update metadata
        self._update_metadata('copied_from_id', source_project.id)
        self._update_metadata('copied_at', datetime.now().isoformat())
        self._update_metadata('source_project_name', source_project.name)
        
        # Increment template usage if copying from template
        if source_project.template_():
            source_project.increment_template_usage_()
        
        self.updated_at = datetime.now()
        
        logger.info(f"Project copied from {source_project.name}: {self.name}")

    # ========================================
    # Rails Class Methods and Scopes
    # ========================================
    
    @classmethod
    def active(cls):
        """Scope for active projects (Rails scope pattern)"""
        return cls.status == ProjectStatuses.ACTIVE
    
    @classmethod
    def inactive(cls):
        """Scope for inactive projects (Rails scope pattern)"""
        return cls.status == ProjectStatuses.INACTIVE
    
    @classmethod
    def archived(cls):
        """Scope for archived projects (Rails scope pattern)"""
        return cls.status == ProjectStatuses.ARCHIVED
    
    @classmethod
    def templates(cls):
        """Scope for template projects (Rails scope pattern)"""
        from sqlalchemy import or_
        return or_(cls.status == ProjectStatuses.TEMPLATE, cls.is_template.is_(True))
    
    @classmethod
    def public(cls):
        """Scope for public projects (Rails scope pattern)"""
        return cls.visibility == ProjectVisibilities.PUBLIC
    
    @classmethod
    def high_priority(cls):
        """Scope for high priority projects (Rails scope pattern)"""
        return cls.priority.in_([ProjectPriorities.HIGH, ProjectPriorities.CRITICAL, ProjectPriorities.URGENT])
    
    @classmethod
    def recent(cls, days: int = 7):
        """Scope for recently active projects (Rails scope pattern)"""
        cutoff_date = datetime.now() - timedelta(days=days)
        return cls.last_activity_at >= cutoff_date
    
    @classmethod
    def stale(cls, days: int = 30):
        """Scope for stale projects (Rails scope pattern)"""
        cutoff_date = datetime.now() - timedelta(days=days)
        from sqlalchemy import or_
        return or_(cls.last_activity_at < cutoff_date, cls.last_activity_at.is_(None))
    
    @classmethod
    def by_owner(cls, owner_id: int):
        """Scope for projects by owner (Rails scope pattern)"""
        return cls.owner_id == owner_id
    
    @classmethod
    def by_org(cls, org_id: int):
        """Scope for projects by organization (Rails scope pattern)"""
        return cls.org_id == org_id
    
    @classmethod
    def by_team(cls, team_id: int):
        """Scope for projects by team (Rails scope pattern)"""
        return cls.team_id == team_id
    
    @classmethod
    def by_type(cls, project_type: ProjectTypes):
        """Scope for projects by type (Rails scope pattern)"""
        return cls.project_type == project_type
    
    @classmethod
    def accessible_to(cls, user, access_level: str = 'read'):
        """Get projects accessible to user (Rails scope pattern)"""
        if not user:
            return cls.query.filter(False)  # Empty query
        
        # Start with user's own projects
        from sqlalchemy import or_, and_
        
        conditions = [cls.owner_id == user.id]
        
        # Add public projects in same org
        if access_level == 'read':
            conditions.append(
                and_(cls.visibility == ProjectVisibilities.PUBLIC,
                     cls.org_id == user.default_org_id)
            )
            
            # Add org-wide projects
            conditions.append(
                and_(cls.visibility == ProjectVisibilities.ORG_WIDE,
                     cls.org_id == user.default_org_id)
            )
        
        # Super users can access all
        if user.super_user_():
            return cls.query  # All projects
        
        return or_(*conditions)
    
    @classmethod
    def find_by_uid(cls, uid: str):
        """Find project by UID (Rails finder pattern)"""
        return cls.query.filter_by(uid=uid).first()
    
    @classmethod
    def find_by_uid_(cls, uid: str):
        """Find project by UID or raise exception (Rails bang finder pattern)"""
        project = cls.find_by_uid(uid)
        if not project:
            raise ValueError(f"Project with UID '{uid}' not found")
        return project
    
    @classmethod
    def create_with_defaults(cls, owner, org, name: str, project_type: ProjectTypes = ProjectTypes.DATA_PIPELINE):
        """Factory method to create project with defaults (Rails pattern)"""
        project = cls(
            name=name,
            owner=owner,
            org=org,
            project_type=project_type,
            status=ProjectStatuses.DRAFT,
            visibility=ProjectVisibilities.PRIVATE,
            priority=ProjectPriorities.MEDIUM,
            max_flows=cls.DEFAULT_MAX_FLOWS,
            max_data_gb=cls.DEFAULT_MAX_DATA_GB,
            max_execution_time_minutes=cls.DEFAULT_MAX_EXECUTION_TIME,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        project.ensure_uid_()
        return project
    
    @classmethod
    def create_from_template(cls, template_project, owner, org, new_name: Optional[str] = None):
        """Factory method to create project from template (Rails pattern)"""
        if not template_project or not template_project.template_():
            raise ValueError("Invalid template project")
        
        project = cls.create_with_defaults(
            owner=owner,
            org=org,
            name=new_name or f"{template_project.name} - {datetime.now().strftime('%Y%m%d')}",
            project_type=template_project.project_type
        )
        
        project.copy_from_(template_project, new_name)
        return project

    # ========================================
    # Rails Validation Methods
    # ========================================
    
    def validate_(self) -> List[str]:
        """Validate project data (Rails validation pattern)"""
        errors = []
        
        if not self.name or not self.name.strip():
            errors.append("Name cannot be blank")
        elif len(self.name) > 255:
            errors.append("Name is too long (maximum 255 characters)")
        
        if self.name and not re.match(r'^[\w\s\-\.\(\)&]+$', self.name):
            errors.append("Name contains invalid characters")
        
        if not self.owner_id:
            errors.append("Owner is required")
        
        if not self.org_id:
            errors.append("Organization is required")
        
        if self.client_url and len(self.client_url) > 500:
            errors.append("Client URL is too long (maximum 500 characters)")
        
        if self.description and len(self.description) > 10000:
            errors.append("Description is too long (maximum 10,000 characters)")
        
        if self.template_description and len(self.template_description) > 1000:
            errors.append("Template description is too long (maximum 1,000 characters)")
        
        if self.max_flows and self.max_flows < 1:
            errors.append("Max flows must be positive")
        
        if self.max_data_gb and self.max_data_gb < 0:
            errors.append("Max data GB cannot be negative")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if project is valid (Rails validation pattern)"""
        return len(self.validate_()) == 0

    # ========================================
    # Rails Display and Formatting Methods
    # ========================================
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        if self.copy_():
            return f"{self.name} (Copy)"
        elif self.template_():
            return f"{self.name} (Template)"
        return self.name or f"Project #{self.id}"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        return self.status.display_name if hasattr(self.status, 'display_name') else str(self.status)
    
    def visibility_display(self) -> str:
        """Get human-readable visibility (Rails pattern)"""
        return self.visibility.display_name if hasattr(self.visibility, 'display_name') else str(self.visibility)
    
    def priority_display(self) -> str:
        """Get human-readable priority (Rails pattern)"""
        priority_map = {
            ProjectPriorities.URGENT: "🔴 Urgent",
            ProjectPriorities.CRITICAL: "🟠 Critical",
            ProjectPriorities.HIGH: "🟡 High",
            ProjectPriorities.MEDIUM: "🔵 Medium",
            ProjectPriorities.LOW: "🟢 Low",
            ProjectPriorities.LOWEST: "⚪ Lowest"
        }
        return priority_map.get(self.priority, "🔵 Medium")
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            ProjectStatuses.ACTIVE: 'green',
            ProjectStatuses.DRAFT: 'blue',
            ProjectStatuses.PUBLISHED: 'teal',
            ProjectStatuses.TEMPLATE: 'purple',
            ProjectStatuses.INACTIVE: 'yellow',
            ProjectStatuses.SUSPENDED: 'orange',
            ProjectStatuses.ARCHIVED: 'gray',
            ProjectStatuses.DEPRECATED: 'red',
            ProjectStatuses.MAINTENANCE: 'orange',
            ProjectStatuses.DELETED: 'red'
        }
        return status_colors.get(self.status, 'gray')
    
    def activity_summary(self) -> Dict[str, Any]:
        """Get activity summary (Rails pattern)"""
        return {
            'flow_count': self.flow_count or 0,
            'active_flow_count': self.active_flow_count or 0,
            'completed_flow_count': self.completed_flow_count or 0,
            'failed_flow_count': self.failed_flow_count or 0,
            'success_rate_percent': self.success_rate_percent or 0.0,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'days_since_activity': (datetime.now() - self.last_activity_at).days if self.last_activity_at else None,
            'recently_active': self.recently_active_(),
            'stale': self.stale_(),
            'healthy': self.healthy_()
        }
    
    def performance_summary(self) -> Dict[str, Any]:
        """Get performance summary (Rails pattern)"""
        return {
            'total_records_processed': self.total_records_processed or 0,
            'total_data_processed_gb': self.total_data_processed_gb or 0.0,
            'avg_execution_time_minutes': self.avg_execution_time_minutes,
            'success_rate_percent': self.success_rate_percent or 0.0,
            'performance_issues': self.performance_issues_(),
            'low_success_rate': self.low_success_rate_()
        }
    
    def resource_summary(self) -> Dict[str, Any]:
        """Get resource usage summary (Rails pattern)"""
        return {
            'flows': {
                'current': self.flow_count or 0,
                'max': self.max_flows,
                'at_limit': self.at_flow_limit_()
            },
            'data_gb': {
                'current': self.total_data_processed_gb or 0.0,
                'max': self.max_data_gb,
                'at_limit': self.at_data_limit_()
            },
            'execution_time': {
                'max_minutes': self.max_execution_time_minutes,
                'avg_minutes': self.avg_execution_time_minutes
            }
        }

    # ========================================
    # Rails API Serialization Methods
    # ========================================
    
    def to_dict(self, include_metadata: bool = False, include_flows: bool = False,
               include_performance: bool = False) -> Dict[str, Any]:
        """Convert project to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'description': self.description,
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'status_display': self.status_display(),
            'status_color': self.status_color(),
            'visibility': self.visibility.value if hasattr(self.visibility, 'value') else str(self.visibility),
            'visibility_display': self.visibility_display(),
            'priority': self.priority.value if hasattr(self.priority, 'value') else str(self.priority),
            'priority_display': self.priority_display(),
            'project_type': self.project_type.value if hasattr(self.project_type, 'value') else str(self.project_type),
            'version': self.version,
            'active': self.active_(),
            'archived': self.archived_(),
            'template': self.template_(),
            'copy': self.copy_(),
            'public': self.public_(),
            'has_flows': self.has_flows_(),
            'documented': self.documented_(),
            'healthy': self.healthy_(),
            'client_identifier': self.client_identifier,
            'client_url': self.client_url,
            'git_repository_url': self.git_repository_url,
            'documentation_url': self.documentation_url,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'team_id': self.team_id,
            'copied_from_id': self.copied_from_id,
            'tags': self.tags_list(),
            'activity_summary': self.activity_summary(),
            'resource_summary': self.resource_summary()
        }
        
        if self.archived_():
            result.update({
                'archived_at': self.archived_at.isoformat() if self.archived_at else None,
                'archived_by_id': self.archived_by_id
            })
        
        if self.template_():
            result.update({
                'template_description': self.template_description,
                'template_category': self.template_category,
                'template_usage_count': self.template_usage_count or 0
            })
        
        if include_performance:
            result['performance_summary'] = self.performance_summary()
        
        if include_metadata:
            result['metadata'] = self.extra_metadata
            result['settings'] = self.get_settings()
        
        if include_flows:
            result['flows'] = [flow.to_summary_dict() for flow in (self.flows or [])]
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert project to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'priority': self.priority.value if hasattr(self.priority, 'value') else str(self.priority),
            'project_type': self.project_type.value if hasattr(self.project_type, 'value') else str(self.project_type),
            'flow_count': self.flow_count or 0,
            'active': self.active_(),
            'template': self.template_(),
            'healthy': self.healthy_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None
        }
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Convert project to dictionary for audit logging (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'flow_count': self.flow_count or 0,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    # ========================================
    # Legacy Methods for Backwards Compatibility
    # ========================================
    
    def active(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.active_()
    
    def inactive(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.inactive_()
    
    def archived(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.archived_()
    
    def template(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.template_()
    
    def public(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.public_()
    
    def private(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.private_()
    
    def copy(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.copy_()
    
    def has_flows(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.has_flows_()
    
    def activate(self) -> None:
        """Legacy method for backwards compatibility"""
        self.activate_()
    
    def deactivate(self, reason: str = None) -> None:
        """Legacy method for backwards compatibility"""
        self.deactivate_(reason)
    
    def archive(self, archived_by_user=None, reason: str = None) -> None:
        """Legacy method for backwards compatibility"""
        self.archive_(archived_by_user, reason)
    
    def make_template(self, description: str = None) -> None:
        """Legacy method for backwards compatibility"""
        self.make_template_(description)
    
    def make_public(self) -> None:
        """Legacy method for backwards compatibility"""
        self.make_public_()
    
    def make_private(self) -> None:
        """Legacy method for backwards compatibility"""
        self.make_private_()
    
    def update_activity(self) -> None:
        """Legacy method for backwards compatibility"""
        self.update_activity_()
    
    def increment_flow_count(self, active: bool = True) -> None:
        """Legacy method for backwards compatibility"""
        self.increment_flow_count_(active)
    
    def decrement_flow_count(self, was_active: bool = True) -> None:
        """Legacy method for backwards compatibility"""
        self.decrement_flow_count_(was_active)
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<Project(id={self.id}, uid='{self.uid}', name='{self.name}', status='{self.status}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return self.display_name()