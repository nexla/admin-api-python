"""
Project Model - Core project management entity.
Manages project lifecycle, status tracking, and team collaboration with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Set
from enum import Enum as PyEnum
from sqlalchemy.orm import Session
import secrets
import re
from ..database import Base

class ProjectStatuses(PyEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ARCHIVED = "archived"
    TEMPLATE = "template"
    SUSPENDED = "suspended"
    
class ProjectVisibilities(PyEnum):
    PRIVATE = "private"
    ORG_WIDE = "org_wide"
    PUBLIC = "public"

class ProjectPriorities(PyEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class Project(Base):
    __tablename__ = "projects"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Additional Rails fields
    copied_from_id = Column(Integer, ForeignKey("projects.id"))
    client_identifier = Column(String(255))
    client_url = Column(String(255))
    
    # Status and visibility
    status = Column(SQLEnum(ProjectStatuses), nullable=False, default=ProjectStatuses.ACTIVE)
    visibility = Column(SQLEnum(ProjectVisibilities), nullable=False, default=ProjectVisibilities.PRIVATE)
    priority = Column(SQLEnum(ProjectPriorities), nullable=False, default=ProjectPriorities.MEDIUM)
    
    # Metadata
    uid = Column(String(24), unique=True, index=True)
    version = Column(Integer, default=1)
    tags = Column(Text)  # JSON string of tags
    extra_metadata = Column(Text)  # JSON string of additional metadata
    
    # Tracking fields
    last_activity_at = Column(DateTime)
    flow_count = Column(Integer, default=0)
    active_flow_count = Column(Integer, default=0)
    
    # Archival fields
    archived_at = Column(DateTime)
    archived_by_id = Column(Integer, ForeignKey("users.id"))
    archive_reason = Column(Text)
    
    # Template fields
    is_template = Column(Boolean, default=False)
    template_description = Column(Text)
    
    # Access control
    is_public = Column(Boolean, default=False)
    allow_member_access = Column(Boolean, default=True)
    require_approval = Column(Boolean, default=False)
    
    # Relationships
    owner = relationship("User", back_populates="projects", foreign_keys=[owner_id])
    org = relationship("Org", back_populates="projects")
    flows = relationship("Flow", back_populates="project")
    copied_from = relationship("Project", remote_side="Project.id", foreign_keys=[copied_from_id])
    flow_nodes = relationship("FlowNode", back_populates="project")
    archived_by = relationship("User", foreign_keys=[archived_by_id])
    project_copies = relationship("Project", remote_side="Project.copied_from_id")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.ensure_uid_()
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if project is active (Rails pattern)"""
        return self.status == ProjectStatuses.ACTIVE
    
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
    
    def public_(self) -> bool:
        """Check if project is public (Rails pattern)"""
        return self.visibility == ProjectVisibilities.PUBLIC or self.is_public
        
    def private_(self) -> bool:
        """Check if project is private (Rails pattern)"""
        return self.visibility == ProjectVisibilities.PRIVATE
        
    def org_wide_(self) -> bool:
        """Check if project is org-wide visible (Rails pattern)"""
        return self.visibility == ProjectVisibilities.ORG_WIDE
    
    def copy_(self) -> bool:
        """Check if project is a copy (Rails pattern)"""
        return self.copied_from_id is not None
        
    def original_(self) -> bool:
        """Check if project is original (not a copy) (Rails pattern)"""
        return self.copied_from_id is None
        
    def has_flows_(self) -> bool:
        """Check if project has any flows (Rails pattern)"""
        return self.flow_count > 0
        
    def has_active_flows_(self) -> bool:
        """Check if project has active flows (Rails pattern)"""
        return self.active_flow_count > 0
    
    def high_priority_(self) -> bool:
        """Check if project has high priority (Rails pattern)"""
        return self.priority in [ProjectPriorities.HIGH, ProjectPriorities.CRITICAL]
        
    def recently_active_(self, days: int = 7) -> bool:
        """Check if project was recently active (Rails pattern)"""
        if not self.last_activity_at:
            return False
        return self.last_activity_at >= datetime.now() - timedelta(days=days)
    
    def stale_(self, days: int = 30) -> bool:
        """Check if project is stale (Rails pattern)"""
        if not self.last_activity_at:
            return True
        return self.last_activity_at < datetime.now() - timedelta(days=days)
    
    def accessible_by_(self, user, access_level: str = 'read') -> bool:
        """Check if user can access project (Rails pattern)"""
        if not user:
            return False
            
        # Owner always has access
        if self.owner_id == user.id:
            return True
            
        # Public projects are readable by anyone in same org
        if self.public_() and access_level == 'read':
            return user.org_id == self.org_id
            
        # Org-wide projects are accessible to org members
        if self.org_wide_() and user.org_id == self.org_id:
            if not self.allow_member_access and access_level != 'read':
                return False
            return True
            
        # Private projects require explicit permission
        # This would integrate with permission system
        return False
    
    def editable_by_(self, user) -> bool:
        """Check if user can edit project (Rails pattern)"""
        return self.accessible_by_(user, 'write')
    
    def deletable_by_(self, user) -> bool:
        """Check if user can delete project (Rails pattern)"""
        return self.accessible_by_(user, 'admin') or self.owner_id == user.id
    
    # Rails-style bang methods (state changes)
    def activate_(self) -> None:
        """Activate project (Rails bang method pattern)"""
        if self.archived_():
            self.archived_at = None
            self.archived_by_id = None
            self.archive_reason = None
            
        self.status = ProjectStatuses.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: Optional[str] = None) -> None:
        """Deactivate project (Rails bang method pattern)"""
        self.status = ProjectStatuses.INACTIVE
        self.updated_at = datetime.now()
        if reason:
            self._update_metadata('deactivation_reason', reason)
    
    def archive_(self, archived_by_user=None, reason: Optional[str] = None) -> None:
        """Archive project (Rails bang method pattern)"""
        self.status = ProjectStatuses.ARCHIVED
        self.archived_at = datetime.now()
        self.archived_by_id = archived_by_user.id if archived_by_user else None
        self.archive_reason = reason
        self.updated_at = datetime.now()
    
    def suspend_(self, reason: Optional[str] = None) -> None:
        """Suspend project (Rails bang method pattern)"""
        self.status = ProjectStatuses.SUSPENDED
        self.updated_at = datetime.now()
        if reason:
            self._update_metadata('suspension_reason', reason)
    
    def make_template_(self, description: Optional[str] = None) -> None:
        """Convert project to template (Rails bang method pattern)"""
        self.status = ProjectStatuses.TEMPLATE
        self.is_template = True
        self.template_description = description
        self.updated_at = datetime.now()
    
    def make_public_(self) -> None:
        """Make project public (Rails bang method pattern)"""
        self.visibility = ProjectVisibilities.PUBLIC
        self.is_public = True
        self.updated_at = datetime.now()
    
    def make_private_(self) -> None:
        """Make project private (Rails bang method pattern)"""
        self.visibility = ProjectVisibilities.PRIVATE
        self.is_public = False
        self.updated_at = datetime.now()
    
    def make_org_wide_(self) -> None:
        """Make project org-wide visible (Rails bang method pattern)"""
        self.visibility = ProjectVisibilities.ORG_WIDE
        self.updated_at = datetime.now()
    
    def update_activity_(self) -> None:
        """Update last activity timestamp (Rails bang method pattern)"""
        self.last_activity_at = datetime.now()
        self.updated_at = datetime.now()
    
    def increment_flow_count_(self, active: bool = True) -> None:
        """Increment flow count (Rails bang method pattern)"""
        self.flow_count = (self.flow_count or 0) + 1
        if active:
            self.active_flow_count = (self.active_flow_count or 0) + 1
        self.update_activity_()
    
    def decrement_flow_count_(self, was_active: bool = True) -> None:
        """Decrement flow count (Rails bang method pattern)"""
        if self.flow_count and self.flow_count > 0:
            self.flow_count -= 1
        if was_active and self.active_flow_count and self.active_flow_count > 0:
            self.active_flow_count -= 1
        self.update_activity_()
    
    def refresh_counts_(self) -> None:
        """Refresh flow counts from database (Rails bang method pattern)"""
        # This would query the Flow model to get accurate counts
        # For now, we'll implement a placeholder
        if hasattr(self, 'flows') and self.flows:
            self.flow_count = len(self.flows)
            self.active_flow_count = sum(1 for flow in self.flows if hasattr(flow, 'active_') and flow.active_())
        else:
            self.flow_count = 0
            self.active_flow_count = 0
        self.updated_at = datetime.now()
    
    # Rails business logic methods
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
        """Set project tags (Rails bang method pattern)"""
        import json
        if isinstance(tags, str):
            # Handle comma-separated string
            tag_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
        else:
            tag_list = list(set(tags)) if tags else []
        
        self.tags = json.dumps(tag_list) if tag_list else None
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
    
    def copy_from_(self, source_project, new_name: Optional[str] = None, copy_flows: bool = True) -> None:
        """Create project as copy of another (Rails bang method pattern)"""
        if not source_project:
            raise ValueError("Source project is required")
            
        self.copied_from_id = source_project.id
        self.name = new_name or f"Copy of {source_project.name}"
        self.description = source_project.description
        self.priority = source_project.priority
        
        # Copy tags
        self.set_tags_(source_project.tags_list())
        
        # Copy metadata (excluding sensitive fields)
        if source_project.metadata:
            import json
            try:
                source_meta = json.loads(source_project.metadata)
                # Filter out sensitive metadata
                filtered_meta = {k: v for k, v in source_meta.items() 
                               if not k.startswith('_') and k not in ['api_keys', 'secrets']}
                self.extra_metadata = json.dumps(filtered_meta) if filtered_meta else None
            except (json.JSONDecodeError, TypeError):
                pass
                
        self.updated_at = datetime.now()
        
        # Note: Flow copying would be implemented separately
        if copy_flows:
            # This would copy flows from source project
            pass
    
    def validate_(self) -> List[str]:
        """Validate project data (Rails validation pattern)"""
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
        
        if self.client_url and len(self.client_url) > 255:
            errors.append("Client URL is too long (maximum 255 characters)")
        
        if self.description and len(self.description) > 10000:
            errors.append("Description is too long (maximum 10,000 characters)")
        
        if self.template_description and len(self.template_description) > 1000:
            errors.append("Template description is too long (maximum 1,000 characters)")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if project is valid (Rails validation pattern)"""
        return len(self.validate_()) == 0
    
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
        
        raise ValueError("Failed to generate unique project UID")
    
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
    
    @classmethod
    def accessible_origin_nodes(cls, accessor, access_role: str, org, scope=None):
        """Get accessible origin nodes for projects (Rails pattern)"""
        if not hasattr(accessor, 'projects'):
            return []
        
        # This would implement access control logic
        if access_role in ['owner', 'admin', 'member'] or access_role == 'all':
            # Get user's accessible projects
            if hasattr(accessor, 'is_a') and accessor.__class__.__name__ == 'User':
                projects = accessor.projects(org, access_role=access_role)
            else:
                projects = accessor.projects(access_role, org)
            
            # Get flow nodes from those projects
            flow_node_ids = []
            for project in projects:
                if hasattr(project, 'flow_nodes'):
                    flow_node_ids.extend([fn.id for fn in project.flow_nodes])
            
            # This would query FlowNode model when implemented
            return flow_node_ids
        
        return []
    
    # ====================
    # Rails Scopes
    # ====================
    
    @classmethod
    def active(cls, db: Session):
        """Rails scope: Get active projects"""
        return db.query(cls).filter(cls.status == ProjectStatuses.ACTIVE)
    
    @classmethod
    def inactive(cls, db: Session):
        """Rails scope: Get inactive projects"""
        return db.query(cls).filter(cls.status == ProjectStatuses.INACTIVE)
    
    @classmethod
    def archived(cls, db: Session):
        """Rails scope: Get archived projects"""
        return db.query(cls).filter(cls.status == ProjectStatuses.ARCHIVED)
    
    @classmethod
    def templates_scope(cls, db: Session):
        """Rails scope: Get template projects"""
        return db.query(cls).filter(
            (cls.status == ProjectStatuses.TEMPLATE) | (cls.is_template == True)
        )
    
    @classmethod
    def suspended(cls, db: Session):
        """Rails scope: Get suspended projects"""
        return db.query(cls).filter(cls.status == ProjectStatuses.SUSPENDED)
    
    @classmethod
    def public_scope(cls, db: Session):
        """Rails scope: Get public projects"""
        return db.query(cls).filter(
            (cls.visibility == ProjectVisibilities.PUBLIC) | (cls.is_public == True)
        )
    
    @classmethod
    def private_scope(cls, db: Session):
        """Rails scope: Get private projects"""
        return db.query(cls).filter(cls.visibility == ProjectVisibilities.PRIVATE)
    
    @classmethod
    def org_wide_scope(cls, db: Session):
        """Rails scope: Get org-wide projects"""
        return db.query(cls).filter(cls.visibility == ProjectVisibilities.ORG_WIDE)
    
    @classmethod
    def by_owner(cls, db: Session, user_id: int):
        """Rails scope: Get projects by owner"""
        return db.query(cls).filter(cls.owner_id == user_id)
    
    @classmethod
    def by_org(cls, db: Session, org_id: int):
        """Rails scope: Get projects by organization"""
        return db.query(cls).filter(cls.org_id == org_id)
    
    @classmethod
    def by_priority_scope(cls, db: Session, priority: ProjectPriorities):
        """Rails scope: Get projects by priority"""
        return db.query(cls).filter(cls.priority == priority)
    
    @classmethod
    def high_priority(cls, db: Session):
        """Rails scope: Get high priority projects"""
        return db.query(cls).filter(
            cls.priority.in_([ProjectPriorities.HIGH, ProjectPriorities.CRITICAL])
        )
    
    @classmethod
    def copies(cls, db: Session):
        """Rails scope: Get copied projects"""
        return db.query(cls).filter(cls.copied_from_id.isnot(None))
    
    @classmethod
    def originals(cls, db: Session):
        """Rails scope: Get original (non-copied) projects"""
        return db.query(cls).filter(cls.copied_from_id.is_(None))
    
    @classmethod
    def with_flows(cls, db: Session):
        """Rails scope: Get projects with flows"""
        return db.query(cls).filter(cls.flow_count > 0)
    
    @classmethod
    def without_flows(cls, db: Session):
        """Rails scope: Get projects without flows"""
        return db.query(cls).filter((cls.flow_count == 0) | (cls.flow_count.is_(None)))
    
    @classmethod
    def recently_active_scope(cls, db: Session, days: int = 7):
        """Rails scope: Get recently active projects"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(cls.last_activity_at >= cutoff).order_by(cls.last_activity_at.desc())
    
    @classmethod
    def stale_scope(cls, db: Session, days: int = 30):
        """Rails scope: Get stale projects"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(
            (cls.last_activity_at < cutoff) | (cls.last_activity_at.is_(None))
        ).filter(cls.status == ProjectStatuses.ACTIVE)
    
    @classmethod
    def recent_scope(cls, db: Session, days: int = 30):
        """Rails scope: Get recently created projects"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(cls.created_at >= cutoff)
    
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
    def active_projects(cls, org=None):
        """Get active projects (Rails scope pattern)"""
        query = cls.query.filter_by(status=ProjectStatuses.ACTIVE)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def recent_projects(cls, days: int = 7, org=None):
        """Get recently active projects (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        query = cls.query.filter(cls.last_activity_at >= cutoff)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query.order_by(cls.last_activity_at.desc())
    
    @classmethod
    def by_priority(cls, priority: ProjectPriorities, org=None):
        """Get projects by priority (Rails scope pattern)"""
        query = cls.query.filter_by(priority=priority)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def templates(cls, org=None):
        """Get template projects (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.status == ProjectStatuses.TEMPLATE) | (cls.is_template == True)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def accessible_to(cls, user, access_level: str = 'read'):
        """Get projects accessible to user (Rails scope pattern)"""
        if not user:
            return cls.query.filter(False)  # Empty query
        
        # Start with user's own projects
        query = cls.query.filter_by(owner_id=user.id)
        
        # Add public projects in same org
        if access_level == 'read':
            public_query = cls.query.filter(
                (cls.visibility == ProjectVisibilities.PUBLIC) |
                (cls.is_public == True)
            ).filter_by(org_id=user.org_id if hasattr(user, 'org_id') else None)
            
            # Add org-wide projects
            org_wide_query = cls.query.filter_by(
                visibility=ProjectVisibilities.ORG_WIDE,
                org_id=user.org_id if hasattr(user, 'org_id') else None
            )
            
            # Union all queries
            query = query.union(public_query).union(org_wide_query)
        
        return query.distinct()
    
    @classmethod
    def stale_projects(cls, days: int = 30, org=None):
        """Get stale projects (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        query = cls.query.filter(
            (cls.last_activity_at < cutoff) | (cls.last_activity_at.is_(None))
        ).filter_by(status=ProjectStatuses.ACTIVE)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def build_from_input(cls, api_user_info: Dict[str, Any], input_data: Dict[str, Any]):
        """Build project from input data (Rails pattern)"""
        if not input_data or not api_user_info.get('input_owner'):
            raise ValueError("Project input missing")
        
        project = cls()
        project.owner = api_user_info['input_owner']
        project.org = api_user_info['input_org']
        project.update_mutable(api_user_info, input_data)
        
        return project
    
    @classmethod
    def build_from_template(cls, template_project, api_user_info: Dict[str, Any], 
                           new_name: Optional[str] = None) -> 'Project':
        """Build project from template (Rails factory pattern)"""
        if not template_project or not template_project.template_():
            raise ValueError("Invalid template project")
            
        project = cls()
        project.owner = api_user_info['input_owner']
        project.org = api_user_info['input_org']
        
        # Copy template attributes
        project.name = new_name or f"{template_project.name} - {datetime.now().strftime('%Y%m%d')}"
        project.description = template_project.description
        project.priority = template_project.priority
        project.visibility = ProjectVisibilities.PRIVATE  # Always start as private
        
        # Copy template metadata and tags
        project.set_tags_(template_project.tags_list())
        if template_project.metadata:
            project.metadata = template_project.metadata
            
        project.copied_from_id = template_project.id
        project._update_metadata('created_from_template', True)
        project._update_metadata('template_id', template_project.id)
        
        return project
    
    def update_mutable(self, api_user_info: Dict[str, Any], input_data: Dict[str, Any]) -> None:
        """Update mutable fields (Rails update_mutable! pattern)"""
        if not input_data or not api_user_info:
            return
        
        # Update owner and org if different
        if self.owner != api_user_info.get('input_owner'):
            self.owner = api_user_info['input_owner']
        
        if self.org != api_user_info.get('input_org'):
            self.org = api_user_info['input_org']
        
        # Update basic fields
        if 'name' in input_data:
            self.name = input_data['name']
        
        if not self.name or self.name.strip() == "":
            raise ValueError("Project name missing")
        
        if 'description' in input_data:
            self.description = input_data['description']
        
        if 'client_identifier' in input_data:
            self.client_identifier = input_data['client_identifier']
        
        if 'client_url' in input_data:
            self.client_url = input_data['client_url']
        
        # Handle flow associations
        if 'flows' in input_data:
            self.update_flows(input_data['flows'], api_user_info)
        elif 'data_flows' in input_data:
            # Backwards compatibility
            self.update_data_flows(input_data['data_flows'], api_user_info)
        
        # Handle tags
        tags = input_data.get('tags')
        if tags:
            # This would integrate with tagging system when implemented
            print(f"DEBUG: Adding tags {tags} to project {self.id}")
    
    def update_data_flows(self, data_flows: List[Dict[str, Any]], api_user_info: Dict[str, Any]) -> None:
        """Update data flows (Rails backwards compatibility pattern)"""
        flow_node_ids = []
        
        for df in data_flows:
            if not isinstance(df, dict):
                continue
            
            # This would identify resource keys and find corresponding resources
            # For now, we'll implement a simplified version
            resource_keys = ['data_source_id', 'data_set_id', 'data_sink_id']
            res_key = None
            res_id = None
            
            for key in resource_keys:
                if key in df and df[key]:
                    res_key = key
                    res_id = df[key]
                    break
            
            if not res_key or not res_id:
                continue
            
            # This would find the resource and get its flow_node_id
            # For now, we'll simulate this
            flow_node_id = self._find_resource_flow_node_id(res_key, res_id)
            if flow_node_id:
                flow_node_ids.append(flow_node_id)
        
        # Update flow associations
        self._update_flow_associations(flow_node_ids)
    
    def update_flows(self, flows: List[Dict[str, Any]], api_user_info: Dict[str, Any]) -> None:
        """Update flows (Rails pattern)"""
        # This would handle flow associations
        flow_ids = []
        
        for flow_data in flows:
            if isinstance(flow_data, dict) and 'id' in flow_data:
                flow_ids.append(flow_data['id'])
        
        self._update_flow_associations(flow_ids)
    
    def set_defaults(self, user, org) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        self.org = org
    
    def _find_resource_flow_node_id(self, resource_key: str, resource_id: int) -> Optional[int]:
        """Find flow node ID for a resource (helper method)"""
        # This would query the appropriate model to find the flow_node_id
        # For now, return None as placeholder
        print(f"DEBUG: Finding flow node ID for {resource_key}={resource_id}")
        return None
    
    def _update_flow_associations(self, flow_node_ids: List[int]) -> None:
        """Update flow associations (helper method)"""
        # This would update the flow node associations for this project
        print(f"DEBUG: Updating flow associations for project {self.id}: {flow_node_ids}")
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<Project(id={self.id}, uid='{self.uid}', name='{self.name}', status='{self.status}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return self.display_name()
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        if self.copy_():
            return f"{self.name} (Copy)"
        elif self.template_():
            return f"{self.name} (Template)"
        return self.name or "Unnamed Project"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        status_map = {
            ProjectStatuses.ACTIVE: "Active",
            ProjectStatuses.INACTIVE: "Inactive",
            ProjectStatuses.ARCHIVED: "Archived",
            ProjectStatuses.TEMPLATE: "Template",
            ProjectStatuses.SUSPENDED: "Suspended"
        }
        return status_map.get(self.status, "Unknown")
    
    def priority_display(self) -> str:
        """Get human-readable priority (Rails pattern)"""
        priority_map = {
            ProjectPriorities.LOW: "Low",
            ProjectPriorities.MEDIUM: "Medium",
            ProjectPriorities.HIGH: "High",
            ProjectPriorities.CRITICAL: "Critical"
        }
        return priority_map.get(self.priority, "Medium")
    
    def visibility_display(self) -> str:
        """Get human-readable visibility (Rails pattern)"""
        visibility_map = {
            ProjectVisibilities.PRIVATE: "Private",
            ProjectVisibilities.ORG_WIDE: "Organization",
            ProjectVisibilities.PUBLIC: "Public"
        }
        return visibility_map.get(self.visibility, "Private")
    
    def activity_summary(self) -> Dict[str, Any]:
        """Get activity summary (Rails pattern)"""
        return {
            'flow_count': self.flow_count or 0,
            'active_flow_count': self.active_flow_count or 0,
            'last_activity': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'days_since_activity': (datetime.now() - self.last_activity_at).days if self.last_activity_at else None,
            'is_stale': self.stale_()
        }
    
    def to_dict(self, include_metadata: bool = False, include_relationships: bool = False) -> Dict[str, Any]:
        """Convert project to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'description': self.description,
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'visibility': self.visibility.value if self.visibility else None,
            'visibility_display': self.visibility_display(),
            'priority': self.priority.value if self.priority else None,
            'priority_display': self.priority_display(),
            'is_template': self.is_template,
            'is_copy': self.copy_(),
            'is_public': self.is_public,
            'client_identifier': self.client_identifier,
            'client_url': self.client_url,
            'version': self.version,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'copied_from_id': self.copied_from_id,
            'tags': self.tags_list(),
            'flow_count': self.flow_count or 0,
            'active_flow_count': self.active_flow_count or 0,
            'activity_summary': self.activity_summary()
        }
        
        if self.archived_():
            result.update({
                'archived_at': self.archived_at.isoformat() if self.archived_at else None,
                'archived_by_id': self.archived_by_id,
                'archive_reason': self.archive_reason
            })
        
        if self.template_():
            result['template_description'] = self.template_description
        
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
                'copied_from': self.copied_from.to_dict() if self.copied_from else None
            })
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert project to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'priority': self.priority.value if self.priority else None,
            'priority_display': self.priority_display(),
            'flow_count': self.flow_count or 0,
            'is_template': self.is_template,
            'is_public': self.is_public,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None
        }