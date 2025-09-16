"""
Permission Model - Fine-grained access control and authorization.
Implements Rails authorization patterns with role-based and resource-based permissions.
Supports hierarchical permissions, conditional access, and audit trails.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Index
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple, Set
from enum import Enum as PyEnum
import json
import logging
from ..database import Base

logger = logging.getLogger(__name__)

class PermissionType(PyEnum):
    """Permission type enumeration"""
    SYSTEM = "system"           # System-level permissions
    RESOURCE = "resource"       # Resource-specific permissions
    FEATURE = "feature"         # Feature access permissions
    API = "api"                # API endpoint permissions
    DATA = "data"              # Data access permissions
    ADMIN = "admin"            # Administrative permissions
    
    @property
    def display_name(self) -> str:
        return {
            self.SYSTEM: "System Permission",
            self.RESOURCE: "Resource Permission",
            self.FEATURE: "Feature Permission",
            self.API: "API Permission",
            self.DATA: "Data Permission",
            self.ADMIN: "Admin Permission"
        }.get(self, "Unknown Permission Type")

class PermissionAction(PyEnum):
    """Permission action enumeration"""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"
    MANAGE = "manage"
    ADMIN = "admin"
    SHARE = "share"
    EXPORT = "export"
    IMPORT = "import"
    APPROVE = "approve"
    REJECT = "reject"
    
    @property
    def display_name(self) -> str:
        return {
            self.CREATE: "Create",
            self.READ: "Read/View",
            self.UPDATE: "Update/Edit", 
            self.DELETE: "Delete",
            self.EXECUTE: "Execute",
            self.MANAGE: "Manage",
            self.ADMIN: "Administer",
            self.SHARE: "Share",
            self.EXPORT: "Export",
            self.IMPORT: "Import",
            self.APPROVE: "Approve",
            self.REJECT: "Reject"
        }.get(self, "Unknown Action")

class PermissionScope(PyEnum):
    """Permission scope enumeration"""
    GLOBAL = "global"           # Global/system-wide
    ORG = "org"                # Organization-scoped
    PROJECT = "project"         # Project-scoped
    TEAM = "team"              # Team-scoped
    RESOURCE = "resource"       # Specific resource
    
    @property
    def display_name(self) -> str:
        return {
            self.GLOBAL: "Global",
            self.ORG: "Organization",
            self.PROJECT: "Project",
            self.TEAM: "Team",
            self.RESOURCE: "Specific Resource"
        }.get(self, "Unknown Scope")

class PermissionStatus(PyEnum):
    """Permission status enumeration"""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    REVOKED = "revoked"
    EXPIRED = "expired"
    PENDING = "pending"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.SUSPENDED: "Suspended",
            self.REVOKED: "Revoked",
            self.EXPIRED: "Expired",
            self.PENDING: "Pending"
        }.get(self, "Unknown Status")

class Permission(Base):
    __tablename__ = "permissions"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)  # e.g., 'projects.create'
    codename = Column(String(100), nullable=False, index=True)  # Unique permission identifier
    
    # Permission classification
    permission_type = Column(SQLEnum(PermissionType), nullable=False, index=True)
    action = Column(SQLEnum(PermissionAction), nullable=False, index=True)
    scope = Column(SQLEnum(PermissionScope), nullable=False, index=True)
    status = Column(SQLEnum(PermissionStatus), default=PermissionStatus.ACTIVE, index=True)
    
    # Resource targeting (polymorphic)
    resource_type = Column(String(100), index=True)  # 'Project', 'DataSource', etc.
    resource_id = Column(Integer, index=True)        # Specific resource ID (optional)
    
    # Description and metadata
    description = Column(Text)
    extra_metadata = Column(JSON)                         # Additional permission data
    conditions = Column(JSON)                       # Conditional access rules
    
    # Hierarchy and inheritance
    parent_permission_id = Column(Integer, ForeignKey("permissions.id"), index=True)
    inherits_from = Column(JSON)                    # List of permission IDs to inherit from
    is_inherited = Column(Boolean, default=False, index=True)
    
    # Context and scope references
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    
    # Permission properties
    is_system = Column(Boolean, default=False, index=True)      # System-defined permission
    is_custom = Column(Boolean, default=False, index=True)      # Custom user permission
    is_temporary = Column(Boolean, default=False, index=True)   # Temporary permission
    requires_approval = Column(Boolean, default=False)          # Requires approval to grant
    
    # Time-based permissions
    granted_at = Column(DateTime, index=True)
    expires_at = Column(DateTime, index=True)
    last_used_at = Column(DateTime, index=True)
    
    # Audit trail
    created_by_id = Column(Integer, ForeignKey("users.id"))
    granted_by_id = Column(Integer, ForeignKey("users.id"))
    revoked_by_id = Column(Integer, ForeignKey("users.id"))
    revoked_at = Column(DateTime)
    revocation_reason = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    org = relationship("Org", foreign_keys=[org_id])
    project = relationship("Project", foreign_keys=[project_id])
    team = relationship("Team", foreign_keys=[team_id])
    created_by = relationship("User", foreign_keys=[created_by_id])
    granted_by = relationship("User", foreign_keys=[granted_by_id])
    revoked_by = relationship("User", foreign_keys=[revoked_by_id])
    
    parent_permission = relationship("Permission", remote_side=[id], foreign_keys=[parent_permission_id])
    child_permissions = relationship("Permission", remote_side=[parent_permission_id])
    
    # Enhanced database indexes
    __table_args__ = (
        Index('idx_permissions_codename_scope', 'codename', 'scope'),
        Index('idx_permissions_type_action', 'permission_type', 'action'),
        Index('idx_permissions_resource', 'resource_type', 'resource_id'),
        Index('idx_permissions_status_active', 'status', 'is_system'),
        Index('idx_permissions_org_scope', 'org_id', 'scope'),
        Index('idx_permissions_project_scope', 'project_id', 'scope'),
        Index('idx_permissions_team_scope', 'team_id', 'scope'),
        Index('idx_permissions_expiry', 'expires_at', 'status'),
        Index('idx_permissions_hierarchy', 'parent_permission_id', 'is_inherited'),
        Index('idx_permissions_approval', 'requires_approval', 'status'),
        # Unique constraint for permission identity
        Index('idx_permissions_unique', 'codename', 'org_id', 'project_id', 'team_id', 'resource_type', 'resource_id', unique=True),
    )
    
    # Rails constants
    DEFAULT_EXPIRY_DAYS = 90
    SYSTEM_PERMISSIONS = {
        'system.admin': 'Full system administration',
        'users.manage': 'Manage all users',
        'orgs.manage': 'Manage organizations',
        'billing.manage': 'Manage billing and payments',
        'audit.view': 'View audit logs',
        'settings.manage': 'Manage system settings'
    }
    
    RESOURCE_PERMISSIONS = {
        'projects.create': 'Create new projects',
        'projects.read': 'View projects',
        'projects.update': 'Edit projects',
        'projects.delete': 'Delete projects',
        'projects.share': 'Share projects',
        'flows.execute': 'Execute data flows',
        'data.export': 'Export data',
        'data.import': 'Import data'
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Auto-set granted_at if not provided
        if not self.granted_at and self.status == PermissionStatus.ACTIVE:
            self.granted_at = datetime.now()
        
        # Auto-calculate expiry for temporary permissions
        if self.is_temporary and not self.expires_at:
            self.expires_at = datetime.now() + timedelta(days=self.DEFAULT_EXPIRY_DAYS)
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if permission is active (Rails pattern)"""
        return (self.status == PermissionStatus.ACTIVE and 
                not self.expired_() and 
                not self.revoked_())
    
    def suspended_(self) -> bool:
        """Check if permission is suspended (Rails pattern)"""
        return self.status == PermissionStatus.SUSPENDED
    
    def revoked_(self) -> bool:
        """Check if permission is revoked (Rails pattern)"""
        return self.status == PermissionStatus.REVOKED
    
    def expired_(self) -> bool:
        """Check if permission is expired (Rails pattern)"""
        if self.status == PermissionStatus.EXPIRED:
            return True
        if self.expires_at and self.expires_at < datetime.now():
            return True
        return False
    
    def pending_(self) -> bool:
        """Check if permission is pending approval (Rails pattern)"""
        return self.status == PermissionStatus.PENDING
    
    def system_(self) -> bool:
        """Check if permission is system-defined (Rails pattern)"""
        return self.is_system
    
    def custom_(self) -> bool:
        """Check if permission is custom (Rails pattern)"""
        return self.is_custom
    
    def temporary_(self) -> bool:
        """Check if permission is temporary (Rails pattern)"""
        return self.is_temporary
    
    def inherited_(self) -> bool:
        """Check if permission is inherited (Rails pattern)"""
        return self.is_inherited
    
    def requires_approval_(self) -> bool:
        """Check if permission requires approval (Rails pattern)"""
        return self.requires_approval
    
    def resource_specific_(self) -> bool:
        """Check if permission is resource-specific (Rails pattern)"""
        return bool(self.resource_type and self.resource_id)
    
    def global_scope_(self) -> bool:
        """Check if permission has global scope (Rails pattern)"""
        return self.scope == PermissionScope.GLOBAL
    
    def org_scope_(self) -> bool:
        """Check if permission is org-scoped (Rails pattern)"""
        return self.scope == PermissionScope.ORG
    
    def project_scope_(self) -> bool:
        """Check if permission is project-scoped (Rails pattern)"""
        return self.scope == PermissionScope.PROJECT
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if permission was recently used (Rails pattern)"""
        if not self.last_used_at:
            return False
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.last_used_at >= cutoff
    
    def expiring_soon_(self, days: int = 7) -> bool:
        """Check if permission is expiring soon (Rails pattern)"""
        if not self.expires_at:
            return False
        cutoff = datetime.now() + timedelta(days=days)
        return self.expires_at <= cutoff
    
    def has_conditions_(self) -> bool:
        """Check if permission has conditional access rules (Rails pattern)"""
        return bool(self.conditions)
    
    def has_children_(self) -> bool:
        """Check if permission has child permissions (Rails pattern)"""
        return len(self.child_permissions) > 0 if self.child_permissions else False
    
    def has_parent_(self) -> bool:
        """Check if permission has parent permission (Rails pattern)"""
        return self.parent_permission_id is not None
    
    def applicable_to_resource_(self, resource_type: str, resource_id: int = None) -> bool:
        """Check if permission applies to specific resource (Rails pattern)"""
        if not self.resource_type:
            return True  # Global permission applies to all resources
        
        if self.resource_type != resource_type:
            return False
        
        if self.resource_id and resource_id and self.resource_id != resource_id:
            return False
        
        return True
    
    def valid_for_context_(self, org_id: int = None, project_id: int = None, 
                          team_id: int = None) -> bool:
        """Check if permission is valid for given context (Rails pattern)"""
        if self.scope == PermissionScope.GLOBAL:
            return True
        elif self.scope == PermissionScope.ORG:
            return self.org_id == org_id if org_id else True
        elif self.scope == PermissionScope.PROJECT:
            return self.project_id == project_id if project_id else True
        elif self.scope == PermissionScope.TEAM:
            return self.team_id == team_id if team_id else True
        
        return True
    
    # Rails bang methods
    def activate_(self, granted_by_id: int = None) -> None:
        """Activate permission (Rails bang method pattern)"""
        if self.active_():
            return
        
        self.status = PermissionStatus.ACTIVE
        self.granted_at = datetime.now()
        self.granted_by_id = granted_by_id
        self.updated_at = datetime.now()
        
        # Clear revocation info
        self.revoked_at = None
        self.revoked_by_id = None
        self.revocation_reason = None
    
    def suspend_(self, reason: str = None, suspended_by_id: int = None) -> None:
        """Suspend permission (Rails bang method pattern)"""
        if self.suspended_():
            return
        
        self.status = PermissionStatus.SUSPENDED
        self.updated_at = datetime.now()
        
        if reason or suspended_by_id:
            if not self.extra_metadata:
                self.extra_metadata = {}
            self.extra_metadata.update({
                'suspension_reason': reason,
                'suspended_by_id': suspended_by_id,
                'suspended_at': datetime.now().isoformat()
            })
    
    def unsuspend_(self, unsuspended_by_id: int = None) -> None:
        """Remove suspension (Rails bang method pattern)"""
        if not self.suspended_():
            return
        
        self.status = PermissionStatus.ACTIVE
        self.updated_at = datetime.now()
        
        if self.extra_metadata:
            # Clear suspension metadata
            suspension_keys = ['suspension_reason', 'suspended_by_id', 'suspended_at']
            for key in suspension_keys:
                self.extra_metadata.pop(key, None)
            
            self.extra_metadata['unsuspended_at'] = datetime.now().isoformat()
            self.extra_metadata['unsuspended_by_id'] = unsuspended_by_id
    
    def revoke_(self, reason: str = None, revoked_by_id: int = None) -> None:
        """Revoke permission (Rails bang method pattern)"""
        if self.revoked_():
            return
        
        self.status = PermissionStatus.REVOKED
        self.revoked_at = datetime.now()
        self.revoked_by_id = revoked_by_id
        self.revocation_reason = reason
        self.updated_at = datetime.now()
    
    def expire_(self) -> None:
        """Mark permission as expired (Rails bang method pattern)"""
        if self.expired_():
            return
        
        self.status = PermissionStatus.EXPIRED
        if not self.expires_at:
            self.expires_at = datetime.now()
        self.updated_at = datetime.now()
    
    def extend_expiry_(self, days: int, extended_by_id: int = None) -> None:
        """Extend permission expiry (Rails bang method pattern)"""
        if self.expires_at:
            self.expires_at += timedelta(days=days)
        else:
            self.expires_at = datetime.now() + timedelta(days=days)
        
        self.updated_at = datetime.now()
        
        # Log extension
        if not self.extra_metadata:
            self.extra_metadata = {}
        extensions = self.extra_metadata.get('expiry_extensions', [])
        extensions.append({
            'extended_days': days,
            'extended_by_id': extended_by_id,
            'extended_at': datetime.now().isoformat(),
            'new_expiry': self.expires_at.isoformat()
        })
        self.extra_metadata['expiry_extensions'] = extensions[-10:]  # Keep last 10
    
    def record_usage_(self, used_by_id: int = None, context: Dict[str, Any] = None) -> None:
        """Record permission usage (Rails bang method pattern)"""
        self.last_used_at = datetime.now()
        self.updated_at = datetime.now()
        
        # Track usage history
        if not self.extra_metadata:
            self.extra_metadata = {}
        
        usage_history = self.extra_metadata.get('usage_history', [])
        usage_entry = {
            'used_at': datetime.now().isoformat(),
            'used_by_id': used_by_id
        }
        
        if context:
            usage_entry['context'] = context
        
        usage_history.append(usage_entry)
        self.extra_metadata['usage_history'] = usage_history[-50:]  # Keep last 50 uses
    
    def add_condition_(self, condition_type: str, condition_data: Dict[str, Any]) -> None:
        """Add conditional access rule (Rails bang method pattern)"""
        if not self.conditions:
            self.conditions = {}
        
        if condition_type not in self.conditions:
            self.conditions[condition_type] = []
        
        self.conditions[condition_type].append(condition_data)
        self.updated_at = datetime.now()
    
    def remove_condition_(self, condition_type: str, condition_index: int = None) -> None:
        """Remove conditional access rule (Rails bang method pattern)"""
        if not self.conditions or condition_type not in self.conditions:
            return
        
        if condition_index is not None:
            # Remove specific condition
            if 0 <= condition_index < len(self.conditions[condition_type]):
                del self.conditions[condition_type][condition_index]
        else:
            # Remove all conditions of this type
            del self.conditions[condition_type]
        
        self.updated_at = datetime.now()
    
    def inherit_from_(self, parent_permission: 'Permission') -> None:
        """Set up inheritance from parent permission (Rails bang method pattern)"""
        self.parent_permission_id = parent_permission.id
        self.is_inherited = True
        
        if not self.inherits_from:
            self.inherits_from = []
        
        if parent_permission.id not in self.inherits_from:
            self.inherits_from.append(parent_permission.id)
        
        self.updated_at = datetime.now()
    
    # Rails helper methods
    def evaluate_conditions(self, context: Dict[str, Any]) -> bool:
        """Evaluate conditional access rules (Rails pattern)"""
        if not self.has_conditions_():
            return True  # No conditions = always allowed
        
        for condition_type, conditions in self.conditions.items():
            if not self._evaluate_condition_type(condition_type, conditions, context):
                return False
        
        return True
    
    def _evaluate_condition_type(self, condition_type: str, conditions: List[Dict], 
                                context: Dict[str, Any]) -> bool:
        """Evaluate specific condition type (Rails private pattern)"""
        if condition_type == 'time_based':
            return self._evaluate_time_conditions(conditions, context)
        elif condition_type == 'ip_based':
            return self._evaluate_ip_conditions(conditions, context)
        elif condition_type == 'attribute_based':
            return self._evaluate_attribute_conditions(conditions, context)
        elif condition_type == 'resource_based':
            return self._evaluate_resource_conditions(conditions, context)
        
        return True  # Unknown condition type = allow
    
    def _evaluate_time_conditions(self, conditions: List[Dict], context: Dict[str, Any]) -> bool:
        """Evaluate time-based conditions (Rails private pattern)"""
        current_time = datetime.now()
        
        for condition in conditions:
            if 'allowed_hours' in condition:
                allowed_hours = condition['allowed_hours']
                current_hour = current_time.hour
                if current_hour not in allowed_hours:
                    return False
            
            if 'allowed_days' in condition:
                allowed_days = condition['allowed_days']  # 0=Monday, 6=Sunday
                current_day = current_time.weekday()
                if current_day not in allowed_days:
                    return False
        
        return True
    
    def _evaluate_ip_conditions(self, conditions: List[Dict], context: Dict[str, Any]) -> bool:
        """Evaluate IP-based conditions (Rails private pattern)"""
        user_ip = context.get('ip_address')
        if not user_ip:
            return False
        
        for condition in conditions:
            if 'allowed_ips' in condition:
                if user_ip not in condition['allowed_ips']:
                    return False
            
            if 'allowed_networks' in condition:
                # Would implement CIDR matching here
                pass
        
        return True
    
    def _evaluate_attribute_conditions(self, conditions: List[Dict], context: Dict[str, Any]) -> bool:
        """Evaluate attribute-based conditions (Rails private pattern)"""
        for condition in conditions:
            attribute = condition.get('attribute')
            required_value = condition.get('value')
            operator = condition.get('operator', 'equals')
            
            context_value = context.get(attribute)
            
            if operator == 'equals' and context_value != required_value:
                return False
            elif operator == 'in' and context_value not in required_value:
                return False
            elif operator == 'greater_than' and (not context_value or context_value <= required_value):
                return False
        
        return True
    
    def _evaluate_resource_conditions(self, conditions: List[Dict], context: Dict[str, Any]) -> bool:
        """Evaluate resource-based conditions (Rails private pattern)"""
        for condition in conditions:
            required_resource_type = condition.get('resource_type')
            context_resource_type = context.get('resource_type')
            
            if required_resource_type and context_resource_type != required_resource_type:
                return False
        
        return True
    
    def get_effective_permissions(self) -> List['Permission']:
        """Get all effective permissions including inherited (Rails pattern)"""
        permissions = [self]
        
        # Add inherited permissions
        if self.inherits_from:
            for parent_id in self.inherits_from:
                parent = self.__class__.query.get(parent_id)
                if parent and parent.active_():
                    permissions.extend(parent.get_effective_permissions())
        
        return list(set(permissions))  # Remove duplicates
    
    # Rails class methods and scopes
    @classmethod
    def by_codename(cls, codename: str):
        """Scope for specific codename (Rails scope pattern)"""
        return cls.query.filter_by(codename=codename)
    
    @classmethod
    def by_type(cls, permission_type: PermissionType):
        """Scope for specific type (Rails scope pattern)"""
        return cls.query.filter_by(permission_type=permission_type)
    
    @classmethod
    def by_action(cls, action: PermissionAction):
        """Scope for specific action (Rails scope pattern)"""
        return cls.query.filter_by(action=action)
    
    @classmethod
    def by_scope(cls, scope: PermissionScope):
        """Scope for specific scope (Rails scope pattern)"""
        return cls.query.filter_by(scope=scope)
    
    @classmethod
    def active_permissions(cls):
        """Scope for active permissions (Rails scope pattern)"""
        return cls.query.filter_by(status=PermissionStatus.ACTIVE)
    
    @classmethod
    def system_permissions(cls):
        """Scope for system permissions (Rails scope pattern)"""
        return cls.query.filter_by(is_system=True)
    
    @classmethod
    def custom_permissions(cls):
        """Scope for custom permissions (Rails scope pattern)"""
        return cls.query.filter_by(is_custom=True)
    
    @classmethod
    def temporary_permissions(cls):
        """Scope for temporary permissions (Rails scope pattern)"""
        return cls.query.filter_by(is_temporary=True)
    
    @classmethod
    def expiring_soon(cls, days: int = 7):
        """Scope for permissions expiring soon (Rails scope pattern)"""
        cutoff = datetime.now() + timedelta(days=days)
        return cls.query.filter(
            cls.expires_at.isnot(None),
            cls.expires_at <= cutoff,
            cls.status == PermissionStatus.ACTIVE
        )
    
    @classmethod
    def expired_permissions(cls):
        """Scope for expired permissions (Rails scope pattern)"""
        return cls.query.filter(
            (cls.expires_at < datetime.now()) | 
            (cls.status == PermissionStatus.EXPIRED)
        )
    
    @classmethod
    def for_resource(cls, resource_type: str, resource_id: int = None):
        """Scope for resource-specific permissions (Rails scope pattern)"""
        query = cls.query.filter_by(resource_type=resource_type)
        if resource_id:
            query = query.filter_by(resource_id=resource_id)
        return query
    
    @classmethod
    def for_org(cls, org_id: int):
        """Scope for org permissions (Rails scope pattern)"""
        return cls.query.filter_by(org_id=org_id)
    
    @classmethod
    def for_project(cls, project_id: int):
        """Scope for project permissions (Rails scope pattern)"""
        return cls.query.filter_by(project_id=project_id)
    
    @classmethod
    def requiring_approval(cls):
        """Scope for permissions requiring approval (Rails scope pattern)"""
        return cls.query.filter_by(requires_approval=True, status=PermissionStatus.PENDING)
    
    @classmethod
    def create_permission(cls, codename: str, name: str, 
                         permission_type: PermissionType, action: PermissionAction,
                         scope: PermissionScope, **kwargs) -> 'Permission':
        """Factory method to create permission (Rails pattern)"""
        permission_data = {
            'codename': codename,
            'name': name,
            'permission_type': permission_type,
            'action': action,
            'scope': scope,
            'status': PermissionStatus.ACTIVE,
            **kwargs
        }
        
        return cls(**permission_data)
    
    @classmethod
    def create_system_permissions(cls) -> List['Permission']:
        """Create standard system permissions (Rails pattern)"""
        permissions = []
        
        for codename, description in cls.SYSTEM_PERMISSIONS.items():
            existing = cls.by_codename(codename).first()
            if not existing:
                permission = cls.create_permission(
                    codename=codename,
                    name=codename.replace('.', ' ').title(),
                    description=description,
                    permission_type=PermissionType.SYSTEM,
                    action=PermissionAction.ADMIN,
                    scope=PermissionScope.GLOBAL,
                    is_system=True
                )
                permissions.append(permission)
        
        return permissions
    
    @classmethod
    def cleanup_expired_permissions(cls) -> int:
        """Clean up expired permissions (Rails pattern)"""
        expired_permissions = cls.expired_permissions().all()
        
        for permission in expired_permissions:
            if permission.status != PermissionStatus.EXPIRED:
                permission.expire_()
        
        return len(expired_permissions)
    
    @classmethod
    def get_permission_statistics(cls, org_id: int = None) -> Dict[str, Any]:
        """Get permission statistics (Rails class method pattern)"""
        query = cls.query
        if org_id:
            query = query.filter_by(org_id=org_id)
        
        total_permissions = query.count()
        active_permissions = query.filter_by(status=PermissionStatus.ACTIVE).count()
        system_permissions = query.filter_by(is_system=True).count()
        custom_permissions = query.filter_by(is_custom=True).count()
        temporary_permissions = query.filter_by(is_temporary=True).count()
        
        return {
            'total_permissions': total_permissions,
            'active_permissions': active_permissions,
            'system_permissions': system_permissions,
            'custom_permissions': custom_permissions,
            'temporary_permissions': temporary_permissions,
            'active_percentage': round((active_permissions / total_permissions * 100), 2) if total_permissions > 0 else 0
        }
    
    # Display and serialization methods
    def display_type(self) -> str:
        """Get human-readable type (Rails pattern)"""
        return self.permission_type.display_name if self.permission_type else "Unknown Type"
    
    def display_action(self) -> str:
        """Get human-readable action (Rails pattern)"""
        return self.action.display_name if self.action else "Unknown Action"
    
    def display_scope(self) -> str:
        """Get human-readable scope (Rails pattern)"""
        return self.scope.display_name if self.scope else "Unknown Scope"
    
    def display_status(self) -> str:
        """Get human-readable status (Rails pattern)"""
        return self.status.display_name if self.status else "Unknown Status"
    
    def to_dict(self, include_conditions: bool = False, include_usage: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'name': self.name,
            'codename': self.codename,
            'permission_type': self.permission_type.value,
            'display_type': self.display_type(),
            'action': self.action.value,
            'display_action': self.display_action(),
            'scope': self.scope.value,
            'display_scope': self.display_scope(),
            'status': self.status.value,
            'display_status': self.display_status(),
            'description': self.description,
            'resource_type': self.resource_type,
            'resource_id': self.resource_id,
            'is_system': self.is_system,
            'is_custom': self.is_custom,
            'is_temporary': self.is_temporary,
            'is_inherited': self.is_inherited,
            'requires_approval': self.requires_approval,
            'active': self.active_(),
            'expired': self.expired_(),
            'expiring_soon': self.expiring_soon_(),
            'recently_used': self.recently_used_(),
            'has_conditions': self.has_conditions_(),
            'granted_at': self.granted_at.isoformat() if self.granted_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'org_id': self.org_id,
            'project_id': self.project_id,
            'team_id': self.team_id
        }
        
        if include_conditions and self.conditions:
            result['conditions'] = self.conditions
        
        if include_usage and self.extra_metadata:
            result['usage_history'] = self.extra_metadata.get('usage_history', [])
        
        return result
    
    def __repr__(self) -> str:
        return f"<Permission(id={self.id}, codename='{self.codename}', action='{self.action.value}', scope='{self.scope.value}')>"
    
    def __str__(self) -> str:
        return f"{self.name} ({self.display_action()}) - {self.display_scope()}"