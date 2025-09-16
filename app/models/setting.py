"""
Setting Model - System and user configuration management.
Handles hierarchical settings with inheritance, validation, and type safety.
Implements Rails configuration patterns for flexible application settings.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Index
from sqlalchemy.orm import relationship, sessionmaker, validates
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import logging
from ..database import Base

logger = logging.getLogger(__name__)

class SettingScope(PyEnum):
    """Setting scope enumeration"""
    SYSTEM = "system"       # Global system settings
    ORG = "org"            # Organization-specific settings
    PROJECT = "project"     # Project-specific settings
    USER = "user"          # User-specific settings
    TEAM = "team"          # Team-specific settings
    
    @property
    def display_name(self) -> str:
        return {
            self.SYSTEM: "System",
            self.ORG: "Organization",
            self.PROJECT: "Project",
            self.USER: "User",
            self.TEAM: "Team"
        }.get(self, "Unknown Scope")
    
    @property
    def hierarchy_level(self) -> int:
        """Hierarchy level for inheritance (lower = higher priority)"""
        return {
            self.USER: 1,      # Highest priority
            self.TEAM: 2,
            self.PROJECT: 3,
            self.ORG: 4,
            self.SYSTEM: 5     # Lowest priority (default fallback)
        }.get(self, 99)

class SettingType(PyEnum):
    """Setting value type enumeration"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    JSON = "json"
    LIST = "list"
    EMAIL = "email"
    URL = "url"
    PASSWORD = "password"
    COLOR = "color"
    TIMEZONE = "timezone"
    CURRENCY = "currency"
    
    @property
    def display_name(self) -> str:
        return {
            self.STRING: "Text",
            self.INTEGER: "Number",
            self.FLOAT: "Decimal",
            self.BOOLEAN: "True/False",
            self.JSON: "JSON Object",
            self.LIST: "List",
            self.EMAIL: "Email Address",
            self.URL: "URL",
            self.PASSWORD: "Password",
            self.COLOR: "Color",
            self.TIMEZONE: "Timezone",
            self.CURRENCY: "Currency"
        }.get(self, "Unknown Type")

class SettingCategory(PyEnum):
    """Setting category enumeration"""
    GENERAL = "general"
    SECURITY = "security"
    NOTIFICATIONS = "notifications"
    APPEARANCE = "appearance"
    PRIVACY = "privacy"
    BILLING = "billing"
    INTEGRATIONS = "integrations"
    PERFORMANCE = "performance"
    COMPLIANCE = "compliance"
    EXPERIMENTAL = "experimental"
    
    @property
    def display_name(self) -> str:
        return {
            self.GENERAL: "General",
            self.SECURITY: "Security",
            self.NOTIFICATIONS: "Notifications",
            self.APPEARANCE: "Appearance",
            self.PRIVACY: "Privacy",
            self.BILLING: "Billing",
            self.INTEGRATIONS: "Integrations",
            self.PERFORMANCE: "Performance",
            self.COMPLIANCE: "Compliance",
            self.EXPERIMENTAL: "Experimental"
        }.get(self, "Unknown Category")

class Setting(Base):
    __tablename__ = "settings"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(255), nullable=False, index=True)  # e.g., 'email.notifications.enabled'
    scope = Column(SQLEnum(SettingScope), nullable=False, index=True)
    category = Column(SQLEnum(SettingCategory), default=SettingCategory.GENERAL, index=True)
    setting_type = Column(SQLEnum(SettingType), default=SettingType.STRING, index=True)
    
    # Value storage (polymorphic based on setting_type)
    value_string = Column(Text)
    value_integer = Column(Integer)
    value_float = Column(Float)
    value_boolean = Column(Boolean)
    value_json = Column(JSON)
    
    # Metadata and configuration
    default_value = Column(Text)        # Serialized default value
    description = Column(Text)          # Human-readable description
    validation_rules = Column(JSON)     # Validation constraints
    options = Column(JSON)              # Available options for select/enum types
    
    # Scope-specific foreign keys (nullable for flexibility)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    
    # Setting properties
    is_sensitive = Column(Boolean, default=False, index=True)  # Hide value in logs/UI
    is_readonly = Column(Boolean, default=False)               # Prevent user modification
    is_system = Column(Boolean, default=False, index=True)     # System-managed setting
    is_deprecated = Column(Boolean, default=False, index=True) # Mark for removal
    requires_restart = Column(Boolean, default=False)          # Requires app restart
    
    # Inheritance and overrides
    inherits_from = Column(String(255))    # Key to inherit from if not set
    overrides = Column(JSON)               # Override rules for child scopes
    
    # Version and change tracking
    version = Column(Integer, default=1)
    last_changed_by_id = Column(Integer, ForeignKey("users.id"))
    last_validated_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    org = relationship("Org", foreign_keys=[org_id])
    project = relationship("Project", foreign_keys=[project_id])
    user = relationship("User", foreign_keys=[user_id])
    team = relationship("Team", foreign_keys=[team_id])
    last_changed_by = relationship("User", foreign_keys=[last_changed_by_id])
    
    # Enhanced database indexes
    __table_args__ = (
        Index('idx_settings_key_scope', 'key', 'scope'),
        Index('idx_settings_scope_entity', 'scope', 'org_id', 'project_id', 'user_id', 'team_id'),
        Index('idx_settings_category_scope', 'category', 'scope'),
        Index('idx_settings_system_readonly', 'is_system', 'is_readonly'),
        Index('idx_settings_sensitive_deprecated', 'is_sensitive', 'is_deprecated'),
        Index('idx_settings_org_category', 'org_id', 'category'),
        Index('idx_settings_user_category', 'user_id', 'category'),
        Index('idx_settings_key_org', 'key', 'org_id'),
        Index('idx_settings_key_user', 'key', 'user_id'),
        # Unique constraint for scope + entity + key
        Index('idx_settings_unique_scope', 'key', 'scope', 'org_id', 'project_id', 'user_id', 'team_id', unique=True),
    )
    
    # Rails constants
    MAX_KEY_LENGTH = 255
    MAX_STRING_VALUE_LENGTH = 10000
    SENSITIVE_MASK = "***HIDDEN***"
    VALIDATION_CACHE_TTL = 300  # 5 minutes
    
    # Common setting keys (for reference)
    COMMON_KEYS = {
        'theme': 'appearance.theme',
        'timezone': 'general.timezone',
        'language': 'general.language',
        'email_notifications': 'notifications.email.enabled',
        'api_rate_limit': 'performance.api.rate_limit',
        'mfa_required': 'security.mfa.required',
        'session_timeout': 'security.session.timeout_minutes',
        'data_retention_days': 'compliance.data_retention_days'
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Auto-validate on creation
        if self.value is not None:
            self._validate_value()
    
    # Rails-style predicate methods
    def sensitive_(self) -> bool:
        """Check if setting is sensitive (Rails pattern)"""
        return self.is_sensitive
    
    def readonly_(self) -> bool:
        """Check if setting is readonly (Rails pattern)"""
        return self.is_readonly
    
    def system_(self) -> bool:
        """Check if setting is system-managed (Rails pattern)"""
        return self.is_system
    
    def deprecated_(self) -> bool:
        """Check if setting is deprecated (Rails pattern)"""
        return self.is_deprecated
    
    def user_setting_(self) -> bool:
        """Check if setting is user-scoped (Rails pattern)"""
        return self.scope == SettingScope.USER
    
    def org_setting_(self) -> bool:
        """Check if setting is org-scoped (Rails pattern)"""
        return self.scope == SettingScope.ORG
    
    def system_setting_(self) -> bool:
        """Check if setting is system-scoped (Rails pattern)"""
        return self.scope == SettingScope.SYSTEM
    
    def has_default_(self) -> bool:
        """Check if setting has default value (Rails pattern)"""
        return self.default_value is not None
    
    def has_options_(self) -> bool:
        """Check if setting has predefined options (Rails pattern)"""
        return bool(self.options)
    
    def requires_restart_(self) -> bool:
        """Check if setting requires restart (Rails pattern)"""
        return self.requires_restart
    
    def inherits_value_(self) -> bool:
        """Check if setting inherits from another (Rails pattern)"""
        return bool(self.inherits_from)
    
    def recently_updated_(self, hours: int = 24) -> bool:
        """Check if setting was recently updated (Rails pattern)"""
        if not self.updated_at:
            return False
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.updated_at >= cutoff
    
    def valid_(self) -> bool:
        """Check if setting value is valid (Rails pattern)"""
        try:
            self._validate_value()
            return True
        except ValueError:
            return False
    
    def editable_by_user_(self, user_id: int) -> bool:
        """Check if setting can be edited by user (Rails pattern)"""
        if self.readonly_() or self.system_():
            return False
        
        if self.scope == SettingScope.USER:
            return self.user_id == user_id
        elif self.scope == SettingScope.ORG:
            # Would need to check org admin permissions
            return True  # Simplified
        elif self.scope == SettingScope.PROJECT:
            # Would need to check project permissions
            return True  # Simplified
        elif self.scope == SettingScope.SYSTEM:
            # Would need to check system admin permissions
            return False  # Simplified
        
        return False
    
    # Value access with type conversion
    @property
    def value(self) -> Any:
        """Get typed value based on setting_type (Rails pattern)"""
        if self.setting_type == SettingType.STRING:
            return self.value_string
        elif self.setting_type == SettingType.INTEGER:
            return self.value_integer
        elif self.setting_type == SettingType.FLOAT:
            return self.value_float
        elif self.setting_type == SettingType.BOOLEAN:
            return self.value_boolean
        elif self.setting_type == SettingType.JSON:
            return self.value_json
        elif self.setting_type == SettingType.LIST:
            return self.value_json if isinstance(self.value_json, list) else []
        else:
            return self.value_string
    
    @value.setter
    def value(self, val: Any) -> None:
        """Set typed value based on setting_type (Rails pattern)"""
        # Clear all value fields first
        self.value_string = None
        self.value_integer = None
        self.value_float = None
        self.value_boolean = None
        self.value_json = None
        
        if val is None:
            return
        
        # Set appropriate field based on type
        if self.setting_type == SettingType.STRING:
            self.value_string = str(val)
        elif self.setting_type == SettingType.INTEGER:
            self.value_integer = int(val)
        elif self.setting_type == SettingType.FLOAT:
            self.value_float = float(val)
        elif self.setting_type == SettingType.BOOLEAN:
            self.value_boolean = bool(val)
        elif self.setting_type in [SettingType.JSON, SettingType.LIST]:
            self.value_json = val
        else:
            self.value_string = str(val)
    
    def display_value(self) -> str:
        """Get display-safe value (Rails pattern)"""
        if self.sensitive_():
            return self.SENSITIVE_MASK
        
        val = self.value
        if val is None:
            return "Not set"
        
        if self.setting_type == SettingType.PASSWORD:
            return self.SENSITIVE_MASK
        elif self.setting_type == SettingType.BOOLEAN:
            return "Yes" if val else "No"
        elif self.setting_type in [SettingType.JSON, SettingType.LIST]:
            return json.dumps(val) if val else "{}"
        else:
            return str(val)
    
    # Rails bang methods
    def update_value_(self, new_value: Any, changed_by_id: int = None) -> None:
        """Update setting value with validation (Rails bang method pattern)"""
        old_value = self.value
        
        # Validate new value
        self.value = new_value
        self._validate_value()
        
        # Update metadata
        self.version += 1
        self.last_changed_by_id = changed_by_id
        self.updated_at = datetime.now()
        
        # Log change if sensitive
        if self.sensitive_():
            logger.info(f"Sensitive setting {self.key} updated by user {changed_by_id}")
    
    def reset_to_default_(self) -> None:
        """Reset setting to default value (Rails bang method pattern)"""
        if self.has_default_():
            try:
                default_val = self._deserialize_value(self.default_value)
                self.value = default_val
                self.version += 1
                self.updated_at = datetime.now()
            except (ValueError, json.JSONDecodeError) as e:
                logger.error(f"Failed to reset setting {self.key} to default: {e}")
                raise ValueError(f"Invalid default value for setting {self.key}")
    
    def deprecate_(self, reason: str = None) -> None:
        """Mark setting as deprecated (Rails bang method pattern)"""
        self.is_deprecated = True
        self.updated_at = datetime.now()
        
        if reason:
            if not self.validation_rules:
                self.validation_rules = {}
            self.validation_rules['deprecation_reason'] = reason
    
    def undeprecate_(self) -> None:
        """Remove deprecated status (Rails bang method pattern)"""
        self.is_deprecated = False
        self.updated_at = datetime.now()
        
        if self.validation_rules and 'deprecation_reason' in self.validation_rules:
            del self.validation_rules['deprecation_reason']
    
    def mark_sensitive_(self) -> None:
        """Mark setting as sensitive (Rails bang method pattern)"""
        self.is_sensitive = True
        self.updated_at = datetime.now()
    
    def unmark_sensitive_(self) -> None:
        """Remove sensitive marking (Rails bang method pattern)"""
        self.is_sensitive = False
        self.updated_at = datetime.now()
    
    def validate_(self) -> List[str]:
        """Validate setting configuration (Rails bang method pattern)"""
        errors = []
        
        try:
            self._validate_value()
            self.last_validated_at = datetime.now()
        except ValueError as e:
            errors.append(str(e))
        
        return errors
    
    # Rails helper methods
    def _validate_value(self) -> None:
        """Validate current value (Rails private pattern)"""
        if self.value is None:
            return  # Null values are generally allowed
        
        # Type-specific validation
        if self.setting_type == SettingType.EMAIL:
            self._validate_email(self.value)
        elif self.setting_type == SettingType.URL:
            self._validate_url(self.value)
        elif self.setting_type == SettingType.INTEGER:
            self._validate_integer(self.value)
        elif self.setting_type == SettingType.FLOAT:
            self._validate_float(self.value)
        
        # Custom validation rules
        if self.validation_rules:
            self._apply_validation_rules(self.value)
        
        # Options validation
        if self.has_options_():
            self._validate_options(self.value)
    
    def _validate_email(self, value: str) -> None:
        """Validate email format (Rails private pattern)"""
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, str(value)):
            raise ValueError(f"Invalid email format: {value}")
    
    def _validate_url(self, value: str) -> None:
        """Validate URL format (Rails private pattern)"""
        import re
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, str(value)):
            raise ValueError(f"Invalid URL format: {value}")
    
    def _validate_integer(self, value: Any) -> None:
        """Validate integer value (Rails private pattern)"""
        try:
            int_val = int(value)
            if self.validation_rules:
                if 'min' in self.validation_rules and int_val < self.validation_rules['min']:
                    raise ValueError(f"Value {int_val} is below minimum {self.validation_rules['min']}")
                if 'max' in self.validation_rules and int_val > self.validation_rules['max']:
                    raise ValueError(f"Value {int_val} is above maximum {self.validation_rules['max']}")
        except (ValueError, TypeError):
            raise ValueError(f"Invalid integer value: {value}")
    
    def _validate_float(self, value: Any) -> None:
        """Validate float value (Rails private pattern)"""
        try:
            float_val = float(value)
            if self.validation_rules:
                if 'min' in self.validation_rules and float_val < self.validation_rules['min']:
                    raise ValueError(f"Value {float_val} is below minimum {self.validation_rules['min']}")
                if 'max' in self.validation_rules and float_val > self.validation_rules['max']:
                    raise ValueError(f"Value {float_val} is above maximum {self.validation_rules['max']}")
        except (ValueError, TypeError):
            raise ValueError(f"Invalid float value: {value}")
    
    def _apply_validation_rules(self, value: Any) -> None:
        """Apply custom validation rules (Rails private pattern)"""
        if not self.validation_rules:
            return
        
        # String length validation
        if 'max_length' in self.validation_rules:
            if len(str(value)) > self.validation_rules['max_length']:
                raise ValueError(f"Value exceeds maximum length of {self.validation_rules['max_length']}")
        
        # Pattern validation
        if 'pattern' in self.validation_rules:
            import re
            if not re.match(self.validation_rules['pattern'], str(value)):
                raise ValueError(f"Value does not match required pattern")
        
        # Required validation
        if self.validation_rules.get('required', False) and not value:
            raise ValueError("Value is required")
    
    def _validate_options(self, value: Any) -> None:
        """Validate value against available options (Rails private pattern)"""
        if value not in self.options:
            raise ValueError(f"Value '{value}' is not in allowed options: {self.options}")
    
    def _serialize_value(self, value: Any) -> str:
        """Serialize value for storage (Rails private pattern)"""
        if value is None:
            return None
        
        if self.setting_type in [SettingType.JSON, SettingType.LIST]:
            return json.dumps(value)
        else:
            return str(value)
    
    def _deserialize_value(self, serialized_value: str) -> Any:
        """Deserialize value from storage (Rails private pattern)"""
        if not serialized_value:
            return None
        
        if self.setting_type == SettingType.INTEGER:
            return int(serialized_value)
        elif self.setting_type == SettingType.FLOAT:
            return float(serialized_value)
        elif self.setting_type == SettingType.BOOLEAN:
            return serialized_value.lower() in ['true', '1', 'yes', 'on']
        elif self.setting_type in [SettingType.JSON, SettingType.LIST]:
            return json.loads(serialized_value)
        else:
            return serialized_value
    
    def get_inherited_value(self) -> Any:
        """Get value through inheritance chain (Rails pattern)"""
        if self.value is not None:
            return self.value
        
        if self.inherits_from:
            # Look for parent setting
            parent_setting = self.__class__.get_setting(self.inherits_from, 
                                                       scope=SettingScope.SYSTEM)
            if parent_setting and parent_setting.value is not None:
                return parent_setting.value
        
        # Fall back to default
        if self.has_default_():
            return self._deserialize_value(self.default_value)
        
        return None
    
    # Rails class methods and scopes
    @classmethod
    def by_key(cls, key: str):
        """Scope for specific key (Rails scope pattern)"""
        return cls.query.filter_by(key=key)
    
    @classmethod
    def by_scope(cls, scope: SettingScope):
        """Scope for specific scope (Rails scope pattern)"""
        return cls.query.filter_by(scope=scope)
    
    @classmethod
    def by_category(cls, category: SettingCategory):
        """Scope for specific category (Rails scope pattern)"""
        return cls.query.filter_by(category=category)
    
    @classmethod
    def for_user(cls, user_id: int):
        """Scope for user settings (Rails scope pattern)"""
        return cls.query.filter_by(scope=SettingScope.USER, user_id=user_id)
    
    @classmethod
    def for_org(cls, org_id: int):
        """Scope for org settings (Rails scope pattern)"""
        return cls.query.filter_by(scope=SettingScope.ORG, org_id=org_id)
    
    @classmethod
    def for_project(cls, project_id: int):
        """Scope for project settings (Rails scope pattern)"""
        return cls.query.filter_by(scope=SettingScope.PROJECT, project_id=project_id)
    
    @classmethod
    def system_settings(cls):
        """Scope for system settings (Rails scope pattern)"""
        return cls.query.filter_by(scope=SettingScope.SYSTEM)
    
    @classmethod
    def sensitive_settings(cls):
        """Scope for sensitive settings (Rails scope pattern)"""
        return cls.query.filter_by(is_sensitive=True)
    
    @classmethod
    def deprecated_settings(cls):
        """Scope for deprecated settings (Rails scope pattern)"""
        return cls.query.filter_by(is_deprecated=True)
    
    @classmethod
    def readonly_settings(cls):
        """Scope for readonly settings (Rails scope pattern)"""
        return cls.query.filter_by(is_readonly=True)
    
    @classmethod
    def get_setting(cls, key: str, scope: SettingScope = None, 
                   user_id: int = None, org_id: int = None, 
                   project_id: int = None, team_id: int = None) -> Optional['Setting']:
        """Get setting with scope resolution (Rails finder pattern)"""
        query = cls.query.filter_by(key=key)
        
        if scope:
            query = query.filter_by(scope=scope)
        
        # Add scope-specific filters
        if user_id is not None:
            query = query.filter_by(user_id=user_id)
        if org_id is not None:
            query = query.filter_by(org_id=org_id)
        if project_id is not None:
            query = query.filter_by(project_id=project_id)
        if team_id is not None:
            query = query.filter_by(team_id=team_id)
        
        return query.first()
    
    @classmethod
    def get_effective_value(cls, key: str, user_id: int = None, org_id: int = None, 
                           project_id: int = None, team_id: int = None) -> Any:
        """Get effective value with hierarchy resolution (Rails pattern)"""
        # Check scopes in priority order
        scopes = [
            (SettingScope.USER, {'user_id': user_id}),
            (SettingScope.TEAM, {'team_id': team_id}),
            (SettingScope.PROJECT, {'project_id': project_id}),
            (SettingScope.ORG, {'org_id': org_id}),
            (SettingScope.SYSTEM, {})
        ]
        
        for scope, filters in scopes:
            # Skip if required ID is not provided
            if scope != SettingScope.SYSTEM and not any(filters.values()):
                continue
            
            setting = cls.get_setting(key, scope=scope, **filters)
            if setting and setting.value is not None:
                return setting.value
        
        return None
    
    @classmethod
    def create_setting(cls, key: str, value: Any, scope: SettingScope, 
                      setting_type: SettingType = SettingType.STRING,
                      category: SettingCategory = SettingCategory.GENERAL,
                      **kwargs) -> 'Setting':
        """Factory method to create setting (Rails pattern)"""
        setting_data = {
            'key': key,
            'scope': scope,
            'setting_type': setting_type,
            'category': category,
            **kwargs
        }
        
        setting = cls(**setting_data)
        setting.value = value
        
        return setting
    
    @classmethod
    def bulk_update_settings(cls, settings_data: Dict[str, Any], 
                           scope: SettingScope, **scope_filters) -> int:
        """Bulk update multiple settings (Rails pattern)"""
        updated_count = 0
        
        for key, value in settings_data.items():
            setting = cls.get_setting(key, scope=scope, **scope_filters)
            if setting:
                setting.update_value_(value)
                updated_count += 1
        
        return updated_count
    
    @classmethod
    def export_settings(cls, scope: SettingScope = None, 
                       exclude_sensitive: bool = True) -> Dict[str, Any]:
        """Export settings to dictionary (Rails pattern)"""
        query = cls.query
        
        if scope:
            query = query.filter_by(scope=scope)
        
        if exclude_sensitive:
            query = query.filter_by(is_sensitive=False)
        
        settings = query.all()
        result = {}
        
        for setting in settings:
            if setting.value is not None:
                result[setting.key] = setting.value
        
        return result
    
    @classmethod
    def cleanup_deprecated_settings(cls) -> int:
        """Remove deprecated settings (Rails pattern)"""
        deprecated_settings = cls.deprecated_settings().all()
        count = len(deprecated_settings)
        
        for setting in deprecated_settings:
            setting.delete()
        
        return count
    
    # Display and serialization methods
    def display_scope(self) -> str:
        """Get human-readable scope (Rails pattern)"""
        return self.scope.display_name if self.scope else "Unknown Scope"
    
    def display_type(self) -> str:
        """Get human-readable type (Rails pattern)"""
        return self.setting_type.display_name if self.setting_type else "Unknown Type"
    
    def display_category(self) -> str:
        """Get human-readable category (Rails pattern)"""
        return self.category.display_name if self.category else "Unknown Category"
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'key': self.key,
            'scope': self.scope.value,
            'display_scope': self.display_scope(),
            'category': self.category.value,
            'display_category': self.display_category(),
            'setting_type': self.setting_type.value,
            'display_type': self.display_type(),
            'description': self.description,
            'is_sensitive': self.is_sensitive,
            'is_readonly': self.is_readonly,
            'is_system': self.is_system,
            'is_deprecated': self.is_deprecated,
            'requires_restart': self.requires_restart,
            'has_default': self.has_default_(),
            'has_options': self.has_options_(),
            'version': self.version,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        
        # Include value based on sensitivity and permissions
        if include_sensitive or not self.is_sensitive:
            result['value'] = self.value
        else:
            result['value'] = self.display_value()
        
        if self.has_options_():
            result['options'] = self.options
        
        if self.validation_rules:
            result['validation_rules'] = self.validation_rules
        
        return result
    
    def __repr__(self) -> str:
        return f"<Setting(id={self.id}, key='{self.key}', scope='{self.scope.value}', type='{self.setting_type.value}')>"
    
    def __str__(self) -> str:
        return f"{self.key} ({self.display_scope()}): {self.display_value()}"