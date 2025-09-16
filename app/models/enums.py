"""
Model Enums - Define status and type constants for models.
Provides consistent enum values across the application.
"""

from enum import Enum


class UserStatus(str, Enum):
    """User status enumeration"""
    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    SUSPENDED = "SUSPENDED"
    PENDING = "PENDING"


class DataSourceStatus(str, Enum):
    """Data source status enumeration"""
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    INACTIVE = "INACTIVE"
    ERROR = "ERROR"
    DRAFT = "DRAFT"


class TaskStatus(str, Enum):
    """Async task status enumeration"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class OrgRole(str, Enum):
    """Organization role enumeration"""
    ADMIN = "admin"
    USER = "user"
    COLLABORATOR = "collaborator"
    VIEWER = "viewer"


class UserStatus(str, Enum):
    """User status enumeration"""
    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED" 
    SUSPENDED = "SUSPENDED"
    PENDING = "PENDING"


class DataSourceStatus(str, Enum):
    """Data source status enumeration"""
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    INACTIVE = "INACTIVE"
    ERROR = "ERROR"
    DRAFT = "DRAFT"