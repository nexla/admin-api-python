"""
Role-Based Access Control (RBAC) system for FastAPI Admin API.
Provides comprehensive permission checking and role management.
"""

from typing import List, Set, Optional, Dict, Any, Callable
from functools import wraps
from sqlalchemy.orm import Session
from fastapi import HTTPException, status, Depends
from enum import Enum

from ..database import get_db
from ..models.user import User
from ..models.role import Role, Permission, UserRole
from .jwt_auth import get_current_user

class SystemRoles(str, Enum):
    """Predefined system roles"""
    SUPER_ADMIN = "super_admin"
    ORG_ADMIN = "org_admin"
    USER_MANAGER = "user_manager"
    DATA_MANAGER = "data_manager"
    DEVELOPER = "developer"
    ANALYST = "analyst"
    VIEWER = "viewer"

class SystemPermissions(str, Enum):
    """Predefined system permissions"""
    # User management
    USER_READ = "users:read"
    USER_WRITE = "users:write"  
    USER_DELETE = "users:delete"
    USER_ADMIN = "users:admin"
    
    # Organization management
    ORG_READ = "organizations:read"
    ORG_WRITE = "organizations:write"
    ORG_DELETE = "organizations:delete"
    ORG_ADMIN = "organizations:admin"
    
    # Data source management
    DATA_SOURCE_READ = "data_sources:read"
    DATA_SOURCE_WRITE = "data_sources:write"
    DATA_SOURCE_DELETE = "data_sources:delete"
    DATA_SOURCE_ADMIN = "data_sources:admin"
    
    # Data set management
    DATA_SET_READ = "data_sets:read"
    DATA_SET_WRITE = "data_sets:write"
    DATA_SET_DELETE = "data_sets:delete"
    DATA_SET_ADMIN = "data_sets:admin"
    
    # Data sink management
    DATA_SINK_READ = "data_sinks:read"
    DATA_SINK_WRITE = "data_sinks:write"
    DATA_SINK_DELETE = "data_sinks:delete"
    DATA_SINK_ADMIN = "data_sinks:admin"
    
    # System administration
    SYSTEM_ADMIN = "system:admin"
    SYSTEM_CONFIG = "system:config"
    
    # API access
    API_ACCESS = "api:access"
    API_ADMIN = "api:admin"

class RBACService:
    """Role-Based Access Control service"""
    
    @staticmethod
    def get_user_permissions(user: User, db: Session, org_id: Optional[int] = None) -> Set[str]:
        """Get all permissions for a user"""
        permissions = set()
        
        # For now, implement a basic permission system based on user status
        # This will be enhanced once database relationships are enabled
        
        if not user or not user.is_active():
            return permissions
            
        # Basic permissions for all active users
        permissions.add(SystemPermissions.API_ACCESS)
        permissions.add(SystemPermissions.USER_READ)
        
        # Admin detection (basic implementation)
        if user.email and ("admin" in user.email.lower() or user.email.endswith("@nexla.com")):
            permissions.update([
                SystemPermissions.SYSTEM_ADMIN,
                SystemPermissions.USER_ADMIN,
                SystemPermissions.ORG_ADMIN,
                SystemPermissions.DATA_SOURCE_ADMIN,
                SystemPermissions.DATA_SET_ADMIN,
                SystemPermissions.DATA_SINK_ADMIN,
                SystemPermissions.API_ADMIN
            ])
        else:
            # Regular user permissions
            permissions.update([
                SystemPermissions.DATA_SOURCE_READ,
                SystemPermissions.DATA_SOURCE_WRITE,
                SystemPermissions.DATA_SET_READ,
                SystemPermissions.DATA_SET_WRITE,
                SystemPermissions.DATA_SINK_READ,
                SystemPermissions.DATA_SINK_WRITE,
                SystemPermissions.ORG_READ
            ])
            
        return permissions
    
    @staticmethod
    def has_permission(user: User, permission: str, db: Session, org_id: Optional[int] = None) -> bool:
        """Check if user has a specific permission"""
        user_permissions = RBACService.get_user_permissions(user, db, org_id)
        return permission in user_permissions
    
    @staticmethod
    def has_any_permission(user: User, permissions: List[str], db: Session, org_id: Optional[int] = None) -> bool:
        """Check if user has any of the specified permissions"""
        user_permissions = RBACService.get_user_permissions(user, db, org_id)
        return any(perm in user_permissions for perm in permissions)
    
    @staticmethod
    def has_all_permissions(user: User, permissions: List[str], db: Session, org_id: Optional[int] = None) -> bool:
        """Check if user has all of the specified permissions"""
        user_permissions = RBACService.get_user_permissions(user, db, org_id)
        return all(perm in user_permissions for perm in permissions)
    
    @staticmethod
    def require_permission(permission: str, org_id: Optional[int] = None):
        """Decorator to require a specific permission"""
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Extract current user and db from dependencies
                current_user = None
                db = None
                
                # Look for current_user and db in function signature
                import inspect
                sig = inspect.signature(func)
                
                for param_name, param in sig.parameters.items():
                    if param_name == 'current_user' and len(args) > list(sig.parameters.keys()).index(param_name):
                        current_user = args[list(sig.parameters.keys()).index(param_name)]
                    elif param_name == 'db' and len(args) > list(sig.parameters.keys()).index(param_name):
                        db = args[list(sig.parameters.keys()).index(param_name)]
                
                # Also check kwargs
                if 'current_user' in kwargs:
                    current_user = kwargs['current_user']
                if 'db' in kwargs:
                    db = kwargs['db']
                
                if not current_user or not db:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Missing authentication dependencies"
                    )
                
                if not RBACService.has_permission(current_user, permission, db, org_id):
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Insufficient permissions. Required: {permission}"
                    )
                
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    
    @staticmethod
    def require_any_permission(permissions: List[str], org_id: Optional[int] = None):
        """Decorator to require any of the specified permissions"""
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Extract current user and db from dependencies
                current_user = None
                db = None
                
                import inspect
                sig = inspect.signature(func)
                
                for param_name, param in sig.parameters.items():
                    if param_name == 'current_user' and len(args) > list(sig.parameters.keys()).index(param_name):
                        current_user = args[list(sig.parameters.keys()).index(param_name)]
                    elif param_name == 'db' and len(args) > list(sig.parameters.keys()).index(param_name):
                        db = args[list(sig.parameters.keys()).index(param_name)]
                
                if 'current_user' in kwargs:
                    current_user = kwargs['current_user']
                if 'db' in kwargs:
                    db = kwargs['db']
                
                if not current_user or not db:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Missing authentication dependencies"
                    )
                
                if not RBACService.has_any_permission(current_user, permissions, db, org_id):
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Insufficient permissions. Required any of: {', '.join(permissions)}"
                    )
                
                return await func(*args, **kwargs)
            return wrapper
        return decorator

def require_admin():
    """Decorator requiring admin permissions"""
    return RBACService.require_permission(SystemPermissions.SYSTEM_ADMIN)

def require_user_admin():
    """Decorator requiring user admin permissions"""
    return RBACService.require_permission(SystemPermissions.USER_ADMIN)

def require_org_admin():
    """Decorator requiring organization admin permissions"""
    return RBACService.require_permission(SystemPermissions.ORG_ADMIN)

def require_data_admin():
    """Decorator requiring data admin permissions"""
    return RBACService.require_any_permission([
        SystemPermissions.DATA_SOURCE_ADMIN,
        SystemPermissions.DATA_SET_ADMIN,
        SystemPermissions.DATA_SINK_ADMIN
    ])

# Dependency functions for FastAPI
def get_permissions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Set[str]:
    """FastAPI dependency to get current user's permissions"""
    return RBACService.get_user_permissions(current_user, db)

def check_permission(permission: str):
    """Factory function to create permission check dependency"""
    def permission_checker(
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
    ) -> bool:
        if not RBACService.has_permission(current_user, permission, db):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions. Required: {permission}"
            )
        return True
    return permission_checker

def check_admin_permission():
    """Dependency to check admin permission"""
    return check_permission(SystemPermissions.SYSTEM_ADMIN)

def check_user_admin_permission():
    """Dependency to check user admin permission"""
    return check_permission(SystemPermissions.USER_ADMIN)