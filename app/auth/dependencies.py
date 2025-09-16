"""
FastAPI Dependencies for Authorization
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from typing import Optional, Any

from app.database import get_db
from app.auth import get_current_user
from app.auth.permissions import AbilityChecker, Action, ResourceType, AuthorizationError
from app.models.user import User

security = HTTPBearer()


def get_ability_checker(
    current_user: User = Depends(get_current_user)
) -> AbilityChecker:
    """Get ability checker for current user"""
    return AbilityChecker(current_user)


def require_permission(action: Action, resource_type: ResourceType):
    """Create a dependency that requires specific permission"""
    
    def permission_checker(
        resource: Optional[Any] = None,
        ability: AbilityChecker = Depends(get_ability_checker)
    ):
        """Check if user has required permission"""
        try:
            ability.authorize(action, resource_type, resource)
        except PermissionError as e:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=str(e)
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Authorization check failed: {str(e)}"
            )
        
        return True
    
    return permission_checker


def can_read_resource(resource_type: ResourceType):
    """Dependency for read permission"""
    return require_permission(Action.READ, resource_type)


def can_manage_resource(resource_type: ResourceType):
    """Dependency for manage permission"""
    return require_permission(Action.MANAGE, resource_type)


def can_operate_resource(resource_type: ResourceType):
    """Dependency for operate permission"""
    return require_permission(Action.OPERATE, resource_type)


def can_create_resource(resource_type: ResourceType):
    """Dependency for create permission"""
    return require_permission(Action.CREATE, resource_type)


def can_update_resource(resource_type: ResourceType):
    """Dependency for update permission"""
    return require_permission(Action.UPDATE, resource_type)


def can_delete_resource(resource_type: ResourceType):
    """Dependency for delete permission"""
    return require_permission(Action.DELETE, resource_type)


def can_transform_resource(resource_type: ResourceType):
    """Dependency for transform permission"""
    return require_permission(Action.TRANSFORM, resource_type)


def can_probe_resource(resource_type: ResourceType):
    """Dependency for probe permission"""
    return require_permission(Action.PROBE, resource_type)


# Common permission dependencies
def require_super_user(
    ability: AbilityChecker = Depends(get_ability_checker)
):
    """Require super user privileges"""
    if not ability.user.is_super_user_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Super user privileges required"
        )
    return True


def require_org_admin(
    org_id: int,
    ability: AbilityChecker = Depends(get_ability_checker),
    db: Session = Depends(get_db)
):
    """Require org admin privileges"""
    from app.models.org import Org
    
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not org.has_admin_access_(ability.user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Organization admin privileges required"
        )
    
    return org


def require_org_member(
    org_id: int,
    ability: AbilityChecker = Depends(get_ability_checker),
    db: Session = Depends(get_db)
):
    """Require org membership"""
    from app.models.org import Org
    
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not ability.user.is_org_member_(org):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Organization membership required"
        )
    
    return org


# Resource-specific authorization dependencies

# Users
can_read_users = can_read_resource(ResourceType.USER)
can_manage_users = can_manage_resource(ResourceType.USER)
can_operate_users = can_operate_resource(ResourceType.USER)

# Organizations
can_read_orgs = can_read_resource(ResourceType.ORG)
can_manage_orgs = can_manage_resource(ResourceType.ORG)
can_operate_orgs = can_operate_resource(ResourceType.ORG)

# Teams
can_read_teams = can_read_resource(ResourceType.TEAM)
can_manage_teams = can_manage_resource(ResourceType.TEAM)

# Projects
can_read_projects = can_read_resource(ResourceType.PROJECT)
can_manage_projects = can_manage_resource(ResourceType.PROJECT)

# Data Sources
can_read_data_sources = can_read_resource(ResourceType.DATA_SOURCE)
can_manage_data_sources = can_manage_resource(ResourceType.DATA_SOURCE)
can_operate_data_sources = can_operate_resource(ResourceType.DATA_SOURCE)

# Data Sinks
can_read_data_sinks = can_read_resource(ResourceType.DATA_SINK)
can_manage_data_sinks = can_manage_resource(ResourceType.DATA_SINK)
can_operate_data_sinks = can_operate_resource(ResourceType.DATA_SINK)

# Data Sets
can_read_data_sets = can_read_resource(ResourceType.DATA_SET)
can_manage_data_sets = can_manage_resource(ResourceType.DATA_SET)
can_operate_data_sets = can_operate_resource(ResourceType.DATA_SET)
can_transform_data_sets = can_transform_resource(ResourceType.DATA_SET)

# Data Schemas
can_read_data_schemas = can_read_resource(ResourceType.DATA_SCHEMA)
can_manage_data_schemas = can_manage_resource(ResourceType.DATA_SCHEMA)

# Data Credentials
can_read_data_credentials = can_read_resource(ResourceType.DATA_CREDENTIALS)
can_manage_data_credentials = can_manage_resource(ResourceType.DATA_CREDENTIALS)

# Data Maps
can_read_data_maps = can_read_resource(ResourceType.DATA_MAP)
can_manage_data_maps = can_manage_resource(ResourceType.DATA_MAP)

# Flow Nodes
can_read_flow_nodes = can_read_resource(ResourceType.FLOW_NODE)
can_manage_flow_nodes = can_manage_resource(ResourceType.FLOW_NODE)
can_operate_flow_nodes = can_operate_resource(ResourceType.FLOW_NODE)

# Flow Triggers
can_read_flow_triggers = can_read_resource(ResourceType.FLOW_TRIGGER)
can_manage_flow_triggers = can_manage_resource(ResourceType.FLOW_TRIGGER)
can_operate_flow_triggers = can_operate_resource(ResourceType.FLOW_TRIGGER)

# Notifications
can_read_notifications = can_read_resource(ResourceType.NOTIFICATION)
can_manage_notifications = can_manage_resource(ResourceType.NOTIFICATION)

# Transforms and Validators
can_read_transforms = can_read_resource(ResourceType.TRANSFORM)
can_manage_transforms = can_manage_resource(ResourceType.TRANSFORM)

can_read_validators = can_read_resource(ResourceType.VALIDATOR)
can_manage_validators = can_manage_resource(ResourceType.VALIDATOR)

# Catalog and AI Configs
can_read_catalog_configs = can_read_resource(ResourceType.CATALOG_CONFIG)
can_manage_catalog_configs = can_manage_resource(ResourceType.CATALOG_CONFIG)

can_read_genai_configs = can_read_resource(ResourceType.GEN_AI_CONFIG)
can_manage_genai_configs = can_manage_resource(ResourceType.GEN_AI_CONFIG)

# Marketplace
can_read_marketplace_items = can_read_resource(ResourceType.MARKETPLACE_ITEM)
can_manage_marketplace_items = can_manage_resource(ResourceType.MARKETPLACE_ITEM)

can_read_domains = can_read_resource(ResourceType.DOMAIN)
can_manage_domains = can_manage_resource(ResourceType.DOMAIN)

# API Keys
can_read_api_keys = can_read_resource(ResourceType.API_KEY)
can_manage_api_keys = can_manage_resource(ResourceType.API_KEY)

# Async Tasks
can_read_async_tasks = can_read_resource(ResourceType.ASYNC_TASK)
can_manage_async_tasks = can_manage_resource(ResourceType.ASYNC_TASK)

# Audit Logs
can_read_audit_logs = can_read_resource(ResourceType.AUDIT_LOG)


# Utility functions for authorization checks

def check_resource_access(
    user: User,
    action: Action,
    resource_type: ResourceType,
    resource: Any = None
) -> bool:
    """Helper function to check resource access"""
    ability = AbilityChecker(user)
    return ability.can(action, resource_type, resource)


def authorize_resource_access(
    user: User,
    action: Action,
    resource_type: ResourceType,
    resource: Any = None
) -> None:
    """Helper function to authorize resource access"""
    ability = AbilityChecker(user)
    try:
        ability.authorize(action, resource_type, resource)
    except PermissionError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )