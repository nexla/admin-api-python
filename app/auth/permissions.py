"""
Authorization and Permissions System for FastAPI
Equivalent to Rails CanCan authorization framework
"""

from enum import Enum
from typing import Dict, List, Optional, Union, Any, Callable
from abc import ABC, abstractmethod
import logging

from app.models.user import User

logger = logging.getLogger(__name__)


class Action(Enum):
    """Available actions for authorization"""
    READ = "read"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    MANAGE = "manage"  # All actions
    OPERATE = "operate"  # Operations like start/stop
    TRANSFORM = "transform"  # Data transformation
    PROBE = "probe"  # Connection testing


class ResourceType(Enum):
    """Resource types that can be authorized"""
    USER = "User"
    ORG = "Org"
    TEAM = "Team"
    PROJECT = "Project"
    DATA_SOURCE = "DataSource"
    DATA_SINK = "DataSink"
    DATA_SET = "DataSet"
    DATA_SCHEMA = "DataSchema"
    DATA_CREDENTIALS = "DataCredentials"
    DATA_CREDENTIALS_GROUP = "DataCredentialsGroup"
    DATA_MAP = "DataMap"
    DATA_FLOW = "DataFlow"
    CUSTOM_DATA_FLOW = "CustomDataFlow"
    FLOW_NODE = "FlowNode"
    FLOW_TRIGGER = "FlowTrigger"
    NOTIFICATION = "Notification"
    NOTIFICATION_CHANNEL_SETTING = "NotificationChannelSetting"
    NOTIFICATION_SETTING = "NotificationSetting"
    QUARANTINE_SETTING = "QuarantineSetting"
    TRANSFORM = "Transform"
    ATTRIBUTE_TRANSFORM = "AttributeTransform"
    VALIDATOR = "Validator"
    CODE_CONTAINER = "CodeContainer"
    DOC_CONTAINER = "DocContainer"
    DASHBOARD_TRANSFORM = "DashboardTransform"
    CATALOG_CONFIG = "CatalogConfig"
    GEN_AI_CONFIG = "GenAiConfig"
    ORG_CUSTODIAN = "OrgCustodian"
    RESOURCE_PARAMETER = "ResourceParameter"
    VENDOR_ENDPOINT = "VendorEndpoint"
    VENDOR = "Vendor"
    AUTH_TEMPLATE = "AuthTemplate"
    AUTH_PARAMETER = "AuthParameter"
    MARKETPLACE_ITEM = "MarketplaceItem"
    DOMAIN = "Domain"
    DATA_SETS_CATALOG_REF = "DataSetsCatalogRef"
    APPROVAL_STEP = "ApprovalStep"
    APPROVAL_REQUEST = "ApprovalRequest"
    USER_SETTING = "UserSetting"
    CLUSTER_ENDPOINT = "ClusterEndpoint"
    RUNTIME = "Runtime"
    API_KEY = "ApiKey"
    ASYNC_TASK = "AsyncTask"
    AUDIT_LOG = "AuditLog"


class Permission:
    """Represents a permission rule"""
    
    def __init__(
        self,
        action: Action,
        resource_type: ResourceType,
        condition: Optional[Callable[[Any, User], bool]] = None
    ):
        self.action = action
        self.resource_type = resource_type
        self.condition = condition
    
    def allows(self, user: User, resource: Any = None) -> bool:
        """Check if this permission allows the action"""
        if self.condition is None:
            return True
        
        try:
            return self.condition(resource, user)
        except Exception as e:
            logger.error(f"Permission check failed: {e}")
            return False


class AbilityChecker:
    """Main authorization class - equivalent to Rails Ability"""
    
    def __init__(self, user: User):
        self.user = user
        self.permissions: List[Permission] = []
        self._define_abilities()
    
    def _define_abilities(self):
        """Define all permissions for the user - equivalent to Rails ability.rb"""
        if self.user is None:
            return
        
        # FlowNode permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.FLOW_NODE, self._can_manage_flow_node),
            Permission(Action.OPERATE, ResourceType.FLOW_NODE, self._can_operate_flow_node),
            Permission(Action.READ, ResourceType.FLOW_NODE, self._can_read_flow_node),
        ])
        
        # FlowTrigger permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.FLOW_TRIGGER, self._can_manage_flow_trigger),
            Permission(Action.OPERATE, ResourceType.FLOW_TRIGGER, self._can_operate_flow_trigger),
            Permission(Action.READ, ResourceType.FLOW_TRIGGER, self._can_read_flow_trigger),
        ])
        
        # CatalogConfig permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.CATALOG_CONFIG, self._can_read_catalog_config),
            Permission(Action.MANAGE, ResourceType.CATALOG_CONFIG, self._can_manage_catalog_config),
        ])
        
        # GenAiConfig permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.GEN_AI_CONFIG, self._can_read_genai_config),
            Permission(Action.MANAGE, ResourceType.GEN_AI_CONFIG, self._can_manage_genai_config),
        ])
        
        # Super user only resources
        super_user_resources = [
            ResourceType.RESOURCE_PARAMETER,
            ResourceType.VENDOR_ENDPOINT,
            ResourceType.VENDOR,
            ResourceType.AUTH_TEMPLATE,
            ResourceType.AUTH_PARAMETER,
        ]
        
        for resource_type in super_user_resources:
            self.permissions.extend([
                Permission(Action.READ, resource_type, self._is_super_user),
                Permission(Action.MANAGE, resource_type, self._is_super_user),
            ])
        
        # User permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.USER, self._can_manage_user),
            Permission(Action.OPERATE, ResourceType.USER, self._can_operate_user),
        ])
        
        # Team permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.TEAM, self._can_read_team),
            Permission(Action.MANAGE, ResourceType.TEAM, self._can_manage_team),
        ])
        
        # Org permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.ORG, self._can_manage_org),
            Permission(Action.READ, ResourceType.ORG, self._can_read_org),
            Permission(Action.OPERATE, ResourceType.ORG, self._can_operate_org),
        ])
        
        # OrgCustodian permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.ORG_CUSTODIAN, self._can_manage_org_custodian),
        ])
        
        # DataSet permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.DATA_SET, self._can_manage_data_set),
            Permission(Action.READ, ResourceType.DATA_SET, self._can_read_data_set),
            Permission(Action.TRANSFORM, ResourceType.DATA_SET, self._can_transform_data_set),
            Permission(Action.OPERATE, ResourceType.DATA_SET, self._can_operate_data_set),
        ])
        
        # DataSource permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.DATA_SOURCE, self._can_manage_data_source),
            Permission(Action.READ, ResourceType.DATA_SOURCE, self._can_read_data_source),
            Permission(Action.OPERATE, ResourceType.DATA_SOURCE, self._can_operate_data_source),
        ])
        
        # DataSchema permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.DATA_SCHEMA, self._can_manage_data_schema),
            Permission(Action.READ, ResourceType.DATA_SCHEMA, self._can_read_data_schema),
        ])
        
        # DataSink permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.DATA_SINK, self._can_read_data_sink),
            Permission(Action.MANAGE, ResourceType.DATA_SINK, self._can_manage_data_sink),
            Permission(Action.OPERATE, ResourceType.DATA_SINK, self._can_operate_data_sink),
        ])
        
        # DataCredentials permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.DATA_CREDENTIALS, self._can_read_data_credentials),
            Permission(Action.MANAGE, ResourceType.DATA_CREDENTIALS, self._can_manage_data_credentials),
        ])
        
        # DataMap permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.DATA_MAP, self._can_read_data_map),
            Permission(Action.MANAGE, ResourceType.DATA_MAP, self._can_manage_data_map),
        ])
        
        # Notification permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.NOTIFICATION, self._can_read_notification),
            Permission(Action.MANAGE, ResourceType.NOTIFICATION, self._can_manage_notification),
        ])
        
        # NotificationChannelSetting permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.NOTIFICATION_CHANNEL_SETTING, self._can_read_notification_channel_setting),
            Permission(Action.MANAGE, ResourceType.NOTIFICATION_CHANNEL_SETTING, self._can_manage_notification_channel_setting),
        ])
        
        # NotificationSetting permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.NOTIFICATION_SETTING, self._can_read_notification_setting),
            Permission(Action.MANAGE, ResourceType.NOTIFICATION_SETTING, self._can_manage_notification_setting),
        ])
        
        # QuarantineSetting permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.QUARANTINE_SETTING, self._can_read_quarantine_setting),
            Permission(Action.MANAGE, ResourceType.QUARANTINE_SETTING, self._can_manage_quarantine_setting),
        ])
        
        # AttributeTransform permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.ATTRIBUTE_TRANSFORM, self._can_read_attribute_transform),
            Permission(Action.MANAGE, ResourceType.ATTRIBUTE_TRANSFORM, self._can_manage_attribute_transform),
        ])
        
        # Transform permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.TRANSFORM, self._can_read_transform),
            Permission(Action.MANAGE, ResourceType.TRANSFORM, self._can_manage_transform),
        ])
        
        # Validator permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.VALIDATOR, self._can_read_validator),
            Permission(Action.MANAGE, ResourceType.VALIDATOR, self._can_manage_validator),
        ])
        
        # CodeContainer permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.CODE_CONTAINER, self._can_read_code_container),
            Permission(Action.MANAGE, ResourceType.CODE_CONTAINER, self._can_manage_code_container),
        ])
        
        # DocContainer permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.DOC_CONTAINER, self._can_read_doc_container),
            Permission(Action.MANAGE, ResourceType.DOC_CONTAINER, self._can_manage_doc_container),
        ])
        
        # Project permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.PROJECT, self._can_read_project),
            Permission(Action.MANAGE, ResourceType.PROJECT, self._can_manage_project),
        ])
        
        # DataFlow permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.DATA_FLOW, self._can_manage_data_flow),
            Permission(Action.READ, ResourceType.DATA_FLOW, self._can_read_data_flow),
            Permission(Action.OPERATE, ResourceType.DATA_FLOW, self._can_operate_data_flow),
        ])
        
        # DashboardTransform permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.DASHBOARD_TRANSFORM, self._can_read_dashboard_transform),
            Permission(Action.MANAGE, ResourceType.DASHBOARD_TRANSFORM, self._can_manage_dashboard_transform),
        ])
        
        # CustomDataFlow permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.CUSTOM_DATA_FLOW, self._can_read_custom_data_flow),
            Permission(Action.MANAGE, ResourceType.CUSTOM_DATA_FLOW, self._can_manage_custom_data_flow),
            Permission(Action.OPERATE, ResourceType.CUSTOM_DATA_FLOW, self._can_operate_custom_data_flow),
        ])
        
        # UserSetting permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.USER_SETTING, self._can_read_user_setting),
            Permission(Action.MANAGE, ResourceType.USER_SETTING, self._can_manage_user_setting),
        ])
        
        # ClusterEndpoint permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.CLUSTER_ENDPOINT, self._can_read_cluster_endpoint),
            Permission(Action.MANAGE, ResourceType.CLUSTER_ENDPOINT, self._can_manage_cluster_endpoint),
        ])
        
        # DataSetsCatalogRef permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.DATA_SETS_CATALOG_REF, self._can_read_data_sets_catalog_ref),
            Permission(Action.MANAGE, ResourceType.DATA_SETS_CATALOG_REF, self._can_manage_data_sets_catalog_ref),
        ])
        
        # Domain permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.DOMAIN, self._can_manage_domain),
            Permission(Action.READ, ResourceType.DOMAIN, self._can_read_domain),
        ])
        
        # MarketplaceItem permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.MARKETPLACE_ITEM, self._can_manage_marketplace_item),
        ])
        
        # ApprovalStep permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.APPROVAL_STEP, self._can_manage_approval_step),
        ])
        
        # ApprovalRequest permissions
        self.permissions.extend([
            Permission(Action.MANAGE, ResourceType.APPROVAL_REQUEST, self._can_manage_approval_request),
            Permission(Action.READ, ResourceType.APPROVAL_REQUEST, self._can_read_approval_request),
        ])
        
        # AsyncTask permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.ASYNC_TASK, self._can_read_async_task),
            Permission(Action.MANAGE, ResourceType.ASYNC_TASK, self._can_manage_async_task),
        ])
        
        # Runtime permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.RUNTIME, self._can_read_runtime),
            Permission(Action.MANAGE, ResourceType.RUNTIME, self._can_manage_runtime),
        ])
        
        # DataCredentialsGroup permissions
        self.permissions.extend([
            Permission(Action.READ, ResourceType.DATA_CREDENTIALS_GROUP, self._can_read_data_credentials_group),
            Permission(Action.MANAGE, ResourceType.DATA_CREDENTIALS_GROUP, self._can_manage_data_credentials_group),
        ])
    
    def can(self, action: Action, resource_type: ResourceType, resource: Any = None) -> bool:
        """Check if user can perform action on resource type/instance"""
        # Check for matching permissions
        for permission in self.permissions:
            if (permission.action == action or permission.action == Action.MANAGE) and \
               permission.resource_type == resource_type:
                if permission.allows(self.user, resource):
                    return True
        
        return False
    
    def authorize(self, action: Action, resource_type: ResourceType, resource: Any = None) -> None:
        """Authorize action or raise exception"""
        if not self.can(action, resource_type, resource):
            raise PermissionError(
                f"User {self.user.id} cannot {action.value} {resource_type.value}"
            )
    
    # Permission condition methods
    
    def _can_manage_flow_node(self, flow_node, user: User) -> bool:
        """Check if user can manage flow node"""
        return hasattr(flow_node, 'has_admin_access_') and flow_node.has_admin_access_(user)
    
    def _can_operate_flow_node(self, flow_node, user: User) -> bool:
        """Check if user can operate flow node"""
        return hasattr(flow_node, 'has_operator_access_') and flow_node.has_operator_access_(user)
    
    def _can_read_flow_node(self, flow_node, user: User) -> bool:
        """Check if user can read flow node"""
        if hasattr(flow_node, 'is_public_') and flow_node.is_public_():
            return True
        return hasattr(flow_node, 'has_collaborator_access_') and flow_node.has_collaborator_access_(user)
    
    def _can_manage_flow_trigger(self, flow_trigger, user: User) -> bool:
        """Check if user can manage flow trigger"""
        return hasattr(flow_trigger, 'has_admin_access_') and flow_trigger.has_admin_access_(user)
    
    def _can_operate_flow_trigger(self, flow_trigger, user: User) -> bool:
        """Check if user can operate flow trigger"""
        return hasattr(flow_trigger, 'has_operator_access_') and flow_trigger.has_operator_access_(user)
    
    def _can_read_flow_trigger(self, flow_trigger, user: User) -> bool:
        """Check if user can read flow trigger"""
        return hasattr(flow_trigger, 'has_collaborator_access_') and flow_trigger.has_collaborator_access_(user)
    
    def _can_read_catalog_config(self, catalog_config, user: User) -> bool:
        """Check if user can read catalog config"""
        if not hasattr(catalog_config, 'org') or catalog_config.org is None:
            return catalog_config.owner_id == user.id
        
        return user.is_org_member_(catalog_config.org) or user.is_super_user_()
    
    def _can_manage_catalog_config(self, catalog_config, user: User) -> bool:
        """Check if user can manage catalog config"""
        if not hasattr(catalog_config, 'org') or catalog_config.org is None:
            return catalog_config.owner_id == user.id
        
        return hasattr(catalog_config.org, 'has_admin_access_') and catalog_config.org.has_admin_access_(user)
    
    def _can_read_genai_config(self, genai_config, user: User) -> bool:
        """Check if user can read GenAI config"""
        return user.is_org_member_(genai_config.org) or user.is_super_user_()
    
    def _can_manage_genai_config(self, genai_config, user: User) -> bool:
        """Check if user can manage GenAI config"""
        return hasattr(genai_config.org, 'has_admin_access_') and genai_config.org.has_admin_access_(user)
    
    def _is_super_user(self, resource, user: User) -> bool:
        """Check if user is super user"""
        return user.is_super_user_()
    
    def _can_manage_user(self, target_user, user: User) -> bool:
        """Check if user can manage another user"""
        return hasattr(target_user, 'has_admin_access_') and target_user.has_admin_access_(user)
    
    def _can_operate_user(self, target_user, user: User) -> bool:
        """Check if user can operate another user"""
        return hasattr(target_user, 'has_operator_access_') and target_user.has_operator_access_(user)
    
    def _can_read_team(self, team, user: User) -> bool:
        """Check if user can read team"""
        if hasattr(team, 'members') and user in team.members:
            return True
        return hasattr(team, 'has_collaborator_access_') and team.has_collaborator_access_(user)
    
    def _can_manage_team(self, team, user: User) -> bool:
        """Check if user can manage team"""
        return hasattr(team, 'has_admin_access_') and team.has_admin_access_(user)
    
    def _can_manage_org(self, org, user: User) -> bool:
        """Check if user can manage org"""
        return hasattr(org, 'has_admin_access_') and org.has_admin_access_(user)
    
    def _can_read_org(self, org, user: User) -> bool:
        """Check if user can read org"""
        if hasattr(org, 'members') and user in org.members:
            return True
        return hasattr(org, 'has_collaborator_access_') and org.has_collaborator_access_(user)
    
    def _can_operate_org(self, org, user: User) -> bool:
        """Check if user can operate org"""
        return hasattr(org, 'has_operator_access_') and org.has_operator_access_(user)
    
    def _can_manage_org_custodian(self, org_custodian, user: User) -> bool:
        """Check if user can manage org custodian"""
        if hasattr(user, 'org') and hasattr(user.org, 'has_admin_access_'):
            if user.org.has_admin_access_(user):
                return True
        
        if hasattr(user, 'org') and hasattr(user.org, 'is_custodian_'):
            return user.org.is_custodian_(user)
        
        return False
    
    def _can_manage_data_set(self, data_set, user: User) -> bool:
        """Check if user can manage data set"""
        return hasattr(data_set, 'has_admin_access_') and data_set.has_admin_access_(user)
    
    def _can_read_data_set(self, data_set, user: User) -> bool:
        """Check if user can read data set"""
        # Public datasets
        if hasattr(data_set, 'is_public_') and data_set.is_public_():
            return True
        
        # Collaborator access
        if hasattr(data_set, 'has_collaborator_access_') and data_set.has_collaborator_access_(user):
            return True
        
        # Sharer access
        if hasattr(data_set, 'has_sharer_access_') and hasattr(user, 'org'):
            if data_set.has_sharer_access_(user, user.org):
                return True
        
        # Marketplace access
        if hasattr(user, 'org'):
            if hasattr(user.org, 'marketplace_data_sets') and data_set in user.org.marketplace_data_sets:
                return True
            if hasattr(user.org, 'pending_approval_marketplace_data_sets') and \
               data_set in user.org.pending_approval_marketplace_data_sets:
                return True
        
        return False
    
    def _can_transform_data_set(self, data_set, user: User) -> bool:
        """Check if user can transform data set"""
        # Public datasets
        if hasattr(data_set, 'is_public_') and data_set.is_public_():
            return True
        
        # Collaborator access
        if hasattr(data_set, 'has_collaborator_access_') and data_set.has_collaborator_access_(user):
            return True
        
        # Sharer access
        if hasattr(data_set, 'has_sharer_access_') and hasattr(user, 'org'):
            if data_set.has_sharer_access_(user, user.org):
                return True
        
        return False
    
    def _can_operate_data_set(self, data_set, user: User) -> bool:
        """Check if user can operate data set"""
        return hasattr(data_set, 'has_operator_access_') and data_set.has_operator_access_(user)
    
    def _can_manage_data_source(self, data_source, user: User) -> bool:
        """Check if user can manage data source"""
        return hasattr(data_source, 'has_admin_access_') and data_source.has_admin_access_(user)
    
    def _can_read_data_source(self, data_source, user: User) -> bool:
        """Check if user can read data source"""
        return hasattr(data_source, 'has_collaborator_access_') and data_source.has_collaborator_access_(user)
    
    def _can_operate_data_source(self, data_source, user: User) -> bool:
        """Check if user can operate data source"""
        return hasattr(data_source, 'has_operator_access_') and data_source.has_operator_access_(user)
    
    def _can_manage_data_schema(self, data_schema, user: User) -> bool:
        """Check if user can manage data schema"""
        return hasattr(data_schema, 'has_admin_access_') and data_schema.has_admin_access_(user)
    
    def _can_read_data_schema(self, data_schema, user: User) -> bool:
        """Check if user can read data schema"""
        if hasattr(data_schema, 'is_public_') and data_schema.is_public_():
            return True
        return hasattr(data_schema, 'has_collaborator_access_') and data_schema.has_collaborator_access_(user)
    
    def _can_read_data_sink(self, data_sink, user: User) -> bool:
        """Check if user can read data sink"""
        return hasattr(data_sink, 'has_collaborator_access_') and data_sink.has_collaborator_access_(user)
    
    def _can_manage_data_sink(self, data_sink, user: User) -> bool:
        """Check if user can manage data sink"""
        return hasattr(data_sink, 'has_admin_access_') and data_sink.has_admin_access_(user)
    
    def _can_operate_data_sink(self, data_sink, user: User) -> bool:
        """Check if user can operate data sink"""
        return hasattr(data_sink, 'has_operator_access_') and data_sink.has_operator_access_(user)
    
    def _can_read_data_credentials(self, data_credentials, user: User) -> bool:
        """Check if user can read data credentials"""
        return hasattr(data_credentials, 'has_collaborator_access_') and data_credentials.has_collaborator_access_(user)
    
    def _can_manage_data_credentials(self, data_credentials, user: User) -> bool:
        """Check if user can manage data credentials"""
        return hasattr(data_credentials, 'has_admin_access_') and data_credentials.has_admin_access_(user)
    
    def _can_read_data_map(self, data_map, user: User) -> bool:
        """Check if user can read data map"""
        if hasattr(data_map, 'is_public_') and data_map.is_public_():
            return True
        return hasattr(data_map, 'has_collaborator_access_') and data_map.has_collaborator_access_(user)
    
    def _can_manage_data_map(self, data_map, user: User) -> bool:
        """Check if user can manage data map"""
        return hasattr(data_map, 'has_admin_access_') and data_map.has_admin_access_(user)
    
    def _can_read_notification(self, notification, user: User) -> bool:
        """Check if user can read notification"""
        # Direct access
        if hasattr(notification, 'has_collaborator_access_') and notification.has_collaborator_access_(user):
            return True
        
        # Resource access
        if hasattr(notification, 'resource') and notification.resource:
            if hasattr(notification.resource, 'has_collaborator_access_'):
                return notification.resource.has_collaborator_access_(user)
        
        return False
    
    def _can_manage_notification(self, notification, user: User) -> bool:
        """Check if user can manage notification"""
        # Direct access
        if hasattr(notification, 'has_admin_access_') and notification.has_admin_access_(user):
            return True
        
        # Resource access
        if hasattr(notification, 'resource') and notification.resource:
            if hasattr(notification.resource, 'has_admin_access_'):
                return notification.resource.has_admin_access_(user)
        
        return False
    
    def _can_read_notification_channel_setting(self, setting, user: User) -> bool:
        """Check if user can read notification channel setting"""
        return hasattr(setting, 'has_collaborator_access_') and setting.has_collaborator_access_(user)
    
    def _can_manage_notification_channel_setting(self, setting, user: User) -> bool:
        """Check if user can manage notification channel setting"""
        return hasattr(setting, 'has_admin_access_') and setting.has_admin_access_(user)
    
    def _can_read_notification_setting(self, setting, user: User) -> bool:
        """Check if user can read notification setting"""
        return hasattr(setting, 'has_collaborator_access_') and setting.has_collaborator_access_(user)
    
    def _can_manage_notification_setting(self, setting, user: User) -> bool:
        """Check if user can manage notification setting"""
        return hasattr(setting, 'has_admin_access_') and setting.has_admin_access_(user)
    
    def _can_read_quarantine_setting(self, setting, user: User) -> bool:
        """Check if user can read quarantine setting"""
        return hasattr(setting, 'has_collaborator_access_') and setting.has_collaborator_access_(user)
    
    def _can_manage_quarantine_setting(self, setting, user: User) -> bool:
        """Check if user can manage quarantine setting"""
        return hasattr(setting, 'has_admin_access_') and setting.has_admin_access_(user)
    
    def _can_read_attribute_transform(self, transform, user: User) -> bool:
        """Check if user can read attribute transform"""
        if hasattr(transform, 'is_public_') and transform.is_public_():
            return True
        return hasattr(transform, 'has_collaborator_access_') and transform.has_collaborator_access_(user)
    
    def _can_manage_attribute_transform(self, transform, user: User) -> bool:
        """Check if user can manage attribute transform"""
        return hasattr(transform, 'has_admin_access_') and transform.has_admin_access_(user)
    
    def _can_read_transform(self, transform, user: User) -> bool:
        """Check if user can read transform"""
        if hasattr(transform, 'is_public_') and transform.is_public_():
            return True
        return hasattr(transform, 'has_collaborator_access_') and transform.has_collaborator_access_(user)
    
    def _can_manage_transform(self, transform, user: User) -> bool:
        """Check if user can manage transform"""
        return hasattr(transform, 'has_admin_access_') and transform.has_admin_access_(user)
    
    def _can_read_validator(self, validator, user: User) -> bool:
        """Check if user can read validator"""
        if hasattr(validator, 'is_public_') and validator.is_public_():
            return True
        return hasattr(validator, 'has_collaborator_access_') and validator.has_collaborator_access_(user)
    
    def _can_manage_validator(self, validator, user: User) -> bool:
        """Check if user can manage validator"""
        return hasattr(validator, 'has_admin_access_') and validator.has_admin_access_(user)
    
    def _can_read_code_container(self, container, user: User) -> bool:
        """Check if user can read code container"""
        if hasattr(container, 'is_public_') and container.is_public_():
            return True
        return hasattr(container, 'has_collaborator_access_') and container.has_collaborator_access_(user)
    
    def _can_manage_code_container(self, container, user: User) -> bool:
        """Check if user can manage code container"""
        return hasattr(container, 'has_admin_access_') and container.has_admin_access_(user)
    
    def _can_read_doc_container(self, container, user: User) -> bool:
        """Check if user can read doc container"""
        if hasattr(container, 'is_public_') and container.is_public_():
            return True
        return hasattr(container, 'has_collaborator_access_') and container.has_collaborator_access_(user)
    
    def _can_manage_doc_container(self, container, user: User) -> bool:
        """Check if user can manage doc container"""
        return hasattr(container, 'has_admin_access_') and container.has_admin_access_(user)
    
    def _can_read_project(self, project, user: User) -> bool:
        """Check if user can read project"""
        return hasattr(project, 'has_collaborator_access_') and project.has_collaborator_access_(user)
    
    def _can_manage_project(self, project, user: User) -> bool:
        """Check if user can manage project"""
        return hasattr(project, 'has_admin_access_') and project.has_admin_access_(user)
    
    def _can_manage_data_flow(self, data_flow, user: User) -> bool:
        """Check if user can manage data flow"""
        return hasattr(data_flow, 'has_admin_access_') and data_flow.has_admin_access_(user)
    
    def _can_read_data_flow(self, data_flow, user: User) -> bool:
        """Check if user can read data flow"""
        return hasattr(data_flow, 'has_collaborator_access_') and data_flow.has_collaborator_access_(user)
    
    def _can_operate_data_flow(self, data_flow, user: User) -> bool:
        """Check if user can operate data flow"""
        return hasattr(data_flow, 'has_operator_access_') and data_flow.has_operator_access_(user)
    
    def _can_read_dashboard_transform(self, transform, user: User) -> bool:
        """Check if user can read dashboard transform"""
        return hasattr(transform, 'has_collaborator_access_') and transform.has_collaborator_access_(user)
    
    def _can_manage_dashboard_transform(self, transform, user: User) -> bool:
        """Check if user can manage dashboard transform"""
        return hasattr(transform, 'has_admin_access_') and transform.has_admin_access_(user)
    
    def _can_read_custom_data_flow(self, data_flow, user: User) -> bool:
        """Check if user can read custom data flow"""
        return hasattr(data_flow, 'has_collaborator_access_') and data_flow.has_collaborator_access_(user)
    
    def _can_manage_custom_data_flow(self, data_flow, user: User) -> bool:
        """Check if user can manage custom data flow"""
        return hasattr(data_flow, 'has_admin_access_') and data_flow.has_admin_access_(user)
    
    def _can_operate_custom_data_flow(self, data_flow, user: User) -> bool:
        """Check if user can operate custom data flow"""
        return hasattr(data_flow, 'has_operator_access_') and data_flow.has_operator_access_(user)
    
    def _can_read_user_setting(self, user_setting, user: User) -> bool:
        """Check if user can read user setting"""
        # Owner can read their own settings
        if hasattr(user_setting, 'owner_id') and user_setting.owner_id == user.id:
            return True
        
        # Org admin can read org settings
        if hasattr(user_setting, 'org') and user_setting.org is not None:
            if hasattr(user_setting.org, 'has_admin_access_') and user_setting.org.has_admin_access_(user):
                return True
        
        # Super user can read all
        return user.is_super_user_()
    
    def _can_manage_user_setting(self, user_setting, user: User) -> bool:
        """Check if user can manage user setting"""
        # Owner can manage their own settings
        if hasattr(user_setting, 'owner_id') and user_setting.owner_id == user.id:
            return True
        
        # Org admin can manage org settings
        if hasattr(user_setting, 'org') and user_setting.org is not None:
            if hasattr(user_setting.org, 'has_admin_access_') and user_setting.org.has_admin_access_(user):
                return True
        
        # Super user can manage all
        return user.is_super_user_()
    
    def _can_read_cluster_endpoint(self, cluster_endpoint, user: User) -> bool:
        """Check if user can read cluster endpoint"""
        return user.is_super_user_()
    
    def _can_manage_cluster_endpoint(self, cluster_endpoint, user: User) -> bool:
        """Check if user can manage cluster endpoint"""
        return user.is_super_user_()
    
    def _can_read_data_sets_catalog_ref(self, catalog_ref, user: User) -> bool:
        """Check if user can read data sets catalog ref"""
        if hasattr(catalog_ref, 'catalog_config') and hasattr(catalog_ref.catalog_config, 'org'):
            return hasattr(catalog_ref.catalog_config.org, 'has_admin_access_') and catalog_ref.catalog_config.org.has_admin_access_(user)
        return False
    
    def _can_manage_data_sets_catalog_ref(self, catalog_ref, user: User) -> bool:
        """Check if user can manage data sets catalog ref"""
        if hasattr(catalog_ref, 'catalog_config') and hasattr(catalog_ref.catalog_config, 'org'):
            return hasattr(catalog_ref.catalog_config.org, 'has_admin_access_') and catalog_ref.catalog_config.org.has_admin_access_(user)
        return False
    
    def _can_manage_domain(self, domain, user: User) -> bool:
        """Check if user can manage domain"""
        # Org admin access
        if hasattr(domain, 'org') and domain.org is not None:
            if hasattr(domain.org, 'has_admin_access_') and domain.org.has_admin_access_(user):
                return True
        
        # Active custodian access
        if hasattr(domain, 'is_active_custodian_user_') and domain.is_active_custodian_user_(user):
            return True
        
        return False
    
    def _can_read_domain(self, domain, user: User) -> bool:
        """Check if user can read domain"""
        if hasattr(domain, 'org') and hasattr(user, 'org'):
            return domain.org == user.org
        return False
    
    def _can_manage_marketplace_item(self, item, user: User) -> bool:
        """Check if user can manage marketplace item"""
        # Org admin access
        if hasattr(item, 'org') and hasattr(item.org, 'has_admin_access_'):
            if item.org.has_admin_access_(user):
                return True
        
        # Domain custodian access
        if hasattr(item, 'domains'):
            for domain in item.domains:
                if hasattr(domain, 'is_active_custodian_user_') and domain.is_active_custodian_user_(user):
                    return True
        
        return False
    
    def _can_manage_approval_step(self, approval_step, user: User) -> bool:
        """Check if user can manage approval step"""
        if hasattr(approval_step, 'approval_request') and hasattr(approval_step.approval_request, 'topic'):
            # Check if user can manage the topic of the approval request
            topic = approval_step.approval_request.topic
            try:
                topic_resource_type = ResourceType(type(topic).__name__)
                return self.can(Action.MANAGE, topic_resource_type, topic)
            except ValueError:
                # If the topic type is not in ResourceType enum, fallback to checking admin access directly
                return hasattr(topic, 'has_admin_access_') and topic.has_admin_access_(user)
        return False
    
    def _can_manage_approval_request(self, approval_request, user: User) -> bool:
        """Check if user can manage approval request"""
        # Org admin access
        if hasattr(approval_request, 'org') and hasattr(approval_request.org, 'has_admin_access_'):
            if approval_request.org.has_admin_access_(user):
                return True
        
        # Can manage topic
        if hasattr(approval_request, 'topic'):
            topic = approval_request.topic
            try:
                topic_resource_type = ResourceType(type(topic).__name__)
                if self.can(Action.MANAGE, topic_resource_type, topic):
                    return True
            except ValueError:
                # If the topic type is not in ResourceType enum, fallback to checking admin access directly
                if hasattr(topic, 'has_admin_access_') and topic.has_admin_access_(user):
                    return True
        
        # Org custodian access
        if hasattr(approval_request, 'org') and hasattr(user, 'is_org_custodian_'):
            if user.is_org_custodian_(approval_request.org):
                return True
        
        return False
    
    def _can_read_approval_request(self, approval_request, user: User) -> bool:
        """Check if user can read approval request"""
        # Can manage approval request
        if self._can_manage_approval_request(approval_request, user):
            return True
        
        # Is the requestor
        if hasattr(approval_request, 'requestor') and approval_request.requestor == user:
            return True
        
        return False
    
    def _can_read_async_task(self, async_task, user: User) -> bool:
        """Check if user can read async task"""
        # Owner can read their own tasks
        if hasattr(async_task, 'owner_id') and async_task.owner_id == user.id:
            return True
        
        # Org collaborator access
        if hasattr(async_task, 'org') and async_task.org is not None:
            if hasattr(async_task.org, 'has_collaborator_access_') and async_task.org.has_collaborator_access_(user):
                return True
        
        return False
    
    def _can_manage_async_task(self, async_task, user: User) -> bool:
        """Check if user can manage async task"""
        # Owner can manage their own tasks
        if hasattr(async_task, 'owner_id') and async_task.owner_id == user.id:
            return True
        
        # Org admin access
        if hasattr(async_task, 'org') and async_task.org is not None:
            if hasattr(async_task.org, 'has_admin_access_') and async_task.org.has_admin_access_(user):
                return True
        
        return False
    
    def _can_read_runtime(self, runtime, user: User) -> bool:
        """Check if user can read runtime"""
        if hasattr(runtime, 'org') and hasattr(user, 'is_org_member_'):
            return user.is_org_member_(runtime.org)
        return False
    
    def _can_manage_runtime(self, runtime, user: User) -> bool:
        """Check if user can manage runtime"""
        if hasattr(runtime, 'org') and hasattr(runtime.org, 'has_admin_access_'):
            return runtime.org.has_admin_access_(user)
        return False
    
    def _can_read_data_credentials_group(self, group, user: User) -> bool:
        """Check if user can read data credentials group"""
        return hasattr(group, 'has_collaborator_access_') and group.has_collaborator_access_(user)
    
    def _can_manage_data_credentials_group(self, group, user: User) -> bool:
        """Check if user can manage data credentials group"""
        return hasattr(group, 'has_admin_access_') and group.has_admin_access_(user)


class AuthorizationError(Exception):
    """Custom exception for authorization errors"""
    pass