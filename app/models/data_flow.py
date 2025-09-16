from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import json
from enum import Enum

class DataFlow:
    """
    DataFlow orchestrates data flows between sources, transforms, and sinks.
    This is a complex business logic model that handles flow building, access control,
    and data lineage. It does not inherit from SQLAlchemy Base as it's not persisted
    directly but orchestrates relationships between other models.
    
    Rails equivalent: app/models/data_flow.rb
    """
    
    # Rails constants
    RESOURCE_KEYS = ['data_source_id', 'data_set_id', 'data_sink_id']
    
    ACCESSIBLE_KEYS = [
        'code_containers',
        'data_sources', 
        'data_sets',
        'data_sinks',
        'data_credentials'
    ]
    
    class AdminLevel(Enum):
        NONE = "none"
        ORG = "org" 
        SUPER = "super"
    
    def __init__(self, params: Dict[str, Any]):
        """Initialize DataFlow with parameters (Rails pattern)"""
        self.data_set_id = params.get('data_set_id')
        self.data_sets = params.get('data_sets')
        self.data_source_id = params.get('data_source_id')
        self.data_sources = params.get('data_sources')
        self.data_sink_id = params.get('data_sink_id')
        self.data_sinks = params.get('data_sinks')
        self.user = params.get('user')
        self.org = params.get('org')
        self.params = params.copy()
        
        # Initialize state
        self.enabling_acls = None
        self.flows_access_roles = []
        self._admin_level = None
        self._flows = None
        self._flows_options = None
        
        # Initialize resource
        self._init_resource()
    
    @classmethod
    def find(cls, params: Dict[str, Any]) -> 'DataFlow':
        """Find and create DataFlow instance (Rails pattern)"""
        # In Rails this sets current_user and current_org
        # params['user'] = current_user
        # params['org'] = current_org
        
        if params.get('data_flow_id'):
            params['data_set_id'] = params['data_flow_id']
        
        if not any(key in params for key in ['data_source_id', 'data_sink_id', 'data_set_id']):
            raise ValueError("Not found - missing required resource ID")
        
        return cls(params)
    
    @classmethod
    def empty_flows(cls) -> Dict[str, List]:
        """Return empty flows structure (Rails pattern)"""
        return {
            'flows': [],
            'code_containers': [],
            'data_sources': [],
            'data_sets': [],
            'data_sinks': [],
            'data_credentials': [],
            'dependent_data_sources': [],
            'origin_data_sinks': [],
            'shared_data_sets': [],
            'orgs': [],
            'users': [],
            'projects': []
        }
    
    @classmethod
    def empty_cache(cls) -> Dict[str, Dict]:
        """Return empty cache structure (Rails pattern)"""
        return {
            'data_source': {},
            'data_sink': {},
            'data_set': {}
        }
    
    @classmethod
    def merge_flows(cls, flows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple flow structures (Rails pattern)"""
        merged_flows = cls.empty_flows()
        
        for flow in flows:
            for key in merged_flows.keys():
                current = merged_flows[key]
                new_items = flow.get(key, [])
                
                if isinstance(current, list) and isinstance(new_items, list):
                    # Remove duplicates while preserving order
                    seen_ids = {item.get('id') for item in current if isinstance(item, dict) and 'id' in item}
                    for item in new_items:
                        if isinstance(item, dict) and 'id' in item:
                            if item['id'] not in seen_ids:
                                current.append(item)
                                seen_ids.add(item['id'])
                        elif item not in current:
                            current.append(item)
                else:
                    merged_flows[key] = current + new_items
        
        merged_flows['flows'] = cls.merge_common_flow_list(merged_flows['flows'])
        return merged_flows
    
    @classmethod
    def merge_common_flow_list(cls, flows_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge common flows in list (Rails pattern)"""
        flows_hash = {}
        flows_new = []
        
        # Group flows by ID
        for flow in flows_list:
            flow_id = flow.get('id')
            if flow_id is not None:
                if flow_id not in flows_hash:
                    flows_hash[flow_id] = []
                flows_hash[flow_id].append(flow)
        
        # Merge flows with same ID
        for flow_id, flows in flows_hash.items():
            while len(flows) > 1:
                merged = cls.merge_common_flows(flows.pop(0), flows.pop(0))
                if merged:
                    flows.insert(0, merged)
            
            if flows:
                flows_new.append(flows[0])
        
        return flows_new
    
    @classmethod
    def merge_common_flows(cls, flow1: Dict[str, Any], flow2: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Merge two flows with same ID (Rails pattern)"""
        if not isinstance(flow1, dict) or not isinstance(flow2, dict):
            return flow1 if isinstance(flow1, dict) else flow2
        
        if flow1.get('id') != flow2.get('id'):
            return None
        
        merged = flow1.copy()
        
        # Merge children if different
        if merged.get('children', []) != flow2.get('children', []):
            merged_children = merged.get('children', []) + flow2.get('children', [])
            merged['children'] = cls.merge_common_flow_list(merged_children)
        
        return merged
    
    @classmethod 
    def resource_key_model(cls, resource_key: str):
        """Get model class from resource key (Rails pattern)"""
        # This would return the appropriate model class
        # In Rails: resource_key.to_s.gsub("_id", "").camelcase.constantize
        model_name = resource_key.replace('_id', '')
        
        if model_name == 'data_source':
            from .data_source import DataSource
            return DataSource
        elif model_name == 'data_set':
            from .data_set import DataSet  
            return DataSet
        elif model_name == 'data_sink':
            from .data_sink import DataSink
            return DataSink
        
        return None
    
    def _init_resource(self) -> None:
        """Initialize resource based on provided IDs (Rails pattern)"""
        self.data_source = None
        self.data_sink = None
        self.data_set = None
        self.resource_key = None
        self._assoc_where = {}
        
        if self.data_source_id:
            # In real implementation: self.data_source = DataSource.find(self.data_source_id)
            self.resource_key = 'data_source_id'
            self._assoc_where = {'data_source_id': self.data_source_id}
        elif self.data_sink_id:
            # In real implementation: self.data_sink = DataSink.find(self.data_sink_id)  
            self.resource_key = 'data_sink_id'
            self._assoc_where = {'data_sink_id': self.data_sink_id}
        elif self.data_set_id:
            # In real implementation: self.data_set = DataSet.find(self.data_set_id)
            self.resource_key = 'data_set_id'
            self._assoc_where = {'data_set_id': self.data_set_id}
        
        # Initialize access roles if we have a resource with origin_node
        if hasattr(self.resource, 'origin_node') and self.resource.origin_node:
            if hasattr(self.resource.origin_node, 'get_access_roles'):
                self.flows_access_roles = self.resource.origin_node.get_access_roles(
                    self.user, self.org, False
                )
    
    @property
    def resource(self):
        """Get the primary resource for this flow (Rails pattern)"""
        if self.data_source:
            return self.data_source
        elif self.data_sink:
            return self.data_sink
        elif self.data_set:
            return self.data_set
        return None
    
    # Rails access control methods
    def has_admin_access(self, accessor) -> bool:
        """Check if accessor has admin access (Rails pattern)"""
        resource = self.resource
        if not resource or not hasattr(resource, 'origin_node') or not resource.origin_node:
            return self._has_admin_access(accessor)
        
        if hasattr(resource.origin_node, 'has_admin_access'):
            return resource.origin_node.has_admin_access(accessor)
        return False
    
    def has_collaborator_access(self, accessor) -> bool:
        """Check if accessor has collaborator access (Rails pattern)"""
        resource = self.resource
        if not resource or not hasattr(resource, 'origin_node') or not resource.origin_node:
            return self._has_collaborator_access(accessor)
        
        if hasattr(resource.origin_node, 'has_collaborator_access'):
            return resource.origin_node.has_collaborator_access(accessor)
        return False
    
    def _has_admin_access(self, accessor) -> bool:
        """Internal admin access check (Rails pattern)"""
        # This would implement the access control logic
        # For now, return True for infrastructure/super users
        if hasattr(accessor, 'infrastructure_user') and accessor.infrastructure_user():
            return True
        if hasattr(accessor, 'super_user') and accessor.super_user():
            return True
        return False
    
    def _has_collaborator_access(self, accessor) -> bool:
        """Internal collaborator access check (Rails pattern)"""
        # This would implement the access control logic
        return self._has_admin_access(accessor)
    
    # Rails admin level methods
    def init_admin_level(self) -> None:
        """Initialize admin level (Rails pattern)"""
        if self._admin_level is not None:
            return
            
        self._admin_level = self.AdminLevel.NONE
        
        if not self.user or not self.org:
            return
            
        if hasattr(self.user, 'super_user') and self.user.super_user():
            self._admin_level = self.AdminLevel.SUPER
        elif hasattr(self.org, 'has_admin_access') and self.org.has_admin_access(self.user):
            self._admin_level = self.AdminLevel.ORG
    
    @property
    def admin_level(self) -> AdminLevel:
        """Get admin level (Rails pattern)"""
        if self._admin_level is None:
            self.init_admin_level()
        return self._admin_level
    
    def is_admin(self) -> bool:
        """Check if user is admin (Rails pattern)"""
        if self._admin_level is None:
            self.init_admin_level()
        return self._admin_level != self.AdminLevel.NONE
    
    def is_super_user(self) -> bool:
        """Check if user is super user (Rails pattern)"""
        if self._admin_level is None:
            self.init_admin_level()
        return self._admin_level == self.AdminLevel.SUPER
    
    # Rails flow retrieval methods
    def flows(self, downstream_only: bool = False, full_tree: bool = False) -> Dict[str, Any]:
        """Get flows structure (Rails pattern)"""
        flows_options = {
            'downstream_only': downstream_only,
            'full_tree': full_tree
        }
        
        # Use cached flows if options match
        if self._cached_flows_valid(flows_options):
            return self._flows
        
        self._flows_options = flows_options
        
        if self.enabling_acls:
            self._flows = self._flows_from_enabling_acls(downstream_only, full_tree)
            return self._flows
        
        # This is a simplified implementation
        # The real Rails version has complex logic for building flows from various resources
        result = self.empty_flows()
        
        if self.data_sources:
            for data_source in self.data_sources:
                # In real implementation: data_source.flows(...)
                pass
        elif self.data_source:
            # In real implementation: self.data_source.flows(...)
            pass
        elif self.data_sinks:
            for data_sink in self.data_sinks:
                # In real implementation: data_sink.flows(...)
                pass
        elif self.data_sink:
            # In real implementation: self.data_sink.flows(...)
            pass
        elif self.data_set:
            # In real implementation: self.data_set.flows(...)
            pass
        
        # Apply flow metadata
        self._apply_flow_tags(result)
        self._apply_flow_access_roles(result)
        self._apply_flow_docs(result)
        
        self._flows = result
        return self._flows
    
    def _cached_flows_valid(self, flow_options: Dict[str, Any]) -> bool:
        """Check if cached flows are still valid (Rails pattern)"""
        if not self._flows_options:
            return False
        
        for key, value in self._flows_options.items():
            if self._flows_options.get(key) != flow_options.get(key):
                return False
        
        return True
    
    def _flows_from_enabling_acls(self, downstream_only: bool, full_tree: bool) -> Dict[str, Any]:
        """Get flows from enabling ACLs (Rails pattern)"""
        all_flows = []
        
        if self.enabling_acls:
            for acl in self.enabling_acls:
                if hasattr(acl, 'data_flow'):
                    acl_flow = acl.data_flow(self.user, self.org)
                    all_flows.append(acl_flow.flows(downstream_only, full_tree))
        
        return self.merge_flows(all_flows)
    
    # Rails utility methods
    def reload(self) -> None:
        """Reload flow data (Rails pattern)"""
        self._admin_level = None
        self._flows_options = {'reload': True}  # Force reload
        self._flows = None
        self._init_resource()
    
    def contains_resource(self, resource) -> bool:
        """Check if flow contains specific resource (Rails pattern)"""
        current_resource = self.resource
        
        if current_resource and type(current_resource) == type(resource):
            if hasattr(current_resource, 'id') and hasattr(resource, 'id'):
                return current_resource.id == resource.id
        
        flows = self.flows()
        resource_key = type(resource).__name__.lower() + 's'
        
        # Handle special cases
        if resource_key in ['transforms', 'attribute_transforms']:
            resource_key = 'code_containers'
        
        if resource_key in flows:
            return any(r.get('id') == resource.id for r in flows[resource_key] if isinstance(r, dict))
        
        return False
    
    # Rails naming methods
    @property
    def name(self) -> str:
        """Get flow name (Rails pattern)"""
        if self.resource and hasattr(self.resource, 'flow_name'):
            return self.resource.flow_name()
        return f"DataFlow {self.resource_key}"
    
    @property  
    def description(self) -> str:
        """Get flow description (Rails pattern)"""
        if self.resource and hasattr(self.resource, 'flow_description'):
            return self.resource.flow_description()
        return f"Data flow for {self.resource_key}"
    
    @property
    def project(self):
        """Get associated project (Rails pattern)"""
        if self.resource and hasattr(self.resource, 'origin_node') and self.resource.origin_node:
            if hasattr(self.resource.origin_node, 'project'):
                return self.resource.origin_node.project
        return None
    
    # Rails metadata application methods (simplified)
    def _apply_flow_tags(self, flows: Dict[str, Any]) -> None:
        """Apply tags to flow resources (Rails pattern)"""
        # This would apply tags from the tagging system
        # For now, initialize empty tags
        for resource_type in ['data_sources', 'data_sets', 'data_sinks', 'data_credentials']:
            if resource_type in flows:
                for resource in flows[resource_type]:
                    if isinstance(resource, dict):
                        resource['tags'] = resource.get('tags', [])
    
    def _apply_flow_access_roles(self, flows: Dict[str, Any]) -> None:
        """Apply access roles to flow resources (Rails pattern)"""
        # This would apply access control roles
        # For now, initialize basic roles
        for resource_type in ['data_sources', 'data_sets', 'data_sinks', 'data_credentials', 'code_containers']:
            if resource_type in flows:
                for resource in flows[resource_type]:
                    if isinstance(resource, dict):
                        resource['access_roles'] = resource.get('access_roles', [])
    
    def _apply_flow_docs(self, flows: Dict[str, Any]) -> None:
        """Apply documentation to flows (Rails pattern)"""
        # This would apply documentation links
        flows['docs'] = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        return {
            'resource_key': self.resource_key,
            'data_source_id': self.data_source_id,
            'data_set_id': self.data_set_id, 
            'data_sink_id': self.data_sink_id,
            'name': self.name,
            'description': self.description,
            'admin_level': self.admin_level.value,
            'is_admin': self.is_admin(),
            'is_super_user': self.is_super_user(),
            'project': {'id': self.project.id, 'name': self.project.name} if self.project else None
        }