from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import json


class DataMap(Base):
    """
    DataMap handles static and dynamic data mapping/lookup tables.
    Manages key-value mappings with validation, versioning, and transform integration.
    Supports both static data maps (stored locally) and dynamic data maps (from data sinks).
    
    Rails equivalent: app/models/data_map.rb
    """
    __tablename__ = "data_maps"
    
    # Rails constants
    SCHEMA_SAMPLE_SIZE = 20
    
    # Database columns
    id = Column(Integer, primary_key=True, index=True)
    
    # Basic fields
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Data configuration
    data_type = Column(String(100))
    data_format = Column(String(100))
    map_primary_key = Column(String(100))
    map_entry_count = Column(Integer, default=0)
    
    # JSON fields for data storage
    data_map = Column(JSON)  # Array of key-value entries
    data_defaults = Column(JSON)  # Default values to merge
    map_entry_schema = Column(JSON)  # Schema for map entries
    
    # Boolean configuration
    emit_data_default = Column(Boolean, default=False)
    use_versioning = Column(Boolean, default=False)
    public = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    data_sink_id = Column(Integer, ForeignKey("data_sinks.id"))  # For dynamic maps
    copied_from_id = Column(Integer, ForeignKey("data_maps.id"))
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    org = relationship("Org")
    data_sink = relationship("DataSink")
    copied_from = relationship("DataMap", remote_side=[id])
    
    # Rails class methods
    @classmethod
    def build_from_input(cls, api_user_info: Dict[str, Any], input_data: Dict[str, Any]) -> 'DataMap':
        """Build DataMap from input data (Rails pattern)"""
        data_map = cls()
        data_map.update_mutable(api_user_info, input_data)
        return data_map
    
    def set_defaults(self, user, org) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        self.org = org
        
        if self.emit_data_default is None:
            self.emit_data_default = False
        
        if self.use_versioning is None:
            self.use_versioning = False
        
        if self.public is None:
            self.public = False
        
        if self.map_entry_count is None:
            self.map_entry_count = 0
    
    def update_mutable(self, api_user_info: Dict[str, Any], input_data: Dict[str, Any]) -> None:
        """Update mutable fields (Rails pattern)"""
        if not input_data:
            return
        
        # Handle tags (would integrate with tagging system)
        tags = input_data.pop('tags', None)
        
        # Update basic fields
        if 'name' in input_data and input_data['name']:
            self.name = input_data['name']
        
        if 'description' in input_data:
            self.description = input_data['description']
        
        # Update public flag (super user only)
        if 'public' in input_data:
            if not (api_user_info.get('user') and hasattr(api_user_info['user'], 'super_user') 
                   and api_user_info['user'].super_user()):
                raise ValueError("Input cannot include public attribute")
            self.public = bool(input_data['public'])
        
        # Update configuration fields
        if 'data_type' in input_data and input_data['data_type']:
            self.data_type = input_data['data_type']
        
        if 'data_format' in input_data:
            self.data_format = input_data['data_format']
        
        if 'emit_data_default' in input_data:
            self.emit_data_default = bool(input_data['emit_data_default'])
        
        if 'map_primary_key' in input_data:
            self.map_primary_key = input_data['map_primary_key']
        
        if 'use_versioning' in input_data:
            self.use_versioning = bool(input_data['use_versioning'])
        
        # Update ownership
        if api_user_info.get('input_owner') and self.owner_id != api_user_info['input_owner'].id:
            self.owner_id = api_user_info['input_owner'].id
        
        if api_user_info.get('input_org') and self.org_id != api_user_info['input_org'].id:
            self.org_id = api_user_info['input_org'].id
        
        # Handle data sink assignment (creates dynamic data map)
        if 'data_sink_id' in input_data:
            # In real implementation: validate access with Ability class
            self.data_sink_id = input_data['data_sink_id']
            # Dynamic data maps don't support versioning initially
            self.use_versioning = False
            self.data_map = None
        
        # Handle data defaults
        if 'data_defaults' in input_data:
            self.data_defaults = None
            
            if isinstance(input_data['data_defaults'], str):
                self.data_defaults = {
                    'key': 'unknown',
                    'value': input_data['data_defaults']
                }
            elif isinstance(input_data['data_defaults'], dict):
                self.data_defaults = input_data['data_defaults']
        
        # Handle data map entries (only for static maps)
        if 'data_map' in input_data and not self.data_sink_id:
            self.data_map = None
            
            if isinstance(input_data['data_map'], dict):
                # Convert hash to array format
                tmp = []
                for key, value in input_data['data_map'].items():
                    tmp.append({'key': key, 'value': value})
                self.data_map = tmp
                self.map_primary_key = 'key'
            elif isinstance(input_data['data_map'], list):
                self.data_map = input_data['data_map']
            
            # Validate primary key presence for static maps
            if self.static_data_map() and self.data_map:
                self.validate_primary_key_presence(self.data_map)
        
        # Validate primary key requirement for static maps
        if self.static_data_map() and not self.map_primary_key:
            raise ValueError("Map should have a primary key")
        
        # In real implementation: save and handle tags
        # self.save()
        # ResourceTagging.add_owned_tags(self, {'tags': tags}, api_user_info['input_owner'])
    
    # Rails validation methods
    def validate_primary_key_uniqueness(self, entries: List[Dict[str, Any]]) -> None:
        """Validate primary key uniqueness (Rails pattern)"""
        if not self.map_primary_key:
            return
        
        seen_values = set()
        
        for row in entries:
            pk_value = row.get(self.map_primary_key)
            if pk_value in seen_values:
                raise ValueError("Primary key should have unique values")
            seen_values.add(pk_value)
    
    def validate_primary_key_presence(self, entries: List[Dict[str, Any]]) -> None:
        """Validate primary key presence (Rails pattern)"""
        if not self.map_primary_key:
            return
        
        for row in entries:
            pk_value = row.get(self.map_primary_key)
            if not pk_value or (isinstance(pk_value, str) and pk_value.strip() == ''):
                raise ValueError("DataMap entries primary key should not be empty")
    
    # Rails entry management methods
    def set_map_entries(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Set map entries with validation (Rails pattern)"""
        entries = input_data.get('entries') or input_data.get('data_map')
        
        if not entries:
            raise ValueError("No valid entries in request")
        
        # Convert hash format to array
        if isinstance(entries, dict):
            tmp = []
            for key, value in entries.items():
                if isinstance(value, dict):
                    tmp.append(value)
                else:
                    tmp.append({'key': key, 'value': value})
            entries = tmp
        
        # Convert all values to strings
        for row in entries:
            for key, value in row.items():
                row[key] = str(value)
        
        # Handle static data maps
        if self.static_data_map():
            current_map = self.data_map or []
            self.validate_primary_key_presence(entries)
            self.validate_primary_key_uniqueness(entries)
            
            # Remove existing entries with same primary keys
            for entry in entries:
                pk_value = entry.get(self.map_primary_key)
                current_map = [e for e in current_map if e.get(self.map_primary_key) != pk_value]
            
            # Add new entries
            for entry in entries:
                current_map.append(entry)
            
            self._skip_refresh = True
            self.data_map = current_map
            # In real implementation: self.save()
        
        # In real implementation: return TransformService.new().set_map_entries(self, entries)
        return {'status': 'success', 'entries_added': len(entries)}
    
    def delete_map_entries(self, keys: Union[str, List[str]], use_post: bool = True) -> Dict[str, Any]:
        """Delete map entries by keys (Rails pattern)"""
        if not isinstance(keys, (str, list)):
            raise ValueError("Invalid entry key format")
        
        # Parse key list
        if isinstance(keys, str):
            # Remove quotes and split by comma
            keys = keys.strip('\'"')
            key_list = [k.strip() for k in keys.split(',')]
        else:
            key_list = [k.strip() if hasattr(k, 'strip') else str(k) for k in keys]
        
        if not key_list:
            raise ValueError("Invalid entry keys")
        
        # Handle static data maps
        if self.static_data_map():
            updated_map = self.data_map or []
            
            # Remove entries matching the keys
            for key in key_list:
                updated_map = [
                    e for e in updated_map 
                    if str(e.get(self.map_primary_key, '')).strip() != str(key)
                ]
            
            self._skip_refresh = True
            self.data_map = updated_map
            # In real implementation: self.save()
        
        # In real implementation: call TransformService
        return {'status': 'success', 'keys_deleted': key_list}
    
    # Rails predicate methods
    def static_data_map(self) -> bool:
        """Check if this is a static data map (Rails pattern)"""
        return self.data_sink_id is None
    
    def dynamic_data_map(self) -> bool:
        """Check if this is a dynamic data map (Rails pattern)"""
        return self.data_sink is not None
    
    def public_data_map(self) -> bool:
        """Check if data map is public (Rails pattern)"""
        return self.public
    
    def versioned_data_map(self) -> bool:
        """Check if data map uses versioning (Rails pattern)"""
        return self.use_versioning
    
    def has_data_defaults(self) -> bool:
        """Check if data map has default values (Rails pattern)"""
        return self.data_defaults is not None and bool(self.data_defaults)
    
    def emits_defaults(self) -> bool:
        """Check if data map emits default values (Rails pattern)"""
        return self.emit_data_default
    
    # Rails data access methods
    def get_map_entry_schema(self) -> Optional[Dict[str, Any]]:
        """Get schema for map entries (Rails pattern)"""
        if self.dynamic_data_map():
            # In real implementation: return self.data_sink.data_set.output_schema
            return None
        elif not self.data_map or len(self.data_map) == 0:
            return None
        else:
            if self.map_entry_schema:
                return self.map_entry_schema
            else:
                # In real implementation: call TransformService.accumulate_schema
                # For now, return basic schema from first entry
                sample_data = self.data_map[:min(100, len(self.data_map))]
                return self._generate_basic_schema(sample_data)
    
    def _generate_basic_schema(self, sample_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate basic schema from sample data (Rails pattern)"""
        if not sample_data:
            return {}
        
        # Simple schema generation
        schema = {}
        for entry in sample_data:
            for key, value in entry.items():
                if key not in schema:
                    schema[key] = {'type': type(value).__name__, 'examples': []}
                
                if len(schema[key]['examples']) < 5:
                    schema[key]['examples'].append(value)
        
        return schema
    
    @property
    def data_set(self):
        """Get associated data set (Rails pattern)"""
        if self.data_sink and hasattr(self.data_sink, 'data_set'):
            return self.data_sink.data_set
        return None
    
    def get_map_entry_count(self, get_dynamic_count: bool = False) -> int:
        """Get map entry count (Rails pattern)"""
        if self.static_data_map():
            # Return stored count or calculate from data
            if self.map_entry_count is not None:
                return self.map_entry_count
            else:
                return len(self.data_map) if self.data_map else 0
        else:
            # For dynamic maps, only get count if explicitly requested
            if get_dynamic_count:
                # In real implementation: call get_map_validation()['cached_entry_count']
                return 0
            else:
                return 0
    
    def get_map_validation(self) -> Dict[str, Any]:
        """Get map validation status (Rails pattern)"""
        result = {
            'cached': False,
            'cached_entry_count': 0,
            'static_entry_count': 0
        }
        
        if self.static_data_map():
            result['static_entry_count'] = len(self.data_map) if self.data_map else 0
        
        # In real implementation: call TransformService.validate_data_map
        # For now, return basic validation
        result['cached'] = True
        result['cached_entry_count'] = self.get_map_entry_count()
        
        return result
    
    def encrypted_credentials(self) -> Dict[str, Any]:
        """Get encrypted credentials (Rails pattern)"""
        return {
            'credsEnc': '',
            'credsEncIv': '',
            'credsId': 1
        }
    
    def apply_default_values(self, data_map_sample: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply default values to data map sample (Rails pattern)"""
        if not self.data_defaults:
            return data_map_sample
        
        return [
            {**self.data_defaults, **row}
            for row in data_map_sample
        ]
    
    # Rails origin node methods
    def origin_node_ids(self) -> List[int]:
        """Get origin node IDs (Rails pattern)"""
        # In real implementation: ResourcesReference.origin_nodes_ids_for(self)
        return []
    
    def origin_nodes(self):
        """Get origin nodes (Rails pattern)"""
        # In real implementation: FlowNode.where(id: self.origin_node_ids())
        return []
    
    # Rails tag support
    def tags_list(self) -> List[str]:
        """Get tags as list (Rails pattern)"""
        # In real implementation: self.tags.pluck(:name)
        return []
    
    # Rails lifecycle methods
    def handle_before_save(self) -> None:
        """Handle before save logic (Rails pattern)"""
        if self.static_data_map():
            if not self.data_map:
                self.map_entry_count = 0
            else:
                self.map_entry_count = len(self.data_map)
                
                # Generate schema from sample data
                sample_size = min(self.SCHEMA_SAMPLE_SIZE, len(self.data_map))
                sample_data = self.apply_default_values(self.data_map[:sample_size])
                
                # In real implementation: call TransformService.accumulate_schema
                schema_result = self._generate_basic_schema(sample_data)
                
                if schema_result:
                    self.map_entry_schema = schema_result
    
    def after_commit(self) -> None:
        """Handle after commit logic (Rails pattern)"""
        if not getattr(self, '_skip_refresh', False):
            # In real implementation: TransformService.new().refresh_data_map(self)
            pass
    
    # Rails copy method
    def copy_pre_save(self, original_data_map, api_user_info: Dict[str, Any], options: Dict[str, Any]) -> None:
        """Handle pre-save logic for copying (Rails pattern)"""
        # When copying, remove data sink reference to make it static
        self.data_sink_id = None
    
    # Rails API response method
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'data_type': self.data_type,
            'data_format': self.data_format,
            'map_primary_key': self.map_primary_key,
            'map_entry_count': self.map_entry_count,
            'emit_data_default': self.emit_data_default,
            'use_versioning': self.use_versioning,
            'public': self.public,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'data_sink_id': self.data_sink_id,
            'copied_from_id': self.copied_from_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            # Include data if static and not too large
            'data_map': self.data_map if self.static_data_map() and self.get_map_entry_count() < 1000 else None,
            'data_defaults': self.data_defaults,
            'map_entry_schema': self.map_entry_schema,
            # Rails predicate methods in response
            'static_data_map': self.static_data_map(),
            'dynamic_data_map': self.dynamic_data_map(),
            'public_data_map': self.public_data_map(),
            'versioned_data_map': self.versioned_data_map(),
            'has_data_defaults': self.has_data_defaults(),
            'emits_defaults': self.emits_defaults(),
            'entry_count': self.get_map_entry_count(),
            'tags': self.tags_list()
        }