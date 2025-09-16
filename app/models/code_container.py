from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, JSON, Enum, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from enum import Enum as PyEnum
import json
import base64


class CodeContainer(Base):
    """
    CodeContainer manages transformation code, validators, and custom scripts.
    Supports multiple languages (Python, JavaScript, SQL, Jolt), external repos (GitHub), 
    and AI functions. This is a core model for data transformation capabilities.
    
    Rails equivalent: app/models/code_container.rb
    """
    __tablename__ = "code_containers"
    
    # Constants from Rails (API_* constants)
    DESCRIPTION_LIMIT = 255
    
    class EncodingTypes(PyEnum):
        NONE = "none"
        BASE64 = "base64"
    
    class ResourceTypes(PyEnum):
        SOURCE = "source"
        SINK = "sink"
        TRANSFORM = "transform"
        ERROR = "error"
        VALIDATOR = "validator"
        SOURCE_CUSTOM = "source_custom"
        AI_FUNCTION = "ai_function"
        SPLITTER = "splitter"
    
    class CodeLanguages(PyEnum):
        JOLT = "jolt"
        JAVASCRIPT = "javascript"
        JAVASCRIPT_ES6 = "javascript_es6"
        PYTHON = "python"
        PYTHON3 = "python3"
        SQL = "sql"
    
    class CodeTypes(PyEnum):
        JOLT_STANDARD = "jolt_standard"
        JOLT_CUSTOM = "jolt_custom"
        PYTHON = "python"
        PYTHON3 = "python3"
        JAVASCRIPT = "javascript"
        JAVASCRIPT_ES6 = "javascript_es6"
        FLINK_SQL = "flink_sql"
        SPARK_SQL = "spark_sql"
    
    class OutputTypes(PyEnum):
        RECORD = "record"
        ATTRIBUTE = "attribute"
        CUSTOM = "custom"
    
    class RepoTypes(PyEnum):
        GITHUB = "github"
    
    class AiFunctionTypes(PyEnum):
        PARSER = "parser"
        CHUNKER = "chunker"
        POST_PROCESSOR = "post_processor"
        CONTEXT_ENRICHER = "context_enricher"
        QUERY_REWRITER = "query_rewriter"
        RERANKER = "reranker"
    
    # Database columns
    id = Column(Integer, primary_key=True, index=True)
    
    # Basic fields
    name = Column(String(DESCRIPTION_LIMIT), nullable=False)
    description = Column(String(DESCRIPTION_LIMIT))
    
    # Code fields
    code = Column(JSON)  # JSON field for code content
    code_config = Column(JSON)  # JSON field for code configuration
    custom_config = Column(JSON)  # JSON field for custom configuration
    repo_config = Column(JSON)  # JSON field for repository configuration
    
    # Type fields
    resource_type = Column(String(50), nullable=False, default=ResourceTypes.TRANSFORM.value)
    code_type = Column(String(50))
    output_type = Column(String(50), default=OutputTypes.RECORD.value)
    code_encoding = Column(String(50), default=EncodingTypes.NONE.value)
    repo_type = Column(String(50))
    ai_function_type = Column(String(50))
    
    # Boolean fields
    reusable = Column(Boolean, default=False)
    public = Column(Boolean, default=False)
    managed = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"))
    runtime_data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"))
    copied_from_id = Column(Integer, ForeignKey("code_containers.id"))
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    org = relationship("Org")
    data_credentials = relationship("DataCredentials", foreign_keys=[data_credentials_id])
    runtime_data_credentials = relationship("DataCredentials", foreign_keys=[runtime_data_credentials_id])
    copied_from = relationship("CodeContainer", remote_side=[id])
    
    # Reverse relationships (these would be defined in the respective models)
    # data_sets = relationship("DataSet", back_populates="code_container")
    # data_sources = relationship("DataSource", back_populates="code_container")
    # dashboard_transforms = relationship("DashboardTransform", back_populates="code_container")
    
    # Rails class methods
    @classmethod
    def build_from_input(cls, api_user_info: Dict[str, Any], input_data: Dict[str, Any]) -> 'CodeContainer':
        """Build CodeContainer from input data (Rails pattern)"""
        if not input_data or not api_user_info.get('input_owner'):
            raise ValueError("Code container input missing")
        
        # Set ownership
        input_data['owner_id'] = api_user_info['input_owner'].id
        input_data['org_id'] = api_user_info.get('input_org').id if api_user_info.get('input_org') else None
        
        # Validate data credentials access
        if input_data.get('data_credentials_id'):
            # In real implementation: validate access with Ability class
            pass
        
        if input_data.get('runtime_data_credentials_id'):
            # In real implementation: validate access with Ability class
            pass
        
        # Handle tags
        tags = input_data.pop('tags', None)
        
        # Validate AI function requirements
        if input_data.get('resource_type') == cls.ResourceTypes.AI_FUNCTION.value:
            if not input_data.get('ai_function_type'):
                raise ValueError("AI function type is required")
            
            if not input_data.get('output_type'):
                input_data['output_type'] = cls.OutputTypes.CUSTOM.value
        
        # Validate splitter restrictions
        if input_data.get('resource_type') == cls.ResourceTypes.SPLITTER.value:
            raise ValueError("Splitter cannot be created directly. Please create a Nexset with transform.")
        
        # Create instance
        code_container = cls()
        code_container.set_defaults()
        code_container.update_mutable(api_user_info, input_data)
        
        # In real implementation: handle referenced resources, validation, etc.
        
        return code_container
    
    def update_mutable(self, api_user_info: Dict[str, Any], input_data: Dict[str, Any]) -> None:
        """Update mutable fields (Rails pattern)"""
        if not input_data or not api_user_info:
            return
        
        # Check public AI function restrictions
        if self.ai_function_type and self.public:
            if not (input_data.get('public') == False):
                raise ValueError("Public AI functions cannot be updated")
        
        # Handle tags
        tags = input_data.pop('tags', None)
        
        # Update basic fields
        if 'name' in input_data:
            self.name = input_data['name']
        
        if 'description' in input_data:
            self.description = input_data['description']
        
        # Update ownership
        if api_user_info.get('input_owner') and self.owner_id != api_user_info['input_owner'].id:
            self.owner_id = api_user_info['input_owner'].id
        
        if api_user_info.get('input_org') and self.org_id != api_user_info['input_org'].id:
            self.org_id = api_user_info['input_org'].id
        
        # Update credentials with access validation
        if 'data_credentials_id' in input_data:
            # In real implementation: validate access with Ability class
            self.data_credentials_id = input_data['data_credentials_id']
        
        if 'runtime_data_credentials_id' in input_data:
            # In real implementation: validate access with Ability class
            self.runtime_data_credentials_id = input_data['runtime_data_credentials_id']
        
        # Update reusable flag with validation
        if 'reusable' in input_data:
            if self.resource_type == self.ResourceTypes.SPLITTER.value and input_data['reusable']:
                raise ValueError("Splitter transform cannot be reusable")
            self.reusable = bool(input_data['reusable'])
        
        # Update public flag (super user only)
        if 'public' in input_data:
            if not (api_user_info.get('user') and hasattr(api_user_info['user'], 'super_user') 
                   and api_user_info['user'].super_user()):
                raise ValueError("Input cannot include public attribute")
            self.public = bool(input_data['public'])
        
        # Update resource type with validation
        if 'resource_type' in input_data and input_data['resource_type']:
            if (self.resource_type != self.ResourceTypes.SPLITTER.value and 
                input_data['resource_type'] == self.ResourceTypes.SPLITTER.value):
                raise ValueError("Cannot change resource type to splitter")
            self.resource_type = input_data['resource_type']
        
        # Update other fields
        if 'output_type' in input_data and input_data['output_type']:
            self.output_type = input_data['output_type']
        
        if 'code_type' in input_data and input_data['code_type']:
            self.code_type = input_data['code_type']
        
        if 'code_encoding' in input_data and input_data['code_encoding']:
            self.code_encoding = input_data['code_encoding']
        
        if 'code_config' in input_data:
            self.code_config = input_data['code_config']
        
        if 'repo_config' in input_data:
            self.repo_config = input_data['repo_config']
        
        if 'custom_config' in input_data:
            self.custom_config = input_data['custom_config']
        
        if 'code' in input_data:
            self.code = self.parse_code_value(input_data['code'])
        
        if 'repo_type' in input_data:
            self.repo_type = input_data['repo_type']
            if self.repo_type and self.repo_type not in [rt.value for rt in self.RepoTypes]:
                raise ValueError("Invalid repo type")
        
        # In real implementation: validate code, handle external repo updates, etc.
    
    def set_defaults(self) -> None:
        """Set default values (Rails pattern)"""
        if not self.resource_type:
            self.resource_type = self.ResourceTypes.TRANSFORM.value
        
        if not self.output_type:
            self.output_type = self.OutputTypes.RECORD.value
        
        if not self.code_encoding:
            self.code_encoding = self.EncodingTypes.NONE.value
        
        if self.reusable is None:
            self.reusable = False
        
        if self.public is None:
            self.public = False
        
        if self.managed is None:
            self.managed = False
    
    # Rails predicate methods
    def has_external_repo(self) -> bool:
        """Check if code container has external repository (Rails pattern)"""
        return self.repo_type == self.RepoTypes.GITHUB.value
    
    def is_validator(self) -> bool:
        """Check if code container is validator type (Rails pattern)"""
        return self.resource_type == self.ResourceTypes.VALIDATOR.value
    
    def is_output_record(self) -> bool:
        """Check if output type is record (Rails pattern)"""
        return self.output_type == self.OutputTypes.RECORD.value
    
    def is_output_attribute(self) -> bool:
        """Check if output type is attribute (Rails pattern)"""
        return self.output_type == self.OutputTypes.ATTRIBUTE.value
    
    def is_output_custom(self) -> bool:
        """Check if output type is custom (Rails pattern)"""
        return self.output_type == self.OutputTypes.CUSTOM.value
    
    def is_base64(self) -> bool:
        """Check if code encoding is base64 (Rails pattern)"""
        return self.code_encoding == self.EncodingTypes.BASE64.value
    
    def is_jolt(self) -> bool:
        """Check if code type is any Jolt variant (Rails pattern)"""
        return self.is_jolt_standard() or self.is_jolt_custom()
    
    def is_jolt_standard(self) -> bool:
        """Check if code type is Jolt standard (Rails pattern)"""
        return self.code_type == self.CodeTypes.JOLT_STANDARD.value
    
    def is_jolt_custom(self) -> bool:
        """Check if code type is Jolt custom (Rails pattern)"""
        return self.code_type == self.CodeTypes.JOLT_CUSTOM.value
    
    def is_script(self) -> bool:
        """Check if code type is script-based (Rails pattern)"""
        script_types = [
            self.CodeTypes.PYTHON.value,
            self.CodeTypes.PYTHON3.value,
            self.CodeTypes.JAVASCRIPT.value,
            self.CodeTypes.JAVASCRIPT_ES6.value
        ]
        return self.code_type in script_types
    
    def is_sql(self) -> bool:
        """Check if code type is SQL-based (Rails pattern)"""
        sql_types = [
            self.CodeTypes.FLINK_SQL.value,
            self.CodeTypes.SPARK_SQL.value
        ]
        return self.code_type in sql_types
    
    def reusable_container(self) -> bool:
        """Check if container is reusable (Rails pattern)"""
        return self.reusable
    
    def public_container(self) -> bool:
        """Check if container is public (Rails pattern)"""
        return self.public
    
    def managed_container(self) -> bool:
        """Check if container is managed (Rails pattern)"""
        return self.managed
    
    # Rails business logic methods
    def parse_code_value(self, input_code: Any) -> Any:
        """Parse code value based on external repo status (Rails pattern)"""
        return {} if self.has_external_repo() else input_code
    
    def get_code(self) -> Any:
        """Get code with external repo support (Rails pattern)"""
        code = self.code or {}
        
        # In real implementation: handle GitHub integration
        if not code and self.has_external_repo():
            # github_service = GithubService(self)
            # code = github_service.get_code()
            pass
        
        # Handle Jolt custom format
        if self.is_jolt_custom() and not isinstance(code, list):
            code = [{
                "operation": "nexla.custom",
                "spec": {
                    "language": self.code_language(),
                    "encoding": self.code_encoding,
                    "script": code
                }
            }]
        
        return code
    
    def code_language(self) -> Optional[str]:
        """Determine code language from type and repo config (Rails pattern)"""
        language = None
        
        # Check file extension from repo path
        if self.repo_config and isinstance(self.repo_config, dict):
            path = self.repo_config.get('path', '')
            if path:
                extension = path.split('.')[-1] if '.' in path else ''
                
                if extension == 'py':
                    language = self.CodeLanguages.PYTHON.value
                elif extension == 'js':
                    language = self.CodeLanguages.JAVASCRIPT.value
                elif extension == 'sql':
                    language = self.CodeLanguages.SQL.value
        
        # Fallback to code type mapping
        if not language:
            if self.code_type in [self.CodeTypes.JOLT_CUSTOM.value, self.CodeTypes.JOLT_STANDARD.value]:
                language = self.CodeLanguages.JOLT.value
            elif self.code_type in [
                self.CodeTypes.PYTHON.value, self.CodeTypes.PYTHON3.value,
                self.CodeTypes.JAVASCRIPT.value, self.CodeTypes.JAVASCRIPT_ES6.value
            ]:
                language = self.code_type
            elif self.code_type in [self.CodeTypes.FLINK_SQL.value, self.CodeTypes.SPARK_SQL.value]:
                language = self.CodeLanguages.SQL.value
        
        return language
    
    def available_as_reranker(self) -> bool:
        """Check if available as reranker AI function (Rails pattern)"""
        if self.resource_type != self.ResourceTypes.AI_FUNCTION.value:
            return False
        
        if self.ai_function_type != self.AiFunctionTypes.RERANKER.value:
            return False
        
        # In real implementation: check if data_sets is empty
        # return self.data_sets.empty?
        return True
    
    def maybe_destroy(self) -> None:
        """Conditionally destroy if not in use and not reusable (Rails pattern)"""
        # In real implementation: reload and check data_sets usage
        # if self.data_sets.empty? and not self.reusable:
        #     self.destroy()
        pass
    
    # Rails flow integration methods
    def flow_attributes(self, user, org) -> List[tuple]:
        """Get flow attributes for integration (Rails pattern)"""
        return [
            ('data_credentials_id', self.data_credentials_id),
            ('public', self.public),
            ('managed', self.managed),
            ('reusable', self.reusable),
            ('resource_type', self.resource_type),
            ('output_type', self.output_type),
            ('code_type', self.code_type),
            ('code_encoding', self.code_encoding)
        ]
    
    def origin_node_ids(self) -> List[int]:
        """Get origin node IDs (Rails pattern)"""
        # In real implementation: 
        # - Query DataSet.where(code_container_id: self.id).distinct.pluck(:origin_node_id)
        # - Add ResourcesReference.origin_nodes_ids_for(self)
        # - Return flattened unique list
        return []
    
    def origin_nodes(self):
        """Get origin flow nodes (Rails pattern)"""
        # In real implementation: FlowNode.where(id: self.origin_node_ids())
        return []
    
    # Rails tag support
    def tags_list(self) -> List[str]:
        """Get tags as list (Rails pattern)"""
        # In real implementation: self.tags.pluck(:name)
        return []
    
    # Rails validation and error handling
    def set_code_error(self, message: str) -> None:
        """Set code error message (Rails pattern)"""
        if not hasattr(self, '_code_error'):
            self._code_error = ''
        
        if self._code_error:
            self._code_error += '\n'
        
        self._code_error += message
    
    @property
    def code_error(self) -> Optional[str]:
        """Get code error messages (Rails pattern)"""
        return getattr(self, '_code_error', None)
    
    # Rails API response method
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'resource_type': self.resource_type,
            'code_type': self.code_type,
            'output_type': self.output_type,
            'code_encoding': self.code_encoding,
            'repo_type': self.repo_type,
            'ai_function_type': self.ai_function_type,
            'reusable': self.reusable,
            'public': self.public,
            'managed': self.managed,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'data_credentials_id': self.data_credentials_id,
            'runtime_data_credentials_id': self.runtime_data_credentials_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            # Rails predicate methods in response
            'has_external_repo': self.has_external_repo(),
            'is_validator': self.is_validator(),
            'is_output_record': self.is_output_record(),
            'is_output_attribute': self.is_output_attribute(),
            'is_output_custom': self.is_output_custom(),
            'is_base64': self.is_base64(),
            'is_jolt': self.is_jolt(),
            'is_jolt_standard': self.is_jolt_standard(),
            'is_jolt_custom': self.is_jolt_custom(),
            'is_script': self.is_script(),
            'is_sql': self.is_sql(),
            'reusable_container': self.reusable_container(),
            'public_container': self.public_container(),
            'managed_container': self.managed_container(),
            'available_as_reranker': self.available_as_reranker(),
            'code_language': self.code_language(),
            'tags': self.tags_list()
        }