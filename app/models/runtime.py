from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, Enum as SQLEnum, JSON
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import json

from app.database import Base

class RuntimeStatuses(PyEnum):
    ACTIVE = "active"
    DEACTIVATED = "deactivated"

class Runtime(Base):
    __tablename__ = 'runtimes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    data_credentials_id = Column(Integer, ForeignKey('data_credentials.id'))
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    dockerpath = Column(String(500), nullable=False)
    
    active = Column(Boolean, default=False)
    managed = Column(Boolean, default=False)
    status = Column(SQLEnum(RuntimeStatuses), default=RuntimeStatuses.DEACTIVATED)
    
    config = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id], back_populates="runtimes")
    org = relationship("Org", foreign_keys=[org_id], back_populates="runtimes")
    data_credentials = relationship("DataCredentials", foreign_keys=[data_credentials_id], back_populates="runtimes")
    
    # Constants
    DEFAULT_STATUS = RuntimeStatuses.DEACTIVATED
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.status:
            self.status = self.DEFAULT_STATUS
    
    @validates('name')
    def validate_name(self, key, name):
        if not name or not name.strip():
            raise ValueError("Name is required")
        return name.strip()
    
    @validates('dockerpath')
    def validate_dockerpath(self, key, dockerpath):
        if not dockerpath or not dockerpath.strip():
            raise ValueError("Docker path is required")
        return dockerpath.strip()
    
    @classmethod
    def build_from_input(cls, user, org, input_data: Dict[str, Any]) -> 'Runtime':
        """Factory method to create runtime from input data"""
        if not input_data.get('name'):
            raise ValueError("Name is required")
        
        runtime = cls(
            owner_id=user.id,
            org_id=org.id
        )
        runtime.update_mutable_(user, input_data)
        return runtime
    
    def update_mutable_(self, user, input_data: Dict[str, Any]) -> None:
        """Update mutable attributes"""
        # Basic attributes
        if 'name' in input_data:
            self.name = input_data['name']
        if 'description' in input_data:
            self.description = input_data['description']
        if 'active' in input_data:
            self.active = bool(input_data['active'])
        if 'dockerpath' in input_data and input_data['dockerpath']:
            self.dockerpath = input_data['dockerpath']
        if 'managed' in input_data:
            self.managed = bool(input_data['managed'])
        if 'config' in input_data:
            self.config = input_data['config']
        
        # Data credentials validation
        if 'data_credentials_id' in input_data:
            data_creds_id = input_data['data_credentials_id']
            if data_creds_id:
                from app.models.data_credentials import DataCredentials
                data_credentials = DataCredentials.query.get(data_creds_id)
                
                if not data_credentials:
                    raise ValueError("Data credentials not found")
                
                if not data_credentials.has_collaborator_access_(user):
                    raise ValueError("Data credentials not accessible")
                
                self.data_credentials_id = data_credentials.id
            else:
                self.data_credentials_id = None
        
        # Update status based on active flag
        if hasattr(self, 'active') and self.active is not None:
            self.status = RuntimeStatuses.ACTIVE if self.active else RuntimeStatuses.DEACTIVATED
    
    # Predicate methods (Rails pattern)
    def active_(self) -> bool:
        """Check if runtime is active"""
        return self.active is True
    
    def deactivated_(self) -> bool:
        """Check if runtime is deactivated"""
        return not self.active_()
    
    def managed_(self) -> bool:
        """Check if runtime is managed"""
        return self.managed is True
    
    def unmanaged_(self) -> bool:
        """Check if runtime is unmanaged"""
        return not self.managed_()
    
    def has_config_(self) -> bool:
        """Check if runtime has configuration"""
        return bool(self.config)
    
    def has_data_credentials_(self) -> bool:
        """Check if runtime has data credentials"""
        return self.data_credentials_id is not None
    
    def has_description_(self) -> bool:
        """Check if runtime has description"""
        return bool(self.description and self.description.strip())
    
    def docker_runtime_(self) -> bool:
        """Check if this is a Docker runtime"""
        return bool(self.dockerpath)
    
    def accessible_by_user_(self, user) -> bool:
        """Check if runtime is accessible by user"""
        # Owner has full access
        if self.owner_id == user.id:
            return True
        
        # Org admins have access
        if self.org.has_admin_access_(user):
            return True
        
        # Check if user has collaborator access through data credentials
        if self.data_credentials and self.data_credentials.has_collaborator_access_(user):
            return True
        
        return False
    
    def manageable_by_user_(self, user) -> bool:
        """Check if runtime can be managed by user"""
        # Owner can manage
        if self.owner_id == user.id:
            return True
        
        # Org admins can manage
        if self.org.has_admin_access_(user):
            return True
        
        return False
    
    def ready_for_execution_(self) -> bool:
        """Check if runtime is ready for execution"""
        return (self.active_() and 
                self.dockerpath and 
                self.has_data_credentials_())
    
    def configuration_complete_(self) -> bool:
        """Check if runtime configuration is complete"""
        return (bool(self.name) and 
                bool(self.dockerpath) and
                self.has_data_credentials_())
    
    def configuration_incomplete_(self) -> bool:
        """Check if runtime configuration is incomplete"""
        return not self.configuration_complete_()
    
    # State management methods (Rails pattern)
    def activate_(self) -> None:
        """Activate the runtime"""
        if not self.active_():
            self.active = True
            self.status = RuntimeStatuses.ACTIVE
            # In production: self.save()
    
    def deactivate_(self) -> None:
        """Deactivate the runtime"""
        if self.active_():
            self.active = False
            self.status = RuntimeStatuses.DEACTIVATED
            # In production: self.save()
    
    def toggle_active_(self) -> None:
        """Toggle active state"""
        if self.active_():
            self.deactivate_()
        else:
            self.activate_()
    
    def enable_management_(self) -> None:
        """Enable management for the runtime"""
        if not self.managed_():
            self.managed = True
            # In production: self.save()
    
    def disable_management_(self) -> None:
        """Disable management for the runtime"""
        if self.managed_():
            self.managed = False
            # In production: self.save()
    
    def clear_config_(self) -> None:
        """Clear runtime configuration"""
        self.config = None
        # In production: self.save()
    
    def update_config_(self, new_config: Dict[str, Any]) -> None:
        """Update runtime configuration"""
        self.config = new_config
        # In production: self.save()
    
    def merge_config_(self, additional_config: Dict[str, Any]) -> None:
        """Merge additional configuration"""
        if self.config is None:
            self.config = {}
        
        if isinstance(self.config, dict) and isinstance(additional_config, dict):
            self.config.update(additional_config)
        else:
            self.config = additional_config
        # In production: self.save()
    
    # Configuration methods
    def get_config_value(self, key: str, default=None):
        """Get a configuration value"""
        if not self.config or not isinstance(self.config, dict):
            return default
        return self.config.get(key, default)
    
    def set_config_value(self, key: str, value: Any) -> None:
        """Set a configuration value"""
        if self.config is None:
            self.config = {}
        elif not isinstance(self.config, dict):
            self.config = {}
        
        self.config[key] = value
        # In production: self.save()
    
    def remove_config_value(self, key: str) -> None:
        """Remove a configuration value"""
        if self.config and isinstance(self.config, dict):
            self.config.pop(key, None)
            # In production: self.save()
    
    # Docker methods
    def docker_image(self) -> Optional[str]:
        """Extract Docker image from dockerpath"""
        if not self.dockerpath:
            return None
        
        # Handle different Docker path formats
        if ':' in self.dockerpath:
            return self.dockerpath.split(':')[0]
        return self.dockerpath
    
    def docker_tag(self) -> Optional[str]:
        """Extract Docker tag from dockerpath"""
        if not self.dockerpath or ':' not in self.dockerpath:
            return 'latest'
        
        return self.dockerpath.split(':', 1)[1]
    
    def docker_registry(self) -> Optional[str]:
        """Extract Docker registry from dockerpath"""
        if not self.dockerpath:
            return None
        
        image = self.docker_image()
        if not image or '/' not in image:
            return 'docker.io'  # Default Docker Hub
        
        parts = image.split('/')
        if '.' in parts[0] or ':' in parts[0]:  # Has registry
            return parts[0]
        
        return 'docker.io'
    
    def full_docker_path(self) -> str:
        """Get full Docker path with registry"""
        if not self.dockerpath:
            return ""
        
        registry = self.docker_registry()
        image = self.docker_image()
        tag = self.docker_tag()
        
        if registry == 'docker.io' and '/' not in image:
            return f"library/{image}:{tag}"
        
        return self.dockerpath
    
    # Display methods
    def status_display(self) -> str:
        """Get human-readable status"""
        status_map = {
            RuntimeStatuses.ACTIVE: "Active",
            RuntimeStatuses.DEACTIVATED: "Deactivated"
        }
        return status_map.get(self.status, self.status.value)
    
    def management_display(self) -> str:
        """Get management status display"""
        return "Managed" if self.managed_() else "Unmanaged"
    
    def config_summary(self) -> str:
        """Get configuration summary"""
        if not self.has_config_():
            return "No configuration"
        
        if isinstance(self.config, dict):
            count = len(self.config)
            return f"{count} configuration {'item' if count == 1 else 'items'}"
        
        return "Custom configuration"
    
    def credentials_summary(self) -> str:
        """Get credentials summary"""
        if not self.has_data_credentials_():
            return "No credentials"
        
        if self.data_credentials:
            return f"Credentials: {self.data_credentials.name}"
        
        return "Credentials configured"
    
    def runtime_summary(self) -> str:
        """Get complete runtime summary"""
        parts = [
            self.status_display(),
            self.management_display(),
            f"Docker: {self.docker_image()}:{self.docker_tag()}"
        ]
        
        if self.has_data_credentials_():
            parts.append("Credentials: Yes")
        
        if self.has_config_():
            parts.append(self.config_summary())
        
        return " | ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'data_credentials_id': self.data_credentials_id,
            'name': self.name,
            'description': self.description,
            'dockerpath': self.dockerpath,
            'active': self.active,
            'managed': self.managed,
            'status': self.status.value if self.status else None,
            'config': self.config,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Docker information
            'docker_image': self.docker_image(),
            'docker_tag': self.docker_tag(),
            'docker_registry': self.docker_registry(),
            'full_docker_path': self.full_docker_path(),
            
            # Predicate methods
            'active_status': self.active_(),
            'deactivated': self.deactivated_(),
            'managed_runtime': self.managed_(),
            'unmanaged': self.unmanaged_(),
            'has_config': self.has_config_(),
            'has_data_credentials': self.has_data_credentials_(),
            'has_description': self.has_description_(),
            'docker_runtime': self.docker_runtime_(),
            'ready_for_execution': self.ready_for_execution_(),
            'configuration_complete': self.configuration_complete_(),
            'configuration_incomplete': self.configuration_incomplete_(),
            
            # Display values
            'status_display': self.status_display(),
            'management_display': self.management_display(),
            'config_summary': self.config_summary(),
            'credentials_summary': self.credentials_summary(),
            'runtime_summary': self.runtime_summary()
        }