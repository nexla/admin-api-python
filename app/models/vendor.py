from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, JSON, ForeignKey
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
import json

from app.database import Base

class Vendor(Base):
    __tablename__ = "vendors"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    display_name = Column(String(255))
    description = Column(Text)
    small_logo = Column(String(500))
    logo = Column(String(500))
    
    is_active = Column(Boolean, default=True)
    config = Column(JSON)
    
    connector_id = Column(Integer, ForeignKey("connectors.id"), nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    connector = relationship("Connector", foreign_keys=[connector_id], back_populates="vendors")
    auth_templates = relationship("AuthTemplate", foreign_keys="AuthTemplate.vendor_id", back_populates="vendor", cascade="all, delete-orphan")
    vendor_endpoints = relationship("VendorEndpoint", foreign_keys="VendorEndpoint.vendor_id", back_populates="vendor", cascade="all, delete-orphan")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    @validates('name')
    def validate_name(self, key, name):
        if not name or not name.strip():
            raise ValueError("Name is required")
        return name.strip()
    
    @validates('connector_id')
    def validate_connector_id(self, key, connector_id):
        if not connector_id:
            raise ValueError("Connector is required")
        return connector_id
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any]) -> 'Vendor':
        """Factory method to create vendor from input data with auth templates"""
        if not input_data.get('name'):
            raise ValueError("Name is required")
        
        # Extract auth templates before creating vendor
        auth_templates_data = input_data.pop('auth_templates', [])
        connection_type = input_data.pop('connection_type', None)
        
        if not connection_type:
            raise ValueError("Connection type is required")
        
        # Find connector by type
        from app.models.connector import Connector
        connector = Connector.query.filter_by(connector_type=connection_type).first()
        if not connector:
            raise ValueError("Invalid connection type")
        
        # Validate auth templates against connector
        non_empty_templates = [t for t in auth_templates_data if t]
        if non_empty_templates:
            cls.validate_auth_templates_(non_empty_templates, connector)
        
        vendor = cls(
            name=input_data['name'],
            display_name=input_data.get('display_name'),
            description=input_data.get('description'),
            small_logo=input_data.get('small_logo'),
            logo=input_data.get('logo'),
            config=input_data.get('config'),
            connector_id=connector.id
        )
        
        # Build auth templates
        for template_data in non_empty_templates:
            template_data['connector'] = connector
            auth_params = template_data.pop('params', [])
            
            from app.models.auth_template import AuthTemplate
            auth_template = AuthTemplate(**template_data)
            vendor.auth_templates.append(auth_template)
            
            # Build auth parameters
            for param_data in auth_params:
                param_data['vendor_id'] = vendor.id
                param_data['auth_template_id'] = auth_template.id
                # auth_template.auth_parameters.append(AuthParameter(**param_data))
        
        return vendor
    
    def update_mutable_(self, input_data: Dict[str, Any]) -> None:
        """Update mutable attributes"""
        if not input_data:
            return
        
        # Connection type cannot be changed
        if 'connection_type' in input_data and input_data['connection_type']:
            raise ValueError("Cannot change connection type once it is set")
        
        # Extract auth templates
        auth_templates_data = input_data.pop('auth_templates', [])
        non_empty_templates = [t for t in auth_templates_data if t]
        
        if non_empty_templates:
            self.validate_auth_templates_(non_empty_templates, self.connector)
        
        # Update basic attributes
        if 'display_name' in input_data and input_data['display_name']:
            self.display_name = input_data['display_name']
        if 'name' in input_data and input_data['name']:
            self.name = input_data['name']
        if 'description' in input_data:
            self.description = input_data['description']
        if 'config' in input_data:
            self.config = input_data['config']
        if 'small_logo' in input_data:
            self.small_logo = input_data['small_logo']
        if 'logo' in input_data:
            self.logo = input_data['logo']
        
        # Update auth templates in transaction-like manner
        self._update_auth_templates(non_empty_templates)
    
    def _update_auth_templates(self, templates_data: List[Dict[str, Any]]) -> None:
        """Update auth templates with proper handling of existing templates"""
        for template_data in templates_data:
            template_id = template_data.pop('id', None)
            auth_params = template_data.pop('params', [])
            template_data['connector'] = self.connector
            
            # Find existing template or create new one
            existing_template = None
            if template_id:
                existing_template = next(
                    (t for t in self.auth_templates if t.id == template_id), 
                    None
                )
            
            if existing_template:
                existing_template.update_mutable_(template_data)
            else:
                from app.models.auth_template import AuthTemplate
                new_template = AuthTemplate(**template_data)
                self.auth_templates.append(new_template)
                existing_template = new_template
            
            # Update auth parameters
            for param_data in auth_params:
                param_id = param_data.pop('id', None)
                param_data['vendor_id'] = self.id
                param_data['auth_template_id'] = existing_template.id
                
                existing_param = None
                if param_id and hasattr(existing_template, 'auth_parameters'):
                    existing_param = next(
                        (p for p in existing_template.auth_parameters if p.id == param_id),
                        None
                    )
                
                if existing_param:
                    existing_param.update_mutable_(param_data)
                # else:
                    # existing_template.auth_parameters.append(AuthParameter(**param_data))
    
    @classmethod
    def validate_auth_templates_(cls, auth_templates: List[Dict[str, Any]], vendor_connector) -> None:
        """Validate auth templates against vendor connector"""
        invalid = []
        
        for template in auth_templates:
            credentials_type = template.get('credentials_type')
            if credentials_type:
                from app.models.connector import Connector
                connector = Connector.query.filter_by(connector_type=credentials_type).first()
                if connector != vendor_connector:
                    invalid.append(template)
        
        if invalid:
            raise ValueError(f"Credentials type for these auth templates did not match vendor's: {invalid}")
    
    # Predicate methods (Rails pattern)
    def active_(self) -> bool:
        """Check if vendor is active"""
        return self.is_active is True
    
    def inactive_(self) -> bool:
        """Check if vendor is inactive"""
        return not self.active_()
    
    def has_config_(self) -> bool:
        """Check if vendor has configuration"""
        return bool(self.config)
    
    def has_display_name_(self) -> bool:
        """Check if vendor has display name"""
        return bool(self.display_name and self.display_name.strip())
    
    def has_description_(self) -> bool:
        """Check if vendor has description"""
        return bool(self.description and self.description.strip())
    
    def has_small_logo_(self) -> bool:
        """Check if vendor has small logo"""
        return bool(self.small_logo and self.small_logo.strip())
    
    def has_logo_(self) -> bool:
        """Check if vendor has logo"""
        return bool(self.logo and self.logo.strip())
    
    def has_any_logo_(self) -> bool:
        """Check if vendor has any logo"""
        return self.has_small_logo_() or self.has_logo_()
    
    def has_auth_templates_(self) -> bool:
        """Check if vendor has auth templates"""
        return bool(self.auth_templates)
    
    def has_endpoints_(self) -> bool:
        """Check if vendor has endpoints"""
        return bool(self.vendor_endpoints)
    
    def has_connector_(self) -> bool:
        """Check if vendor has connector"""
        return self.connector_id is not None
    
    def connector_type_matches_(self, connection_type: str) -> bool:
        """Check if vendor's connector matches given type"""
        if not self.connector:
            return False
        return self.connector.connector_type == connection_type
    
    def supports_auth_type_(self, auth_type: str) -> bool:
        """Check if vendor supports given auth type"""
        if not self.auth_templates:
            return False
        return any(
            template.auth_type == auth_type 
            for template in self.auth_templates
        )
    
    def fully_configured_(self) -> bool:
        """Check if vendor is fully configured"""
        return (bool(self.name) and
                self.has_connector_() and
                self.has_auth_templates_() and
                self.active_())
    
    def ready_for_use_(self) -> bool:
        """Check if vendor is ready for use"""
        return (self.fully_configured_() and
                self.has_endpoints_())
    
    def configuration_incomplete_(self) -> bool:
        """Check if vendor configuration is incomplete"""
        return not self.fully_configured_()
    
    def needs_setup_(self) -> bool:
        """Check if vendor needs setup"""
        return (not self.has_auth_templates_() or
                not self.has_endpoints_() or
                self.inactive_())
    
    # State management methods (Rails pattern)
    def activate_(self) -> None:
        """Activate the vendor"""
        if not self.active_():
            self.is_active = True
    
    def deactivate_(self) -> None:
        """Deactivate the vendor"""
        if self.active_():
            self.is_active = False
    
    def toggle_active_(self) -> None:
        """Toggle active state"""
        if self.active_():
            self.deactivate_()
        else:
            self.activate_()
    
    def clear_config_(self) -> None:
        """Clear vendor configuration"""
        self.config = None
    
    def update_config_(self, new_config: Dict[str, Any]) -> None:
        """Update vendor configuration"""
        self.config = new_config
    
    def merge_config_(self, additional_config: Dict[str, Any]) -> None:
        """Merge additional configuration"""
        if self.config is None:
            self.config = {}
        
        if isinstance(self.config, dict) and isinstance(additional_config, dict):
            self.config.update(additional_config)
        else:
            self.config = additional_config
    
    def remove_all_auth_templates_(self) -> None:
        """Remove all auth templates"""
        self.auth_templates.clear()
    
    def remove_all_endpoints_(self) -> None:
        """Remove all vendor endpoints"""
        self.vendor_endpoints.clear()
    
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
    
    def remove_config_value(self, key: str) -> None:
        """Remove a configuration value"""
        if self.config and isinstance(self.config, dict):
            self.config.pop(key, None)
    
    # Template and endpoint query methods
    def auth_templates_by_type(self, auth_type: str) -> List:
        """Get auth templates by type"""
        if not self.auth_templates:
            return []
        return [t for t in self.auth_templates if t.auth_type == auth_type]
    
    def active_auth_templates(self) -> List:
        """Get active auth templates"""
        if not self.auth_templates:
            return []
        return [t for t in self.auth_templates if hasattr(t, 'active_') and t.active_()]
    
    def active_endpoints(self) -> List:
        """Get active vendor endpoints"""
        if not self.vendor_endpoints:
            return []
        return [e for e in self.vendor_endpoints if hasattr(e, 'active_') and e.active_()]
    
    def endpoint_count(self) -> int:
        """Get count of vendor endpoints"""
        return len(self.vendor_endpoints) if self.vendor_endpoints else 0
    
    def auth_template_count(self) -> int:
        """Get count of auth templates"""
        return len(self.auth_templates) if self.auth_templates else 0
    
    # Display methods
    def display_name_or_name(self) -> str:
        """Get display name or fallback to name"""
        return self.display_name if self.has_display_name_() else self.name
    
    def status_display(self) -> str:
        """Get human-readable status"""
        return "Active" if self.active_() else "Inactive"
    
    def config_summary(self) -> str:
        """Get configuration summary"""
        if not self.has_config_():
            return "No configuration"
        
        if isinstance(self.config, dict):
            count = len(self.config)
            return f"{count} configuration {'item' if count == 1 else 'items'}"
        
        return "Custom configuration"
    
    def connector_summary(self) -> str:
        """Get connector summary"""
        if not self.connector:
            return "No connector"
        return f"Connector: {self.connector.name or self.connector.connector_type}"
    
    def auth_summary(self) -> str:
        """Get authentication summary"""
        count = self.auth_template_count()
        if count == 0:
            return "No authentication templates"
        return f"{count} auth {'template' if count == 1 else 'templates'}"
    
    def endpoint_summary(self) -> str:
        """Get endpoint summary"""
        count = self.endpoint_count()
        if count == 0:
            return "No endpoints"
        return f"{count} {'endpoint' if count == 1 else 'endpoints'}"
    
    def vendor_summary(self) -> str:
        """Get complete vendor summary"""
        parts = [
            self.status_display(),
            self.connector_summary(),
            self.auth_summary()
        ]
        
        if self.has_endpoints_():
            parts.append(self.endpoint_summary())
        
        if self.has_config_():
            parts.append(self.config_summary())
        
        return " | ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'name': self.name,
            'display_name': self.display_name,
            'description': self.description,
            'small_logo': self.small_logo,
            'logo': self.logo,
            'is_active': self.is_active,
            'config': self.config,
            'connector_id': self.connector_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Predicate methods
            'active': self.active_(),
            'inactive': self.inactive_(),
            'has_config': self.has_config_(),
            'has_display_name': self.has_display_name_(),
            'has_description': self.has_description_(),
            'has_small_logo': self.has_small_logo_(),
            'has_logo': self.has_logo_(),
            'has_any_logo': self.has_any_logo_(),
            'has_auth_templates': self.has_auth_templates_(),
            'has_endpoints': self.has_endpoints_(),
            'has_connector': self.has_connector_(),
            'fully_configured': self.fully_configured_(),
            'ready_for_use': self.ready_for_use_(),
            'configuration_incomplete': self.configuration_incomplete_(),
            'needs_setup': self.needs_setup_(),
            
            # Counts
            'auth_template_count': self.auth_template_count(),
            'endpoint_count': self.endpoint_count(),
            
            # Display values
            'display_name_or_name': self.display_name_or_name(),
            'status_display': self.status_display(),
            'config_summary': self.config_summary(),
            'connector_summary': self.connector_summary(),
            'auth_summary': self.auth_summary(),
            'endpoint_summary': self.endpoint_summary(),
            'vendor_summary': self.vendor_summary()
        }