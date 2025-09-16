from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, Enum as SQLEnum
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import secrets
import uuid
import string
import random

from app.database import Base

class ServiceKeyStatuses(PyEnum):
    INIT = "INIT"
    PAUSED = "PAUSED"
    ACTIVE = "ACTIVE"

class ServiceKey(Base):
    __tablename__ = 'service_keys'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    data_source_id = Column(Integer, ForeignKey('data_sources.id'))
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    api_key = Column(String(255))
    external_id = Column(String(255))
    last_rotated_key = Column(String(255))
    last_rotated_at = Column(DateTime)
    
    status = Column(SQLEnum(ServiceKeyStatuses), default=ServiceKeyStatuses.INIT)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id], back_populates="owned_service_keys")
    user = relationship("User", foreign_keys=[user_id], back_populates="service_keys") 
    org = relationship("Org", foreign_keys=[org_id], back_populates="service_keys")
    data_source = relationship("DataSource", foreign_keys=[data_source_id], back_populates="service_keys")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.status:
            self.status = ServiceKeyStatuses.INIT
    
    @validates('name')
    def validate_name(self, key, name):
        if not name or not name.strip():
            raise ValueError("Name is required")
        return name.strip()
    
    @classmethod
    def build_from_input(cls, user, org, input_data: Dict[str, Any]) -> 'ServiceKey':
        """Factory method to create service key from input data"""
        key = cls(
            owner_id=user.id,
            user_id=user.id,
            org_id=org.id,
            name=input_data['name'],
            description=input_data.get('description'),
            status=ServiceKeyStatuses.ACTIVE
        )
        
        if 'data_source_id' in input_data:
            key.data_source_id = input_data['data_source_id']
        
        # Generate keys before saving
        key._generate_keys()
        # In production: key.save()
        
        return key
    
    def update_mutable_(self, user, input_data: Dict[str, Any]) -> None:
        """Update mutable attributes"""
        if 'name' in input_data and input_data['name']:
            self.name = input_data['name']
        if 'description' in input_data:
            self.description = input_data['description']
        # In production: self.save()
    
    def _generate_keys(self) -> None:
        """Generate API key and external ID if not present"""
        if not self.api_key:
            # Generate UUID and remove dashes (Rails pattern)
            self.api_key = str(uuid.uuid4()).replace('-', '')
            
        if not self.external_id:
            # Generate 32-character alphanumeric string
            self.external_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
    
    def rotate_(self) -> None:
        """Rotate the API key"""
        self.last_rotated_key = self.api_key
        self.last_rotated_at = datetime.utcnow()
        self.api_key = None
        self._generate_keys()  # Generate new key
        # In production: self.save()
    
    def pause_(self) -> None:
        """Pause the service key"""
        if not self.paused_():
            self.status = ServiceKeyStatuses.PAUSED
            # In production: self.save()
    
    def activate_(self) -> None:
        """Activate the service key"""
        if not self.active_():
            self.status = ServiceKeyStatuses.ACTIVE
            # In production: self.save()
    
    # Predicate methods (Rails pattern)
    def active_(self) -> bool:
        """Check if service key is active"""
        return self.status == ServiceKeyStatuses.ACTIVE
    
    def paused_(self) -> bool:
        """Check if service key is paused"""
        return self.status == ServiceKeyStatuses.PAUSED
    
    def init_(self) -> bool:
        """Check if service key is in init state"""
        return self.status == ServiceKeyStatuses.INIT
    
    def has_api_key_(self) -> bool:
        """Check if service key has an API key"""
        return bool(self.api_key)
    
    def has_external_id_(self) -> bool:
        """Check if service key has an external ID"""
        return bool(self.external_id)
    
    def has_data_source_(self) -> bool:
        """Check if service key is associated with a data source"""
        return self.data_source_id is not None
    
    def has_description_(self) -> bool:
        """Check if service key has a description"""
        return bool(self.description and self.description.strip())
    
    def rotated_(self) -> bool:
        """Check if service key has been rotated"""
        return bool(self.last_rotated_key)
    
    def never_rotated_(self) -> bool:
        """Check if service key has never been rotated"""
        return not self.rotated_()
    
    def recently_rotated_(self, hours: int = 24) -> bool:
        """Check if service key was recently rotated"""
        if not self.last_rotated_at:
            return False
        
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return self.last_rotated_at > cutoff
    
    def usable_(self) -> bool:
        """Check if service key is usable (active and has keys)"""
        return self.active_() and self.has_api_key_() and self.has_external_id_()
    
    def ready_for_service_(self) -> bool:
        """Check if service key is ready for service authentication"""
        return (self.usable_() and 
                bool(self.name) and
                self.org_id is not None)
    
    def owned_by_user_(self, user) -> bool:
        """Check if service key is owned by user"""
        return self.owner_id == user.id
    
    def accessible_by_user_(self, user) -> bool:
        """Check if service key is accessible by user"""
        # Owner has access
        if self.owned_by_user_(user):
            return True
        
        # User assigned to key has access
        if self.user_id == user.id:
            return True
        
        # Org admins have access
        if self.org and self.org.has_admin_access_(user):
            return True
        
        return False
    
    def manageable_by_user_(self, user) -> bool:
        """Check if service key can be managed by user"""
        # Owner can manage
        if self.owned_by_user_(user):
            return True
        
        # Org admins can manage
        if self.org and self.org.has_admin_access_(user):
            return True
        
        return False
    
    # State management methods
    def toggle_status_(self) -> None:
        """Toggle between active and paused"""
        if self.active_():
            self.pause_()
        elif self.paused_():
            self.activate_()
        else:  # init state
            self.activate_()
    
    def reset_to_init_(self) -> None:
        """Reset to init state"""
        self.status = ServiceKeyStatuses.INIT
        # In production: self.save()
    
    def regenerate_keys_(self) -> None:
        """Regenerate both API key and external ID"""
        old_api_key = self.api_key
        old_external_id = self.external_id
        
        self.api_key = None
        self.external_id = None
        self._generate_keys()
        
        # Keep track of rotation
        if old_api_key:
            self.last_rotated_key = old_api_key
            self.last_rotated_at = datetime.utcnow()
        
        # In production: self.save()
    
    def revoke_(self) -> None:
        """Revoke the service key"""
        self.pause_()
        # In production, might also want to blacklist the key
    
    # Key management methods
    def mask_api_key(self, show_chars: int = 4) -> str:
        """Get masked API key for display"""
        if not self.api_key:
            return "Not generated"
        
        if len(self.api_key) <= show_chars:
            return "*" * len(self.api_key)
        
        return self.api_key[:show_chars] + "*" * (len(self.api_key) - show_chars)
    
    def mask_external_id(self, show_chars: int = 4) -> str:
        """Get masked external ID for display"""
        if not self.external_id:
            return "Not generated"
        
        if len(self.external_id) <= show_chars:
            return "*" * len(self.external_id)
        
        return self.external_id[:show_chars] + "*" * (len(self.external_id) - show_chars)
    
    def key_age_days(self) -> Optional[int]:
        """Get age of current key in days"""
        if not self.last_rotated_at:
            # Key has never been rotated, use creation date
            if self.created_at:
                delta = datetime.utcnow() - self.created_at
                return delta.days
            return None
        
        delta = datetime.utcnow() - self.last_rotated_at
        return delta.days
    
    def needs_rotation_(self, max_age_days: int = 90) -> bool:
        """Check if key needs rotation based on age"""
        age = self.key_age_days()
        return age is not None and age > max_age_days
    
    def rotation_due_(self) -> bool:
        """Check if key rotation is due (90 days default)"""
        return self.needs_rotation_()
    
    def rotation_overdue_(self, overdue_days: int = 120) -> bool:
        """Check if key rotation is overdue"""
        return self.needs_rotation_(overdue_days)
    
    # Authentication methods
    def authenticate(self, provided_api_key: str) -> bool:
        """Authenticate using API key"""
        return (self.usable_() and 
                self.api_key and 
                self.api_key == provided_api_key)
    
    def authenticate_external(self, provided_external_id: str) -> bool:
        """Authenticate using external ID"""
        return (self.usable_() and 
                self.external_id and 
                self.external_id == provided_external_id)
    
    def valid_for_data_source_(self, data_source_id: int) -> bool:
        """Check if key is valid for specific data source"""
        if not self.usable_():
            return False
        
        # If no data source restriction, valid for any
        if not self.data_source_id:
            return True
        
        # Must match specific data source
        return self.data_source_id == data_source_id
    
    # Display methods
    def status_display(self) -> str:
        """Get human-readable status"""
        status_map = {
            ServiceKeyStatuses.INIT: "Initializing",
            ServiceKeyStatuses.PAUSED: "Paused",
            ServiceKeyStatuses.ACTIVE: "Active"
        }
        return status_map.get(self.status, self.status.value)
    
    def rotation_status_display(self) -> str:
        """Get rotation status display"""
        if self.never_rotated_():
            return "Never rotated"
        elif self.rotation_overdue_():
            return "Rotation overdue"
        elif self.rotation_due_():
            return "Rotation due"
        elif self.recently_rotated_():
            return "Recently rotated"
        else:
            age = self.key_age_days()
            return f"Rotated {age} days ago" if age else "Unknown rotation status"
    
    def data_source_display(self) -> str:
        """Get data source display"""
        if not self.has_data_source_():
            return "All data sources"
        
        if self.data_source:
            return f"Data source: {self.data_source.name}"
        
        return f"Data source ID: {self.data_source_id}"
    
    def key_summary(self) -> str:
        """Get complete key summary"""
        parts = [
            self.status_display(),
            f"API Key: {self.mask_api_key()}",
            f"External ID: {self.mask_external_id()}",
            self.rotation_status_display()
        ]
        
        if self.has_data_source_():
            parts.append(self.data_source_display())
        
        return " | ".join(parts)
    
    def to_dict(self, include_secrets: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        result = {
            'id': self.id,
            'owner_id': self.owner_id,
            'user_id': self.user_id,
            'org_id': self.org_id,
            'data_source_id': self.data_source_id,
            'name': self.name,
            'description': self.description,
            'status': self.status.value if self.status else None,
            'last_rotated_at': self.last_rotated_at.isoformat() if self.last_rotated_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Masked keys (always safe to include)
            'api_key_masked': self.mask_api_key(),
            'external_id_masked': self.mask_external_id(),
            
            # Key management info
            'key_age_days': self.key_age_days(),
            
            # Predicate methods
            'active': self.active_(),
            'paused': self.paused_(),
            'init': self.init_(),
            'has_api_key': self.has_api_key_(),
            'has_external_id': self.has_external_id_(),
            'has_data_source': self.has_data_source_(),
            'has_description': self.has_description_(),
            'rotated': self.rotated_(),
            'never_rotated': self.never_rotated_(),
            'recently_rotated': self.recently_rotated_(),
            'usable': self.usable_(),
            'ready_for_service': self.ready_for_service_(),
            'needs_rotation': self.needs_rotation_(),
            'rotation_due': self.rotation_due_(),
            'rotation_overdue': self.rotation_overdue_(),
            
            # Display values
            'status_display': self.status_display(),
            'rotation_status_display': self.rotation_status_display(),
            'data_source_display': self.data_source_display(),
            'key_summary': self.key_summary()
        }
        
        # Include actual secrets only if explicitly requested and secure context
        if include_secrets:
            result.update({
                'api_key': self.api_key,
                'external_id': self.external_id,
                'last_rotated_key': self.last_rotated_key
            })
        
        return result