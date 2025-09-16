from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, Enum as SQLEnum, JSON
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import json

from app.database import Base

class FlowTypes(PyEnum):
    AIRFLOW = "airflow"

class CustomDataFlowStatuses(PyEnum):
    INIT = "INIT"
    PAUSED = "PAUSED"
    ACTIVE = "ACTIVE"
    RATE_LIMITED = "RATE_LIMITED"

class CustomDataFlow(Base):
    __tablename__ = 'custom_data_flows'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    copied_from_id = Column(Integer, ForeignKey('custom_data_flows.id'))
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    flow_type = Column(String(50), nullable=False, default=FlowTypes.AIRFLOW.value)
    status = Column(SQLEnum(CustomDataFlowStatuses), nullable=False, default=CustomDataFlowStatuses.INIT)
    
    managed = Column(Boolean, default=False)
    config = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id], back_populates="custom_data_flows")
    org = relationship("Org", foreign_keys=[org_id], back_populates="custom_data_flows")
    copied_from = relationship("CustomDataFlow", remote_side=[id], back_populates="copies")
    copies = relationship("CustomDataFlow", back_populates="copied_from")
    
    # Many-to-many relationships (through association tables)
    code_containers = relationship("CodeContainer", secondary="custom_data_flows_code_containers", back_populates="custom_data_flows")
    data_credentials = relationship("DataCredentials", secondary="custom_data_flows_data_credentials", back_populates="custom_data_flows")
    
    # Constants
    DEFAULT_FLOW_TYPE = FlowTypes.AIRFLOW
    
    @classmethod
    def flow_types_enum(cls) -> str:
        """Generate ENUM string for database schema"""
        values = [f"'{ft.value}'" for ft in FlowTypes]
        return f"ENUM({','.join(values)})"
    
    @classmethod
    def validate_flow_type_str(cls, flow_type_str: Optional[str]) -> Optional[str]:
        """Validate flow type string"""
        if not isinstance(flow_type_str, str):
            return None
        
        try:
            FlowTypes(flow_type_str)
            return flow_type_str
        except ValueError:
            return None
    
    @classmethod
    def validate_status_str(cls, status_str: Optional[str]) -> Optional[str]:
        """Validate status string"""
        if not isinstance(status_str, str):
            return None
        
        status_str = status_str.upper()
        try:
            CustomDataFlowStatuses(status_str)
            return status_str
        except ValueError:
            return None
    
    @classmethod
    def build_from_input(cls, user, org, input_data: Dict[str, Any]) -> 'CustomDataFlow':
        """Factory method to create custom data flow from input data"""
        if not input_data.get('name'):
            raise ValueError("Name attribute must not be blank")
        
        cdf = cls(
            owner_id=user.id,
            org_id=org.id,
            status=CustomDataFlowStatuses.INIT
        )
        cdf.update_mutable_(user, org, input_data)
        
        return cdf
    
    def update_mutable_(self, user, org, input_data: Dict[str, Any]) -> None:
        """Update mutable attributes"""
        if not input_data or not user:
            return
        
        # Remove tags for separate processing (if implementing tagging)
        tags = input_data.pop('tags', None)
        
        # Basic attributes
        if input_data.get('name'):
            self.name = input_data['name']
        if 'description' in input_data:
            self.description = input_data['description']
        
        # Update ownership
        if user.id != self.owner_id:
            self.owner_id = user.id
        if org.id != self.org_id:
            self.org_id = org.id
        
        # Flow type validation
        if input_data.get('flow_type'):
            flow_type = self.validate_flow_type_str(input_data['flow_type'])
            if flow_type is None:
                raise ValueError("Invalid custom data flow type")
            self.flow_type = flow_type
        elif not self.flow_type:
            self.flow_type = self.DEFAULT_FLOW_TYPE.value
        
        if 'managed' in input_data:
            self.managed = bool(input_data['managed'])
        if 'config' in input_data:
            self.config = input_data['config']
        
        # Status validation
        if 'status' in input_data:
            status = self.validate_status_str(input_data['status'])
            if status is None:
                raise ValueError("Invalid status")
            self.status = CustomDataFlowStatuses(status)
        
        # Update associations (code containers and data credentials)
        self._update_code_containers(user, input_data)
        self._update_data_credentials(user, input_data)
    
    def _update_code_containers(self, user, input_data: Dict[str, Any]) -> None:
        """Update code container associations"""
        if 'code_container_ids' not in input_data:
            return
        
        container_ids = input_data['code_container_ids']
        if not container_ids:
            return
        
        # Get current container IDs
        current_ids = [cc.id for cc in self.code_containers]
        
        # Filter out already associated containers
        new_ids = [cid for cid in container_ids if cid not in current_ids]
        
        # Add new containers (with permission checking)
        from app.models.code_container import CodeContainer
        for container_id in new_ids:
            container = CodeContainer.query.get(container_id)
            if not container:
                raise ValueError(f"Code container not found: {container_id}")
            
            # Check permissions (simplified - in production would use proper permission system)
            if not user.can_read_resource_(container):
                raise ValueError("Invalid access to code container")
            
            self.code_containers.append(container)
    
    def _update_data_credentials(self, user, input_data: Dict[str, Any]) -> None:
        """Update data credentials associations"""
        if 'data_credentials_ids' not in input_data:
            return
        
        credential_ids = input_data['data_credentials_ids']
        if not credential_ids:
            return
        
        # Get current credential IDs
        current_ids = [dc.id for dc in self.data_credentials]
        
        # Filter out already associated credentials
        new_ids = [cid for cid in credential_ids if cid not in current_ids]
        
        # Add new credentials (with permission checking)
        from app.models.data_credentials import DataCredentials
        for credential_id in new_ids:
            credential = DataCredentials.query.get(credential_id)
            if not credential:
                raise ValueError(f"Data credential not found: {credential_id}")
            
            # Check permissions
            if not user.can_read_resource_(credential):
                raise ValueError("Invalid access to data credentials")
            
            self.data_credentials.append(credential)
    
    def copy_post_save(self, original_flow: 'CustomDataFlow', user, options: Dict[str, Any]) -> None:
        """Post-save copy operations for associations"""
        # Copy code containers
        for code_container in original_flow.code_containers:
            if code_container:
                if code_container.reusable_:
                    self.code_containers.append(code_container)
                else:
                    copied_container = code_container.copy(user, options)
                    self.code_containers.append(copied_container)
        
        # Copy data credentials
        reuse_creds = bool(options.get('reuse_data_credentials', False))
        for data_credential in original_flow.data_credentials:
            if data_credential:
                if reuse_creds:
                    self.data_credentials.append(data_credential)
                else:
                    copied_credential = data_credential.copy(user, options)
                    self.data_credentials.append(copied_credential)
    
    # Predicate methods (Rails pattern)
    def active_(self) -> bool:
        """Check if flow is active"""
        return self.status == CustomDataFlowStatuses.ACTIVE
    
    def paused_(self) -> bool:
        """Check if flow is paused"""
        return self.status == CustomDataFlowStatuses.PAUSED
    
    def init_(self) -> bool:
        """Check if flow is in init state"""
        return self.status == CustomDataFlowStatuses.INIT
    
    def rate_limited_(self) -> bool:
        """Check if flow is rate limited"""
        return self.status == CustomDataFlowStatuses.RATE_LIMITED
    
    def managed_(self) -> bool:
        """Check if flow is managed"""
        return self.managed is True
    
    def unmanaged_(self) -> bool:
        """Check if flow is unmanaged"""
        return not self.managed_()
    
    def airflow_type_(self) -> bool:
        """Check if flow type is airflow"""
        return self.flow_type == FlowTypes.AIRFLOW.value
    
    def has_config_(self) -> bool:
        """Check if flow has configuration"""
        return bool(self.config)
    
    def has_code_containers_(self) -> bool:
        """Check if flow has associated code containers"""
        return bool(self.code_containers)
    
    def has_data_credentials_(self) -> bool:
        """Check if flow has associated data credentials"""
        return bool(self.data_credentials)
    
    def copy_(self) -> bool:
        """Check if this is a copy of another flow"""
        return self.copied_from_id is not None
    
    def original_(self) -> bool:
        """Check if this is an original flow (not a copy)"""
        return not self.copy_()
    
    def has_copies_(self) -> bool:
        """Check if this flow has been copied"""
        return bool(self.copies)
    
    # State management methods (Rails pattern)
    def activate_(self) -> None:
        """Activate the flow"""
        if not self.active_():
            self.status = CustomDataFlowStatuses.ACTIVE
            # In production: self.save()
    
    def pause_(self) -> None:
        """Pause the flow"""
        if not self.paused_():
            self.status = CustomDataFlowStatuses.PAUSED
            # In production: self.save()
    
    def reset_to_init_(self) -> None:
        """Reset flow to init state"""
        self.status = CustomDataFlowStatuses.INIT
        # In production: self.save()
    
    def set_rate_limited_(self) -> None:
        """Set flow to rate limited state"""
        self.status = CustomDataFlowStatuses.RATE_LIMITED
        # In production: self.save()
    
    # Association management methods
    def add_code_container(self, container, user) -> None:
        """Add a code container with permission checking"""
        if not user.can_read_resource_(container):
            raise ValueError("Invalid access to code container")
        
        if container not in self.code_containers:
            self.code_containers.append(container)
    
    def remove_code_container(self, container) -> None:
        """Remove a code container"""
        if container in self.code_containers:
            self.code_containers.remove(container)
    
    def add_data_credential(self, credential, user) -> None:
        """Add a data credential with permission checking"""
        if not user.can_read_resource_(credential):
            raise ValueError("Invalid access to data credential")
        
        if credential not in self.data_credentials:
            self.data_credentials.append(credential)
    
    def remove_data_credential(self, credential) -> None:
        """Remove a data credential"""
        if credential in self.data_credentials:
            self.data_credentials.remove(credential)
    
    def clear_associations(self) -> None:
        """Clear all associations"""
        self.code_containers.clear()
        self.data_credentials.clear()
    
    # Utility methods
    def code_container_count(self) -> int:
        """Get count of associated code containers"""
        return len(self.code_containers)
    
    def data_credential_count(self) -> int:
        """Get count of associated data credentials"""
        return len(self.data_credentials)
    
    def status_display(self) -> str:
        """Get human-readable status"""
        status_map = {
            CustomDataFlowStatuses.INIT: "Initializing",
            CustomDataFlowStatuses.PAUSED: "Paused",
            CustomDataFlowStatuses.ACTIVE: "Active",
            CustomDataFlowStatuses.RATE_LIMITED: "Rate Limited"
        }
        return status_map.get(self.status, self.status.value)
    
    def flow_type_display(self) -> str:
        """Get human-readable flow type"""
        type_map = {
            FlowTypes.AIRFLOW.value: "Apache Airflow"
        }
        return type_map.get(self.flow_type, self.flow_type)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'copied_from_id': self.copied_from_id,
            'name': self.name,
            'description': self.description,
            'flow_type': self.flow_type,
            'status': self.status.value if self.status else None,
            'managed': self.managed,
            'config': self.config,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Association counts
            'code_container_count': self.code_container_count(),
            'data_credential_count': self.data_credential_count(),
            
            # Predicate methods
            'active': self.active_(),
            'paused': self.paused_(),
            'init': self.init_(),
            'rate_limited': self.rate_limited_(),
            'managed': self.managed_(),
            'unmanaged': self.unmanaged_(),
            'airflow_type': self.airflow_type_(),
            'has_config': self.has_config_(),
            'has_code_containers': self.has_code_containers_(),
            'has_data_credentials': self.has_data_credentials_(),
            'is_copy': self.copy_(),
            'is_original': self.original_(),
            'has_copies': self.has_copies_(),
            
            # Display values
            'status_display': self.status_display(),
            'flow_type_display': self.flow_type_display()
        }