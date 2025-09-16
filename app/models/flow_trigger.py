from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Enum as SQLEnum
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import json

from app.database import Base

class FlowTriggerStatuses(PyEnum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"

class ResourceTypes(PyEnum):
    DATA_SOURCE = "data_source"
    DATA_SINK = "data_sink"

class FlowTrigger(Base):
    __tablename__ = 'flow_triggers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    
    # Triggering side (what causes the trigger)
    triggering_flow_node_id = Column(Integer, ForeignKey('flow_nodes.id'), nullable=False)
    triggering_origin_node_id = Column(Integer, ForeignKey('flow_nodes.id'))
    triggering_event_type = Column(String(100))
    
    # Triggered side (what gets triggered)
    triggered_origin_node_id = Column(Integer, ForeignKey('flow_nodes.id'), nullable=False)
    triggered_event_type = Column(String(100))
    
    status = Column(SQLEnum(FlowTriggerStatuses), default=FlowTriggerStatuses.ACTIVE)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id], back_populates="flow_triggers")
    org = relationship("Org", foreign_keys=[org_id], back_populates="flow_triggers")
    
    triggering_flow_node = relationship("FlowNode", foreign_keys=[triggering_flow_node_id], back_populates="triggering_flow_triggers")
    triggering_origin_node = relationship("FlowNode", foreign_keys=[triggering_origin_node_id], back_populates="triggering_origin_flow_triggers")
    triggered_origin_node = relationship("FlowNode", foreign_keys=[triggered_origin_node_id], back_populates="triggered_flow_triggers")
    
    # Control messages flag
    control_messages_enabled = True
    
    @classmethod
    def validate_resource_type(cls, resource_type_str: str = "unknown") -> Optional[type]:
        """Validate and return resource type class"""
        resource_mapping = {
            ResourceTypes.DATA_SOURCE.value: "DataSource",
            ResourceTypes.DATA_SINK.value: "DataSink"
        }
        
        try:
            resource_type = ResourceTypes(resource_type_str.lower())
            return resource_mapping.get(resource_type.value)
        except (ValueError, AttributeError):
            return None
    
    @classmethod
    def accessible_by_user(cls, user, org, input_opts: Dict[str, Any] = None) -> 'Query':
        """Get flow triggers accessible by user based on flow node access"""
        from app.models.flow_node import FlowNode
        
        # Get accessible flow node IDs (admin access required)
        accessible_opts = input_opts or {}
        accessible_opts['access_role'] = 'admin'
        
        fn_ids = FlowNode.accessible_by_user(user, org, accessible_opts).with_entities(FlowNode.id).all()
        fn_id_list = [fn_id[0] for fn_id in fn_ids]
        
        # Return triggers where user has admin access to either triggered or triggering nodes
        return cls.query.filter(
            (cls.triggered_origin_node_id.in_(fn_id_list)) |
            (cls.triggering_flow_node_id.in_(fn_id_list))
        )
    
    @classmethod
    def build_from_input(cls, user, org, input_data: Dict[str, Any]) -> 'FlowTrigger':
        """Factory method to create flow trigger from input data"""
        if not isinstance(input_data, dict) or not user:
            return None
        
        # Validate required inputs for triggered side
        if (not input_data.get('triggered_origin_node_id') and 
            not input_data.get('triggered_resource_id') and
            not input_data.get('data_source_id')):
            raise ValueError("triggered_origin_node_id or triggered_resource_id is required")
        
        # Validate required inputs for triggering side
        if (not input_data.get('triggering_flow_node_id') and
            not input_data.get('triggering_resource_id') and
            not input_data.get('data_sink_id') and 
            not input_data.get('data_source_id')):
            raise ValueError("triggering_flow_node_id or triggering_resource_id is required")
        
        # Check for existing trigger
        existing_trigger = cls.query.filter_by(
            triggered_origin_node_id=input_data.get('triggered_origin_node_id'),
            triggering_flow_node_id=input_data.get('triggering_flow_node_id')
        ).first()
        
        if existing_trigger:
            raise ValueError("A flow trigger already exists for triggered and triggering resources")
        
        ft = cls()
        ft.update_mutable_(user, org, input_data)
        return ft
    
    def update_mutable_(self, user, org, input_data: Dict[str, Any]) -> 'FlowTrigger':
        """Update mutable attributes with transaction safety"""
        
        # Update basic attributes
        if user.id != self.owner_id:
            self.owner_id = user.id
        if org.id != self.org_id:
            self.org_id = org.id
        
        if 'triggering_event_type' in input_data:
            self.triggering_event_type = input_data['triggering_event_type']
        if 'triggered_event_type' in input_data:
            self.triggered_event_type = input_data['triggered_event_type']
        
        # Update triggered origin node
        if (input_data.get('triggered_origin_node_id') or 
            input_data.get('triggered_resource_id') or
            input_data.get('data_source_id')):
            self.triggered_origin_node_id = self._get_triggered_origin_node_id(input_data, user)
        
        # Update triggering flow node
        if (input_data.get('triggering_flow_node_id') or
            input_data.get('triggering_resource_id') or
            input_data.get('data_sink_id') or
            input_data.get('data_source_id')):
            self.triggering_flow_node_id = self._get_triggering_flow_node_id(input_data, user)
            
            # Set triggering origin node from triggering flow node
            if self.triggering_flow_node:
                self.triggering_origin_node_id = self.triggering_flow_node.origin_node_id
        
        # Validate no self-triggering
        if (self.triggering_flow_node and self.triggered_origin_node and
            self.triggering_flow_node.origin_node_id == self.triggered_origin_node_id):
            raise ValueError("A flow node cannot trigger its own origin node")
        
        # Check for cycles (simplified - in production would use proper cycle detection)
        if self._would_create_cycle():
            raise ValueError("Trigger would create a cycle in flows")
        
        return self
    
    def _get_triggered_origin_node_id(self, input_data: Dict[str, Any], user) -> int:
        """Get triggered origin node ID from input with permission checking"""
        from app.models.flow_node import FlowNode
        from app.models.data_source import DataSource
        
        triggered_origin_node = None
        
        if input_data.get('triggered_origin_node_id'):
            triggered_origin_node = FlowNode.query.get(input_data['triggered_origin_node_id'])
        elif input_data.get('data_source_id'):
            data_source = DataSource.query.get(input_data['data_source_id'])
            if data_source:
                triggered_origin_node = data_source.origin_node
        else:
            resource_type = self.validate_resource_type(input_data.get('triggered_resource_type', ''))
            if not resource_type:
                raise ValueError("A valid triggered_resource_type is required")
            
            # Dynamic model loading (simplified)
            resource_id = input_data.get('triggered_resource_id')
            if resource_type == "DataSource":
                from app.models.data_source import DataSource
                resource = DataSource.query.get(resource_id)
                triggered_origin_node = resource.origin_node if resource else None
            elif resource_type == "DataSink":
                from app.models.data_sink import DataSink
                resource = DataSink.query.get(resource_id)
                triggered_origin_node = resource.origin_node if resource else None
        
        if not triggered_origin_node:
            raise ValueError("Triggered origin node not found")
        
        # Check permissions (simplified)
        if not user.can_manage_resource_(triggered_origin_node):
            raise ValueError("Unauthorized access to triggered resource")
        
        return triggered_origin_node.id
    
    def _get_triggering_flow_node_id(self, input_data: Dict[str, Any], user) -> int:
        """Get triggering flow node ID from input with permission checking"""
        from app.models.flow_node import FlowNode
        from app.models.data_source import DataSource
        from app.models.data_sink import DataSink
        
        triggering_flow_node = None
        
        if input_data.get('triggering_flow_node_id'):
            triggering_flow_node = FlowNode.query.get(input_data['triggering_flow_node_id'])
        elif input_data.get('data_sink_id'):
            data_sink = DataSink.query.get(input_data['data_sink_id'])
            if data_sink:
                triggering_flow_node = data_sink.flow_node
        elif input_data.get('data_source_id'):
            data_source = DataSource.query.get(input_data['data_source_id'])
            if data_source:
                triggering_flow_node = data_source.origin_node
        else:
            resource_type = self.validate_resource_type(input_data.get('triggering_resource_type', ''))
            if not resource_type:
                raise ValueError("A valid triggering_resource_type is required")
            
            # Dynamic model loading (simplified)
            resource_id = input_data.get('triggering_resource_id')
            if resource_type == "DataSource":
                resource = DataSource.query.get(resource_id)
                triggering_flow_node = resource.origin_node if resource else None
            elif resource_type == "DataSink":
                resource = DataSink.query.get(resource_id)
                triggering_flow_node = resource.flow_node if resource else None
        
        if not triggering_flow_node:
            raise ValueError("Triggering flow node not found")
        
        # Check permissions (simplified)
        if not user.can_manage_resource_(triggering_flow_node):
            raise ValueError("Unauthorized access to triggering resource")
        
        return triggering_flow_node.id
    
    def _would_create_cycle(self) -> bool:
        """Simple cycle detection (in production would use proper graph traversal)"""
        # Simplified check - in production would implement full cycle detection
        return False
    
    def triggered_resource(self):
        """Get the triggered resource"""
        if self.triggered_origin_node:
            return self.triggered_origin_node.resource
        return None
    
    def triggering_resource(self):
        """Get the triggering resource"""
        if self.triggering_flow_node:
            return self.triggering_flow_node.resource
        return None
    
    # Predicate methods (Rails pattern)
    def active_(self) -> bool:
        """Check if trigger is active"""
        return self.status == FlowTriggerStatuses.ACTIVE
    
    def paused_(self) -> bool:
        """Check if trigger is paused"""
        return self.status == FlowTriggerStatuses.PAUSED
    
    def has_triggered_resource_(self) -> bool:
        """Check if trigger has a triggered resource"""
        return bool(self.triggered_resource())
    
    def has_triggering_resource_(self) -> bool:
        """Check if trigger has a triggering resource"""
        return bool(self.triggering_resource())
    
    def complete_(self) -> bool:
        """Check if trigger configuration is complete"""
        return (self.triggered_origin_node_id is not None and 
                self.triggering_flow_node_id is not None and
                self.triggering_event_type is not None and
                self.triggered_event_type is not None)
    
    def incomplete_(self) -> bool:
        """Check if trigger configuration is incomplete"""
        return not self.complete_()
    
    def control_messages_enabled_(self) -> bool:
        """Check if control messages are enabled"""
        return bool(self.control_messages_enabled)
    
    def same_origin_(self) -> bool:
        """Check if triggering and triggered nodes have the same origin"""
        return (self.triggering_origin_node_id is not None and 
                self.triggered_origin_node_id is not None and
                self.triggering_origin_node_id == self.triggered_origin_node_id)
    
    def cross_origin_(self) -> bool:
        """Check if triggering and triggered nodes have different origins"""
        return not self.same_origin_()
    
    def data_source_trigger_(self) -> bool:
        """Check if this is a data source trigger"""
        resource = self.triggering_resource()
        return resource and resource.__class__.__name__ == 'DataSource'
    
    def data_sink_trigger_(self) -> bool:
        """Check if this is a data sink trigger"""
        resource = self.triggering_resource()
        return resource and resource.__class__.__name__ == 'DataSink'
    
    # State management methods (Rails pattern)
    def activate_(self) -> None:
        """Activate the trigger"""
        if not self.active_():
            self.status = FlowTriggerStatuses.ACTIVE
            # In production: self.save()
    
    def pause_(self) -> None:
        """Pause the trigger"""
        if not self.paused_():
            self.status = FlowTriggerStatuses.PAUSED
            # In production: self.save()
    
    def toggle_status_(self) -> None:
        """Toggle between active and paused"""
        if self.active_():
            self.pause_()
        else:
            self.activate_()
    
    # Access control methods (Rails pattern)
    def has_admin_access_(self, user) -> bool:
        """Check if user has admin access to trigger"""
        if not self.triggered_origin_node or not self.triggering_flow_node:
            return False
        
        if not self.triggered_origin_node.has_admin_access_(user):
            return False
        
        return self.triggering_flow_node.has_admin_access_(user)
    
    def has_operator_access_(self, user) -> bool:
        """Check if user has operator access to trigger"""
        if not self.triggered_origin_node or not self.triggering_flow_node:
            return False
        
        if not self.triggered_origin_node.has_operator_access_(user):
            return False
        
        return self.triggering_flow_node.has_operator_access_(user)
    
    def has_collaborator_access_(self, user) -> bool:
        """Check if user has collaborator access to trigger"""
        if not self.triggered_origin_node or not self.triggering_flow_node:
            return False
        
        if not self.triggered_origin_node.has_collaborator_access_(user):
            return False
        
        return self.triggering_flow_node.has_collaborator_access_(user)
    
    def accessible_by_user_(self, user, access_level: str = 'collaborator') -> bool:
        """Check if trigger is accessible by user at given access level"""
        if access_level == 'admin':
            return self.has_admin_access_(user)
        elif access_level == 'operator':
            return self.has_operator_access_(user)
        else:
            return self.has_collaborator_access_(user)
    
    # Event handling methods
    def send_control_events_(self) -> None:
        """Send control events to resources if enabled"""
        if not self.control_messages_enabled_():
            return
        
        triggered_resource = self.triggered_resource()
        if triggered_resource and hasattr(triggered_resource, 'send_control_event'):
            triggered_resource.send_control_event('update')
        
        triggering_resource = self.triggering_resource()
        if triggering_resource and hasattr(triggering_resource, 'send_control_event'):
            triggering_resource.send_control_event('update')
    
    def disable_control_messages_(self) -> None:
        """Disable control message sending"""
        self.control_messages_enabled = False
    
    def enable_control_messages_(self) -> None:
        """Enable control message sending"""
        self.control_messages_enabled = True
    
    # Utility methods
    def event_type_display(self, event_type: str) -> str:
        """Get human-readable event type"""
        event_map = {
            'DATA_SOURCE_READ_START': 'Data Source Read Start',
            'DATA_SOURCE_READ_DONE': 'Data Source Read Done',
            'DATA_SINK_WRITE_DONE': 'Data Sink Write Done'
        }
        return event_map.get(event_type, event_type)
    
    def triggering_event_display(self) -> str:
        """Get human-readable triggering event"""
        return self.event_type_display(self.triggering_event_type or '')
    
    def triggered_event_display(self) -> str:
        """Get human-readable triggered event"""
        return self.event_type_display(self.triggered_event_type or '')
    
    def status_display(self) -> str:
        """Get human-readable status"""
        status_map = {
            FlowTriggerStatuses.ACTIVE: "Active",
            FlowTriggerStatuses.PAUSED: "Paused"
        }
        return status_map.get(self.status, self.status.value)
    
    def trigger_description(self) -> str:
        """Get human-readable trigger description"""
        triggering_res = self.triggering_resource()
        triggered_res = self.triggered_resource()
        
        triggering_name = triggering_res.name if triggering_res else "Unknown"
        triggered_name = triggered_res.name if triggered_res else "Unknown"
        
        return f"When {triggering_name} {self.triggering_event_display()}, trigger {triggered_name} {self.triggered_event_display()}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'triggering_flow_node_id': self.triggering_flow_node_id,
            'triggering_origin_node_id': self.triggering_origin_node_id,
            'triggered_origin_node_id': self.triggered_origin_node_id,
            'triggering_event_type': self.triggering_event_type,
            'triggered_event_type': self.triggered_event_type,
            'status': self.status.value if self.status else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Predicate methods
            'active': self.active_(),
            'paused': self.paused_(),
            'has_triggered_resource': self.has_triggered_resource_(),
            'has_triggering_resource': self.has_triggering_resource_(),
            'complete': self.complete_(),
            'incomplete': self.incomplete_(),
            'control_messages_enabled': self.control_messages_enabled_(),
            'same_origin': self.same_origin_(),
            'cross_origin': self.cross_origin_(),
            'data_source_trigger': self.data_source_trigger_(),
            'data_sink_trigger': self.data_sink_trigger_(),
            
            # Display values
            'status_display': self.status_display(),
            'triggering_event_display': self.triggering_event_display(),
            'triggered_event_display': self.triggered_event_display(),
            'trigger_description': self.trigger_description()
        }