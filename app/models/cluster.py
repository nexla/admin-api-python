from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, JSON, ForeignKey
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import json
import base64

from app.database import Base

class ClusterStatuses(PyEnum):
    INIT = "INIT"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"

class Cluster(Base):
    __tablename__ = "clusters"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    uid = Column(String(255), unique=True)
    description = Column(Text)
    status = Column(String(50), default=ClusterStatuses.ACTIVE.value)
    
    region = Column(String(255))
    provider = Column(String(255))
    is_default = Column(Boolean, default=False)
    is_private = Column(Boolean, default=True)
    
    config = Column(JSON)
    endpoint_url = Column(String(500))
    
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    org = relationship("Org", foreign_keys=[org_id], back_populates="clusters")
    cluster_endpoints = relationship("ClusterEndpoint", foreign_keys="ClusterEndpoint.cluster_id", back_populates="cluster", cascade="all, delete-orphan")
    
    # Class constants
    STATUSES = {
        'init': ClusterStatuses.INIT.value,
        'active': ClusterStatuses.ACTIVE.value, 
        'paused': ClusterStatuses.PAUSED.value
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    @validates('name')
    def validate_name(self, key, name):
        if not name or not name.strip():
            raise ValueError("Name is required")
        return name.strip()
    
    @validates('org_id')
    def validate_org_id(self, key, org_id):
        if not org_id:
            raise ValueError("Organization is required")
        return org_id
    
    @validates('is_default')
    def validate_is_default(self, key, is_default):
        if is_default:
            existing_default = self.get_default_cluster()
            if existing_default and existing_default.id != getattr(self, 'id', None):
                raise ValueError("Cannot create default cluster while one already exists")
        return is_default
    
    @validates('uid')
    def validate_uid(self, key, uid):
        if uid:
            existing_cluster = self.query.filter_by(uid=uid).first()
            if existing_cluster and existing_cluster.id != getattr(self, 'id', None):
                raise ValueError(f"A cluster with uid {uid} already exists")
        return uid
    
    @classmethod
    def get_default_cluster(cls):
        """Get the default cluster"""
        return cls.query.filter_by(is_default=True).first()
    
    @classmethod
    def get_default_endpoint(cls, service: str):
        """Get default cluster endpoint for service"""
        default_cluster = cls.get_default_cluster()
        if not default_cluster or not default_cluster.cluster_endpoints:
            return None
        
        return next(
            (ep for ep in default_cluster.cluster_endpoints if ep.service == service),
            None
        )
    
    @classmethod
    def get_infrastructure_access_key(cls, uid: Optional[str] = None) -> Optional[str]:
        """Get infrastructure access key for cluster"""
        if not uid:
            default_cluster = cls.get_default_cluster()
            uid = default_cluster.dataplane_key() if default_cluster else None
        
        if not uid:
            return None
        
        # In production, this would fetch from secrets management
        # Return mock value for now
        return f"infrastructure_key_{uid}"
    
    @classmethod
    def validate_cluster_for_org(cls, cluster_id: int, org_id: Optional[int] = None):
        """Validate cluster assignment for organization"""
        if not cluster_id:
            raise ValueError("A cluster id is required")
        
        cluster = cls.query.get(cluster_id)
        if not cluster:
            raise ValueError(f"Cluster not found: {cluster_id}")
        
        if org_id is None:
            # New orgs must be assigned to available cluster
            if not cluster.available_():
                raise ValueError(f"Cluster is either private or not active: {cluster.name}, {cluster.id}")
            return
        
        # Org being assigned to private cluster it owns
        if org_id == cluster.org_id:
            return
        
        # Check if assigning to available Nexla cluster
        if cluster.available_():
            return
        
        raise ValueError(f"Cannot assign org to cluster: {cluster_id}")
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any], endpoints: Optional[List[Dict[str, Any]]] = None) -> 'Cluster':
        """Factory method to create cluster from input data"""
        if input_data.get('is_default') and cls.get_default_cluster():
            raise ValueError("A default cluster already exists")
        
        # Validate org
        org_id = input_data.get('org_id')
        if not org_id:
            raise ValueError("Organization is required")
        
        from app.models.org import Org
        org = Org.query.get(org_id)
        if not org:
            raise ValueError("Organization not found")
        
        # Validate public cluster requirements
        if not input_data.get('is_private', True):
            if not org.is_nexla_admin_org_():
                raise ValueError("Public clusters must belong to Nexla org")
        
        cluster = cls(
            name=input_data['name'],
            uid=input_data.get('uid'),
            description=input_data.get('description'),
            region=input_data.get('region'),
            provider=input_data.get('provider'),
            is_default=input_data.get('is_default', False),
            is_private=input_data.get('is_private', True),
            config=input_data.get('config'),
            endpoint_url=input_data.get('endpoint_url'),
            org_id=org_id,
            status=input_data.get('status', ClusterStatuses.ACTIVE.value)
        )
        
        # Build endpoints if provided
        if endpoints and isinstance(endpoints, list):
            for endpoint_data in endpoints:
                endpoint_data['cluster_id'] = cluster.id
                endpoint_data['org_id'] = cluster.org_id
                # from app.models.cluster_endpoint import ClusterEndpoint
                # cluster.cluster_endpoints.append(ClusterEndpoint(**endpoint_data))
        
        return cluster
    
    def update_mutable_(self, input_data: Dict[str, Any], endpoints: Optional[List[Dict[str, Any]]] = None) -> None:
        """Update mutable attributes"""
        # Handle default cluster changes
        if 'is_default' in input_data:
            default_cluster = self.get_default_cluster()
            if default_cluster:
                if input_data['is_default'] and default_cluster.id != self.id:
                    raise ValueError("A default cluster already exists")
                if not input_data['is_default'] and default_cluster.id == self.id:
                    raise ValueError("Cannot change a cluster to non-default")
            self.is_default = input_data['is_default']
        
        # Update basic attributes
        if 'region' in input_data and input_data['region']:
            self.region = input_data['region']
        if 'provider' in input_data and input_data['provider']:
            self.provider = input_data['provider']
        if 'org_id' in input_data:
            self.org_id = input_data['org_id']
        if 'description' in input_data:
            self.description = input_data['description']
        if 'config' in input_data:
            self.config = input_data['config']
        if 'endpoint_url' in input_data:
            self.endpoint_url = input_data['endpoint_url']
        
        # Update endpoints
        if endpoints and isinstance(endpoints, list):
            self._update_endpoints(endpoints)
    
    def _update_endpoints(self, endpoints_data: List[Dict[str, Any]]) -> None:
        """Update cluster endpoints"""
        for ep_data in endpoints_data:
            service = ep_data.get('service')
            if not service:
                continue
            
            # Find existing endpoint by service
            existing_ep = next(
                (ep for ep in self.cluster_endpoints if ep.service == service),
                None
            )
            
            if existing_ep:
                # Don't allow changing cluster_id in this path
                ep_data.pop('cluster_id', None)
                existing_ep.update_mutable_(ep_data)
            else:
                ep_data['cluster_id'] = self.id
                ep_data['org_id'] = self.org_id
                # from app.models.cluster_endpoint import ClusterEndpoint
                # self.cluster_endpoints.append(ClusterEndpoint(**ep_data))
    
    # Predicate methods (Rails pattern)
    def active_(self) -> bool:
        """Check if cluster is active"""
        return self.status == ClusterStatuses.ACTIVE.value
    
    def paused_(self) -> bool:
        """Check if cluster is paused"""
        return self.status == ClusterStatuses.PAUSED.value
    
    def init_(self) -> bool:
        """Check if cluster is in init state"""
        return self.status == ClusterStatuses.INIT.value
    
    def default_(self) -> bool:
        """Check if cluster is default"""
        return self.is_default is True
    
    def private_(self) -> bool:
        """Check if cluster is private"""
        return self.is_private is True
    
    def public_(self) -> bool:
        """Check if cluster is public"""
        return not self.private_()
    
    def has_uid_(self) -> bool:
        """Check if cluster has UID"""
        return bool(self.uid and self.uid.strip())
    
    def has_description_(self) -> bool:
        """Check if cluster has description"""
        return bool(self.description and self.description.strip())
    
    def has_config_(self) -> bool:
        """Check if cluster has configuration"""
        return bool(self.config)
    
    def has_endpoints_(self) -> bool:
        """Check if cluster has endpoints"""
        return bool(self.cluster_endpoints)
    
    def has_region_(self) -> bool:
        """Check if cluster has region"""
        return bool(self.region and self.region.strip())
    
    def has_provider_(self) -> bool:
        """Check if cluster has provider"""
        return bool(self.provider and self.provider.strip())
    
    def has_endpoint_url_(self) -> bool:
        """Check if cluster has endpoint URL"""
        return bool(self.endpoint_url and self.endpoint_url.strip())
    
    def nexla_owned_(self) -> bool:
        """Check if cluster is owned by Nexla"""
        if not self.org:
            return False
        return self.org.is_nexla_admin_org_()
    
    def available_(self) -> bool:
        """Check if cluster is available for assignment"""
        # Available if Nexla-owned, active, and public
        return (self.nexla_owned_() and 
                self.active_() and 
                self.public_())
    
    def supports_multi_dataplane_(self) -> bool:
        """Check if cluster supports multi-dataplane"""
        if not self.has_uid_() or not self.name:
            return False
        return self.dataplane_key() != self.name.lower()
    
    def fully_configured_(self) -> bool:
        """Check if cluster is fully configured"""
        return (bool(self.name) and
                self.has_uid_() and
                self.has_endpoints_() and
                self.active_())
    
    def ready_for_use_(self) -> bool:
        """Check if cluster is ready for use"""
        return (self.fully_configured_() and
                (self.available_() or self.private_()))
    
    def configuration_incomplete_(self) -> bool:
        """Check if cluster configuration is incomplete"""
        return not self.fully_configured_()
    
    def needs_setup_(self) -> bool:
        """Check if cluster needs setup"""
        return (not self.has_endpoints_() or
                not self.active_() or
                not self.has_uid_())
    
    # State management methods (Rails pattern)
    def activate_(self) -> None:
        """Activate the cluster"""
        if not self.active_():
            # Should validate cluster has valid endpoints
            self.status = ClusterStatuses.ACTIVE.value
    
    def pause_(self) -> None:
        """Pause the cluster"""
        if not self.paused_():
            self.status = ClusterStatuses.PAUSED.value
    
    def set_default_(self) -> None:
        """Set as default cluster"""
        if not self.available_():
            raise ValueError(f"Private or inactive cluster cannot be made default: {self.name}, {self.id}")
        
        # In production, this would be done in a transaction
        current_default = self.get_default_cluster()
        if current_default and current_default.id != self.id:
            current_default.is_default = False
            # current_default.save()
        
        self.is_default = True
        # self.save()
    
    def make_private_(self) -> None:
        """Make cluster private"""
        if self.public_():
            self.is_private = True
    
    def make_public_(self) -> None:
        """Make cluster public"""
        if self.private_():
            if not self.nexla_owned_():
                raise ValueError("Only Nexla-owned clusters can be made public")
            self.is_private = False
    
    def toggle_privacy_(self) -> None:
        """Toggle privacy state"""
        if self.private_():
            self.make_public_()
        else:
            self.make_private_()
    
    def clear_config_(self) -> None:
        """Clear cluster configuration"""
        self.config = None
    
    def update_config_(self, new_config: Dict[str, Any]) -> None:
        """Update cluster configuration"""
        self.config = new_config
    
    def merge_config_(self, additional_config: Dict[str, Any]) -> None:
        """Merge additional configuration"""
        if self.config is None:
            self.config = {}
        
        if isinstance(self.config, dict) and isinstance(additional_config, dict):
            self.config.update(additional_config)
        else:
            self.config = additional_config
    
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
    
    # Utility methods
    def dataplane_key(self) -> Optional[str]:
        """Get dataplane key from UID"""
        return self.uid.lower() if self.uid else None
    
    def authorization_header(self) -> str:
        """Get authorization header for cluster"""
        # In production, fetch from secrets management
        dataplane_key = self.dataplane_key()
        if not dataplane_key:
            default_cluster = self.get_default_cluster()
            dataplane_key = default_cluster.dataplane_key() if default_cluster else None
        
        # Mock credentials - in production fetch from secrets
        username = f"cluster_{dataplane_key}" if dataplane_key else "default"
        password = f"password_{dataplane_key}" if dataplane_key else "default"
        
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        return f"Basic {credentials}"
    
    def script_config(self, resource_type: str) -> Dict[str, Any]:
        """Get script configuration for resource type"""
        key = f"script_{resource_type.replace('data_', '')}"
        
        # In production, fetch from dataplane services
        dataplane_key = self.dataplane_key()
        if not dataplane_key:
            return {}
        
        # Mock configuration
        return {
            'service_url': f"https://{dataplane_key}.example.com",
            'version': '1.0',
            'timeout': 30
        }
    
    def endpoint_count(self) -> int:
        """Get count of cluster endpoints"""
        return len(self.cluster_endpoints) if self.cluster_endpoints else 0
    
    def active_endpoints(self) -> List:
        """Get active cluster endpoints"""
        if not self.cluster_endpoints:
            return []
        return [ep for ep in self.cluster_endpoints if hasattr(ep, 'active_') and ep.active_()]
    
    def get_endpoint_for_service(self, service: str):
        """Get endpoint for specific service"""
        if not self.cluster_endpoints:
            return None
        return next(
            (ep for ep in self.cluster_endpoints if ep.service == service),
            None
        )
    
    def supports_service_(self, service: str) -> bool:
        """Check if cluster supports given service"""
        return self.get_endpoint_for_service(service) is not None
    
    # Display methods
    def status_display(self) -> str:
        """Get human-readable status"""
        status_map = {
            ClusterStatuses.INIT.value: "Initializing",
            ClusterStatuses.ACTIVE.value: "Active", 
            ClusterStatuses.PAUSED.value: "Paused"
        }
        return status_map.get(self.status, self.status)
    
    def privacy_display(self) -> str:
        """Get privacy status display"""
        return "Private" if self.private_() else "Public"
    
    def ownership_display(self) -> str:
        """Get ownership display"""
        return "Nexla-owned" if self.nexla_owned_() else "Customer-owned"
    
    def availability_display(self) -> str:
        """Get availability display"""
        return "Available" if self.available_() else "Not available"
    
    def config_summary(self) -> str:
        """Get configuration summary"""
        if not self.has_config_():
            return "No configuration"
        
        if isinstance(self.config, dict):
            count = len(self.config)
            return f"{count} configuration {'item' if count == 1 else 'items'}"
        
        return "Custom configuration"
    
    def endpoint_summary(self) -> str:
        """Get endpoint summary"""
        count = self.endpoint_count()
        if count == 0:
            return "No endpoints"
        return f"{count} {'endpoint' if count == 1 else 'endpoints'}"
    
    def region_provider_summary(self) -> str:
        """Get region and provider summary"""
        parts = []
        if self.has_region_():
            parts.append(f"Region: {self.region}")
        if self.has_provider_():
            parts.append(f"Provider: {self.provider}")
        return " | ".join(parts) if parts else "No region/provider info"
    
    def cluster_summary(self) -> str:
        """Get complete cluster summary"""
        parts = [
            self.status_display(),
            self.privacy_display(),
            self.ownership_display()
        ]
        
        if self.default_():
            parts.append("Default")
        
        if self.has_endpoints_():
            parts.append(self.endpoint_summary())
        
        if self.has_region_() or self.has_provider_():
            parts.append(self.region_provider_summary())
        
        return " | ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'name': self.name,
            'uid': self.uid,
            'description': self.description,
            'status': self.status,
            'region': self.region,
            'provider': self.provider,
            'is_default': self.is_default,
            'is_private': self.is_private,
            'config': self.config,
            'endpoint_url': self.endpoint_url,
            'org_id': self.org_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Predicate methods
            'active': self.active_(),
            'paused': self.paused_(),
            'init': self.init_(),
            'default': self.default_(),
            'private': self.private_(),
            'public': self.public_(),
            'has_uid': self.has_uid_(),
            'has_description': self.has_description_(),
            'has_config': self.has_config_(),
            'has_endpoints': self.has_endpoints_(),
            'has_region': self.has_region_(),
            'has_provider': self.has_provider_(),
            'has_endpoint_url': self.has_endpoint_url_(),
            'nexla_owned': self.nexla_owned_(),
            'available': self.available_(),
            'supports_multi_dataplane': self.supports_multi_dataplane_(),
            'fully_configured': self.fully_configured_(),
            'ready_for_use': self.ready_for_use_(),
            'configuration_incomplete': self.configuration_incomplete_(),
            'needs_setup': self.needs_setup_(),
            
            # Counts and derived data
            'endpoint_count': self.endpoint_count(),
            'dataplane_key': self.dataplane_key(),
            
            # Display values
            'status_display': self.status_display(),
            'privacy_display': self.privacy_display(),
            'ownership_display': self.ownership_display(),
            'availability_display': self.availability_display(),
            'config_summary': self.config_summary(),
            'endpoint_summary': self.endpoint_summary(),
            'region_provider_summary': self.region_provider_summary(),
            'cluster_summary': self.cluster_summary()
        }