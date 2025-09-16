"""
Connector Model - Data connectivity and integration management entity.
Handles external system connections and integration patterns with comprehensive Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import uuid
import os
from ..database import Base


class ConnectorStatuses(PyEnum):
    """Connector status enumeration"""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DRAFT = "DRAFT"
    TESTING = "TESTING"
    CONFIGURED = "CONFIGURED"
    ERROR = "ERROR"
    DEPRECATED = "DEPRECATED"
    ARCHIVED = "ARCHIVED"
    MAINTENANCE = "MAINTENANCE"


class ConnectorTypes(PyEnum):
    """Connector type enumeration"""
    S3 = "s3"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    ORACLE = "oracle"
    MONGODB = "mongodb"
    ELASTICSEARCH = "elasticsearch"
    KAFKA = "kafka"
    REDIS = "redis"
    REST_API = "rest_api"
    WEBHOOK = "webhook"
    FTP = "ftp"
    SFTP = "sftp"
    HDFS = "hdfs"
    CASSANDRA = "cassandra"
    DYNAMODB = "dynamodb"
    AZURE_BLOB = "azure_blob"
    GCS = "gcs"
    CUSTOM = "custom"


class ConnectionTypes(PyEnum):
    """Connection type enumeration"""
    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAMING = "streaming"
    CLOUD = "cloud"
    MESSAGING = "messaging"
    CACHE = "cache"
    WAREHOUSE = "warehouse"


class IngestionModes(PyEnum):
    """Ingestion mode enumeration"""
    SAMPLING = "sampling"
    FULL_INGESTION = "full_ingestion"
    INCREMENTAL = "incremental"
    STREAMING = "streaming"
    BATCH = "batch"
    REALTIME = "realtime"


class AuthTypes(PyEnum):
    """Authentication type enumeration"""
    NONE = "none"
    BASIC = "basic"
    OAUTH2 = "oauth2"
    API_KEY = "api_key"
    TOKEN = "token"
    CERTIFICATE = "certificate"
    IAM = "iam"
    CUSTOM = "custom"


class Connector(Base):
    __tablename__ = "connectors"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    type = Column(SQLEnum(ConnectorTypes), nullable=False, index=True)
    connection_type = Column(SQLEnum(ConnectionTypes), nullable=False, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    version = Column(String(50), default="1.0")
    status = Column(SQLEnum(ConnectorStatuses), default=ConnectorStatuses.DRAFT, nullable=False, index=True)
    
    # Configuration and capabilities
    config_schema = Column(JSON)  # JSON schema for configuration
    default_config = Column(JSON)  # Default configuration values
    capabilities = Column(JSON)   # Supported features and operations
    limitations = Column(JSON)    # Known limitations and restrictions
    
    # Authentication and security
    auth_type = Column(SQLEnum(AuthTypes), default=AuthTypes.NONE)
    auth_config = Column(JSON)    # Authentication configuration template
    encryption_supported = Column(Boolean, default=False)
    ssl_required = Column(Boolean, default=False)
    
    # Ingestion and processing
    ingestion_mode = Column(SQLEnum(IngestionModes), default=IngestionModes.FULL_INGESTION, nullable=False)
    supported_formats = Column(JSON)  # Supported data formats
    batch_size_limit = Column(Integer, default=1000)
    rate_limit_per_second = Column(Integer, default=100)
    
    # API and compatibility
    nexset_api_compatible = Column(Boolean, default=False, index=True)
    api_version = Column(String(50))
    sdk_version = Column(String(50))
    driver_version = Column(String(50))
    
    # Performance and reliability
    connection_timeout_ms = Column(Integer, default=30000)
    read_timeout_ms = Column(Integer, default=60000)
    retry_attempts = Column(Integer, default=3)
    health_check_interval_minutes = Column(Integer, default=15)
    
    # Usage tracking
    usage_count = Column(Integer, default=0)
    last_used_at = Column(DateTime)
    error_count = Column(Integer, default=0)
    last_error_at = Column(DateTime)
    
    # Metadata and tags
    tags = Column(JSON)
    extra_metadata = Column(JSON)
    documentation_url = Column(String(500))
    support_url = Column(String(500))
    
    # State flags
    is_system = Column(Boolean, default=False)
    is_custom = Column(Boolean, default=False)
    is_deprecated = Column(Boolean, default=False)
    is_beta = Column(Boolean, default=False)
    requires_approval = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    last_health_check_at = Column(DateTime)
    
    # Foreign keys
    created_by_id = Column(Integer, ForeignKey("users.id"), index=True)
    
    # Relationships
    created_by = relationship("User", foreign_keys=[created_by_id])
    auth_templates = relationship("AuthTemplate", back_populates="connector")
    data_sources = relationship("DataSource", back_populates="connector")
    data_sinks = relationship("DataSink", back_populates="connector")
    
    # Rails business logic constants
    CONFIG_CACHE_TTL_SECONDS = 300
    HEALTH_CHECK_TIMEOUT_MS = 5000
    MAX_ERROR_COUNT = 100
    BATCH_SIZE_LIMITS = {
        'small': 100,
        'medium': 1000,
        'large': 10000
    }
    PERFORMANCE_THRESHOLDS = {
        'connection_timeout_warning_ms': 10000,
        'read_timeout_warning_ms': 30000
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self._config_cache = {}
        self._health_status = {}
        self._performance_metrics = {}
    
    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if connector is active (Rails pattern)"""
        return self.status == ConnectorStatuses.ACTIVE and not self.is_deprecated
    
    def inactive_(self) -> bool:
        """Check if connector is inactive (Rails pattern)"""
        return self.status == ConnectorStatuses.INACTIVE
    
    def draft_(self) -> bool:
        """Check if connector is in draft state (Rails pattern)"""
        return self.status == ConnectorStatuses.DRAFT
    
    def testing_(self) -> bool:
        """Check if connector is being tested (Rails pattern)"""
        return self.status == ConnectorStatuses.TESTING
    
    def configured_(self) -> bool:
        """Check if connector is configured (Rails pattern)"""
        return self.status == ConnectorStatuses.CONFIGURED
    
    def error_(self) -> bool:
        """Check if connector has errors (Rails pattern)"""
        return self.status == ConnectorStatuses.ERROR
    
    def deprecated_(self) -> bool:
        """Check if connector is deprecated (Rails pattern)"""
        return self.status == ConnectorStatuses.DEPRECATED or self.is_deprecated
    
    def archived_(self) -> bool:
        """Check if connector is archived (Rails pattern)"""
        return self.status == ConnectorStatuses.ARCHIVED
    
    def maintenance_(self) -> bool:
        """Check if connector is under maintenance (Rails pattern)"""
        return self.status == ConnectorStatuses.MAINTENANCE
    
    def healthy_(self) -> bool:
        """Check if connector is healthy (Rails pattern)"""
        return (self.active_() and 
                self.error_count < self.MAX_ERROR_COUNT and
                not self.maintenance_())
    
    def system_(self) -> bool:
        """Check if connector is system-managed (Rails pattern)"""
        return self.is_system is True
    
    def custom_(self) -> bool:
        """Check if connector is custom (Rails pattern)"""
        return self.is_custom is True
    
    def beta_(self) -> bool:
        """Check if connector is in beta (Rails pattern)"""
        return self.is_beta is True
    
    def requires_approval_(self) -> bool:
        """Check if connector requires approval (Rails pattern)"""
        return self.requires_approval is True
    
    def nexset_api_compatible_(self) -> bool:
        """Check if connector is nexset API compatible (Rails pattern)"""
        return self.nexset_api_compatible is True
    
    def database_connector_(self) -> bool:
        """Check if connector is database type (Rails pattern)"""
        return self.connection_type == ConnectionTypes.DATABASE
    
    def file_connector_(self) -> bool:
        """Check if connector is file type (Rails pattern)"""
        return self.connection_type == ConnectionTypes.FILE
    
    def api_connector_(self) -> bool:
        """Check if connector is API type (Rails pattern)"""
        return self.connection_type == ConnectionTypes.API
    
    def streaming_connector_(self) -> bool:
        """Check if connector is streaming type (Rails pattern)"""
        return self.connection_type == ConnectionTypes.STREAMING
    
    def cloud_connector_(self) -> bool:
        """Check if connector is cloud type (Rails pattern)"""
        return self.connection_type == ConnectionTypes.CLOUD
    
    def messaging_connector_(self) -> bool:
        """Check if connector is messaging type (Rails pattern)"""
        return self.connection_type == ConnectionTypes.MESSAGING
    
    def warehouse_connector_(self) -> bool:
        """Check if connector is warehouse type (Rails pattern)"""
        return self.connection_type == ConnectionTypes.WAREHOUSE
    
    def sampling_mode_(self) -> bool:
        """Check if connector uses sampling ingestion (Rails pattern)"""
        return self.ingestion_mode == IngestionModes.SAMPLING
    
    def full_ingestion_mode_(self) -> bool:
        """Check if connector uses full ingestion (Rails pattern)"""
        return self.ingestion_mode == IngestionModes.FULL_INGESTION
    
    def incremental_mode_(self) -> bool:
        """Check if connector uses incremental ingestion (Rails pattern)"""
        return self.ingestion_mode == IngestionModes.INCREMENTAL
    
    def streaming_mode_(self) -> bool:
        """Check if connector uses streaming ingestion (Rails pattern)"""
        return self.ingestion_mode == IngestionModes.STREAMING
    
    def batch_mode_(self) -> bool:
        """Check if connector uses batch processing (Rails pattern)"""
        return self.ingestion_mode == IngestionModes.BATCH
    
    def realtime_mode_(self) -> bool:
        """Check if connector uses realtime processing (Rails pattern)"""
        return self.ingestion_mode == IngestionModes.REALTIME
    
    def requires_auth_(self) -> bool:
        """Check if connector requires authentication (Rails pattern)"""
        return self.auth_type != AuthTypes.NONE
    
    def oauth_auth_(self) -> bool:
        """Check if connector uses OAuth authentication (Rails pattern)"""
        return self.auth_type == AuthTypes.OAUTH2
    
    def api_key_auth_(self) -> bool:
        """Check if connector uses API key authentication (Rails pattern)"""
        return self.auth_type == AuthTypes.API_KEY
    
    def basic_auth_(self) -> bool:
        """Check if connector uses basic authentication (Rails pattern)"""
        return self.auth_type == AuthTypes.BASIC
    
    def ssl_required_(self) -> bool:
        """Check if connector requires SSL (Rails pattern)"""
        return self.ssl_required is True
    
    def encryption_supported_(self) -> bool:
        """Check if connector supports encryption (Rails pattern)"""
        return self.encryption_supported is True
    
    def has_errors_(self) -> bool:
        """Check if connector has errors (Rails pattern)"""
        return self.error_count > 0
    
    def performance_issues_(self) -> bool:
        """Check if connector has performance issues (Rails pattern)"""
        return (self.connection_timeout_ms > self.PERFORMANCE_THRESHOLDS['connection_timeout_warning_ms'] or
                self.read_timeout_ms > self.PERFORMANCE_THRESHOLDS['read_timeout_warning_ms'])
    
    def needs_health_check_(self) -> bool:
        """Check if connector needs health check (Rails pattern)"""
        if not self.last_health_check_at:
            return True
        
        threshold = datetime.now() - timedelta(minutes=self.health_check_interval_minutes)
        return self.last_health_check_at < threshold
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if connector was recently used (Rails pattern)"""
        if not self.last_used_at:
            return False
        
        threshold = datetime.now() - timedelta(hours=hours)
        return self.last_used_at > threshold
    
    def can_be_used_(self) -> bool:
        """Check if connector can be used (Rails pattern)"""
        return (self.active_() and 
                not self.maintenance_() and 
                not self.deprecated_() and
                self.error_count < self.MAX_ERROR_COUNT)
    
    def can_be_tested_(self) -> bool:
        """Check if connector can be tested (Rails pattern)"""
        return self.status in [ConnectorStatuses.DRAFT, ConnectorStatuses.CONFIGURED, ConnectorStatuses.ACTIVE]
    
    def can_be_deleted_(self) -> bool:
        """Check if connector can be deleted (Rails pattern)"""
        return not self.system_() and self.usage_count == 0
    
    def supports_format_(self, format_name: str) -> bool:
        """Check if connector supports specific format (Rails pattern)"""
        return bool(self.supported_formats and format_name.lower() in 
                   [f.lower() for f in self.supported_formats])
    
    def has_capability_(self, capability: str) -> bool:
        """Check if connector has specific capability (Rails pattern)"""
        return bool(self.capabilities and capability in self.capabilities)
    
    def has_limitation_(self, limitation: str) -> bool:
        """Check if connector has specific limitation (Rails pattern)"""
        return bool(self.limitations and limitation in self.limitations)
    
    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self) -> None:
        """Activate connector (Rails bang method pattern)"""
        if self.active_():
            return
        
        # Validate connector can be activated
        if self.deprecated_():
            raise ValueError("Cannot activate deprecated connector")
        
        self.status = ConnectorStatuses.ACTIVE
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate connector (Rails bang method pattern)"""
        if self.inactive_():
            return
        
        self.status = ConnectorStatuses.INACTIVE
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deactivation_reason'] = reason
            self.extra_metadata['deactivated_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def configure_(self, config_data: Dict[str, Any]) -> None:
        """Configure connector (Rails bang method pattern)"""
        # Validate configuration against schema if available
        if self.config_schema:
            # In a real implementation, this would validate against JSON schema
            pass
        
        self.status = ConnectorStatuses.CONFIGURED
        self.default_config = {**(self.default_config or {}), **config_data}
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def start_testing_(self) -> None:
        """Start testing connector (Rails bang method pattern)"""
        if not self.can_be_tested_():
            raise ValueError(f"Connector cannot be tested in {self.status} status")
        
        self.status = ConnectorStatuses.TESTING
        self.updated_at = datetime.now()
    
    def complete_testing_(self, success: bool) -> None:
        """Complete testing connector (Rails bang method pattern)"""
        if not self.testing_():
            return
        
        if success:
            self.status = ConnectorStatuses.CONFIGURED
        else:
            self.status = ConnectorStatuses.ERROR
            self.error_count += 1
        
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def mark_error_(self, error_message: str = None) -> None:
        """Mark connector as having error (Rails bang method pattern)"""
        self.status = ConnectorStatuses.ERROR
        self.error_count += 1
        self.last_error_at = datetime.now()
        self.updated_at = datetime.now()
        
        if error_message:
            self.extra_metadata = self.extra_metadata or {}
            errors = self.extra_metadata.get('recent_errors', [])
            errors.append({
                'message': error_message,
                'timestamp': datetime.now().isoformat()
            })
            # Keep only recent errors
            self.extra_metadata['recent_errors'] = errors[-10:]
        
        self._clear_cache()
    
    def clear_errors_(self) -> None:
        """Clear connector errors (Rails bang method pattern)"""
        self.error_count = 0
        self.last_error_at = None
        
        if self.error_():
            self.status = ConnectorStatuses.ACTIVE
        
        if self.extra_metadata and 'recent_errors' in self.extra_metadata:
            del self.extra_metadata['recent_errors']
        
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def deprecate_(self, reason: str = None) -> None:
        """Deprecate connector (Rails bang method pattern)"""
        if self.deprecated_():
            return
        
        self.status = ConnectorStatuses.DEPRECATED
        self.is_deprecated = True
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deprecation_reason'] = reason
            self.extra_metadata['deprecated_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def archive_(self) -> None:
        """Archive connector (Rails bang method pattern)"""
        if self.archived_():
            return
        
        self.status = ConnectorStatuses.ARCHIVED
        self.updated_at = datetime.now()
        self._clear_cache()
    
    def enter_maintenance_(self, reason: str = None) -> None:
        """Enter maintenance mode (Rails bang method pattern)"""
        if self.maintenance_():
            return
        
        self.status = ConnectorStatuses.MAINTENANCE
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['maintenance_reason'] = reason
            self.extra_metadata['maintenance_started_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def exit_maintenance_(self) -> None:
        """Exit maintenance mode (Rails bang method pattern)"""
        if not self.maintenance_():
            return
        
        self.status = ConnectorStatuses.ACTIVE
        self.updated_at = datetime.now()
        
        if self.extra_metadata:
            self.extra_metadata.pop('maintenance_reason', None)
            self.extra_metadata.pop('maintenance_started_at', None)
            self.extra_metadata['maintenance_ended_at'] = datetime.now().isoformat()
        
        self._clear_cache()
    
    def increment_usage_(self) -> None:
        """Increment usage counter (Rails bang method pattern)"""
        self.usage_count += 1
        self.last_used_at = datetime.now()
        self.updated_at = datetime.now()
    
    def update_health_check_(self, status: str, details: Dict[str, Any] = None) -> None:
        """Update health check status (Rails bang method pattern)"""
        self.last_health_check_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['health_status'] = {
            'status': status,
            'checked_at': datetime.now().isoformat(),
            'details': details or {}
        }
        
        self.updated_at = datetime.now()
    
    def add_capability_(self, capability: str) -> None:
        """Add capability to connector (Rails bang method pattern)"""
        if not self.capabilities:
            self.capabilities = []
        
        if capability not in self.capabilities:
            self.capabilities.append(capability)
            self.updated_at = datetime.now()
    
    def remove_capability_(self, capability: str) -> None:
        """Remove capability from connector (Rails bang method pattern)"""
        if self.capabilities and capability in self.capabilities:
            self.capabilities.remove(capability)
            self.updated_at = datetime.now()
    
    def add_supported_format_(self, format_name: str) -> None:
        """Add supported format (Rails bang method pattern)"""
        if not self.supported_formats:
            self.supported_formats = []
        
        if format_name not in self.supported_formats:
            self.supported_formats.append(format_name)
            self.updated_at = datetime.now()
    
    def remove_supported_format_(self, format_name: str) -> None:
        """Remove supported format (Rails bang method pattern)"""
        if self.supported_formats and format_name in self.supported_formats:
            self.supported_formats.remove(format_name)
            self.updated_at = datetime.now()
    
    def add_tag_(self, tag_name: str) -> None:
        """Add tag to connector (Rails bang method pattern)"""
        if not self.tags:
            self.tags = []
        if tag_name not in self.tags:
            self.tags.append(tag_name)
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag_name: str) -> None:
        """Remove tag from connector (Rails bang method pattern)"""
        if self.tags and tag_name in self.tags:
            self.tags.remove(tag_name)
            self.updated_at = datetime.now()
    
    # ========================================
    # Rails Class Methods and Scopes
    # ========================================
    
    @classmethod
    def active(cls):
        """Scope for active connectors (Rails scope pattern)"""
        from sqlalchemy import and_
        return and_(cls.status == ConnectorStatuses.ACTIVE, cls.is_deprecated.is_(False))
    
    @classmethod
    def inactive(cls):
        """Scope for inactive connectors (Rails scope pattern)"""
        return cls.status == ConnectorStatuses.INACTIVE
    
    @classmethod
    def deprecated(cls):
        """Scope for deprecated connectors (Rails scope pattern)"""
        from sqlalchemy import or_
        return or_(cls.status == ConnectorStatuses.DEPRECATED, cls.is_deprecated.is_(True))
    
    @classmethod
    def by_type(cls, connector_type: ConnectorTypes):
        """Scope for connectors by type (Rails scope pattern)"""
        return cls.type == connector_type
    
    @classmethod
    def by_connection_type(cls, connection_type: ConnectionTypes):
        """Scope for connectors by connection type (Rails scope pattern)"""
        return cls.connection_type == connection_type
    
    @classmethod
    def nexset_api_compatible(cls):
        """Scope for nexset API compatible connectors (Rails scope pattern)"""
        return cls.nexset_api_compatible.is_(True)
    
    @classmethod
    def database_connectors(cls):
        """Scope for database connectors (Rails scope pattern)"""
        return cls.connection_type == ConnectionTypes.DATABASE
    
    @classmethod
    def cloud_connectors(cls):
        """Scope for cloud connectors (Rails scope pattern)"""
        return cls.connection_type == ConnectionTypes.CLOUD
    
    @classmethod
    def streaming_connectors(cls):
        """Scope for streaming connectors (Rails scope pattern)"""
        return cls.connection_type == ConnectionTypes.STREAMING
    
    @classmethod
    def with_errors(cls):
        """Scope for connectors with errors (Rails scope pattern)"""
        return cls.error_count > 0
    
    @classmethod
    def recently_used(cls, hours: int = 24):
        """Scope for recently used connectors (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return cls.last_used_at >= cutoff
    
    @classmethod
    def needs_health_check(cls):
        """Scope for connectors needing health check (Rails scope pattern)"""
        from sqlalchemy import or_
        cutoff = datetime.now() - timedelta(minutes=15)  # Default health check interval
        return or_(
            cls.last_health_check_at.is_(None),
            cls.last_health_check_at < cutoff
        )
    
    @classmethod
    def beta_connectors(cls):
        """Scope for beta connectors (Rails scope pattern)"""
        return cls.is_beta.is_(True)
    
    @classmethod
    def system_connectors(cls):
        """Scope for system connectors (Rails scope pattern)"""
        return cls.is_system.is_(True)
    
    @classmethod
    def custom_connectors(cls):
        """Scope for custom connectors (Rails scope pattern)"""
        return cls.is_custom.is_(True)
    
    @classmethod
    def find_by_type(cls, connector_type: str):
        """Find connector by type string (Rails pattern)"""
        # Implementation would query the database
        # return session.query(cls).filter(cls.type == connector_type).first()
        return None
    
    @classmethod
    def default_connection_type(cls):
        """Get default S3 connector (Rails pattern)"""
        return cls.find_by_type("s3")
    
    @classmethod
    def all_types_hash(cls) -> Dict[str, str]:
        """Get all connector types as hash (Rails pattern)"""
        return {ct.value: ct.value for ct in ConnectorTypes}
    
    @classmethod
    def load_connectors_from_config(cls, config_path: str = None) -> List[Dict[str, Any]]:
        """Load connectors from JSON config file (Rails pattern)"""
        if not config_path:
            config_path = "/config/api/connectors.json"
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as file:
                    return json.load(file)
        except Exception as e:
            # Log error in production
            pass
        
        return []
    
    @classmethod
    def create_with_defaults(cls, connector_type: ConnectorTypes, connection_type: ConnectionTypes, 
                           name: str, **kwargs):
        """Factory method to create connector with defaults (Rails pattern)"""
        connector_data = {
            'type': connector_type,
            'connection_type': connection_type,
            'name': name,
            'status': ConnectorStatuses.DRAFT,
            'ingestion_mode': IngestionModes.FULL_INGESTION,
            'auth_type': AuthTypes.NONE,
            'nexset_api_compatible': False,
            **kwargs
        }
        
        return cls(**connector_data)
    
    @classmethod
    def create_from_template(cls, template_data: Dict[str, Any], name: str, **overrides):
        """Factory method to create connector from template (Rails pattern)"""
        connector_data = template_data.copy()
        connector_data.update({
            'name': name,
            'status': ConnectorStatuses.DRAFT,
            **overrides
        })
        
        return cls(**connector_data)
    
    @classmethod
    def bulk_activate(cls, connector_ids: List[int]):
        """Bulk activate connectors (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_deprecate(cls, connector_ids: List[int], reason: str = None):
        """Bulk deprecate connectors (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    # ========================================
    # Rails Instance Methods
    # ========================================
    
    def build_from_input(self, api_user_info: Dict[str, Any], input_data: Dict[str, Any]):
        """Build connector from input data (Rails pattern)"""
        if not input_data:
            raise ValueError("Connector input missing")
        
        self.set_defaults()
        self.update_mutable(input_data)
        return self
    
    def update_mutable(self, input_data: Dict[str, Any]) -> None:
        """Update mutable fields from input (Rails pattern)"""
        if not input_data:
            return
        
        # Validate and update ingestion mode
        if 'ingestion_mode' in input_data:
            try:
                ingestion_mode = IngestionModes(input_data['ingestion_mode'])
                self.ingestion_mode = ingestion_mode
            except ValueError:
                raise ValueError(f"Invalid ingestion_mode. Valid values: {[m.value for m in IngestionModes]}")
        
        # Update basic fields
        updatable_fields = ['name', 'description', 'nexset_api_compatible', 'version',
                          'batch_size_limit', 'rate_limit_per_second', 'documentation_url',
                          'support_url', 'is_beta']
        
        for field in updatable_fields:
            if field in input_data:
                setattr(self, field, input_data[field])
        
        # Update JSON fields
        json_fields = ['capabilities', 'limitations', 'supported_formats', 'tags']
        for field in json_fields:
            if field in input_data:
                setattr(self, field, input_data[field])
        
        self.updated_at = datetime.now()
    
    def set_defaults(self) -> None:
        """Set default values (Rails pattern)"""
        if not self.ingestion_mode:
            self.ingestion_mode = IngestionModes.FULL_INGESTION
        
        if self.nexset_api_compatible is None:
            self.nexset_api_compatible = False
        
        if not self.auth_type:
            self.auth_type = AuthTypes.NONE
        
        if not self.version:
            self.version = "1.0"
        
        if not self.batch_size_limit:
            self.batch_size_limit = self.BATCH_SIZE_LIMITS['medium']
        
        if not self.rate_limit_per_second:
            self.rate_limit_per_second = 100
    
    def get_config_schema(self) -> Optional[Dict[str, Any]]:
        """Get configuration schema with caching (Rails pattern)"""
        cache_key = 'config_schema'
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]
        
        schema = self.config_schema
        self._config_cache[cache_key] = schema
        return schema
    
    def get_effective_config(self, user_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get effective configuration merging defaults with user config (Rails pattern)"""
        effective_config = self.default_config.copy() if self.default_config else {}
        
        if user_config:
            effective_config.update(user_config)
        
        return effective_config
    
    def test_connection(self, config: Dict[str, Any] = None) -> Tuple[bool, str]:
        """Test connector connection (Rails pattern)"""
        # This would implement actual connection testing
        # For now, return success for non-error connectors
        if self.error_():
            return False, "Connector is in error state"
        
        if not self.can_be_tested_():
            return False, f"Connector cannot be tested in {self.status.value} status"
        
        # Simulate connection test
        return True, "Connection test successful"
    
    def get_usage_statistics(self) -> Dict[str, Any]:
        """Get usage statistics (Rails pattern)"""
        return {
            'usage_count': self.usage_count,
            'error_count': self.error_count,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'last_error_at': self.last_error_at.isoformat() if self.last_error_at else None,
            'recently_used': self.recently_used_(),
            'has_errors': self.has_errors_(),
            'error_rate_percent': (self.error_count / max(self.usage_count, 1)) * 100
        }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics (Rails pattern)"""
        return {
            'connection_timeout_ms': self.connection_timeout_ms,
            'read_timeout_ms': self.read_timeout_ms,
            'batch_size_limit': self.batch_size_limit,
            'rate_limit_per_second': self.rate_limit_per_second,
            'retry_attempts': self.retry_attempts,
            'performance_issues': self.performance_issues_(),
            'health_check_interval_minutes': self.health_check_interval_minutes,
            'needs_health_check': self.needs_health_check_()
        }
    
    def get_capabilities_list(self) -> List[str]:
        """Get list of capabilities (Rails pattern)"""
        return self.capabilities or []
    
    def get_limitations_list(self) -> List[str]:
        """Get list of limitations (Rails pattern)"""
        return self.limitations or []
    
    def get_supported_formats_list(self) -> List[str]:
        """Get list of supported formats (Rails pattern)"""
        return self.supported_formats or []
    
    def has_tag(self, tag_name: str) -> bool:
        """Check if connector has specific tag (Rails pattern)"""
        return bool(self.tags and tag_name in self.tags)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        return self.tags or []
    
    def _clear_cache(self) -> None:
        """Clear internal cache (Rails private pattern)"""
        self._config_cache.clear()
        self._health_status.clear()
        self._performance_metrics.clear()
    
    # ========================================
    # Rails Validation and Display Methods
    # ========================================
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        return self.name or f"{self.type.value.title()} Connector #{self.id}"
    
    def display_status(self) -> str:
        """Get formatted status for display (Rails pattern)"""
        return self.status.value.replace('_', ' ').title()
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            ConnectorStatuses.ACTIVE: 'green',
            ConnectorStatuses.CONFIGURED: 'blue',
            ConnectorStatuses.TESTING: 'yellow',
            ConnectorStatuses.DRAFT: 'gray',
            ConnectorStatuses.ERROR: 'red',
            ConnectorStatuses.DEPRECATED: 'orange',
            ConnectorStatuses.ARCHIVED: 'gray',
            ConnectorStatuses.MAINTENANCE: 'purple'
        }
        return status_colors.get(self.status, 'gray')
    
    def type_display(self) -> str:
        """Get formatted type for display (Rails pattern)"""
        return self.type.value.replace('_', ' ').title()
    
    def connection_type_display(self) -> str:
        """Get formatted connection type for display (Rails pattern)"""
        return self.connection_type.value.replace('_', ' ').title()
    
    def validate_for_activation(self) -> Tuple[bool, List[str]]:
        """Validate connector can be activated (Rails pattern)"""
        errors = []
        
        if self.active_():
            errors.append("Connector is already active")
        
        if self.deprecated_():
            errors.append("Cannot activate deprecated connector")
        
        if not self.name:
            errors.append("Name is required")
        
        if not self.type:
            errors.append("Type is required")
        
        if not self.connection_type:
            errors.append("Connection type is required")
        
        return len(errors) == 0, errors
    
    # ========================================
    # Rails API and Serialization Methods
    # ========================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for basic API responses (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'type': self.type.value,
            'type_display': self.type_display(),
            'connection_type': self.connection_type.value,
            'connection_type_display': self.connection_type_display(),
            'name': self.name,
            'description': self.description,
            'version': self.version,
            'status': self.status.value,
            'display_status': self.display_status(),
            'status_color': self.status_color(),
            'ingestion_mode': self.ingestion_mode.value,
            'auth_type': self.auth_type.value,
            'nexset_api_compatible': self.nexset_api_compatible,
            'active': self.active_(),
            'healthy': self.healthy_(),
            'deprecated': self.deprecated_(),
            'beta': self.beta_(),
            'system': self.system_(),
            'custom': self.custom_(),
            'requires_auth': self.requires_auth_(),
            'ssl_required': self.ssl_required_(),
            'recently_used': self.recently_used_(),
            'has_errors': self.has_errors_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'tags': self.tags_list()
        }
    
    def to_detailed_dict(self) -> Dict[str, Any]:
        """Convert to detailed dictionary for full API responses (Rails pattern)"""
        base_dict = self.to_dict()
        
        detailed_info = {
            'api_version': self.api_version,
            'sdk_version': self.sdk_version,
            'driver_version': self.driver_version,
            'created_by_id': self.created_by_id,
            'capabilities': self.get_capabilities_list(),
            'limitations': self.get_limitations_list(),
            'supported_formats': self.get_supported_formats_list(),
            'usage_statistics': self.get_usage_statistics(),
            'performance_metrics': self.get_performance_metrics(),
            'documentation_url': self.documentation_url,
            'support_url': self.support_url,
            'metadata': self.extra_metadata,
            'last_health_check_at': self.last_health_check_at.isoformat() if self.last_health_check_at else None,
            'relationships': {
                'created_by_name': self.created_by.name if self.created_by else None,
                'auth_templates_count': len(self.auth_templates or []),
                'data_sources_count': len(self.data_sources or []),
                'data_sinks_count': len(self.data_sinks or [])
            }
        }
        
        base_dict.update(detailed_info)
        return base_dict
    
    def to_config_dict(self) -> Dict[str, Any]:
        """Convert to configuration dictionary (Rails pattern)"""
        return {
            'type': self.type.value,
            'connection_type': self.connection_type.value,
            'name': self.name,
            'description': self.description,
            'version': self.version,
            'config_schema': self.config_schema,
            'default_config': self.default_config,
            'capabilities': self.capabilities,
            'limitations': self.limitations,
            'supported_formats': self.supported_formats,
            'auth_type': self.auth_type.value,
            'auth_config': self.auth_config,
            'ingestion_mode': self.ingestion_mode.value,
            'nexset_api_compatible': self.nexset_api_compatible
        }
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for audit logging (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'type': self.type.value,
            'name': self.name,
            'status': self.status.value,
            'version': self.version,
            'usage_count': self.usage_count,
            'error_count': self.error_count,
            'nexset_api_compatible': self.nexset_api_compatible,
            'created_by_id': self.created_by_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self) -> str:
        return f"<Connector(id={self.id}, type='{self.type.value}', name='{self.name}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        return f"Connector: {self.display_name()} ({self.type_display()}) - {self.display_status()}"