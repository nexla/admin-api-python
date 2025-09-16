from datetime import datetime, timedelta
from enum import Enum as PyEnum
import hashlib
import hmac
import json
import secrets
from typing import Dict, List, Optional, Any, Union
import uuid

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint, BigInteger
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class IntegrationStatus(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    PENDING = "PENDING"
    FAILED = "FAILED"
    DISCONNECTED = "DISCONNECTED"
    SUSPENDED = "SUSPENDED"
    DEPRECATED = "DEPRECATED"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.PENDING: "Pending Connection",
            self.FAILED: "Connection Failed",
            self.DISCONNECTED: "Disconnected",
            self.SUSPENDED: "Suspended",
            self.DEPRECATED: "Deprecated"
        }.get(self, self.value)
    
    @property
    def is_operational(self) -> bool:
        return self == self.ACTIVE


class IntegrationType(PyEnum):
    DATABASE = "DATABASE"
    API = "API"
    WEBHOOK = "WEBHOOK"
    OAUTH = "OAUTH"
    SAML = "SAML"
    LDAP = "LDAP"
    CLOUD_STORAGE = "CLOUD_STORAGE"
    MESSAGING = "MESSAGING"
    MONITORING = "MONITORING"
    ANALYTICS = "ANALYTICS"
    CRM = "CRM"
    ERP = "ERP"
    CUSTOM = "CUSTOM"
    
    @property
    def display_name(self) -> str:
        return {
            self.DATABASE: "Database",
            self.API: "REST API",
            self.WEBHOOK: "Webhook",
            self.OAUTH: "OAuth Provider",
            self.SAML: "SAML Provider",
            self.LDAP: "LDAP Directory",
            self.CLOUD_STORAGE: "Cloud Storage",
            self.MESSAGING: "Messaging Service",
            self.MONITORING: "Monitoring Tool",
            self.ANALYTICS: "Analytics Platform",
            self.CRM: "CRM System",
            self.ERP: "ERP System",
            self.CUSTOM: "Custom Integration"
        }.get(self, self.value)


class AuthMethod(PyEnum):
    API_KEY = "API_KEY"
    BASIC_AUTH = "BASIC_AUTH"
    OAUTH1 = "OAUTH1"
    OAUTH2 = "OAUTH2"
    JWT = "JWT"
    CUSTOM_TOKEN = "CUSTOM_TOKEN"
    CERTIFICATE = "CERTIFICATE"
    NONE = "NONE"
    
    @property
    def display_name(self) -> str:
        return {
            self.API_KEY: "API Key",
            self.BASIC_AUTH: "Basic Authentication",
            self.OAUTH1: "OAuth 1.0",
            self.OAUTH2: "OAuth 2.0",
            self.JWT: "JWT Token",
            self.CUSTOM_TOKEN: "Custom Token",
            self.CERTIFICATE: "Certificate",
            self.NONE: "No Authentication"
        }.get(self, self.value)


class DataFlow(PyEnum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"
    BIDIRECTIONAL = "BIDIRECTIONAL"
    
    @property
    def display_name(self) -> str:
        return {
            self.INBOUND: "Inbound Only",
            self.OUTBOUND: "Outbound Only", 
            self.BIDIRECTIONAL: "Bidirectional"
        }.get(self, self.value)


class Integration(Base):
    __tablename__ = 'integrations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    integration_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    provider = Column(String(100), nullable=False)
    provider_version = Column(String(50))
    
    integration_type = Column(SQLEnum(IntegrationType), nullable=False)
    status = Column(SQLEnum(IntegrationStatus), nullable=False, default=IntegrationStatus.PENDING)
    auth_method = Column(SQLEnum(AuthMethod), nullable=False, default=AuthMethod.API_KEY)
    data_flow = Column(SQLEnum(DataFlow), nullable=False, default=DataFlow.BIDIRECTIONAL)
    
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    project_id = Column(Integer, ForeignKey('projects.id'))
    
    # Connection details
    endpoint_url = Column(String(2048))
    webhook_url = Column(String(2048))
    callback_url = Column(String(2048))
    
    # Authentication
    api_key_encrypted = Column(Text)
    username = Column(String(255))
    password_encrypted = Column(Text)
    client_id = Column(String(255))
    client_secret_encrypted = Column(Text)
    token_encrypted = Column(Text)
    refresh_token_encrypted = Column(Text)
    certificate_path = Column(String(500))
    
    # OAuth specific
    oauth_scopes = Column(JSON, default=list)
    oauth_state = Column(String(128))
    token_expires_at = Column(DateTime)
    
    # Configuration
    config_json = Column(JSON, default=dict)
    headers = Column(JSON, default=dict)
    parameters = Column(JSON, default=dict)
    
    # Rate limiting
    rate_limit_requests = Column(Integer, default=1000)
    rate_limit_window_seconds = Column(Integer, default=3600)
    current_rate_count = Column(Integer, default=0)
    rate_limit_reset_at = Column(DateTime)
    
    # Health and monitoring
    last_sync_at = Column(DateTime)
    last_success_at = Column(DateTime)
    last_failure_at = Column(DateTime)
    consecutive_failures = Column(Integer, default=0)
    
    # Usage metrics
    total_requests = Column(BigInteger, default=0)
    successful_requests = Column(BigInteger, default=0)
    failed_requests = Column(BigInteger, default=0)
    data_transferred_bytes = Column(BigInteger, default=0)
    
    # Sync configuration
    sync_enabled = Column(Boolean, default=True)
    sync_frequency_minutes = Column(Integer, default=60)
    next_sync_at = Column(DateTime)
    batch_size = Column(Integer, default=100)
    
    # Retry settings
    max_retry_attempts = Column(Integer, default=3)
    retry_delay_seconds = Column(Integer, default=60)
    backoff_multiplier = Column(Float, default=2.0)
    
    # Timeout settings
    connection_timeout_seconds = Column(Integer, default=30)
    read_timeout_seconds = Column(Integer, default=60)
    
    active = Column(Boolean, default=True, nullable=False)
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    error_log = Column(JSON, default=list)
    sync_log = Column(JSON, default=list)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    connected_at = Column(DateTime)
    disconnected_at = Column(DateTime)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    org = relationship("Org", back_populates="integrations")
    user = relationship("User", foreign_keys=[user_id])
    project = relationship("Project", back_populates="integrations")
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    
    __table_args__ = (
        Index('idx_integration_org_id', 'org_id'),
        Index('idx_integration_user_id', 'user_id'),
        Index('idx_integration_project_id', 'project_id'),
        Index('idx_integration_type', 'integration_type'),
        Index('idx_integration_status', 'status'),
        Index('idx_integration_provider', 'provider'),
        Index('idx_integration_active', 'active'),
        Index('idx_integration_next_sync', 'next_sync_at'),
        Index('idx_integration_last_sync', 'last_sync_at'),
        UniqueConstraint('org_id', 'name', name='uq_integration_org_name'),
        CheckConstraint('total_requests >= 0', name='ck_integration_total_requests_non_negative'),
        CheckConstraint('successful_requests >= 0', name='ck_integration_successful_requests_non_negative'),
        CheckConstraint('failed_requests >= 0', name='ck_integration_failed_requests_non_negative'),
        CheckConstraint('consecutive_failures >= 0', name='ck_integration_consecutive_failures_non_negative'),
        CheckConstraint('rate_limit_requests > 0', name='ck_integration_rate_limit_positive'),
    )
    
    MAX_CONSECUTIVE_FAILURES = 10
    DEFAULT_TIMEOUT_SECONDS = 30
    MAX_ERROR_LOG_ENTRIES = 100
    MAX_SYNC_LOG_ENTRIES = 500
    UNHEALTHY_FAILURE_RATE = 0.1
    
    def __repr__(self):
        return f"<Integration(id={self.id}, name='{self.name}', provider='{self.provider}', status='{self.status.value}')>"
    
    def active_(self) -> bool:
        """Check if integration is active (Rails pattern)"""
        return (self.active and 
                self.status == IntegrationStatus.ACTIVE and
                not self.suspended_())
    
    def operational_(self) -> bool:
        """Check if integration is operational (Rails pattern)"""
        return self.status.is_operational
    
    def connected_(self) -> bool:
        """Check if integration is connected (Rails pattern)"""
        return self.status == IntegrationStatus.ACTIVE and self.connected_at is not None
    
    def disconnected_(self) -> bool:
        """Check if integration is disconnected (Rails pattern)"""
        return self.status == IntegrationStatus.DISCONNECTED
    
    def pending_(self) -> bool:
        """Check if integration is pending (Rails pattern)"""
        return self.status == IntegrationStatus.PENDING
    
    def failed_(self) -> bool:
        """Check if integration has failed (Rails pattern)"""
        return self.status == IntegrationStatus.FAILED
    
    def suspended_(self) -> bool:
        """Check if integration is suspended (Rails pattern)"""
        return self.status == IntegrationStatus.SUSPENDED
    
    def deprecated_(self) -> bool:
        """Check if integration is deprecated (Rails pattern)"""
        return self.status == IntegrationStatus.DEPRECATED
    
    def oauth_(self) -> bool:
        """Check if integration uses OAuth (Rails pattern)"""
        return self.auth_method in [AuthMethod.OAUTH1, AuthMethod.OAUTH2]
    
    def token_expired_(self) -> bool:
        """Check if OAuth token is expired (Rails pattern)"""
        return (self.oauth_() and 
                self.token_expires_at and 
                self.token_expires_at < datetime.now())
    
    def sync_enabled_(self) -> bool:
        """Check if sync is enabled (Rails pattern)"""
        return self.sync_enabled and self.active_()
    
    def sync_overdue_(self) -> bool:
        """Check if sync is overdue (Rails pattern)"""
        return (self.sync_enabled_() and 
                self.next_sync_at and 
                self.next_sync_at < datetime.now())
    
    def rate_limited_(self) -> bool:
        """Check if integration is rate limited (Rails pattern)"""
        if not self.rate_limit_reset_at:
            return False
        return (self.rate_limit_reset_at > datetime.now() and 
                self.current_rate_count >= self.rate_limit_requests)
    
    def unhealthy_(self) -> bool:
        """Check if integration is unhealthy (Rails pattern)"""
        if self.total_requests == 0:
            return False
        failure_rate = self.failed_requests / self.total_requests
        return (failure_rate > self.UNHEALTHY_FAILURE_RATE or 
                self.consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES)
    
    def recently_active_(self, hours: int = 24) -> bool:
        """Check if integration was recently active (Rails pattern)"""
        if not self.last_sync_at:
            return False
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.last_sync_at > cutoff
    
    def needs_attention_(self) -> bool:
        """Check if integration needs attention (Rails pattern)"""
        return (self.failed_() or 
                self.unhealthy_() or
                self.token_expired_() or
                self.sync_overdue_() or
                self.suspended_())
    
    def connect_(self) -> None:
        """Connect integration (Rails bang method pattern)"""
        self.status = IntegrationStatus.ACTIVE
        self.connected_at = datetime.now()
        self.consecutive_failures = 0
        self.updated_at = datetime.now()
    
    def disconnect_(self, reason: str = None) -> None:
        """Disconnect integration (Rails bang method pattern)"""
        self.status = IntegrationStatus.DISCONNECTED
        self.disconnected_at = datetime.now()
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['disconnect_reason'] = reason
        
        self._log_error('disconnected', reason or 'Manual disconnection')
    
    def suspend_(self, reason: str) -> None:
        """Suspend integration (Rails bang method pattern)"""
        self.status = IntegrationStatus.SUSPENDED
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['suspension_reason'] = reason
        self.extra_metadata['suspended_at'] = datetime.now().isoformat()
        
        self._log_error('suspended', reason)
    
    def unsuspend_(self) -> None:
        """Unsuspend integration (Rails bang method pattern)"""
        if self.status == IntegrationStatus.SUSPENDED:
            self.status = IntegrationStatus.ACTIVE
            self.updated_at = datetime.now()
    
    def mark_failed_(self, error_message: str) -> None:
        """Mark integration as failed (Rails bang method pattern)"""
        self.status = IntegrationStatus.FAILED
        self.last_failure_at = datetime.now()
        self.consecutive_failures += 1
        self.failed_requests += 1
        self.updated_at = datetime.now()
        
        self._log_error('connection_failed', error_message)
        
        # Auto-suspend if too many failures
        if self.consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
            self.suspend_(f"Too many consecutive failures: {self.consecutive_failures}")
    
    def record_success_(self) -> None:
        """Record successful operation (Rails bang method pattern)"""
        self.last_success_at = datetime.now()
        self.consecutive_failures = 0
        self.successful_requests += 1
        self.total_requests += 1
        
        if self.status == IntegrationStatus.FAILED:
            self.status = IntegrationStatus.ACTIVE
        
        self.updated_at = datetime.now()
    
    def record_failure_(self, error_message: str) -> None:
        """Record failed operation (Rails bang method pattern)"""
        self.last_failure_at = datetime.now()
        self.consecutive_failures += 1
        self.failed_requests += 1
        self.total_requests += 1
        self.updated_at = datetime.now()
        
        self._log_error('request_failed', error_message)
    
    def refresh_token_(self, new_token: str, expires_at: datetime = None) -> None:
        """Refresh OAuth token (Rails bang method pattern)"""
        self.token_encrypted = self._encrypt_value(new_token)
        self.token_expires_at = expires_at
        self.updated_at = datetime.now()
        
        self._log_sync('token_refreshed', {'expires_at': expires_at.isoformat() if expires_at else None})
    
    def schedule_sync_(self, minutes_from_now: int = None) -> None:
        """Schedule next sync (Rails bang method pattern)"""
        if minutes_from_now is None:
            minutes_from_now = self.sync_frequency_minutes
        
        self.next_sync_at = datetime.now() + timedelta(minutes=minutes_from_now)
        self.updated_at = datetime.now()
    
    def perform_sync_(self) -> bool:
        """Perform sync operation (Rails bang method pattern)"""
        if not self.sync_enabled_() or self.rate_limited_():
            return False
        
        try:
            self.last_sync_at = datetime.now()
            # Actual sync logic would go here
            
            self.record_success_()
            self.schedule_sync_()
            self._log_sync('sync_completed', {'timestamp': datetime.now().isoformat()})
            
            return True
            
        except Exception as e:
            self.record_failure_(str(e))
            # Schedule retry with exponential backoff
            retry_delay = self.retry_delay_seconds * (self.backoff_multiplier ** (self.consecutive_failures - 1))
            self.schedule_sync_(int(retry_delay / 60))
            
            return False
    
    def test_connection_(self) -> bool:
        """Test integration connection (Rails bang method pattern)"""
        try:
            # Connection test logic would go here
            self.record_success_()
            self._log_sync('connection_test_passed', {})
            return True
            
        except Exception as e:
            self.record_failure_(f"Connection test failed: {str(e)}")
            return False
    
    def update_rate_limit_(self, requests: int, window_seconds: int) -> None:
        """Update rate limit settings (Rails bang method pattern)"""
        self.rate_limit_requests = requests
        self.rate_limit_window_seconds = window_seconds
        self.current_rate_count = 0
        self.rate_limit_reset_at = None
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to integration (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from integration (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def set_config_(self, key: str, value: Any) -> None:
        """Set configuration value (Rails bang method pattern)"""
        config = dict(self.config_json or {})
        config[key] = value
        self.config_json = config
        self.updated_at = datetime.now()
    
    def remove_config_(self, key: str) -> None:
        """Remove configuration key (Rails bang method pattern)"""
        config = dict(self.config_json or {})
        if key in config:
            del config[key]
            self.config_json = config
            self.updated_at = datetime.now()
    
    def _encrypt_value(self, value: str) -> str:
        """Encrypt sensitive value (private helper)"""
        # In real implementation, this would use proper encryption
        import base64
        return base64.b64encode(value.encode()).decode()
    
    def _decrypt_value(self, encrypted_value: str) -> str:
        """Decrypt sensitive value (private helper)"""
        # In real implementation, this would use proper decryption
        import base64
        return base64.b64decode(encrypted_value.encode()).decode()
    
    def _log_error(self, error_type: str, message: str) -> None:
        """Log error event (private helper)"""
        error_entry = {
            'type': error_type,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'consecutive_failures': self.consecutive_failures
        }
        
        self.error_log = self.error_log or []
        self.error_log.append(error_entry)
        
        # Keep only recent errors
        if len(self.error_log) > self.MAX_ERROR_LOG_ENTRIES:
            self.error_log = self.error_log[-self.MAX_ERROR_LOG_ENTRIES:]
    
    def _log_sync(self, sync_type: str, data: Dict[str, Any]) -> None:
        """Log sync event (private helper)"""
        sync_entry = {
            'type': sync_type,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        self.sync_log = self.sync_log or []
        self.sync_log.append(sync_entry)
        
        # Keep only recent entries
        if len(self.sync_log) > self.MAX_SYNC_LOG_ENTRIES:
            self.sync_log = self.sync_log[-self.MAX_SYNC_LOG_ENTRIES:]
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value (Rails pattern)"""
        return (self.config_json or {}).get(key, default)
    
    def success_rate(self) -> float:
        """Calculate success rate (Rails pattern)"""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests
    
    def failure_rate(self) -> float:
        """Calculate failure rate (Rails pattern)"""
        return 1.0 - self.success_rate()
    
    def days_since_last_sync(self) -> Optional[int]:
        """Calculate days since last sync (Rails pattern)"""
        if not self.last_sync_at:
            return None
        return (datetime.now() - self.last_sync_at).days
    
    def days_since_connection(self) -> Optional[int]:
        """Calculate days since connection (Rails pattern)"""
        if not self.connected_at:
            return None
        return (datetime.now() - self.connected_at).days
    
    def uptime_percentage(self, days: int = 30) -> float:
        """Calculate uptime percentage (Rails pattern)"""
        if not self.connected_at:
            return 0.0
        
        # Simplified calculation - in real implementation would track downtime
        return max(0.0, 100.0 - (self.consecutive_failures * 10))
    
    def connection_summary(self) -> Dict[str, Any]:
        """Get connection summary (Rails pattern)"""
        return {
            'integration_id': self.integration_id,
            'provider': self.provider,
            'connected': self.connected_(),
            'status': self.status.value,
            'auth_method': self.auth_method.value,
            'data_flow': self.data_flow.value,
            'last_sync_at': self.last_sync_at.isoformat() if self.last_sync_at else None,
            'days_since_last_sync': self.days_since_last_sync(),
            'sync_overdue': self.sync_overdue_(),
            'token_expired': self.token_expired_()
        }
    
    def performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics (Rails pattern)"""
        return {
            'integration_id': self.integration_id,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate': self.success_rate(),
            'failure_rate': self.failure_rate(),
            'consecutive_failures': self.consecutive_failures,
            'data_transferred_mb': self.data_transferred_bytes / (1024 * 1024) if self.data_transferred_bytes else 0,
            'uptime_percentage': self.uptime_percentage(),
            'unhealthy': self.unhealthy_(),
            'rate_limited': self.rate_limited_()
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate integration health report (Rails pattern)"""
        return {
            'integration_id': self.integration_id,
            'healthy': not self.needs_attention_(),
            'active': self.active_(),
            'operational': self.operational_(),
            'status': self.status.value,
            'connected': self.connected_(),
            'suspended': self.suspended_(),
            'failed': self.failed_(),
            'token_expired': self.token_expired_(),
            'sync_overdue': self.sync_overdue_(),
            'unhealthy': self.unhealthy_(),
            'needs_attention': self.needs_attention_(),
            'connection_summary': self.connection_summary(),
            'performance_metrics': self.performance_metrics()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'integration_id': self.integration_id,
            'name': self.name,
            'description': self.description,
            'provider': self.provider,
            'provider_version': self.provider_version,
            'integration_type': self.integration_type.value,
            'status': self.status.value,
            'auth_method': self.auth_method.value,
            'data_flow': self.data_flow.value,
            'sync_enabled': self.sync_enabled,
            'sync_frequency_minutes': self.sync_frequency_minutes,
            'success_rate': self.success_rate(),
            'total_requests': self.total_requests,
            'consecutive_failures': self.consecutive_failures,
            'last_sync_at': self.last_sync_at.isoformat() if self.last_sync_at else None,
            'tags': self.tags,
            'active': self.active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'endpoint_url': self.endpoint_url,
                'webhook_url': self.webhook_url,
                'config_json': self.config_json,
                'headers': self.headers,
                'parameters': self.parameters,
                'oauth_scopes': self.oauth_scopes,
                'metadata': self.extra_metadata,
                'error_log': self.error_log[-10:] if self.error_log else [],  # Last 10 errors
                'sync_log': self.sync_log[-10:] if self.sync_log else []  # Last 10 sync events
            })
        
        return result