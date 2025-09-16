from enum import Enum as PyEnum
from typing import Optional, Dict, List, Any, Union
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from dataclasses import dataclass
import json
import secrets
import hashlib

from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, JSON, Boolean, Index, Enum as SQLEnum
from sqlalchemy.orm import relationship, Session, validates
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property

from ..database import Base


logger = logging.getLogger(__name__)


class AuthTemplateStatuses(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DRAFT = "DRAFT"
    DEPRECATED = "DEPRECATED"
    ARCHIVED = "ARCHIVED"
    TESTING = "TESTING"
    MAINTENANCE = "MAINTENANCE"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class AuthTypes(PyEnum):
    OAUTH2 = "OAUTH2"
    API_KEY = "API_KEY"
    BASIC_AUTH = "BASIC_AUTH"
    BEARER_TOKEN = "BEARER_TOKEN"
    CUSTOM_HEADER = "CUSTOM_HEADER"
    JWT = "JWT"
    SAML = "SAML"
    CERTIFICATE = "CERTIFICATE"
    HMAC = "HMAC"
    DIGEST = "DIGEST"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class SecurityLevels(PyEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.title()


class AuthMethodComplexity(PyEnum):
    SIMPLE = "SIMPLE"          # API key, basic auth
    MODERATE = "MODERATE"      # OAuth2 client credentials
    COMPLEX = "COMPLEX"        # OAuth2 authorization code
    ENTERPRISE = "ENTERPRISE"   # SAML, certificate-based
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.title()


@dataclass
class AuthValidationResult:
    """Result of auth template validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    required_fields: List[str]
    optional_fields: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'required_fields': self.required_fields,
            'optional_fields': self.optional_fields
        }


@dataclass
class AuthMetrics:
    """Metrics for auth template usage"""
    total_credentials: int = 0
    active_credentials: int = 0
    failed_authentications: int = 0
    success_rate: float = 0.0
    average_setup_time_minutes: float = 0.0
    last_used_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_credentials': self.total_credentials,
            'active_credentials': self.active_credentials,
            'failed_authentications': self.failed_authentications,
            'success_rate': float(self.success_rate),
            'average_setup_time_minutes': float(self.average_setup_time_minutes),
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None
        }


class AuthTemplate(Base):
    __tablename__ = "auth_templates"
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_auth_template_name', 'name'),
        Index('idx_auth_template_status', 'status'),
        Index('idx_auth_template_type', 'auth_type'),
        Index('idx_auth_template_connector', 'connector_id', 'status'),
        Index('idx_auth_template_vendor', 'vendor_id', 'status'),
        Index('idx_auth_template_active', 'status', 'is_active'),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Core identification
    name = Column(String(255), unique=True, nullable=False, index=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text)
    version = Column(String(50), default="1.0.0")
    
    # Authentication configuration
    auth_type = Column(SQLEnum(AuthTypes), nullable=False)
    status = Column(SQLEnum(AuthTemplateStatuses), nullable=False, default=AuthTemplateStatuses.DRAFT)
    security_level = Column(SQLEnum(SecurityLevels), nullable=False, default=SecurityLevels.MEDIUM)
    complexity = Column(SQLEnum(AuthMethodComplexity), nullable=False, default=AuthMethodComplexity.MODERATE)
    
    # State flags
    is_active = Column(Boolean, nullable=False, default=True)
    is_deprecated = Column(Boolean, nullable=False, default=False)
    is_public = Column(Boolean, nullable=False, default=False)  # Can be used by all users
    is_validated = Column(Boolean, nullable=False, default=False)
    requires_approval = Column(Boolean, nullable=False, default=False)
    
    # Configuration stored as JSON
    config = Column(JSON, nullable=False)  # Primary auth configuration
    default_config = Column(JSON)  # Default values for new credentials
    validation_config = Column(JSON)  # Validation rules and requirements
    ui_config = Column(JSON)  # UI rendering configuration
    
    # Security and encryption
    encryption_enabled = Column(Boolean, nullable=False, default=True)
    config_hash = Column(String(255))  # Hash of config for change detection
    
    # Usage tracking
    usage_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    last_used_at = Column(DateTime)
    
    # Lifecycle management
    tested_at = Column(DateTime)
    approved_at = Column(DateTime)
    deprecated_at = Column(DateTime)
    archived_at = Column(DateTime)
    
    # Documentation and help
    documentation_url = Column(String(500))
    help_text = Column(Text)
    setup_instructions = Column(Text)
    troubleshooting_guide = Column(Text)
    
    # Metadata
    tags = Column(JSON)  # List of tags for categorization
    extra_metadata = Column(JSON)  # Additional metadata
    
    # Foreign keys
    connector_id = Column(Integer, ForeignKey("connectors.id"), nullable=False)
    vendor_id = Column(Integer, ForeignKey("vendors.id"), nullable=False)
    created_by_user_id = Column(Integer, ForeignKey("users.id"))
    approved_by_user_id = Column(Integer, ForeignKey("users.id"))
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    connector = relationship("Connector", back_populates="auth_templates")
    # vendor = relationship("Vendor", back_populates="auth_templates")
    # created_by = relationship("User", foreign_keys=[created_by_user_id])
    # approved_by = relationship("User", foreign_keys=[approved_by_user_id])
    data_credentials = relationship("DataCredentials", back_populates="auth_template")
    
    # Constants
    MAX_CONFIG_SIZE = 10000  # Maximum JSON config size in chars
    MAX_RETRY_ATTEMPTS = 3
    CONFIG_VERSION = "1.0"
    
    def __repr__(self) -> str:
        return f"<AuthTemplate(id={self.id}, name='{self.name}', auth_type='{self.auth_type.value}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        return f"{self.display_name} ({self.auth_type.get_display_name()})"
    
    # === Rails-style Predicate Methods ===
    
    def active_(self) -> bool:
        """Check if auth template is active and available"""
        return self.status == AuthTemplateStatuses.ACTIVE and self.is_active and not self.is_deprecated
    
    def inactive_(self) -> bool:
        """Check if auth template is inactive"""
        return not self.active_()
    
    def draft_(self) -> bool:
        """Check if auth template is in draft status"""
        return self.status == AuthTemplateStatuses.DRAFT
    
    def deprecated_(self) -> bool:
        """Check if auth template is deprecated"""
        return self.is_deprecated or self.status == AuthTemplateStatuses.DEPRECATED
    
    def archived_(self) -> bool:
        """Check if auth template is archived"""
        return self.status == AuthTemplateStatuses.ARCHIVED or self.archived_at is not None
    
    def testing_(self) -> bool:
        """Check if auth template is in testing status"""
        return self.status == AuthTemplateStatuses.TESTING
    
    def maintenance_(self) -> bool:
        """Check if auth template is in maintenance mode"""
        return self.status == AuthTemplateStatuses.MAINTENANCE
    
    def public_(self) -> bool:
        """Check if auth template is public (available to all users)"""
        return self.is_public and self.active_()
    
    def validated_(self) -> bool:
        """Check if auth template is validated"""
        return self.is_validated and self.tested_at is not None
    
    def approved_(self) -> bool:
        """Check if auth template is approved for use"""
        return self.approved_at is not None and self.approved_by_user_id is not None
    
    def requires_approval_(self) -> bool:
        """Check if auth template requires approval"""
        return self.requires_approval and not self.approved_()
    
    def ready_for_use_(self) -> bool:
        """Check if auth template is ready for production use"""
        return (self.active_() and 
                self.validated_() and 
                (not self.requires_approval_() or self.approved_()))
    
    def encryption_enabled_(self) -> bool:
        """Check if encryption is enabled"""
        return self.encryption_enabled
    
    def oauth2_(self) -> bool:
        """Check if auth type is OAuth2"""
        return self.auth_type == AuthTypes.OAUTH2
    
    def api_key_(self) -> bool:
        """Check if auth type is API key"""
        return self.auth_type == AuthTypes.API_KEY
    
    def basic_auth_(self) -> bool:
        """Check if auth type is basic authentication"""
        return self.auth_type == AuthTypes.BASIC_AUTH
    
    def bearer_token_(self) -> bool:
        """Check if auth type is bearer token"""
        return self.auth_type == AuthTypes.BEARER_TOKEN
    
    def high_security_(self) -> bool:
        """Check if template has high security level"""
        return self.security_level in [SecurityLevels.HIGH, SecurityLevels.CRITICAL]
    
    def critical_security_(self) -> bool:
        """Check if template has critical security level"""
        return self.security_level == SecurityLevels.CRITICAL
    
    def simple_complexity_(self) -> bool:
        """Check if template has simple complexity"""
        return self.complexity == AuthMethodComplexity.SIMPLE
    
    def enterprise_complexity_(self) -> bool:
        """Check if template has enterprise complexity"""
        return self.complexity == AuthMethodComplexity.ENTERPRISE
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if template was used recently"""
        if not self.last_used_at:
            return False
        threshold = datetime.utcnow() - timedelta(hours=hours)
        return self.last_used_at > threshold
    
    def has_failures_(self) -> bool:
        """Check if template has recorded failures"""
        return self.failure_count > 0
    
    def high_failure_rate_(self, threshold: float = 0.1) -> bool:
        """Check if template has high failure rate"""
        if self.usage_count == 0:
            return False
        failure_rate = self.failure_count / self.usage_count
        return failure_rate > threshold
    
    def well_tested_(self, min_usage: int = 10) -> bool:
        """Check if template is well tested"""
        return self.usage_count >= min_usage and self.tested_at is not None
    
    def config_valid_(self) -> bool:
        """Check if configuration is valid"""
        return self.has_config_() and self.is_validated
    
    def has_config_(self) -> bool:
        """Check if template has configuration"""
        return bool(self.config)
    
    def has_documentation_(self) -> bool:
        """Check if template has documentation"""
        return bool(self.documentation_url or self.help_text or self.setup_instructions)
    
    def config_changed_(self) -> bool:
        """Check if config has changed since last hash"""
        if not self.config_hash:
            return True
        current_hash = self._calculate_config_hash()
        return current_hash != self.config_hash
    
    # === Rails-style Bang Methods ===
    
    def activate_(self) -> None:
        """Activate the auth template"""
        if self.active_():
            return
        if self.archived_():
            raise ValueError("Cannot activate archived auth template")
        if not self.validated_():
            raise ValueError("Cannot activate unvalidated auth template")
        
        self.status = AuthTemplateStatuses.ACTIVE
        self.is_active = True
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Activated auth template: {self.name}")
    
    def deactivate_(self) -> None:
        """Deactivate the auth template"""
        if not self.active_():
            return
        
        self.status = AuthTemplateStatuses.INACTIVE
        self.is_active = False
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Deactivated auth template: {self.name}")
    
    def deprecate_(self, reason: str = None) -> None:
        """Deprecate the auth template"""
        if self.deprecated_():
            return
        
        self.is_deprecated = True
        self.status = AuthTemplateStatuses.DEPRECATED
        self.deprecated_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Deprecated auth template {self.name}: {reason or 'No reason specified'}")
    
    def archive_(self) -> None:
        """Archive the auth template"""
        if self.archived_():
            return
        
        self.status = AuthTemplateStatuses.ARCHIVED
        self.is_active = False
        self.archived_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Archived auth template: {self.name}")
    
    def mark_as_testing_(self) -> None:
        """Mark template as in testing"""
        if self.testing_():
            return
        
        self.status = AuthTemplateStatuses.TESTING
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Marked auth template as testing: {self.name}")
    
    def mark_as_maintenance_(self) -> None:
        """Mark template as in maintenance"""
        if self.maintenance_():
            return
        
        self.status = AuthTemplateStatuses.MAINTENANCE
        self.updated_at = datetime.utcnow()
        
        logger.warning(f"Marked auth template as maintenance: {self.name}")
    
    def validate_(self) -> AuthValidationResult:
        """Validate the auth template configuration"""
        result = self._perform_validation()
        
        if result.is_valid:
            self.is_validated = True
            self.tested_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()
            logger.info(f"Validated auth template: {self.name}")
        else:
            self.is_validated = False
            logger.warning(f"Validation failed for auth template {self.name}: {result.errors}")
        
        return result
    
    def approve_(self, approved_by_user_id: int) -> None:
        """Approve the auth template for production use"""
        if self.approved_():
            return
        if not self.validated_():
            raise ValueError("Cannot approve unvalidated auth template")
        
        self.approved_at = datetime.utcnow()
        self.approved_by_user_id = approved_by_user_id
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Approved auth template {self.name} by user {approved_by_user_id}")
    
    def revoke_approval_(self) -> None:
        """Revoke approval from auth template"""
        if not self.approved_():
            return
        
        self.approved_at = None
        self.approved_by_user_id = None
        self.updated_at = datetime.utcnow()
        
        logger.warning(f"Revoked approval for auth template: {self.name}")
    
    def make_public_(self) -> None:
        """Make template available to all users"""
        if self.public_():
            return
        if not self.ready_for_use_():
            raise ValueError("Template must be ready for use before making public")
        
        self.is_public = True
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Made auth template public: {self.name}")
    
    def make_private_(self) -> None:
        """Make template private"""
        if not self.public_():
            return
        
        self.is_public = False
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Made auth template private: {self.name}")
    
    def enable_encryption_(self) -> None:
        """Enable encryption for template"""
        if self.encryption_enabled_():
            return
        
        self.encryption_enabled = True
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Enabled encryption for auth template: {self.name}")
    
    def disable_encryption_(self) -> None:
        """Disable encryption for template"""
        if not self.encryption_enabled_():
            return
        if self.high_security_():
            raise ValueError("Cannot disable encryption for high security templates")
        
        self.encryption_enabled = False
        self.updated_at = datetime.utcnow()
        
        logger.warning(f"Disabled encryption for auth template: {self.name}")
    
    def increment_usage_(self, success: bool = True) -> None:
        """Increment usage counters"""
        self.usage_count = (self.usage_count or 0) + 1
        if success:
            self.success_count = (self.success_count or 0) + 1
        else:
            self.failure_count = (self.failure_count or 0) + 1
        
        self.last_used_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def reset_counters_(self) -> None:
        """Reset usage counters"""
        self.usage_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Reset counters for auth template: {self.name}")
    
    def update_config_(self, new_config: Dict[str, Any], validate: bool = True) -> None:
        """Update template configuration"""
        if validate:
            # Store old config in case validation fails
            old_config = self.config
            old_is_validated = self.is_validated
            
            self.config = new_config
            validation_result = self.validate_()
            
            if not validation_result.is_valid:
                # Restore old config
                self.config = old_config
                self.is_validated = old_is_validated
                raise ValueError(f"Config validation failed: {validation_result.errors}")
        else:
            self.config = new_config
            self.is_validated = False
        
        self._update_config_hash()
        self.updated_at = datetime.utcnow()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to template"""
        tags = self.get_tags()
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.utcnow()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from template"""
        tags = self.get_tags()
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.utcnow()
    
    # === Helper Methods ===
    
    def _calculate_config_hash(self) -> str:
        """Calculate hash of current configuration"""
        if not self.config:
            return ""
        config_str = json.dumps(self.config, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()
    
    def _update_config_hash(self) -> None:
        """Update the configuration hash"""
        self.config_hash = self._calculate_config_hash()
    
    def _perform_validation(self) -> AuthValidationResult:
        """Perform comprehensive validation of the auth template"""
        errors = []
        warnings = []
        required_fields = []
        optional_fields = []
        
        # Basic validation
        if not self.config:
            errors.append("Configuration is required")
        else:
            config_str = json.dumps(self.config)
            if len(config_str) > self.MAX_CONFIG_SIZE:
                errors.append(f"Configuration size exceeds maximum of {self.MAX_CONFIG_SIZE} characters")
        
        # Auth type specific validation
        if self.auth_type == AuthTypes.OAUTH2:
            required_fields.extend(['client_id', 'client_secret', 'token_url'])
            optional_fields.extend(['scope', 'redirect_uri'])
        elif self.auth_type == AuthTypes.API_KEY:
            required_fields.extend(['key_field', 'key_location'])
            optional_fields.extend(['key_prefix'])
        elif self.auth_type == AuthTypes.BASIC_AUTH:
            required_fields.extend(['username_field', 'password_field'])
        elif self.auth_type == AuthTypes.BEARER_TOKEN:
            required_fields.extend(['token_field'])
            optional_fields.extend(['token_prefix'])
        
        # Check required fields in config
        if self.config:
            for field in required_fields:
                if field not in self.config:
                    errors.append(f"Required field '{field}' missing from configuration")
        
        # Security validation
        if self.high_security_() and not self.encryption_enabled:
            warnings.append("High security templates should have encryption enabled")
        
        if not self.has_documentation_():
            warnings.append("Template should have documentation for better user experience")
        
        is_valid = len(errors) == 0
        
        return AuthValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            required_fields=required_fields,
            optional_fields=optional_fields
        )
    
    def get_config_value(self, key: str, default=None):
        """Get a specific value from the configuration"""
        if not self.config:
            return default
        return self.config.get(key, default)
    
    def set_config_value(self, key: str, value):
        """Set a specific value in the configuration"""
        if not self.config:
            self.config = {}
        self.config[key] = value
        self._update_config_hash()
        self.updated_at = datetime.utcnow()
    
    def get_tags(self) -> List[str]:
        """Get list of tags"""
        if not self.tags:
            return []
        try:
            return json.loads(self.tags) if isinstance(self.tags, str) else self.tags
        except (json.JSONDecodeError, TypeError):
            return []
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata as dictionary"""
        if not self.extra_metadata:
            return {}
        try:
            return json.loads(self.extra_metadata) if isinstance(self.extra_metadata, str) else self.extra_metadata
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata value"""
        current_metadata = self.get_metadata()
        current_metadata[key] = value
        self.extra_metadata = current_metadata
        self.updated_at = datetime.utcnow()
    
    def get_success_rate(self) -> float:
        """Calculate success rate"""
        if not self.usage_count:
            return 0.0
        return float(self.success_count) / float(self.usage_count)
    
    def get_metrics(self) -> AuthMetrics:
        """Get comprehensive metrics"""
        return AuthMetrics(
            total_credentials=self.usage_count or 0,
            active_credentials=self.success_count or 0,
            failed_authentications=self.failure_count or 0,
            success_rate=self.get_success_rate(),
            average_setup_time_minutes=0.0,  # Would need to calculate from actual data
            last_used_at=self.last_used_at
        )
    
    # === Class Methods (Rails-style Scopes) ===
    
    @classmethod
    def active(cls, session: Session):
        """Get all active auth templates"""
        return session.query(cls).filter(
            cls.status == AuthTemplateStatuses.ACTIVE,
            cls.is_active == True,
            cls.is_deprecated == False
        )
    
    @classmethod
    def public_templates(cls, session: Session):
        """Get all public templates"""
        return cls.active(session).filter(cls.is_public == True)
    
    @classmethod
    def validated_templates(cls, session: Session):
        """Get all validated templates"""
        return cls.active(session).filter(cls.is_validated == True)
    
    @classmethod
    def ready_for_use(cls, session: Session):
        """Get all templates ready for production use"""
        return session.query(cls).filter(
            cls.status == AuthTemplateStatuses.ACTIVE,
            cls.is_active == True,
            cls.is_validated == True,
            ((cls.requires_approval == False) | (cls.approved_at.isnot(None)))
        )
    
    @classmethod
    def by_auth_type(cls, session: Session, auth_type: AuthTypes):
        """Get templates by authentication type"""
        return cls.active(session).filter(cls.auth_type == auth_type)
    
    @classmethod
    def by_security_level(cls, session: Session, security_level: SecurityLevels):
        """Get templates by security level"""
        return cls.active(session).filter(cls.security_level == security_level)
    
    @classmethod
    def by_complexity(cls, session: Session, complexity: AuthMethodComplexity):
        """Get templates by complexity level"""
        return cls.active(session).filter(cls.complexity == complexity)
    
    @classmethod
    def by_connector(cls, session: Session, connector_id: int):
        """Get templates for specific connector"""
        return cls.active(session).filter(cls.connector_id == connector_id)
    
    @classmethod
    def by_vendor(cls, session: Session, vendor_id: int):
        """Get templates for specific vendor"""
        return cls.active(session).filter(cls.vendor_id == vendor_id)
    
    @classmethod
    def recently_used(cls, session: Session, hours: int = 24):
        """Get recently used templates"""
        threshold = datetime.utcnow() - timedelta(hours=hours)
        return cls.active(session).filter(cls.last_used_at > threshold)
    
    @classmethod
    def high_failure_rate(cls, session: Session, threshold: float = 0.1):
        """Get templates with high failure rate"""
        return cls.active(session).filter(
            cls.usage_count > 0,
            (cls.failure_count.cast(float) / cls.usage_count.cast(float)) > threshold
        )
    
    @classmethod
    def find_by_name(cls, session: Session, name: str):
        """Find template by name"""
        return session.query(cls).filter(cls.name == name).first()
    
    @classmethod
    def search_by_name_or_description(cls, session: Session, query: str):
        """Search templates by name or description"""
        search_term = f"%{query.lower()}%"
        return cls.active(session).filter(
            (func.lower(cls.name).like(search_term)) |
            (func.lower(cls.display_name).like(search_term)) |
            (func.lower(cls.description).like(search_term))
        )
    
    @classmethod
    def build_from_input(cls, input_data: dict):
        """Create a new AuthTemplate from input data (Rails pattern)"""
        if not input_data.get('name'):
            raise ValueError("Template name is required")
        if not input_data.get('auth_type'):
            raise ValueError("Auth type is required")
        if not input_data.get('config'):
            raise ValueError("Configuration is required")
        
        input_data = dict(input_data)  # Make a copy
        
        # Set defaults
        defaults = {
            'status': AuthTemplateStatuses.DRAFT,
            'security_level': SecurityLevels.MEDIUM,
            'complexity': AuthMethodComplexity.MODERATE,
            'version': '1.0.0',
            'is_active': True,
            'is_deprecated': False,
            'is_public': False,
            'is_validated': False,
            'requires_approval': False,
            'encryption_enabled': True,
            'usage_count': 0,
            'success_count': 0,
            'failure_count': 0
        }
        
        # Merge with input data
        template_data = {**defaults, **input_data}
        
        # Handle enum conversions
        if isinstance(template_data.get('auth_type'), str):
            template_data['auth_type'] = AuthTypes(template_data['auth_type'])
        if isinstance(template_data.get('status'), str):
            template_data['status'] = AuthTemplateStatuses(template_data['status'])
        if isinstance(template_data.get('security_level'), str):
            template_data['security_level'] = SecurityLevels(template_data['security_level'])
        if isinstance(template_data.get('complexity'), str):
            template_data['complexity'] = AuthMethodComplexity(template_data['complexity'])
        
        # Handle vendor lookup by name or ID
        if template_data.get('vendor_name'):
            # In a real implementation, this would query the Vendor table
            # vendor = Vendor.find_by_name(template_data['vendor_name'])
            # if not vendor:
            #     raise ValueError("Vendor not found")
            # template_data['vendor_id'] = vendor.id
            # template_data['connector_id'] = vendor.connector_id
            template_data.pop('vendor_name')
        elif template_data.get('vendor_id'):
            # In a real implementation, this would validate the vendor exists
            # vendor = Vendor.find_by_id(template_data['vendor_id'])
            # if not vendor:
            #     raise ValueError("Invalid vendor")
            # template_data['connector_id'] = vendor.connector_id
            pass
        else:
            raise ValueError("vendor_name or vendor_id input is required")
        
        auth_template = cls(**template_data)
        auth_template._update_config_hash()
        
        return auth_template
    
    def update_mutable(self, input_data: dict) -> None:
        """Update mutable fields from input data"""
        if not input_data:
            return
        
        mutable_fields = {
            'display_name', 'description', 'version', 'status', 'security_level',
            'complexity', 'is_active', 'is_deprecated', 'is_public', 
            'requires_approval', 'encryption_enabled', 'config', 'default_config',
            'validation_config', 'ui_config', 'documentation_url', 'help_text',
            'setup_instructions', 'troubleshooting_guide', 'tags', 'metadata'
        }
        
        for field, value in input_data.items():
            if field in mutable_fields and hasattr(self, field):
                # Handle enum conversions
                if field == 'status' and isinstance(value, str):
                    value = AuthTemplateStatuses(value)
                elif field == 'security_level' and isinstance(value, str):
                    value = SecurityLevels(value)
                elif field == 'complexity' and isinstance(value, str):
                    value = AuthMethodComplexity(value)
                elif field == 'auth_type' and isinstance(value, str):
                    value = AuthTypes(value)
                
                setattr(self, field, value)
        
        if 'config' in input_data:
            self._update_config_hash()
        
        self.updated_at = datetime.utcnow()
    
    def after_create_setup(self):
        """Setup default name and display_name after creation (similar to Rails after_create)"""
        if not self.name:
            # In a real implementation, this would use the vendor name
            # self.name = f"{self.vendor.name}_template_{self.id}"
            self.name = f"template_{self.id}"
        
        if not self.display_name:
            # In a real implementation, this would use the vendor name
            # self.display_name = f"{self.vendor.name.title()} Template {self.id}"
            self.display_name = f"Template {self.id}"
        
        self._update_config_hash()
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert auth template to dictionary for API responses"""
        data = {
            'id': self.id,
            'name': self.name,
            'display_name': self.display_name,
            'description': self.description,
            'version': self.version,
            'auth_type': self.auth_type.value,
            'status': self.status.value,
            'security_level': self.security_level.value,
            'complexity': self.complexity.value,
            'is_active': self.is_active,
            'is_deprecated': self.is_deprecated,
            'is_public': self.is_public,
            'is_validated': self.is_validated,
            'requires_approval': self.requires_approval,
            'encryption_enabled': self.encryption_enabled,
            'usage_count': self.usage_count,
            'success_count': self.success_count,
            'failure_count': self.failure_count,
            'success_rate': self.get_success_rate(),
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'tested_at': self.tested_at.isoformat() if self.tested_at else None,
            'approved_at': self.approved_at.isoformat() if self.approved_at else None,
            'documentation_url': self.documentation_url,
            'help_text': self.help_text,
            'tags': self.get_tags(),
            'metadata': self.get_metadata(),
            'connector_id': self.connector_id,
            'vendor_id': self.vendor_id,
            'created_by_user_id': self.created_by_user_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            # Computed properties
            'active': self.active_(),
            'ready_for_use': self.ready_for_use_(),
            'has_documentation': self.has_documentation_(),
            'well_tested': self.well_tested_(),
            'metrics': self.get_metrics().to_dict()
        }
        
        if include_sensitive:
            data.update({
                'config': self.config,
                'default_config': self.default_config,
                'validation_config': self.validation_config,
                'ui_config': self.ui_config,
                'config_hash': self.config_hash,
                'setup_instructions': self.setup_instructions,
                'troubleshooting_guide': self.troubleshooting_guide,
                'approved_by_user_id': self.approved_by_user_id,
                'deprecated_at': self.deprecated_at.isoformat() if self.deprecated_at else None,
                'archived_at': self.archived_at.isoformat() if self.archived_at else None,
                'config_changed': self.config_changed_(),
                'validation_result': self._perform_validation().to_dict() if self.config else None
            })
        else:
            # Public config (sanitized for security)
            public_config = {}
            if self.config:
                # Only include non-sensitive config fields for public view
                safe_fields = ['auth_url', 'token_url', 'scope', 'key_location', 'key_field']
                for field in safe_fields:
                    if field in self.config:
                        public_config[field] = self.config[field]
            data['public_config'] = public_config
        
        return data
    
    def to_json(self, include_sensitive: bool = False) -> str:
        """Convert auth template to JSON string"""
        return json.dumps(self.to_dict(include_sensitive=include_sensitive), indent=2)