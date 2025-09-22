from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Set
from enum import Enum as PyEnum
import json
import os
import secrets
import re
from ..database import Base

class CredentialStatuses(PyEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    EXPIRED = "expired"
    REVOKED = "revoked"
    PENDING_VERIFICATION = "pending_verification"
    VERIFICATION_FAILED = "verification_failed"
    SUSPENDED = "suspended"

class CredentialTypes(PyEnum):
    DATABASE = "database"
    CLOUD_STORAGE = "cloud_storage"
    API = "api"
    SFTP = "sftp"
    FTP = "ftp"
    EMAIL = "email"
    MESSAGING = "messaging"
    MONITORING = "monitoring"
    OTHER = "other"

class VerificationStatuses(PyEnum):
    NOT_VERIFIED = "not_verified"
    VERIFIED = "verified"
    FAILED = "failed"
    EXPIRED = "expired"
    PENDING = "pending"

class DataCredentials(Base):
    __tablename__ = "data_credentials"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Rails-compatible fields
    connector_type = Column(String(255), nullable=False)  # Rails uses this instead of connection_type
    connection_type = Column(String(255))  # Keep for backward compatibility
    credentials_version = Column(String(255))
    verified_status = Column(Text)
    verified_at = Column(DateTime)
    
    # Status and metadata
    status = Column(SQLEnum(CredentialStatuses), nullable=False, default=CredentialStatuses.ACTIVE)
    verification_status = Column(SQLEnum(VerificationStatuses), nullable=False, default=VerificationStatuses.NOT_VERIFIED)
    credential_type = Column(SQLEnum(CredentialTypes), nullable=False, default=CredentialTypes.OTHER)
    
    # Metadata
    uid = Column(String(24), unique=True, index=True)
    version = Column(Integer, default=1)
    tags = Column(Text)  # JSON string of tags
    extra_metadata = Column(Text)  # JSON string of additional metadata
    
    # Expiration and lifecycle
    expires_at = Column(DateTime)
    last_verified_at = Column(DateTime)
    last_used_at = Column(DateTime)
    verification_attempts = Column(Integer, default=0)
    max_verification_attempts = Column(Integer, default=3)
    
    # Usage tracking
    usage_count = Column(Integer, default=0)
    last_error = Column(Text)
    last_error_at = Column(DateTime)
    
    # Security and access
    is_shared = Column(Boolean, default=False)
    allow_org_access = Column(Boolean, default=False)
    require_approval = Column(Boolean, default=False)
    
    # Refresh and rotation
    auto_refresh_enabled = Column(Boolean, default=False)
    refresh_token_enc = Column(Text)
    refresh_token_enc_iv = Column(String(255))
    next_rotation_at = Column(DateTime)
    
    # Configuration (encrypted)
    credentials_enc = Column(Text)  # Rails uses this naming
    credentials_enc_iv = Column(String(255))  # IV for encryption
    config_enc = Column(Text)  # Keep for backward compatibility
    config_enc_iv = Column(String(255))  # Keep for backward compatibility
    
    # Auth configuration
    auth_template_id = Column(Integer, ForeignKey("auth_templates.id"))
    vendor_id = Column(Integer, ForeignKey("vendors.id"))
    users_api_key_id = Column(Integer, ForeignKey("api_keys.id"))
    api_key_reference = Column(String(255))
    copied_from_id = Column(Integer, ForeignKey("data_credentials.id"))
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    owner = relationship("User", back_populates="data_credentials")
    org = relationship("Org", back_populates="data_credentials")
    auth_template = relationship("AuthTemplate", foreign_keys=[auth_template_id])
    vendor = relationship("Vendor", foreign_keys=[vendor_id])
    users_api_key = relationship("ApiKey", foreign_keys=[users_api_key_id])
    copied_from = relationship("DataCredentials", remote_side=[id], foreign_keys=[copied_from_id])
    data_sources = relationship("DataSource", back_populates="data_credentials")
    data_sinks = relationship("DataSink", back_populates="data_credentials")
    
    # Rails constants
    VERIFIED_STATUS_MAX_LENGTH = 1024
    SKIP_ATTRIBUTES_IN_SEARCH = ['users_api_key_id', 'credentials_enc', 'credentials_enc_iv']
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.ensure_uid_()
        self._credentials = None
        self._config_spec = None
        self._decrypted_credentials = None
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if credentials are active (Rails pattern)"""
        return self.status == CredentialStatuses.ACTIVE
    
    def inactive_(self) -> bool:
        """Check if credentials are inactive (Rails pattern)"""
        return self.status == CredentialStatuses.INACTIVE
        
    def expired_(self) -> bool:
        """Check if credentials are expired (Rails pattern)"""
        return (self.status == CredentialStatuses.EXPIRED or 
                (self.expires_at and self.expires_at <= datetime.now()))
        
    def revoked_(self) -> bool:
        """Check if credentials are revoked (Rails pattern)"""
        return self.status == CredentialStatuses.REVOKED
        
    def suspended_(self) -> bool:
        """Check if credentials are suspended (Rails pattern)"""
        return self.status == CredentialStatuses.SUSPENDED
    
    def verified_(self) -> bool:
        """Check if credentials are verified (Rails pattern)"""
        return (self.verification_status == VerificationStatuses.VERIFIED and
                not self.verification_expired_())
        
    def verification_pending_(self) -> bool:
        """Check if verification is pending (Rails pattern)"""
        return self.verification_status == VerificationStatuses.PENDING
        
    def verification_failed_(self) -> bool:
        """Check if verification failed (Rails pattern)"""
        return self.verification_status == VerificationStatuses.FAILED
    
    def verification_expired_(self) -> bool:
        """Check if verification is expired (Rails pattern)"""
        if not self.last_verified_at:
            return True
        # Consider verification expired after 24 hours
        return self.last_verified_at < datetime.now() - timedelta(hours=24)
    
    def needs_verification_(self) -> bool:
        """Check if credentials need verification (Rails pattern)"""
        return (not self.verified_() or 
                self.verification_expired_() or
                self.verification_status == VerificationStatuses.NOT_VERIFIED)
    
    def can_be_verified_(self) -> bool:
        """Check if credentials can be verified (Rails pattern)"""
        return (self.active_() and 
                self.verification_attempts < self.max_verification_attempts and
                bool(self.credentials))
    
    def shared_(self) -> bool:
        """Check if credentials are shared (Rails pattern)"""
        return self.is_shared
        
    def private_(self) -> bool:
        """Check if credentials are private (Rails pattern)"""
        return not self.is_shared
    
    def copy_(self) -> bool:
        """Check if credentials are a copy (Rails pattern)"""
        return self.copied_from_id is not None
        
    def original_(self) -> bool:
        """Check if credentials are original (not a copy) (Rails pattern)"""
        return self.copied_from_id is None
    
    def has_refresh_token_(self) -> bool:
        """Check if has refresh token (Rails pattern)"""
        return bool(self.refresh_token_enc)
    
    def auto_refresh_enabled_(self) -> bool:
        """Check if auto refresh is enabled (Rails pattern)"""
        return self.auto_refresh_enabled
    
    def requires_rotation_(self) -> bool:
        """Check if credentials require rotation (Rails pattern)"""
        return (self.next_rotation_at and 
                self.next_rotation_at <= datetime.now())
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if credentials were recently used (Rails pattern)"""
        if not self.last_used_at:
            return False
        return self.last_used_at >= datetime.now() - timedelta(hours=hours)
    
    def stale_(self, days: int = 30) -> bool:
        """Check if credentials are stale (Rails pattern)"""
        if not self.last_used_at:
            return True
        return self.last_used_at < datetime.now() - timedelta(days=days)
    
    def has_errors_(self) -> bool:
        """Check if credentials have recent errors (Rails pattern)"""
        return bool(self.last_error)
    
    def accessible_by_(self, user, access_level: str = 'read') -> bool:
        """Check if user can access credentials (Rails pattern)"""
        if not user:
            return False
            
        # Owner always has access
        if self.owner_id == user.id:
            return True
            
        # Shared credentials are accessible to org members
        if self.shared_() and user.org_id == self.org_id:
            if not self.allow_org_access and access_level != 'read':
                return False
            return True
            
        # Private credentials require explicit permission
        return False
    
    def editable_by_(self, user) -> bool:
        """Check if user can edit credentials (Rails pattern)"""
        return self.accessible_by_(user, 'write')
    
    def deletable_by_(self, user) -> bool:
        """Check if user can delete credentials (Rails pattern)"""
        return self.accessible_by_(user, 'admin') or self.owner_id == user.id
    
    # Rails-style bang methods (state changes)
    def activate_(self) -> None:
        """Activate credentials (Rails bang method pattern)"""
        self.status = CredentialStatuses.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: Optional[str] = None) -> None:
        """Deactivate credentials (Rails bang method pattern)"""
        self.status = CredentialStatuses.INACTIVE
        self.updated_at = datetime.now()
        if reason:
            self._update_metadata('deactivation_reason', reason)
    
    def expire_(self) -> None:
        """Expire credentials (Rails bang method pattern)"""
        self.status = CredentialStatuses.EXPIRED
        self.expires_at = datetime.now()
        self.updated_at = datetime.now()
    
    def revoke_(self, reason: Optional[str] = None) -> None:
        """Revoke credentials (Rails bang method pattern)"""
        self.status = CredentialStatuses.REVOKED
        self.updated_at = datetime.now()
        if reason:
            self._update_metadata('revocation_reason', reason)
    
    def suspend_(self, reason: Optional[str] = None) -> None:
        """Suspend credentials (Rails bang method pattern)"""
        self.status = CredentialStatuses.SUSPENDED
        self.updated_at = datetime.now()
        if reason:
            self._update_metadata('suspension_reason', reason)
    
    def verify_(self) -> None:
        """Mark credentials as verified (Rails bang method pattern)"""
        self.verification_status = VerificationStatuses.VERIFIED
        self.last_verified_at = datetime.now()
        self.verified_at = datetime.now()
        self.verification_attempts = 0
        self.last_error = None
        self.last_error_at = None
        self.updated_at = datetime.now()
    
    def mark_verification_failed_(self, error: Optional[str] = None) -> None:
        """Mark verification as failed (Rails bang method pattern)"""
        self.verification_status = VerificationStatuses.FAILED
        self.verification_attempts = (self.verification_attempts or 0) + 1
        self.verified_at = None
        
        if error:
            self.last_error = error
            self.last_error_at = datetime.now()
        
        # Suspend if max attempts reached
        if self.verification_attempts >= self.max_verification_attempts:
            self.suspend_("Max verification attempts exceeded")
        
        self.updated_at = datetime.now()
    
    def mark_verification_pending_(self) -> None:
        """Mark verification as pending (Rails bang method pattern)"""
        self.verification_status = VerificationStatuses.PENDING
        self.updated_at = datetime.now()
    
    def track_usage_(self) -> None:
        """Track credential usage (Rails bang method pattern)"""
        self.usage_count = (self.usage_count or 0) + 1
        self.last_used_at = datetime.now()
        self.updated_at = datetime.now()
    
    def clear_errors_(self) -> None:
        """Clear error information (Rails bang method pattern)"""
        self.last_error = None
        self.last_error_at = None
        self.updated_at = datetime.now()
    
    def enable_auto_refresh_(self) -> None:
        """Enable auto refresh (Rails bang method pattern)"""
        self.auto_refresh_enabled = True
        self.updated_at = datetime.now()
    
    def disable_auto_refresh_(self) -> None:
        """Disable auto refresh (Rails bang method pattern)"""
        self.auto_refresh_enabled = False
        self.updated_at = datetime.now()
    
    def schedule_rotation_(self, days_from_now: int = 90) -> None:
        """Schedule credential rotation (Rails bang method pattern)"""
        self.next_rotation_at = datetime.now() + timedelta(days=days_from_now)
        self.updated_at = datetime.now()
    
    def make_shared_(self) -> None:
        """Make credentials shared (Rails bang method pattern)"""
        self.is_shared = True
        self.updated_at = datetime.now()
    
    def make_private_(self) -> None:
        """Make credentials private (Rails bang method pattern)"""
        self.is_shared = False
        self.allow_org_access = False
        self.updated_at = datetime.now()
    
    def allow_org_access_(self) -> None:
        """Allow organization access (Rails bang method pattern)"""
        self.allow_org_access = True
        self.is_shared = True  # Must be shared to allow org access
        self.updated_at = datetime.now()
    
    def restrict_org_access_(self) -> None:
        """Restrict organization access (Rails bang method pattern)"""
        self.allow_org_access = False
        self.updated_at = datetime.now()
    
    def copy_from_(self, source_credentials, new_name: Optional[str] = None) -> None:
        """Create credentials as copy of another (Rails bang method pattern)"""
        if not source_credentials:
            raise ValueError("Source credentials is required")
            
        self.copied_from_id = source_credentials.id
        self.name = new_name or f"Copy of {source_credentials.name}"
        self.description = source_credentials.description
        self.connector_type = source_credentials.connector_type
        self.credential_type = source_credentials.credential_type
        
        # Copy non-sensitive metadata
        if source_credentials.metadata:
            try:
                source_meta = json.loads(source_credentials.metadata)
                filtered_meta = {k: v for k, v in source_meta.items() 
                               if not k.startswith('_') and k not in ['api_keys', 'secrets']}
                self.extra_metadata = json.dumps(filtered_meta) if filtered_meta else None
            except (json.JSONDecodeError, TypeError):
                pass
                
        # Copy tags
        self.set_tags_(source_credentials.tags_list())
        
        # Note: Don't copy actual credentials for security
        self.status = CredentialStatuses.PENDING_VERIFICATION
        self.verification_status = VerificationStatuses.NOT_VERIFIED
        self.updated_at = datetime.now()
    
    def ensure_uid_(self) -> None:
        """Ensure unique UID is set (Rails before_save pattern)"""
        if self.uid:
            return
        
        max_attempts = 10
        for _ in range(max_attempts):
            uid = secrets.token_hex(12)  # 24 character hex string
            if not self.__class__.query.filter_by(uid=uid).first():
                self.uid = uid
                return
        
        raise ValueError("Failed to generate unique credentials UID")
    
    def _update_metadata(self, key: str, value: Any) -> None:
        """Update metadata field (Rails helper pattern)"""
        try:
            current_meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
        except (json.JSONDecodeError, TypeError):
            current_meta = {}
            
        current_meta[key] = value
        self.extra_metadata = json.dumps(current_meta)
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Get metadata value (Rails helper pattern)"""
        try:
            meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
            return meta.get(key, default)
        except (json.JSONDecodeError, TypeError):
            return default
    
    # Rails class methods and scopes
    @classmethod
    def find_by_uid(cls, uid: str):
        """Find credentials by UID (Rails finder pattern)"""
        return cls.query.filter_by(uid=uid).first()
    
    @classmethod
    def find_by_uid_(cls, uid: str):
        """Find credentials by UID or raise exception (Rails bang finder pattern)"""
        credentials = cls.find_by_uid(uid)
        if not credentials:
            raise ValueError(f"DataCredentials with UID '{uid}' not found")
        return credentials
    
    @classmethod
    def active_credentials(cls, org=None):
        """Get active credentials (Rails scope pattern)"""
        query = cls.query.filter_by(status=CredentialStatuses.ACTIVE)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def verified_credentials(cls, org=None):
        """Get verified credentials (Rails scope pattern)"""
        query = cls.query.filter_by(
            status=CredentialStatuses.ACTIVE,
            verification_status=VerificationStatuses.VERIFIED
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def by_connector_type(cls, connector_type: str, org=None):
        """Get credentials by connector type (Rails scope pattern)"""
        query = cls.query.filter_by(connector_type=connector_type)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def needing_verification(cls, org=None):
        """Get credentials needing verification (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=24)
        query = cls.query.filter(
            (cls.verification_status == VerificationStatuses.NOT_VERIFIED) |
            (cls.verification_status == VerificationStatuses.FAILED) |
            (cls.last_verified_at < cutoff)
        ).filter_by(status=CredentialStatuses.ACTIVE)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def expired_credentials(cls, org=None):
        """Get expired credentials (Rails scope pattern)"""
        now = datetime.now()
        query = cls.query.filter(
            (cls.status == CredentialStatuses.EXPIRED) |
            (cls.expires_at <= now)
        )
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def stale_credentials(cls, days: int = 30, org=None):
        """Get stale credentials (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        query = cls.query.filter(
            (cls.last_used_at < cutoff) | (cls.last_used_at.is_(None))
        ).filter_by(status=CredentialStatuses.ACTIVE)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def accessible_to(cls, user, access_level: str = 'read'):
        """Get credentials accessible to user (Rails scope pattern)"""
        if not user:
            return cls.query.filter(False)  # Empty query
        
        # Start with user's own credentials
        query = cls.query.filter_by(owner_id=user.id)
        
        # Add shared credentials in same org
        shared_query = cls.query.filter_by(
            is_shared=True,
            org_id=user.org_id if hasattr(user, 'org_id') else None
        )
        
        if access_level == 'read':
            # Union all queries for read access
            query = query.union(shared_query)
        elif access_level in ['write', 'admin']:
            # Only include shared credentials with org access for write/admin
            shared_with_access = shared_query.filter_by(allow_org_access=True)
            query = query.union(shared_with_access)
        
        return query.distinct()
    
    @classmethod
    def requiring_rotation(cls, org=None):
        """Get credentials requiring rotation (Rails scope pattern)"""
        now = datetime.now()
        query = cls.query.filter(
            cls.next_rotation_at <= now
        ).filter_by(status=CredentialStatuses.ACTIVE)
        if org:
            query = query.filter_by(org_id=org.id if hasattr(org, 'id') else org)
        return query
    
    @classmethod
    def validate_connector_type(cls, connector_type: str) -> Optional[str]:
        """Validate connector type (Rails pattern)"""
        # This would validate against DataSink.connector_types
        # For now, accept common connector types
        valid_types = ['mysql', 'postgres', 'snowflake', 's3', 'nexla_monitor', 'gdrive', 'api', 'sftp', 
                      'bigquery', 'redshift', 'azure_blob', 'gcs', 'ftp', 'http', 'webhook']
        return connector_type if connector_type in valid_types else None
    
    # Rails property accessors
    @property  
    def api_key(self):
        """Alias for users_api_key (Rails pattern)"""
        return self.users_api_key
    
    @property
    def credentials_type(self) -> Optional[str]:
        """Get credentials type (Rails pattern)"""
        return self.connector_type
    
    @property
    def credentials(self) -> Dict[str, Any]:
        """Get decrypted credentials (Rails pattern)"""
        if self._credentials is not None:
            return self._credentials
        
        # This would decrypt the credentials_enc field
        # For now, return empty dict or mock data
        if self.credentials_enc:
            try:
                # Mock decryption - in real implementation would use encryption key
                return json.loads(self.credentials_enc) if self.credentials_enc != 'encrypted_data' else {}
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    @credentials.setter
    def credentials(self, value: Dict[str, Any]):
        """Set credentials with encryption (Rails pattern)"""
        self._credentials = value
        # This would encrypt the credentials
        # For now, store as JSON or placeholder
        if value:
            if isinstance(value, dict):
                # Mock encryption - in real implementation would encrypt
                self.credentials_enc = json.dumps(value)
                self.credentials_enc_iv = "mock_iv_" + str(hash(str(value)))[:16]
            else:
                self.credentials_enc = str(value)
        else:
            self.credentials_enc = None
            self.credentials_enc_iv = None
    
    @property
    def api_keys(self) -> List:
        """Get associated API keys (Rails pattern)"""
        return [self.api_key] if self.api_key else []
    
    def credentials_non_secure_data(self) -> Dict[str, Any]:
        """Get non-secure credentials data (Rails pattern)"""
        self.load_config()
        credentials = self.credentials
        credentials_data = {}
        
        if self._config_spec and credentials:
            config_credentials_type = self._config_spec.get("spec", {}).get(self.connector_type)
            
            if config_credentials_type and config_credentials_type.get("non_secure_config"):
                non_secure_config = config_credentials_type["non_secure_config"]
                for key, val in credentials.items():
                    if key in non_secure_config:
                        credentials_data[key] = val
        
        return credentials_data
    
    def template_config(self, secure_fields: bool = False) -> Dict[str, Any]:
        """Get template configuration (Rails pattern)"""
        template_config = {}
        
        if self.vendor:
            if secure_fields:
                template_config = self.credentials
            else:
                credentials = self.credentials
                # This would query AuthParameter.where(vendor_id: self.vendor_id)
                # For now, mock the auth parameters
                auth_params = self._get_auth_parameters(secure_fields)
                
                for auth_param in auth_params:
                    cred_value = credentials.get(auth_param)
                    if cred_value is not None:
                        template_config[auth_param] = cred_value
        
        return template_config
    
    def enc_key(self) -> str:
        """Get encryption key (Rails pattern)"""
        # This would use API_SECRETS[:enc][:credentials_key][0..31]
        # For now, return a mock key
        return os.environ.get('CREDENTIALS_ENC_KEY', 'mock_encryption_key_32_chars_x')[:32]
    
    def set_defaults(self, user, org) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        self.org = org
    
    # Rails business logic methods
    def update_mutable(self, api_user_info: Dict[str, Any], input_data: Dict[str, Any], request=None) -> None:
        """Update mutable fields (Rails update_mutable! pattern)"""
        if not input_data or not api_user_info:
            return
        
        # Handle referenced resource fields
        ref_fields = input_data.pop('referenced_resource_ids', None)
        if ref_fields:
            self.verify_ref_resources(api_user_info.get('input_owner'), ref_fields)
        
        # Handle tags
        tags = input_data.pop('tags', None)
        
        # Update basic fields
        if 'credentials_version' in input_data:
            self.credentials_version = input_data['credentials_version']
        
        if 'name' in input_data:
            self.name = input_data['name']
        
        if 'description' in input_data:
            self.description = input_data['description']
        
        # Update owner and org if different
        if self.owner != api_user_info.get('input_owner'):
            self.owner = api_user_info['input_owner']
        
        if self.org != api_user_info.get('input_org'):
            self.org = api_user_info['input_org']
        
        # Update verification status
        if 'verified_status' in input_data:
            self.verified_status = input_data['verified_status']
        
        # Handle connector type changes
        if 'credentials_type' in input_data:
            # Convert backwards-compatible key to new key
            if 'connector_type' not in input_data:
                input_data['connector_type'] = input_data['credentials_type']
        
        if 'connector_type' in input_data:
            if not self.validate_connector_type(input_data['connector_type']):
                raise ValueError("Invalid connector type")
            self.connector_type = input_data['connector_type']
        
        # Update credentials
        if 'credentials' in input_data and isinstance(input_data['credentials'], dict):
            # Validate config
            validation_result = self.validate_config(input_data['credentials'])
            if validation_result:
                raise ValueError(validation_result.get('description', 'Invalid configuration'))
            
            # Set credentials_type for backwards compatibility
            input_data['credentials']['credentials_type'] = (
                self.connector.connection_type if hasattr(self, 'connector') and self.connector 
                else self.connector_type
            )
            self.credentials = input_data['credentials']
        
        elif 'vendor_id' in input_data or 'vendor_name' in input_data:
            if 'template_config' in input_data:
                self.update_from_template(input_data, api_user_info)
        
        # Handle tags if provided
        if tags:
            self.add_owned_tags(tags, api_user_info.get('input_owner'))
        
        # Update referenced resources
        if ref_fields:
            self.update_referenced_resources(ref_fields)
    
    def update_from_template(self, input_data: Dict[str, Any], api_user_info: Dict[str, Any]) -> None:
        """Update credentials from vendor template (Rails pattern)"""
        # Find vendor
        vendor = None
        if 'vendor_name' in input_data:
            vendor = self._find_vendor_by_name(input_data['vendor_name'])
        elif 'vendor_id' in input_data:
            vendor = self._find_vendor_by_id(input_data['vendor_id'])
        
        if not vendor:
            raise ValueError("Invalid vendor")
        
        # Find auth template
        auth_template = self._find_auth_template(vendor, input_data)
        if not auth_template:
            raise ValueError("No auth template found")
        
        self.auth_template = auth_template
        
        # Get auth parameters
        auth_params = self._get_auth_template_parameters(auth_template)
        
        # Process template configuration
        self.add_data_cred_params(auth_params, input_data['template_config'])
        template_config = input_data['template_config']
        credentials = {}
        
        # Process template config
        for key, value in auth_template.config.items():
            if '${' in str(value):
                processed_value = self._process_template_value(str(value), auth_params, template_config)
                credentials[key] = processed_value
            else:
                credentials[key] = value
        
        # Set final credentials
        self.connector_type = auth_template.connector.type
        credentials['credentials_type'] = self.connector_type
        self.vendor_id = vendor.id
        
        # Validate configuration
        self._validate_vendor_endpoint_config(credentials, auth_params)
        self.credentials = credentials
    
    def generate_api_key(self, api_user_info: Dict[str, Any]) -> None:
        """Generate API key for Nexla Monitor (Rails pattern)"""
        # This would call api_user_info.user.build_api_key_from_input
        # For now, create a placeholder API key
        self._create_nexla_monitor_api_key(api_user_info)
    
    def set_verified_status(self, status_str: str) -> None:
        """Set verified status with success detection (Rails pattern)"""
        status_str = str(status_str)[:self.VERIFIED_STATUS_MAX_LENGTH]
        
        # Check if status indicates success (Rails success_code? method)
        if self._is_success_code(status_str.split(' ')[0]):
            self.verified_at = datetime.now()
            # UI depends on this specific status string
            status_str = "200 Ok"
        else:
            self.verified_at = None
        
        self.verified_status = status_str if status_str else None
    
    def add_data_cred_params(self, auth_params: List[str], config: Dict[str, Any]) -> None:
        """Add data credential parameters (Rails pattern)"""
        if isinstance(config, dict):
            for key in config.keys():
                if key not in auth_params:
                    auth_params.append(key)
    
    def validate_gdrive_credentials(self, request, user) -> None:
        """Validate Google Drive credentials (Rails pattern)"""
        credentials = self.credentials
        
        if credentials.get('one_time_code') and not credentials.get('access_token'):
            result = self._exchange_google_one_time_code(user.email, credentials, request)
            if not result:
                raise ValueError("Invalid Google Drive credentials")
            
            result_dict = json.loads(json.dumps(result))
            self.credentials = {**result_dict, **credentials}
        
        if not credentials.get('access_token'):
            raise ValueError("Invalid Google Drive credentials type")
    
    def refresh(self) -> None:
        """Refresh credentials (Rails pattern)"""
        # For now, only handle Google Drive
        if self.credentials_type != 'gdrive':
            return
        
        credentials = self._refresh_google_credentials(self.credentials)
        if isinstance(credentials, str):
            raise ValueError(credentials)
        
        self.credentials = credentials
        # This would call save()
    
    def has_credentials(self) -> bool:
        """Check if has valid credentials (Rails pattern)"""
        return (self.validate_connector_type(self.connector_type) is not None 
                and bool(self.credentials))
    
    def resources(self) -> Optional[Dict[str, List[int]]]:
        """Get associated resources (Rails pattern)"""
        result = {}
        
        # This would query each model for resources using this credentials
        result['data_sources'] = self._get_associated_resource_ids('DataSource')
        result['data_sinks'] = self._get_associated_resource_ids('DataSink')  
        result['data_sets'] = self._get_associated_resource_ids('DataSet')
        result['gen_ai_configs'] = self._get_associated_resource_ids('GenAiConfig')
        result['catalog_configs'] = self._get_associated_resource_ids('CatalogConfig')
        result['code_containers'] = self._get_associated_resource_ids('CodeContainer')
        
        # Return None if no resources found
        if all(not resources for resources in result.values()):
            return None
        
        return result
    
    def origin_node_ids(self) -> List[int]:
        """Get origin node IDs (Rails pattern)"""
        ids = []
        
        # This would query DataSource and DataSink models
        data_source_nodes = self._get_origin_node_ids_for_model('DataSource')
        data_sink_nodes = self._get_origin_node_ids_for_model('DataSink')
        
        ids.extend(data_source_nodes)
        ids.extend(data_sink_nodes)
        
        return list(set(ids))  # Remove duplicates
    
    # Rails tagging methods
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        if not self.tags:
            return []
        try:
            return json.loads(self.tags)
        except (json.JSONDecodeError, TypeError):
            return []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    def set_tags_(self, tags: Union[List[str], Set[str], str]) -> None:
        """Set credentials tags (Rails bang method pattern)"""
        if isinstance(tags, str):
            # Handle comma-separated string
            tag_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
        else:
            tag_list = list(set(tags)) if tags else []
        
        self.tags = json.dumps(tag_list) if tag_list else None
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add a tag to credentials (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.add(tag.strip())
        self.set_tags_(current_tags)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove a tag from credentials (Rails bang method pattern)"""
        current_tags = set(self.tags_list())
        current_tags.discard(tag.strip())
        self.set_tags_(current_tags)
    
    def has_tag_(self, tag: str) -> bool:
        """Check if credentials has specific tag (Rails pattern)"""
        return tag.strip() in self.tags_list()
    
# Helper methods (would be private in Rails)
    def load_config(self) -> None:
        """Load configuration spec (Rails pattern)"""
        if not self._config_spec:
            # This would load the configuration specification
            self._config_spec = {"spec": {}}
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Validate configuration (Rails CodeUtils.validate_config pattern)"""
        # Mock validation - would implement actual validation logic
        if not isinstance(config, dict):
            return {"description": "Configuration must be a dictionary"}
        return None
    
    def _get_auth_parameters(self, secure_fields: bool = False) -> List[str]:
        """Get auth parameters for vendor (helper method)"""
        # Mock auth parameters
        return ['username', 'password', 'host', 'port', 'database'] if self.vendor_id else []
    
    def _find_vendor_by_name(self, name: str):
        """Find vendor by name (helper method)"""
        # This would query Vendor.find_by_name(name)
        return None  # Placeholder
    
    def _find_vendor_by_id(self, vendor_id: int):
        """Find vendor by ID (helper method)"""
        # This would query Vendor.find(vendor_id)
        return None  # Placeholder
    
    def _find_auth_template(self, vendor, input_data: Dict[str, Any]):
        """Find auth template for vendor (helper method)"""
        # Mock auth template finding logic
        return None  # Placeholder
    
    def _get_auth_template_parameters(self, auth_template) -> List[str]:
        """Get parameters for auth template (helper method)"""
        return []  # Placeholder
    
    def _process_template_value(self, value: str, auth_params: List[str], 
                               template_config: Dict[str, Any]) -> Any:
        """Process template value with parameter substitution (helper method)"""
        # Mock template processing
        return value
    
    def _validate_vendor_endpoint_config(self, credentials: Dict[str, Any], 
                                        auth_params: List[str]) -> None:
        """Validate vendor endpoint config (helper method)"""
        pass  # Placeholder
    
    def _create_nexla_monitor_api_key(self, api_user_info: Dict[str, Any]) -> None:
        """Create Nexla Monitor API key (helper method)"""
        pass  # Placeholder
    
    def _is_success_code(self, status_code: str) -> bool:
        """Check if status code indicates success (helper method)"""
        try:
            code = int(status_code)
            return 200 <= code < 300
        except (ValueError, TypeError):
            return False
    
    def _exchange_google_one_time_code(self, email: str, credentials: Dict[str, Any], request) -> Optional[Dict]:
        """Exchange Google one-time code for tokens (helper method)"""
        return None  # Placeholder
    
    def _refresh_google_credentials(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Refresh Google credentials (helper method)"""
        return credentials  # Placeholder
    
    def _get_associated_resource_ids(self, model_name: str) -> List[int]:
        """Get resource IDs associated with this credentials (helper method)"""
        # This would query the appropriate model
        return []  # Placeholder
    
    def _get_origin_node_ids_for_model(self, model_name: str) -> List[int]:
        """Get origin node IDs for a specific model (helper method)"""
        return []  # Placeholder
    
    def verify_ref_resources(self, owner, ref_fields) -> None:
        """Verify referenced resources (helper method)"""
        pass  # Placeholder
    
    def update_referenced_resources(self, ref_fields) -> None:
        """Update referenced resources (helper method)"""
        pass  # Placeholder
    
    def add_owned_tags(self, tags, owner) -> None:
        """Add owned tags (helper method)"""
        if isinstance(tags, (list, tuple)):
            self.set_tags_(tags)
        elif tags:
            self.set_tags_([tags])
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        if self.copy_():
            return f"{self.name} (Copy)"
        elif self.shared_():
            return f"{self.name} (Shared)"
        return self.name or "Unnamed Credentials"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        status_map = {
            CredentialStatuses.ACTIVE: "Active",
            CredentialStatuses.INACTIVE: "Inactive",
            CredentialStatuses.EXPIRED: "Expired",
            CredentialStatuses.REVOKED: "Revoked",
            CredentialStatuses.PENDING_VERIFICATION: "Pending Verification",
            CredentialStatuses.VERIFICATION_FAILED: "Verification Failed",
            CredentialStatuses.SUSPENDED: "Suspended"
        }
        return status_map.get(self.status, "Unknown")
    
    def verification_status_display(self) -> str:
        """Get human-readable verification status (Rails pattern)"""
        status_map = {
            VerificationStatuses.NOT_VERIFIED: "Not Verified",
            VerificationStatuses.VERIFIED: "Verified",
            VerificationStatuses.FAILED: "Failed",
            VerificationStatuses.EXPIRED: "Expired",
            VerificationStatuses.PENDING: "Pending"
        }
        return status_map.get(self.verification_status, "Unknown")
    
    def credential_type_display(self) -> str:
        """Get human-readable credential type (Rails pattern)"""
        type_map = {
            CredentialTypes.DATABASE: "Database",
            CredentialTypes.CLOUD_STORAGE: "Cloud Storage",
            CredentialTypes.API: "API",
            CredentialTypes.SFTP: "SFTP",
            CredentialTypes.FTP: "FTP",
            CredentialTypes.EMAIL: "Email",
            CredentialTypes.MESSAGING: "Messaging",
            CredentialTypes.MONITORING: "Monitoring",
            CredentialTypes.OTHER: "Other"
        }
        return type_map.get(self.credential_type, "Other")
    
    def usage_summary(self) -> Dict[str, Any]:
        """Get usage summary (Rails pattern)"""
        return {
            'usage_count': self.usage_count or 0,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'last_verified_at': self.last_verified_at.isoformat() if self.last_verified_at else None,
            'verification_attempts': self.verification_attempts or 0,
            'days_since_last_use': (datetime.now() - self.last_used_at).days if self.last_used_at else None,
            'is_stale': self.stale_()
        }
    
    def security_summary(self) -> Dict[str, Any]:
        """Get security summary (Rails pattern)"""
        return {
            'is_shared': self.is_shared,
            'allow_org_access': self.allow_org_access,
            'require_approval': self.require_approval,
            'is_copy': self.copy_(),
            'has_errors': self.has_errors_(),
            'needs_verification': self.needs_verification_(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'next_rotation_at': self.next_rotation_at.isoformat() if self.next_rotation_at else None,
            'auto_refresh_enabled': self.auto_refresh_enabled
        }
    
    def validate_(self) -> List[str]:
        """Validate credentials data (Rails validation pattern)"""
        errors = []
        
        if not self.name or not self.name.strip():
            errors.append("Name cannot be blank")
        elif len(self.name) > 255:
            errors.append("Name is too long (maximum 255 characters)")
        
        if self.name and not re.match(r'^[\w\s\-\.\(\)]+$', self.name):
            errors.append("Name contains invalid characters")
        
        if not self.owner_id:
            errors.append("Owner is required")
        
        if not self.org_id:
            errors.append("Organization is required")
        
        if not self.connector_type:
            errors.append("Connector type is required")
        elif not self.validate_connector_type(self.connector_type):
            errors.append("Invalid connector type")
        
        if self.description and len(self.description) > 10000:
            errors.append("Description is too long (maximum 10,000 characters)")
        
        if self.expires_at and self.expires_at <= datetime.now():
            errors.append("Expiration date must be in the future")
        
        if self.verification_attempts and self.verification_attempts < 0:
            errors.append("Verification attempts cannot be negative")
        
        if self.usage_count and self.usage_count < 0:
            errors.append("Usage count cannot be negative")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if credentials are valid (Rails validation pattern)"""
        return len(self.validate_()) == 0
    
    def to_dict(self, include_metadata: bool = False, include_credentials: bool = False, 
               include_relationships: bool = False) -> Dict[str, Any]:
        """Convert data credentials to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'description': self.description,
            'connector_type': self.connector_type,
            'credentials_type': self.credentials_type,
            'connection_type': self.connection_type,
            'credential_type': self.credential_type.value if self.credential_type else None,
            'credential_type_display': self.credential_type_display(),
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'verification_status': self.verification_status.value if self.verification_status else None,
            'verification_status_display': self.verification_status_display(),
            'credentials_version': self.credentials_version,
            'verified_status': self.verified_status,
            'verified_at': self.verified_at.isoformat() if self.verified_at else None,
            'last_verified_at': self.last_verified_at.isoformat() if self.last_verified_at else None,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'vendor_id': self.vendor_id,
            'auth_template_id': self.auth_template_id,
            'copied_from_id': self.copied_from_id,
            'version': self.version,
            'is_shared': self.is_shared,
            'is_copy': self.copy_(),
            'allow_org_access': self.allow_org_access,
            'require_approval': self.require_approval,
            'auto_refresh_enabled': self.auto_refresh_enabled,
            'has_credentials': self.has_credentials(),
            'verified': self.verified_(),
            'needs_verification': self.needs_verification_(),
            'tags': self.tags_list(),
            'usage_count': self.usage_count or 0,
            'verification_attempts': self.verification_attempts or 0,
            'api_keys_count': len(self.api_keys),
            'usage_summary': self.usage_summary(),
            'security_summary': self.security_summary()
        }
        
        if self.has_errors_():
            result.update({
                'last_error': self.last_error,
                'last_error_at': self.last_error_at.isoformat() if self.last_error_at else None
            })
        
        if self.requires_rotation_():
            result['next_rotation_at'] = self.next_rotation_at.isoformat() if self.next_rotation_at else None
        
        if include_metadata and self.extra_metadata:
            try:
                result['metadata'] = json.loads(self.extra_metadata)
            except (json.JSONDecodeError, TypeError):
                pass
        
        if include_credentials and self.has_credentials():
            # Only include non-secure data
            result['non_secure_credentials'] = self.credentials_non_secure_data()
        
        if include_relationships:
            result.update({
                'owner': self.owner.to_dict() if self.owner else None,
                'org': self.org.to_dict() if self.org else None,
                'vendor': self.vendor.to_dict() if self.vendor else None,
                'auth_template': self.auth_template.to_dict() if self.auth_template else None,
                'copied_from': self.copied_from.to_dict() if self.copied_from else None
            })
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert credentials to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'display_name': self.display_name(),
            'connector_type': self.connector_type,
            'status': self.status.value if self.status else None,
            'status_display': self.status_display(),
            'verification_status': self.verification_status.value if self.verification_status else None,
            'verification_status_display': self.verification_status_display(),
            'is_shared': self.is_shared,
            'verified': self.verified_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None
        }
    
    def __repr__(self) -> str:
        """String representation (Rails pattern)"""
        return f"<DataCredentials(id={self.id}, uid='{self.uid}', name='{self.name}', connector_type='{self.connector_type}')>"
    
    def __str__(self) -> str:
        """Human-readable string (Rails pattern)"""
        return self.display_name()