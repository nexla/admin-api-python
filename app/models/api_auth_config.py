from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, Enum as SQLEnum, JSON
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, Union
import json
import hashlib
import random
import string

from app.database import Base

class Protocols(PyEnum):
    SAML = "saml"
    OIDC = "oidc"
    PASSWORD = "password"
    GOOGLE = "google"
    OMNI = "omni"

class ApiAuthConfig(Base):
    __tablename__ = 'api_auth_configs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    
    uid = Column(String(255), unique=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    protocol = Column(SQLEnum(Protocols), nullable=False, default=Protocols.SAML)
    
    # Global configuration flag
    global_ = Column('global', Boolean, default=False)
    
    # User auto-creation settings
    auto_create_users_enabled = Column(Boolean, default=False)
    
    # Base URL configuration
    nexla_base_url = Column(Text)
    
    # SAML-specific fields
    name_identifier_format = Column(Text)
    service_entity_id = Column(Text)
    assertion_consumer_url = Column(Text)
    idp_entity_id = Column(Text)
    idp_sso_target_url = Column(Text)
    idp_slo_target_url = Column(Text)
    idp_cert = Column(Text)
    
    # OIDC-specific fields
    oidc_domain = Column(Text)
    oidc_keys_url_key = Column(Text)
    oidc_id_claims = Column(JSON)
    oidc_access_claims = Column(JSON)
    
    # Configuration storage
    security_settings = Column(JSON)
    extra_metadata = Column(Text)
    client_config = Column(JSON)
    secret_config = Column(Text)  # Encrypted in production
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id], back_populates="api_auth_configs")
    org = relationship("Org", foreign_keys=[org_id], back_populates="api_auth_configs")
    
    # Constants
    DEFAULT_IDP_PROTOCOL = Protocols.SAML
    DEFAULT_NAME_IDENTIFIER_FORMAT = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"
    DEFAULT_SECURITY_SETTINGS = {
        "authn_requests_signed": False,
        "logout_requests_signed": False,
        "logout_responses_signed": False,
        "metadata_signed": False,
        "digest_method": "SHA1",
        "signature_method": "RSA_SHA1"
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.uid:
            self.uid = self.generate_uid()
        if not self.security_settings:
            self.security_settings = self.DEFAULT_SECURITY_SETTINGS.copy()
    
    @classmethod
    def generate_uid(cls, max_attempts: int = 5) -> str:
        """Generate unique UID for auth config"""
        for attempt in range(max_attempts):
            # Generate 8 character alphanumeric string
            tmp_uid = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            
            # Check if UID already exists
            existing = cls.query.filter_by(uid=tmp_uid).first()
            if not existing:
                return tmp_uid
        
        raise ValueError("Could not generate unique auth config UID")
    
    @classmethod
    def generate_base_url(cls, request_host: str = None, request_port: str = None, 
                         is_development: bool = False, is_qa: bool = False) -> str:
        """Generate base URL for auth configuration"""
        proto = "http" if is_development else "https"
        
        if request_host:
            base_url = f"{proto}://{request_host}"
            
            if request_port and request_port not in ["80", "443"]:
                base_url += f":{request_port}"
        else:
            # Default fallback
            base_url = f"{proto}://localhost"
        
        # Add context path based on environment
        context = ""
        if is_qa:
            context = "admin-api"
        elif not is_development:
            context = "nexla-api"
        
        if context:
            base_url += f"/{context}"
        
        return base_url.rstrip('/')
    
    @classmethod
    def sso_options(cls) -> list:
        """Get public SSO options for unauthenticated routes"""
        global_configs = cls.query.filter_by(global_=True).all()
        return [config.public_attributes() for config in global_configs]
    
    @classmethod
    def get_mapping_config(cls, uid: str) -> Optional[Dict[str, Any]]:
        """Get merged mapping configuration by UID"""
        mapping = cls.query.filter_by(uid=uid).first()
        if not mapping:
            return None
        
        config = mapping.client_config or {}
        
        # Merge with secret config if available
        if mapping.secret_config:
            try:
                secret_config = json.loads(mapping.secret_config) if isinstance(mapping.secret_config, str) else mapping.secret_config
                if isinstance(secret_config, dict):
                    config.update(secret_config)
            except (json.JSONDecodeError, TypeError):
                pass
        
        return config
    
    @classmethod
    def build_from_input(cls, user, org, input_data: Dict[str, Any], 
                        request_info: Dict[str, Any] = None) -> 'ApiAuthConfig':
        """Factory method to create auth config from input data"""
        if not input_data or not user or not org:
            raise ValueError("Required input missing")
        
        if not org.has_admin_access_(user):
            raise ValueError("Auth configs can only be created by org admins")
        
        if input_data.get('global') and not user.super_user_:
            raise ValueError("Global mappings can only be created by Nexla administrators")
        
        if input_data.get('uid') and not input_data.get('global'):
            raise ValueError("UID can only be specified for global mappings")
        
        protocol = (input_data.get('protocol', cls.DEFAULT_IDP_PROTOCOL.value)).lower()
        try:
            protocol_enum = Protocols(protocol)
        except ValueError:
            raise ValueError("Unsupported IDP protocol")
        
        if not input_data.get('name'):
            raise ValueError("Mapping name must not be blank")
        
        auth_config = cls(protocol=protocol_enum)
        auth_config.update_mutable_(user, org, input_data, request_info)
        
        return auth_config
    
    def update_mutable_(self, user, org, input_data: Dict[str, Any], 
                       request_info: Dict[str, Any] = None) -> None:
        """Update mutable attributes"""
        self._update_mapping_owner(user, org)
        
        # Basic attributes
        if input_data.get('name'):
            self.name = input_data['name']
        if 'description' in input_data:
            self.description = input_data['description']
        
        # Global flag (admin only)
        if 'global' in input_data:
            if input_data['global'] and not user.super_user_:
                raise ValueError("Global mappings can only be created by Nexla administrators")
            self.global_ = input_data['global']
        
        # UID handling (global mappings only)
        if 'uid' in input_data:
            if not input_data.get('global'):
                raise ValueError("UID can only be specified for global mappings")
            
            existing = self.__class__.query.filter_by(uid=input_data['uid']).first()
            if existing and existing.id != self.id:
                raise ValueError("UID already in use")
            
            self.uid = input_data['uid']
        
        if 'auto_create_users_enabled' in input_data:
            self.auto_create_users_enabled = input_data['auto_create_users_enabled']
        
        # Base URL handling
        if 'nexla_base_url' in input_data or not self.nexla_base_url:
            if input_data.get('nexla_base_url'):
                base_url = input_data['nexla_base_url'].rstrip('/')
            else:
                # Generate from request info
                base_url = self.generate_base_url(
                    request_info.get('host') if request_info else None,
                    request_info.get('port') if request_info else None,
                    request_info.get('is_development', False) if request_info else False,
                    request_info.get('is_qa', False) if request_info else False
                )
            self.nexla_base_url = base_url
        
        # Protocol-specific configuration
        if self.is_saml_():
            self._update_saml_config(input_data)
        elif self.is_oidc_():
            self._update_oidc_config(input_data)
        
        # General configuration
        if 'security_settings' in input_data:
            self.security_settings = input_data['security_settings']
        if 'metadata' in input_data:
            self.extra_metadata = input_data['metadata']
        if 'client_config' in input_data:
            self.client_config = input_data['client_config']
        if 'secret_config' in input_data:
            self.secret_config = json.dumps(input_data['secret_config']) if isinstance(input_data['secret_config'], dict) else input_data['secret_config']
    
    def _update_saml_config(self, input_data: Dict[str, Any]) -> None:
        """Update SAML-specific configuration"""
        self.name_identifier_format = input_data.get(
            'name_identifier_format', self.DEFAULT_NAME_IDENTIFIER_FORMAT)
        
        if 'service_entity_id' in input_data or not self.service_entity_id:
            self.service_entity_id = input_data.get('service_entity_id') or self.nexla_base_url
        
        if 'assertion_consumer_url' in input_data:
            self.assertion_consumer_url = input_data['assertion_consumer_url']
        if 'idp_entity_id' in input_data:
            self.idp_entity_id = input_data['idp_entity_id']
        if 'idp_sso_target_url' in input_data:
            self.idp_sso_target_url = input_data['idp_sso_target_url']
        if 'idp_slo_target_url' in input_data:
            self.idp_slo_target_url = input_data['idp_slo_target_url']
        if 'idp_cert' in input_data:
            self.idp_cert = input_data['idp_cert']
    
    def _update_oidc_config(self, input_data: Dict[str, Any]) -> None:
        """Update OIDC-specific configuration"""
        if 'oidc_domain' in input_data:
            self.oidc_domain = input_data['oidc_domain']
        if 'oidc_keys_url_key' in input_data:
            self.oidc_keys_url_key = input_data['oidc_keys_url_key']
        if 'oidc_id_claims' in input_data and isinstance(input_data['oidc_id_claims'], dict):
            self.oidc_id_claims = input_data['oidc_id_claims']
        if 'oidc_access_claims' in input_data and isinstance(input_data['oidc_access_claims'], dict):
            self.oidc_access_claims = input_data['oidc_access_claims']
    
    def _update_mapping_owner(self, user, org) -> None:
        """Update mapping ownership with validation"""
        if not user:
            raise ValueError("User not found")
        if not org:
            raise ValueError("Input org_id required")
        
        # Skip if already correct
        if user.id == self.owner_id and org.id == self.org_id:
            return
        
        if not user.org_member_(org):
            raise ValueError("Mapping owner must be an org member")
        
        if not org.has_admin_access_(user):
            raise ValueError("Mapping owner must be an org admin")
        
        self.owner_id = user.id
        self.org_id = org.id
    
    @validates('name')
    def validate_name(self, key, name):
        if not name or not name.strip():
            raise ValueError("Name cannot be blank")
        return name.strip()
    
    @validates('uid')
    def validate_uid_uniqueness(self, key, uid):
        if uid:
            existing = self.__class__.query.filter_by(uid=uid).first()
            if existing and existing.id != self.id:
                raise ValueError("UID must be unique")
        return uid
    
    # Protocol predicate methods (Rails pattern)
    def is_saml_(self) -> bool:
        """Check if protocol is SAML"""
        return self.protocol == Protocols.SAML
    
    def is_oidc_(self) -> bool:
        """Check if protocol is OIDC"""
        return self.protocol == Protocols.OIDC
    
    def is_password_(self) -> bool:
        """Check if protocol is password"""
        return self.protocol == Protocols.PASSWORD
    
    def is_google_(self) -> bool:
        """Check if protocol is Google"""
        return self.protocol == Protocols.GOOGLE
    
    def is_omni_(self) -> bool:
        """Check if protocol is Omni"""
        return self.protocol == Protocols.OMNI
    
    def global_(self) -> bool:
        """Check if config is global"""
        return self.global_ is True
    
    def local_(self) -> bool:
        """Check if config is local (not global)"""
        return not self.global_
    
    def auto_create_enabled_(self) -> bool:
        """Check if auto user creation is enabled"""
        return self.auto_create_users_enabled is True
    
    def has_secret_config_(self) -> bool:
        """Check if secret config is present"""
        return bool(self.secret_config)
    
    def complete_saml_config_(self) -> bool:
        """Check if SAML configuration is complete"""
        if not self.is_saml_():
            return False
        
        required_fields = [
            self.service_entity_id,
            self.idp_entity_id,
            self.idp_sso_target_url,
            self.nexla_base_url
        ]
        
        return all(field for field in required_fields)
    
    def complete_oidc_config_(self) -> bool:
        """Check if OIDC configuration is complete"""
        if not self.is_oidc_():
            return False
        
        return bool(self.oidc_domain and self.client_config)
    
    # URL generation methods
    def assertion_consumer_url_computed(self) -> Optional[str]:
        """Get computed assertion consumer URL"""
        if not self.is_saml_():
            return None
        
        if self.assertion_consumer_url:
            return self.assertion_consumer_url
        
        if self.nexla_base_url and self.uid:
            return f"{self.nexla_base_url}/token/{self.uid}"
        
        return None
    
    def logout_url(self) -> Optional[str]:
        """Get logout URL"""
        if not self.nexla_base_url or not self.uid:
            return None
        return f"{self.nexla_base_url}/logout/{self.uid}"
    
    def metadata_url(self) -> Optional[str]:
        """Get metadata URL"""
        if not self.nexla_base_url or not self.uid:
            return None
        return f"{self.nexla_base_url}/metadata/{self.uid}"
    
    def oidc_token_verify_url(self) -> Optional[str]:
        """Get OIDC token verification URL"""
        if not self.is_oidc_() or not self.nexla_base_url or not self.uid:
            return None
        return f"{self.nexla_base_url}/token/{self.uid}"
    
    def public_attributes(self) -> Dict[str, Any]:
        """Get public attributes (safe for unauthenticated routes)"""
        # Base public attributes
        attrs = {
            'uid': self.uid,
            'name': self.name,
            'protocol': self.protocol.value,
            'global': self.global_
        }
        
        # Protocol-specific public attributes
        if self.is_saml_():
            attrs.update({
                'service_entity_id': self.service_entity_id,
                'assertion_consumer_url': self.assertion_consumer_url_computed(),
                'name_identifier_format': self.name_identifier_format,
                'metadata_url': self.extra_metadata_url()
            })
        
        elif self.is_oidc_():
            attrs.update({
                'oidc_domain': self.oidc_domain,
                'oidc_token_verify_url': self.oidc_token_verify_url()
            })
            
            # Include public client config (exclude secrets)
            if self.client_config:
                public_config = self.client_config.copy()
                # Remove sensitive information
                if 'web' in public_config:
                    public_config['web'].pop('client_secret', None)
                attrs['client_config'] = public_config
        
        return attrs
    
    def get_assertion_settings(self) -> Dict[str, Any]:
        """Get SAML assertion settings (for SAML protocol)"""
        if not self.is_saml_():
            return {}
        
        settings = {
            'soft': True,
            'issuer': self.service_entity_id,
            'assertion_consumer_service_url': self.assertion_consumer_url_computed(),
            'assertion_consumer_logout_service_url': self.logout_url(),
            'idp_entity_id': self.idp_entity_id,
            'idp_sso_target_url': self.idp_sso_target_url,
            'idp_slo_target_url': self.idp_slo_target_url,
            'idp_cert': self.idp_cert,
            'name_identifier_format': self.name_identifier_format
        }
        
        # Add security settings
        security_settings = self.security_settings or self.DEFAULT_SECURITY_SETTINGS
        settings['security'] = security_settings
        
        return settings
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'uid': self.uid,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'name': self.name,
            'description': self.description,
            'protocol': self.protocol.value if self.protocol else None,
            'global': self.global_,
            'auto_create_users_enabled': self.auto_create_users_enabled,
            'nexla_base_url': self.nexla_base_url,
            
            # SAML fields
            'name_identifier_format': self.name_identifier_format if self.is_saml_() else None,
            'service_entity_id': self.service_entity_id if self.is_saml_() else None,
            'assertion_consumer_url': self.assertion_consumer_url_computed() if self.is_saml_() else None,
            'idp_entity_id': self.idp_entity_id if self.is_saml_() else None,
            'idp_sso_target_url': self.idp_sso_target_url if self.is_saml_() else None,
            'idp_slo_target_url': self.idp_slo_target_url if self.is_saml_() else None,
            'logout_url': self.logout_url(),
            'metadata_url': self.extra_metadata_url(),
            
            # OIDC fields
            'oidc_domain': self.oidc_domain if self.is_oidc_() else None,
            'oidc_keys_url_key': self.oidc_keys_url_key if self.is_oidc_() else None,
            'oidc_token_verify_url': self.oidc_token_verify_url() if self.is_oidc_() else None,
            
            # Configuration
            'security_settings': self.security_settings,
            'client_config': self.client_config,
            
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Predicate methods
            'is_saml': self.is_saml_(),
            'is_oidc': self.is_oidc_(),
            'is_password': self.is_password_(),
            'is_google': self.is_google_(),
            'is_omni': self.is_omni_(),
            'is_global': self.global_(),
            'is_local': self.local_(),
            'auto_create_enabled': self.auto_create_enabled_(),
            'has_secret_config': self.has_secret_config_(),
            'complete_saml_config': self.complete_saml_config_(),
            'complete_oidc_config': self.complete_oidc_config_()
        }