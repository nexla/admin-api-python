"""
ApiAuthConfigs model - Generated from api_auth_configs table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ApiAuthConfigs(Base):
    __tablename__ = "api_auth_configs"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    uid = Column(String(24), nullable=False, index=True)
    protocol = Column(String(255), nullable=False, default="'saml'")
    name = Column(String(255), nullable=False)
    description = Column(String(255))
    global = Column(Integer, default="'0'")
    auto_create_users_enabled = Column(Integer, default="'1'")
    name_identifier_format = Column(String(255))
    nexla_base_url = Column(String(255))
    assertion_consumer_url = Column(String(255))
    service_entity_id = Column(String(255))
    idp_entity_id = Column(String(255))
    idp_sso_target_url = Column(String(255))
    idp_slo_target_url = Column(String(255))
    idp_cert = Column(Text)
    security_settings = Column(Text)
    metadata = Column(Text)
    oidc_domain = Column(String(255))
    oidc_keys_url_key = Column(String(255))
    oidc_id_claims = Column(Text)
    oidc_access_claims = Column(Text)
    client_config = Column(Text)
    secret_config_enc = Column(Text)
    secret_config_enc_iv = Column(String(255), unique=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")

    def __repr__(self):
        return f"<ApiAuthConfigs({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
