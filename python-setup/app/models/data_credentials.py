"""
DataCredentials model - Generated from data_credentials table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataCredentials(Base):
    __tablename__ = "data_credentials"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    connector_type = Column(String(255), ForeignKey("connectors.type"), default="'s3'", index=True)
    users_api_key_id = Column(Integer, ForeignKey("users_api_keys.id"), index=True)
    name = Column(String(255))
    description = Column(String(255))
    credentials_version = Column(String(255), default="'1'")
    credentials_enc = Column(Text)
    credentials_enc_iv = Column(String(255))
    vendor_id = Column(Integer)
    auth_template_id = Column(Integer, ForeignKey("auth_templates.id"), index=True)
    verified_status = Column(String(1024))
    verified_at = Column(DateTime)
    managed = Column(Integer, default="'0'")
    referenced_resources_enabled = Column(Integer)
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    users_api_key = relationship("UsersApiKeys")
    connector_type = relationship("Connectors")
    org = relationship("Orgs")
    owner = relationship("Users")
    auth_template = relationship("AuthTemplates")

    def __repr__(self):
        return f"<DataCredentials({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
