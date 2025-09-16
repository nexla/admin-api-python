"""
Orgs model - Generated from orgs table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Orgs(Base):
    __tablename__ = "orgs"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    cluster_id = Column(Integer, ForeignKey("clusters.id"), index=True)
    nexla_admin_org = Column(String(255), unique=True)
    new_cluster_id = Column(Integer)
    cluster_status = Column(String(50), default="'ACTIVE'")
    billing_owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_tier_id = Column(Integer)
    name = Column(String(255), nullable=False)
    description = Column(String(255))
    email_domain = Column(String(255))
    client_identifier = Column(String(255))
    members_default_access_role = Column(Integer, default="'0'")
    allow_api_key_access = Column(Integer, nullable=False, default="'0'")
    require_org_admin_to_publish = Column(Integer, default="'1'")
    require_org_admin_to_subscribe = Column(Integer, default="'1'")
    email_domain_verified_at = Column(DateTime)
    name_verified_at = Column(DateTime)
    email = Column(String(255))
    enable_nexla_password_login = Column(Integer, default="'1'")
    referenced_resources_enabled = Column(Integer)
    status = Column(String(50), default="'ACTIVE'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    webhook_host = Column(String(255))
    search_index_name = Column(String(255), unique=True)
    rate_limit_id = Column(Integer)
    throttle_until = Column(DateTime)
    features_enabled = Column(JSON)
    self_signup = Column(Integer, default="'0'")
    trial_expires_at = Column(DateTime)
    self_signup_members_limit = Column(Integer)

    # Relationships
    owner = relationship("Users")
    billing_owner = relationship("Users")
    cluster = relationship("Clusters")

    def __repr__(self):
        return f"<Orgs({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)

    def is_active(self) -> bool:
        """Check if status is active."""
        return getattr(self, "status", None) == "ACTIVE"

    def is_deactivated(self) -> bool:
        """Check if status is deactivated."""
        return getattr(self, "status", None) == "DEACTIVATED"
