"""
AuthTemplates model - Generated from auth_templates table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class AuthTemplates(Base):
    __tablename__ = "auth_templates"
    
    id = Column(Integer, nullable=False)
    connector_id = Column(Integer, ForeignKey("connectors.id"), nullable=False, index=True)
    vendor_id = Column(Integer, ForeignKey("vendors.id"), nullable=False, index=True)
    name = Column(String(255), unique=True)
    display_name = Column(String(255))
    description = Column(String(255))
    config = Column(JSON, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    vendor = relationship("Vendors")
    connector = relationship("Connectors")

    def __repr__(self):
        return f"<AuthTemplates({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
