"""
DataCredentialsMemberships model - Generated from data_credentials_memberships table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataCredentialsMemberships(Base):
    __tablename__ = "data_credentials_memberships"
    
    id = Column(Integer, nullable=False)
    data_credentials_group_id = Column(Integer, ForeignKey("data_credentials_groups.id"), nullable=False, unique=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), nullable=False, unique=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_credentials = relationship("DataCredentials")
    data_credentials_group = relationship("DataCredentialsGroups")

    def __repr__(self):
        return f"<DataCredentialsMemberships({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
