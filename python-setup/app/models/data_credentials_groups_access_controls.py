"""
DataCredentialsGroupsAccessControls model - Generated from data_credentials_groups_access_controls table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataCredentialsGroupsAccessControls(Base):
    __tablename__ = "data_credentials_groups_access_controls"
    
    id = Column(Integer, nullable=False)
    data_credentials_group_id = Column(Integer, ForeignKey("data_credentials_groups.id"), nullable=False, index=True)
    accessor_id = Column(Integer, nullable=False)
    accessor_type = Column(String(50), nullable=False, default="'USER'")
    accessor_org_id = Column(Integer)
    role_index = Column(Integer)
    access_roles = Column(Integer)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_credentials_group = relationship("DataCredentialsGroups")

    def __repr__(self):
        return f"<DataCredentialsGroupsAccessControls({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
