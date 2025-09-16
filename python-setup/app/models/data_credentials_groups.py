"""
DataCredentialsGroups model - Generated from data_credentials_groups table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataCredentialsGroups(Base):
    __tablename__ = "data_credentials_groups"
    
    id = Column(Integer, nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(String(255))
    credentials_type = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")
    owner = relationship("Users")

    def __repr__(self):
        return f"<DataCredentialsGroups({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
