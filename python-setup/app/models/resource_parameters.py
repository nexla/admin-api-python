"""
ResourceParameters model - Generated from resource_parameters table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ResourceParameters(Base):
    __tablename__ = "resource_parameters"
    
    id = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False)
    display_name = Column(String(255), nullable=False)
    description = Column(String(255))
    vendor_endpoint_id = Column(Integer)
    resource_type = Column(String(50))
    data_type = Column(String(255))
    order = Column(Integer)
    allowed_values = Column(Text)
    global = Column(Integer, default="'0'")
    config = Column(Text)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<ResourceParameters({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
