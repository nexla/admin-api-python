"""
EndpointMappings model - Generated from endpoint_mappings table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class EndpointMappings(Base):
    __tablename__ = "endpoint_mappings"
    
    id = Column(Integer, nullable=False)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False, index=True)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(String(255))
    method = Column(String(50), nullable=False, default="'GET'")
    route = Column(String(255))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_set = relationship("DataSets")
    data_source = relationship("DataSources")

    def __repr__(self):
        return f"<EndpointMappings({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
