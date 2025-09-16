"""
EndpointSpecs model - Generated from endpoint_specs table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class EndpointSpecs(Base):
    __tablename__ = "endpoint_specs"
    
    id = Column(Integer, nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), nullable=False, index=True)
    method = Column(String(50), nullable=False, default="'GET'")
    route = Column(String(255))
    headers = Column(JSON)
    path_params = Column(JSON)
    query_params = Column(JSON)
    body = Column(JSON)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_set = relationship("DataSets")
    org = relationship("Orgs")
    owner = relationship("Users")

    def __repr__(self):
        return f"<EndpointSpecs({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
