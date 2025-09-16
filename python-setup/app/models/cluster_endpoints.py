"""
ClusterEndpoints model - Generated from cluster_endpoints table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ClusterEndpoints(Base):
    __tablename__ = "cluster_endpoints"
    
    id = Column(Integer, nullable=False)
    org_id = Column(Integer, nullable=False)
    cluster_id = Column(Integer, ForeignKey("clusters.id"), nullable=False, unique=True)
    service = Column(String(50), unique=True)
    protocol = Column(String(255), default="'http'")
    host = Column(String(255))
    port = Column(String(255))
    context = Column(String(255))
    header_host = Column(String(255))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    cluster = relationship("Clusters")

    def __repr__(self):
        return f"<ClusterEndpoints({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
