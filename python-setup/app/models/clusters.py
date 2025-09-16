"""
Clusters model - Generated from clusters table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Clusters(Base):
    __tablename__ = "clusters"
    
    id = Column(Integer, nullable=False)
    uid = Column(String(255))
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    is_default = Column(Integer, nullable=False, default="'0'")
    is_private = Column(Integer, nullable=False, default="'1'")
    name = Column(String(255), nullable=False, unique=True)
    description = Column(String(255))
    status = Column(String(50), default="'INIT'")
    region = Column(String(255), nullable=False)
    provider = Column(String(50), nullable=False, default="'aws'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")

    def __repr__(self):
        return f"<Clusters({self.id if hasattr(self, 'id') else 'no-id'})"

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
