"""
ExternalSharers model - Generated from external_sharers table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ExternalSharers(Base):
    __tablename__ = "external_sharers"
    
    id = Column(Integer, nullable=False)
    data_set_id = Column(Integer, nullable=False, index=True)
    email = Column(String(255), nullable=False, index=True)
    org_id = Column(String(255))
    team_id = Column(String(255))
    name = Column(String(255))
    description = Column(String(255))
    notified_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<ExternalSharers({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
