"""
ApiCronjobs model - Generated from api_cronjobs table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ApiCronjobs(Base):
    __tablename__ = "api_cronjobs"
    
    id = Column(Integer, nullable=False)
    descriptor = Column(String(255), nullable=False, unique=True)
    description = Column(String(255))
    window_seconds = Column(Integer, nullable=False, default="'60'")
    last_performed = Column(DateTime)
    enabled = Column(Integer, nullable=False, default="'1'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<ApiCronjobs({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
