"""
NotificationTypes model - Generated from notification_types table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class NotificationTypes(Base):
    __tablename__ = "notification_types"
    
    id = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String(255))
    code = Column(Integer)
    category = Column(String(50))
    default = Column(Integer, default="'0'")
    status = Column(Integer, default="'0'")
    visible = Column(Integer, default="'1'")
    event_type = Column(String(50))
    resource_type = Column(String(50))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<NotificationTypes({self.id if hasattr(self, 'id') else 'no-id'})"

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
