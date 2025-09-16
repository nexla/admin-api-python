"""
Notifications model - Generated from notifications table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Notifications(Base):
    __tablename__ = "notifications"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    resource_id = Column(Integer, index=True)
    resource_type = Column(String(50), index=True)
    level = Column(String(50), nullable=False, index=True)
    message_id = Column(Integer, nullable=False, default="'0'")
    message = Column(Text)
    read_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), index=True)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now(), index=True)
    timestamp = Column(DateTime, index=True)

    # Relationships
    org = relationship("Orgs")
    owner = relationship("Users")

    def __repr__(self):
        return f"<Notifications({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
