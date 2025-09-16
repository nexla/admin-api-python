"""
ServiceKeyEvents model - Generated from service_key_events table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ServiceKeyEvents(Base):
    __tablename__ = "service_key_events"
    
    id = Column(Integer, nullable=False)
    request_url = Column(String(2048))
    request_user_agent = Column(String(2048))
    request_ip = Column(String(255))
    owner_id = Column(Integer)
    org_id = Column(Integer)
    scope = Column(String(255))
    service_key_id = Column(Integer)
    service_key_api_key = Column(String(255))
    time_of_authentication = Column(DateTime)
    usage_count = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<ServiceKeyEvents({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
