"""
ApiKeyEvents model - Generated from api_key_events table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ApiKeyEvents(Base):
    __tablename__ = "api_key_events"
    
    id = Column(Integer, nullable=False)
    api_key_type = Column(String(255))
    api_key_id = Column(Integer)
    api_key_api_key = Column(String(255))
    resource_id = Column(Integer)
    scope = Column(String(255))
    owner_id = Column(Integer, nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    time_of_authentication = Column(DateTime, nullable=False)
    request_user_agent = Column(String(255))
    request_url = Column(String(255))
    request_ip = Column(String(255))
    usage_count = Column(Integer, default="'0'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")

    def __repr__(self):
        return f"<ApiKeyEvents({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
