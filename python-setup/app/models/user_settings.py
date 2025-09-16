"""
UserSettings model - Generated from user_settings table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class UserSettings(Base):
    __tablename__ = "user_settings"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, nullable=False, index=True)
    org_id = Column(Integer)
    user_settings_type_id = Column(Integer, nullable=False, index=True)
    primary_key_value = Column(String(255), index=True)
    description = Column(String(255))
    settings = Column(Text)
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<UserSettings({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
