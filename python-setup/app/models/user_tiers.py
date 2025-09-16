"""
UserTiers model - Generated from user_tiers table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class UserTiers(Base):
    __tablename__ = "user_tiers"
    
    id = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False, default="'FREE'")
    display_name = Column(String(255))
    record_count_limit = Column(Integer)
    record_count_limit_time = Column(String(50), nullable=False, default="'DAILY'")
    data_source_count_limit = Column(Integer)
    trial_period_days = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<UserTiers({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
