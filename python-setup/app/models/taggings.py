"""
Taggings model - Generated from taggings table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Taggings(Base):
    __tablename__ = "taggings"
    
    id = Column(Integer, nullable=False)
    tag_id = Column(Integer, unique=True)
    taggable_id = Column(Integer, unique=True)
    taggable_type = Column(String(255), unique=True)
    tagger_id = Column(Integer, unique=True)
    tagger_type = Column(String(255), unique=True)
    context = Column(String(128), unique=True)
    created_at = Column(DateTime, server_default=func.now())

    def __repr__(self):
        return f"<Taggings({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
