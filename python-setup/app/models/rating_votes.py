"""
RatingVotes model - Generated from rating_votes table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class RatingVotes(Base):
    __tablename__ = "rating_votes"
    
    id = Column(Integer, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    vote = Column(Integer)
    item_id = Column(Integer)
    item_type = Column(String(255))
    rating_type = Column(String(255))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    user = relationship("Users")

    def __repr__(self):
        return f"<RatingVotes({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
