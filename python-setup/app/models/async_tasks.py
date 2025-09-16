"""
AsyncTasks model - Generated from async_tasks table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class AsyncTasks(Base):
    __tablename__ = "async_tasks"
    
    id = Column(Integer, nullable=False)
    org_id = Column(Integer, index=True)
    owner_id = Column(Integer, index=True)
    task_type = Column(String(255), index=True)
    priority = Column(Integer)
    arguments = Column(Text)
    status = Column(String(50), default="'pending'", index=True)
    error = Column(Text)
    progress = Column(Integer)
    result = Column(Text)
    result_url = Column(Text)
    started_at = Column(DateTime)
    stopped_at = Column(DateTime, index=True)
    retries_count = Column(Integer)
    acknowledged_at = Column(DateTime)
    base_cluster_uid = Column(String(255))
    result_purged = Column(Integer, index=True)
    should_be_killed = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    request_data = Column(Text)
    result_url_created_at = Column(DateTime)

    def __repr__(self):
        return f"<AsyncTasks({self.id if hasattr(self, 'id') else 'no-id'})"

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
