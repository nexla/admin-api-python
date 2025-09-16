"""
ApprovalRequests model - Generated from approval_requests table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ApprovalRequests(Base):
    __tablename__ = "approval_requests"
    
    id = Column(Integer, nullable=False)
    org_id = Column(Integer, nullable=False)
    requestor_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    topic_id = Column(Integer, nullable=False)
    topic_type = Column(String(255), nullable=False)
    request_type = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False, default="'pending'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    requestor = relationship("Users")

    def __repr__(self):
        return f"<ApprovalRequests({self.id if hasattr(self, 'id') else 'no-id'})"

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
