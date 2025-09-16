"""
DomainCustodians model - Generated from domain_custodians table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DomainCustodians(Base):
    __tablename__ = "domain_custodians"
    
    id = Column(Integer, nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    domain_id = Column(Integer, nullable=False, index=True)
    status = Column(String(50), default="'active'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")
    user = relationship("Users")

    def __repr__(self):
        return f"<DomainCustodians({self.id if hasattr(self, 'id') else 'no-id'})"

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
