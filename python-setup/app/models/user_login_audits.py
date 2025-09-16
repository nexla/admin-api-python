"""
UserLoginAudits model - Generated from user_login_audits table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class UserLoginAudits(Base):
    __tablename__ = "user_login_audits"
    
    id = Column(Integer, nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    time_of_issue = Column(DateTime)
    time_of_invalidation = Column(DateTime)
    time_of_expiration = Column(DateTime)
    token_key = Column(String(50), nullable=False)
    request_user_agent = Column(String(255))
    request_url = Column(String(255))
    request_ip = Column(String(255))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    audit_type = Column(String(50), nullable=False)

    # Relationships
    user = relationship("Users")
    org = relationship("Orgs")

    def __repr__(self):
        return f"<UserLoginAudits({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
