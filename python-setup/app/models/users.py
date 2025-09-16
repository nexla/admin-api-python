"""
Users model - Generated from users table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Users(Base):
    __tablename__ = "users"
    
    id = Column(Integer, nullable=False)
    default_org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    email = Column(String(255), nullable=False, index=True)
    api_key = Column(String(255))
    full_name = Column(String(255), nullable=False)
    password_digest = Column(String(255))
    password_digest_1 = Column(String(255))
    password_digest_2 = Column(String(255))
    password_digest_3 = Column(String(255))
    password_digest_4 = Column(String(255))
    email_verified_at = Column(DateTime)
    user_tier_id = Column(Integer)
    tos_signed_at = Column(DateTime)
    status = Column(String(50), default="'ACTIVE'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    password_retry_count = Column(Integer, default="'0'")
    password_change_required_at = Column(DateTime)
    password_reset_token = Column(Text)
    password_reset_token_count = Column(Integer, default="'0'")
    password_reset_token_at = Column(DateTime)
    account_locked_at = Column(DateTime)
    rate_limit_id = Column(Integer)
    throttle_until = Column(DateTime)

    # Relationships
    default_org = relationship("Orgs")

    def __repr__(self):
        return f"<Users({self.id if hasattr(self, 'id') else 'no-id'})"

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
