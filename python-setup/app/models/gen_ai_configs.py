"""
GenAiConfigs model - Generated from gen_ai_configs table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class GenAiConfigs(Base):
    __tablename__ = "gen_ai_configs"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    name = Column(String(255), nullable=False)
    description = Column(String(255))
    status = Column(String(50), nullable=False, default="'ACTIVE'")
    type = Column(String(255), nullable=False)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), nullable=False, index=True)
    config = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")
    data_credentials = relationship("DataCredentials")
    owner = relationship("Users")

    def __repr__(self):
        return f"<GenAiConfigs({self.id if hasattr(self, 'id') else 'no-id'})"

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
