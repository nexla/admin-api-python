"""
GenAiOrgSettings model - Generated from gen_ai_org_settings table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class GenAiOrgSettings(Base):
    __tablename__ = "gen_ai_org_settings"
    
    id = Column(Integer, nullable=False)
    gen_ai_config_id = Column(Integer, ForeignKey("gen_ai_configs.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    gen_ai_usage = Column(String(50), nullable=False)
    global = Column(Integer, nullable=False, default="'0'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    gen_ai_config = relationship("GenAiConfigs")
    org = relationship("Orgs")

    def __repr__(self):
        return f"<GenAiOrgSettings({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
