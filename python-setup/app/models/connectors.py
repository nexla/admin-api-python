"""
Connectors model - Generated from connectors table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Connectors(Base):
    __tablename__ = "connectors"
    
    id = Column(Integer, nullable=False)
    type = Column(String(255), unique=True)
    connection_type = Column(String(255))
    name = Column(String(255))
    description = Column(String(255))
    nexset_api_compatible = Column(Integer, default="'0'")
    ingestion_mode = Column(String(50), nullable=False, default="'full_ingestion'")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Connectors({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
