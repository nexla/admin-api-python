"""
Vendors model - Generated from vendors table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Vendors(Base):
    __tablename__ = "vendors"
    
    id = Column(Integer, nullable=False)
    connector_id = Column(Integer, ForeignKey("connectors.id"), index=True)
    name = Column(String(255), nullable=False, unique=True)
    display_name = Column(String(255), nullable=False)
    description = Column(String(255))
    connection_type = Column(String(50), default="'rest'")
    auth_template = Column(Text)
    logo = Column(String(255))
    small_logo = Column(String(255))
    config = Column(Text)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    connector = relationship("Connectors")

    def __repr__(self):
        return f"<Vendors({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
