"""
DataSchemas model - Generated from data_schemas table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSchemas(Base):
    __tablename__ = "data_schemas"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    name = Column(String(255))
    description = Column(String(255))
    detected = Column(Integer, nullable=False, default="'0'")
    managed = Column(Integer, nullable=False, default="'0'")
    template = Column(Integer, nullable=False, default="'0'")
    public = Column(Integer, nullable=False, default="'0'")
    schema = Column(Text)
    annotations = Column(Text)
    validations = Column(Text)
    data_samples = Column(Text)
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")
    data_credentials = relationship("DataCredentials")
    owner = relationship("Users")

    def __repr__(self):
        return f"<DataSchemas({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
