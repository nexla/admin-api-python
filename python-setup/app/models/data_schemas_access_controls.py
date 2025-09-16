"""
DataSchemasAccessControls model - Generated from data_schemas_access_controls table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSchemasAccessControls(Base):
    __tablename__ = "data_schemas_access_controls"
    
    id = Column(Integer, nullable=False)
    data_schema_id = Column(Integer, ForeignKey("data_schemas.id"), nullable=False, index=True)
    accessor_id = Column(Integer, nullable=False, index=True)
    accessor_type = Column(String(50), nullable=False, default="'USER'", index=True)
    accessor_org_id = Column(Integer)
    role_index = Column(Integer, index=True)
    access_roles = Column(Integer)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_schema = relationship("DataSchemas")

    def __repr__(self):
        return f"<DataSchemasAccessControls({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
