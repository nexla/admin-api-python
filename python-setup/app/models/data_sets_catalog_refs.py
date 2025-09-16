"""
DataSetsCatalogRefs model - Generated from data_sets_catalog_refs table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSetsCatalogRefs(Base):
    __tablename__ = "data_sets_catalog_refs"
    
    id = Column(Integer, nullable=False)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), nullable=False, index=True)
    catalog_config_id = Column(Integer, ForeignKey("catalog_configs.id"), nullable=False, index=True)
    status = Column(String(50), default="'PENDING'")
    reference_id = Column(String(255))
    link = Column(String(255))
    error_msg = Column(Text)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    catalog_config = relationship("CatalogConfigs")
    data_set = relationship("DataSets")

    def __repr__(self):
        return f"<DataSetsCatalogRefs({self.id if hasattr(self, 'id') else 'no-id'})"

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
