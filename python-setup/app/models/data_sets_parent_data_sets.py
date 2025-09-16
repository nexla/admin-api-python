"""
DataSetsParentDataSets model - Generated from data_sets_parent_data_sets table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSetsParentDataSets(Base):
    __tablename__ = "data_sets_parent_data_sets"
    
    id = Column(Integer, nullable=False)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), nullable=False, index=True)
    parent_data_set_id = Column(Integer, ForeignKey("data_sets.id"), index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    parent_data_set = relationship("DataSets")
    data_set = relationship("DataSets")

    def __repr__(self):
        return f"<DataSetsParentDataSets({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
