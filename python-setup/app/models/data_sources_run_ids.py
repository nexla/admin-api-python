"""
DataSourcesRunIds model - Generated from data_sources_run_ids table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSourcesRunIds(Base):
    __tablename__ = "data_sources_run_ids"
    
    id = Column(Integer, nullable=False)
    run_id = Column(Integer)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), index=True)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_source = relationship("DataSources")

    def __repr__(self):
        return f"<DataSourcesRunIds({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
