"""
DataSourcesDocContainers model - Generated from data_sources_doc_containers table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSourcesDocContainers(Base):
    __tablename__ = "data_sources_doc_containers"
    
    id = Column(Integer, nullable=False)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False, index=True)
    doc_container_id = Column(Integer, ForeignKey("doc_containers.id"), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_source = relationship("DataSources")
    doc_container = relationship("DocContainers")

    def __repr__(self):
        return f"<DataSourcesDocContainers({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
