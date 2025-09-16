"""
ProjectsDataFlows model - Generated from projects_data_flows table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class ProjectsDataFlows(Base):
    __tablename__ = "projects_data_flows"
    
    id = Column(Integer, nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"), index=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), index=True)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), index=True)
    data_sink_id = Column(Integer, ForeignKey("data_sinks.id"), index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_source = relationship("DataSources")
    project = relationship("Projects")
    data_sink = relationship("DataSinks")
    data_set = relationship("DataSets")

    def __repr__(self):
        return f"<ProjectsDataFlows({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
