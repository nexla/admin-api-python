"""
FlowNodes model - Generated from flow_nodes table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class FlowNodes(Base):
    __tablename__ = "flow_nodes"
    
    id = Column(Integer, nullable=False)
    origin_node_id = Column(Integer, index=True)
    parent_node_id = Column(Integer, index=True)
    shared_origin_node_id = Column(Integer, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    cluster_id = Column(Integer, index=True)
    name = Column(String(255))
    description = Column(String(255))
    status = Column(String(50), default="'INIT'")
    ingestion_mode = Column(String(50), default="'sampling'")
    flow_type = Column(String(50), default="'streaming'")
    project_id = Column(Integer, index=True)
    data_source_id = Column(Integer, index=True)
    data_set_id = Column(Integer, index=True)
    data_sink_id = Column(Integer, index=True)
    nexset_api_compatible = Column(Integer, nullable=False, default="'0'")
    managed = Column(Integer, nullable=False, default="'0'")
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")
    owner = relationship("Users")

    def __repr__(self):
        return f"<FlowNodes({self.id if hasattr(self, 'id') else 'no-id'})"

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
