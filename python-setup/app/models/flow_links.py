"""
FlowLinks model - Generated from flow_links table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class FlowLinks(Base):
    __tablename__ = "flow_links"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    status = Column(String(50), nullable=False, default="'ACTIVE'")
    link_type = Column(String(255), ForeignKey("flow_link_types.type"), nullable=False, index=True)
    left_origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), nullable=False, index=True)
    right_origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    retriever_data_set_id = Column(Integer, ForeignKey("data_sets.id"), index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    retriever_data_set = relationship("DataSets")
    org = relationship("Orgs")
    owner = relationship("Users")
    left_origin_node = relationship("FlowNodes")
    link_type = relationship("FlowLinkTypes")
    right_origin_node = relationship("FlowNodes")

    def __repr__(self):
        return f"<FlowLinks({self.id if hasattr(self, 'id') else 'no-id'})"

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
