"""
FlowTriggers model - Generated from flow_triggers table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class FlowTriggers(Base):
    __tablename__ = "flow_triggers"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    status = Column(String(50), nullable=False, default="'ACTIVE'")
    triggering_event_type = Column(String(255), ForeignKey("orchestration_event_types.type"), nullable=False, index=True)
    triggering_flow_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    triggering_origin_node_id = Column(Integer)
    triggered_event_type = Column(String(255), ForeignKey("orchestration_event_types.type"), nullable=False, index=True)
    triggered_origin_node_id = Column(Integer, ForeignKey("flow_nodes.id"), index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    triggering_event_type = relationship("OrchestrationEventTypes")
    triggered_origin_node = relationship("FlowNodes")
    org = relationship("Orgs")
    owner = relationship("Users")
    triggered_event_type = relationship("OrchestrationEventTypes")
    triggering_flow_node = relationship("FlowNodes")

    def __repr__(self):
        return f"<FlowTriggers({self.id if hasattr(self, 'id') else 'no-id'})"

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
