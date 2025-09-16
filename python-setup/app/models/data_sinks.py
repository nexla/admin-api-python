"""
DataSinks model - Generated from data_sinks table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSinks(Base):
    __tablename__ = "data_sinks"
    
    id = Column(Integer, nullable=False)
    flow_node_id = Column(Integer, index=True)
    origin_node_id = Column(Integer, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    connector_type = Column(String(255), ForeignKey("connectors.type"), default="'s3'", index=True)
    data_set_id = Column(Integer, index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    data_credentials_group_id = Column(Integer, ForeignKey("data_credentials_groups.id"), index=True)
    vendor_endpoint_id = Column(Integer)
    data_map_id = Column(Integer)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), index=True)
    code_container_id = Column(Integer)
    name = Column(String(255), nullable=False)
    description = Column(String(255))
    flow_name = Column(String(255))
    flow_description = Column(String(255))
    status = Column(String(50), default="'INIT'", index=True)
    runtime_status = Column(String(50), default="'IDLE'")
    sink_format = Column(String(50))
    sink_config = Column(Text)
    template_config = Column(Text)
    sink_schedule = Column(String(255))
    managed = Column(Integer, default="'0'")
    referenced_resources_enabled = Column(Integer)
    in_memory = Column(Integer, nullable=False, default="'0'")
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    owner = relationship("Users")
    org = relationship("Orgs")
    data_credentials = relationship("DataCredentials")
    data_source = relationship("DataSources")
    data_credentials_group = relationship("DataCredentialsGroups")
    connector_type = relationship("Connectors")

    def __repr__(self):
        return f"<DataSinks({self.id if hasattr(self, 'id') else 'no-id'})"

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
