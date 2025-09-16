"""
DataSources model - Generated from data_sources table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSources(Base):
    __tablename__ = "data_sources"
    
    id = Column(Integer, nullable=False)
    flow_node_id = Column(Integer, index=True)
    origin_node_id = Column(Integer, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    connector_type = Column(String(255), ForeignKey("connectors.type"), default="'s3'", index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    data_credentials_group_id = Column(Integer, ForeignKey("data_credentials_groups.id"), index=True)
    name = Column(String(255), nullable=False)
    description = Column(String(255))
    flow_name = Column(String(255))
    flow_description = Column(String(255))
    status = Column(String(50), index=True)
    runtime_status = Column(String(50), default="'IDLE'")
    ingest_method = Column(String(50))
    source_format = Column(String(50))
    source_config = Column(Text)
    template_config = Column(Text)
    poll_schedule = Column(String(255))
    data_sink_id = Column(Integer)
    vendor_endpoint_id = Column(Integer)
    code_container_id = Column(Integer)
    managed = Column(Integer, default="'0'")
    adaptive_flow = Column(Integer, default="'0'")
    referenced_resources_enabled = Column(Integer)
    copied_from_id = Column(Integer)
    run_now_at = Column(DateTime)
    run_now_status = Column(String(255))
    last_run_id = Column(Integer)
    reingest_at = Column(DateTime)
    reingest_status = Column(String(255))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    data_credentials_group = relationship("DataCredentialsGroups")
    owner = relationship("Users")
    data_credentials = relationship("DataCredentials")
    org = relationship("Orgs")
    connector_type = relationship("Connectors")

    def __repr__(self):
        return f"<DataSources({self.id if hasattr(self, 'id') else 'no-id'})"

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
