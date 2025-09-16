"""
DataSets model - Generated from data_sets table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataSets(Base):
    __tablename__ = "data_sets"
    
    id = Column(Integer, nullable=False)
    flow_node_id = Column(Integer, index=True)
    origin_node_id = Column(Integer, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    data_source_id = Column(Integer, unique=True)
    parent_data_set_id = Column(Integer)
    data_credentials_id = Column(Integer, unique=True)
    sample_service_id = Column(Integer)
    name = Column(String(255), unique=True)
    description = Column(String(255))
    flow_name = Column(String(255))
    flow_description = Column(String(255))
    status = Column(String(50), default="'INIT'", index=True)
    runtime_status = Column(String(50), default="'IDLE'")
    source_path = Column(Text)
    source_schema_id = Column(String(255), unique=True)
    source_schema = Column(Text)
    code_container_id = Column(Integer, ForeignKey("code_containers.id"), index=True)
    output_schema = Column(Text)
    output_schema_annotations = Column(Text)
    output_schema_validation_enabled = Column(Integer, default="'0'")
    output_validation_schema = Column(Text)
    output_validator_id = Column(Integer)
    semantic_schema_id = Column(Integer)
    data_samples = Column(Text)
    data_sample_id = Column(Integer)
    managed = Column(Integer, default="'0'")
    referenced_resources_enabled = Column(Integer)
    public = Column(Integer, default="'0'", index=True)
    out_validation_enabled = Column(Integer, default="'0'")
    custom_config = Column(Text)
    runtime_config = Column(Text)
    copied_from_id = Column(Integer)
    main_rating = Column(Float)
    main_rating_count = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    owner = relationship("Users")
    org = relationship("Orgs")
    code_container = relationship("CodeContainers")

    def __repr__(self):
        return f"<DataSets({self.id if hasattr(self, 'id') else 'no-id'})"

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
