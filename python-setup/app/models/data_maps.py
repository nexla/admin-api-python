"""
DataMaps model - Generated from data_maps table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DataMaps(Base):
    __tablename__ = "data_maps"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    name = Column(String(255), nullable=False)
    description = Column(String(255))
    public = Column(Integer, nullable=False, default="'0'")
    data_type = Column(String(255), nullable=False)
    data_format = Column(String(255))
    emit_data_default = Column(Integer, default="'1'")
    use_versioning = Column(Integer, default="'1'")
    data_default = Column(String(255))
    data_defaults = Column(Text)
    data_map = Column(Text)
    map_entry_count = Column(Integer)
    map_entry_schema = Column(Text)
    data_sink_id = Column(Integer)
    map_primary_key = Column(String(255))
    managed = Column(Integer, default="'0'")
    referenced_resources_enabled = Column(Integer)
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    org = relationship("Orgs")
    owner = relationship("Users")

    def __repr__(self):
        return f"<DataMaps({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
