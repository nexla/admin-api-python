"""
DashboardTransformsAccessControlVersions model - Generated from dashboard_transforms_access_control_versions table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DashboardTransformsAccessControlVersions(Base):
    __tablename__ = "dashboard_transforms_access_control_versions"
    
    id = Column(Integer, nullable=False)
    item_type = Column(String(191), nullable=False, index=True)
    item_id = Column(Integer, nullable=False, index=True)
    event = Column(String(255), nullable=False)
    whodunnit = Column(String(255))
    user_id = Column(Integer, index=True)
    user_email = Column(String(255))
    org_id = Column(Integer, index=True)
    impersonator_id = Column(Integer)
    owner_id = Column(Integer)
    owner_email = Column(String(255))
    request_ip = Column(String(255))
    request_user_agent = Column(String(255))
    request_url = Column(String(255))
    association_resource = Column(Text)
    resource_type = Column(String(255))
    resource_id = Column(Integer)
    object_changes = Column(Text)
    object = Column(Text)
    created_at = Column(DateTime, server_default=func.now())

    def __repr__(self):
        return f"<DashboardTransformsAccessControlVersions({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
