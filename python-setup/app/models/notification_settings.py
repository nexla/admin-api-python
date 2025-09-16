"""
NotificationSettings model - Generated from notification_settings table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class NotificationSettings(Base):
    __tablename__ = "notification_settings"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    channel = Column(String(50))
    status = Column(String(50))
    notification_resource_type = Column(String(50), default="'USER'", index=True)
    resource_id = Column(Integer, index=True)
    config = Column(Text)
    priority = Column(Integer, default="'1'")
    notification_channel_setting_id = Column(Integer, ForeignKey("notification_channel_settings.id"), index=True)
    notification_type_id = Column(Integer, ForeignKey("notification_types.id"), index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    notification_type = relationship("NotificationTypes")
    org = relationship("Orgs")
    notification_channel_setting = relationship("NotificationChannelSettings")
    owner = relationship("Users")

    def __repr__(self):
        return f"<NotificationSettings({self.id if hasattr(self, 'id') else 'no-id'})"

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
