"""
CustomDataFlowsDataCredentials model - Generated from custom_data_flows_data_credentials table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class CustomDataFlowsDataCredentials(Base):
    __tablename__ = "custom_data_flows_data_credentials"
    
    id = Column(Integer, nullable=False)
    custom_data_flow_id = Column(Integer, ForeignKey("custom_data_flows.id"), nullable=False, index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    custom_data_flow = relationship("CustomDataFlows")
    data_credentials = relationship("DataCredentials")

    def __repr__(self):
        return f"<CustomDataFlowsDataCredentials({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
