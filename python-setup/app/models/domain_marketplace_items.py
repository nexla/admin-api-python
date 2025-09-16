"""
DomainMarketplaceItems model - Generated from domain_marketplace_items table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DomainMarketplaceItems(Base):
    __tablename__ = "domain_marketplace_items"
    
    id = Column(Integer, nullable=False)
    domain_id = Column(Integer, ForeignKey("domains.id"), index=True)
    marketplace_item_id = Column(Integer, ForeignKey("marketplace_items.id"), index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    domain = relationship("Domains")
    marketplace_item = relationship("MarketplaceItems")

    def __repr__(self):
        return f"<DomainMarketplaceItems({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
