"""
DocContainers model - Generated from doc_containers table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class DocContainers(Base):
    __tablename__ = "doc_containers"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    name = Column(String(255))
    description = Column(String(255))
    doc_type = Column(String(50), nullable=False, default="'txt'")
    public = Column(Integer, default="'0'")
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    repo_type = Column(String(50), nullable=False, default="'embedded'")
    repo_config = Column(Text)
    text = Column(Text)
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    owner = relationship("Users")
    data_credentials = relationship("DataCredentials")
    org = relationship("Orgs")

    def __repr__(self):
        return f"<DocContainers({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
