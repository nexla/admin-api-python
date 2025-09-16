"""
CodeContainers model - Generated from code_containers table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class CodeContainers(Base):
    __tablename__ = "code_containers"
    
    id = Column(Integer, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    runtime_data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"), index=True)
    name = Column(String(255))
    description = Column(String(255))
    reusable = Column(Integer, nullable=False, default="'0'")
    public = Column(Integer, nullable=False, default="'0'")
    resource_type = Column(String(50), nullable=False, default="'transform'")
    output_type = Column(String(50), nullable=False, default="'record'")
    code_type = Column(String(50), nullable=False, default="'jolt_standard'")
    code_encoding = Column(String(50), nullable=False, default="'none'")
    code_config = Column(Text)
    code = Column(Text)
    managed = Column(Integer, default="'0'")
    referenced_resources_enabled = Column(Integer)
    copied_from_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    custom_config = Column(Text)
    repo_type = Column(String(50), nullable=False, default="'embedded'")
    repo_config = Column(Text)
    ai_function_type = Column(String(32))

    # Relationships
    owner = relationship("Users")
    org = relationship("Orgs")
    runtime_data_credentials = relationship("DataCredentials")
    data_credentials = relationship("DataCredentials")

    def __repr__(self):
        return f"<CodeContainers({self.id if hasattr(self, 'id') else 'no-id'})"

    @property
    def created_recently(self) -> bool:
        """Check if created within last 24 hours."""
        if not hasattr(self, "created_at") or not self.created_at:
            return False
        from datetime import datetime, timedelta
        return datetime.utcnow() - self.created_at < timedelta(days=1)
