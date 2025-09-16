"""
Transform Model - Data transformation definitions
"""

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

from app.database import Base


class Transform(Base):
    """Data transformation definition"""
    
    __tablename__ = "transforms"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Basic information
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    transform_type = Column(String(50), nullable=False, index=True)  # field, record, batch, stream
    
    # Schema definitions
    source_schema = Column(JSON, nullable=False)
    target_schema = Column(JSON, nullable=False)
    
    # Transform configuration
    transform_config = Column(JSON, nullable=False)
    
    # Status
    is_active = Column(Boolean, default=True, index=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    created_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    
    # Foreign key relationships
    created_by = relationship("User", back_populates="created_transforms")
    org = relationship("Org", back_populates="transforms")
    attribute_transforms = relationship("AttributeTransform", back_populates="transform", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Transform(id={self.id}, name='{self.name}', type='{self.transform_type}')>"
    
    @property
    def transform_function_count(self):
        """Get the number of attribute transforms for this transform"""
        return len(self.attribute_transforms)
    
    def to_dict(self):
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "transform_type": self.transform_type,
            "source_schema": self.source_schema,
            "target_schema": self.target_schema,
            "transform_config": self.transform_config,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "created_by_id": self.created_by_id,
            "org_id": self.org_id,
            "attribute_count": self.transform_function_count
        }