"""
Attribute Transform Model - Field-level transformation definitions
"""

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

from app.database import Base


class AttributeTransform(Base):
    """Field-level transformation definition"""
    
    __tablename__ = "attribute_transforms"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Parent transform
    transform_id = Column(Integer, ForeignKey("transforms.id"), nullable=False, index=True)
    
    # Field information
    field_name = Column(String(255), nullable=False, index=True)
    source_type = Column(String(100), nullable=False)
    target_type = Column(String(100), nullable=False)
    
    # Transform function
    transform_function = Column(String(100), nullable=False)
    transform_params = Column(JSON)  # Parameters for the transform function
    
    # Validation rules
    validation_rules = Column(JSON)  # List of validation rules to apply
    
    # Configuration
    is_required = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    transform = relationship("Transform", back_populates="attribute_transforms")
    
    def __repr__(self):
        return f"<AttributeTransform(id={self.id}, field='{self.field_name}', function='{self.transform_function}')>"
    
    def to_dict(self):
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "transform_id": self.transform_id,
            "field_name": self.field_name,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "transform_function": self.transform_function,
            "transform_params": self.transform_params,
            "validation_rules": self.validation_rules,
            "is_required": self.is_required,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }