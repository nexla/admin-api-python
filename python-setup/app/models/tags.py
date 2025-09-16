"""
Tags model - Generated from tags table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class Tags(Base):
    __tablename__ = "tags"
    
    id = Column(Integer, nullable=False)
    name = Column(String(255), unique=True)
    taggings_count = Column(Integer, default="'0'")

    def __repr__(self):
        return f"<Tags({self.id if hasattr(self, 'id') else 'no-id'})"
