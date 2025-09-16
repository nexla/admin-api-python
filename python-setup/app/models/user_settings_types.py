"""
UserSettingsTypes model - Generated from user_settings_types table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class UserSettingsTypes(Base):
    __tablename__ = "user_settings_types"
    
    id = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False, index=True)
    description = Column(String(255))
    primary_key = Column(String(255))

    def __repr__(self):
        return f"<UserSettingsTypes({self.id if hasattr(self, 'id') else 'no-id'})"
