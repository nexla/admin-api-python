"""
SchemaMigrations model - Generated from schema_migrations table
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime
from ..database import Base


class SchemaMigrations(Base):
    __tablename__ = "schema_migrations"
    
    version = Column(String(255), nullable=False, unique=True)

    def __repr__(self):
        return f"<SchemaMigrations({self.id if hasattr(self, 'id') else 'no-id'})"
