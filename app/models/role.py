from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

# Association table for role-permission many-to-many relationship
role_permissions = Table(
    'role_permissions',
    Base.metadata,
    Column('role_id', Integer, ForeignKey('roles.id')),
    Column('permission_id', Integer, ForeignKey('permissions.id'))
)

# Association table for user-role many-to-many relationship  
user_roles = Table(
    'user_roles',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('role_id', Integer, ForeignKey('roles.id')),
    Column('org_id', Integer, ForeignKey('orgs.id'), nullable=True),
    Column('granted_at', DateTime, default=func.now()),
    Column('granted_by', Integer, ForeignKey('users.id'), nullable=True)
)

class Role(Base):
    __tablename__ = "roles"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    display_name = Column(String(255), nullable=False)
    description = Column(Text)
    is_system_role = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships - commented out temporarily to avoid FK conflicts
    # permissions = relationship("Permission", secondary=role_permissions, back_populates="roles")
    # users = relationship("User", secondary=user_roles, back_populates="roles")

class Permission(Base):
    __tablename__ = "rbac_permissions"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    resource = Column(String(100), nullable=False)  # e.g., 'users', 'data_sources', 'organizations'
    action = Column(String(50), nullable=False)     # e.g., 'read', 'write', 'delete', 'admin'
    description = Column(Text)
    is_system_permission = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships - commented out temporarily to avoid FK conflicts  
    # roles = relationship("Role", secondary=role_permissions, back_populates="permissions")

class UserRole(Base):
    """
    Explicit model for user-role assignments with additional metadata
    """
    __tablename__ = "user_role_assignments"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    role_id = Column(Integer, ForeignKey('roles.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=True)  # Organization-specific role
    granted_at = Column(DateTime, default=func.now(), nullable=False)
    granted_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    expires_at = Column(DateTime, nullable=True)  # Optional role expiration
    is_active = Column(Boolean, default=True)
    
    # Relationships - commented out temporarily to avoid FK conflicts
    # user = relationship("User", foreign_keys=[user_id])
    # role = relationship("Role")
    # organization = relationship("Org")
    # granted_by_user = relationship("User", foreign_keys=[granted_by])