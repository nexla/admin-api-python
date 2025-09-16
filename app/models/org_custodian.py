"""
OrgCustodian Model - Organization custodian relationship management.
Manages org custodianship assignments and permissions with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from ..database import Base


class OrgCustodian(Base):
    __tablename__ = "org_custodians"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    assigned_by = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    
    # Custodian permissions and settings
    role_level = Column(String(50), default="CUSTODIAN", index=True)  # CUSTODIAN, SUPER_CUSTODIAN
    permissions = Column(String(500))  # Comma-separated permission list
    is_active = Column(Boolean, default=True, index=True)
    
    # Access control
    can_manage_users = Column(Boolean, default=True)
    can_manage_data = Column(Boolean, default=True)
    can_manage_billing = Column(Boolean, default=False)
    can_manage_security = Column(Boolean, default=False)
    can_assign_custodians = Column(Boolean, default=False)
    
    # Delegation settings
    can_delegate = Column(Boolean, default=False)
    max_delegations = Column(Integer, default=0)
    
    # Timestamps
    assigned_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    expires_at = Column(DateTime, nullable=True, index=True)
    revoked_at = Column(DateTime, nullable=True, index=True)
    last_activity_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    org = relationship("Org", back_populates="org_custodians")
    user = relationship("User", foreign_keys=[user_id])
    assigned_by_user = relationship("User", foreign_keys=[assigned_by])
    
    # Rails business logic constants
    PERMISSION_LEVELS = {
        "CUSTODIAN": ["manage_users", "manage_data", "view_reports"],
        "SUPER_CUSTODIAN": ["manage_users", "manage_data", "manage_billing", "manage_security", "assign_custodians"]
    }
    DEFAULT_EXPIRY_DAYS = 365
    MAX_DELEGATION_DEPTH = 3
    
    # Rails predicate methods
    def active_(self) -> bool:
        """Rails predicate: Check if custodianship is active"""
        return (self.is_active and 
                self.revoked_at is None and
                (self.expires_at is None or self.expires_at > datetime.utcnow()))
    
    def expired_(self) -> bool:
        """Rails predicate: Check if custodianship has expired"""
        return self.expires_at is not None and self.expires_at <= datetime.utcnow()
    
    def revoked_(self) -> bool:
        """Rails predicate: Check if custodianship has been revoked"""
        return self.revoked_at is not None
    
    def super_custodian_(self) -> bool:
        """Rails predicate: Check if this is a super custodian"""
        return self.role_level == "SUPER_CUSTODIAN"
    
    def can_delegate_(self) -> bool:
        """Rails predicate: Check if can delegate custodianship"""
        return self.can_delegate and self.max_delegations > 0 and self.active_()
    
    def has_permission_(self, permission: str) -> bool:
        """Rails predicate: Check if has specific permission"""
        if not self.active_():
            return False
        
        # Check role-based permissions
        role_permissions = self.PERMISSION_LEVELS.get(self.role_level, [])
        if permission in role_permissions:
            return True
        
        # Check custom permissions
        if self.permissions:
            custom_permissions = [p.strip() for p in self.permissions.split(",")]
            return permission in custom_permissions
        
        return False
    
    def recent_activity_(self, days: int = 30) -> bool:
        """Rails predicate: Check if has recent activity"""
        if not self.last_activity_at:
            return False
        cutoff = datetime.utcnow() - timedelta(days=days)
        return self.last_activity_at >= cutoff
    
    # Rails business logic methods
    def grant_permission(self, permission: str) -> bool:
        """Grant additional permission to custodian (Rails pattern)"""
        if not self.active_():
            return False
        
        current_permissions = self.get_permissions_list()
        if permission not in current_permissions:
            current_permissions.append(permission)
            self.permissions = ",".join(current_permissions)
            return True
        return False
    
    def revoke_permission(self, permission: str) -> bool:
        """Revoke permission from custodian (Rails pattern)"""
        current_permissions = self.get_permissions_list()
        if permission in current_permissions:
            current_permissions.remove(permission)
            self.permissions = ",".join(current_permissions)
            return True
        return False
    
    def get_permissions_list(self) -> List[str]:
        """Get list of all permissions (Rails pattern)"""
        permissions = []
        
        # Add role-based permissions
        role_permissions = self.PERMISSION_LEVELS.get(self.role_level, [])
        permissions.extend(role_permissions)
        
        # Add custom permissions
        if self.permissions:
            custom_permissions = [p.strip() for p in self.permissions.split(",")]
            permissions.extend(custom_permissions)
        
        return list(set(permissions))  # Remove duplicates
    
    def extend_expiry(self, days: int = None) -> None:
        """Extend custodianship expiry (Rails pattern)"""
        days = days or self.DEFAULT_EXPIRY_DAYS
        if self.expires_at:
            self.expires_at = max(self.expires_at, datetime.utcnow()) + timedelta(days=days)
        else:
            self.expires_at = datetime.utcnow() + timedelta(days=days)
    
    def revoke_custodianship(self, revoked_by_user_id: int = None, reason: str = None) -> None:
        """Revoke custodianship (Rails pattern)"""
        self.revoked_at = datetime.utcnow()
        self.is_active = False
        # Would log revocation reason and user in audit system
        print(f"DEBUG: Custodianship revoked for user {self.user_id} in org {self.org_id} by user {revoked_by_user_id}")
    
    def update_activity(self) -> None:
        """Update last activity timestamp (Rails pattern)"""
        self.last_activity_at = datetime.utcnow()
    
    def can_manage_user_(self, target_user) -> bool:
        """Check if can manage specific user (Rails pattern)"""
        if not self.active_() or not self.can_manage_users:
            return False
        
        # Super custodians can manage all users in org
        if self.super_custodian_():
            return True
        
        # Regular custodians cannot manage other custodians
        if hasattr(target_user, 'is_custodian_of_org'):
            return not target_user.is_custodian_of_org(self.org_id)
        
        return True
    
    def can_access_billing_(self) -> bool:
        """Check if can access billing information (Rails pattern)"""
        return self.active_() and (self.can_manage_billing or self.super_custodian_())
    
    def can_assign_custodians_(self) -> bool:
        """Check if can assign other custodians (Rails pattern)"""
        return self.active_() and (self.can_assign_custodians or self.super_custodian_())
    
    def delegation_count(self) -> int:
        """Get number of active delegations (Rails pattern)"""
        # This would count delegations when delegation system is implemented
        return 0
    
    def can_create_delegation_(self) -> bool:
        """Check if can create new delegation (Rails pattern)"""
        return (self.can_delegate_() and 
                self.delegation_count() < self.max_delegations)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert custodian record to dictionary for API responses"""
        return {
            'id': self.id,
            'org_id': self.org_id,
            'user_id': self.user_id,
            'role_level': self.role_level,
            'permissions': self.get_permissions_list(),
            'is_active': self.is_active,
            'can_manage_users': self.can_manage_users,
            'can_manage_data': self.can_manage_data,
            'can_manage_billing': self.can_manage_billing,
            'can_manage_security': self.can_manage_security,
            'can_assign_custodians': self.can_assign_custodians,
            'can_delegate': self.can_delegate,
            'assigned_at': self.assigned_at.isoformat() if self.assigned_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'revoked_at': self.revoked_at.isoformat() if self.revoked_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'active': self.active_(),
            'expired': self.expired_(),
            'revoked': self.revoked_(),
            'super_custodian': self.super_custodian_(),
            'can_delegate_more': self.can_create_delegation_(),
            'delegation_count': self.delegation_count()
        }
    
    @classmethod
    def assign_custodian(cls, org, user, assigned_by_user=None, role_level="CUSTODIAN", 
                        expires_in_days=None, **permissions):
        """Assign custodianship to user (Rails pattern)"""
        expires_at = None
        if expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_in_days)
        elif role_level == "CUSTODIAN":
            expires_at = datetime.utcnow() + timedelta(days=cls.DEFAULT_EXPIRY_DAYS)
        
        custodian = cls(
            org_id=org.id if hasattr(org, 'id') else org,
            user_id=user.id if hasattr(user, 'id') else user,
            assigned_by=assigned_by_user.id if assigned_by_user and hasattr(assigned_by_user, 'id') else assigned_by_user,
            role_level=role_level,
            expires_at=expires_at,
            **permissions
        )
        
        # Set default permissions based on role
        if role_level == "SUPER_CUSTODIAN":
            custodian.can_manage_billing = True
            custodian.can_manage_security = True
            custodian.can_assign_custodians = True
        
        return custodian
    
    @classmethod
    def find_active_for_org(cls, org_id: int, session=None):
        """Find all active custodians for organization (Rails pattern)"""
        # This would query active custodians when session is available
        # For now, return empty list as placeholder
        return []
    
    @classmethod
    def find_by_user_and_org(cls, user_id: int, org_id: int, session=None):
        """Find custodianship by user and org (Rails pattern)"""
        # This would query custodianship when session is available
        # For now, return None as placeholder
        return None