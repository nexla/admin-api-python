"""
DomainCustodian Model - Domain custodian relationship management.
Manages domain custodianship assignments and DNS/domain permissions with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from ..database import Base


class DomainCustodian(Base):
    __tablename__ = "domain_custodians"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign keys
    domain_id = Column(Integer, ForeignKey("domains.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    assigned_by = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    
    # Custodian permissions and settings
    role_level = Column(String(50), default="DOMAIN_CUSTODIAN", index=True)  # DOMAIN_CUSTODIAN, DOMAIN_ADMIN
    permissions = Column(String(500))  # Comma-separated permission list
    is_active = Column(Boolean, default=True, index=True)
    
    # Domain-specific permissions
    can_manage_dns = Column(Boolean, default=True)
    can_manage_subdomains = Column(Boolean, default=True)
    can_manage_certificates = Column(Boolean, default=False)
    can_manage_redirects = Column(Boolean, default=True)
    can_manage_email_routing = Column(Boolean, default=False)
    can_delegate_domain = Column(Boolean, default=False)
    
    # Access control settings
    allowed_record_types = Column(String(200))  # Comma-separated DNS record types: A,CNAME,MX,TXT
    restricted_subdomains = Column(Text)  # JSON list of restricted subdomain patterns
    max_subdomains = Column(Integer, default=100)
    
    # Security settings
    requires_approval = Column(Boolean, default=False)  # Changes require approval
    approval_required_for = Column(String(300))  # Comma-separated: dns,certificates,email
    
    # Timestamps
    assigned_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    expires_at = Column(DateTime, nullable=True, index=True)
    revoked_at = Column(DateTime, nullable=True, index=True)
    last_activity_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    domain = relationship("Domain", back_populates="domain_custodians")
    user = relationship("User", foreign_keys=[user_id])
    org = relationship("Org", foreign_keys=[org_id])
    assigned_by_user = relationship("User", foreign_keys=[assigned_by])
    
    # Rails business logic constants
    PERMISSION_LEVELS = {
        "DOMAIN_CUSTODIAN": ["manage_dns", "manage_subdomains", "manage_redirects"],
        "DOMAIN_ADMIN": ["manage_dns", "manage_subdomains", "manage_certificates", 
                        "manage_redirects", "manage_email_routing", "delegate_domain"]
    }
    
    DEFAULT_RECORD_TYPES = ["A", "AAAA", "CNAME", "TXT"]
    ADMIN_RECORD_TYPES = ["A", "AAAA", "CNAME", "MX", "TXT", "SRV", "CAA"]
    DEFAULT_EXPIRY_DAYS = 180  # Shorter than org custodians
    MAX_SUBDOMAINS_DEFAULT = 100
    
    # Rails predicate methods
    def active_(self) -> bool:
        """Rails predicate: Check if domain custodianship is active"""
        return (self.is_active and 
                self.revoked_at is None and
                (self.expires_at is None or self.expires_at > datetime.utcnow()))
    
    def expired_(self) -> bool:
        """Rails predicate: Check if domain custodianship has expired"""
        return self.expires_at is not None and self.expires_at <= datetime.utcnow()
    
    def revoked_(self) -> bool:
        """Rails predicate: Check if domain custodianship has been revoked"""
        return self.revoked_at is not None
    
    def domain_admin_(self) -> bool:
        """Rails predicate: Check if this is a domain admin"""
        return self.role_level == "DOMAIN_ADMIN"
    
    def can_delegate_(self) -> bool:
        """Rails predicate: Check if can delegate domain custodianship"""
        return self.can_delegate_domain and self.active_() and self.domain_admin_()
    
    def requires_approval_(self) -> bool:
        """Rails predicate: Check if changes require approval"""
        return self.requires_approval and self.active_()
    
    def has_dns_permission_(self, record_type: str = None) -> bool:
        """Rails predicate: Check if has DNS management permission"""
        if not self.active_() or not self.can_manage_dns:
            return False
        
        if not record_type:
            return True
        
        allowed_types = self.get_allowed_record_types()
        return record_type.upper() in allowed_types
    
    def can_manage_subdomain_(self, subdomain: str) -> bool:
        """Rails predicate: Check if can manage specific subdomain"""
        if not self.active_() or not self.can_manage_subdomains:
            return False
        
        # Check against restricted subdomain patterns
        restricted = self.get_restricted_subdomains()
        for pattern in restricted:
            if self._matches_pattern(subdomain, pattern):
                return False
        
        return True
    
    def at_subdomain_limit_(self) -> bool:
        """Rails predicate: Check if at subdomain creation limit"""
        current_count = self.get_managed_subdomain_count()
        return current_count >= self.max_subdomains
    
    def recent_activity_(self, days: int = 7) -> bool:
        """Rails predicate: Check if has recent activity"""
        if not self.last_activity_at:
            return False
        cutoff = datetime.utcnow() - timedelta(days=days)
        return self.last_activity_at >= cutoff
    
    # Rails business logic methods
    def get_allowed_record_types(self) -> List[str]:
        """Get list of allowed DNS record types (Rails pattern)"""
        if self.allowed_record_types:
            return [rt.strip().upper() for rt in self.allowed_record_types.split(",")]
        
        # Default based on role level
        if self.domain_admin_():
            return self.ADMIN_RECORD_TYPES
        else:
            return self.DEFAULT_RECORD_TYPES
    
    def get_restricted_subdomains(self) -> List[str]:
        """Get list of restricted subdomain patterns (Rails pattern)"""
        if self.restricted_subdomains:
            import json
            try:
                return json.loads(self.restricted_subdomains)
            except:
                return []
        return []
    
    def add_allowed_record_type(self, record_type: str) -> bool:
        """Add allowed DNS record type (Rails pattern)"""
        current_types = self.get_allowed_record_types()
        record_type = record_type.upper()
        
        if record_type not in current_types:
            current_types.append(record_type)
            self.allowed_record_types = ",".join(current_types)
            return True
        return False
    
    def remove_allowed_record_type(self, record_type: str) -> bool:
        """Remove allowed DNS record type (Rails pattern)"""
        current_types = self.get_allowed_record_types()
        record_type = record_type.upper()
        
        if record_type in current_types:
            current_types.remove(record_type)
            self.allowed_record_types = ",".join(current_types)
            return True
        return False
    
    def add_restricted_subdomain(self, pattern: str) -> bool:
        """Add restricted subdomain pattern (Rails pattern)"""
        restricted = self.get_restricted_subdomains()
        if pattern not in restricted:
            restricted.append(pattern)
            import json
            self.restricted_subdomains = json.dumps(restricted)
            return True
        return False
    
    def remove_restricted_subdomain(self, pattern: str) -> bool:
        """Remove restricted subdomain pattern (Rails pattern)"""
        restricted = self.get_restricted_subdomains()
        if pattern in restricted:
            restricted.remove(pattern)
            import json
            self.restricted_subdomains = json.dumps(restricted)
            return True
        return False
    
    def _matches_pattern(self, subdomain: str, pattern: str) -> bool:
        """Check if subdomain matches restriction pattern (helper)"""
        import re
        # Convert shell-style wildcards to regex
        regex_pattern = pattern.replace('*', '.*').replace('?', '.')
        return bool(re.match(f"^{regex_pattern}$", subdomain, re.IGNORECASE))
    
    def get_managed_subdomain_count(self) -> int:
        """Get number of subdomains currently managed (Rails pattern)"""
        # This would count subdomains when subdomain tracking is implemented
        return 0
    
    def grant_permission(self, permission: str) -> bool:
        """Grant additional permission to domain custodian (Rails pattern)"""
        if not self.active_():
            return False
        
        permission_mapping = {
            'manage_certificates': 'can_manage_certificates',
            'manage_email_routing': 'can_manage_email_routing',
            'delegate_domain': 'can_delegate_domain'
        }
        
        if permission in permission_mapping:
            attr_name = permission_mapping[permission]
            if hasattr(self, attr_name):
                setattr(self, attr_name, True)
                return True
        
        return False
    
    def revoke_permission(self, permission: str) -> bool:
        """Revoke permission from domain custodian (Rails pattern)"""
        permission_mapping = {
            'manage_certificates': 'can_manage_certificates',
            'manage_email_routing': 'can_manage_email_routing',
            'delegate_domain': 'can_delegate_domain'
        }
        
        if permission in permission_mapping:
            attr_name = permission_mapping[permission]
            if hasattr(self, attr_name):
                setattr(self, attr_name, False)
                return True
        
        return False
    
    def extend_expiry(self, days: int = None) -> None:
        """Extend domain custodianship expiry (Rails pattern)"""
        days = days or self.DEFAULT_EXPIRY_DAYS
        if self.expires_at:
            self.expires_at = max(self.expires_at, datetime.utcnow()) + timedelta(days=days)
        else:
            self.expires_at = datetime.utcnow() + timedelta(days=days)
    
    def revoke_custodianship(self, revoked_by_user_id: int = None, reason: str = None) -> None:
        """Revoke domain custodianship (Rails pattern)"""
        self.revoked_at = datetime.utcnow()
        self.is_active = False
        # Would log revocation reason and user in audit system
        print(f"DEBUG: Domain custodianship revoked for user {self.user_id} on domain {self.domain_id} by user {revoked_by_user_id}")
    
    def update_activity(self) -> None:
        """Update last activity timestamp (Rails pattern)"""
        self.last_activity_at = datetime.utcnow()
    
    def requires_approval_for_(self, action: str) -> bool:
        """Check if specific action requires approval (Rails pattern)"""
        if not self.requires_approval_():
            return False
        
        if not self.approval_required_for:
            return True  # If approval is required but no specific actions listed, require for all
        
        approval_actions = [a.strip() for a in self.approval_required_for.split(",")]
        return action in approval_actions
    
    def can_perform_action_(self, action: str, **kwargs) -> bool:
        """Check if can perform specific domain action (Rails pattern)"""
        if not self.active_():
            return False
        
        action_permissions = {
            'create_dns_record': self.can_manage_dns,
            'update_dns_record': self.can_manage_dns,
            'delete_dns_record': self.can_manage_dns,
            'create_subdomain': self.can_manage_subdomains,
            'manage_certificate': self.can_manage_certificates,
            'setup_redirect': self.can_manage_redirects,
            'configure_email': self.can_manage_email_routing
        }
        
        base_permission = action_permissions.get(action, False)
        if not base_permission:
            return False
        
        # Additional checks for specific actions
        if action in ['create_dns_record', 'update_dns_record', 'delete_dns_record']:
            record_type = kwargs.get('record_type')
            if record_type and not self.has_dns_permission_(record_type):
                return False
        
        if action == 'create_subdomain':
            subdomain = kwargs.get('subdomain')
            if subdomain and not self.can_manage_subdomain_(subdomain):
                return False
            if self.at_subdomain_limit_():
                return False
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert domain custodian record to dictionary for API responses"""
        return {
            'id': self.id,
            'domain_id': self.domain_id,
            'user_id': self.user_id,
            'org_id': self.org_id,
            'role_level': self.role_level,
            'is_active': self.is_active,
            'can_manage_dns': self.can_manage_dns,
            'can_manage_subdomains': self.can_manage_subdomains,
            'can_manage_certificates': self.can_manage_certificates,
            'can_manage_redirects': self.can_manage_redirects,
            'can_manage_email_routing': self.can_manage_email_routing,
            'can_delegate_domain': self.can_delegate_domain,
            'allowed_record_types': self.get_allowed_record_types(),
            'restricted_subdomains': self.get_restricted_subdomains(),
            'max_subdomains': self.max_subdomains,
            'requires_approval': self.requires_approval,
            'assigned_at': self.assigned_at.isoformat() if self.assigned_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'revoked_at': self.revoked_at.isoformat() if self.revoked_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'active': self.active_(),
            'expired': self.expired_(),
            'revoked': self.revoked_(),
            'domain_admin': self.domain_admin_(),
            'can_delegate': self.can_delegate_(),
            'at_subdomain_limit': self.at_subdomain_limit_(),
            'managed_subdomain_count': self.get_managed_subdomain_count()
        }
    
    @classmethod
    def assign_domain_custodian(cls, domain, user, org, assigned_by_user=None, 
                               role_level="DOMAIN_CUSTODIAN", expires_in_days=None, **permissions):
        """Assign domain custodianship to user (Rails pattern)"""
        expires_at = None
        if expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_in_days)
        else:
            expires_at = datetime.utcnow() + timedelta(days=cls.DEFAULT_EXPIRY_DAYS)
        
        custodian = cls(
            domain_id=domain.id if hasattr(domain, 'id') else domain,
            user_id=user.id if hasattr(user, 'id') else user,
            org_id=org.id if hasattr(org, 'id') else org,
            assigned_by=assigned_by_user.id if assigned_by_user and hasattr(assigned_by_user, 'id') else assigned_by_user,
            role_level=role_level,
            expires_at=expires_at,
            **permissions
        )
        
        # Set default permissions based on role
        if role_level == "DOMAIN_ADMIN":
            custodian.can_manage_certificates = True
            custodian.can_manage_email_routing = True
            custodian.can_delegate_domain = True
            custodian.allowed_record_types = ",".join(cls.ADMIN_RECORD_TYPES)
        else:
            custodian.allowed_record_types = ",".join(cls.DEFAULT_RECORD_TYPES)
        
        return custodian
    
    @classmethod
    def find_active_for_domain(cls, domain_id: int, session=None):
        """Find all active custodians for domain (Rails pattern)"""
        # This would query active domain custodians when session is available
        # For now, return empty list as placeholder
        return []
    
    @classmethod
    def find_by_user_and_domain(cls, user_id: int, domain_id: int, session=None):
        """Find domain custodianship by user and domain (Rails pattern)"""
        # This would query domain custodianship when session is available
        # For now, return None as placeholder
        return None