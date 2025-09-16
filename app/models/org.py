from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from sqlalchemy.orm import Session
from ..database import Base

class Org(Base):
    __tablename__ = "orgs"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    status = Column(String(50), default="ACTIVE")
    
    # Organization settings
    allow_api_key_access = Column(Boolean, default=True)
    search_index_name = Column(String(255))
    client_identifier = Column(String(255))
    nexla_admin_org = Column(Boolean, default=False)  # Rails nexla_admin_org field
    
    # Timestamps
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"))
    org_tier_id = Column(Integer, ForeignKey("org_tiers.id"))
    cluster_id = Column(Integer, ForeignKey("clusters.id"))
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    users = relationship("User", foreign_keys="User.default_org_id", back_populates="default_org")
    org_tier = relationship("OrgTier")
    cluster = relationship("Cluster")
    org_memberships = relationship("OrgMembership", back_populates="org")
    data_sources = relationship("DataSource", back_populates="org")
    data_sets = relationship("DataSet", back_populates="org")
    data_sinks = relationship("DataSink", back_populates="org")
    flows = relationship("Flow", back_populates="org")
    projects = relationship("Project", back_populates="org")
    # teams = relationship("Team", back_populates="organization")  # TODO: Import Team model
    
    # Rails business logic constants
    STATUSES = {
        "active": "ACTIVE",
        "deactivated": "DEACTIVATED",
        "source_count_capped": "SOURCE_COUNT_CAPPED", 
        "source_data_capped": "SOURCE_DATA_CAPPED",
        "trial_expired": "TRIAL_EXPIRED"
    }
    
    CLUSTER_STATUS = {
        "active": "ACTIVE",
        "migrating": "MIGRATING"
    }
    
    NEXLA_ADMIN_EMAIL_DOMAIN = "nexla.com"
    NEXLA_ADMIN_ORG_BIT = b"\x01"
    
    # Additional Rails fields
    billing_owner_id = Column(Integer, ForeignKey("users.id"))
    new_cluster_id = Column(Integer, ForeignKey("clusters.id"))
    
    # Additional relationships for Rails parity
    billing_owner = relationship("User", foreign_keys=[billing_owner_id])
    new_cluster = relationship("Cluster", foreign_keys=[new_cluster_id])
    
    # Custodian relationships (Rails org_custodians equivalent)
    org_custodians = relationship("OrgCustodian", back_populates="org")
    
    # Billing relationships (Rails billing_accounts, subscriptions equivalent)
    billing_accounts = relationship("BillingAccount", back_populates="org")
    subscriptions = relationship("Subscription", back_populates="org")
    
    @property
    def member_users(self) -> List:
        """Rails-style through relationship: Get users who are members of this org"""
        from .user import User
        return [membership.user for membership in self.org_memberships 
                if membership.status == "ACTIVE"]
    
    # Rails predicate methods (status checking)
    def is_active(self) -> bool:
        """Check if org is active (Rails active? pattern)"""
        return self.status == self.STATUSES["active"]
    
    def active_(self) -> bool:
        """Rails predicate: Check if org is active"""
        return self.is_active()
    
    def active(self) -> bool:
        """Alias for is_active (Rails pattern)"""
        return self.is_active()
    
    def deactivated_(self) -> bool:
        """Rails predicate: Check if org is deactivated"""
        return self.deactivated()
    
    def deactivated(self) -> bool:
        """Check if org is deactivated (Rails deactivated? pattern)"""
        return self.status == self.STATUSES["deactivated"]
    
    def is_nexla_admin_org(self) -> bool:
        """Check if this is the Nexla admin org (Rails pattern)"""
        return bool(self.nexla_admin_org)
    
    def nexla_admin_org_(self) -> bool:
        """Rails predicate: Check if this is the Nexla admin org"""
        return self.is_nexla_admin_org()
    
    def nexla_admin_org_q(self) -> bool:
        """Alias for is_nexla_admin_org (Rails nexla_admin_org? pattern)"""
        return self.is_nexla_admin_org()
    
    # Rails bang methods (status manipulation)
    def activate_(self) -> None:
        """Rails bang method: Activate org"""
        self.activate()
    
    def activate(self) -> None:
        """Activate org (Rails activate! pattern)"""
        if self.owner.deactivated_():
            raise ValueError(f"Org cannot be activated while owner's account is deactivated: {self.owner.id}")
        
        if self.owner.account_locked_():
            raise ValueError(f"Org cannot be activated while owner's account is locked: {self.owner.id}")
        
        if not self.owner.org_member_(self):
            raise ValueError(f"Org cannot be activated while owner's membership is deactivated: {self.owner.id}")
        
        self.status = self.STATUSES["active"]
    
    def deactivate_(self, pause_flows: bool = False) -> None:
        """Rails bang method: Deactivate org"""
        self.deactivate(pause_flows)
    
    def deactivate(self, pause_flows: bool = False) -> None:
        """Deactivate org (Rails deactivate! pattern)"""
        self.status = self.STATUSES["deactivated"]
        
        # Would pause flows if requested
        if pause_flows:
            # self.pause_flows()
            pass
    
    @classmethod
    def get_nexla_admin_org(cls):
        """Get the Nexla admin org (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            
            # This would need proper session handling in real implementation
            # For now, simulate the logic
            Session = sessionmaker(bind=cls.__table__.bind if hasattr(cls.__table__, 'bind') else None)
            if Session.bind:
                db = Session()
                try:
                    org = db.query(cls).filter(cls.nexla_admin_org.is_not(None)).first()
                    if not org:
                        raise ValueError("ERROR: Nexla admin org not found!")
                    return org
                finally:
                    db.close()
            else:
                raise ValueError("ERROR: Nexla admin org not found!")
        except Exception:
            raise ValueError("ERROR: Nexla admin org not found!")
    
    @classmethod
    def validate_status(cls, status_str: str) -> Optional[str]:
        """Validate org status string (Rails pattern)"""
        if not isinstance(status_str, str):
            return None
        
        valid_statuses = list(cls.STATUSES.values())
        if status_str in valid_statuses:
            return status_str
        return None
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any], api_user, api_org):
        """Build org from input data (Rails pattern)"""
        if not isinstance(input_data, dict):
            return None
        
        # This would be a complex factory method for creating orgs
        # Implementation would depend on complete user/cluster system
        return None
    
    def set_defaults(self, user, cluster_id: Optional[int] = None) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        if cluster_id:
            self.cluster_id = cluster_id
    
    def admin_users(self) -> List:
        """Get list of admin users (Rails pattern)"""
        admin_users = [self.owner] if self.owner else []
        
        # This would query access controls when implemented
        # For now, just return owner
        return admin_users
    
    def owner_(self, user) -> bool:
        """Rails predicate: Check if user is owner of org"""
        return self.is_owner(user)
    
    def is_owner(self, user) -> bool:
        """Check if user is owner of org (Rails is_owner? pattern)"""
        return self.owner_id == user.id if user else False
    
    def admin_access_(self, user) -> bool:
        """Rails predicate: Check if user has admin access to org"""
        return self.has_admin_access(user)
    
    def has_admin_access(self, user) -> bool:
        """Check if user has admin access to org (Rails pattern)"""
        if not user:
            return False
        
        # Owner always has admin access
        if self.is_owner(user):
            return True
        
        # Super users have admin access
        if user.super_user_():
            return True
        
        # This would check access controls when implemented
        return False
    
    def add_members_(self, user_or_users) -> None:
        """Rails bang method: Add members to org"""
        self.add_members(user_or_users)
    
    def add_members(self, user_or_users) -> None:
        """Add members to org (Rails pattern)"""
        # This would create org memberships when implemented
        pass
    
    def remove_members_(self, user_or_users) -> None:
        """Rails bang method: Remove members from org"""
        self.remove_members(user_or_users)
    
    def remove_members(self, user_or_users) -> None:
        """Remove members from org (Rails pattern)"""
        # This would deactivate org memberships when implemented
        pass
    
    def add_admin_(self, user) -> None:
        """Rails bang method: Add admin role to user"""
        self.add_admin(user)
    
    def add_admin(self, user) -> None:
        """Add admin role to user (Rails pattern)"""
        # This would create admin access control when implemented
        pass
    
    def remove_admin_(self, user) -> None:
        """Rails bang method: Remove admin role from user"""
        self.remove_admin(user)
    
    def remove_admin(self, user) -> None:
        """Remove admin role from user (Rails pattern)"""
        # This would remove admin access control when implemented
        pass
    
    def role_(self, user, role: str, context_org=None) -> bool:
        """Rails predicate: Check if user has specific role in org"""
        return self.has_role(user, role, context_org)
    
    def has_role(self, user, role: str, context_org=None) -> bool:
        """Check if user has specific role in org (Rails pattern)"""
        # This would check role system when implemented
        return False
    
    def validate_billing_owner(self) -> None:
        """Validate billing owner (Rails pattern)"""
        # This would validate billing owner constraints
        pass
    
    def handle_after_create(self) -> None:
        """Handle post-creation tasks (Rails pattern)"""
        # This would handle post-creation setup
        pass
    
    def org_webhook_host(self) -> Optional[str]:
        """Get org webhook host (Rails pattern)"""
        # This would return webhook host configuration
        return None
    
    # ====================
    # Rails Scopes
    # ====================
    
    @classmethod
    def active(cls, db: Session):
        """Rails scope: Get active orgs"""
        return db.query(cls).filter(cls.status == cls.STATUSES["active"])
    
    @classmethod
    def deactivated(cls, db: Session):
        """Rails scope: Get deactivated orgs"""
        return db.query(cls).filter(cls.status == cls.STATUSES["deactivated"])
    
    @classmethod
    def by_status(cls, db: Session, status: str):
        """Rails scope: Find orgs by status"""
        return db.query(cls).filter(cls.status == status)
    
    @classmethod
    def by_owner(cls, db: Session, user_id: int):
        """Rails scope: Find orgs owned by user"""
        return db.query(cls).filter(cls.owner_id == user_id)
    
    @classmethod
    def with_owner(cls, db: Session):
        """Rails scope: Find orgs that have an owner"""
        return db.query(cls).filter(cls.owner_id.isnot(None))
    
    @classmethod
    def without_owner(cls, db: Session):
        """Rails scope: Find orgs without an owner"""
        return db.query(cls).filter(cls.owner_id.is_(None))
    
    @classmethod
    def nexla_admin_orgs(cls, db: Session):
        """Rails scope: Find Nexla admin orgs"""
        return db.query(cls).filter(cls.nexla_admin_org == True)
    
    @classmethod
    def non_admin_orgs(cls, db: Session):
        """Rails scope: Find non-admin orgs"""
        return db.query(cls).filter(cls.nexla_admin_org != True)
    
    @classmethod
    def by_cluster(cls, db: Session, cluster_id: int):
        """Rails scope: Find orgs by cluster"""
        return db.query(cls).filter(cls.cluster_id == cluster_id)
    
    @classmethod
    def with_api_key_access(cls, db: Session):
        """Rails scope: Find orgs that allow API key access"""
        return db.query(cls).filter(cls.allow_api_key_access == True)
    
    @classmethod
    def without_api_key_access(cls, db: Session):
        """Rails scope: Find orgs that don't allow API key access"""
        return db.query(cls).filter(cls.allow_api_key_access == False)
    
    @classmethod
    def by_tier(cls, db: Session, tier_id: int):
        """Rails scope: Find orgs by tier"""
        return db.query(cls).filter(cls.org_tier_id == tier_id)
    
    @classmethod
    def with_members(cls, db: Session):
        """Rails scope: Find orgs with active members"""
        from .org_membership import OrgMembership
        return db.query(cls).join(OrgMembership).filter(
            OrgMembership.status == "ACTIVE"
        ).distinct()
    
    @classmethod
    def by_member(cls, db: Session, user_id: int):
        """Rails scope: Find orgs where user is a member"""
        from .org_membership import OrgMembership
        return db.query(cls).join(OrgMembership).filter(
            OrgMembership.user_id == user_id,
            OrgMembership.status == "ACTIVE"
        )
    
    @classmethod
    def with_search_index(cls, db: Session):
        """Rails scope: Find orgs with search index configured"""
        return db.query(cls).filter(cls.search_index_name.isnot(None))
    
    @classmethod
    def recent(cls, db: Session, days: int = 30):
        """Rails scope: Find recently created orgs"""
        from datetime import datetime, timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(cls.created_at >= cutoff_date)
    
    @classmethod
    def statuses_enum(cls) -> str:
        """Get SQL ENUM string for statuses (Rails pattern)"""
        enum = "ENUM("
        first = True
        for status in cls.STATUSES.values():
            if not first:
                enum += ","
            enum += f"'{status}'"
            first = False
        enum += ")"
        return enum
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert org to dictionary for API responses"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'status': self.status,
            'is_active': self.active_(),
            'deactivated': self.deactivated_(),
            'is_nexla_admin_org': self.nexla_admin_org_(),
            'allow_api_key_access': self.allow_api_key_access,
            'search_index_name': self.search_index_name,
            'client_identifier': self.client_identifier,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'owner_id': self.owner_id,
            'org_tier_id': self.org_tier_id,
            'cluster_id': self.cluster_id,
            'billing_owner_id': self.billing_owner_id
        }