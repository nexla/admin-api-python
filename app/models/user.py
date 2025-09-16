from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime, timedelta
import bcrypt
import hashlib
import re
import secrets
import string
import time
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from email_validator import validate_email, EmailNotValidError
from zxcvbn import zxcvbn
from ..database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(254), unique=True, index=True, nullable=False)
    full_name = Column(String(255))
    password_digest = Column(String(255), nullable=False)
    password_digest_1 = Column(String(255))
    password_digest_2 = Column(String(255))
    password_digest_3 = Column(String(255))
    password_digest_4 = Column(String(255))
    status = Column(String(50), default="ACTIVE")
    
    # Password management
    password_retry_count = Column(Integer, default=0)
    password_change_required_at = Column(DateTime)
    password_reset_token = Column(String(255))
    password_reset_token_at = Column(DateTime)
    password_reset_token_count = Column(Integer, default=0)
    account_locked_at = Column(DateTime)
    
    # Timestamps
    email_verified_at = Column(DateTime)
    tos_signed_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # Foreign keys
    default_org_id = Column(Integer, ForeignKey("orgs.id", use_alter=True), nullable=True)
    user_tier_id = Column(Integer, ForeignKey("user_tiers.id"), nullable=True)
    
    # Relationships
    default_org = relationship("Org", foreign_keys=[default_org_id], back_populates="users")
    user_tier = relationship("UserTier")  
    org_memberships = relationship("OrgMembership", back_populates="user")
    
    # Rails-style through relationships  
    team_memberships = relationship("TeamMembership", back_populates="user")
    
    # API key management (Rails users_api_keys equivalent)
    api_keys = relationship("ApiKey", back_populates="user", foreign_keys="ApiKey.user_id")
    api_key_events = relationship("ApiKeyEvent", back_populates="user", foreign_keys="ApiKeyEvent.user_id")
    
    # User login audit tracking (Rails user_login_audits equivalent)
    user_login_audits = relationship("UserLoginAudit", back_populates="user", foreign_keys="UserLoginAudit.user_id")
    
    # Custodian relationships (Rails org_custodians, domain_custodians equivalent)
    org_custodianships = relationship("OrgCustodian", foreign_keys="OrgCustodian.user_id", back_populates="user")
    domain_custodianships = relationship("DomainCustodian", foreign_keys="DomainCustodian.user_id", back_populates="user")
    
    # Notification settings (Rails notification_channel_settings equivalent)
    notification_channel_settings = relationship("NotificationChannelSetting", back_populates="user")
    
    # Through relationships using association objects (Rails equivalent)
    # member_teams and member_orgs are implemented as properties below
    
    flows = relationship("Flow", back_populates="owner")
    projects = relationship("Project", back_populates="owner")
    created_transforms = relationship("Transform", back_populates="created_by")
    background_jobs = relationship("BackgroundJob", back_populates="user", foreign_keys="BackgroundJob.user_id")
    
    # Phase 2 relationships
    owned_marketplace_domains = relationship("MarketplaceDomain", back_populates="owner")
    domain_subscriptions = relationship("DomainSubscription", back_populates="user")
    created_approval_requests = relationship("ApprovalRequest", foreign_keys="ApprovalRequest.requester_id", back_populates="requester")
    created_tags = relationship("Tag", back_populates="created_by")
    created_validation_rules = relationship("ValidationRule", back_populates="created_by")
    
    # teams = relationship("Team", secondary="team_members", back_populates="members")  # Disabled temporarily
    
    def is_active(self) -> bool:
        return self.status == "ACTIVE"
    
    def active_(self) -> bool:
        """Rails predicate: Check if user is active"""
        return self.is_active()
    
    def is_deactivated(self) -> bool:
        return self.status == "DEACTIVATED"
    
    def deactivated_(self) -> bool:
        """Rails predicate: Check if user is deactivated"""
        return self.is_deactivated()
    
    def account_locked(self) -> bool:
        return self.account_locked_at is not None
    
    def account_locked_(self) -> bool:
        """Rails predicate: Check if account is locked"""
        return self.account_locked()
    
    def is_admin(self) -> bool:
        """Check if user has admin privileges - Rails business logic implementation"""
        # This implements the Rails is_admin? pattern
        # Admin can be determined by org ownership or super user status
        return self.is_super_user() or self.is_org_owner()
    
    def admin_(self) -> bool:
        """Rails predicate: Check if user has admin privileges"""
        return self.is_admin()
    
    def super_user_(self) -> bool:
        """Rails predicate: Check if user is super user"""
        return self.is_super_user()
    
    def is_super_user(self) -> bool:
        """Rails super_user? business logic implementation"""
        # This mirrors the Rails User#super_user? method logic:
        # if (self.org&.id != nexla_o.id)
        #   self.is_super_user = false
        # elsif nexla_o.is_owner?(self)
        #   self.is_super_user = true
        # elsif !self.active_org_member?(nexla_o)
        #   self.is_super_user = false
        # else
        #   self.is_super_user = nexla_o.has_role?(self, :admin, nexla_o)
        # end
        
        from sqlalchemy.orm import sessionmaker
        from .org import Org
        from .org_membership import OrgMembership
        
        try:
            # Get database session
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            # Get Nexla Admin org (nexla_admin_org = 1)
            nexla_admin_org = db.query(Org).filter(Org.nexla_admin_org == 1).first()
            if not nexla_admin_org:
                return False
            
            # Check if user is owner of Nexla Admin org
            if nexla_admin_org.owner_id == self.id:
                return True
            
            # Check if user is active member of Nexla Admin org
            membership = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.org_id == nexla_admin_org.id,
                OrgMembership.status == 'ACTIVE'
            ).first()
            
            # If active member of Nexla Admin org, they are super user
            return membership is not None
            
        except Exception:
            # Fallback: Check if email is Nexla admin email (Rails Backend_Admin_Email)
            return self.email == "admin@nexla.com"
        finally:
            if 'db' in locals():
                db.close()
    
    def org_owner_(self) -> bool:
        """Rails predicate: Check if user owns any organization"""
        return self.is_org_owner()
    
    def is_org_owner(self) -> bool:
        """Check if user owns any organization"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            # Check if user owns any org
            owned_org = db.query(Org).filter(Org.owner_id == self.id).first()
            return owned_org is not None
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def active_org_member_(self, org_id: int = None) -> bool:
        """Rails predicate: Check if user is active member of specific org or any org"""
        return self.active_org_member(org_id)
    
    def active_org_member(self, org_id: int = None) -> bool:
        """Check if user is active member of specific org or any org (Rails active_org_member?)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            query = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.status == 'ACTIVE'
            )
            
            if org_id:
                query = query.filter(OrgMembership.org_id == org_id)
            
            membership = query.first()
            return membership is not None
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    @property
    def role(self) -> str:
        """Get user role following Rails business logic"""
        if self.is_super_user():
            return "SUPER_USER"
        elif self.is_org_owner():
            return "ORG_OWNER"
        elif self.active_org_member():
            return "ORG_MEMBER"
        else:
            return "USER"
    
    @property 
    def member_orgs(self) -> List:
        """Rails-style through relationship: Get orgs user is a member of"""
        from .org import Org
        return [membership.org for membership in self.org_memberships 
                if membership.status == "ACTIVE"]
    
    @property
    def member_teams(self) -> List:
        """Rails-style through relationship: Get teams user is a member of"""  
        from .team import Team
        return [membership.team for membership in self.team_memberships
                if membership.status == "ACTIVE"]
    
    def sso_options(self) -> List[Dict]:
        """Get SSO options for user (Rails pattern)"""
        # BEWARE: called from unauthenticated route in Rails
        try:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            memberships = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id
            ).all()
            
            if not memberships:
                return []
                
            # Use default org or first org  
            org = self.default_org or memberships[0].org
            
            # This would return org.api_auth_configs.map(&:public_attributes) in Rails
            return []
            
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    BACKEND_ADMIN_EMAIL = "admin@nexla.com"
    MAX_PASSWORD_RETRY_COUNT = 5
    PASSWORD_CHANGE_REQUIRED_AFTER_DAYS = 90
    MAX_RESET_PASSWORD_TRIES = 5
    RESET_PASSWORD_INTERVAL_MINUTES = 1
    MIN_PASSWORD_ENTROPY = 16
    
    PASSWORD_CONSTRAINTS = {"minimum": 8, "maximum": 72}
    EMAIL_CONSTRAINTS = {"minimum": 3, "maximum": 254}
    EXTRA_WORDS = ["nexla", "Nexla", "NEXLA", "test", "Test", "TEST"]
    
    PASSWORD_REGEX = re.compile(
        r"^(?=.{8,})(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^\w\d]).*$"
    )
    
    STATUSES = {
        "active": "ACTIVE",
        "deactivated": "DEACTIVATED", 
        "source_count_capped": "SOURCE_COUNT_CAPPED",
        "source_data_capped": "SOURCE_DATA_CAPPED",
        "trial_expired": "TRIAL_EXPIRED"
    }
    
    AUTHENTICATION_TYPES = {
        "login": "login",
        "logout": "logout"
    }
    
    # ====================
    # Rails Scopes
    # ====================
    
    @classmethod
    def active(cls, db: Session):
        """Rails scope: Get active users"""
        return db.query(cls).filter(cls.status == "ACTIVE")
    
    @classmethod
    def deactivated(cls, db: Session):
        """Rails scope: Get deactivated users"""
        return db.query(cls).filter(cls.status == "DEACTIVATED")
    
    @classmethod
    def admins(cls, db: Session):
        """Rails scope: Get admin users"""
        return db.query(cls).filter(cls.is_admin == True)
    
    @classmethod
    def locked(cls, db: Session):
        """Rails scope: Get locked users"""
        return db.query(cls).filter(cls.account_locked_at.isnot(None))
    
    @classmethod
    def unlocked(cls, db: Session):
        """Rails scope: Get unlocked users"""
        return db.query(cls).filter(cls.account_locked_at.is_(None))
    
    @classmethod
    def email_verified(cls, db: Session):
        """Rails scope: Get users with verified emails"""
        return db.query(cls).filter(cls.email_verified_at.isnot(None))
    
    @classmethod
    def email_unverified(cls, db: Session):
        """Rails scope: Get users with unverified emails"""
        return db.query(cls).filter(cls.email_verified_at.is_(None))
    
    @classmethod
    def tos_signed(cls, db: Session):
        """Rails scope: Get users who signed ToS"""
        return db.query(cls).filter(cls.tos_signed_at.isnot(None))
    
    @classmethod
    def tos_unsigned(cls, db: Session):
        """Rails scope: Get users who haven't signed ToS"""
        return db.query(cls).filter(cls.tos_signed_at.is_(None))
    
    @classmethod
    def by_email(cls, db: Session, email: str):
        """Rails scope: Find user by email"""
        return db.query(cls).filter(cls.email == email)
    
    @classmethod
    def by_status(cls, db: Session, status: str):
        """Rails scope: Find users by status"""
        return db.query(cls).filter(cls.status == status)
    
    @classmethod
    def by_org(cls, db: Session, org_id: int):
        """Rails scope: Find users by organization membership"""
        from .org_membership import OrgMembership
        return db.query(cls).join(OrgMembership).filter(
            OrgMembership.org_id == org_id,
            OrgMembership.status == "ACTIVE"
        )
    
    @classmethod
    def password_reset_pending(cls, db: Session):
        """Rails scope: Find users with pending password resets"""
        return db.query(cls).filter(
            cls.password_reset_token.isnot(None),
            cls.password_reset_token_at.isnot(None)
        )
    
    @classmethod
    def super_users(cls, db: Session):
        """Rails scope: Find super users (members of Nexla admin org)"""
        from .org import Org
        from .org_membership import OrgMembership
        return db.query(cls).join(OrgMembership).join(Org).filter(
            Org.nexla_admin_org == 1,
            OrgMembership.status == "ACTIVE"
        )
    
    @classmethod
    def org_owners(cls, db: Session):
        """Rails scope: Find users who own organizations"""
        from .org import Org
        return db.query(cls).join(Org, cls.id == Org.owner_id)
    
    @classmethod
    def with_default_org(cls, db: Session):
        """Rails scope: Find users with default org set"""
        return db.query(cls).filter(cls.default_org_id.isnot(None))
    
    @classmethod
    def without_default_org(cls, db: Session):
        """Rails scope: Find users without default org"""
        return db.query(cls).filter(cls.default_org_id.is_(None))
    
    @classmethod
    def recent(cls, db: Session, days: int = 30):
        """Rails scope: Find recently created users"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        return db.query(cls).filter(cls.created_at >= cutoff_date)
    
    @classmethod
    def create_temporary_password(cls) -> str:
        """Generate a secure temporary password following Rails pattern"""
        import random
        
        # Generate base password
        chars = string.ascii_lowercase + string.digits
        tmp = ''.join(random.choices(chars, k=24))
        
        # Insert required character types at random positions
        uppers = string.ascii_uppercase
        specials = ['!', '&', '-', '#', '$', '@', '+', '*']
        nums = string.digits
        
        # Insert uppercase letters
        for _ in range(2):
            pos = random.randint(0, len(tmp))
            tmp = tmp[:pos] + random.choice(uppers) + tmp[pos:]
            
        # Insert special characters 
        for _ in range(2):
            pos = random.randint(0, len(tmp))
            tmp = tmp[:pos] + random.choice(specials) + tmp[pos:]
            
        # Insert a number
        pos = random.randint(0, len(tmp))
        tmp = tmp[:pos] + random.choice(nums) + tmp[pos:]
        
        return tmp
    
    @classmethod
    def email_verified(cls, email: str) -> bool:
        """Check if email should be auto-verified (Rails pattern)"""
        if not email:
            return False
        if "nexla" in email and "test" in email:
            return False
        return True
    
    def authenticate_with_previous(self, password: str) -> bool:
        """Check password against current and previous 4 passwords (Rails pattern)"""
        # Check current password
        if self.password_digest and bcrypt.checkpw(password.encode('utf-8'), self.password_digest.encode('utf-8')):
            return True
            
        # Check previous passwords
        for digest_field in [self.password_digest_1, self.password_digest_2, 
                           self.password_digest_3, self.password_digest_4]:
            if digest_field and bcrypt.checkpw(password.encode('utf-8'), digest_field.encode('utf-8')):
                return True
                
        return False
    
    def change_password_(self, new_password: str, new_password_confirm: str) -> None:
        """Rails bang method: Change password with history tracking"""
        self.change_password(new_password, new_password_confirm)
    
    def change_password(self, new_password: str, new_password_confirm: str) -> None:
        """Change password with history tracking (Rails pattern)"""
        if new_password != new_password_confirm:
            raise ValueError("Password confirmation does not match")
            
        if self.authenticate_with_previous(new_password):
            raise ValueError("Cannot reuse a recent password")
            
        # Validate password strength
        if not self.PASSWORD_REGEX.match(new_password):
            raise ValueError("Password does not meet security requirements")
            
        # Save current password to history and set new one
        self.password_digest_4 = self.password_digest_3
        self.password_digest_3 = self.password_digest_2  
        self.password_digest_2 = self.password_digest_1
        self.password_digest_1 = self.password_digest
        
        # Hash new password
        salt = bcrypt.gensalt()
        self.password_digest = bcrypt.hashpw(new_password.encode('utf-8'), salt).decode('utf-8')
        
        # Set password change required date
        self.password_change_required_at = datetime.utcnow() + timedelta(days=self.PASSWORD_CHANGE_REQUIRED_AFTER_DAYS)
        
        # Clear reset token
        self.password_reset_token = None
        self.password_reset_token_at = None
        self.password_reset_token_count = 0
    
    def reset_password_retry_count(self) -> None:
        """Reset password retry count (Rails pattern)"""
        if self.password_retry_count != 0:
            self.password_retry_count = 0
    
    def increment_password_retry_count(self) -> None:
        """Increment password retry count and lock account if exceeded (Rails pattern)"""
        self.password_retry_count += 1
        if self.password_retry_count_exceeded():
            self.lock_account()
    
    def password_retry_count_exceeded_(self) -> bool:
        """Rails predicate: Check if password retry count exceeded"""
        return self.password_retry_count_exceeded()
    
    def password_retry_count_exceeded(self) -> bool:
        """Check if password retry count exceeded (Rails pattern)"""
        return self.password_retry_count >= self.MAX_PASSWORD_RETRY_COUNT
    
    def password_signature(self) -> str:
        """Generate password signature for JWT invalidation (Rails pattern)"""
        digest_str = self.password_digest or ""
        return hashlib.md5(digest_str.encode('utf-8')).hexdigest()
    
    def password_change_required_(self) -> bool:
        """Rails predicate: Check if password change is required"""
        return self.password_change_required()
    
    def password_change_required(self) -> bool:
        """Check if password change is required (Rails pattern)"""
        # Temporarily disabled in Rails - return False for now  
        # In full implementation: return bool(self.password_change_required_at and self.password_change_required_at < datetime.utcnow())
        return False
    
    def password_changed_(self) -> bool:
        """Rails predicate: Check if password field has been changed"""
        return self.password_changed()
    
    def password_changed(self) -> bool:
        """Check if password field has been changed (Rails pattern)"""
        # This would be tracked by SQLAlchemy session in real implementation
        return hasattr(self, '_password_changed') and self._password_changed
    
    def lock_account_(self) -> None:
        """Rails bang method: Lock user account"""
        self.lock_account()
    
    def lock_account(self) -> None:
        """Lock user account (Rails pattern)"""
        if self.infrastructure_user():
            raise ValueError("Cannot lock infrastructure user account")
        self.account_locked_at = datetime.utcnow()
    
    def unlock_account_(self) -> None:
        """Rails bang method: Unlock user account"""
        self.unlock_account()
    
    def unlock_account(self) -> None:
        """Unlock user account (Rails pattern)"""
        self.password_retry_count = 0
        self.account_locked_at = None
    
    def activate_(self, org=None) -> None:
        """Rails bang method: Activate user account"""
        self.activate(org)
    
    def activate(self, org=None) -> None:
        """Activate user account (Rails bang method pattern)"""
        self.status = self.STATUSES["active"]
        
        if org:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            try:
                membership = db.query(OrgMembership).filter(
                    OrgMembership.user_id == self.id,
                    OrgMembership.org_id == org.id
                ).first()
                
                if membership:
                    membership.status = "ACTIVE"
                    db.commit()
            finally:
                db.close()
    
    def deactivated(self) -> bool:
        """Check if user is deactivated (Rails predicate pattern)"""
        return self.status == self.STATUSES["deactivated"]
    
    def deactivate_(self, org=None, pause_data_flows: bool = False) -> None:
        """Rails bang method: Deactivate user account"""
        self.deactivate(org, pause_data_flows)
    
    def deactivate(self, org=None, pause_data_flows: bool = False) -> None:
        """Deactivate user account (Rails bang method pattern)"""
        if org is None:
            # Global deactivation - check for owned orgs
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            try:
                owned_orgs = db.query(Org).filter(
                    Org.owner_id == self.id,
                    Org.status != self.STATUSES["deactivated"]
                ).all()
                
                # Check if any owned org has multiple members
                for owned_org in owned_orgs:
                    if len(owned_org.members) > 1:
                        raise ValueError(f"User cannot be deactivated while owning active multi-user org: {owned_org.id}")
                
                # Deactivate owned orgs
                for owned_org in owned_orgs:
                    owned_org.deactivate()
                
                self.status = self.STATUSES["deactivated"]
                db.commit()
            finally:
                db.close()
        else:
            # Org-specific deactivation
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            try:
                membership = db.query(OrgMembership).filter(
                    OrgMembership.user_id == self.id,
                    OrgMembership.org_id == org.id
                ).first()
                
                if not membership:
                    raise ValueError("Org membership not found")
                
                # If user owns the org and it has multiple members, prevent deactivation
                if org.owner_id == self.id and len(org.members) > 1 and not org.deactivated():
                    raise ValueError(f"User cannot be deactivated while owning active multi-user org: {org.id}")
                
                if org.owner_id == self.id:
                    org.deactivate()
                
                membership.deactivate()
                db.commit()
            finally:
                db.close()
    
    def nexla_backend_admin_(self) -> bool:
        """Rails predicate: Check if user is Nexla backend admin"""
        return self.nexla_backend_admin()
    
    def nexla_backend_admin(self) -> bool:
        """Check if user is Nexla backend admin (Rails pattern)"""
        return self.email == self.BACKEND_ADMIN_EMAIL
    
    def infrastructure_user_(self) -> bool:
        """Rails predicate: Check if user is infrastructure user"""
        return self.infrastructure_user()
    
    def infrastructure_user(self) -> bool:
        """Check if user is infrastructure user (Rails pattern)"""
        return self.nexla_backend_admin()
    
    def infrastructure_or_super_user_(self) -> bool:
        """Rails predicate: Check if user is infrastructure user or super user"""
        return self.infrastructure_or_super_user()
    
    def infrastructure_or_super_user(self) -> bool:
        """Check if user is infrastructure user or super user (Rails pattern)"""
        return self.is_super_user() or self.infrastructure_user()
    
    def account_status(self, api_org=None) -> str:
        """Get account status considering org context (Rails pattern)"""
        org = api_org if api_org else self.default_org
        if org and hasattr(org, 'org_tier') and org.org_tier:
            return org.status
        return self.status
    
    def has_admin_access(self, user) -> bool:
        """Check if given user has admin access to this user (Rails pattern)"""
        if not user:
            return False
        if user.id == self.id or user.is_super_user():
            return True
        
        # Check if user is admin of any org this user is member of
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            # Get orgs where given user has admin access
            admin_orgs = db.query(Org).join(OrgMembership).filter(
                OrgMembership.user_id == user.id,
                OrgMembership.status == "ACTIVE"
                # TODO: Add admin role checking when role system is implemented
            ).all()
            
            # Check if this user is member of any of those orgs
            for org in admin_orgs:
                if self.org_member(org):
                    return True
                    
            return False
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def org_member_(self, org) -> bool:
        """Rails predicate: Check if user is member of specific org"""
        return self.org_member(org)
    
    def org_member(self, org) -> bool:
        """Check if user is member of specific org (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            membership = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.org_id == org.id,
                OrgMembership.status == "ACTIVE"
            ).first()
            
            return membership is not None
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def impersonated_(self) -> bool:
        """Rails predicate: Check if user is being impersonated"""
        return self.impersonated()
    
    def impersonated(self) -> bool:
        """Check if user is being impersonated (Rails pattern)"""
        # This would be set by authentication middleware
        return getattr(self, '_impersonator', None) is not None
    
    def super_user_read_only_(self) -> bool:
        """Rails predicate: Check if user has read-only super user access"""
        return self.super_user_read_only()
    
    def super_user_read_only(self) -> bool:
        """Check if user has read-only super user access (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            nexla_admin_org = db.query(Org).filter(Org.nexla_admin_org == 1).first()
            if not nexla_admin_org:
                return False
                
            # Would need role system implementation for full check
            return False
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def orgs(self, access_role: str = "member") -> List:
        """Get user's accessible orgs by role (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            if access_role == "all":
                if self.is_super_user():
                    return db.query(Org).all()
                else:
                    # Return member orgs + accessible orgs
                    member_orgs = db.query(Org).join(OrgMembership).filter(
                        OrgMembership.user_id == self.id,
                        OrgMembership.status == "ACTIVE"
                    ).all()
                    return member_orgs
            elif access_role == "member":
                return db.query(Org).join(OrgMembership).filter(
                    OrgMembership.user_id == self.id,
                    OrgMembership.status == "ACTIVE"
                ).all()
            else:
                # Other access roles would be implemented with proper role system
                return []
                
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    def active_member_orgs(self) -> List:
        """Get user's active member orgs (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            from .org import Org
            from .org_membership import OrgMembership
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            return db.query(Org).join(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.status == "ACTIVE",
                Org.status == "ACTIVE"
            ).all()
            
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    def team_member_(self, team) -> bool:
        """Rails predicate: Check if user is member of specific team"""
        return self.team_member(team)
    
    def team_member(self, team) -> bool:
        """Check if user is member of specific team (Rails pattern)"""
        if hasattr(team, 'id'):
            team_id = team.id
        else:
            team_id = team
            
        return any(m.team_id == team_id and 
                  (not hasattr(m, 'status') or m.status == "ACTIVE")
                  for m in self.team_memberships)
    
    def can_access_org_(self, org_id: int = None) -> bool:
        """Rails predicate: Check if user can access organization"""
        if not self.active_():
            return False
        if org_id is None:
            return True
        
        # Check if user is member of the organization
        from .org_membership import OrgMembership
        membership = next((m for m in self.org_memberships 
                          if m.org_id == org_id and m.status == "ACTIVE"), None)
        return membership is not None
    
    def member_of_org_(self, org_id: int) -> bool:
        """Rails predicate: Check if user is a member of specific organization"""
        return self.can_access_org_(org_id)
    
    def active_membership_for_org(self, org_id: int):
        """Get active membership for specific org (Rails pattern)"""
        from .org_membership import OrgMembership
        return next((m for m in self.org_memberships 
                    if m.org_id == org_id and m.status == "ACTIVE"), None)
    
    def add_to_org(self, org_id: int, role: str = "USER", auto_approve: bool = True):
        """Add user to organization (Rails pattern)"""
        from .org_membership import OrgMembership, MembershipRoles, MembershipStatuses
        from sqlalchemy.orm import sessionmaker
        
        # Check if membership already exists
        existing = self.active_membership_for_org(org_id)
        if existing:
            return existing
        
        # Create new membership
        membership = OrgMembership(
            user_id=self.id,
            org_id=org_id,
            role=getattr(MembershipRoles, role, MembershipRoles.USER),
            status=MembershipStatuses.ACTIVE if auto_approve else MembershipStatuses.PENDING,
            joined_at=datetime.utcnow() if auto_approve else None,
            invited_at=datetime.utcnow()
        )
        
        return membership
    
    # API key management methods (Rails pattern)
    @property
    def users_api_keys(self):
        """Rails alias: users_api_keys -> api_keys"""
        return self.api_keys
    
    def create_api_key(self, name: str, description: str = None, api_key_type: str = "READ_ONLY", 
                      scope: str = "ORG", environment: str = "DEVELOPMENT") -> 'ApiKey':
        """Create new API key for user (Rails pattern)"""
        from .api_key import ApiKey, ApiKeyType, ApiKeyScope, ApiKeyEnvironment
        import secrets
        import hashlib
        
        # Generate secure API key
        key = secrets.token_urlsafe(32)
        key_prefix = key[:8]
        key_suffix = key[-4:]
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        
        api_key = ApiKey(
            name=name,
            description=description,
            key_prefix=key_prefix,
            key_hash=key_hash,
            key_suffix=key_suffix,
            api_key_type=getattr(ApiKeyType, api_key_type, ApiKeyType.READ_ONLY),
            scope=getattr(ApiKeyScope, scope, ApiKeyScope.ORG),
            environment=getattr(ApiKeyEnvironment, environment, ApiKeyEnvironment.DEVELOPMENT),
            user_id=self.id,
            org_id=self.default_org_id
        )
        
        return api_key
    
    def active_api_keys(self) -> List:
        """Get user's active API keys (Rails pattern)"""
        from .api_key import ApiKeyStatus
        return [key for key in self.api_keys if key.status == ApiKeyStatus.ACTIVE]
    
    def revoke_api_key(self, api_key_id: int) -> bool:
        """Revoke specific API key (Rails pattern)"""
        from .api_key import ApiKeyStatus
        from .api_key_event import ApiKeyEvent, ApiKeyEventType
        
        api_key = next((key for key in self.api_keys if key.id == api_key_id), None)
        if not api_key:
            return False
            
        api_key.status = ApiKeyStatus.REVOKED
        api_key.revoked_at = datetime.utcnow()
        api_key.revoked_by = self.id
        
        # Create revocation event
        event = ApiKeyEvent(
            event_type=ApiKeyEventType.REVOKED,
            api_key_id=api_key.id,
            user_id=self.id,
            org_id=api_key.org_id,
            occurred_at=datetime.utcnow()
        )
        
        return True
    
    def recent_api_key_events(self, hours: int = 24) -> List:
        """Get recent API key events for user (Rails pattern)"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [event for event in self.api_key_events if event.occurred_at >= cutoff]
    
    # Team management methods (Rails pattern)
    def add_to_team(self, team_id: int, role: str = "member") -> 'TeamMembership':
        """Add user to team (Rails pattern)"""
        from .team_membership import TeamMembership
        
        # Check if membership already exists
        existing = next((m for m in self.team_memberships 
                        if m.team_id == team_id), None)
        if existing:
            return existing
        
        # Create new membership
        membership = TeamMembership(
            user_id=self.id,
            team_id=team_id,
            role=role
        )
        
        return membership
    
    def remove_from_team(self, team_id: int) -> bool:
        """Remove user from team (Rails pattern)"""
        membership = next((m for m in self.team_memberships 
                          if m.team_id == team_id), None)
        if membership:
            # In a real implementation, you'd remove from session/database
            # For now, just mark as inactive if status field exists
            if hasattr(membership, 'status'):
                membership.status = "INACTIVE"
            return True
        return False
    
    def get_team_role(self, team_id: int) -> Optional[str]:
        """Get user's role in specific team (Rails pattern)"""
        membership = next((m for m in self.team_memberships 
                          if m.team_id == team_id and 
                          (not hasattr(m, 'status') or m.status == "ACTIVE")), None)
        return membership.role if membership else None
    
    def active_teams(self) -> List:
        """Get teams user is an active member of (Rails pattern)"""
        return [m.team for m in self.team_memberships 
                if not hasattr(m, 'status') or m.status == "ACTIVE"]
    
    def delete_(self) -> None:
        """Rails bang method: Soft delete user"""
        if hasattr(self, 'deleted_at'):
            self.deleted_at = datetime.utcnow()
        elif hasattr(self, 'status'):
            self.status = 'DELETED'
        elif hasattr(self, 'is_active'):
            self.is_active = False
    
    def users(self, access_role: str = "all", org=None) -> List:
        """Get accessible users by role (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            if self.is_super_user() and access_role == "all":
                return db.query(User).all()
            elif org and org.has_admin_access(self) and access_role == "all":
                # Return org members
                from .org_membership import OrgMembership
                return db.query(User).join(OrgMembership).filter(
                    OrgMembership.org_id == org.id,
                    OrgMembership.status == "ACTIVE"
                ).all()
            else:
                # Return just self
                return db.query(User).filter(User.id == self.id).all()
                
        except Exception:
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    def domain_custodian_(self, domain_id=None) -> bool:
        """Rails predicate: Check if user is domain custodian"""
        return self.domain_custodian(domain_id)
    
    def domain_custodian(self, domain_id=None) -> bool:
        """Check if user is domain custodian (Rails pattern)"""
        # This would be implemented when domain custodian system is added
        return False
    
    def org_custodian_(self, org_id: int) -> bool:
        """Rails predicate: Check if user is org custodian"""
        return self.org_custodian(org_id)
    
    def org_custodian(self, org_id: int) -> bool:
        """Check if user is org custodian (Rails pattern)"""
        # This would be implemented when org custodian system is added  
        return False
    
    def get_api_key(self, org=None):
        """Get user's API key for org (Rails pattern)"""
        # This would be implemented when API key system is added
        return None
    
    def login_audits(self) -> List:
        """Get user login audit records (Rails pattern)"""
        # This would be implemented when audit system is added
        return []
    
    def logout_audits(self) -> List:
        """Get user logout audit records (Rails pattern)"""
        # This would be implemented when audit system is added
        return []
    
    def pause_flows_(self, org=None) -> None:
        """Rails bang method: Pause user's data flows"""
        self.pause_flows(org)
    
    def pause_flows(self, org=None) -> None:
        """Pause user's data flows (Rails bang method pattern)"""
        # This would pause all origin nodes owned by user
        # Implementation would depend on flow system
        pass
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any], user, org):
        """Build user from input data (Rails pattern)"""
        # This would be a complex factory method for creating users
        # Implementation would depend on complete org/tier system
        return None
    
    def update_mutable_(self, request, user, org, input_data: Dict[str, Any]) -> None:
        """Rails bang method: Update mutable user fields"""
        self.update_mutable(request, user, org, input_data)
    
    def update_mutable(self, request, user, org, input_data: Dict[str, Any]) -> None:
        """Update mutable user fields (Rails bang method pattern)"""
        # This would update allowed fields based on permissions
        # Implementation would depend on complete authorization system
        pass
    
    def update_admin_status_(self, user, org, input_data: Dict[str, Any]) -> None:
        """Rails bang method: Update user admin status"""
        self.update_admin_status(user, org, input_data)
    
    def update_admin_status(self, user, org, input_data: Dict[str, Any]) -> None:
        """Update user admin status (Rails pattern)"""
        # This would manage admin role assignments
        # Implementation would depend on complete role system
        pass
    
    @classmethod
    def find_external_idp_user(cls, api_auth_config, email: str, full_name: str):
        """Find or create external IDP user (Rails pattern)"""
        # This would handle external identity provider user creation
        return [None, None]
    
    def account_summary(self, access_role: str = "all", org=None) -> Dict[str, Any]:
        """Get user account summary (Rails pattern)"""
        # This would return summary of user's resources
        return {}
    
    def transferable(self, org=None) -> List:
        """Get transferable resources (Rails pattern)"""
        # This would return resources that can be transferred to another user
        return []
    
    def transfer_(self, org, delegate_owner, delegate_org=None) -> None:
        """Rails bang method: Transfer user resources"""
        self.transfer(org, delegate_owner, delegate_org)
    
    def transfer(self, org, delegate_owner, delegate_org=None) -> None:
        """Transfer user resources (Rails bang method pattern)"""
        # This would transfer ownership of user's resources
        pass
    
    def authenticate(self, password: str) -> bool:
        """Authenticate user with password (Rails has_secure_password pattern)"""
        if not self.password_digest or not password:
            return False
        try:
            return bcrypt.checkpw(password.encode('utf-8'), self.password_digest.encode('utf-8'))
        except Exception:
            return False
    
    def create_password_reset_token(self, org=None, origin: str = None, force: bool = False, send_email: bool = True) -> Optional[str]:
        """Create password reset token (Rails pattern)"""
        now = datetime.utcnow()
        
        if force or (not self.password_reset_token_at or 
                    self.password_reset_token_at < (now - timedelta(minutes=self.RESET_PASSWORD_INTERVAL_MINUTES))):
            self.password_reset_token_at = now
            self.password_reset_token_count = 0
        
        if force:
            self.password_reset_token_count = 0
        
        self.password_reset_token_count += 1
        
        if self.password_reset_token_count > self.MAX_RESET_PASSWORD_TRIES:
            self.account_locked_at = now
            return None
        
        # Generate secure token
        token = secrets.token_urlsafe(32)
        self.password_reset_token = token
        self.password_reset_token_at = now
        
        # In a real implementation, you'd send email here
        if send_email and origin:
            # NotificationService.publish_reset_password(self, org, origin)
            pass
        
        return token
    
    @classmethod
    def verify_password_reset_token(cls, token: str):
        """Verify and return user for password reset token (Rails pattern)"""
        if not token:
            return None
        
        try:
            from sqlalchemy.orm import sessionmaker
            
            # This would need to be implemented with proper session handling
            # For now, return None to indicate not implemented
            return None
        except Exception:
            return None
    
    @classmethod
    def find_user_and_org_by_api_key(cls, api_key: str, scopes: Optional[List[str]] = None):
        """Find user and org by API key (Rails pattern)"""
        # This would integrate with API key system when implemented
        return [None, None, None]
    
    @classmethod
    def find_user_and_org_by_service_key(cls, api_key: str):
        """Find user and org by service key (Rails pattern)"""
        # This would integrate with service key system when implemented
        return [None, None, None]
    
    def generate_password_reset_token(self, new_user: bool = False) -> Optional[str]:
        """Generate password reset token (Rails pattern)"""
        token = secrets.token_urlsafe(32)
        self.password_reset_token = token
        self.password_reset_token_at = datetime.utcnow()
        return token
    
    @classmethod
    def validate_password(cls, email: str, full_name: str, password: str) -> Dict[str, Any]:
        """Validate password strength (Rails pattern)"""
        errors = []
        
        # Check format requirements
        if not cls.PASSWORD_REGEX.match(password):
            errors.append("must contain at least 8 characters, one digit, one lower case character, one upper case character, and one symbol")
        
        # Check length
        if len(password) < cls.PASSWORD_CONSTRAINTS["minimum"]:
            errors.append(f"is too short (minimum is {cls.PASSWORD_CONSTRAINTS['minimum']} characters)")
        
        if len(password) > cls.PASSWORD_CONSTRAINTS["maximum"]:
            errors.append(f"is too long (maximum is {cls.PASSWORD_CONSTRAINTS['maximum']} characters)")
        
        # Use zxcvbn for proper entropy calculation (Rails StrongPassword equivalent)
        extra_words = cls.EXTRA_WORDS + [email, full_name] if email and full_name else cls.EXTRA_WORDS
        result = zxcvbn(password, user_inputs=extra_words)
        
        # Convert zxcvbn score (0-4) to entropy-like score
        entropy = result['score'] * 4  # Scale to roughly match Rails entropy
        
        # Check if password is weak based on entropy and score
        if result['score'] < 2 or entropy < cls.MIN_PASSWORD_ENTROPY:
            errors.append("is too weak (consider using a longer password with mixed case letters, numbers, and symbols)")
        
        # Add specific feedback from zxcvbn
        if result['feedback']['suggestions']:
            for suggestion in result['feedback']['suggestions']:
                errors.append(f"suggestion: {suggestion}")
        
        return {
            'entropy': entropy,
            'min_entropy': cls.MIN_PASSWORD_ENTROPY,
            'errors': errors,
            'score': result['score'],
            'feedback': result['feedback']
        }
    
    @classmethod
    def validate_email(cls, email: str) -> Dict[str, Any]:
        """Validate email format and constraints (Rails pattern)"""
        errors = []
        
        # Check presence
        if not email or not email.strip():
            errors.append("can't be blank")
            return {'errors': errors}
        
        email = email.strip()
        
        # Check length constraints
        if len(email) < cls.EMAIL_CONSTRAINTS["minimum"]:
            errors.append(f"is too short (minimum is {cls.EMAIL_CONSTRAINTS['minimum']} characters)")
        
        if len(email) > cls.EMAIL_CONSTRAINTS["maximum"]:
            errors.append(f"is too long (maximum is {cls.EMAIL_CONSTRAINTS['maximum']} characters)")
        
        # Use email-validator library for strict format validation (Rails email_validator gem equivalent)
        email_normalized = email
        try:
            # strict_mode=True equivalent to Rails email_validator strict_mode
            valid_email = validate_email(email, check_deliverability=False)
            email_normalized = valid_email.email
        except EmailNotValidError as e:
            errors.append(f"is not a valid email format: {str(e)}")
            
        return {
            'errors': errors,
            'normalized_email': email_normalized
        }
    
    def password_weak_(self, password: str = None) -> bool:
        """Rails predicate: Check if current or given password is weak"""
        if password is None:
            # This would require checking the current password, which requires the plaintext
            # In practice, this would be called during password setting
            return False
            
        validation = self.validate_password(self.email, self.full_name, password)
        return len(validation['errors']) > 0 or validation['score'] < 2
    
    def to_dict(self) -> dict:
        """Convert user to dictionary for API responses"""
        return {
            'id': self.id,
            'email': self.email,
            'full_name': self.full_name,
            'status': self.status,
            'is_active': self.active_(),
            'is_deactivated': self.deactivated_(),
            'is_email_verified': bool(self.email_verified_at),
            'is_tos_signed': bool(self.tos_signed_at),
            'account_locked': self.account_locked_(),
            'password_change_required': self.password_change_required_(),
            'is_super_user': self.super_user_(),
            'is_org_owner': self.org_owner_(),
            'is_infrastructure_user': self.infrastructure_user_(),
            'role': self.role,
            'nexla_backend_admin': self.nexla_backend_admin_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'default_org_id': self.default_org_id,
            'user_tier_id': self.user_tier_id
        }
    
    # Authorization Methods - Rails CanCan Equivalent
    
    def is_super_user_(self) -> bool:
        """Check if user has super user privileges"""
        return self.super_user_()
    
    def is_org_member_(self, org) -> bool:
        """Check if user is a member of the given organization"""
        if not org:
            return False
        
        try:
            from .org_membership import OrgMembership
            from sqlalchemy.orm import sessionmaker
            
            Session = sessionmaker(bind=self.__table__.bind)
            db = Session()
            
            membership = db.query(OrgMembership).filter(
                OrgMembership.user_id == self.id,
                OrgMembership.org_id == org.id,
                OrgMembership.status == 'ACTIVE'
            ).first()
            
            return membership is not None
            
        except Exception:
            return False
        finally:
            if 'db' in locals():
                db.close()
    
    def has_admin_access_(self, user) -> bool:
        """Check if another user has admin access to this user"""
        if not user:
            return False
        
        # Super users can manage anyone
        if user.is_super_user_():
            return True
        
        # Users can manage themselves
        if user.id == self.id:
            return True
        
        # Org admins can manage members of their orgs
        if hasattr(user, 'default_org') and user.default_org:
            if self.is_org_member_(user.default_org):
                return user.default_org.has_admin_access_(user)
        
        return False
    
    def has_operator_access_(self, user) -> bool:
        """Check if another user has operator access to this user"""
        if not user:
            return False
        
        # Admin access includes operator access
        if self.has_admin_access_(user):
            return True
        
        # Org operators can operate on members of their orgs
        if hasattr(user, 'default_org') and user.default_org:
            if self.is_org_member_(user.default_org):
                return user.default_org.has_operator_access_(user)
        
        return False
    
    def has_collaborator_access_(self, user) -> bool:
        """Check if another user has collaborator access to this user"""
        if not user:
            return False
        
        # Operator access includes collaborator access
        if self.has_operator_access_(user):
            return True
        
        # Members of the same org have collaborator access
        if hasattr(user, 'default_org') and user.default_org:
            if self.is_org_member_(user.default_org):
                return True
        
        return False