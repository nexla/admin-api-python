"""
Org Model - Enhanced with comprehensive Rails business logic patterns.
Core organization management with Rails-style patterns for enterprise functionality.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum as PyEnum
import json
import secrets
import re
import logging
from ..database import Base

logger = logging.getLogger(__name__)


class OrgStatuses(PyEnum):
    """Organization status enumeration with Rails-style constants"""
    ACTIVE = "ACTIVE"
    DEACTIVATED = "DEACTIVATED"
    SUSPENDED = "SUSPENDED"
    TRIAL = "TRIAL"
    TRIAL_EXPIRED = "TRIAL_EXPIRED"
    SOURCE_COUNT_CAPPED = "SOURCE_COUNT_CAPPED"
    SOURCE_DATA_CAPPED = "SOURCE_DATA_CAPPED"
    MIGRATING = "MIGRATING"
    ARCHIVED = "ARCHIVED"
    PENDING_ACTIVATION = "PENDING_ACTIVATION"
    BILLING_SUSPENDED = "BILLING_SUSPENDED"
    COMPLIANCE_SUSPENDED = "COMPLIANCE_SUSPENDED"

    @property
    def display_name(self) -> str:
        """Human readable status name"""
        return self.value.replace('_', ' ').title()


class OrgTypes(PyEnum):
    """Organization type enumeration"""
    ENTERPRISE = "ENTERPRISE"
    TEAM = "TEAM"
    INDIVIDUAL = "INDIVIDUAL"
    TRIAL = "TRIAL"
    PARTNER = "PARTNER"
    INTERNAL = "INTERNAL"
    NEXLA_ADMIN = "NEXLA_ADMIN"
    SANDBOX = "SANDBOX"

    @property
    def display_name(self) -> str:
        """Human readable type name"""
        return self.value.replace('_', ' ').title()


class ClusterStatuses(PyEnum):
    """Cluster status enumeration"""
    ACTIVE = "ACTIVE"
    MIGRATING = "MIGRATING"
    INACTIVE = "INACTIVE"
    MAINTENANCE = "MAINTENANCE"
    FAILED = "FAILED"


class BillingStatuses(PyEnum):
    """Billing status enumeration"""
    CURRENT = "CURRENT"
    PAST_DUE = "PAST_DUE"
    SUSPENDED = "SUSPENDED"
    CANCELLED = "CANCELLED"
    TRIAL = "TRIAL"
    FREE = "FREE"


class Org(Base):
    __tablename__ = "orgs"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    status = Column(SQLEnum(OrgStatuses), default=OrgStatuses.ACTIVE, nullable=False, index=True)
    org_type = Column(SQLEnum(OrgTypes), default=OrgTypes.TEAM, nullable=False)
    
    # Organization settings
    allow_api_key_access = Column(Boolean, default=True)
    search_index_name = Column(String(255))
    client_identifier = Column(String(255), unique=True, index=True)
    nexla_admin_org = Column(Boolean, default=False)  # Rails nexla_admin_org field
    
    # Trial and billing
    trial_expires_at = Column(DateTime)
    billing_status = Column(SQLEnum(BillingStatuses), default=BillingStatuses.FREE)
    billing_suspended_at = Column(DateTime)
    billing_suspension_reason = Column(Text)
    
    # Lifecycle timestamps
    activated_at = Column(DateTime)
    deactivated_at = Column(DateTime)
    suspended_at = Column(DateTime)
    suspension_reason = Column(Text)
    archived_at = Column(DateTime)
    archive_reason = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    last_activity_at = Column(DateTime)
    
    # Capacity and usage
    max_users = Column(Integer, default=10)
    max_data_sources = Column(Integer, default=100)
    max_flows = Column(Integer, default=500)
    current_users = Column(Integer, default=0)
    current_data_sources = Column(Integer, default=0)
    current_flows = Column(Integer, default=0)
    
    # Data usage limits
    monthly_data_limit_gb = Column(Float)
    current_month_usage_gb = Column(Float, default=0.0)
    total_data_processed_gb = Column(Float, default=0.0)
    
    # Feature flags and settings
    feature_flags = Column(JSON)  # JSON object for feature toggles
    settings = Column(JSON)  # JSON object for org-specific settings
    extra_metadata = Column(JSON)  # JSON object for additional data
    
    # Security and compliance
    enforce_sso = Column(Boolean, default=False)
    require_mfa = Column(Boolean, default=False)
    data_retention_days = Column(Integer, default=90)
    audit_logging_enabled = Column(Boolean, default=True)
    
    # Domain and branding
    domain = Column(String(255))
    logo_url = Column(String(500))
    primary_color = Column(String(7))  # Hex color code
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_tier_id = Column(Integer, ForeignKey("org_tiers.id"))
    cluster_id = Column(Integer, ForeignKey("clusters.id"))
    billing_owner_id = Column(Integer, ForeignKey("users.id"))
    new_cluster_id = Column(Integer, ForeignKey("clusters.id"))
    parent_org_id = Column(Integer, ForeignKey("orgs.id"))
    archived_by_id = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    users = relationship("User", foreign_keys="User.default_org_id", back_populates="default_org")
    org_tier = relationship("OrgTier")
    cluster = relationship("Cluster", foreign_keys=[cluster_id])
    billing_owner = relationship("User", foreign_keys=[billing_owner_id])
    new_cluster = relationship("Cluster", foreign_keys=[new_cluster_id])
    parent_org = relationship("Org", remote_side=[id], foreign_keys=[parent_org_id])
    child_orgs = relationship("Org", remote_side=[parent_org_id])
    archived_by = relationship("User", foreign_keys=[archived_by_id])
    
    org_memberships = relationship("OrgMembership", back_populates="org")
    data_sources = relationship("DataSource", back_populates="org")
    data_sets = relationship("DataSet", back_populates="org")
    data_sinks = relationship("DataSink", back_populates="org")
    flows = relationship("Flow", back_populates="org")
    projects = relationship("Project", back_populates="org")
    members = relationship("User", secondary="org_memberships", viewonly=True)
    
    # Rails business logic constants
    STATUSES = {
        "active": OrgStatuses.ACTIVE.value,
        "deactivated": OrgStatuses.DEACTIVATED.value,
        "suspended": OrgStatuses.SUSPENDED.value,
        "trial": OrgStatuses.TRIAL.value,
        "trial_expired": OrgStatuses.TRIAL_EXPIRED.value,
        "source_count_capped": OrgStatuses.SOURCE_COUNT_CAPPED.value,
        "source_data_capped": OrgStatuses.SOURCE_DATA_CAPPED.value,
        "migrating": OrgStatuses.MIGRATING.value,
        "archived": OrgStatuses.ARCHIVED.value
    }
    
    CLUSTER_STATUS = {
        "active": ClusterStatuses.ACTIVE.value,
        "migrating": ClusterStatuses.MIGRATING.value,
        "inactive": ClusterStatuses.INACTIVE.value,
        "maintenance": ClusterStatuses.MAINTENANCE.value,
        "failed": ClusterStatuses.FAILED.value
    }
    
    NEXLA_ADMIN_EMAIL_DOMAIN = "nexla.com"
    NEXLA_ADMIN_ORG_BIT = b"\\x01"
    
    DEFAULT_TRIAL_DAYS = 30
    DEFAULT_DATA_RETENTION_DAYS = 90
    MAX_USERS_FREE_TIER = 5
    MAX_DATA_SOURCES_FREE_TIER = 10
    USAGE_WARNING_THRESHOLD_PERCENT = 80
    USAGE_CRITICAL_THRESHOLD_PERCENT = 95

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self._cache = {}
        self._usage_cache = {}
        self._member_cache = {}

    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if org is active (Rails pattern)"""
        return (self.status == OrgStatuses.ACTIVE and
                not self.suspended_() and
                not self.billing_suspended_() and
                not self.trial_expired_())
    
    def deactivated_(self) -> bool:
        """Check if org is deactivated (Rails pattern)"""
        return self.status == OrgStatuses.DEACTIVATED
    
    def suspended_(self) -> bool:
        """Check if org is suspended (Rails pattern)"""
        return self.status == OrgStatuses.SUSPENDED or self.suspended_at is not None
    
    def trial_(self) -> bool:
        """Check if org is on trial (Rails pattern)"""
        return self.status == OrgStatuses.TRIAL or self.org_type == OrgTypes.TRIAL
    
    def trial_expired_(self) -> bool:
        """Check if org trial has expired (Rails pattern)"""
        return (self.status == OrgStatuses.TRIAL_EXPIRED or
                (self.trial_expires_at and self.trial_expires_at < datetime.now()))
    
    def source_count_capped_(self) -> bool:
        """Check if org is source count capped (Rails pattern)"""
        return self.status == OrgStatuses.SOURCE_COUNT_CAPPED
    
    def source_data_capped_(self) -> bool:
        """Check if org is source data capped (Rails pattern)"""
        return self.status == OrgStatuses.SOURCE_DATA_CAPPED
    
    def migrating_(self) -> bool:
        """Check if org is migrating (Rails pattern)"""
        return self.status == OrgStatuses.MIGRATING
    
    def archived_(self) -> bool:
        """Check if org is archived (Rails pattern)"""
        return self.status == OrgStatuses.ARCHIVED
    
    def billing_suspended_(self) -> bool:
        """Check if org is billing suspended (Rails pattern)"""
        return (self.status == OrgStatuses.BILLING_SUSPENDED or
                self.billing_status == BillingStatuses.SUSPENDED or
                self.billing_suspended_at is not None)
    
    def compliance_suspended_(self) -> bool:
        """Check if org is compliance suspended (Rails pattern)"""
        return self.status == OrgStatuses.COMPLIANCE_SUSPENDED
    
    def pending_activation_(self) -> bool:
        """Check if org is pending activation (Rails pattern)"""
        return self.status == OrgStatuses.PENDING_ACTIVATION
    
    def nexla_admin_org_(self) -> bool:
        """Check if this is the Nexla admin org (Rails pattern)"""
        return bool(self.nexla_admin_org)
    
    def enterprise_(self) -> bool:
        """Check if org is enterprise type (Rails pattern)"""
        return self.org_type == OrgTypes.ENTERPRISE
    
    def team_(self) -> bool:
        """Check if org is team type (Rails pattern)"""
        return self.org_type == OrgTypes.TEAM
    
    def individual_(self) -> bool:
        """Check if org is individual type (Rails pattern)"""
        return self.org_type == OrgTypes.INDIVIDUAL
    
    def partner_(self) -> bool:
        """Check if org is partner type (Rails pattern)"""
        return self.org_type == OrgTypes.PARTNER
    
    def internal_(self) -> bool:
        """Check if org is internal type (Rails pattern)"""
        return self.org_type == OrgTypes.INTERNAL
    
    def sandbox_(self) -> bool:
        """Check if org is sandbox type (Rails pattern)"""
        return self.org_type == OrgTypes.SANDBOX
    
    def has_owner_(self) -> bool:
        """Check if org has an owner (Rails pattern)"""
        return self.owner_id is not None
    
    def has_billing_owner_(self) -> bool:
        """Check if org has billing owner (Rails pattern)"""
        return self.billing_owner_id is not None
    
    def has_parent_(self) -> bool:
        """Check if org has parent organization (Rails pattern)"""
        return self.parent_org_id is not None
    
    def has_children_(self) -> bool:
        """Check if org has child organizations (Rails pattern)"""
        return len(self.child_orgs or []) > 0
    
    def multi_user_(self) -> bool:
        """Check if org has multiple users (Rails pattern)"""
        return self.current_users > 1
    
    def single_user_(self) -> bool:
        """Check if org has only one user (Rails pattern)"""
        return self.current_users == 1
    
    def empty_(self) -> bool:
        """Check if org has no users (Rails pattern)"""
        return self.current_users == 0
    
    def at_user_limit_(self) -> bool:
        """Check if org is at user limit (Rails pattern)"""
        return self.max_users and self.current_users >= self.max_users
    
    def at_data_source_limit_(self) -> bool:
        """Check if org is at data source limit (Rails pattern)"""
        return self.max_data_sources and self.current_data_sources >= self.max_data_sources
    
    def at_flow_limit_(self) -> bool:
        """Check if org is at flow limit (Rails pattern)"""
        return self.max_flows and self.current_flows >= self.max_flows
    
    def over_data_limit_(self) -> bool:
        """Check if org is over monthly data limit (Rails pattern)"""
        if not self.monthly_data_limit_gb:
            return False
        return self.current_month_usage_gb >= self.monthly_data_limit_gb
    
    def near_data_limit_(self) -> bool:
        """Check if org is near monthly data limit (Rails pattern)"""
        if not self.monthly_data_limit_gb:
            return False
        usage_percent = (self.current_month_usage_gb / self.monthly_data_limit_gb) * 100
        return usage_percent >= self.USAGE_WARNING_THRESHOLD_PERCENT
    
    def critical_data_usage_(self) -> bool:
        """Check if org has critical data usage (Rails pattern)"""
        if not self.monthly_data_limit_gb:
            return False
        usage_percent = (self.current_month_usage_gb / self.monthly_data_limit_gb) * 100
        return usage_percent >= self.USAGE_CRITICAL_THRESHOLD_PERCENT
    
    def sso_enforced_(self) -> bool:
        """Check if SSO is enforced (Rails pattern)"""
        return self.enforce_sso is True
    
    def mfa_required_(self) -> bool:
        """Check if MFA is required (Rails pattern)"""
        return self.require_mfa is True
    
    def audit_logging_enabled_(self) -> bool:
        """Check if audit logging is enabled (Rails pattern)"""
        return self.audit_logging_enabled is True
    
    def recently_active_(self, days: int = 7) -> bool:
        """Check if org was recently active (Rails pattern)"""
        if not self.last_activity_at:
            return False
        return self.last_activity_at >= datetime.now() - timedelta(days=days)
    
    def stale_(self, days: int = 30) -> bool:
        """Check if org is stale (Rails pattern)"""
        if not self.last_activity_at:
            return True
        return self.last_activity_at < datetime.now() - timedelta(days=days)
    
    def can_be_activated_(self) -> bool:
        """Check if org can be activated (Rails pattern)"""
        return (not self.active_() and 
                self.has_owner_() and 
                not self.archived_())
    
    def can_be_deactivated_(self) -> bool:
        """Check if org can be deactivated (Rails pattern)"""
        return self.active_() or self.suspended_()
    
    def can_add_user_(self) -> bool:
        """Check if org can add another user (Rails pattern)"""
        return not self.at_user_limit_() and self.active_()
    
    def can_add_data_source_(self) -> bool:
        """Check if org can add another data source (Rails pattern)"""
        return not self.at_data_source_limit_() and self.active_()
    
    def can_process_data_(self) -> bool:
        """Check if org can process data (Rails pattern)"""
        return (self.active_() and 
                not self.over_data_limit_() and 
                not self.source_data_capped_())

    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def activate_(self) -> None:
        """Activate org (Rails bang method pattern)"""
        if self.active_():
            return
        
        # Validate owner can activate org
        if not self.owner:
            raise ValueError("Org cannot be activated without owner")
        
        if self.owner.deactivated_():
            raise ValueError(f"Org cannot be activated while owner's account is deactivated: {self.owner.id}")
        
        if self.owner.account_locked_():
            raise ValueError(f"Org cannot be activated while owner's account is locked: {self.owner.id}")
        
        if not self.owner.org_member_(self):
            raise ValueError(f"Org cannot be activated while owner's membership is deactivated: {self.owner.id}")
        
        self.status = OrgStatuses.ACTIVE
        self.activated_at = datetime.now()
        self.deactivated_at = None
        self.suspended_at = None
        self.suspension_reason = None
        self.updated_at = datetime.now()
        
        logger.info(f"Org activated: {self.name} (ID: {self.id})")
    
    def deactivate_(self, pause_flows: bool = False, reason: str = None) -> None:
        """Deactivate org (Rails bang method pattern)"""
        if self.deactivated_():
            return
        
        self.status = OrgStatuses.DEACTIVATED
        self.deactivated_at = datetime.now()
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('deactivation_reason', reason)
        
        # Pause flows if requested
        if pause_flows:
            self._pause_all_flows()
        
        logger.info(f"Org deactivated: {self.name} (ID: {self.id})")
    
    def suspend_(self, reason: str = None, suspended_by=None) -> None:
        """Suspend org (Rails bang method pattern)"""
        if self.suspended_():
            return
        
        self.status = OrgStatuses.SUSPENDED
        self.suspended_at = datetime.now()
        self.suspension_reason = reason
        self.updated_at = datetime.now()
        
        if suspended_by:
            self._update_metadata('suspended_by', suspended_by.id)
        
        # Auto-pause flows when suspended
        self._pause_all_flows()
        
        logger.warning(f"Org suspended: {self.name} (ID: {self.id}), reason: {reason}")
    
    def unsuspend_(self) -> None:
        """Remove suspension from org (Rails bang method pattern)"""
        if not self.suspended_():
            return
        
        self.status = OrgStatuses.ACTIVE
        self.suspended_at = None
        self.suspension_reason = None
        self.updated_at = datetime.now()
        
        logger.info(f"Org suspension removed: {self.name} (ID: {self.id})")
    
    def archive_(self, archived_by=None, reason: str = None) -> None:
        """Archive org (Rails bang method pattern)"""
        if self.archived_():
            return
        
        self.status = OrgStatuses.ARCHIVED
        self.archived_at = datetime.now()
        self.archived_by_id = archived_by.id if archived_by else None
        self.archive_reason = reason
        self.updated_at = datetime.now()
        
        # Deactivate all flows and data sources
        self._deactivate_all_resources()
        
        logger.info(f"Org archived: {self.name} (ID: {self.id})")
    
    def unarchive_(self) -> None:
        """Unarchive org (Rails bang method pattern)"""
        if not self.archived_():
            return
        
        self.status = OrgStatuses.ACTIVE
        self.archived_at = None
        self.archived_by_id = None
        self.archive_reason = None
        self.updated_at = datetime.now()
        
        logger.info(f"Org unarchived: {self.name} (ID: {self.id})")
    
    def start_trial_(self, trial_days: int = None) -> None:
        """Start trial period for org (Rails bang method pattern)"""
        if self.trial_():
            return
        
        days = trial_days or self.DEFAULT_TRIAL_DAYS
        self.status = OrgStatuses.TRIAL
        self.org_type = OrgTypes.TRIAL
        self.trial_expires_at = datetime.now() + timedelta(days=days)
        self.billing_status = BillingStatuses.TRIAL
        self.updated_at = datetime.now()
        
        logger.info(f"Trial started for org: {self.name} (ID: {self.id}), expires: {self.trial_expires_at}")
    
    def extend_trial_(self, additional_days: int = 30) -> None:
        """Extend trial period (Rails bang method pattern)"""
        if not self.trial_():
            raise ValueError("Cannot extend trial for non-trial org")
        
        current_expiry = self.trial_expires_at or datetime.now()
        self.trial_expires_at = current_expiry + timedelta(days=additional_days)
        self.updated_at = datetime.now()
        
        logger.info(f"Trial extended for org: {self.name} (ID: {self.id}) by {additional_days} days")
    
    def convert_from_trial_(self, new_org_type: OrgTypes = OrgTypes.TEAM) -> None:
        """Convert org from trial to paid (Rails bang method pattern)"""
        if not self.trial_():
            raise ValueError("Cannot convert non-trial org")
        
        self.status = OrgStatuses.ACTIVE
        self.org_type = new_org_type
        self.trial_expires_at = None
        self.billing_status = BillingStatuses.CURRENT
        self.updated_at = datetime.now()
        
        logger.info(f"Org converted from trial: {self.name} (ID: {self.id}) to {new_org_type.value}")
    
    def expire_trial_(self) -> None:
        """Expire trial period (Rails bang method pattern)"""
        if not self.trial_():
            return
        
        self.status = OrgStatuses.TRIAL_EXPIRED
        self.billing_status = BillingStatuses.SUSPENDED
        self.updated_at = datetime.now()
        
        # Pause all flows and data sources
        self._pause_all_flows()
        
        logger.warning(f"Trial expired for org: {self.name} (ID: {self.id})")
    
    def suspend_billing_(self, reason: str = None) -> None:
        """Suspend org for billing issues (Rails bang method pattern)"""
        self.status = OrgStatuses.BILLING_SUSPENDED
        self.billing_status = BillingStatuses.SUSPENDED
        self.billing_suspended_at = datetime.now()
        self.billing_suspension_reason = reason
        self.updated_at = datetime.now()
        
        # Pause flows when billing suspended
        self._pause_all_flows()
        
        logger.warning(f"Billing suspended for org: {self.name} (ID: {self.id}), reason: {reason}")
    
    def resume_billing_(self) -> None:
        """Resume billing for org (Rails bang method pattern)"""
        if not self.billing_suspended_():
            return
        
        self.status = OrgStatuses.ACTIVE
        self.billing_status = BillingStatuses.CURRENT
        self.billing_suspended_at = None
        self.billing_suspension_reason = None
        self.updated_at = datetime.now()
        
        logger.info(f"Billing resumed for org: {self.name} (ID: {self.id})")
    
    def cap_source_count_(self, reason: str = None) -> None:
        """Cap org source count (Rails bang method pattern)"""
        self.status = OrgStatuses.SOURCE_COUNT_CAPPED
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('source_count_cap_reason', reason)
        
        logger.warning(f"Source count capped for org: {self.name} (ID: {self.id})")
    
    def cap_source_data_(self, reason: str = None) -> None:
        """Cap org source data (Rails bang method pattern)"""
        self.status = OrgStatuses.SOURCE_DATA_CAPPED
        self.updated_at = datetime.now()
        
        if reason:
            self._update_metadata('source_data_cap_reason', reason)
        
        # Pause flows when data capped
        self._pause_all_flows()
        
        logger.warning(f"Source data capped for org: {self.name} (ID: {self.id})")
    
    def remove_caps_(self) -> None:
        """Remove all caps from org (Rails bang method pattern)"""
        if self.source_count_capped_() or self.source_data_capped_():
            self.status = OrgStatuses.ACTIVE
            self.updated_at = datetime.now()
            
            logger.info(f"Caps removed from org: {self.name} (ID: {self.id})")
    
    def start_migration_(self, new_cluster_id: int) -> None:
        """Start org migration to new cluster (Rails bang method pattern)"""
        self.status = OrgStatuses.MIGRATING
        self.new_cluster_id = new_cluster_id
        self.updated_at = datetime.now()
        
        logger.info(f"Migration started for org: {self.name} (ID: {self.id}) to cluster {new_cluster_id}")
    
    def complete_migration_(self) -> None:
        """Complete org migration (Rails bang method pattern)"""
        if not self.migrating_():
            return
        
        if self.new_cluster_id:
            self.cluster_id = self.new_cluster_id
            self.new_cluster_id = None
        
        self.status = OrgStatuses.ACTIVE
        self.updated_at = datetime.now()
        
        logger.info(f"Migration completed for org: {self.name} (ID: {self.id})")
    
    def update_activity_(self) -> None:
        """Update last activity timestamp (Rails bang method pattern)"""
        self.last_activity_at = datetime.now()
        # Don't update updated_at for activity tracking
    
    def increment_usage_(self, data_gb: float) -> None:
        """Increment data usage (Rails bang method pattern)"""
        self.current_month_usage_gb += data_gb
        self.total_data_processed_gb += data_gb
        self.last_activity_at = datetime.now()
        self.updated_at = datetime.now()
        
        # Check if approaching limits
        if self.near_data_limit_():
            logger.warning(f"Org {self.name} approaching data limit: {self.current_month_usage_gb}/{self.monthly_data_limit_gb} GB")
        
        if self.over_data_limit_():
            self.cap_source_data_("Monthly data limit exceeded")
    
    def reset_monthly_usage_(self) -> None:
        """Reset monthly data usage (Rails bang method pattern)"""
        self.current_month_usage_gb = 0.0
        self.updated_at = datetime.now()
        
        # Remove data cap if it was set due to usage
        if self.source_data_capped_():
            self.remove_caps_()
    
    def add_member_(self, user, role: str = "member") -> None:
        """Add member to org (Rails bang method pattern)"""
        if not self.can_add_user_():
            raise ValueError("Cannot add user: at capacity or org not active")
        
        from sqlalchemy.orm import sessionmaker
        from .org_membership import OrgMembership
        
        Session = sessionmaker(bind=self.__table__.bind)
        db = Session()
        try:
            # Check if membership already exists
            existing = db.query(OrgMembership).filter(
                OrgMembership.user_id == user.id,
                OrgMembership.org_id == self.id
            ).first()
            
            if existing:
                if existing.status == 'ACTIVE':
                    return  # Already active member
                else:
                    # Reactivate membership
                    existing.status = 'ACTIVE'
                    existing.role = role
            else:
                # Create new membership
                membership = OrgMembership(
                    user_id=user.id,
                    org_id=self.id,
                    role=role,
                    status='ACTIVE'
                )
                db.add(membership)
            
            self.current_users = (self.current_users or 0) + 1
            self.update_activity_()
            db.commit()
            
            logger.info(f"User {user.email} added to org {self.name}")
            
        finally:
            db.close()
    
    def remove_member_(self, user) -> None:
        """Remove member from org (Rails bang method pattern)"""
        if user.id == self.owner_id:
            raise ValueError("Cannot remove org owner")
        
        from sqlalchemy.orm import sessionmaker
        from .org_membership import OrgMembership
        
        Session = sessionmaker(bind=self.__table__.bind)
        db = Session()
        try:
            membership = db.query(OrgMembership).filter(
                OrgMembership.user_id == user.id,
                OrgMembership.org_id == self.id,
                OrgMembership.status == 'ACTIVE'
            ).first()
            
            if membership:
                membership.status = 'INACTIVE'
                self.current_users = max(0, (self.current_users or 1) - 1)
                self.update_activity_()
                db.commit()
                
                logger.info(f"User {user.email} removed from org {self.name}")
        finally:
            db.close()

    # ========================================
    # Rails Class Methods and Scopes
    # ========================================
    
    @classmethod
    def active(cls):
        """Scope for active orgs (Rails scope pattern)"""
        return cls.status == OrgStatuses.ACTIVE
    
    @classmethod
    def trial(cls):
        """Scope for trial orgs (Rails scope pattern)"""
        from sqlalchemy import or_
        return or_(cls.status == OrgStatuses.TRIAL, cls.org_type == OrgTypes.TRIAL)
    
    @classmethod
    def suspended(cls):
        """Scope for suspended orgs (Rails scope pattern)"""
        return cls.status.in_([OrgStatuses.SUSPENDED, OrgStatuses.BILLING_SUSPENDED, OrgStatuses.COMPLIANCE_SUSPENDED])
    
    @classmethod
    def enterprise(cls):
        """Scope for enterprise orgs (Rails scope pattern)"""
        return cls.org_type == OrgTypes.ENTERPRISE
    
    @classmethod
    def over_limits(cls):
        """Scope for orgs over usage limits (Rails scope pattern)"""
        return cls.status.in_([OrgStatuses.SOURCE_COUNT_CAPPED, OrgStatuses.SOURCE_DATA_CAPPED])
    
    @classmethod
    def recently_active(cls, days: int = 7):
        """Scope for recently active orgs (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        return cls.last_activity_at >= cutoff
    
    @classmethod
    def get_nexla_admin_org(cls):
        """Get the Nexla admin org (Rails pattern)"""
        try:
            from sqlalchemy.orm import sessionmaker
            
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
    def create_with_defaults(cls, owner, name: str, org_type: OrgTypes = OrgTypes.TEAM):
        """Factory method to create org with defaults (Rails pattern)"""
        org = cls(
            name=name,
            owner=owner,
            org_type=org_type,
            status=OrgStatuses.PENDING_ACTIVATION,
            current_users=1,
            max_users=cls.MAX_USERS_FREE_TIER if org_type == OrgTypes.INDIVIDUAL else None,
            max_data_sources=cls.MAX_DATA_SOURCES_FREE_TIER if org_type == OrgTypes.INDIVIDUAL else None,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        if org_type == OrgTypes.TRIAL:
            org.start_trial_()
        
        return org

    # ========================================
    # Rails Helper and Utility Methods
    # ========================================
    
    def _update_metadata(self, key: str, value: Any) -> None:
        """Update metadata field (Rails helper pattern)"""
        try:
            current_meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
        except (json.JSONDecodeError, TypeError):
            current_meta = {}
            
        current_meta[key] = value
        self.extra_metadata = json.dumps(current_meta)
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Get metadata value (Rails helper pattern)"""
        try:
            meta = json.loads(self.extra_metadata) if self.extra_metadata else {}
            return meta.get(key, default)
        except (json.JSONDecodeError, TypeError):
            return default
    
    def get_feature_flags(self) -> Dict[str, Any]:
        """Get org feature flags (Rails pattern)"""
        try:
            return json.loads(self.feature_flags) if self.feature_flags else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_feature_flag_(self, flag_name: str, enabled: bool) -> None:
        """Set feature flag for org (Rails bang method pattern)"""
        flags = self.get_feature_flags()
        flags[flag_name] = enabled
        self.feature_flags = json.dumps(flags)
        self.updated_at = datetime.now()
    
    def has_feature_flag_(self, flag_name: str) -> bool:
        """Check if org has feature flag enabled (Rails pattern)"""
        flags = self.get_feature_flags()
        return flags.get(flag_name, False) is True
    
    def get_settings(self) -> Dict[str, Any]:
        """Get org settings (Rails pattern)"""
        try:
            return json.loads(self.settings) if self.settings else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_setting_(self, key: str, value: Any) -> None:
        """Set org setting (Rails bang method pattern)"""
        settings = self.get_settings()
        settings[key] = value
        self.settings = json.dumps(settings)
        self.updated_at = datetime.now()
    
    def get_setting(self, key: str, default=None) -> Any:
        """Get org setting value (Rails pattern)"""
        settings = self.get_settings()
        return settings.get(key, default)
    
    def _pause_all_flows(self) -> None:
        """Pause all flows in org (Rails helper pattern)"""
        # This would pause all flows - implementation depends on flow system
        logger.info(f"Pausing all flows for org: {self.name}")
    
    def _deactivate_all_resources(self) -> None:
        """Deactivate all resources in org (Rails helper pattern)"""
        # This would deactivate flows, data sources, etc.
        logger.info(f"Deactivating all resources for org: {self.name}")
    
    def usage_percentage(self) -> float:
        """Get data usage percentage (Rails pattern)"""
        if not self.monthly_data_limit_gb:
            return 0.0
        return (self.current_month_usage_gb / self.monthly_data_limit_gb) * 100
    
    def remaining_data_gb(self) -> float:
        """Get remaining data allowance (Rails pattern)"""
        if not self.monthly_data_limit_gb:
            return float('inf')
        return max(0, self.monthly_data_limit_gb - self.current_month_usage_gb)
    
    def time_until_trial_expiry(self) -> Optional[timedelta]:
        """Get time until trial expires (Rails pattern)"""
        if not self.trial_() or not self.trial_expires_at:
            return None
        
        now = datetime.now()
        if self.trial_expires_at > now:
            return self.trial_expires_at - now
        else:
            return timedelta(0)
    
    def admin_users(self) -> List:
        """Get list of admin users (Rails pattern)"""
        admin_users = [self.owner] if self.owner else []
        
        # Add billing owner if different
        if self.billing_owner and self.billing_owner.id != self.owner_id:
            admin_users.append(self.billing_owner)
        
        # This would add other admin users when role system is implemented
        return admin_users
    
    def is_owner(self, user) -> bool:
        """Check if user is owner of org (Rails pattern)"""
        return self.owner_id == user.id if user else False
    
    def is_billing_owner(self, user) -> bool:
        """Check if user is billing owner of org (Rails pattern)"""
        return self.billing_owner_id == user.id if user else False
    
    def has_admin_access(self, user) -> bool:
        """Check if user has admin access to org (Rails pattern)"""
        if not user:
            return False
        
        # Owner always has admin access
        if self.is_owner(user):
            return True
        
        # Billing owner has admin access
        if self.is_billing_owner(user):
            return True
        
        # Super users have admin access
        if user.super_user_():
            return True
        
        # This would check role-based access when role system is implemented
        return False

    # ========================================
    # Rails Validation Methods
    # ========================================
    
    def validate_(self) -> List[str]:
        """Validate org data (Rails validation pattern)"""
        errors = []
        
        if not self.name or not self.name.strip():
            errors.append("Name cannot be blank")
        elif len(self.name) > 255:
            errors.append("Name is too long (maximum 255 characters)")
        
        if self.name and not re.match(r'^[\w\s\-\.\(\)&]+$', self.name):
            errors.append("Name contains invalid characters")
        
        if not self.owner_id:
            errors.append("Owner is required")
        
        if self.client_identifier and len(self.client_identifier) > 255:
            errors.append("Client identifier is too long")
        
        if self.domain and not re.match(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', self.domain):
            errors.append("Invalid domain format")
        
        if self.max_users and self.max_users < 1:
            errors.append("Max users must be positive")
        
        if self.monthly_data_limit_gb and self.monthly_data_limit_gb < 0:
            errors.append("Data limit cannot be negative")
        
        return errors
    
    def valid_(self) -> bool:
        """Check if org is valid (Rails validation pattern)"""
        return len(self.validate_()) == 0
    
    def validate_billing_owner(self) -> None:
        """Validate billing owner (Rails pattern)"""
        if self.billing_owner_id and not self.billing_owner:
            raise ValueError("Invalid billing owner")

    # ========================================
    # Rails Display and Formatting Methods
    # ========================================
    
    def display_name(self) -> str:
        """Get display name for UI (Rails pattern)"""
        return self.name or f"Organization #{self.id}"
    
    def status_display(self) -> str:
        """Get human-readable status (Rails pattern)"""
        return self.status.display_name if hasattr(self.status, 'display_name') else str(self.status)
    
    def org_type_display(self) -> str:
        """Get human-readable org type (Rails pattern)"""
        return self.org_type.display_name if hasattr(self.org_type, 'display_name') else str(self.org_type)
    
    def status_color(self) -> str:
        """Get status color for UI (Rails pattern)"""
        status_colors = {
            OrgStatuses.ACTIVE: 'green',
            OrgStatuses.TRIAL: 'blue',
            OrgStatuses.PENDING_ACTIVATION: 'yellow',
            OrgStatuses.SUSPENDED: 'orange',
            OrgStatuses.DEACTIVATED: 'gray',
            OrgStatuses.ARCHIVED: 'gray',
            OrgStatuses.BILLING_SUSPENDED: 'red',
            OrgStatuses.COMPLIANCE_SUSPENDED: 'red',
            OrgStatuses.TRIAL_EXPIRED: 'red',
            OrgStatuses.SOURCE_COUNT_CAPPED: 'orange',
            OrgStatuses.SOURCE_DATA_CAPPED: 'orange',
            OrgStatuses.MIGRATING: 'purple'
        }
        return status_colors.get(self.status, 'gray')
    
    def usage_summary(self) -> Dict[str, Any]:
        """Get usage summary (Rails pattern)"""
        return {
            'users': {
                'current': self.current_users or 0,
                'max': self.max_users,
                'at_limit': self.at_user_limit_()
            },
            'data_sources': {
                'current': self.current_data_sources or 0,
                'max': self.max_data_sources,
                'at_limit': self.at_data_source_limit_()
            },
            'flows': {
                'current': self.current_flows or 0,
                'max': self.max_flows,
                'at_limit': self.at_flow_limit_()
            },
            'data_usage': {
                'current_gb': self.current_month_usage_gb or 0.0,
                'limit_gb': self.monthly_data_limit_gb,
                'percentage': self.usage_percentage(),
                'remaining_gb': self.remaining_data_gb(),
                'over_limit': self.over_data_limit_(),
                'near_limit': self.near_data_limit_()
            }
        }
    
    def billing_summary(self) -> Dict[str, Any]:
        """Get billing summary (Rails pattern)"""
        return {
            'status': self.billing_status.value if self.billing_status else None,
            'suspended': self.billing_suspended_(),
            'suspended_at': self.billing_suspended_at.isoformat() if self.billing_suspended_at else None,
            'suspension_reason': self.billing_suspension_reason,
            'billing_owner_id': self.billing_owner_id
        }
    
    def trial_summary(self) -> Dict[str, Any]:
        """Get trial information summary (Rails pattern)"""
        return {
            'is_trial': self.trial_(),
            'trial_expires_at': self.trial_expires_at.isoformat() if self.trial_expires_at else None,
            'trial_expired': self.trial_expired_(),
            'time_until_expiry': str(self.time_until_trial_expiry()) if self.time_until_trial_expiry() else None
        }

    # ========================================
    # Rails API Serialization Methods
    # ========================================
    
    def to_dict(self, include_usage: bool = False, include_members: bool = False) -> Dict[str, Any]:
        """Convert org to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'name': self.name,
            'display_name': self.display_name(),
            'description': self.description,
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'status_display': self.status_display(),
            'status_color': self.status_color(),
            'org_type': self.org_type.value if hasattr(self.org_type, 'value') else str(self.org_type),
            'org_type_display': self.org_type_display(),
            'active': self.active_(),
            'deactivated': self.deactivated_(),
            'suspended': self.suspended_(),
            'trial': self.trial_(),
            'nexla_admin_org': self.nexla_admin_org_(),
            'allow_api_key_access': self.allow_api_key_access,
            'search_index_name': self.search_index_name,
            'client_identifier': self.client_identifier,
            'domain': self.domain,
            'logo_url': self.logo_url,
            'primary_color': self.primary_color,
            'enforce_sso': self.sso_enforced_(),
            'require_mfa': self.mfa_required_(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'last_activity_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'owner_id': self.owner_id,
            'org_tier_id': self.org_tier_id,
            'cluster_id': self.cluster_id,
            'billing_owner_id': self.billing_owner_id,
            'parent_org_id': self.parent_org_id
        }
        
        if self.trial_():
            result.update(self.trial_summary())
        
        if self.billing_suspended_():
            result.update(self.billing_summary())
        
        if include_usage:
            result['usage_summary'] = self.usage_summary()
        
        if include_members:
            result['member_count'] = self.current_users
            result['admin_users'] = [user.to_summary_dict() for user in self.admin_users()]
        
        return result
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert org to summary dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'name': self.name,
            'display_name': self.display_name(),
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'org_type': self.org_type.value if hasattr(self.org_type, 'value') else str(self.org_type),
            'active': self.active_(),
            'trial': self.trial_(),
            'member_count': self.current_users,
            'logo_url': self.logo_url
        }
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Convert org to dictionary for audit logging (Rails pattern)"""
        return {
            'id': self.id,
            'name': self.name,
            'status': self.status.value if hasattr(self.status, 'value') else str(self.status),
            'org_type': self.org_type.value if hasattr(self.org_type, 'value') else str(self.org_type),
            'owner_id': self.owner_id,
            'member_count': self.current_users,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    # ========================================
    # Legacy Methods for Backwards Compatibility
    # ========================================
    
    def is_active(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.active_()
    
    def active(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.active_()
    
    def deactivated(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.deactivated_()
    
    def is_nexla_admin_org(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.nexla_admin_org_()
    
    def nexla_admin_org_q(self) -> bool:
        """Legacy method for backwards compatibility"""
        return self.nexla_admin_org_()
    
    def activate(self) -> None:
        """Legacy method for backwards compatibility"""
        self.activate_()
    
    def deactivate(self, pause_flows: bool = False) -> None:
        """Legacy method for backwards compatibility"""
        self.deactivate_(pause_flows)
    
    def set_defaults(self, user, cluster_id: Optional[int] = None) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        if cluster_id:
            self.cluster_id = cluster_id
    
    def add_members(self, user_or_users) -> None:
        """Add members to org (Rails pattern)"""
        if hasattr(user_or_users, '__iter__'):
            for user in user_or_users:
                self.add_member_(user)
        else:
            self.add_member_(user_or_users)
    
    def remove_members(self, user_or_users) -> None:
        """Remove members from org (Rails pattern)"""
        if hasattr(user_or_users, '__iter__'):
            for user in user_or_users:
                self.remove_member_(user)
        else:
            self.remove_member_(user_or_users)
    
    def add_admin(self, user) -> None:
        """Add admin role to user (Rails pattern)"""
        # This would create admin access control when implemented
        self.add_member_(user, role="admin")
    
    def remove_admin(self, user) -> None:
        """Remove admin role from user (Rails pattern)"""
        # This would remove admin access control when implemented
        pass
    
    def has_role(self, user, role: str, context_org=None) -> bool:
        """Check if user has specific role in org (Rails pattern)"""
        # This would check role system when implemented
        return False
    
    def handle_after_create(self) -> None:
        """Handle post-creation tasks (Rails pattern)"""
        # This would handle post-creation setup
        pass
    
    def org_webhook_host(self) -> Optional[str]:
        """Get org webhook host (Rails pattern)"""
        # This would return webhook host configuration
        return None
    
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
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any], api_user, api_org):
        """Build org from input data (Rails pattern)"""
        if not isinstance(input_data, dict):
            return None
        
        # This would be a complex factory method for creating orgs
        # Implementation would depend on complete user/cluster system
        return None
    
    def __repr__(self) -> str:
        return f"<Org(id={self.id}, name='{self.name}', status='{self.status}', type='{self.org_type}')>"
    
    def __str__(self) -> str:
        return self.display_name()