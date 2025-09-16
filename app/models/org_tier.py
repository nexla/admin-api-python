from enum import Enum as PyEnum
from typing import Optional, Dict, List, Any, Union, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from dataclasses import dataclass
import json

from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Enum as SQLEnum, Boolean, Text, Numeric, Index, ForeignKey
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property

from ..database import Base


logger = logging.getLogger(__name__)


class OrgTimePeriods(PyEnum):
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    YEARLY = "YEARLY"
    
    def get_seconds(self) -> int:
        """Get duration in seconds"""
        mapping = {
            self.HOURLY: 3600,
            self.DAILY: 86400,
            self.WEEKLY: 604800,
            self.MONTHLY: 2629746,  # Average month
            self.YEARLY: 31556952   # Average year
        }
        return mapping[self]
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        mapping = {
            self.HOURLY: "Per Hour",
            self.DAILY: "Per Day", 
            self.WEEKLY: "Per Week",
            self.MONTHLY: "Per Month",
            self.YEARLY: "Per Year"
        }
        return mapping[self]


class OrgTierTypes(PyEnum):
    FREE = "FREE"
    TRIAL = "TRIAL"
    STARTUP = "STARTUP"
    GROWTH = "GROWTH"
    PROFESSIONAL = "PROFESSIONAL"
    BUSINESS = "BUSINESS"
    ENTERPRISE = "ENTERPRISE"
    CUSTOM = "CUSTOM"
    LEGACY = "LEGACY"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()
    
    def get_sort_order(self) -> int:
        """Get sort order for tier hierarchy"""
        mapping = {
            self.FREE: 0,
            self.TRIAL: 1,
            self.STARTUP: 2,
            self.GROWTH: 3,
            self.PROFESSIONAL: 4,
            self.BUSINESS: 5,
            self.ENTERPRISE: 6,
            self.CUSTOM: 7,
            self.LEGACY: 8
        }
        return mapping[self]


class OrgTierStatuses(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DEPRECATED = "DEPRECATED"
    ARCHIVED = "ARCHIVED"
    MAINTENANCE = "MAINTENANCE"
    SUSPENDED = "SUSPENDED"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class OrgFeatureFlags(PyEnum):
    MULTI_USER = "MULTI_USER"
    API_ACCESS = "API_ACCESS"
    PREMIUM_SUPPORT = "PREMIUM_SUPPORT"
    ADVANCED_ANALYTICS = "ADVANCED_ANALYTICS"
    CUSTOM_BRANDING = "CUSTOM_BRANDING"
    PRIORITY_PROCESSING = "PRIORITY_PROCESSING"
    BULK_OPERATIONS = "BULK_OPERATIONS"
    ADVANCED_SECURITY = "ADVANCED_SECURITY"
    DEDICATED_RESOURCES = "DEDICATED_RESOURCES"
    SLA_GUARANTEE = "SLA_GUARANTEE"
    WHITE_LABELING = "WHITE_LABELING"
    AUDIT_LOGGING = "AUDIT_LOGGING"
    TEAM_MANAGEMENT = "TEAM_MANAGEMENT"
    ROLE_BASED_ACCESS = "ROLE_BASED_ACCESS"
    SINGLE_SIGN_ON = "SINGLE_SIGN_ON"


class ThrottleReasons(PyEnum):
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    RESOURCE_QUOTA_EXCEEDED = "RESOURCE_QUOTA_EXCEEDED"
    BILLING_ISSUE = "BILLING_ISSUE"
    ABUSE_DETECTION = "ABUSE_DETECTION"
    MAINTENANCE_MODE = "MAINTENANCE_MODE"
    MANUAL_OVERRIDE = "MANUAL_OVERRIDE"


@dataclass
class OrgLimitInfo:
    """Information about organizational limits"""
    limit_type: str
    current_value: int
    max_value: Optional[int]
    percentage_used: float
    time_period: Optional[OrgTimePeriods] = None
    is_unlimited: bool = False
    org_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'limit_type': self.limit_type,
            'current_value': self.current_value,
            'max_value': self.max_value,
            'percentage_used': self.percentage_used,
            'time_period': self.time_period.value if self.time_period else None,
            'is_unlimited': self.is_unlimited,
            'org_count': self.org_count
        }


@dataclass 
class OrgTierMetrics:
    """Metrics for organizational tier usage and performance"""
    total_orgs: int = 0
    active_orgs: int = 0
    trial_orgs: int = 0
    conversion_rate: float = 0.0
    average_team_size: float = 0.0
    average_usage: Dict[str, float] = None
    revenue_impact: Decimal = Decimal('0.00')
    churn_rate: float = 0.0
    
    def __post_init__(self):
        if self.average_usage is None:
            self.average_usage = {}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_orgs': self.total_orgs,
            'active_orgs': self.active_orgs,
            'trial_orgs': self.trial_orgs,
            'conversion_rate': float(self.conversion_rate),
            'average_team_size': float(self.average_team_size),
            'average_usage': self.average_usage,
            'revenue_impact': float(self.revenue_impact),
            'churn_rate': float(self.churn_rate)
        }


class OrgTier(Base):
    __tablename__ = "org_tiers"
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_org_tier_name', 'name'),
        Index('idx_org_tier_status', 'status'),
        Index('idx_org_tier_type', 'tier_type'),
        Index('idx_org_tier_active', 'status', 'is_active'),
        Index('idx_org_tier_billing', 'billing_cycle', 'monthly_price'),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Core tier information
    name = Column(String(255), nullable=False, unique=True, index=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text)
    tier_type = Column(SQLEnum(OrgTierTypes), nullable=False, default=OrgTierTypes.FREE)
    status = Column(SQLEnum(OrgTierStatuses), nullable=False, default=OrgTierStatuses.ACTIVE)
    
    # State flags
    is_active = Column(Boolean, nullable=False, default=True)
    is_deprecated = Column(Boolean, nullable=False, default=False)
    is_trial_available = Column(Boolean, nullable=False, default=False)
    is_publicly_available = Column(Boolean, nullable=False, default=True)
    is_custom = Column(Boolean, nullable=False, default=False)
    is_throttled = Column(Boolean, nullable=False, default=False)
    
    # Core limits
    record_count_limit = Column(BigInteger)  # null = unlimited
    record_count_limit_time = Column(SQLEnum(OrgTimePeriods), nullable=False, default=OrgTimePeriods.DAILY)
    data_source_count_limit = Column(Integer)  # null = unlimited
    api_request_limit = Column(BigInteger)  # null = unlimited
    api_request_limit_time = Column(SQLEnum(OrgTimePeriods), nullable=False, default=OrgTimePeriods.DAILY)
    storage_limit_gb = Column(Numeric(12, 2))  # null = unlimited
    
    # Team and user limits
    member_count_limit = Column(Integer)  # max team members
    admin_count_limit = Column(Integer)  # max admins
    project_count_limit = Column(Integer)  # max projects
    
    # Advanced limits
    concurrent_jobs_limit = Column(Integer, default=5)
    export_limit = Column(Integer)  # exports per time period
    export_limit_time = Column(SQLEnum(OrgTimePeriods), nullable=False, default=OrgTimePeriods.DAILY)
    retention_days = Column(Integer, default=90)
    max_file_size_mb = Column(Integer, default=500)
    bandwidth_limit_gb = Column(Numeric(10, 2))  # monthly bandwidth
    
    # Trial configuration  
    trial_period_days = Column(Integer, default=30)
    trial_record_limit = Column(BigInteger)
    trial_data_source_limit = Column(Integer, default=3)
    trial_member_limit = Column(Integer, default=5)
    
    # Pricing information
    monthly_price = Column(Numeric(12, 2), default=Decimal('0.00'))
    yearly_price = Column(Numeric(12, 2), default=Decimal('0.00'))
    setup_fee = Column(Numeric(10, 2), default=Decimal('0.00'))
    overage_rate = Column(Numeric(10, 4), default=Decimal('0.00'))  # per record over limit
    per_user_price = Column(Numeric(10, 2), default=Decimal('0.00'))  # additional user cost
    
    # Billing configuration
    billing_cycle = Column(String(50), default='monthly')  # monthly, yearly, custom
    min_commitment_months = Column(Integer, default=1)
    max_discount_percentage = Column(Numeric(5, 2), default=Decimal('0.00'))
    
    # Feature flags (JSON storage)
    feature_flags = Column(Text)  # JSON list of enabled features
    
    # Rate limiting
    rate_limit_id = Column(Integer, ForeignKey("rate_limits.id"))
    throttle_reason = Column(SQLEnum(ThrottleReasons))
    throttle_until = Column(DateTime)
    
    # Priority and ordering
    sort_order = Column(Integer, default=0)
    upgrade_tier_id = Column(Integer)  # ID of tier to upgrade to
    downgrade_tier_id = Column(Integer)  # ID of tier to downgrade to
    
    # Sales and marketing
    is_popular = Column(Boolean, nullable=False, default=False)
    marketing_label = Column(String(100))  # "Most Popular", "Best Value", etc.
    sales_contact_required = Column(Boolean, nullable=False, default=False)
    
    # Lifecycle timestamps
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    deprecated_at = Column(DateTime)
    archived_at = Column(DateTime)
    
    # Relationships
    orgs = relationship("Org", back_populates="org_tier")
    rate_limit = relationship("RateLimit")
    
    # Constants
    UNLIMITED = -1
    DEFAULT_TRIAL_DAYS = 30
    MAX_TRIAL_DAYS = 365
    MIN_MONTHLY_PRICE = Decimal('0.00')
    MAX_MONTHLY_PRICE = Decimal('999999.99')
    MAX_TEAM_SIZE = 10000
    
    def __repr__(self) -> str:
        return f"<OrgTier(id={self.id}, name='{self.name}', type='{self.tier_type.value}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        return f"{self.display_name} ({self.name})"
    
    # === Rails-style Predicate Methods ===
    
    def active_(self) -> bool:
        """Check if tier is active and available"""
        return self.status == OrgTierStatuses.ACTIVE and self.is_active and not self.is_deprecated
    
    def inactive_(self) -> bool:
        """Check if tier is inactive"""
        return not self.active_()
    
    def deprecated_(self) -> bool:
        """Check if tier is deprecated"""
        return self.is_deprecated or self.status == OrgTierStatuses.DEPRECATED
    
    def archived_(self) -> bool:
        """Check if tier is archived"""
        return self.status == OrgTierStatuses.ARCHIVED or self.archived_at is not None
    
    def suspended_(self) -> bool:
        """Check if tier is suspended"""
        return self.status == OrgTierStatuses.SUSPENDED
    
    def trial_available_(self) -> bool:
        """Check if trial is available for this tier"""
        return self.is_trial_available and self.active_() and self.trial_period_days > 0
    
    def publicly_available_(self) -> bool:
        """Check if tier is publicly available for signup"""
        return self.is_publicly_available and self.active_() and not self.is_custom and not self.sales_contact_required
    
    def custom_(self) -> bool:
        """Check if this is a custom tier"""
        return self.is_custom or self.tier_type == OrgTierTypes.CUSTOM
    
    def free_(self) -> bool:
        """Check if this is a free tier"""
        return self.tier_type == OrgTierTypes.FREE or self.monthly_price == Decimal('0.00')
    
    def paid_(self) -> bool:
        """Check if this is a paid tier"""
        return not self.free_()
    
    def enterprise_(self) -> bool:
        """Check if this is an enterprise tier"""
        return self.tier_type == OrgTierTypes.ENTERPRISE
    
    def legacy_(self) -> bool:
        """Check if this is a legacy tier"""
        return self.tier_type == OrgTierTypes.LEGACY
    
    def maintenance_(self) -> bool:
        """Check if tier is in maintenance mode"""
        return self.status == OrgTierStatuses.MAINTENANCE
    
    def throttled_(self) -> bool:
        """Check if tier is currently throttled"""
        if not self.is_throttled:
            return False
        if self.throttle_until and self.throttle_until > datetime.now():
            return True
        if self.throttle_until and self.throttle_until <= datetime.now():
            # Auto-clear expired throttles
            self.is_throttled = False
            self.throttle_until = None
            self.throttle_reason = None
            return False
        return self.is_throttled
    
    def popular_(self) -> bool:
        """Check if tier is marked as popular"""
        return self.is_popular and self.active_()
    
    def requires_sales_contact_(self) -> bool:
        """Check if tier requires sales contact"""
        return self.sales_contact_required or self.enterprise_()
    
    def unlimited_records_(self) -> bool:
        """Check if this tier has unlimited record processing"""
        return self.record_count_limit is None or self.record_count_limit == self.UNLIMITED
    
    def unlimited_data_sources_(self) -> bool:
        """Check if this tier has unlimited data sources"""
        return self.data_source_count_limit is None or self.data_source_count_limit == self.UNLIMITED
    
    def unlimited_storage_(self) -> bool:
        """Check if this tier has unlimited storage"""
        return self.storage_limit_gb is None or self.storage_limit_gb <= 0
    
    def unlimited_api_requests_(self) -> bool:
        """Check if this tier has unlimited API requests"""
        return self.api_request_limit is None or self.api_request_limit == self.UNLIMITED
    
    def unlimited_members_(self) -> bool:
        """Check if this tier has unlimited team members"""
        return self.member_count_limit is None or self.member_count_limit == self.UNLIMITED
    
    def unlimited_projects_(self) -> bool:
        """Check if this tier has unlimited projects"""
        return self.project_count_limit is None or self.project_count_limit == self.UNLIMITED
    
    def has_feature_(self, feature: OrgFeatureFlags) -> bool:
        """Check if tier has specific feature enabled"""
        if not self.feature_flags:
            return False
        try:
            features = json.loads(self.feature_flags)
            return feature.value in features
        except (json.JSONDecodeError, TypeError):
            return False
    
    def supports_multi_user_(self) -> bool:
        """Check if tier supports multiple users"""
        return self.has_feature_(OrgFeatureFlags.MULTI_USER) or not self.free_()
    
    def supports_api_(self) -> bool:
        """Check if tier supports API access"""
        return self.has_feature_(OrgFeatureFlags.API_ACCESS)
    
    def has_premium_support_(self) -> bool:
        """Check if tier includes premium support"""
        return self.has_feature_(OrgFeatureFlags.PREMIUM_SUPPORT)
    
    def has_sla_(self) -> bool:
        """Check if tier includes SLA guarantee"""
        return self.has_feature_(OrgFeatureFlags.SLA_GUARANTEE)
    
    def has_sso_(self) -> bool:
        """Check if tier includes Single Sign-On"""
        return self.has_feature_(OrgFeatureFlags.SINGLE_SIGN_ON)
    
    def has_audit_logging_(self) -> bool:
        """Check if tier includes audit logging"""
        return self.has_feature_(OrgFeatureFlags.AUDIT_LOGGING)
    
    def over_limit_(self, usage_type: str, current_usage: int) -> bool:
        """Check if current usage exceeds tier limits"""
        limit = self.get_limit_for_type(usage_type)
        if limit is None or limit == self.UNLIMITED:
            return False
        return current_usage > limit
    
    def approaching_limit_(self, usage_type: str, current_usage: int, threshold: float = 0.8) -> bool:
        """Check if usage is approaching limit (default 80%)"""
        limit = self.get_limit_for_type(usage_type)
        if limit is None or limit == self.UNLIMITED:
            return False
        return current_usage >= (limit * threshold)
    
    def can_upgrade_(self) -> bool:
        """Check if tier can be upgraded"""
        return self.upgrade_tier_id is not None and self.active_()
    
    def can_downgrade_(self) -> bool:
        """Check if tier can be downgraded"""
        return self.downgrade_tier_id is not None and self.active_()
    
    def has_commitment_(self) -> bool:
        """Check if tier has minimum commitment period"""
        return self.min_commitment_months > 1
    
    def yearly_billing_available_(self) -> bool:
        """Check if yearly billing is available"""
        return self.yearly_price > 0 and self.paid_()
    
    # === Rails-style Bang Methods ===
    
    def activate_(self) -> None:
        """Activate the tier"""
        if self.active_():
            return
        if self.archived_():
            raise ValueError("Cannot activate archived tier")
        
        self.status = OrgTierStatuses.ACTIVE
        self.is_active = True
        self.updated_at = datetime.now()
        logger.info(f"Activated org tier {self.name}")
    
    def deactivate_(self) -> None:
        """Deactivate the tier"""
        if not self.active_():
            return
        
        self.status = OrgTierStatuses.INACTIVE
        self.is_active = False
        self.updated_at = datetime.now()
        logger.info(f"Deactivated org tier {self.name}")
    
    def deprecate_(self, reason: str = None) -> None:
        """Deprecate the tier"""
        if self.deprecated_():
            return
        
        self.is_deprecated = True
        self.status = OrgTierStatuses.DEPRECATED
        self.deprecated_at = datetime.now()
        self.updated_at = datetime.now()
        logger.info(f"Deprecated org tier {self.name}: {reason or 'No reason specified'}")
    
    def archive_(self) -> None:
        """Archive the tier"""
        if self.archived_():
            return
        
        self.status = OrgTierStatuses.ARCHIVED
        self.is_active = False
        self.archived_at = datetime.now()
        self.updated_at = datetime.now()
        logger.info(f"Archived org tier {self.name}")
    
    def suspend_(self, reason: ThrottleReasons = None) -> None:
        """Suspend the tier"""
        if self.suspended_():
            return
        
        self.status = OrgTierStatuses.SUSPENDED
        if reason:
            self.throttle_reason = reason
        self.updated_at = datetime.now()
        logger.warning(f"Suspended org tier {self.name}: {reason.value if reason else 'No reason specified'}")
    
    def unsuspend_(self) -> None:
        """Remove suspension from tier"""
        if not self.suspended_():
            return
        
        self.status = OrgTierStatuses.ACTIVE
        self.throttle_reason = None
        self.updated_at = datetime.now()
        logger.info(f"Removed suspension from org tier {self.name}")
    
    def throttle_(self, reason: ThrottleReasons, duration_hours: int = 24) -> None:
        """Throttle the tier for a specified duration"""
        self.is_throttled = True
        self.throttle_reason = reason
        self.throttle_until = datetime.now() + timedelta(hours=duration_hours)
        self.updated_at = datetime.now()
        logger.warning(f"Throttled org tier {self.name} for {duration_hours}h: {reason.value}")
    
    def unthrottle_(self) -> None:
        """Remove throttling from tier"""
        if not self.throttled_():
            return
        
        self.is_throttled = False
        self.throttle_reason = None
        self.throttle_until = None
        self.updated_at = datetime.now()
        logger.info(f"Removed throttling from org tier {self.name}")
    
    def enable_trial_(self, days: int = None, record_limit: int = None, member_limit: int = None) -> None:
        """Enable trial for this tier"""
        if days is None:
            days = self.DEFAULT_TRIAL_DAYS
        if days > self.MAX_TRIAL_DAYS:
            raise ValueError(f"Trial period cannot exceed {self.MAX_TRIAL_DAYS} days")
        
        self.is_trial_available = True
        self.trial_period_days = days
        if record_limit is not None:
            self.trial_record_limit = record_limit
        if member_limit is not None:
            self.trial_member_limit = member_limit
        self.updated_at = datetime.now()
    
    def disable_trial_(self) -> None:
        """Disable trial for this tier"""
        self.is_trial_available = False
        self.updated_at = datetime.now()
    
    def mark_popular_(self, label: str = "Most Popular") -> None:
        """Mark tier as popular with optional label"""
        self.is_popular = True
        self.marketing_label = label
        self.updated_at = datetime.now()
    
    def unmark_popular_(self) -> None:
        """Remove popular marking from tier"""
        self.is_popular = False
        self.marketing_label = None
        self.updated_at = datetime.now()
    
    def add_feature_(self, feature: OrgFeatureFlags) -> None:
        """Add feature to tier"""
        features = self.get_features_list()
        if feature.value not in features:
            features.append(feature.value)
            self.feature_flags = json.dumps(features)
            self.updated_at = datetime.now()
    
    def remove_feature_(self, feature: OrgFeatureFlags) -> None:
        """Remove feature from tier"""
        features = self.get_features_list()
        if feature.value in features:
            features.remove(feature.value)
            self.feature_flags = json.dumps(features)
            self.updated_at = datetime.now()
    
    def update_pricing_(self, monthly_price: Decimal = None, yearly_price: Decimal = None, 
                       setup_fee: Decimal = None, overage_rate: Decimal = None, 
                       per_user_price: Decimal = None) -> None:
        """Update tier pricing"""
        if monthly_price is not None:
            if monthly_price < self.MIN_MONTHLY_PRICE or monthly_price > self.MAX_MONTHLY_PRICE:
                raise ValueError(f"Monthly price must be between {self.MIN_MONTHLY_PRICE} and {self.MAX_MONTHLY_PRICE}")
            self.monthly_price = monthly_price
        
        if yearly_price is not None:
            self.yearly_price = yearly_price
        
        if setup_fee is not None:
            self.setup_fee = setup_fee
        
        if overage_rate is not None:
            self.overage_rate = overage_rate
        
        if per_user_price is not None:
            self.per_user_price = per_user_price
        
        self.updated_at = datetime.now()
    
    def update_limits_(self, **limits) -> None:
        """Update tier limits"""
        for limit_type, value in limits.items():
            if hasattr(self, limit_type):
                setattr(self, limit_type, value)
        self.updated_at = datetime.now()
    
    # === Class Methods (Rails-style Scopes) ===
    
    @classmethod
    def active(cls, session: Session):
        """Get all active tiers"""
        return session.query(cls).filter(
            cls.status == OrgTierStatuses.ACTIVE,
            cls.is_active == True,
            cls.is_deprecated == False
        ).order_by(cls.sort_order)
    
    @classmethod
    def publicly_available(cls, session: Session):
        """Get all publicly available tiers"""
        return cls.active(session).filter(
            cls.is_publicly_available == True,
            cls.is_custom == False,
            cls.sales_contact_required == False
        )
    
    @classmethod
    def trial_enabled(cls, session: Session):
        """Get all tiers with trial enabled"""
        return cls.active(session).filter(cls.is_trial_available == True)
    
    @classmethod
    def popular_tiers(cls, session: Session):
        """Get popular tiers"""
        return cls.publicly_available(session).filter(cls.is_popular == True)
    
    @classmethod
    def by_type(cls, session: Session, tier_type: OrgTierTypes):
        """Get tiers by type"""
        return session.query(cls).filter(cls.tier_type == tier_type)
    
    @classmethod
    def paid_tiers(cls, session: Session):
        """Get all paid tiers"""
        return cls.active(session).filter(cls.monthly_price > 0)
    
    @classmethod
    def free_tiers(cls, session: Session):
        """Get all free tiers"""
        return cls.active(session).filter(cls.monthly_price == 0)
    
    @classmethod
    def enterprise_tiers(cls, session: Session):
        """Get all enterprise tiers"""
        return cls.active(session).filter(cls.tier_type == OrgTierTypes.ENTERPRISE)
    
    @classmethod
    def by_price_range(cls, session: Session, min_price: Decimal, max_price: Decimal):
        """Get tiers within price range"""
        return cls.active(session).filter(
            cls.monthly_price >= min_price,
            cls.monthly_price <= max_price
        ).order_by(cls.monthly_price)
    
    @classmethod
    def by_team_size(cls, session: Session, team_size: int):
        """Get tiers suitable for team size"""
        return cls.active(session).filter(
            (cls.member_count_limit >= team_size) | (cls.member_count_limit.is_(None))
        ).order_by(cls.monthly_price)
    
    @classmethod
    def find_by_name(cls, session: Session, name: str):
        """Find tier by name"""
        return session.query(cls).filter(cls.name == name).first()
    
    @classmethod
    def find_upgrade_path(cls, session: Session, from_tier_id: int):
        """Find upgrade path from given tier"""
        current = session.query(cls).filter(cls.id == from_tier_id).first()
        if not current or not current.upgrade_tier_id:
            return None
        return session.query(cls).filter(cls.id == current.upgrade_tier_id).first()
    
    @classmethod
    def find_downgrade_path(cls, session: Session, from_tier_id: int):
        """Find downgrade path from given tier"""
        current = session.query(cls).filter(cls.id == from_tier_id).first()
        if not current or not current.downgrade_tier_id:
            return None
        return session.query(cls).filter(cls.id == current.downgrade_tier_id).first()
    
    @classmethod
    def get_default_free_tier(cls, session: Session):
        """Get default free tier"""
        return cls.free_tiers(session).filter(cls.tier_type == OrgTierTypes.FREE).first()
    
    @classmethod
    def throttled_tiers(cls, session: Session):
        """Get all throttled tiers"""
        return session.query(cls).filter(
            (cls.is_throttled == True) | 
            (cls.throttle_until > datetime.now())
        )
    
    @classmethod
    def validate_data_source_activate(cls, session: Session, data_source, org):
        """Validate if a data source can be activated based on org tier limits"""
        if not org or not org.org_tier:
            return False, "No organization tier found"
        
        tier = org.org_tier
        
        # Check if tier is active
        if not tier.active_():
            return False, f"Organization tier '{tier.name}' is not active"
        
        # Check if tier is throttled
        if tier.throttled_():
            return False, f"Organization tier '{tier.name}' is currently throttled"
        
        # Check data source count limit
        if not tier.unlimited_data_sources_():
            current_count = session.query(DataSource).filter(
                DataSource.org_id == org.id,
                DataSource.status.in_(['ACTIVE', 'PAUSED', 'PROCESSING'])
            ).count()
            
            if current_count >= tier.data_source_count_limit:
                return False, f"Data source limit of {tier.data_source_count_limit} exceeded"
        
        return True, "Validation passed"
    
    @classmethod
    def tier_resource(cls, session: Session, resource):
        """Get the tier resource for a given resource (org tier vs user tier)"""
        # For organizational resources, return the org tier
        if hasattr(resource, 'org') and resource.org:
            return resource.org.org_tier
        
        # For user resources, check if they have an org, otherwise use user tier
        if hasattr(resource, 'user') and resource.user:
            if hasattr(resource.user, 'primary_org') and resource.user.primary_org:
                return resource.user.primary_org.org_tier
            return resource.user.user_tier
        
        return None
    
    @classmethod
    def account_resource(cls, resource):
        """Get the account resource (org vs user) for a given resource"""
        # Check for org first (org-level resources)
        if hasattr(resource, 'org') and resource.org:
            return resource.org
        
        # Check for user (user-level resources)
        if hasattr(resource, 'user') and resource.user:
            # If user has a primary org, return that, otherwise return user
            if hasattr(resource.user, 'primary_org') and resource.user.primary_org:
                return resource.user.primary_org
            return resource.user
        
        return None
    
    @classmethod
    def build_from_input(cls, input_data: dict):
        """Create new OrgTier from input data (Rails pattern)"""
        if not input_data.get('name'):
            raise ValueError("Tier name is required")
        if not input_data.get('display_name'):
            raise ValueError("Tier display name is required")
        
        # Set defaults
        defaults = {
            'tier_type': OrgTierTypes.FREE,
            'status': OrgTierStatuses.ACTIVE,
            'is_active': True,
            'is_deprecated': False,
            'is_trial_available': False,
            'is_publicly_available': True,
            'is_custom': False,
            'is_throttled': False,
            'record_count_limit_time': OrgTimePeriods.DAILY,
            'api_request_limit_time': OrgTimePeriods.DAILY,
            'export_limit_time': OrgTimePeriods.DAILY,
            'concurrent_jobs_limit': 5,
            'retention_days': 90,
            'max_file_size_mb': 500,
            'trial_period_days': cls.DEFAULT_TRIAL_DAYS,
            'trial_data_source_limit': 3,
            'trial_member_limit': 5,
            'monthly_price': Decimal('0.00'),
            'yearly_price': Decimal('0.00'),
            'setup_fee': Decimal('0.00'),
            'overage_rate': Decimal('0.00'),
            'per_user_price': Decimal('0.00'),
            'billing_cycle': 'monthly',
            'min_commitment_months': 1,
            'max_discount_percentage': Decimal('0.00'),
            'sort_order': 0,
            'is_popular': False,
            'sales_contact_required': False
        }
        
        # Merge with input data
        tier_data = {**defaults, **input_data}
        
        # Handle enum conversions
        if isinstance(tier_data.get('tier_type'), str):
            tier_data['tier_type'] = OrgTierTypes(tier_data['tier_type'])
        if isinstance(tier_data.get('status'), str):
            tier_data['status'] = OrgTierStatuses(tier_data['status'])
        if isinstance(tier_data.get('record_count_limit_time'), str):
            tier_data['record_count_limit_time'] = OrgTimePeriods(tier_data['record_count_limit_time'])
        
        return cls(**tier_data)
    
    # === Instance Methods ===
    
    def update_mutable(self, input_data: dict) -> None:
        """Update mutable fields from input data"""
        if not input_data:
            return
        
        mutable_fields = {
            'display_name', 'description', 'tier_type', 'status',
            'is_active', 'is_deprecated', 'is_trial_available',
            'is_publicly_available', 'is_custom', 'is_throttled',
            'record_count_limit', 'record_count_limit_time',
            'data_source_count_limit', 'api_request_limit', 'api_request_limit_time',
            'storage_limit_gb', 'member_count_limit', 'admin_count_limit',
            'project_count_limit', 'concurrent_jobs_limit', 'export_limit',
            'export_limit_time', 'retention_days', 'max_file_size_mb',
            'bandwidth_limit_gb', 'trial_period_days', 'trial_record_limit',
            'trial_data_source_limit', 'trial_member_limit',
            'monthly_price', 'yearly_price', 'setup_fee', 'overage_rate',
            'per_user_price', 'billing_cycle', 'min_commitment_months',
            'max_discount_percentage', 'sort_order', 'upgrade_tier_id',
            'downgrade_tier_id', 'is_popular', 'marketing_label',
            'sales_contact_required', 'rate_limit_id'
        }
        
        for field, value in input_data.items():
            if field in mutable_fields and hasattr(self, field):
                # Handle enum conversions
                if field == 'tier_type' and isinstance(value, str):
                    value = OrgTierTypes(value)
                elif field == 'status' and isinstance(value, str):
                    value = OrgTierStatuses(value)
                elif field.endswith('_time') and isinstance(value, str):
                    value = OrgTimePeriods(value)
                
                setattr(self, field, value)
        
        self.updated_at = datetime.now()
    
    def get_features_list(self) -> List[str]:
        """Get list of enabled features"""
        if not self.feature_flags:
            return []
        try:
            return json.loads(self.feature_flags)
        except (json.JSONDecodeError, TypeError):
            return []
    
    def get_limit_for_type(self, usage_type: str) -> Optional[int]:
        """Get limit value for specific usage type"""
        limit_mapping = {
            'records': self.record_count_limit,
            'data_sources': self.data_source_count_limit,
            'api_requests': self.api_request_limit,
            'exports': self.export_limit,
            'concurrent_jobs': self.concurrent_jobs_limit,
            'file_size': self.max_file_size_mb,
            'members': self.member_count_limit,
            'admins': self.admin_count_limit,
            'projects': self.project_count_limit,
            'bandwidth': int(self.bandwidth_limit_gb * 1024) if self.bandwidth_limit_gb else None  # Convert to MB
        }
        return limit_mapping.get(usage_type)
    
    def get_usage_info(self, current_usage: Dict[str, int], org_count: int = 1) -> Dict[str, OrgLimitInfo]:
        """Get comprehensive usage information"""
        usage_info = {}
        
        for usage_type, current_value in current_usage.items():
            limit = self.get_limit_for_type(usage_type)
            is_unlimited = limit is None or limit == self.UNLIMITED
            percentage_used = 0.0 if is_unlimited else (current_value / limit * 100 if limit > 0 else 100)
            
            time_period = None
            if usage_type in ['records', 'api_requests', 'exports']:
                time_period = getattr(self, f"{usage_type.rstrip('s')}_limit_time", None)
            
            usage_info[usage_type] = OrgLimitInfo(
                limit_type=usage_type,
                current_value=current_value,
                max_value=limit if not is_unlimited else None,
                percentage_used=min(percentage_used, 100.0),
                time_period=time_period,
                is_unlimited=is_unlimited,
                org_count=org_count
            )
        
        return usage_info
    
    def calculate_overage_cost(self, usage_type: str, current_usage: int) -> Decimal:
        """Calculate overage cost for exceeding limits"""
        if usage_type != 'records' or self.overage_rate == 0:
            return Decimal('0.00')
        
        limit = self.record_count_limit
        if limit is None or limit == self.UNLIMITED:
            return Decimal('0.00')
        
        if current_usage <= limit:
            return Decimal('0.00')
        
        overage = current_usage - limit
        return Decimal(str(overage)) * self.overage_rate
    
    def calculate_total_cost(self, billing_cycle: str, team_size: int = 0, usage_overages: Dict[str, int] = None) -> Decimal:
        """Calculate total cost including base price, per-user fees, and overages"""
        if billing_cycle == 'yearly' and self.yearly_price > 0:
            base_cost = self.yearly_price
        else:
            base_cost = self.monthly_price
            if billing_cycle == 'yearly':
                base_cost *= 12
        
        # Add per-user costs
        if team_size > 0 and self.per_user_price > 0:
            user_cost = Decimal(str(team_size)) * self.per_user_price
            if billing_cycle == 'yearly':
                user_cost *= 12
            base_cost += user_cost
        
        # Add overage costs
        total_overage = Decimal('0.00')
        if usage_overages:
            for usage_type, overage_amount in usage_overages.items():
                total_overage += self.calculate_overage_cost(usage_type, overage_amount)
        
        return base_cost + total_overage + self.setup_fee
    
    def get_upgrade_benefits(self, session: Session) -> List[str]:
        """Get list of benefits from upgrading to next tier"""
        if not self.upgrade_tier_id:
            return []
        
        upgrade_tier = session.query(OrgTier).filter(OrgTier.id == self.upgrade_tier_id).first()
        if not upgrade_tier:
            return []
        
        benefits = []
        
        # Compare limits
        if upgrade_tier.unlimited_records_() and not self.unlimited_records_():
            benefits.append("Unlimited record processing")
        elif upgrade_tier.record_count_limit and self.record_count_limit and upgrade_tier.record_count_limit > self.record_count_limit:
            benefits.append(f"Increased record limit to {upgrade_tier.record_count_limit:,}")
        
        if upgrade_tier.unlimited_data_sources_() and not self.unlimited_data_sources_():
            benefits.append("Unlimited data sources")
        elif upgrade_tier.data_source_count_limit and self.data_source_count_limit and upgrade_tier.data_source_count_limit > self.data_source_count_limit:
            benefits.append(f"Increased data source limit to {upgrade_tier.data_source_count_limit}")
        
        if upgrade_tier.unlimited_members_() and not self.unlimited_members_():
            benefits.append("Unlimited team members")
        elif upgrade_tier.member_count_limit and self.member_count_limit and upgrade_tier.member_count_limit > self.member_count_limit:
            benefits.append(f"Increased team size to {upgrade_tier.member_count_limit} members")
        
        # Compare features
        current_features = set(self.get_features_list())
        upgrade_features = set(upgrade_tier.get_features_list())
        new_features = upgrade_features - current_features
        
        for feature in new_features:
            try:
                feature_enum = OrgFeatureFlags(feature)
                benefits.append(f"Access to {feature_enum.value.replace('_', ' ').title()}")
            except ValueError:
                continue
        
        return benefits
    
    def get_display_info(self) -> Dict[str, Any]:
        """Get formatted display information"""
        return {
            'name': self.display_name,
            'type': self.tier_type.get_display_name(),
            'status': self.status.get_display_name(),
            'price_monthly': f"${float(self.monthly_price):.2f}",
            'price_yearly': f"${float(self.yearly_price):.2f}" if self.yearly_price > 0 else None,
            'per_user_price': f"${float(self.per_user_price):.2f}" if self.per_user_price > 0 else None,
            'setup_fee': f"${float(self.setup_fee):.2f}" if self.setup_fee > 0 else None,
            'trial_available': self.trial_available_(),
            'trial_days': self.trial_period_days if self.trial_available_() else None,
            'popular': self.popular_(),
            'marketing_label': self.marketing_label,
            'record_limit': "Unlimited" if self.unlimited_records_() else f"{self.record_count_limit:,}",
            'record_period': self.record_count_limit_time.get_display_name() if not self.unlimited_records_() else None,
            'data_source_limit': "Unlimited" if self.unlimited_data_sources_() else str(self.data_source_count_limit),
            'member_limit': "Unlimited" if self.unlimited_members_() else str(self.member_count_limit),
            'storage_limit': "Unlimited" if self.unlimited_storage_() else f"{float(self.storage_limit_gb)} GB",
            'api_access': self.supports_api_(),
            'multi_user': self.supports_multi_user_(),
            'premium_support': self.has_premium_support_(),
            'sla_guarantee': self.has_sla_(),
            'sso': self.has_sso_(),
            'audit_logging': self.has_audit_logging_(),
            'sales_contact_required': self.requires_sales_contact_(),
            'features': [feature.replace('_', ' ').title() for feature in self.get_features_list()]
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert tier to dictionary for API responses"""
        data = {
            'id': self.id,
            'name': self.name,
            'display_name': self.display_name,
            'description': self.description,
            'tier_type': self.tier_type.value,
            'status': self.status.value,
            'is_active': self.is_active,
            'is_deprecated': self.is_deprecated,
            'is_trial_available': self.is_trial_available,
            'is_publicly_available': self.is_publicly_available,
            'is_custom': self.is_custom,
            'is_throttled': self.is_throttled,
            'is_popular': self.is_popular,
            'marketing_label': self.marketing_label,
            'sales_contact_required': self.sales_contact_required,
            'record_count_limit': self.record_count_limit,
            'record_count_limit_time': self.record_count_limit_time.value,
            'data_source_count_limit': self.data_source_count_limit,
            'api_request_limit': self.api_request_limit,
            'api_request_limit_time': self.api_request_limit_time.value if self.api_request_limit_time else None,
            'storage_limit_gb': float(self.storage_limit_gb) if self.storage_limit_gb else None,
            'member_count_limit': self.member_count_limit,
            'admin_count_limit': self.admin_count_limit,
            'project_count_limit': self.project_count_limit,
            'concurrent_jobs_limit': self.concurrent_jobs_limit,
            'export_limit': self.export_limit,
            'export_limit_time': self.export_limit_time.value if self.export_limit_time else None,
            'retention_days': self.retention_days,
            'max_file_size_mb': self.max_file_size_mb,
            'bandwidth_limit_gb': float(self.bandwidth_limit_gb) if self.bandwidth_limit_gb else None,
            'trial_period_days': self.trial_period_days,
            'trial_record_limit': self.trial_record_limit,
            'trial_data_source_limit': self.trial_data_source_limit,
            'trial_member_limit': self.trial_member_limit,
            'monthly_price': float(self.monthly_price),
            'yearly_price': float(self.yearly_price),
            'per_user_price': float(self.per_user_price),
            'billing_cycle': self.billing_cycle,
            'min_commitment_months': self.min_commitment_months,
            'sort_order': self.sort_order,
            'features': self.get_features_list(),
            'display_info': self.get_display_info(),
            'rate_limit_id': self.rate_limit_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        
        if include_sensitive:
            data.update({
                'setup_fee': float(self.setup_fee),
                'overage_rate': float(self.overage_rate),
                'max_discount_percentage': float(self.max_discount_percentage),
                'upgrade_tier_id': self.upgrade_tier_id,
                'downgrade_tier_id': self.downgrade_tier_id,
                'throttle_reason': self.throttle_reason.value if self.throttle_reason else None,
                'throttle_until': self.throttle_until.isoformat() if self.throttle_until else None,
                'deprecated_at': self.deprecated_at.isoformat() if self.deprecated_at else None,
                'archived_at': self.archived_at.isoformat() if self.archived_at else None
            })
        
        return data
    
    def to_json(self, include_sensitive: bool = False) -> str:
        """Convert tier to JSON string"""
        return json.dumps(self.to_dict(include_sensitive=include_sensitive), indent=2)
    
    @hybrid_property
    def is_available(self) -> bool:
        """Hybrid property to check if tier is available"""
        return self.active_() and self.publicly_available_() and not self.throttled_()
    
    @hybrid_property
    def yearly_savings(self) -> Decimal:
        """Calculate yearly savings compared to monthly billing"""
        if self.yearly_price <= 0 or self.monthly_price <= 0:
            return Decimal('0.00')
        monthly_yearly_cost = self.monthly_price * 12
        return monthly_yearly_cost - self.yearly_price
    
    @hybrid_property
    def yearly_savings_percentage(self) -> float:
        """Calculate yearly savings percentage"""
        if self.yearly_price <= 0 or self.monthly_price <= 0:
            return 0.0
        monthly_yearly_cost = self.monthly_price * 12
        return float((monthly_yearly_cost - self.yearly_price) / monthly_yearly_cost * 100)
    
    @hybrid_property
    def effective_monthly_cost(self) -> Decimal:
        """Get effective monthly cost (yearly price / 12 if yearly billing available)"""
        if self.yearly_price > 0:
            return self.yearly_price / 12
        return self.monthly_price