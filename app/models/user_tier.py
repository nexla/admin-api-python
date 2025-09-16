from enum import Enum as PyEnum
from typing import Optional, Dict, List, Any, Union, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from dataclasses import dataclass
import json

from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Enum as SQLEnum, Boolean, Text, Numeric, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property

from ..database import Base


logger = logging.getLogger(__name__)


class TimePeriods(PyEnum):
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


class TierTypes(PyEnum):
    FREE = "FREE"
    TRIAL = "TRIAL"
    STARTER = "STARTER"
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
            self.STARTER: 2,
            self.PROFESSIONAL: 3,
            self.BUSINESS: 4,
            self.ENTERPRISE: 5,
            self.CUSTOM: 6,
            self.LEGACY: 7
        }
        return mapping[self]


class TierStatuses(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DEPRECATED = "DEPRECATED"
    ARCHIVED = "ARCHIVED"
    MAINTENANCE = "MAINTENANCE"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class FeatureFlags(PyEnum):
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


@dataclass
class LimitInfo:
    """Information about a specific limit"""
    limit_type: str
    current_value: int
    max_value: Optional[int]
    percentage_used: float
    time_period: Optional[TimePeriods] = None
    is_unlimited: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'limit_type': self.limit_type,
            'current_value': self.current_value,
            'max_value': self.max_value,
            'percentage_used': self.percentage_used,
            'time_period': self.time_period.value if self.time_period else None,
            'is_unlimited': self.is_unlimited
        }


@dataclass 
class TierMetrics:
    """Metrics for tier usage and performance"""
    total_users: int = 0
    active_users: int = 0
    trial_users: int = 0
    conversion_rate: float = 0.0
    average_usage: Dict[str, float] = None
    revenue_impact: Decimal = Decimal('0.00')
    
    def __post_init__(self):
        if self.average_usage is None:
            self.average_usage = {}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_users': self.total_users,
            'active_users': self.active_users,
            'trial_users': self.trial_users,
            'conversion_rate': float(self.conversion_rate),
            'average_usage': self.average_usage,
            'revenue_impact': float(self.revenue_impact)
        }


class UserTier(Base):
    __tablename__ = "user_tiers"
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_user_tier_name', 'name'),
        Index('idx_user_tier_status', 'status'),
        Index('idx_user_tier_type', 'tier_type'),
        Index('idx_user_tier_active', 'status', 'is_active'),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Core tier information
    name = Column(String(255), nullable=False, unique=True, index=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text)
    tier_type = Column(SQLEnum(TierTypes), nullable=False, default=TierTypes.FREE)
    status = Column(SQLEnum(TierStatuses), nullable=False, default=TierStatuses.ACTIVE)
    
    # State flags
    is_active = Column(Boolean, nullable=False, default=True)
    is_deprecated = Column(Boolean, nullable=False, default=False)
    is_trial_available = Column(Boolean, nullable=False, default=False)
    is_publicly_available = Column(Boolean, nullable=False, default=True)
    is_custom = Column(Boolean, nullable=False, default=False)
    
    # Core limits
    record_count_limit = Column(BigInteger)  # null = unlimited
    record_count_limit_time = Column(SQLEnum(TimePeriods), nullable=False, default=TimePeriods.DAILY)
    data_source_count_limit = Column(Integer)  # null = unlimited
    api_request_limit = Column(BigInteger)  # null = unlimited
    api_request_limit_time = Column(SQLEnum(TimePeriods), nullable=False, default=TimePeriods.DAILY)
    storage_limit_gb = Column(Numeric(10, 2))  # null = unlimited
    
    # Advanced limits
    concurrent_jobs_limit = Column(Integer, default=1)
    export_limit = Column(Integer)  # exports per time period
    export_limit_time = Column(SQLEnum(TimePeriods), nullable=False, default=TimePeriods.DAILY)
    retention_days = Column(Integer, default=30)
    max_file_size_mb = Column(Integer, default=100)
    
    # Trial configuration  
    trial_period_days = Column(Integer, default=14)
    trial_record_limit = Column(BigInteger)
    trial_data_source_limit = Column(Integer, default=1)
    
    # Pricing information
    monthly_price = Column(Numeric(10, 2), default=Decimal('0.00'))
    yearly_price = Column(Numeric(10, 2), default=Decimal('0.00'))
    setup_fee = Column(Numeric(10, 2), default=Decimal('0.00'))
    overage_rate = Column(Numeric(10, 4), default=Decimal('0.00'))  # per record over limit
    
    # Feature flags (JSON storage)
    feature_flags = Column(Text)  # JSON list of enabled features
    
    # Priority and ordering
    sort_order = Column(Integer, default=0)
    upgrade_tier_id = Column(Integer)  # ID of tier to upgrade to
    downgrade_tier_id = Column(Integer)  # ID of tier to downgrade to
    
    # Lifecycle timestamps
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    deprecated_at = Column(DateTime)
    archived_at = Column(DateTime)
    
    # Relationships
    users = relationship("User", back_populates="user_tier")
    
    # Constants
    UNLIMITED = -1
    DEFAULT_TRIAL_DAYS = 14
    MAX_TRIAL_DAYS = 90
    MIN_MONTHLY_PRICE = Decimal('0.00')
    MAX_MONTHLY_PRICE = Decimal('99999.99')
    
    def __repr__(self) -> str:
        return f"<UserTier(id={self.id}, name='{self.name}', type='{self.tier_type.value}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        return f"{self.display_name} ({self.name})"
    
    # === Rails-style Predicate Methods ===
    
    def active_(self) -> bool:
        """Check if tier is active and available"""
        return self.status == TierStatuses.ACTIVE and self.is_active and not self.is_deprecated
    
    def inactive_(self) -> bool:
        """Check if tier is inactive"""
        return not self.active_()
    
    def deprecated_(self) -> bool:
        """Check if tier is deprecated"""
        return self.is_deprecated or self.status == TierStatuses.DEPRECATED
    
    def archived_(self) -> bool:
        """Check if tier is archived"""
        return self.status == TierStatuses.ARCHIVED or self.archived_at is not None
    
    def trial_available_(self) -> bool:
        """Check if trial is available for this tier"""
        return self.is_trial_available and self.active_() and self.trial_period_days > 0
    
    def publicly_available_(self) -> bool:
        """Check if tier is publicly available for signup"""
        return self.is_publicly_available and self.active_() and not self.is_custom
    
    def custom_(self) -> bool:
        """Check if this is a custom tier"""
        return self.is_custom or self.tier_type == TierTypes.CUSTOM
    
    def free_(self) -> bool:
        """Check if this is a free tier"""
        return self.tier_type == TierTypes.FREE or self.monthly_price == Decimal('0.00')
    
    def paid_(self) -> bool:
        """Check if this is a paid tier"""
        return not self.free_()
    
    def enterprise_(self) -> bool:
        """Check if this is an enterprise tier"""
        return self.tier_type == TierTypes.ENTERPRISE
    
    def legacy_(self) -> bool:
        """Check if this is a legacy tier"""
        return self.tier_type == TierTypes.LEGACY
    
    def maintenance_(self) -> bool:
        """Check if tier is in maintenance mode"""
        return self.status == TierStatuses.MAINTENANCE
    
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
    
    def has_feature_(self, feature: FeatureFlags) -> bool:
        """Check if tier has specific feature enabled"""
        if not self.feature_flags:
            return False
        try:
            features = json.loads(self.feature_flags)
            return feature.value in features
        except (json.JSONDecodeError, TypeError):
            return False
    
    def supports_api_(self) -> bool:
        """Check if tier supports API access"""
        return self.has_feature_(FeatureFlags.API_ACCESS)
    
    def has_premium_support_(self) -> bool:
        """Check if tier includes premium support"""
        return self.has_feature_(FeatureFlags.PREMIUM_SUPPORT)
    
    def has_sla_(self) -> bool:
        """Check if tier includes SLA guarantee"""
        return self.has_feature_(FeatureFlags.SLA_GUARANTEE)
    
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
    
    # === Rails-style Bang Methods ===
    
    def activate_(self) -> None:
        """Activate the tier"""
        if self.active_():
            return
        if self.archived_():
            raise ValueError("Cannot activate archived tier")
        
        self.status = TierStatuses.ACTIVE
        self.is_active = True
        self.updated_at = datetime.now()
        logger.info(f"Activated tier {self.name}")
    
    def deactivate_(self) -> None:
        """Deactivate the tier"""
        if not self.active_():
            return
        
        self.status = TierStatuses.INACTIVE
        self.is_active = False
        self.updated_at = datetime.now()
        logger.info(f"Deactivated tier {self.name}")
    
    def deprecate_(self, reason: str = None) -> None:
        """Deprecate the tier"""
        if self.deprecated_():
            return
        
        self.is_deprecated = True
        self.status = TierStatuses.DEPRECATED
        self.deprecated_at = datetime.now()
        self.updated_at = datetime.now()
        logger.info(f"Deprecated tier {self.name}: {reason or 'No reason specified'}")
    
    def archive_(self) -> None:
        """Archive the tier"""
        if self.archived_():
            return
        
        self.status = TierStatuses.ARCHIVED
        self.is_active = False
        self.archived_at = datetime.now()
        self.updated_at = datetime.now()
        logger.info(f"Archived tier {self.name}")
    
    def enable_trial_(self, days: int = None) -> None:
        """Enable trial for this tier"""
        if days is None:
            days = self.DEFAULT_TRIAL_DAYS
        if days > self.MAX_TRIAL_DAYS:
            raise ValueError(f"Trial period cannot exceed {self.MAX_TRIAL_DAYS} days")
        
        self.is_trial_available = True
        self.trial_period_days = days
        self.updated_at = datetime.now()
    
    def disable_trial_(self) -> None:
        """Disable trial for this tier"""
        self.is_trial_available = False
        self.updated_at = datetime.now()
    
    def add_feature_(self, feature: FeatureFlags) -> None:
        """Add feature to tier"""
        features = self.get_features_list()
        if feature.value not in features:
            features.append(feature.value)
            self.feature_flags = json.dumps(features)
            self.updated_at = datetime.now()
    
    def remove_feature_(self, feature: FeatureFlags) -> None:
        """Remove feature from tier"""
        features = self.get_features_list()
        if feature.value in features:
            features.remove(feature.value)
            self.feature_flags = json.dumps(features)
            self.updated_at = datetime.now()
    
    def update_pricing_(self, monthly_price: Decimal = None, yearly_price: Decimal = None, 
                       setup_fee: Decimal = None, overage_rate: Decimal = None) -> None:
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
        
        self.updated_at = datetime.now()
    
    def update_limits_(self, **limits) -> None:
        """Update tier limits"""
        for limit_type, value in limits.items():
            if hasattr(self, limit_type):
                setattr(self, limit_type, value)
        self.updated_at = datetime.now()
    
    # === Class Methods (Rails-style Scopes) ===
    
    @classmethod
    def active(cls, session):
        """Get all active tiers"""
        return session.query(cls).filter(
            cls.status == TierStatuses.ACTIVE,
            cls.is_active == True,
            cls.is_deprecated == False
        ).order_by(cls.sort_order)
    
    @classmethod
    def publicly_available(cls, session):
        """Get all publicly available tiers"""
        return cls.active(session).filter(
            cls.is_publicly_available == True,
            cls.is_custom == False
        )
    
    @classmethod
    def trial_enabled(cls, session):
        """Get all tiers with trial enabled"""
        return cls.active(session).filter(cls.is_trial_available == True)
    
    @classmethod
    def by_type(cls, session, tier_type: TierTypes):
        """Get tiers by type"""
        return session.query(cls).filter(cls.tier_type == tier_type)
    
    @classmethod
    def paid_tiers(cls, session):
        """Get all paid tiers"""
        return cls.active(session).filter(cls.monthly_price > 0)
    
    @classmethod
    def free_tiers(cls, session):
        """Get all free tiers"""
        return cls.active(session).filter(cls.monthly_price == 0)
    
    @classmethod
    def by_price_range(cls, session, min_price: Decimal, max_price: Decimal):
        """Get tiers within price range"""
        return cls.active(session).filter(
            cls.monthly_price >= min_price,
            cls.monthly_price <= max_price
        ).order_by(cls.monthly_price)
    
    @classmethod
    def find_by_name(cls, session, name: str):
        """Find tier by name"""
        return session.query(cls).filter(cls.name == name).first()
    
    @classmethod
    def find_upgrade_path(cls, session, from_tier_id: int):
        """Find upgrade path from given tier"""
        current = session.query(cls).filter(cls.id == from_tier_id).first()
        if not current or not current.upgrade_tier_id:
            return None
        return session.query(cls).filter(cls.id == current.upgrade_tier_id).first()
    
    @classmethod
    def find_downgrade_path(cls, session, from_tier_id: int):
        """Find downgrade path from given tier"""
        current = session.query(cls).filter(cls.id == from_tier_id).first()
        if not current or not current.downgrade_tier_id:
            return None
        return session.query(cls).filter(cls.id == current.downgrade_tier_id).first()
    
    @classmethod
    def get_default_free_tier(cls, session):
        """Get default free tier"""
        return cls.free_tiers(session).filter(cls.tier_type == TierTypes.FREE).first()
    
    @classmethod
    def build_from_input(cls, input_data: dict):
        """Create new UserTier from input data (Rails pattern)"""
        if not input_data.get('name'):
            raise ValueError("Tier name is required")
        if not input_data.get('display_name'):
            raise ValueError("Tier display name is required")
        
        # Set defaults
        defaults = {
            'tier_type': TierTypes.FREE,
            'status': TierStatuses.ACTIVE,
            'is_active': True,
            'is_deprecated': False,
            'is_trial_available': False,
            'is_publicly_available': True,
            'is_custom': False,
            'record_count_limit_time': TimePeriods.DAILY,
            'api_request_limit_time': TimePeriods.DAILY,
            'export_limit_time': TimePeriods.DAILY,
            'concurrent_jobs_limit': 1,
            'retention_days': 30,
            'max_file_size_mb': 100,
            'trial_period_days': cls.DEFAULT_TRIAL_DAYS,
            'trial_data_source_limit': 1,
            'monthly_price': Decimal('0.00'),
            'yearly_price': Decimal('0.00'),
            'setup_fee': Decimal('0.00'),
            'overage_rate': Decimal('0.00'),
            'sort_order': 0
        }
        
        # Merge with input data
        tier_data = {**defaults, **input_data}
        
        # Handle enum conversions
        if isinstance(tier_data.get('tier_type'), str):
            tier_data['tier_type'] = TierTypes(tier_data['tier_type'])
        if isinstance(tier_data.get('status'), str):
            tier_data['status'] = TierStatuses(tier_data['status'])
        if isinstance(tier_data.get('record_count_limit_time'), str):
            tier_data['record_count_limit_time'] = TimePeriods(tier_data['record_count_limit_time'])
        
        return cls(**tier_data)
    
    # === Instance Methods ===
    
    def update_mutable(self, input_data: dict) -> None:
        """Update mutable fields from input data"""
        if not input_data:
            return
        
        mutable_fields = {
            'display_name', 'description', 'tier_type', 'status',
            'is_active', 'is_deprecated', 'is_trial_available',
            'is_publicly_available', 'is_custom',
            'record_count_limit', 'record_count_limit_time',
            'data_source_count_limit', 'api_request_limit', 'api_request_limit_time',
            'storage_limit_gb', 'concurrent_jobs_limit', 'export_limit',
            'export_limit_time', 'retention_days', 'max_file_size_mb',
            'trial_period_days', 'trial_record_limit', 'trial_data_source_limit',
            'monthly_price', 'yearly_price', 'setup_fee', 'overage_rate',
            'sort_order', 'upgrade_tier_id', 'downgrade_tier_id'
        }
        
        for field, value in input_data.items():
            if field in mutable_fields and hasattr(self, field):
                # Handle enum conversions
                if field == 'tier_type' and isinstance(value, str):
                    value = TierTypes(value)
                elif field == 'status' and isinstance(value, str):
                    value = TierStatuses(value)
                elif field.endswith('_time') and isinstance(value, str):
                    value = TimePeriods(value)
                
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
            'file_size': self.max_file_size_mb
        }
        return limit_mapping.get(usage_type)
    
    def get_usage_info(self, current_usage: Dict[str, int]) -> Dict[str, LimitInfo]:
        """Get comprehensive usage information"""
        usage_info = {}
        
        for usage_type, current_value in current_usage.items():
            limit = self.get_limit_for_type(usage_type)
            is_unlimited = limit is None or limit == self.UNLIMITED
            percentage_used = 0.0 if is_unlimited else (current_value / limit * 100 if limit > 0 else 100)
            
            time_period = None
            if usage_type in ['records', 'api_requests', 'exports']:
                time_period = getattr(self, f"{usage_type.rstrip('s')}_limit_time", None)
            
            usage_info[usage_type] = LimitInfo(
                limit_type=usage_type,
                current_value=current_value,
                max_value=limit if not is_unlimited else None,
                percentage_used=min(percentage_used, 100.0),
                time_period=time_period,
                is_unlimited=is_unlimited
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
    
    def get_upgrade_benefits(self, session) -> List[str]:
        """Get list of benefits from upgrading to next tier"""
        if not self.upgrade_tier_id:
            return []
        
        upgrade_tier = session.query(UserTier).filter(UserTier.id == self.upgrade_tier_id).first()
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
        
        # Compare features
        current_features = set(self.get_features_list())
        upgrade_features = set(upgrade_tier.get_features_list())
        new_features = upgrade_features - current_features
        
        for feature in new_features:
            try:
                feature_enum = FeatureFlags(feature)
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
            'trial_available': self.trial_available_(),
            'trial_days': self.trial_period_days if self.trial_available_() else None,
            'record_limit': "Unlimited" if self.unlimited_records_() else f"{self.record_count_limit:,}",
            'record_period': self.record_count_limit_time.get_display_name() if not self.unlimited_records_() else None,
            'data_source_limit': "Unlimited" if self.unlimited_data_sources_() else str(self.data_source_count_limit),
            'storage_limit': "Unlimited" if self.unlimited_storage_() else f"{float(self.storage_limit_gb)} GB",
            'api_access': self.supports_api_(),
            'premium_support': self.has_premium_support_(),
            'sla_guarantee': self.has_sla_(),
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
            'record_count_limit': self.record_count_limit,
            'record_count_limit_time': self.record_count_limit_time.value,
            'data_source_count_limit': self.data_source_count_limit,
            'api_request_limit': self.api_request_limit,
            'api_request_limit_time': self.api_request_limit_time.value if self.api_request_limit_time else None,
            'storage_limit_gb': float(self.storage_limit_gb) if self.storage_limit_gb else None,
            'concurrent_jobs_limit': self.concurrent_jobs_limit,
            'export_limit': self.export_limit,
            'export_limit_time': self.export_limit_time.value if self.export_limit_time else None,
            'retention_days': self.retention_days,
            'max_file_size_mb': self.max_file_size_mb,
            'trial_period_days': self.trial_period_days,
            'trial_record_limit': self.trial_record_limit,
            'trial_data_source_limit': self.trial_data_source_limit,
            'monthly_price': float(self.monthly_price),
            'yearly_price': float(self.yearly_price),
            'sort_order': self.sort_order,
            'features': self.get_features_list(),
            'display_info': self.get_display_info(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        
        if include_sensitive:
            data.update({
                'setup_fee': float(self.setup_fee),
                'overage_rate': float(self.overage_rate),
                'upgrade_tier_id': self.upgrade_tier_id,
                'downgrade_tier_id': self.downgrade_tier_id,
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
        return self.active_() and self.publicly_available_()
    
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