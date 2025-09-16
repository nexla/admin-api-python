"""
Subscription Model - Service subscription and plan management.
Manages service subscriptions, pricing tiers, and feature access with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Text, Numeric, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum as PyEnum
from decimal import Decimal
from ..database import Base


class SubscriptionStatus(PyEnum):
    """Subscription status enumeration"""
    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    SUSPENDED = "SUSPENDED"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"
    TRIAL = "TRIAL"


class SubscriptionType(PyEnum):
    """Subscription type enumeration"""
    BASIC = "BASIC"
    PROFESSIONAL = "PROFESSIONAL"
    ENTERPRISE = "ENTERPRISE"
    CUSTOM = "CUSTOM"
    ADD_ON = "ADD_ON"


class BillingInterval(PyEnum):
    """Billing interval enumeration"""
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    YEARLY = "YEARLY"
    ONE_TIME = "ONE_TIME"


class Subscription(Base):
    __tablename__ = "subscriptions"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    billing_account_id = Column(Integer, ForeignKey("billing_accounts.id"), nullable=False, index=True)
    plan_id = Column(String(100), nullable=False, index=True)  # Reference to pricing plan
    
    # Subscription details
    subscription_type = Column(SQLEnum(SubscriptionType), nullable=False, index=True)
    status = Column(SQLEnum(SubscriptionStatus), default=SubscriptionStatus.PENDING, index=True)
    external_id = Column(String(100), unique=True, nullable=True, index=True)  # External billing system ID
    
    # Pricing and billing
    base_price = Column(Numeric(precision=10, scale=2), nullable=False)
    current_price = Column(Numeric(precision=10, scale=2), nullable=False)
    currency = Column(String(3), default="USD")
    billing_interval = Column(SQLEnum(BillingInterval), default=BillingInterval.MONTHLY, index=True)
    
    # Usage and limits
    usage_limits = Column(JSON)  # JSON with usage limits and quotas
    current_usage = Column(JSON)  # JSON with current usage metrics
    overage_rate = Column(Numeric(precision=8, scale=4))  # Rate per unit over limit
    included_features = Column(JSON)  # JSON list of included features
    
    # Subscription lifecycle
    trial_days = Column(Integer, default=0)
    auto_renew = Column(Boolean, default=True)
    proration_enabled = Column(Boolean, default=True)
    cancellation_policy = Column(String(50), default="END_OF_PERIOD")  # IMMEDIATE, END_OF_PERIOD
    
    # Discount and promotional
    discount_percentage = Column(Numeric(precision=5, scale=2), default=0)
    discount_amount = Column(Numeric(precision=10, scale=2), default=0)
    promotional_code = Column(String(50))
    discount_end_date = Column(DateTime)
    
    # Contract terms
    contract_length_months = Column(Integer)
    minimum_commitment_months = Column(Integer, default=1)
    early_termination_fee = Column(Numeric(precision=10, scale=2))
    renewal_terms = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    started_at = Column(DateTime, index=True)
    trial_end_date = Column(DateTime, index=True)
    current_period_start = Column(DateTime, index=True)
    current_period_end = Column(DateTime, index=True)
    cancelled_at = Column(DateTime)
    expires_at = Column(DateTime, index=True)
    
    # Metadata
    subscription_metadata = Column(JSON)
    notes = Column(Text)
    tags = Column(String(500))  # Comma-separated tags
    
    # Relationships
    org = relationship("Org", back_populates="subscriptions")
    billing_account = relationship("BillingAccount", back_populates="subscriptions")
    
    # Rails business logic constants
    DEFAULT_TRIAL_DAYS = 14
    MAX_OVERAGE_MULTIPLIER = 2.0  # Max 200% of base price from overages
    GRACE_PERIOD_DAYS = 3
    RENEWAL_NOTICE_DAYS = 30
    
    # Rails predicate methods
    def active_(self) -> bool:
        """Rails predicate: Check if subscription is active"""
        return (self.status == SubscriptionStatus.ACTIVE and
                (not self.expires_at or self.expires_at > datetime.utcnow()))
    
    def trial_(self) -> bool:
        """Rails predicate: Check if subscription is in trial"""
        return self.status == SubscriptionStatus.TRIAL
    
    def cancelled_(self) -> bool:
        """Rails predicate: Check if subscription is cancelled"""
        return self.status == SubscriptionStatus.CANCELLED
    
    def expired_(self) -> bool:
        """Rails predicate: Check if subscription has expired"""
        return (self.status == SubscriptionStatus.EXPIRED or
                (self.expires_at and self.expires_at <= datetime.utcnow()))
    
    def suspended_(self) -> bool:
        """Rails predicate: Check if subscription is suspended"""
        return self.status == SubscriptionStatus.SUSPENDED
    
    def trial_active_(self) -> bool:
        """Rails predicate: Check if trial is currently active"""
        return (self.trial_() and 
                self.trial_end_date and 
                self.trial_end_date > datetime.utcnow())
    
    def trial_expired_(self) -> bool:
        """Rails predicate: Check if trial has expired"""
        return (self.trial_end_date and 
                self.trial_end_date <= datetime.utcnow())
    
    def auto_renew_enabled_(self) -> bool:
        """Rails predicate: Check if auto-renewal is enabled"""
        return self.auto_renew and self.active_()
    
    def has_discount_(self) -> bool:
        """Rails predicate: Check if subscription has active discount"""
        if not (self.discount_percentage > 0 or self.discount_amount > 0):
            return False
        
        if self.discount_end_date:
            return self.discount_end_date > datetime.utcnow()
        
        return True
    
    def discount_expired_(self) -> bool:
        """Rails predicate: Check if discount has expired"""
        return (self.discount_end_date and 
                self.discount_end_date <= datetime.utcnow())
    
    def in_grace_period_(self) -> bool:
        """Rails predicate: Check if subscription is in grace period"""
        if not self.current_period_end:
            return False
        
        grace_end = self.current_period_end + timedelta(days=self.GRACE_PERIOD_DAYS)
        return (self.current_period_end <= datetime.utcnow() <= grace_end)
    
    def needs_renewal_notice_(self) -> bool:
        """Rails predicate: Check if renewal notice should be sent"""
        if not self.current_period_end or not self.auto_renew:
            return False
        
        notice_date = self.current_period_end - timedelta(days=self.RENEWAL_NOTICE_DAYS)
        return datetime.utcnow() >= notice_date
    
    def over_usage_limit_(self, feature: str = None) -> bool:
        """Rails predicate: Check if over usage limits"""
        if not self.usage_limits or not self.current_usage:
            return False
        
        usage_limits = self.get_usage_limits()
        current_usage = self.get_current_usage()
        
        if feature:
            return (feature in usage_limits and 
                   feature in current_usage and
                   current_usage[feature] > usage_limits[feature])
        
        # Check any feature over limit
        for feature_name, limit in usage_limits.items():
            if feature_name in current_usage and current_usage[feature_name] > limit:
                return True
        
        return False
    
    def can_upgrade_(self) -> bool:
        """Rails predicate: Check if subscription can be upgraded"""
        return self.active_() and self.subscription_type != SubscriptionType.ENTERPRISE
    
    def can_downgrade_(self) -> bool:
        """Rails predicate: Check if subscription can be downgraded"""
        return self.active_() and self.subscription_type != SubscriptionType.BASIC
    
    # Rails business logic methods
    def activate_subscription(self) -> bool:
        """Activate subscription (Rails pattern)"""
        if self.status in [SubscriptionStatus.PENDING, SubscriptionStatus.TRIAL]:
            self.status = SubscriptionStatus.ACTIVE
            self.started_at = datetime.utcnow()
            self.calculate_current_period()
            return True
        return False
    
    def start_trial(self, trial_days: int = None) -> bool:
        """Start trial period (Rails pattern)"""
        if self.status != SubscriptionStatus.PENDING:
            return False
        
        trial_days = trial_days or self.trial_days or self.DEFAULT_TRIAL_DAYS
        
        self.status = SubscriptionStatus.TRIAL
        self.started_at = datetime.utcnow()
        self.trial_end_date = datetime.utcnow() + timedelta(days=trial_days)
        self.current_period_start = datetime.utcnow()
        self.current_period_end = self.trial_end_date
        
        return True
    
    def convert_from_trial(self) -> bool:
        """Convert from trial to active subscription (Rails pattern)"""
        if not self.trial_():
            return False
        
        self.status = SubscriptionStatus.ACTIVE
        self.calculate_current_period()
        return True
    
    def cancel_subscription(self, immediate: bool = False, reason: str = None) -> bool:
        """Cancel subscription (Rails pattern)"""
        if not self.active_():
            return False
        
        self.status = SubscriptionStatus.CANCELLED
        self.cancelled_at = datetime.utcnow()
        self.auto_renew = False
        
        if immediate or self.cancellation_policy == "IMMEDIATE":
            self.expires_at = datetime.utcnow()
        else:
            self.expires_at = self.current_period_end
        
        if reason:
            self.notes = f"{self.notes or ''}\nCancelled: {reason}".strip()
        
        return True
    
    def suspend_subscription(self, reason: str = None) -> bool:
        """Suspend subscription (Rails pattern)"""
        if self.active_():
            self.status = SubscriptionStatus.SUSPENDED
            if reason:
                self.notes = f"{self.notes or ''}\nSuspended: {reason}".strip()
            return True
        return False
    
    def reactivate_subscription(self) -> bool:
        """Reactivate suspended subscription (Rails pattern)"""
        if self.suspended_():
            self.status = SubscriptionStatus.ACTIVE
            return True
        return False
    
    def upgrade_plan(self, new_plan_id: str, new_price: Decimal, 
                    new_type: SubscriptionType = None, prorate: bool = None) -> bool:
        """Upgrade subscription plan (Rails pattern)"""
        if not self.can_upgrade_():
            return False
        
        old_price = self.current_price
        prorate = prorate if prorate is not None else self.proration_enabled
        
        self.plan_id = new_plan_id
        self.current_price = new_price
        
        if new_type:
            self.subscription_type = new_type
        
        if prorate:
            self._calculate_proration_credit(old_price, new_price)
        
        return True
    
    def downgrade_plan(self, new_plan_id: str, new_price: Decimal,
                      new_type: SubscriptionType = None, effective_date: datetime = None) -> bool:
        """Downgrade subscription plan (Rails pattern)"""
        if not self.can_downgrade_():
            return False
        
        # Downgrades typically happen at end of current period
        effective_date = effective_date or self.current_period_end
        
        # Store pending downgrade info
        pending_changes = {
            'plan_id': new_plan_id,
            'price': str(new_price),
            'type': new_type.value if new_type else None,
            'effective_date': effective_date.isoformat()
        }
        
        metadata = self.get_subscription_metadata()
        metadata['pending_downgrade'] = pending_changes
        self.subscription_metadata = metadata
        
        return True
    
    def apply_discount(self, percentage: Decimal = None, amount: Decimal = None,
                      promo_code: str = None, end_date: datetime = None) -> bool:
        """Apply discount to subscription (Rails pattern)"""
        if percentage:
            self.discount_percentage = percentage
        
        if amount:
            self.discount_amount = amount
        
        if promo_code:
            self.promotional_code = promo_code
        
        if end_date:
            self.discount_end_date = end_date
        
        return True
    
    def remove_discount(self) -> None:
        """Remove discount from subscription (Rails pattern)"""
        self.discount_percentage = 0
        self.discount_amount = 0
        self.promotional_code = None
        self.discount_end_date = None
    
    def calculate_current_period(self) -> None:
        """Calculate current billing period dates (Rails pattern)"""
        if not self.started_at:
            self.started_at = datetime.utcnow()
        
        start_date = self.current_period_end or self.started_at
        self.current_period_start = start_date
        
        if self.billing_interval == BillingInterval.MONTHLY:
            if start_date.month == 12:
                self.current_period_end = start_date.replace(year=start_date.year + 1, month=1)
            else:
                self.current_period_end = start_date.replace(month=start_date.month + 1)
        elif self.billing_interval == BillingInterval.QUARTERLY:
            self.current_period_end = start_date + timedelta(days=90)
        elif self.billing_interval == BillingInterval.YEARLY:
            self.current_period_end = start_date.replace(year=start_date.year + 1)
    
    def renew_subscription(self) -> bool:
        """Renew subscription for next period (Rails pattern)"""
        if not self.auto_renew_enabled_():
            return False
        
        # Move to next period
        self.calculate_current_period()
        
        # Apply any pending changes
        self._apply_pending_changes()
        
        # Reset usage for new period
        self._reset_usage_counters()
        
        return True
    
    def calculate_effective_price(self) -> Decimal:
        """Calculate effective price after discounts (Rails pattern)"""
        effective_price = self.current_price
        
        if self.has_discount_():
            if self.discount_percentage > 0:
                discount = (self.current_price * self.discount_percentage) / 100
                effective_price -= discount
            
            if self.discount_amount > 0:
                effective_price -= self.discount_amount
        
        return max(Decimal('0.00'), effective_price)
    
    def calculate_overage_charges(self) -> Decimal:
        """Calculate overage charges for current period (Rails pattern)"""
        if not self.overage_rate or not self.over_usage_limit_():
            return Decimal('0.00')
        
        total_overage = Decimal('0.00')
        usage_limits = self.get_usage_limits()
        current_usage = self.get_current_usage()
        
        for feature, limit in usage_limits.items():
            if feature in current_usage and current_usage[feature] > limit:
                overage_units = current_usage[feature] - limit
                total_overage += Decimal(str(overage_units)) * self.overage_rate
        
        # Cap overage at maximum multiplier
        max_overage = self.current_price * Decimal(str(self.MAX_OVERAGE_MULTIPLIER))
        return min(total_overage, max_overage)
    
    def get_usage_limits(self) -> Dict[str, int]:
        """Get usage limits as dictionary (Rails pattern)"""
        if not self.usage_limits:
            return {}
        
        if isinstance(self.usage_limits, str):
            import json
            return json.loads(self.usage_limits)
        
        return self.usage_limits
    
    def get_current_usage(self) -> Dict[str, int]:
        """Get current usage as dictionary (Rails pattern)"""
        if not self.current_usage:
            return {}
        
        if isinstance(self.current_usage, str):
            import json
            return json.loads(self.current_usage)
        
        return self.current_usage
    
    def get_included_features(self) -> List[str]:
        """Get included features as list (Rails pattern)"""
        if not self.included_features:
            return []
        
        if isinstance(self.included_features, str):
            import json
            return json.loads(self.included_features)
        
        return self.included_features
    
    def get_subscription_metadata(self) -> Dict[str, Any]:
        """Get subscription metadata as dictionary (Rails pattern)"""
        if not self.subscription_metadata:
            return {}
        
        if isinstance(self.subscription_metadata, str):
            import json
            return json.loads(self.subscription_metadata)
        
        return self.subscription_metadata
    
    def update_usage(self, feature: str, usage_amount: int) -> None:
        """Update usage counter for feature (Rails pattern)"""
        current_usage = self.get_current_usage()
        current_usage[feature] = usage_amount
        self.current_usage = current_usage
    
    def increment_usage(self, feature: str, increment: int = 1) -> int:
        """Increment usage counter for feature (Rails pattern)"""
        current_usage = self.get_current_usage()
        current_usage[feature] = current_usage.get(feature, 0) + increment
        self.current_usage = current_usage
        return current_usage[feature]
    
    def _calculate_proration_credit(self, old_price: Decimal, new_price: Decimal) -> Decimal:
        """Calculate proration credit for plan changes (helper)"""
        if not self.current_period_end:
            return Decimal('0.00')
        
        # Calculate remaining days in period
        remaining_days = (self.current_period_end - datetime.utcnow()).days
        
        if remaining_days <= 0:
            return Decimal('0.00')
        
        # Calculate daily rates
        period_days = (self.current_period_end - self.current_period_start).days
        old_daily_rate = old_price / period_days
        new_daily_rate = new_price / period_days
        
        # Calculate proration
        proration = (new_daily_rate - old_daily_rate) * remaining_days
        return proration
    
    def _apply_pending_changes(self) -> None:
        """Apply pending subscription changes (helper)"""
        metadata = self.get_subscription_metadata()
        
        if 'pending_downgrade' in metadata:
            pending = metadata['pending_downgrade']
            effective_date = datetime.fromisoformat(pending['effective_date'])
            
            if datetime.utcnow() >= effective_date:
                self.plan_id = pending['plan_id']
                self.current_price = Decimal(pending['price'])
                if pending.get('type'):
                    self.subscription_type = SubscriptionType(pending['type'])
                
                # Remove pending change
                del metadata['pending_downgrade']
                self.subscription_metadata = metadata
    
    def _reset_usage_counters(self) -> None:
        """Reset usage counters for new billing period (helper)"""
        # This would reset usage counters to 0 for new period
        # For now, just clear current usage
        self.current_usage = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert subscription to dictionary for API responses"""
        return {
            'id': self.id,
            'org_id': self.org_id,
            'billing_account_id': self.billing_account_id,
            'plan_id': self.plan_id,
            'subscription_type': self.subscription_type.value if self.subscription_type else None,
            'status': self.status.value if self.status else None,
            'base_price': float(self.base_price) if self.base_price else 0.0,
            'current_price': float(self.current_price) if self.current_price else 0.0,
            'effective_price': float(self.calculate_effective_price()),
            'currency': self.currency,
            'billing_interval': self.billing_interval.value if self.billing_interval else None,
            'auto_renew': self.auto_renew,
            'trial_days': self.trial_days,
            'discount_percentage': float(self.discount_percentage) if self.discount_percentage else 0.0,
            'discount_amount': float(self.discount_amount) if self.discount_amount else 0.0,
            'promotional_code': self.promotional_code,
            'usage_limits': self.get_usage_limits(),
            'current_usage': self.get_current_usage(),
            'included_features': self.get_included_features(),
            'overage_charges': float(self.calculate_overage_charges()),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'trial_end_date': self.trial_end_date.isoformat() if self.trial_end_date else None,
            'current_period_start': self.current_period_start.isoformat() if self.current_period_start else None,
            'current_period_end': self.current_period_end.isoformat() if self.current_period_end else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'cancelled_at': self.cancelled_at.isoformat() if self.cancelled_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'active': self.active_(),
            'trial': self.trial_(),
            'trial_active': self.trial_active_(),
            'cancelled': self.cancelled_(),
            'expired': self.expired_(),
            'suspended': self.suspended_(),
            'has_discount': self.has_discount_(),
            'in_grace_period': self.in_grace_period_(),
            'needs_renewal_notice': self.needs_renewal_notice_(),
            'over_usage_limit': self.over_usage_limit_(),
            'can_upgrade': self.can_upgrade_(),
            'can_downgrade': self.can_downgrade_()
        }
    
    @classmethod
    def create_subscription(cls, org, billing_account, plan_id: str, 
                          subscription_type: SubscriptionType, price: Decimal, **kwargs):
        """Create new subscription (Rails pattern)"""
        subscription = cls(
            org_id=org.id if hasattr(org, 'id') else org,
            billing_account_id=billing_account.id if hasattr(billing_account, 'id') else billing_account,
            plan_id=plan_id,
            subscription_type=subscription_type,
            base_price=price,
            current_price=price,
            **kwargs
        )
        
        return subscription
    
    @classmethod
    def find_active_for_org(cls, org_id: int, session=None):
        """Find active subscriptions for organization (Rails pattern)"""
        # This would query active subscriptions when session is available
        # For now, return empty list as placeholder
        return []