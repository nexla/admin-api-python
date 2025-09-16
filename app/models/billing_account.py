"""
BillingAccount Model - Organization billing and payment management.
Manages billing accounts, payment methods, and billing history with Rails business logic patterns.
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


class BillingStatus(PyEnum):
    """Billing status enumeration"""
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    CANCELLED = "CANCELLED"
    PAST_DUE = "PAST_DUE"
    TRIAL = "TRIAL"


class PaymentMethod(PyEnum):
    """Payment method enumeration"""
    CREDIT_CARD = "CREDIT_CARD"
    BANK_TRANSFER = "BANK_TRANSFER"
    INVOICE = "INVOICE"
    PAYPAL = "PAYPAL"
    CRYPTO = "CRYPTO"


class BillingCycle(PyEnum):
    """Billing cycle enumeration"""
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    YEARLY = "YEARLY"
    CUSTOM = "CUSTOM"


class BillingAccount(Base):
    __tablename__ = "billing_accounts"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    billing_contact_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    
    # Account identification
    account_number = Column(String(50), unique=True, nullable=False, index=True)
    external_id = Column(String(100), unique=True, nullable=True, index=True)  # External billing system ID
    
    # Billing details
    status = Column(SQLEnum(BillingStatus), default=BillingStatus.TRIAL, index=True)
    billing_cycle = Column(SQLEnum(BillingCycle), default=BillingCycle.MONTHLY, index=True)
    currency = Column(String(3), default="USD", index=True)
    
    # Contact information
    billing_email = Column(String(254), nullable=False)
    company_name = Column(String(255))
    billing_address = Column(JSON)  # JSON with address components
    tax_id = Column(String(50))  # VAT/Tax ID
    
    # Payment settings
    primary_payment_method = Column(SQLEnum(PaymentMethod), nullable=True)
    payment_method_details = Column(JSON)  # Encrypted payment details
    auto_pay_enabled = Column(Boolean, default=True)
    
    # Financial tracking
    current_balance = Column(Numeric(precision=10, scale=2), default=0)
    credit_limit = Column(Numeric(precision=10, scale=2), default=0)
    total_paid = Column(Numeric(precision=10, scale=2), default=0)
    total_outstanding = Column(Numeric(precision=10, scale=2), default=0)
    
    # Account limits and quotas
    monthly_limit = Column(Numeric(precision=10, scale=2))
    usage_threshold_warning = Column(Integer, default=80)  # Percentage
    usage_threshold_limit = Column(Integer, default=100)  # Percentage
    
    # Trial and promotional
    trial_start_date = Column(DateTime)
    trial_end_date = Column(DateTime)
    promotional_credits = Column(Numeric(precision=10, scale=2), default=0)
    discount_codes = Column(JSON)  # Applied discount codes
    
    # Billing preferences
    invoice_delivery_method = Column(String(20), default="EMAIL")  # EMAIL, POSTAL, BOTH
    payment_terms_days = Column(Integer, default=30)
    grace_period_days = Column(Integer, default=7)
    
    # Account management
    account_manager_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    notes = Column(Text)
    tags = Column(String(500))  # Comma-separated tags
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    activated_at = Column(DateTime)
    suspended_at = Column(DateTime)
    last_payment_at = Column(DateTime)
    next_billing_date = Column(DateTime, index=True)
    
    # Relationships
    org = relationship("Org", back_populates="billing_accounts")
    billing_contact = relationship("User", foreign_keys=[billing_contact_id])
    account_manager = relationship("User", foreign_keys=[account_manager_id])
    subscriptions = relationship("Subscription", back_populates="billing_account")
    # invoices = relationship("Invoice", back_populates="billing_account")  # Future implementation
    # payments = relationship("Payment", back_populates="billing_account")  # Future implementation
    
    # Rails business logic constants
    TRIAL_DURATION_DAYS = 30
    DEFAULT_GRACE_PERIOD = 7
    WARNING_THRESHOLDS = [50, 75, 90, 95]
    MAX_CREDIT_LIMIT = Decimal('100000.00')
    
    # Rails predicate methods
    def active_(self) -> bool:
        """Rails predicate: Check if billing account is active"""
        return self.status == BillingStatus.ACTIVE
    
    def trial_(self) -> bool:
        """Rails predicate: Check if account is in trial"""
        return self.status == BillingStatus.TRIAL
    
    def suspended_(self) -> bool:
        """Rails predicate: Check if account is suspended"""
        return self.status == BillingStatus.SUSPENDED
    
    def past_due_(self) -> bool:
        """Rails predicate: Check if account is past due"""
        return self.status == BillingStatus.PAST_DUE
    
    def trial_active_(self) -> bool:
        """Rails predicate: Check if trial is currently active"""
        if not self.trial_():
            return False
        
        if not self.trial_end_date:
            return False
        
        return datetime.utcnow() <= self.trial_end_date
    
    def trial_expired_(self) -> bool:
        """Rails predicate: Check if trial has expired"""
        if not self.trial_end_date:
            return False
        
        return datetime.utcnow() > self.trial_end_date
    
    def auto_pay_enabled_(self) -> bool:
        """Rails predicate: Check if auto-pay is enabled"""
        return self.auto_pay_enabled and self.primary_payment_method is not None
    
    def over_credit_limit_(self) -> bool:
        """Rails predicate: Check if over credit limit"""
        if self.credit_limit <= 0:
            return False
        return self.total_outstanding > self.credit_limit
    
    def payment_overdue_(self, days: int = None) -> bool:
        """Rails predicate: Check if payment is overdue"""
        if not self.next_billing_date:
            return False
        
        grace_days = days or self.grace_period_days
        overdue_date = self.next_billing_date + timedelta(days=grace_days)
        return datetime.utcnow() > overdue_date
    
    def usage_warning_threshold_(self) -> bool:
        """Rails predicate: Check if usage is at warning threshold"""
        if not self.monthly_limit or self.monthly_limit <= 0:
            return False
        
        current_usage = self.get_current_month_usage()
        threshold_amount = (self.monthly_limit * self.usage_threshold_warning) / 100
        return current_usage >= threshold_amount
    
    def usage_limit_reached_(self) -> bool:
        """Rails predicate: Check if usage limit is reached"""
        if not self.monthly_limit or self.monthly_limit <= 0:
            return False
        
        current_usage = self.get_current_month_usage()
        threshold_amount = (self.monthly_limit * self.usage_threshold_limit) / 100
        return current_usage >= threshold_amount
    
    # Rails business logic methods
    def activate_account(self) -> bool:
        """Activate billing account (Rails pattern)"""
        if self.status in [BillingStatus.SUSPENDED, BillingStatus.TRIAL]:
            self.status = BillingStatus.ACTIVE
            self.activated_at = datetime.utcnow()
            self.suspended_at = None
            return True
        return False
    
    def suspend_account(self, reason: str = None) -> bool:
        """Suspend billing account (Rails pattern)"""
        if self.status == BillingStatus.ACTIVE:
            self.status = BillingStatus.SUSPENDED
            self.suspended_at = datetime.utcnow()
            if reason:
                self.notes = f"{self.notes or ''}\nSuspended: {reason}".strip()
            return True
        return False
    
    def cancel_account(self, reason: str = None) -> bool:
        """Cancel billing account (Rails pattern)"""
        self.status = BillingStatus.CANCELLED
        if reason:
            self.notes = f"{self.notes or ''}\nCancelled: {reason}".strip()
        return True
    
    def extend_trial(self, days: int) -> bool:
        """Extend trial period (Rails pattern)"""
        if not self.trial_():
            return False
        
        if self.trial_end_date:
            self.trial_end_date += timedelta(days=days)
        else:
            self.trial_end_date = datetime.utcnow() + timedelta(days=days)
        
        return True
    
    def convert_from_trial(self) -> bool:
        """Convert from trial to active account (Rails pattern)"""
        if not self.trial_():
            return False
        
        self.status = BillingStatus.ACTIVE
        self.activated_at = datetime.utcnow()
        self.calculate_next_billing_date()
        return True
    
    def add_promotional_credit(self, amount: Decimal, description: str = None) -> None:
        """Add promotional credit (Rails pattern)"""
        self.promotional_credits = (self.promotional_credits or 0) + amount
        if description:
            self.notes = f"{self.notes or ''}\nPromo credit: {description}".strip()
    
    def apply_payment(self, amount: Decimal, payment_method: str = None) -> bool:
        """Apply payment to account (Rails pattern)"""
        if amount <= 0:
            return False
        
        self.current_balance += amount
        self.total_paid += amount
        self.total_outstanding = max(0, self.total_outstanding - amount)
        self.last_payment_at = datetime.utcnow()
        
        # Update status if payment resolves past due status
        if self.past_due_() and self.total_outstanding <= 0:
            self.status = BillingStatus.ACTIVE
        
        return True
    
    def add_charge(self, amount: Decimal, description: str = None) -> None:
        """Add charge to account (Rails pattern)"""
        self.current_balance -= amount
        self.total_outstanding += amount
        
        if description:
            self.notes = f"{self.notes or ''}\nCharge: {description}".strip()
    
    def calculate_next_billing_date(self) -> None:
        """Calculate next billing date based on cycle (Rails pattern)"""
        if not self.activated_at:
            base_date = datetime.utcnow()
        elif self.next_billing_date:
            base_date = self.next_billing_date
        else:
            base_date = self.activated_at
        
        if self.billing_cycle == BillingCycle.MONTHLY:
            # Add one month
            if base_date.month == 12:
                self.next_billing_date = base_date.replace(year=base_date.year + 1, month=1)
            else:
                self.next_billing_date = base_date.replace(month=base_date.month + 1)
        elif self.billing_cycle == BillingCycle.QUARTERLY:
            self.next_billing_date = base_date + timedelta(days=90)
        elif self.billing_cycle == BillingCycle.YEARLY:
            self.next_billing_date = base_date.replace(year=base_date.year + 1)
    
    def update_payment_method(self, method: PaymentMethod, details: Dict[str, Any]) -> bool:
        """Update payment method (Rails pattern)"""
        self.primary_payment_method = method
        # In production, this would encrypt sensitive payment details
        self.payment_method_details = details
        return True
    
    def get_current_month_usage(self) -> Decimal:
        """Get current month usage amount (Rails pattern)"""
        # This would calculate usage from billing records
        # For now, return placeholder
        return Decimal('0.00')
    
    def get_usage_percentage(self) -> float:
        """Get usage percentage of monthly limit (Rails pattern)"""
        if not self.monthly_limit or self.monthly_limit <= 0:
            return 0.0
        
        current_usage = self.get_current_month_usage()
        return float((current_usage / self.monthly_limit) * 100)
    
    def should_send_usage_warning_(self) -> bool:
        """Check if should send usage warning (Rails pattern)"""
        usage_pct = self.get_usage_percentage()
        return any(usage_pct >= threshold for threshold in self.WARNING_THRESHOLDS)
    
    def get_available_credit(self) -> Decimal:
        """Get available credit amount (Rails pattern)"""
        return max(0, self.credit_limit - self.total_outstanding)
    
    def get_account_tags(self) -> List[str]:
        """Get list of account tags (Rails pattern)"""
        if not self.tags:
            return []
        return [tag.strip() for tag in self.tags.split(",") if tag.strip()]
    
    def add_tag(self, tag: str) -> bool:
        """Add tag to account (Rails pattern)"""
        current_tags = self.get_account_tags()
        if tag not in current_tags:
            current_tags.append(tag)
            self.tags = ",".join(current_tags)
            return True
        return False
    
    def remove_tag(self, tag: str) -> bool:
        """Remove tag from account (Rails pattern)"""
        current_tags = self.get_account_tags()
        if tag in current_tags:
            current_tags.remove(tag)
            self.tags = ",".join(current_tags)
            return True
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert billing account to dictionary for API responses"""
        return {
            'id': self.id,
            'org_id': self.org_id,
            'account_number': self.account_number,
            'status': self.status.value if self.status else None,
            'billing_cycle': self.billing_cycle.value if self.billing_cycle else None,
            'currency': self.currency,
            'billing_email': self.billing_email,
            'company_name': self.company_name,
            'current_balance': float(self.current_balance) if self.current_balance else 0.0,
            'credit_limit': float(self.credit_limit) if self.credit_limit else 0.0,
            'total_outstanding': float(self.total_outstanding) if self.total_outstanding else 0.0,
            'monthly_limit': float(self.monthly_limit) if self.monthly_limit else None,
            'promotional_credits': float(self.promotional_credits) if self.promotional_credits else 0.0,
            'auto_pay_enabled': self.auto_pay_enabled,
            'trial_end_date': self.trial_end_date.isoformat() if self.trial_end_date else None,
            'next_billing_date': self.next_billing_date.isoformat() if self.next_billing_date else None,
            'last_payment_at': self.last_payment_at.isoformat() if self.last_payment_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'active': self.active_(),
            'trial': self.trial_(),
            'trial_active': self.trial_active_(),
            'trial_expired': self.trial_expired_(),
            'suspended': self.suspended_(),
            'past_due': self.past_due_(),
            'over_credit_limit': self.over_credit_limit_(),
            'payment_overdue': self.payment_overdue_(),
            'usage_warning': self.usage_warning_threshold_(),
            'usage_limit_reached': self.usage_limit_reached_(),
            'usage_percentage': self.get_usage_percentage(),
            'available_credit': float(self.get_available_credit()),
            'tags': self.get_account_tags()
        }
    
    @classmethod
    def create_trial_account(cls, org, billing_contact=None, trial_days=None):
        """Create new trial billing account (Rails pattern)"""
        import secrets
        
        trial_days = trial_days or cls.TRIAL_DURATION_DAYS
        trial_end = datetime.utcnow() + timedelta(days=trial_days)
        
        account = cls(
            org_id=org.id if hasattr(org, 'id') else org,
            billing_contact_id=billing_contact.id if billing_contact and hasattr(billing_contact, 'id') else billing_contact,
            account_number=f"TRIAL-{secrets.token_hex(8).upper()}",
            status=BillingStatus.TRIAL,
            billing_email=billing_contact.email if billing_contact and hasattr(billing_contact, 'email') else None,
            trial_start_date=datetime.utcnow(),
            trial_end_date=trial_end,
            auto_pay_enabled=False
        )
        
        return account
    
    @classmethod
    def find_by_org(cls, org_id: int, session=None):
        """Find billing accounts for organization (Rails pattern)"""
        # This would query billing accounts when session is available
        # For now, return empty list as placeholder
        return []