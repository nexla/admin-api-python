"""
Billing Router - Billing account and subscription management endpoints.
Provides Rails-style billing operations with FastAPI patterns.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import secrets
from decimal import Decimal

from ..database import get_db
from ..auth import get_current_user
from ..models.billing_account import BillingAccount, BillingStatus, PaymentMethod, BillingCycle
from ..models.subscription import Subscription, SubscriptionStatus, SubscriptionType
from ..models.org import Org
from ..models.user import User

router = APIRouter(prefix="/billing", tags=["billing"])


@router.post("/setup-trial/{org_id}")
async def setup_trial_billing(
    org_id: int, 
    billing_email: Optional[str] = None,
    trial_days: int = 30,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Set up trial billing account for organization"""
    
    # Get organization and verify access
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    # Check if billing account already exists
    existing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if existing_account:
        raise HTTPException(status_code=400, detail="Billing account already exists for this organization")
    
    # Create trial billing account
    billing_account = BillingAccount.create_trial_account(
        org=org,
        billing_contact=current_user,
        trial_days=trial_days
    )
    
    # Override billing email if provided
    if billing_email:
        billing_account.billing_email = billing_email
    else:
        billing_account.billing_email = current_user.email
    
    db.add(billing_account)
    db.commit()
    db.refresh(billing_account)
    
    return {
        "status": "success",
        "message": f"Trial billing account created for {trial_days} days",
        "account": billing_account.to_dict()
    }


@router.get("/account/{org_id}")
async def get_billing_account(
    org_id: int, 
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get billing account information for organization"""
    
    # Find billing account
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="No billing account found for this organization")
    
    return {
        "account": billing_account.to_dict(),
        "trial_active": billing_account.trial_active_(),
        "trial_expired": billing_account.trial_expired_(),
        "usage_warning": billing_account.usage_warning_threshold_(),
        "usage_limit_reached": billing_account.usage_limit_reached_(),
        "payment_overdue": billing_account.payment_overdue_(),
        "available_credit": float(billing_account.get_available_credit())
    }


@router.post("/account/{org_id}/activate")
async def activate_billing_account(
    org_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Activate billing account (convert from trial)"""
    
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="Billing account not found")
    
    if billing_account.convert_from_trial():
        db.commit()
        return {
            "status": "success", 
            "message": "Billing account activated",
            "account": billing_account.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Cannot activate account - not in trial status")


@router.post("/account/{org_id}/suspend")
async def suspend_billing_account(
    org_id: int,
    reason: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Suspend billing account"""
    
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="Billing account not found")
    
    if billing_account.suspend_account(reason):
        db.commit()
        return {
            "status": "success", 
            "message": "Billing account suspended",
            "account": billing_account.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Cannot suspend account - not in active status")


@router.post("/account/{org_id}/payment")
async def apply_payment(
    org_id: int,
    amount: float,
    payment_method: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Apply payment to billing account"""
    
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Payment amount must be positive")
    
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="Billing account not found")
    
    payment_amount = Decimal(str(amount))
    
    if billing_account.apply_payment(payment_amount, payment_method):
        db.commit()
        return {
            "status": "success",
            "message": f"Payment of ${amount} applied successfully",
            "account": billing_account.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Payment could not be processed")


@router.post("/account/{org_id}/extend-trial")
async def extend_trial(
    org_id: int,
    days: int = 7,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Extend trial period"""
    
    if days <= 0 or days > 90:
        raise HTTPException(status_code=400, detail="Extension days must be between 1 and 90")
    
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="Billing account not found")
    
    if billing_account.extend_trial(days):
        db.commit()
        return {
            "status": "success",
            "message": f"Trial extended by {days} days",
            "account": billing_account.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Cannot extend trial - account not in trial status")


@router.post("/account/{org_id}/promotional-credit")
async def add_promotional_credit(
    org_id: int,
    amount: float,
    description: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Add promotional credit to account"""
    
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Credit amount must be positive")
    
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="Billing account not found")
    
    credit_amount = Decimal(str(amount))
    billing_account.add_promotional_credit(credit_amount, description)
    
    db.commit()
    return {
        "status": "success",
        "message": f"Promotional credit of ${amount} added",
        "account": billing_account.to_dict()
    }


@router.get("/subscriptions/{org_id}")
async def get_subscriptions(
    org_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all subscriptions for organization"""
    
    subscriptions = db.query(Subscription).filter(Subscription.org_id == org_id).all()
    
    return {
        "subscriptions": [sub.to_dict() for sub in subscriptions],
        "total_subscriptions": len(subscriptions),
        "active_subscriptions": len([sub for sub in subscriptions if sub.active_()])
    }


@router.post("/subscriptions/{org_id}/create")
async def create_subscription(
    org_id: int,
    plan_id: str,
    subscription_type: str,
    base_price: float,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create new subscription"""
    
    # Get billing account
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="Billing account required to create subscription")
    
    # Validate subscription type
    try:
        sub_type = SubscriptionType(subscription_type.upper())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid subscription type")
    
    if base_price <= 0:
        raise HTTPException(status_code=400, detail="Base price must be positive")
    
    # Create subscription
    subscription = Subscription.create_subscription(
        org=org_id,
        billing_account=billing_account.id,
        plan_id=plan_id,
        subscription_type=sub_type,
        base_price=Decimal(str(base_price))
    )
    
    db.add(subscription)
    db.commit()
    db.refresh(subscription)
    
    return {
        "status": "success",
        "message": "Subscription created successfully",
        "subscription": subscription.to_dict()
    }


@router.post("/subscriptions/{subscription_id}/activate")
async def activate_subscription(
    subscription_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Activate subscription"""
    
    subscription = db.query(Subscription).filter(Subscription.id == subscription_id).first()
    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found")
    
    if subscription.activate_subscription():
        db.commit()
        return {
            "status": "success",
            "message": "Subscription activated",
            "subscription": subscription.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Cannot activate subscription")


@router.post("/subscriptions/{subscription_id}/cancel")
async def cancel_subscription(
    subscription_id: int,
    reason: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Cancel subscription"""
    
    subscription = db.query(Subscription).filter(Subscription.id == subscription_id).first()
    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found")
    
    if subscription.cancel_subscription(reason):
        db.commit()
        return {
            "status": "success",
            "message": "Subscription cancelled",
            "subscription": subscription.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Cannot cancel subscription")


@router.get("/account/{org_id}/usage")
async def get_usage_summary(
    org_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get usage summary for billing account"""
    
    billing_account = db.query(BillingAccount).filter(BillingAccount.org_id == org_id).first()
    if not billing_account:
        raise HTTPException(status_code=404, detail="Billing account not found")
    
    subscriptions = db.query(Subscription).filter(Subscription.org_id == org_id).all()
    
    # Calculate total subscription costs
    total_monthly_cost = sum(float(sub.calculate_effective_price()) for sub in subscriptions if sub.active_())
    
    return {
        "billing_account": billing_account.to_dict(),
        "usage_percentage": billing_account.get_usage_percentage(),
        "current_month_usage": float(billing_account.get_current_month_usage()),
        "monthly_limit": float(billing_account.monthly_limit) if billing_account.monthly_limit else None,
        "total_monthly_subscription_cost": total_monthly_cost,
        "active_subscriptions": len([sub for sub in subscriptions if sub.active_()]),
        "usage_warnings": {
            "at_warning_threshold": billing_account.usage_warning_threshold_(),
            "at_usage_limit": billing_account.usage_limit_reached_()
        }
    }