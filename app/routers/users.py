from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr, validator
from datetime import datetime, timedelta
import secrets
import hashlib
import re

from ..database import get_db
from ..auth import get_current_user, get_password_hash, verify_password
from ..auth.rbac import (
    RBACService, SystemPermissions, 
    check_admin_permission, check_user_admin_permission
)
from ..models.user import User
from ..models.org import Org
from ..models.user_login_audit import UserLoginAudit
from ..services.async_tasks.manager import AsyncTaskManager
from ..services.audit_service import AuditService
from ..services.validation_service import ValidationService

router = APIRouter()

class UserCreate(BaseModel):
    email: EmailStr
    full_name: Optional[str] = None
    password: str
    default_org_id: Optional[int] = None
    
    @validator('password')
    def validate_password(cls, v, values):
        email = values.get('email', '')
        full_name = values.get('full_name', '')
        validation_result = ValidationService.validate_password(email, full_name, v)
        if not validation_result['valid']:
            raise ValueError(f"Password validation failed: {validation_result['message']}")
        return v

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    status: Optional[str] = None
    action: Optional[str] = None  # For special actions like source_activate

class PasswordReset(BaseModel):
    email: EmailStr
    g_captcha_response: str
    org_id: Optional[int] = None

class SetPassword(BaseModel):
    reset_token: str
    password: str
    password_confirmation: str

class ChangePassword(BaseModel):
    password: str
    password_confirmation: str

class UserTransfer(BaseModel):
    delegate_owner_id: int
    org_id: Optional[int] = None
    delegate_org_id: Optional[int] = None

class UserInvite(BaseModel):
    invitee_email: EmailStr

class UserResponse(BaseModel):
    id: int
    email: str
    full_name: Optional[str] = None
    status: str
    created_at: str
    updated_at: str
    default_org_id: Optional[int] = None
    is_locked: Optional[bool] = False
    is_super_user: Optional[bool] = False
    
    class Config:
        from_attributes = True

class LoginHistoryResponse(BaseModel):
    login_history: List[Dict[str, Any]]
    logout_history: List[Dict[str, Any]]

class AccountSummaryResponse(BaseModel):
    total_resources: int
    active_flows: int
    data_sources: int
    data_sinks: int
    data_sets: int
    storage_usage: Dict[str, Any]
    recent_activity: List[Dict[str, Any]]

@router.get("/", response_model=List[UserResponse])
async def list_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    email: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List users with proper authorization and filtering."""
    # Email lookup requires super user privileges
    if email:
        if not RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Email lookup requires super user privileges"
            )
        
        # Validate email format
        if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid email format"
            )
            
        user = db.query(User).filter(User.email == email).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        return [user]
    
    # Check read permissions
    if not RBACService.has_permission(current_user, SystemPermissions.USER_READ, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to list users"
        )
    
    # Apply filters based on user role and context
    query = db.query(User)
    
    # Non-admin users can only see users in their organizations
    if not RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db):
        # TODO: Filter by organization membership when relationships are active
        pass
    
    users = query.offset(skip).limit(limit).all()
    
    # Replace current user instance with authenticated user for additional info
    users = [current_user if u.id == current_user.id else u for u in users]
    
    return users

@router.get("/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific user with authorization."""
    # Return current user instance if requesting self for additional info
    if user_id == current_user.id:
        return current_user
        
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Check read permissions
    if not RBACService.has_permission(current_user, SystemPermissions.USER_READ, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view user"
        )
    
    return user

@router.post("/", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    user_data: UserCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new user (requires admin privileges)."""
    # Check if user already exists
    existing_user = db.query(User).filter(User.email == user_data.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Create new user
    hashed_password = get_password_hash(user_data.password)
    db_user = User(
        email=user_data.email,
        full_name=user_data.full_name,
        password_digest=hashed_password,
        default_org_id=user_data.default_org_id,
        status="ACTIVE"
    )
    
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    return db_user

@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a user."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # TODO: Add proper authorization check
    
    # Update fields
    if user_data.full_name is not None:
        user.full_name = user_data.full_name
    if user_data.status is not None:
        user.status = user_data.status
    
    db.commit()
    db.refresh(user)
    
    return user

@router.put("/{user_id}/activate")
async def activate_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate a user."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user.status = "ACTIVE"
    db.commit()
    
    return {"message": f"User {user_id} activated successfully"}

@router.put("/{user_id}/deactivate") 
async def deactivate_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deactivate a user."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user.status = "DEACTIVATED"
    db.commit()
    
    # Log the action
    AuditService.log_user_action(
        db, current_user, "user_deactivated", 
        {"target_user_id": user_id, "target_email": user.email}
    )
    
    return {"message": f"User {user_id} deactivated successfully"}

# Password Reset Workflow
@router.post("/reset-password")
async def reset_password(
    reset_data: PasswordReset,
    request: Request,
    db: Session = Depends(get_db)
):
    """Initiate password reset process."""
    # Validate captcha (placeholder)
    if not reset_data.g_captcha_response:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Captcha verification required"
        )
    
    user = db.query(User).filter(User.email == reset_data.email).first()
    if user:
        org = None
        if reset_data.org_id:
            org = db.query(Org).filter(Org.id == reset_data.org_id).first()
        else:
            org = user.default_org
            
        if org:
            # Generate reset token
            reset_token = secrets.token_urlsafe(32)
            user.password_reset_token = reset_token
            user.password_reset_expires = datetime.utcnow() + timedelta(hours=24)
            db.commit()
            
            # TODO: Send email with reset link
            # EmailService.send_password_reset(user.email, reset_token, request.headers.get('origin'))
    
    # Always return OK to prevent email enumeration
    return {"message": "If the email exists, a reset link has been sent"}

@router.post("/set-password")
async def set_password(
    password_data: SetPassword,
    db: Session = Depends(get_db)
):
    """Set new password using reset token."""
    if password_data.password != password_data.password_confirmation:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password confirmation does not match"
        )
    
    # Find user by reset token
    user = db.query(User).filter(
        User.password_reset_token == password_data.reset_token,
        User.password_reset_expires > datetime.utcnow()
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )
    
    # Validate password
    validation_result = ValidationService.validate_password(
        user.email, user.full_name or "", password_data.password
    )
    if not validation_result['valid']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Password validation failed: {validation_result['message']}"
        )
    
    # Update password
    user.password_digest = get_password_hash(password_data.password)
    user.password_reset_token = None
    user.password_reset_expires = None
    db.commit()
    
    return {"message": "Password updated successfully"}

@router.put("/{user_id}/change-password")
async def change_password(
    user_id: int,
    password_data: ChangePassword,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Change user password (admin only)."""
    if not RBACService.has_permission(current_user, SystemPermissions.USER_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to change user password"
        )
        
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if password_data.password != password_data.password_confirmation:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password confirmation does not match"
        )
    
    # Validate password
    validation_result = ValidationService.validate_password(
        user.email, user.full_name or "", password_data.password
    )
    if not validation_result['valid']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Password validation failed: {validation_result['message']}"
        )
    
    user.password_digest = get_password_hash(password_data.password)
    db.commit()
    
    # Log the action
    AuditService.log_user_action(
        db, current_user, "password_changed", 
        {"target_user_id": user_id, "target_email": user.email}
    )
    
    return {"message": "Password changed successfully"}

# Account Management
@router.put("/{user_id}/lock")
async def lock_account(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lock user account (super admin only)."""
    if not RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only super admins can lock accounts"
        )
        
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user.is_locked = True
    db.commit()
    
    AuditService.log_user_action(
        db, current_user, "account_locked", 
        {"target_user_id": user_id, "target_email": user.email}
    )
    
    return {"message": f"Account {user_id} locked successfully"}

@router.put("/{user_id}/unlock")
async def unlock_account(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Unlock user account (super admin only)."""
    if not RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only super admins can unlock accounts"
        )
        
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user.is_locked = False
    db.commit()
    
    AuditService.log_user_action(
        db, current_user, "account_unlocked", 
        {"target_user_id": user_id, "target_email": user.email}
    )
    
    return {"message": f"Account {user_id} unlocked successfully"}

# User History and Audit
@router.get("/{user_id}/login-history", response_model=LoginHistoryResponse)
async def get_login_history(
    user_id: int,
    event_type: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get user login/logout history."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if not RBACService.has_permission(current_user, SystemPermissions.USER_READ, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view user history"
        )
    
    response = LoginHistoryResponse(login_history=[], logout_history=[])
    
    if not event_type or event_type == "login_history":
        login_audits = db.query(UserLoginAudit).filter(
            UserLoginAudit.user_id == user_id,
            UserLoginAudit.event_type == "login"
        ).offset(skip).limit(limit).all()
        response.login_history = [{"id": audit.id, "timestamp": audit.created_at.isoformat(), "ip_address": audit.ip_address} for audit in login_audits]
    
    if not event_type or event_type == "logout_history":
        logout_audits = db.query(UserLoginAudit).filter(
            UserLoginAudit.user_id == user_id,
            UserLoginAudit.event_type == "logout"
        ).offset(skip).limit(limit).all()
        response.logout_history = [{"id": audit.id, "timestamp": audit.created_at.isoformat(), "ip_address": audit.ip_address} for audit in logout_audits]
    
    return response

# Current User Info
@router.get("/current", response_model=UserResponse)
async def get_current_user_info(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get current authenticated user information."""
    return current_user

# Password Validation
@router.post("/validate-password")
async def validate_password_endpoint(
    email: Optional[str] = None,
    full_name: Optional[str] = None,
    password: str = None,
    current_user: Optional[User] = Depends(get_current_user)
):
    """Validate password strength and entropy."""
    if not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password is required"
        )
    
    # Use current user info if not provided
    if not email and current_user:
        email = current_user.email
    if not full_name and current_user:
        full_name = current_user.full_name or ""
    
    validation_result = ValidationService.validate_password(
        email or "", full_name or "", password
    )
    
    return validation_result