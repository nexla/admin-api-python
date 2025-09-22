from datetime import timedelta, datetime
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from ..database import get_db
from ..auth import authenticate_user, create_access_token, get_current_user
from ..models.user import User
from ..config import settings

router = APIRouter()

class Token(BaseModel):
    access_token: str
    token_type: str
    user_id: int
    expires_in: int

class TokenWithRefresh(Token):
    refresh_token: str
    refresh_expires_in: int

class UserLogin(BaseModel):
    email: EmailStr
    password: str
    remember_me: bool = Field(False)
    device_name: Optional[str] = Field(None)

class UserResponse(BaseModel):
    id: int
    email: str
    full_name: Optional[str] = None
    status: str
    
    class Config:
        from_attributes = True

class ApiKeyCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    scopes: List[str] = Field(default_factory=list)
    expires_at: Optional[datetime] = None
    ip_whitelist: Optional[List[str]] = None

class ApiKeyResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    key_prefix: str
    scopes: List[str]
    created_at: datetime
    expires_at: Optional[datetime]
    last_used_at: Optional[datetime]
    is_active: bool
    usage_count: int
    ip_whitelist: Optional[List[str]]

class ApiKeySecret(BaseModel):
    api_key: str
    message: str

class SessionInfo(BaseModel):
    session_id: str
    device_name: Optional[str]
    ip_address: str
    user_agent: str
    created_at: datetime
    last_active_at: datetime
    expires_at: datetime
    is_current: bool

class PasswordResetRequest(BaseModel):
    email: EmailStr

class PasswordReset(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8)

class PasswordChange(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)

class TwoFactorSetup(BaseModel):
    method: str = Field("totp", pattern="^(totp|sms)$")
    phone_number: Optional[str] = None

class TwoFactorVerify(BaseModel):
    code: str = Field(..., min_length=6, max_length=8)
    method: str = Field("totp", pattern="^(totp|sms)$")

class LoginAttempt(BaseModel):
    email: str
    ip_address: str
    user_agent: str
    success: bool
    attempted_at: datetime
    failure_reason: Optional[str]

@router.post("/token", response_model=TokenWithRefresh)
async def login(
    user_credentials: UserLogin,
    request: Request,
    db: Session = Depends(get_db)
):
    """Authenticate user and return access token."""
    client_ip = request.client.host
    user_agent = request.headers.get("user-agent", "")
    
    # Log login attempt
    login_attempt = {
        "email": user_credentials.email,
        "ip_address": client_ip,
        "user_agent": user_agent,
        "attempted_at": datetime.utcnow()
    }
    
    user = authenticate_user(db, user_credentials.email, user_credentials.password)
    if not user:
        login_attempt.update({"success": False, "failure_reason": "Invalid credentials"})
        # In production, log this attempt
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if user.account_locked_():
        login_attempt.update({"success": False, "failure_reason": "Account locked"})
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is locked"
        )
    
    if user.requires_2fa_() and not user_credentials.get("two_factor_code"):
        login_attempt.update({"success": False, "failure_reason": "2FA required"})
        raise HTTPException(
            status_code=status.HTTP_200_OK,  # Special case for 2FA
            detail="Two-factor authentication required",
            headers={"X-2FA-Required": "true"}
        )
    
    # Create session
    session = user.create_session_(
        device_name=user_credentials.device_name,
        ip_address=client_ip,
        user_agent=user_agent,
        remember_me=user_credentials.remember_me
    )
    
    # Create tokens
    token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_expires = timedelta(days=30 if user_credentials.remember_me else 7)
    
    access_token = create_access_token(
        data={"user_id": user.id, "email": user.email, "session_id": session.id},
        expires_delta=token_expires
    )
    
    refresh_token = user.create_refresh_token_(
        session_id=session.id,
        expires_delta=refresh_expires
    )
    
    login_attempt.update({"success": True})
    user.update_last_login_()
    db.commit()
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user_id": user.id,
        "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "refresh_token": refresh_token,
        "refresh_expires_in": int(refresh_expires.total_seconds())
    }

@router.post("/token/refresh", response_model=Token)
async def refresh_token(
    refresh_token: str,
    request: Request,
    db: Session = Depends(get_db)
):
    """Refresh access token using refresh token."""
    user = User.validate_refresh_token_(db, refresh_token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token"
        )
    
    if user.account_locked_():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is locked"
        )
    
    session = user.get_session_from_refresh_token_(refresh_token)
    if not session or session.is_expired_():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired"
        )
    
    # Update session activity
    session.update_last_active_()
    
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"user_id": user.id, "email": user.email, "session_id": session.id},
        expires_delta=access_token_expires
    )
    
    db.commit()
    
    return {
        "access_token": access_token,
        "token_type": "bearer", 
        "user_id": user.id,
        "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }

# API Key Management
@router.post("/api-keys", response_model=ApiKeySecret, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    api_key_data: ApiKeyCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new API key for the current user."""
    if not current_user.can_create_api_keys_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to create API keys"
        )
    
    api_key = current_user.create_api_key_(
        name=api_key_data.name,
        description=api_key_data.description,
        scopes=api_key_data.scopes,
        expires_at=api_key_data.expires_at,
        ip_whitelist=api_key_data.ip_whitelist
    )
    
    db.add(api_key)
    db.commit()
    db.refresh(api_key)
    
    return {
        "api_key": api_key.full_key,
        "message": "API key created successfully. Store this securely - it won't be shown again."
    }

@router.get("/api-keys", response_model=List[ApiKeyResponse])
async def list_api_keys(
    active_only: bool = Query(True),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List API keys for the current user."""
    api_keys = current_user.get_api_keys_(
        active_only=active_only,
        limit=limit,
        offset=offset
    )
    
    return api_keys

@router.put("/api-keys/{api_key_id}", response_model=ApiKeyResponse)
async def update_api_key(
    api_key_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
    scopes: Optional[List[str]] = None,
    ip_whitelist: Optional[List[str]] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update an API key."""
    api_key = current_user.get_api_key_(api_key_id)
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    if name:
        api_key.name = name
    if description is not None:
        api_key.description = description
    if scopes is not None:
        api_key.scopes = scopes
    if ip_whitelist is not None:
        api_key.ip_whitelist = ip_whitelist
    
    api_key.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(api_key)
    
    return api_key

@router.delete("/api-keys/{api_key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(
    api_key_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete an API key."""
    api_key = current_user.get_api_key_(api_key_id)
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    api_key.deactivate_()
    db.commit()

@router.post("/api-keys/{api_key_id}/rotate", response_model=ApiKeySecret)
async def rotate_api_key(
    api_key_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Rotate an API key (generate new secret while keeping metadata)."""
    api_key = current_user.get_api_key_(api_key_id)
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    new_key = api_key.rotate_()
    db.commit()
    
    return {
        "api_key": new_key,
        "message": "API key rotated successfully. Update your applications with the new key."
    }

# Session Management
@router.get("/sessions", response_model=List[SessionInfo])
async def list_sessions(
    active_only: bool = Query(True),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List active sessions for the current user."""
    sessions = current_user.get_sessions_(active_only=active_only)
    return sessions

@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def terminate_session(
    session_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Terminate a specific session."""
    session = current_user.get_session_(session_id)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )
    
    session.invalidate_()
    db.commit()

# Password Management
@router.post("/password/reset-request")
async def request_password_reset(
    reset_request: PasswordResetRequest,
    db: Session = Depends(get_db)
):
    """Request password reset token."""
    user = db.query(User).filter(User.email == reset_request.email).first()
    if user:
        reset_token = user.create_password_reset_token_()
        # In production, send email with reset_token
        db.commit()
    
    # Always return success to prevent user enumeration
    return {"message": "If the email exists, a password reset link has been sent."}

@router.post("/password/reset")
async def reset_password(
    reset_data: PasswordReset,
    db: Session = Depends(get_db)
):
    """Reset password using reset token."""
    user = User.validate_password_reset_token_(db, reset_data.token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )
    
    user.set_password_(reset_data.new_password)
    user.invalidate_password_reset_tokens_()
    user.invalidate_all_sessions_()  # Force re-login everywhere
    
    db.commit()
    return {"message": "Password reset successfully"}

@router.post("/password/change")
async def change_password(
    password_change: PasswordChange,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Change password for authenticated user."""
    if not current_user.verify_password_(password_change.current_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )
    
    current_user.set_password_(password_change.new_password)
    current_user.password_changed_at = datetime.utcnow()
    
    # Optionally invalidate other sessions
    # current_user.invalidate_other_sessions_(current_session_id)
    
    db.commit()
    return {"message": "Password changed successfully"}

# Two-Factor Authentication
@router.post("/2fa/setup", response_model=Dict[str, Any])
async def setup_two_factor(
    setup_data: TwoFactorSetup,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Setup two-factor authentication."""
    if current_user.has_2fa_enabled_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Two-factor authentication is already enabled"
        )
    
    setup_result = current_user.setup_2fa_(
        method=setup_data.method,
        phone_number=setup_data.phone_number
    )
    
    db.commit()
    return setup_result

@router.post("/2fa/verify")
async def verify_two_factor(
    verify_data: TwoFactorVerify,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Verify and enable two-factor authentication."""
    if current_user.has_2fa_enabled_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Two-factor authentication is already enabled"
        )
    
    if not current_user.verify_2fa_setup_(verify_data.code, verify_data.method):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid verification code"
        )
    
    current_user.enable_2fa_()
    db.commit()
    return {"message": "Two-factor authentication enabled successfully"}

@router.delete("/2fa", status_code=status.HTTP_204_NO_CONTENT)
async def disable_two_factor(
    verification_code: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Disable two-factor authentication."""
    if not current_user.has_2fa_enabled_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Two-factor authentication is not enabled"
        )
    
    if not current_user.verify_2fa_code_(verification_code):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid verification code"
        )
    
    current_user.disable_2fa_()
    db.commit()

@router.get("/2fa/backup-codes", response_model=List[str])
async def get_backup_codes(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get backup codes for two-factor authentication."""
    if not current_user.has_2fa_enabled_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Two-factor authentication is not enabled"
        )
    
    backup_codes = current_user.get_2fa_backup_codes_()
    return backup_codes

@router.post("/2fa/backup-codes/regenerate", response_model=List[str])
async def regenerate_backup_codes(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Regenerate backup codes for two-factor authentication."""
    if not current_user.has_2fa_enabled_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Two-factor authentication is not enabled"
        )
    
    backup_codes = current_user.regenerate_2fa_backup_codes_()
    db.commit()
    return backup_codes

# Security and Audit
@router.get("/login-history", response_model=List[LoginAttempt])
async def get_login_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    success_only: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get login history for the current user."""
    login_history = current_user.get_login_history_(
        limit=limit,
        offset=offset,
        success_only=success_only
    )
    
    return login_history

@router.get("/security-events", response_model=List[Dict[str, Any]])
async def get_security_events(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    event_type: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get security events for the current user."""
    security_events = current_user.get_security_events_(
        limit=limit,
        offset=offset,
        event_type=event_type
    )
    
    return security_events

@router.get("/me", response_model=UserResponse)
async def read_users_me(current_user: User = Depends(get_current_user)):
    """Get current user information."""
    return current_user

@router.post("/logout")
async def logout(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Logout current user and invalidate session."""
    session_id = request.state.session_id if hasattr(request.state, 'session_id') else None
    
    if session_id:
        session = current_user.get_session_(session_id)
        if session:
            session.invalidate_()
    
    # Invalidate all refresh tokens for this user (optional)
    # current_user.invalidate_all_refresh_tokens_()
    
    db.commit()
    return {"message": "Successfully logged out"}

@router.post("/logout/all")
async def logout_all(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Logout from all devices and invalidate all sessions."""
    current_user.invalidate_all_sessions_()
    db.commit()
    return {"message": "Successfully logged out from all devices"}