"""
Authentication API endpoints - Python equivalent of Rails authentication controllers.
Handles login, registration, password reset, and token refresh.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, EmailStr, validator
from typing import Optional
import logging

from ...database import get_db
from ...auth.jwt_auth import AuthService, JWTAuth, get_current_user
from ...models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Authentication"])


# Pydantic models for request/response
class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserRegister(BaseModel):
    email: EmailStr
    password: str
    full_name: str
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v

class TokenRefresh(BaseModel):
    refresh_token: str

class PasswordResetRequest(BaseModel):
    email: EmailStr

class PasswordReset(BaseModel):
    token: str
    new_password: str
    
    @validator('new_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v

class UserResponse(BaseModel):
    id: int
    email: str
    full_name: Optional[str]
    status: str
    is_active: bool
    is_email_verified: bool
    is_tos_signed: bool
    created_at: Optional[str]
    default_org_id: Optional[int]

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserResponse

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


@router.post("/login", response_model=LoginResponse, summary="User Login")
async def login(
    user_credentials: UserLogin,
    db: Session = Depends(get_db)
):
    """
    Authenticate user and return access/refresh tokens.
    
    Equivalent to Rails sessions#create or devise sessions#create
    """
    try:
        result = AuthService.login(
            email=user_credentials.email,
            password=user_credentials.password,
            db=db
        )
        return result
    except HTTPException as e:
        # Log failed login attempt
        logger.warning(f"Failed login attempt for email: {user_credentials.email}")
        raise e
    except Exception as e:
        logger.error(f"Login error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Login failed"
        )


@router.post("/register", response_model=UserResponse, summary="User Registration")
async def register(
    user_data: UserRegister,
    db: Session = Depends(get_db)
):
    """
    Register new user account.
    
    Equivalent to Rails users#create or devise registrations#create
    """
    try:
        user = AuthService.create_user(
            email=user_data.email,
            password=user_data.password,
            full_name=user_data.full_name,
            db=db
        )
        
        return UserResponse(**user.to_dict())
        
    except HTTPException as e:
        raise e
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Registration failed"
        )


@router.post("/refresh", response_model=TokenResponse, summary="Refresh Access Token")
async def refresh_token(
    token_data: TokenRefresh,
    db: Session = Depends(get_db)
):
    """
    Refresh access token using refresh token.
    
    Equivalent to Rails JWT token refresh endpoint
    """
    try:
        new_access_token = JWTAuth.refresh_access_token(
            refresh_token=token_data.refresh_token,
            db=db
        )
        
        return TokenResponse(access_token=new_access_token)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Token refresh error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Token refresh failed"
        )


@router.post("/password-reset-request", summary="Request Password Reset")
async def request_password_reset(
    request_data: PasswordResetRequest,
    db: Session = Depends(get_db)
):
    """
    Request password reset token via email.
    
    Equivalent to Rails devise passwords#create
    """
    try:
        # Find user by email
        user = db.query(User).filter(func.lower(User.email) == func.lower(request_data.email)).first()
        if not user:
            # Don't reveal if email exists or not (security)
            logger.warning(f"Password reset requested for non-existent email: {request_data.email}")
            return {"message": "If the email exists, a password reset link has been sent"}
        
        # Generate reset token
        reset_token = user.generate_password_reset_token()
        db.commit()
        
        # TODO: Send password reset email
        # EmailService.send_password_reset_email(user.email, reset_token)
        
        logger.info(f"Password reset requested for user: {user.email}")
        return {"message": "If the email exists, a password reset link has been sent"}
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Password reset request error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Password reset request failed"
        )


@router.post("/password-reset", summary="Reset Password")
async def reset_password(
    reset_data: PasswordReset,
    db: Session = Depends(get_db)
):
    """
    Reset password using token.
    
    Equivalent to Rails devise passwords#update
    """
    try:
        # Find user by reset token (would need to implement token lookup)
        # For now, we'll need to find users and verify token
        users = db.query(User).filter(
            User.password_reset_token.isnot(None)
        ).all()
        
        user = None
        for u in users:
            if u.verify_password_reset_token(reset_data.token):
                user = u
                break
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired reset token"
            )
        
        # Reset password
        user.reset_password_with_token(reset_data.token, reset_data.new_password)
        db.commit()
        
        logger.info(f"Password reset completed for user: {user.email}")
        return {"message": "Password has been reset successfully"}
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Password reset error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Password reset failed"
        )


@router.get("/me", response_model=UserResponse, summary="Get Current User")
async def get_current_user_info(
    current_user: User = Depends(get_current_user)
):
    """
    Get current authenticated user information.
    
    Equivalent to Rails current_user endpoint
    """
    return UserResponse(**current_user.to_dict())


@router.post("/logout", summary="User Logout")
async def logout():
    """
    Logout user (client-side token removal).
    
    Since JWT is stateless, logout is handled client-side.
    In a production app, you might maintain a token blacklist.
    
    Equivalent to Rails sessions#destroy or devise sessions#destroy
    """
    return {"message": "Logged out successfully"}


@router.post("/verify-email/{user_id}", summary="Verify Email")
async def verify_email(
    user_id: int,
    db: Session = Depends(get_db)
):
    """
    Verify user email address.
    
    In a full implementation, this would use a verification token.
    Equivalent to Rails email verification endpoints.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user.verify_email()
    db.commit()
    
    logger.info(f"Email verified for user: {user.email}")
    return {"message": "Email verified successfully"}


@router.post("/sign-tos/{user_id}", summary="Sign Terms of Service")
async def sign_tos(
    user_id: int,
    db: Session = Depends(get_db)
):
    """
    Mark terms of service as signed for user.
    
    Equivalent to Rails ToS acceptance endpoints.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    user.sign_tos()
    db.commit()
    
    logger.info(f"ToS signed for user: {user.email}")
    return {"message": "Terms of service signed successfully"}