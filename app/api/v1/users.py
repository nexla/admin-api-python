"""
Users API endpoints - Python equivalent of Rails users controller.
Handles comprehensive user management, profile operations, security, and administrative functions.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, Field, EmailStr
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user, PasswordUtils
from ...auth.rbac import (
    RBACService, SystemPermissions, require_admin, require_user_admin,
    check_admin_permission, check_user_admin_permission
)
from ...models.user import User
from ...models.org import Org
from ...models.org_membership import OrgMembership

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/users", tags=["Users"])


# Pydantic models for request/response
class UserCreate(BaseModel):
    email: EmailStr
    full_name: str = Field(..., min_length=1, max_length=255)
    password: str = Field(..., min_length=8)
    status: Optional[str] = "ACTIVE"
    role: Optional[str] = "USER"
    
    @validator('full_name')
    def validate_full_name(cls, v):
        if len(v.strip()) < 1:
            raise ValueError('Full name cannot be empty')
        return v.strip()
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    status: Optional[str] = None
    role: Optional[str] = None
    
    @validator('full_name')
    def validate_full_name(cls, v):
        if v is not None and len(v.strip()) < 1:
            raise ValueError('Full name cannot be empty')
        return v.strip() if v else v


class UserResponse(BaseModel):
    id: int
    email: str
    full_name: str
    status: str
    role: str
    email_verified_at: Optional[str]
    tos_signed_at: Optional[str]
    last_login_at: Optional[str]
    default_org_id: Optional[int]
    created_at: str
    updated_at: str


class UserListResponse(BaseModel):
    users: List[UserResponse]
    total: int
    page: int
    per_page: int


class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)
    
    @validator('new_password')
    def validate_new_password(cls, v):
        if len(v) < 8:
            raise ValueError('New password must be at least 8 characters long')
        return v


class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordSetRequest(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8)


class UserMetrics(BaseModel):
    total_data_sources: Optional[int] = None
    total_data_sets: Optional[int] = None
    total_data_sinks: Optional[int] = None
    total_organizations: Optional[int] = None
    last_activity: Optional[str] = None
    storage_used_bytes: Optional[int] = None


class AccountSummary(BaseModel):
    user: UserResponse
    metrics: UserMetrics
    organizations: List[Dict[str, Any]]
    recent_activity: List[Dict[str, Any]]


class UserInviteRequest(BaseModel):
    email: EmailStr
    full_name: str
    org_id: Optional[int] = None
    role: str = "USER"
    
    @validator('role')
    def validate_role(cls, v):
        valid_roles = ['USER', 'ADMIN', 'OWNER']
        if v not in valid_roles:
            raise ValueError(f'Invalid role. Must be one of: {valid_roles}')
        return v


# Helper functions
def _user_to_response(user: User) -> UserResponse:
    """Convert User model to response format"""
    return UserResponse(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        status=user.status,
        role=user.role or "USER",
        email_verified_at=user.email_verified_at.isoformat() if user.email_verified_at else None,
        tos_signed_at=user.tos_signed_at.isoformat() if user.tos_signed_at else None,
        last_login_at=user.last_login_at.isoformat() if user.last_login_at else None,
        default_org_id=user.default_org_id,
        created_at=user.created_at.isoformat() if user.created_at else None,
        updated_at=user.updated_at.isoformat() if user.updated_at else None
    )


# Current user endpoints
@router.get("/current", response_model=UserResponse, summary="Get Current User")
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get current authenticated user information.
    
    Equivalent to Rails UsersController#current
    """
    return _user_to_response(current_user)


@router.get("/account_summary", response_model=AccountSummary, summary="Get Account Summary")
async def get_account_summary(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get comprehensive account summary for current user.
    
    Equivalent to Rails UsersController#current_user_account_summary
    """
    try:
        # TODO: Implement actual metrics collection when relationships are enabled
        # For now, return mock data
        metrics = UserMetrics(
            total_data_sources=5,
            total_data_sets=12,
            total_data_sinks=8,
            total_organizations=2,
            last_activity=datetime.utcnow().isoformat(),
            storage_used_bytes=1024000
        )
        
        # TODO: Get actual organizations when relationships are enabled
        organizations = [
            {
                "id": current_user.default_org_id,
                "name": "Default Organization",
                "role": "OWNER"
            }
        ]
        
        recent_activity = [
            {
                "type": "data_source_created",
                "description": "Created new data source",
                "timestamp": datetime.utcnow().isoformat()
            }
        ]
        
        return AccountSummary(
            user=_user_to_response(current_user),
            metrics=metrics,
            organizations=organizations,
            recent_activity=recent_activity
        )
        
    except Exception as e:
        logger.error(f"Account summary error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get account summary"
        )


# CRUD operations
@router.post("/", response_model=UserResponse, summary="Create User")
async def create_user(
    user_data: UserCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_user_admin_permission())
):
    """
    Create a new user (admin only).
    
    Equivalent to Rails UsersController#create
    """
    try:
        # Check if current user has admin privileges
        if not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin privileges required"
            )
        
        # Check if user already exists
        existing_user = db.query(User).filter(
            func.lower(User.email) == func.lower(user_data.email)
        ).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )
        
        # Create new user
        now = datetime.utcnow()
        hashed_password = PasswordUtils.hash_password(user_data.password)
        
        user = User(
            email=user_data.email.lower(),
            full_name=user_data.full_name,
            status=user_data.status,
            role=user_data.role,
            password_digest=hashed_password,
            created_at=now,
            updated_at=now
        )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        logger.info(f"User created: {user.email} by admin {current_user.email}")
        
        return _user_to_response(user)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"User creation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )


@router.get("/", response_model=UserListResponse, summary="List Users")
async def list_users(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    role: Optional[str] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List users with filtering and pagination (admin only).
    
    Equivalent to Rails UsersController#index
    """
    try:
        # Check if current user has admin privileges
        if not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin privileges required"
            )
        
        offset = (page - 1) * per_page
        
        query = db.query(User)
        
        # Apply filters
        if search:
            query = query.filter(
                User.email.contains(search) |
                User.full_name.contains(search)
            )
        
        if status:
            query = query.filter(User.status == status)
            
        if role:
            query = query.filter(User.role == role)
        
        # Get total count
        total = query.count()
        
        # Apply pagination and ordering
        users = query.order_by(User.created_at.desc()).offset(offset).limit(per_page).all()
        
        # Convert to response format
        user_responses = [_user_to_response(u) for u in users]
        
        return UserListResponse(
            users=user_responses,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Users list error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list users"
        )


@router.get("/{user_id}", response_model=UserResponse, summary="Get User")
async def get_user(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific user.
    
    Equivalent to Rails UsersController#show
    """
    try:
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Check permissions - users can see themselves, admins can see anyone
        if user.id != current_user.id and not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        return _user_to_response(user)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get user error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get user"
        )


@router.put("/{user_id}", response_model=UserResponse, summary="Update User")
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a user.
    
    Equivalent to Rails UsersController#update
    """
    try:
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Check permissions - users can update themselves, admins can update anyone
        if user.id != current_user.id and not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Update fields
        if user_data.email is not None:
            # Check if email is already taken
            existing_user = db.query(User).filter(
                func.lower(User.email) == func.lower(user_data.email),
                User.id != user_id
            ).first()
            if existing_user:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Email already in use"
                )
            user.email = user_data.email.lower()
        
        if user_data.full_name is not None:
            user.full_name = user_data.full_name
        
        # Only admins can change status and role
        if current_user.is_admin():
            if user_data.status is not None:
                user.status = user_data.status
            if user_data.role is not None:
                user.role = user_data.role
        
        user.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(user)
        
        logger.info(f"User updated: {user.email} by {current_user.email}")
        
        return _user_to_response(user)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"User update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user"
        )


@router.delete("/{user_id}", summary="Delete User")
async def delete_user(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_user_admin_permission())
):
    """
    Delete (deactivate) a user (admin only).
    
    Equivalent to Rails UsersController#destroy
    """
    try:
        # Check if current user has admin privileges
        if not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin privileges required"
            )
        
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Prevent self-deletion
        if user.id == current_user.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete your own account"
            )
        
        # Soft delete by setting status to DEACTIVATED
        user.status = "DEACTIVATED"
        user.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"User deleted: {user.email} by admin {current_user.email}")
        
        return {"message": "User deleted successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"User delete error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete user"
        )


# Account management endpoints
@router.put("/{user_id}/activate", summary="Activate User")
async def activate_user(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_user_admin_permission())
):
    """
    Activate a user account (admin only).
    
    Equivalent to Rails UsersController#activate
    """
    return await _update_user_status(user_id, "ACTIVE", current_user, db)


@router.put("/{user_id}/deactivate", summary="Deactivate User")
async def deactivate_user(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_user_admin_permission())
):
    """
    Deactivate a user account (admin only).
    
    Equivalent to Rails UsersController#activate with activate=false
    """
    return await _update_user_status(user_id, "DEACTIVATED", current_user, db)


@router.put("/{user_id}/lock_account", summary="Lock User Account")
async def lock_user_account(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_user_admin_permission())
):
    """
    Lock a user account (admin only).
    
    Equivalent to Rails UsersController#lock_account
    """
    return await _update_user_status(user_id, "LOCKED", current_user, db)


@router.put("/{user_id}/unlock_account", summary="Unlock User Account")
async def unlock_user_account(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_user_admin_permission())
):
    """
    Unlock a user account (admin only).
    
    Equivalent to Rails UsersController#unlock_account
    """
    return await _update_user_status(user_id, "ACTIVE", current_user, db)


async def _update_user_status(user_id: int, new_status: str, current_user: User, db: Session):
    """Helper function to update user status"""
    try:
        # Check if current user has admin privileges
        if not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin privileges required"
            )
        
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Prevent changing own status
        if user.id == current_user.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot change your own account status"
            )
        
        user.status = new_status
        user.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"User status updated to {new_status}: {user.email} by admin {current_user.email}")
        
        return {"message": f"User account {new_status.lower()} successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"User status update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user status"
        )


# Password management endpoints
@router.put("/change_password", summary="Change Password")
async def change_password(
    password_data: PasswordChangeRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Change user's password.
    
    Equivalent to Rails UsersController#change_password
    """
    try:
        # Verify current password
        if not current_user.password_digest or not PasswordUtils.verify_password(
            password_data.current_password, current_user.password_digest
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is incorrect"
            )
        
        # Check if new password is different
        if PasswordUtils.verify_password(password_data.new_password, current_user.password_digest):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="New password must be different from current password"
            )
        
        # Update password
        current_user.password_digest = PasswordUtils.hash_password(password_data.new_password)
        current_user.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Password changed for user: {current_user.email}")
        
        return {"message": "Password changed successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Password change error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to change password"
        )


@router.post("/reset_password", summary="Reset Password")
async def reset_password(
    reset_data: PasswordResetRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Initiate password reset process.
    
    Equivalent to Rails UsersController#reset_password
    """
    try:
        user = db.query(User).filter(
            func.lower(User.email) == func.lower(reset_data.email)
        ).first()
        
        if not user:
            # Don't reveal if email exists or not
            return {"message": "If the email exists, a password reset link has been sent"}
        
        if user.status != "ACTIVE":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Account is not active"
            )
        
        # TODO: Generate reset token and send email
        # For now, just log the action
        logger.info(f"Password reset requested for user: {user.email}")
        
        # TODO: Add background task to send email
        # background_tasks.add_task(send_password_reset_email, user.email, reset_token)
        
        return {"message": "If the email exists, a password reset link has been sent"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Password reset error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process password reset"
        )


@router.post("/set_password", summary="Set New Password")
async def set_password(
    password_data: PasswordSetRequest,
    db: Session = Depends(get_db)
):
    """
    Set new password using reset token.
    
    Equivalent to Rails UsersController#set_password
    """
    try:
        # TODO: Implement token validation logic
        # For now, return error as token system is not implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Password reset token system not implemented yet"
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Set password error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to set password"
        )


# User metrics and analytics
@router.get("/{user_id}/metrics", response_model=UserMetrics, summary="Get User Metrics")
async def get_user_metrics(
    user_id: int,
    metrics_name: Optional[str] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get metrics for a user.
    
    Equivalent to Rails UsersController#metrics
    """
    try:
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Check permissions - users can see their own metrics, admins can see anyone's
        if user.id != current_user.id and not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual metrics collection when relationships are enabled
        # For now, return mock data
        return UserMetrics(
            total_data_sources=5,
            total_data_sets=12,
            total_data_sinks=8,
            total_organizations=2,
            last_activity=datetime.utcnow().isoformat(),
            storage_used_bytes=1024000
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get user metrics error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get user metrics"
        )


@router.get("/{user_id}/orgs", summary="Get User Organizations")
async def get_user_organizations(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get organizations for a user.
    
    Equivalent to Rails UsersController#orgs
    """
    try:
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Check permissions
        if user.id != current_user.id and not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual organization membership when relationships are enabled
        # For now, return mock data
        return [
            {
                "id": user.default_org_id,
                "name": "Default Organization",
                "role": "OWNER",
                "status": "ACTIVE"
            }
        ]
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get user organizations error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get user organizations"
        )


# User invitation system
@router.post("/send_invite", summary="Send User Invitation")
async def send_user_invitation(
    invite_data: UserInviteRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_user_admin_permission())
):
    """
    Send invitation to a new user.
    
    Equivalent to Rails UsersController#send_invite
    """
    try:
        # Check if current user has admin privileges
        if not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin privileges required"
            )
        
        # Check if user already exists
        existing_user = db.query(User).filter(
            func.lower(User.email) == func.lower(invite_data.email)
        ).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )
        
        # TODO: Create invitation record and send email
        # For now, just log the action
        logger.info(f"User invitation sent to: {invite_data.email} by admin {current_user.email}")
        
        # TODO: Add background task to send invitation email
        # background_tasks.add_task(send_invitation_email, invite_data.email, invite_token)
        
        return {"message": "Invitation sent successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Send invitation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send invitation"
        )


# Search and utility endpoints
@router.post("/search", summary="Search Users")
async def search_users(
    search_params: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Search users with complex filters (admin only).
    """
    try:
        # Check if current user has admin privileges
        if not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin privileges required"
            )
        
        query = db.query(User)
        
        # Apply search filters
        if search_params.get('email'):
            query = query.filter(User.email.contains(search_params['email']))
        
        if search_params.get('full_name'):
            query = query.filter(User.full_name.contains(search_params['full_name']))
        
        if search_params.get('status'):
            query = query.filter(User.status == search_params['status'])
        
        if search_params.get('role'):
            query = query.filter(User.role == search_params['role'])
        
        users = query.order_by(User.updated_at.desc()).all()
        
        return [_user_to_response(u) for u in users]
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"User search error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search users"
        )