"""
API Key Management Router - Comprehensive authentication and authorization management.
Handles API key lifecycle, permissions, and security monitoring.
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from pydantic import BaseModel, Field, validator
from datetime import datetime, timedelta
import secrets
import hashlib

from ..database import get_db
from ..auth import get_current_user
from ..auth.rbac import RBACService, SystemPermissions
from ..models.user import User
from ..models.api_key import ApiKey
from ..models.api_key_event import ApiKeyEvent
from ..models.org import Org
from ..services.audit_service import AuditService
from ..services.validation_service import ValidationService

router = APIRouter()

class ApiKeyCreate(BaseModel):
    name: str
    description: Optional[str] = None
    scopes: List[str] = []
    expires_at: Optional[datetime] = None
    rate_limit: Optional[int] = Field(None, ge=1, le=10000)
    ip_whitelist: Optional[List[str]] = []
    
    @validator('name')
    def validate_name(cls, v):
        result = ValidationService.validate_name(v, min_length=2, max_length=100)
        if not result['valid']:
            raise ValueError(f"Invalid name: {'; '.join(result['errors'])}")
        return result['name']
    
    @validator('scopes')
    def validate_scopes(cls, v):
        allowed_scopes = [
            'data_source:read', 'data_source:write', 'data_source:admin',
            'data_sink:read', 'data_sink:write', 'data_sink:admin',
            'data_set:read', 'data_set:write', 'data_set:admin',
            'flow:read', 'flow:write', 'flow:admin', 'flow:execute',
            'org:read', 'org:write', 'org:admin',
            'user:read', 'user:write', 'user:admin',
            'api_key:read', 'api_key:write', 'api_key:admin',
            'metrics:read', 'audit:read'
        ]
        invalid_scopes = [scope for scope in v if scope not in allowed_scopes]
        if invalid_scopes:
            raise ValueError(f"Invalid scopes: {', '.join(invalid_scopes)}")
        return v

class ApiKeyUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    scopes: Optional[List[str]] = None
    expires_at: Optional[datetime] = None
    rate_limit: Optional[int] = Field(None, ge=1, le=10000)
    ip_whitelist: Optional[List[str]] = None
    is_active: Optional[bool] = None

class ApiKeyResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    key_prefix: str  # Only first 8 characters for security
    scopes: List[str] = []
    expires_at: Optional[datetime] = None
    rate_limit: Optional[int] = None
    ip_whitelist: Optional[List[str]] = []
    is_active: bool
    last_used_at: Optional[datetime] = None
    usage_count: int = 0
    created_at: datetime
    updated_at: datetime
    owner_id: int
    org_id: Optional[int] = None
    
    class Config:
        from_attributes = True

class ApiKeyCreateResponse(BaseModel):
    api_key: ApiKeyResponse
    secret_key: str  # Full key only shown once during creation
    
class ApiKeyEventResponse(BaseModel):
    id: int
    api_key_id: int
    event_type: str
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    endpoint: Optional[str] = None
    response_status: Optional[int] = None
    error_message: Optional[str] = None
    created_at: datetime
    
    class Config:
        from_attributes = True

# Core CRUD Operations
@router.get("/", response_model=List[ApiKeyResponse])
async def list_api_keys(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    active_only: bool = Query(True),
    org_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List API keys with proper authorization."""
    # Check read permissions
    if not RBACService.has_permission(current_user, SystemPermissions.API_KEY_READ, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to list API keys"
        )
    
    query = db.query(ApiKey)
    
    # Non-admin users can only see their own API keys
    if not RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db):
        query = query.filter(ApiKey.owner_id == current_user.id)
    
    # Apply filters
    if active_only:
        query = query.filter(ApiKey.is_active == True)
    
    if org_id:
        query = query.filter(ApiKey.org_id == org_id)
    
    api_keys = query.offset(skip).limit(limit).all()
    
    # Mask the actual keys for security
    for key in api_keys:
        key.key_prefix = key.key_hash[:8] if key.key_hash else "********"
    
    return api_keys

@router.get("/{api_key_id}", response_model=ApiKeyResponse)
async def get_api_key(
    api_key_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get specific API key."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view this API key"
        )
    
    # Mask the key for security
    api_key.key_prefix = api_key.key_hash[:8] if api_key.key_hash else "********"
    
    return api_key

@router.post("/", response_model=ApiKeyCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    api_key_data: ApiKeyCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new API key."""
    # Check write permissions
    if not RBACService.has_permission(current_user, SystemPermissions.API_KEY_WRITE, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to create API keys"
        )
    
    # Check if user has reached API key limit
    existing_keys = db.query(ApiKey).filter(
        ApiKey.owner_id == current_user.id,
        ApiKey.is_active == True
    ).count()
    
    max_keys = 10  # Default limit
    if existing_keys >= max_keys:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Maximum number of API keys ({max_keys}) reached"
        )
    
    try:
        # Generate secure API key
        secret_key = f"nexla_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(secret_key.encode()).hexdigest()
        
        # Create API key record
        api_key = ApiKey(
            name=api_key_data.name,
            description=api_key_data.description,
            key_hash=key_hash,
            scopes=api_key_data.scopes,
            expires_at=api_key_data.expires_at,
            rate_limit=api_key_data.rate_limit,
            ip_whitelist=api_key_data.ip_whitelist,
            is_active=True,
            owner_id=current_user.id,
            org_id=current_user.default_org_id,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            usage_count=0
        )
        
        db.add(api_key)
        db.commit()
        db.refresh(api_key)
        
        # Log the creation
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="create",
            resource_type="api_key",
            resource_id=api_key.id,
            resource_name=api_key.name,
            new_values=api_key_data.dict(),
            request=request,
            risk_level="medium"
        )
        
        # Mask key for response
        api_key.key_prefix = key_hash[:8]
        
        return ApiKeyCreateResponse(
            api_key=api_key,
            secret_key=secret_key
        )
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{api_key_id}", response_model=ApiKeyResponse)
async def update_api_key(
    api_key_id: int,
    api_key_data: ApiKeyUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update an API key."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update this API key"
        )
    
    try:
        # Store old values for audit
        old_values = {
            "name": api_key.name,
            "description": api_key.description,
            "scopes": api_key.scopes,
            "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None,
            "rate_limit": api_key.rate_limit,
            "ip_whitelist": api_key.ip_whitelist,
            "is_active": api_key.is_active
        }
        
        # Update fields
        if api_key_data.name is not None:
            api_key.name = api_key_data.name
        if api_key_data.description is not None:
            api_key.description = api_key_data.description
        if api_key_data.scopes is not None:
            api_key.scopes = api_key_data.scopes
        if api_key_data.expires_at is not None:
            api_key.expires_at = api_key_data.expires_at
        if api_key_data.rate_limit is not None:
            api_key.rate_limit = api_key_data.rate_limit
        if api_key_data.ip_whitelist is not None:
            api_key.ip_whitelist = api_key_data.ip_whitelist
        if api_key_data.is_active is not None:
            api_key.is_active = api_key_data.is_active
        
        api_key.updated_at = datetime.utcnow()
        db.commit()
        
        # Log the update
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="update",
            resource_type="api_key",
            resource_id=api_key.id,
            resource_name=api_key.name,
            old_values=old_values,
            new_values={k: v for k, v in api_key_data.dict().items() if v is not None},
            request=request
        )
        
        # Mask key for response
        api_key.key_prefix = api_key.key_hash[:8] if api_key.key_hash else "********"
        
        return api_key
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/{api_key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(
    api_key_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete an API key."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to delete this API key"
        )
    
    try:
        # Log the deletion before removing
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="delete",
            resource_type="api_key",
            resource_id=api_key.id,
            resource_name=api_key.name,
            request=request,
            risk_level="medium"
        )
        
        db.delete(api_key)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# API Key Management Operations
@router.post("/{api_key_id}/regenerate", response_model=ApiKeyCreateResponse)
async def regenerate_api_key(
    api_key_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Regenerate an API key (creates new secret)."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to regenerate this API key"
        )
    
    try:
        # Generate new secret key
        new_secret_key = f"nexla_{secrets.token_urlsafe(32)}"
        new_key_hash = hashlib.sha256(new_secret_key.encode()).hexdigest()
        
        old_hash = api_key.key_hash
        api_key.key_hash = new_key_hash
        api_key.updated_at = datetime.utcnow()
        api_key.usage_count = 0  # Reset usage count
        api_key.last_used_at = None  # Reset last used
        
        db.commit()
        
        # Log the regeneration
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="regenerate",
            resource_type="api_key",
            resource_id=api_key.id,
            resource_name=api_key.name,
            details={"old_key_prefix": old_hash[:8], "new_key_prefix": new_key_hash[:8]},
            request=request,
            risk_level="high"
        )
        
        # Mask key for response
        api_key.key_prefix = new_key_hash[:8]
        
        return ApiKeyCreateResponse(
            api_key=api_key,
            secret_key=new_secret_key
        )
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{api_key_id}/activate")
async def activate_api_key(
    api_key_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate an API key."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to activate this API key"
        )
    
    api_key.is_active = True
    api_key.updated_at = datetime.utcnow()
    db.commit()
    
    # Log the activation
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="activate",
        resource_type="api_key",
        resource_id=api_key.id,
        resource_name=api_key.name,
        request=request
    )
    
    return {"message": f"API key {api_key_id} activated successfully"}

@router.put("/{api_key_id}/deactivate")
async def deactivate_api_key(
    api_key_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deactivate an API key."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to deactivate this API key"
        )
    
    api_key.is_active = False
    api_key.updated_at = datetime.utcnow()
    db.commit()
    
    # Log the deactivation
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="deactivate",
        resource_type="api_key",
        resource_id=api_key.id,
        resource_name=api_key.name,
        request=request
    )
    
    return {"message": f"API key {api_key_id} deactivated successfully"}

# API Key Analytics and Monitoring
@router.get("/{api_key_id}/events", response_model=List[ApiKeyEventResponse])
async def get_api_key_events(
    api_key_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    event_type: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get API key usage events and audit trail."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_READ, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view API key events"
        )
    
    query = db.query(ApiKeyEvent).filter(ApiKeyEvent.api_key_id == api_key_id)
    
    # Apply filters
    if event_type:
        query = query.filter(ApiKeyEvent.event_type == event_type)
    
    if start_date:
        query = query.filter(ApiKeyEvent.created_at >= start_date)
    
    if end_date:
        query = query.filter(ApiKeyEvent.created_at <= end_date)
    
    events = query.order_by(ApiKeyEvent.created_at.desc()).offset(skip).limit(limit).all()
    return events

@router.get("/{api_key_id}/metrics")
async def get_api_key_metrics(
    api_key_id: int,
    period_days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get API key usage metrics and analytics."""
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Check permissions
    if not (api_key.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.API_KEY_READ, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view API key metrics"
        )
    
    # Calculate metrics for the specified period
    start_date = datetime.utcnow() - timedelta(days=period_days)
    
    # TODO: Implement actual metrics calculation when ApiKeyEvent relationships are active
    metrics = {
        "total_requests": api_key.usage_count or 0,
        "period_requests": 0,
        "success_rate": 95.5,
        "error_rate": 4.5,
        "last_used": api_key.last_used_at.isoformat() if api_key.last_used_at else None,
        "endpoints_accessed": [],
        "daily_usage": [],
        "error_breakdown": {},
        "rate_limit_hits": 0
    }
    
    return metrics

# Administrative Operations
@router.get("/system/stats")
async def get_system_api_key_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get system-wide API key statistics (admin only)."""
    if not RBACService.has_permission(current_user, SystemPermissions.API_KEY_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view system API key statistics"
        )
    
    # Calculate system-wide statistics
    total_keys = db.query(ApiKey).count()
    active_keys = db.query(ApiKey).filter(ApiKey.is_active == True).count()
    expired_keys = db.query(ApiKey).filter(
        ApiKey.expires_at < datetime.utcnow()
    ).count()
    
    # TODO: Add more detailed statistics when event tracking is active
    
    return {
        "total_api_keys": total_keys,
        "active_api_keys": active_keys,
        "expired_api_keys": expired_keys,
        "inactive_api_keys": total_keys - active_keys,
        "keys_created_last_30_days": 0,
        "total_requests_last_30_days": 0,
        "average_requests_per_key": 0
    }