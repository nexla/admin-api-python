from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from datetime import datetime

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.data_credentials import DataCredentials, CredentialStatuses, CredentialTypes, VerificationStatuses

router = APIRouter()

# Pydantic models for request/response validation
class DataCredentialsBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=10000)
    connector_type: str = Field(..., min_length=1, max_length=255)
    credentials_version: Optional[str] = Field(None, max_length=255)
    credential_type: Optional[CredentialTypes] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None
    expires_at: Optional[datetime] = None
    is_shared: bool = Field(False)
    allow_org_access: bool = Field(False)
    require_approval: bool = Field(False)
    auto_refresh_enabled: bool = Field(False)

class DataCredentialsCreate(DataCredentialsBase):
    credentials: Optional[Dict[str, Any]] = None
    vendor_id: Optional[int] = None
    auth_template_id: Optional[int] = None
    
class DataCredentialsUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=10000)
    connector_type: Optional[str] = Field(None, min_length=1, max_length=255)
    credentials_version: Optional[str] = Field(None, max_length=255)
    credential_type: Optional[CredentialTypes] = None
    credentials: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None
    expires_at: Optional[datetime] = None
    is_shared: Optional[bool] = None
    allow_org_access: Optional[bool] = None
    require_approval: Optional[bool] = None
    auto_refresh_enabled: Optional[bool] = None
    verified_status: Optional[str] = None

class DataCredentialsResponse(DataCredentialsBase):
    id: int
    uid: str
    status: CredentialStatuses
    verification_status: VerificationStatuses
    owner_id: int
    org_id: int
    vendor_id: Optional[int]
    auth_template_id: Optional[int]
    copied_from_id: Optional[int]
    version: int
    verified_at: Optional[datetime]
    last_verified_at: Optional[datetime]
    last_used_at: Optional[datetime]
    usage_count: int
    verification_attempts: int
    has_credentials: bool
    verified: bool
    needs_verification: bool
    is_copy: bool
    created_at: datetime
    updated_at: Optional[datetime]
    
    class Config:
        from_attributes = True

# Core CRUD operations
@router.get("/", response_model=List[DataCredentialsResponse])
async def list_credentials(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    status: Optional[CredentialStatuses] = Query(None),
    connector_type: Optional[str] = Query(None),
    shared_only: bool = Query(False),
    verified_only: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of data credentials accessible to the current user."""
    query = DataCredentials.accessible_to(current_user, 'read')
    
    # Apply filters
    if status:
        query = query.filter(DataCredentials.status == status)
    if connector_type:
        query = query.filter(DataCredentials.connector_type == connector_type)
    if shared_only:
        query = query.filter(DataCredentials.is_shared == True)
    if verified_only:
        query = query.filter(DataCredentials.verification_status == VerificationStatuses.VERIFIED)
    
    credentials = query.offset(offset).limit(limit).all()
    return credentials

@router.post("/", response_model=DataCredentialsResponse, status_code=status.HTTP_201_CREATED)
async def create_credentials(
    credentials_data: DataCredentialsCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create new data credentials."""
    # Validate connector type
    if not DataCredentials.validate_connector_type(credentials_data.connector_type):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid connector type"
        )
    
    # Create credentials
    credentials = DataCredentials(
        name=credentials_data.name,
        description=credentials_data.description,
        connector_type=credentials_data.connector_type,
        credentials_version=credentials_data.credentials_version,
        credential_type=credentials_data.credential_type or CredentialTypes.OTHER,
        metadata=credentials_data.metadata,
        expires_at=credentials_data.expires_at,
        is_shared=credentials_data.is_shared,
        allow_org_access=credentials_data.allow_org_access,
        require_approval=credentials_data.require_approval,
        auto_refresh_enabled=credentials_data.auto_refresh_enabled,
        owner_id=current_user.id,
        org_id=current_user.org_id,
        vendor_id=credentials_data.vendor_id,
        auth_template_id=credentials_data.auth_template_id
    )
    
    # Set credentials if provided
    if credentials_data.credentials:
        credentials.credentials = credentials_data.credentials
    
    # Set tags if provided
    if credentials_data.tags:
        credentials.set_tags_(credentials_data.tags)
    
    db.add(credentials)
    db.commit()
    db.refresh(credentials)
    
    return credentials

@router.get("/{credentials_id}", response_model=DataCredentialsResponse)
async def get_credentials(
    credentials_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get specific data credentials by ID."""
    credentials = db.query(DataCredentials).filter(DataCredentials.id == credentials_id).first()
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Credentials not found"
        )
    
    if not credentials.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access these credentials"
        )
    
    return credentials