"""
Organizations API endpoints - Python equivalent of Rails organizations controller.
Handles CRUD operations for organizations, membership management, and org-level settings.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, validator
from typing import Optional, List
import logging

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...models.user import User
from ...models.org import Org
from ...models.org_membership import OrgMembership

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/organizations", tags=["Organizations"])


# Pydantic models for request/response
class OrgCreate(BaseModel):
    name: str
    description: Optional[str] = None
    allow_api_key_access: bool = True
    search_index_name: Optional[str] = None
    client_identifier: Optional[str] = None
    
    @validator('name')
    def validate_name(cls, v):
        if len(v.strip()) < 2:
            raise ValueError('Organization name must be at least 2 characters')
        if len(v) > 255:
            raise ValueError('Organization name must be less than 255 characters')
        return v.strip()


class OrgUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    allow_api_key_access: Optional[bool] = None
    search_index_name: Optional[str] = None
    client_identifier: Optional[str] = None
    
    @validator('name')
    def validate_name(cls, v):
        if v is not None:
            if len(v.strip()) < 2:
                raise ValueError('Organization name must be at least 2 characters')
            if len(v) > 255:
                raise ValueError('Organization name must be less than 255 characters')
            return v.strip()
        return v


class OrgResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    status: str
    allow_api_key_access: bool
    search_index_name: Optional[str]
    client_identifier: Optional[str]
    owner_id: int
    created_at: str
    updated_at: str


class OrgListResponse(BaseModel):
    organizations: List[OrgResponse]
    total: int
    page: int
    per_page: int


@router.post("/", response_model=OrgResponse, summary="Create Organization")
async def create_organization(
    org_data: OrgCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new organization.
    
    Equivalent to Rails OrganizationsController#create
    """
    try:
        from datetime import datetime
        
        # Check if organization name already exists for this user
        existing_org = db.query(Org).filter(
            func.lower(Org.name) == func.lower(org_data.name),
            Org.owner_id == current_user.id
        ).first()
        
        if existing_org:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Organization with this name already exists"
            )
        
        # Create new organization
        now = datetime.utcnow()
        org = Org(
            name=org_data.name,
            description=org_data.description,
            status="ACTIVE",
            allow_api_key_access=org_data.allow_api_key_access,
            search_index_name=org_data.search_index_name,
            client_identifier=org_data.client_identifier,
            owner_id=current_user.id,
            created_at=now,
            updated_at=now
        )
        
        db.add(org)
        db.commit()
        db.refresh(org)
        
        # Create org membership for the owner
        membership = OrgMembership(
            user_id=current_user.id,
            org_id=org.id,
            role="admin",
            status="ACTIVE",
            created_at=now,
            updated_at=now
        )
        db.add(membership)
        db.commit()
        
        logger.info(f"Organization created: {org.name} by user {current_user.email}")
        
        return OrgResponse(
            id=org.id,
            name=org.name,
            description=org.description,
            status=org.status,
            allow_api_key_access=org.allow_api_key_access,
            search_index_name=org.search_index_name,
            client_identifier=org.client_identifier,
            owner_id=org.owner_id,
            created_at=org.created_at.isoformat() if org.created_at else None,
            updated_at=org.updated_at.isoformat() if org.updated_at else None
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Organization creation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create organization"
        )


@router.get("/", response_model=OrgListResponse, summary="List Organizations")
async def list_organizations(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List organizations for current user.
    
    Equivalent to Rails OrganizationsController#index
    """
    try:
        # Get organizations where user is owner or member
        offset = (page - 1) * per_page
        
        # Query for organizations where user is owner
        owned_orgs_query = db.query(Org).filter(
            Org.owner_id == current_user.id,
            Org.status == "ACTIVE"
        )
        
        # For now, just show owned organizations
        # TODO: Add memberships via OrgMembership joins
        orgs_query = owned_orgs_query.order_by(Org.created_at.desc())
        
        total = orgs_query.count()
        orgs = orgs_query.offset(offset).limit(per_page).all()
        
        org_responses = []
        for org in orgs:
            org_responses.append(OrgResponse(
                id=org.id,
                name=org.name,
                description=org.description,
                status=org.status,
                allow_api_key_access=org.allow_api_key_access,
                search_index_name=org.search_index_name,
                client_identifier=org.client_identifier,
                owner_id=org.owner_id,
                created_at=org.created_at.isoformat() if org.created_at else None,
                updated_at=org.updated_at.isoformat() if org.updated_at else None
            ))
        
        return OrgListResponse(
            organizations=org_responses,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except Exception as e:
        logger.error(f"Organization list error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list organizations"
        )


@router.get("/{org_id}", response_model=OrgResponse, summary="Get Organization")
async def get_organization(
    org_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific organization.
    
    Equivalent to Rails OrganizationsController#show
    """
    try:
        # Get organization
        org = db.query(Org).filter(Org.id == org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Check if user has access (owner or member)
        if org.owner_id != current_user.id:
            # TODO: Check membership via OrgMembership
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        return OrgResponse(
            id=org.id,
            name=org.name,
            description=org.description,
            status=org.status,
            allow_api_key_access=org.allow_api_key_access,
            search_index_name=org.search_index_name,
            client_identifier=org.client_identifier,
            owner_id=org.owner_id,
            created_at=org.created_at.isoformat() if org.created_at else None,
            updated_at=org.updated_at.isoformat() if org.updated_at else None
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Organization get error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get organization"
        )


@router.put("/{org_id}", response_model=OrgResponse, summary="Update Organization")
async def update_organization(
    org_id: int,
    org_data: OrgUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update an organization.
    
    Equivalent to Rails OrganizationsController#update
    """
    try:
        from datetime import datetime
        
        # Get organization
        org = db.query(Org).filter(Org.id == org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Check if user is owner (only owner can update)
        if org.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only organization owner can update"
            )
        
        # Check for name conflicts if name is being changed
        if org_data.name and org_data.name != org.name:
            existing_org = db.query(Org).filter(
                func.lower(Org.name) == func.lower(org_data.name),
                Org.owner_id == current_user.id,
                Org.id != org_id
            ).first()
            
            if existing_org:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Organization with this name already exists"
                )
        
        # Update fields
        if org_data.name is not None:
            org.name = org_data.name
        if org_data.description is not None:
            org.description = org_data.description
        if org_data.allow_api_key_access is not None:
            org.allow_api_key_access = org_data.allow_api_key_access
        if org_data.search_index_name is not None:
            org.search_index_name = org_data.search_index_name
        if org_data.client_identifier is not None:
            org.client_identifier = org_data.client_identifier
        
        org.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(org)
        
        logger.info(f"Organization updated: {org.name} by user {current_user.email}")
        
        return OrgResponse(
            id=org.id,
            name=org.name,
            description=org.description,
            status=org.status,
            allow_api_key_access=org.allow_api_key_access,
            search_index_name=org.search_index_name,
            client_identifier=org.client_identifier,
            owner_id=org.owner_id,
            created_at=org.created_at.isoformat() if org.created_at else None,
            updated_at=org.updated_at.isoformat() if org.updated_at else None
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Organization update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update organization"
        )


@router.delete("/{org_id}", summary="Delete Organization")
async def delete_organization(
    org_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete (deactivate) an organization.
    
    Equivalent to Rails OrganizationsController#destroy
    """
    try:
        from datetime import datetime
        
        # Get organization
        org = db.query(Org).filter(Org.id == org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Check if user is owner
        if org.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only organization owner can delete"
            )
        
        # Soft delete by setting status to DEACTIVATED
        org.status = "DEACTIVATED"
        org.updated_at = datetime.utcnow()
        
        # Also deactivate all memberships
        memberships = db.query(OrgMembership).filter(
            OrgMembership.org_id == org_id
        ).all()
        
        for membership in memberships:
            membership.status = "DEACTIVATED"
            membership.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Organization deleted: {org.name} by user {current_user.email}")
        
        return {"message": "Organization deleted successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Organization delete error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete organization"
        )