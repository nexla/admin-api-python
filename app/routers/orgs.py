from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from ..database import get_db
from ..auth import get_current_user
from ..models.user import User
from ..models.org import Org

router = APIRouter()

class OrgCreate(BaseModel):
    name: str
    description: Optional[str] = None

class OrgUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None

class OrgResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    status: str
    owner_id: int
    created_at: str
    updated_at: str
    
    class Config:
        from_attributes = True

@router.get("/", response_model=List[OrgResponse])
async def list_orgs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List organizations."""
    orgs = db.query(Org).offset(skip).limit(limit).all()
    return orgs

@router.get("/all", response_model=List[OrgResponse])
async def list_all_orgs(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all organizations (admin only)."""
    # TODO: Add super user check
    orgs = db.query(Org).all()
    return orgs

@router.get("/{org_id}", response_model=OrgResponse)
async def get_org(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific organization."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # TODO: Add proper authorization check
    return org

@router.post("/", response_model=OrgResponse, status_code=status.HTTP_201_CREATED)
async def create_org(
    org_data: OrgCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new organization."""
    db_org = Org(
        name=org_data.name,
        description=org_data.description,
        owner_id=current_user.id,
        status="ACTIVE"
    )
    
    db.add(db_org)
    db.commit()
    db.refresh(db_org)
    
    return db_org

@router.put("/{org_id}", response_model=OrgResponse)
async def update_org(
    org_id: int,
    org_data: OrgUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update an organization."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # TODO: Add proper authorization check
    
    # Update fields
    if org_data.name is not None:
        org.name = org_data.name
    if org_data.description is not None:
        org.description = org_data.description
    if org_data.status is not None:
        org.status = org_data.status
    
    db.commit()
    db.refresh(org)
    
    return org

@router.put("/{org_id}/activate")
async def activate_org(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate an organization."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    org.status = "ACTIVE"
    db.commit()
    
    return {"message": f"Organization {org_id} activated successfully"}

@router.put("/{org_id}/deactivate")
async def deactivate_org(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deactivate an organization."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    org.status = "DEACTIVATED"
    db.commit()
    
    return {"message": f"Organization {org_id} deactivated successfully"}