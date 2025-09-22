from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from pydantic import BaseModel, validator
from datetime import datetime

from ..database import get_db
from ..auth import get_current_user
from ..auth.rbac import RBACService, SystemPermissions, check_admin_permission
from ..models.user import User
from ..models.org import Org
from ..models.org_membership import OrgMembership
from ..models.org_custodian import OrgCustodian
from ..models.cluster import Cluster
from ..models.user_login_audit import UserLoginAudit
from ..services.audit_service import AuditService
from ..services.validation_service import ValidationService
from ..services.async_tasks.manager import AsyncTaskManager

router = APIRouter()

class OrgCreate(BaseModel):
    name: str
    description: Optional[str] = None
    domain: Optional[str] = None
    client_identifier: Optional[str] = None
    features: Optional[Dict[str, Any]] = None
    
    @validator('name')
    def validate_name(cls, v):
        result = ValidationService.validate_name(v, min_length=2, max_length=100)
        if not result['valid']:
            raise ValueError(f"Invalid name: {'; '.join(result['errors'])}")
        return result['name']

class OrgUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    action: Optional[str] = None  # For special actions like source_activate
    features: Optional[Dict[str, Any]] = None
    domain: Optional[str] = None

class MemberUpdate(BaseModel):
    user_id: int
    role: str
    status: Optional[str] = "active"

class CustodianUpdate(BaseModel):
    custodians: List[int]
    mode: str  # add, remove, replace

class ClusterUpdate(BaseModel):
    cluster_id: int

class OrgResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    status: str
    owner_id: int
    domain: Optional[str] = None
    client_identifier: Optional[str] = None
    features: Optional[Dict[str, Any]] = None
    member_count: Optional[int] = 0
    active_flows_count: Optional[int] = 0
    created_at: str
    updated_at: str
    
    class Config:
        from_attributes = True

class MemberResponse(BaseModel):
    id: int
    user_id: int
    user_email: str
    user_name: Optional[str]
    role: str
    status: str
    joined_at: str
    
    class Config:
        from_attributes = True

class LoginHistoryResponse(BaseModel):
    login_history: List[Dict[str, Any]]
    logout_history: List[Dict[str, Any]]

@router.get("/", response_model=List[OrgResponse])
async def list_orgs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    status: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List organizations the user has access to."""
    # Start with organizations the user is a member of
    query = db.query(Org)
    
    # Non-admin users can only see orgs they're members of
    if not RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db):
        # TODO: Filter by user's organization memberships when relationships are active
        # For now, limit to user's default org or orgs they own
        query = query.filter(
            or_(
                Org.owner_id == current_user.id,
                Org.id == current_user.default_org_id
            )
        )
    
    # Apply filters
    if status:
        query = query.filter(Org.status == status)
    
    if name:
        query = query.filter(Org.name.ilike(f"%{name}%"))
    
    orgs = query.offset(skip).limit(limit).all()
    
    # Add computed fields
    for org in orgs:
        # TODO: Calculate actual counts when relationships are active
        org.member_count = 0
        org.active_flows_count = 0
    
    return orgs

@router.get("/all", response_model=List[OrgResponse])
async def list_all_orgs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    dataplane: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all organizations (super admin only)."""
    if not RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only super users can list all organizations"
        )
    
    query = db.query(Org)
    
    # Apply dataplane filter if specified
    if dataplane:
        # TODO: Add dataplane filtering when cluster relationships are active
        pass
    
    orgs = query.offset(skip).limit(limit).all()
    
    # Add computed fields
    for org in orgs:
        org.member_count = 0
        org.active_flows_count = 0
    
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
    
    # Check read permissions
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_READ, db):
        # Allow if user is owner or member
        if org.owner_id != current_user.id and org.id != current_user.default_org_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view organization"
            )
    
    # Add computed fields
    org.member_count = 0  # TODO: Calculate when relationships active
    org.active_flows_count = 0
    
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

# Member Management
@router.get("/{org_id}/members", response_model=List[MemberResponse])
async def list_org_members(
    org_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    status: Optional[str] = Query(None),
    role: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List organization members."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Check permissions
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_READ, db):
        if org.owner_id != current_user.id and org.id != current_user.default_org_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view members"
            )
    
    # TODO: Implement when OrgMembership relationships are active
    # For now, return empty list
    return []

@router.post("/{org_id}/members")
async def add_org_member(
    org_id: int,
    member_data: MemberUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Add a member to organization."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to add members"
        )
    
    # Verify user exists
    user = db.query(User).filter(User.id == member_data.user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # TODO: Create membership when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="add_member",
        resource_type="organization",
        resource_id=org_id,
        details={"new_member_id": member_data.user_id, "role": member_data.role}
    )
    
    return {"message": f"User {member_data.user_id} added to organization {org_id}"}

@router.put("/{org_id}/members/{user_id}")
async def update_org_member(
    org_id: int,
    user_id: int,
    member_data: MemberUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update organization member role or status."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update members"
        )
    
    # TODO: Update membership when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="update_member",
        resource_type="organization",
        resource_id=org_id,
        details={"target_user_id": user_id, "new_role": member_data.role, "new_status": member_data.status}
    )
    
    return {"message": f"Member {user_id} updated in organization {org_id}"}

@router.delete("/{org_id}/members/{user_id}")
async def remove_org_member(
    org_id: int,
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Remove member from organization."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to remove members"
        )
    
    # Prevent removing organization owner
    if user_id == org.owner_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove organization owner"
        )
    
    # TODO: Remove membership when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="remove_member",
        resource_type="organization",
        resource_id=org_id,
        details={"removed_user_id": user_id}
    )
    
    return {"message": f"Member {user_id} removed from organization {org_id}"}

# Custodian Management
@router.get("/{org_id}/custodians")
async def list_custodians(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List organization custodians."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_READ, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view custodians"
        )
    
    # TODO: Implement when OrgCustodian relationships are active
    return []

@router.post("/{org_id}/custodians")
async def update_custodians(
    org_id: int,
    custodian_data: CustodianUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update organization custodians."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to manage custodians"
        )
    
    # TODO: Implement custodian management when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="update_custodians",
        resource_type="organization",
        resource_id=org_id,
        details={"mode": custodian_data.mode, "custodians": custodian_data.custodians}
    )
    
    return {"message": f"Custodians updated for organization {org_id}"}

# Login History
@router.get("/{org_id}/login-history", response_model=LoginHistoryResponse)
async def get_org_login_history(
    org_id: int,
    event_type: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get organization login history."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Check permissions - super admin or org admin
    if not (RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db) or 
            (org.owner_id == current_user.id and RBACService.has_permission(current_user, SystemPermissions.ORG_ADMIN, db))):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view login history"
        )
    
    response = LoginHistoryResponse(login_history=[], logout_history=[])
    
    # TODO: Implement when UserLoginAudit has org_id field
    # For now return empty
    
    return response

# Cluster Management
@router.put("/{org_id}/cluster")
async def update_org_cluster(
    org_id: int,
    cluster_data: ClusterUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update organization cluster assignment (super admin only)."""
    if not RBACService.has_permission(current_user, SystemPermissions.SYSTEM_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only super users can update cluster assignments"
        )
    
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Verify cluster exists
    cluster = db.query(Cluster).filter(Cluster.id == cluster_data.cluster_id).first()
    if not cluster:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cluster not found"
        )
    
    # TODO: Update cluster assignment when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="update_cluster",
        resource_type="organization",
        resource_id=org_id,
        details={"cluster_id": cluster_data.cluster_id},
        risk_level="high"
    )
    
    return {"message": f"Cluster updated for organization {org_id}"}

# Rate Limiting Management
@router.put("/{org_id}/rate-limits")
async def manage_rate_limits(
    org_id: int,
    action: str = Query(..., pattern="^(activate|pause)$"),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate or pause rate-limited sources for organization."""
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to manage rate limits"
        )
    
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # TODO: Implement rate limit management when org tier relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action=f"rate_limits_{action}",
        resource_type="organization",
        resource_id=org_id,
        details={"action": action, "status": status},
        risk_level="medium"
    )
    
    return {"message": f"Rate limits {action}d for organization {org_id}"}

# Organization Features
@router.get("/{org_id}/features")
async def get_org_features(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get organization feature flags and settings."""
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_READ, db):
        if org.owner_id != current_user.id and org.id != current_user.default_org_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view features"
            )
    
    # TODO: Implement feature flag retrieval
    features = org.features or {}
    
    return {"features": features}

@router.put("/{org_id}/features")
async def update_org_features(
    org_id: int,
    features: Dict[str, Any],
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update organization features (admin only)."""
    if not RBACService.has_permission(current_user, SystemPermissions.ORG_ADMIN, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update features"
        )
    
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Update features
    old_features = org.features or {}
    org.features = features
    org.updated_at = datetime.utcnow()
    db.commit()
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="update_features",
        resource_type="organization",
        resource_id=org_id,
        old_values={"features": old_features},
        new_values={"features": features}
    )
    
    return {"message": f"Features updated for organization {org_id}"}