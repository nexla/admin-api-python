"""
Custodians Router - Organization custodian and permission management endpoints.
Provides Rails-style custodian operations with FastAPI patterns.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from ..database import get_db
from ..auth import get_current_user
from ..models.org_custodian import OrgCustodian
from ..models.org import Org
from ..models.user import User

router = APIRouter(prefix="/custodians", tags=["custodians"])


@router.post("/assign")
async def assign_custodian(
    org_id: int,
    user_id: int, 
    role_level: str = "CUSTODIAN",
    permissions: Optional[List[str]] = None,
    expires_in_days: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Assign custodian role to user for organization"""
    
    # Validate organization exists
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    # Validate user exists
    target_user = db.query(User).filter(User.id == user_id).first()
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check if current user can assign custodians
    current_custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == current_user.id,
        OrgCustodian.org_id == org_id
    ).first()
    
    if not current_custodian or not current_custodian.can_assign_custodians_():
        raise HTTPException(status_code=403, detail="Insufficient permissions to assign custodians")
    
    # Check if custodian assignment already exists
    existing_custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == user_id,
        OrgCustodian.org_id == org_id,
        OrgCustodian.is_active == True
    ).first()
    
    if existing_custodian:
        raise HTTPException(status_code=400, detail="User is already a custodian for this organization")
    
    # Validate role level
    valid_roles = ["CUSTODIAN", "SUPER_CUSTODIAN", "ADMIN", "VIEWER"]
    if role_level not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Invalid role level. Must be one of: {valid_roles}")
    
    # Create custodian assignment
    custodian = OrgCustodian.assign_custodian(
        org=org_id,
        user=user_id,
        assigned_by_user=current_user.id,
        role_level=role_level
    )
    
    # Set expiration if specified
    if expires_in_days and expires_in_days > 0:
        custodian.expires_at = datetime.utcnow() + timedelta(days=expires_in_days)
    
    # Set custom permissions if provided
    if permissions:
        custodian.permissions = ",".join(permissions)
    
    db.add(custodian)
    db.commit()
    db.refresh(custodian)
    
    return {
        "status": "success",
        "message": f"Custodian role '{role_level}' assigned to user {target_user.name or target_user.email}",
        "custodian": custodian.to_dict()
    }


@router.get("/org/{org_id}")
async def list_org_custodians(
    org_id: int,
    include_inactive: bool = False,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List all custodians for organization"""
    
    # Check organization exists
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    # Check if current user has permission to view custodians
    current_custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == current_user.id,
        OrgCustodian.org_id == org_id
    ).first()
    
    if not current_custodian or not current_custodian.active_():
        raise HTTPException(status_code=403, detail="Insufficient permissions to view custodians")
    
    # Build query
    query = db.query(OrgCustodian).filter(OrgCustodian.org_id == org_id)
    
    if not include_inactive:
        query = query.filter(OrgCustodian.is_active == True)
    
    custodians = query.all()
    
    return {
        "custodians": [custodian.to_dict() for custodian in custodians],
        "total_custodians": len(custodians),
        "active_custodians": len([c for c in custodians if c.active_()]),
        "organization": {
            "id": org.id,
            "name": org.name
        }
    }


@router.get("/user/{user_id}")
async def list_user_custodianships(
    user_id: int,
    include_inactive: bool = False,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List all custodianships for user"""
    
    # Check if requesting own custodianships or have permission
    if current_user.id != user_id:
        # Check if current user has elevated permissions
        admin_custodian = db.query(OrgCustodian).filter(
            OrgCustodian.user_id == current_user.id,
            OrgCustodian.role_level.in_(["SUPER_CUSTODIAN", "ADMIN"])
        ).first()
        
        if not admin_custodian or not admin_custodian.active_():
            raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Build query
    query = db.query(OrgCustodian).filter(OrgCustodian.user_id == user_id)
    
    if not include_inactive:
        query = query.filter(OrgCustodian.is_active == True)
    
    custodianships = query.all()
    
    return {
        "custodianships": [custodian.to_dict() for custodian in custodianships],
        "total_custodianships": len(custodianships),
        "active_custodianships": len([c for c in custodianships if c.active_()])
    }


@router.post("/revoke/{custodian_id}")
async def revoke_custodian(
    custodian_id: int,
    reason: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Revoke custodian assignment"""
    
    custodian = db.query(OrgCustodian).filter(OrgCustodian.id == custodian_id).first()
    if not custodian:
        raise HTTPException(status_code=404, detail="Custodian assignment not found")
    
    # Check if current user can revoke custodians
    current_custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == current_user.id,
        OrgCustodian.org_id == custodian.org_id
    ).first()
    
    if not current_custodian or not current_custodian.can_assign_custodians_():
        raise HTTPException(status_code=403, detail="Insufficient permissions to revoke custodians")
    
    # Revoke custodian
    if custodian.revoke_access(reason):
        db.commit()
        return {
            "status": "success",
            "message": "Custodian access revoked",
            "custodian": custodian.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Custodian assignment was already revoked")


@router.post("/reactivate/{custodian_id}")
async def reactivate_custodian(
    custodian_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Reactivate custodian assignment"""
    
    custodian = db.query(OrgCustodian).filter(OrgCustodian.id == custodian_id).first()
    if not custodian:
        raise HTTPException(status_code=404, detail="Custodian assignment not found")
    
    # Check if current user can reactivate custodians
    current_custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == current_user.id,
        OrgCustodian.org_id == custodian.org_id
    ).first()
    
    if not current_custodian or not current_custodian.can_assign_custodians_():
        raise HTTPException(status_code=403, detail="Insufficient permissions to reactivate custodians")
    
    # Reactivate custodian
    if custodian.reactivate():
        db.commit()
        return {
            "status": "success",
            "message": "Custodian access reactivated",
            "custodian": custodian.to_dict()
        }
    else:
        raise HTTPException(status_code=400, detail="Cannot reactivate custodian")


@router.get("/check-permission/{org_id}/{user_id}/{permission}")
async def check_permission(
    org_id: int, 
    user_id: int, 
    permission: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Check if user has specific permission for organization"""
    
    custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == user_id,
        OrgCustodian.org_id == org_id
    ).first()
    
    has_permission = (custodian and 
                     custodian.active_() and 
                     custodian.has_permission_(permission))
    
    return {
        "user_id": user_id,
        "org_id": org_id,
        "permission": permission,
        "has_permission": has_permission,
        "custodian_role": custodian.role_level if custodian else None,
        "custodian_active": custodian.active_() if custodian else False
    }


@router.post("/update-permissions/{custodian_id}")
async def update_custodian_permissions(
    custodian_id: int,
    role_level: Optional[str] = None,
    can_manage_users: Optional[bool] = None,
    can_manage_data: Optional[bool] = None,
    can_manage_billing: Optional[bool] = None,
    custom_permissions: Optional[List[str]] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update custodian permissions"""
    
    custodian = db.query(OrgCustodian).filter(OrgCustodian.id == custodian_id).first()
    if not custodian:
        raise HTTPException(status_code=404, detail="Custodian assignment not found")
    
    # Check if current user can modify permissions
    current_custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == current_user.id,
        OrgCustodian.org_id == custodian.org_id
    ).first()
    
    if not current_custodian or not current_custodian.can_assign_custodians_():
        raise HTTPException(status_code=403, detail="Insufficient permissions to modify custodian permissions")
    
    # Update permissions
    if role_level is not None:
        valid_roles = ["CUSTODIAN", "SUPER_CUSTODIAN", "ADMIN", "VIEWER"]
        if role_level not in valid_roles:
            raise HTTPException(status_code=400, detail=f"Invalid role level. Must be one of: {valid_roles}")
        custodian.role_level = role_level
    
    if can_manage_users is not None:
        custodian.can_manage_users = can_manage_users
    
    if can_manage_data is not None:
        custodian.can_manage_data = can_manage_data
    
    if can_manage_billing is not None:
        custodian.can_manage_billing = can_manage_billing
    
    if custom_permissions is not None:
        custodian.permissions = ",".join(custom_permissions)
    
    db.commit()
    
    return {
        "status": "success",
        "message": "Custodian permissions updated",
        "custodian": custodian.to_dict()
    }


@router.get("/permissions/available")
async def get_available_permissions(
    current_user: User = Depends(get_current_user)
):
    """Get list of available permissions"""
    
    return {
        "role_levels": ["CUSTODIAN", "SUPER_CUSTODIAN", "ADMIN", "VIEWER"],
        "basic_permissions": [
            "manage_users", "manage_data", "manage_billing",
            "view_reports", "manage_settings", "manage_integrations"
        ],
        "advanced_permissions": [
            "assign_custodians", "manage_security", "manage_api_keys",
            "view_audit_logs", "manage_webhooks", "manage_teams"
        ],
        "permission_descriptions": {
            "manage_users": "Create, update, and delete users",
            "manage_data": "Access and manage data sources and flows",
            "manage_billing": "View and manage billing and subscriptions",
            "assign_custodians": "Assign and revoke custodian roles",
            "manage_security": "Configure security settings and policies",
            "view_audit_logs": "Access security and activity audit logs"
        }
    }


@router.get("/stats/{org_id}")
async def get_custodian_stats(
    org_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get custodian statistics for organization"""
    
    # Check if current user has permission to view stats
    current_custodian = db.query(OrgCustodian).filter(
        OrgCustodian.user_id == current_user.id,
        OrgCustodian.org_id == org_id
    ).first()
    
    if not current_custodian or not current_custodian.active_():
        raise HTTPException(status_code=403, detail="Insufficient permissions to view custodian stats")
    
    # Get all custodians for org
    all_custodians = db.query(OrgCustodian).filter(OrgCustodian.org_id == org_id).all()
    active_custodians = [c for c in all_custodians if c.active_()]
    
    # Group by role level
    role_counts = {}
    for custodian in active_custodians:
        role = custodian.role_level
        role_counts[role] = role_counts.get(role, 0) + 1
    
    # Get expiring custodians (within 30 days)
    expiring_soon = []
    for custodian in active_custodians:
        if custodian.expires_at and custodian.expires_at <= datetime.utcnow() + timedelta(days=30):
            expiring_soon.append(custodian)
    
    return {
        "total_custodians": len(all_custodians),
        "active_custodians": len(active_custodians),
        "inactive_custodians": len(all_custodians) - len(active_custodians),
        "role_distribution": role_counts,
        "expiring_soon": len(expiring_soon),
        "expiring_custodians": [c.to_dict() for c in expiring_soon]
    }