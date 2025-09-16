"""
Project Management Router - Comprehensive project and team management.
Handles project lifecycle, team membership, and collaboration features.
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from pydantic import BaseModel, Field, validator
from datetime import datetime

from ..database import get_db
from ..auth import get_current_user
from ..auth.rbac import RBACService, SystemPermissions
from ..models.user import User
from ..models.project import Project
from ..models.team import Team
from ..models.team_membership import TeamMembership
from ..models.org import Org
from ..services.audit_service import AuditService
from ..services.validation_service import ValidationService

router = APIRouter()

class ProjectCreate(BaseModel):
    name: str
    description: Optional[str] = None
    team_id: Optional[int] = None
    settings: Optional[Dict[str, Any]] = {}
    is_public: bool = False
    
    @validator('name')
    def validate_name(cls, v):
        result = ValidationService.validate_name(v, min_length=2, max_length=100)
        if not result['valid']:
            raise ValueError(f"Invalid name: {'; '.join(result['errors'])}")
        return result['name']

class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    team_id: Optional[int] = None
    settings: Optional[Dict[str, Any]] = None
    is_public: Optional[bool] = None
    status: Optional[str] = None

class TeamMemberAdd(BaseModel):
    user_id: int
    role: str = Field(default="member", regex="^(admin|member|viewer)$")

class TeamMemberUpdate(BaseModel):
    role: str = Field(..., regex="^(admin|member|viewer)$")

class ProjectResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    team_id: Optional[int] = None
    team_name: Optional[str] = None
    settings: Optional[Dict[str, Any]] = {}
    is_public: bool = False
    status: str
    owner_id: int
    org_id: int
    created_at: datetime
    updated_at: datetime
    member_count: int = 0
    flow_count: int = 0
    data_source_count: int = 0
    
    class Config:
        from_attributes = True

class TeamMemberResponse(BaseModel):
    id: int
    user_id: int
    user_email: str
    user_name: Optional[str] = None
    role: str
    joined_at: datetime
    
    class Config:
        from_attributes = True

# Core CRUD Operations
@router.get("/", response_model=List[ProjectResponse])
async def list_projects(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    team_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    public_only: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List projects with proper authorization."""
    # Check read permissions
    if not RBACService.has_permission(current_user, SystemPermissions.PROJECT_READ, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to list projects"
        )
    
    query = db.query(Project)
    
    # Non-admin users can only see their own projects, public projects, or projects they're team members of
    if not RBACService.has_permission(current_user, SystemPermissions.PROJECT_ADMIN, db):
        query = query.filter(
            or_(
                Project.owner_id == current_user.id,
                Project.is_public == True,
                # TODO: Add team membership filtering when relationships are active
            )
        )
    
    # Apply filters
    if team_id:
        query = query.filter(Project.team_id == team_id)
    
    if status:
        query = query.filter(Project.status == status)
    
    if public_only:
        query = query.filter(Project.is_public == True)
    
    projects = query.offset(skip).limit(limit).all()
    
    # Add computed fields
    for project in projects:
        # TODO: Calculate actual counts when relationships are active
        project.member_count = 0
        project.flow_count = 0
        project.data_source_count = 0
        
        # Add team name if team exists
        if project.team_id:
            team = db.query(Team).filter(Team.id == project.team_id).first()
            project.team_name = team.name if team else None
    
    return projects

@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get specific project."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check read permissions
    if not (project.owner_id == current_user.id or 
            project.is_public or
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_READ, db)):
        # TODO: Check team membership when relationships are active
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view this project"
        )
    
    # Add computed fields
    project.member_count = 0
    project.flow_count = 0
    project.data_source_count = 0
    
    # Add team name if team exists
    if project.team_id:
        team = db.query(Team).filter(Team.id == project.team_id).first()
        project.team_name = team.name if team else None
    
    return project

@router.post("/", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    project_data: ProjectCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new project."""
    # Check write permissions
    if not RBACService.has_permission(current_user, SystemPermissions.PROJECT_WRITE, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to create projects"
        )
    
    # Verify team exists if specified
    if project_data.team_id:
        team = db.query(Team).filter(Team.id == project_data.team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # Check if user is team member or admin
        # TODO: Implement team membership check when relationships are active
    
    try:
        project = Project(
            name=project_data.name,
            description=project_data.description,
            team_id=project_data.team_id,
            settings=project_data.settings,
            is_public=project_data.is_public,
            status="ACTIVE",
            owner_id=current_user.id,
            org_id=current_user.default_org_id,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        db.add(project)
        db.commit()
        db.refresh(project)
        
        # Log the creation
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="create",
            resource_type="project",
            resource_id=project.id,
            resource_name=project.name,
            new_values=project_data.dict(),
            request=request
        )
        
        # Add computed fields for response
        project.member_count = 1  # Creator is first member
        project.flow_count = 0
        project.data_source_count = 0
        
        return project
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: int,
    project_data: ProjectUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a project."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check write permissions
    if not (project.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_ADMIN, db)):
        # TODO: Check team admin permissions when relationships are active
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update this project"
        )
    
    try:
        # Store old values for audit
        old_values = {
            "name": project.name,
            "description": project.description,
            "team_id": project.team_id,
            "settings": project.settings,
            "is_public": project.is_public,
            "status": project.status
        }
        
        # Update fields
        if project_data.name is not None:
            project.name = project_data.name
        if project_data.description is not None:
            project.description = project_data.description
        if project_data.team_id is not None:
            # Verify team exists
            if project_data.team_id:
                team = db.query(Team).filter(Team.id == project_data.team_id).first()
                if not team:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Team not found"
                    )
            project.team_id = project_data.team_id
        if project_data.settings is not None:
            project.settings = project_data.settings
        if project_data.is_public is not None:
            project.is_public = project_data.is_public
        if project_data.status is not None:
            project.status = project_data.status
        
        project.updated_at = datetime.utcnow()
        db.commit()
        
        # Log the update
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="update",
            resource_type="project",
            resource_id=project.id,
            resource_name=project.name,
            old_values=old_values,
            new_values={k: v for k, v in project_data.dict().items() if v is not None},
            request=request
        )
        
        return project
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_project(
    project_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a project."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check delete permissions
    if not (project.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to delete this project"
        )
    
    try:
        # Log the deletion before removing
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="delete",
            resource_type="project",
            resource_id=project.id,
            resource_name=project.name,
            request=request,
            risk_level="medium"
        )
        
        db.delete(project)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# Team Management within Projects
@router.get("/{project_id}/members", response_model=List[TeamMemberResponse])
async def list_project_members(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List project team members."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check read permissions
    if not (project.owner_id == current_user.id or 
            project.is_public or
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_READ, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view project members"
        )
    
    # TODO: Implement actual team member listing when relationships are active
    members = []
    
    # Always include project owner
    owner = db.query(User).filter(User.id == project.owner_id).first()
    if owner:
        members.append({
            "id": 0,  # Special ID for owner
            "user_id": owner.id,
            "user_email": owner.email,
            "user_name": owner.full_name,
            "role": "owner",
            "joined_at": project.created_at
        })
    
    return members

@router.post("/{project_id}/members")
async def add_project_member(
    project_id: int,
    member_data: TeamMemberAdd,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Add member to project team."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check admin permissions
    if not (project.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to add project members"
        )
    
    # Verify user exists
    user = db.query(User).filter(User.id == member_data.user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # TODO: Implement actual team member addition when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="add_member",
        resource_type="project",
        resource_id=project_id,
        details={"new_member_id": member_data.user_id, "role": member_data.role},
        request=request
    )
    
    return {"message": f"User {member_data.user_id} added to project {project_id}"}

@router.put("/{project_id}/members/{user_id}")
async def update_project_member(
    project_id: int,
    user_id: int,
    member_data: TeamMemberUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update project member role."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check admin permissions
    if not (project.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update project members"
        )
    
    # Cannot change owner role
    if user_id == project.owner_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot change project owner role"
        )
    
    # TODO: Implement actual team member role update when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="update_member",
        resource_type="project",
        resource_id=project_id,
        details={"target_user_id": user_id, "new_role": member_data.role},
        request=request
    )
    
    return {"message": f"Member {user_id} role updated in project {project_id}"}

@router.delete("/{project_id}/members/{user_id}")
async def remove_project_member(
    project_id: int,
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Remove member from project team."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check admin permissions
    if not (project.owner_id == current_user.id or 
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_ADMIN, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to remove project members"
        )
    
    # Cannot remove project owner
    if user_id == project.owner_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove project owner"
        )
    
    # TODO: Implement actual team member removal when relationships are active
    
    # Log the action
    AuditService.log_action(
        db=db,
        user_id=current_user.id,
        action="remove_member",
        resource_type="project",
        resource_id=project_id,
        details={"removed_user_id": user_id},
        request=request
    )
    
    return {"message": f"Member {user_id} removed from project {project_id}"}

# Project Analytics
@router.get("/{project_id}/stats")
async def get_project_stats(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get project statistics and analytics."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check read permissions
    if not (project.owner_id == current_user.id or 
            project.is_public or
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_READ, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view project statistics"
        )
    
    # TODO: Calculate actual statistics when relationships are active
    stats = {
        "member_count": 1,
        "flow_count": 0,
        "data_source_count": 0,
        "data_sink_count": 0,
        "data_set_count": 0,
        "total_runs": 0,
        "successful_runs": 0,
        "failed_runs": 0,
        "data_processed_mb": 0,
        "last_activity": project.updated_at.isoformat(),
        "creation_date": project.created_at.isoformat()
    }
    
    return stats

# Project Activity Feed
@router.get("/{project_id}/activity")
async def get_project_activity(
    project_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get project activity feed."""
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check read permissions
    if not (project.owner_id == current_user.id or 
            project.is_public or
            RBACService.has_permission(current_user, SystemPermissions.PROJECT_READ, db)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view project activity"
        )
    
    # TODO: Implement actual activity feed when audit relationships are active
    activities = [
        {
            "id": 1,
            "type": "project_created",
            "message": f"Project {project.name} was created",
            "user_id": project.owner_id,
            "timestamp": project.created_at.isoformat()
        }
    ]
    
    return {"activities": activities}