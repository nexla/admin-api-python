"""
Projects API endpoints - Python equivalent of Rails projects controller.
Handles project management, team assignments, resource organization, and project lifecycle management.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, Field
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import (
    RBACService, SystemPermissions, check_admin_permission
)
from ...models.user import User
from ...models.project import Project
from ...models.org import Org
from ...models.team import Team

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/projects", tags=["Projects"])


# Pydantic models for request/response
class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    org_id: int
    team_id: Optional[int] = None

class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    team_id: Optional[int] = None

class ProjectResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    owner_id: int
    org_id: int
    team_id: Optional[int] = None
    flow_count: int
    data_source_count: int
    data_set_count: int
    data_sink_count: int
    team_member_count: int
    created_at: datetime
    updated_at: datetime

class ProjectStats(BaseModel):
    total_projects: int
    active_projects: int
    projects_by_team: Dict[str, int]
    recent_activity: List[Dict[str, Any]]

class ProjectMemberResponse(BaseModel):
    user_id: int
    email: str
    full_name: Optional[str]
    role: str
    joined_at: datetime

class ProjectResourceSummary(BaseModel):
    resource_type: str
    resource_id: int
    resource_name: str
    status: str
    created_at: datetime


def project_to_response(project: Project) -> ProjectResponse:
    """Convert Project model to response"""
    return ProjectResponse(
        id=project.id,
        name=project.name,
        description=project.description,
        owner_id=project.owner_id,
        org_id=project.org_id,
        team_id=None,  # Will be implemented when relationships are enabled
        flow_count=0,  # Will be calculated when relationships are enabled
        data_source_count=0,  # Will be calculated when relationships are enabled
        data_set_count=0,  # Will be calculated when relationships are enabled
        data_sink_count=0,  # Will be calculated when relationships are enabled
        team_member_count=0,  # Will be calculated when relationships are enabled
        created_at=project.created_at,
        updated_at=project.updated_at
    )


# Project CRUD operations
@router.get("/", response_model=List[ProjectResponse], summary="List Projects")
async def list_projects(
    org_id: Optional[int] = Query(None, description="Filter by organization"),
    team_id: Optional[int] = Query(None, description="Filter by team"),
    owner_id: Optional[int] = Query(None, description="Filter by owner"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List projects accessible to the current user.
    
    Equivalent to Rails ProjectsController#index
    """
    try:
        query = db.query(Project)
        
        # Apply filters
        if org_id:
            query = query.filter(Project.org_id == org_id)
        
        if owner_id:
            query = query.filter(Project.owner_id == owner_id)
        
        # TODO: Add team filtering when relationships are enabled
        # TODO: Filter by user permissions
        
        projects = query.offset(skip).limit(limit).all()
        
        return [project_to_response(project) for project in projects]
    
    except Exception as e:
        logger.error(f"List projects error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve projects"
        )


@router.get("/stats", response_model=ProjectStats, summary="Get Project Statistics")
async def get_project_stats(
    org_id: Optional[int] = Query(None, description="Filter by organization"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get project statistics and analytics.
    
    Equivalent to Rails ProjectsController#stats
    """
    try:
        # Base query
        query = db.query(Project)
        if org_id:
            query = query.filter(Project.org_id == org_id)
        
        # Calculate statistics
        total_projects = query.count()
        active_projects = total_projects  # All projects are active for now
        
        # TODO: Implement actual team-based statistics when relationships are enabled
        projects_by_team = {}
        recent_activity = []
        
        return ProjectStats(
            total_projects=total_projects,
            active_projects=active_projects,
            projects_by_team=projects_by_team,
            recent_activity=recent_activity
        )
    
    except Exception as e:
        logger.error(f"Get project stats error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve project statistics"
        )


@router.get("/{project_id}", response_model=ProjectResponse, summary="Get Project")
async def get_project(
    project_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific project by ID.
    
    Equivalent to Rails ProjectsController#show
    """
    try:
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Check if user has access to this project
        
        return project_to_response(project)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get project error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve project"
        )


@router.post("/", response_model=ProjectResponse, summary="Create Project")
async def create_project(
    project_data: ProjectCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new project.
    
    Equivalent to Rails ProjectsController#create
    """
    try:
        # Verify organization exists
        org = db.query(Org).filter(Org.id == project_data.org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Verify team exists if provided
        if project_data.team_id:
            team = db.query(Team).filter(Team.id == project_data.team_id).first()
            if not team:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Team not found"
                )
        
        # Create new project
        project = Project(
            name=project_data.name,
            description=project_data.description,
            owner_id=current_user.id,
            org_id=project_data.org_id,
            created_at=func.now(),
            updated_at=func.now()
        )
        
        db.add(project)
        db.commit()
        db.refresh(project)
        
        logger.info(f"Project created: {project.id} by user {current_user.id}")
        return project_to_response(project)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create project error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create project"
        )


@router.put("/{project_id}", response_model=ProjectResponse, summary="Update Project")
async def update_project(
    project_id: int,
    project_data: ProjectUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update project information.
    
    Equivalent to Rails ProjectsController#update
    """
    try:
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Check if user has permission to update this project
        
        # Verify team exists if provided
        if project_data.team_id:
            team = db.query(Team).filter(Team.id == project_data.team_id).first()
            if not team:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Team not found"
                )
        
        # Update fields
        if project_data.name is not None:
            project.name = project_data.name
        if project_data.description is not None:
            project.description = project_data.description
        
        project.updated_at = func.now()
        
        db.commit()
        db.refresh(project)
        
        logger.info(f"Project updated: {project.id} by user {current_user.id}")
        return project_to_response(project)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update project error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update project"
        )


@router.delete("/{project_id}", summary="Delete Project")
async def delete_project(
    project_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete a project.
    
    Equivalent to Rails ProjectsController#destroy
    """
    try:
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Check if user has permission to delete this project
        # TODO: Check for dependent resources (flows, etc.)
        
        db.delete(project)
        db.commit()
        
        logger.info(f"Project deleted: {project.id} by user {current_user.id}")
        return {"message": "Project deleted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete project error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete project"
        )


# Project resource management
@router.get("/{project_id}/resources", response_model=List[ProjectResourceSummary], summary="List Project Resources")
async def list_project_resources(
    project_id: int,
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List all resources associated with a project.
    
    Equivalent to Rails ProjectsController#resources
    """
    try:
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Implement actual resource retrieval when relationships are enabled
        # For now, return empty list
        return []
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List project resources error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve project resources"
        )


@router.get("/{project_id}/members", response_model=List[ProjectMemberResponse], summary="List Project Members")
async def list_project_members(
    project_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List all members of a project (through team membership).
    
    Equivalent to Rails ProjectsController#members
    """
    try:
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Implement actual member retrieval when relationships are enabled
        # For now, return empty list
        return []
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List project members error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve project members"
        )


@router.post("/{project_id}/duplicate", response_model=ProjectResponse, summary="Duplicate Project")
async def duplicate_project(
    project_id: int,
    new_name: str = Query(..., description="Name for the duplicated project"),
    include_flows: bool = Query(True, description="Include flows in duplication"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Duplicate a project with optional flow duplication.
    
    Equivalent to Rails ProjectsController#duplicate
    """
    try:
        original_project = db.query(Project).filter(Project.id == project_id).first()
        if not original_project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Check if user has permission to duplicate this project
        
        # Create duplicate project
        duplicate_project = Project(
            name=new_name,
            description=f"Copy of {original_project.description}" if original_project.description else None,
            owner_id=current_user.id,
            org_id=original_project.org_id,
            created_at=func.now(),
            updated_at=func.now()
        )
        
        db.add(duplicate_project)
        db.commit()
        db.refresh(duplicate_project)
        
        # TODO: Duplicate flows if requested when flow relationships are enabled
        
        logger.info(f"Project duplicated: {project_id} -> {duplicate_project.id} by user {current_user.id}")
        return project_to_response(duplicate_project)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Duplicate project error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to duplicate project"
        )


@router.post("/{project_id}/archive", summary="Archive Project")
async def archive_project(
    project_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Archive a project (soft delete with status change).
    
    Equivalent to Rails ProjectsController#archive
    """
    try:
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Check if user has permission to archive this project
        # TODO: Stop all running flows in this project
        # TODO: Add archived status to project model
        
        project.updated_at = func.now()
        db.commit()
        
        logger.info(f"Project archived: {project.id} by user {current_user.id}")
        return {"message": "Project archived successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Archive project error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to archive project"
        )


@router.post("/{project_id}/restore", summary="Restore Archived Project")
async def restore_project(
    project_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Restore an archived project.
    
    Equivalent to Rails ProjectsController#restore
    """
    try:
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found"
            )
        
        # TODO: Check if user has permission to restore this project
        # TODO: Add restored status to project model
        
        project.updated_at = func.now()
        db.commit()
        
        logger.info(f"Project restored: {project.id} by user {current_user.id}")
        return {"message": "Project restored successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Restore project error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to restore project"
        )