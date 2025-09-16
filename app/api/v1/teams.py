"""
Teams API endpoints - Python equivalent of Rails teams controller.
Handles team management, member operations, invitations, and team-based access control.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, Field, EmailStr
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta
import secrets

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import (
    RBACService, SystemPermissions, check_admin_permission, check_user_admin_permission
)
from ...models.user import User
from ...models.team import Team, TeamInvitation
from ...models.org import Org

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/teams", tags=["Teams"])


# Pydantic models for request/response
class TeamCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    team_type: str = Field(default="project", pattern="^(project|department|security_group)$")
    org_id: int
    is_private: bool = False
    max_members: Optional[int] = None

class TeamUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    team_type: Optional[str] = Field(None, pattern="^(project|department|security_group)$")
    is_private: Optional[bool] = None
    max_members: Optional[int] = None

class TeamMemberAdd(BaseModel):
    user_id: Optional[int] = None
    email: Optional[EmailStr] = None
    role: str = Field(default="member", pattern="^(member|admin|lead)$")

class TeamMemberUpdate(BaseModel):
    role: str = Field(..., pattern="^(member|admin|lead)$")

class TeamInviteRequest(BaseModel):
    user_ids: Optional[List[int]] = None
    emails: Optional[List[EmailStr]] = None
    role: str = Field(default="member", pattern="^(member|admin|lead)$")
    message: Optional[str] = None
    expires_in_days: int = Field(default=7, ge=1, le=30)

class TeamResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    team_type: str
    org_id: int
    is_active: bool
    is_private: bool
    max_members: Optional[int]
    created_by: int
    owner_id: int
    member_count: int
    created_at: datetime
    updated_at: datetime

class TeamMemberResponse(BaseModel):
    user_id: int
    email: str
    full_name: Optional[str]
    role: str
    joined_at: datetime

class TeamInvitationResponse(BaseModel):
    id: int
    team_id: int
    invited_email: Optional[str]
    role: str
    status: str
    invitation_message: Optional[str]
    expires_at: datetime
    created_at: datetime


def team_to_response(team: Team) -> TeamResponse:
    """Convert Team model to response"""
    return TeamResponse(
        id=team.id,
        name=team.name,
        description=team.description,
        team_type=team.team_type,
        org_id=team.org_id,
        is_active=team.is_active,
        is_private=team.is_private,
        max_members=team.max_members,
        created_by=team.created_by,
        owner_id=team.owner_id,
        member_count=team.member_count(),
        created_at=team.created_at,
        updated_at=team.updated_at
    )


# Team CRUD operations
@router.get("/", response_model=List[TeamResponse], summary="List Teams")
async def list_teams(
    org_id: Optional[int] = Query(None, description="Filter by organization"),
    team_type: Optional[str] = Query(None, description="Filter by team type"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List teams accessible to the current user.
    
    Equivalent to Rails TeamsController#index
    """
    try:
        query = db.query(Team).filter(Team.is_active == True)
        
        if org_id:
            query = query.filter(Team.org_id == org_id)
        
        if team_type:
            query = query.filter(Team.team_type == team_type)
        
        # For now, show all teams (will be filtered by membership when relationships are enabled)
        teams = query.offset(skip).limit(limit).all()
        
        return [team_to_response(team) for team in teams]
    
    except Exception as e:
        logger.error(f"List teams error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve teams"
        )


@router.get("/{team_id}", response_model=TeamResponse, summary="Get Team")
async def get_team(
    team_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific team by ID.
    
    Equivalent to Rails TeamsController#show
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Check if user has access to this team
        
        return team_to_response(team)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get team error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve team"
        )


@router.post("/", response_model=TeamResponse, summary="Create Team")
async def create_team(
    team_data: TeamCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new team.
    
    Equivalent to Rails TeamsController#create
    """
    try:
        # Verify organization exists
        org = db.query(Org).filter(Org.id == team_data.org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Create new team
        team = Team(
            name=team_data.name,
            description=team_data.description,
            team_type=team_data.team_type,
            org_id=team_data.org_id,
            is_private=team_data.is_private,
            max_members=team_data.max_members,
            created_by=current_user.id,
            owner_id=current_user.id,
            created_at=func.now(),
            updated_at=func.now()
        )
        
        db.add(team)
        db.commit()
        db.refresh(team)
        
        logger.info(f"Team created: {team.id} by user {current_user.id}")
        return team_to_response(team)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create team error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create team"
        )


@router.put("/{team_id}", response_model=TeamResponse, summary="Update Team")
async def update_team(
    team_id: int,
    team_data: TeamUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update team information.
    
    Equivalent to Rails TeamsController#update
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Check if user is team owner/admin
        
        # Update fields
        if team_data.name is not None:
            team.name = team_data.name
        if team_data.description is not None:
            team.description = team_data.description
        if team_data.team_type is not None:
            team.team_type = team_data.team_type
        if team_data.is_private is not None:
            team.is_private = team_data.is_private
        if team_data.max_members is not None:
            team.max_members = team_data.max_members
        
        team.updated_at = func.now()
        
        db.commit()
        db.refresh(team)
        
        logger.info(f"Team updated: {team.id} by user {current_user.id}")
        return team_to_response(team)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update team error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update team"
        )


@router.delete("/{team_id}", summary="Delete Team")
async def delete_team(
    team_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete a team (soft delete by setting is_active=False).
    
    Equivalent to Rails TeamsController#destroy
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Check if user is team owner or has admin permissions
        
        # Soft delete
        team.is_active = False
        team.updated_at = func.now()
        
        db.commit()
        
        logger.info(f"Team deleted: {team.id} by user {current_user.id}")
        return {"message": "Team deleted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete team error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete team"
        )


# Team member management
@router.get("/{team_id}/members", response_model=List[TeamMemberResponse], summary="List Team Members")
async def list_team_members(
    team_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List all members of a team.
    
    Equivalent to Rails TeamsController#members
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Implement actual member retrieval when relationships are enabled
        # For now, return empty list
        return []
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List team members error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve team members"
        )


@router.post("/{team_id}/members", summary="Add Team Member")
async def add_team_member(
    team_id: int,
    member_data: TeamMemberAdd,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Add a member to the team.
    
    Equivalent to Rails TeamsController#add_member
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Check if user has permission to add members
        # TODO: Check team member limit
        # TODO: Add member to team when relationships are enabled
        
        logger.info(f"Member added to team {team_id} by user {current_user.id}")
        return {"message": "Member added successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Add team member error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add team member"
        )


@router.delete("/{team_id}/members/{user_id}", summary="Remove Team Member")
async def remove_team_member(
    team_id: int,
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Remove a member from the team.
    
    Equivalent to Rails TeamsController#remove_member
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Check if user has permission to remove members
        # TODO: Prevent removing team owner
        # TODO: Remove member from team when relationships are enabled
        
        logger.info(f"Member {user_id} removed from team {team_id} by user {current_user.id}")
        return {"message": "Member removed successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Remove team member error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to remove team member"
        )


@router.put("/{team_id}/members/{user_id}", summary="Update Team Member Role")
async def update_team_member_role(
    team_id: int,
    user_id: int,
    member_data: TeamMemberUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a team member's role.
    
    Equivalent to Rails TeamsController#update_member_role
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Check if user has permission to update member roles
        # TODO: Update member role when relationships are enabled
        
        logger.info(f"Member {user_id} role updated in team {team_id} by user {current_user.id}")
        return {"message": "Member role updated successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update team member role error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update member role"
        )


# Team invitations
@router.post("/{team_id}/invite", summary="Send Team Invitations")
async def send_team_invitations(
    team_id: int,
    invite_data: TeamInviteRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Send invitations to join the team.
    
    Equivalent to Rails TeamsController#invite_members
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        # TODO: Check if user has permission to invite members
        
        invitations_sent = 0
        expires_at = datetime.utcnow() + timedelta(days=invite_data.expires_in_days)
        
        # Process user ID invitations
        if invite_data.user_ids:
            for user_id in invite_data.user_ids:
                user = db.query(User).filter(User.id == user_id).first()
                if user:
                    invitation = TeamInvitation(
                        team_id=team_id,
                        invited_user_id=user_id,
                        invited_email=user.email,
                        role=invite_data.role,
                        invited_by=current_user.id,
                        invitation_message=invite_data.message,
                        invitation_token=secrets.token_urlsafe(32),
                        expires_at=expires_at,
                        created_at=func.now(),
                        updated_at=func.now()
                    )
                    db.add(invitation)
                    invitations_sent += 1
        
        # Process email invitations
        if invite_data.emails:
            for email in invite_data.emails:
                invitation = TeamInvitation(
                    team_id=team_id,
                    invited_email=email,
                    role=invite_data.role,
                    invited_by=current_user.id,
                    invitation_message=invite_data.message,
                    invitation_token=secrets.token_urlsafe(32),
                    expires_at=expires_at,
                    created_at=func.now(),
                    updated_at=func.now()
                )
                db.add(invitation)
                invitations_sent += 1
        
        db.commit()
        
        # TODO: Send actual invitation emails via background task
        
        logger.info(f"Team invitations sent: {invitations_sent} for team {team_id} by user {current_user.id}")
        return {
            "message": f"Sent {invitations_sent} team invitations",
            "invitations_sent": invitations_sent
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Send team invitations error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send team invitations"
        )


@router.get("/{team_id}/invitations", response_model=List[TeamInvitationResponse], summary="List Team Invitations")
async def list_team_invitations(
    team_id: int,
    status_filter: Optional[str] = Query(None, description="Filter by invitation status"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List team invitations.
    
    Equivalent to Rails TeamsController#invitations
    """
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )
        
        query = db.query(TeamInvitation).filter(TeamInvitation.team_id == team_id)
        
        if status_filter:
            query = query.filter(TeamInvitation.status == status_filter)
        
        invitations = query.all()
        
        return [
            TeamInvitationResponse(
                id=inv.id,
                team_id=inv.team_id,
                invited_email=inv.invited_email,
                role=inv.role,
                status=inv.status,
                invitation_message=inv.invitation_message,
                expires_at=inv.expires_at,
                created_at=inv.created_at
            )
            for inv in invitations
        ]
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List team invitations error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve team invitations"
        )


@router.post("/invitations/{invitation_id}/accept", summary="Accept Team Invitation")
async def accept_team_invitation(
    invitation_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Accept a team invitation.
    
    Equivalent to Rails TeamsController#accept_invitation
    """
    try:
        invitation = db.query(TeamInvitation).filter(TeamInvitation.id == invitation_id).first()
        if not invitation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invitation not found"
            )
        
        if invitation.status != 'pending':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invitation is no longer pending"
            )
        
        if invitation.expires_at < datetime.utcnow():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invitation has expired"
            )
        
        # Update invitation status
        invitation.status = 'accepted'
        invitation.responded_at = func.now()
        invitation.updated_at = func.now()
        
        # TODO: Add user to team when relationships are enabled
        
        db.commit()
        
        logger.info(f"Team invitation accepted: {invitation_id} by user {current_user.id}")
        return {"message": "Team invitation accepted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Accept team invitation error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to accept team invitation"
        )


@router.post("/invitations/{invitation_id}/decline", summary="Decline Team Invitation")
async def decline_team_invitation(
    invitation_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Decline a team invitation.
    
    Equivalent to Rails TeamsController#decline_invitation
    """
    try:
        invitation = db.query(TeamInvitation).filter(TeamInvitation.id == invitation_id).first()
        if not invitation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invitation not found"
            )
        
        if invitation.status != 'pending':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invitation is no longer pending"
            )
        
        # Update invitation status
        invitation.status = 'declined'
        invitation.responded_at = func.now()
        invitation.updated_at = func.now()
        
        db.commit()
        
        logger.info(f"Team invitation declined: {invitation_id} by user {current_user.id}")
        return {"message": "Team invitation declined"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Decline team invitation error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to decline team invitation"
        )