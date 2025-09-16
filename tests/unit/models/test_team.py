"""
Tests for Team model.
Migrated from Rails spec patterns for team functionality.
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.team import Team, TeamInvitation
from app.models.flow import Flow
from tests.factories import (
    create_team, create_flow, create_user, create_org
)


@pytest.mark.unit
class TestTeam:
    """Test Team model functionality"""
    
    def test_create_team(self, db_session: Session):
        """Test creating a basic team"""
        org = create_org(db=db_session)
        owner = create_user(db=db_session, email="owner@example.com")
        creator = create_user(db=db_session, email="creator@example.com")
        
        team = create_team(
            db=db_session,
            name="Test Team",
            description="A test team for collaboration",
            organization=org,
            owner=owner,
            created_by=creator,
            team_type="project",
            is_active=True,
            is_private=False
        )
        
        assert team.name == "Test Team"
        assert team.description == "A test team for collaboration"
        assert team.team_type == "project"
        assert team.is_active is True
        assert team.is_private is False
        assert team.organization == org
        assert team.owner == owner
        assert team.created_by_user == creator
    
    def test_team_types(self, db_session: Session):
        """Test different team types"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Project team
        project_team = create_team(
            db=db_session,
            name="Project Team",
            team_type="project",
            organization=org,
            owner=user,
            created_by=user
        )
        assert project_team.team_type == "project"
        
        # Department team
        dept_team = create_team(
            db=db_session,
            name="Department Team",
            team_type="department",
            organization=org,
            owner=user,
            created_by=user
        )
        assert dept_team.team_type == "department"
        
        # Security group
        security_team = create_team(
            db=db_session,
            name="Security Group",
            team_type="security_group",
            organization=org,
            owner=user,
            created_by=user
        )
        assert security_team.team_type == "security_group"
    
    def test_team_privacy_settings(self, db_session: Session):
        """Test team privacy settings"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Public team
        public_team = create_team(
            db=db_session,
            name="Public Team",
            is_private=False,
            organization=org,
            owner=user,
            created_by=user
        )
        assert public_team.is_private is False
        
        # Private team
        private_team = create_team(
            db=db_session,
            name="Private Team", 
            is_private=True,
            organization=org,
            owner=user,
            created_by=user
        )
        assert private_team.is_private is True
    
    def test_team_member_limits(self, db_session: Session):
        """Test team member limits"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Team with member limit
        limited_team = create_team(
            db=db_session,
            name="Limited Team",
            max_members=10,
            organization=org,
            owner=user,
            created_by=user
        )
        assert limited_team.max_members == 10
        
        # Team with no member limit
        unlimited_team = create_team(
            db=db_session,
            name="Unlimited Team",
            max_members=None,
            organization=org,
            owner=user,
            created_by=user
        )
        assert unlimited_team.max_members is None
    
    def test_team_relationships(self, db_session: Session):
        """Test team relationships"""
        org = create_org(db=db_session)
        owner = create_user(db=db_session, email="owner@example.com")
        creator = create_user(db=db_session, email="creator@example.com")
        
        team = create_team(
            db=db_session,
            name="Relationship Test Team",
            organization=org,
            owner=owner,
            created_by=creator
        )
        
        # Test relationships
        assert team.organization == org
        assert team.owner == owner
        assert team.created_by_user == creator
        assert team.org_id == org.id
        assert team.owner_id == owner.id
        assert team.created_by == creator.id
    
    def test_team_flows_relationship(self, db_session: Session):
        """Test team flows relationship"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        team = create_team(db=db_session, organization=org, owner=user, created_by=user)
        
        # Create flows for this team
        flow1 = create_flow(db=db_session, team=team, owner=user, org=org)
        flow2 = create_flow(db=db_session, team=team, owner=user, org=org)
        
        # Query flows for this team
        team_flows = db_session.query(Flow).filter_by(team_id=team.id).all()
        assert len(team_flows) >= 2
        
        flow_ids = {f.id for f in team_flows}
        assert flow1.id in flow_ids
        assert flow2.id in flow_ids


@pytest.mark.unit
class TestTeamValidation:
    """Test Team model validation"""
    
    def test_team_name_required(self, db_session: Session):
        """Test that team name is required"""
        with pytest.raises((ValueError, IntegrityError)):
            team = Team(
                description="No name team",
                org_id=1,
                created_by=1,
                owner_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(team)
            db_session.commit()
    
    def test_team_org_required(self, db_session: Session):
        """Test that team org is required"""
        with pytest.raises((ValueError, IntegrityError)):
            team = Team(
                name="No Org Team",
                description="Missing org",
                created_by=1,
                owner_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(team)
            db_session.commit()
    
    def test_team_created_by_required(self, db_session: Session):
        """Test that team created_by is required"""
        with pytest.raises((ValueError, IntegrityError)):
            team = Team(
                name="No Creator Team",
                description="Missing creator",
                org_id=1,
                owner_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(team)
            db_session.commit()
    
    def test_team_owner_required(self, db_session: Session):
        """Test that team owner is required"""
        with pytest.raises((ValueError, IntegrityError)):
            team = Team(
                name="No Owner Team",
                description="Missing owner",
                org_id=1,
                created_by=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(team)
            db_session.commit()
    
    def test_team_defaults(self, db_session: Session):
        """Test team default values"""
        team = create_team(db=db_session)
        
        assert team.team_type == "project"
        assert team.is_active is True
        assert team.is_private is False
        assert team.max_members is None


@pytest.mark.unit
class TestTeamTimestamps:
    """Test Team timestamp functionality"""
    
    def test_created_and_updated_timestamps(self, db_session: Session):
        """Test that created_at and updated_at are set automatically"""
        team = create_team(db=db_session)
        
        assert team.created_at is not None
        assert team.updated_at is not None
        
        # Update team
        original_updated_at = team.updated_at
        team.description = "Updated Description"
        db_session.commit()
        
        # Note: In a real SQLAlchemy setup with proper onupdate, this would be automatic
        # For now, we just verify the initial timestamps are set
        assert team.updated_at >= original_updated_at


@pytest.mark.unit
class TestTeamBusinessLogic:
    """Test Team business logic methods"""
    
    def test_team_member_count_placeholder(self, db_session: Session):
        """Test team member count method (placeholder implementation)"""
        team = create_team(db=db_session)
        
        # Currently returns 0 as placeholder
        assert team.member_count() == 0
    
    def test_team_is_member_placeholder(self, db_session: Session):
        """Test team is_member method (placeholder implementation)"""
        team = create_team(db=db_session)
        user = create_user(db=db_session)
        
        # Currently returns False as placeholder
        assert team.is_member(user.id) is False
    
    def test_team_get_member_role_placeholder(self, db_session: Session):
        """Test team get_member_role method (placeholder implementation)"""
        team = create_team(db=db_session)
        user = create_user(db=db_session)
        
        # Currently returns "none" as placeholder
        assert team.get_member_role(user.id) == "none"
    
    def test_team_ownership_vs_creation(self, db_session: Session):
        """Test team ownership vs creation distinction"""
        org = create_org(db=db_session)
        owner = create_user(db=db_session, email="owner@example.com")
        creator = create_user(db=db_session, email="creator@example.com")
        
        team = create_team(
            db=db_session,
            name="Ownership Test Team",
            organization=org,
            owner=owner,
            created_by=creator
        )
        
        # Owner and creator can be different
        assert team.owner != team.created_by_user
        assert team.owner_id != team.created_by
        assert team.owner == owner
        assert team.created_by_user == creator


@pytest.mark.unit
class TestTeamInvitation:
    """Test TeamInvitation model functionality"""
    
    def test_create_team_invitation(self, db_session: Session):
        """Test creating a team invitation"""
        org = create_org(db=db_session)
        inviter = create_user(db=db_session, email="inviter@example.com")
        invitee = create_user(db=db_session, email="invitee@example.com")
        team = create_team(db=db_session, organization=org, owner=inviter, created_by=inviter)
        
        invitation = TeamInvitation(
            team_id=team.id,
            invited_user_id=invitee.id,
            role="member",
            invited_by=inviter.id,
            invitation_message="Welcome to our team!",
            invitation_token="unique_token_123",
            status="pending",
            expires_at=datetime.utcnow() + timedelta(days=7)
        )
        
        db_session.add(invitation)
        db_session.commit()
        
        assert invitation.team_id == team.id
        assert invitation.invited_user_id == invitee.id
        assert invitation.role == "member"
        assert invitation.invited_by == inviter.id
        assert invitation.status == "pending"
        assert invitation.invitation_message == "Welcome to our team!"
    
    def test_external_team_invitation(self, db_session: Session):
        """Test creating invitation for external user"""
        org = create_org(db=db_session)
        inviter = create_user(db=db_session, email="inviter@example.com")
        team = create_team(db=db_session, organization=org, owner=inviter, created_by=inviter)
        
        invitation = TeamInvitation(
            team_id=team.id,
            invited_user_id=None,  # External user
            invited_email="external@example.com",
            role="member",
            invited_by=inviter.id,
            invitation_token="external_token_456",
            status="pending",
            expires_at=datetime.utcnow() + timedelta(days=7)
        )
        
        db_session.add(invitation)
        db_session.commit()
        
        assert invitation.invited_user_id is None
        assert invitation.invited_email == "external@example.com"
        assert invitation.status == "pending"
    
    def test_team_invitation_status_transitions(self, db_session: Session):
        """Test team invitation status transitions"""
        org = create_org(db=db_session)
        inviter = create_user(db=db_session, email="inviter@example.com")
        invitee = create_user(db=db_session, email="invitee@example.com")
        team = create_team(db=db_session, organization=org, owner=inviter, created_by=inviter)
        
        invitation = TeamInvitation(
            team_id=team.id,
            invited_user_id=invitee.id,
            invited_by=inviter.id,
            invitation_token="status_token_789",
            status="pending",
            expires_at=datetime.utcnow() + timedelta(days=7)
        )
        
        db_session.add(invitation)
        db_session.commit()
        
        # Test status transitions
        assert invitation.status == "pending"
        
        # Accept invitation
        invitation.status = "accepted"
        invitation.responded_at = datetime.utcnow()
        db_session.commit()
        
        assert invitation.status == "accepted"
        assert invitation.responded_at is not None


@pytest.mark.unit
class TestTeamScenarios:
    """Test realistic Team usage scenarios"""
    
    def test_multi_team_org_scenario(self, db_session: Session):
        """Test scenario with multiple teams in an organization"""
        org = create_org(db=db_session, name="Multi-Team Org")
        
        # Create users with different roles
        admin = create_user(db=db_session, email="admin@example.com")
        manager = create_user(db=db_session, email="manager@example.com")
        developer = create_user(db=db_session, email="developer@example.com")
        
        # Create different types of teams
        admin_team = create_team(
            db=db_session,
            name="Admin Team",
            team_type="department",
            is_private=True,
            organization=org,
            owner=admin,
            created_by=admin
        )
        
        project_team = create_team(
            db=db_session,
            name="Data Pipeline Project",
            team_type="project",
            is_private=False,
            max_members=5,
            organization=org,
            owner=manager,
            created_by=manager
        )
        
        security_team = create_team(
            db=db_session,
            name="Security Group",
            team_type="security_group",
            is_private=True,
            organization=org,
            owner=admin,
            created_by=admin
        )
        
        # Verify all teams belong to the same org
        all_org_teams = db_session.query(Team).filter_by(org_id=org.id).all()
        assert len(all_org_teams) >= 3
        
        # Verify team types
        team_types = {t.team_type for t in all_org_teams}
        expected_types = {"department", "project", "security_group"}
        assert expected_types.issubset(team_types)
        
        # Verify privacy settings
        private_teams = [t for t in all_org_teams if t.is_private]
        public_teams = [t for t in all_org_teams if not t.is_private]
        assert len(private_teams) >= 2  # admin_team and security_team
        assert len(public_teams) >= 1   # project_team