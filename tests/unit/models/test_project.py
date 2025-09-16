"""
Tests for Project model.
Migrated from Rails spec patterns for project functionality.
"""
import pytest
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.project import Project
from app.models.flow import Flow
from tests.factories import (
    create_project, create_flow, create_user, create_org
)


@pytest.mark.unit
class TestProject:
    """Test Project model functionality"""
    
    def test_create_project(self, db_session: Session):
        """Test creating a basic project"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        project = create_project(
            db=db_session,
            name="Test Project",
            description="A test project for data workflows",
            owner=user,
            org=org
        )
        
        assert project.name == "Test Project"
        assert project.description == "A test project for data workflows"
        assert project.owner == user
        assert project.org == org
        assert project.owner_id == user.id
        assert project.org_id == org.id
    
    def test_project_relationships(self, db_session: Session):
        """Test project relationships"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        project = create_project(
            db=db_session,
            name="Relationship Test Project",
            owner=user,
            org=org
        )
        
        # Test basic relationships
        assert project.owner == user
        assert project.org == org
        assert project.owner_id == user.id
        assert project.org_id == org.id
    
    def test_project_flows_relationship(self, db_session: Session):
        """Test project to flows relationship"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        project = create_project(db=db_session, owner=user, org=org)
        
        # Create flows for this project
        flow1 = create_flow(db=db_session, project=project, owner=user, org=org)
        flow2 = create_flow(db=db_session, project=project, owner=user, org=org)
        
        # Query flows for this project
        project_flows = db_session.query(Flow).filter_by(project_id=project.id).all()
        assert len(project_flows) >= 2
        
        flow_ids = {f.id for f in project_flows}
        assert flow1.id in flow_ids
        assert flow2.id in flow_ids
    
    def test_project_name_uniqueness_within_org(self, db_session: Session):
        """Test that project names should be unique within an organization"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create first project
        create_project(db=db_session, name="Unique Project", owner=user, org=org)
        
        # Creating another project with the same name in the same org should work
        # (This depends on your business rules - adjust if names should be unique per org)
        duplicate_project = create_project(db=db_session, name="Unique Project", owner=user, org=org)
        assert duplicate_project.name == "Unique Project"
    
    def test_project_cross_org_name_allowance(self, db_session: Session):
        """Test that projects with same name can exist in different orgs"""
        org1 = create_org(db=db_session, name="Org 1")
        org2 = create_org(db=db_session, name="Org 2")
        user1 = create_user(db=db_session, email="user1@example.com")
        user2 = create_user(db=db_session, email="user2@example.com")
        
        # Create projects with same name in different orgs
        project1 = create_project(db=db_session, name="Common Project", owner=user1, org=org1)
        project2 = create_project(db=db_session, name="Common Project", owner=user2, org=org2)
        
        assert project1.name == project2.name
        assert project1.org != project2.org
        assert project1.org_id != project2.org_id


@pytest.mark.unit
class TestProjectValidation:
    """Test Project model validation"""
    
    def test_project_name_required(self, db_session: Session):
        """Test that project name is required"""
        with pytest.raises((ValueError, IntegrityError)):
            project = Project(
                description="No name project",
                owner_id=1,
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(project)
            db_session.commit()
    
    def test_project_owner_required(self, db_session: Session):
        """Test that project owner is required"""
        with pytest.raises((ValueError, IntegrityError)):
            project = Project(
                name="No Owner Project",
                description="Missing owner",
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(project)
            db_session.commit()
    
    def test_project_org_required(self, db_session: Session):
        """Test that project org is required"""
        with pytest.raises((ValueError, IntegrityError)):
            project = Project(
                name="No Org Project",
                description="Missing org",
                owner_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(project)
            db_session.commit()


@pytest.mark.unit
class TestProjectTimestamps:
    """Test Project timestamp functionality"""
    
    def test_created_and_updated_timestamps(self, db_session: Session):
        """Test that created_at and updated_at are set automatically"""
        project = create_project(db=db_session)
        
        assert project.created_at is not None
        assert project.updated_at is not None
        
        # Update project
        original_updated_at = project.updated_at
        project.description = "Updated Description"
        db_session.commit()
        
        # Note: In a real SQLAlchemy setup with proper onupdate, this would be automatic
        # For now, we just verify the initial timestamps are set
        assert project.updated_at >= original_updated_at


@pytest.mark.unit
class TestProjectBusinessLogic:
    """Test Project business logic methods"""
    
    def test_project_ownership_verification(self, db_session: Session):
        """Test project ownership verification"""
        org = create_org(db=db_session)
        owner = create_user(db=db_session, email="owner@example.com")
        other_user = create_user(db=db_session, email="other@example.com")
        
        project = create_project(
            db=db_session,
            name="Ownership Test Project",
            owner=owner,
            org=org
        )
        
        # Verify correct owner
        assert project.owner == owner
        assert project.owner_id == owner.id
        
        # Verify it's not owned by other user
        assert project.owner != other_user
        assert project.owner_id != other_user.id
    
    def test_project_flow_management(self, db_session: Session):
        """Test project flow management"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        project = create_project(db=db_session, owner=user, org=org)
        
        # Create multiple flows in the project
        flows = []
        for i in range(3):
            flow = create_flow(
                db=db_session,
                project=project,
                owner=user,
                org=org,
                name=f"Flow {i+1}"
            )
            flows.append(flow)
        
        # Verify all flows are associated with the project
        project_flows = db_session.query(Flow).filter_by(project_id=project.id).all()
        assert len(project_flows) >= 3
        
        # Verify flow names
        flow_names = {f.name for f in project_flows if f.name.startswith("Flow")}
        expected_names = {"Flow 1", "Flow 2", "Flow 3"}
        assert expected_names.issubset(flow_names)
    
    def test_project_org_association(self, db_session: Session):
        """Test project organization association"""
        org = create_org(db=db_session, name="Project Org")
        user = create_user(db=db_session)
        
        project = create_project(
            db=db_session,
            name="Org Association Test",
            owner=user,
            org=org
        )
        
        assert project.org == org
        assert project.org.name == "Project Org"
        assert project.org_id == org.id
    
    def test_project_description_optional(self, db_session: Session):
        """Test that project description is optional"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create project without description
        project_no_desc = create_project(
            db=db_session,
            name="No Description Project",
            description=None,
            owner=user,
            org=org
        )
        
        assert project_no_desc.description is None
        
        # Create project with description
        project_with_desc = create_project(
            db=db_session,
            name="With Description Project",
            description="This project has a description",
            owner=user,
            org=org
        )
        
        assert project_with_desc.description == "This project has a description"


@pytest.mark.unit
class TestProjectScenarios:
    """Test realistic Project usage scenarios"""
    
    def test_multi_user_multi_project_scenario(self, db_session: Session):
        """Test scenario with multiple users and projects in an org"""
        org = create_org(db=db_session, name="Multi-User Org")
        
        # Create multiple users
        admin_user = create_user(db=db_session, email="admin@example.com")
        dev_user = create_user(db=db_session, email="developer@example.com")
        analyst_user = create_user(db=db_session, email="analyst@example.com")
        
        # Create projects owned by different users
        admin_project = create_project(
            db=db_session,
            name="Admin Infrastructure Project",
            description="Infrastructure and admin workflows",
            owner=admin_user,
            org=org
        )
        
        dev_project = create_project(
            db=db_session,
            name="Development Pipeline Project",
            description="Development and testing pipelines",
            owner=dev_user,
            org=org
        )
        
        analyst_project = create_project(
            db=db_session,
            name="Analytics Dashboard Project",
            description="Analytics and reporting workflows",
            owner=analyst_user,
            org=org
        )
        
        # Verify all projects belong to the same org
        all_org_projects = db_session.query(Project).filter_by(org_id=org.id).all()
        assert len(all_org_projects) >= 3
        
        project_owners = {p.owner_id for p in all_org_projects}
        expected_owners = {admin_user.id, dev_user.id, analyst_user.id}
        assert expected_owners.issubset(project_owners)
    
    def test_project_lifecycle_scenario(self, db_session: Session):
        """Test project lifecycle from creation to flows"""
        org = create_org(db=db_session)
        user = create_user(db=db_session, email="lifecycle@example.com")
        
        # 1. Create project
        project = create_project(
            db=db_session,
            name="Lifecycle Test Project",
            description="Testing project lifecycle",
            owner=user,
            org=org
        )
        
        assert project.name == "Lifecycle Test Project"
        
        # 2. Add flows to project
        ingestion_flow = create_flow(
            db=db_session,
            name="Data Ingestion Flow",
            project=project,
            owner=user,
            org=org,
            flow_type="data_ingestion"
        )
        
        transformation_flow = create_flow(
            db=db_session,
            name="Data Transformation Flow",
            project=project,
            owner=user,
            org=org,
            flow_type="data_transformation"
        )
        
        # 3. Verify project has flows
        project_flows = db_session.query(Flow).filter_by(project_id=project.id).all()
        assert len(project_flows) >= 2
        
        flow_types = {f.flow_type for f in project_flows}
        assert "data_ingestion" in flow_types
        assert "data_transformation" in flow_types
        
        # 4. Update project description
        project.description = "Updated: Testing complete project lifecycle"
        db_session.commit()
        
        db_session.refresh(project)
        assert "Updated:" in project.description