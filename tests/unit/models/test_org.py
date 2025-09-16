"""
Tests for Org model.
Migrated from Rails spec/models/org_spec.rb
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.org import Org
from app.models.user import User
from app.models.org_membership import OrgMembership
from tests.factories import (
    create_org, create_user, create_org_membership, create_org_admin,
    create_data_source, create_data_set, create_data_sink, create_project, create_team
)


@pytest.mark.unit
class TestOrg:
    """Test Org model functionality"""
    
    def test_create_org(self, db_session: Session):
        """Test creating a basic organization"""
        org = create_org(
            db=db_session,
            name="Test Organization", 
            description="A test organization"
        )
        
        assert org.name == "Test Organization"
        assert org.description == "A test organization"
        assert org.status == "ACTIVE"
        assert org.is_active() is True
        assert org.allow_api_key_access is True
        assert org.owner is not None
    
    def test_org_status_methods(self, db_session: Session):
        """Test organization status checking methods"""
        # Test active org
        active_org = create_org(db=db_session, status="ACTIVE")
        assert active_org.is_active() is True
        
        # Test inactive org
        inactive_org = create_org(db=db_session, status="INACTIVE")
        assert inactive_org.is_active() is False
    
    def test_org_owner_relationship(self, db_session: Session):
        """Test organization owner relationship"""
        user = create_user(db=db_session, email="owner@example.com")
        org = create_org(db=db_session, name="Owned Org", owner=user)
        
        assert org.owner == user
        assert org.owner.email == "owner@example.com"
    
    def test_org_memberships_relationship(self, db_session: Session):
        """Test organization membership relationships"""
        org = create_org(db=db_session, name="Membership Org")
        user1 = create_user(db=db_session, email="member1@example.com")
        user2 = create_user(db=db_session, email="member2@example.com")
        
        # Create memberships
        membership1 = create_org_membership(db=db_session, user=user1, org=org)
        membership2 = create_org_membership(db=db_session, user=user2, org=org)
        
        # Refresh org to get relationships
        db_session.refresh(org)
        
        # Test that memberships are linked
        org_memberships = db_session.query(OrgMembership).filter_by(org_id=org.id).all()
        assert len(org_memberships) >= 2
        
        user_ids = {m.user_id for m in org_memberships}
        assert user1.id in user_ids
        assert user2.id in user_ids
    
    def test_org_tier_relationship(self, db_session: Session):
        """Test organization tier relationship"""
        org = create_org(db=db_session)
        
        # Org should have a tier (created by factory)
        assert org.org_tier is not None
        assert org.org_tier_id is not None
    
    def test_org_cluster_relationship(self, db_session: Session):
        """Test organization cluster relationship"""
        org = create_org(db=db_session)
        
        # Org should have a cluster (created by factory)
        assert org.cluster is not None
        assert org.cluster_id is not None
    
    def test_org_api_key_access(self, db_session: Session):
        """Test organization API key access setting"""
        # Default should allow API key access
        org = create_org(db=db_session)
        assert org.allow_api_key_access is True
        
        # Test disabling API key access
        org.allow_api_key_access = False
        db_session.commit()
        assert org.allow_api_key_access is False
    
    def test_org_search_index_name(self, db_session: Session):
        """Test organization search index name"""
        org = create_org(db=db_session, search_index_name="test_search_index")
        assert org.search_index_name == "test_search_index"


@pytest.mark.unit
class TestOrgResourceOwnership:
    """Test Org resource ownership and relationships"""
    
    def test_org_data_sources_relationship(self, db_session: Session):
        """Test organization data sources relationship"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create data sources for this org
        data_source1 = create_data_source(db=db_session, org=org, owner=user)
        data_source2 = create_data_source(db=db_session, org=org, owner=user)
        
        # Query data sources for this org
        org_sources = db_session.query(type(data_source1)).filter_by(org_id=org.id).all()
        assert len(org_sources) >= 2
    
    def test_org_data_sets_relationship(self, db_session: Session):
        """Test organization data sets relationship"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create data sets for this org
        data_set1 = create_data_set(db=db_session, org=org, owner=user)
        data_set2 = create_data_set(db=db_session, org=org, owner=user)
        
        # Query data sets for this org
        org_sets = db_session.query(type(data_set1)).filter_by(org_id=org.id).all()
        assert len(org_sets) >= 2
    
    def test_org_data_sinks_relationship(self, db_session: Session):
        """Test organization data sinks relationship"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create data sinks for this org
        data_sink1 = create_data_sink(db=db_session, org=org, owner=user)
        data_sink2 = create_data_sink(db=db_session, org=org, owner=user)
        
        # Query data sinks for this org
        org_sinks = db_session.query(type(data_sink1)).filter_by(org_id=org.id).all()
        assert len(org_sinks) >= 2
    
    def test_org_projects_relationship(self, db_session: Session):
        """Test organization projects relationship"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create projects for this org
        project1 = create_project(db=db_session, org=org, owner=user)
        project2 = create_project(db=db_session, org=org, owner=user)
        
        # Query projects for this org
        org_projects = db_session.query(type(project1)).filter_by(org_id=org.id).all()
        assert len(org_projects) >= 2


@pytest.mark.unit
class TestOrgValidation:
    """Test Org model validation"""
    
    def test_org_name_required(self, db_session: Session):
        """Test that org name is required"""
        # This would typically be handled by SQLAlchemy constraints
        with pytest.raises((ValueError, IntegrityError)):
            org = Org(
                description="No name org",
                status="ACTIVE",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(org)
            db_session.commit()
    
    def test_org_timestamps_required(self, db_session: Session):
        """Test that timestamps are required"""
        with pytest.raises((ValueError, IntegrityError)):
            org = Org(
                name="No Timestamps Org",
                description="Missing timestamps"
            )
            db_session.add(org)
            db_session.commit()
    
    def test_org_status_defaults(self, db_session: Session):
        """Test organization status defaults"""
        org = create_org(db=db_session)
        assert org.status == "ACTIVE"
        assert org.allow_api_key_access is True


@pytest.mark.unit
class TestOrgMembershipManagement:
    """Test Org membership management functionality"""
    
    def test_org_admin_creation(self, db_session: Session):
        """Test creating an organization admin"""
        org = create_org(db=db_session, name="Admin Test Org")
        admin_user = create_org_admin(org=org, db=db_session, email="admin@adminorg.test")
        
        # Verify admin user was created
        assert admin_user.email == "admin@adminorg.test"
        assert admin_user.full_name == "Admin User"
        
        # Verify admin membership was created
        admin_membership = db_session.query(OrgMembership).filter_by(
            user_id=admin_user.id, 
            org_id=org.id
        ).first()
        
        assert admin_membership is not None
        assert admin_membership.api_key.startswith("ADMIN_API_KEY_")
    
    def test_multiple_org_memberships(self, db_session: Session):
        """Test user can have multiple organization memberships"""
        user = create_user(db=db_session, email="multiorg@example.com")
        
        # Create multiple orgs and memberships
        org1 = create_org(db=db_session, name="Org 1")
        org2 = create_org(db=db_session, name="Org 2")
        org3 = create_org(db=db_session, name="Org 3")
        
        membership1 = create_org_membership(db=db_session, user=user, org=org1)
        membership2 = create_org_membership(db=db_session, user=user, org=org2)
        membership3 = create_org_membership(db=db_session, user=user, org=org3)
        
        # Verify user has multiple memberships
        user_memberships = db_session.query(OrgMembership).filter_by(user_id=user.id).all()
        assert len(user_memberships) >= 3
        
        org_ids = {m.org_id for m in user_memberships}
        assert org1.id in org_ids
        assert org2.id in org_ids
        assert org3.id in org_ids
    
    def test_org_membership_uniqueness(self, db_session: Session):
        """Test that user cannot have duplicate memberships in same org"""
        user = create_user(db=db_session)
        org = create_org(db=db_session)
        
        # Create initial membership
        create_org_membership(db=db_session, user=user, org=org)
        
        # Attempt to create duplicate membership should fail
        with pytest.raises(IntegrityError):
            create_org_membership(db=db_session, user=user, org=org)


@pytest.mark.unit
class TestOrgTimestamps:
    """Test Org timestamp functionality"""
    
    def test_created_and_updated_timestamps(self, db_session: Session):
        """Test that created_at and updated_at are set"""
        org = create_org(db=db_session)
        
        assert org.created_at is not None
        assert org.updated_at is not None
        
        # Update org and check timestamp changes
        original_updated_at = org.updated_at
        org.description = "Updated Description"
        org.updated_at = datetime.utcnow()  # In real app, this would be automatic
        db_session.commit()
        
        assert org.updated_at > original_updated_at
    
    def test_org_creation_default_values(self, db_session: Session):
        """Test that org gets proper default values on creation"""
        org = create_org(db=db_session)
        
        assert org.status == "ACTIVE"
        assert org.allow_api_key_access is True
        assert org.created_at is not None
        assert org.updated_at is not None


@pytest.mark.unit  
class TestOrgClientSettings:
    """Test Org client-specific settings"""
    
    def test_client_id_setting(self, db_session: Session):
        """Test organization client ID setting"""
        org = create_org(db=db_session, client_id="CLIENT_123")
        assert org.client_id == "CLIENT_123"
    
    def test_search_index_name_setting(self, db_session: Session):
        """Test organization search index name setting"""
        org = create_org(db=db_session, search_index_name="custom_search_index")
        assert org.search_index_name == "custom_search_index"
    
    def test_org_description(self, db_session: Session):
        """Test organization description"""
        org = create_org(db=db_session, description="This is a detailed organization description")
        assert org.description == "This is a detailed organization description"


@pytest.mark.unit
class TestOrgBusinessLogic:
    """Test Org business logic methods"""
    
    def test_org_resource_counting(self, db_session: Session):
        """Test counting resources associated with org"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create multiple resources
        create_data_source(db=db_session, org=org, owner=user)
        create_data_source(db=db_session, org=org, owner=user)
        create_data_set(db=db_session, org=org, owner=user)
        create_project(db=db_session, org=org, owner=user)
        
        # Count resources (using direct queries since we don't have count methods yet)
        source_count = db_session.query(type(create_data_source(db=db_session, org=org, owner=user))).filter_by(org_id=org.id).count()
        set_count = db_session.query(type(create_data_set(db=db_session, org=org, owner=user))).filter_by(org_id=org.id).count() 
        project_count = db_session.query(type(create_project(db=db_session, org=org, owner=user))).filter_by(org_id=org.id).count()
        
        # These counts include the ones created above plus the ones created in this test
        assert source_count >= 2
        assert set_count >= 1  
        assert project_count >= 1