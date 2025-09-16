"""
Tests for OrgMembership model.
Migrated from Rails spec/models/org_membership_spec.rb
"""
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.org_membership import OrgMembership
from tests.factories import create_org_membership, create_user, create_org


@pytest.mark.unit
class TestOrgMembership:
    """Test OrgMembership model functionality"""
    
    def test_create_org_membership(self, db_session: Session):
        """Test creating an organization membership"""
        # Create membership with API key
        membership = create_org_membership(db=db_session, api_key="TEST_API_KEY_123")
        
        # The membership should have a non-null org
        assert membership.org is not None
        
        # The membership should have the expected api_key
        assert membership.api_key == "TEST_API_KEY_123"
        
        # The membership should be reflected in the org
        assert len(membership.org.org_memberships) >= 1
        
        # Should be able to find the membership
        found_membership = db_session.query(OrgMembership).filter(
            OrgMembership.user_id == membership.user_id
        ).first()
        assert found_membership is not None
        assert found_membership.id == membership.id
    
    def test_duplicate_membership_prevention(self, db_session: Session):
        """Test that duplicate memberships are not allowed"""
        # Create initial membership
        membership = create_org_membership(db=db_session)
        user = membership.user
        org = membership.org
        
        # Attempt to create duplicate membership should raise IntegrityError
        with pytest.raises(IntegrityError):
            create_org_membership(db=db_session, user=user, org=org)
    
    def test_membership_status_methods(self, db_session: Session):
        """Test membership status checking methods"""
        # Create active membership
        membership = create_org_membership(db=db_session, status="ACTIVE")
        
        assert membership.is_active() is True
        assert membership.is_deactivated() is False
        
        # Deactivate membership
        membership.status = "DEACTIVATED"
        db_session.commit()
        
        assert membership.is_active() is False
        assert membership.is_deactivated() is True
    
    def test_membership_activation_deactivation(self, db_session: Session):
        """Test membership activation and deactivation methods"""
        membership = create_org_membership(db=db_session, status="ACTIVE")
        
        # Test deactivation
        membership.deactivate()
        db_session.commit()
        assert membership.status == "DEACTIVATED"
        assert membership.is_deactivated() is True
        
        # Test activation
        membership.activate()
        db_session.commit()
        assert membership.status == "ACTIVE"
        assert membership.is_active() is True
    
    def test_membership_to_dict(self, db_session: Session):
        """Test membership serialization to dictionary"""
        membership = create_org_membership(
            db=db_session, 
            api_key="DICT_TEST_KEY",
            status="ACTIVE"
        )
        
        result = membership.to_dict()
        
        # Check required fields
        assert result["id"] == membership.id
        assert result["user_id"] == membership.user_id
        assert result["org_id"] == membership.org_id
        assert result["status"] == "ACTIVE"
        assert result["is_active"] is True
        assert result["api_key"] == "DICT_TEST_KEY"
        assert "created_at" in result
        assert "updated_at" in result
    
    def test_user_org_relationship(self, db_session: Session):
        """Test that membership correctly links user and org"""
        user = create_user(db=db_session, email="test@example.com")
        org = create_org(db=db_session, name="Test Org")
        
        membership = create_org_membership(db=db_session, user=user, org=org)
        
        # Test relationships work correctly
        assert membership.user.email == "test@example.com"
        assert membership.org.name == "Test Org"
        
        # Test reverse relationships
        assert membership in user.org_memberships
        assert membership in org.org_memberships