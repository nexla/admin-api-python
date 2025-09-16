"""
Organization and User Management API Tests.
Migrated from Rails org_user_spec.rb - comprehensive org and user management tests.

This test suite covers:
- Organization creation with different owner configurations
- User creation and management within organizations  
- Organization membership management
- Admin privilege management
- User activation/deactivation
- Cross-organization operations
- Billing owner management
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership
from tests.factories import (
    create_user, create_org, create_org_membership, create_org_admin
)
from tests.utils import (
    TestAuthHelper, TestResponseHelper, TestDataHelper,
    status_ok, status_created, status_bad_request, status_forbidden, status_not_found
)


@pytest.mark.api
@pytest.mark.orgs
class TestOrgsUsersAPI:
    """Test organization and user management API endpoints"""
    
    def setup_method(self):
        """Set up test data for each test"""
        pass
    
    def test_create_org_with_new_user_as_owner(self, client: TestClient, db_session: Session):
        """Test creating org with a new user as owner"""
        # Set up super user
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org, api_key="SUPER_KEY")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Create org with new user
        org_data = {
            "name": "TEST ORG",
            "description": "TEST ORG",
            "email_domain": "test-org.com",
            "owner": {
                "email": "jcw@test-org.com",
                "full_name": "Jeff"
            }
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=org_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "TEST ORG"
        assert data["owner"]["email"] == "jcw@test-org.com"
        assert data["owner"]["full_name"] == "Jeff"
    
    def test_create_org_with_existing_user_as_owner_by_email(self, client: TestClient, db_session: Session):
        """Test creating org with existing user as owner (by email and name)"""
        # Set up super user and target user
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        existing_user = create_user(db=db_session, email="existing@test.com", full_name="Existing User")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Create org with existing user by email/name
        org_data = {
            "name": "SOME ORG",
            "description": "SOME ORG", 
            "email_domain": "some-org.com",
            "owner": {
                "email": existing_user.email,
                "full_name": existing_user.full_name
            }
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=org_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["owner"]["id"] == existing_user.id
        assert data["owner"]["email"] == existing_user.email
    
    def test_create_org_with_existing_user_as_owner_by_id(self, client: TestClient, db_session: Session):
        """Test creating org with existing user as owner (by ID)"""
        # Set up super user and target user
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        existing_user = create_user(db=db_session, email="existing@test.com")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Create org with existing user by ID
        org_data = {
            "name": "SOME ORG",
            "description": "SOME ORG",
            "email_domain": "some-org.com",
            "owner": {
                "id": existing_user.id
            }
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=org_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["owner"]["id"] == existing_user.id
    
    def test_create_org_with_owner_id_field(self, client: TestClient, db_session: Session):
        """Test creating org with owner_id field"""
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        existing_user = create_user(db=db_session, email="existing@test.com")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Create org with owner_id field
        org_data = {
            "name": "SOME ORG",
            "description": "SOME ORG",
            "email_domain": "some-org.com",
            "owner_id": existing_user.id
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=org_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["owner"]["id"] == existing_user.id
    
    def test_create_org_with_owner_and_billing_owner(self, client: TestClient, db_session: Session):
        """Test creating org with both owner and billing_owner"""
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        target_user = create_user(db=db_session, email="target@test.com")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        org_data = {
            "name": "SOME ORG",
            "description": "SOME ORG",
            "email_domain": "some-org.com",
            "owner_id": target_user.id,
            "billing_owner_id": target_user.id
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=org_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["owner"]["id"] == target_user.id
        assert data["billing_owner"]["id"] == target_user.id
    
    def test_create_org_and_change_billing_owner(self, client: TestClient, db_session: Session):
        """Test creating org and changing billing owner"""
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        owner_user = create_user(db=db_session, email="owner@test.com")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Create org with initial users
        org_data = {
            "name": "SOME ORG",
            "description": "SOME ORG",
            "email_domain": "some-org.com",
            "owner_id": owner_user.id,
            "billing_owner_id": owner_user.id,
            "users": [
                {
                    "email": "jcw@some-org.com",
                    "full_name": "Jeff",
                    "admin": True
                }
            ]
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=org_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        org_id = data["id"]
        assert data["owner"]["id"] == owner_user.id
        assert data["billing_owner"]["id"] == owner_user.id
        
        # Find the new user that was created
        new_user = db_session.query(User).filter_by(email="jcw@some-org.com").first()
        assert new_user is not None
        
        # Change billing owner
        update_data = {
            "billing_owner_id": new_user.id
        }
        
        response = client.put(f"/api/v1/orgs/{org_id}", headers=headers, json=update_data)
        updated_data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert updated_data["owner"]["id"] == owner_user.id
        assert updated_data["billing_owner"]["id"] == new_user.id
    
    def test_update_org_with_new_user(self, client: TestClient, db_session: Session):
        """Test updating org with new user"""
        # Set up org and admin
        admin_user = create_user(db=db_session, email="admin@test-org.com", full_name="Admin")
        org = create_org(db=db_session, name="Test Org", owner=admin_user, email_domain="test-org.com")
        create_org_membership(db=db_session, user=admin_user, org=org, api_key="ADMIN_KEY")
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Update org with new user
        update_data = {
            "users": [
                {
                    "email": "jarah@test-org.com",
                    "full_name": "Jarah"
                }
            ]
        }
        
        response = client.put(f"/api/v1/orgs/{org.id}", headers=headers, json=update_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Check org members
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Find the new user
        jarah_member = next((m for m in members if m["email"] == "jarah@test-org.com"), None)
        assert jarah_member is not None
        assert jarah_member["is_admin"] is False
    
    def test_update_org_with_new_admin_user(self, client: TestClient, db_session: Session):
        """Test updating org with new user who is promoted to admin"""
        admin_user = create_user(db=db_session, email="admin@test-org.com")
        org = create_org(db=db_session, name="Test Org", owner=admin_user)
        create_org_membership(db=db_session, user=admin_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Update org with new admin user
        update_data = {
            "users": [
                {
                    "email": "jarah@test-org.com",
                    "full_name": "Jarah",
                    "admin": True
                }
            ]
        }
        
        response = client.put(f"/api/v1/orgs/{org.id}", headers=headers, json=update_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Check org members
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        jarah_member = next((m for m in members if m["email"] == "jarah@test-org.com"), None)
        assert jarah_member is not None
        assert jarah_member["is_admin"] is True
    
    def test_toggle_user_admin_privileges(self, client: TestClient, db_session: Session):
        """Test toggling user admin privileges"""
        admin_user = create_user(db=db_session, email="admin@test-org.com")
        org = create_org(db=db_session, name="Test Org", owner=admin_user)
        create_org_membership(db=db_session, user=admin_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        member_email = "jarah@test-org.com"
        
        # Create user as admin
        update_data = {
            "users": [
                {
                    "email": member_email,
                    "full_name": "Jarah",
                    "admin": True
                }
            ]
        }
        
        response = client.put(f"/api/v1/orgs/{org.id}", headers=headers, json=update_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Get members to find the user ID
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        member = next((m for m in members if m["email"] == member_email), None)
        assert member is not None
        assert member["is_admin"] is True
        member_id = member["id"]
        
        # Remove admin privileges
        update_data = {
            "users": [
                {
                    "id": member_id,
                    "admin": False
                }
            ]
        }
        
        response = client.put(f"/api/v1/orgs/{org.id}", headers=headers, json=update_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify admin privileges removed
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        member = next((m for m in members if m["email"] == member_email), None)
        assert member is not None
        assert member["is_admin"] is False
        
        # Restore admin privileges using email
        update_data = {
            "users": [
                {
                    "email": member_email,
                    "admin": True
                }
            ]
        }
        
        response = client.put(f"/api/v1/orgs/{org.id}", headers=headers, json=update_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify admin privileges restored
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        member = next((m for m in members if m["email"] == member_email), None)
        assert member is not None
        assert member["is_admin"] is True
    
    def test_create_user_with_default_org_non_admin(self, client: TestClient, db_session: Session):
        """Test creating user with default org (non-admin)"""
        org_admin = create_user(db=db_session, email="admin@test-org.com")
        org = create_org(db=db_session, name="Test Org", owner=org_admin)
        create_org_membership(db=db_session, user=org_admin, org=org)
        
        headers = TestAuthHelper.get_auth_headers(org_admin, org)
        
        user_data = {
            "email": "jarah@test-org.com",
            "full_name": "Jarah",
            "default_org_id": org.id
        }
        
        response = client.post("/api/v1/users", headers=headers, json=user_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["email"] == "jarah@test-org.com"
        assert data["super_user"] is None or data["super_user"] is False
        assert data["org_memberships"][0]["is_admin"] is False
        assert data["default_org"]["id"] == org.id
    
    def test_create_user_with_default_org_admin(self, client: TestClient, db_session: Session):
        """Test creating user with default org (admin)"""
        org_admin = create_user(db=db_session, email="admin@test-org.com")
        org = create_org(db=db_session, name="Test Org", owner=org_admin)
        create_org_membership(db=db_session, user=org_admin, org=org)
        
        headers = TestAuthHelper.get_auth_headers(org_admin, org)
        
        user_data = {
            "email": "jarah@test-org.com",
            "full_name": "Jarah",
            "default_org_id": org.id,
            "admin": True
        }
        
        response = client.post("/api/v1/users", headers=headers, json=user_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["email"] == "jarah@test-org.com"
        assert data["super_user"] is None or data["super_user"] is False
        assert data["org_memberships"][0]["is_admin"] is True
        assert data["default_org"]["id"] == org.id
    
    def test_super_user_create_user_with_default_org_admin(self, client: TestClient, db_session: Session):
        """Test super user creating user with default org (admin)"""
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        target_org = create_org(db=db_session, name="Target Org")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        user_data = {
            "email": "jarah@target-org.com",
            "full_name": "Jarah",
            "default_org_id": target_org.id,
            "admin": True
        }
        
        response = client.post("/api/v1/users", headers=headers, json=user_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["email"] == "jarah@target-org.com"
        assert data["super_user"] is False
        assert data["org_memberships"][0]["is_admin"] is True
        assert data["default_org"]["id"] == target_org.id
    
    def test_super_user_create_user_and_add_to_nexla_as_super_user(self, client: TestClient, db_session: Session):
        """Test super user creating user and adding to Nexla as super user"""
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        target_org = create_org(db=db_session, name="Target Org")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Create user with default org
        user_data = {
            "email": "jarah@target-org.com",
            "full_name": "Jarah",
            "default_org_id": target_org.id,
            "admin": True
        }
        
        response = client.post("/api/v1/users", headers=headers, json=user_data)
        user_data = TestResponseHelper.assert_success_response(response, status_created())
        
        user_id = user_data["id"]
        user_email = user_data["email"]
        
        # Add user to Nexla org
        update_data = {
            "users": [
                {
                    "email": user_email,
                    "full_name": user_data["full_name"],
                    "admin": True
                }
            ]
        }
        
        response = client.put(f"/api/v1/orgs/{nexla_org.id}", headers=headers, json=update_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Check nexla org members
        response = client.get(f"/api/v1/orgs/{nexla_org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        user_member = next((m for m in members if m["email"] == user_email), None)
        assert user_member is not None
        assert user_member["is_admin"] is True
        
        # Get updated user info
        response = client.get(f"/api/v1/users/{user_id}", headers=headers)
        updated_user = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert updated_user["super_user"] is True
        assert len(updated_user["org_memberships"]) == 2
    
    def test_multi_org_admin_management(self, client: TestClient, db_session: Session):
        """Test managing user admin status across multiple orgs"""
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        target_org = create_org(db=db_session, name="Target Org")
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Create user with default org (non-admin)
        user_data = {
            "email": "jarah@target-org.com",
            "full_name": "Jarah",
            "default_org_id": target_org.id,
            "admin": False
        }
        
        response = client.post("/api/v1/users", headers=headers, json=user_data)
        user_data = TestResponseHelper.assert_success_response(response, status_created())
        
        user_id = user_data["id"]
        assert user_data["super_user"] is False
        assert user_data["org_memberships"][0]["is_admin"] is False
        
        # Add user to nexla org as admin
        update_data = {
            "users": [
                {
                    "email": user_data["email"],
                    "full_name": user_data["full_name"],
                    "admin": True
                }
            ]
        }
        
        response = client.put(f"/api/v1/orgs/{nexla_org.id}", headers=headers, json=update_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Get updated user - should now be super user
        response = client.get(f"/api/v1/users/{user_id}", headers=headers)
        updated_user = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert updated_user["super_user"] is True
        assert len(updated_user["org_memberships"]) == 2
        
        # Update admin status on both orgs
        admin_data = {
            "admin": [
                {
                    "org_id": nexla_org.id,
                    "admin": False
                },
                {
                    "org_id": target_org.id,
                    "admin": True
                }
            ]
        }
        
        response = client.put(f"/api/v1/users/{user_id}", headers=headers, json=admin_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Get final user state
        response = client.get(f"/api/v1/users/{user_id}", headers=headers)
        final_user = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert final_user["super_user"] is False
        assert len(final_user["org_memberships"]) == 2
        
        # Check admin status for each org
        nexla_membership = next((om for om in final_user["org_memberships"] if om["id"] == nexla_org.id), None)
        target_membership = next((om for om in final_user["org_memberships"] if om["id"] == target_org.id), None)
        
        assert nexla_membership is not None
        assert nexla_membership["is_admin"] is False
        
        assert target_membership is not None  
        assert target_membership["is_admin"] is True
    
    def test_unauthorized_org_creation_denied(self, client: TestClient, db_session: Session):
        """Test that unauthorized users cannot create orgs"""
        regular_user = create_user(db=db_session, email="regular@test.com")
        org = create_org(db=db_session, name="Regular Org")
        create_org_membership(db=db_session, user=regular_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(regular_user, org)
        
        org_data = {
            "name": "Unauthorized Org",
            "description": "Should not be created",
            "email_domain": "unauthorized.com"
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=org_data)
        TestResponseHelper.assert_forbidden(response)
    
    def test_unauthorized_user_creation_denied(self, client: TestClient):
        """Test that unauthenticated requests are denied"""
        user_data = {
            "email": "unauthorized@test.com",
            "full_name": "Unauthorized User"
        }
        
        response = client.post("/api/v1/users", json=user_data)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_invalid_org_data_validation(self, client: TestClient, db_session: Session):
        """Test validation of invalid org data"""
        super_user = create_user(db=db_session, email="super@nexla.com")
        nexla_org = create_org(db=db_session, name="Nexla", owner=super_user)
        create_org_membership(db=db_session, user=super_user, org=nexla_org)
        
        headers = TestAuthHelper.get_auth_headers(super_user, nexla_org)
        
        # Missing required fields
        invalid_org_data = {
            "description": "Missing name"
        }
        
        response = client.post("/api/v1/orgs", headers=headers, json=invalid_org_data)
        TestResponseHelper.assert_error_response(response, status_bad_request())
    
    def test_invalid_user_data_validation(self, client: TestClient, db_session: Session):
        """Test validation of invalid user data"""
        admin_user = create_user(db=db_session, email="admin@test.com")
        org = create_org(db=db_session, name="Test Org", owner=admin_user)
        create_org_membership(db=db_session, user=admin_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Missing required email
        invalid_user_data = {
            "full_name": "No Email User"
        }
        
        response = client.post("/api/v1/users", headers=headers, json=invalid_user_data)
        TestResponseHelper.assert_error_response(response, status_bad_request())
        
        # Invalid email format
        invalid_email_data = {
            "email": "invalid-email-format",
            "full_name": "Invalid Email"
        }
        
        response = client.post("/api/v1/users", headers=headers, json=invalid_email_data)
        TestResponseHelper.assert_error_response(response, status_bad_request())


@pytest.mark.api
@pytest.mark.org_members
class TestOrgMembersAPI:
    """Test organization membership management endpoints"""
    
    def test_get_org_members(self, client: TestClient, db_session: Session):
        """Test getting organization members"""
        admin_user = create_user(db=db_session, email="admin@test.com")
        member_user = create_user(db=db_session, email="member@test.com")
        org = create_org(db=db_session, name="Test Org", owner=admin_user)
        
        create_org_membership(db=db_session, user=admin_user, org=org, api_key="ADMIN_KEY")
        create_org_membership(db=db_session, user=member_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert len(members) >= 2
        assert any(m["email"] == admin_user.email for m in members)
        assert any(m["email"] == member_user.email for m in members)
    
    def test_add_member_to_org(self, client: TestClient, db_session: Session):
        """Test adding member to organization"""
        admin_user = create_user(db=db_session, email="admin@test.com")
        org = create_org(db=db_session, name="Test Org", owner=admin_user)
        create_org_membership(db=db_session, user=admin_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        member_data = {
            "email": "newmember@test.com",
            "full_name": "New Member",
            "admin": False
        }
        
        response = client.post(f"/api/v1/orgs/{org.id}/members", headers=headers, json=member_data)
        TestResponseHelper.assert_success_response(response, status_created())
        
        # Verify member was added
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        new_member = next((m for m in members if m["email"] == "newmember@test.com"), None)
        assert new_member is not None
        assert new_member["is_admin"] is False
    
    def test_remove_member_from_org(self, client: TestClient, db_session: Session):
        """Test removing member from organization"""
        admin_user = create_user(db=db_session, email="admin@test.com")
        member_user = create_user(db=db_session, email="member@test.com")
        org = create_org(db=db_session, name="Test Org", owner=admin_user)
        
        create_org_membership(db=db_session, user=admin_user, org=org)
        membership = create_org_membership(db=db_session, user=member_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        response = client.delete(f"/api/v1/orgs/{org.id}/members/{membership.id}", headers=headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify member was removed
        response = client.get(f"/api/v1/orgs/{org.id}/members", headers=headers)
        members = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert not any(m["email"] == member_user.email for m in members)