"""
Users API tests.
Tests for user management endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from tests.factories import create_user, create_org, create_org_membership
from tests.utils import TestAuthHelper, TestResponseHelper, status_ok, status_created, status_not_found, status_forbidden


@pytest.mark.api
class TestUsersAPI:
    """Test users API endpoints"""
    
    def test_get_users_list(self, client: TestClient, db_session: Session):
        """Test getting list of users"""
        # Set up test data
        org = create_org(db=db_session)
        admin_user = create_user(db=db_session, email="admin@test.com")
        regular_user = create_user(db=db_session, email="user@test.com")
        
        create_org_membership(db=db_session, user=admin_user, org=org)
        create_org_membership(db=db_session, user=regular_user, org=org)
        
        # Get auth headers for admin
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Get users list
        response = client.get("/api/v1/users", headers=headers)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert isinstance(data, list)
        assert len(data) >= 2  # At least admin and regular user
        
        # Check user data structure
        user_data = data[0]
        assert "id" in user_data
        assert "email" in user_data
        assert "full_name" in user_data
        assert "status" in user_data
    
    def test_get_user_by_id(self, client: TestClient, db_session: Session):
        """Test getting specific user by ID"""
        org = create_org(db=db_session)
        admin_user = create_user(db=db_session, email="admin@test.com")
        target_user = create_user(db=db_session, email="target@test.com", full_name="Target User")
        
        create_org_membership(db=db_session, user=admin_user, org=org)
        create_org_membership(db=db_session, user=target_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Get specific user
        response = client.get(f"/api/v1/users/{target_user.id}", headers=headers)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert data["id"] == target_user.id
        assert data["email"] == "target@test.com"
        assert data["full_name"] == "Target User"
    
    def test_get_nonexistent_user(self, client: TestClient, db_session: Session):
        """Test getting non-existent user returns 404"""
        org = create_org(db=db_session)
        admin_user = create_user(db=db_session)
        create_org_membership(db=db_session, user=admin_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Try to get non-existent user
        response = client.get("/api/v1/users/99999", headers=headers)
        
        # Should return 404
        TestResponseHelper.assert_not_found(response)
    
    def test_create_user(self, client: TestClient, db_session: Session):
        """Test creating a new user"""
        org = create_org(db=db_session)
        admin_user = create_user(db=db_session)
        create_org_membership(db=db_session, user=admin_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Create new user
        user_data = {
            "email": "newuser@test.com",
            "full_name": "New Test User",
            "password": "securepassword123"
        }
        
        response = client.post("/api/v1/users", headers=headers, json=user_data)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_created())
        assert data["email"] == "newuser@test.com"
        assert data["full_name"] == "New Test User"
        assert "password" not in data  # Password should not be returned
    
    def test_create_user_with_duplicate_email(self, client: TestClient, db_session: Session):
        """Test creating user with duplicate email fails"""
        org = create_org(db=db_session)
        admin_user = create_user(db=db_session)
        existing_user = create_user(db=db_session, email="existing@test.com")
        
        create_org_membership(db=db_session, user=admin_user, org=org)
        create_org_membership(db=db_session, user=existing_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Try to create user with existing email
        user_data = {
            "email": "existing@test.com",  # Duplicate email
            "full_name": "Duplicate User",
            "password": "password123"
        }
        
        response = client.post("/api/v1/users", headers=headers, json=user_data)
        
        # Should fail
        TestResponseHelper.assert_error_response(response, 400)
    
    def test_update_user(self, client: TestClient, db_session: Session):
        """Test updating a user"""
        org = create_org(db=db_session)
        admin_user = create_user(db=db_session)
        target_user = create_user(db=db_session, email="target@test.com", full_name="Original Name")
        
        create_org_membership(db=db_session, user=admin_user, org=org)
        create_org_membership(db=db_session, user=target_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Update user
        update_data = {
            "full_name": "Updated Name",
            "status": "ACTIVE"
        }
        
        response = client.put(f"/api/v1/users/{target_user.id}", headers=headers, json=update_data)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert data["full_name"] == "Updated Name"
        assert data["email"] == "target@test.com"  # Email unchanged
    
    def test_deactivate_user(self, client: TestClient, db_session: Session):
        """Test deactivating a user"""
        org = create_org(db=db_session)
        admin_user = create_user(db=db_session)
        target_user = create_user(db=db_session, status="ACTIVE")
        
        create_org_membership(db=db_session, user=admin_user, org=org)
        create_org_membership(db=db_session, user=target_user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin_user, org)
        
        # Deactivate user
        response = client.post(f"/api/v1/users/{target_user.id}/deactivate", headers=headers)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert data["status"] == "DEACTIVATED"
    
    def test_user_profile_access(self, client: TestClient, db_session: Session):
        """Test users can access their own profile"""
        org = create_org(db=db_session)
        user = create_user(db=db_session, email="user@test.com", full_name="Test User")
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Get own profile
        response = client.get("/api/v1/users/me", headers=headers)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert data["email"] == "user@test.com"
        assert data["full_name"] == "Test User"
    
    def test_user_profile_update(self, client: TestClient, db_session: Session):
        """Test users can update their own profile"""
        org = create_org(db=db_session)
        user = create_user(db=db_session, email="user@test.com", full_name="Original Name")
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Update own profile
        update_data = {
            "full_name": "Updated Name"
        }
        
        response = client.put("/api/v1/users/me", headers=headers, json=update_data)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert data["full_name"] == "Updated Name"
    
    def test_unauthorized_user_access_denied(self, client: TestClient):
        """Test that unauthenticated requests are denied"""
        # Try to get users without authentication
        response = client.get("/api/v1/users")
        
        # Should be unauthorized
        TestResponseHelper.assert_unauthorized(response)