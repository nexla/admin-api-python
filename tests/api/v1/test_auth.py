"""
Authentication API tests.
Migrated from Rails user_api_key_spec.rb and auth-related tests.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from tests.factories import create_user, create_org, create_org_membership, create_data_source, create_data_set
from tests.utils import TestAuthHelper, TestResponseHelper, status_ok, status_unauthorized


@pytest.mark.api
@pytest.mark.auth
class TestAuthAPI:
    """Test authentication API endpoints"""
    
    def setup_method(self):
        """Set up test data for each test"""
        pass
    
    def test_login_success(self, client: TestClient, db_session: Session):
        """Test successful login"""
        # Create test user
        user = create_user(
            db=db_session,
            email="test@example.com",
            password_digest="$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LeVZhDgNZeQ1TvABS"  # "password123"
        )
        
        # Attempt login
        response = client.post("/api/v1/auth/login", json={
            "email": "test@example.com",
            "password": "password123"
        })
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert "access_token" in data
        assert "token_type" in data
        assert data["token_type"] == "bearer"
    
    def test_login_invalid_credentials(self, client: TestClient, db_session: Session):
        """Test login with invalid credentials"""
        # Create test user
        create_user(db=db_session, email="test@example.com")
        
        # Attempt login with wrong password
        response = client.post("/api/v1/auth/login", json={
            "email": "test@example.com", 
            "password": "wrongpassword"
        })
        
        # Should fail
        TestResponseHelper.assert_error_response(response, status_unauthorized())
    
    def test_token_refresh(self, client: TestClient, db_session: Session):
        """Test token refresh functionality"""
        user = create_user(db=db_session)
        org = create_org(db=db_session)
        
        # Get auth headers with valid token
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Attempt token refresh
        response = client.post("/api/v1/auth/refresh", headers=headers)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert "access_token" in data
    
    def test_protected_endpoint_access_with_token(self, client: TestClient, db_session: Session):
        """Test accessing protected endpoints with valid token"""
        # Set up test data
        user = create_user(db=db_session)
        org = create_org(db=db_session, owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        # Get auth headers
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Access protected endpoint
        response = client.get("/api/v1/data_sources", headers=headers)
        
        # Should succeed and return data
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert isinstance(data, list)
        if data:  # If data sources exist
            assert data[0]["id"] == data_source.id
    
    def test_protected_endpoint_access_without_token(self, client: TestClient):
        """Test accessing protected endpoints without token"""
        # Attempt to access protected endpoint without auth
        response = client.get("/api/v1/data_sources")
        
        # Should be unauthorized
        TestResponseHelper.assert_unauthorized(response)
    
    def test_api_key_authentication(self, client: TestClient, db_session: Session):
        """Test API key authentication (equivalent to Rails API key tests)"""
        # Set up test data
        user = create_user(db=db_session)
        org = create_org(db=db_session)
        membership = create_org_membership(db=db_session, user=user, org=org, api_key="TEST_API_KEY_123")
        create_data_source(db=db_session, owner=user, org=org)
        
        # Get API key headers
        headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        
        # Get access token using API key
        response = client.post("/api/v1/auth/token", headers=headers)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert "access_token" in data
        
        # Use access token to access protected endpoint
        auth_headers = {
            "Authorization": f"Bearer {data['access_token']}",
            "Content-Type": "application/json"
        }
        
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
    
    def test_deactivated_api_key_access_denied(self, client: TestClient, db_session: Session):
        """Test that deactivated API keys are denied access"""
        # Set up test data with deactivated membership
        user = create_user(db=db_session)
        org = create_org(db=db_session)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="DEACTIVATED_KEY",
            status="DEACTIVATED"
        )
        
        # Attempt to use deactivated API key
        headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        response = client.post("/api/v1/auth/token", headers=headers)
        
        # Should be unauthorized
        TestResponseHelper.assert_unauthorized(response)
    
    def test_user_impersonation_with_api_key(self, client: TestClient, db_session: Session):
        """Test user impersonation functionality (from Rails tests)"""
        # Set up admin user and regular user
        admin_user = create_user(db=db_session, email="admin@test.com")
        regular_user = create_user(db=db_session, email="user@test.com") 
        org = create_org(db=db_session, owner=admin_user)
        
        admin_membership = create_org_membership(db=db_session, user=admin_user, org=org, api_key="ADMIN_KEY")
        create_org_membership(db=db_session, user=regular_user, org=org)
        
        # Create data for both users
        create_data_source(db=db_session, owner=admin_user, org=org, name="Admin Source")
        create_data_set(db=db_session, owner=regular_user, org=org, name="User Set")
        
        # Get token with impersonation
        headers = TestAuthHelper.get_api_key_headers(admin_membership.api_key)
        impersonation_data = {
            "user_id": regular_user.id,
            "org_id": org.id
        }
        
        response = client.post("/api/v1/auth/token", headers=headers, json=impersonation_data)
        
        # Should succeed
        data = TestResponseHelper.assert_success_response(response, status_ok())
        access_token = data["access_token"]
        
        # Use impersonation token to access data
        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Should see regular user's data, not admin's
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        sources = TestResponseHelper.assert_success_response(response, status_ok())
        assert len(sources) == 0  # Regular user has no data sources
        
        response = client.get("/api/v1/data_sets", headers=auth_headers)
        datasets = TestResponseHelper.assert_success_response(response, status_ok())
        assert len(datasets) > 0  # Regular user has data sets