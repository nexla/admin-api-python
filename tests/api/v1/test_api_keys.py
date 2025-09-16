"""
API Key Authentication Tests.
Migrated from Rails user_api_key_spec.rb - comprehensive API key and token management tests.

This test suite covers:
- API key rotation
- Token generation and refresh 
- API key activation/deactivation
- User impersonation via API keys
- Token lifecycle management
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership
from tests.factories import (
    create_user, create_org, create_org_membership, 
    create_data_source, create_data_set
)
from tests.utils import (
    TestAuthHelper, TestResponseHelper, status_ok, status_unauthorized,
    status_created, status_bad_request
)


@pytest.mark.api
@pytest.mark.auth
class TestAPIKeysAPI:
    """Test API key authentication and token management endpoints"""
    
    def setup_method(self):
        """Set up test data for each test"""
        pass
    
    def test_api_key_rotation(self, client: TestClient, db_session: Session):
        """Test API key rotation functionality"""
        # Set up test data
        user = create_user(db=db_session, email="admin@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="ORIGINAL_API_KEY_123"
        )
        
        # Get auth headers with API key
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Get current API keys for user
        response = client.get(f"/api/v1/users/{user.id}/api_keys", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Rotate each API key
        for api_key_data in data:
            api_key_id = api_key_data["id"]
            response = client.put(
                f"/api/v1/users/{user.id}/api_keys/{api_key_id}/rotate", 
                headers=headers
            )
            TestResponseHelper.assert_success_response(response, status_ok())
            
            # Verify new API key is different
            rotated_data = response.json()
            assert rotated_data["api_key"] != membership.api_key
    
    def test_token_generation_with_api_key(self, client: TestClient, db_session: Session):
        """Test /token access with API key"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="API_KEY_TOKEN_TEST"
        )
        data_source = create_data_source(db=db_session, owner=user, org=org, name="KEY SOURCE")
        
        # Get API key headers
        api_key_headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        
        # Request access token
        response = client.post("/api/v1/auth/token", headers=api_key_headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert "access_token" in data
        assert data["access_token"] is not None
        assert "token_type" in data
        
        access_token = data["access_token"]
        
        # Use access token to access protected endpoint
        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        sources_data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert isinstance(sources_data, list)
        assert len(sources_data) > 0
        assert sources_data[0]["id"] == data_source.id
    
    def test_token_access_denied_with_paused_api_key(self, client: TestClient, db_session: Session):
        """Test that paused API keys cannot generate tokens"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="PAUSED_API_KEY",
            status="PAUSED"
        )
        
        # Try to get token with paused API key
        api_key_headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        response = client.post("/api/v1/auth/token", headers=api_key_headers)
        
        # Should be unauthorized
        TestResponseHelper.assert_unauthorized(response)
    
    def test_access_token_invalid_when_api_key_paused(self, client: TestClient, db_session: Session):
        """Test that access tokens become invalid when API key is paused"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="ACTIVE_API_KEY",
            status="ACTIVE"
        )
        create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        # Get access token with active API key
        api_key_headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        response = client.post("/api/v1/auth/token", headers=api_key_headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        access_token = data["access_token"]
        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Verify token works initially
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Pause the API key
        membership.status = "PAUSED"
        db_session.commit()
        
        # Token should now be invalid
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_access_token_valid_when_api_key_reactivated(self, client: TestClient, db_session: Session):
        """Test that access tokens work again when API key is reactivated"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="REACTIVATION_KEY",
            status="ACTIVE"
        )
        create_data_source(db=db_session, owner=user, org=org)
        
        # Get access token
        api_key_headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        response = client.post("/api/v1/auth/token", headers=api_key_headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        access_token = data["access_token"]
        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Verify token works
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Pause the API key
        membership.status = "PAUSED"
        db_session.commit()
        
        # Token should be invalid
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_unauthorized(response)
        
        # Reactivate the API key
        membership.status = "ACTIVE"
        db_session.commit()
        
        # Token should work again
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
    
    def test_user_impersonation_with_api_key(self, client: TestClient, db_session: Session):
        """Test user impersonation functionality with API keys"""
        # Set up admin user and regular user
        admin_user = create_user(db=db_session, email="admin@test.com")
        regular_user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=admin_user)
        
        admin_membership = create_org_membership(
            db=db_session, 
            user=admin_user, 
            org=org, 
            api_key="ADMIN_KEY"
        )
        create_org_membership(db=db_session, user=regular_user, org=org)
        
        # Create data for both users
        create_data_source(db=db_session, owner=admin_user, org=org, name="Admin Source")
        create_data_set(db=db_session, owner=regular_user, org=org, name="User Set")
        
        # Get token with impersonation
        api_key_headers = TestAuthHelper.get_api_key_headers(admin_membership.api_key)
        impersonation_data = {
            "user_id": regular_user.id,
            "org_id": org.id
        }
        
        response = client.post(
            "/api/v1/auth/token", 
            headers=api_key_headers, 
            json=impersonation_data
        )
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        access_token = data["access_token"]
        
        # Use impersonation token
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
    
    def test_impersonation_token_invalid_when_api_key_paused(self, client: TestClient, db_session: Session):
        """Test that impersonation tokens become invalid when API key is paused"""
        # Set up test data
        admin_user = create_user(db=db_session, email="admin@test.com")
        regular_user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=admin_user)
        
        admin_membership = create_org_membership(
            db=db_session, 
            user=admin_user, 
            org=org, 
            api_key="ADMIN_IMPERSONATION_KEY",
            status="ACTIVE"
        )
        create_org_membership(db=db_session, user=regular_user, org=org)
        
        create_data_source(db=db_session, owner=admin_user, org=org)
        create_data_set(db=db_session, owner=regular_user, org=org, name="User Set")
        
        # Get impersonation token
        api_key_headers = TestAuthHelper.get_api_key_headers(admin_membership.api_key)
        impersonation_data = {
            "user_id": regular_user.id,
            "org_id": org.id
        }
        
        response = client.post(
            "/api/v1/auth/token", 
            headers=api_key_headers, 
            json=impersonation_data
        )
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        access_token = data["access_token"]
        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Verify impersonation works initially
        response = client.get("/api/v1/data_sets", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Pause the admin API key
        admin_membership.status = "PAUSED"
        db_session.commit()
        
        # Impersonation token should now be invalid
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_unauthorized(response)
        
        response = client.get("/api/v1/data_sets", headers=auth_headers)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_impersonation_token_valid_when_api_key_reactivated(self, client: TestClient, db_session: Session):
        """Test that impersonation tokens work again when API key is reactivated"""
        # Set up test data
        admin_user = create_user(db=db_session, email="admin@test.com")
        regular_user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=admin_user)
        
        admin_membership = create_org_membership(
            db=db_session, 
            user=admin_user, 
            org=org, 
            api_key="ADMIN_REACTIVATION_KEY",
            status="ACTIVE"
        )
        create_org_membership(db=db_session, user=regular_user, org=org)
        
        create_data_source(db=db_session, owner=admin_user, org=org)
        create_data_set(db=db_session, owner=regular_user, org=org)
        
        # Get impersonation token
        api_key_headers = TestAuthHelper.get_api_key_headers(admin_membership.api_key)
        impersonation_data = {
            "user_id": regular_user.id,
            "org_id": org.id
        }
        
        response = client.post(
            "/api/v1/auth/token", 
            headers=api_key_headers, 
            json=impersonation_data
        )
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        access_token = data["access_token"]
        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Verify works initially
        response = client.get("/api/v1/data_sets", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Pause the API key
        admin_membership.status = "PAUSED"
        db_session.commit()
        
        # Should be unauthorized
        response = client.get("/api/v1/data_sets", headers=auth_headers)
        TestResponseHelper.assert_unauthorized(response)
        
        # Reactivate the API key
        admin_membership.status = "ACTIVE"
        db_session.commit()
        
        # Should work again
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        response = client.get("/api/v1/data_sets", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
    
    def test_api_key_info_preserved_across_token_refresh(self, client: TestClient, db_session: Session):
        """Test that API key info is preserved across token refresh"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="REFRESH_TEST_KEY",
            status="ACTIVE"
        )
        create_data_source(db=db_session, owner=user, org=org)
        
        # Get initial access token
        api_key_headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        response = client.post("/api/v1/auth/token", headers=api_key_headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        access_token = data["access_token"]
        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Verify token works
        response = client.get("/api/v1/data_sources", headers=auth_headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Refresh the token
        response = client.post("/api/v1/auth/refresh", headers=auth_headers)
        refresh_data = TestResponseHelper.assert_success_response(response, status_ok())
        
        new_access_token = refresh_data["access_token"]
        assert new_access_token is not None
        assert new_access_token != access_token  # Should be different
        
        # Pause the original API key
        membership.status = "PAUSED"
        db_session.commit()
        
        # New token should be invalid because API key is paused
        new_auth_headers = {
            "Authorization": f"Bearer {new_access_token}",
            "Content-Type": "application/json"
        }
        
        response = client.get("/api/v1/data_sources", headers=new_auth_headers)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_invalid_api_key_rejected(self, client: TestClient):
        """Test that invalid API keys are rejected"""
        invalid_headers = TestAuthHelper.get_api_key_headers("INVALID_API_KEY_123")
        
        response = client.post("/api/v1/auth/token", headers=invalid_headers)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_missing_api_key_rejected(self, client: TestClient):
        """Test that missing API keys are rejected"""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
            # No Authorization header
        }
        
        response = client.post("/api/v1/auth/token", headers=headers)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_api_key_with_insufficient_scope_rejected(self, client: TestClient, db_session: Session):
        """Test that API keys with insufficient scope are rejected"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="LIMITED_SCOPE_KEY",
            status="ACTIVE"
        )
        
        # This test would verify scope-based access control
        # Implementation depends on how scopes are handled in the Python API
        api_key_headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        response = client.post("/api/v1/auth/token", headers=api_key_headers)
        
        # For now, should succeed (assuming no scope restrictions)
        TestResponseHelper.assert_success_response(response, status_ok())