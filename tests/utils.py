"""
Test utilities and helper functions.
This replaces Rails request_helper and nexla_helper functionality.
"""
from typing import Dict, Optional
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from app.auth.jwt_auth import JWTAuth
from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership
from tests.factories import create_user, create_org, create_org_membership


class TestAuthHelper:
    """Authentication helper for tests"""
    
    @staticmethod
    def create_access_token(user: User, org: Org = None) -> str:
        """Create a JWT access token for testing"""
        return JWTAuth.create_access_token(user)
    
    @staticmethod
    def get_auth_headers(user: User, org: Org = None) -> Dict[str, str]:
        """Get authentication headers for API requests"""
        token = TestAuthHelper.create_access_token(user, org)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    @staticmethod
    def get_api_key_headers(api_key: str) -> Dict[str, str]:
        """Get API key headers for API requests"""
        return {
            "Authorization": f"Basic {api_key}",
            "Accept": "application/vnd.nexla.api.v1+json",
            "Content-Type": "application/json"
        }


class TestDataHelper:
    """Helper for creating test data"""
    
    @staticmethod
    def setup_test_org(db: Session) -> Dict[str, any]:
        """Set up a complete test organization with admin user"""
        org = create_org(db=db, name="Test Organization")
        admin_user = create_user(db=db, email="admin@testorg.com", full_name="Admin User")
        membership = create_org_membership(db=db, user=admin_user, org=org)
        
        return {
            "org": org,
            "admin": admin_user,
            "membership": membership
        }
    
    @staticmethod
    def setup_test_users(db: Session, org: Org, count: int = 3) -> list[User]:
        """Create multiple test users for an organization"""
        users = []
        for i in range(count):
            user = create_user(db=db, email=f"user{i}@testorg.com")
            create_org_membership(db=db, user=user, org=org)
            users.append(user)
        return users


class TestResponseHelper:
    """Helper for testing API responses"""
    
    @staticmethod
    def assert_success_response(response, expected_status: int = 200):
        """Assert that response is successful"""
        assert response.status_code == expected_status
        return response.json()
    
    @staticmethod
    def assert_error_response(response, expected_status: int = 400):
        """Assert that response is an error"""
        assert response.status_code == expected_status
        data = response.json()
        assert "detail" in data or "error" in data
        return data
    
    @staticmethod
    def assert_unauthorized(response):
        """Assert that response is unauthorized"""
        assert response.status_code == 401
        return response.json()
    
    @staticmethod
    def assert_forbidden(response):
        """Assert that response is forbidden"""
        assert response.status_code == 403
        return response.json()
    
    @staticmethod
    def assert_not_found(response):
        """Assert that response is not found"""
        assert response.status_code == 404
        return response.json()


class APITestCase:
    """Base test case for API tests"""
    
    def __init__(self, client: TestClient, db: Session):
        self.client = client
        self.db = db
        self.test_data = None
        
    def setup_test_data(self):
        """Set up common test data"""
        self.test_data = TestDataHelper.setup_test_org(self.db)
        
    def get_auth_headers(self, user: User = None, org: Org = None) -> Dict[str, str]:
        """Get authentication headers"""
        user = user or self.test_data["admin"]
        org = org or self.test_data["org"] 
        return TestAuthHelper.get_auth_headers(user, org)
        
    def assert_success(self, response, status_code: int = 200):
        """Assert successful response"""
        return TestResponseHelper.assert_success_response(response, status_code)
        
    def assert_error(self, response, status_code: int = 400):
        """Assert error response"""
        return TestResponseHelper.assert_error_response(response, status_code)


# Status code helpers (equivalent to Rails status helpers)
def status_ok() -> int:
    return 200

def status_created() -> int:
    return 201

def status_no_content() -> int:
    return 204

def status_bad_request() -> int:
    return 400

def status_unauthorized() -> int:
    return 401

def status_forbidden() -> int:
    return 403

def status_not_found() -> int:
    return 404

def status_unprocessable_entity() -> int:
    return 422

def status_internal_server_error() -> int:
    return 500


# Time helpers for testing
def freeze_time(time: datetime):
    """Mock datetime for testing (would use freezegun in real implementation)"""
    # This is a placeholder - in real tests we'd use freezegun or similar
    pass


def travel_to(time: datetime):
    """Travel to specific time for testing"""
    # This is a placeholder - in real tests we'd use freezegun or similar  
    pass


# Database helpers
def clear_database(db: Session):
    """Clear all data from test database"""
    # This would clear all tables in proper order
    # For now, it's handled by the test transaction rollback
    pass


def reset_sequences(db: Session):
    """Reset database sequences"""
    # This would reset auto-increment sequences
    # SQLite handles this automatically
    pass