"""
Basic tests to verify test infrastructure is working.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org


@pytest.mark.unit
def test_basic_app_functionality(client: TestClient):
    """Test that the basic app endpoints work"""
    # Test root endpoint
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    
    # Test status endpoint
    response = client.get("/api/v1/status")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


@pytest.mark.unit
def test_database_session(db_session: Session):
    """Test that database session works"""
    # Create a simple user without factories first
    from datetime import datetime
    
    user = User(
        email="test@example.com",
        full_name="Test User",
        password_digest="test_hash",
        status="ACTIVE",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    db_session.add(user)
    db_session.commit()
    
    # Query it back
    found_user = db_session.query(User).filter(User.email == "test@example.com").first()
    assert found_user is not None
    assert found_user.email == "test@example.com"
    assert found_user.full_name == "Test User"


@pytest.mark.unit 
def test_jwt_auth_basic():
    """Test JWT auth basic functionality"""
    from app.auth.jwt_auth import JWTAuth
    from datetime import datetime
    
    # Create a simple user object for testing
    class MockUser:
        def __init__(self):
            self.id = 1
            self.email = "test@example.com"
            self.default_org_id = None
    
    user = MockUser()
    
    # Test token creation
    token = JWTAuth.create_access_token(user)
    assert token is not None
    assert isinstance(token, str)
    
    # Test token verification
    decoded = JWTAuth.verify_token(token)
    assert decoded is not None
    assert decoded["user_id"] == 1
    assert decoded["email"] == "test@example.com"