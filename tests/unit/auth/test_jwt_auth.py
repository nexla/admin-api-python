"""
Tests for JWT authentication functionality.
"""
import pytest
from datetime import datetime, timedelta

from app.auth.jwt_auth import JWTAuth


@pytest.mark.unit
@pytest.mark.auth
class TestJWTAuth:
    """Test JWT authentication functionality"""
    
    def test_create_access_token(self):
        """Test JWT token creation"""
        token_data = {"user_id": 123, "email": "test@example.com"}
        token = JWTAuth.create_access_token(token_data)
        
        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_verify_token_success(self):
        """Test successful token verification"""
        token_data = {"user_id": 123, "email": "test@example.com"}
        token = JWTAuth.create_access_token(token_data)
        
        decoded = JWTAuth.verify_token(token)
        
        assert decoded is not None
        assert decoded["user_id"] == 123
        assert decoded["email"] == "test@example.com"
        assert "exp" in decoded  # Expiration should be set
    
    def test_verify_invalid_token(self):
        """Test verification of invalid token"""
        invalid_token = "invalid.token.here"
        
        decoded = JWTAuth.verify_token(invalid_token)
        
        assert decoded is None
    
    def test_token_expiration(self):
        """Test that tokens expire correctly"""
        # Create token with very short expiration
        token_data = {"user_id": 123}
        token = JWTAuth.create_access_token(token_data, expires_delta=timedelta(seconds=-1))
        
        # Should be expired and return None
        decoded = JWTAuth.verify_token(token)
        assert decoded is None