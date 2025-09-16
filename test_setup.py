#!/usr/bin/env python3
"""
Basic test script to verify FastAPI setup works correctly.
Run this to validate your migration setup.
"""

import sys
import os
import asyncio
from fastapi.testclient import TestClient

# Add app to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")
    
    try:
        from app.main import app
        print("✓ Main app imported successfully")
    except Exception as e:
        print(f"✗ Failed to import main app: {e}")
        return False
    
    try:
        from app.config import settings
        print("✓ Settings imported successfully")
    except Exception as e:
        print(f"✗ Failed to import settings: {e}")
        return False
    
    try:
        from app.models import User, Org, DataSource
        print("✓ Models imported successfully")
    except Exception as e:
        print(f"✗ Failed to import models: {e}")
        return False
    
    try:
        from app.auth import create_access_token, verify_password
        print("✓ Auth functions imported successfully")
    except Exception as e:
        print(f"✗ Failed to import auth functions: {e}")
        return False
    
    return True

def test_basic_endpoints():
    """Test basic API endpoints."""
    print("\nTesting basic endpoints...")
    
    try:
        from app.main import app
        client = TestClient(app)
        
        # Test root endpoint
        response = client.get("/")
        if response.status_code == 200:
            print("✓ Root endpoint working")
        else:
            print(f"✗ Root endpoint failed: {response.status_code}")
            return False
        
        # Test status endpoint
        response = client.get("/api/v1/status")
        if response.status_code == 200:
            print("✓ Status endpoint working")
        else:
            print(f"✗ Status endpoint failed: {response.status_code}")
            return False
        
        # Test docs endpoint (should work if DEBUG=True)
        response = client.get("/docs")
        if response.status_code in [200, 404]:  # 404 is ok if DEBUG=False
            print("✓ Docs endpoint accessible")
        else:
            print(f"✗ Docs endpoint failed: {response.status_code}")
            return False
            
        return True
        
    except Exception as e:
        print(f"✗ Failed to test endpoints: {e}")
        return False

def test_configuration():
    """Test configuration loading."""
    print("\nTesting configuration...")
    
    try:
        from app.config import settings
        
        required_settings = [
            'DATABASE_URL', 'SECRET_KEY', 'ALGORITHM', 
            'ACCESS_TOKEN_EXPIRE_MINUTES', 'HOST', 'PORT'
        ]
        
        for setting in required_settings:
            if hasattr(settings, setting):
                print(f"✓ {setting} configured")
            else:
                print(f"✗ {setting} missing")
                return False
                
        return True
        
    except Exception as e:
        print(f"✗ Failed to test configuration: {e}")
        return False

def test_password_hashing():
    """Test password hashing functions."""
    print("\nTesting password hashing...")
    
    try:
        from app.auth import get_password_hash, verify_password
        
        test_password = "test123!@#"
        hashed = get_password_hash(test_password)
        
        if verify_password(test_password, hashed):
            print("✓ Password hashing working")
            return True
        else:
            print("✗ Password verification failed")
            return False
            
    except Exception as e:
        print(f"✗ Failed to test password hashing: {e}")
        return False

def test_jwt_tokens():
    """Test JWT token creation and verification."""
    print("\nTesting JWT tokens...")
    
    try:
        from app.auth import create_access_token, verify_token
        
        test_data = {"user_id": 123, "email": "test@example.com"}
        token = create_access_token(test_data)
        
        decoded = verify_token(token)
        
        if decoded and decoded.get("user_id") == 123:
            print("✓ JWT tokens working")
            return True
        else:
            print("✗ JWT token verification failed")
            return False
            
    except Exception as e:
        print(f"✗ Failed to test JWT tokens: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 50)
    print("FastAPI Migration Setup Test")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_configuration,
        test_password_hashing,
        test_jwt_tokens,
        test_basic_endpoints,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! FastAPI setup is working correctly.")
        print("\nNext steps:")
        print("1. Set up your database connection in .env")
        print("2. Run: alembic upgrade head")
        print("3. Start the server: uvicorn app.main:app --reload")
        return True
    else:
        print("❌ Some tests failed. Please fix the issues above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)