#!/usr/bin/env python3
"""
Test script to verify business logic implementation.
Tests core functionality like validation, authorization, and user management.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.validation_service import ValidationService
from app.auth.rbac import RBACService, SystemPermissions
from app.models.user import User

def test_password_validation():
    """Test password validation service"""
    print("Testing Password Validation...")
    
    # Test weak password
    result = ValidationService.validate_password("user@test.com", "John Doe", "123")
    assert not result['valid'], "Weak password should be invalid"
    print(f"✓ Weak password correctly rejected: {result['message']}")
    
    # Test strong password
    result = ValidationService.validate_password("user@test.com", "John Doe", "StrongP@ssw0rd123")
    assert result['valid'], f"Strong password should be valid: {result['message']}"
    print(f"✓ Strong password accepted: Strength={result['strength']}, Entropy={result['entropy_score']}")
    
    # Test password with email
    result = ValidationService.validate_password("user@test.com", "John Doe", "user@test.comPassword123!")
    assert not result['valid'], "Password containing email should be invalid"
    print("✓ Password containing email correctly rejected")
    
    print("Password validation tests passed!\n")

def test_email_validation():
    """Test email validation"""
    print("Testing Email Validation...")
    
    # Valid email
    result = ValidationService.validate_email("test@example.com")
    assert result['valid'], "Valid email should pass"
    print("✓ Valid email accepted")
    
    # Invalid email
    result = ValidationService.validate_email("not-an-email")
    assert not result['valid'], "Invalid email should be rejected"
    print("✓ Invalid email rejected")
    
    print("Email validation tests passed!\n")

def test_rbac_permissions():
    """Test RBAC permission system"""
    print("Testing RBAC System...")
    
    # Mock user
    class MockUser:
        def __init__(self, email, is_active=True):
            self.id = 1
            self.email = email
            self.full_name = "Test User"
            
        def is_active(self):
            return True
    
    # Test admin user
    admin_user = MockUser("admin@nexla.com")
    
    # Mock DB session
    class MockDB:
        pass
    
    db = MockDB()
    
    # Test admin permissions
    permissions = RBACService.get_user_permissions(admin_user, db)
    assert SystemPermissions.SYSTEM_ADMIN in permissions, "Admin should have system admin permissions"
    print("✓ Admin user has correct permissions")
    
    # Test regular user
    regular_user = MockUser("user@company.com")
    permissions = RBACService.get_user_permissions(regular_user, db)
    assert SystemPermissions.API_ACCESS in permissions, "Regular user should have API access"
    assert SystemPermissions.SYSTEM_ADMIN not in permissions, "Regular user should not have admin permissions"
    print("✓ Regular user has correct permissions")
    
    print("RBAC tests passed!\n")

def test_user_model_logic():
    """Test user model methods"""
    print("Testing User Model Logic...")
    
    # Test is_active method
    user = User(
        email="test@example.com",
        full_name="Test User",
        status="ACTIVE"
    )
    
    assert user.is_active(), "Active user should return True for is_active()"
    print("✓ User.is_active() works correctly")
    
    user.status = "DEACTIVATED"
    assert not user.is_active(), "Deactivated user should return False for is_active()"
    print("✓ User.is_active() correctly identifies deactivated users")
    
    print("User model tests passed!\n")

def main():
    """Run all tests"""
    print("=" * 50)
    print("BUSINESS LOGIC TESTS")
    print("=" * 50)
    print()
    
    try:
        test_password_validation()
        test_email_validation()
        test_rbac_permissions()
        test_user_model_logic()
        
        print("=" * 50)
        print("✅ ALL TESTS PASSED!")
        print("Business logic implementation is working correctly.")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ TEST FAILED: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()