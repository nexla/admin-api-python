"""
Tests for User model.
Migrated from Rails spec/models/user_spec.rb
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership
from tests.factories import (
    create_user, create_org, create_org_membership, 
    create_data_source, create_data_set, create_team, create_project
)


@pytest.mark.unit
class TestUser:
    """Test User model functionality"""
    
    def test_create_user(self, db_session: Session):
        """Test creating a basic user"""
        user = create_user(
            db=db_session, 
            email="testuser@example.com",
            full_name="Test User"
        )
        
        assert user.email == "testuser@example.com"
        assert user.full_name == "Test User"
        assert user.status == "ACTIVE"
        assert user.is_active() is True
        assert user.is_deactivated() is False
        assert user.account_locked() is False
        assert user.password_digest is not None
    
    def test_user_email_uniqueness(self, db_session: Session):
        """Test that user emails must be unique"""
        create_user(db=db_session, email="unique@example.com")
        
        # Attempting to create another user with the same email should raise IntegrityError
        with pytest.raises(IntegrityError):
            create_user(db=db_session, email="unique@example.com")
    
    def test_user_status_methods(self, db_session: Session):
        """Test user status checking methods"""
        # Test active user
        active_user = create_user(db=db_session, status="ACTIVE")
        assert active_user.is_active() is True
        assert active_user.is_deactivated() is False
        
        # Test deactivated user
        deactivated_user = create_user(db=db_session, status="DEACTIVATED") 
        assert deactivated_user.is_active() is False
        assert deactivated_user.is_deactivated() is True
    
    def test_user_account_locked(self, db_session: Session):
        """Test user account locking functionality"""
        user = create_user(db=db_session)
        
        # Initially account should not be locked
        assert user.account_locked() is False
        
        # Lock the account
        user.account_locked_at = datetime.utcnow()
        db_session.commit()
        
        assert user.account_locked() is True
    
    def test_user_to_dict(self, db_session: Session):
        """Test user serialization to dictionary"""
        user = create_user(
            db=db_session, 
            email="serialize@example.com",
            full_name="Serialize User",
            status="ACTIVE"
        )
        
        result = user.to_dict()
        
        # Check required fields
        assert result["id"] == user.id
        assert result["email"] == "serialize@example.com"
        assert result["full_name"] == "Serialize User"
        assert result["status"] == "ACTIVE"
        assert result["is_active"] is True
        assert result["is_email_verified"] is False
        assert result["is_tos_signed"] is False
        assert "created_at" in result
        assert "updated_at" in result
    
    def test_user_org_relationships(self, db_session: Session):
        """Test user organization relationships"""
        org = create_org(db=db_session, name="Test Organization")
        user = create_user(db=db_session, email="orguser@example.com")
        
        # Create org membership
        membership = create_org_membership(db=db_session, user=user, org=org)
        
        # Test relationships work correctly
        assert membership in user.org_memberships
        assert membership.org == org
        assert membership.user == user
    
    def test_user_default_org(self, db_session: Session):
        """Test user default organization setting"""
        org = create_org(db=db_session, name="Default Org")
        user = create_user(db=db_session, default_org_id=org.id)
        
        # Refresh to get the relationship
        db_session.refresh(user)
        
        assert user.default_org_id == org.id
        # Note: relationship might need explicit loading in SQLAlchemy
    
    def test_user_resource_ownership(self, db_session: Session):
        """Test user ownership of various resources"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create resources owned by user
        data_source = create_data_source(db=db_session, owner=user, org=org)
        data_set = create_data_set(db=db_session, owner=user, org=org)
        project = create_project(db=db_session, owner=user, org=org)
        
        # Test relationships (Note: these might need proper back_populates setup)
        db_session.refresh(user)
        
        # For now, we can test by querying directly
        owned_sources = db_session.query(type(data_source)).filter_by(owner_id=user.id).all()
        owned_sets = db_session.query(type(data_set)).filter_by(owner_id=user.id).all()
        owned_projects = db_session.query(type(project)).filter_by(owner_id=user.id).all()
        
        assert len(owned_sources) >= 1
        assert len(owned_sets) >= 1
        assert len(owned_projects) >= 1


@pytest.mark.unit
class TestUserPasswordManagement:
    """Test User password management functionality"""
    
    def test_password_retry_count(self, db_session: Session):
        """Test password retry count tracking"""
        user = create_user(db=db_session, password_retry_count=0)
        
        assert user.password_retry_count == 0
        
        # Simulate failed login attempts
        user.password_retry_count = 3
        db_session.commit()
        
        assert user.password_retry_count == 3
    
    def test_password_reset_token(self, db_session: Session):
        """Test password reset token functionality"""
        user = create_user(db=db_session)
        
        # Set password reset token
        reset_token = "secure_reset_token_123"
        user.password_reset_token = reset_token
        user.password_reset_token_at = datetime.utcnow()
        user.password_reset_token_count = 1
        db_session.commit()
        
        assert user.password_reset_token == reset_token
        assert user.password_reset_token_at is not None
        assert user.password_reset_token_count == 1
    
    def test_password_change_required(self, db_session: Session):
        """Test password change requirement"""
        user = create_user(db=db_session)
        
        # Require password change
        user.password_change_required_at = datetime.utcnow()
        db_session.commit()
        
        assert user.password_change_required_at is not None


@pytest.mark.unit
class TestUserValidation:
    """Test User model validation"""
    
    def test_email_required(self, db_session: Session):
        """Test that email is required"""
        # This would typically be handled by SQLAlchemy constraints
        # but we can test our factory validation
        with pytest.raises((ValueError, IntegrityError)):
            user = User(
                full_name="No Email User",
                password_digest="hashed_password",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(user)
            db_session.commit()
    
    def test_password_digest_required(self, db_session: Session):
        """Test that password_digest is required"""
        with pytest.raises((ValueError, IntegrityError)):
            user = User(
                email="nopassword@example.com",
                full_name="No Password User",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(user)
            db_session.commit()
    
    def test_email_format_validation(self, db_session: Session):
        """Test basic email format expectations"""
        # Valid email formats should work
        valid_emails = [
            "user@example.com",
            "user.name@example.com", 
            "user+tag@example.co.uk",
            "user123@test-domain.org"
        ]
        
        for email in valid_emails:
            user = create_user(db=db_session, email=email)
            assert user.email == email
            db_session.delete(user)
            db_session.commit()


@pytest.mark.unit  
class TestUserTimestamps:
    """Test User timestamp functionality"""
    
    def test_created_and_updated_timestamps(self, db_session: Session):
        """Test that created_at and updated_at are set"""
        user = create_user(db=db_session)
        
        assert user.created_at is not None
        assert user.updated_at is not None
        
        # Update user and check timestamp changes
        original_updated_at = user.updated_at
        user.full_name = "Updated Name"
        user.updated_at = datetime.utcnow()  # In real app, this would be automatic
        db_session.commit()
        
        assert user.updated_at > original_updated_at
    
    def test_email_verified_timestamp(self, db_session: Session):
        """Test email verification timestamp"""
        user = create_user(db=db_session)
        
        # Initially not verified
        assert user.email_verified_at is None
        assert user.to_dict()["is_email_verified"] is False
        
        # Verify email
        user.email_verified_at = datetime.utcnow()
        db_session.commit()
        
        assert user.email_verified_at is not None
        assert user.to_dict()["is_email_verified"] is True
    
    def test_tos_signed_timestamp(self, db_session: Session):
        """Test terms of service signing timestamp"""
        user = create_user(db=db_session)
        
        # Initially not signed
        assert user.tos_signed_at is None
        assert user.to_dict()["is_tos_signed"] is False
        
        # Sign TOS
        user.tos_signed_at = datetime.utcnow()
        db_session.commit()
        
        assert user.tos_signed_at is not None
        assert user.to_dict()["is_tos_signed"] is True


@pytest.mark.unit
class TestUserAuthentication:
    """Test User authentication functionality (Rails business logic)"""
    
    def test_authenticate_with_correct_password(self, db_session: Session):
        """Test authentication with correct password"""
        user = create_user(db=db_session, email="auth@example.com")
        # Assuming create_user sets a known password
        assert user.authenticate("testpassword123") is True
    
    def test_authenticate_with_incorrect_password(self, db_session: Session):
        """Test authentication with incorrect password"""
        user = create_user(db=db_session, email="auth@example.com")
        assert user.authenticate("wrongpassword") is False
    
    def test_authenticate_with_empty_password(self, db_session: Session):
        """Test authentication with empty password"""
        user = create_user(db=db_session, email="auth@example.com")
        assert user.authenticate("") is False
        assert user.authenticate(None) is False
    
    def test_password_signature(self, db_session: Session):
        """Test password signature generation for JWT invalidation"""
        user = create_user(db=db_session)
        signature = user.password_signature()
        assert isinstance(signature, str)
        assert len(signature) == 32  # MD5 hex digest length
    
    def test_create_temporary_password(self):
        """Test temporary password generation"""
        temp_password = User.create_temporary_password()
        assert isinstance(temp_password, str)
        assert len(temp_password) > 8
        # Should contain uppercase, lowercase, digits, and special chars
        assert any(c.isupper() for c in temp_password)
        assert any(c.islower() for c in temp_password)
        assert any(c.isdigit() for c in temp_password)
        assert any(c in '!&-#$@+*' for c in temp_password)
    
    def test_email_verified_check(self):
        """Test email verification status check"""
        assert User.email_verified("user@example.com") is True
        assert User.email_verified("nexla.test@example.com") is False
        assert User.email_verified("") is False
        assert User.email_verified(None) is False


@pytest.mark.unit
class TestUserPasswordHistory:
    """Test User password history functionality (Rails business logic)"""
    
    def test_authenticate_with_previous_passwords(self, db_session: Session):
        """Test authentication against previous password history"""
        user = create_user(db=db_session)
        
        # Set some previous password digests (would be set by change_password)
        import bcrypt
        old_password = "oldpassword123"
        salt = bcrypt.gensalt()
        user.password_digest_1 = bcrypt.hashpw(old_password.encode('utf-8'), salt).decode('utf-8')
        db_session.commit()
        
        # Should authenticate with previous password
        assert user.authenticate_with_previous(old_password) is True
        assert user.authenticate_with_previous("wrongpassword") is False
    
    def test_change_password(self, db_session: Session):
        """Test password change with history tracking"""
        user = create_user(db=db_session)
        old_digest = user.password_digest
        
        new_password = "NewSecure123!"
        user.change_password(new_password, new_password)
        db_session.commit()
        
        # Password should be changed
        assert user.password_digest != old_digest
        assert user.authenticate(new_password) is True
        
        # Old password should be stored in history
        assert user.password_digest_1 == old_digest
        
        # Password change required date should be set
        assert user.password_change_required_at is not None
        
        # Reset tokens should be cleared
        assert user.password_reset_token is None
        assert user.password_reset_token_at is None
        assert user.password_reset_token_count == 0
    
    def test_change_password_confirmation_mismatch(self, db_session: Session):
        """Test password change with mismatched confirmation"""
        user = create_user(db=db_session)
        
        with pytest.raises(ValueError, match="Password confirmation does not match"):
            user.change_password("NewPassword123!", "DifferentPassword123!")
    
    def test_change_password_reuse_prevention(self, db_session: Session):
        """Test prevention of password reuse"""
        user = create_user(db=db_session)
        current_password = "testpassword123"  # Assuming this is the default
        
        with pytest.raises(ValueError, match="Cannot reuse a recent password"):
            user.change_password(current_password, current_password)
    
    def test_validate_password(self):
        """Test password validation"""
        email = "test@example.com"
        full_name = "Test User"
        
        # Valid password
        result = User.validate_password(email, full_name, "ValidPass123!")
        assert result['errors'] == []
        assert result['entropy'] > 0
        
        # Invalid password - too short
        result = User.validate_password(email, full_name, "short")
        assert len(result['errors']) > 0
        
        # Invalid password - no special character
        result = User.validate_password(email, full_name, "NoSpecial123")
        assert len(result['errors']) > 0


@pytest.mark.unit
class TestUserAccountLocking:
    """Test User account locking functionality (Rails business logic)"""
    
    def test_password_retry_count_increment(self, db_session: Session):
        """Test password retry count incrementing"""
        user = create_user(db=db_session, password_retry_count=0)
        
        user.increment_password_retry_count()
        db_session.commit()
        
        assert user.password_retry_count == 1
    
    def test_password_retry_count_exceeded(self, db_session: Session):
        """Test password retry count exceeded check"""
        user = create_user(db=db_session)
        
        # Not exceeded initially
        assert user.password_retry_count_exceeded() is False
        
        # Set to exceeded
        user.password_retry_count = User.MAX_PASSWORD_RETRY_COUNT
        assert user.password_retry_count_exceeded() is True
    
    def test_lock_account(self, db_session: Session):
        """Test account locking"""
        user = create_user(db=db_session)
        
        # Account initially unlocked
        assert user.account_locked() is False
        
        # Lock account
        user.lock_account()
        db_session.commit()
        
        assert user.account_locked() is True
        assert user.account_locked_at is not None
    
    def test_unlock_account(self, db_session: Session):
        """Test account unlocking"""
        user = create_user(db=db_session)
        
        # Lock account first
        user.lock_account()
        user.password_retry_count = 5
        db_session.commit()
        
        # Unlock account
        user.unlock_account()
        db_session.commit()
        
        assert user.account_locked() is False
        assert user.account_locked_at is None
        assert user.password_retry_count == 0
    
    def test_reset_password_retry_count(self, db_session: Session):
        """Test resetting password retry count"""
        user = create_user(db=db_session, password_retry_count=3)
        
        user.reset_password_retry_count()
        db_session.commit()
        
        assert user.password_retry_count == 0
    
    def test_lock_account_on_retry_exceeded(self, db_session: Session):
        """Test automatic account locking when retry count exceeded"""
        user = create_user(db=db_session, password_retry_count=User.MAX_PASSWORD_RETRY_COUNT - 1)
        
        # This should lock the account
        user.increment_password_retry_count()
        db_session.commit()
        
        assert user.password_retry_count == User.MAX_PASSWORD_RETRY_COUNT
        assert user.account_locked() is True


@pytest.mark.unit
class TestUserStatusMethods:
    """Test User status methods (Rails business logic)"""
    
    def test_activate_user(self, db_session: Session):
        """Test user activation"""
        user = create_user(db=db_session, status="DEACTIVATED")
        
        user.activate()
        db_session.commit()
        
        assert user.status == User.STATUSES["active"]
        assert user.is_active() is True
    
    def test_deactivate_user(self, db_session: Session):
        """Test user deactivation"""
        user = create_user(db=db_session, status="ACTIVE")
        
        # Mock deactivate method (full implementation would need org system)
        user.status = User.STATUSES["deactivated"]
        db_session.commit()
        
        assert user.deactivated() is True
        assert user.is_active() is False
    
    def test_user_status_constants(self):
        """Test user status constants match Rails"""
        assert User.STATUSES["active"] == "ACTIVE"
        assert User.STATUSES["deactivated"] == "DEACTIVATED"
        assert User.STATUSES["source_count_capped"] == "SOURCE_COUNT_CAPPED"
        assert User.STATUSES["source_data_capped"] == "SOURCE_DATA_CAPPED"
        assert User.STATUSES["trial_expired"] == "TRIAL_EXPIRED"
    
    def test_nexla_backend_admin(self, db_session: Session):
        """Test Nexla backend admin check"""
        admin_user = create_user(db=db_session, email=User.BACKEND_ADMIN_EMAIL)
        regular_user = create_user(db=db_session, email="regular@example.com")
        
        assert admin_user.nexla_backend_admin() is True
        assert regular_user.nexla_backend_admin() is False
    
    def test_infrastructure_user(self, db_session: Session):
        """Test infrastructure user check"""
        admin_user = create_user(db=db_session, email=User.BACKEND_ADMIN_EMAIL)
        regular_user = create_user(db=db_session, email="regular@example.com")
        
        assert admin_user.infrastructure_user() is True
        assert regular_user.infrastructure_user() is False
    
    def test_account_status(self, db_session: Session):
        """Test account status retrieval"""
        user = create_user(db=db_session, status="ACTIVE")
        
        # Without org context, should return user status
        assert user.account_status() == "ACTIVE"
        
        # With org context would depend on org tier (not implemented yet)
        assert user.account_status(None) == "ACTIVE"
    
    def test_password_change_required(self, db_session: Session):
        """Test password change required check"""
        user = create_user(db=db_session)
        
        # Currently disabled in Rails, should return False
        assert user.password_change_required() is False
        
        # Even with date set, should return False (temporarily disabled)
        user.password_change_required_at = datetime.utcnow() - timedelta(days=1)
        assert user.password_change_required() is False


@pytest.mark.unit
class TestUserRoleMethods:
    """Test User role-based methods (Rails business logic)"""
    
    def test_user_role_property(self, db_session: Session):
        """Test user role property calculation"""
        # Regular user
        user = create_user(db=db_session)
        assert user.role == "USER"
        
        # Would test other roles with proper org/membership system
        # For now, just test the basic case
    
    def test_impersonated_check(self, db_session: Session):
        """Test impersonation status check"""
        user = create_user(db=db_session)
        
        # Initially not impersonated
        assert user.impersonated() is False
        
        # Set impersonator (would be done by auth middleware)
        user._impersonator = create_user(db=db_session, email="admin@example.com")
        assert user.impersonated() is True
    
    def test_sso_options(self, db_session: Session):
        """Test SSO options retrieval"""
        user = create_user(db=db_session)
        
        # Without org memberships, should return empty list
        options = user.sso_options()
        assert options == []
    
    def test_to_dict_includes_new_fields(self, db_session: Session):
        """Test that to_dict includes all new Rails business logic fields"""
        user = create_user(db=db_session)
        result = user.to_dict()
        
        # Check new fields are included
        assert "is_deactivated" in result
        assert "account_locked" in result
        assert "password_change_required" in result
        assert "is_super_user" in result
        assert "is_org_owner" in result
        assert "is_infrastructure_user" in result
        assert "role" in result
        assert "nexla_backend_admin" in result
        
        # Check values are correct for basic user
        assert result["is_deactivated"] is False
        assert result["account_locked"] is False
        assert result["password_change_required"] is False
        assert result["is_infrastructure_user"] is False
        assert result["role"] == "USER"


@pytest.mark.unit
class TestUserPasswordResetTokens:
    """Test User password reset token functionality (Rails business logic)"""
    
    def test_create_password_reset_token(self, db_session: Session):
        """Test password reset token creation"""
        user = create_user(db=db_session)
        
        token = user.create_password_reset_token()
        db_session.commit()
        
        assert token is not None
        assert isinstance(token, str)
        assert user.password_reset_token == token
        assert user.password_reset_token_at is not None
        assert user.password_reset_token_count == 1
    
    def test_generate_password_reset_token(self, db_session: Session):
        """Test password reset token generation"""
        user = create_user(db=db_session)
        
        token = user.generate_password_reset_token(new_user=True)
        db_session.commit()
        
        assert token is not None
        assert user.password_reset_token == token
        assert user.password_reset_token_at is not None
    
    def test_reset_token_count_limit(self, db_session: Session):
        """Test password reset token count limiting"""
        user = create_user(db=db_session)
        user.password_reset_token_count = User.MAX_RESET_PASSWORD_TRIES
        db_session.commit()
        
        token = user.create_password_reset_token()
        db_session.commit()
        
        # Should lock account when exceeding max tries
        assert token is None
        assert user.account_locked() is True
    
    def test_password_changed_tracker(self, db_session: Session):
        """Test password changed tracking"""
        user = create_user(db=db_session)
        
        # Initially no change tracked
        assert user.password_changed() is False
        
        # Mark as changed (would be set by form/validation layer)
        user._password_changed = True
        assert user.password_changed() is True