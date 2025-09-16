"""
Tests for DeactivateUserTask service.
Tests user deactivation, resource management, and audit logging.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from sqlalchemy.orm import Session

from app.services.async_tasks.tasks.deactivate_user import DeactivateUserTask
from app.models.async_task import AsyncTask
from app.models.enums import TaskStatus, UserStatus, DataSourceStatus
from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership
from app.models.data_source import DataSource


class TestDeactivateUserTask:
    """Test DeactivateUserTask functionality"""
    
    @pytest.fixture
    def mock_db_session(self):
        """Mock database session"""
        mock_session = Mock(spec=Session)
        mock_session.query.return_value = mock_session
        mock_session.filter.return_value = mock_session
        mock_session.all.return_value = []
        mock_session.first.return_value = None
        mock_session.scalar.return_value = None
        mock_session.commit.return_value = None
        mock_session.rollback.return_value = None
        mock_session.merge.return_value = None
        return mock_session
    
    @pytest.fixture
    def sample_task(self):
        """Create a sample DeactivateUser task"""
        return AsyncTask(
            id=1,
            task_type="DeactivateUser",
            arguments={"user_id": 789},
            owner_id=456,
            org_id=123,
            request_data={
                "host": "test.nexla.com",
                "request_ip": "192.168.1.1",
                "request_url": "https://test.nexla.com/async_tasks",
                "request_user_agent": "TestAgent/1.0"
            }
        )
    
    @pytest.fixture
    def target_user(self):
        """Create target user to be deactivated"""
        return User(
            id=789,
            email="target@example.com",
            status=UserStatus.ACTIVE
        )
    
    @pytest.fixture
    def owner_user(self):
        """Create task owner user"""
        return User(
            id=456,
            email="admin@example.com",
            status=UserStatus.ACTIVE
        )
    
    @pytest.fixture
    def admin_membership(self):
        """Create admin membership for permission testing"""
        return OrgMembership(
            user_id=456,
            org_id=123,
            role='admin'
        )
    
    @pytest.fixture
    def target_membership(self):
        """Create target user membership"""
        return OrgMembership(
            user_id=789,
            org_id=123,
            role='user'
        )
    
    @pytest.fixture
    def sample_data_sources(self):
        """Create sample data sources owned by target user"""
        return [
            DataSource(
                id=1001,
                name="Test DataSource 1",
                owner_id=789,
                org_id=123,
                status=DataSourceStatus.ACTIVE
            ),
            DataSource(
                id=1002,
                name="Test DataSource 2",
                owner_id=789,
                org_id=123,
                status=DataSourceStatus.ACTIVE
            )
        ]
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    def test_check_preconditions_success(self, mock_session_local, mock_db_session, sample_task, 
                                       target_user, admin_membership):
        """Test successful precondition check"""
        # Setup
        mock_session_local.return_value = mock_db_session
        
        # Mock queries
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=target_user)),  # Target user exists
            Mock(subquery=Mock(return_value=Mock())),    # Owner admin orgs
            Mock(subquery=Mock(return_value=Mock())),    # Target user orgs
            Mock(first=Mock(return_value=admin_membership))  # Shared admin access
        ]
        
        task = DeactivateUserTask(sample_task)
        
        # Execute - should not raise any exception
        task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    def test_check_preconditions_no_user_id(self, mock_session_local, mock_db_session):
        """Test precondition check fails when user_id is missing"""
        # Setup
        task_without_user = AsyncTask(
            id=1,
            task_type="DeactivateUser",
            arguments={},
            owner_id=456
        )
        mock_session_local.return_value = mock_db_session
        
        task = DeactivateUserTask(task_without_user)
        
        # Execute and verify
        with pytest.raises(ApiError, match="User ID is required"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    def test_check_preconditions_user_not_found(self, mock_session_local, mock_db_session, sample_task):
        """Test precondition check fails when target user not found"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.first.return_value = None
        
        task = DeactivateUserTask(sample_task)
        
        # Execute and verify
        with pytest.raises(ApiError, match="Target user not found"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    def test_check_preconditions_no_admin_access(self, mock_session_local, mock_db_session, 
                                                sample_task, target_user):
        """Test precondition check fails when owner has no admin access"""
        # Setup
        mock_session_local.return_value = mock_db_session
        
        # Mock queries - user exists but no shared admin access
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=target_user)),  # Target user exists
            Mock(subquery=Mock(return_value=Mock())),    # Owner admin orgs
            Mock(subquery=Mock(return_value=Mock())),    # Target user orgs
            Mock(first=Mock(return_value=None))          # No shared admin access
        ]
        
        task = DeactivateUserTask(sample_task)
        
        # Execute and verify
        with pytest.raises(ApiError, match="do not have admin access"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    def test_check_preconditions_already_deactivated(self, mock_session_local, mock_db_session, 
                                                    sample_task, admin_membership):
        """Test precondition check fails when user is already deactivated"""
        # Setup
        deactivated_user = User(
            id=789,
            email="target@example.com",
            status=UserStatus.DEACTIVATED
        )
        
        mock_session_local.return_value = mock_db_session
        
        # Mock queries
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=deactivated_user)),  # Already deactivated user
            Mock(subquery=Mock(return_value=Mock())),         # Owner admin orgs
            Mock(subquery=Mock(return_value=Mock())),         # Target user orgs
            Mock(first=Mock(return_value=admin_membership))   # Shared admin access
        ]
        
        task = DeactivateUserTask(sample_task)
        
        # Execute and verify
        with pytest.raises(ApiError, match="already deactivated"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    @patch('app.services.async_tasks.tasks.deactivate_user.AuditService')
    def test_perform_success(self, mock_audit_service, mock_session_local, mock_db_session, 
                           sample_task, target_user, sample_data_sources):
        """Test successful user deactivation"""
        # Setup
        mock_session_local.return_value = mock_db_session
        
        # Mock queries
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=target_user)),      # Get target user
            Mock(all=Mock(return_value=sample_data_sources))  # Get user's data sources
        ]
        
        # Mock owner email query
        mock_db_session.query.return_value.filter.return_value.scalar.return_value = "admin@example.com"
        
        task = DeactivateUserTask(sample_task)
        
        # Execute
        task.perform()
        
        # Verify user was deactivated
        assert target_user.status == UserStatus.DEACTIVATED
        assert target_user.deactivated_at is not None
        assert target_user.updated_at is not None
        
        # Verify data sources were paused
        for ds in sample_data_sources:
            assert ds.status == DataSourceStatus.PAUSED
            assert ds.updated_at is not None
        
        # Verify audit logging was called
        assert mock_audit_service.log_action.call_count >= 3  # 2 data sources + 1 user
        
        # Verify task result was set
        assert sample_task.status == TaskStatus.COMPLETED
        assert sample_task.result is not None
        assert sample_task.result['user_id'] == 789
        assert sample_task.result['paused_sources_count'] == 2
        
        # Verify database commit
        mock_db_session.commit.assert_called_once()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    @patch('app.services.async_tasks.tasks.deactivate_user.AuditService')
    def test_perform_no_data_sources(self, mock_audit_service, mock_session_local, mock_db_session, 
                                   sample_task, target_user):
        """Test user deactivation when user has no data sources"""
        # Setup
        mock_session_local.return_value = mock_db_session
        
        # Mock queries
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=target_user)),  # Get target user
            Mock(all=Mock(return_value=[]))              # No data sources
        ]
        
        # Mock owner email query
        mock_db_session.query.return_value.filter.return_value.scalar.return_value = "admin@example.com"
        
        task = DeactivateUserTask(sample_task)
        
        # Execute
        task.perform()
        
        # Verify user was deactivated
        assert target_user.status == UserStatus.DEACTIVATED
        
        # Verify task result
        assert sample_task.result['paused_sources_count'] == 0
        assert sample_task.result['paused_data_sources'] == []
        
        # Verify only user audit log was created (no data source logs)
        mock_audit_service.log_action.assert_called_once()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    @patch('app.services.async_tasks.tasks.deactivate_user.AuditService')
    def test_perform_data_source_pause_failure(self, mock_audit_service, mock_session_local, 
                                              mock_db_session, sample_task, target_user):
        """Test handling when some data source pausing fails"""
        # Setup
        mock_session_local.return_value = mock_db_session
        
        # Create data sources with one that will fail
        failing_ds = DataSource(
            id=1001,
            name="Failing DataSource",
            owner_id=789,
            org_id=123,
            status=DataSourceStatus.ACTIVE
        )
        
        # Make the audit service fail for the first data source
        mock_audit_service.log_action.side_effect = [
            Exception("Audit logging failed"),  # First call fails
            None,  # Second call succeeds (user deactivation)
        ]
        
        # Mock queries
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=target_user)),    # Get target user
            Mock(all=Mock(return_value=[failing_ds]))      # Get user's data sources
        ]
        
        # Mock owner email query
        mock_db_session.query.return_value.filter.return_value.scalar.return_value = "admin@example.com"
        
        task = DeactivateUserTask(sample_task)
        
        # Execute
        task.perform()
        
        # Verify user was still deactivated despite data source failure
        assert target_user.status == UserStatus.DEACTIVATED
        
        # Verify task completed
        assert sample_task.status == TaskStatus.COMPLETED
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    def test_perform_user_not_found_during_execution(self, mock_session_local, mock_db_session, sample_task):
        """Test handling when target user is not found during execution"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.first.return_value = None
        
        task = DeactivateUserTask(sample_task)
        
        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Target user not found"):
            task.perform()
        
        # Verify rollback was called
        mock_db_session.rollback.assert_called_once()
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    @patch('app.services.async_tasks.tasks.deactivate_user.AuditService')
    def test_perform_with_version_creation(self, mock_audit_service, mock_session_local, 
                                         mock_db_session, sample_task, target_user):
        """Test user deactivation with version creation for audit trail"""
        # Setup
        mock_session_local.return_value = mock_db_session
        target_user.create_version = Mock()  # Add version creation method
        
        # Mock queries
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=target_user)),  # Get target user
            Mock(all=Mock(return_value=[]))              # No data sources
        ]
        
        # Mock owner email query
        mock_db_session.query.return_value.filter.return_value.scalar.return_value = "admin@example.com"
        
        task = DeactivateUserTask(sample_task)
        
        # Execute
        task.perform()
        
        # Verify version was created with proper metadata
        target_user.create_version.assert_called_once()
        call_args = target_user.create_version.call_args
        metadata = call_args[1]['metadata']
        
        assert metadata['request_ip'] == "192.168.1.1"
        assert metadata['request_url'] == "https://test.nexla.com/async_tasks"
        assert metadata['request_user_agent'] == "TestAgent/1.0"
        assert metadata['host'] == "test.nexla.com"
        assert metadata['user_id'] == 456
        assert metadata['user_email'] == "admin@example.com"
        assert metadata['org_id'] == 123
    
    @patch('app.services.async_tasks.tasks.deactivate_user.SessionLocal')
    @patch('app.services.async_tasks.tasks.deactivate_user.AuditService')
    def test_audit_logging_parameters(self, mock_audit_service, mock_session_local, 
                                    mock_db_session, sample_task, target_user, sample_data_sources):
        """Test that audit logging is called with correct parameters"""
        # Setup
        mock_session_local.return_value = mock_db_session
        
        # Mock queries
        mock_db_session.query.return_value.filter.side_effect = [
            Mock(first=Mock(return_value=target_user)),      # Get target user
            Mock(all=Mock(return_value=sample_data_sources)) # Get user's data sources
        ]
        
        # Mock owner email query
        mock_db_session.query.return_value.filter.return_value.scalar.return_value = "admin@example.com"
        
        task = DeactivateUserTask(sample_task)
        
        # Execute
        task.perform()
        
        # Verify audit logging calls
        audit_calls = mock_audit_service.log_action.call_args_list
        
        # Should have 3 calls: 2 data sources + 1 user
        assert len(audit_calls) == 3
        
        # Check data source audit logs
        for i in range(2):
            call_kwargs = audit_calls[i][1]
            assert call_kwargs['action'] == 'update'
            assert call_kwargs['resource_type'] == 'DataSource'
            assert call_kwargs['resource_id'] == sample_data_sources[i].id
            assert call_kwargs['old_values']['status'] == DataSourceStatus.ACTIVE.value
            assert call_kwargs['new_values']['status'] == DataSourceStatus.PAUSED.value
            assert call_kwargs['risk_level'] == 'medium'
        
        # Check user audit log
        user_audit_call = audit_calls[2][1]
        assert user_audit_call['action'] == 'deactivate'
        assert user_audit_call['resource_type'] == 'User'
        assert user_audit_call['resource_id'] == 789
        assert user_audit_call['old_values']['status'] == UserStatus.ACTIVE.value
        assert user_audit_call['new_values']['status'] == UserStatus.DEACTIVATED.value
        assert user_audit_call['risk_level'] == 'high'
        assert 'paused_data_sources' in user_audit_call['details']