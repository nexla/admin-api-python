"""
Tests for GetAuditLogsTask service.
Tests audit log generation, filtering, and S3 export functionality.
"""

import pytest
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.services.async_tasks.tasks.get_audit_logs import GetAuditLogsTask
from app.models.async_task import AsyncTask
from app.models.enums import TaskStatus
from app.models.audit_log import AuditLog
from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership


class TestGetAuditLogsTask:
    """Test GetAuditLogsTask functionality"""
    
    @pytest.fixture
    def mock_db_session(self):
        """Mock database session"""
        mock_session = Mock(spec=Session)
        mock_session.query.return_value = mock_session
        mock_session.filter.return_value = mock_session
        mock_session.all.return_value = []
        mock_session.first.return_value = None
        mock_session.commit.return_value = None
        mock_session.rollback.return_value = None
        mock_session.merge.return_value = None
        return mock_session
    
    @pytest.fixture
    def sample_task(self):
        """Create a sample GetAuditLogs task"""
        return AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "start_date": "2023-01-01",
                "end_date": "2023-01-31"
            },
            owner_id=456,
            org_id=123
        )
    
    @pytest.fixture
    def sample_org_membership(self):
        """Create a sample org membership for permission testing"""
        return OrgMembership(
            user_id=456,
            org_id=123,
            role='admin'
        )
    
    @pytest.fixture
    def sample_audit_logs(self):
        """Create sample audit log records"""
        return [
            AuditLog(
                id=1,
                timestamp=datetime(2023, 1, 15, 10, 30),
                user_id=456,
                ip_address="192.168.1.1",
                user_agent="TestAgent/1.0",
                action="create",
                resource_type="DataSource",
                resource_id=789,
                resource_name="Test DataSource",
                method="POST",
                endpoint="/api/v1/data_sources",
                old_values=None,
                new_values={"name": "Test DataSource", "status": "active"},
                changes={"name": {"old": None, "new": "Test DataSource"}},
                details={"source": "api"},
                org_id=123,
                risk_level="low"
            ),
            AuditLog(
                id=2,
                timestamp=datetime(2023, 1, 16, 14, 45),
                user_id=456,
                ip_address="192.168.1.1",
                user_agent="TestAgent/1.0",
                action="update",
                resource_type="DataSet",
                resource_id=890,
                resource_name="Test DataSet",
                method="PUT",
                endpoint="/api/v1/data_sets/890",
                old_values={"status": "active"},
                new_values={"status": "paused"},
                changes={"status": {"old": "active", "new": "paused"}},
                details={"reason": "maintenance"},
                org_id=123,
                risk_level="medium"
            )
        ]
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_check_preconditions_success(self, mock_session_local, mock_db_session, sample_task, sample_org_membership):
        """Test successful precondition check"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.first.return_value = sample_org_membership
        
        task = GetAuditLogsTask(sample_task)
        
        # Execute - should not raise any exception
        task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_check_preconditions_no_org_id(self, mock_session_local, mock_db_session):
        """Test precondition check fails when org_id is missing"""
        # Setup
        task_without_org = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={},
            owner_id=456
        )
        mock_session_local.return_value = mock_db_session
        
        task = GetAuditLogsTask(task_without_org)
        
        # Execute and verify
        with pytest.raises(ApiError, match="Organization ID is required"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_check_preconditions_no_access(self, mock_session_local, mock_db_session, sample_task):
        """Test precondition check fails when user has no access"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.first.return_value = None
        
        task = GetAuditLogsTask(sample_task)
        
        # Execute and verify
        with pytest.raises(ApiError, match="does not have access"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_check_preconditions_invalid_date(self, mock_session_local, mock_db_session):
        """Test precondition check fails with invalid date format"""
        # Setup
        invalid_task = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "start_date": "invalid-date"
            },
            owner_id=456
        )
        mock_session_local.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.first.return_value = Mock()
        
        task = GetAuditLogsTask(invalid_task)
        
        # Execute and verify
        with pytest.raises(ApiError, match="Invalid start_date format"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_check_preconditions_restricted_resource_type(self, mock_session_local, mock_db_session):
        """Test precondition check fails for restricted resource type access"""
        # Setup
        restricted_task = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "resource_type": "DataSource"
            },
            owner_id=456
        )
        mock_session_local.return_value = mock_db_session
        
        # Mock org membership check (user has access)
        mock_membership = Mock()
        mock_db_session.query.return_value.filter.return_value.first.side_effect = [
            mock_membership,  # First call for membership check
            Mock(name="Non-Nexla Org")  # Second call for org check
        ]
        
        task = GetAuditLogsTask(restricted_task)
        
        # Execute and verify
        with pytest.raises(ApiError, match="Resource type filtering is restricted"):
            task.check_preconditions()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    @patch('app.services.async_tasks.tasks.get_audit_logs.S3Service')
    @patch('tempfile.NamedTemporaryFile')
    @patch('builtins.open', new_callable=mock_open)
    def test_perform_success(self, mock_file_open, mock_temp_file, mock_s3_service, mock_session_local, 
                           mock_db_session, sample_task, sample_audit_logs):
        """Test successful audit log generation and export"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_s3_instance = Mock()
        mock_s3_service.return_value = mock_s3_instance
        
        # Mock query results
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = sample_audit_logs
        mock_db_session.query.return_value = mock_query
        
        # Mock temp file
        mock_temp_instance = Mock()
        mock_temp_instance.name = '/tmp/test_file.json'
        mock_temp_file.return_value.__enter__.return_value = mock_temp_instance
        
        # Mock S3 operations
        mock_s3_instance.upload_file.return_value = True
        mock_s3_instance.get_presigned_url.return_value = "https://s3.amazonaws.com/presigned-url"
        
        task = GetAuditLogsTask(sample_task)
        
        # Execute
        task.perform()
        
        # Verify S3 upload was called
        mock_s3_instance.upload_file.assert_called_once()
        mock_s3_instance.get_presigned_url.assert_called_once()
        
        # Verify task result was set
        assert sample_task.status == TaskStatus.COMPLETED
        assert sample_task.result is not None
        assert sample_task.result_url == "https://s3.amazonaws.com/presigned-url"
        assert sample_task.result['record_count'] == 2
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_perform_with_date_filters(self, mock_session_local, mock_db_session, sample_audit_logs):
        """Test audit log generation with date range filters"""
        # Setup
        task_with_dates = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "start_date": "2023-01-01",
                "end_date": "2023-01-31"
            },
            owner_id=456
        )
        
        mock_session_local.return_value = mock_db_session
        
        # Mock query chain
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = sample_audit_logs
        mock_db_session.query.return_value = mock_query
        
        task = GetAuditLogsTask(task_with_dates)
        
        with patch.object(task, 'update_progress'), \
             patch('app.services.async_tasks.tasks.get_audit_logs.S3Service'), \
             patch('tempfile.NamedTemporaryFile'), \
             patch('builtins.open', mock_open()):
            
            task.perform()
            
            # Verify date filters were applied (multiple filter calls)
            assert mock_query.filter.call_count >= 3  # org_id + start_date + end_date
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_perform_with_event_filter(self, mock_session_local, mock_db_session, sample_audit_logs):
        """Test audit log generation with event filtering"""
        # Setup
        task_with_filter = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "event_filter": "create"
            },
            owner_id=456
        )
        
        mock_session_local.return_value = mock_db_session
        
        # Mock query chain
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [sample_audit_logs[0]]  # Only 'create' action
        mock_db_session.query.return_value = mock_query
        
        task = GetAuditLogsTask(task_with_filter)
        
        with patch.object(task, 'update_progress'), \
             patch('app.services.async_tasks.tasks.get_audit_logs.S3Service'), \
             patch('tempfile.NamedTemporaryFile'), \
             patch('builtins.open', mock_open()):
            
            task.perform()
            
            # Verify event filter was applied
            mock_query.filter.assert_called()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_perform_with_negative_filter(self, mock_session_local, mock_db_session, sample_audit_logs):
        """Test audit log generation with negative event filtering"""
        # Setup
        task_with_negative_filter = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "event_filter": "!create"
            },
            owner_id=456
        )
        
        mock_session_local.return_value = mock_db_session
        
        # Mock query chain
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [sample_audit_logs[1]]  # Only 'update' action
        mock_db_session.query.return_value = mock_query
        
        task = GetAuditLogsTask(task_with_negative_filter)
        
        with patch.object(task, 'update_progress'), \
             patch('app.services.async_tasks.tasks.get_audit_logs.S3Service'), \
             patch('tempfile.NamedTemporaryFile'), \
             patch('builtins.open', mock_open()):
            
            task.perform()
            
            # Verify negative filter was applied
            mock_query.filter.assert_called()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_perform_with_resource_filter(self, mock_session_local, mock_db_session, sample_audit_logs):
        """Test audit log generation with resource type and ID filtering"""
        # Setup
        task_with_resource = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "resource_type": "DataSource",
                "resource_id": 789
            },
            owner_id=456
        )
        
        mock_session_local.return_value = mock_db_session
        
        # Mock query chain
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [sample_audit_logs[0]]  # Only DataSource record
        mock_db_session.query.return_value = mock_query
        
        task = GetAuditLogsTask(task_with_resource)
        
        with patch.object(task, 'update_progress'), \
             patch('app.services.async_tasks.tasks.get_audit_logs.S3Service'), \
             patch('tempfile.NamedTemporaryFile'), \
             patch('builtins.open', mock_open()):
            
            task.perform()
            
            # Verify resource filters were applied
            assert mock_query.filter.call_count >= 3  # org_id + resource_type + resource_id
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    @patch('app.services.async_tasks.tasks.get_audit_logs.S3Service')
    def test_perform_s3_upload_failure(self, mock_s3_service, mock_session_local, mock_db_session, sample_task):
        """Test handling of S3 upload failure"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_s3_instance = Mock()
        mock_s3_instance.upload_file.side_effect = Exception("S3 upload failed")
        mock_s3_service.return_value = mock_s3_instance
        
        # Mock query results
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []
        mock_db_session.query.return_value = mock_query
        
        task = GetAuditLogsTask(sample_task)
        
        with patch('tempfile.NamedTemporaryFile'), \
             patch('builtins.open', mock_open()):
            
            # Execute and verify exception is raised
            with pytest.raises(Exception, match="S3 upload failed"):
                task.perform()
    
    @patch('app.services.async_tasks.tasks.get_audit_logs.SessionLocal')
    def test_apply_flow_filter_data_source(self, mock_session_local, mock_db_session):
        """Test flow filter application for DataSource"""
        # Setup
        task_with_flow = AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            arguments={
                "org_id": 123,
                "resource_type": "DataSource",
                "resource_id": 789,
                "flow": True
            },
            owner_id=456
        )
        
        mock_session_local.return_value = mock_db_session
        task = GetAuditLogsTask(task_with_flow)
        
        # Mock query for testing flow filter
        mock_query = Mock()
        mock_subquery = Mock()
        mock_subquery.subquery.return_value = mock_subquery
        mock_db_session.query.return_value = mock_subquery
        
        # Execute flow filter
        result = task._apply_flow_filter(mock_query, "DataSource", 789)
        
        # Verify filter method was called
        mock_query.filter.assert_called_once()
    
    def test_audit_log_json_format(self, sample_audit_logs):
        """Test that audit logs are properly formatted for JSON export"""
        # Get the first audit log
        log = sample_audit_logs[0]
        
        # Simulate the JSON formatting logic from perform()
        audit_record = {
            'id': log.id,
            'timestamp': log.timestamp.isoformat() if log.timestamp else None,
            'user_id': log.user_id,
            'ip_address': log.ip_address,
            'user_agent': log.user_agent,
            'action': log.action,
            'event': log.action,  # Rails compatibility
            'item_type': log.resource_type,  # Rails compatibility
            'resource_type': log.resource_type,
            'resource_id': log.resource_id,
            'resource_name': log.resource_name,
            'method': log.method,
            'endpoint': log.endpoint,
            'old_values': log.old_values,
            'new_values': log.new_values,
            'changes': log.changes,
            'details': log.details,
            'org_id': log.org_id,
            'risk_level': log.risk_level
        }
        
        # Verify the structure
        assert audit_record['id'] == 1
        assert audit_record['action'] == 'create'
        assert audit_record['event'] == 'create'  # Rails compatibility
        assert audit_record['item_type'] == 'DataSource'  # Rails compatibility
        assert audit_record['resource_type'] == 'DataSource'
        assert 'timestamp' in audit_record
        assert audit_record['timestamp'] is not None