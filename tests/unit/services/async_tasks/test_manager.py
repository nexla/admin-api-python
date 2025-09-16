"""
Tests for AsyncTaskManager service.
Tests task lifecycle management, cleanup operations, and task execution.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from typing import List
from app.services.async_tasks.manager import AsyncTaskManager, TaskRegistry, BaseAsyncTask
from app.models.async_task import AsyncTask
from app.models.enums import TaskStatus
from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership


class TestAsyncTaskManager:
    """Test AsyncTaskManager functionality"""
    
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
        mock_session.close.return_value = None
        return mock_session
    
    @pytest.fixture
    def sample_task(self):
        """Create a sample async task"""
        return AsyncTask(
            id=1,
            task_type="GetAuditLogs",
            status=TaskStatus.COMPLETED,
            result_url="s3://bucket/key",
            result={"storage": "s3", "bucket": "bucket", "file_key": "key1"},
            stopped_at=datetime.utcnow() - timedelta(days=6),
            owner_id=123,
            org_id=456
        )
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    @patch('app.services.async_tasks.manager.S3Service')
    def test_purge_expired_results_success(self, mock_s3_service, mock_session_local, mock_db_session, sample_task):
        """Test successful purging of expired task results"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_s3_instance = Mock()
        mock_s3_service.return_value = mock_s3_instance
        
        # Mock expired tasks query
        mock_db_session.query.return_value.filter.return_value.all.return_value = [sample_task]
        
        # Execute
        AsyncTaskManager.purge_expired_results()
        
        # Verify S3 deletion was called
        mock_s3_instance.delete_file.assert_called_once_with("bucket", "key1")
        
        # Verify task was updated
        assert sample_task.result_purged is True
        assert sample_task.result_url is None
        
        # Verify database operations
        mock_db_session.commit.assert_called_once()
        mock_db_session.close.assert_called_once()
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    @patch('app.services.async_tasks.manager.S3Service')
    def test_purge_expired_results_s3_failure(self, mock_s3_service, mock_session_local, mock_db_session, sample_task):
        """Test purge handling when S3 deletion fails"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_s3_instance = Mock()
        mock_s3_instance.delete_file.side_effect = Exception("S3 error")
        mock_s3_service.return_value = mock_s3_instance
        
        mock_db_session.query.return_value.filter.return_value.all.return_value = [sample_task]
        
        # Execute
        AsyncTaskManager.purge_expired_results()
        
        # Verify S3 deletion was attempted
        mock_s3_instance.delete_file.assert_called_once()
        
        # Verify task was still updated despite S3 failure
        mock_db_session.commit.assert_called_once()
    
    def test_get_task_status_success(self, mock_db_session):
        """Test successful task status retrieval"""
        # Setup
        task = AsyncTask(id=1, owner_id=123, status=TaskStatus.RUNNING)
        mock_db_session.query.return_value.filter.return_value.first.return_value = task
        
        # Execute
        result = AsyncTaskManager.get_task_status(mock_db_session, task_id=1, user_id=123)
        
        # Verify
        assert result == task
        mock_db_session.query.assert_called_once_with(AsyncTask)
    
    def test_get_task_status_no_permission(self, mock_db_session):
        """Test task status retrieval with no permission"""
        # Setup
        mock_db_session.query.return_value.filter.return_value.first.return_value = None
        
        # Execute
        result = AsyncTaskManager.get_task_status(mock_db_session, task_id=1, user_id=456)
        
        # Verify
        assert result is None
    
    def test_cancel_task_success(self, mock_db_session):
        """Test successful task cancellation"""
        # Setup
        task = AsyncTask(id=1, owner_id=123, status=TaskStatus.RUNNING)
        mock_db_session.query.return_value.filter.return_value.first.return_value = task
        
        # Execute
        result = AsyncTaskManager.cancel_task(mock_db_session, task_id=1, user_id=123)
        
        # Verify
        assert result is True
        assert task.status == TaskStatus.CANCELLED
        assert task.stopped_at is not None
        mock_db_session.commit.assert_called_once()
    
    def test_cancel_task_not_found(self, mock_db_session):
        """Test task cancellation when task not found or no permission"""
        # Setup
        mock_db_session.query.return_value.filter.return_value.first.return_value = None
        
        # Execute
        result = AsyncTaskManager.cancel_task(mock_db_session, task_id=1, user_id=123)
        
        # Verify
        assert result is False
        mock_db_session.commit.assert_not_called()
    
    def test_get_user_tasks_with_filters(self, mock_db_session):
        """Test getting user tasks with various filters"""
        # Setup
        tasks = [
            AsyncTask(id=1, owner_id=123, status=TaskStatus.COMPLETED, task_type="GetAuditLogs"),
            AsyncTask(id=2, owner_id=123, status=TaskStatus.RUNNING, task_type="DeactivateUser")
        ]
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.offset.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = tasks
        
        mock_db_session.query.return_value = mock_query
        
        # Execute
        result = AsyncTaskManager.get_user_tasks(
            mock_db_session,
            user_id=123,
            status=TaskStatus.COMPLETED,
            task_type="GetAuditLogs",
            limit=10,
            offset=0
        )
        
        # Verify
        assert result == tasks
        assert mock_query.filter.call_count >= 3  # user_id, status, task_type filters
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_cleanup_old_tasks(self, mock_session_local, mock_db_session):
        """Test cleanup of old task records"""
        # Setup
        mock_session_local.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.delete.return_value = 5
        
        # Execute
        AsyncTaskManager.cleanup_old_tasks()
        
        # Verify
        mock_db_session.query.assert_called_once_with(AsyncTask)
        mock_db_session.commit.assert_called_once()
        mock_db_session.close.assert_called_once()


class TestTaskRegistry:
    """Test TaskRegistry functionality"""
    
    def test_get_task_class_valid_type(self):
        """Test getting valid task class"""
        with patch('app.services.async_tasks.manager.__import__') as mock_import:
            mock_module = Mock()
            mock_module.GetAuditLogsTask = Mock()
            mock_import.return_value = mock_module
            
            result = TaskRegistry.get_task_class('GetAuditLogs')
            
            assert result == mock_module.GetAuditLogsTask
    
    def test_get_task_class_invalid_type(self):
        """Test getting invalid task class raises error"""
        with pytest.raises(ValueError, match="Unknown task type"):
            TaskRegistry.get_task_class('InvalidTaskType')
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_execute_task_success(self, mock_session_local):
        """Test successful task execution"""
        # Setup
        task = AsyncTask(id=1, task_type="GetAuditLogs")
        mock_task_class = Mock()
        mock_task_instance = Mock()
        mock_task_class.return_value = mock_task_instance
        
        with patch.object(TaskRegistry, 'get_task_class', return_value=mock_task_class):
            # Execute
            TaskRegistry.execute_task(task)
            
            # Verify
            mock_task_instance.check_preconditions.assert_called_once()
            mock_task_instance.perform.assert_called_once()
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_execute_task_failure(self, mock_session_local, mock_db_session):
        """Test task execution failure handling"""
        # Setup
        mock_session_local.return_value = mock_db_session
        task = AsyncTask(id=1, task_type="GetAuditLogs", status=TaskStatus.RUNNING)
        mock_task_class = Mock()
        mock_task_instance = Mock()
        mock_task_instance.perform.side_effect = Exception("Task failed")
        mock_task_class.return_value = mock_task_instance
        
        with patch.object(TaskRegistry, 'get_task_class', return_value=mock_task_class):
            # Execute and verify exception is raised
            with pytest.raises(Exception, match="Task failed"):
                TaskRegistry.execute_task(task)
            
            # Verify task was marked as failed
            assert task.status == TaskStatus.FAILED
            assert task.error_message == "Task failed"
            assert task.stopped_at is not None


class TestBaseAsyncTask:
    """Test BaseAsyncTask functionality"""
    
    @pytest.fixture
    def mock_task(self):
        """Mock async task object"""
        return AsyncTask(
            id=1,
            task_type="TestTask",
            arguments={"param1": "value1"},
            owner_id=123
        )
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_base_task_initialization(self, mock_session_local, mock_task):
        """Test base task initialization"""
        mock_db_session = Mock()
        mock_session_local.return_value = mock_db_session
        
        base_task = BaseAsyncTask(mock_task)
        
        assert base_task.task == mock_task
        assert base_task.arguments == {"param1": "value1"}
        assert base_task.db == mock_db_session
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_check_preconditions_default(self, mock_session_local, mock_task):
        """Test default check_preconditions does nothing"""
        base_task = BaseAsyncTask(mock_task)
        
        # Should not raise any exception
        base_task.check_preconditions()
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_perform_not_implemented(self, mock_session_local, mock_task):
        """Test perform method raises NotImplementedError"""
        base_task = BaseAsyncTask(mock_task)
        
        with pytest.raises(NotImplementedError, match="Task classes must implement perform"):
            base_task.perform()
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_update_progress(self, mock_session_local, mock_task):
        """Test progress update functionality"""
        mock_db_session = Mock()
        mock_session_local.return_value = mock_db_session
        
        base_task = BaseAsyncTask(mock_task)
        base_task.update_progress(50, "Half done")
        
        assert mock_task.progress == 50
        assert mock_task.progress_message == "Half done"
        assert mock_task.updated_at is not None
        mock_db_session.merge.assert_called_once_with(mock_task)
        mock_db_session.commit.assert_called_once()
    
    @patch('app.services.async_tasks.manager.SessionLocal')
    def test_set_result(self, mock_session_local, mock_task):
        """Test setting task result"""
        mock_db_session = Mock()
        mock_session_local.return_value = mock_db_session
        
        base_task = BaseAsyncTask(mock_task)
        result_data = {"output": "test result"}
        result_url = "https://example.com/result"
        
        base_task.set_result(result_data, result_url)
        
        assert mock_task.result == result_data
        assert mock_task.result_url == result_url
        assert mock_task.status == TaskStatus.COMPLETED
        assert mock_task.stopped_at is not None
        mock_db_session.merge.assert_called_once_with(mock_task)
        mock_db_session.commit.assert_called_once()