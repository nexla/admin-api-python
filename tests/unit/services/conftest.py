"""
Service Test Configuration and Fixtures.
Provides common fixtures and utilities for service testing.
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Optional

from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership
from app.models.async_task import AsyncTask
from app.models.audit_log import AuditLog
from app.models.data_set import DataSet
from app.models.data_source import DataSource
from app.models.enums import UserStatus, TaskStatus, DataSourceStatus, OrgRole


@pytest.fixture
def mock_db_session():
    """Create a mock database session for testing"""
    mock_session = Mock(spec=Session)
    
    # Configure common query patterns
    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.filter_by.return_value = mock_query
    mock_query.join.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.offset.return_value = mock_query
    mock_query.all.return_value = []
    mock_query.first.return_value = None
    mock_query.scalar.return_value = None
    mock_query.count.return_value = 0
    
    mock_session.query.return_value = mock_query
    mock_session.add.return_value = None
    mock_session.commit.return_value = None
    mock_session.rollback.return_value = None
    mock_session.merge.return_value = None
    mock_session.close.return_value = None
    
    return mock_session


@pytest.fixture
def sample_user():
    """Create a sample user for testing"""
    return User(
        id=123,
        email="test@example.com",
        first_name="Test",
        last_name="User",
        status=UserStatus.ACTIVE,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_admin_user():
    """Create a sample admin user for testing"""
    return User(
        id=456,
        email="admin@example.com",
        first_name="Admin",
        last_name="User",
        status=UserStatus.ACTIVE,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_org():
    """Create a sample organization for testing"""
    return Org(
        id=789,
        name="Test Organization",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_org_membership(sample_user, sample_org):
    """Create a sample organization membership"""
    return OrgMembership(
        id=1,
        user_id=sample_user.id,
        org_id=sample_org.id,
        role=OrgRole.USER.value,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_admin_membership(sample_admin_user, sample_org):
    """Create a sample admin organization membership"""
    return OrgMembership(
        id=2,
        user_id=sample_admin_user.id,
        org_id=sample_org.id,
        role=OrgRole.ADMIN.value,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_async_task(sample_user, sample_org):
    """Create a sample async task for testing"""
    return AsyncTask(
        id=1,
        task_type="TestTask",
        status=TaskStatus.PENDING,
        arguments={"test_param": "test_value"},
        owner_id=sample_user.id,
        org_id=sample_org.id,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        progress=0,
        progress_message=None,
        result=None,
        result_url=None,
        error_message=None,
        started_at=None,
        stopped_at=None
    )


@pytest.fixture
def sample_completed_task(sample_async_task):
    """Create a completed async task for testing"""
    task = sample_async_task
    task.status = TaskStatus.COMPLETED
    task.progress = 100
    task.result = {"output": "test result"}
    task.result_url = "https://example.com/result"
    task.started_at = datetime.utcnow() - timedelta(minutes=5)
    task.stopped_at = datetime.utcnow()
    return task


@pytest.fixture
def sample_audit_log(sample_user, sample_org):
    """Create a sample audit log for testing"""
    return AuditLog(
        id=1,
        user_id=sample_user.id,
        org_id=sample_org.id,
        ip_address="192.168.1.1",
        user_agent="TestAgent/1.0",
        action="create",
        resource_type="TestResource",
        resource_id=123,
        resource_name="Test Resource",
        method="POST",
        endpoint="/api/v1/test",
        old_values=None,
        new_values={"name": "Test Resource"},
        changes={"name": {"old": None, "new": "Test Resource"}},
        details={"source": "test"},
        risk_level="low",
        timestamp=datetime.utcnow()
    )


@pytest.fixture
def sample_data_source(sample_user, sample_org):
    """Create a sample data source for testing"""
    return DataSource(
        id=1,
        name="Test Data Source",
        description="Test data source description",
        owner_id=sample_user.id,
        org_id=sample_org.id,
        status=DataSourceStatus.ACTIVE,
        connector_type="api",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_data_set(sample_user, sample_org, sample_data_source):
    """Create a sample data set for testing"""
    return DataSet(
        id=1,
        name="Test Data Set",
        description="Test data set description",
        owner_id=sample_user.id,
        org_id=sample_org.id,
        data_source_id=sample_data_source.id,
        public=False,
        output_schema={"properties": {"field1": {"type": "string"}}},
        source_schema={"properties": {"field2": {"type": "number"}}},
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def mock_request():
    """Create a mock HTTP request for testing"""
    request = Mock()
    request.host = "test.example.com"
    request.fullpath = "/api/v1/test"
    request.path = "/api/v1/test"
    request.method = "GET"
    request.user_agent = "TestAgent/1.0"
    request.headers = {
        "User-Agent": "TestAgent/1.0",
        "Content-Type": "application/json"
    }
    request.client = Mock()
    request.client.host = "192.168.1.1"
    request.remote_addr = "192.168.1.1"
    return request


@pytest.fixture
def mock_response():
    """Create a mock HTTP response for testing"""
    response = Mock()
    response.response_code = 200
    response.status_code = 200
    return response


@pytest.fixture
def mock_api_user_info(sample_user, sample_org):
    """Create a mock API user info object"""
    api_user_info = Mock()
    api_user_info.user = sample_user
    api_user_info.org = sample_org
    return api_user_info


class MockS3Service:
    """Mock S3 service for testing"""
    
    def __init__(self):
        self.uploaded_files = {}
        self.deleted_files = set()
    
    def upload_file(self, file_obj, file_key, bucket, **kwargs):
        """Mock file upload"""
        self.uploaded_files[f"{bucket}/{file_key}"] = {
            'file_obj': file_obj,
            'kwargs': kwargs
        }
        return True
    
    def delete_file(self, bucket, file_key):
        """Mock file deletion"""
        self.deleted_files.add(f"{bucket}/{file_key}")
        return True
    
    def get_presigned_url(self, bucket, file_key, expiration=3600, **kwargs):
        """Mock presigned URL generation"""
        return f"https://mock-s3.amazonaws.com/{bucket}/{file_key}?presigned=true"
    
    def file_exists(self, bucket, file_key):
        """Mock file existence check"""
        return f"{bucket}/{file_key}" in self.uploaded_files


@pytest.fixture
def mock_s3_service():
    """Create a mock S3 service for testing"""
    return MockS3Service()


class ServiceTestHelpers:
    """Helper functions for service testing"""
    
    @staticmethod
    def create_task_with_status(status: TaskStatus, **kwargs) -> AsyncTask:
        """Create an async task with specific status"""
        defaults = {
            'id': 1,
            'task_type': 'TestTask',
            'status': status,
            'owner_id': 123,
            'org_id': 456,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        defaults.update(kwargs)
        return AsyncTask(**defaults)
    
    @staticmethod
    def create_user_with_status(status: UserStatus, **kwargs) -> User:
        """Create a user with specific status"""
        defaults = {
            'id': 123,
            'email': 'test@example.com',
            'status': status,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        defaults.update(kwargs)
        return User(**defaults)
    
    @staticmethod
    def create_data_source_with_status(status: DataSourceStatus, **kwargs) -> DataSource:
        """Create a data source with specific status"""
        defaults = {
            'id': 1,
            'name': 'Test DataSource',
            'status': status,
            'owner_id': 123,
            'org_id': 456,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        defaults.update(kwargs)
        return DataSource(**defaults)
    
    @staticmethod
    def mock_query_result(mock_session, model_class, result):
        """Configure mock session to return specific query result"""
        mock_session.query.return_value.filter.return_value.first.return_value = result
        mock_session.query.return_value.filter.return_value.all.return_value = (
            [result] if result else []
        )
        return mock_session
    
    @staticmethod
    def mock_query_results(mock_session, model_class, results):
        """Configure mock session to return multiple query results"""
        mock_session.query.return_value.filter.return_value.all.return_value = results
        mock_session.query.return_value.filter.return_value.first.return_value = (
            results[0] if results else None
        )
        return mock_session


@pytest.fixture
def service_helpers():
    """Provide service test helper functions"""
    return ServiceTestHelpers


# Mock model fixtures for testing
@pytest.fixture
def mock_models(monkeypatch):
    """Mock all model imports for isolated testing"""
    mock_user = Mock(spec=User)
    mock_org = Mock(spec=Org)
    mock_task = Mock(spec=AsyncTask)
    mock_audit_log = Mock(spec=AuditLog)
    
    # These can be used to replace model imports in tests
    return {
        'User': mock_user,
        'Org': mock_org,
        'AsyncTask': mock_task,
        'AuditLog': mock_audit_log
    }


# Performance testing fixtures
@pytest.fixture
def large_dataset():
    """Create a large dataset for performance testing"""
    return [
        {
            'id': i,
            'name': f'item_{i}',
            'value': i * 10,
            'created_at': datetime.utcnow() - timedelta(days=i % 30)
        }
        for i in range(1000)
    ]


@pytest.fixture
def performance_timer():
    """Timer fixture for performance testing"""
    import time
    
    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None
        
        def start(self):
            self.start_time = time.time()
        
        def stop(self):
            self.end_time = time.time()
        
        def elapsed(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None
    
    return Timer()


# Async testing fixtures
@pytest.fixture
def event_loop():
    """Create event loop for async testing"""
    import asyncio
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# Common test data
TEST_EMAIL = "test@example.com"
TEST_ORG_NAME = "Test Organization"
TEST_TASK_TYPE = "TestTask"
TEST_IP_ADDRESS = "192.168.1.1"
TEST_USER_AGENT = "TestAgent/1.0"