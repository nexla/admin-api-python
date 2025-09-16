"""
Tests for RequestLoggerService.
Tests request logging, buffering, and async processing.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from queue import Queue, Empty
import time
import threading

from app.services.request_logger_service import RequestLoggerService, RequestLogEntry


class TestRequestLoggerService:
    """Test RequestLoggerService functionality"""
    
    def setup_method(self):
        """Setup for each test method"""
        # Clear singleton instance between tests
        RequestLoggerService._instance = None
    
    def test_singleton_pattern(self):
        """Test singleton pattern works correctly"""
        instance1 = RequestLoggerService.instance()
        instance2 = RequestLoggerService.instance()
        
        assert instance1 is instance2
    
    def test_initialization_enabled(self):
        """Test service initialization when enabled"""
        with patch('app.services.request_logger_service.settings') as mock_settings:
            mock_settings.REQUEST_LOGGING_ENABLED = True
            mock_settings.REQUEST_LOG_BUFFER_SIZE = 25
            
            service = RequestLoggerService()
            
            assert service.enabled() is True
            assert service._buffer_size == 25
    
    def test_initialization_disabled(self):
        """Test service initialization when disabled"""
        with patch('app.services.request_logger_service.settings') as mock_settings:
            mock_settings.REQUEST_LOGGING_ENABLED = False
            
            service = RequestLoggerService()
            
            assert service.enabled() is False
    
    def test_log_request_basic(self):
        """Test basic request logging"""
        # Setup
        service = RequestLoggerService()
        service._enabled = True
        service.clear_buffer()  # Ensure clean state
        
        # Mock objects
        user = Mock()
        user.id = 123
        org = Mock()
        org.id = 456
        request = Mock()
        request.host = 'test.example.com'
        request.fullpath = '/api/v1/test'
        request.method = 'GET'
        request.user_agent = 'TestAgent/1.0'
        request.client = Mock()
        request.client.host = '192.168.1.1'
        response = Mock()
        response.response_code = 200
        
        # Execute
        service.log(user, org, request, response, processing_time_ms=150)
        
        # Verify entry was added to buffer
        assert service._buffer.qsize() == 1
        
        # Get the entry and verify
        entry = service._buffer.get_nowait()
        assert isinstance(entry, RequestLogEntry)
        assert entry.user_id == 123
        assert entry.org_id == 456
        assert entry.host == 'test.example.com'
        assert entry.path == '/api/v1/test'
        assert entry.method == 'GET'
        assert entry.user_agent == 'TestAgent/1.0'
        assert entry.ip_address == '192.168.1.1'
        assert entry.response_code == 200
        assert entry.processing_time_ms == 150
    
    def test_log_request_anonymous_user(self):
        """Test logging request with anonymous user"""
        # Setup
        service = RequestLoggerService()
        service._enabled = True
        service.clear_buffer()
        
        # Mock objects (no user/org)
        request = Mock()
        request.host = 'test.example.com'
        request.fullpath = '/public'
        request.method = 'GET'
        response = Mock()
        response.response_code = 200
        
        # Execute
        service.log(None, None, request, response)
        
        # Verify
        entry = service._buffer.get_nowait()
        assert entry.user_id is None
        assert entry.org_id is None
        assert entry.path == '/public'
    
    def test_log_request_disabled(self):
        """Test logging when service is disabled"""
        # Setup
        service = RequestLoggerService()
        service._enabled = False
        service.clear_buffer()
        
        # Mock objects
        request = Mock()
        response = Mock()
        
        # Execute
        service.log(Mock(), Mock(), request, response)
        
        # Verify nothing was logged
        assert service._buffer.qsize() == 0
    
    def test_buffer_flushing_on_size_limit(self):
        """Test buffer flushing when size limit is reached"""
        # Setup
        service = RequestLoggerService()
        service._enabled = True
        service._buffer_size = 2  # Small buffer for testing
        service.clear_buffer()
        
        # Mock flush method to track calls
        with patch.object(service, '_flush_buffer') as mock_flush:
            # Mock objects
            request = Mock()
            request.host = 'test.com'
            request.fullpath = '/'
            request.method = 'GET'
            response = Mock()
            response.response_code = 200
            
            # Log first request (should not flush)
            service.log(Mock(), Mock(), request, response)
            assert mock_flush.call_count == 0
            
            # Log second request (should trigger flush)
            service.log(Mock(), Mock(), request, response)
            assert mock_flush.call_count == 1
    
    def test_extract_request_time_from_datetime(self):
        """Test extracting request time from datetime header"""
        service = RequestLoggerService()
        
        test_time = datetime(2023, 1, 15, 10, 30, 0)
        request = Mock()
        request.req_time = test_time
        
        result = service._extract_request_time(request)
        
        assert result == test_time
    
    def test_extract_request_time_from_timestamp(self):
        """Test extracting request time from timestamp header"""
        service = RequestLoggerService()
        
        timestamp = 1673781000.0  # Unix timestamp
        request = Mock()
        request.req_time = timestamp
        
        result = service._extract_request_time(request)
        
        assert result == datetime.fromtimestamp(timestamp)
    
    def test_extract_request_time_fallback(self):
        """Test request time fallback to current time"""
        service = RequestLoggerService()
        
        request = Mock()
        # No req_time attribute
        
        before = datetime.utcnow()
        result = service._extract_request_time(request)
        after = datetime.utcnow()
        
        assert before <= result <= after
    
    def test_extract_header_direct_attribute(self):
        """Test header extraction from direct attribute"""
        service = RequestLoggerService()
        
        request = Mock()
        request.user_agent = 'TestAgent/1.0'
        
        result = service._extract_header(request, 'user_agent')
        
        assert result == 'TestAgent/1.0'
    
    def test_extract_header_from_headers_dict(self):
        """Test header extraction from headers dictionary"""
        service = RequestLoggerService()
        
        request = Mock()
        request.headers = {'User-Agent': 'TestAgent/1.0'}
        
        result = service._extract_header(request, 'user_agent', 'User-Agent')
        
        assert result == 'TestAgent/1.0'
    
    def test_extract_header_not_found(self):
        """Test header extraction when header not found"""
        service = RequestLoggerService()
        
        request = Mock()
        request.headers = {}
        
        result = service._extract_header(request, 'nonexistent')
        
        assert result is None
    
    def test_extract_ip_address_client_attribute(self):
        """Test IP extraction from client attribute"""
        service = RequestLoggerService()
        
        request = Mock()
        request.client = Mock()
        request.client.host = '192.168.1.1'
        
        result = service._extract_ip_address(request)
        
        assert result == '192.168.1.1'
    
    def test_extract_ip_address_forwarded_header(self):
        """Test IP extraction from X-Forwarded-For header"""
        service = RequestLoggerService()
        
        request = Mock()
        request.client = None
        request.headers = {'X-Forwarded-For': '203.0.113.1, 198.51.100.1'}
        
        result = service._extract_ip_address(request)
        
        assert result == '203.0.113.1'  # First IP from the list
    
    def test_extract_ip_address_remote_addr(self):
        """Test IP extraction from remote_addr attribute"""
        service = RequestLoggerService()
        
        request = Mock()
        request.client = None
        request.headers = {}
        request.remote_addr = '10.0.0.1'
        
        result = service._extract_ip_address(request)
        
        assert result == '10.0.0.1'
    
    def test_extract_ip_address_not_found(self):
        """Test IP extraction when no IP found"""
        service = RequestLoggerService()
        
        request = Mock()
        request.client = None
        request.headers = {}
        
        result = service._extract_ip_address(request)
        
        assert result is None
    
    @patch('app.services.request_logger_service.logger')
    def test_flush_buffer_success(self, mock_logger):
        """Test successful buffer flushing"""
        service = RequestLoggerService()
        service._enabled = True
        
        # Add test entries to buffer
        entry1 = RequestLogEntry(
            user_id=1, org_id=1, request_time=datetime.utcnow(),
            host='test.com', path='/', method='GET', user_agent='Test',
            ip_address='127.0.0.1', response_code=200
        )
        entry2 = RequestLogEntry(
            user_id=2, org_id=2, request_time=datetime.utcnow(),
            host='test.com', path='/api', method='POST', user_agent='Test',
            ip_address='127.0.0.1', response_code=201
        )
        
        service._buffer.put(entry1)
        service._buffer.put(entry2)
        
        # Mock worker sending
        with patch.object(service, '_send_to_worker') as mock_send:
            # Execute
            service._flush_buffer()
            
            # Verify
            assert service._buffer.empty()
            mock_send.assert_called_once()
            log_data = mock_send.call_args[0][0]
            assert len(log_data) == 2
            assert all(isinstance(entry, dict) for entry in log_data)
    
    def test_flush_buffer_empty(self):
        """Test flushing empty buffer"""
        service = RequestLoggerService()
        service._enabled = True
        service.clear_buffer()
        
        with patch.object(service, '_send_to_worker') as mock_send:
            # Execute
            service._flush_buffer()
            
            # Verify no worker call
            mock_send.assert_not_called()
    
    @patch('app.services.request_logger_service.logger')
    def test_send_to_worker_logging(self, mock_logger):
        """Test sending log data to worker"""
        service = RequestLoggerService()
        
        log_data = [{'user_id': 1, 'path': '/test'}]
        
        # Execute
        service._send_to_worker(log_data)
        
        # Verify info log was called
        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert '1 request log entries' in log_message
    
    def test_clear_buffer_utility(self):
        """Test buffer clearing utility method"""
        service = RequestLoggerService()
        
        # Add entries to buffer
        service._buffer.put('entry1')
        service._buffer.put('entry2')
        assert service._buffer.qsize() == 2
        
        # Clear buffer
        service.clear_buffer()
        
        # Verify empty
        assert service._buffer.qsize() == 0
    
    @patch('app.services.request_logger_service.logger')
    def test_error_handling_in_log_method(self, mock_logger):
        """Test error handling in log method"""
        service = RequestLoggerService()
        service._enabled = True
        
        # Create request that will cause an error
        request = Mock()
        request.host = Mock(side_effect=Exception("Test error"))
        response = Mock()
        
        # Execute
        service.log(Mock(), Mock(), request, response)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert 'Failed to log request' in error_message
    
    def test_request_log_entry_to_dict(self):
        """Test RequestLogEntry conversion to dictionary"""
        entry = RequestLogEntry(
            user_id=123,
            org_id=456,
            request_time=datetime(2023, 1, 15, 10, 30, 0),
            host='test.com',
            path='/api/test',
            method='POST',
            user_agent='TestAgent/1.0',
            ip_address='192.168.1.1',
            response_code=201,
            processing_time_ms=150
        )
        
        result = entry.to_dict()
        
        assert result['user_id'] == 123
        assert result['org_id'] == 456
        assert result['request_time'] == '2023-01-15T10:30:00'
        assert result['host'] == 'test.com'
        assert result['path'] == '/api/test'
        assert result['method'] == 'POST'
        assert result['user_agent'] == 'TestAgent/1.0'
        assert result['ip_address'] == '192.168.1.1'
        assert result['response_code'] == 201
        assert result['processing_time_ms'] == 150