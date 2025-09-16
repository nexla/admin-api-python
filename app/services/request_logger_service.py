"""
Request Logger Service - Log HTTP requests for analytics and monitoring.
Provides buffered request logging with async processing.
"""

import logging
import json
import threading
from typing import Optional, Dict, Any, List
from datetime import datetime
from dataclasses import dataclass, asdict
from queue import Queue, Empty
import time

from ..config import settings

logger = logging.getLogger(__name__)


@dataclass
class RequestLogEntry:
    """Request log entry data structure"""
    user_id: Optional[int]
    org_id: Optional[int]
    request_time: datetime
    host: str
    path: str
    method: str
    user_agent: Optional[str]
    ip_address: Optional[str]
    response_code: int
    processing_time_ms: Optional[int] = None
    request_size: Optional[int] = None
    response_size: Optional[int] = None
    session_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        # Convert datetime to ISO string
        if data['request_time']:
            data['request_time'] = data['request_time'].isoformat()
        return data


class RequestLoggerService:
    """Singleton service for request logging with buffering"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(RequestLoggerService, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._buffer = Queue()
        self._buffer_size = getattr(settings, 'REQUEST_LOG_BUFFER_SIZE', 50)
        self._enabled = getattr(settings, 'REQUEST_LOGGING_ENABLED', True)
        self._background_thread = None
        self._shutdown_event = threading.Event()
        self._initialized = True
        
        if self._enabled:
            self._start_background_processor()
    
    @classmethod
    def instance(cls) -> 'RequestLoggerService':
        """Get singleton instance"""
        return cls()
    
    def enabled(self) -> bool:
        """Check if request logging is enabled"""
        return self._enabled
    
    def log(
        self,
        user: Optional[Any],
        org: Optional[Any],
        request: Any,
        response: Any,
        processing_time_ms: Optional[int] = None
    ):
        """
        Log a request to the buffer.
        
        Args:
            user: User object (can be None for anonymous requests)
            org: Organization object (can be None)
            request: Request object with headers, method, path, etc.
            response: Response object with status code
            processing_time_ms: Request processing time in milliseconds
        """
        if not self._enabled:
            return
        
        try:
            # Extract request data
            user_id = getattr(user, 'id', None) if user else None
            org_id = getattr(org, 'id', None) if org else None
            
            # Get request time from headers or use current time
            request_time = self._extract_request_time(request)
            
            # Extract request details
            host = getattr(request, 'host', 'unknown')
            path = getattr(request, 'fullpath', getattr(request, 'path', '/'))
            method = getattr(request, 'method', 'UNKNOWN')
            user_agent = self._extract_header(request, 'user_agent', 'User-Agent')
            ip_address = self._extract_ip_address(request)
            
            # Extract response details
            response_code = getattr(response, 'response_code', getattr(response, 'status_code', 200))
            
            # Create log entry
            log_entry = RequestLogEntry(
                user_id=user_id,
                org_id=org_id,
                request_time=request_time,
                host=host,
                path=path,
                method=method,
                user_agent=user_agent,
                ip_address=ip_address,
                response_code=response_code,
                processing_time_ms=processing_time_ms
            )
            
            # Add to buffer
            self._buffer.put(log_entry)
            
            # Check if buffer needs flushing
            if self._buffer.qsize() >= self._buffer_size:
                self._flush_buffer()
            
        except Exception as e:
            logger.error(f"Failed to log request: {str(e)}")
    
    def _extract_request_time(self, request) -> datetime:
        """Extract request time from request headers or use current time"""
        try:
            # Try to get from req_time header first
            req_time_header = self._extract_header(request, 'req_time')
            if req_time_header:
                if isinstance(req_time_header, datetime):
                    return req_time_header
                elif isinstance(req_time_header, (int, float)):
                    return datetime.fromtimestamp(req_time_header)
                elif isinstance(req_time_header, str):
                    # Try to parse ISO format
                    try:
                        return datetime.fromisoformat(req_time_header.replace('Z', '+00:00'))
                    except ValueError:
                        pass
            
            return datetime.utcnow()
            
        except Exception:
            return datetime.utcnow()
    
    def _extract_header(self, request, *header_names) -> Optional[str]:
        """Extract header value by trying multiple header names"""
        try:
            # Try direct attribute access first
            for name in header_names:
                if hasattr(request, name):
                    value = getattr(request, name)
                    if value:
                        return value
            
            # Try headers dictionary
            if hasattr(request, 'headers'):
                headers = request.headers
                for name in header_names:
                    # Try as-is
                    if name in headers:
                        return headers[name]
                    # Try lowercase
                    if name.lower() in headers:
                        return headers[name.lower()]
                    # Try with dashes instead of underscores
                    dash_name = name.replace('_', '-')
                    if dash_name in headers:
                        return headers[dash_name]
            
            return None
            
        except Exception:
            return None
    
    def _extract_ip_address(self, request) -> Optional[str]:
        """Extract client IP address from request"""
        try:
            # Try various methods to get IP address
            
            # FastAPI/Starlette
            if hasattr(request, 'client') and request.client:
                return request.client.host
            
            # Try headers for proxy scenarios
            ip_headers = [
                'X-Forwarded-For',
                'X-Real-IP',
                'CF-Connecting-IP',
                'X-Forwarded-Host'
            ]
            
            for header in ip_headers:
                ip = self._extract_header(request, header)
                if ip:
                    # X-Forwarded-For can contain multiple IPs
                    return ip.split(',')[0].strip()
            
            # Try remote_addr attribute
            if hasattr(request, 'remote_addr'):
                return request.remote_addr
            
            return None
            
        except Exception:
            return None
    
    def _start_background_processor(self):
        """Start background thread for processing log buffer"""
        if self._background_thread is None or not self._background_thread.is_alive():
            self._background_thread = threading.Thread(
                target=self._background_processor,
                daemon=True,
                name="RequestLoggerProcessor"
            )
            self._background_thread.start()
    
    def _background_processor(self):
        """Background processor for log buffer"""
        logger.info("Request logger background processor started")
        
        while not self._shutdown_event.is_set():
            try:
                # Process buffer periodically
                time.sleep(1.0)  # Check every second
                
                if not self._buffer.empty():
                    self._flush_buffer()
                    
            except Exception as e:
                logger.error(f"Error in request logger background processor: {str(e)}")
    
    def _flush_buffer(self):
        """Flush buffer to storage/analytics service"""
        try:
            if self._buffer.empty():
                return
            
            # Collect all entries from buffer
            entries = []
            while True:
                try:
                    entry = self._buffer.get_nowait()
                    entries.append(entry)
                except Empty:
                    break
            
            if not entries:
                return
            
            # Convert to serializable format
            log_data = [entry.to_dict() for entry in entries]
            
            # Send to background worker/queue for processing
            self._send_to_worker(log_data)
            
            logger.debug(f"Flushed {len(entries)} request log entries")
            
        except Exception as e:
            logger.error(f"Failed to flush request log buffer: {str(e)}")
    
    def _send_to_worker(self, log_data: List[Dict[str, Any]]):
        """Send log data to background worker for processing"""
        try:
            # In a real implementation, this would:
            # 1. Send to Celery worker
            # 2. Send to message queue
            # 3. Write to database
            # 4. Send to analytics service
            
            # For now, just log the data
            logger.info(f"Processing {len(log_data)} request log entries")
            
            # Mock worker call - replace with actual implementation
            # RequestLoggingWorker.perform_async(log_data)
            
        except Exception as e:
            logger.error(f"Failed to send request logs to worker: {str(e)}")
    
    def shutdown(self):
        """Shutdown the request logger service"""
        logger.info("Shutting down request logger service")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Flush remaining buffer
        self._flush_buffer()
        
        # Wait for background thread
        if self._background_thread and self._background_thread.is_alive():
            self._background_thread.join(timeout=5.0)
    
    def clear_buffer(self):
        """Clear the buffer (for testing)"""
        while not self._buffer.empty():
            try:
                self._buffer.get_nowait()
            except Empty:
                break


# Global instance for easy access
request_logger = RequestLoggerService.instance()