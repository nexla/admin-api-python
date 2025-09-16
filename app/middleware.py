"""
Middleware components for request/response processing
"""

import time
import uuid
from typing import Callable
from fastapi import Request, Response
from fastapi.responses import JSONResponse
import logging

from app.services.prometheus_metric_service import PrometheusMetricService
from app.services.request_logger_service import RequestLoggerService
from app.services.audit_service import AuditService

logger = logging.getLogger(__name__)


class RequestMetricsMiddleware:
    """Middleware to collect request metrics"""
    
    def __init__(self, app):
        self.app = app
        self.prometheus = PrometheusMetricService.instance()
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        start_time = time.time()
        request_id = str(uuid.uuid4())
        
        # Create request object
        request = Request(scope, receive)
        
        # Add request ID to headers
        scope["headers"].append((b"x-request-id", request_id.encode()))
        
        # Track request start
        self.prometheus.counter("http_requests_total").increment({
            "method": request.method,
            "endpoint": str(request.url.path)
        })
        
        response_status = 500  # Default to error
        
        async def send_wrapper(message):
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = message["status"]
            await send(message)
        
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as e:
            logger.error(f"Request {request_id} failed: {str(e)}")
            response_status = 500
            raise
        finally:
            # Calculate duration and record metrics
            duration = time.time() - start_time
            
            self.prometheus.histogram("http_request_duration_seconds").observe(
                duration, {
                    "method": request.method,
                    "endpoint": str(request.url.path),
                    "status": str(response_status)
                }
            )
            
            # Track response status
            self.prometheus.counter("http_responses_total").increment({
                "method": request.method,
                "endpoint": str(request.url.path),
                "status": str(response_status)
            })


class RequestLoggingMiddleware:
    """Middleware to log requests and responses"""
    
    def __init__(self, app):
        self.app = app
        self.request_logger = RequestLoggerService()
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        start_time = time.time()
        request = Request(scope, receive)
        
        # Log request start
        request_data = {
            "method": request.method,
            "url": str(request.url),
            "headers": dict(request.headers),
            "client_ip": request.client.host if request.client else None
        }
        
        response_data = {}
        
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                response_data["status"] = message["status"]
                response_data["headers"] = dict(message.get("headers", []))
            await send(message)
        
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as e:
            response_data["status"] = 500
            response_data["error"] = str(e)
            raise
        finally:
            # Log request completion
            duration = time.time() - start_time
            response_data["duration_ms"] = duration * 1000
            
            await self.request_logger.log_request(request_data, response_data)


class AuditLoggingMiddleware:
    """Middleware to audit API requests"""
    
    def __init__(self, app):
        self.app = app
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        request = Request(scope, receive)
        
        # Skip audit for health checks and docs
        skip_paths = ["/docs", "/redoc", "/openapi.json", "/health", "/status"]
        if any(request.url.path.startswith(path) for path in skip_paths):
            await self.app(scope, receive, send)
            return
        
        start_time = time.time()
        user_id = None
        response_status = 500
        
        async def send_wrapper(message):
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = message["status"]
            await send(message)
        
        try:
            # Try to get user from request if authenticated
            if hasattr(request.state, "user"):
                user_id = request.state.user.id
            
            await self.app(scope, receive, send_wrapper)
            
        except Exception as e:
            response_status = 500
            raise
        finally:
            # Log audit entry
            duration = time.time() - start_time
            
            try:
                await AuditService.log_action(
                    user_id=user_id,
                    action="api.request",
                    resource_type="http_request",
                    details={
                        "method": request.method,
                        "path": request.url.path,
                        "status": response_status,
                        "duration_ms": duration * 1000,
                        "client_ip": request.client.host if request.client else None
                    }
                )
            except Exception as audit_error:
                logger.error(f"Failed to log audit entry: {str(audit_error)}")


class ErrorHandlingMiddleware:
    """Middleware for centralized error handling"""
    
    def __init__(self, app):
        self.app = app
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        try:
            await self.app(scope, receive, send)
        except Exception as e:
            # Log the error
            logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
            
            # Create error response
            if isinstance(e, ValueError):
                status_code = 400
                detail = str(e)
            elif isinstance(e, PermissionError):
                status_code = 403
                detail = "Insufficient permissions"
            elif isinstance(e, FileNotFoundError):
                status_code = 404
                detail = "Resource not found"
            else:
                status_code = 500
                detail = "Internal server error"
            
            response = JSONResponse(
                status_code=status_code,
                content={
                    "error": {
                        "type": type(e).__name__,
                        "detail": detail,
                        "timestamp": time.time()
                    }
                }
            )
            
            await response(scope, receive, send)


class CORSMiddleware:
    """Custom CORS middleware with enhanced features"""
    
    def __init__(self, app, allowed_origins=None, allowed_methods=None, allowed_headers=None):
        self.app = app
        self.allowed_origins = allowed_origins or ["*"]
        self.allowed_methods = allowed_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
        self.allowed_headers = allowed_headers or ["*"]
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        request = Request(scope, receive)
        
        # Handle preflight requests
        if request.method == "OPTIONS":
            response = Response(status_code=200)
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = ", ".join(self.allowed_methods)
            response.headers["Access-Control-Allow-Headers"] = ", ".join(self.allowed_headers)
            response.headers["Access-Control-Max-Age"] = "86400"
            await response(scope, receive, send)
            return
        
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                # Add CORS headers
                headers = list(message.get("headers", []))
                headers.append((b"access-control-allow-origin", b"*"))
                headers.append((b"access-control-allow-credentials", b"true"))
                message["headers"] = headers
            await send(message)
        
        await self.app(scope, receive, send_wrapper)


class SecurityHeadersMiddleware:
    """Middleware to add security headers"""
    
    def __init__(self, app):
        self.app = app
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                
                # Add security headers
                security_headers = [
                    (b"x-frame-options", b"DENY"),
                    (b"x-content-type-options", b"nosniff"),
                    (b"x-xss-protection", b"1; mode=block"),
                    (b"strict-transport-security", b"max-age=31536000; includeSubDomains"),
                    (b"referrer-policy", b"strict-origin-when-cross-origin"),
                    (b"content-security-policy", b"default-src 'self'")
                ]
                
                headers.extend(security_headers)
                message["headers"] = headers
                
            await send(message)
        
        await self.app(scope, receive, send_wrapper)


class RateLimitingMiddleware:
    """Simple rate limiting middleware"""
    
    def __init__(self, app, requests_per_minute=60):
        self.app = app
        self.requests_per_minute = requests_per_minute
        self.client_requests = {}
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        request = Request(scope, receive)
        client_ip = request.client.host if request.client else "unknown"
        current_time = time.time()
        
        # Clean old entries
        self._cleanup_old_entries(current_time)
        
        # Check rate limit
        if client_ip in self.client_requests:
            request_times = self.client_requests[client_ip]
            if len(request_times) >= self.requests_per_minute:
                # Rate limit exceeded
                response = JSONResponse(
                    status_code=429,
                    content={
                        "error": {
                            "type": "RateLimitExceeded",
                            "detail": f"Rate limit exceeded. Maximum {self.requests_per_minute} requests per minute.",
                            "retry_after": 60
                        }
                    }
                )
                await response(scope, receive, send)
                return
        else:
            self.client_requests[client_ip] = []
        
        # Record this request
        self.client_requests[client_ip].append(current_time)
        
        await self.app(scope, receive, send)
    
    def _cleanup_old_entries(self, current_time):
        """Clean up old request entries"""
        cutoff_time = current_time - 60  # 1 minute ago
        
        for client_ip in list(self.client_requests.keys()):
            self.client_requests[client_ip] = [
                t for t in self.client_requests[client_ip] if t > cutoff_time
            ]
            
            if not self.client_requests[client_ip]:
                del self.client_requests[client_ip]