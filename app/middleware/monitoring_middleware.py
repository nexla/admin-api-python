import time
import json
import asyncio
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import redis
import logging
from datetime import datetime

from ..services.monitoring_service import monitoring_service

logger = logging.getLogger(__name__)

class RealTimeMonitoringMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        redis_url: str = "redis://localhost:6379",
        enable_detailed_metrics: bool = True
    ):
        super().__init__(app)
        self.redis_client = redis.from_url(redis_url)
        self.enable_detailed_metrics = enable_detailed_metrics
        self.request_counter = 0
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        
        # Track request start
        await self._track_request_start(request)
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate processing time
            process_time = time.time() - start_time
            
            # Track request completion
            await self._track_request_completion(request, response, process_time)
            
            return response
            
        except Exception as e:
            # Track request error
            process_time = time.time() - start_time
            await self._track_request_error(request, e, process_time)
            
            # Re-raise the exception
            raise e
    
    async def _track_request_start(self, request: Request):
        """Track request start metrics"""
        self.request_counter += 1
        
        # Increment request counter in Redis
        self.redis_client.incr("metrics:api:request_count")
        
        # Track concurrent requests
        self.redis_client.incr("metrics:api:concurrent_requests")
        
        # Publish real-time request event
        request_data = {
            "event": "request_start",
            "method": request.method,
            "url": str(request.url),
            "user_agent": request.headers.get("user-agent", ""),
            "timestamp": datetime.utcnow().isoformat(),
            "request_id": self.request_counter
        }
        
        self.redis_client.publish("api:requests", json.dumps(request_data))
        
        # Store request start time
        self.redis_client.setex(
            f"request:{self.request_counter}:start_time",
            300,  # 5 minute TTL
            start_time
        )
    
    async def _track_request_completion(self, request: Request, response: Response, process_time: float):
        """Track successful request completion"""
        # Decrement concurrent requests
        self.redis_client.decr("metrics:api:concurrent_requests")
        
        # Track response metrics
        status_code = response.status_code
        method = request.method
        endpoint = self._get_endpoint_pattern(request)
        
        # Increment status code counters
        self.redis_client.incr(f"metrics:api:status:{status_code}")
        self.redis_client.incr(f"metrics:api:method:{method}")
        self.redis_client.incr(f"metrics:api:endpoint:{endpoint}")
        
        # Track response time
        self.redis_client.lpush("metrics:api:response_times", process_time)
        self.redis_client.ltrim("metrics:api:response_times", 0, 999)  # Keep last 1000
        
        # Track detailed metrics if enabled
        if self.enable_detailed_metrics:
            await self._track_detailed_metrics(request, response, process_time)
        
        # Publish real-time completion event
        completion_data = {
            "event": "request_complete",
            "method": method,
            "endpoint": endpoint,
            "status_code": status_code,
            "process_time": process_time,
            "timestamp": datetime.utcnow().isoformat(),
            "request_id": self.request_counter
        }
        
        self.redis_client.publish("api:requests", json.dumps(completion_data))
        
        # Send metrics to monitoring service
        try:
            await monitoring_service.publish_metric_update(
                "api.request.duration_seconds",
                process_time,
                {
                    "method": method,
                    "endpoint": endpoint,
                    "status_code": str(status_code)
                }
            )
            
            await monitoring_service.publish_metric_update(
                "api.request.count",
                1,
                {
                    "method": method,
                    "endpoint": endpoint,
                    "status_code": str(status_code)
                }
            )
        except Exception as e:
            logger.error(f"Error publishing metrics: {e}")
    
    async def _track_request_error(self, request: Request, error: Exception, process_time: float):
        """Track request errors"""
        # Decrement concurrent requests
        self.redis_client.decr("metrics:api:concurrent_requests")
        
        # Increment error counter
        self.redis_client.incr("metrics:api:error_count")
        
        # Track error by type
        error_type = type(error).__name__
        self.redis_client.incr(f"metrics:api:error_type:{error_type}")
        
        # Track error by endpoint
        endpoint = self._get_endpoint_pattern(request)
        self.redis_client.incr(f"metrics:api:error_endpoint:{endpoint}")
        
        # Publish real-time error event
        error_data = {
            "event": "request_error",
            "method": request.method,
            "endpoint": endpoint,
            "error_type": error_type,
            "error_message": str(error),
            "process_time": process_time,
            "timestamp": datetime.utcnow().isoformat(),
            "request_id": self.request_counter
        }
        
        self.redis_client.publish("api:errors", json.dumps(error_data))
        
        # Send error metrics to monitoring service
        try:
            await monitoring_service.publish_metric_update(
                "api.error.count",
                1,
                {
                    "method": request.method,
                    "endpoint": endpoint,
                    "error_type": error_type
                }
            )
        except Exception as e:
            logger.error(f"Error publishing error metrics: {e}")
    
    async def _track_detailed_metrics(self, request: Request, response: Response, process_time: float):
        """Track detailed request metrics"""
        try:
            # Request size
            content_length = request.headers.get("content-length")
            if content_length:
                await monitoring_service.publish_metric_update(
                    "api.request.size_bytes",
                    float(content_length),
                    {"endpoint": self._get_endpoint_pattern(request)}
                )
            
            # Response size
            if hasattr(response, "headers") and "content-length" in response.headers:
                await monitoring_service.publish_metric_update(
                    "api.response.size_bytes",
                    float(response.headers["content-length"]),
                    {"endpoint": self._get_endpoint_pattern(request)}
                )
            
            # Track authentication metrics
            if "authorization" in request.headers:
                await monitoring_service.publish_metric_update(
                    "api.request.authenticated",
                    1,
                    {"endpoint": self._get_endpoint_pattern(request)}
                )
            else:
                await monitoring_service.publish_metric_update(
                    "api.request.unauthenticated",
                    1,
                    {"endpoint": self._get_endpoint_pattern(request)}
                )
            
        except Exception as e:
            logger.error(f"Error tracking detailed metrics: {e}")
    
    def _get_endpoint_pattern(self, request: Request) -> str:
        """Extract endpoint pattern from request"""
        try:
            if hasattr(request, "scope") and "route" in request.scope:
                route = request.scope["route"]
                if hasattr(route, "path"):
                    return route.path
            
            # Fallback to path pattern matching
            path = request.url.path
            
            # Replace common ID patterns
            import re
            path = re.sub(r'/\d+', '/{id}', path)
            path = re.sub(r'/[0-9a-f-]{36}', '/{uuid}', path)
            
            return path
            
        except Exception:
            return request.url.path

class PerformanceMonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware for performance monitoring and alerting"""
    
    def __init__(
        self,
        app,
        slow_request_threshold: float = 5.0,  # seconds
        memory_threshold: float = 500.0,  # MB
        redis_url: str = "redis://localhost:6379"
    ):
        super().__init__(app)
        self.slow_request_threshold = slow_request_threshold
        self.memory_threshold = memory_threshold
        self.redis_client = redis.from_url(redis_url)
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        import psutil
        import os
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        start_time = time.time()
        
        try:
            response = await call_next(request)
            
            # Calculate metrics
            process_time = time.time() - start_time
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_used = final_memory - initial_memory
            
            # Check for performance issues
            await self._check_performance_alerts(request, process_time, memory_used)
            
            return response
            
        except Exception as e:
            process_time = time.time() - start_time
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            
            # Log performance data even for errors
            await self._log_performance_data(request, process_time, final_memory - initial_memory, error=str(e))
            
            raise e
    
    async def _check_performance_alerts(self, request: Request, process_time: float, memory_used: float):
        """Check for performance-related alerts"""
        endpoint = self._get_endpoint_pattern(request)
        
        # Check for slow requests
        if process_time > self.slow_request_threshold:
            alert_data = {
                "type": "slow_request",
                "endpoint": endpoint,
                "process_time": process_time,
                "threshold": self.slow_request_threshold,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.redis_client.publish("alerts:performance", json.dumps(alert_data))
            
            # Send to monitoring service
            try:
                await monitoring_service.publish_metric_update(
                    "api.performance.slow_requests",
                    1,
                    {"endpoint": endpoint}
                )
            except Exception as e:
                logger.error(f"Error publishing slow request metric: {e}")
        
        # Check for high memory usage
        if memory_used > self.memory_threshold:
            alert_data = {
                "type": "high_memory_usage",
                "endpoint": endpoint,
                "memory_used": memory_used,
                "threshold": self.memory_threshold,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.redis_client.publish("alerts:performance", json.dumps(alert_data))
            
            # Send to monitoring service
            try:
                await monitoring_service.publish_metric_update(
                    "api.performance.high_memory_requests",
                    1,
                    {"endpoint": endpoint}
                )
            except Exception as e:
                logger.error(f"Error publishing memory usage metric: {e}")
    
    async def _log_performance_data(self, request: Request, process_time: float, memory_used: float, error: str = None):
        """Log detailed performance data"""
        performance_data = {
            "endpoint": self._get_endpoint_pattern(request),
            "method": request.method,
            "process_time": process_time,
            "memory_used": memory_used,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if error:
            performance_data["error"] = error
        
        # Store in Redis for analysis
        self.redis_client.lpush("performance:logs", json.dumps(performance_data))
        self.redis_client.ltrim("performance:logs", 0, 9999)  # Keep last 10000 entries
    
    def _get_endpoint_pattern(self, request: Request) -> str:
        """Extract endpoint pattern from request"""
        try:
            if hasattr(request, "scope") and "route" in request.scope:
                route = request.scope["route"]
                if hasattr(route, "path"):
                    return route.path
            
            path = request.url.path
            
            # Replace common ID patterns
            import re
            path = re.sub(r'/\d+', '/{id}', path)
            path = re.sub(r'/[0-9a-f-]{36}', '/{uuid}', path)
            
            return path
            
        except Exception:
            return request.url.path