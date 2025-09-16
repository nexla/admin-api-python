import json
import time
import hashlib
from typing import Callable, List, Optional, Dict, Any
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import logging

from ..services.caching_service import api_response_cache, cache_manager

logger = logging.getLogger(__name__)

class ResponseCachingMiddleware(BaseHTTPMiddleware):
    """Middleware for caching API responses"""
    
    def __init__(
        self,
        app,
        cacheable_methods: List[str] = None,
        cacheable_paths: List[str] = None,
        default_ttl: int = 300,  # 5 minutes
        cache_private_responses: bool = False,
        exclude_patterns: List[str] = None
    ):
        super().__init__(app)
        self.cacheable_methods = cacheable_methods or ["GET"]
        self.cacheable_paths = cacheable_paths or ["/api/v1/"]
        self.default_ttl = default_ttl
        self.cache_private_responses = cache_private_responses
        self.exclude_patterns = exclude_patterns or [
            "/auth/", "/login", "/logout", "/admin/", "/docs", "/redoc"
        ]
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Check if request is cacheable
        if not self._is_cacheable_request(request):
            return await call_next(request)
        
        # Generate cache key
        cache_key_data = await self._generate_cache_key_data(request)
        
        # Check cache for existing response
        cached_response = await api_response_cache.get_cached_response(
            cache_key_data["method"],
            cache_key_data["path"],
            cache_key_data["params"],
            cache_key_data.get("user_id")
        )
        
        if cached_response:
            # Return cached response
            response = JSONResponse(
                content=cached_response["content"],
                status_code=cached_response["status_code"],
                headers=cached_response.get("headers", {})
            )
            
            # Add cache headers
            response.headers["X-Cache"] = "HIT"
            response.headers["X-Cache-Key"] = cache_key_data["cache_key"]
            
            return response
        
        # Process request
        response = await call_next(request)
        
        # Cache response if appropriate
        if self._is_cacheable_response(request, response):
            await self._cache_response(request, response, cache_key_data)
        
        # Add cache miss header
        response.headers["X-Cache"] = "MISS"
        
        return response
    
    def _is_cacheable_request(self, request: Request) -> bool:
        """Check if request is cacheable"""
        # Check HTTP method
        if request.method not in self.cacheable_methods:
            return False
        
        # Check if path should be excluded
        path = request.url.path
        for exclude_pattern in self.exclude_patterns:
            if exclude_pattern in path:
                return False
        
        # Check if path is in cacheable paths
        for cacheable_path in self.cacheable_paths:
            if path.startswith(cacheable_path):
                return True
        
        return False
    
    def _is_cacheable_response(self, request: Request, response: Response) -> bool:
        """Check if response is cacheable"""
        # Only cache successful responses
        if response.status_code >= 400:
            return False
        
        # Check for cache control headers
        if hasattr(response, "headers"):
            cache_control = response.headers.get("Cache-Control", "")
            if "no-cache" in cache_control or "no-store" in cache_control:
                return False
        
        # Check for private responses
        if not self.cache_private_responses:
            if "authorization" in request.headers:
                return False
        
        return True
    
    async def _generate_cache_key_data(self, request: Request) -> Dict[str, Any]:
        """Generate cache key data for request"""
        method = request.method
        path = request.url.path
        
        # Extract query parameters
        params = dict(request.query_params)
        
        # Extract user ID from request if available
        user_id = None
        if hasattr(request.state, "user") and request.state.user:
            user_id = getattr(request.state.user, "id", None)
        
        # Generate cache key
        cache_key = api_response_cache.generate_response_key(
            method, path, params, user_id
        )
        
        return {
            "method": method,
            "path": path,
            "params": params,
            "user_id": user_id,
            "cache_key": cache_key
        }
    
    async def _cache_response(
        self, 
        request: Request, 
        response: Response, 
        cache_key_data: Dict[str, Any]
    ):
        """Cache the response"""
        try:
            # Extract response content
            if hasattr(response, "body"):
                content = json.loads(response.body.decode())
            else:
                content = {}
            
            # Prepare cache data
            cache_data = {
                "content": content,
                "status_code": response.status_code,
                "headers": dict(response.headers) if hasattr(response, "headers") else {}
            }
            
            # Determine TTL based on endpoint
            ttl = self._get_ttl_for_endpoint(request.url.path)
            
            # Cache the response
            await api_response_cache.cache_response(
                cache_key_data["method"],
                cache_key_data["path"],
                cache_data,
                ttl,
                cache_key_data["params"],
                cache_key_data.get("user_id")
            )
            
        except Exception as e:
            logger.error(f"Error caching response: {e}")
    
    def _get_ttl_for_endpoint(self, path: str) -> int:
        """Get TTL based on endpoint characteristics"""
        # Static/reference data - longer cache
        if any(pattern in path for pattern in ["/clusters", "/tiers", "/vendors"]):
            return 3600  # 1 hour
        
        # User/org data - medium cache
        elif any(pattern in path for pattern in ["/users", "/organizations"]):
            return 900  # 15 minutes
        
        # Dynamic data - short cache
        elif any(pattern in path for pattern in ["/flows", "/metrics", "/alerts"]):
            return 180  # 3 minutes
        
        # Default TTL
        return self.default_ttl

class ETagMiddleware(BaseHTTPMiddleware):
    """ETag middleware for conditional requests"""
    
    def __init__(self, app):
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Process request
        response = await call_next(request)
        
        # Generate ETag for cacheable responses
        if self._should_add_etag(request, response):
            etag = await self._generate_etag(response)
            response.headers["ETag"] = f'"{etag}"'
            
            # Check if client has matching ETag
            if_none_match = request.headers.get("If-None-Match")
            if if_none_match and f'"{etag}"' in if_none_match:
                # Return 304 Not Modified
                return Response(status_code=304, headers={"ETag": f'"{etag}"'})
        
        return response
    
    def _should_add_etag(self, request: Request, response: Response) -> bool:
        """Check if ETag should be added"""
        # Only for GET requests
        if request.method != "GET":
            return False
        
        # Only for successful responses
        if response.status_code >= 400:
            return False
        
        # Skip if ETag already exists
        if "ETag" in response.headers:
            return False
        
        return True
    
    async def _generate_etag(self, response: Response) -> str:
        """Generate ETag from response content"""
        try:
            if hasattr(response, "body"):
                content = response.body
            else:
                content = b""
            
            # Generate hash of content
            return hashlib.md5(content).hexdigest()
            
        except Exception as e:
            logger.error(f"Error generating ETag: {e}")
            return str(time.time())

class CompressionMiddleware(BaseHTTPMiddleware):
    """Compression middleware for large responses"""
    
    def __init__(
        self,
        app,
        minimum_size: int = 1024,  # Compress responses larger than 1KB
        compression_level: int = 6
    ):
        super().__init__(app)
        self.minimum_size = minimum_size
        self.compression_level = compression_level
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # Check if compression is supported
        accept_encoding = request.headers.get("Accept-Encoding", "")
        if "gzip" not in accept_encoding:
            return response
        
        # Check if response should be compressed
        if not self._should_compress(response):
            return response
        
        # Compress response
        compressed_response = await self._compress_response(response)
        
        return compressed_response
    
    def _should_compress(self, response: Response) -> bool:
        """Check if response should be compressed"""
        # Check content type
        content_type = response.headers.get("Content-Type", "")
        compressible_types = [
            "application/json",
            "application/xml",
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript"
        ]
        
        if not any(ct in content_type for ct in compressible_types):
            return False
        
        # Check content length
        if hasattr(response, "body") and len(response.body) < self.minimum_size:
            return False
        
        # Skip if already compressed
        if "Content-Encoding" in response.headers:
            return False
        
        return True
    
    async def _compress_response(self, response: Response) -> Response:
        """Compress response content"""
        try:
            import gzip
            
            if hasattr(response, "body"):
                # Compress the content
                compressed_content = gzip.compress(
                    response.body, 
                    compresslevel=self.compression_level
                )
                
                # Create new response with compressed content
                new_response = Response(
                    content=compressed_content,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type
                )
                
                # Update headers
                new_response.headers["Content-Encoding"] = "gzip"
                new_response.headers["Content-Length"] = str(len(compressed_content))
                
                return new_response
            
            return response
            
        except Exception as e:
            logger.error(f"Error compressing response: {e}")
            return response

class CacheInvalidationMiddleware(BaseHTTPMiddleware):
    """Middleware for automatic cache invalidation"""
    
    def __init__(
        self,
        app,
        invalidation_rules: Dict[str, List[str]] = None
    ):
        super().__init__(app)
        self.invalidation_rules = invalidation_rules or {
            "POST": ["user", "org", "project"],
            "PUT": ["user", "org", "project"], 
            "PATCH": ["user", "org", "project"],
            "DELETE": ["user", "org", "project"]
        }
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # Invalidate caches for modifying operations
        if response.status_code < 400:  # Only for successful operations
            await self._invalidate_related_caches(request, response)
        
        return response
    
    async def _invalidate_related_caches(self, request: Request, response: Response):
        """Invalidate related caches based on request"""
        method = request.method
        path = request.url.path
        
        if method not in self.invalidation_rules:
            return
        
        # Extract entity information from path
        entity_info = self._extract_entity_info(path)
        
        # Invalidate relevant cache namespaces
        for namespace in self.invalidation_rules[method]:
            if entity_info.get("entity_type") == namespace:
                # Invalidate specific entity
                if entity_info.get("entity_id"):
                    await cache_manager.invalidate_pattern(
                        namespace, 
                        f"*{entity_info['entity_id']}*"
                    )
                else:
                    # Invalidate all entities of this type
                    await cache_manager.invalidate_pattern(namespace, "*")
            
            # Always invalidate API response cache for this namespace
            await cache_manager.invalidate_pattern("api_response", f"*{namespace}*")
    
    def _extract_entity_info(self, path: str) -> Dict[str, Any]:
        """Extract entity information from URL path"""
        parts = path.strip("/").split("/")
        
        entity_info = {}
        
        # Look for entity type and ID patterns
        for i, part in enumerate(parts):
            if part in ["users", "organizations", "projects", "flows", "data-sources"]:
                entity_info["entity_type"] = part.rstrip("s")  # Remove plural
                
                # Check if next part is an ID
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    entity_info["entity_id"] = parts[i + 1]
                
                break
        
        return entity_info