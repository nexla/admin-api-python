import json
import pickle
import hashlib
import asyncio
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta
from functools import wraps
import redis
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
import logging

from ..database import get_db
from ..models.user import User
from ..models.org import Org

logger = logging.getLogger(__name__)

class CacheManager:
    """Advanced caching manager with multiple cache strategies"""
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        default_ttl: int = 3600,  # 1 hour
        compression_threshold: int = 1024  # Compress data larger than 1KB
    ):
        self.redis_client = redis.from_url(redis_url)
        self.default_ttl = default_ttl
        self.compression_threshold = compression_threshold
        
        # Cache namespaces
        self.namespaces = {
            "user": "cache:user",
            "org": "cache:org", 
            "project": "cache:project",
            "data_source": "cache:data_source",
            "api_response": "cache:api",
            "query": "cache:query",
            "session": "cache:session",
            "metrics": "cache:metrics"
        }
    
    def _generate_cache_key(self, namespace: str, identifier: str, **kwargs) -> str:
        """Generate a standardized cache key"""
        base_key = f"{self.namespaces.get(namespace, f'cache:{namespace}')}:{identifier}"
        
        if kwargs:
            # Sort kwargs for consistent key generation
            sorted_kwargs = sorted(kwargs.items())
            kwargs_str = "&".join([f"{k}={v}" for k, v in sorted_kwargs])
            hash_obj = hashlib.md5(kwargs_str.encode())
            base_key += f":{hash_obj.hexdigest()[:8]}"
        
        return base_key
    
    def _serialize_data(self, data: Any) -> bytes:
        """Serialize data for caching with optional compression"""
        try:
            # Try JSON first for simple data types
            json_data = json.dumps(data, default=str)
            serialized = json_data.encode('utf-8')
            
            # Use compression for large data
            if len(serialized) > self.compression_threshold:
                import gzip
                serialized = gzip.compress(serialized)
                return b"gzip:" + serialized
            
            return b"json:" + serialized
            
        except (TypeError, ValueError):
            # Fall back to pickle for complex objects
            pickled = pickle.dumps(data)
            
            if len(pickled) > self.compression_threshold:
                import gzip
                pickled = gzip.compress(pickled)
                return b"gzip_pickle:" + pickled
            
            return b"pickle:" + pickled
    
    def _deserialize_data(self, data: bytes) -> Any:
        """Deserialize cached data"""
        if data.startswith(b"gzip:"):
            import gzip
            decompressed = gzip.decompress(data[5:])
            return json.loads(decompressed.decode('utf-8'))
        
        elif data.startswith(b"gzip_pickle:"):
            import gzip
            decompressed = gzip.decompress(data[12:])
            return pickle.loads(decompressed)
        
        elif data.startswith(b"json:"):
            return json.loads(data[5:].decode('utf-8'))
        
        elif data.startswith(b"pickle:"):
            return pickle.loads(data[7:])
        
        else:
            # Legacy format - try JSON first, then pickle
            try:
                return json.loads(data.decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return pickle.loads(data)
    
    async def get(self, namespace: str, identifier: str, **kwargs) -> Optional[Any]:
        """Get data from cache"""
        cache_key = self._generate_cache_key(namespace, identifier, **kwargs)
        
        try:
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                return self._deserialize_data(cached_data)
            return None
        except Exception as e:
            logger.error(f"Cache get error for key {cache_key}: {e}")
            return None
    
    async def set(
        self, 
        namespace: str, 
        identifier: str, 
        data: Any, 
        ttl: Optional[int] = None,
        **kwargs
    ) -> bool:
        """Set data in cache"""
        cache_key = self._generate_cache_key(namespace, identifier, **kwargs)
        ttl = ttl or self.default_ttl
        
        try:
            serialized_data = self._serialize_data(data)
            self.redis_client.setex(cache_key, ttl, serialized_data)
            
            # Track cache usage
            self.redis_client.incr(f"cache:stats:sets:{namespace}")
            
            return True
        except Exception as e:
            logger.error(f"Cache set error for key {cache_key}: {e}")
            return False
    
    async def delete(self, namespace: str, identifier: str, **kwargs) -> bool:
        """Delete data from cache"""
        cache_key = self._generate_cache_key(namespace, identifier, **kwargs)
        
        try:
            result = self.redis_client.delete(cache_key)
            
            # Track cache usage
            self.redis_client.incr(f"cache:stats:deletes:{namespace}")
            
            return result > 0
        except Exception as e:
            logger.error(f"Cache delete error for key {cache_key}: {e}")
            return False
    
    async def invalidate_pattern(self, namespace: str, pattern: str = "*") -> int:
        """Invalidate cache entries matching a pattern"""
        cache_pattern = f"{self.namespaces.get(namespace, f'cache:{namespace}')}:{pattern}"
        
        try:
            keys = self.redis_client.keys(cache_pattern)
            if keys:
                result = self.redis_client.delete(*keys)
                
                # Track cache usage
                self.redis_client.incr(f"cache:stats:invalidations:{namespace}", len(keys))
                
                return result
            return 0
        except Exception as e:
            logger.error(f"Cache invalidation error for pattern {cache_pattern}: {e}")
            return 0
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            info = self.redis_client.info('memory')
            
            stats = {
                "memory_usage_bytes": info.get('used_memory', 0),
                "memory_usage_human": info.get('used_memory_human', '0B'),
                "total_connections": self.redis_client.info('clients').get('connected_clients', 0),
                "total_commands_processed": self.redis_client.info('stats').get('total_commands_processed', 0),
                "cache_hit_rate": 0.0,
                "namespace_stats": {}
            }
            
            # Calculate cache hit rates by namespace
            for namespace in self.namespaces.keys():
                hits = int(self.redis_client.get(f"cache:stats:hits:{namespace}") or 0)
                misses = int(self.redis_client.get(f"cache:stats:misses:{namespace}") or 0)
                sets = int(self.redis_client.get(f"cache:stats:sets:{namespace}") or 0)
                deletes = int(self.redis_client.get(f"cache:stats:deletes:{namespace}") or 0)
                
                total_requests = hits + misses
                hit_rate = (hits / total_requests * 100) if total_requests > 0 else 0
                
                stats["namespace_stats"][namespace] = {
                    "hits": hits,
                    "misses": misses,
                    "sets": sets,
                    "deletes": deletes,
                    "hit_rate_percent": round(hit_rate, 2)
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}

# Global cache manager instance
cache_manager = CacheManager()

def cached(
    namespace: str,
    ttl: Optional[int] = None,
    key_generator: Optional[Callable] = None,
    invalidate_on: Optional[List[str]] = None
):
    """Decorator for caching function results"""
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            if key_generator:
                cache_key = key_generator(*args, **kwargs)
            else:
                # Use function name and arguments as key
                func_name = f"{func.__module__}.{func.__name__}"
                args_str = "_".join([str(arg) for arg in args])
                kwargs_str = "_".join([f"{k}_{v}" for k, v in kwargs.items()])
                cache_key = f"{func_name}_{args_str}_{kwargs_str}"
                cache_key = hashlib.md5(cache_key.encode()).hexdigest()
            
            # Try to get from cache
            cached_result = await cache_manager.get(namespace, cache_key)
            if cached_result is not None:
                # Track cache hit
                cache_manager.redis_client.incr(f"cache:stats:hits:{namespace}")
                return cached_result
            
            # Track cache miss
            cache_manager.redis_client.incr(f"cache:stats:misses:{namespace}")
            
            # Execute function
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Cache the result
            await cache_manager.set(namespace, cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator

class QueryCache:
    """Specialized cache for database queries"""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.namespace = "query"
    
    async def get_cached_query(
        self, 
        query_hash: str, 
        params: Dict[str, Any] = None
    ) -> Optional[List[Dict]]:
        """Get cached query result"""
        return await self.cache_manager.get(
            self.namespace, 
            query_hash, 
            **(params or {})
        )
    
    async def cache_query_result(
        self, 
        query_hash: str, 
        result: List[Dict], 
        ttl: int = 1800,  # 30 minutes
        params: Dict[str, Any] = None
    ):
        """Cache query result"""
        await self.cache_manager.set(
            self.namespace, 
            query_hash, 
            result, 
            ttl, 
            **(params or {})
        )
    
    def generate_query_hash(self, sql: str, params: Dict[str, Any] = None) -> str:
        """Generate hash for SQL query and parameters"""
        query_str = sql
        if params:
            sorted_params = sorted(params.items())
            params_str = "&".join([f"{k}={v}" for k, v in sorted_params])
            query_str += f"?{params_str}"
        
        return hashlib.sha256(query_str.encode()).hexdigest()

class SessionCache:
    """Specialized cache for user sessions"""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.namespace = "session"
    
    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data"""
        return await self.cache_manager.get(self.namespace, session_id)
    
    async def set_session(
        self, 
        session_id: str, 
        session_data: Dict[str, Any], 
        ttl: int = 86400  # 24 hours
    ):
        """Set session data"""
        await self.cache_manager.set(self.namespace, session_id, session_data, ttl)
    
    async def delete_session(self, session_id: str):
        """Delete session"""
        await self.cache_manager.delete(self.namespace, session_id)
    
    async def extend_session(self, session_id: str, ttl: int = 86400):
        """Extend session TTL"""
        session_data = await self.get_session(session_id)
        if session_data:
            await self.set_session(session_id, session_data, ttl)

class APIResponseCache:
    """Specialized cache for API responses"""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.namespace = "api_response"
    
    def generate_response_key(
        self, 
        method: str, 
        path: str, 
        params: Dict[str, Any] = None, 
        user_id: Optional[int] = None
    ) -> str:
        """Generate cache key for API response"""
        key_parts = [method.upper(), path]
        
        if params:
            sorted_params = sorted(params.items())
            params_str = "&".join([f"{k}={v}" for k, v in sorted_params])
            key_parts.append(params_str)
        
        if user_id:
            key_parts.append(f"user:{user_id}")
        
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()
    
    async def get_cached_response(
        self, 
        method: str, 
        path: str, 
        params: Dict[str, Any] = None, 
        user_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get cached API response"""
        cache_key = self.generate_response_key(method, path, params, user_id)
        return await self.cache_manager.get(self.namespace, cache_key)
    
    async def cache_response(
        self, 
        method: str, 
        path: str, 
        response_data: Dict[str, Any], 
        ttl: int = 300,  # 5 minutes
        params: Dict[str, Any] = None, 
        user_id: Optional[int] = None
    ):
        """Cache API response"""
        cache_key = self.generate_response_key(method, path, params, user_id)
        await self.cache_manager.set(self.namespace, cache_key, response_data, ttl)

class PerformanceOptimizer:
    """Performance optimization utilities"""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.query_cache = QueryCache(cache_manager)
    
    async def optimize_user_queries(self, db: Session, user_id: int) -> Dict[str, Any]:
        """Optimize frequently used user queries"""
        cache_key = f"user_data:{user_id}"
        
        # Check cache first
        cached_data = await self.cache_manager.get("user", cache_key)
        if cached_data:
            return cached_data
        
        # Fetch and aggregate user data
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return {}
        
        user_data = {
            "user": {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "status": user.status,
                "is_active": user.active_(),
                "default_org_id": user.default_org_id
            },
            "org": None,
            "permissions": [],
            "recent_activity": []
        }
        
        # Add org data if available
        if user.default_org:
            user_data["org"] = {
                "id": user.default_org.id,
                "name": user.default_org.name,
                "status": user.default_org.status,
                "is_active": user.default_org.active_()
            }
        
        # Cache the aggregated data
        await self.cache_manager.set("user", cache_key, user_data, ttl=1800)  # 30 minutes
        
        return user_data
    
    async def preload_org_data(self, db: Session, org_id: int):
        """Preload commonly accessed org data"""
        cache_key = f"org_summary:{org_id}"
        
        org = db.query(Org).filter(Org.id == org_id).first()
        if not org:
            return
        
        # Aggregate org summary data
        org_summary = {
            "org": {
                "id": org.id,
                "name": org.name,
                "status": org.status,
                "is_active": org.active_(),
                "member_count": len(org.member_users),
                "project_count": len(org.projects) if org.projects else 0
            },
            "stats": {
                "data_sources": len(org.data_sources) if org.data_sources else 0,
                "data_sets": len(org.data_sets) if org.data_sets else 0,
                "flows": len(org.flows) if org.flows else 0
            }
        }
        
        await self.cache_manager.set("org", cache_key, org_summary, ttl=3600)  # 1 hour
    
    async def bulk_cache_invalidation(
        self, 
        entity_type: str, 
        entity_ids: List[int],
        related_entities: List[str] = None
    ):
        """Perform bulk cache invalidation for related entities"""
        # Invalidate primary entity caches
        for entity_id in entity_ids:
            await self.cache_manager.invalidate_pattern(entity_type, f"*{entity_id}*")
        
        # Invalidate related entity caches
        if related_entities:
            for related_type in related_entities:
                for entity_id in entity_ids:
                    await self.cache_manager.invalidate_pattern(related_type, f"*{entity_id}*")

# Global instances
query_cache = QueryCache(cache_manager)
session_cache = SessionCache(cache_manager)
api_response_cache = APIResponseCache(cache_manager)
performance_optimizer = PerformanceOptimizer(cache_manager)