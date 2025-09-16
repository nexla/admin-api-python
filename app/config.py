"""
Configuration settings for the Python/FastAPI application.
This replaces Rails configuration with pure Python settings.
"""

try:
    from pydantic_settings import BaseSettings
except ImportError:
    from pydantic import BaseSettings
from typing import List, Optional
import os

class Settings(BaseSettings):
    # Application
    APP_NAME: str = "Admin API"
    APP_VERSION: str = "2.0.0"
    DEBUG: bool = True
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    ENVIRONMENT: str = "development"
    
    # Database (replaces Rails database.yml)
    DATABASE_URL: str = "mysql+pymysql://root:nexla123@localhost:3306/nexla_admin_dev"
    DATABASE_POOL_SIZE: int = 10
    DATABASE_MAX_OVERFLOW: int = 20
    DATABASE_POOL_TIMEOUT: int = 30
    DATABASE_POOL_RECYCLE: int = 3600
    
    # Security (replaces Rails secrets)
    SECRET_KEY: str = "bd4d7cdcc633749f38ce03b39b9ec648bca8ff2844327e9fe8a68a2fbf5f7db119fe09d8273a249639ea63b1ada22e42dc705be571a1454c4bd8bbea54eac318"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30
    PASSWORD_RESET_TOKEN_EXPIRE_HOURS: int = 24
    
    # CORS (replaces Rails CORS configuration)
    ALLOWED_ORIGINS: List[str] = [
        "http://localhost:3000", 
        "http://localhost:8080",
        "http://127.0.0.1:3000", 
        "http://127.0.0.1:8080"
    ]
    ALLOWED_METHODS: List[str] = ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
    ALLOWED_HEADERS: List[str] = ["*"]
    
    # Redis (replaces Rails Redis configuration)
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CACHE_DB: int = 1
    REDIS_SESSION_DB: int = 2
    
    # Elasticsearch (replaces Rails Elasticsearch configuration)
    ELASTICSEARCH_URL: str = "http://localhost:9200"
    ELASTICSEARCH_INDEX_PREFIX: str = "admin_api"
    
    # Email (replaces Rails ActionMailer)
    SMTP_HOST: str = "localhost"
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM_EMAIL: str = "noreply@nexla.com"
    SMTP_USE_TLS: bool = True
    
    # File Storage (replaces Rails ActiveStorage)
    FILE_STORAGE_TYPE: str = "local"  # local, s3, gcs
    FILE_UPLOAD_MAX_SIZE: int = 100 * 1024 * 1024  # 100MB
    FILE_UPLOAD_ALLOWED_TYPES: List[str] = [
        "image/jpeg", "image/png", "image/gif",
        "application/pdf", "text/csv", 
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ]
    
    # AWS S3 (if using S3 storage)
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: str = "us-east-1"
    AWS_S3_BUCKET: Optional[str] = None
    
    # Rate Limiting (replaces Rails Rack::Attack)
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_REQUESTS: int = 100
    RATE_LIMIT_WINDOW: int = 300  # 5 minutes
    
    # Logging (replaces Rails logging)
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    LOG_FILE: Optional[str] = None
    
    # Monitoring
    SENTRY_DSN: Optional[str] = None
    METRICS_ENABLED: bool = True
    HEALTH_CHECK_ENABLED: bool = True
    
    # Background Jobs (Celery configuration)
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    CELERY_TASK_SERIALIZER: str = "json"
    CELERY_ACCEPT_CONTENT: List[str] = ["json"]
    CELERY_TIMEZONE: str = "UTC"
    
    # Frontend Integration
    FRONTEND_URL: str = "http://localhost:3000"
    API_BASE_URL: str = "http://localhost:8000"
    
    # Feature Flags
    ENABLE_REGISTRATION: bool = True
    ENABLE_EMAIL_VERIFICATION: bool = True
    ENABLE_TWO_FACTOR_AUTH: bool = False
    ENABLE_API_RATE_LIMITING: bool = True
    ENABLE_AUDIT_LOGGING: bool = True
    
    # Pagination
    DEFAULT_PAGE_SIZE: int = 20
    MAX_PAGE_SIZE: int = 100
    
    # Cache Settings
    CACHE_TTL_DEFAULT: int = 300  # 5 minutes
    CACHE_TTL_USER_SESSION: int = 3600  # 1 hour
    CACHE_TTL_SEARCH_RESULTS: int = 900  # 15 minutes
    
    # API Versioning
    API_VERSION: str = "v1"
    API_PREFIX: str = "/api"
    
    # Development Settings
    RELOAD_ON_CHANGE: bool = True
    SHOW_DOCS: bool = True
    ENABLE_PROFILING: bool = False
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = True
        extra = "allow"  # Allow additional environment variables

# Create global settings instance
settings = Settings()

# Environment-specific overrides
if settings.ENVIRONMENT == "production":
    settings.DEBUG = False
    settings.SHOW_DOCS = False
    settings.LOG_LEVEL = "WARNING"
    settings.RELOAD_ON_CHANGE = False
elif settings.ENVIRONMENT == "test":
    settings.DATABASE_URL = settings.DATABASE_URL.replace("nexla_admin_dev", "nexla_admin_test")
    settings.REDIS_URL = "redis://localhost:6379/15"  # Use different Redis DB for tests