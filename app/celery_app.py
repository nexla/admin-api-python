"""
Celery configuration for background jobs.
This replaces Rails Sidekiq functionality with pure Python.
"""

from celery import Celery
from .config import settings
import os

# Create Celery app
celery_app = Celery(
    "admin_api",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        'app.tasks.email_tasks',
        'app.tasks.data_processing_tasks',
        'app.tasks.notification_tasks',
        'app.tasks.cleanup_tasks',
        'app.tasks.flow_tasks',
    ]
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task routing
    task_routes={
        'app.tasks.email_tasks.*': {'queue': 'email'},
        'app.tasks.data_processing_tasks.*': {'queue': 'data_processing'},
        'app.tasks.notification_tasks.*': {'queue': 'notifications'},
        'app.tasks.cleanup_tasks.*': {'queue': 'cleanup'},
        'app.tasks.flow_tasks.*': {'queue': 'flows'},
    },
    
    # Task execution
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    
    # Retry policy
    task_default_retry_delay=60,  # 1 minute
    task_max_retries=3,
    
    # Result backend settings
    result_expires=3600,  # 1 hour
    
    # Beat schedule (replaces Rails whenever gem)
    beat_schedule={
        'cleanup-expired-tokens': {
            'task': 'app.tasks.cleanup_tasks.cleanup_expired_tokens',
            'schedule': 3600.0,  # Every hour
        },
        'send-daily-reports': {
            'task': 'app.tasks.email_tasks.send_daily_reports',
            'schedule': 86400.0,  # Every 24 hours
        },
        'update-elasticsearch-index': {
            'task': 'app.tasks.data_processing_tasks.update_search_index',
            'schedule': 1800.0,  # Every 30 minutes
        },
        'check-scheduled-flows': {
            'task': 'flow.schedule_check',
            'schedule': 300.0,  # Every 5 minutes
        },
        'cleanup-old-flow-runs': {
            'task': 'flow.cleanup_old_runs',
            'schedule': 86400.0,  # Every 24 hours
        },
        'validate-flows': {
            'task': 'flow.validate_all',
            'schedule': 3600.0,  # Every hour
        },
    },
)