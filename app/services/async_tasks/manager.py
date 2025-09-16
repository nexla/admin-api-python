"""
Async Task Manager - Handles background task lifecycle and management.
Provides task execution, monitoring, and cleanup capabilities.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_

from ...database import SessionLocal
from ...models.async_task import AsyncTask, TaskStatus
from ...models.user import User
from ...models.org import Org
from ..storage.s3_service import S3Service

logger = logging.getLogger(__name__)


class AsyncTaskManager:
    """Manager for async task lifecycle and cleanup operations"""
    
    TASK_EXPIRY_DAYS = 5  # Tasks older than 5 days get their results purged
    
    @staticmethod
    def purge_expired_results():
        """
        Purge expired task results from storage and update database.
        Removes files from S3 and clears result URLs for completed tasks older than expiry period.
        """
        try:
            db = SessionLocal()
            
            # Calculate expiry cutoff
            expiry_cutoff = datetime.utcnow() - timedelta(days=AsyncTaskManager.TASK_EXPIRY_DAYS)
            
            # Find completed tasks with results that are expired
            expired_tasks = db.query(AsyncTask).filter(
                and_(
                    AsyncTask.status == TaskStatus.COMPLETED,
                    AsyncTask.stopped_at < expiry_cutoff,
                    AsyncTask.result_url.isnot(None),
                    AsyncTask.result_purged == False
                )
            ).all()
            
            s3_service = S3Service()
            
            for task in expired_tasks:
                try:
                    # Extract S3 details from result
                    if task.result and isinstance(task.result, dict):
                        storage_type = task.result.get('storage')
                        bucket = task.result.get('bucket')
                        file_key = task.result.get('file_key')
                        
                        # Delete file from S3 if it's S3 storage
                        if storage_type == 's3' and bucket and file_key:
                            s3_service.delete_file(bucket, file_key)
                            logger.info(f"Deleted expired task result file: s3://{bucket}/{file_key}")
                    
                    # Update task to mark result as purged
                    task.result_purged = True
                    task.result_url = None
                    task.updated_at = datetime.utcnow()
                    
                except Exception as e:
                    logger.error(f"Failed to purge result for task {task.id}: {str(e)}")
                    continue
            
            # Commit all changes
            db.commit()
            
            logger.info(f"Purged results for {len(expired_tasks)} expired tasks")
            
        except Exception as e:
            logger.error(f"Failed to purge expired task results: {str(e)}")
            db.rollback()
        finally:
            db.close()
    
    @staticmethod
    def get_task_status(db: Session, task_id: int, user_id: int) -> Optional[AsyncTask]:
        """
        Get task status with permission check.
        
        Args:
            db: Database session
            task_id: ID of the task
            user_id: ID of the requesting user
            
        Returns:
            AsyncTask if user has permission, None otherwise
        """
        try:
            task = db.query(AsyncTask).filter(
                and_(
                    AsyncTask.id == task_id,
                    AsyncTask.owner_id == user_id
                )
            ).first()
            
            return task
            
        except Exception as e:
            logger.error(f"Failed to get task status: {str(e)}")
            return None
    
    @staticmethod
    def cancel_task(db: Session, task_id: int, user_id: int) -> bool:
        """
        Cancel a running task if user has permission.
        
        Args:
            db: Database session
            task_id: ID of the task to cancel
            user_id: ID of the requesting user
            
        Returns:
            True if task was cancelled, False otherwise
        """
        try:
            task = db.query(AsyncTask).filter(
                and_(
                    AsyncTask.id == task_id,
                    AsyncTask.owner_id == user_id,
                    AsyncTask.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING])
                )
            ).first()
            
            if not task:
                return False
            
            task.status = TaskStatus.CANCELLED
            task.stopped_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            
            db.commit()
            
            logger.info(f"Task {task_id} cancelled by user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel task {task_id}: {str(e)}")
            db.rollback()
            return False
    
    @staticmethod
    def get_user_tasks(
        db: Session,
        user_id: int,
        status: Optional[TaskStatus] = None,
        task_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[AsyncTask]:
        """
        Get tasks for a specific user with optional filtering.
        
        Args:
            db: Database session
            user_id: ID of the user
            status: Optional status filter
            task_type: Optional task type filter
            limit: Maximum number of tasks to return
            offset: Number of tasks to skip
            
        Returns:
            List of tasks
        """
        try:
            query = db.query(AsyncTask).filter(AsyncTask.owner_id == user_id)
            
            if status:
                query = query.filter(AsyncTask.status == status)
            
            if task_type:
                query = query.filter(AsyncTask.task_type == task_type)
            
            return query.order_by(AsyncTask.created_at.desc()).offset(offset).limit(limit).all()
            
        except Exception as e:
            logger.error(f"Failed to get user tasks: {str(e)}")
            return []
    
    @staticmethod
    def cleanup_old_tasks():
        """
        Clean up very old task records to prevent database bloat.
        Removes task records older than 30 days.
        """
        try:
            db = SessionLocal()
            
            # Calculate cleanup cutoff (30 days)
            cleanup_cutoff = datetime.utcnow() - timedelta(days=30)
            
            # Delete old completed or failed tasks
            deleted_count = db.query(AsyncTask).filter(
                and_(
                    AsyncTask.created_at < cleanup_cutoff,
                    AsyncTask.status.in_([
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED
                    ])
                )
            ).delete()
            
            db.commit()
            
            logger.info(f"Cleaned up {deleted_count} old task records")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old tasks: {str(e)}")
            db.rollback()
        finally:
            db.close()


class TaskRegistry:
    """Registry for available async task types"""
    
    # Task type mappings
    TASK_CLASSES = {
        'GetAuditLogs': 'async_tasks.tasks.get_audit_logs.GetAuditLogsTask',
        'DeactivateUser': 'async_tasks.tasks.deactivate_user.DeactivateUserTask',
        'BulkPauseFlows': 'async_tasks.tasks.bulk_pause_flows.BulkPauseFlowsTask',
        'BulkDeleteNotifications': 'async_tasks.tasks.bulk_delete_notifications.BulkDeleteNotificationsTask',
        'ChownUserResources': 'async_tasks.tasks.chown_user_resources.ChownUserResourcesTask',
        'BulkMarkAsReadNotifications': 'async_tasks.tasks.bulk_mark_as_read_notifications.BulkMarkAsReadNotificationsTask',
        'CallProbe': 'async_tasks.tasks.call_probe.CallProbeTask',
    }
    
    @classmethod
    def get_task_class(cls, task_type: str):
        """Get the task class for a given task type"""
        if task_type not in cls.TASK_CLASSES:
            raise ValueError(f"Unknown task type: {task_type}")
        
        # Dynamic import of task class
        module_path, class_name = cls.TASK_CLASSES[task_type].rsplit('.', 1)
        module = __import__(f"app.services.{module_path}", fromlist=[class_name])
        return getattr(module, class_name)
    
    @classmethod
    def execute_task(cls, task: AsyncTask):
        """Execute a task based on its type"""
        try:
            task_class = cls.get_task_class(task.task_type)
            task_instance = task_class(task)
            
            # Check preconditions
            task_instance.check_preconditions()
            
            # Execute the task
            task_instance.perform()
            
            logger.info(f"Task {task.id} ({task.task_type}) completed successfully")
            
        except Exception as e:
            logger.error(f"Task {task.id} ({task.task_type}) failed: {str(e)}")
            
            # Update task status to failed
            db = SessionLocal()
            try:
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
                task.stopped_at = datetime.utcnow()
                task.updated_at = datetime.utcnow()
                db.merge(task)
                db.commit()
            except:
                db.rollback()
            finally:
                db.close()
            
            raise


class BaseAsyncTask:
    """Base class for all async tasks"""
    
    def __init__(self, task: AsyncTask):
        self.task = task
        self.arguments = task.arguments or {}
        self.db = SessionLocal()
    
    def __del__(self):
        """Cleanup database connection"""
        if hasattr(self, 'db'):
            self.db.close()
    
    def check_preconditions(self):
        """Check if task can be executed. Override in subclasses."""
        pass
    
    def perform(self):
        """Execute the task. Must be overridden in subclasses."""
        raise NotImplementedError("Task classes must implement perform() method")
    
    def update_progress(self, progress: int, message: Optional[str] = None):
        """Update task progress"""
        try:
            self.task.progress = progress
            if message:
                self.task.progress_message = message
            self.task.updated_at = datetime.utcnow()
            self.db.merge(self.task)
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to update task progress: {str(e)}")
    
    def set_result(self, result: Dict[str, Any], result_url: Optional[str] = None):
        """Set task result and mark as completed"""
        try:
            self.task.result = result
            self.task.result_url = result_url
            self.task.status = TaskStatus.COMPLETED
            self.task.stopped_at = datetime.utcnow()
            self.task.updated_at = datetime.utcnow()
            self.db.merge(self.task)
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to set task result: {str(e)}")
            raise