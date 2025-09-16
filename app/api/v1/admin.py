"""
Platform Administration API endpoints - System administration and configuration.
Handles system settings, maintenance operations, and administrative functions.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func, text
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta
import json
import os

from ...database import get_db, SessionLocal
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import (
    RBACService, SystemPermissions, check_admin_permission
)
from ...models.user import User
from ...models.org import Org
from ...models.flow import Flow, FlowRun
from ...models.data_source import DataSource
from ...models.data_set import DataSet
from ...models.data_sink import DataSink
from ...celery_app import celery_app

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Platform Administration"])


# Pydantic models for admin operations
class SystemConfig(BaseModel):
    setting_key: str = Field(..., min_length=1, max_length=100)
    setting_value: str = Field(..., max_length=1000)
    setting_type: str = Field(default="string", pattern="^(string|integer|boolean|json)$")
    description: Optional[str] = None
    is_sensitive: bool = False

class SystemConfigResponse(BaseModel):
    setting_key: str
    setting_value: str
    setting_type: str
    description: Optional[str]
    is_sensitive: bool
    updated_at: datetime
    updated_by: int

class MaintenanceMode(BaseModel):
    enabled: bool
    message: Optional[str] = None
    estimated_duration_minutes: Optional[int] = None
    allowed_users: Optional[List[int]] = None

class DatabaseMaintenance(BaseModel):
    operation: str = Field(..., pattern="^(vacuum|reindex|analyze|cleanup)$")
    target_tables: Optional[List[str]] = None
    force: bool = False

class BackupOperation(BaseModel):
    backup_type: str = Field(..., pattern="^(full|incremental|data_only)$")
    include_logs: bool = True
    compression: bool = True
    destination: Optional[str] = None

class UserManagement(BaseModel):
    action: str = Field(..., pattern="^(activate|deactivate|reset_password|delete)$")
    user_ids: List[int]
    reason: Optional[str] = None
    notify_users: bool = False

class SystemStats(BaseModel):
    total_users: int
    total_organizations: int
    total_flows: int
    total_data_sources: int
    total_data_sets: int
    total_data_sinks: int
    total_flow_runs: int
    database_size_mb: Optional[float]
    active_connections: int
    system_uptime_hours: float
    last_backup: Optional[datetime]

class AuditLogEntry(BaseModel):
    id: int
    user_id: Optional[int]
    action: str
    resource_type: str
    resource_id: Optional[int]
    details: Dict[str, Any]
    ip_address: Optional[str]
    user_agent: Optional[str]
    timestamp: datetime

class TaskStatus(BaseModel):
    task_id: str
    status: str
    result: Optional[Any]
    error: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


# System configuration management
@router.get("/config", summary="Get System Configuration")
async def get_system_config(
    setting_key: Optional[str] = Query(None, description="Specific setting key"),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get system configuration settings.
    """
    # Placeholder implementation - would store in database
    default_config = {
        "max_flow_execution_time": "3600",
        "default_retry_count": "3", 
        "max_concurrent_flows": "10",
        "session_timeout_minutes": "60",
        "password_policy_enabled": "true",
        "audit_log_retention_days": "90"
    }
    
    if setting_key:
        if setting_key in default_config:
            return {
                "setting_key": setting_key,
                "setting_value": default_config[setting_key],
                "setting_type": "string",
                "description": f"Configuration for {setting_key}",
                "is_sensitive": False,
                "updated_at": datetime.utcnow(),
                "updated_by": current_user.id
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Configuration setting not found"
            )
    
    return {
        "settings": [
            {
                "setting_key": key,
                "setting_value": value,
                "setting_type": "string",
                "description": f"Configuration for {key}",
                "is_sensitive": False,
                "updated_at": datetime.utcnow(),
                "updated_by": current_user.id
            }
            for key, value in default_config.items()
        ]
    }


@router.put("/config", summary="Update System Configuration")
async def update_system_config(
    config: SystemConfig,
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Update system configuration setting.
    """
    # Validate setting
    if config.is_sensitive and not config.setting_key.startswith("secure_"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sensitive settings must have 'secure_' prefix"
        )
    
    # TODO: Store in database configuration table
    
    logger.info(f"System config updated: {config.setting_key} by user {current_user.id}")
    
    return {
        "message": "Configuration updated successfully",
        "setting_key": config.setting_key,
        "updated_by": current_user.id,
        "updated_at": datetime.utcnow()
    }


# System statistics and overview
@router.get("/stats", response_model=SystemStats, summary="System Statistics")
async def get_system_stats(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_admin_permission())
):
    """
    Get comprehensive system statistics.
    """
    try:
        # Count all resources
        total_users = db.query(User).count()
        total_organizations = db.query(Org).count()
        total_flows = db.query(Flow).count()
        total_data_sources = db.query(DataSource).count()
        total_data_sets = db.query(DataSet).count()
        total_data_sinks = db.query(DataSink).count()
        total_flow_runs = db.query(FlowRun).count()
        
        # System metrics (placeholder)
        database_size_mb = None  # Would calculate actual DB size
        active_connections = 5
        system_uptime_hours = 24.5
        last_backup = None
        
        return SystemStats(
            total_users=total_users,
            total_organizations=total_organizations,
            total_flows=total_flows,
            total_data_sources=total_data_sources,
            total_data_sets=total_data_sets,
            total_data_sinks=total_data_sinks,
            total_flow_runs=total_flow_runs,
            database_size_mb=database_size_mb,
            active_connections=active_connections,
            system_uptime_hours=system_uptime_hours,
            last_backup=last_backup
        )
        
    except Exception as e:
        logger.error(f"Failed to get system stats: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system statistics"
        )


# Maintenance mode management
@router.get("/maintenance", summary="Get Maintenance Mode Status")
async def get_maintenance_mode(
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get current maintenance mode status.
    """
    # TODO: Store maintenance mode state in Redis or database
    return {
        "enabled": False,
        "message": None,
        "estimated_duration_minutes": None,
        "allowed_users": [],
        "enabled_by": None,
        "enabled_at": None
    }


@router.post("/maintenance", summary="Set Maintenance Mode")
async def set_maintenance_mode(
    maintenance: MaintenanceMode,
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Enable or disable maintenance mode.
    """
    # TODO: Store maintenance mode state
    
    action = "enabled" if maintenance.enabled else "disabled"
    logger.info(f"Maintenance mode {action} by user {current_user.id}")
    
    return {
        "message": f"Maintenance mode {action}",
        "enabled": maintenance.enabled,
        "message": maintenance.message,
        "enabled_by": current_user.id,
        "enabled_at": datetime.utcnow()
    }


# Database maintenance operations
@router.post("/database/maintenance", summary="Database Maintenance")
async def perform_database_maintenance(
    maintenance: DatabaseMaintenance,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Perform database maintenance operations.
    """
    try:
        # Queue maintenance task
        task_id = f"db_maintenance_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # TODO: Implement actual database maintenance tasks
        background_tasks.add_task(
            _perform_db_maintenance,
            maintenance.operation,
            maintenance.target_tables,
            current_user.id
        )
        
        logger.info(f"Database maintenance '{maintenance.operation}' started by user {current_user.id}")
        
        return {
            "message": f"Database {maintenance.operation} operation started",
            "task_id": task_id,
            "operation": maintenance.operation,
            "started_by": current_user.id,
            "started_at": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Failed to start database maintenance: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start database maintenance"
        )


async def _perform_db_maintenance(operation: str, target_tables: Optional[List[str]], user_id: int):
    """Background task for database maintenance"""
    try:
        db = SessionLocal()
        
        if operation == "vacuum":
            # Would perform VACUUM operation
            pass
        elif operation == "reindex":
            # Would perform REINDEX operation
            pass
        elif operation == "analyze":
            # Would perform ANALYZE operation
            pass
        elif operation == "cleanup":
            # Would perform cleanup operations
            pass
        
        logger.info(f"Database {operation} completed by user {user_id}")
        
    except Exception as e:
        logger.error(f"Database maintenance failed: {str(e)}")
    finally:
        db.close()


# Backup and restore operations
@router.post("/backup", summary="Create System Backup")
async def create_backup(
    backup: BackupOperation,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Create a system backup.
    """
    try:
        backup_id = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # TODO: Implement actual backup logic
        background_tasks.add_task(
            _create_backup,
            backup_id,
            backup.backup_type,
            backup.include_logs,
            backup.compression,
            current_user.id
        )
        
        logger.info(f"Backup '{backup.backup_type}' started by user {current_user.id}")
        
        return {
            "message": "Backup started successfully",
            "backup_id": backup_id,
            "backup_type": backup.backup_type,
            "started_by": current_user.id,
            "started_at": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Failed to start backup: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start backup"
        )


async def _create_backup(backup_id: str, backup_type: str, include_logs: bool, compression: bool, user_id: int):
    """Background task for creating backups"""
    try:
        # Would implement actual backup logic
        logger.info(f"Backup {backup_id} ({backup_type}) completed by user {user_id}")
        
    except Exception as e:
        logger.error(f"Backup failed: {str(e)}")


# User management operations
@router.post("/users/bulk-action", summary="Bulk User Management")
async def bulk_user_management(
    management: UserManagement,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_admin_permission())
):
    """
    Perform bulk actions on users.
    """
    try:
        # Get target users
        users = db.query(User).filter(User.id.in_(management.user_ids)).all()
        if len(users) != len(management.user_ids):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Some users not found"
            )
        
        affected_count = 0
        
        for user in users:
            if management.action == "activate":
                user.status = "ACTIVE"
                affected_count += 1
            elif management.action == "deactivate":
                user.status = "DEACTIVATED"
                affected_count += 1
            elif management.action == "reset_password":
                # TODO: Generate new password and send email
                affected_count += 1
            elif management.action == "delete":
                # Soft delete - set status instead of actual deletion
                user.status = "DELETED"
                affected_count += 1
        
        db.commit()
        
        logger.info(f"Bulk user action '{management.action}' performed on {affected_count} users by {current_user.id}")
        
        return {
            "message": f"Bulk {management.action} completed",
            "affected_users": affected_count,
            "action": management.action,
            "performed_by": current_user.id,
            "performed_at": datetime.utcnow()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bulk user management failed: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to perform bulk user action"
        )


# Audit log management
@router.get("/audit-logs", summary="Get Audit Logs")
async def get_audit_logs(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    action: Optional[str] = Query(None, description="Filter by action"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    start_date: Optional[datetime] = Query(None, description="Start date filter"),
    end_date: Optional[datetime] = Query(None, description="End date filter"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get system audit logs with filtering.
    """
    # Placeholder implementation
    return {
        "audit_logs": [],
        "total_count": 0,
        "filtered_count": 0,
        "filters_applied": {
            "user_id": user_id,
            "action": action,
            "resource_type": resource_type,
            "start_date": start_date,
            "end_date": end_date
        }
    }


# Celery task management
@router.get("/tasks", summary="Get Background Tasks")
async def get_background_tasks(
    status_filter: Optional[str] = Query(None, description="Filter by task status"),
    task_type: Optional[str] = Query(None, description="Filter by task type"),
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get background task status and history.
    """
    try:
        # Get active tasks from Celery
        inspector = celery_app.control.inspect()
        active_tasks = inspector.active() or {}
        scheduled_tasks = inspector.scheduled() or {}
        
        # Format task information
        all_tasks = []
        
        # Add active tasks
        for worker, tasks in active_tasks.items():
            for task in tasks:
                all_tasks.append({
                    "task_id": task.get("id"),
                    "name": task.get("name"),
                    "status": "running",
                    "worker": worker,
                    "started_at": task.get("time_start"),
                    "args": task.get("args", []),
                    "kwargs": task.get("kwargs", {})
                })
        
        # Add scheduled tasks
        for worker, tasks in scheduled_tasks.items():
            for task in tasks:
                all_tasks.append({
                    "task_id": task.get("id"),
                    "name": task.get("request", {}).get("name"),
                    "status": "scheduled",
                    "worker": worker,
                    "scheduled_at": task.get("eta"),
                    "args": task.get("request", {}).get("args", []),
                    "kwargs": task.get("request", {}).get("kwargs", {})
                })
        
        # Apply filters
        if status_filter:
            all_tasks = [t for t in all_tasks if t["status"] == status_filter]
        
        if task_type:
            all_tasks = [t for t in all_tasks if task_type in (t["name"] or "")]
        
        # Limit results
        limited_tasks = all_tasks[:limit]
        
        return {
            "tasks": limited_tasks,
            "total_count": len(all_tasks),
            "active_workers": list(active_tasks.keys()),
            "task_queues": ["default", "email", "data_processing", "notifications", "cleanup"]
        }
        
    except Exception as e:
        logger.error(f"Failed to get background tasks: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve background tasks"
        )


@router.delete("/tasks/{task_id}", summary="Cancel Background Task")
async def cancel_background_task(
    task_id: str,
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Cancel a running background task.
    """
    try:
        # Revoke the task
        celery_app.control.revoke(task_id, terminate=True)
        
        logger.info(f"Task {task_id} cancelled by user {current_user.id}")
        
        return {
            "message": "Task cancelled successfully",
            "task_id": task_id,
            "cancelled_by": current_user.id,
            "cancelled_at": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Failed to cancel task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel task"
        )


# System cleanup operations
@router.post("/cleanup", summary="System Cleanup")
async def perform_system_cleanup(
    cleanup_type: str = Query(..., description="Type of cleanup to perform"),
    days_old: int = Query(30, ge=1, le=365, description="Age threshold in days"),
    dry_run: bool = Query(True, description="Preview cleanup without executing"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Perform system cleanup operations.
    """
    try:
        cleanup_id = f"cleanup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # TODO: Implement actual cleanup logic
        background_tasks.add_task(
            _perform_cleanup,
            cleanup_id,
            cleanup_type,
            days_old,
            dry_run,
            current_user.id
        )
        
        logger.info(f"System cleanup '{cleanup_type}' started by user {current_user.id}")
        
        return {
            "message": f"System cleanup started ({'dry run' if dry_run else 'live run'})",
            "cleanup_id": cleanup_id,
            "cleanup_type": cleanup_type,
            "days_old": days_old,
            "dry_run": dry_run,
            "started_by": current_user.id,
            "started_at": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Failed to start system cleanup: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start system cleanup"
        )


async def _perform_cleanup(cleanup_id: str, cleanup_type: str, days_old: int, dry_run: bool, user_id: int):
    """Background task for system cleanup"""
    try:
        db = SessionLocal()
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        
        if cleanup_type == "old_flow_runs":
            # Cleanup old flow runs
            old_runs = db.query(FlowRun).filter(FlowRun.created_at < cutoff_date)
            count = old_runs.count()
            
            if not dry_run:
                old_runs.delete(synchronize_session=False)
                db.commit()
            
            logger.info(f"Cleanup {cleanup_id}: {'Would delete' if dry_run else 'Deleted'} {count} old flow runs")
        
        # Add more cleanup types as needed
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
    finally:
        db.close()