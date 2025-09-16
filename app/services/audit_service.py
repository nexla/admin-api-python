"""
Audit and Logging Service - Comprehensive system activity tracking.
Provides audit logging, security event tracking, and system monitoring.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import Request
import json
import asyncio

from ..database import SessionLocal
from ..models.audit_log import AuditLog, SecurityEvent, SystemLog
from ..models.user import User

logger = logging.getLogger(__name__)


class AuditService:
    """Service for audit logging and security event tracking"""
    
    @staticmethod
    def log_action(
        db: Session,
        user_id: Optional[int],
        action: str,
        resource_type: str,
        resource_id: Optional[int] = None,
        resource_name: Optional[str] = None,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        request: Optional[Request] = None,
        org_id: Optional[int] = None,
        risk_level: str = "low"
    ):
        """
        Log an audit action to the database.
        
        Args:
            db: Database session
            user_id: ID of the user performing the action
            action: Action being performed (e.g., 'create', 'update', 'delete')
            resource_type: Type of resource (e.g., 'user', 'flow', 'data_source')
            resource_id: ID of the resource being acted upon
            resource_name: Name of the resource for readability
            old_values: Previous values before change
            new_values: New values after change
            details: Additional context information
            request: FastAPI request object for extracting metadata
            org_id: Organization context
            risk_level: Risk assessment of the action
        """
        try:
            # Extract request metadata
            ip_address = None
            user_agent = None
            method = None
            endpoint = None
            
            if request:
                ip_address = request.client.host if request.client else None
                user_agent = request.headers.get("user-agent")
                method = request.method
                endpoint = str(request.url.path)
            
            # Calculate changes if old and new values provided
            changes = None
            if old_values and new_values:
                changes = {}
                for key in set(old_values.keys()) | set(new_values.keys()):
                    old_val = old_values.get(key)
                    new_val = new_values.get(key)
                    if old_val != new_val:
                        changes[key] = {
                            "old": old_val,
                            "new": new_val
                        }
            
            # Create audit log entry
            audit_log = AuditLog(
                user_id=user_id,
                ip_address=ip_address,
                user_agent=user_agent,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                resource_name=resource_name,
                method=method,
                endpoint=endpoint,
                old_values=old_values,
                new_values=new_values,
                changes=changes,
                details=details,
                org_id=org_id,
                risk_level=risk_level,
                timestamp=datetime.utcnow()
            )
            
            db.add(audit_log)
            db.commit()
            
            # Log high-risk actions
            if risk_level in ["high", "critical"]:
                logger.warning(f"High-risk audit action: {action} on {resource_type} by user {user_id}")
            
            return audit_log.id
            
        except Exception as e:
            logger.error(f"Failed to log audit action: {str(e)}")
            db.rollback()
            return None
    
    @staticmethod
    def log_security_event(
        db: Session,
        event_type: str,
        severity: str,
        category: str,
        title: str,
        description: Optional[str] = None,
        user_id: Optional[int] = None,
        ip_address: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[int] = None,
        org_id: Optional[int] = None
    ):
        """
        Log a security event.
        
        Args:
            db: Database session
            event_type: Type of security event
            severity: Severity level (info, warning, error, critical)
            category: Event category (authentication, authorization, etc.)
            title: Short description of the event
            description: Detailed description
            user_id: User involved in the event
            ip_address: Source IP address
            details: Additional event details
            resource_type: Resource type involved
            resource_id: Resource ID involved
            org_id: Organization context
        """
        try:
            security_event = SecurityEvent(
                event_type=event_type,
                severity=severity,
                category=category,
                title=title,
                description=description,
                user_id=user_id,
                ip_address=ip_address,
                details=details,
                resource_type=resource_type,
                resource_id=resource_id,
                org_id=org_id,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            db.add(security_event)
            db.commit()
            
            # Alert on critical security events
            if severity == "critical":
                logger.critical(f"Critical security event: {title} - {description}")
                # TODO: Send immediate alerts to security team
            
            return security_event.id
            
        except Exception as e:
            logger.error(f"Failed to log security event: {str(e)}")
            db.rollback()
            return None
    
    @staticmethod
    def log_system_event(
        level: str,
        component: str,
        message: str,
        user_id: Optional[int] = None,
        session_id: Optional[str] = None,
        request_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        exception_type: Optional[str] = None,
        stack_trace: Optional[str] = None,
        execution_time_ms: Optional[int] = None,
        memory_usage_mb: Optional[int] = None
    ):
        """
        Log a system event asynchronously.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            component: System component (api, celery, database, etc.)
            message: Log message
            user_id: User context
            session_id: Session context
            request_id: Request tracking ID
            details: Additional details
            exception_type: Exception class name if applicable
            stack_trace: Full stack trace if applicable
            execution_time_ms: Performance metric
            memory_usage_mb: Memory usage metric
        """
        try:
            # Log to database asynchronously to avoid blocking
            asyncio.create_task(
                AuditService._async_log_system_event(
                    level, component, message, user_id, session_id,
                    request_id, details, exception_type, stack_trace,
                    execution_time_ms, memory_usage_mb
                )
            )
            
        except Exception as e:
            # Fallback to standard logging if database logging fails
            logger.error(f"Failed to queue system log: {str(e)}")
    
    @staticmethod
    async def _async_log_system_event(
        level: str,
        component: str,
        message: str,
        user_id: Optional[int],
        session_id: Optional[str],
        request_id: Optional[str],
        details: Optional[Dict[str, Any]],
        exception_type: Optional[str],
        stack_trace: Optional[str],
        execution_time_ms: Optional[int],
        memory_usage_mb: Optional[int]
    ):
        """Async system event logging"""
        try:
            db = SessionLocal()
            
            system_log = SystemLog(
                level=level,
                component=component,
                message=message,
                user_id=user_id,
                session_id=session_id,
                request_id=request_id,
                details=details,
                exception_type=exception_type,
                stack_trace=stack_trace,
                execution_time_ms=execution_time_ms,
                memory_usage_mb=memory_usage_mb,
                timestamp=datetime.utcnow()
            )
            
            db.add(system_log)
            db.commit()
            
        except Exception as e:
            logger.error(f"Failed to log system event to database: {str(e)}")
        finally:
            db.close()
    
    @staticmethod
    def get_audit_logs(
        db: Session,
        user_id: Optional[int] = None,
        action: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        risk_level: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[AuditLog]:
        """Get filtered audit logs"""
        try:
            query = db.query(AuditLog)
            
            if user_id:
                query = query.filter(AuditLog.user_id == user_id)
            
            if action:
                query = query.filter(AuditLog.action == action)
            
            if resource_type:
                query = query.filter(AuditLog.resource_type == resource_type)
            
            if resource_id:
                query = query.filter(AuditLog.resource_id == resource_id)
            
            if start_date:
                query = query.filter(AuditLog.timestamp >= start_date)
            
            if end_date:
                query = query.filter(AuditLog.timestamp <= end_date)
            
            if risk_level:
                query = query.filter(AuditLog.risk_level == risk_level)
            
            return query.order_by(AuditLog.timestamp.desc()).offset(offset).limit(limit).all()
            
        except Exception as e:
            logger.error(f"Failed to get audit logs: {str(e)}")
            return []
    
    @staticmethod
    def get_security_events(
        db: Session,
        severity: Optional[str] = None,
        category: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[SecurityEvent]:
        """Get filtered security events"""
        try:
            query = db.query(SecurityEvent)
            
            if severity:
                query = query.filter(SecurityEvent.severity == severity)
            
            if category:
                query = query.filter(SecurityEvent.category == category)
            
            if status:
                query = query.filter(SecurityEvent.status == status)
            
            if start_date:
                query = query.filter(SecurityEvent.created_at >= start_date)
            
            if end_date:
                query = query.filter(SecurityEvent.created_at <= end_date)
            
            return query.order_by(SecurityEvent.created_at.desc()).offset(offset).limit(limit).all()
            
        except Exception as e:
            logger.error(f"Failed to get security events: {str(e)}")
            return []
    
    @staticmethod
    def log_user_action(
        db: Session,
        performing_user: User,
        action: str,
        details: Optional[Dict[str, Any]] = None,
        target_user_id: Optional[int] = None,
        request: Optional[Request] = None
    ):
        """Convenience method for logging user-related actions"""
        return AuditService.log_action(
            db=db,
            user_id=performing_user.id,
            action=action,
            resource_type="user",
            resource_id=target_user_id,
            details=details,
            request=request,
            risk_level="medium" if action in ["password_changed", "account_locked"] else "low"
        )


class SecurityEventTypes:
    """Predefined security event types"""
    
    # Authentication events
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    LOGOUT = "logout"
    PASSWORD_CHANGE = "password_change"
    PASSWORD_RESET = "password_reset"
    ACCOUNT_LOCKED = "account_locked"
    ACCOUNT_UNLOCKED = "account_unlocked"
    
    # Authorization events
    ACCESS_DENIED = "access_denied"
    PERMISSION_GRANTED = "permission_granted"
    PERMISSION_REVOKED = "permission_revoked"
    ROLE_ASSIGNED = "role_assigned"
    ROLE_REMOVED = "role_removed"
    
    # Data access events
    SENSITIVE_DATA_ACCESS = "sensitive_data_access"
    DATA_EXPORT = "data_export"
    DATA_DELETION = "data_deletion"
    BULK_OPERATION = "bulk_operation"
    
    # System events
    CONFIGURATION_CHANGE = "configuration_change"
    MAINTENANCE_MODE = "maintenance_mode"
    BACKUP_CREATED = "backup_created"
    SYSTEM_ERROR = "system_error"
    
    # Suspicious activity
    MULTIPLE_LOGIN_FAILURES = "multiple_login_failures"
    UNUSUAL_ACCESS_PATTERN = "unusual_access_pattern"
    SUSPICIOUS_API_USAGE = "suspicious_api_usage"


class AuditActions:
    """Predefined audit action types"""
    
    # CRUD operations
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    
    # Specific actions
    LOGIN = "login"
    LOGOUT = "logout"
    EXECUTE = "execute"
    STOP = "stop"
    SCHEDULE = "schedule"
    SHARE = "share"
    EXPORT = "export"
    IMPORT = "import"
    BACKUP = "backup"
    RESTORE = "restore"
    
    # Administrative actions
    ACTIVATE = "activate"
    DEACTIVATE = "deactivate"
    LOCK = "lock"
    UNLOCK = "unlock"
    GRANT_PERMISSION = "grant_permission"
    REVOKE_PERMISSION = "revoke_permission"
    
    # User management actions
    USER_CREATED = "user_created"
    USER_UPDATED = "user_updated"
    USER_DEACTIVATED = "user_deactivated"
    USER_ACTIVATED = "user_activated"
    PASSWORD_CHANGED = "password_changed"
    ACCOUNT_LOCKED = "account_locked"
    ACCOUNT_UNLOCKED = "account_unlocked"
    
