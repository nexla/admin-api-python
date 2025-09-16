"""
AuditLog Enhanced Model - Comprehensive change tracking and compliance logging.
Tracks all changes across the system with detailed before/after values and user attribution.
Implements Rails acts_as_audited pattern for comprehensive audit trails.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Index
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import logging
from ..database import Base

logger = logging.getLogger(__name__)

class AuditAction(PyEnum):
    """Audit action types"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    SOFT_DELETE = "soft_delete"
    RESTORE = "restore"
    LOGIN = "login"
    LOGOUT = "logout"
    FAILED_LOGIN = "failed_login"
    PASSWORD_CHANGE = "password_change"
    PERMISSION_GRANTED = "permission_granted"
    PERMISSION_REVOKED = "permission_revoked"
    ROLE_CHANGED = "role_changed"
    STATUS_CHANGED = "status_changed"
    EXPORT = "export"
    IMPORT = "import"
    VIEW = "view"
    DOWNLOAD = "download"
    
    @property
    def display_name(self) -> str:
        return {
            self.CREATE: "Created",
            self.UPDATE: "Updated", 
            self.DELETE: "Deleted",
            self.SOFT_DELETE: "Soft Deleted",
            self.RESTORE: "Restored",
            self.LOGIN: "Logged In",
            self.LOGOUT: "Logged Out",
            self.FAILED_LOGIN: "Failed Login",
            self.PASSWORD_CHANGE: "Password Changed",
            self.PERMISSION_GRANTED: "Permission Granted",
            self.PERMISSION_REVOKED: "Permission Revoked",
            self.ROLE_CHANGED: "Role Changed",
            self.STATUS_CHANGED: "Status Changed",
            self.EXPORT: "Exported",
            self.IMPORT: "Imported",
            self.VIEW: "Viewed",
            self.DOWNLOAD: "Downloaded"
        }.get(self, "Unknown Action")
    
    @property
    def is_destructive(self) -> bool:
        """Check if action is potentially destructive"""
        return self in [self.DELETE, self.SOFT_DELETE, self.PERMISSION_REVOKED]
    
    @property
    def is_security_related(self) -> bool:
        """Check if action is security-related"""
        return self in [self.LOGIN, self.LOGOUT, self.FAILED_LOGIN, self.PASSWORD_CHANGE, 
                       self.PERMISSION_GRANTED, self.PERMISSION_REVOKED, self.ROLE_CHANGED]

class AuditSeverity(PyEnum):
    """Audit log severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    
    @property
    def display_name(self) -> str:
        return {
            self.INFO: "Information",
            self.WARNING: "Warning",
            self.ERROR: "Error", 
            self.CRITICAL: "Critical"
        }.get(self, "Unknown Severity")

class AuditLogEnhanced(Base):
    __tablename__ = "audit_logs"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    action = Column(SQLEnum(AuditAction), nullable=False, index=True)
    severity = Column(SQLEnum(AuditSeverity), default=AuditSeverity.INFO, index=True)
    
    # Auditable resource (polymorphic association)
    auditable_type = Column(String(100), nullable=False, index=True)  # 'User', 'Project', etc.
    auditable_id = Column(Integer, nullable=False, index=True)
    
    # Change tracking
    audited_changes = Column(JSON)  # {'field_name': {'from': old_val, 'to': new_val}}
    old_values = Column(JSON)       # Complete snapshot of old values
    new_values = Column(JSON)       # Complete snapshot of new values
    
    # User and context
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    
    # Request context
    request_id = Column(String(64), index=True)  # For tracing requests across services
    session_id = Column(String(64), index=True)
    ip_address = Column(String(45))  # IPv6 compatible
    user_agent = Column(Text)
    endpoint = Column(String(255))   # API endpoint or page
    method = Column(String(10))      # HTTP method
    
    # Additional context
    comment = Column(Text)           # Optional human-readable description
    tags = Column(JSON)              # For categorization and filtering
    extra_metadata = Column(JSON)          # Additional context data
    
    # Compliance and retention
    retention_days = Column(Integer, default=2555)  # 7 years default
    is_sensitive = Column(Boolean, default=False, index=True)
    is_exported = Column(Boolean, default=False)
    export_batch_id = Column(String(64))
    
    # Error context (for failed operations)
    error_code = Column(String(50))
    error_message = Column(Text)
    stack_trace = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    expires_at = Column(DateTime, index=True)  # Auto-calculated from retention_days
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id])
    org = relationship("Org", foreign_keys=[org_id])
    
    # Enhanced database indexes for query performance
    __table_args__ = (
        Index('idx_audit_logs_auditable', 'auditable_type', 'auditable_id'),
        Index('idx_audit_logs_user_action', 'user_id', 'action', 'created_at'),
        Index('idx_audit_logs_org_action', 'org_id', 'action', 'created_at'),
        Index('idx_audit_logs_action_severity', 'action', 'severity'),
        Index('idx_audit_logs_created_action', 'created_at', 'action'),
        Index('idx_audit_logs_request_session', 'request_id', 'session_id'),
        Index('idx_audit_logs_retention', 'expires_at', 'is_exported'),
        Index('idx_audit_logs_security', 'action', 'user_id', 'created_at'),
        Index('idx_audit_logs_compliance', 'is_sensitive', 'retention_days', 'created_at'),
    )
    
    # Rails constants
    DEFAULT_RETENTION_DAYS = 2555  # 7 years
    SECURITY_RETENTION_DAYS = 3650  # 10 years for security events
    MAX_RETENTION_DAYS = 3650
    BULK_EXPORT_BATCH_SIZE = 10000
    SENSITIVE_FIELDS = ['password', 'api_key', 'secret', 'token', 'private_key']
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Auto-calculate expiry date
        if not self.expires_at and self.retention_days:
            self.expires_at = datetime.now() + timedelta(days=self.retention_days)
        
        # Auto-detect sensitive data
        if not hasattr(self, '_is_sensitive_set'):
            self.is_sensitive = self._detect_sensitive_data()
    
    # Rails-style predicate methods
    def create_action_(self) -> bool:
        """Check if this is a create action (Rails pattern)"""
        return self.action == AuditAction.CREATE
    
    def update_action_(self) -> bool:
        """Check if this is an update action (Rails pattern)"""
        return self.action == AuditAction.UPDATE
    
    def delete_action_(self) -> bool:
        """Check if this is a delete action (Rails pattern)"""
        return self.action in [AuditAction.DELETE, AuditAction.SOFT_DELETE]
    
    def security_action_(self) -> bool:
        """Check if this is a security-related action (Rails pattern)"""
        return self.action.is_security_related
    
    def destructive_action_(self) -> bool:
        """Check if this is a destructive action (Rails pattern)"""
        return self.action.is_destructive
    
    def sensitive_(self) -> bool:
        """Check if audit log contains sensitive data (Rails pattern)"""
        return self.is_sensitive
    
    def expired_(self) -> bool:
        """Check if audit log has expired (Rails pattern)"""
        return self.expires_at and self.expires_at < datetime.now()
    
    def exportable_(self) -> bool:
        """Check if audit log can be exported (Rails pattern)"""
        return not self.is_exported and not self.expired_()
    
    def critical_(self) -> bool:
        """Check if audit log is critical severity (Rails pattern)"""
        return self.severity == AuditSeverity.CRITICAL
    
    def error_(self) -> bool:
        """Check if audit log has error information (Rails pattern)"""
        return bool(self.error_code or self.error_message)
    
    def has_changes_(self) -> bool:
        """Check if audit log contains field changes (Rails pattern)"""
        return bool(self.audited_changes)
    
    def field_changed_(self, field_name: str) -> bool:
        """Check if specific field was changed (Rails pattern)"""
        return bool(self.audited_changes and field_name in self.audited_changes)
    
    def user_action_(self) -> bool:
        """Check if action was performed by a user (Rails pattern)"""
        return self.user_id is not None
    
    def system_action_(self) -> bool:
        """Check if action was performed by system (Rails pattern)"""
        return self.user_id is None
    
    def recent_(self, hours: int = 24) -> bool:
        """Check if audit log is recent (Rails pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.created_at >= cutoff
    
    def same_session_(self, other_log) -> bool:
        """Check if two logs are from same session (Rails pattern)"""
        return bool(self.session_id and other_log.session_id and 
                   self.session_id == other_log.session_id)
    
    def same_request_(self, other_log) -> bool:
        """Check if two logs are from same request (Rails pattern)"""
        return bool(self.request_id and other_log.request_id and 
                   self.request_id == other_log.request_id)
    
    # Rails helper methods
    def get_field_change(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Get change details for specific field (Rails pattern)"""
        if not self.audited_changes or field_name not in self.audited_changes:
            return None
        return self.audited_changes[field_name]
    
    def get_old_value(self, field_name: str) -> Any:
        """Get old value for specific field (Rails pattern)"""
        change = self.get_field_change(field_name)
        return change['from'] if change else None
    
    def get_new_value(self, field_name: str) -> Any:
        """Get new value for specific field (Rails pattern)"""
        change = self.get_field_change(field_name)
        return change['to'] if change else None
    
    def changed_fields(self) -> List[str]:
        """Get list of changed field names (Rails pattern)"""
        return list(self.audited_changes.keys()) if self.audited_changes else []
    
    def sensitive_fields_changed(self) -> List[str]:
        """Get list of changed sensitive fields (Rails pattern)"""
        if not self.audited_changes:
            return []
        
        return [field for field in self.audited_changes.keys() 
                if any(sensitive in field.lower() for sensitive in self.SENSITIVE_FIELDS)]
    
    def format_change_summary(self) -> str:
        """Format human-readable change summary (Rails pattern)"""
        if not self.audited_changes:
            return f"{self.action.display_name} {self.auditable_type}"
        
        changes = []
        for field, change_data in self.audited_changes.items():
            if isinstance(change_data, dict) and 'from' in change_data and 'to' in change_data:
                old_val = change_data['from']
                new_val = change_data['to']
                
                # Mask sensitive fields
                if any(sensitive in field.lower() for sensitive in self.SENSITIVE_FIELDS):
                    changes.append(f"{field}: [REDACTED]")
                else:
                    changes.append(f"{field}: '{old_val}' → '{new_val}'")
        
        return f"{self.action.display_name} {self.auditable_type}: {', '.join(changes)}"
    
    def _detect_sensitive_data(self) -> bool:
        """Detect if audit log contains sensitive data (Rails private pattern)"""
        # Check if any changed fields are sensitive
        if self.audited_changes:
            for field_name in self.audited_changes.keys():
                if any(sensitive in field_name.lower() for sensitive in self.SENSITIVE_FIELDS):
                    return True
        
        # Check metadata for sensitive data
        if self.extra_metadata:
            metadata_str = json.dumps(self.extra_metadata).lower()
            if any(sensitive in metadata_str for sensitive in self.SENSITIVE_FIELDS):
                return True
        
        return False
    
    def extend_retention_(self, days: int, reason: str = None) -> None:
        """Extend retention period (Rails bang method pattern)"""
        if days > self.MAX_RETENTION_DAYS:
            raise ValueError(f"Retention period cannot exceed {self.MAX_RETENTION_DAYS} days")
        
        self.retention_days = days
        self.expires_at = self.created_at + timedelta(days=days)
        
        if reason:
            if not self.extra_metadata:
                self.extra_metadata = {}
            extensions = self.extra_metadata.get('retention_extensions', [])
            extensions.append({
                'extended_to_days': days,
                'reason': reason,
                'extended_at': datetime.now().isoformat()
            })
            self.extra_metadata['retention_extensions'] = extensions
    
    def mark_exported_(self, batch_id: str = None) -> None:
        """Mark as exported for compliance (Rails bang method pattern)"""
        self.is_exported = True
        self.export_batch_id = batch_id
        
        if not self.extra_metadata:
            self.extra_metadata = {}
        self.extra_metadata['exported_at'] = datetime.now().isoformat()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag for categorization (Rails bang method pattern)"""
        if not self.tags:
            self.tags = []
        if tag not in self.tags:
            self.tags.append(tag)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag (Rails bang method pattern)"""
        if self.tags and tag in self.tags:
            self.tags.remove(tag)
    
    def has_tag_(self, tag: str) -> bool:
        """Check if has specific tag (Rails pattern)"""
        return bool(self.tags and tag in self.tags)
    
    # Rails class methods and scopes
    @classmethod
    def by_auditable(cls, auditable_type: str, auditable_id: int):
        """Scope for specific auditable resource (Rails scope pattern)"""
        return cls.query.filter_by(auditable_type=auditable_type, auditable_id=auditable_id)
    
    @classmethod
    def by_action(cls, action: AuditAction):
        """Scope for specific action (Rails scope pattern)"""
        return cls.query.filter_by(action=action)
    
    @classmethod
    def by_user(cls, user_id: int):
        """Scope for specific user (Rails scope pattern)"""
        return cls.query.filter_by(user_id=user_id)
    
    @classmethod
    def by_org(cls, org_id: int):
        """Scope for specific organization (Rails scope pattern)"""
        return cls.query.filter_by(org_id=org_id)
    
    @classmethod
    def security_events(cls):
        """Scope for security-related events (Rails scope pattern)"""
        security_actions = [action for action in AuditAction if action.is_security_related]
        return cls.query.filter(cls.action.in_(security_actions))
    
    @classmethod
    def destructive_actions(cls):
        """Scope for destructive actions (Rails scope pattern)"""
        destructive_actions = [action for action in AuditAction if action.is_destructive]
        return cls.query.filter(cls.action.in_(destructive_actions))
    
    @classmethod
    def recent(cls, hours: int = 24):
        """Scope for recent logs (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return cls.query.filter(cls.created_at >= cutoff)
    
    @classmethod
    def sensitive_data(cls):
        """Scope for sensitive audit logs (Rails scope pattern)"""
        return cls.query.filter_by(is_sensitive=True)
    
    @classmethod
    def expired(cls):
        """Scope for expired logs (Rails scope pattern)"""
        return cls.query.filter(cls.expires_at < datetime.now())
    
    @classmethod
    def exportable(cls):
        """Scope for exportable logs (Rails scope pattern)"""
        return cls.query.filter_by(is_exported=False).filter(
            (cls.expires_at.is_(None)) | (cls.expires_at > datetime.now())
        )
    
    @classmethod
    def by_severity(cls, severity: AuditSeverity):
        """Scope for specific severity (Rails scope pattern)"""
        return cls.query.filter_by(severity=severity)
    
    @classmethod
    def critical_events(cls):
        """Scope for critical events (Rails scope pattern)"""
        return cls.query.filter_by(severity=AuditSeverity.CRITICAL)
    
    @classmethod
    def with_errors(cls):
        """Scope for logs with errors (Rails scope pattern)"""
        return cls.query.filter(
            (cls.error_code.isnot(None)) | (cls.error_message.isnot(None))
        )
    
    @classmethod
    def by_ip_address(cls, ip_address: str):
        """Scope for specific IP address (Rails scope pattern)"""
        return cls.query.filter_by(ip_address=ip_address)
    
    @classmethod
    def by_session(cls, session_id: str):
        """Scope for specific session (Rails scope pattern)"""
        return cls.query.filter_by(session_id=session_id)
    
    @classmethod
    def create_audit_log(cls, action: AuditAction, auditable_type: str, auditable_id: int,
                        user_id: int = None, org_id: int = None, **kwargs):
        """Factory method to create audit log (Rails pattern)"""
        audit_data = {
            'action': action,
            'auditable_type': auditable_type,
            'auditable_id': auditable_id,
            'user_id': user_id,
            'org_id': org_id,
            **kwargs
        }
        
        # Set appropriate retention based on action type
        if action.is_security_related and 'retention_days' not in kwargs:
            audit_data['retention_days'] = cls.SECURITY_RETENTION_DAYS
        
        return cls(**audit_data)
    
    @classmethod
    def cleanup_expired(cls) -> int:
        """Clean up expired audit logs (Rails pattern)"""
        expired_logs = cls.expired().all()
        count = len(expired_logs)
        
        for log in expired_logs:
            # Only delete non-sensitive or already exported logs
            if not log.is_sensitive or log.is_exported:
                log.delete()
        
        return count
    
    @classmethod
    def export_batch(cls, batch_size: int = None) -> Tuple[List['AuditLogEnhanced'], str]:
        """Export batch of audit logs (Rails pattern)"""
        batch_size = batch_size or cls.BULK_EXPORT_BATCH_SIZE
        batch_id = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        exportable_logs = cls.exportable().limit(batch_size).all()
        
        for log in exportable_logs:
            log.mark_exported_(batch_id)
        
        return exportable_logs, batch_id
    
    @classmethod
    def get_audit_statistics(cls, org_id: int = None, days: int = 30) -> Dict[str, Any]:
        """Get audit statistics (Rails class method pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        query = cls.query.filter(cls.created_at >= cutoff)
        
        if org_id:
            query = query.filter_by(org_id=org_id)
        
        total_events = query.count()
        security_events = query.filter(cls.action.in_([
            action for action in AuditAction if action.is_security_related
        ])).count()
        
        destructive_events = query.filter(cls.action.in_([
            action for action in AuditAction if action.is_destructive
        ])).count()
        
        critical_events = query.filter_by(severity=AuditSeverity.CRITICAL).count()
        
        return {
            'period_days': days,
            'total_events': total_events,
            'security_events': security_events,
            'destructive_events': destructive_events,
            'critical_events': critical_events,
            'average_events_per_day': round(total_events / days, 2) if days > 0 else 0,
            'security_percentage': round((security_events / total_events * 100), 2) if total_events > 0 else 0
        }
    
    # Display and serialization methods
    def display_action(self) -> str:
        """Get human-readable action (Rails pattern)"""
        return self.action.display_name if self.action else "Unknown Action"
    
    def display_severity(self) -> str:
        """Get human-readable severity (Rails pattern)"""
        return self.severity.display_name if self.severity else "Unknown Severity"
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'action': self.action.value,
            'display_action': self.display_action(),
            'severity': self.severity.value,
            'display_severity': self.display_severity(),
            'auditable_type': self.auditable_type,
            'auditable_id': self.auditable_id,
            'user_id': self.user_id,
            'org_id': self.org_id,
            'ip_address': self.ip_address,
            'endpoint': self.endpoint,
            'method': self.method,
            'comment': self.comment,
            'tags': self.tags or [],
            'is_sensitive': self.is_sensitive,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'change_summary': self.format_change_summary()
        }
        
        if include_sensitive or not self.is_sensitive:
            result.update({
                'audited_changes': self.audited_changes,
                'metadata': self.extra_metadata
            })
        
        if self.error_():
            result.update({
                'error_code': self.error_code,
                'error_message': self.error_message
            })
        
        return result
    
    def __repr__(self) -> str:
        return f"<AuditLogEnhanced(id={self.id}, action='{self.action.value}', auditable_type='{self.auditable_type}', auditable_id={self.auditable_id})>"
    
    def __str__(self) -> str:
        return self.format_change_summary()

# Backwards compatibility alias
AuditLog = AuditLogEnhanced