"""
Notification Model - System notification and alert management entity.
Handles user notifications, alerts, and messaging with comprehensive Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import uuid
from ..database import Base


class NotificationLevels(PyEnum):
    """Notification level enumeration"""
    ALL = "ALL"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    RECOVERED = "RECOVERED"
    RESOLVED = "RESOLVED"
    SUCCESS = "SUCCESS"


class ResourceTypes(PyEnum):
    """Resource type enumeration for notifications"""
    SOURCE = "SOURCE"
    PUB = "PUB"
    SUB = "SUB"
    DATASET = "DATASET"
    SINK = "SINK"
    USER = "USER"
    ORG = "ORG"
    CUSTOM_DATA_FLOW = "CUSTOM_DATA_FLOW"
    DATA_FLOW = "DATA_FLOW"
    CATALOG_CONFIG = "CATALOG_CONFIG"
    APPROVAL_STEP = "APPROVAL_STEP"
    PROJECT = "PROJECT"
    INVITE = "INVITE"
    FLOW_NODE = "FLOW_NODE"
    CONNECTOR = "CONNECTOR"


class NotificationStatuses(PyEnum):
    """Notification status enumeration"""
    ACTIVE = "ACTIVE"
    READ = "READ"
    ARCHIVED = "ARCHIVED"
    DISMISSED = "DISMISSED"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"


class NotificationChannels(PyEnum):
    """Notification delivery channel enumeration"""
    IN_APP = "in_app"
    EMAIL = "email"
    SMS = "sms"
    WEBHOOK = "webhook"
    SLACK = "slack"
    TEAMS = "teams"
    PUSH = "push"


class NotificationPriorities(PyEnum):
    """Notification priority enumeration"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"
    CRITICAL = "critical"


class Notification(Base):
    __tablename__ = 'notifications'
    
    # Primary attributes
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    uuid = Column(String(36), default=lambda: str(uuid.uuid4()), unique=True, index=True)
    title = Column(String(255))
    message = Column(Text)
    level = Column(SQLEnum(NotificationLevels), nullable=False, index=True)
    status = Column(SQLEnum(NotificationStatuses), default=NotificationStatuses.ACTIVE, nullable=False, index=True)
    priority = Column(SQLEnum(NotificationPriorities), default=NotificationPriorities.NORMAL, index=True)
    
    # Resource association
    resource_type = Column(SQLEnum(ResourceTypes), index=True)
    resource_id = Column(Integer, index=True)
    resource_name = Column(String(255))
    
    # Delivery and tracking
    channels = Column(JSON)  # Array of delivery channels
    delivery_status = Column(JSON)  # Delivery status per channel
    delivery_attempts = Column(Integer, default=0)
    max_delivery_attempts = Column(Integer, default=3)
    
    # Timing and expiry
    timestamp = Column(DateTime, default=func.now(), nullable=False, index=True)
    read_at = Column(DateTime, index=True)
    dismissed_at = Column(DateTime)
    expires_at = Column(DateTime, index=True)
    retry_after = Column(DateTime)
    
    # Metadata and context
    context = Column(JSON)  # Additional context data
    extra_metadata = Column(JSON)  # System metadata
    tags = Column(JSON)     # Notification tags
    
    # Grouping and threading
    group_key = Column(String(255), index=True)
    thread_id = Column(String(255), index=True)
    parent_notification_id = Column(Integer, ForeignKey('notifications.id'), index=True)
    
    # State flags
    is_system = Column(Boolean, default=False)
    is_actionable = Column(Boolean, default=False)
    is_persistent = Column(Boolean, default=False)
    requires_acknowledgment = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False, index=True)
    sender_id = Column(Integer, ForeignKey('users.id'), index=True)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id], back_populates="notifications")
    org = relationship("Org", foreign_keys=[org_id], back_populates="notifications")
    sender = relationship("User", foreign_keys=[sender_id])
    parent_notification = relationship("Notification", remote_side="Notification.id", foreign_keys=[parent_notification_id])
    child_notifications = relationship("Notification", remote_side="Notification.parent_notification_id")
    
    # Rails business logic constants
    MESSAGE_SIZE_LIMIT = 65535  # 64KB - 1
    ARCHIVE_BEFORE_DAYS = 365   # Production default
    DEFAULT_EXPIRY_DAYS = 30
    MAX_DELIVERY_ATTEMPTS = 5
    RETRY_INTERVALS = [300, 900, 3600, 7200, 14400]  # 5min, 15min, 1h, 2h, 4h
    BATCH_SIZE = 1000
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Rails-style instance variables
        self._resource_cache = None
        self._delivery_results = {}
        self._context_cache = {}
    
    # ========================================
    # Rails Predicate Methods (status checking with _() suffix)
    # ========================================
    
    def active_(self) -> bool:
        """Check if notification is active (Rails pattern)"""
        return self.status == NotificationStatuses.ACTIVE and not self.expired_()
    
    def read_(self) -> bool:
        """Check if notification is read (Rails pattern)"""
        return self.read_at is not None or self.status == NotificationStatuses.READ
    
    def unread_(self) -> bool:
        """Check if notification is unread (Rails pattern)"""
        return not self.read_()
    
    def archived_(self) -> bool:
        """Check if notification is archived (Rails pattern)"""
        return self.status == NotificationStatuses.ARCHIVED
    
    def dismissed_(self) -> bool:
        """Check if notification is dismissed (Rails pattern)"""
        return self.dismissed_at is not None or self.status == NotificationStatuses.DISMISSED
    
    def expired_(self) -> bool:
        """Check if notification is expired (Rails pattern)"""
        return (self.expires_at is not None and self.expires_at < datetime.now()) or \
               self.status == NotificationStatuses.EXPIRED
    
    def pending_(self) -> bool:
        """Check if notification is pending (Rails pattern)"""
        return self.status == NotificationStatuses.PENDING
    
    def error_(self) -> bool:
        """Check if notification is an error level (Rails pattern)"""
        return self.level in [NotificationLevels.ERROR, NotificationLevels.CRITICAL, 
                             NotificationLevels.WARN, NotificationLevels.WARNING]
    
    def info_(self) -> bool:
        """Check if notification is informational (Rails pattern)"""
        return self.level in [NotificationLevels.INFO, NotificationLevels.DEBUG, NotificationLevels.SUCCESS]
    
    def critical_(self) -> bool:
        """Check if notification is critical (Rails pattern)"""
        return self.level == NotificationLevels.CRITICAL
    
    def warning_(self) -> bool:
        """Check if notification is warning level (Rails pattern)"""
        return self.level in [NotificationLevels.WARN, NotificationLevels.WARNING]
    
    def resolved_(self) -> bool:
        """Check if notification indicates resolution (Rails pattern)"""
        return self.level in [NotificationLevels.RESOLVED, NotificationLevels.RECOVERED, NotificationLevels.SUCCESS]
    
    def system_(self) -> bool:
        """Check if notification is system-generated (Rails pattern)"""
        return self.is_system is True
    
    def actionable_(self) -> bool:
        """Check if notification requires action (Rails pattern)"""
        return self.is_actionable is True
    
    def persistent_(self) -> bool:
        """Check if notification is persistent (Rails pattern)"""
        return self.is_persistent is True
    
    def requires_acknowledgment_(self) -> bool:
        """Check if notification requires acknowledgment (Rails pattern)"""
        return self.requires_acknowledgment is True
    
    def high_priority_(self) -> bool:
        """Check if notification is high priority (Rails pattern)"""
        return self.priority in [NotificationPriorities.HIGH, NotificationPriorities.URGENT, NotificationPriorities.CRITICAL]
    
    def urgent_(self) -> bool:
        """Check if notification is urgent (Rails pattern)"""
        return self.priority in [NotificationPriorities.URGENT, NotificationPriorities.CRITICAL]
    
    def has_resource_(self) -> bool:
        """Check if notification has associated resource (Rails pattern)"""
        return self.resource_type is not None and self.resource_id is not None
    
    def has_parent_(self) -> bool:
        """Check if notification has parent (Rails pattern)"""
        return self.parent_notification_id is not None
    
    def has_children_(self) -> bool:
        """Check if notification has children (Rails pattern)"""
        return len(self.child_notifications or []) > 0
    
    def is_thread_starter_(self) -> bool:
        """Check if notification starts a thread (Rails pattern)"""
        return not self.has_parent_() and self.has_children_()
    
    def recent_(self, hours: int = 24) -> bool:
        """Check if notification is recent (Rails pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.created_at > cutoff
    
    def delivery_failed_(self) -> bool:
        """Check if delivery failed (Rails pattern)"""
        return self.delivery_attempts >= self.max_delivery_attempts
    
    def can_retry_delivery_(self) -> bool:
        """Check if delivery can be retried (Rails pattern)"""
        return (not self.delivery_failed_() and 
                self.retry_after is not None and 
                self.retry_after <= datetime.now())
    
    def needs_delivery_(self) -> bool:
        """Check if notification needs delivery (Rails pattern)"""
        return (self.active_() and 
                self.channels and 
                (not self.delivery_status or 
                 any(status != 'delivered' for status in self.delivery_status.values())))
    
    def delivered_(self) -> bool:
        """Check if notification is delivered (Rails pattern)"""
        return (self.delivery_status and 
                all(status == 'delivered' for status in self.delivery_status.values()))
    
    def grouped_(self) -> bool:
        """Check if notification is part of a group (Rails pattern)"""
        return self.group_key is not None
    
    def threaded_(self) -> bool:
        """Check if notification is part of a thread (Rails pattern)"""
        return self.thread_id is not None
    
    # ========================================
    # Rails Bang Methods (state manipulation with _() suffix)
    # ========================================
    
    def mark_read_(self) -> None:
        """Mark notification as read (Rails bang method pattern)"""
        if self.read_():
            return
        
        self.read_at = datetime.now()
        self.status = NotificationStatuses.READ
        self.updated_at = datetime.now()
    
    def mark_unread_(self) -> None:
        """Mark notification as unread (Rails bang method pattern)"""
        if self.unread_():
            return
        
        self.read_at = None
        if self.status == NotificationStatuses.READ:
            self.status = NotificationStatuses.ACTIVE
        self.updated_at = datetime.now()
    
    def dismiss_(self) -> None:
        """Dismiss notification (Rails bang method pattern)"""
        if self.dismissed_():
            return
        
        self.dismissed_at = datetime.now()
        self.status = NotificationStatuses.DISMISSED
        self.updated_at = datetime.now()
    
    def archive_(self) -> None:
        """Archive notification (Rails bang method pattern)"""
        if self.archived_():
            return
        
        self.status = NotificationStatuses.ARCHIVED
        self.updated_at = datetime.now()
    
    def expire_(self) -> None:
        """Expire notification (Rails bang method pattern)"""
        if self.expired_():
            return
        
        self.status = NotificationStatuses.EXPIRED
        self.expires_at = datetime.now()
        self.updated_at = datetime.now()
    
    def activate_(self) -> None:
        """Activate notification (Rails bang method pattern)"""
        if self.active_():
            return
        
        self.status = NotificationStatuses.ACTIVE
        self.dismissed_at = None
        self.updated_at = datetime.now()
    
    def set_priority_(self, priority: NotificationPriorities) -> None:
        """Set notification priority (Rails bang method pattern)"""
        self.priority = priority
        self.updated_at = datetime.now()
    
    def escalate_priority_(self) -> None:
        """Escalate notification priority (Rails bang method pattern)"""
        priority_order = [
            NotificationPriorities.LOW,
            NotificationPriorities.NORMAL,
            NotificationPriorities.HIGH,
            NotificationPriorities.URGENT,
            NotificationPriorities.CRITICAL
        ]
        
        current_index = priority_order.index(self.priority)
        if current_index < len(priority_order) - 1:
            self.priority = priority_order[current_index + 1]
            self.updated_at = datetime.now()
    
    def add_channel_(self, channel: NotificationChannels) -> None:
        """Add delivery channel (Rails bang method pattern)"""
        if not self.channels:
            self.channels = []
        
        if channel.value not in self.channels:
            self.channels.append(channel.value)
            self.updated_at = datetime.now()
    
    def remove_channel_(self, channel: NotificationChannels) -> None:
        """Remove delivery channel (Rails bang method pattern)"""
        if self.channels and channel.value in self.channels:
            self.channels.remove(channel.value)
            self.updated_at = datetime.now()
    
    def set_expiry_(self, expires_at: datetime) -> None:
        """Set expiry time (Rails bang method pattern)"""
        self.expires_at = expires_at
        self.updated_at = datetime.now()
    
    def extend_expiry_(self, days: int = None, hours: int = None) -> None:
        """Extend expiry time (Rails bang method pattern)"""
        if days is None and hours is None:
            days = self.DEFAULT_EXPIRY_DAYS
        
        extension = timedelta(days=days or 0, hours=hours or 0)
        
        if self.expires_at:
            self.expires_at += extension
        else:
            self.expires_at = datetime.now() + extension
        
        self.updated_at = datetime.now()
    
    def record_delivery_attempt_(self, channel: str, status: str, error: str = None) -> None:
        """Record delivery attempt (Rails bang method pattern)"""
        if not self.delivery_status:
            self.delivery_status = {}
        
        self.delivery_status[channel] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'error': error,
            'attempt': self.delivery_attempts + 1
        }
        
        self.delivery_attempts += 1
        
        # Set retry time if needed
        if status == 'failed' and not self.delivery_failed_():
            attempt_index = min(self.delivery_attempts - 1, len(self.RETRY_INTERVALS) - 1)
            retry_delay = self.RETRY_INTERVALS[attempt_index]
            self.retry_after = datetime.now() + timedelta(seconds=retry_delay)
        
        self.updated_at = datetime.now()
    
    def acknowledge_(self) -> None:
        """Acknowledge notification (Rails bang method pattern)"""
        if not self.requires_acknowledgment_():
            return
        
        self.context = self.context or {}
        self.context['acknowledged'] = True
        self.context['acknowledged_at'] = datetime.now().isoformat()
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag_name: str) -> None:
        """Add tag to notification (Rails bang method pattern)"""
        if not self.tags:
            self.tags = []
        if tag_name not in self.tags:
            self.tags.append(tag_name)
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag_name: str) -> None:
        """Remove tag from notification (Rails bang method pattern)"""
        if self.tags and tag_name in self.tags:
            self.tags.remove(tag_name)
            self.updated_at = datetime.now()
    
    # ========================================
    # Rails Class Methods and Scopes
    # ========================================
    
    @classmethod
    def active(cls):
        """Scope for active notifications (Rails scope pattern)"""
        return cls.status == NotificationStatuses.ACTIVE
    
    @classmethod
    def unread(cls):
        """Scope for unread notifications (Rails scope pattern)"""
        from sqlalchemy import and_
        return and_(cls.read_at.is_(None), cls.status != NotificationStatuses.READ)
    
    @classmethod
    def read(cls):
        """Scope for read notifications (Rails scope pattern)"""
        from sqlalchemy import or_
        return or_(cls.read_at.isnot(None), cls.status == NotificationStatuses.READ)
    
    @classmethod
    def errors(cls):
        """Scope for error notifications (Rails scope pattern)"""
        return cls.level.in_([NotificationLevels.ERROR, NotificationLevels.CRITICAL,
                             NotificationLevels.WARN, NotificationLevels.WARNING])
    
    @classmethod
    def critical(cls):
        """Scope for critical notifications (Rails scope pattern)"""
        return cls.level == NotificationLevels.CRITICAL
    
    @classmethod
    def high_priority(cls):
        """Scope for high priority notifications (Rails scope pattern)"""
        return cls.priority.in_([NotificationPriorities.HIGH, NotificationPriorities.URGENT,
                                NotificationPriorities.CRITICAL])
    
    @classmethod
    def recent(cls, hours: int = 24):
        """Scope for recent notifications (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return cls.created_at >= cutoff
    
    @classmethod
    def expired(cls):
        """Scope for expired notifications (Rails scope pattern)"""
        from sqlalchemy import and_, or_
        return or_(
            and_(cls.expires_at.isnot(None), cls.expires_at < datetime.now()),
            cls.status == NotificationStatuses.EXPIRED
        )
    
    @classmethod
    def by_owner(cls, owner_id: int):
        """Scope for notifications by owner (Rails scope pattern)"""
        return cls.owner_id == owner_id
    
    @classmethod
    def by_org(cls, org_id: int):
        """Scope for notifications by organization (Rails scope pattern)"""
        return cls.org_id == org_id
    
    @classmethod
    def by_level(cls, level: NotificationLevels):
        """Scope for notifications by level (Rails scope pattern)"""
        return cls.level == level
    
    @classmethod
    def by_resource(cls, resource_type: ResourceTypes, resource_id: int = None):
        """Scope for notifications by resource (Rails scope pattern)"""
        if resource_id:
            from sqlalchemy import and_
            return and_(cls.resource_type == resource_type, cls.resource_id == resource_id)
        return cls.resource_type == resource_type
    
    @classmethod
    def grouped(cls, group_key: str):
        """Scope for grouped notifications (Rails scope pattern)"""
        return cls.group_key == group_key
    
    @classmethod
    def threaded(cls, thread_id: str):
        """Scope for threaded notifications (Rails scope pattern)"""
        return cls.thread_id == thread_id
    
    @classmethod
    def needs_delivery(cls):
        """Scope for notifications needing delivery (Rails scope pattern)"""
        from sqlalchemy import and_
        return and_(
            cls.status == NotificationStatuses.ACTIVE,
            cls.channels.isnot(None),
            cls.delivery_attempts < cls.max_delivery_attempts
        )
    
    @classmethod
    def delivery_failed(cls):
        """Scope for notifications with failed delivery (Rails scope pattern)"""
        return cls.delivery_attempts >= cls.max_delivery_attempts
    
    @classmethod
    def needs_archival(cls, days: int = None):
        """Scope for notifications needing archival (Rails scope pattern)"""
        days = days or cls.ARCHIVE_BEFORE_DAYS
        cutoff = datetime.now() - timedelta(days=days)
        return cls.created_at < cutoff
    
    @classmethod
    def create_error(cls, owner, org, message: str, title: str = None, resource=None, **kwargs):
        """Factory method to create error notification (Rails pattern)"""
        notification_data = {
            'owner': owner,
            'org': org,
            'level': NotificationLevels.ERROR,
            'message': message,
            'title': title or 'Error Notification',
            'priority': NotificationPriorities.HIGH,
            **kwargs
        }
        
        if resource:
            resource_type = cls._get_resource_type(resource)
            if resource_type:
                notification_data['resource_type'] = resource_type
                notification_data['resource_id'] = resource.id
                notification_data['resource_name'] = getattr(resource, 'name', str(resource))
        
        return cls(**notification_data)
    
    @classmethod
    def create_info(cls, owner, org, message: str, title: str = None, resource=None, **kwargs):
        """Factory method to create info notification (Rails pattern)"""
        notification_data = {
            'owner': owner,
            'org': org,
            'level': NotificationLevels.INFO,
            'message': message,
            'title': title or 'Information',
            'priority': NotificationPriorities.NORMAL,
            **kwargs
        }
        
        if resource:
            resource_type = cls._get_resource_type(resource)
            if resource_type:
                notification_data['resource_type'] = resource_type
                notification_data['resource_id'] = resource.id
                notification_data['resource_name'] = getattr(resource, 'name', str(resource))
        
        return cls(**notification_data)
    
    @classmethod
    def create_warning(cls, owner, org, message: str, title: str = None, resource=None, **kwargs):
        """Factory method to create warning notification (Rails pattern)"""
        notification_data = {
            'owner': owner,
            'org': org,
            'level': NotificationLevels.WARNING,
            'message': message,
            'title': title or 'Warning',
            'priority': NotificationPriorities.HIGH,
            **kwargs
        }
        
        if resource:
            resource_type = cls._get_resource_type(resource)
            if resource_type:
                notification_data['resource_type'] = resource_type
                notification_data['resource_id'] = resource.id
                notification_data['resource_name'] = getattr(resource, 'name', str(resource))
        
        return cls(**notification_data)
    
    @classmethod
    def create_success(cls, owner, org, message: str, title: str = None, resource=None, **kwargs):
        """Factory method to create success notification (Rails pattern)"""
        notification_data = {
            'owner': owner,
            'org': org,
            'level': NotificationLevels.SUCCESS,
            'message': message,
            'title': title or 'Success',
            'priority': NotificationPriorities.NORMAL,
            **kwargs
        }
        
        if resource:
            resource_type = cls._get_resource_type(resource)
            if resource_type:
                notification_data['resource_type'] = resource_type
                notification_data['resource_id'] = resource.id
                notification_data['resource_name'] = getattr(resource, 'name', str(resource))
        
        return cls(**notification_data)
    
    @classmethod
    def bulk_mark_read(cls, notification_ids: List[int], user_id: int):
        """Bulk mark notifications as read (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_dismiss(cls, notification_ids: List[int], user_id: int):
        """Bulk dismiss notifications (Rails pattern)"""
        # Implementation would update multiple records efficiently
        pass
    
    @classmethod
    def bulk_archive(cls, notification_ids: List[int]):
        """Bulk archive notifications (Rails pattern)"""
        # Implementation would move notifications to archive efficiently
        pass
    
    @classmethod
    def cleanup_expired(cls) -> int:
        """Clean up expired notifications (Rails pattern)"""
        # Implementation would remove expired notifications
        return 0
    
    @classmethod
    def archive_old(cls, days: int = None) -> int:
        """Archive old notifications (Rails pattern)"""
        days = days or cls.ARCHIVE_BEFORE_DAYS
        cutoff = datetime.now() - timedelta(days=days)
        
        # Implementation would move old notifications to archive
        return 0
    
    # ========================================
    # Rails Instance Methods
    # ========================================
    
    def resource(self):
        """Get the associated resource object (Rails pattern)"""
        if self._resource_cache:
            return self._resource_cache
        
        if not self.has_resource_():
            return None
        
        from ..models.data_source import DataSource
        from ..models.data_set import DataSet
        from ..models.data_sink import DataSink
        from ..models.user import User
        from ..models.org import Org
        from ..models.project import Project
        from ..models.connector import Connector
        
        resource_mapping = {
            ResourceTypes.SOURCE.value: DataSource,
            ResourceTypes.DATASET.value: DataSet,
            ResourceTypes.SINK.value: DataSink,
            ResourceTypes.USER.value: User,
            ResourceTypes.ORG.value: Org,
            ResourceTypes.PROJECT.value: Project,
            ResourceTypes.CONNECTOR.value: Connector
        }
        
        resource_class = resource_mapping.get(self.resource_type.value)
        if resource_class:
            # In real implementation, this would query the database
            # self._resource_cache = resource_class.query.get(self.resource_id)
            pass
        
        return self._resource_cache
    
    def create_child_notification(self, message: str, level: NotificationLevels = None, **kwargs):
        """Create child notification in thread (Rails pattern)"""
        child_data = {
            'owner': self.owner,
            'org': self.org,
            'message': message,
            'level': level or self.level,
            'parent_notification_id': self.id,
            'thread_id': self.thread_id or str(uuid.uuid4()),
            'group_key': self.group_key,
            **kwargs
        }
        
        return self.__class__(**child_data)
    
    def get_thread_notifications(self) -> List['Notification']:
        """Get all notifications in thread (Rails pattern)"""
        if not self.thread_id:
            return [self]
        
        # Implementation would query for all notifications with same thread_id
        return [self]
    
    def get_grouped_notifications(self) -> List['Notification']:
        """Get all notifications in group (Rails pattern)"""
        if not self.group_key:
            return [self]
        
        # Implementation would query for all notifications with same group_key
        return [self]
    
    def duplicate_for_user(self, target_user, target_org=None):
        """Duplicate notification for another user (Rails pattern)"""
        duplicate_data = {
            'owner': target_user,
            'org': target_org or target_user.default_org,
            'title': self.title,
            'message': self.message,
            'level': self.level,
            'priority': self.priority,
            'resource_type': self.resource_type,
            'resource_id': self.resource_id,
            'resource_name': self.resource_name,
            'channels': self.channels,
            'context': self.context.copy() if self.context else None,
            'tags': self.tags.copy() if self.tags else None,
            'group_key': self.group_key,
            'is_system': self.is_system,
            'is_actionable': self.is_actionable,
            'expires_at': self.expires_at
        }
        
        return self.__class__(**duplicate_data)
    
    def get_delivery_summary(self) -> Dict[str, Any]:
        """Get delivery summary (Rails pattern)"""
        if not self.delivery_status:
            return {'delivered': False, 'attempts': self.delivery_attempts, 'channels': {}}
        
        delivered_count = sum(1 for status in self.delivery_status.values() 
                             if status.get('status') == 'delivered')
        total_channels = len(self.channels) if self.channels else 0
        
        return {
            'delivered': delivered_count > 0,
            'fully_delivered': delivered_count == total_channels,
            'delivered_count': delivered_count,
            'total_channels': total_channels,
            'attempts': self.delivery_attempts,
            'max_attempts': self.max_delivery_attempts,
            'failed': self.delivery_failed_(),
            'channels': self.delivery_status
        }
    
    def has_tag(self, tag_name: str) -> bool:
        """Check if notification has specific tag (Rails pattern)"""
        return bool(self.tags and tag_name in self.tags)
    
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        return self.tags or []
    
    def channels_list(self) -> List[str]:
        """Get list of delivery channels (Rails pattern)"""
        return self.channels or []
    
    @classmethod
    def _get_resource_type(cls, resource) -> Optional[str]:
        """Helper to determine resource type from object (Rails private pattern)"""
        class_name = resource.__class__.__name__
        
        resource_mapping = {
            'DataSource': ResourceTypes.SOURCE.value,
            'DataSet': ResourceTypes.DATASET.value,
            'DataSink': ResourceTypes.SINK.value,
            'User': ResourceTypes.USER.value,
            'Org': ResourceTypes.ORG.value,
            'Project': ResourceTypes.PROJECT.value,
            'Invite': ResourceTypes.INVITE.value,
            'FlowNode': ResourceTypes.FLOW_NODE.value,
            'Connector': ResourceTypes.CONNECTOR.value,
            'CustomDataFlow': ResourceTypes.CUSTOM_DATA_FLOW.value,
            'DataFlow': ResourceTypes.DATA_FLOW.value
        }
        
        return resource_mapping.get(class_name)
    
    # ========================================
    # Rails Validation and Display Methods
    # ========================================
    
    def display_title(self) -> str:
        """Get display title for UI (Rails pattern)"""
        return self.title or f"Notification #{self.id}"
    
    def display_message(self, max_length: int = 100) -> str:
        """Get truncated message for display (Rails pattern)"""
        if not self.message:
            return ""
        
        if len(self.message) <= max_length:
            return self.message
        
        return self.message[:max_length-3] + "..."
    
    def display_level(self) -> str:
        """Get formatted level for display (Rails pattern)"""
        return self.level.value.replace('_', ' ').title()
    
    def level_color(self) -> str:
        """Get level color for UI (Rails pattern)"""
        level_colors = {
            NotificationLevels.SUCCESS: 'green',
            NotificationLevels.INFO: 'blue',
            NotificationLevels.DEBUG: 'gray',
            NotificationLevels.WARNING: 'yellow',
            NotificationLevels.WARN: 'yellow',
            NotificationLevels.ERROR: 'red',
            NotificationLevels.CRITICAL: 'red',
            NotificationLevels.RESOLVED: 'green',
            NotificationLevels.RECOVERED: 'green'
        }
        return level_colors.get(self.level, 'gray')
    
    def priority_color(self) -> str:
        """Get priority color for UI (Rails pattern)"""
        priority_colors = {
            NotificationPriorities.LOW: 'gray',
            NotificationPriorities.NORMAL: 'blue',
            NotificationPriorities.HIGH: 'orange',
            NotificationPriorities.URGENT: 'red',
            NotificationPriorities.CRITICAL: 'purple'
        }
        return priority_colors.get(self.priority, 'blue')
    
    def time_ago(self) -> str:
        """Get human readable time ago (Rails pattern)"""
        delta = datetime.now() - self.created_at
        
        if delta.days > 0:
            return f"{delta.days}d ago"
        elif delta.seconds > 3600:
            hours = delta.seconds // 3600
            return f"{hours}h ago"
        elif delta.seconds > 60:
            minutes = delta.seconds // 60
            return f"{minutes}m ago"
        else:
            return "just now"
    
    def validate_for_delivery(self) -> Tuple[bool, List[str]]:
        """Validate notification can be delivered (Rails pattern)"""
        errors = []
        
        if not self.active_():
            errors.append(f"Notification is not active (status: {self.status.value})")
        
        if not self.channels:
            errors.append("No delivery channels configured")
        
        if not self.message and not self.title:
            errors.append("Message or title is required")
        
        if self.expired_():
            errors.append("Notification has expired")
        
        if self.delivery_failed_():
            errors.append("Maximum delivery attempts exceeded")
        
        return len(errors) == 0, errors
    
    # ========================================
    # Rails API and Serialization Methods
    # ========================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for basic API responses (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'title': self.title,
            'message': self.message,
            'level': self.level.value,
            'display_level': self.display_level(),
            'level_color': self.level_color(),
            'status': self.status.value,
            'priority': self.priority.value,
            'priority_color': self.priority_color(),
            'resource_type': self.resource_type.value if self.resource_type else None,
            'resource_id': self.resource_id,
            'resource_name': self.resource_name,
            'read': self.read_(),
            'unread': self.unread_(),
            'dismissed': self.dismissed_(),
            'expired': self.expired_(),
            'critical': self.critical_(),
            'urgent': self.urgent_(),
            'has_resource': self.has_resource_(),
            'actionable': self.actionable_(),
            'persistent': self.persistent_(),
            'system': self.system_(),
            'recent': self.recent_(),
            'time_ago': self.time_ago(),
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'read_at': self.read_at.isoformat() if self.read_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'tags': self.tags_list()
        }
    
    def to_detailed_dict(self) -> Dict[str, Any]:
        """Convert to detailed dictionary for full API responses (Rails pattern)"""
        base_dict = self.to_dict()
        
        detailed_info = {
            'sender_id': self.sender_id,
            'parent_notification_id': self.parent_notification_id,
            'group_key': self.group_key,
            'thread_id': self.thread_id,
            'channels': self.channels_list(),
            'delivery_summary': self.get_delivery_summary(),
            'context': self.context,
            'metadata': self.extra_metadata,
            'has_parent': self.has_parent_(),
            'has_children': self.has_children_(),
            'thread_starter': self.is_thread_starter_(),
            'grouped': self.grouped_(),
            'threaded': self.threaded_(),
            'relationships': {
                'owner_name': self.owner.name if self.owner else None,
                'org_name': self.org.name if self.org else None,
                'sender_name': self.sender.name if self.sender else None,
                'parent_title': self.parent_notification.title if self.parent_notification else None,
                'child_count': len(self.child_notifications or [])
            }
        }
        
        base_dict.update(detailed_info)
        return base_dict
    
    def to_audit_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for audit logging (Rails pattern)"""
        return {
            'id': self.id,
            'uuid': self.uuid,
            'level': self.level.value,
            'status': self.status.value,
            'priority': self.priority.value,
            'resource_type': self.resource_type.value if self.resource_type else None,
            'resource_id': self.resource_id,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'read': self.read_(),
            'dismissed': self.dismissed_(),
            'delivery_attempts': self.delivery_attempts,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def to_delivery_payload(self, channel: str) -> Dict[str, Any]:
        """Convert to delivery payload for specific channel (Rails pattern)"""
        base_payload = {
            'id': self.id,
            'uuid': self.uuid,
            'title': self.title,
            'message': self.message,
            'level': self.level.value,
            'priority': self.priority.value,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id
        }
        
        # Channel-specific customization
        if channel == NotificationChannels.EMAIL.value:
            base_payload['subject'] = self.title
            base_payload['body'] = self.message
            base_payload['html'] = f"<h2>{self.title}</h2><p>{self.message}</p>"
        
        elif channel == NotificationChannels.SMS.value:
            # Truncate for SMS
            base_payload['text'] = f"{self.title}: {self.display_message(140)}"
        
        elif channel == NotificationChannels.WEBHOOK.value:
            base_payload['resource_type'] = self.resource_type.value if self.resource_type else None
            base_payload['resource_id'] = self.resource_id
            base_payload['context'] = self.context
        
        return base_payload
    
    def __repr__(self) -> str:
        return f"<Notification(id={self.id}, level='{self.level.value}', status='{self.status.value}', owner_id={self.owner_id})>"
    
    def __str__(self) -> str:
        return f"Notification: {self.display_title()} ({self.display_level()}) - {self.time_ago()}"