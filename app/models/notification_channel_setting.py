"""
NotificationChannelSetting Model - User notification preferences and channel settings.
Manages notification delivery preferences across different channels with Rails business logic patterns.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum as PyEnum
import json
from ..database import Base


class NotificationChannel(PyEnum):
    """Notification channel enumeration"""
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"
    SLACK = "SLACK"
    WEBHOOK = "WEBHOOK"
    IN_APP = "IN_APP"
    DESKTOP = "DESKTOP"


class NotificationFrequency(PyEnum):
    """Notification frequency enumeration"""
    REAL_TIME = "REAL_TIME"
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    NEVER = "NEVER"


class NotificationType(PyEnum):
    """Notification type enumeration"""
    SYSTEM_ALERT = "SYSTEM_ALERT"
    DATA_PIPELINE = "DATA_PIPELINE"
    SECURITY = "SECURITY"
    BILLING = "BILLING"
    USER_ACTIVITY = "USER_ACTIVITY"
    API_USAGE = "API_USAGE"
    MAINTENANCE = "MAINTENANCE"
    MARKETING = "MARKETING"


class NotificationChannelSetting(Base):
    __tablename__ = "notification_channel_settings"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign keys
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=True, index=True)
    
    # Channel configuration
    channel = Column(SQLEnum(NotificationChannel), nullable=False, index=True)
    notification_type = Column(SQLEnum(NotificationType), nullable=False, index=True)
    frequency = Column(SQLEnum(NotificationFrequency), default=NotificationFrequency.REAL_TIME, index=True)
    
    # Channel settings
    is_enabled = Column(Boolean, default=True, index=True)
    is_muted = Column(Boolean, default=False, index=True)
    muted_until = Column(DateTime, nullable=True)
    
    # Delivery settings
    delivery_address = Column(String(255))  # Email, phone, webhook URL, etc.
    delivery_config = Column(JSON)  # Channel-specific configuration
    
    # Content preferences
    include_details = Column(Boolean, default=True)
    include_attachments = Column(Boolean, default=False)
    template_id = Column(String(100))  # Custom template for notifications
    
    # Filtering settings
    filter_rules = Column(JSON)  # JSON rules for filtering notifications
    priority_threshold = Column(String(20), default="LOW")  # LOW, MEDIUM, HIGH, CRITICAL
    keyword_filters = Column(Text)  # Comma-separated keywords
    
    # Quiet hours
    quiet_hours_enabled = Column(Boolean, default=False)
    quiet_hours_start = Column(String(8))  # HH:MM:SS format
    quiet_hours_end = Column(String(8))  # HH:MM:SS format
    quiet_hours_timezone = Column(String(50), default="UTC")
    
    # Rate limiting
    rate_limit_enabled = Column(Boolean, default=False)
    max_notifications_per_hour = Column(Integer, default=60)
    max_notifications_per_day = Column(Integer, default=500)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    last_notification_at = Column(DateTime)
    last_delivery_attempt_at = Column(DateTime)
    
    # Relationships
    user = relationship("User", back_populates="notification_channel_settings")
    org = relationship("Org", foreign_keys=[org_id])
    
    # Rails business logic constants
    DEFAULT_RATE_LIMITS = {
        NotificationChannel.EMAIL: {'hourly': 20, 'daily': 100},
        NotificationChannel.SMS: {'hourly': 5, 'daily': 20},
        NotificationChannel.PUSH: {'hourly': 50, 'daily': 200},
        NotificationChannel.SLACK: {'hourly': 30, 'daily': 150},
        NotificationChannel.WEBHOOK: {'hourly': 100, 'daily': 1000}
    }
    
    PRIORITY_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    MAX_KEYWORD_FILTERS = 50
    
    # Rails predicate methods
    def enabled_(self) -> bool:
        """Rails predicate: Check if notifications are enabled"""
        return self.is_enabled and not self.muted_()
    
    def muted_(self) -> bool:
        """Rails predicate: Check if notifications are muted"""
        if not self.is_muted:
            return False
        
        if self.muted_until and self.muted_until <= datetime.utcnow():
            # Automatically unmute if mute period has expired
            self.is_muted = False
            self.muted_until = None
            return False
        
        return True
    
    def rate_limited_(self) -> bool:
        """Rails predicate: Check if currently rate limited"""
        if not self.rate_limit_enabled:
            return False
        
        now = datetime.utcnow()
        
        # Check hourly limit
        hour_ago = now - timedelta(hours=1)
        hourly_count = self._get_notification_count_since(hour_ago)
        if hourly_count >= self.max_notifications_per_hour:
            return True
        
        # Check daily limit
        day_ago = now - timedelta(days=1)
        daily_count = self._get_notification_count_since(day_ago)
        if daily_count >= self.max_notifications_per_day:
            return True
        
        return False
    
    def in_quiet_hours_(self) -> bool:
        """Rails predicate: Check if currently in quiet hours"""
        if not self.quiet_hours_enabled or not self.quiet_hours_start or not self.quiet_hours_end:
            return False
        
        # This would implement timezone-aware quiet hours checking
        # For now, simplified implementation
        from datetime import time
        try:
            start_time = time.fromisoformat(self.quiet_hours_start)
            end_time = time.fromisoformat(self.quiet_hours_end)
            current_time = datetime.utcnow().time()
            
            if start_time <= end_time:
                return start_time <= current_time <= end_time
            else:  # Quiet hours span midnight
                return current_time >= start_time or current_time <= end_time
        except:
            return False
    
    def should_deliver_(self, notification_data: Dict[str, Any] = None) -> bool:
        """Rails predicate: Check if notification should be delivered"""
        if not self.enabled_():
            return False
        
        if self.muted_() or self.rate_limited_() or self.in_quiet_hours_():
            return False
        
        if notification_data:
            if not self._passes_priority_filter(notification_data):
                return False
            
            if not self._passes_keyword_filter(notification_data):
                return False
            
            if not self._passes_custom_filters(notification_data):
                return False
        
        return True
    
    def recent_activity_(self, hours: int = 24) -> bool:
        """Rails predicate: Check if has recent notification activity"""
        if not self.last_notification_at:
            return False
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return self.last_notification_at >= cutoff
    
    # Rails business logic methods
    def mute_for(self, duration_minutes: int) -> None:
        """Mute notifications for specified duration (Rails pattern)"""
        self.is_muted = True
        self.muted_until = datetime.utcnow() + timedelta(minutes=duration_minutes)
    
    def unmute(self) -> None:
        """Unmute notifications (Rails pattern)"""
        self.is_muted = False
        self.muted_until = None
    
    def set_quiet_hours(self, start_time: str, end_time: str, timezone: str = "UTC") -> bool:
        """Set quiet hours for notifications (Rails pattern)"""
        try:
            # Validate time format
            from datetime import time
            time.fromisoformat(start_time)
            time.fromisoformat(end_time)
            
            self.quiet_hours_start = start_time
            self.quiet_hours_end = end_time
            self.quiet_hours_timezone = timezone
            self.quiet_hours_enabled = True
            return True
        except:
            return False
    
    def clear_quiet_hours(self) -> None:
        """Clear quiet hours settings (Rails pattern)"""
        self.quiet_hours_enabled = False
        self.quiet_hours_start = None
        self.quiet_hours_end = None
    
    def update_delivery_address(self, address: str) -> bool:
        """Update delivery address with validation (Rails pattern)"""
        if self.channel == NotificationChannel.EMAIL:
            # Basic email validation
            if '@' not in address or '.' not in address:
                return False
        elif self.channel == NotificationChannel.SMS:
            # Basic phone validation
            if not address.replace('+', '').replace('-', '').replace(' ', '').isdigit():
                return False
        elif self.channel == NotificationChannel.WEBHOOK:
            # Basic URL validation
            if not address.startswith(('http://', 'https://')):
                return False
        
        self.delivery_address = address
        return True
    
    def add_keyword_filter(self, keyword: str) -> bool:
        """Add keyword filter (Rails pattern)"""
        current_keywords = self.get_keyword_filters()
        if len(current_keywords) >= self.MAX_KEYWORD_FILTERS:
            return False
        
        if keyword not in current_keywords:
            current_keywords.append(keyword.lower())
            self.keyword_filters = ",".join(current_keywords)
            return True
        return False
    
    def remove_keyword_filter(self, keyword: str) -> bool:
        """Remove keyword filter (Rails pattern)"""
        current_keywords = self.get_keyword_filters()
        keyword_lower = keyword.lower()
        
        if keyword_lower in current_keywords:
            current_keywords.remove(keyword_lower)
            self.keyword_filters = ",".join(current_keywords)
            return True
        return False
    
    def get_keyword_filters(self) -> List[str]:
        """Get list of keyword filters (Rails pattern)"""
        if not self.keyword_filters:
            return []
        return [k.strip() for k in self.keyword_filters.split(",") if k.strip()]
    
    def update_rate_limits(self, hourly_limit: int = None, daily_limit: int = None) -> None:
        """Update rate limiting settings (Rails pattern)"""
        if hourly_limit is not None:
            self.max_notifications_per_hour = max(0, hourly_limit)
        
        if daily_limit is not None:
            self.max_notifications_per_day = max(0, daily_limit)
        
        self.rate_limit_enabled = (self.max_notifications_per_hour > 0 or 
                                  self.max_notifications_per_day > 0)
    
    def set_priority_threshold(self, threshold: str) -> bool:
        """Set priority threshold for notifications (Rails pattern)"""
        if threshold.upper() in self.PRIORITY_LEVELS:
            self.priority_threshold = threshold.upper()
            return True
        return False
    
    def _passes_priority_filter(self, notification_data: Dict[str, Any]) -> bool:
        """Check if notification passes priority filter (helper)"""
        notification_priority = notification_data.get('priority', 'LOW').upper()
        threshold_index = self.PRIORITY_LEVELS.index(self.priority_threshold)
        notification_index = self.PRIORITY_LEVELS.index(notification_priority)
        return notification_index >= threshold_index
    
    def _passes_keyword_filter(self, notification_data: Dict[str, Any]) -> bool:
        """Check if notification passes keyword filter (helper)"""
        keyword_filters = self.get_keyword_filters()
        if not keyword_filters:
            return True
        
        notification_text = (
            str(notification_data.get('title', '')) + ' ' +
            str(notification_data.get('message', ''))
        ).lower()
        
        # Any keyword match means it passes the filter
        return any(keyword in notification_text for keyword in keyword_filters)
    
    def _passes_custom_filters(self, notification_data: Dict[str, Any]) -> bool:
        """Check if notification passes custom filters (helper)"""
        if not self.filter_rules:
            return True
        
        try:
            rules = json.loads(self.filter_rules) if isinstance(self.filter_rules, str) else self.filter_rules
            # This would implement custom filtering logic based on JSON rules
            # For now, simplified implementation that always passes
            return True
        except:
            return True
    
    def _get_notification_count_since(self, since: datetime) -> int:
        """Get notification count since timestamp (helper)"""
        # This would query notification history when available
        # For now, return 0 as placeholder
        return 0
    
    def record_notification_sent(self) -> None:
        """Record that a notification was sent (Rails pattern)"""
        self.last_notification_at = datetime.utcnow()
    
    def record_delivery_attempt(self) -> None:
        """Record delivery attempt (Rails pattern)"""
        self.last_delivery_attempt_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert setting to dictionary for API responses"""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'org_id': self.org_id,
            'channel': self.channel.value if self.channel else None,
            'notification_type': self.notification_type.value if self.notification_type else None,
            'frequency': self.frequency.value if self.frequency else None,
            'is_enabled': self.is_enabled,
            'is_muted': self.is_muted,
            'muted_until': self.muted_until.isoformat() if self.muted_until else None,
            'delivery_address': self.delivery_address,
            'include_details': self.include_details,
            'include_attachments': self.include_attachments,
            'priority_threshold': self.priority_threshold,
            'keyword_filters': self.get_keyword_filters(),
            'quiet_hours_enabled': self.quiet_hours_enabled,
            'quiet_hours_start': self.quiet_hours_start,
            'quiet_hours_end': self.quiet_hours_end,
            'quiet_hours_timezone': self.quiet_hours_timezone,
            'rate_limit_enabled': self.rate_limit_enabled,
            'max_notifications_per_hour': self.max_notifications_per_hour,
            'max_notifications_per_day': self.max_notifications_per_day,
            'last_notification_at': self.last_notification_at.isoformat() if self.last_notification_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'enabled': self.enabled_(),
            'muted': self.muted_(),
            'rate_limited': self.rate_limited_(),
            'in_quiet_hours': self.in_quiet_hours_(),
            'recent_activity': self.recent_activity_()
        }
    
    @classmethod
    def create_default_settings(cls, user, org=None):
        """Create default notification settings for user (Rails pattern)"""
        default_settings = []
        
        # Create default settings for each notification type and important channels
        important_types = [NotificationType.SYSTEM_ALERT, NotificationType.SECURITY, NotificationType.DATA_PIPELINE]
        important_channels = [NotificationChannel.EMAIL, NotificationChannel.IN_APP]
        
        for notification_type in important_types:
            for channel in important_channels:
                setting = cls(
                    user_id=user.id if hasattr(user, 'id') else user,
                    org_id=org.id if org and hasattr(org, 'id') else org,
                    channel=channel,
                    notification_type=notification_type,
                    frequency=NotificationFrequency.REAL_TIME,
                    is_enabled=True
                )
                
                # Set default rate limits based on channel
                if channel in cls.DEFAULT_RATE_LIMITS:
                    limits = cls.DEFAULT_RATE_LIMITS[channel]
                    setting.max_notifications_per_hour = limits['hourly']
                    setting.max_notifications_per_day = limits['daily']
                    setting.rate_limit_enabled = True
                
                default_settings.append(setting)
        
        return default_settings
    
    @classmethod
    def find_user_settings(cls, user_id: int, channel: NotificationChannel = None, 
                          notification_type: NotificationType = None, session=None):
        """Find user notification settings (Rails pattern)"""
        # This would query settings when session is available
        # For now, return empty list as placeholder
        return []