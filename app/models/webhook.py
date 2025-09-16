from datetime import datetime, timedelta
from enum import Enum as PyEnum
import hashlib
import hmac
import json
import secrets
from typing import Dict, List, Optional, Any, Union
import uuid

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class WebhookStatus(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE" 
    PAUSED = "PAUSED"
    DISABLED = "DISABLED"
    PENDING_VERIFICATION = "PENDING_VERIFICATION"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    RATE_LIMITED = "RATE_LIMITED"
    SUSPENDED = "SUSPENDED"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.PAUSED: "Paused", 
            self.DISABLED: "Disabled",
            self.PENDING_VERIFICATION: "Pending Verification",
            self.VERIFICATION_FAILED: "Verification Failed",
            self.RATE_LIMITED: "Rate Limited",
            self.SUSPENDED: "Suspended"
        }.get(self, self.value)
    
    @property
    def is_operational(self) -> bool:
        return self in [self.ACTIVE, self.PAUSED]


class WebhookEvent(PyEnum):
    USER_CREATED = "user.created"
    USER_UPDATED = "user.updated"
    USER_DELETED = "user.deleted"
    ORG_CREATED = "org.created"
    ORG_UPDATED = "org.updated"
    ORG_DELETED = "org.deleted"
    PROJECT_CREATED = "project.created"
    PROJECT_UPDATED = "project.updated"
    PROJECT_PUBLISHED = "project.published"
    PROJECT_DELETED = "project.deleted"
    FLOW_STARTED = "flow.started"
    FLOW_COMPLETED = "flow.completed"
    FLOW_FAILED = "flow.failed"
    DATA_PROCESSED = "data.processed"
    ERROR_OCCURRED = "error.occurred"
    SUBSCRIPTION_CREATED = "subscription.created"
    SUBSCRIPTION_UPDATED = "subscription.updated"
    SUBSCRIPTION_CANCELLED = "subscription.cancelled"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    CUSTOM = "custom"
    
    @property
    def display_name(self) -> str:
        return {
            self.USER_CREATED: "User Created",
            self.USER_UPDATED: "User Updated", 
            self.USER_DELETED: "User Deleted",
            self.ORG_CREATED: "Organization Created",
            self.ORG_UPDATED: "Organization Updated",
            self.ORG_DELETED: "Organization Deleted",
            self.PROJECT_CREATED: "Project Created",
            self.PROJECT_UPDATED: "Project Updated",
            self.PROJECT_PUBLISHED: "Project Published",
            self.PROJECT_DELETED: "Project Deleted",
            self.FLOW_STARTED: "Flow Started",
            self.FLOW_COMPLETED: "Flow Completed",
            self.FLOW_FAILED: "Flow Failed",
            self.DATA_PROCESSED: "Data Processed",
            self.ERROR_OCCURRED: "Error Occurred",
            self.SUBSCRIPTION_CREATED: "Subscription Created",
            self.SUBSCRIPTION_UPDATED: "Subscription Updated", 
            self.SUBSCRIPTION_CANCELLED: "Subscription Cancelled",
            self.PAYMENT_SUCCEEDED: "Payment Succeeded",
            self.PAYMENT_FAILED: "Payment Failed",
            self.CUSTOM: "Custom Event"
        }.get(self, self.value)
    
    @property
    def category(self) -> str:
        if self.value.startswith('user.'):
            return 'user'
        elif self.value.startswith('org.'):
            return 'organization'
        elif self.value.startswith('project.'):
            return 'project'
        elif self.value.startswith('flow.'):
            return 'flow'
        elif self.value.startswith('data.'):
            return 'data'
        elif self.value.startswith('subscription.'):
            return 'billing'
        elif self.value.startswith('payment.'):
            return 'billing'
        elif self.value.startswith('error.'):
            return 'system'
        else:
            return 'custom'


class WebhookDeliveryStatus(PyEnum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    
    @property
    def display_name(self) -> str:
        return {
            self.PENDING: "Pending",
            self.DELIVERED: "Delivered",
            self.FAILED: "Failed", 
            self.RETRYING: "Retrying",
            self.CANCELLED: "Cancelled",
            self.TIMEOUT: "Timeout"
        }.get(self, self.value)


class Webhook(Base):
    __tablename__ = 'webhooks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    webhook_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    url = Column(String(2048), nullable=False)
    secret = Column(String(255), nullable=False)
    
    status = Column(SQLEnum(WebhookStatus), nullable=False, default=WebhookStatus.PENDING_VERIFICATION)
    events = Column(JSON, nullable=False, default=list)
    
    headers = Column(JSON, default=dict)
    timeout_seconds = Column(Integer, default=30)
    retry_attempts = Column(Integer, default=3)
    retry_delay_seconds = Column(Integer, default=60)
    
    active = Column(Boolean, default=True, nullable=False)
    verified = Column(Boolean, default=False, nullable=False)
    
    last_delivery_at = Column(DateTime)
    last_success_at = Column(DateTime)
    last_failure_at = Column(DateTime)
    
    total_deliveries = Column(Integer, default=0, nullable=False)
    successful_deliveries = Column(Integer, default=0, nullable=False)
    failed_deliveries = Column(Integer, default=0, nullable=False)
    
    rate_limit_requests = Column(Integer, default=1000)
    rate_limit_window_seconds = Column(Integer, default=3600)
    current_rate_count = Column(Integer, default=0, nullable=False)
    rate_limit_reset_at = Column(DateTime)
    
    verification_token = Column(String(255))
    verification_challenge = Column(String(255))
    verified_at = Column(DateTime)
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    org = relationship("Org", back_populates="webhooks")
    user = relationship("User", foreign_keys=[user_id])
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    deliveries = relationship("WebhookDelivery", back_populates="webhook", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_webhook_org_id', 'org_id'),
        Index('idx_webhook_status', 'status'),
        Index('idx_webhook_active', 'active'),
        Index('idx_webhook_events', 'events'),
        Index('idx_webhook_last_delivery', 'last_delivery_at'),
        Index('idx_webhook_rate_limit', 'rate_limit_reset_at'),
        UniqueConstraint('org_id', 'name', name='uq_webhook_org_name'),
        CheckConstraint('timeout_seconds > 0 AND timeout_seconds <= 300', name='ck_webhook_timeout'),
        CheckConstraint('retry_attempts >= 0 AND retry_attempts <= 10', name='ck_webhook_retry_attempts'),
        CheckConstraint('rate_limit_requests > 0', name='ck_webhook_rate_limit_positive'),
    )
    
    SUCCESS_RATE_THRESHOLD = 0.95
    RATE_LIMIT_GRACE_PERIOD = 300
    MAX_PAYLOAD_SIZE = 1024 * 1024  # 1MB
    
    def __init__(self, **kwargs):
        if 'secret' not in kwargs:
            kwargs['secret'] = self._generate_secret()
        if 'verification_token' not in kwargs:
            kwargs['verification_token'] = self._generate_verification_token()
        super().__init__(**kwargs)
    
    def __repr__(self):
        return f"<Webhook(id={self.id}, name='{self.name}', org_id={self.org_id}, status='{self.status.value}')>"
    
    @classmethod
    def _generate_secret(cls) -> str:
        return secrets.token_urlsafe(32)
    
    @classmethod
    def _generate_verification_token(cls) -> str:
        return secrets.token_urlsafe(16)
    
    def active_(self) -> bool:
        """Check if webhook is active (Rails pattern)"""
        return (self.active and 
                self.status == WebhookStatus.ACTIVE and
                self.verified and
                not self.rate_limited_())
    
    def operational_(self) -> bool:
        """Check if webhook is operational (Rails pattern)"""
        return self.status.is_operational and self.verified
    
    def verified_(self) -> bool:
        """Check if webhook is verified (Rails pattern)"""
        return self.verified and self.verified_at is not None
    
    def suspended_(self) -> bool:
        """Check if webhook is suspended (Rails pattern)"""
        return self.status == WebhookStatus.SUSPENDED
    
    def rate_limited_(self) -> bool:
        """Check if webhook is rate limited (Rails pattern)"""
        if self.status == WebhookStatus.RATE_LIMITED:
            return True
        if not self.rate_limit_reset_at:
            return False
        return (self.rate_limit_reset_at > datetime.now() and 
                self.current_rate_count >= self.rate_limit_requests)
    
    def pending_verification_(self) -> bool:
        """Check if webhook is pending verification (Rails pattern)"""
        return self.status == WebhookStatus.PENDING_VERIFICATION
    
    def verification_failed_(self) -> bool:
        """Check if webhook verification failed (Rails pattern)"""
        return self.status == WebhookStatus.VERIFICATION_FAILED
    
    def healthy_(self) -> bool:
        """Check if webhook is healthy based on success rate (Rails pattern)"""
        if self.total_deliveries == 0:
            return True
        success_rate = self.successful_deliveries / self.total_deliveries
        return success_rate >= self.SUCCESS_RATE_THRESHOLD
    
    def supports_event_(self, event: WebhookEvent) -> bool:
        """Check if webhook supports specific event (Rails pattern)"""
        return event.value in self.events or 'all' in self.events
    
    def has_recent_activity_(self, hours: int = 24) -> bool:
        """Check if webhook has recent activity (Rails pattern)"""
        if not self.last_delivery_at:
            return False
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.last_delivery_at > cutoff
    
    def needs_attention_(self) -> bool:
        """Check if webhook needs attention (Rails pattern)"""
        return (not self.healthy_() or 
                self.verification_failed_() or
                self.rate_limited_() or
                self.suspended_())
    
    def can_deliver_(self) -> bool:
        """Check if webhook can deliver events (Rails pattern)"""
        return (self.active_() and 
                not self.rate_limited_() and
                not self.suspended_())
    
    def activate_(self, force: bool = False) -> None:
        """Activate webhook (Rails bang method pattern)"""
        if not force and not self.verified_():
            raise ValueError("Cannot activate unverified webhook")
        
        self.active = True
        self.status = WebhookStatus.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate webhook (Rails bang method pattern)"""
        self.active = False
        self.status = WebhookStatus.INACTIVE
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deactivation_reason'] = reason
    
    def pause_(self, reason: str = None) -> None:
        """Pause webhook temporarily (Rails bang method pattern)"""
        self.status = WebhookStatus.PAUSED
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['pause_reason'] = reason
    
    def resume_(self) -> None:
        """Resume paused webhook (Rails bang method pattern)"""
        if self.status == WebhookStatus.PAUSED:
            self.status = WebhookStatus.ACTIVE if self.verified else WebhookStatus.PENDING_VERIFICATION
            self.updated_at = datetime.now()
    
    def suspend_(self, reason: str) -> None:
        """Suspend webhook for violations (Rails bang method pattern)"""
        self.status = WebhookStatus.SUSPENDED
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['suspension_reason'] = reason
        self.extra_metadata['suspended_at'] = datetime.now().isoformat()
    
    def verify_(self, challenge_response: str = None) -> bool:
        """Verify webhook (Rails bang method pattern)"""
        if challenge_response and challenge_response == self.verification_challenge:
            self.verified = True
            self.verified_at = datetime.now()
            self.status = WebhookStatus.ACTIVE if self.active else WebhookStatus.INACTIVE
            self.verification_challenge = None
            return True
        return False
    
    def regenerate_secret_(self) -> str:
        """Regenerate webhook secret (Rails bang method pattern)"""
        new_secret = self._generate_secret()
        self.secret = new_secret
        self.updated_at = datetime.now()
        return new_secret
    
    def add_event_(self, event: WebhookEvent) -> None:
        """Add event subscription (Rails bang method pattern)"""
        if event.value not in self.events:
            self.events = self.events + [event.value]
            self.updated_at = datetime.now()
    
    def remove_event_(self, event: WebhookEvent) -> None:
        """Remove event subscription (Rails bang method pattern)"""
        if event.value in self.events:
            events_list = list(self.events)
            events_list.remove(event.value)
            self.events = events_list
            self.updated_at = datetime.now()
    
    def increment_rate_count_(self) -> bool:
        """Increment rate limit counter (Rails bang method pattern)"""
        now = datetime.now()
        
        if not self.rate_limit_reset_at or self.rate_limit_reset_at <= now:
            self.current_rate_count = 0
            self.rate_limit_reset_at = now + timedelta(seconds=self.rate_limit_window_seconds)
        
        self.current_rate_count += 1
        
        if self.current_rate_count > self.rate_limit_requests:
            self.status = WebhookStatus.RATE_LIMITED
            return False
        
        return True
    
    def reset_rate_limit_(self) -> None:
        """Reset rate limit manually (Rails bang method pattern)"""
        self.current_rate_count = 0
        self.rate_limit_reset_at = None
        if self.status == WebhookStatus.RATE_LIMITED:
            self.status = WebhookStatus.ACTIVE
    
    def record_delivery_(self, success: bool, response_code: int = None, 
                        response_time_ms: int = None, error_message: str = None) -> None:
        """Record delivery attempt (Rails bang method pattern)"""
        now = datetime.now()
        self.total_deliveries += 1
        self.last_delivery_at = now
        
        if success:
            self.successful_deliveries += 1
            self.last_success_at = now
        else:
            self.failed_deliveries += 1
            self.last_failure_at = now
        
        if not self.healthy_() and self.status == WebhookStatus.ACTIVE:
            self.status = WebhookStatus.PAUSED
        
        self.updated_at = now
    
    def generate_signature(self, payload: str) -> str:
        """Generate HMAC signature for payload (Rails pattern)"""
        return hmac.new(
            self.secret.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def validate_signature(self, payload: str, signature: str) -> bool:
        """Validate HMAC signature (Rails pattern)"""
        expected_signature = self.generate_signature(payload)
        return hmac.compare_digest(expected_signature, signature)
    
    def create_delivery_payload(self, event: WebhookEvent, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create delivery payload (Rails pattern)"""
        return {
            'id': str(uuid.uuid4()),
            'event': event.value,
            'timestamp': datetime.now().isoformat(),
            'webhook_id': self.webhook_id,
            'org_id': self.org_id,
            'data': data,
            'metadata': {
                'delivery_attempt': 1,
                'webhook_name': self.name
            }
        }
    
    def success_rate(self) -> float:
        """Calculate success rate (Rails pattern)"""
        if self.total_deliveries == 0:
            return 1.0
        return self.successful_deliveries / self.total_deliveries
    
    def failure_rate(self) -> float:
        """Calculate failure rate (Rails pattern)"""
        return 1.0 - self.success_rate()
    
    def avg_response_time_ms(self) -> Optional[float]:
        """Calculate average response time (Rails pattern)"""
        if not self.deliveries:
            return None
        
        response_times = [d.response_time_ms for d in self.deliveries if d.response_time_ms]
        if not response_times:
            return None
        
        return sum(response_times) / len(response_times)
    
    def recent_deliveries(self, hours: int = 24) -> List['WebhookDelivery']:
        """Get recent deliveries (Rails pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return [d for d in self.deliveries if d.created_at > cutoff]
    
    def deliveries_for_event(self, event: WebhookEvent) -> List['WebhookDelivery']:
        """Get deliveries for specific event (Rails pattern)"""
        return [d for d in self.deliveries if d.event == event.value]
    
    def health_report(self) -> Dict[str, Any]:
        """Generate health report (Rails pattern)"""
        return {
            'webhook_id': self.webhook_id,
            'healthy': self.healthy_(),
            'active': self.active_(),
            'verified': self.verified_(),
            'rate_limited': self.rate_limited_(),
            'success_rate': self.success_rate(),
            'total_deliveries': self.total_deliveries,
            'recent_activity': self.has_recent_activity_(),
            'needs_attention': self.needs_attention_(),
            'avg_response_time_ms': self.avg_response_time_ms(),
            'last_success': self.last_success_at.isoformat() if self.last_success_at else None,
            'last_failure': self.last_failure_at.isoformat() if self.last_failure_at else None
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'webhook_id': self.webhook_id,
            'name': self.name,
            'description': self.description,
            'url': self.url,
            'status': self.status.value,
            'events': self.events,
            'active': self.active,
            'verified': self.verified,
            'success_rate': self.success_rate(),
            'total_deliveries': self.total_deliveries,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'secret': self.secret,
                'headers': self.headers,
                'verification_token': self.verification_token
            })
        
        return result


class WebhookDelivery(Base):
    __tablename__ = 'webhook_deliveries'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    delivery_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    webhook_id = Column(Integer, ForeignKey('webhooks.id'), nullable=False)
    event = Column(String(100), nullable=False)
    
    payload = Column(Text, nullable=False)
    signature = Column(String(255))
    
    status = Column(SQLEnum(WebhookDeliveryStatus), nullable=False, default=WebhookDeliveryStatus.PENDING)
    
    attempt_count = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=3, nullable=False)
    
    response_code = Column(Integer)
    response_body = Column(Text)
    response_time_ms = Column(Integer)
    
    error_message = Column(Text)
    
    scheduled_at = Column(DateTime, default=datetime.now, nullable=False)
    delivered_at = Column(DateTime)
    failed_at = Column(DateTime)
    next_retry_at = Column(DateTime)
    
    extra_metadata = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    webhook = relationship("Webhook", back_populates="deliveries")
    
    __table_args__ = (
        Index('idx_webhook_delivery_webhook_id', 'webhook_id'),
        Index('idx_webhook_delivery_status', 'status'),
        Index('idx_webhook_delivery_event', 'event'),
        Index('idx_webhook_delivery_scheduled', 'scheduled_at'),
        Index('idx_webhook_delivery_next_retry', 'next_retry_at'),
        CheckConstraint('attempt_count >= 0', name='ck_delivery_attempt_count'),
        CheckConstraint('max_attempts > 0', name='ck_delivery_max_attempts'),
    )
    
    def __repr__(self):
        return f"<WebhookDelivery(id={self.id}, webhook_id={self.webhook_id}, event='{self.event}', status='{self.status.value}')>"
    
    def pending_(self) -> bool:
        """Check if delivery is pending (Rails pattern)"""
        return self.status == WebhookDeliveryStatus.PENDING
    
    def delivered_(self) -> bool:
        """Check if delivery was successful (Rails pattern)"""
        return self.status == WebhookDeliveryStatus.DELIVERED
    
    def failed_(self) -> bool:
        """Check if delivery failed (Rails pattern)"""
        return self.status == WebhookDeliveryStatus.FAILED
    
    def retrying_(self) -> bool:
        """Check if delivery is retrying (Rails pattern)"""
        return self.status == WebhookDeliveryStatus.RETRYING
    
    def cancelled_(self) -> bool:
        """Check if delivery was cancelled (Rails pattern)"""
        return self.status == WebhookDeliveryStatus.CANCELLED
    
    def timed_out_(self) -> bool:
        """Check if delivery timed out (Rails pattern)"""
        return self.status == WebhookDeliveryStatus.TIMEOUT
    
    def can_retry_(self) -> bool:
        """Check if delivery can be retried (Rails pattern)"""
        return (self.attempt_count < self.max_attempts and 
                self.status in [WebhookDeliveryStatus.FAILED, WebhookDeliveryStatus.TIMEOUT])
    
    def should_retry_(self) -> bool:
        """Check if delivery should be retried now (Rails pattern)"""
        return (self.can_retry_() and 
                self.next_retry_at and 
                self.next_retry_at <= datetime.now())
    
    def mark_delivered_(self, response_code: int, response_body: str = None, 
                       response_time_ms: int = None) -> None:
        """Mark delivery as successful (Rails bang method pattern)"""
        self.status = WebhookDeliveryStatus.DELIVERED
        self.delivered_at = datetime.now()
        self.response_code = response_code
        self.response_body = response_body
        self.response_time_ms = response_time_ms
        self.updated_at = datetime.now()
    
    def mark_failed_(self, error_message: str, response_code: int = None, 
                    response_body: str = None, response_time_ms: int = None) -> None:
        """Mark delivery as failed (Rails bang method pattern)"""
        self.status = WebhookDeliveryStatus.FAILED
        self.failed_at = datetime.now()
        self.error_message = error_message
        self.response_code = response_code
        self.response_body = response_body
        self.response_time_ms = response_time_ms
        
        if self.can_retry_():
            self.schedule_retry_()
        
        self.updated_at = datetime.now()
    
    def mark_timeout_(self) -> None:
        """Mark delivery as timed out (Rails bang method pattern)"""
        self.status = WebhookDeliveryStatus.TIMEOUT
        self.failed_at = datetime.now()
        self.error_message = "Request timed out"
        
        if self.can_retry_():
            self.schedule_retry_()
        
        self.updated_at = datetime.now()
    
    def cancel_(self, reason: str = None) -> None:
        """Cancel delivery (Rails bang method pattern)"""
        self.status = WebhookDeliveryStatus.CANCELLED
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['cancellation_reason'] = reason
    
    def schedule_retry_(self, delay_seconds: int = None) -> None:
        """Schedule retry attempt (Rails bang method pattern)"""
        if not self.can_retry_():
            return
        
        self.status = WebhookDeliveryStatus.RETRYING
        self.attempt_count += 1
        
        if delay_seconds is None:
            delay_seconds = min(60 * (2 ** (self.attempt_count - 1)), 3600)
        
        self.next_retry_at = datetime.now() + timedelta(seconds=delay_seconds)
        self.updated_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        return {
            'id': self.id,
            'delivery_id': self.delivery_id,
            'webhook_id': self.webhook_id,
            'event': self.event,
            'status': self.status.value,
            'attempt_count': self.attempt_count,
            'response_code': self.response_code,
            'response_time_ms': self.response_time_ms,
            'error_message': self.error_message,
            'scheduled_at': self.scheduled_at.isoformat(),
            'delivered_at': self.delivered_at.isoformat() if self.delivered_at else None,
            'next_retry_at': self.next_retry_at.isoformat() if self.next_retry_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }