"""
FeatureFlag Model - Feature toggling and A/B testing framework.
Enables controlled feature rollouts, experimentation, and dynamic configuration.
Implements Rails feature flag patterns with targeting rules and analytics.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Index, Float
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import hashlib
import logging
from ..database import Base

logger = logging.getLogger(__name__)

class FeatureFlagType(PyEnum):
    """Feature flag type enumeration"""
    BOOLEAN = "boolean"         # Simple on/off flag
    ROLLOUT = "rollout"        # Percentage-based rollout
    TARGETING = "targeting"     # User/group targeting
    EXPERIMENT = "experiment"   # A/B testing experiment
    KILL_SWITCH = "kill_switch" # Emergency disable
    CONFIGURATION = "configuration" # Dynamic config values
    
    @property
    def display_name(self) -> str:
        return {
            self.BOOLEAN: "Boolean Flag",
            self.ROLLOUT: "Percentage Rollout",
            self.TARGETING: "Targeted Release",
            self.EXPERIMENT: "A/B Experiment",
            self.KILL_SWITCH: "Kill Switch",
            self.CONFIGURATION: "Configuration Flag"
        }.get(self, "Unknown Type")

class FeatureFlagStatus(PyEnum):
    """Feature flag status enumeration"""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ARCHIVED = "archived"
    DISABLED = "disabled"
    
    @property
    def display_name(self) -> str:
        return {
            self.DRAFT: "Draft",
            self.ACTIVE: "Active",
            self.PAUSED: "Paused",
            self.COMPLETED: "Completed",
            self.ARCHIVED: "Archived",
            self.DISABLED: "Disabled"
        }.get(self, "Unknown Status")

class FeatureFlagEnvironment(PyEnum):
    """Feature flag environment enumeration"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"
    
    @property
    def display_name(self) -> str:
        return {
            self.DEVELOPMENT: "Development",
            self.STAGING: "Staging",
            self.PRODUCTION: "Production",
            self.TESTING: "Testing"
        }.get(self, "Unknown Environment")

class FeatureFlag(Base):
    __tablename__ = "feature_flags"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(255), nullable=False, index=True, unique=True)  # e.g., 'new_dashboard_ui'
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Flag configuration
    flag_type = Column(SQLEnum(FeatureFlagType), nullable=False, index=True)
    status = Column(SQLEnum(FeatureFlagStatus), default=FeatureFlagStatus.DRAFT, index=True)
    environment = Column(SQLEnum(FeatureFlagEnvironment), default=FeatureFlagEnvironment.DEVELOPMENT, index=True)
    
    # Boolean flag settings
    default_value = Column(Boolean, default=False, index=True)
    
    # Rollout settings
    rollout_percentage = Column(Float, default=0.0, index=True)  # 0-100
    rollout_increment = Column(Float, default=10.0)  # How much to increase per step
    max_rollout = Column(Float, default=100.0)  # Maximum rollout percentage
    
    # Targeting rules
    targeting_rules = Column(JSON)  # Complex targeting logic
    user_whitelist = Column(JSON)   # Specific user IDs always enabled
    user_blacklist = Column(JSON)   # Specific user IDs always disabled
    org_whitelist = Column(JSON)    # Specific org IDs always enabled
    
    # Experiment settings (for A/B testing)
    experiment_variants = Column(JSON)  # Different variants and their weights
    experiment_goal = Column(String(255))  # Success metric
    experiment_hypothesis = Column(Text)   # What we're testing
    
    # Dynamic configuration
    config_value = Column(JSON)  # For configuration-type flags
    
    # Lifecycle management
    start_date = Column(DateTime, index=True)
    end_date = Column(DateTime, index=True)
    auto_disable_date = Column(DateTime, index=True)
    
    # Approval and governance
    requires_approval = Column(Boolean, default=False)
    approved_by_id = Column(Integer, ForeignKey("users.id"))
    approved_at = Column(DateTime)
    
    # Analytics and tracking
    impression_count = Column(Integer, default=0)
    conversion_count = Column(Integer, default=0)
    last_evaluated_at = Column(DateTime)
    
    # Organization and ownership
    org_id = Column(Integer, ForeignKey("orgs.id"), index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    
    # State flags
    is_permanent = Column(Boolean, default=False, index=True)
    is_global = Column(Boolean, default=False, index=True)
    is_sensitive = Column(Boolean, default=False, index=True)
    can_override = Column(Boolean, default=True)
    
    # Metadata
    tags = Column(JSON)
    extra_metadata = Column(JSON)
    dependencies = Column(JSON)  # Other flags this depends on
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    org = relationship("Org", foreign_keys=[org_id])
    owner = relationship("User", foreign_keys=[owner_id])
    approved_by = relationship("User", foreign_keys=[approved_by_id])
    team = relationship("Team", foreign_keys=[team_id])
    
    # Enhanced database indexes
    __table_args__ = (
        Index('idx_feature_flags_key_env', 'key', 'environment'),
        Index('idx_feature_flags_status_env', 'status', 'environment'),
        Index('idx_feature_flags_type_status', 'flag_type', 'status'),
        Index('idx_feature_flags_org_status', 'org_id', 'status'),
        Index('idx_feature_flags_owner_status', 'owner_id', 'status'),
        Index('idx_feature_flags_rollout', 'rollout_percentage', 'status'),
        Index('idx_feature_flags_dates', 'start_date', 'end_date'),
        Index('idx_feature_flags_approval', 'requires_approval', 'approved_at'),
        Index('idx_feature_flags_global', 'is_global', 'status'),
        Index('idx_feature_flags_permanent', 'is_permanent', 'status'),
    )
    
    # Rails constants
    MAX_ROLLOUT_PERCENTAGE = 100.0
    MIN_ROLLOUT_PERCENTAGE = 0.0
    DEFAULT_ROLLOUT_INCREMENT = 10.0
    IMPRESSION_TRACKING_THRESHOLD = 1000
    HASH_SEED = "feature_flag_hash_seed"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Initialize targeting rules if not provided
        if not self.targeting_rules:
            self.targeting_rules = {}
        
        # Set start date if not provided and flag is active
        if not self.start_date and self.status == FeatureFlagStatus.ACTIVE:
            self.start_date = datetime.now()
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if feature flag is active (Rails pattern)"""
        return (self.status == FeatureFlagStatus.ACTIVE and 
                not self.expired_() and 
                self.within_date_range_())
    
    def draft_(self) -> bool:
        """Check if feature flag is in draft (Rails pattern)"""
        return self.status == FeatureFlagStatus.DRAFT
    
    def paused_(self) -> bool:
        """Check if feature flag is paused (Rails pattern)"""
        return self.status == FeatureFlagStatus.PAUSED
    
    def completed_(self) -> bool:
        """Check if feature flag is completed (Rails pattern)"""
        return self.status == FeatureFlagStatus.COMPLETED
    
    def archived_(self) -> bool:
        """Check if feature flag is archived (Rails pattern)"""
        return self.status == FeatureFlagStatus.ARCHIVED
    
    def disabled_(self) -> bool:
        """Check if feature flag is disabled (Rails pattern)"""
        return self.status == FeatureFlagStatus.DISABLED
    
    def expired_(self) -> bool:
        """Check if feature flag has expired (Rails pattern)"""
        if self.end_date and self.end_date < datetime.now():
            return True
        if self.auto_disable_date and self.auto_disable_date < datetime.now():
            return True
        return False
    
    def within_date_range_(self) -> bool:
        """Check if current time is within flag's date range (Rails pattern)"""
        now = datetime.now()
        
        if self.start_date and now < self.start_date:
            return False
        
        if self.end_date and now > self.end_date:
            return False
        
        return True
    
    def boolean_flag_(self) -> bool:
        """Check if flag is boolean type (Rails pattern)"""
        return self.flag_type == FeatureFlagType.BOOLEAN
    
    def rollout_flag_(self) -> bool:
        """Check if flag is rollout type (Rails pattern)"""
        return self.flag_type == FeatureFlagType.ROLLOUT
    
    def experiment_flag_(self) -> bool:
        """Check if flag is experiment type (Rails pattern)"""
        return self.flag_type == FeatureFlagType.EXPERIMENT
    
    def kill_switch_(self) -> bool:
        """Check if flag is kill switch type (Rails pattern)"""
        return self.flag_type == FeatureFlagType.KILL_SWITCH
    
    def targeting_flag_(self) -> bool:
        """Check if flag uses targeting (Rails pattern)"""
        return self.flag_type == FeatureFlagType.TARGETING
    
    def configuration_flag_(self) -> bool:
        """Check if flag is configuration type (Rails pattern)"""
        return self.flag_type == FeatureFlagType.CONFIGURATION
    
    def permanent_(self) -> bool:
        """Check if flag is permanent (Rails pattern)"""
        return self.is_permanent
    
    def global_(self) -> bool:
        """Check if flag is global (Rails pattern)"""
        return self.is_global
    
    def sensitive_(self) -> bool:
        """Check if flag is sensitive (Rails pattern)"""
        return self.is_sensitive
    
    def requires_approval_(self) -> bool:
        """Check if flag requires approval (Rails pattern)"""
        return self.requires_approval
    
    def approved_(self) -> bool:
        """Check if flag is approved (Rails pattern)"""
        return self.approved_at is not None
    
    def has_dependencies_(self) -> bool:
        """Check if flag has dependencies (Rails pattern)"""
        return bool(self.dependencies)
    
    def fully_rolled_out_(self) -> bool:
        """Check if flag is fully rolled out (Rails pattern)"""
        return self.rollout_percentage >= self.max_rollout
    
    def recently_updated_(self, hours: int = 24) -> bool:
        """Check if flag was recently updated (Rails pattern)"""
        if not self.updated_at:
            return False
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.updated_at >= cutoff
    
    def high_impressions_(self) -> bool:
        """Check if flag has high impression count (Rails pattern)"""
        return self.impression_count >= self.IMPRESSION_TRACKING_THRESHOLD
    
    def converting_(self) -> bool:
        """Check if flag has conversions (Rails pattern)"""
        return self.conversion_count > 0
    
    def conversion_rate_(self) -> float:
        """Calculate conversion rate (Rails pattern)"""
        if self.impression_count == 0:
            return 0.0
        return (self.conversion_count / self.impression_count) * 100
    
    def enabled_for_user_(self, user_id: int, org_id: int = None, context: Dict[str, Any] = None) -> bool:
        """Check if flag is enabled for specific user (Rails pattern)"""
        if not self.active_():
            return self.default_value
        
        # Check blacklist first
        if self.user_blacklist and user_id in self.user_blacklist:
            return False
        
        # Check whitelist
        if self.user_whitelist and user_id in self.user_whitelist:
            return True
        
        # Check org whitelist
        if org_id and self.org_whitelist and org_id in self.org_whitelist:
            return True
        
        # Apply targeting rules
        if self.targeting_flag_() and self.targeting_rules:
            return self._evaluate_targeting_rules(user_id, org_id, context or {})
        
        # Apply rollout percentage
        if self.rollout_flag_():
            return self._is_user_in_rollout(user_id)
        
        # For experiments, check variant assignment
        if self.experiment_flag_():
            variant = self.get_experiment_variant(user_id)
            return variant != 'control'
        
        return self.default_value
    
    def get_value_for_user_(self, user_id: int, org_id: int = None, context: Dict[str, Any] = None) -> Any:
        """Get flag value for specific user (Rails pattern)"""
        if self.configuration_flag_():
            if self.enabled_for_user_(user_id, org_id, context):
                return self.config_value
            return None
        
        return self.enabled_for_user_(user_id, org_id, context)
    
    # Rails bang methods
    def activate_(self, activated_by_id: int = None) -> None:
        """Activate feature flag (Rails bang method pattern)"""
        if self.active_():
            return
        
        self.status = FeatureFlagStatus.ACTIVE
        if not self.start_date:
            self.start_date = datetime.now()
        self.updated_at = datetime.now()
        
        self._log_status_change('activated', activated_by_id)
    
    def pause_(self, paused_by_id: int = None, reason: str = None) -> None:
        """Pause feature flag (Rails bang method pattern)"""
        if self.paused_():
            return
        
        self.status = FeatureFlagStatus.PAUSED
        self.updated_at = datetime.now()
        
        self._log_status_change('paused', paused_by_id, reason)
    
    def resume_(self, resumed_by_id: int = None) -> None:
        """Resume paused feature flag (Rails bang method pattern)"""
        if not self.paused_():
            return
        
        self.status = FeatureFlagStatus.ACTIVE
        self.updated_at = datetime.now()
        
        self._log_status_change('resumed', resumed_by_id)
    
    def disable_(self, disabled_by_id: int = None, reason: str = None) -> None:
        """Disable feature flag (Rails bang method pattern)"""
        self.status = FeatureFlagStatus.DISABLED
        self.updated_at = datetime.now()
        
        self._log_status_change('disabled', disabled_by_id, reason)
    
    def complete_(self, completed_by_id: int = None) -> None:
        """Mark feature flag as completed (Rails bang method pattern)"""
        self.status = FeatureFlagStatus.COMPLETED
        self.end_date = datetime.now()
        self.updated_at = datetime.now()
        
        self._log_status_change('completed', completed_by_id)
    
    def archive_(self, archived_by_id: int = None) -> None:
        """Archive feature flag (Rails bang method pattern)"""
        self.status = FeatureFlagStatus.ARCHIVED
        self.updated_at = datetime.now()
        
        self._log_status_change('archived', archived_by_id)
    
    def increase_rollout_(self, increment: float = None, increased_by_id: int = None) -> None:
        """Increase rollout percentage (Rails bang method pattern)"""
        if not self.rollout_flag_():
            raise ValueError("Cannot increase rollout on non-rollout flag")
        
        increment = increment or self.rollout_increment
        new_percentage = min(self.rollout_percentage + increment, self.max_rollout)
        
        if new_percentage != self.rollout_percentage:
            old_percentage = self.rollout_percentage
            self.rollout_percentage = new_percentage
            self.updated_at = datetime.now()
            
            self._log_rollout_change(old_percentage, new_percentage, increased_by_id)
    
    def decrease_rollout_(self, decrement: float = None, decreased_by_id: int = None) -> None:
        """Decrease rollout percentage (Rails bang method pattern)"""
        if not self.rollout_flag_():
            raise ValueError("Cannot decrease rollout on non-rollout flag")
        
        decrement = decrement or self.rollout_increment
        new_percentage = max(self.rollout_percentage - decrement, self.MIN_ROLLOUT_PERCENTAGE)
        
        if new_percentage != self.rollout_percentage:
            old_percentage = self.rollout_percentage
            self.rollout_percentage = new_percentage
            self.updated_at = datetime.now()
            
            self._log_rollout_change(old_percentage, new_percentage, decreased_by_id)
    
    def set_rollout_(self, percentage: float, set_by_id: int = None) -> None:
        """Set specific rollout percentage (Rails bang method pattern)"""
        if not self.rollout_flag_():
            raise ValueError("Cannot set rollout on non-rollout flag")
        
        if not (self.MIN_ROLLOUT_PERCENTAGE <= percentage <= self.max_rollout):
            raise ValueError(f"Rollout percentage must be between {self.MIN_ROLLOUT_PERCENTAGE} and {self.max_rollout}")
        
        old_percentage = self.rollout_percentage
        self.rollout_percentage = percentage
        self.updated_at = datetime.now()
        
        self._log_rollout_change(old_percentage, percentage, set_by_id)
    
    def approve_(self, approved_by_id: int) -> None:
        """Approve feature flag (Rails bang method pattern)"""
        if self.approved_():
            return
        
        self.approved_by_id = approved_by_id
        self.approved_at = datetime.now()
        self.updated_at = datetime.now()
    
    def add_to_whitelist_(self, user_id: int) -> None:
        """Add user to whitelist (Rails bang method pattern)"""
        if not self.user_whitelist:
            self.user_whitelist = []
        
        if user_id not in self.user_whitelist:
            self.user_whitelist.append(user_id)
            self.updated_at = datetime.now()
    
    def remove_from_whitelist_(self, user_id: int) -> None:
        """Remove user from whitelist (Rails bang method pattern)"""
        if self.user_whitelist and user_id in self.user_whitelist:
            self.user_whitelist.remove(user_id)
            self.updated_at = datetime.now()
    
    def add_to_blacklist_(self, user_id: int) -> None:
        """Add user to blacklist (Rails bang method pattern)"""
        if not self.user_blacklist:
            self.user_blacklist = []
        
        if user_id not in self.user_blacklist:
            self.user_blacklist.append(user_id)
            self.updated_at = datetime.now()
    
    def remove_from_blacklist_(self, user_id: int) -> None:
        """Remove user from blacklist (Rails bang method pattern)"""
        if self.user_blacklist and user_id in self.user_blacklist:
            self.user_blacklist.remove(user_id)
            self.updated_at = datetime.now()
    
    def record_impression_(self, user_id: int = None, context: Dict[str, Any] = None) -> None:
        """Record flag impression (Rails bang method pattern)"""
        self.impression_count += 1
        self.last_evaluated_at = datetime.now()
        
        # Track impression details
        if not self.extra_metadata:
            self.extra_metadata = {}
        
        impressions = self.extra_metadata.get('recent_impressions', [])
        impressions.append({
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'context': context
        })
        
        # Keep last 100 impressions
        self.extra_metadata['recent_impressions'] = impressions[-100:]
    
    def record_conversion_(self, user_id: int = None, value: float = None) -> None:
        """Record flag conversion (Rails bang method pattern)"""
        self.conversion_count += 1
        
        # Track conversion details
        if not self.extra_metadata:
            self.extra_metadata = {}
        
        conversions = self.extra_metadata.get('recent_conversions', [])
        conversions.append({
            'user_id': user_id,
            'value': value,
            'timestamp': datetime.now().isoformat()
        })
        
        # Keep last 100 conversions
        self.extra_metadata['recent_conversions'] = conversions[-100:]
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to feature flag (Rails bang method pattern)"""
        if not self.tags:
            self.tags = []
        if tag not in self.tags:
            self.tags.append(tag)
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from feature flag (Rails bang method pattern)"""
        if self.tags and tag in self.tags:
            self.tags.remove(tag)
            self.updated_at = datetime.now()
    
    def has_tag_(self, tag: str) -> bool:
        """Check if flag has specific tag (Rails pattern)"""
        return bool(self.tags and tag in self.tags)
    
    # Rails helper methods
    def _is_user_in_rollout(self, user_id: int) -> bool:
        """Check if user is included in rollout percentage (Rails private pattern)"""
        if self.rollout_percentage >= 100:
            return True
        if self.rollout_percentage <= 0:
            return False
        
        # Use consistent hashing to determine rollout inclusion
        hash_input = f"{self.key}:{user_id}:{self.HASH_SEED}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        user_percentage = (hash_value % 10000) / 100.0  # 0-99.99
        
        return user_percentage < self.rollout_percentage
    
    def _evaluate_targeting_rules(self, user_id: int, org_id: int = None, 
                                 context: Dict[str, Any] = None) -> bool:
        """Evaluate targeting rules (Rails private pattern)"""
        if not self.targeting_rules:
            return self.default_value
        
        context = context or {}
        context.update({
            'user_id': user_id,
            'org_id': org_id
        })
        
        # Simple rule evaluation (would be more complex in production)
        for rule in self.targeting_rules.get('rules', []):
            if self._evaluate_single_rule(rule, context):
                return rule.get('result', True)
        
        return self.default_value
    
    def _evaluate_single_rule(self, rule: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """Evaluate single targeting rule (Rails private pattern)"""
        conditions = rule.get('conditions', [])
        
        for condition in conditions:
            attribute = condition.get('attribute')
            operator = condition.get('operator')
            value = condition.get('value')
            
            context_value = context.get(attribute)
            
            if operator == 'equals' and context_value != value:
                return False
            elif operator == 'in' and context_value not in value:
                return False
            elif operator == 'greater_than' and (not context_value or context_value <= value):
                return False
        
        return True
    
    def get_experiment_variant(self, user_id: int) -> str:
        """Get experiment variant for user (Rails pattern)"""
        if not self.experiment_flag_() or not self.experiment_variants:
            return 'control'
        
        # Use consistent hashing for variant assignment
        hash_input = f"{self.key}:experiment:{user_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        
        # Distribute based on variant weights
        total_weight = sum(variant.get('weight', 1) for variant in self.experiment_variants)
        user_bucket = hash_value % total_weight
        
        current_weight = 0
        for variant in self.experiment_variants:
            current_weight += variant.get('weight', 1)
            if user_bucket < current_weight:
                return variant.get('name', 'control')
        
        return 'control'
    
    def _log_status_change(self, action: str, user_id: int = None, reason: str = None) -> None:
        """Log status change for audit (Rails private pattern)"""
        if not self.extra_metadata:
            self.extra_metadata = {}
        
        status_history = self.extra_metadata.get('status_history', [])
        status_history.append({
            'action': action,
            'user_id': user_id,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        })
        
        self.extra_metadata['status_history'] = status_history[-20:]  # Keep last 20 changes
    
    def _log_rollout_change(self, old_percentage: float, new_percentage: float, 
                          user_id: int = None) -> None:
        """Log rollout change for audit (Rails private pattern)"""
        if not self.extra_metadata:
            self.extra_metadata = {}
        
        rollout_history = self.extra_metadata.get('rollout_history', [])
        rollout_history.append({
            'from_percentage': old_percentage,
            'to_percentage': new_percentage,
            'user_id': user_id,
            'timestamp': datetime.now().isoformat()
        })
        
        self.extra_metadata['rollout_history'] = rollout_history[-50:]  # Keep last 50 changes
    
    # Rails class methods and scopes
    @classmethod
    def by_key(cls, key: str):
        """Scope for specific key (Rails scope pattern)"""
        return cls.query.filter_by(key=key)
    
    @classmethod
    def by_environment(cls, environment: FeatureFlagEnvironment):
        """Scope for specific environment (Rails scope pattern)"""
        return cls.query.filter_by(environment=environment)
    
    @classmethod
    def active_flags(cls):
        """Scope for active flags (Rails scope pattern)"""
        return cls.query.filter_by(status=FeatureFlagStatus.ACTIVE)
    
    @classmethod
    def boolean_flags(cls):
        """Scope for boolean flags (Rails scope pattern)"""
        return cls.query.filter_by(flag_type=FeatureFlagType.BOOLEAN)
    
    @classmethod
    def rollout_flags(cls):
        """Scope for rollout flags (Rails scope pattern)"""
        return cls.query.filter_by(flag_type=FeatureFlagType.ROLLOUT)
    
    @classmethod
    def experiment_flags(cls):
        """Scope for experiment flags (Rails scope pattern)"""
        return cls.query.filter_by(flag_type=FeatureFlagType.EXPERIMENT)
    
    @classmethod
    def kill_switches(cls):
        """Scope for kill switch flags (Rails scope pattern)"""
        return cls.query.filter_by(flag_type=FeatureFlagType.KILL_SWITCH)
    
    @classmethod
    def permanent_flags(cls):
        """Scope for permanent flags (Rails scope pattern)"""
        return cls.query.filter_by(is_permanent=True)
    
    @classmethod
    def global_flags(cls):
        """Scope for global flags (Rails scope pattern)"""
        return cls.query.filter_by(is_global=True)
    
    @classmethod
    def requiring_approval(cls):
        """Scope for flags requiring approval (Rails scope pattern)"""
        return cls.query.filter_by(requires_approval=True, approved_at=None)
    
    @classmethod
    def expiring_soon(cls, days: int = 7):
        """Scope for flags expiring soon (Rails scope pattern)"""
        cutoff = datetime.now() + timedelta(days=days)
        return cls.query.filter(
            cls.end_date.isnot(None),
            cls.end_date <= cutoff,
            cls.status.in_([FeatureFlagStatus.ACTIVE, FeatureFlagStatus.PAUSED])
        )
    
    @classmethod
    def for_org(cls, org_id: int):
        """Scope for org-specific flags (Rails scope pattern)"""
        return cls.query.filter((cls.org_id == org_id) | (cls.is_global == True))
    
    @classmethod
    def high_conversion_flags(cls, min_rate: float = 5.0):
        """Scope for high-converting flags (Rails scope pattern)"""
        return cls.query.filter(
            cls.impression_count > 0,
            (cls.conversion_count * 100.0 / cls.impression_count) >= min_rate
        )
    
    @classmethod
    def create_boolean_flag(cls, key: str, name: str, owner_id: int, 
                           default_value: bool = False, **kwargs) -> 'FeatureFlag':
        """Factory method for boolean flag (Rails pattern)"""
        return cls(
            key=key,
            name=name,
            flag_type=FeatureFlagType.BOOLEAN,
            default_value=default_value,
            owner_id=owner_id,
            **kwargs
        )
    
    @classmethod
    def create_rollout_flag(cls, key: str, name: str, owner_id: int, 
                           initial_percentage: float = 0.0, **kwargs) -> 'FeatureFlag':
        """Factory method for rollout flag (Rails pattern)"""
        return cls(
            key=key,
            name=name,
            flag_type=FeatureFlagType.ROLLOUT,
            rollout_percentage=initial_percentage,
            owner_id=owner_id,
            **kwargs
        )
    
    @classmethod
    def create_experiment(cls, key: str, name: str, owner_id: int,
                         variants: List[Dict[str, Any]], **kwargs) -> 'FeatureFlag':
        """Factory method for experiment flag (Rails pattern)"""
        return cls(
            key=key,
            name=name,
            flag_type=FeatureFlagType.EXPERIMENT,
            experiment_variants=variants,
            owner_id=owner_id,
            **kwargs
        )
    
    @classmethod
    def cleanup_expired_flags(cls) -> int:
        """Clean up expired flags (Rails pattern)"""
        expired_flags = cls.query.filter(
            (cls.end_date < datetime.now()) |
            (cls.auto_disable_date < datetime.now())
        ).filter(cls.status != FeatureFlagStatus.ARCHIVED).all()
        
        for flag in expired_flags:
            flag.archive_()
        
        return len(expired_flags)
    
    @classmethod
    def get_flag_statistics(cls, org_id: int = None) -> Dict[str, Any]:
        """Get flag statistics (Rails class method pattern)"""
        query = cls.query
        if org_id:
            query = query.filter((cls.org_id == org_id) | (cls.is_global == True))
        
        total_flags = query.count()
        active_flags = query.filter_by(status=FeatureFlagStatus.ACTIVE).count()
        experiment_flags = query.filter_by(flag_type=FeatureFlagType.EXPERIMENT).count()
        rollout_flags = query.filter_by(flag_type=FeatureFlagType.ROLLOUT).count()
        
        return {
            'total_flags': total_flags,
            'active_flags': active_flags,
            'experiment_flags': experiment_flags,
            'rollout_flags': rollout_flags,
            'active_percentage': round((active_flags / total_flags * 100), 2) if total_flags > 0 else 0
        }
    
    # Display and serialization methods
    def display_type(self) -> str:
        """Get human-readable type (Rails pattern)"""
        return self.flag_type.display_name if self.flag_type else "Unknown Type"
    
    def display_status(self) -> str:
        """Get human-readable status (Rails pattern)"""
        return self.status.display_name if self.status else "Unknown Status"
    
    def display_environment(self) -> str:
        """Get human-readable environment (Rails pattern)"""
        return self.environment.display_name if self.environment else "Unknown Environment"
    
    def to_dict(self, include_targeting: bool = False, include_analytics: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'key': self.key,
            'name': self.name,
            'description': self.description,
            'flag_type': self.flag_type.value,
            'display_type': self.display_type(),
            'status': self.status.value,
            'display_status': self.display_status(),
            'environment': self.environment.value,
            'display_environment': self.display_environment(),
            'default_value': self.default_value,
            'is_permanent': self.is_permanent,
            'is_global': self.is_global,
            'is_sensitive': self.is_sensitive,
            'active': self.active_(),
            'approved': self.approved_(),
            'requires_approval': self.requires_approval,
            'tags': self.tags or [],
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id
        }
        
        if self.rollout_flag_():
            result.update({
                'rollout_percentage': self.rollout_percentage,
                'max_rollout': self.max_rollout,
                'fully_rolled_out': self.fully_rolled_out_()
            })
        
        if self.experiment_flag_():
            result.update({
                'experiment_variants': self.experiment_variants,
                'experiment_goal': self.experiment_goal,
                'experiment_hypothesis': self.experiment_hypothesis
            })
        
        if self.configuration_flag_():
            result['config_value'] = self.config_value
        
        if include_targeting:
            result.update({
                'targeting_rules': self.targeting_rules,
                'user_whitelist': self.user_whitelist,
                'user_blacklist': self.user_blacklist,
                'org_whitelist': self.org_whitelist
            })
        
        if include_analytics:
            result.update({
                'impression_count': self.impression_count,
                'conversion_count': self.conversion_count,
                'conversion_rate': self.conversion_rate_(),
                'last_evaluated_at': self.last_evaluated_at.isoformat() if self.last_evaluated_at else None
            })
        
        return result
    
    def __repr__(self) -> str:
        return f"<FeatureFlag(id={self.id}, key='{self.key}', type='{self.flag_type.value}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        return f"{self.name} ({self.key}) - {self.display_status()}"