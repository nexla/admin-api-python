from sqlalchemy import Column, Integer, String, DateTime, BigInteger, ForeignKey, Text
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
import json
import os

from app.database import Base

class RateLimit(Base):
    __tablename__ = "rate_limits"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Rate limit levels (matching Rails implementation)
    common = Column(Integer)
    light = Column(Integer) 
    medium = Column(Integer)
    high = Column(Integer)
    
    # Legacy rate limiting fields (backward compatibility)
    requests_per_minute = Column(Integer)
    requests_per_hour = Column(Integer)
    requests_per_day = Column(Integer)
    bytes_per_minute = Column(BigInteger)
    bytes_per_hour = Column(BigInteger)
    bytes_per_day = Column(BigInteger)
    
    # Resource assignment
    resource_type = Column(String(50))
    resource_id = Column(Integer)
    
    # Metadata
    name = Column(String(255))
    description = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    users = relationship("User", foreign_keys="User.rate_limit_id", back_populates="rate_limit")
    orgs = relationship("Org", foreign_keys="Org.rate_limit_id", back_populates="rate_limit")
    
    # Class constants matching Rails
    GEN_AI_LIMIT = int(os.getenv('GEN_AI_LIMIT', '30'))
    UI_LIMIT = int(os.getenv('UI_LIMIT', '30'))
    ADAPTIVE_FLOWS_RATE_LIMIT = int(os.getenv('ADAPTIVE_FLOWS_RATE_LIMIT', '100'))
    
    DAILY_MULTIPLIER = 60 * 60 * 6  # Quarter of a day in seconds
    LIMITS = ['common', 'light', 'medium', 'high']
    
    # Default rate limits
    DEFAULT_LIMITS = {
        'common': 10,
        'light': 12,
        'medium': 8,
        'high': 5
    }
    
    NOT_LOGGED_LIMITS = {
        'common': 2,
        'light': 2,
        'medium': 2,
        'high': 1
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    @validates('common', 'light', 'medium', 'high')
    def validate_limit_values(self, key, value):
        if value is not None and value < 0:
            raise ValueError(f"{key} limit must be non-negative")
        return value
    
    @validates('resource_type')
    def validate_resource_type(self, key, resource_type):
        valid_types = ['user', 'org', 'data_source', 'project', 'global']
        if resource_type and resource_type not in valid_types:
            raise ValueError(f"Invalid resource_type: {resource_type}. Must be one of {valid_types}")
        return resource_type
    
    @classmethod
    def get_default(cls) -> 'RateLimit':
        """Get default rate limit configuration"""
        return cls(**cls.DEFAULT_LIMITS)
    
    @classmethod
    def get_not_logged(cls) -> 'RateLimit':
        """Get rate limit configuration for non-logged users"""
        return cls(**cls.NOT_LOGGED_LIMITS)
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any]) -> 'RateLimit':
        """Factory method to create rate limit from input data"""
        rate_limit = cls(
            name=input_data.get('name'),
            description=input_data.get('description'),
            resource_type=input_data.get('resource_type'),
            resource_id=input_data.get('resource_id'),
            common=input_data.get('common'),
            light=input_data.get('light'),
            medium=input_data.get('medium'),
            high=input_data.get('high'),
            # Legacy fields
            requests_per_minute=input_data.get('requests_per_minute'),
            requests_per_hour=input_data.get('requests_per_hour'),
            requests_per_day=input_data.get('requests_per_day'),
            bytes_per_minute=input_data.get('bytes_per_minute'),
            bytes_per_hour=input_data.get('bytes_per_hour'),
            bytes_per_day=input_data.get('bytes_per_day')
        )
        
        return rate_limit
    
    def update_mutable_(self, input_data: Dict[str, Any]) -> None:
        """Update mutable attributes"""
        if 'name' in input_data:
            self.name = input_data['name']
        if 'description' in input_data:
            self.description = input_data['description']
        if 'resource_type' in input_data:
            self.resource_type = input_data['resource_type']
        if 'resource_id' in input_data:
            self.resource_id = input_data['resource_id']
        
        # Update rate limit levels
        for limit_type in self.LIMITS:
            if limit_type in input_data:
                setattr(self, limit_type, input_data[limit_type])
        
        # Update legacy fields
        legacy_fields = [
            'requests_per_minute', 'requests_per_hour', 'requests_per_day',
            'bytes_per_minute', 'bytes_per_hour', 'bytes_per_day'
        ]
        for field in legacy_fields:
            if field in input_data:
                setattr(self, field, input_data[field])
    
    # Rate limit level accessors (Rails pattern)
    def common_limit(self) -> int:
        """Get common rate limit with fallback to default"""
        return self.common if self.common is not None else self.DEFAULT_LIMITS['common']
    
    def light_limit(self) -> int:
        """Get light rate limit with fallback to default"""
        return self.light if self.light is not None else self.DEFAULT_LIMITS['light']
    
    def medium_limit(self) -> int:
        """Get medium rate limit with fallback to default"""
        return self.medium if self.medium is not None else self.DEFAULT_LIMITS['medium']
    
    def high_limit(self) -> int:
        """Get high rate limit with fallback to default"""
        return self.high if self.high is not None else self.DEFAULT_LIMITS['high']
    
    def get_limit(self, level: str) -> int:
        """Get rate limit for specific level"""
        if level not in self.LIMITS:
            raise ValueError(f"Invalid limit level: {level}")
        
        method_name = f"{level}_limit"
        return getattr(self, method_name)()
    
    # Predicate methods (Rails pattern)
    def has_name_(self) -> bool:
        """Check if rate limit has name"""
        return bool(self.name and self.name.strip())
    
    def has_description_(self) -> bool:
        """Check if rate limit has description"""
        return bool(self.description and self.description.strip())
    
    def has_resource_(self) -> bool:
        """Check if rate limit is assigned to a resource"""
        return bool(self.resource_type and self.resource_id)
    
    def is_global_(self) -> bool:
        """Check if rate limit is global"""
        return self.resource_type == 'global'
    
    def is_user_limit_(self) -> bool:
        """Check if rate limit applies to users"""
        return self.resource_type == 'user'
    
    def is_org_limit_(self) -> bool:
        """Check if rate limit applies to organizations"""
        return self.resource_type == 'org'
    
    def is_data_source_limit_(self) -> bool:
        """Check if rate limit applies to data sources"""
        return self.resource_type == 'data_source'
    
    def is_project_limit_(self) -> bool:
        """Check if rate limit applies to projects"""
        return self.resource_type == 'project'
    
    def has_limits_defined_(self) -> bool:
        """Check if any rate limits are defined"""
        return any(getattr(self, limit) is not None for limit in self.LIMITS)
    
    def has_legacy_limits_(self) -> bool:
        """Check if legacy rate limits are defined"""
        legacy_fields = [
            self.requests_per_minute, self.requests_per_hour, self.requests_per_day,
            self.bytes_per_minute, self.bytes_per_hour, self.bytes_per_day
        ]
        return any(field is not None for field in legacy_fields)
    
    def is_default_config_(self) -> bool:
        """Check if rate limit matches default configuration"""
        for level in self.LIMITS:
            if getattr(self, level, None) != self.DEFAULT_LIMITS[level]:
                return False
        return True
    
    def is_not_logged_config_(self) -> bool:
        """Check if rate limit matches not-logged configuration"""
        for level in self.LIMITS:
            if getattr(self, level, None) != self.NOT_LOGGED_LIMITS[level]:
                return False
        return True
    
    def is_restrictive_(self) -> bool:
        """Check if rate limit is more restrictive than default"""
        for level in self.LIMITS:
            current_limit = self.get_limit(level)
            default_limit = self.DEFAULT_LIMITS[level]
            if current_limit > default_limit:
                return False
        return True
    
    def is_permissive_(self) -> bool:
        """Check if rate limit is more permissive than default"""
        for level in self.LIMITS:
            current_limit = self.get_limit(level)
            default_limit = self.DEFAULT_LIMITS[level]
            if current_limit < default_limit:
                return False
        return True
    
    def fully_configured_(self) -> bool:
        """Check if rate limit is fully configured"""
        return (self.has_limits_defined_() and
                bool(self.resource_type))
    
    def needs_configuration_(self) -> bool:
        """Check if rate limit needs configuration"""
        return not self.fully_configured_()
    
    def applies_to_resource_(self, resource_type: str, resource_id: int) -> bool:
        """Check if rate limit applies to specific resource"""
        if self.is_global_():
            return True
        
        return (self.resource_type == resource_type and 
                self.resource_id == resource_id)
    
    # Rate limit calculation methods
    def daily_multiplier(self) -> int:
        """Get daily multiplier for rate calculations"""
        return self.DAILY_MULTIPLIER
    
    def calculate_daily_limit(self, level: str) -> int:
        """Calculate daily limit for given level"""
        per_second_limit = self.get_limit(level)
        return per_second_limit * self.daily_multiplier()
    
    def calculate_hourly_limit(self, level: str) -> int:
        """Calculate hourly limit for given level"""
        per_second_limit = self.get_limit(level)
        return per_second_limit * 3600  # 60 * 60 seconds
    
    def calculate_minute_limit(self, level: str) -> int:
        """Calculate per-minute limit for given level"""
        per_second_limit = self.get_limit(level)
        return per_second_limit * 60
    
    def get_effective_limit(self, level: str, time_window: str = 'second') -> int:
        """Get effective limit for level and time window"""
        if time_window == 'second':
            return self.get_limit(level)
        elif time_window == 'minute':
            return self.calculate_minute_limit(level)
        elif time_window == 'hour':
            return self.calculate_hourly_limit(level)
        elif time_window == 'day':
            return self.calculate_daily_limit(level)
        else:
            raise ValueError(f"Invalid time window: {time_window}")
    
    def exceeds_limit_(self, level: str, current_usage: int, time_window: str = 'second') -> bool:
        """Check if current usage exceeds limit"""
        effective_limit = self.get_effective_limit(level, time_window)
        return current_usage > effective_limit
    
    def remaining_quota(self, level: str, current_usage: int, time_window: str = 'second') -> int:
        """Calculate remaining quota for level"""
        effective_limit = self.get_effective_limit(level, time_window)
        remaining = effective_limit - current_usage
        return max(0, remaining)
    
    def usage_percentage(self, level: str, current_usage: int, time_window: str = 'second') -> float:
        """Calculate usage as percentage of limit"""
        effective_limit = self.get_effective_limit(level, time_window)
        if effective_limit == 0:
            return 100.0
        return min(100.0, (current_usage / effective_limit) * 100.0)
    
    # State management methods (Rails pattern)
    def reset_to_defaults_(self) -> None:
        """Reset rate limits to default values"""
        for level in self.LIMITS:
            setattr(self, level, self.DEFAULT_LIMITS[level])
    
    def apply_not_logged_limits_(self) -> None:
        """Apply not-logged user limits"""
        for level in self.LIMITS:
            setattr(self, level, self.NOT_LOGGED_LIMITS[level])
    
    def make_restrictive_(self, factor: float = 0.5) -> None:
        """Make rate limits more restrictive by applying factor"""
        for level in self.LIMITS:
            current_value = getattr(self, level) or self.DEFAULT_LIMITS[level]
            new_value = max(1, int(current_value * factor))
            setattr(self, level, new_value)
    
    def make_permissive_(self, factor: float = 2.0) -> None:
        """Make rate limits more permissive by applying factor"""
        for level in self.LIMITS:
            current_value = getattr(self, level) or self.DEFAULT_LIMITS[level]
            new_value = int(current_value * factor)
            setattr(self, level, new_value)
    
    def scale_limits_(self, scale_factor: float) -> None:
        """Scale all limits by given factor"""
        for level in self.LIMITS:
            current_value = getattr(self, level)
            if current_value is not None:
                new_value = max(1, int(current_value * scale_factor))
                setattr(self, level, new_value)
    
    def copy_limits_from_(self, other_rate_limit: 'RateLimit') -> None:
        """Copy limits from another rate limit"""
        for level in self.LIMITS:
            other_value = getattr(other_rate_limit, level)
            if other_value is not None:
                setattr(self, level, other_value)
    
    def clear_limits_(self) -> None:
        """Clear all rate limit values"""
        for level in self.LIMITS:
            setattr(self, level, None)
    
    def assign_to_resource_(self, resource_type: str, resource_id: int) -> None:
        """Assign rate limit to specific resource"""
        self.resource_type = resource_type
        self.resource_id = resource_id
    
    def make_global_(self) -> None:
        """Make rate limit global"""
        self.resource_type = 'global'
        self.resource_id = None
    
    def unassign_(self) -> None:
        """Remove resource assignment"""
        self.resource_type = None
        self.resource_id = None
    
    # Display methods
    def resource_display(self) -> str:
        """Get human-readable resource assignment"""
        if self.is_global_():
            return "Global"
        elif self.has_resource_():
            return f"{self.resource_type.title()} #{self.resource_id}"
        else:
            return "Unassigned"
    
    def limits_summary(self) -> str:
        """Get summary of rate limits"""
        limits = []
        for level in self.LIMITS:
            value = self.get_limit(level)
            limits.append(f"{level}: {value}/s")
        return " | ".join(limits)
    
    def config_type_display(self) -> str:
        """Get configuration type display"""
        if self.is_default_config_():
            return "Default Configuration"
        elif self.is_not_logged_config_():
            return "Not-Logged Configuration"
        elif self.is_restrictive_():
            return "Restrictive Configuration"
        elif self.is_permissive_():
            return "Permissive Configuration"
        else:
            return "Custom Configuration"
    
    def usage_summary(self, current_usage: Dict[str, int], time_window: str = 'second') -> str:
        """Get usage summary across all levels"""
        summaries = []
        for level in self.LIMITS:
            usage = current_usage.get(level, 0)
            percentage = self.usage_percentage(level, usage, time_window)
            summaries.append(f"{level}: {percentage:.1f}%")
        return " | ".join(summaries)
    
    def rate_limit_summary(self) -> str:
        """Get complete rate limit summary"""
        parts = [
            self.config_type_display(),
            self.resource_display(),
            self.limits_summary()
        ]
        return " | ".join(parts)
    
    def to_json(self, _=None) -> Dict[str, int]:
        """Convert rate limits to JSON (Rails compatibility)"""
        return {level: self.get_limit(level) for level in self.LIMITS}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'common': self.common,
            'light': self.light,
            'medium': self.medium,
            'high': self.high,
            'resource_type': self.resource_type,
            'resource_id': self.resource_id,
            
            # Legacy fields
            'requests_per_minute': self.requests_per_minute,
            'requests_per_hour': self.requests_per_hour,
            'requests_per_day': self.requests_per_day,
            'bytes_per_minute': self.bytes_per_minute,
            'bytes_per_hour': self.bytes_per_hour,
            'bytes_per_day': self.bytes_per_day,
            
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Rate limit accessors
            'common_limit': self.common_limit(),
            'light_limit': self.light_limit(),
            'medium_limit': self.medium_limit(),
            'high_limit': self.high_limit(),
            
            # Predicate methods
            'has_name': self.has_name_(),
            'has_description': self.has_description_(),
            'has_resource': self.has_resource_(),
            'is_global': self.is_global_(),
            'is_user_limit': self.is_user_limit_(),
            'is_org_limit': self.is_org_limit_(),
            'is_data_source_limit': self.is_data_source_limit_(),
            'is_project_limit': self.is_project_limit_(),
            'has_limits_defined': self.has_limits_defined_(),
            'has_legacy_limits': self.has_legacy_limits_(),
            'is_default_config': self.is_default_config_(),
            'is_not_logged_config': self.is_not_logged_config_(),
            'is_restrictive': self.is_restrictive_(),
            'is_permissive': self.is_permissive_(),
            'fully_configured': self.fully_configured_(),
            'needs_configuration': self.needs_configuration_(),
            
            # Calculations
            'daily_multiplier': self.daily_multiplier(),
            
            # Display values
            'resource_display': self.resource_display(),
            'limits_summary': self.limits_summary(),
            'config_type_display': self.config_type_display(),
            'rate_limit_summary': self.rate_limit_summary(),
            
            # JSON compatibility
            'limits_json': self.to_json()
        }