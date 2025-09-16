"""
Marketplace Domain Model - Domain-specific marketplace configuration and management
"""

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum as PyEnum

from app.database import Base


class DomainStatus(PyEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    SUSPENDED = "suspended"
    ARCHIVED = "archived"


class DomainVisibility(PyEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    INTERNAL = "internal"
    RESTRICTED = "restricted"


class MarketplaceDomain(Base):
    """Domain-specific marketplace configuration"""
    
    __tablename__ = "marketplace_domains"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    slug = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)
    
    # Configuration
    status = Column(String(50), default=DomainStatus.ACTIVE.value, index=True)
    visibility = Column(String(50), default=DomainVisibility.PUBLIC.value, index=True)
    
    # Domain settings
    allow_external_access = Column(Boolean, default=True)
    require_approval = Column(Boolean, default=False)
    enable_analytics = Column(Boolean, default=True)
    enable_comments = Column(Boolean, default=True)
    enable_ratings = Column(Boolean, default=True)
    
    # Branding and customization
    logo_url = Column(String(500))
    banner_url = Column(String(500))
    primary_color = Column(String(7))  # Hex color
    secondary_color = Column(String(7))  # Hex color
    custom_css = Column(Text)
    
    # SEO and metadata
    meta_title = Column(String(255))
    meta_description = Column(Text)
    keywords = Column(JSON)  # Array of keywords
    
    # Configuration
    settings = Column(JSON, default=dict)  # Additional domain-specific settings
    featured_items = Column(JSON, default=list)  # List of featured marketplace item IDs
    
    # Analytics
    view_count = Column(Integer, default=0)
    item_count = Column(Integer, default=0)
    subscriber_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    
    # Relationships
    owner = relationship("User", back_populates="owned_marketplace_domains")
    org = relationship("Org", back_populates="marketplace_domains")
    marketplace_items = relationship("MarketplaceItem", back_populates="domain")
    domain_subscriptions = relationship("DomainSubscription", back_populates="domain")
    domain_stats = relationship("DomainStats", back_populates="domain")
    
    def __repr__(self):
        return f"<MarketplaceDomain(id={self.id}, name='{self.name}', slug='{self.slug}')>"
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if domain is active"""
        return self.status == DomainStatus.ACTIVE.value
    
    def inactive_(self) -> bool:
        """Check if domain is inactive"""
        return self.status == DomainStatus.INACTIVE.value
    
    def pending_(self) -> bool:
        """Check if domain is pending approval"""
        return self.status == DomainStatus.PENDING.value
    
    def suspended_(self) -> bool:
        """Check if domain is suspended"""
        return self.status == DomainStatus.SUSPENDED.value
    
    def archived_(self) -> bool:
        """Check if domain is archived"""
        return self.status == DomainStatus.ARCHIVED.value
    
    def public_(self) -> bool:
        """Check if domain is public"""
        return self.visibility == DomainVisibility.PUBLIC.value
    
    def private_(self) -> bool:
        """Check if domain is private"""
        return self.visibility == DomainVisibility.PRIVATE.value
    
    def requires_approval_(self) -> bool:
        """Check if domain requires approval for new items"""
        return self.require_approval
    
    def allows_external_access_(self) -> bool:
        """Check if domain allows external access"""
        return self.allow_external_access
    
    def has_analytics_(self) -> bool:
        """Check if analytics are enabled"""
        return self.enable_analytics
    
    def has_branding_(self) -> bool:
        """Check if domain has custom branding"""
        return bool(self.logo_url or self.banner_url or self.primary_color)
    
    # Rails helper methods
    def increment_view_count_(self) -> None:
        """Increment view count"""
        self.view_count = (self.view_count or 0) + 1
        self.updated_at = datetime.now()
    
    def update_item_count_(self) -> None:
        """Update item count based on associated items"""
        self.item_count = len([item for item in self.marketplace_items if item.published_])
        self.updated_at = datetime.now()
    
    def add_featured_item_(self, item_id: int) -> None:
        """Add item to featured list"""
        featured = list(self.featured_items or [])
        if item_id not in featured:
            featured.append(item_id)
            self.featured_items = featured
            self.updated_at = datetime.now()
    
    def remove_featured_item_(self, item_id: int) -> None:
        """Remove item from featured list"""
        featured = list(self.featured_items or [])
        if item_id in featured:
            featured.remove(item_id)
            self.featured_items = featured
            self.updated_at = datetime.now()
    
    def is_featured_item_(self, item_id: int) -> bool:
        """Check if item is featured"""
        return item_id in (self.featured_items or [])
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get domain setting value"""
        return (self.settings or {}).get(key, default)
    
    def set_setting_(self, key: str, value: Any) -> None:
        """Set domain setting value"""
        settings = dict(self.settings or {})
        settings[key] = value
        self.settings = settings
        self.updated_at = datetime.now()
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        result = {
            "id": self.id,
            "name": self.name,
            "slug": self.slug,
            "description": self.description,
            "status": self.status,
            "visibility": self.visibility,
            "allow_external_access": self.allow_external_access,
            "require_approval": self.require_approval,
            "enable_analytics": self.enable_analytics,
            "enable_comments": self.enable_comments,
            "enable_ratings": self.enable_ratings,
            "logo_url": self.logo_url,
            "banner_url": self.banner_url,
            "primary_color": self.primary_color,
            "secondary_color": self.secondary_color,
            "meta_title": self.meta_title,
            "meta_description": self.meta_description,
            "keywords": self.keywords,
            "view_count": self.view_count,
            "item_count": self.item_count,
            "subscriber_count": self.subscriber_count,
            "featured_items": self.featured_items,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "owner_id": self.owner_id,
            "org_id": self.org_id
        }
        
        if include_sensitive:
            result.update({
                "settings": self.settings,
                "custom_css": self.custom_css
            })
        
        return result


class DomainSubscription(Base):
    """User subscriptions to marketplace domains"""
    
    __tablename__ = "domain_subscriptions"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign keys
    domain_id = Column(Integer, ForeignKey("marketplace_domains.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Subscription details
    subscribed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    notification_preferences = Column(JSON, default=dict)  # Email, push, etc.
    
    # Relationships
    domain = relationship("MarketplaceDomain", back_populates="domain_subscriptions")
    user = relationship("User", back_populates="domain_subscriptions")
    
    def __repr__(self):
        return f"<DomainSubscription(domain_id={self.domain_id}, user_id={self.user_id})>"


class DomainStats(Base):
    """Daily statistics for marketplace domains"""
    
    __tablename__ = "domain_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign key
    domain_id = Column(Integer, ForeignKey("marketplace_domains.id"), nullable=False)
    
    # Statistics
    date = Column(DateTime(timezone=True), nullable=False, index=True)
    views = Column(Integer, default=0)
    unique_visitors = Column(Integer, default=0)
    item_views = Column(Integer, default=0)
    downloads = Column(Integer, default=0)
    new_subscribers = Column(Integer, default=0)
    
    # Engagement metrics
    avg_session_duration = Column(Float)  # in seconds
    bounce_rate = Column(Float)  # percentage
    conversion_rate = Column(Float)  # percentage
    
    # Additional metrics
    top_items = Column(JSON, default=list)  # List of top viewed items
    traffic_sources = Column(JSON, default=dict)  # Referrer information
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Relationships
    domain = relationship("MarketplaceDomain", back_populates="domain_stats")
    
    def __repr__(self):
        return f"<DomainStats(domain_id={self.domain_id}, date='{self.date}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "domain_id": self.domain_id,
            "date": self.date.isoformat() if self.date else None,
            "views": self.views,
            "unique_visitors": self.unique_visitors,
            "item_views": self.item_views,
            "downloads": self.downloads,
            "new_subscribers": self.new_subscribers,
            "avg_session_duration": self.avg_session_duration,
            "bounce_rate": self.bounce_rate,
            "conversion_rate": self.conversion_rate,
            "top_items": self.top_items,
            "traffic_sources": self.traffic_sources,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }