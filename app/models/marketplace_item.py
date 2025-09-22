from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, JSON, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship, validates
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import json

from app.database import Base

class MarketplaceItemStatuses(PyEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    DISCONTINUED = "discontinued"

class ItemStatus(PyEnum):
    DRAFT = "draft"
    PUBLISHED = "published"
    PENDING_APPROVAL = "pending_approval"

class ItemVisibility(PyEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    ORG_ONLY = "org_only"

class ItemCategory(PyEnum):
    DATASET = "dataset"
    INTEGRATION = "integration"
    TRANSFORM = "transform"
    VALIDATOR = "validator"
    CONNECTOR = "connector"

class MarketplaceItem(Base):
    __tablename__ = "marketplace_items"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Core attributes
    title = Column(String(255), nullable=False)
    description = Column(Text)
    summary = Column(Text)
    
    # Status and lifecycle
    status = Column(SQLEnum(MarketplaceItemStatuses), default=MarketplaceItemStatuses.DRAFT)
    
    # Pricing and metadata
    price = Column(Integer, default=0)  # In cents
    is_free = Column(Boolean, default=True)
    is_featured = Column(Boolean, default=False)
    
    # Content and samples
    data_samples = Column(JSON)  # JSON accessor for data samples
    tags = Column(JSON)  # Tags for categorization
    
    # Relationships
    data_set_id = Column(Integer, ForeignKey("data_sets.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    published_at = Column(DateTime)
    discontinued_at = Column(DateTime)
    
    # Relationships
    data_set = relationship("DataSet", foreign_keys=[data_set_id], back_populates="marketplace_items")
    org = relationship("Org", foreign_keys=[org_id], back_populates="marketplace_items")
    domain_marketplace_items = relationship("DomainMarketplaceItem", back_populates="marketplace_item", cascade="all, delete-orphan")
    
    # Class constants
    DEFAULT_STATUS = MarketplaceItemStatuses.DRAFT
    PUBLISHABLE_STATUSES = [MarketplaceItemStatuses.ACTIVE]
    LISTABLE_STATUSES = [MarketplaceItemStatuses.ACTIVE]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.status:
            self.status = self.DEFAULT_STATUS
    
    @validates('title')
    def validate_title(self, key, title):
        if not title or not title.strip():
            raise ValueError("Title is required")
        if len(title.strip()) > 255:
            raise ValueError("Title must be 255 characters or less")
        return title.strip()
    
    @validates('data_set_id')
    def validate_data_set_id(self, key, data_set_id):
        if not data_set_id:
            raise ValueError("Data set is required")
        return data_set_id
    
    @validates('price')
    def validate_price(self, key, price):
        if price is not None and price < 0:
            raise ValueError("Price must be non-negative")
        return price
    
    def set_org_id_(self):
        """Set org_id from data_set (Rails callback pattern)"""
        if self.data_set:
            self.org_id = self.data_set.org_id
    
    @classmethod
    def build_from_input(cls, data_set, input_data: Dict[str, Any]) -> 'MarketplaceItem':
        """Factory method to create marketplace item from input data"""
        if not data_set:
            raise ValueError("Data set is required")
        
        marketplace_item = cls(
            title=input_data['title'],
            description=input_data.get('description'),
            summary=input_data.get('summary'),
            price=input_data.get('price', 0),
            is_free=input_data.get('is_free', True),
            is_featured=input_data.get('is_featured', False),
            data_samples=input_data.get('data_samples'),
            tags=input_data.get('tags'),
            data_set_id=data_set.id,
            org_id=data_set.org_id,
            status=input_data.get('status', cls.DEFAULT_STATUS)
        )
        
        return marketplace_item
    
    def update_mutable_(self, input_data: Dict[str, Any]) -> None:
        """Update mutable attributes"""
        if 'title' in input_data:
            self.title = input_data['title']
        if 'description' in input_data:
            self.description = input_data['description']
        if 'summary' in input_data:
            self.summary = input_data['summary']
        if 'price' in input_data:
            self.price = input_data['price']
            # Auto-set is_free based on price
            self.is_free = (self.price == 0)
        if 'is_free' in input_data:
            self.is_free = input_data['is_free']
            # If marked as free, set price to 0
            if self.is_free:
                self.price = 0
        if 'is_featured' in input_data:
            self.is_featured = input_data['is_featured']
        if 'data_samples' in input_data:
            self.data_samples = input_data['data_samples']
        if 'tags' in input_data:
            self.tags = input_data['tags']
    
    # Predicate methods (Rails pattern)
    def draft_(self) -> bool:
        """Check if marketplace item is in draft status"""
        return self.status == MarketplaceItemStatuses.DRAFT
    
    def active_(self) -> bool:
        """Check if marketplace item is active"""
        return self.status == MarketplaceItemStatuses.ACTIVE
    
    def discontinued_(self) -> bool:
        """Check if marketplace item is discontinued"""
        return self.status == MarketplaceItemStatuses.DISCONTINUED
    
    def published_(self) -> bool:
        """Check if marketplace item is published (active)"""
        return self.active_()
    
    def unpublished_(self) -> bool:
        """Check if marketplace item is unpublished"""
        return not self.published_()
    
    def free_(self) -> bool:
        """Check if marketplace item is free"""
        return self.is_free is True or self.price == 0
    
    def paid_(self) -> bool:
        """Check if marketplace item is paid"""
        return not self.free_()
    
    def featured_(self) -> bool:
        """Check if marketplace item is featured"""
        return self.is_featured is True
    
    def has_title_(self) -> bool:
        """Check if marketplace item has title"""
        return bool(self.title and self.title.strip())
    
    def has_description_(self) -> bool:
        """Check if marketplace item has description"""
        return bool(self.description and self.description.strip())
    
    def has_summary_(self) -> bool:
        """Check if marketplace item has summary"""
        return bool(self.summary and self.summary.strip())
    
    def has_data_samples_(self) -> bool:
        """Check if marketplace item has data samples"""
        return bool(self.data_samples)
    
    def has_tags_(self) -> bool:
        """Check if marketplace item has tags"""
        return bool(self.tags and isinstance(self.tags, list) and len(self.tags) > 0)
    
    def has_domains_(self) -> bool:
        """Check if marketplace item is assigned to domains"""
        return bool(self.domain_marketplace_items)
    
    def has_price_(self) -> bool:
        """Check if marketplace item has a price set"""
        return self.price is not None and self.price > 0
    
    def complete_listing_(self) -> bool:
        """Check if marketplace item has complete listing information"""
        return (self.has_title_() and
                self.has_description_() and
                self.has_summary_() and
                self.has_data_samples_())
    
    def incomplete_listing_(self) -> bool:
        """Check if marketplace item has incomplete listing information"""
        return not self.complete_listing_()
    
    def publishable_(self) -> bool:
        """Check if marketplace item can be published"""
        return (self.complete_listing_() and
                self.data_set and
                hasattr(self.data_set, 'active_') and
                self.data_set.active_())
    
    def ready_for_market_(self) -> bool:
        """Check if marketplace item is ready for marketplace"""
        return (self.publishable_() and
                self.has_domains_() and
                self.active_())
    
    def listable_(self) -> bool:
        """Check if marketplace item can be listed"""
        return self.status in self.LISTABLE_STATUSES
    
    def searchable_(self) -> bool:
        """Check if marketplace item should appear in search"""
        return (self.listable_() and
                self.complete_listing_())
    
    def recently_published_(self, days: int = 30) -> bool:
        """Check if marketplace item was recently published"""
        if not self.published_at:
            return False
        
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)
        return self.published_at > cutoff
    
    def recently_discontinued_(self, days: int = 30) -> bool:
        """Check if marketplace item was recently discontinued"""
        if not self.discontinued_at:
            return False
        
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)
        return self.discontinued_at > cutoff
    
    def owned_by_org_(self, org) -> bool:
        """Check if marketplace item is owned by organization"""
        return self.org_id == org.id
    
    def accessible_by_user_(self, user) -> bool:
        """Check if marketplace item is accessible by user"""
        # Owner org has access
        if self.org and self.org.has_member_access_(user):
            return True
        
        # Data set owner has access
        if self.data_set and self.data_set.accessible_by_user_(user):
            return True
        
        # Published items are publicly accessible
        if self.published_():
            return True
        
        return False
    
    def manageable_by_user_(self, user) -> bool:
        """Check if marketplace item can be managed by user"""
        # Owner org admins can manage
        if self.org and self.org.has_admin_access_(user):
            return True
        
        # Data set owners can manage
        if self.data_set and self.data_set.manageable_by_user_(user):
            return True
        
        return False
    
    def belongs_to_domain_(self, domain_id: int) -> bool:
        """Check if marketplace item belongs to specific domain"""
        return any(
            dmi.domain_id == domain_id 
            for dmi in self.domain_marketplace_items
        )
    
    def matches_tag_(self, tag: str) -> bool:
        """Check if marketplace item has specific tag"""
        if not self.has_tags_():
            return False
        return tag.lower() in [t.lower() for t in self.tags]
    
    def matches_any_tag_(self, tags: List[str]) -> bool:
        """Check if marketplace item matches any of the given tags"""
        return any(self.matches_tag_(tag) for tag in tags)
    
    def price_in_range_(self, min_price: int = None, max_price: int = None) -> bool:
        """Check if marketplace item price is in range"""
        if min_price is not None and self.price < min_price:
            return False
        if max_price is not None and self.price > max_price:
            return False
        return True
    
    # State management methods (Rails pattern)
    def publish_(self) -> None:
        """Publish the marketplace item"""
        if not self.publishable_():
            raise ValueError("Marketplace item is not ready for publishing")
        
        if not self.published_():
            self.status = MarketplaceItemStatuses.ACTIVE
            self.published_at = datetime.utcnow()
    
    def unpublish_(self) -> None:
        """Unpublish the marketplace item"""
        if self.published_():
            self.status = MarketplaceItemStatuses.DRAFT
            self.published_at = None
    
    def delist_(self) -> None:
        """Delist (discontinue) the marketplace item"""
        if not self.discontinued_():
            self.status = MarketplaceItemStatuses.DISCONTINUED
            self.discontinued_at = datetime.utcnow()
    
    def relist_(self) -> None:
        """Relist a discontinued marketplace item"""
        if self.discontinued_():
            self.status = MarketplaceItemStatuses.ACTIVE
            self.discontinued_at = None
            if not self.published_at:
                self.published_at = datetime.utcnow()
    
    def feature_(self) -> None:
        """Feature the marketplace item"""
        if not self.featured_():
            self.is_featured = True
    
    def unfeature_(self) -> None:
        """Unfeature the marketplace item"""
        if self.featured_():
            self.is_featured = False
    
    def toggle_featured_(self) -> None:
        """Toggle featured status"""
        if self.featured_():
            self.unfeature_()
        else:
            self.feature_()
    
    def make_free_(self) -> None:
        """Make marketplace item free"""
        if not self.free_():
            self.price = 0
            self.is_free = True
    
    def set_price_(self, price: int) -> None:
        """Set price for marketplace item"""
        if price < 0:
            raise ValueError("Price must be non-negative")
        
        self.price = price
        self.is_free = (price == 0)
    
    def reset_to_draft_(self) -> None:
        """Reset marketplace item to draft status"""
        self.status = MarketplaceItemStatuses.DRAFT
        self.published_at = None
        self.discontinued_at = None
    
    def add_tag_(self, tag: str) -> None:
        """Add a tag to marketplace item"""
        if not tag or not tag.strip():
            return
        
        tag = tag.strip().lower()
        if self.tags is None:
            self.tags = []
        elif not isinstance(self.tags, list):
            self.tags = []
        
        if tag not in [t.lower() for t in self.tags]:
            self.tags.append(tag)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove a tag from marketplace item"""
        if not self.has_tags_() or not tag:
            return
        
        tag = tag.strip().lower()
        self.tags = [t for t in self.tags if t.lower() != tag]
    
    def clear_tags_(self) -> None:
        """Clear all tags"""
        self.tags = []
    
    def add_data_sample_(self, sample: Dict[str, Any]) -> None:
        """Add a data sample"""
        if self.data_samples is None:
            self.data_samples = []
        elif not isinstance(self.data_samples, list):
            self.data_samples = []
        
        self.data_samples.append(sample)
    
    def clear_data_samples_(self) -> None:
        """Clear all data samples"""
        self.data_samples = []
    
    def assign_to_domain_(self, domain_id: int) -> None:
        """Assign marketplace item to domain"""
        if not self.belongs_to_domain_(domain_id):
            # from app.models.domain_marketplace_item import DomainMarketplaceItem
            # dmi = DomainMarketplaceItem(marketplace_item_id=self.id, domain_id=domain_id)
            # self.domain_marketplace_items.append(dmi)
            pass
    
    def remove_from_domain_(self, domain_id: int) -> None:
        """Remove marketplace item from domain"""
        self.domain_marketplace_items = [
            dmi for dmi in self.domain_marketplace_items
            if dmi.domain_id != domain_id
        ]
    
    # Calculation methods
    def price_in_dollars(self) -> float:
        """Get price in dollars"""
        return (self.price or 0) / 100.0
    
    def domain_count(self) -> int:
        """Get count of domains item belongs to"""
        return len(self.domain_marketplace_items) if self.domain_marketplace_items else 0
    
    def tag_count(self) -> int:
        """Get count of tags"""
        return len(self.tags) if self.has_tags_() else 0
    
    def data_sample_count(self) -> int:
        """Get count of data samples"""
        return len(self.data_samples) if self.has_data_samples_() else 0
    
    def days_since_published(self) -> Optional[int]:
        """Get days since published"""
        if not self.published_at:
            return None
        
        delta = datetime.utcnow() - self.published_at
        return delta.days
    
    def days_since_discontinued(self) -> Optional[int]:
        """Get days since discontinued"""
        if not self.discontinued_at:
            return None
        
        delta = datetime.utcnow() - self.discontinued_at
        return delta.days
    
    def completion_percentage(self) -> float:
        """Get completion percentage for listing"""
        required_fields = [
            self.has_title_(),
            self.has_description_(),
            self.has_summary_(),
            self.has_data_samples_(),
            self.has_tags_()
        ]
        
        completed = sum(1 for field in required_fields if field)
        return (completed / len(required_fields)) * 100.0
    
    # Display methods
    def status_display(self) -> str:
        """Get human-readable status"""
        status_map = {
            MarketplaceItemStatuses.DRAFT: "Draft",
            MarketplaceItemStatuses.ACTIVE: "Active",
            MarketplaceItemStatuses.DISCONTINUED: "Discontinued"
        }
        return status_map.get(self.status, self.status.value)
    
    def price_display(self) -> str:
        """Get price display"""
        if self.free_():
            return "Free"
        else:
            return f"${self.price_in_dollars():.2f}"
    
    def featured_display(self) -> str:
        """Get featured status display"""
        return "Featured" if self.featured_() else "Standard"
    
    def completion_display(self) -> str:
        """Get completion status display"""
        percentage = self.completion_percentage()
        if percentage == 100.0:
            return "Complete"
        elif percentage >= 75.0:
            return "Nearly Complete"
        elif percentage >= 50.0:
            return "Partially Complete"
        else:
            return "Incomplete"
    
    def availability_display(self) -> str:
        """Get availability display"""
        if self.active_():
            return "Available"
        elif self.draft_():
            return "Draft"
        elif self.discontinued_():
            return "Discontinued"
        else:
            return "Unavailable"
    
    def tags_display(self) -> str:
        """Get tags as display string"""
        if not self.has_tags_():
            return "No tags"
        return ", ".join(self.tags)
    
    def domains_display(self) -> str:
        """Get domains summary"""
        count = self.domain_count()
        if count == 0:
            return "No domains"
        return f"{count} {'domain' if count == 1 else 'domains'}"
    
    def listing_summary(self) -> str:
        """Get complete listing summary"""
        parts = [
            self.status_display(),
            self.price_display(),
            self.completion_display()
        ]
        
        if self.featured_():
            parts.append("Featured")
        
        if self.has_domains_():
            parts.append(self.domains_display())
        
        return " | ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'summary': self.summary,
            'status': self.status.value if self.status else None,
            'price': self.price,
            'is_free': self.is_free,
            'is_featured': self.is_featured,
            'data_samples': self.data_samples,
            'tags': self.tags,
            'data_set_id': self.data_set_id,
            'org_id': self.org_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'published_at': self.published_at.isoformat() if self.published_at else None,
            'discontinued_at': self.discontinued_at.isoformat() if self.discontinued_at else None,
            
            # Predicate methods
            'draft': self.draft_(),
            'active': self.active_(),
            'discontinued': self.discontinued_(),
            'published': self.published_(),
            'unpublished': self.unpublished_(),
            'free': self.free_(),
            'paid': self.paid_(),
            'featured': self.featured_(),
            'has_title': self.has_title_(),
            'has_description': self.has_description_(),
            'has_summary': self.has_summary_(),
            'has_data_samples': self.has_data_samples_(),
            'has_tags': self.has_tags_(),
            'has_domains': self.has_domains_(),
            'has_price': self.has_price_(),
            'complete_listing': self.complete_listing_(),
            'incomplete_listing': self.incomplete_listing_(),
            'publishable': self.publishable_(),
            'ready_for_market': self.ready_for_market_(),
            'listable': self.listable_(),
            'searchable': self.searchable_(),
            'recently_published': self.recently_published_(),
            'recently_discontinued': self.recently_discontinued_(),
            
            # Calculations
            'price_in_dollars': self.price_in_dollars(),
            'domain_count': self.domain_count(),
            'tag_count': self.tag_count(),
            'data_sample_count': self.data_sample_count(),
            'days_since_published': self.days_since_published(),
            'days_since_discontinued': self.days_since_discontinued(),
            'completion_percentage': self.completion_percentage(),
            
            # Display values
            'status_display': self.status_display(),
            'price_display': self.price_display(),
            'featured_display': self.featured_display(),
            'completion_display': self.completion_display(),
            'availability_display': self.availability_display(),
            'tags_display': self.tags_display(),
            'domains_display': self.domains_display(),
            'listing_summary': self.listing_summary()
        }