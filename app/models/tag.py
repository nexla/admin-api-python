"""
Tag Model - Resource tagging and categorization system
"""

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON, Float, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum as PyEnum

from app.database import Base


class TagType(PyEnum):
    CATEGORY = "category"
    LABEL = "label"
    STATUS = "status"
    PRIORITY = "priority"
    CUSTOM = "custom"
    SYSTEM = "system"


class TagScope(PyEnum):
    GLOBAL = "global"  # Available across the entire org
    PROJECT = "project"  # Scoped to specific project
    DOMAIN = "domain"  # Scoped to marketplace domain
    PERSONAL = "personal"  # User's personal tags


class Tag(Base):
    """Tag definition for resource categorization"""
    
    __tablename__ = "tags"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, index=True)
    slug = Column(String(100), nullable=False, index=True)
    description = Column(Text)
    
    # Tag configuration
    tag_type = Column(String(20), default=TagType.LABEL.value, index=True)
    scope = Column(String(20), default=TagScope.GLOBAL.value, index=True)
    
    # Visual configuration
    color = Column(String(7))  # Hex color code
    icon = Column(String(50))  # Icon identifier
    background_color = Column(String(7))  # Background hex color
    
    # Usage and behavior
    is_active = Column(Boolean, default=True, index=True)
    is_system = Column(Boolean, default=False, index=True)  # System-managed tags
    auto_apply_rules = Column(JSON)  # Conditions for auto-applying tag
    
    # Statistics
    usage_count = Column(Integer, default=0, index=True)
    last_used_at = Column(DateTime(timezone=True))
    
    # Hierarchy support
    parent_tag_id = Column(Integer, ForeignKey("tags.id"))
    hierarchy_path = Column(String(500))  # Materialized path for efficient queries
    
    # Scope constraints
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"))  # For project-scoped tags
    domain_id = Column(Integer, ForeignKey("marketplace_domains.id"))  # For domain-scoped tags
    created_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Metadata
    tag_metadata = Column(JSON, default=dict)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    org = relationship("Org", back_populates="tags")
    project = relationship("Project", back_populates="tags")
    domain = relationship("MarketplaceDomain", back_populates="tags")
    created_by = relationship("User", back_populates="created_tags")
    
    # Hierarchy relationships
    parent_tag = relationship("Tag", remote_side="Tag.id", back_populates="child_tags")
    child_tags = relationship("Tag", back_populates="parent_tag")
    
    # Tagged resources
    resource_tags = relationship("ResourceTag", back_populates="tag", cascade="all, delete-orphan")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('slug', 'org_id', 'scope', name='uq_tag_slug_org_scope'),
        Index('idx_tag_hierarchy', 'parent_tag_id', 'hierarchy_path'),
        Index('idx_tag_scope_active', 'scope', 'is_active'),
        Index('idx_tag_usage', 'usage_count', 'last_used_at'),
    )
    
    def __repr__(self):
        return f"<Tag(id={self.id}, name='{self.name}', scope='{self.scope}')>"
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if tag is active"""
        return self.is_active
    
    def system_(self) -> bool:
        """Check if tag is system-managed"""
        return self.is_system
    
    def global_(self) -> bool:
        """Check if tag has global scope"""
        return self.scope == TagScope.GLOBAL.value
    
    def project_scoped_(self) -> bool:
        """Check if tag is project-scoped"""
        return self.scope == TagScope.PROJECT.value
    
    def domain_scoped_(self) -> bool:
        """Check if tag is domain-scoped"""
        return self.scope == TagScope.DOMAIN.value
    
    def personal_(self) -> bool:
        """Check if tag is personal"""
        return self.scope == TagScope.PERSONAL.value
    
    def has_parent_(self) -> bool:
        """Check if tag has a parent"""
        return self.parent_tag_id is not None
    
    def has_children_(self) -> bool:
        """Check if tag has child tags"""
        return len(self.child_tags) > 0
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if tag was used recently"""
        if not self.last_used_at:
            return False
        
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.last_used_at >= cutoff
    
    def auto_applied_(self) -> bool:
        """Check if tag has auto-apply rules"""
        return bool(self.auto_apply_rules)
    
    # Rails helper methods
    def increment_usage_(self) -> None:
        """Increment usage count and update last used time"""
        self.usage_count = (self.usage_count or 0) + 1
        self.last_used_at = datetime.now()
        self.updated_at = datetime.now()
    
    def decrement_usage_(self) -> None:
        """Decrement usage count"""
        self.usage_count = max(0, (self.usage_count or 1) - 1)
        self.updated_at = datetime.now()
    
    def build_hierarchy_path_(self) -> None:
        """Build materialized path for hierarchy queries"""
        if self.parent_tag_id:
            # This would typically be done in a transaction
            # For now, just create a simple path
            parent_path = self.parent_tag.hierarchy_path if self.parent_tag else ""
            self.hierarchy_path = f"{parent_path}/{self.id}" if parent_path else str(self.id)
        else:
            self.hierarchy_path = str(self.id)
    
    def get_ancestors(self) -> List['Tag']:
        """Get all ancestor tags"""
        ancestors = []
        current = self.parent_tag
        
        while current:
            ancestors.append(current)
            current = current.parent_tag
        
        return ancestors
    
    def get_descendants(self) -> List['Tag']:
        """Get all descendant tags"""
        descendants = []
        
        def collect_children(tag):
            for child in tag.child_tags:
                descendants.append(child)
                collect_children(child)
        
        collect_children(self)
        return descendants
    
    def get_siblings(self) -> List['Tag']:
        """Get sibling tags (same parent)"""
        if not self.parent_tag_id:
            # Root level tags
            return [tag for tag in self.org.tags 
                   if tag.parent_tag_id is None and tag.id != self.id]
        else:
            return [tag for tag in self.parent_tag.child_tags if tag.id != self.id]
    
    def can_apply_to_resource_(self, resource_type: str, resource_id: int) -> bool:
        """Check if tag can be applied to a specific resource"""
        # Check scope constraints
        if self.scope == TagScope.PROJECT.value and self.project_id:
            # For project-scoped tags, check if resource belongs to the project
            # This would need resource-specific logic
            pass
        
        if self.scope == TagScope.DOMAIN.value and self.domain_id:
            # For domain-scoped tags, check if resource belongs to the domain
            # This would need resource-specific logic
            pass
        
        # Check auto-apply rules
        if self.auto_apply_rules:
            # Implement rule evaluation based on resource properties
            # This would be customized based on business logic
            pass
        
        return True
    
    def get_tagged_resources(self, resource_type: Optional[str] = None) -> List['ResourceTag']:
        """Get resources tagged with this tag"""
        query = self.resource_tags
        
        if resource_type:
            query = [rt for rt in query if rt.resource_type == resource_type]
        
        return query
    
    def merge_with_(self, other_tag: 'Tag') -> None:
        """Merge this tag with another tag"""
        if other_tag.org_id != self.org_id:
            raise ValueError("Cannot merge tags from different organizations")
        
        # Move all resource tags from other tag to this tag
        for resource_tag in other_tag.resource_tags:
            # Check if resource is already tagged with this tag
            existing = next((rt for rt in self.resource_tags 
                           if rt.resource_type == resource_tag.resource_type 
                           and rt.resource_id == resource_tag.resource_id), None)
            
            if not existing:
                resource_tag.tag_id = self.id
        
        # Update usage count
        self.usage_count = (self.usage_count or 0) + (other_tag.usage_count or 0)
        
        # Handle child tags
        for child in other_tag.child_tags:
            child.parent_tag_id = self.id
            child.build_hierarchy_path_()
    
    def to_dict(self, include_hierarchy: bool = False) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        result = {
            "id": self.id,
            "name": self.name,
            "slug": self.slug,
            "description": self.description,
            "tag_type": self.tag_type,
            "scope": self.scope,
            "color": self.color,
            "icon": self.icon,
            "background_color": self.background_color,
            "is_active": self.is_active,
            "is_system": self.is_system,
            "usage_count": self.usage_count,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "domain_id": self.domain_id,
            "created_by_id": self.created_by_id,
            "parent_tag_id": self.parent_tag_id,
            "metadata": self.metadata
        }
        
        if include_hierarchy:
            result.update({
                "hierarchy_path": self.hierarchy_path,
                "child_count": len(self.child_tags),
                "has_parent": self.has_parent_(),
                "has_children": self.has_children_()
            })
        
        return result


class ResourceTag(Base):
    """Association between tags and resources"""
    
    __tablename__ = "resource_tags"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Tag relationship
    tag_id = Column(Integer, ForeignKey("tags.id"), nullable=False, index=True)
    
    # Resource identification (polymorphic)
    resource_type = Column(String(100), nullable=False, index=True)  # 'Project', 'DataSource', etc.
    resource_id = Column(Integer, nullable=False, index=True)
    
    # Tagging metadata
    tagged_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    tagged_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    auto_applied = Column(Boolean, default=False)  # Was tag applied automatically
    
    # Context
    context = Column(JSON)  # Additional context about why tag was applied
    
    # Relationships
    tag = relationship("Tag", back_populates="resource_tags")
    tagged_by = relationship("User")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('tag_id', 'resource_type', 'resource_id', name='uq_resource_tag'),
        Index('idx_resource_tags_resource', 'resource_type', 'resource_id'),
        Index('idx_resource_tags_tagged_by', 'tagged_by_id', 'tagged_at'),
    )
    
    def __repr__(self):
        return f"<ResourceTag(tag_id={self.tag_id}, resource_type='{self.resource_type}', resource_id={self.resource_id})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "tag_id": self.tag_id,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "tagged_by_id": self.tagged_by_id,
            "tagged_at": self.tagged_at.isoformat() if self.tagged_at else None,
            "auto_applied": self.auto_applied,
            "context": self.context
        }


class TagCollection(Base):
    """Named collections of tags for easy management"""
    
    __tablename__ = "tag_collections"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Collection properties
    is_public = Column(Boolean, default=False)
    is_template = Column(Boolean, default=False)  # Can be used as template for new projects
    
    # Ownership
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    created_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Tags in collection (stored as JSON array of tag IDs)
    tag_ids = Column(JSON, default=list)
    
    # Metadata
    tag_metadata = Column(JSON, default=dict)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    org = relationship("Org")
    created_by = relationship("User")
    
    def __repr__(self):
        return f"<TagCollection(id={self.id}, name='{self.name}')>"
    
    def get_tags(self, db_session) -> List[Tag]:
        """Get all tags in this collection"""
        if not self.tag_ids:
            return []
        
        return db_session.query(Tag).filter(Tag.id.in_(self.tag_ids)).all()
    
    def add_tag_(self, tag_id: int) -> None:
        """Add tag to collection"""
        tag_ids = list(self.tag_ids or [])
        if tag_id not in tag_ids:
            tag_ids.append(tag_id)
            self.tag_ids = tag_ids
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag_id: int) -> None:
        """Remove tag from collection"""
        tag_ids = list(self.tag_ids or [])
        if tag_id in tag_ids:
            tag_ids.remove(tag_id)
            self.tag_ids = tag_ids
            self.updated_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "is_public": self.is_public,
            "is_template": self.is_template,
            "tag_ids": self.tag_ids,
            "tag_count": len(self.tag_ids or []),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "org_id": self.org_id,
            "created_by_id": self.created_by_id,
            "metadata": self.metadata
        }