"""
Tags Router - API endpoints for tag management and resource tagging
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.tag import Tag, TagType, TagScope, ResourceTag, TagCollection

router = APIRouter()

# Pydantic models
class TagCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    slug: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    tag_type: TagType = TagType.LABEL
    scope: TagScope = TagScope.GLOBAL
    color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    icon: Optional[str] = None
    background_color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    parent_tag_id: Optional[int] = None
    project_id: Optional[int] = None
    domain_id: Optional[int] = None
    auto_apply_rules: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class TagUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    icon: Optional[str] = None
    background_color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    is_active: Optional[bool] = None
    auto_apply_rules: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

class TagResponse(BaseModel):
    id: int
    name: str
    slug: str
    description: Optional[str]
    tag_type: str
    scope: str
    color: Optional[str]
    icon: Optional[str]
    background_color: Optional[str]
    is_active: bool
    is_system: bool
    usage_count: int
    last_used_at: Optional[str]
    parent_tag_id: Optional[int]
    created_at: str
    updated_at: Optional[str]
    org_id: int
    project_id: Optional[int]
    domain_id: Optional[int]
    created_by_id: int
    metadata: Dict[str, Any]

class ResourceTagRequest(BaseModel):
    resource_type: str = Field(..., min_length=1, max_length=100)
    resource_id: int = Field(..., gt=0)
    context: Optional[Dict[str, Any]] = None

class ResourceTagResponse(BaseModel):
    id: int
    tag_id: int
    resource_type: str
    resource_id: int
    tagged_by_id: int
    tagged_at: str
    auto_applied: bool
    context: Optional[Dict[str, Any]]

class TagCollectionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    is_public: bool = False
    is_template: bool = False
    tag_ids: List[int] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

class TagCollectionResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    is_public: bool
    is_template: bool
    tag_ids: List[int]
    tag_count: int
    created_at: str
    updated_at: Optional[str]
    org_id: int
    created_by_id: int
    metadata: Dict[str, Any]

# Tag management endpoints
@router.post("/", response_model=TagResponse, status_code=status.HTTP_201_CREATED)
async def create_tag(
    tag_data: TagCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new tag"""
    # Check if slug is unique within scope
    existing_tag = db.query(Tag).filter(
        Tag.slug == tag_data.slug,
        Tag.org_id == current_user.default_org_id,
        Tag.scope == tag_data.scope.value
    ).first()
    
    if existing_tag:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tag slug already exists in this scope"
        )
    
    # Validate parent tag if specified
    if tag_data.parent_tag_id:
        parent_tag = db.query(Tag).filter(
            Tag.id == tag_data.parent_tag_id,
            Tag.org_id == current_user.default_org_id
        ).first()
        
        if not parent_tag:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Parent tag not found"
            )
    
    try:
        tag = Tag(
            name=tag_data.name,
            slug=tag_data.slug,
            description=tag_data.description,
            tag_type=tag_data.tag_type.value,
            scope=tag_data.scope.value,
            color=tag_data.color,
            icon=tag_data.icon,
            background_color=tag_data.background_color,
            parent_tag_id=tag_data.parent_tag_id,
            auto_apply_rules=tag_data.auto_apply_rules,
            metadata=tag_data.metadata,
            org_id=current_user.default_org_id,
            project_id=tag_data.project_id,
            domain_id=tag_data.domain_id,
            created_by_id=current_user.id
        )
        
        # Build hierarchy path
        tag.build_hierarchy_path_()
        
        db.add(tag)
        db.commit()
        db.refresh(tag)
        
        return TagResponse(**tag.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create tag: {str(e)}"
        )

@router.get("/", response_model=List[TagResponse])
async def list_tags(
    tag_type: Optional[TagType] = Query(None),
    scope: Optional[TagScope] = Query(None),
    parent_tag_id: Optional[int] = Query(None),
    active_only: bool = Query(True),
    search: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
    domain_id: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List tags with filtering options"""
    try:
        query = db.query(Tag).filter(Tag.org_id == current_user.default_org_id)
        
        # Apply filters
        if tag_type:
            query = query.filter(Tag.tag_type == tag_type.value)
        
        if scope:
            query = query.filter(Tag.scope == scope.value)
        
        if parent_tag_id is not None:
            query = query.filter(Tag.parent_tag_id == parent_tag_id)
        
        if active_only:
            query = query.filter(Tag.is_active == True)
        
        if project_id:
            query = query.filter(
                (Tag.project_id == project_id) | (Tag.scope == TagScope.GLOBAL.value)
            )
        
        if domain_id:
            query = query.filter(
                (Tag.domain_id == domain_id) | (Tag.scope == TagScope.GLOBAL.value)
            )
        
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (Tag.name.like(search_pattern)) |
                (Tag.description.like(search_pattern))
            )
        
        # Order by usage count and name
        query = query.order_by(Tag.usage_count.desc(), Tag.name)
        
        # Apply pagination
        tags = query.offset(offset).limit(limit).all()
        
        return [TagResponse(**tag.to_dict()) for tag in tags]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list tags: {str(e)}"
        )

@router.get("/{tag_id}", response_model=TagResponse)
async def get_tag(
    tag_id: int,
    include_hierarchy: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get tag details"""
    tag = db.query(Tag).filter(
        Tag.id == tag_id,
        Tag.org_id == current_user.default_org_id
    ).first()
    
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tag not found"
        )
    
    return TagResponse(**tag.to_dict(include_hierarchy=include_hierarchy))

@router.put("/{tag_id}", response_model=TagResponse)
async def update_tag(
    tag_id: int,
    tag_data: TagUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update tag"""
    tag = db.query(Tag).filter(
        Tag.id == tag_id,
        Tag.org_id == current_user.default_org_id
    ).first()
    
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tag not found"
        )
    
    # Check if tag is system-managed
    if tag.is_system:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot modify system tags"
        )
    
    try:
        # Update fields
        update_data = tag_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            if hasattr(tag, field):
                setattr(tag, field, value)
        
        tag.updated_at = datetime.now()
        
        db.commit()
        db.refresh(tag)
        
        return TagResponse(**tag.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update tag: {str(e)}"
        )

@router.delete("/{tag_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tag(
    tag_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete tag"""
    tag = db.query(Tag).filter(
        Tag.id == tag_id,
        Tag.org_id == current_user.default_org_id
    ).first()
    
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tag not found"
        )
    
    # Check if tag is system-managed
    if tag.is_system:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot delete system tags"
        )
    
    # Check if tag has children
    if tag.has_children_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete tag with child tags"
        )
    
    try:
        db.delete(tag)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete tag: {str(e)}"
        )

# Resource tagging endpoints
@router.post("/{tag_id}/resources", response_model=ResourceTagResponse, status_code=status.HTTP_201_CREATED)
async def tag_resource(
    tag_id: int,
    resource_data: ResourceTagRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Tag a resource"""
    tag = db.query(Tag).filter(
        Tag.id == tag_id,
        Tag.org_id == current_user.default_org_id
    ).first()
    
    if not tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tag not found"
        )
    
    if not tag.active_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot apply inactive tag"
        )
    
    # Check if resource is already tagged
    existing_tag = db.query(ResourceTag).filter(
        ResourceTag.tag_id == tag_id,
        ResourceTag.resource_type == resource_data.resource_type,
        ResourceTag.resource_id == resource_data.resource_id
    ).first()
    
    if existing_tag:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Resource is already tagged with this tag"
        )
    
    try:
        resource_tag = ResourceTag(
            tag_id=tag_id,
            resource_type=resource_data.resource_type,
            resource_id=resource_data.resource_id,
            tagged_by_id=current_user.id,
            context=resource_data.context
        )
        
        db.add(resource_tag)
        
        # Update tag usage
        tag.increment_usage_()
        
        db.commit()
        db.refresh(resource_tag)
        
        return ResourceTagResponse(**resource_tag.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to tag resource: {str(e)}"
        )

@router.delete("/{tag_id}/resources/{resource_type}/{resource_id}", status_code=status.HTTP_204_NO_CONTENT)
async def untag_resource(
    tag_id: int,
    resource_type: str,
    resource_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Remove tag from resource"""
    resource_tag = db.query(ResourceTag).join(Tag).filter(
        ResourceTag.tag_id == tag_id,
        ResourceTag.resource_type == resource_type,
        ResourceTag.resource_id == resource_id,
        Tag.org_id == current_user.default_org_id
    ).first()
    
    if not resource_tag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resource tag not found"
        )
    
    try:
        # Update tag usage
        tag = resource_tag.tag
        tag.decrement_usage_()
        
        db.delete(resource_tag)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to untag resource: {str(e)}"
        )

@router.get("/resources/{resource_type}/{resource_id}", response_model=List[TagResponse])
async def get_resource_tags(
    resource_type: str,
    resource_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all tags for a resource"""
    try:
        resource_tags = db.query(ResourceTag).join(Tag).filter(
            ResourceTag.resource_type == resource_type,
            ResourceTag.resource_id == resource_id,
            Tag.org_id == current_user.default_org_id
        ).all()
        
        tags = [resource_tag.tag for resource_tag in resource_tags]
        
        return [TagResponse(**tag.to_dict()) for tag in tags]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get resource tags: {str(e)}"
        )

# Tag collections endpoints
@router.post("/collections", response_model=TagCollectionResponse, status_code=status.HTTP_201_CREATED)
async def create_tag_collection(
    collection_data: TagCollectionCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a tag collection"""
    try:
        # Validate that all tag IDs exist and belong to the org
        if collection_data.tag_ids:
            tag_count = db.query(Tag).filter(
                Tag.id.in_(collection_data.tag_ids),
                Tag.org_id == current_user.default_org_id
            ).count()
            
            if tag_count != len(collection_data.tag_ids):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="One or more tags not found"
                )
        
        collection = TagCollection(
            name=collection_data.name,
            description=collection_data.description,
            is_public=collection_data.is_public,
            is_template=collection_data.is_template,
            tag_ids=collection_data.tag_ids,
            metadata=collection_data.metadata,
            org_id=current_user.default_org_id,
            created_by_id=current_user.id
        )
        
        db.add(collection)
        db.commit()
        db.refresh(collection)
        
        return TagCollectionResponse(**collection.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create tag collection: {str(e)}"
        )

@router.get("/collections", response_model=List[TagCollectionResponse])
async def list_tag_collections(
    public_only: bool = Query(False),
    templates_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List tag collections"""
    try:
        query = db.query(TagCollection).filter(
            TagCollection.org_id == current_user.default_org_id
        )
        
        if public_only:
            query = query.filter(TagCollection.is_public == True)
        
        if templates_only:
            query = query.filter(TagCollection.is_template == True)
        
        query = query.order_by(TagCollection.created_at.desc())
        collections = query.offset(offset).limit(limit).all()
        
        return [TagCollectionResponse(**collection.to_dict()) for collection in collections]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list tag collections: {str(e)}"
        )

# Tag analytics endpoints
@router.get("/analytics/usage", response_model=Dict[str, Any])
async def get_tag_usage_analytics(
    days: int = Query(30, ge=1, le=365),
    tag_type: Optional[TagType] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get tag usage analytics"""
    try:
        query = db.query(Tag).filter(
            Tag.org_id == current_user.default_org_id,
            Tag.is_active == True
        )
        
        if tag_type:
            query = query.filter(Tag.tag_type == tag_type.value)
        
        # Get most used tags
        top_tags = query.order_by(Tag.usage_count.desc()).limit(limit).all()
        
        # Calculate statistics
        total_tags = query.count()
        active_tags = query.filter(Tag.usage_count > 0).count()
        unused_tags = total_tags - active_tags
        
        analytics = {
            "total_tags": total_tags,
            "active_tags": active_tags,
            "unused_tags": unused_tags,
            "usage_rate": (active_tags / total_tags * 100) if total_tags > 0 else 0,
            "top_tags": [
                {
                    "id": tag.id,
                    "name": tag.name,
                    "usage_count": tag.usage_count,
                    "last_used_at": tag.last_used_at.isoformat() if tag.last_used_at else None
                }
                for tag in top_tags
            ]
        }
        
        return analytics
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get tag analytics: {str(e)}"
        )