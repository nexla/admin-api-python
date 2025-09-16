"""
Marketplace Router - API endpoints for marketplace management and item discovery
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status, UploadFile, File
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.marketplace_item import MarketplaceItem, ItemStatus, ItemVisibility, ItemCategory
from app.models.marketplace_domain import MarketplaceDomain, DomainStatus, DomainVisibility, DomainSubscription, DomainStats

router = APIRouter()

# Pydantic models for domains
class DomainCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    slug: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    visibility: DomainVisibility = DomainVisibility.PUBLIC
    allow_external_access: bool = True
    require_approval: bool = False
    enable_analytics: bool = True
    enable_comments: bool = True
    enable_ratings: bool = True
    logo_url: Optional[str] = None
    banner_url: Optional[str] = None
    primary_color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    secondary_color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    meta_title: Optional[str] = None
    meta_description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    settings: Dict[str, Any] = Field(default_factory=dict)

class DomainUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    visibility: Optional[DomainVisibility] = None
    allow_external_access: Optional[bool] = None
    require_approval: Optional[bool] = None
    enable_analytics: Optional[bool] = None
    enable_comments: Optional[bool] = None
    enable_ratings: Optional[bool] = None
    logo_url: Optional[str] = None
    banner_url: Optional[str] = None
    primary_color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    secondary_color: Optional[str] = Field(None, regex="^#[0-9A-Fa-f]{6}$")
    meta_title: Optional[str] = None
    meta_description: Optional[str] = None
    keywords: Optional[List[str]] = None
    settings: Optional[Dict[str, Any]] = None

class DomainResponse(BaseModel):
    id: int
    name: str
    slug: str
    description: Optional[str]
    status: str
    visibility: str
    allow_external_access: bool
    require_approval: bool
    enable_analytics: bool
    enable_comments: bool
    enable_ratings: bool
    logo_url: Optional[str]
    banner_url: Optional[str]
    primary_color: Optional[str]
    secondary_color: Optional[str]
    meta_title: Optional[str]
    meta_description: Optional[str]
    keywords: List[str]
    view_count: int
    item_count: int
    subscriber_count: int
    featured_items: List[int]
    created_at: str
    updated_at: Optional[str]
    owner_id: int
    org_id: int

class DomainStatsResponse(BaseModel):
    id: int
    domain_id: int
    date: str
    views: int
    unique_visitors: int
    item_views: int
    downloads: int
    new_subscribers: int
    avg_session_duration: Optional[float]
    bounce_rate: Optional[float]
    conversion_rate: Optional[float]
    top_items: List[Any]
    traffic_sources: Dict[str, Any]
    created_at: str

# Pydantic models for marketplace items
class MarketplaceItemCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    category: ItemCategory
    visibility: ItemVisibility = ItemVisibility.PUBLIC
    price: float = Field(0.0, ge=0)
    currency: str = Field("USD", min_length=3, max_length=3)
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    domain_id: Optional[int] = None
    requires_approval: bool = False
    license_type: Optional[str] = None
    license_url: Optional[str] = None
    documentation_url: Optional[str] = None
    demo_url: Optional[str] = None

class MarketplaceItemUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    category: Optional[ItemCategory] = None
    visibility: Optional[ItemVisibility] = None
    price: Optional[float] = Field(None, ge=0)
    currency: Optional[str] = Field(None, min_length=3, max_length=3)
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None
    requires_approval: Optional[bool] = None
    license_type: Optional[str] = None
    license_url: Optional[str] = None
    documentation_url: Optional[str] = None
    demo_url: Optional[str] = None

class MarketplaceItemResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    category: str
    status: str
    visibility: str
    price: float
    currency: str
    view_count: int
    download_count: int
    rating: Optional[float]
    rating_count: int
    tags: List[str]
    created_at: str
    updated_at: Optional[str]
    published_at: Optional[str]
    owner_id: int
    org_id: int
    domain_id: Optional[int]

# Domain management endpoints
@router.post("/domains", response_model=DomainResponse, status_code=status.HTTP_201_CREATED)
async def create_domain(
    domain_data: DomainCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new marketplace domain"""
    # Check if slug is unique
    existing_domain = db.query(MarketplaceDomain).filter(
        MarketplaceDomain.slug == domain_data.slug
    ).first()
    
    if existing_domain:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Domain slug already exists"
        )
    
    try:
        domain = MarketplaceDomain(
            name=domain_data.name,
            slug=domain_data.slug,
            description=domain_data.description,
            visibility=domain_data.visibility.value,
            allow_external_access=domain_data.allow_external_access,
            require_approval=domain_data.require_approval,
            enable_analytics=domain_data.enable_analytics,
            enable_comments=domain_data.enable_comments,
            enable_ratings=domain_data.enable_ratings,
            logo_url=domain_data.logo_url,
            banner_url=domain_data.banner_url,
            primary_color=domain_data.primary_color,
            secondary_color=domain_data.secondary_color,
            meta_title=domain_data.meta_title,
            meta_description=domain_data.meta_description,
            keywords=domain_data.keywords,
            settings=domain_data.settings,
            owner_id=current_user.id,
            org_id=current_user.default_org_id
        )
        
        db.add(domain)
        db.commit()
        db.refresh(domain)
        
        return DomainResponse(**domain.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create domain: {str(e)}"
        )

@router.get("/domains", response_model=List[DomainResponse])
async def list_domains(
    visibility: Optional[DomainVisibility] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    search: Optional[str] = Query(None),
    owner_id: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List marketplace domains"""
    try:
        query = db.query(MarketplaceDomain)
        
        # Filter by organization for non-public domains
        if not visibility or visibility != DomainVisibility.PUBLIC:
            query = query.filter(MarketplaceDomain.org_id == current_user.default_org_id)
        
        # Apply filters
        if visibility:
            query = query.filter(MarketplaceDomain.visibility == visibility.value)
        
        if status_filter:
            query = query.filter(MarketplaceDomain.status == status_filter)
        
        if owner_id:
            query = query.filter(MarketplaceDomain.owner_id == owner_id)
        
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (MarketplaceDomain.name.like(search_pattern)) |
                (MarketplaceDomain.description.like(search_pattern))
            )
        
        # Order by creation time (newest first)
        query = query.order_by(MarketplaceDomain.created_at.desc())
        
        # Apply pagination
        domains = query.offset(offset).limit(limit).all()
        
        return [DomainResponse(**domain.to_dict()) for domain in domains]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list domains: {str(e)}"
        )

@router.get("/domains/{domain_id}", response_model=DomainResponse)
async def get_domain(
    domain_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get domain details"""
    domain = db.query(MarketplaceDomain).filter(
        MarketplaceDomain.id == domain_id
    ).first()
    
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Domain not found"
        )
    
    # Check access permissions
    if (domain.visibility == DomainVisibility.PRIVATE.value and 
        domain.org_id != current_user.default_org_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to private domain"
        )
    
    # Increment view count
    domain.increment_view_count_()
    db.commit()
    
    return DomainResponse(**domain.to_dict())

@router.put("/domains/{domain_id}", response_model=DomainResponse)
async def update_domain(
    domain_id: int,
    domain_data: DomainUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update domain"""
    domain = db.query(MarketplaceDomain).filter(
        MarketplaceDomain.id == domain_id,
        MarketplaceDomain.org_id == current_user.default_org_id
    ).first()
    
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Domain not found"
        )
    
    # Check permissions (only owner can update)
    if domain.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only domain owner can update"
        )
    
    try:
        # Update fields
        update_data = domain_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            if hasattr(domain, field):
                if field in ['visibility'] and hasattr(value, 'value'):
                    setattr(domain, field, value.value)
                else:
                    setattr(domain, field, value)
        
        domain.updated_at = datetime.now()
        
        db.commit()
        db.refresh(domain)
        
        return DomainResponse(**domain.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update domain: {str(e)}"
        )

@router.delete("/domains/{domain_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_domain(
    domain_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete domain"""
    domain = db.query(MarketplaceDomain).filter(
        MarketplaceDomain.id == domain_id,
        MarketplaceDomain.org_id == current_user.default_org_id
    ).first()
    
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Domain not found"
        )
    
    # Check permissions (only owner can delete)
    if domain.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only domain owner can delete"
        )
    
    try:
        db.delete(domain)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete domain: {str(e)}"
        )

# Domain subscription endpoints
@router.post("/domains/{domain_id}/subscribe", status_code=status.HTTP_201_CREATED)
async def subscribe_to_domain(
    domain_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Subscribe to domain updates"""
    domain = db.query(MarketplaceDomain).filter(
        MarketplaceDomain.id == domain_id
    ).first()
    
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Domain not found"
        )
    
    # Check if already subscribed
    existing_subscription = db.query(DomainSubscription).filter(
        DomainSubscription.domain_id == domain_id,
        DomainSubscription.user_id == current_user.id
    ).first()
    
    if existing_subscription:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Already subscribed to this domain"
        )
    
    try:
        subscription = DomainSubscription(
            domain_id=domain_id,
            user_id=current_user.id
        )
        
        db.add(subscription)
        
        # Update domain subscriber count
        domain.subscriber_count = (domain.subscriber_count or 0) + 1
        
        db.commit()
        
        return {"message": "Successfully subscribed to domain"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to subscribe: {str(e)}"
        )

@router.delete("/domains/{domain_id}/subscribe", status_code=status.HTTP_204_NO_CONTENT)
async def unsubscribe_from_domain(
    domain_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Unsubscribe from domain updates"""
    subscription = db.query(DomainSubscription).filter(
        DomainSubscription.domain_id == domain_id,
        DomainSubscription.user_id == current_user.id
    ).first()
    
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found"
        )
    
    try:
        db.delete(subscription)
        
        # Update domain subscriber count
        domain = db.query(MarketplaceDomain).filter(
            MarketplaceDomain.id == domain_id
        ).first()
        
        if domain:
            domain.subscriber_count = max(0, (domain.subscriber_count or 1) - 1)
        
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to unsubscribe: {str(e)}"
        )

# Domain statistics endpoints
@router.get("/domains/{domain_id}/stats", response_model=List[DomainStatsResponse])
async def get_domain_stats(
    domain_id: int,
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(30, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get domain statistics"""
    domain = db.query(MarketplaceDomain).filter(
        MarketplaceDomain.id == domain_id
    ).first()
    
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Domain not found"
        )
    
    # Check access permissions
    if (domain.org_id != current_user.default_org_id and 
        domain.visibility == DomainVisibility.PRIVATE.value):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to domain statistics"
        )
    
    try:
        query = db.query(DomainStats).filter(
            DomainStats.domain_id == domain_id
        )
        
        # Apply date filters
        if start_date:
            query = query.filter(DomainStats.date >= start_date)
        
        if end_date:
            query = query.filter(DomainStats.date <= end_date)
        
        # Order by date (newest first)
        query = query.order_by(DomainStats.date.desc())
        
        stats = query.limit(limit).all()
        
        return [DomainStatsResponse(**stat.to_dict()) for stat in stats]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get domain stats: {str(e)}"
        )

# Marketplace item endpoints
@router.post("/items", response_model=MarketplaceItemResponse, status_code=status.HTTP_201_CREATED)
async def create_marketplace_item(
    item_data: MarketplaceItemCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new marketplace item"""
    try:
        # Validate domain if specified
        domain = None
        if item_data.domain_id:
            domain = db.query(MarketplaceDomain).filter(
                MarketplaceDomain.id == item_data.domain_id,
                MarketplaceDomain.org_id == current_user.default_org_id
            ).first()
            
            if not domain:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Domain not found"
                )
        
        # Determine initial status
        initial_status = ItemStatus.DRAFT.value
        if domain and domain.require_approval:
            initial_status = ItemStatus.PENDING_APPROVAL.value
        
        item = MarketplaceItem(
            name=item_data.name,
            description=item_data.description,
            category=item_data.category.value,
            status=initial_status,
            visibility=item_data.visibility.value,
            price=item_data.price,
            currency=item_data.currency,
            tags=item_data.tags,
            metadata=item_data.metadata,
            domain_id=item_data.domain_id,
            requires_approval=item_data.requires_approval,
            license_type=item_data.license_type,
            license_url=item_data.license_url,
            documentation_url=item_data.documentation_url,
            demo_url=item_data.demo_url,
            owner_id=current_user.id,
            org_id=current_user.default_org_id
        )
        
        db.add(item)
        db.commit()
        db.refresh(item)
        
        # Update domain item count if applicable
        if domain:
            domain.update_item_count_()
            db.commit()
        
        return MarketplaceItemResponse(**item.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create marketplace item: {str(e)}"
        )

@router.get("/items", response_model=List[MarketplaceItemResponse])
async def list_marketplace_items(
    category: Optional[ItemCategory] = Query(None),
    domain_id: Optional[int] = Query(None),
    search: Optional[str] = Query(None),
    tags: Optional[str] = Query(None, description="Comma-separated list of tags"),
    min_price: Optional[float] = Query(None, ge=0),
    max_price: Optional[float] = Query(None, ge=0),
    featured_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List marketplace items"""
    try:
        query = db.query(MarketplaceItem).filter(
            MarketplaceItem.status == ItemStatus.PUBLISHED.value
        )
        
        # Filter by category
        if category:
            query = query.filter(MarketplaceItem.category == category.value)
        
        # Filter by domain
        if domain_id:
            query = query.filter(MarketplaceItem.domain_id == domain_id)
        
        # Filter by price range
        if min_price is not None:
            query = query.filter(MarketplaceItem.price >= min_price)
        
        if max_price is not None:
            query = query.filter(MarketplaceItem.price <= max_price)
        
        # Search filter
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (MarketplaceItem.name.like(search_pattern)) |
                (MarketplaceItem.description.like(search_pattern))
            )
        
        # Tag filter
        if tags:
            tag_list = [tag.strip() for tag in tags.split(",")]
            for tag in tag_list:
                query = query.filter(MarketplaceItem.tags.op('JSON_CONTAINS')(f'"{tag}"'))
        
        # Featured items filter
        if featured_only:
            # This would require joining with domain to check featured_items
            # For now, we'll order by view_count as a proxy
            query = query.order_by(MarketplaceItem.view_count.desc())
        else:
            query = query.order_by(MarketplaceItem.created_at.desc())
        
        # Apply pagination
        items = query.offset(offset).limit(limit).all()
        
        return [MarketplaceItemResponse(**item.to_dict()) for item in items]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list marketplace items: {str(e)}"
        )

@router.get("/items/{item_id}", response_model=MarketplaceItemResponse)
async def get_marketplace_item(
    item_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get marketplace item details"""
    item = db.query(MarketplaceItem).filter(
        MarketplaceItem.id == item_id
    ).first()
    
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Marketplace item not found"
        )
    
    # Check access permissions
    if (item.visibility == ItemVisibility.PRIVATE.value and 
        item.org_id != current_user.default_org_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to private item"
        )
    
    # Increment view count
    item.increment_view_count_()
    db.commit()
    
    return MarketplaceItemResponse(**item.to_dict())

@router.get("/stats/summary", response_model=Dict[str, Any])
async def get_marketplace_stats(
    domain_id: Optional[int] = Query(None),
    days: int = Query(30, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get marketplace statistics summary"""
    try:
        since = datetime.now() - timedelta(days=days)
        
        # Base queries
        items_query = db.query(MarketplaceItem)
        domains_query = db.query(MarketplaceDomain)
        
        # Filter by organization
        items_query = items_query.filter(MarketplaceItem.org_id == current_user.default_org_id)
        domains_query = domains_query.filter(MarketplaceDomain.org_id == current_user.default_org_id)
        
        # Filter by domain if specified
        if domain_id:
            items_query = items_query.filter(MarketplaceItem.domain_id == domain_id)
            domains_query = domains_query.filter(MarketplaceDomain.id == domain_id)
        
        # Calculate statistics
        stats = {
            "total_items": items_query.count(),
            "published_items": items_query.filter(MarketplaceItem.status == ItemStatus.PUBLISHED.value).count(),
            "draft_items": items_query.filter(MarketplaceItem.status == ItemStatus.DRAFT.value).count(),
            "pending_items": items_query.filter(MarketplaceItem.status == ItemStatus.PENDING_APPROVAL.value).count(),
            "total_domains": domains_query.count(),
            "active_domains": domains_query.filter(MarketplaceDomain.status == DomainStatus.ACTIVE.value).count(),
            "recent_items": items_query.filter(MarketplaceItem.created_at >= since).count(),
            "total_views": 0,
            "total_downloads": 0
        }
        
        # Calculate total views and downloads
        items = items_query.all()
        stats["total_views"] = sum(item.view_count or 0 for item in items)
        stats["total_downloads"] = sum(item.download_count or 0 for item in items)
        
        # Calculate averages
        if stats["total_items"] > 0:
            stats["avg_views_per_item"] = round(stats["total_views"] / stats["total_items"], 2)
            stats["avg_downloads_per_item"] = round(stats["total_downloads"] / stats["total_items"], 2)
        else:
            stats["avg_views_per_item"] = 0
            stats["avg_downloads_per_item"] = 0
        
        return stats
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get marketplace stats: {str(e)}"
        )