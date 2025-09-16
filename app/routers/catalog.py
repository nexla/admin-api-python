"""
Catalog Router - API endpoints for data catalog operations
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.services.catalog_service import CatalogService

router = APIRouter()

# Pydantic models
class CatalogSearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    filters: Dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(50, ge=1, le=200)
    offset: int = Field(0, ge=0)

class CatalogDiscoveryRequest(BaseModel):
    filters: Dict[str, Any] = Field(default_factory=dict)
    include_schemas: bool = True
    include_metadata: bool = True

class DatasetResponse(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str]
    source_name: str
    source_type: str
    schema_info: Dict[str, Any]
    metadata: Optional[Dict[str, Any]]
    tags: Optional[List[str]]
    created_at: Optional[str]
    updated_at: Optional[str]
    row_count: Optional[int]
    size_bytes: Optional[int]

class SearchResultResponse(BaseModel):
    type: str  # dataset, marketplace_item, etc.
    id: int
    name: str
    description: Optional[str]
    source: Optional[str]
    category: Optional[str]
    relevance_score: float
    metadata: Optional[Dict[str, Any]]
    tags: Optional[List[str]]

class LineageResponse(BaseModel):
    dataset_id: int
    upstream: List[Dict[str, Any]]
    downstream: List[Dict[str, Any]]
    transformations: List[Dict[str, Any]]

class QualityMetricsResponse(BaseModel):
    dataset_id: int
    completeness: float
    validity: float
    consistency: float
    accuracy: float
    timeliness: float
    uniqueness: float
    issues: List[Dict[str, Any]]
    recommendations: List[str]

class CatalogStatsResponse(BaseModel):
    total_datasets: int
    total_sources: int
    total_schemas: int
    data_volume_bytes: int
    recent_additions: int
    top_categories: List[tuple]
    data_freshness: Dict[str, Any]
    quality_score: float

# Dataset discovery endpoints
@router.get("/datasets/discover", response_model=List[DatasetResponse])
async def discover_datasets(
    source_type: Optional[str] = Query(None),
    tags: Optional[str] = Query(None, description="Comma-separated list of tags"),
    min_size: Optional[int] = Query(None, ge=0),
    max_size: Optional[int] = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=500),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Discover available datasets across all data sources"""
    try:
        filters = {}
        
        if source_type:
            filters['source_type'] = source_type
        
        if tags:
            filters['tags'] = [tag.strip() for tag in tags.split(",")]
        
        if min_size is not None:
            filters['min_size'] = min_size
        
        if max_size is not None:
            filters['max_size'] = max_size
        
        catalog_service = CatalogService(db)
        datasets = await catalog_service.discover_datasets(
            org_id=current_user.default_org_id,
            filters=filters
        )
        
        # Apply limit
        datasets = datasets[:limit]
        
        return [DatasetResponse(**dataset) for dataset in datasets]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to discover datasets: {str(e)}"
        )

@router.post("/search", response_model=List[SearchResultResponse])
async def search_catalog(
    search_request: CatalogSearchRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Search the data catalog with advanced filtering"""
    try:
        catalog_service = CatalogService(db)
        search_results = await catalog_service.search_catalog(
            org_id=current_user.default_org_id,
            query=search_request.query,
            filters=search_request.filters
        )
        
        # Apply pagination
        start = search_request.offset
        end = start + search_request.limit
        paginated_results = search_results[start:end]
        
        return [SearchResultResponse(**result) for result in paginated_results]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search catalog: {str(e)}"
        )

@router.get("/datasets/{dataset_id}/lineage", response_model=LineageResponse)
async def get_dataset_lineage(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get data lineage for a specific dataset"""
    try:
        catalog_service = CatalogService(db)
        lineage = await catalog_service.get_dataset_lineage(dataset_id)
        
        if "error" in lineage:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=lineage["error"]
            )
        
        return LineageResponse(**lineage)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get dataset lineage: {str(e)}"
        )

@router.get("/datasets/{dataset_id}/quality", response_model=QualityMetricsResponse)
async def get_dataset_quality(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get data quality metrics for a specific dataset"""
    try:
        catalog_service = CatalogService(db)
        quality_metrics = await catalog_service.analyze_dataset_quality(dataset_id)
        
        if "error" in quality_metrics:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=quality_metrics["error"]
            )
        
        return QualityMetricsResponse(**quality_metrics)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get dataset quality: {str(e)}"
        )

@router.get("/datasets/{dataset_id}/related", response_model=List[Dict[str, Any]])
async def get_related_datasets(
    dataset_id: int,
    limit: int = Query(10, ge=1, le=50),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get datasets related to the specified dataset"""
    try:
        catalog_service = CatalogService(db)
        related_datasets = await catalog_service.suggest_related_datasets(
            dataset_id=dataset_id,
            limit=limit
        )
        
        return related_datasets
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get related datasets: {str(e)}"
        )

@router.get("/stats", response_model=CatalogStatsResponse)
async def get_catalog_statistics(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get overall catalog statistics"""
    try:
        catalog_service = CatalogService(db)
        stats = await catalog_service.get_catalog_statistics(
            org_id=current_user.default_org_id
        )
        
        if "error" in stats:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=stats["error"]
            )
        
        return CatalogStatsResponse(**stats)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get catalog statistics: {str(e)}"
        )

# Catalog management endpoints
@router.post("/sync", response_model=Dict[str, Any])
async def sync_catalog_metadata(
    force_sync: bool = Query(False),
    source_ids: Optional[str] = Query(None, description="Comma-separated list of source IDs"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Synchronize catalog metadata with external systems"""
    try:
        catalog_service = CatalogService(db)
        
        # If specific sources are provided, validate them
        if source_ids:
            from app.models.data_source import DataSource
            source_id_list = [int(sid.strip()) for sid in source_ids.split(",")]
            
            # Validate that all sources exist and belong to the user's org
            source_count = db.query(DataSource).filter(
                DataSource.id.in_(source_id_list),
                DataSource.org_id == current_user.default_org_id
            ).count()
            
            if source_count != len(source_id_list):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="One or more data sources not found"
                )
        
        sync_results = await catalog_service.sync_catalog_metadata(
            org_id=current_user.default_org_id
        )
        
        if "error" in sync_results:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=sync_results["error"]
            )
        
        return {
            "success": True,
            "message": "Catalog metadata sync completed",
            "results": sync_results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync catalog metadata: {str(e)}"
        )

@router.post("/index", response_model=Dict[str, Any])
async def rebuild_search_index(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Rebuild search index for catalog content"""
    try:
        catalog_service = CatalogService(db)
        index_results = await catalog_service.index_catalog_for_search(
            org_id=current_user.default_org_id
        )
        
        if "error" in index_results:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=index_results["error"]
            )
        
        return {
            "success": True,
            "message": "Search index rebuild completed",
            "results": index_results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to rebuild search index: {str(e)}"
        )

# Advanced search endpoints
@router.get("/search/suggestions", response_model=List[str])
async def get_search_suggestions(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get search suggestions based on partial query"""
    try:
        # This would typically use a search suggestion service
        # For now, return simple dataset name matches
        from app.models.data_set import DataSet
        from app.models.data_source import DataSource
        from sqlalchemy import or_
        
        search_pattern = f"%{q}%"
        
        datasets = db.query(DataSet).join(DataSource).filter(
            DataSource.org_id == current_user.default_org_id,
            or_(
                DataSet.name.like(search_pattern),
                DataSet.description.like(search_pattern)
            )
        ).limit(limit).all()
        
        suggestions = []
        for dataset in datasets:
            suggestions.append(dataset.name)
            if dataset.description and q.lower() in dataset.description.lower():
                # Add relevant description phrases
                words = dataset.description.split()
                for i, word in enumerate(words):
                    if q.lower() in word.lower():
                        context = " ".join(words[max(0, i-2):i+3])
                        suggestions.append(context)
        
        # Remove duplicates and limit
        unique_suggestions = list(dict.fromkeys(suggestions))[:limit]
        
        return unique_suggestions
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get search suggestions: {str(e)}"
        )

@router.get("/browse/categories", response_model=List[Dict[str, Any]])
async def browse_by_category(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Browse catalog content by categories"""
    try:
        from app.models.marketplace_item import MarketplaceItem
        from app.models.data_source import DataSource
        from app.models.data_set import DataSet
        from sqlalchemy import func
        
        categories = []
        
        # Get marketplace categories
        marketplace_categories = db.query(
            MarketplaceItem.category,
            func.count(MarketplaceItem.id).label('count')
        ).filter(
            MarketplaceItem.org_id == current_user.default_org_id,
            MarketplaceItem.status == "PUBLISHED"
        ).group_by(MarketplaceItem.category).all()
        
        for category, count in marketplace_categories:
            categories.append({
                "type": "marketplace",
                "name": category,
                "display_name": category.replace("_", " ").title(),
                "count": count,
                "icon": "marketplace"
            })
        
        # Get data source types
        source_types = db.query(
            DataSource.type,
            func.count(DataSource.id).label('count')
        ).filter(
            DataSource.org_id == current_user.default_org_id,
            DataSource.status == "ACTIVE"
        ).group_by(DataSource.type).all()
        
        for source_type, count in source_types:
            # Count datasets for this source type
            dataset_count = db.query(DataSet).join(DataSource).filter(
                DataSource.org_id == current_user.default_org_id,
                DataSource.type == source_type
            ).count()
            
            categories.append({
                "type": "data_source",
                "name": source_type,
                "display_name": source_type.replace("_", " ").title(),
                "count": dataset_count,
                "source_count": count,
                "icon": "database"
            })
        
        return categories
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to browse categories: {str(e)}"
        )

@router.get("/browse/tags", response_model=List[Dict[str, Any]])
async def browse_by_tags(
    limit: int = Query(50, ge=1, le=200),
    min_usage: int = Query(1, ge=1),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Browse catalog content by tags"""
    try:
        from app.models.tag import Tag
        
        # Get most used tags
        tags = db.query(Tag).filter(
            Tag.org_id == current_user.default_org_id,
            Tag.is_active == True,
            Tag.usage_count >= min_usage
        ).order_by(Tag.usage_count.desc()).limit(limit).all()
        
        tag_info = []
        for tag in tags:
            tag_info.append({
                "id": tag.id,
                "name": tag.name,
                "description": tag.description,
                "usage_count": tag.usage_count,
                "color": tag.color,
                "icon": tag.icon,
                "tag_type": tag.tag_type
            })
        
        return tag_info
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to browse tags: {str(e)}"
        )