from typing import List, Optional, Dict, Any, Union
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body, File, UploadFile
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from pydantic import BaseModel, Field
from datetime import datetime
import json
from ..database import get_db
from ..auth import get_current_user
from ..models.user import User
from ..models.data_set import DataSet
from ..models.data_source import DataSource
from ..models.data_sink import DataSink
from ..models.org import Org

router = APIRouter()

class DataSetCreate(BaseModel):
    name: str
    description: Optional[str] = None
    data_source_id: Optional[int] = None
    tags: Optional[List[str]] = []
    is_active: bool = True
    output_schema: Optional[Dict[str, Any]] = None
    data_samples: Optional[List[Dict[str, Any]]] = []
    schedule_config: Optional[Dict[str, Any]] = None

class DataSetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    data_source_id: Optional[int] = None
    tags: Optional[List[str]] = None
    is_active: Optional[bool] = None
    output_schema: Optional[Dict[str, Any]] = None
    data_samples: Optional[List[Dict[str, Any]]] = None
    schedule_config: Optional[Dict[str, Any]] = None

class DataSetSearch(BaseModel):
    query: Optional[str] = None
    data_source_ids: Optional[List[int]] = None
    statuses: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    owner_ids: Optional[List[int]] = None
    org_id: Optional[int] = None
    active_only: bool = True
    has_schema: Optional[bool] = None
    has_samples: Optional[bool] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None

class DataSetCopy(BaseModel):
    new_name: Optional[str] = None
    copy_schema: bool = True
    copy_samples: bool = True
    copy_tags: bool = True
    copy_transformations: bool = False

class DataSetTransform(BaseModel):
    transformation_type: str = Field(..., pattern="^(add|replace|remove)$")
    transformation_config: Dict[str, Any]
    apply_to_schema: bool = True
    apply_to_samples: bool = False

class ShareRequest(BaseModel):
    user_ids: List[int] = []
    team_ids: List[int] = []
    permission_level: str = Field(default="read", pattern="^(read|write|admin)$")
    mode: str = Field(default="add", pattern="^(add|remove|reset)$")

class DataSetMetricsResponse(BaseModel):
    record_count: int
    size_bytes: int
    processing_count: int
    success_count: int
    failure_count: int
    success_rate: float
    last_processed_at: Optional[datetime]
    avg_processing_time_seconds: Optional[int]
    data_quality_score: Optional[float]
    schema_version: str

class DataSetResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    status: str
    data_source_id: Optional[int] = None
    tags: List[str] = []
    is_active: bool
    output_schema: Optional[Dict[str, Any]] = None
    data_samples: Optional[List[Dict[str, Any]]] = []
    schedule_config: Optional[Dict[str, Any]] = None
    owner_id: int
    org_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    last_processed_at: Optional[datetime] = None
    record_count: int = 0
    size_bytes: int = 0
    processing_count: int = 0
    
    # Computed properties from Rails model
    active: bool = True
    healthy: bool = True
    has_schema: bool = False
    has_samples: bool = False
    recently_processed: bool = False
    
    class Config:
        from_attributes = True

class DataSetSummaryResponse(BaseModel):
    id: int
    name: str
    status: str
    data_source_id: Optional[int] = None
    is_active: bool
    healthy: bool
    record_count: int = 0
    last_processed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

# ========== Core CRUD Operations ==========

@router.get("/", response_model=List[DataSetResponse])
async def list_data_sets(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List data sets accessible to user."""
    data_sets = DataSet.accessible_to_user(db, current_user).all()
    return data_sets

@router.get("/all", response_model=List[DataSetResponse])
async def list_all_data_sets(
    limit: int = Query(1000, ge=1, le=5000),
    most_recent_limit: Optional[int] = Query(None),
    org_id: Optional[int] = Query(None),
    data_source_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all data sets with optional filters."""
    # Use Rails scope method
    query = DataSet.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSet.org_id == org_id)
    
    if data_source_id:
        query = query.filter(DataSet.data_source_id == data_source_id)
    
    # Handle most_recent_limit for performance (Rails pattern)
    if most_recent_limit and most_recent_limit > 0:
        query = query.order_by(DataSet.updated_at.desc()).limit(most_recent_limit)
    else:
        query = query.limit(limit)
    
    data_sets = query.all()
    return data_sets

@router.get("/{data_set_id}", response_model=DataSetResponse)
async def get_data_set(
    data_set_id: int,
    include_sensitive: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view this data set"
        )
    
    return data_set

@router.post("/", response_model=DataSetResponse, status_code=status.HTTP_201_CREATED)
async def create_data_set(
    data_set_data: DataSetCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new data set."""
    try:
        # Use Rails model factory method
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        input_data = data_set_data.dict()
        
        # Validate data source if provided
        if data_set_data.data_source_id:
            data_source = db.query(DataSource).filter(DataSource.id == data_set_data.data_source_id).first()
            if not data_source:
                raise ValueError("Invalid data source ID")
            if not data_source.accessible_by_(current_user, 'read'):
                raise ValueError("Not authorized to use this data source")
        
        db_data_set = DataSet.build_from_input(api_user_info, input_data)
        
        db.add(db_data_set)
        db.commit()
        db.refresh(db_data_set)
        
        return db_data_set
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{data_set_id}", response_model=DataSetResponse)
async def update_data_set(
    data_set_id: int,
    data_set_data: DataSetUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    # Check ownership/permission
    if not data_set.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this data set"
        )
    
    try:
        # Use Rails model update method
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        input_data = {k: v for k, v in data_set_data.dict().items() if v is not None}
        data_set.update_mutable(api_user_info, input_data)
        
        db.commit()
        db.refresh(data_set)
        
        return data_set
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/{data_set_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_data_set(
    data_set_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    # Check ownership/permission
    if not data_set.deletable_by_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this data set"
        )
    
    try:
        db.delete(data_set)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ========== Custom Action Endpoints ==========

@router.put("/{data_set_id}/activate", response_model=DataSetResponse)
async def activate_data_set(
    data_set_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to activate this data set"
        )
    
    try:
        data_set.activate_()
        db.commit()
        db.refresh(data_set)
        return data_set
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{data_set_id}/pause", response_model=DataSetResponse)
async def pause_data_set(
    data_set_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Pause a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to pause this data set"
        )
    
    try:
        data_set.pause_()
        db.commit()
        db.refresh(data_set)
        return data_set
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{data_set_id}/copy", response_model=DataSetResponse)
async def copy_data_set(
    data_set_id: int,
    copy_data: DataSetCopy,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Copy a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to copy this data set"
        )
    
    try:
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        new_data_set = DataSet.copy_from_(
            data_set,
            api_user_info,
            copy_data.new_name,
            copy_schema=copy_data.copy_schema,
            copy_samples=copy_data.copy_samples,
            copy_tags=copy_data.copy_tags,
            copy_transformations=copy_data.copy_transformations
        )
        
        db.add(new_data_set)
        db.commit()
        db.refresh(new_data_set)
        
        return new_data_set
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ========== Data Set Specific Endpoints ==========

@router.get("/{data_set_id}/samples", response_model=List[Dict[str, Any]])
async def get_data_set_samples(
    data_set_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get data samples for a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view samples for this data set"
        )
    
    try:
        samples = data_set.get_samples(limit=limit, offset=offset)
        return samples
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to retrieve samples: {str(e)}"
        )

@router.post("/{data_set_id}/samples", response_model=Dict[str, Any])
async def update_data_set_samples(
    data_set_id: int,
    samples: List[Dict[str, Any]] = Body(...),
    replace: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update data samples for a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update samples for this data set"
        )
    
    try:
        result = data_set.update_samples_(samples, replace=replace)
        db.commit()
        return {
            "message": "Samples updated successfully",
            "samples_count": len(samples),
            "replaced": replace,
            "result": result
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update samples: {str(e)}"
        )

@router.post("/{data_set_id}/transform", response_model=DataSetResponse)
async def apply_transformation(
    data_set_id: int,
    transform_data: DataSetTransform,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Apply transformation to a data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to transform this data set"
        )
    
    try:
        data_set.apply_transformation_(
            transform_data.transformation_type,
            transform_data.transformation_config,
            apply_to_schema=transform_data.apply_to_schema,
            apply_to_samples=transform_data.apply_to_samples
        )
        db.commit()
        db.refresh(data_set)
        return data_set
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Transformation failed: {str(e)}"
        )

@router.put("/{data_set_id}/share", response_model=Dict[str, Any])
async def share_data_set(
    data_set_id: int,
    share_request: ShareRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Share data set with users or teams."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'admin'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to share this data set"
        )
    
    try:
        result = data_set.share_with_users_(
            user_ids=share_request.user_ids,
            team_ids=share_request.team_ids,
            permission_level=share_request.permission_level,
            mode=share_request.mode,
            shared_by=current_user
        )
        db.commit()
        return {
            "message": "Data set sharing updated successfully",
            "mode": share_request.mode,
            "users_affected": len(share_request.user_ids),
            "teams_affected": len(share_request.team_ids),
            "result": result
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Sharing failed: {str(e)}"
        )

# ========== Query and Search Endpoints ==========

@router.post("/search", response_model=List[DataSetResponse])
async def search_data_sets(
    search_params: DataSetSearch,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Search data sets with advanced filters."""
    query = DataSet.accessible_to_user(db, current_user)
    
    # Apply filters
    if search_params.query:
        search_term = f"%{search_params.query}%"
        query = query.filter(
            or_(
                DataSet.name.ilike(search_term),
                DataSet.description.ilike(search_term)
            )
        )
    
    if search_params.data_source_ids:
        query = query.filter(DataSet.data_source_id.in_(search_params.data_source_ids))
    
    if search_params.statuses:
        query = query.filter(DataSet.status.in_(search_params.statuses))
    
    if search_params.active_only:
        query = query.filter(DataSet.is_active == True)
    
    if search_params.has_schema is not None:
        if search_params.has_schema:
            query = query.filter(DataSet.output_schema.isnot(None))
        else:
            query = query.filter(DataSet.output_schema.is_(None))
    
    if search_params.has_samples is not None:
        if search_params.has_samples:
            query = query.filter(DataSet.data_samples.isnot(None))
        else:
            query = query.filter(DataSet.data_samples.is_(None))
    
    if search_params.created_after:
        query = query.filter(DataSet.created_at >= search_params.created_after)
    
    if search_params.created_before:
        query = query.filter(DataSet.created_at <= search_params.created_before)
    
    if search_params.org_id:
        query = query.filter(DataSet.org_id == search_params.org_id)
    
    # Apply pagination
    data_sets = query.offset(offset).limit(limit).all()
    return data_sets

@router.get("/all/condensed", response_model=List[DataSetSummaryResponse])
async def list_all_data_sets_condensed(
    limit: int = Query(1000, ge=1, le=5000),
    org_id: Optional[int] = Query(None),
    data_source_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all data sets in condensed format for performance."""
    query = DataSet.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSet.org_id == org_id)
    
    if data_source_id:
        query = query.filter(DataSet.data_source_id == data_source_id)
    
    data_sets = query.limit(limit).all()
    
    # Convert to summary format
    return [
        DataSetSummaryResponse(
            id=ds.id,
            name=ds.name,
            status=ds.status.value if ds.status else "unknown",
            data_source_id=ds.data_source_id,
            is_active=ds.is_active,
            healthy=ds.healthy_(),
            record_count=ds.record_count or 0,
            last_processed_at=ds.last_processed_at
        )
        for ds in data_sets
    ]

@router.get("/all/ids", response_model=List[int])
async def list_all_data_set_ids(
    org_id: Optional[int] = Query(None),
    data_source_id: Optional[int] = Query(None),
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of data set IDs for bulk operations."""
    query = DataSet.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSet.org_id == org_id)
    
    if data_source_id:
        query = query.filter(DataSet.data_source_id == data_source_id)
    
    if active_only:
        query = query.filter(DataSet.is_active == True)
    
    data_sets = query.with_entities(DataSet.id).all()
    return [ds.id for ds in data_sets]

# ========== Metrics and Monitoring Endpoints ==========

@router.get("/{data_set_id}/metrics", response_model=DataSetMetricsResponse)
async def get_data_set_metrics(
    data_set_id: int,
    metrics_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get data set metrics and performance data."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view metrics for this data set"
        )
    
    return DataSetMetricsResponse(
        record_count=data_set.record_count or 0,
        size_bytes=data_set.size_bytes or 0,
        processing_count=data_set.processing_count or 0,
        success_count=data_set.success_count or 0,
        failure_count=data_set.failure_count or 0,
        success_rate=data_set.success_rate(),
        last_processed_at=data_set.last_processed_at,
        avg_processing_time_seconds=data_set.avg_processing_time_seconds,
        data_quality_score=data_set.data_quality_score(),
        schema_version=data_set.schema_version or "1.0"
    )

@router.get("/{data_set_id}/audit_log", response_model=List[Dict[str, Any]])
async def get_data_set_audit_log(
    data_set_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get audit log for data set changes."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view audit log for this data set"
        )
    
    # TODO: Implement actual audit log retrieval
    # This would integrate with AuditEntry model when available
    return [
        {
            "id": 1,
            "action": "update",
            "user_id": current_user.id,
            "timestamp": datetime.now().isoformat(),
            "changes": {"status": "ACTIVE"}
        }
    ]

# ========== Data Quality and Validation Endpoints ==========

@router.post("/{data_set_id}/validate", response_model=Dict[str, Any])
async def validate_data_set(
    data_set_id: int,
    validation_config: Optional[Dict[str, Any]] = Body(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Validate data set quality and schema compliance."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to validate this data set"
        )
    
    try:
        validation_result = data_set.validate_data_quality_(validation_config)
        return {
            "valid": validation_result.get('valid', False),
            "quality_score": validation_result.get('quality_score', 0.0),
            "issues": validation_result.get('issues', []),
            "recommendations": validation_result.get('recommendations', []),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Validation failed: {str(e)}"
        )

@router.get("/{data_set_id}/schema", response_model=Dict[str, Any])
async def get_data_set_schema(
    data_set_id: int,
    include_samples: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get data set schema information."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view schema for this data set"
        )
    
    schema_info = {
        "schema": data_set.output_schema,
        "version": data_set.schema_version or "1.0",
        "field_count": len(data_set.output_schema.get('properties', {})) if data_set.output_schema else 0,
        "has_schema": data_set.has_output_schema_(),
        "last_updated": data_set.updated_at.isoformat()
    }
    
    if include_samples and data_set.has_data_samples_():
        schema_info["samples"] = data_set.get_samples(limit=5)
    
    return schema_info

# ========== File Upload and Processing Endpoints ==========

@router.post("/{data_set_id}/upload", response_model=Dict[str, Any])
async def upload_data_file(
    data_set_id: int,
    file: UploadFile = File(...),
    replace_data: bool = Query(False),
    auto_infer_schema: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Upload data file to data set."""
    data_set = db.query(DataSet).filter(DataSet.id == data_set_id).first()
    if not data_set:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data set not found"
        )
    
    if not data_set.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to upload to this data set"
        )
    
    try:
        # Read file content
        content = await file.read()
        
        # Process upload
        upload_result = data_set.process_uploaded_file_(
            file_content=content,
            filename=file.filename,
            replace_data=replace_data,
            auto_infer_schema=auto_infer_schema,
            uploaded_by=current_user
        )
        
        db.commit()
        
        return {
            "message": "File uploaded successfully",
            "filename": file.filename,
            "size_bytes": len(content),
            "records_processed": upload_result.get('records_processed', 0),
            "schema_updated": upload_result.get('schema_updated', False),
            "result": upload_result
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Upload failed: {str(e)}"
        )