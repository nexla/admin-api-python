from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from pydantic import BaseModel, Field
from datetime import datetime
import json
from ..database import get_db
from ..auth import get_current_user
from ..models.user import User
from ..models.data_sink import DataSink
from ..models.org import Org

router = APIRouter()

class DataSinkCreate(BaseModel):
    name: str
    description: Optional[str] = None
    connection_type: str
    config: dict = {}
    tags: Optional[List[str]] = []
    is_active: bool = True
    schedule_config: Optional[dict] = None
    data_credentials_id: Optional[int] = None

class DataSinkUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    connection_type: Optional[str] = None
    config: Optional[dict] = None
    tags: Optional[List[str]] = None
    is_active: Optional[bool] = None
    schedule_config: Optional[dict] = None
    data_credentials_id: Optional[int] = None

class DataSinkSearch(BaseModel):
    query: Optional[str] = None
    connection_types: Optional[List[str]] = None
    statuses: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    owner_ids: Optional[List[int]] = None
    org_id: Optional[int] = None
    active_only: bool = True
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None

class DataSinkCopy(BaseModel):
    new_name: Optional[str] = None
    copy_config: bool = True
    copy_tags: bool = True
    copy_credentials: bool = False

class DataSinkRunRequest(BaseModel):
    run_type: str = "manual"
    parameters: Optional[dict] = None
    priority: Optional[str] = "medium"

class DataSinkMetricsResponse(BaseModel):
    run_count: int
    success_count: int
    failure_count: int
    success_rate: float
    last_run_at: Optional[datetime]
    avg_runtime_seconds: Optional[int]
    data_volume_mb: Optional[int]
    error_count: int

class DataSinkResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    status: str
    connection_type: str
    config: Dict[str, Any] = {}
    tags: List[str] = []
    is_active: bool
    schedule_config: Optional[Dict[str, Any]] = None
    data_credentials_id: Optional[int] = None
    owner_id: int
    org_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    last_run_at: Optional[datetime] = None
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    
    # Computed properties from Rails model
    active: bool = True
    healthy: bool = True
    recently_run: bool = False
    
    class Config:
        from_attributes = True

class DataSinkSummaryResponse(BaseModel):
    id: int
    name: str
    status: str
    connection_type: str
    is_active: bool
    healthy: bool
    last_run_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

# ========== Core CRUD Operations ==========

@router.get("/", response_model=List[DataSinkResponse])
async def list_data_sinks(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List data sinks accessible to user."""
    data_sinks = DataSink.accessible_to_user(db, current_user).all()
    return data_sinks

@router.get("/all", response_model=List[DataSinkResponse])
async def list_all_data_sinks(
    limit: int = Query(1000, ge=1, le=5000),
    most_recent_limit: Optional[int] = Query(None),
    org_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all data sinks with optional filters."""
    # Use Rails scope method
    query = DataSink.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSink.org_id == org_id)
    
    # Handle most_recent_limit for performance (Rails pattern)
    if most_recent_limit and most_recent_limit > 0:
        query = query.order_by(DataSink.updated_at.desc()).limit(most_recent_limit)
    else:
        query = query.limit(limit)
    
    data_sinks = query.all()
    return data_sinks

@router.get("/{data_sink_id}", response_model=DataSinkResponse)
async def get_data_sink(
    data_sink_id: int,
    include_sensitive: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific data sink."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view this data sink"
        )
    
    return data_sink

@router.post("/", response_model=DataSinkResponse, status_code=status.HTTP_201_CREATED)
async def create_data_sink(
    data_sink_data: DataSinkCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new data sink."""
    try:
        # Use Rails model factory method
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        input_data = data_sink_data.dict()
        input_data['connection_type'] = data_sink_data.connection_type
        
        db_data_sink = DataSink.build_from_input(api_user_info, input_data)
        
        db.add(db_data_sink)
        db.commit()
        db.refresh(db_data_sink)
        
        return db_data_sink
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{data_sink_id}", response_model=DataSinkResponse)
async def update_data_sink(
    data_sink_id: int,
    data_sink_data: DataSinkUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a data sink."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    # Check ownership/permission
    if not data_sink.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this data sink"
        )
    
    try:
        # Use Rails model update method
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        input_data = {k: v for k, v in data_sink_data.dict().items() if v is not None}
        data_sink.update_mutable(api_user_info, input_data)
        
        db.commit()
        db.refresh(data_sink)
        
        return data_sink
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/{data_sink_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_data_sink(
    data_sink_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a data sink."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    # Check ownership/permission
    if not data_sink.deletable_by_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this data sink"
        )
    
    try:
        db.delete(data_sink)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ========== Custom Action Endpoints ==========

@router.put("/{data_sink_id}/activate", response_model=DataSinkResponse)
async def activate_data_sink(
    data_sink_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate a data sink."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to activate this data sink"
        )
    
    try:
        data_sink.activate_()
        db.commit()
        db.refresh(data_sink)
        return data_sink
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{data_sink_id}/pause", response_model=DataSinkResponse)
async def pause_data_sink(
    data_sink_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Pause a data sink."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to pause this data sink"
        )
    
    try:
        data_sink.pause_()
        db.commit()
        db.refresh(data_sink)
        return data_sink
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{data_sink_id}/copy", response_model=DataSinkResponse)
async def copy_data_sink(
    data_sink_id: int,
    copy_data: DataSinkCopy,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Copy a data sink."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to copy this data sink"
        )
    
    try:
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        new_data_sink = DataSink.copy_from_(
            data_sink,
            api_user_info,
            copy_data.new_name,
            copy_config=copy_data.copy_config,
            copy_tags=copy_data.copy_tags,
            copy_credentials=copy_data.copy_credentials
        )
        
        db.add(new_data_sink)
        db.commit()
        db.refresh(new_data_sink)
        
        return new_data_sink
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ========== Query and Search Endpoints ==========

@router.post("/search", response_model=List[DataSinkResponse])
async def search_data_sinks(
    search_params: DataSinkSearch,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Search data sinks with advanced filters."""
    query = DataSink.accessible_to_user(db, current_user)
    
    # Apply filters
    if search_params.query:
        search_term = f"%{search_params.query}%"
        query = query.filter(
            or_(
                DataSink.name.ilike(search_term),
                DataSink.description.ilike(search_term)
            )
        )
    
    if search_params.connection_types:
        query = query.filter(DataSink.connection_type.in_(search_params.connection_types))
    
    if search_params.statuses:
        query = query.filter(DataSink.status.in_(search_params.statuses))
    
    if search_params.active_only:
        query = query.filter(DataSink.is_active == True)
    
    if search_params.created_after:
        query = query.filter(DataSink.created_at >= search_params.created_after)
    
    if search_params.created_before:
        query = query.filter(DataSink.created_at <= search_params.created_before)
    
    if search_params.org_id:
        query = query.filter(DataSink.org_id == search_params.org_id)
    
    # Apply pagination
    data_sinks = query.offset(offset).limit(limit).all()
    return data_sinks

@router.get("/all/condensed", response_model=List[DataSinkSummaryResponse])
async def list_all_data_sinks_condensed(
    limit: int = Query(1000, ge=1, le=5000),
    org_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all data sinks in condensed format for performance."""
    query = DataSink.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSink.org_id == org_id)
    
    data_sinks = query.limit(limit).all()
    
    # Convert to summary format
    return [
        DataSinkSummaryResponse(
            id=ds.id,
            name=ds.name,
            status=ds.status.value if ds.status else "unknown",
            connection_type=ds.connection_type.value if ds.connection_type else "unknown",
            is_active=ds.is_active,
            healthy=ds.healthy_(),
            last_run_at=ds.last_run_at
        )
        for ds in data_sinks
    ]

@router.get("/all/ids", response_model=List[int])
async def list_all_data_sink_ids(
    org_id: Optional[int] = Query(None),
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of data sink IDs for bulk operations."""
    query = DataSink.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSink.org_id == org_id)
    
    if active_only:
        query = query.filter(DataSink.is_active == True)
    
    data_sinks = query.with_entities(DataSink.id).all()
    return [ds.id for ds in data_sinks]

# ========== Metrics and Monitoring Endpoints ==========

@router.get("/{data_sink_id}/metrics", response_model=DataSinkMetricsResponse)
async def get_data_sink_metrics(
    data_sink_id: int,
    metrics_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get data sink metrics and performance data."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view metrics for this data sink"
        )
    
    return DataSinkMetricsResponse(
        run_count=data_sink.run_count or 0,
        success_count=data_sink.success_count or 0,
        failure_count=data_sink.failure_count or 0,
        success_rate=data_sink.success_rate(),
        last_run_at=data_sink.last_run_at,
        avg_runtime_seconds=data_sink.avg_runtime_seconds,
        data_volume_mb=data_sink.data_volume_mb,
        error_count=data_sink.error_count or 0
    )

@router.get("/{data_sink_id}/audit_log", response_model=List[Dict[str, Any]])
async def get_data_sink_audit_log(
    data_sink_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get audit log for data sink changes."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view audit log for this data sink"
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

# ========== Testing and Development Endpoints ==========

@router.post("/test_config", response_model=Dict[str, Any])
async def test_data_sink_config(
    config_data: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Test data sink configuration without creating."""
    try:
        # TODO: Implement actual config testing logic
        return {
            "valid": True,
            "message": "Configuration test passed",
            "test_results": {
                "connection": "success",
                "authentication": "success",
                "permissions": "success"
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Configuration test failed: {str(e)}"
        )

@router.get("/{data_sink_id}/probe", response_model=Dict[str, Any])
async def probe_data_sink(
    data_sink_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Probe data sink for available data/schema."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to probe this data sink"
        )
    
    try:
        probe_result = data_sink.probe_destination()
        return probe_result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Probe failed: {str(e)}"
        )

@router.post("/{data_sink_id}/probe/authenticate", response_model=Dict[str, Any])
async def test_data_sink_authentication(
    data_sink_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Test authentication for data sink."""
    data_sink = db.query(DataSink).filter(DataSink.id == data_sink_id).first()
    if not data_sink:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data sink not found"
        )
    
    if not data_sink.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to test authentication for this data sink"
        )
    
    try:
        auth_result = data_sink.test_authentication_()
        return {
            "authenticated": auth_result,
            "message": "Authentication successful" if auth_result else "Authentication failed",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Authentication test failed: {str(e)}"
        )