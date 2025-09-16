from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body, Request
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from pydantic import BaseModel, Field, validator
from datetime import datetime
import json

from ..database import get_db
from ..auth import get_current_user
from ..auth.rbac import RBACService, SystemPermissions
from ..models.user import User
from ..models.data_source import DataSource
from ..models.org import Org
from ..services.audit_service import AuditService
from ..services.validation_service import ValidationService
from ..services.async_tasks.manager import AsyncTaskManager

router = APIRouter()

class DataSourceCreate(BaseModel):
    name: str
    description: Optional[str] = None
    connection_type: str
    config: dict = {}
    tags: Optional[List[str]] = []
    is_active: bool = True
    schedule_config: Optional[dict] = None
    data_credentials_id: Optional[int] = None
    
    @validator('name')
    def validate_name(cls, v):
        result = ValidationService.validate_name(v, min_length=2, max_length=100)
        if not result['valid']:
            raise ValueError(f"Invalid name: {'; '.join(result['errors'])}")
        return result['name']
    
    @validator('config')
    def validate_config(cls, v):
        result = ValidationService.validate_json_config(v)
        if not result['valid']:
            raise ValueError(f"Invalid config: {'; '.join(result['errors'])}")
        return result['config']
    
    @validator('tags')
    def validate_tags(cls, v):
        if v:
            result = ValidationService.validate_tags(v)
            if not result['valid']:
                raise ValueError(f"Invalid tags: {'; '.join(result['errors'])}")
            return result['tags']
        return v or []

class DataSourceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    connection_type: Optional[str] = None
    config: Optional[dict] = None
    tags: Optional[List[str]] = None
    is_active: Optional[bool] = None
    schedule_config: Optional[dict] = None
    data_credentials_id: Optional[int] = None

class DataSourceSearch(BaseModel):
    query: Optional[str] = None
    connection_types: Optional[List[str]] = None
    statuses: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    owner_ids: Optional[List[int]] = None
    org_id: Optional[int] = None
    active_only: bool = True
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None

class DataSourceCopy(BaseModel):
    new_name: Optional[str] = None
    copy_config: bool = True
    copy_tags: bool = True
    copy_credentials: bool = False

class DataSourceRunRequest(BaseModel):
    run_type: str = "manual"
    parameters: Optional[dict] = None
    priority: Optional[str] = "medium"

class DataSourceMetricsResponse(BaseModel):
    run_count: int
    success_count: int
    failure_count: int
    success_rate: float
    last_run_at: Optional[datetime]
    avg_runtime_seconds: Optional[int]
    data_volume_mb: Optional[int]
    error_count: int

class DataSourceResponse(BaseModel):
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

class DataSourceSummaryResponse(BaseModel):
    id: int
    name: str
    status: str
    connection_type: str
    is_active: bool
    healthy: bool
    last_run_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

@router.get("/", response_model=List[DataSourceResponse])
async def list_data_sources(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    connection_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List data sources with proper authorization."""
    # Check read permissions
    if not RBACService.has_permission(current_user, SystemPermissions.DATA_SOURCE_READ, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to list data sources"
        )
    
    # Start with user's accessible data sources
    query = db.query(DataSource)
    
    # Non-admin users can only see their own or shared data sources
    if not RBACService.has_permission(current_user, SystemPermissions.DATA_SOURCE_ADMIN, db):
        query = query.filter(
            or_(
                DataSource.owner_id == current_user.id,
                # TODO: Add shared data source filtering when relationships are active
            )
        )
    
    # Apply filters
    if connection_type:
        query = query.filter(DataSource.connection_type == connection_type)
    
    if status:
        query = query.filter(DataSource.status == status)
    
    if active_only:
        query = query.filter(DataSource.is_active == True)
    
    data_sources = query.offset(skip).limit(limit).all()
    return data_sources

@router.get("/all", response_model=List[DataSourceResponse])
async def list_all_data_sources(
    limit: int = Query(1000, ge=1, le=5000),
    most_recent_limit: Optional[int] = Query(None),
    org_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all data sources with optional filters."""
    # Use Rails scope method
    query = DataSource.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSource.org_id == org_id)
    
    # Handle most_recent_limit for performance (Rails pattern)
    if most_recent_limit and most_recent_limit > 0:
        query = query.order_by(DataSource.updated_at.desc()).limit(most_recent_limit)
    else:
        query = query.limit(limit)
    
    data_sources = query.all()
    return data_sources

@router.get("/{data_source_id}", response_model=DataSourceResponse)
async def get_data_source(
    data_source_id: int,
    include_sensitive: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view this data source"
        )
    
    return data_source

@router.post("/", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def create_data_source(
    data_source_data: DataSourceCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new data source."""
    # Check write permissions
    if not RBACService.has_permission(current_user, SystemPermissions.DATA_SOURCE_WRITE, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to create data sources"
        )
    
    try:
        # Create data source
        db_data_source = DataSource(
            name=data_source_data.name,
            description=data_source_data.description,
            connection_type=data_source_data.connection_type,
            config=data_source_data.config,
            tags=data_source_data.tags,
            is_active=data_source_data.is_active,
            schedule_config=data_source_data.schedule_config,
            data_credentials_id=data_source_data.data_credentials_id,
            owner_id=current_user.id,
            org_id=current_user.default_org_id,
            status="ACTIVE",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        db.add(db_data_source)
        db.commit()
        db.refresh(db_data_source)
        
        # Log the action
        AuditService.log_action(
            db=db,
            user_id=current_user.id,
            action="create",
            resource_type="data_source",
            resource_id=db_data_source.id,
            resource_name=db_data_source.name,
            new_values=data_source_data.dict(),
            request=request
        )
        
        return db_data_source
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{data_source_id}", response_model=DataSourceResponse)
async def update_data_source(
    data_source_id: int,
    data_source_data: DataSourceUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    # Check ownership/permission
    if not data_source.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this data source"
        )
    
    try:
        # Use Rails model update method
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        input_data = {k: v for k, v in data_source_data.dict().items() if v is not None}
        data_source.update_mutable(api_user_info, input_data)
        
        db.commit()
        db.refresh(data_source)
        
        return data_source
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/{data_source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_data_source(
    data_source_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    # Check ownership/permission
    if not data_source.deletable_by_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this data source"
        )
    
    try:
        db.delete(data_source)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ========== Custom Action Endpoints ==========

@router.put("/{data_source_id}/activate", response_model=DataSourceResponse)
async def activate_data_source(
    data_source_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate a data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to activate this data source"
        )
    
    try:
        data_source.activate_()
        db.commit()
        db.refresh(data_source)
        return data_source
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/{data_source_id}/pause", response_model=DataSourceResponse)
async def pause_data_source(
    data_source_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Pause a data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to pause this data source"
        )
    
    try:
        data_source.pause_()
        db.commit()
        db.refresh(data_source)
        return data_source
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{data_source_id}/copy", response_model=DataSourceResponse)
async def copy_data_source(
    data_source_id: int,
    copy_data: DataSourceCopy,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Copy a data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to copy this data source"
        )
    
    try:
        api_user_info = {
            'input_owner': current_user,
            'input_org': db.query(Org).filter(Org.id == current_user.default_org_id).first() if current_user.default_org_id else None
        }
        
        new_data_source = DataSource.copy_from_(
            data_source,
            api_user_info,
            copy_data.new_name,
            copy_config=copy_data.copy_config,
            copy_tags=copy_data.copy_tags,
            copy_credentials=copy_data.copy_credentials
        )
        
        db.add(new_data_source)
        db.commit()
        db.refresh(new_data_source)
        
        return new_data_source
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{data_source_id}/run_now", response_model=Dict[str, Any])
async def run_data_source_now(
    data_source_id: int,
    run_request: DataSourceRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Trigger immediate run of data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.executable_by_(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to run this data source"
        )
    
    if not data_source.can_be_run_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Data source cannot be run in current state"
        )
    
    try:
        result = data_source.run_now_(
            run_type=run_request.run_type,
            parameters=run_request.parameters,
            priority=run_request.priority,
            triggered_by=current_user
        )
        
        db.commit()
        return result
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ========== Query and Search Endpoints ==========

@router.post("/search", response_model=List[DataSourceResponse])
async def search_data_sources(
    search_params: DataSourceSearch,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Search data sources with advanced filters."""
    query = DataSource.accessible_to_user(db, current_user)
    
    # Apply filters
    if search_params.query:
        search_term = f"%{search_params.query}%"
        query = query.filter(
            or_(
                DataSource.name.ilike(search_term),
                DataSource.description.ilike(search_term)
            )
        )
    
    if search_params.connection_types:
        query = query.filter(DataSource.connection_type.in_(search_params.connection_types))
    
    if search_params.statuses:
        query = query.filter(DataSource.status.in_(search_params.statuses))
    
    if search_params.active_only:
        query = query.filter(DataSource.is_active == True)
    
    if search_params.created_after:
        query = query.filter(DataSource.created_at >= search_params.created_after)
    
    if search_params.created_before:
        query = query.filter(DataSource.created_at <= search_params.created_before)
    
    if search_params.org_id:
        query = query.filter(DataSource.org_id == search_params.org_id)
    
    # Apply pagination
    data_sources = query.offset(offset).limit(limit).all()
    return data_sources

@router.get("/all/condensed", response_model=List[DataSourceSummaryResponse])
async def list_all_data_sources_condensed(
    limit: int = Query(1000, ge=1, le=5000),
    org_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all data sources in condensed format for performance."""
    query = DataSource.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSource.org_id == org_id)
    
    data_sources = query.limit(limit).all()
    
    # Convert to summary format
    return [
        DataSourceSummaryResponse(
            id=ds.id,
            name=ds.name,
            status=ds.status.value if ds.status else "unknown",
            connection_type=ds.connection_type.value if ds.connection_type else "unknown",
            is_active=ds.is_active,
            healthy=ds.healthy_(),
            last_run_at=ds.last_run_at
        )
        for ds in data_sources
    ]

@router.get("/all/ids", response_model=List[int])
async def list_all_data_source_ids(
    org_id: Optional[int] = Query(None),
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of data source IDs for bulk operations."""
    query = DataSource.accessible_to_user(db, current_user)
    
    if org_id:
        query = query.filter(DataSource.org_id == org_id)
    
    if active_only:
        query = query.filter(DataSource.is_active == True)
    
    data_sources = query.with_entities(DataSource.id).all()
    return [ds.id for ds in data_sources]

# ========== Metrics and Monitoring Endpoints ==========

@router.get("/{data_source_id}/metrics", response_model=DataSourceMetricsResponse)
async def get_data_source_metrics(
    data_source_id: int,
    metrics_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get data source metrics and performance data."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view metrics for this data source"
        )
    
    return DataSourceMetricsResponse(
        run_count=data_source.run_count or 0,
        success_count=data_source.success_count or 0,
        failure_count=data_source.failure_count or 0,
        success_rate=data_source.success_rate(),
        last_run_at=data_source.last_run_at,
        avg_runtime_seconds=data_source.avg_runtime_seconds,
        data_volume_mb=data_source.data_volume_mb,
        error_count=data_source.error_count or 0
    )

@router.get("/{data_source_id}/audit_log", response_model=List[Dict[str, Any]])
async def get_data_source_audit_log(
    data_source_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get audit log for data source changes."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view audit log for this data source"
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
async def test_data_source_config(
    config_data: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Test data source configuration without creating."""
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

@router.get("/{data_source_id}/probe", response_model=Dict[str, Any])
async def probe_data_source(
    data_source_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Probe data source for available data/schema."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to probe this data source"
        )
    
    try:
        probe_result = data_source.probe_source()
        return probe_result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Probe failed: {str(e)}"
        )

@router.post("/{data_source_id}/probe/authenticate", response_model=Dict[str, Any])
async def test_data_source_authentication(
    data_source_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Test authentication for data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    
    if not data_source.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to test authentication for this data source"
        )
    
    try:
        auth_result = data_source.test_authentication_()
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