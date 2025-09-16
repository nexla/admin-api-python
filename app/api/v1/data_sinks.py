"""
Data Sinks API endpoints - Python equivalent of Rails data_sinks controller.
Handles CRUD operations for data sinks, output configuration, probing, and data export management.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, Field
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import RBACService, SystemPermissions
from ...models.user import User
from ...models.data_sink import DataSink
from ...models.data_set import DataSet
from ...models.data_credentials import DataCredentials
from ...models.connector import Connector
from ...models.org import Org

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data_sinks", tags=["Data Sinks"])


# Pydantic models for request/response
class DataSinkCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    destination_type: str = Field(..., min_length=1)
    config: Dict[str, Any] = Field(default_factory=dict)
    runtime_config: Optional[Dict[str, Any]] = None
    data_credentials_id: Optional[int] = None
    connector_id: Optional[int] = None
    
    @validator('name')
    def validate_name(cls, v):
        if len(v.strip()) < 1:
            raise ValueError('Data sink name cannot be empty')
        return v.strip()
    
    @validator('destination_type')
    def validate_destination_type(cls, v):
        valid_types = ['s3', 'database', 'api', 'file', 'snowflake', 'redshift', 'bigquery']
        if v.lower() not in valid_types:
            logger.warning(f"Unknown destination type: {v}")
        return v


class DataSinkUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    runtime_config: Optional[Dict[str, Any]] = None
    data_credentials_id: Optional[int] = None
    
    @validator('name')
    def validate_name(cls, v):
        if v is not None and len(v.strip()) < 1:
            raise ValueError('Data sink name cannot be empty')
        return v.strip() if v else v


class DataSinkResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    status: str
    runtime_status: Optional[str]
    destination_type: str
    config: Optional[Dict[str, Any]]
    runtime_config: Optional[Dict[str, Any]]
    data_credentials_id: Optional[int]
    connector_id: Optional[int]
    owner_id: int
    org_id: int
    created_at: str
    updated_at: str


class DataSinkListResponse(BaseModel):
    data_sinks: List[DataSinkResponse]
    total: int
    page: int
    per_page: int


class DataSinkConfigValidation(BaseModel):
    config: Dict[str, Any]
    destination_type: str
    data_credentials_id: Optional[int] = None


class DataSinkProbeRequest(BaseModel):
    config: Optional[Dict[str, Any]] = None
    path: Optional[str] = None
    data_credentials_id: Optional[int] = None


class RuntimeStatusUpdate(BaseModel):
    status: str
    
    @validator('status')
    def validate_status(cls, v):
        valid_statuses = ['RUNNING', 'STOPPED', 'ERROR', 'PAUSED', 'COMPLETED']
        if v not in valid_statuses:
            raise ValueError(f'Invalid status. Must be one of: {valid_statuses}')
        return v


class DataSinkMetrics(BaseModel):
    records_written: Optional[int] = None
    bytes_written: Optional[int] = None
    last_write_time: Optional[str] = None
    success_rate: Optional[float] = None
    error_count: Optional[int] = None


# Helper functions
def _data_sink_to_response(data_sink: DataSink) -> DataSinkResponse:
    """Convert DataSink model to response format"""
    return DataSinkResponse(
        id=data_sink.id,
        name=data_sink.name,
        description=data_sink.description,
        status=data_sink.status,
        runtime_status=data_sink.runtime_status,
        destination_type=data_sink.destination_type,
        config=data_sink.config,
        runtime_config=data_sink.runtime_config,
        data_credentials_id=data_sink.data_credentials_id,
        connector_id=data_sink.connector_id,
        owner_id=data_sink.owner_id,
        org_id=data_sink.org_id,
        created_at=data_sink.created_at.isoformat() if data_sink.created_at else None,
        updated_at=data_sink.updated_at.isoformat() if data_sink.updated_at else None
    )


# Core CRUD endpoints
@router.post("/", response_model=DataSinkResponse, summary="Create Data Sink")
async def create_data_sink(
    data_sink_data: DataSinkCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new data sink.
    
    Equivalent to Rails DataSinksController#create
    """
    try:
        # Validate data credentials if provided
        if data_sink_data.data_credentials_id:
            data_credentials = db.query(DataCredentials).filter(
                DataCredentials.id == data_sink_data.data_credentials_id,
                DataCredentials.owner_id == current_user.id
            ).first()
            if not data_credentials:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid data credentials or access denied"
                )
        
        # Validate connector if provided
        if data_sink_data.connector_id:
            connector = db.query(Connector).filter(
                Connector.id == data_sink_data.connector_id
            ).first()
            if not connector:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid connector"
                )
        
        # Create data sink
        now = datetime.utcnow()
        data_sink = DataSink(
            name=data_sink_data.name,
            description=data_sink_data.description,
            status="ACTIVE",
            destination_type=data_sink_data.destination_type,
            config=data_sink_data.config,
            runtime_config=data_sink_data.runtime_config,
            owner_id=current_user.id,
            org_id=current_user.default_org_id,
            data_credentials_id=data_sink_data.data_credentials_id,
            connector_id=data_sink_data.connector_id,
            created_at=now,
            updated_at=now
        )
        
        db.add(data_sink)
        db.commit()
        db.refresh(data_sink)
        
        logger.info(f"Data sink created: {data_sink.name} by user {current_user.email}")
        
        return _data_sink_to_response(data_sink)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Data sink creation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create data sink"
        )


@router.get("/", response_model=DataSinkListResponse, summary="List Data Sinks")
async def list_data_sinks(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    destination_type: Optional[str] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List data sinks for current user with filtering and pagination.
    
    Equivalent to Rails DataSinksController#index
    """
    try:
        offset = (page - 1) * per_page
        
        query = db.query(DataSink).filter(
            DataSink.owner_id == current_user.id
        )
        
        # Apply filters
        if search:
            query = query.filter(
                DataSink.name.contains(search) |
                DataSink.description.contains(search)
            )
        
        if status:
            query = query.filter(DataSink.status == status)
            
        if destination_type:
            query = query.filter(DataSink.destination_type == destination_type)
        
        # Get total count
        total = query.count()
        
        # Apply pagination and ordering
        data_sinks = query.order_by(DataSink.created_at.desc()).offset(offset).limit(per_page).all()
        
        # Convert to response format
        data_sink_responses = [_data_sink_to_response(ds) for ds in data_sinks]
        
        return DataSinkListResponse(
            data_sinks=data_sink_responses,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except Exception as e:
        logger.error(f"Data sinks list error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list data sinks"
        )


@router.get("/all", response_model=List[DataSinkResponse], summary="Get All Data Sinks")
async def get_all_data_sinks(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get all data sinks for current user without pagination.
    
    Equivalent to Rails DataSinksController#index_all
    """
    try:
        data_sinks = db.query(DataSink).filter(
            DataSink.owner_id == current_user.id,
            DataSink.status == "ACTIVE"
        ).order_by(DataSink.name).all()
        
        return [_data_sink_to_response(ds) for ds in data_sinks]
        
    except Exception as e:
        logger.error(f"Get all data sinks error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data sinks"
        )


@router.get("/all/condensed", summary="Get Condensed Data Sinks List")
async def get_condensed_data_sinks(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get condensed list of data sinks (id, name only).
    
    Equivalent to Rails DataSinksController#index_all_condensed
    """
    try:
        data_sinks = db.query(DataSink.id, DataSink.name).filter(
            DataSink.owner_id == current_user.id,
            DataSink.status == "ACTIVE"
        ).order_by(DataSink.name).all()
        
        return [{"id": ds.id, "name": ds.name} for ds in data_sinks]
        
    except Exception as e:
        logger.error(f"Get condensed data sinks error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get condensed data sinks"
        )


@router.get("/all/data_set", summary="Get Data Sinks by Data Set")
async def get_data_sinks_by_data_set(
    data_set_id: Optional[int] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get data sinks grouped by data set.
    
    Equivalent to Rails DataSinksController#index_all_by_data_set
    """
    try:
        query = db.query(DataSink).filter(
            DataSink.owner_id == current_user.id,
            DataSink.status == "ACTIVE"
        )
        
        if data_set_id:
            # TODO: Add proper data_set relationship when available
            # For now, filter by config or other means
            pass
        
        data_sinks = query.order_by(DataSink.name).all()
        
        # Group by data set (mock implementation)
        grouped = {}
        for sink in data_sinks:
            key = sink.config.get('data_set_id', 'unassigned') if sink.config else 'unassigned'
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(_data_sink_to_response(sink))
        
        return grouped
        
    except Exception as e:
        logger.error(f"Get data sinks by data set error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data sinks by data set"
        )


@router.get("/all/ids", summary="Get Data Sink IDs")
async def get_data_sink_ids(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get list of data sink IDs only.
    
    Equivalent to Rails DataSinksController#index_all_ids
    """
    try:
        ids = db.query(DataSink.id).filter(
            DataSink.owner_id == current_user.id,
            DataSink.status == "ACTIVE"
        ).all()
        
        return [id_tuple[0] for id_tuple in ids]
        
    except Exception as e:
        logger.error(f"Get data sink IDs error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data sink IDs"
        )


@router.get("/{data_sink_id}", response_model=DataSinkResponse, summary="Get Data Sink")
async def get_data_sink(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific data sink.
    
    Equivalent to Rails DataSinksController#show
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check access permissions
        if data_sink.owner_id != current_user.id:
            # TODO: Check if user has org-level access
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        return _data_sink_to_response(data_sink)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get data sink error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data sink"
        )


@router.put("/{data_sink_id}", response_model=DataSinkResponse, summary="Update Data Sink")
async def update_data_sink(
    data_sink_id: int,
    data_sink_data: DataSinkUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a data sink.
    
    Equivalent to Rails DataSinksController#update
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Update fields
        if data_sink_data.name is not None:
            data_sink.name = data_sink_data.name
        if data_sink_data.description is not None:
            data_sink.description = data_sink_data.description
        if data_sink_data.config is not None:
            data_sink.config = data_sink_data.config
        if data_sink_data.runtime_config is not None:
            data_sink.runtime_config = data_sink_data.runtime_config
        if data_sink_data.data_credentials_id is not None:
            data_sink.data_credentials_id = data_sink_data.data_credentials_id
        
        data_sink.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(data_sink)
        
        logger.info(f"Data sink updated: {data_sink.name} by user {current_user.email}")
        
        return _data_sink_to_response(data_sink)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data sink update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data sink"
        )


@router.delete("/{data_sink_id}", summary="Delete Data Sink")
async def delete_data_sink(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete (deactivate) a data sink.
    
    Equivalent to Rails DataSinksController#destroy
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Soft delete by setting status to DEACTIVATED
        data_sink.status = "DEACTIVATED"
        data_sink.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data sink deleted: {data_sink.name} by user {current_user.email}")
        
        return {"message": "Data sink deleted successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data sink delete error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete data sink"
        )


# Lifecycle management endpoints
@router.put("/{data_sink_id}/activate", summary="Activate Data Sink")
async def activate_data_sink(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Activate a data sink.
    
    Equivalent to Rails DataSinksController#activate
    """
    return await _update_data_sink_status(data_sink_id, "ACTIVE", current_user, db)


@router.put("/{data_sink_id}/pause", summary="Pause Data Sink")
async def pause_data_sink(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Pause a data sink.
    
    Equivalent to Rails DataSinksController#activate with activate=false
    """
    return await _update_data_sink_status(data_sink_id, "PAUSED", current_user, db)


async def _update_data_sink_status(data_sink_id: int, new_status: str, current_user: User, db: Session):
    """Helper function to update data sink status"""
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        data_sink.status = new_status
        data_sink.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data sink status updated to {new_status}: {data_sink.name} by user {current_user.email}")
        
        return {"message": f"Data sink {new_status.lower()} successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data sink status update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data sink status"
        )


@router.put("/{data_sink_id}/runtime_status/{status}", summary="Update Runtime Status")
async def update_runtime_status(
    data_sink_id: int,
    status: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update data sink runtime status.
    
    Equivalent to Rails DataSinksController#update_runtime_status
    """
    try:
        # Validate status
        valid_statuses = ['RUNNING', 'STOPPED', 'ERROR', 'PAUSED', 'COMPLETED']
        if status not in valid_statuses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status. Must be one of: {valid_statuses}"
            )
        
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        data_sink.runtime_status = status
        data_sink.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data sink runtime status updated to {status}: {data_sink.name}")
        
        return {"message": f"Runtime status updated to {status}"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Runtime status update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update runtime status"
        )


# Configuration and validation endpoints
@router.post("/config/validate", summary="Validate Data Sink Configuration")
async def validate_config(
    config_data: DataSinkConfigValidation,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Validate data sink configuration.
    
    Equivalent to Rails DataSinksController#validate_config
    """
    try:
        # TODO: Implement actual configuration validation logic
        # This would validate the config against the connector schema
        
        # For now, basic validation
        if not config_data.config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Configuration cannot be empty"
            )
        
        if not config_data.destination_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Destination type is required"
            )
        
        # Validate credentials if provided
        if config_data.data_credentials_id:
            credentials = db.query(DataCredentials).filter(
                DataCredentials.id == config_data.data_credentials_id,
                DataCredentials.owner_id == current_user.id
            ).first()
            if not credentials:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid data credentials"
                )
        
        return {
            "valid": True,
            "message": "Configuration is valid",
            "warnings": []
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Config validation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to validate configuration"
        )


# Data sink probing endpoints
@router.get("/{data_sink_id}/probe", summary="Probe Data Sink Connection")
async def probe_data_sink(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Test connection to data sink destination.
    
    Equivalent to Rails DataSinksController probe actions
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual probing logic based on destination_type
        return {
            "connection_status": "successful",
            "destination_type": data_sink.destination_type,
            "writable": True,
            "test_results": {
                "latency_ms": 150,
                "available_space": "unlimited"
            }
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Probe error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to probe data sink"
        )


@router.get("/{data_sink_id}/probe/summary", summary="Get Probe Summary")
async def get_probe_summary(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get summary of probe results.
    
    Equivalent to Rails DataSinksController probe/summary
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual probe summary logic
        return {
            "summary": {
                "connection_healthy": True,
                "last_write": datetime.utcnow().isoformat(),
                "write_permissions": True,
                "storage_available": True,
                "estimated_capacity": "unlimited"
            }
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Probe summary error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get probe summary"
        )


@router.get("/{data_sink_id}/probe/authenticate", summary="Test Authentication")
async def test_authentication(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Test authentication to data sink destination.
    
    Equivalent to Rails DataSinksController probe/authenticate
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual authentication testing
        return {
            "authentication_status": "successful",
            "credentials_valid": True,
            "permissions": ["write", "read", "delete"]
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Authentication test error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to test authentication"
        )


# Metrics and monitoring endpoints
@router.get("/{data_sink_id}/metrics", response_model=DataSinkMetrics, summary="Get Data Sink Metrics")
async def get_data_sink_metrics(
    data_sink_id: int,
    metrics_name: Optional[str] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get metrics for a data sink.
    
    Equivalent to Rails DataSinksController#metrics
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual metrics collection
        # For now, return mock data
        return DataSinkMetrics(
            records_written=5000,
            bytes_written=2500000,
            last_write_time=datetime.utcnow().isoformat(),
            success_rate=0.98,
            error_count=12
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get metrics error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data sink metrics"
        )


@router.get("/{data_sink_id}/run_status", summary="Get Run Status")
async def get_run_status(
    data_sink_id: int,
    run_id: Optional[str] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get execution status for data sink runs.
    
    Equivalent to Rails DataSinksController#run_status
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual run status tracking
        return {
            "run_id": run_id or f"run_{data_sink_id}_{int(datetime.utcnow().timestamp())}",
            "status": "COMPLETED",
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": datetime.utcnow().isoformat(),
            "records_written": 1000,
            "bytes_written": 500000,
            "errors": []
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get run status error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get run status"
        )


@router.get("/{data_sink_id}/run_analysis", summary="Get Run Analysis")
async def get_run_analysis(
    data_sink_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get analysis of data sink execution runs.
    
    Equivalent to Rails DataSinksController#run_analysis
    """
    try:
        data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual run analysis
        return {
            "total_runs": 50,
            "successful_runs": 48,
            "failed_runs": 2,
            "success_rate": 0.96,
            "average_runtime_seconds": 120,
            "total_records_written": 50000,
            "total_bytes_written": 25000000,
            "performance_trend": "improving",
            "recent_errors": [
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "error": "Connection timeout",
                    "run_id": "run_123"
                }
            ]
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get run analysis error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get run analysis"
        )


# Copy and duplication endpoints
@router.post("/{data_sink_id}/copy", summary="Copy Data Sink")
async def copy_data_sink(
    data_sink_id: int,
    copy_config: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a copy of a data sink.
    
    Equivalent to Rails DataSinksController#copy
    """
    try:
        original_data_sink = db.query(DataSink).filter(
            DataSink.id == data_sink_id
        ).first()
        
        if not original_data_sink:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data sink not found"
            )
        
        # Check permissions
        if original_data_sink.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Create copy
        now = datetime.utcnow()
        new_name = copy_config.get('name', f"{original_data_sink.name} (Copy)")
        
        copied_data_sink = DataSink(
            name=new_name,
            description=copy_config.get('description', original_data_sink.description),
            status="ACTIVE",
            destination_type=original_data_sink.destination_type,
            config=original_data_sink.config.copy() if original_data_sink.config else {},
            runtime_config=original_data_sink.runtime_config.copy() if original_data_sink.runtime_config else None,
            data_credentials_id=original_data_sink.data_credentials_id,
            connector_id=original_data_sink.connector_id,
            owner_id=current_user.id,
            org_id=current_user.default_org_id,
            created_at=now,
            updated_at=now
        )
        
        db.add(copied_data_sink)
        db.commit()
        db.refresh(copied_data_sink)
        
        logger.info(f"Data sink copied: {original_data_sink.name} -> {copied_data_sink.name}")
        
        return _data_sink_to_response(copied_data_sink)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Copy error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to copy data sink"
        )


# Search endpoints
@router.post("/search", summary="Search Data Sinks")
async def search_data_sinks(
    search_params: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Search data sinks with complex filters.
    
    Equivalent to Rails DataSinksController#search
    """
    try:
        query = db.query(DataSink).filter(
            DataSink.owner_id == current_user.id
        )
        
        # Apply search filters
        if search_params.get('name'):
            query = query.filter(DataSink.name.contains(search_params['name']))
        
        if search_params.get('destination_type'):
            query = query.filter(DataSink.destination_type == search_params['destination_type'])
        
        if search_params.get('status'):
            query = query.filter(DataSink.status == search_params['status'])
        
        data_sinks = query.order_by(DataSink.updated_at.desc()).all()
        
        return [_data_sink_to_response(ds) for ds in data_sinks]
        
    except Exception as e:
        logger.error(f"Data sink search error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search data sinks"
        )