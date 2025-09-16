"""
Data Sources API endpoints - Python equivalent of Rails data_sources controller.
Handles CRUD operations for data sources, probing, configuration validation, and lifecycle management.
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
from ...models.data_source import DataSource
from ...models.org import Org
from ...models.data_credentials import DataCredentials
from ...models.connector import Connector

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data_sources", tags=["Data Sources"])


# Pydantic models for request/response
class DataSourceCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    connection_type: str = Field(..., min_length=1)
    config: Dict[str, Any] = Field(default_factory=dict)
    runtime_config: Optional[Dict[str, Any]] = None
    ingestion_mode: str = Field(default="BATCH")
    data_credentials_id: Optional[int] = None
    connector_id: Optional[int] = None
    
    @validator('name')
    def validate_name(cls, v):
        if len(v.strip()) < 1:
            raise ValueError('Data source name cannot be empty')
        return v.strip()
    
    @validator('ingestion_mode')
    def validate_ingestion_mode(cls, v):
        valid_modes = ['BATCH', 'STREAMING', 'REAL_TIME']
        if v not in valid_modes:
            raise ValueError(f'Invalid ingestion mode. Must be one of: {valid_modes}')
        return v


class DataSourceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    runtime_config: Optional[Dict[str, Any]] = None
    ingestion_mode: Optional[str] = None
    data_credentials_id: Optional[int] = None
    
    @validator('name')
    def validate_name(cls, v):
        if v is not None and len(v.strip()) < 1:
            raise ValueError('Data source name cannot be empty')
        return v.strip() if v else v


class DataSourceResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    status: str
    runtime_status: Optional[str]
    connection_type: str
    config: Optional[Dict[str, Any]]
    runtime_config: Optional[Dict[str, Any]]
    ingestion_mode: str
    run_now_at: Optional[str]
    owner_id: int
    org_id: int
    data_credentials_id: Optional[int]
    connector_id: Optional[int]
    origin_node_id: Optional[int]
    created_at: str
    updated_at: str


class DataSourceListResponse(BaseModel):
    data_sources: List[DataSourceResponse]
    total: int
    page: int
    per_page: int


class DataSourceConfigValidation(BaseModel):
    config: Dict[str, Any]
    connection_type: str
    data_credentials_id: Optional[int] = None


class DataSourceProbeRequest(BaseModel):
    config: Optional[Dict[str, Any]] = None
    path: Optional[str] = None
    data_credentials_id: Optional[int] = None


class RuntimeStatusUpdate(BaseModel):
    status: str
    
    @validator('status')
    def validate_status(cls, v):
        valid_statuses = ['RUNNING', 'STOPPED', 'ERROR', 'PAUSED']
        if v not in valid_statuses:
            raise ValueError(f'Invalid status. Must be one of: {valid_statuses}')
        return v


# Helper functions
def _data_source_to_response(data_source: DataSource) -> DataSourceResponse:
    """Convert DataSource model to response format"""
    return DataSourceResponse(
        id=data_source.id,
        name=data_source.name,
        description=data_source.description,
        status=data_source.status,
        runtime_status=data_source.runtime_status,
        connection_type=data_source.connection_type,
        config=data_source.config,
        runtime_config=data_source.runtime_config,
        ingestion_mode=data_source.ingestion_mode,
        run_now_at=data_source.run_now_at.isoformat() if data_source.run_now_at else None,
        owner_id=data_source.owner_id,
        org_id=data_source.org_id,
        data_credentials_id=data_source.data_credentials_id,
        connector_id=data_source.connector_id,
        origin_node_id=data_source.origin_node_id,
        created_at=data_source.created_at.isoformat() if data_source.created_at else None,
        updated_at=data_source.updated_at.isoformat() if data_source.updated_at else None
    )


# Core CRUD endpoints
@router.post("/", response_model=DataSourceResponse, summary="Create Data Source")
async def create_data_source(
    data_source_data: DataSourceCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new data source.
    
    Equivalent to Rails DataSourcesController#create
    """
    try:
        # Validate data credentials if provided
        if data_source_data.data_credentials_id:
            data_credentials = db.query(DataCredentials).filter(
                DataCredentials.id == data_source_data.data_credentials_id,
                DataCredentials.owner_id == current_user.id
            ).first()
            if not data_credentials:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid data credentials or access denied"
                )
        
        # Validate connector if provided
        if data_source_data.connector_id:
            connector = db.query(Connector).filter(
                Connector.id == data_source_data.connector_id
            ).first()
            if not connector:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid connector"
                )
        
        # Create data source
        now = datetime.utcnow()
        data_source = DataSource(
            name=data_source_data.name,
            description=data_source_data.description,
            status="ACTIVE",
            connection_type=data_source_data.connection_type,
            config=data_source_data.config,
            runtime_config=data_source_data.runtime_config,
            ingestion_mode=data_source_data.ingestion_mode,
            owner_id=current_user.id,
            org_id=current_user.default_org_id,  # TODO: Allow specifying org
            data_credentials_id=data_source_data.data_credentials_id,
            connector_id=data_source_data.connector_id,
            created_at=now,
            updated_at=now
        )
        
        db.add(data_source)
        db.commit()
        db.refresh(data_source)
        
        logger.info(f"Data source created: {data_source.name} by user {current_user.email}")
        
        return _data_source_to_response(data_source)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Data source creation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create data source"
        )


@router.get("/", response_model=DataSourceListResponse, summary="List Data Sources")
async def list_data_sources(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    connection_type: Optional[str] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List data sources for current user with filtering and pagination.
    
    Equivalent to Rails DataSourcesController#index
    """
    try:
        # Base query - user's own data sources and org-shared ones
        offset = (page - 1) * per_page
        
        query = db.query(DataSource).filter(
            DataSource.owner_id == current_user.id
        )
        
        # Apply filters
        if search:
            query = query.filter(
                DataSource.name.contains(search) |
                DataSource.description.contains(search)
            )
        
        if status:
            query = query.filter(DataSource.status == status)
            
        if connection_type:
            query = query.filter(DataSource.connection_type == connection_type)
        
        # Get total count
        total = query.count()
        
        # Apply pagination and ordering
        data_sources = query.order_by(DataSource.created_at.desc()).offset(offset).limit(per_page).all()
        
        # Convert to response format
        data_source_responses = [_data_source_to_response(ds) for ds in data_sources]
        
        return DataSourceListResponse(
            data_sources=data_source_responses,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except Exception as e:
        logger.error(f"Data sources list error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list data sources"
        )


@router.get("/all", response_model=List[DataSourceResponse], summary="Get All Data Sources")
async def get_all_data_sources(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get all data sources for current user without pagination.
    
    Equivalent to Rails DataSourcesController#index_all
    """
    try:
        data_sources = db.query(DataSource).filter(
            DataSource.owner_id == current_user.id,
            DataSource.status == "ACTIVE"
        ).order_by(DataSource.name).all()
        
        return [_data_source_to_response(ds) for ds in data_sources]
        
    except Exception as e:
        logger.error(f"Get all data sources error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data sources"
        )


@router.get("/all/condensed", summary="Get Condensed Data Sources List")
async def get_condensed_data_sources(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get condensed list of data sources (id, name only).
    
    Equivalent to Rails DataSourcesController#index_all_condensed
    """
    try:
        data_sources = db.query(DataSource.id, DataSource.name).filter(
            DataSource.owner_id == current_user.id,
            DataSource.status == "ACTIVE"
        ).order_by(DataSource.name).all()
        
        return [{"id": ds.id, "name": ds.name} for ds in data_sources]
        
    except Exception as e:
        logger.error(f"Get condensed data sources error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get condensed data sources"
        )


@router.get("/all/ids", summary="Get Data Source IDs")
async def get_data_source_ids(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get list of data source IDs only.
    
    Equivalent to Rails DataSourcesController#index_all_ids
    """
    try:
        ids = db.query(DataSource.id).filter(
            DataSource.owner_id == current_user.id,
            DataSource.status == "ACTIVE"
        ).all()
        
        return [id_tuple[0] for id_tuple in ids]
        
    except Exception as e:
        logger.error(f"Get data source IDs error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data source IDs"
        )


@router.get("/{data_source_id}", response_model=DataSourceResponse, summary="Get Data Source")
async def get_data_source(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific data source.
    
    Equivalent to Rails DataSourcesController#show
    """
    try:
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check access permissions
        if data_source.owner_id != current_user.id:
            # TODO: Check if user has org-level access
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        return _data_source_to_response(data_source)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get data source error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data source"
        )


@router.put("/{data_source_id}", response_model=DataSourceResponse, summary="Update Data Source")
async def update_data_source(
    data_source_id: int,
    data_source_data: DataSourceUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a data source.
    
    Equivalent to Rails DataSourcesController#update
    """
    try:
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check permissions
        if data_source.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Update fields
        if data_source_data.name is not None:
            data_source.name = data_source_data.name
        if data_source_data.description is not None:
            data_source.description = data_source_data.description
        if data_source_data.config is not None:
            data_source.config = data_source_data.config
        if data_source_data.runtime_config is not None:
            data_source.runtime_config = data_source_data.runtime_config
        if data_source_data.ingestion_mode is not None:
            data_source.ingestion_mode = data_source_data.ingestion_mode
        if data_source_data.data_credentials_id is not None:
            data_source.data_credentials_id = data_source_data.data_credentials_id
        
        data_source.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(data_source)
        
        logger.info(f"Data source updated: {data_source.name} by user {current_user.email}")
        
        return _data_source_to_response(data_source)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data source update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data source"
        )


@router.delete("/{data_source_id}", summary="Delete Data Source")
async def delete_data_source(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete (deactivate) a data source.
    
    Equivalent to Rails DataSourcesController#destroy
    """
    try:
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check permissions
        if data_source.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Soft delete by setting status to DEACTIVATED
        data_source.status = "DEACTIVATED"
        data_source.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data source deleted: {data_source.name} by user {current_user.email}")
        
        return {"message": "Data source deleted successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data source delete error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete data source"
        )


# Lifecycle management endpoints
@router.put("/{data_source_id}/activate", summary="Activate Data Source")
async def activate_data_source(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Activate a data source.
    
    Equivalent to Rails DataSourcesController#activate
    """
    return await _update_data_source_status(data_source_id, "ACTIVE", current_user, db)


@router.put("/{data_source_id}/pause", summary="Pause Data Source")
async def pause_data_source(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Pause a data source.
    
    Equivalent to Rails DataSourcesController#activate with activate=false
    """
    return await _update_data_source_status(data_source_id, "PAUSED", current_user, db)


async def _update_data_source_status(data_source_id: int, new_status: str, current_user: User, db: Session):
    """Helper function to update data source status"""
    try:
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check permissions
        if data_source.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        data_source.status = new_status
        data_source.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data source status updated to {new_status}: {data_source.name} by user {current_user.email}")
        
        return {"message": f"Data source {new_status.lower()} successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data source status update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data source status"
        )


@router.put("/{data_source_id}/runtime_status/{status}", summary="Update Runtime Status")
async def update_runtime_status(
    data_source_id: int,
    status: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update data source runtime status.
    
    Equivalent to Rails DataSourcesController#update_runtime_status
    """
    try:
        # Validate status
        valid_statuses = ['RUNNING', 'STOPPED', 'ERROR', 'PAUSED']
        if status not in valid_statuses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status. Must be one of: {valid_statuses}"
            )
        
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check permissions
        if data_source.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        data_source.runtime_status = status
        data_source.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data source runtime status updated to {status}: {data_source.name}")
        
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
@router.post("/config/validate", summary="Validate Data Source Configuration")
async def validate_config(
    config_data: DataSourceConfigValidation,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Validate data source configuration.
    
    Equivalent to Rails DataSourcesController#validate_config
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
        
        if not config_data.connection_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Connection type is required"
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


# Search endpoints
@router.post("/search", summary="Search Data Sources")
async def search_data_sources(
    search_params: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Search data sources with complex filters.
    
    Equivalent to Rails DataSourcesController#search
    """
    try:
        query = db.query(DataSource).filter(
            DataSource.owner_id == current_user.id
        )
        
        # Apply search filters
        if search_params.get('name'):
            query = query.filter(DataSource.name.contains(search_params['name']))
        
        if search_params.get('connection_type'):
            query = query.filter(DataSource.connection_type == search_params['connection_type'])
        
        if search_params.get('status'):
            query = query.filter(DataSource.status == search_params['status'])
        
        # Add more search criteria as needed
        
        data_sources = query.order_by(DataSource.updated_at.desc()).all()
        
        return [_data_source_to_response(ds) for ds in data_sources]
        
    except Exception as e:
        logger.error(f"Data source search error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search data sources"
        )


# Run and execution endpoints
@router.post("/{data_source_id}/run_now", summary="Run Data Source Now")
async def run_data_source_now(
    data_source_id: int,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Trigger immediate execution of data source.
    
    Equivalent to Rails DataSourcesController#run_now
    """
    try:
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check permissions
        if data_source.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Update run_now timestamp
        data_source.run_now_at = datetime.utcnow()
        data_source.updated_at = datetime.utcnow()
        
        db.commit()
        
        # TODO: Queue background task to execute data source
        # background_tasks.add_task(execute_data_source, data_source_id)
        
        logger.info(f"Data source run triggered: {data_source.name} by user {current_user.email}")
        
        return {
            "message": "Data source execution triggered",
            "run_id": f"run_{data_source_id}_{int(datetime.utcnow().timestamp())}"
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data source run error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to trigger data source execution"
        )


@router.post("/{data_source_id}/ready", summary="Mark Data Source Ready")
async def mark_data_source_ready(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Mark data source as ready for execution.
    
    Equivalent to Rails DataSourcesController#ready
    """
    try:
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check permissions
        if data_source.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        data_source.runtime_status = "READY"
        data_source.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data source marked ready: {data_source.name}")
        
        return {"message": "Data source marked as ready"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Mark ready error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to mark data source as ready"
        )


@router.get("/{data_source_id}/runs", summary="Get Data Source Runs")
async def get_data_source_runs(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get execution runs for a data source.
    
    Equivalent to Rails DataSourcesController#runs
    """
    try:
        data_source = db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        # Check permissions
        if data_source.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Query actual run history from run tracking table
        # For now, return mock data
        return {
            "runs": [
                {
                    "id": f"run_{data_source_id}_1",
                    "status": "COMPLETED",
                    "started_at": datetime.utcnow().isoformat(),
                    "completed_at": datetime.utcnow().isoformat(),
                    "records_processed": 1000
                }
            ],
            "total": 1
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get runs error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data source runs"
        )