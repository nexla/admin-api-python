"""
Data Sets API endpoints - Python equivalent of Rails data_sets controller.
Handles CRUD operations for data sets, sharing, samples, transformations, and analytics.
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
from ...models.data_set import DataSet
from ...models.data_source import DataSource
from ...models.org import Org

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data_sets", tags=["Data Sets"])


# Pydantic models for request/response
class DataSetCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    runtime_config: Optional[Dict[str, Any]] = None
    data_source_id: int
    data_schema_id: Optional[int] = None
    
    @validator('name')
    def validate_name(cls, v):
        if len(v.strip()) < 1:
            raise ValueError('Data set name cannot be empty')
        return v.strip()


class DataSetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    runtime_config: Optional[Dict[str, Any]] = None
    data_schema_id: Optional[int] = None
    
    @validator('name')
    def validate_name(cls, v):
        if v is not None and len(v.strip()) < 1:
            raise ValueError('Data set name cannot be empty')
        return v.strip() if v else v


class DataSetResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    status: str
    runtime_status: Optional[str]
    config: Optional[Dict[str, Any]]
    runtime_config: Optional[Dict[str, Any]]
    data_source_id: int
    data_schema_id: Optional[int]
    owner_id: int
    org_id: int
    created_at: str
    updated_at: str


class DataSetListResponse(BaseModel):
    data_sets: List[DataSetResponse]
    total: int
    page: int
    per_page: int


class DataSetSample(BaseModel):
    sample_data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None


class DataSetTransform(BaseModel):
    transform_config: Dict[str, Any]
    mode: str = Field(default="add", pattern="^(add|replace|remove)$")


class ShareRequest(BaseModel):
    user_ids: Optional[List[int]] = None
    org_ids: Optional[List[int]] = None
    public: bool = False
    mode: str = Field(default="add", pattern="^(add|remove|reset)$")


class DataSetCharacteristics(BaseModel):
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    data_types: Optional[Dict[str, str]] = None
    null_counts: Optional[Dict[str, int]] = None
    unique_counts: Optional[Dict[str, int]] = None
    size_bytes: Optional[int] = None


# Helper functions
def _data_set_to_response(data_set: DataSet) -> DataSetResponse:
    """Convert DataSet model to response format"""
    return DataSetResponse(
        id=data_set.id,
        name=data_set.name,
        description=data_set.description,
        status=data_set.status,
        runtime_status=data_set.runtime_status,
        config=data_set.config,
        runtime_config=data_set.runtime_config,
        data_source_id=data_set.data_source_id,
        data_schema_id=data_set.data_schema_id,
        owner_id=data_set.owner_id,
        org_id=data_set.org_id,
        created_at=data_set.created_at.isoformat() if data_set.created_at else None,
        updated_at=data_set.updated_at.isoformat() if data_set.updated_at else None
    )


# Core CRUD endpoints
@router.post("/", response_model=DataSetResponse, summary="Create Data Set")
async def create_data_set(
    data_set_data: DataSetCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new data set.
    
    Equivalent to Rails DataSetsController#create
    """
    try:
        # Validate data source exists and user has access
        data_source = db.query(DataSource).filter(
            DataSource.id == data_set_data.data_source_id,
            DataSource.owner_id == current_user.id
        ).first()
        
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid data source or access denied"
            )
        
        # Create data set
        now = datetime.utcnow()
        data_set = DataSet(
            name=data_set_data.name,
            description=data_set_data.description,
            status="ACTIVE",
            config=data_set_data.config,
            runtime_config=data_set_data.runtime_config,
            data_source_id=data_set_data.data_source_id,
            data_schema_id=data_set_data.data_schema_id,
            owner_id=current_user.id,
            org_id=current_user.default_org_id,
            created_at=now,
            updated_at=now
        )
        
        db.add(data_set)
        db.commit()
        db.refresh(data_set)
        
        logger.info(f"Data set created: {data_set.name} by user {current_user.email}")
        
        return _data_set_to_response(data_set)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Data set creation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create data set"
        )


@router.get("/", response_model=DataSetListResponse, summary="List Data Sets")
async def list_data_sets(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    data_source_id: Optional[int] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List data sets for current user with filtering and pagination.
    
    Equivalent to Rails DataSetsController#index
    """
    try:
        offset = (page - 1) * per_page
        
        query = db.query(DataSet).filter(
            DataSet.owner_id == current_user.id
        )
        
        # Apply filters
        if search:
            query = query.filter(
                DataSet.name.contains(search) |
                DataSet.description.contains(search)
            )
        
        if status:
            query = query.filter(DataSet.status == status)
            
        if data_source_id:
            query = query.filter(DataSet.data_source_id == data_source_id)
        
        # Get total count
        total = query.count()
        
        # Apply pagination and ordering
        data_sets = query.order_by(DataSet.created_at.desc()).offset(offset).limit(per_page).all()
        
        # Convert to response format
        data_set_responses = [_data_set_to_response(ds) for ds in data_sets]
        
        return DataSetListResponse(
            data_sets=data_set_responses,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except Exception as e:
        logger.error(f"Data sets list error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list data sets"
        )


@router.get("/all", response_model=List[DataSetResponse], summary="Get All Data Sets")
async def get_all_data_sets(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get all data sets for current user without pagination.
    
    Equivalent to Rails DataSetsController#index_all
    """
    try:
        data_sets = db.query(DataSet).filter(
            DataSet.owner_id == current_user.id,
            DataSet.status == "ACTIVE"
        ).order_by(DataSet.name).all()
        
        return [_data_set_to_response(ds) for ds in data_sets]
        
    except Exception as e:
        logger.error(f"Get all data sets error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data sets"
        )


@router.get("/all/condensed", summary="Get Condensed Data Sets List")
async def get_condensed_data_sets(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get condensed list of data sets (id, name only).
    
    Equivalent to Rails DataSetsController#index_all_condensed
    """
    try:
        data_sets = db.query(DataSet.id, DataSet.name).filter(
            DataSet.owner_id == current_user.id,
            DataSet.status == "ACTIVE"
        ).order_by(DataSet.name).all()
        
        return [{"id": ds.id, "name": ds.name} for ds in data_sets]
        
    except Exception as e:
        logger.error(f"Get condensed data sets error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get condensed data sets"
        )


@router.get("/all/ids", summary="Get Data Set IDs")
async def get_data_set_ids(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get list of data set IDs only.
    
    Equivalent to Rails DataSetsController#index_all_ids
    """
    try:
        ids = db.query(DataSet.id).filter(
            DataSet.owner_id == current_user.id,
            DataSet.status == "ACTIVE"
        ).all()
        
        return [id_tuple[0] for id_tuple in ids]
        
    except Exception as e:
        logger.error(f"Get data set IDs error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data set IDs"
        )


@router.get("/shared", summary="Get Data Sets Shared by User")
async def get_shared_data_sets(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get data sets shared by the current user.
    
    Equivalent to Rails DataSetsController#shared_by_user
    """
    try:
        # TODO: Implement sharing logic when sharing tables are available
        # For now, return user's own data sets
        data_sets = db.query(DataSet).filter(
            DataSet.owner_id == current_user.id,
            DataSet.status == "ACTIVE"
        ).order_by(DataSet.name).all()
        
        return [_data_set_to_response(ds) for ds in data_sets]
        
    except Exception as e:
        logger.error(f"Get shared data sets error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get shared data sets"
        )


@router.get("/available", summary="Get Data Sets Available to User")
async def get_available_data_sets(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get data sets shared with the current user.
    
    Equivalent to Rails DataSetsController#shared_with_user
    """
    try:
        # TODO: Implement sharing logic when sharing tables are available
        # For now, return user's own data sets
        data_sets = db.query(DataSet).filter(
            DataSet.owner_id == current_user.id,
            DataSet.status == "ACTIVE"
        ).order_by(DataSet.name).all()
        
        return [_data_set_to_response(ds) for ds in data_sets]
        
    except Exception as e:
        logger.error(f"Get available data sets error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get available data sets"
        )


@router.get("/public", summary="Get Public Data Sets")
async def get_public_data_sets(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get publicly available data sets.
    
    Equivalent to Rails DataSetsController#public
    """
    try:
        # TODO: Implement public data sets logic
        # For now, return empty list
        return []
        
    except Exception as e:
        logger.error(f"Get public data sets error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get public data sets"
        )


@router.get("/{data_set_id}", response_model=DataSetResponse, summary="Get Data Set")
async def get_data_set(
    data_set_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific data set.
    
    Equivalent to Rails DataSetsController#show
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check access permissions
        if data_set.owner_id != current_user.id:
            # TODO: Check if user has shared access
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        return _data_set_to_response(data_set)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get data set error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data set"
        )


@router.put("/{data_set_id}", response_model=DataSetResponse, summary="Update Data Set")
async def update_data_set(
    data_set_id: int,
    data_set_data: DataSetUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a data set.
    
    Equivalent to Rails DataSetsController#update
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Update fields
        if data_set_data.name is not None:
            data_set.name = data_set_data.name
        if data_set_data.description is not None:
            data_set.description = data_set_data.description
        if data_set_data.config is not None:
            data_set.config = data_set_data.config
        if data_set_data.runtime_config is not None:
            data_set.runtime_config = data_set_data.runtime_config
        if data_set_data.data_schema_id is not None:
            data_set.data_schema_id = data_set_data.data_schema_id
        
        data_set.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(data_set)
        
        logger.info(f"Data set updated: {data_set.name} by user {current_user.email}")
        
        return _data_set_to_response(data_set)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data set update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data set"
        )


@router.delete("/{data_set_id}", summary="Delete Data Set")
async def delete_data_set(
    data_set_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete (deactivate) a data set.
    
    Equivalent to Rails DataSetsController#destroy
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Soft delete by setting status to DEACTIVATED
        data_set.status = "DEACTIVATED"
        data_set.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data set deleted: {data_set.name} by user {current_user.email}")
        
        return {"message": "Data set deleted successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data set delete error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete data set"
        )


# Lifecycle management endpoints
@router.put("/{data_set_id}/activate", summary="Activate Data Set")
async def activate_data_set(
    data_set_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Activate a data set.
    
    Equivalent to Rails DataSetsController#activate
    """
    return await _update_data_set_status(data_set_id, "ACTIVE", current_user, db)


@router.put("/{data_set_id}/pause", summary="Pause Data Set")
async def pause_data_set(
    data_set_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Pause a data set.
    
    Equivalent to Rails DataSetsController#activate with activate=false
    """
    return await _update_data_set_status(data_set_id, "PAUSED", current_user, db)


async def _update_data_set_status(data_set_id: int, new_status: str, current_user: User, db: Session):
    """Helper function to update data set status"""
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        data_set.status = new_status
        data_set.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data set status updated to {new_status}: {data_set.name} by user {current_user.email}")
        
        return {"message": f"Data set {new_status.lower()} successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Data set status update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update data set status"
        )


@router.put("/{data_set_id}/runtime_status/{status}", summary="Update Runtime Status")
async def update_runtime_status(
    data_set_id: int,
    status: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update data set runtime status.
    
    Equivalent to Rails DataSetsController#update_runtime_status
    """
    try:
        # Validate status
        valid_statuses = ['RUNNING', 'STOPPED', 'ERROR', 'PAUSED']
        if status not in valid_statuses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status. Must be one of: {valid_statuses}"
            )
        
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        data_set.runtime_status = status
        data_set.updated_at = datetime.utcnow()
        
        db.commit()
        
        logger.info(f"Data set runtime status updated to {status}: {data_set.name}")
        
        return {"message": f"Runtime status updated to {status}"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Runtime status update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update runtime status"
        )


# Data management endpoints
@router.get("/{data_set_id}/samples", summary="Get Data Set Samples")
async def get_data_set_samples(
    data_set_id: int,
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get sample data from a data set.
    
    Equivalent to Rails DataSetsController#samples
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual data sampling logic
        # For now, return mock data
        return {
            "samples": [
                {"id": 1, "name": "Sample 1", "value": 100},
                {"id": 2, "name": "Sample 2", "value": 200}
            ],
            "total_rows": 1000,
            "limit": limit,
            "metadata": {
                "columns": ["id", "name", "value"],
                "data_types": {"id": "integer", "name": "string", "value": "integer"}
            }
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get samples error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data set samples"
        )


@router.post("/{data_set_id}/samples", summary="Update Data Set Samples")
async def update_data_set_samples(
    data_set_id: int,
    sample_data: DataSetSample,
    replace: bool = Query(True, description="Replace existing samples if true, append if false"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update sample data for a data set.
    
    Equivalent to Rails DataSetsController#update_samples
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual sample data update logic
        data_set.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Data set samples updated: {data_set.name}")
        
        return {"message": "Sample data updated successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Update samples error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update sample data"
        )


@router.get("/{data_set_id}/characteristics", response_model=DataSetCharacteristics, summary="Get Data Set Characteristics")
async def get_data_set_characteristics(
    data_set_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get data set characteristics and statistics.
    
    Equivalent to Rails DataSetsController#get_characteristics
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual characteristics calculation
        # For now, return mock data
        return DataSetCharacteristics(
            row_count=1000,
            column_count=5,
            data_types={"id": "integer", "name": "string", "value": "integer", "created_at": "datetime", "updated_at": "datetime"},
            null_counts={"id": 0, "name": 5, "value": 10, "created_at": 0, "updated_at": 0},
            unique_counts={"id": 1000, "name": 950, "value": 800, "created_at": 1000, "updated_at": 1000},
            size_bytes=50000
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get characteristics error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data set characteristics"
        )


@router.get("/{data_set_id}/summary", summary="Get Data Set Summary")
async def get_data_set_summary(
    data_set_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get data set summary information.
    
    Equivalent to Rails DataSetsController#summary
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual summary logic
        return {
            "id": data_set.id,
            "name": data_set.name,
            "description": data_set.description,
            "status": data_set.status,
            "runtime_status": data_set.runtime_status,
            "row_count": 1000,
            "column_count": 5,
            "size_bytes": 50000,
            "last_updated": data_set.updated_at.isoformat() if data_set.updated_at else None,
            "data_source": {
                "id": data_set.data_source_id,
                "name": "Source Name"  # TODO: Join with data_source
            }
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Get summary error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get data set summary"
        )


# Transformation endpoints
@router.post("/{data_set_id}/transform", summary="Transform Data Set")
@router.put("/{data_set_id}/transform", summary="Transform Data Set")
async def transform_data_set(
    data_set_id: int,
    transform_data: DataSetTransform,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Apply transformation to a data set.
    
    Equivalent to Rails DataSetsController#transform
    """
    try:
        data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # TODO: Implement actual transformation logic
        data_set.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Data set transformation applied: {data_set.name}")
        
        return {
            "message": "Transformation applied successfully",
            "transform_id": f"transform_{data_set_id}_{int(datetime.utcnow().timestamp())}"
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Transform error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to apply transformation"
        )


@router.post("/{data_set_id}/copy", summary="Copy Data Set")
async def copy_data_set(
    data_set_id: int,
    copy_config: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a copy of a data set.
    
    Equivalent to Rails DataSetsController#copy
    """
    try:
        original_data_set = db.query(DataSet).filter(
            DataSet.id == data_set_id
        ).first()
        
        if not original_data_set:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data set not found"
            )
        
        # Check permissions
        if original_data_set.owner_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Create copy
        now = datetime.utcnow()
        new_name = copy_config.get('name', f"{original_data_set.name} (Copy)")
        
        copied_data_set = DataSet(
            name=new_name,
            description=copy_config.get('description', original_data_set.description),
            status="ACTIVE",
            config=original_data_set.config.copy() if original_data_set.config else {},
            runtime_config=original_data_set.runtime_config.copy() if original_data_set.runtime_config else None,
            data_source_id=original_data_set.data_source_id,
            data_schema_id=original_data_set.data_schema_id,
            owner_id=current_user.id,
            org_id=current_user.default_org_id,
            created_at=now,
            updated_at=now
        )
        
        db.add(copied_data_set)
        db.commit()
        db.refresh(copied_data_set)
        
        logger.info(f"Data set copied: {original_data_set.name} -> {copied_data_set.name}")
        
        return _data_set_to_response(copied_data_set)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Copy error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to copy data set"
        )


# Search endpoints
@router.post("/search", summary="Search Data Sets")
async def search_data_sets(
    search_params: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Search data sets with complex filters.
    
    Equivalent to Rails DataSetsController#search
    """
    try:
        query = db.query(DataSet).filter(
            DataSet.owner_id == current_user.id
        )
        
        # Apply search filters
        if search_params.get('name'):
            query = query.filter(DataSet.name.contains(search_params['name']))
        
        if search_params.get('data_source_id'):
            query = query.filter(DataSet.data_source_id == search_params['data_source_id'])
        
        if search_params.get('status'):
            query = query.filter(DataSet.status == search_params['status'])
        
        data_sets = query.order_by(DataSet.updated_at.desc()).all()
        
        return [_data_set_to_response(ds) for ds in data_sets]
        
    except Exception as e:
        logger.error(f"Data set search error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search data sets"
        )