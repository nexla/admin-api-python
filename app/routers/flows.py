from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, File, UploadFile, BackgroundTasks, Request
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from pydantic import BaseModel, Field, validator
from datetime import datetime, timedelta
import json

from app.database import get_db
from app.auth import get_current_user
from app.auth.rbac import RBACService, SystemPermissions
from app.models.user import User
from app.models.flow_node import FlowNode
from app.models.flow import Flow
from app.models.org import Org
from app.models.project import Project
from app.services.audit_service import AuditService
from app.services.validation_service import ValidationService
from app.services.async_tasks.manager import AsyncTaskManager

router = APIRouter()

# Pydantic models for request/response validation
class FlowNodeBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    flow_type: str = Field(..., min_length=1, max_length=50)
    source_config: Optional[Dict[str, Any]] = None
    sink_config: Optional[Dict[str, Any]] = None
    transform_config: Optional[Dict[str, Any]] = None
    schedule_config: Optional[Dict[str, Any]] = None
    notification_config: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None

class FlowNodeCreate(FlowNodeBase):
    org_id: Optional[int] = None
    project_id: Optional[int] = None
    
class FlowNodeUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    flow_type: Optional[str] = Field(None, min_length=1, max_length=50)
    source_config: Optional[Dict[str, Any]] = None
    sink_config: Optional[Dict[str, Any]] = None
    transform_config: Optional[Dict[str, Any]] = None
    schedule_config: Optional[Dict[str, Any]] = None
    notification_config: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None

class FlowNodeResponse(FlowNodeBase):
    id: int
    org_id: int
    project_id: Optional[int]
    status: str
    created_at: datetime
    updated_at: Optional[datetime]
    created_by_id: int
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    run_count: int
    error_count: int
    success_rate: Optional[float]
    last_error: Optional[str]
    is_active: bool
    
    class Config:
        from_attributes = True

class FlowNodeSearch(BaseModel):
    name: Optional[str] = None
    flow_type: Optional[str] = None
    status: Optional[str] = None
    project_id: Optional[int] = None
    created_by_id: Optional[int] = None
    tags: Optional[List[str]] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    last_run_after: Optional[datetime] = None
    last_run_before: Optional[datetime] = None

class FlowNodeCopy(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    copy_config: bool = Field(True)
    copy_schedule: bool = Field(False)
    copy_notifications: bool = Field(False)

class FlowRunRequest(BaseModel):
    force: bool = Field(False)
    async_execution: bool = Field(True)
    run_config: Optional[Dict[str, Any]] = None

class FlowScheduleUpdate(BaseModel):
    enabled: bool
    cron_expression: Optional[str] = None
    timezone: Optional[str] = Field("UTC")
    max_retries: Optional[int] = Field(3, ge=0, le=10)
    retry_delay: Optional[int] = Field(300, ge=60, le=3600)
    
    @validator('cron_expression')
    def validate_cron(cls, v):
        if v:
            result = ValidationService.validate_cron_expression(v)
            if not result['valid']:
                raise ValueError(f"Invalid cron expression: {'; '.join(result['errors'])}")
            return result['cron']
        return v

class FlowExecutionRequest(BaseModel):
    priority: str = Field("medium", pattern="^(low|medium|high|urgent)$")
    parameters: Optional[Dict[str, Any]] = None
    timeout_seconds: Optional[int] = Field(None, ge=60, le=86400)
    retry_on_failure: bool = True
    send_notifications: bool = True

class FlowBulkAction(BaseModel):
    flow_ids: List[int]
    action: str = Field(..., pattern="^(start|stop|pause|resume|delete)$")
    force: bool = False

class FlowMetricsResponse(BaseModel):
    total_runs: int
    successful_runs: int
    failed_runs: int
    avg_runtime_seconds: float
    success_rate: float
    last_24h_runs: int
    last_7d_runs: int
    error_rate: float
    data_processed_mb: int
    estimated_cost: float

class FlowStatusResponse(BaseModel):
    id: int
    name: str
    status: str
    current_run_id: Optional[str] = None
    progress_percentage: Optional[int] = None
    estimated_completion: Optional[datetime] = None
    runtime_seconds: Optional[int] = None
    error_message: Optional[str] = None
    last_heartbeat: Optional[datetime] = None

# Core CRUD operations
@router.get("/", response_model=List[FlowNodeResponse])
async def list_flows(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    project_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    flow_type: Optional[str] = Query(None),
    active_only: bool = Query(False),
    include_metrics: bool = Query(False),
    sort_by: str = Query("updated_at", pattern="^(name|created_at|updated_at|last_run_at|status)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of flows accessible to the current user."""
    query = FlowNode.accessible_to_user(db, current_user)
    
    # Apply filters
    if project_id:
        query = query.filter(FlowNode.project_id == project_id)
    if status:
        query = query.filter(FlowNode.status == status.upper())
    if flow_type:
        query = query.filter(FlowNode.flow_type == flow_type)
    
    # Apply sorting
    if sort_by == "name":
        order_attr = FlowNode.name
    elif sort_by == "created_at":
        order_attr = FlowNode.created_at
    elif sort_by == "last_run_at":
        order_attr = FlowNode.last_run_at
    elif sort_by == "status":
        order_attr = FlowNode.status
    else:
        order_attr = FlowNode.updated_at
    
    if sort_order == "desc":
        query = query.order_by(order_attr.desc())
    else:
        query = query.order_by(order_attr.asc())
    
    flows = query.offset(offset).limit(limit).all()
    return flows

@router.post("/", response_model=FlowNodeResponse, status_code=status.HTTP_201_CREATED)
async def create_flow(
    flow_data: FlowNodeCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new flow."""
    # Use org from current user if not specified
    if not flow_data.org_id:
        flow_data.org_id = current_user.org_id
    
    if not current_user.can_manage_org_resource_(flow_data.org_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to create flows in this organization"
        )
    
    flow_node = FlowNode.build_from_input_(
        db=db,
        input_data=flow_data.dict(),
        created_by=current_user,
        org_id=flow_data.org_id
    )
    
    db.add(flow_node)
    db.commit()
    db.refresh(flow_node)
    return flow_node

@router.get("/{flow_id}", response_model=FlowNodeResponse)
async def get_flow(
    flow_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific flow by ID."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this flow"
        )
    
    return flow_node

@router.put("/{flow_id}", response_model=FlowNodeResponse)
async def update_flow(
    flow_id: int,
    flow_data: FlowNodeUpdate,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a flow."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this flow"
        )
    
    # Validate flow is not running unless force is true
    if flow_node.is_running_() and not force:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot update running flow. Use force=true to override."
        )
    
    update_data = {k: v for k, v in flow_data.dict(exclude_unset=True).items() if v is not None}
    flow_node.update_mutable_(
        db=db,
        input_data=update_data,
        updated_by=current_user,
        force=force
    )
    
    db.commit()
    db.refresh(flow_node)
    return flow_node

@router.delete("/{flow_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_flow(
    flow_id: int,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a flow."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'delete'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this flow"
        )
    
    # Check if flow is running
    if flow_node.is_running_() and not force:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete running flow. Use force=true to override."
        )
    
    flow_node.soft_delete_(deleted_by=current_user)
    db.commit()

# Flow control operations
@router.put("/{flow_id}/activate", response_model=FlowNodeResponse)
async def activate_flow(
    flow_id: int,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate a flow."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to activate this flow"
        )
    
    if not flow_node.can_activate_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Flow cannot be activated: {flow_node.activation_blocked_reason_()}"
        )
    
    flow_node.activate_(force=force)
    db.commit()
    db.refresh(flow_node)
    return flow_node

@router.put("/{flow_id}/pause", response_model=FlowNodeResponse)
async def pause_flow(
    flow_id: int,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Pause a flow."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to pause this flow"
        )
    
    flow_node.pause_(force=force)
    db.commit()
    db.refresh(flow_node)
    return flow_node

@router.put("/{flow_id}/stop", response_model=FlowNodeResponse)
async def stop_flow(
    flow_id: int,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Stop a running flow."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to stop this flow"
        )
    
    if not flow_node.is_running_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Flow is not currently running"
        )
    
    flow_node.stop_(force=force)
    db.commit()
    db.refresh(flow_node)
    return flow_node

@router.post("/{flow_id}/run", response_model=Dict[str, Any])
async def run_flow(
    flow_id: int,
    run_request: FlowRunRequest = FlowRunRequest(),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Trigger a flow run."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'execute'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to execute this flow"
        )
    
    if flow_node.is_running_() and not run_request.force:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Flow is already running. Use force=true to override."
        )
    
    run_result = flow_node.run_now_(
        force=run_request.force,
        run_config=run_request.run_config,
        triggered_by=current_user
    )
    
    db.commit()
    
    if run_request.async_execution:
        background_tasks.add_task(flow_node.execute_async_, run_result)
        return {
            "message": "Flow execution started",
            "run_id": run_result.get("run_id"),
            "async": True
        }
    else:
        execution_result = flow_node.execute_sync_(run_result)
        return {
            "message": "Flow execution completed",
            "run_id": run_result.get("run_id"),
            "result": execution_result,
            "async": False
        }

# Flow copying and templating
@router.post("/{flow_id}/copy", response_model=FlowNodeResponse, status_code=status.HTTP_201_CREATED)
async def copy_flow(
    flow_id: int,
    copy_data: FlowNodeCopy,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a copy of an existing flow."""
    source_flow = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not source_flow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source flow not found"
        )
    
    if not source_flow.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to copy this flow"
        )
    
    if not current_user.can_manage_org_resource_(source_flow.org_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to create flows in this organization"
        )
    
    copied_flow = source_flow.create_copy_(
        db=db,
        new_name=copy_data.name,
        new_description=copy_data.description,
        copy_config=copy_data.copy_config,
        copy_schedule=copy_data.copy_schedule,
        copy_notifications=copy_data.copy_notifications,
        created_by=current_user
    )
    
    db.add(copied_flow)
    db.commit()
    db.refresh(copied_flow)
    return copied_flow

# Search and filtering
@router.post("/search", response_model=List[FlowNodeResponse])
async def search_flows(
    search_params: FlowNodeSearch,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("updated_at", pattern="^(name|created_at|updated_at|last_run_at|status)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Search flows with advanced filters."""
    query = FlowNode.accessible_to_user(db, current_user)
    
    # Apply search filters
    if search_params.name:
        query = query.filter(FlowNode.name.ilike(f"%{search_params.name}%"))
    
    if search_params.flow_type:
        query = query.filter(FlowNode.flow_type == search_params.flow_type)
    
    if search_params.status:
        query = query.filter(FlowNode.status == search_params.status.upper())
    
    if search_params.project_id:
        query = query.filter(FlowNode.project_id == search_params.project_id)
    
    if search_params.created_by_id:
        query = query.filter(FlowNode.created_by_id == search_params.created_by_id)
    
    if search_params.created_after:
        query = query.filter(FlowNode.created_at >= search_params.created_after)
    
    if search_params.created_before:
        query = query.filter(FlowNode.created_at <= search_params.created_before)
    
    if search_params.last_run_after:
        query = query.filter(FlowNode.last_run_at >= search_params.last_run_after)
    
    if search_params.last_run_before:
        query = query.filter(FlowNode.last_run_at <= search_params.last_run_before)
    
    if search_params.tags:
        for tag in search_params.tags:
            query = query.filter(FlowNode.tags.contains([tag]))
    
    # Apply sorting
    if sort_by == "name":
        order_attr = FlowNode.name
    elif sort_by == "created_at":
        order_attr = FlowNode.created_at
    elif sort_by == "last_run_at":
        order_attr = FlowNode.last_run_at
    elif sort_by == "status":
        order_attr = FlowNode.status
    else:
        order_attr = FlowNode.updated_at
    
    if sort_order == "desc":
        query = query.order_by(order_attr.desc())
    else:
        query = query.order_by(order_attr.asc())
    
    flows = query.offset(offset).limit(limit).all()
    return flows

# Schedule management
@router.get("/{flow_id}/schedule", response_model=Dict[str, Any])
async def get_flow_schedule(
    flow_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get flow schedule configuration."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this flow"
        )
    
    return {
        "schedule_config": flow_node.schedule_config,
        "next_run_at": flow_node.next_run_at,
        "is_scheduled": flow_node.is_scheduled_(),
        "schedule_enabled": flow_node.schedule_enabled_()
    }

@router.put("/{flow_id}/schedule", response_model=Dict[str, Any])
async def update_flow_schedule(
    flow_id: int,
    schedule_data: FlowScheduleUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update flow schedule configuration."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this flow"
        )
    
    schedule_result = flow_node.update_schedule_(
        enabled=schedule_data.enabled,
        cron_expression=schedule_data.cron_expression,
        timezone=schedule_data.timezone,
        max_retries=schedule_data.max_retries,
        retry_delay=schedule_data.retry_delay,
        updated_by=current_user
    )
    
    db.commit()
    db.refresh(flow_node)
    
    return {
        "message": "Schedule updated successfully",
        "schedule_config": flow_node.schedule_config,
        "next_run_at": flow_node.next_run_at,
        "validation_result": schedule_result
    }

# Flow metrics and monitoring
@router.get("/{flow_id}/metrics", response_model=Dict[str, Any])
async def get_flow_metrics(
    flow_id: int,
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$"),
    include_runs: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get flow execution metrics and statistics."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this flow"
        )
    
    metrics = flow_node.get_metrics_(period=period, include_runs=include_runs)
    return metrics

@router.get("/{flow_id}/status", response_model=Dict[str, Any])
async def get_flow_status(
    flow_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get detailed flow status and health information."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this flow"
        )
    
    return {
        "id": flow_node.id,
        "name": flow_node.name,
        "status": flow_node.status,
        "is_active": flow_node.is_active,
        "is_running": flow_node.is_running_(),
        "is_scheduled": flow_node.is_scheduled_(),
        "is_healthy": flow_node.is_healthy_(),
        "health_score": flow_node.health_score_(),
        "last_run_at": flow_node.last_run_at,
        "next_run_at": flow_node.next_run_at,
        "run_count": flow_node.run_count,
        "error_count": flow_node.error_count,
        "success_rate": flow_node.success_rate,
        "last_error": flow_node.last_error,
        "blocking_issues": flow_node.get_blocking_issues_()
    }

# Flow validation and testing
@router.post("/{flow_id}/validate", response_model=Dict[str, Any])
async def validate_flow(
    flow_id: int,
    validate_config: bool = Query(True),
    validate_connections: bool = Query(True),
    validate_dependencies: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Validate flow configuration and connectivity."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to validate this flow"
        )
    
    validation_result = flow_node.validate_configuration_(
        validate_config=validate_config,
        validate_connections=validate_connections,
        validate_dependencies=validate_dependencies
    )
    
    return validation_result

@router.post("/{flow_id}/test", response_model=Dict[str, Any])
async def test_flow(
    flow_id: int,
    test_config: Optional[Dict[str, Any]] = None,
    dry_run: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Test flow execution with sample data."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'execute'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to test this flow"
        )
    
    test_result = flow_node.test_execution_(
        test_config=test_config,
        dry_run=dry_run,
        tested_by=current_user
    )
    
    return test_result

# Bulk operations
@router.post("/bulk/activate", response_model=Dict[str, Any])
async def bulk_activate_flows(
    flow_ids: List[int],
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate multiple flows in bulk."""
    results = {"success": [], "failed": []}
    
    for flow_id in flow_ids:
        try:
            flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
            if not flow_node:
                results["failed"].append({"id": flow_id, "error": "Flow not found"})
                continue
            
            if not flow_node.accessible_by_(current_user, 'write'):
                results["failed"].append({"id": flow_id, "error": "Not authorized"})
                continue
            
            if not flow_node.can_activate_():
                results["failed"].append({
                    "id": flow_id, 
                    "error": flow_node.activation_blocked_reason_()
                })
                continue
            
            flow_node.activate_(force=force)
            results["success"].append({"id": flow_id, "status": "activated"})
            
        except Exception as e:
            results["failed"].append({"id": flow_id, "error": str(e)})
    
    db.commit()
    return results

@router.post("/bulk/pause", response_model=Dict[str, Any])
async def bulk_pause_flows(
    flow_ids: List[int],
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Pause multiple flows in bulk."""
    results = {"success": [], "failed": []}
    
    for flow_id in flow_ids:
        try:
            flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
            if not flow_node:
                results["failed"].append({"id": flow_id, "error": "Flow not found"})
                continue
            
            if not flow_node.accessible_by_(current_user, 'write'):
                results["failed"].append({"id": flow_id, "error": "Not authorized"})
                continue
            
            flow_node.pause_(force=force)
            results["success"].append({"id": flow_id, "status": "paused"})
            
        except Exception as e:
            results["failed"].append({"id": flow_id, "error": str(e)})
    
    db.commit()
    return results