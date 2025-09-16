"""
Flows API endpoints - Python equivalent of Rails flows controller.
Handles data flow management, execution, scheduling, monitoring, and flow lifecycle operations.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, Field
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import (
    RBACService, SystemPermissions, check_admin_permission
)
from ...models.user import User
from ...models.flow import Flow, FlowRun, FlowTemplate, FlowPermission
from ...models.project import Project
from ...models.org import Org
from ...models.team import Team

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flows", tags=["Flows"])


# Pydantic models for request/response
class FlowCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    flow_type: str = Field(default="data_pipeline", pattern="^(data_pipeline|analytics|etl|ml_pipeline)$")
    project_id: Optional[int] = None
    team_id: Optional[int] = None
    org_id: int
    schedule_type: str = Field(default="manual", pattern="^(manual|cron|event_driven)$")
    schedule_config: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    priority: int = Field(default=5, ge=1, le=10)
    auto_start: bool = False
    retry_count: int = Field(default=3, ge=0, le=10)
    timeout_minutes: int = Field(default=60, ge=1, le=1440)

class FlowUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    flow_type: Optional[str] = Field(None, pattern="^(data_pipeline|analytics|etl|ml_pipeline)$")
    project_id: Optional[int] = None
    team_id: Optional[int] = None
    schedule_type: Optional[str] = Field(None, pattern="^(manual|cron|event_driven)$")
    schedule_config: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    priority: Optional[int] = Field(None, ge=1, le=10)
    auto_start: Optional[bool] = None
    retry_count: Optional[int] = Field(None, ge=0, le=10)
    timeout_minutes: Optional[int] = Field(None, ge=1, le=1440)

class FlowExecuteRequest(BaseModel):
    parameters: Optional[Dict[str, Any]] = None
    trigger_type: str = Field(default="manual", pattern="^(manual|api|webhook|scheduled)$")
    run_async: bool = True

class FlowScheduleRequest(BaseModel):
    schedule_type: str = Field(..., pattern="^(cron|event_driven)$")
    schedule_config: Dict[str, Any]
    is_active: bool = True

class FlowResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    flow_type: str
    status: str
    schedule_type: str
    schedule_config: Optional[Dict[str, Any]]
    version: str
    tags: Optional[List[str]]
    priority: int
    is_active: bool
    is_template: bool
    auto_start: bool
    retry_count: int
    timeout_minutes: int
    node_count: int
    last_run_at: Optional[datetime]
    last_run_status: Optional[str]
    next_run_at: Optional[datetime]
    run_count: int
    success_count: int
    failure_count: int
    owner_id: int
    org_id: int
    project_id: Optional[int]
    team_id: Optional[int]
    created_at: datetime
    updated_at: datetime

class FlowRunResponse(BaseModel):
    id: int
    flow_id: int
    run_number: int
    status: str
    trigger_type: str
    triggered_by: Optional[int]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_display: str
    error_message: Optional[str]
    records_processed: int
    records_success: int
    records_failed: int
    bytes_processed: int
    created_at: datetime

class FlowStats(BaseModel):
    total_flows: int
    active_flows: int
    running_flows: int
    failed_flows: int
    flows_by_type: Dict[str, int]
    flows_by_status: Dict[str, int]
    recent_runs: List[FlowRunResponse]

class FlowTemplateResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    category: Optional[str]
    is_public: bool
    is_verified: bool
    usage_count: int
    rating: int
    created_by: int
    created_at: datetime


def flow_to_response(flow: Flow) -> FlowResponse:
    """Convert Flow model to response"""
    return FlowResponse(
        id=flow.id,
        name=flow.name,
        description=flow.description,
        flow_type=flow.flow_type,
        status=flow.status,
        schedule_type=flow.schedule_type,
        schedule_config=flow.schedule_config,
        version=flow.version,
        tags=flow.tags,
        priority=flow.priority,
        is_active=flow.is_active,
        is_template=flow.is_template,
        auto_start=flow.auto_start,
        retry_count=flow.retry_count,
        timeout_minutes=flow.timeout_minutes,
        node_count=flow.get_node_count(),
        last_run_at=flow.last_run_at,
        last_run_status=flow.last_run_status,
        next_run_at=flow.next_run_at,
        run_count=flow.run_count,
        success_count=flow.success_count,
        failure_count=flow.failure_count,
        owner_id=flow.owner_id,
        org_id=flow.org_id,
        project_id=flow.project_id,
        team_id=flow.team_id,
        created_at=flow.created_at,
        updated_at=flow.updated_at
    )

def flow_run_to_response(flow_run: FlowRun) -> FlowRunResponse:
    """Convert FlowRun model to response"""
    return FlowRunResponse(
        id=flow_run.id,
        flow_id=flow_run.flow_id,
        run_number=flow_run.run_number,
        status=flow_run.status,
        trigger_type=flow_run.trigger_type,
        triggered_by=flow_run.triggered_by,
        started_at=flow_run.started_at,
        completed_at=flow_run.completed_at,
        duration_display=flow_run.get_duration_display(),
        error_message=flow_run.error_message,
        records_processed=flow_run.records_processed,
        records_success=flow_run.records_success,
        records_failed=flow_run.records_failed,
        bytes_processed=flow_run.bytes_processed,
        created_at=flow_run.created_at
    )


# Flow CRUD operations
@router.get("/", response_model=List[FlowResponse], summary="List Flows")
async def list_flows(
    org_id: Optional[int] = Query(None, description="Filter by organization"),
    project_id: Optional[int] = Query(None, description="Filter by project"),
    team_id: Optional[int] = Query(None, description="Filter by team"),
    flow_type: Optional[str] = Query(None, description="Filter by flow type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List flows accessible to the current user.
    
    Equivalent to Rails FlowsController#index
    """
    try:
        query = db.query(Flow)
        
        # Apply filters
        if org_id:
            query = query.filter(Flow.org_id == org_id)
        
        if project_id:
            query = query.filter(Flow.project_id == project_id)
        
        if team_id:
            query = query.filter(Flow.team_id == team_id)
        
        if flow_type:
            query = query.filter(Flow.flow_type == flow_type)
        
        if status:
            query = query.filter(Flow.status == status)
        
        # TODO: Filter by user permissions
        
        flows = query.offset(skip).limit(limit).all()
        
        return [flow_to_response(flow) for flow in flows]
    
    except Exception as e:
        logger.error(f"List flows error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve flows"
        )


@router.get("/stats", response_model=FlowStats, summary="Get Flow Statistics")
async def get_flow_stats(
    org_id: Optional[int] = Query(None, description="Filter by organization"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get flow statistics and analytics.
    
    Equivalent to Rails FlowsController#stats
    """
    try:
        # Base query
        query = db.query(Flow)
        if org_id:
            query = query.filter(Flow.org_id == org_id)
        
        # Calculate statistics
        total_flows = query.count()
        active_flows = query.filter(Flow.is_active == True).count()
        running_flows = query.filter(Flow.last_run_status == "running").count()
        failed_flows = query.filter(Flow.last_run_status == "failed").count()
        
        # Flows by type
        flows_by_type = {}
        for flow_type in ["data_pipeline", "analytics", "etl", "ml_pipeline"]:
            flows_by_type[flow_type] = query.filter(Flow.flow_type == flow_type).count()
        
        # Flows by status
        flows_by_status = {}
        for flow_status in ["draft", "active", "paused", "stopped", "failed"]:
            flows_by_status[flow_status] = query.filter(Flow.status == flow_status).count()
        
        # Recent runs
        recent_runs_query = db.query(FlowRun).order_by(FlowRun.created_at.desc()).limit(10)
        if org_id:
            # TODO: Filter by organization when relationships are enabled
            pass
        
        recent_runs = [flow_run_to_response(run) for run in recent_runs_query.all()]
        
        return FlowStats(
            total_flows=total_flows,
            active_flows=active_flows,
            running_flows=running_flows,
            failed_flows=failed_flows,
            flows_by_type=flows_by_type,
            flows_by_status=flows_by_status,
            recent_runs=recent_runs
        )
    
    except Exception as e:
        logger.error(f"Get flow stats error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve flow statistics"
        )


@router.get("/{flow_id}", response_model=FlowResponse, summary="Get Flow")
async def get_flow(
    flow_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific flow by ID.
    
    Equivalent to Rails FlowsController#show
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        # TODO: Check if user has access to this flow
        
        return flow_to_response(flow)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get flow error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve flow"
        )


@router.post("/", response_model=FlowResponse, summary="Create Flow")
async def create_flow(
    flow_data: FlowCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new flow.
    
    Equivalent to Rails FlowsController#create
    """
    try:
        # Verify organization exists
        org = db.query(Org).filter(Org.id == flow_data.org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Verify project exists if provided
        if flow_data.project_id:
            project = db.query(Project).filter(Project.id == flow_data.project_id).first()
            if not project:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Project not found"
                )
        
        # Verify team exists if provided
        if flow_data.team_id:
            team = db.query(Team).filter(Team.id == flow_data.team_id).first()
            if not team:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Team not found"
                )
        
        # Create new flow
        flow = Flow(
            name=flow_data.name,
            description=flow_data.description,
            flow_type=flow_data.flow_type,
            schedule_type=flow_data.schedule_type,
            schedule_config=flow_data.schedule_config,
            tags=flow_data.tags,
            priority=flow_data.priority,
            auto_start=flow_data.auto_start,
            retry_count=flow_data.retry_count,
            timeout_minutes=flow_data.timeout_minutes,
            owner_id=current_user.id,
            org_id=flow_data.org_id,
            project_id=flow_data.project_id,
            team_id=flow_data.team_id,
            created_at=func.now(),
            updated_at=func.now()
        )
        
        db.add(flow)
        db.commit()
        db.refresh(flow)
        
        logger.info(f"Flow created: {flow.id} by user {current_user.id}")
        return flow_to_response(flow)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create flow error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create flow"
        )


@router.put("/{flow_id}", response_model=FlowResponse, summary="Update Flow")
async def update_flow(
    flow_id: int,
    flow_data: FlowUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update flow information.
    
    Equivalent to Rails FlowsController#update
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        # TODO: Check if user has permission to update this flow
        
        # Update fields
        update_fields = [
            'name', 'description', 'flow_type', 'project_id', 'team_id',
            'schedule_type', 'schedule_config', 'tags', 'priority',
            'auto_start', 'retry_count', 'timeout_minutes'
        ]
        
        for field in update_fields:
            value = getattr(flow_data, field)
            if value is not None:
                setattr(flow, field, value)
        
        flow.updated_at = func.now()
        
        db.commit()
        db.refresh(flow)
        
        logger.info(f"Flow updated: {flow.id} by user {current_user.id}")
        return flow_to_response(flow)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update flow error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update flow"
        )


@router.delete("/{flow_id}", summary="Delete Flow")
async def delete_flow(
    flow_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete a flow.
    
    Equivalent to Rails FlowsController#destroy
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        # TODO: Check if user has permission to delete this flow
        # TODO: Stop flow if running
        # TODO: Handle dependent resources
        
        db.delete(flow)
        db.commit()
        
        logger.info(f"Flow deleted: {flow.id} by user {current_user.id}")
        return {"message": "Flow deleted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete flow error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete flow"
        )


# Flow execution and control
@router.post("/{flow_id}/execute", response_model=FlowRunResponse, summary="Execute Flow")
async def execute_flow(
    flow_id: int,
    execute_request: FlowExecuteRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Execute a flow manually or via API.
    
    Equivalent to Rails FlowsController#execute
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        # TODO: Check if user has permission to execute this flow
        
        if not flow.can_be_started():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Flow cannot be started (inactive or already running)"
            )
        
        # Get next run number
        last_run = db.query(FlowRun).filter(FlowRun.flow_id == flow_id).order_by(FlowRun.run_number.desc()).first()
        run_number = (last_run.run_number + 1) if last_run else 1
        
        # Create flow run
        flow_run = FlowRun(
            flow_id=flow_id,
            run_number=run_number,
            status="queued",
            trigger_type=execute_request.trigger_type,
            triggered_by=current_user.id,
            created_at=func.now(),
            updated_at=func.now()
        )
        
        db.add(flow_run)
        db.commit()
        db.refresh(flow_run)
        
        # Update flow statistics
        flow.run_count = (flow.run_count or 0) + 1
        flow.last_run_at = func.now()
        flow.last_run_status = "running"
        db.commit()
        
        # TODO: Queue flow execution in background task system
        if execute_request.run_async:
            # background_tasks.add_task(execute_flow_async, flow_run.id)
            pass
        
        logger.info(f"Flow execution started: {flow_id} run {run_number} by user {current_user.id}")
        return flow_run_to_response(flow_run)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Execute flow error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to execute flow"
        )


@router.post("/{flow_id}/stop", summary="Stop Flow")
async def stop_flow(
    flow_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Stop a running flow.
    
    Equivalent to Rails FlowsController#stop
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        # TODO: Check if user has permission to stop this flow
        
        if not flow.is_running():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Flow is not currently running"
            )
        
        # TODO: Implement actual flow stopping logic
        
        # Update flow status
        flow.last_run_status = "cancelled"
        flow.updated_at = func.now()
        
        # Update current flow run
        current_run = db.query(FlowRun).filter(
            FlowRun.flow_id == flow_id,
            FlowRun.status == "running"
        ).first()
        
        if current_run:
            current_run.status = "cancelled"
            current_run.completed_at = func.now()
            current_run.updated_at = func.now()
        
        db.commit()
        
        logger.info(f"Flow stopped: {flow_id} by user {current_user.id}")
        return {"message": "Flow stopped successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stop flow error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to stop flow"
        )


@router.get("/{flow_id}/runs", response_model=List[FlowRunResponse], summary="List Flow Runs")
async def list_flow_runs(
    flow_id: int,
    status_filter: Optional[str] = Query(None, description="Filter by run status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List runs for a specific flow.
    
    Equivalent to Rails FlowsController#runs
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        query = db.query(FlowRun).filter(FlowRun.flow_id == flow_id)
        
        if status_filter:
            query = query.filter(FlowRun.status == status_filter)
        
        runs = query.order_by(FlowRun.created_at.desc()).offset(skip).limit(limit).all()
        
        return [flow_run_to_response(run) for run in runs]
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List flow runs error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve flow runs"
        )


@router.post("/{flow_id}/schedule", summary="Schedule Flow")
async def schedule_flow(
    flow_id: int,
    schedule_request: FlowScheduleRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Schedule a flow for automatic execution.
    
    Equivalent to Rails FlowsController#schedule
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        # TODO: Check if user has permission to schedule this flow
        
        # Update flow schedule
        flow.schedule_type = schedule_request.schedule_type
        flow.schedule_config = schedule_request.schedule_config
        flow.is_active = schedule_request.is_active
        flow.updated_at = func.now()
        
        # TODO: Register with scheduler service
        
        db.commit()
        
        logger.info(f"Flow scheduled: {flow_id} by user {current_user.id}")
        return {"message": "Flow scheduled successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Schedule flow error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to schedule flow"
        )


@router.delete("/{flow_id}/schedule", summary="Unschedule Flow")
async def unschedule_flow(
    flow_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Remove scheduling from a flow.
    
    Equivalent to Rails FlowsController#unschedule
    """
    try:
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow not found"
            )
        
        # TODO: Check if user has permission to unschedule this flow
        
        # Remove schedule
        flow.schedule_type = "manual"
        flow.schedule_config = None
        flow.next_run_at = None
        flow.updated_at = func.now()
        
        # TODO: Unregister from scheduler service
        
        db.commit()
        
        logger.info(f"Flow unscheduled: {flow_id} by user {current_user.id}")
        return {"message": "Flow unscheduled successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unschedule flow error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to unschedule flow"
        )