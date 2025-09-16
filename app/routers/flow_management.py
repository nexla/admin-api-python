from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.flow_node import FlowNode
from app.models.data_source import DataSource
from app.models.data_sink import DataSink

router = APIRouter()

class FlowRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class FlowTriggerType(str, Enum):
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT_DRIVEN = "event_driven"
    API = "api"

class FlowRunSummary(BaseModel):
    run_id: str
    flow_id: int
    flow_name: str
    status: FlowRunStatus
    trigger_type: FlowTriggerType
    started_at: datetime
    completed_at: Optional[datetime]
    duration_seconds: Optional[int]
    records_processed: Optional[int]
    records_failed: Optional[int]
    error_message: Optional[str]
    triggered_by_id: Optional[int]
    triggered_by_name: Optional[str]

class FlowRunDetail(FlowRunSummary):
    flow_config: Dict[str, Any]
    run_config: Optional[Dict[str, Any]]
    execution_log: List[Dict[str, Any]]
    performance_metrics: Dict[str, Any]
    resource_usage: Dict[str, Any]

class FlowDependency(BaseModel):
    upstream_flow_id: int
    upstream_flow_name: str
    dependency_type: str
    condition: Optional[str]
    created_at: datetime

class FlowExecutionPlan(BaseModel):
    flow_id: int
    execution_order: int
    estimated_duration_seconds: int
    dependencies: List[FlowDependency]
    resource_requirements: Dict[str, Any]

class BatchExecutionRequest(BaseModel):
    flow_ids: List[int]
    execution_mode: str = Field("parallel", regex="^(parallel|sequential|dependency_aware)$")
    max_concurrent: int = Field(5, ge=1, le=20)
    timeout_seconds: int = Field(3600, ge=60, le=86400)
    run_config: Optional[Dict[str, Any]] = None
    notify_on_completion: bool = Field(True)

class FlowSchedulingRequest(BaseModel):
    flow_id: int
    schedule_expression: str = Field(..., description="Cron expression")
    timezone: str = Field("UTC")
    enabled: bool = Field(True)
    max_concurrent_runs: int = Field(1, ge=1, le=10)
    retry_policy: Dict[str, Any] = Field(default_factory=dict)
    notification_config: Optional[Dict[str, Any]] = None

# Flow execution endpoints
@router.get("/{flow_id}/runs", response_model=List[FlowRunSummary])
async def get_flow_runs(
    flow_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    status: Optional[FlowRunStatus] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get execution history for a specific flow."""
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
    
    runs = flow_node.get_execution_history_(
        limit=limit,
        offset=offset,
        status=status,
        start_date=start_date,
        end_date=end_date
    )
    
    return runs

@router.get("/{flow_id}/runs/{run_id}", response_model=FlowRunDetail)
async def get_flow_run_details(
    flow_id: int,
    run_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get detailed information about a specific flow run."""
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
    
    run_details = flow_node.get_run_details_(run_id)
    if not run_details:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow run not found"
        )
    
    return run_details

@router.post("/{flow_id}/runs/{run_id}/cancel", response_model=Dict[str, Any])
async def cancel_flow_run(
    flow_id: int,
    run_id: str,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Cancel a running flow execution."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'execute'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to cancel executions for this flow"
        )
    
    cancel_result = flow_node.cancel_run_(run_id, force=force, cancelled_by=current_user)
    
    if not cancel_result.get("success"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=cancel_result.get("error", "Failed to cancel flow run")
        )
    
    return cancel_result

@router.post("/{flow_id}/runs/{run_id}/retry", response_model=Dict[str, Any])
async def retry_flow_run(
    flow_id: int,
    run_id: str,
    retry_config: Optional[Dict[str, Any]] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Retry a failed flow execution."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'execute'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to retry executions for this flow"
        )
    
    retry_result = flow_node.retry_run_(
        run_id, 
        retry_config=retry_config, 
        retried_by=current_user
    )
    
    if not retry_result.get("success"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=retry_result.get("error", "Failed to retry flow run")
        )
    
    return retry_result

# Flow dependency management
@router.get("/{flow_id}/dependencies", response_model=List[FlowDependency])
async def get_flow_dependencies(
    flow_id: int,
    include_downstream: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get flow dependencies (upstream and optionally downstream)."""
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
    
    dependencies = flow_node.get_dependencies_(include_downstream=include_downstream)
    return dependencies

@router.post("/{flow_id}/dependencies", response_model=Dict[str, Any])
async def create_flow_dependency(
    flow_id: int,
    upstream_flow_id: int,
    dependency_type: str = Query("success", regex="^(success|completion|failure|conditional)$"),
    condition: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a dependency between flows."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    upstream_flow = db.query(FlowNode).filter(FlowNode.id == upstream_flow_id).first()
    
    if not flow_node or not upstream_flow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="One or both flows not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write') or not upstream_flow.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to create dependency"
        )
    
    # Check for circular dependencies
    if flow_node.would_create_circular_dependency_(upstream_flow_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Creating this dependency would result in a circular dependency"
        )
    
    dependency_result = flow_node.add_dependency_(
        upstream_flow_id=upstream_flow_id,
        dependency_type=dependency_type,
        condition=condition,
        created_by=current_user
    )
    
    db.commit()
    return dependency_result

@router.delete("/{flow_id}/dependencies/{upstream_flow_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_flow_dependency(
    flow_id: int,
    upstream_flow_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Remove a dependency between flows."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to modify this flow"
        )
    
    dependency_removed = flow_node.remove_dependency_(upstream_flow_id)
    if not dependency_removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dependency not found"
        )
    
    db.commit()

# Batch operations
@router.post("/batch/execute", response_model=Dict[str, Any])
async def execute_flows_batch(
    batch_request: BatchExecutionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Execute multiple flows in batch with different modes."""
    # Validate all flows exist and are accessible
    flows = []
    for flow_id in batch_request.flow_ids:
        flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
        if not flow_node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Flow {flow_id} not found"
            )
        
        if not flow_node.accessible_by_(current_user, 'execute'):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not authorized to execute flow {flow_id}"
            )
        
        flows.append(flow_node)
    
    # Create batch execution plan
    if batch_request.execution_mode == "dependency_aware":
        execution_plan = _create_dependency_aware_execution_plan(flows)
    else:
        execution_plan = [FlowExecutionPlan(
            flow_id=flow.id,
            execution_order=i,
            estimated_duration_seconds=flow.estimated_duration_seconds_() or 300,
            dependencies=[],
            resource_requirements=flow.get_resource_requirements_()
        ) for i, flow in enumerate(flows)]
    
    # Start batch execution
    batch_id = f"batch_{datetime.utcnow().timestamp()}"
    
    if batch_request.execution_mode == "parallel":
        background_tasks.add_task(
            _execute_flows_parallel,
            flows, batch_request, execution_plan, batch_id, current_user
        )
    elif batch_request.execution_mode == "sequential":
        background_tasks.add_task(
            _execute_flows_sequential,
            flows, batch_request, execution_plan, batch_id, current_user
        )
    else:  # dependency_aware
        background_tasks.add_task(
            _execute_flows_dependency_aware,
            flows, batch_request, execution_plan, batch_id, current_user
        )
    
    return {
        "batch_id": batch_id,
        "execution_mode": batch_request.execution_mode,
        "flow_count": len(flows),
        "execution_plan": execution_plan,
        "estimated_duration_seconds": max(plan.estimated_duration_seconds for plan in execution_plan),
        "message": "Batch execution started"
    }

@router.get("/batch/{batch_id}/status", response_model=Dict[str, Any])
async def get_batch_execution_status(
    batch_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get status of a batch execution."""
    # In a real implementation, this would query a batch execution tracking table
    # For now, we'll return a placeholder response
    return {
        "batch_id": batch_id,
        "status": "running",
        "started_at": datetime.utcnow() - timedelta(minutes=5),
        "completed_flows": 2,
        "total_flows": 5,
        "failed_flows": 0,
        "progress_percentage": 40.0,
        "estimated_completion": datetime.utcnow() + timedelta(minutes=10)
    }

# Flow scheduling
@router.post("/schedule", response_model=Dict[str, Any])
async def schedule_flow(
    scheduling_request: FlowSchedulingRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Schedule a flow for automatic execution."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == scheduling_request.flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to schedule this flow"
        )
    
    scheduling_result = flow_node.create_schedule_(
        schedule_expression=scheduling_request.schedule_expression,
        timezone=scheduling_request.timezone,
        enabled=scheduling_request.enabled,
        max_concurrent_runs=scheduling_request.max_concurrent_runs,
        retry_policy=scheduling_request.retry_policy,
        notification_config=scheduling_request.notification_config,
        created_by=current_user
    )
    
    db.commit()
    return scheduling_result

@router.get("/schedules", response_model=List[Dict[str, Any]])
async def get_scheduled_flows(
    active_only: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of scheduled flows."""
    # Get flows accessible to user that have schedules
    scheduled_flows = FlowNode.get_scheduled_flows_(
        user=current_user,
        active_only=active_only,
        limit=limit,
        offset=offset
    )
    
    return scheduled_flows

@router.delete("/schedule/{flow_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unschedule_flow(
    flow_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Remove scheduling from a flow."""
    flow_node = db.query(FlowNode).filter(FlowNode.id == flow_id).first()
    if not flow_node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flow not found"
        )
    
    if not flow_node.accessible_by_(current_user, 'write'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to modify this flow"
        )
    
    flow_node.remove_schedule_()
    db.commit()

# Data lineage and flow impact analysis
@router.get("/{flow_id}/lineage", response_model=Dict[str, Any])
async def get_flow_lineage(
    flow_id: int,
    direction: str = Query("both", regex="^(upstream|downstream|both)$"),
    max_depth: int = Query(5, ge=1, le=10),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get data lineage for a flow showing data sources and sinks."""
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
    
    lineage = flow_node.get_data_lineage_(
        direction=direction,
        max_depth=max_depth
    )
    
    return lineage

@router.post("/{flow_id}/impact-analysis", response_model=Dict[str, Any])
async def analyze_flow_impact(
    flow_id: int,
    change_type: str = Query("config", regex="^(config|source|sink|transform|schedule)$"),
    proposed_changes: Optional[Dict[str, Any]] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Analyze the impact of proposed changes to a flow."""
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
    
    impact_analysis = flow_node.analyze_change_impact_(
        change_type=change_type,
        proposed_changes=proposed_changes
    )
    
    return impact_analysis

# Helper functions for batch execution
async def _execute_flows_parallel(flows, batch_request, execution_plan, batch_id, user):
    """Execute flows in parallel."""
    # Implementation would use asyncio.gather or similar for parallel execution
    pass

async def _execute_flows_sequential(flows, batch_request, execution_plan, batch_id, user):
    """Execute flows sequentially."""
    # Implementation would execute flows one by one
    pass

async def _execute_flows_dependency_aware(flows, batch_request, execution_plan, batch_id, user):
    """Execute flows respecting dependencies."""
    # Implementation would use topological sort and execute based on dependency order
    pass

def _create_dependency_aware_execution_plan(flows: List[FlowNode]) -> List[FlowExecutionPlan]:
    """Create execution plan that respects flow dependencies."""
    # Implementation would analyze dependencies and create proper execution order
    # For now, return basic plan
    return [
        FlowExecutionPlan(
            flow_id=flow.id,
            execution_order=i,
            estimated_duration_seconds=flow.estimated_duration_seconds_() or 300,
            dependencies=flow.get_dependencies_(),
            resource_requirements=flow.get_resource_requirements_()
        ) for i, flow in enumerate(flows)
    ]