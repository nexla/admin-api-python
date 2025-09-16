"""
Background Jobs Router - API endpoints for background job management
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.background_job import BackgroundJob, JobStatus, JobPriority, JobType, JobDependency
from app.services.async_tasks.manager import AsyncTaskManager, TaskRegistry

router = APIRouter()

# Pydantic models
class JobCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    job_type: JobType
    job_class: str = Field(..., min_length=1, max_length=255)
    job_method: str = Field(default="perform", max_length=100)
    job_args: List[Any] = Field(default_factory=list)
    job_kwargs: Dict[str, Any] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.NORMAL
    queue_name: str = Field(default="default", max_length=100)
    scheduled_at: Optional[datetime] = None
    timeout_seconds: int = Field(default=3600, gt=0)
    max_retries: int = Field(default=3, ge=0)
    retry_delay_seconds: int = Field(default=60, gt=0)
    cron_expression: Optional[str] = None
    recurring: bool = False
    tags: List[str] = Field(default_factory=list)
    extra_metadata: Dict[str, Any] = Field(default_factory=dict)
    context: Dict[str, Any] = Field(default_factory=dict)

class JobUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    priority: Optional[JobPriority] = None
    scheduled_at: Optional[datetime] = None
    timeout_seconds: Optional[int] = Field(None, gt=0)
    max_retries: Optional[int] = Field(None, ge=0)
    retry_delay_seconds: Optional[int] = Field(None, gt=0)
    cron_expression: Optional[str] = None
    recurring: Optional[bool] = None
    active: Optional[bool] = None
    tags: Optional[List[str]] = None
    extra_metadata: Optional[Dict[str, Any]] = None

class JobExecuteRequest(BaseModel):
    job_args: Optional[List[Any]] = None
    job_kwargs: Optional[Dict[str, Any]] = None
    timeout_seconds: Optional[int] = Field(None, gt=0)

class JobProgressUpdate(BaseModel):
    progress_percentage: float = Field(..., ge=0, le=100)
    progress_message: Optional[str] = Field(None, max_length=500)
    progress_data: Optional[Dict[str, Any]] = None

class JobResponse(BaseModel):
    id: int
    job_id: str
    name: str
    description: Optional[str]
    job_type: str
    status: str
    priority: str
    queue_name: str
    progress_percentage: float
    progress_message: Optional[str]
    retry_count: int
    max_retries: int
    recurring: bool
    active: bool
    tags: List[str]
    scheduled_at: str
    created_at: str
    updated_at: str

class JobDetailResponse(JobResponse):
    job_class: str
    job_method: str
    job_args: List[Any]
    job_kwargs: Dict[str, Any]
    result: Optional[Dict[str, Any]]
    error_message: Optional[str]
    progress_data: Optional[Dict[str, Any]]
    timeout_seconds: int
    retry_delay_seconds: int
    started_at: Optional[str]
    completed_at: Optional[str]
    failed_at: Optional[str]
    duration_seconds: Optional[float]
    cpu_usage_percent: Optional[float]
    memory_usage_mb: Optional[float]
    cron_expression: Optional[str]
    next_run_at: Optional[str]
    last_run_at: Optional[str]
    extra_metadata: Dict[str, Any]
    context: Dict[str, Any]
    org_id: Optional[int]
    user_id: Optional[int]
    project_id: Optional[int]
    worker_id: Optional[str]

class JobStatsResponse(BaseModel):
    total_jobs: int
    pending_jobs: int
    running_jobs: int
    completed_jobs: int
    failed_jobs: int
    retrying_jobs: int
    active_jobs: int
    avg_duration_seconds: Optional[float]
    success_rate: float

class JobHealthResponse(BaseModel):
    job_id: str
    healthy: bool
    active: bool
    status: str
    overdue: bool
    timed_out: bool
    stuck: bool
    can_retry: bool
    needs_attention: bool
    resource_usage: Dict[str, Any]
    execution_summary: Dict[str, Any]

# Job management endpoints
@router.post("/", response_model=JobDetailResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    job_data: JobCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new background job"""
    try:
        # Create the job
        job = BackgroundJob(
            name=job_data.name,
            description=job_data.description,
            job_type=job_data.job_type,
            job_class=job_data.job_class,
            job_method=job_data.job_method,
            job_args=job_data.job_args,
            job_kwargs=job_data.job_kwargs,
            priority=job_data.priority,
            queue_name=job_data.queue_name,
            scheduled_at=job_data.scheduled_at or datetime.now(),
            timeout_seconds=job_data.timeout_seconds,
            max_retries=job_data.max_retries,
            retry_delay_seconds=job_data.retry_delay_seconds,
            cron_expression=job_data.cron_expression,
            recurring=job_data.recurring,
            tags=job_data.tags,
            extra_metadata=job_data.extra_metadata,
            context=job_data.context,
            created_by=current_user.id,
            user_id=current_user.id,
            org_id=current_user.default_org_id
        )
        
        db.add(job)
        db.commit()
        db.refresh(job)
        
        return JobDetailResponse(**job.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create job: {str(e)}"
        )

@router.get("/", response_model=List[JobResponse])
async def list_jobs(
    status_filter: Optional[JobStatus] = Query(None, alias="status"),
    job_type_filter: Optional[JobType] = Query(None, alias="job_type"),
    priority_filter: Optional[JobPriority] = Query(None, alias="priority"),
    queue_name: Optional[str] = Query(None),
    active_only: bool = Query(False),
    recurring_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List background jobs with filtering options"""
    try:
        query = db.query(BackgroundJob)
        
        # Apply filters
        if status_filter:
            query = query.filter(BackgroundJob.status == status_filter)
        if job_type_filter:
            query = query.filter(BackgroundJob.job_type == job_type_filter)
        if priority_filter:
            query = query.filter(BackgroundJob.priority == priority_filter)
        if queue_name:
            query = query.filter(BackgroundJob.queue_name == queue_name)
        if active_only:
            query = query.filter(BackgroundJob.active == True)
        if recurring_only:
            query = query.filter(BackgroundJob.recurring == True)
        
        # Filter by user's organization
        query = query.filter(BackgroundJob.org_id == current_user.default_org_id)
        
        # Order by creation time (newest first)
        query = query.order_by(BackgroundJob.created_at.desc())
        
        # Apply pagination
        jobs = query.offset(offset).limit(limit).all()
        
        return [JobResponse(**job.to_dict()) for job in jobs]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list jobs: {str(e)}"
        )

@router.get("/{job_id}", response_model=JobDetailResponse)
async def get_job(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get detailed information about a specific job"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    return JobDetailResponse(**job.to_dict(include_sensitive=True))

@router.put("/{job_id}", response_model=JobDetailResponse)
async def update_job(
    job_id: int,
    job_data: JobUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update a background job"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    # Only allow updates to non-running jobs
    if job.status == JobStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot update running job"
        )
    
    try:
        # Update fields
        update_data = job_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(job, field, value)
        
        job.updated_by = current_user.id
        job.updated_at = datetime.now()
        
        db.commit()
        db.refresh(job)
        
        return JobDetailResponse(**job.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update job: {str(e)}"
        )

@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_job(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete a background job"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    # Only allow deletion of non-running jobs
    if job.status == JobStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete running job. Cancel it first."
        )
    
    try:
        db.delete(job)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete job: {str(e)}"
        )

# Job execution endpoints
@router.post("/{job_id}/execute", response_model=JobDetailResponse)
async def execute_job(
    job_id: int,
    execute_data: Optional[JobExecuteRequest] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Execute a job immediately"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job.status == JobStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job is already running"
        )
    
    try:
        # Update job with execution parameters if provided
        if execute_data:
            if execute_data.job_args is not None:
                job.job_args = execute_data.job_args
            if execute_data.job_kwargs is not None:
                job.job_kwargs = execute_data.job_kwargs
            if execute_data.timeout_seconds is not None:
                job.timeout_seconds = execute_data.timeout_seconds
        
        # Reset job state for execution
        job.reset_()
        job.scheduled_at = datetime.now()
        
        db.commit()
        
        # TODO: Queue job for execution by background worker
        # For now, we'll just mark it as pending
        
        db.refresh(job)
        return JobDetailResponse(**job.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute job: {str(e)}"
        )

@router.post("/{job_id}/cancel", response_model=JobDetailResponse)
async def cancel_job(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Cancel a running or pending job"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job.status not in [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.RETRYING]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status: {job.status.value}"
        )
    
    try:
        job.cancel_(f"Cancelled by user {current_user.id}")
        db.commit()
        db.refresh(job)
        
        return JobDetailResponse(**job.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel job: {str(e)}"
        )

@router.post("/{job_id}/retry", response_model=JobDetailResponse)
async def retry_job(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Retry a failed job"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if not job.can_retry_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job cannot be retried"
        )
    
    try:
        job.retry_()
        db.commit()
        db.refresh(job)
        
        return JobDetailResponse(**job.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retry job: {str(e)}"
        )

@router.post("/{job_id}/reset", response_model=JobDetailResponse)
async def reset_job(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Reset a job to pending state"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job.status == JobStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot reset running job. Cancel it first."
        )
    
    try:
        job.reset_()
        db.commit()
        db.refresh(job)
        
        return JobDetailResponse(**job.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reset job: {str(e)}"
        )

@router.patch("/{job_id}/progress", response_model=JobDetailResponse)
async def update_job_progress(
    job_id: int,
    progress_data: JobProgressUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update job progress (typically called by the job worker)"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    try:
        job.update_progress_(
            progress_data.progress_percentage,
            progress_data.progress_message,
            progress_data.progress_data
        )
        db.commit()
        db.refresh(job)
        
        return JobDetailResponse(**job.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update job progress: {str(e)}"
        )

# Monitoring and analytics endpoints
@router.get("/stats/summary", response_model=JobStatsResponse)
async def get_job_stats(
    hours: int = Query(24, ge=1, le=168),  # Last 24 hours by default, max 1 week
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get job statistics summary"""
    try:
        since = datetime.now() - timedelta(hours=hours)
        
        # Base query for user's organization
        base_query = db.query(BackgroundJob).filter(
            BackgroundJob.org_id == current_user.default_org_id,
            BackgroundJob.created_at >= since
        )
        
        stats = {
            "total_jobs": base_query.count(),
            "pending_jobs": base_query.filter(BackgroundJob.status == JobStatus.PENDING).count(),
            "running_jobs": base_query.filter(BackgroundJob.status == JobStatus.RUNNING).count(),
            "completed_jobs": base_query.filter(BackgroundJob.status == JobStatus.COMPLETED).count(),
            "failed_jobs": base_query.filter(BackgroundJob.status == JobStatus.FAILED).count(),
            "retrying_jobs": base_query.filter(BackgroundJob.status == JobStatus.RETRYING).count(),
            "active_jobs": base_query.filter(BackgroundJob.active == True).count(),
        }
        
        # Calculate average duration for completed jobs
        completed_jobs = base_query.filter(
            BackgroundJob.status == JobStatus.COMPLETED,
            BackgroundJob.duration_seconds.isnot(None)
        ).all()
        
        if completed_jobs:
            avg_duration = sum(job.duration_seconds for job in completed_jobs) / len(completed_jobs)
            stats["avg_duration_seconds"] = avg_duration
        else:
            stats["avg_duration_seconds"] = None
        
        # Calculate success rate
        total_finished = stats["completed_jobs"] + stats["failed_jobs"]
        if total_finished > 0:
            stats["success_rate"] = stats["completed_jobs"] / total_finished
        else:
            stats["success_rate"] = 0.0
        
        return JobStatsResponse(**stats)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get job stats: {str(e)}"
        )

@router.get("/{job_id}/health", response_model=JobHealthResponse)
async def get_job_health(
    job_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get job health status"""
    job = db.query(BackgroundJob).filter(
        BackgroundJob.id == job_id,
        BackgroundJob.org_id == current_user.default_org_id
    ).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    return JobHealthResponse(**job.health_report())

@router.get("/queue/{queue_name}/stats", response_model=JobStatsResponse)
async def get_queue_stats(
    queue_name: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get statistics for a specific queue"""
    try:
        # Base query for specific queue in user's organization
        base_query = db.query(BackgroundJob).filter(
            BackgroundJob.org_id == current_user.default_org_id,
            BackgroundJob.queue_name == queue_name
        )
        
        stats = {
            "total_jobs": base_query.count(),
            "pending_jobs": base_query.filter(BackgroundJob.status == JobStatus.PENDING).count(),
            "running_jobs": base_query.filter(BackgroundJob.status == JobStatus.RUNNING).count(),
            "completed_jobs": base_query.filter(BackgroundJob.status == JobStatus.COMPLETED).count(),
            "failed_jobs": base_query.filter(BackgroundJob.status == JobStatus.FAILED).count(),
            "retrying_jobs": base_query.filter(BackgroundJob.status == JobStatus.RETRYING).count(),
            "active_jobs": base_query.filter(BackgroundJob.active == True).count(),
        }
        
        # Calculate average duration and success rate
        completed_jobs = base_query.filter(
            BackgroundJob.status == JobStatus.COMPLETED,
            BackgroundJob.duration_seconds.isnot(None)
        ).all()
        
        if completed_jobs:
            avg_duration = sum(job.duration_seconds for job in completed_jobs) / len(completed_jobs)
            stats["avg_duration_seconds"] = avg_duration
        else:
            stats["avg_duration_seconds"] = None
        
        total_finished = stats["completed_jobs"] + stats["failed_jobs"]
        if total_finished > 0:
            stats["success_rate"] = stats["completed_jobs"] / total_finished
        else:
            stats["success_rate"] = 0.0
        
        return JobStatsResponse(**stats)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get queue stats: {str(e)}"
        )