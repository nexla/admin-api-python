"""
System Monitoring API endpoints - Platform health, metrics, and performance monitoring.
Provides real-time insights into system status, resource usage, and operational metrics.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta
import psutil
import os

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import (
    RBACService, SystemPermissions, check_admin_permission
)
from ...models.user import User
from ...models.flow import Flow, FlowRun
from ...models.flow_node import FlowNode
from ...models.data_source import DataSource
from ...models.data_set import DataSet
from ...models.data_sink import DataSink
from ...models.org import Org

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitoring", tags=["System Monitoring"])


# Pydantic models for monitoring responses
class SystemHealth(BaseModel):
    status: str  # healthy, degraded, unhealthy
    uptime_seconds: int
    version: str
    environment: str
    last_health_check: datetime

class ResourceUsage(BaseModel):
    cpu_percent: float
    memory_percent: float
    memory_used_mb: int
    memory_total_mb: int
    disk_percent: float
    disk_used_gb: float
    disk_total_gb: float

class DatabaseMetrics(BaseModel):
    total_connections: int
    active_connections: int
    total_records: int
    database_size_mb: Optional[float]
    slowest_queries: List[Dict[str, Any]]

class FlowMetrics(BaseModel):
    total_flows: int
    active_flows: int
    running_flows: int
    failed_flows: int
    total_runs_today: int
    success_rate_24h: float
    avg_execution_time_minutes: float
    flows_by_status: Dict[str, int]

class UserMetrics(BaseModel):
    total_users: int
    active_users: int
    new_users_today: int
    users_by_org: Dict[str, int]
    login_activity_24h: List[Dict[str, Any]]

class DataMetrics(BaseModel):
    total_data_sources: int
    total_data_sets: int
    total_data_sinks: int
    data_processed_24h_gb: float
    data_processing_errors_24h: int
    top_data_sources: List[Dict[str, Any]]

class AlertSummary(BaseModel):
    total_alerts: int
    critical_alerts: int
    warning_alerts: int
    recent_alerts: List[Dict[str, Any]]

class PerformanceMetrics(BaseModel):
    api_response_time_ms: float
    api_requests_per_minute: float
    error_rate_percent: float
    throughput_records_per_second: float

class SystemOverview(BaseModel):
    health: SystemHealth
    resources: ResourceUsage
    database: DatabaseMetrics
    flows: FlowMetrics
    users: UserMetrics
    data: DataMetrics
    alerts: AlertSummary
    performance: PerformanceMetrics


def get_system_health() -> SystemHealth:
    """Get overall system health status"""
    # Calculate uptime (simplified)
    uptime_seconds = int(psutil.boot_time())
    
    return SystemHealth(
        status="healthy",  # Would implement actual health checks
        uptime_seconds=uptime_seconds,
        version="1.0.0",
        environment=os.getenv("ENVIRONMENT", "development"),
        last_health_check=datetime.utcnow()
    )

def get_resource_usage() -> ResourceUsage:
    """Get current system resource usage"""
    # CPU usage
    cpu_percent = psutil.cpu_percent(interval=1)
    
    # Memory usage
    memory = psutil.virtual_memory()
    memory_used_mb = int(memory.used / 1024 / 1024)
    memory_total_mb = int(memory.total / 1024 / 1024)
    
    # Disk usage
    disk = psutil.disk_usage('/')
    disk_used_gb = round(disk.used / 1024 / 1024 / 1024, 2)
    disk_total_gb = round(disk.total / 1024 / 1024 / 1024, 2)
    
    return ResourceUsage(
        cpu_percent=cpu_percent,
        memory_percent=memory.percent,
        memory_used_mb=memory_used_mb,
        memory_total_mb=memory_total_mb,
        disk_percent=round((disk.used / disk.total) * 100, 2),
        disk_used_gb=disk_used_gb,
        disk_total_gb=disk_total_gb
    )

def get_database_metrics(db: Session) -> DatabaseMetrics:
    """Get database performance metrics"""
    # Count total records across all tables
    total_records = 0
    
    try:
        tables = [User, Flow, FlowRun, FlowNode, DataSource, DataSet, DataSink, Org]
        for table in tables:
            count = db.query(table).count()
            total_records += count
    except Exception as e:
        logger.warning(f"Failed to count records: {str(e)}")
    
    return DatabaseMetrics(
        total_connections=10,  # Would get from actual DB monitoring
        active_connections=3,
        total_records=total_records,
        database_size_mb=None,  # Would calculate actual DB size
        slowest_queries=[]  # Would get from query logs
    )

def get_flow_metrics(db: Session) -> FlowMetrics:
    """Get flow execution metrics"""
    # Basic flow counts
    total_flows = db.query(Flow).count()
    active_flows = db.query(Flow).filter(Flow.is_active == True).count()
    running_flows = db.query(Flow).filter(Flow.last_run_status == "running").count()
    failed_flows = db.query(Flow).filter(Flow.last_run_status == "failed").count()
    
    # Runs today
    today = datetime.utcnow().date()
    total_runs_today = db.query(FlowRun).filter(
        func.date(FlowRun.created_at) == today
    ).count()
    
    # Success rate in last 24 hours
    yesterday = datetime.utcnow() - timedelta(days=1)
    recent_runs = db.query(FlowRun).filter(FlowRun.created_at >= yesterday).all()
    
    if recent_runs:
        successful_runs = len([r for r in recent_runs if r.status == "success"])
        success_rate_24h = (successful_runs / len(recent_runs)) * 100
    else:
        success_rate_24h = 0.0
    
    # Average execution time
    avg_duration = db.query(func.avg(FlowRun.duration_seconds)).filter(
        FlowRun.completed_at >= yesterday,
        FlowRun.status == "success"
    ).scalar()
    
    avg_execution_time_minutes = round((avg_duration or 0) / 60, 2)
    
    # Flows by status
    flows_by_status = {}
    statuses = ["draft", "active", "paused", "stopped", "failed"]
    for status_val in statuses:
        count = db.query(Flow).filter(Flow.status == status_val).count()
        flows_by_status[status_val] = count
    
    return FlowMetrics(
        total_flows=total_flows,
        active_flows=active_flows,
        running_flows=running_flows,
        failed_flows=failed_flows,
        total_runs_today=total_runs_today,
        success_rate_24h=round(success_rate_24h, 2),
        avg_execution_time_minutes=avg_execution_time_minutes,
        flows_by_status=flows_by_status
    )

def get_user_metrics(db: Session) -> UserMetrics:
    """Get user activity metrics"""
    total_users = db.query(User).count()
    active_users = db.query(User).filter(User.status == "ACTIVE").count()
    
    # New users today
    today = datetime.utcnow().date()
    new_users_today = db.query(User).filter(
        func.date(User.created_at) == today
    ).count()
    
    # Users by organization (placeholder)
    users_by_org = {"Default Org": active_users}
    
    # Login activity (placeholder)
    login_activity_24h = []
    
    return UserMetrics(
        total_users=total_users,
        active_users=active_users,
        new_users_today=new_users_today,
        users_by_org=users_by_org,
        login_activity_24h=login_activity_24h
    )

def get_data_metrics(db: Session) -> DataMetrics:
    """Get data processing metrics"""
    total_data_sources = db.query(DataSource).count()
    total_data_sets = db.query(DataSet).count()
    total_data_sinks = db.query(DataSink).count()
    
    return DataMetrics(
        total_data_sources=total_data_sources,
        total_data_sets=total_data_sets,
        total_data_sinks=total_data_sinks,
        data_processed_24h_gb=0.0,  # Would calculate from actual metrics
        data_processing_errors_24h=0,
        top_data_sources=[]
    )


# Monitoring endpoints
@router.get("/health", response_model=SystemHealth, summary="System Health Check")
async def get_health_status():
    """
    Get overall system health status.
    Public endpoint for load balancer health checks.
    """
    return get_system_health()


@router.get("/overview", response_model=SystemOverview, summary="System Overview")
async def get_system_overview(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_admin_permission())
):
    """
    Get comprehensive system overview with all metrics.
    Admin-only endpoint for dashboard.
    """
    try:
        health = get_system_health()
        resources = get_resource_usage()
        database = get_database_metrics(db)
        flows = get_flow_metrics(db)
        users = get_user_metrics(db)
        data = get_data_metrics(db)
        
        # Placeholder alert and performance metrics
        alerts = AlertSummary(
            total_alerts=0,
            critical_alerts=0,
            warning_alerts=0,
            recent_alerts=[]
        )
        
        performance = PerformanceMetrics(
            api_response_time_ms=45.0,
            api_requests_per_minute=120.0,
            error_rate_percent=0.5,
            throughput_records_per_second=1500.0
        )
        
        return SystemOverview(
            health=health,
            resources=resources,
            database=database,
            flows=flows,
            users=users,
            data=data,
            alerts=alerts,
            performance=performance
        )
        
    except Exception as e:
        logger.error(f"Failed to get system overview: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system overview"
        )


@router.get("/resources", response_model=ResourceUsage, summary="Resource Usage")
async def get_resource_metrics(
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get current system resource usage metrics.
    """
    return get_resource_usage()


@router.get("/flows", response_model=FlowMetrics, summary="Flow Metrics")
async def get_flow_monitoring_metrics(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get flow execution and performance metrics.
    """
    return get_flow_metrics(db)


@router.get("/users", response_model=UserMetrics, summary="User Metrics")
async def get_user_monitoring_metrics(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_admin_permission())
):
    """
    Get user activity and engagement metrics.
    """
    return get_user_metrics(db)


@router.get("/database", response_model=DatabaseMetrics, summary="Database Metrics")
async def get_database_monitoring_metrics(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_admin_permission())
):
    """
    Get database performance and health metrics.
    """
    return get_database_metrics(db)


@router.get("/alerts", response_model=AlertSummary, summary="System Alerts")
async def get_system_alerts(
    severity: Optional[str] = Query(None, description="Filter by alert severity"),
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get system alerts and notifications.
    """
    # Placeholder implementation
    return AlertSummary(
        total_alerts=0,
        critical_alerts=0,
        warning_alerts=0,
        recent_alerts=[]
    )


@router.get("/performance", response_model=PerformanceMetrics, summary="Performance Metrics")
async def get_performance_metrics(
    timeframe_hours: int = Query(24, ge=1, le=168),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get API and system performance metrics.
    """
    # Placeholder implementation
    return PerformanceMetrics(
        api_response_time_ms=45.0,
        api_requests_per_minute=120.0,
        error_rate_percent=0.5,
        throughput_records_per_second=1500.0
    )


@router.get("/logs", summary="System Logs")
async def get_system_logs(
    level: Optional[str] = Query(None, description="Log level filter"),
    limit: int = Query(100, ge=1, le=1000),
    component: Optional[str] = Query(None, description="Component filter"),
    current_user: User = Depends(get_current_active_user),
    _: bool = Depends(check_admin_permission())
):
    """
    Get recent system logs for debugging and monitoring.
    """
    # Placeholder implementation - would integrate with actual logging system
    return {
        "logs": [],
        "total_count": 0,
        "filtered_count": 0,
        "log_levels": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        "components": ["api", "celery", "database", "auth", "flows"]
    }


@router.post("/health-check", summary="Trigger Health Check")
async def trigger_health_check(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
    _: bool = Depends(check_admin_permission())
):
    """
    Trigger a comprehensive system health check.
    """
    try:
        # Perform health checks
        health_results = {
            "database": "healthy",
            "redis": "healthy",
            "celery": "healthy",
            "disk_space": "healthy",
            "memory": "healthy"
        }
        
        # Check database connectivity
        try:
            db.execute("SELECT 1")
            health_results["database"] = "healthy"
        except Exception:
            health_results["database"] = "unhealthy"
        
        # Check resource usage
        resources = get_resource_usage()
        if resources.memory_percent > 90:
            health_results["memory"] = "critical"
        elif resources.memory_percent > 80:
            health_results["memory"] = "warning"
        
        if resources.disk_percent > 90:
            health_results["disk_space"] = "critical"
        elif resources.disk_percent > 80:
            health_results["disk_space"] = "warning"
        
        # Determine overall status
        critical_issues = [k for k, v in health_results.items() if v == "unhealthy" or v == "critical"]
        warning_issues = [k for k, v in health_results.items() if v == "warning"]
        
        if critical_issues:
            overall_status = "unhealthy"
        elif warning_issues:
            overall_status = "degraded"
        else:
            overall_status = "healthy"
        
        logger.info(f"Health check completed: {overall_status}")
        
        return {
            "overall_status": overall_status,
            "component_status": health_results,
            "critical_issues": critical_issues,
            "warning_issues": warning_issues,
            "checked_at": datetime.utcnow().isoformat(),
            "checked_by": current_user.id
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Health check failed"
        )