from typing import List, Optional, Dict, Any, Union
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.data_source import DataSource
from app.models.data_sink import DataSink
from app.models.data_set import DataSet
from app.models.flow_node import FlowNode
from app.models.org import Org

router = APIRouter()

class MetricType(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    RATE = "rate"

class TimeRange(str, Enum):
    LAST_HOUR = "1h"
    LAST_6_HOURS = "6h"
    LAST_24_HOURS = "24h"
    LAST_7_DAYS = "7d"
    LAST_30_DAYS = "30d"
    LAST_90_DAYS = "90d"

class AggregationType(str, Enum):
    SUM = "sum"
    AVERAGE = "avg" 
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    RATE = "rate"
    P95 = "p95"
    P99 = "p99"

class MetricPoint(BaseModel):
    timestamp: datetime
    value: Union[int, float]
    labels: Optional[Dict[str, str]] = None

class MetricSeries(BaseModel):
    metric_name: str
    metric_type: MetricType
    labels: Dict[str, str]
    points: List[MetricPoint]
    summary: Dict[str, Union[int, float]]

class SystemMetrics(BaseModel):
    cpu_usage_percent: float
    memory_usage_percent: float
    disk_usage_percent: float
    network_io_mbps: float
    active_connections: int
    queue_depth: int
    error_rate_percent: float
    response_time_ms: float
    timestamp: datetime

class ResourceMetrics(BaseModel):
    resource_type: str
    resource_id: int
    resource_name: str
    metrics: Dict[str, Any]
    health_score: float
    status: str
    last_updated: datetime

class OrganizationMetrics(BaseModel):
    org_id: int
    org_name: str
    total_users: int
    active_users_24h: int
    total_data_sources: int
    active_data_sources: int
    total_data_sinks: int
    active_data_sinks: int
    total_flows: int
    running_flows: int
    data_processed_gb_24h: float
    api_requests_24h: int
    error_count_24h: int
    success_rate_percent: float
    cost_usd_24h: Optional[float]

class AlertRule(BaseModel):
    rule_id: str
    name: str
    description: str
    metric_name: str
    threshold: float
    operator: str = Field(..., regex="^(gt|gte|lt|lte|eq|ne)$")
    severity: str = Field(..., regex="^(low|medium|high|critical)$")
    enabled: bool
    notification_channels: List[str]

class AlertStatus(BaseModel):
    alert_id: str
    rule_id: str
    resource_type: str
    resource_id: int
    status: str = Field(..., regex="^(firing|resolved|silenced)$")
    current_value: float
    threshold: float
    severity: str
    triggered_at: datetime
    resolved_at: Optional[datetime]
    duration_minutes: Optional[int]
    message: str

# System-wide metrics
@router.get("/system", response_model=SystemMetrics)
async def get_system_metrics(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get current system metrics."""
    if not current_user.can_view_system_metrics_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view system metrics"
        )
    
    # In production, this would collect real system metrics
    system_metrics = SystemMetrics(
        cpu_usage_percent=45.2,
        memory_usage_percent=67.8,
        disk_usage_percent=23.1,
        network_io_mbps=124.5,
        active_connections=1247,
        queue_depth=45,
        error_rate_percent=0.12,
        response_time_ms=89.3,
        timestamp=datetime.utcnow()
    )
    
    return system_metrics

@router.get("/system/timeseries", response_model=List[MetricSeries])
async def get_system_timeseries(
    metrics: List[str] = Query(..., description="Metric names to fetch"),
    time_range: TimeRange = Query(TimeRange.LAST_24_HOURS),
    aggregation: AggregationType = Query(AggregationType.AVERAGE),
    resolution_minutes: int = Query(5, ge=1, le=1440),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get system metrics time series data."""
    if not current_user.can_view_system_metrics_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view system metrics"
        )
    
    # Calculate time window
    end_time = datetime.utcnow()
    if time_range == TimeRange.LAST_HOUR:
        start_time = end_time - timedelta(hours=1)
    elif time_range == TimeRange.LAST_6_HOURS:
        start_time = end_time - timedelta(hours=6)
    elif time_range == TimeRange.LAST_24_HOURS:
        start_time = end_time - timedelta(days=1)
    elif time_range == TimeRange.LAST_7_DAYS:
        start_time = end_time - timedelta(days=7)
    elif time_range == TimeRange.LAST_30_DAYS:
        start_time = end_time - timedelta(days=30)
    else:  # LAST_90_DAYS
        start_time = end_time - timedelta(days=90)
    
    # In production, this would query a time series database
    metric_series = []
    for metric_name in metrics:
        series = MetricSeries(
            metric_name=metric_name,
            metric_type=MetricType.GAUGE,
            labels={"host": "api-server-1"},
            points=[
                MetricPoint(
                    timestamp=start_time + timedelta(minutes=i * resolution_minutes),
                    value=50.0 + (i % 20),
                    labels={}
                ) for i in range(int((end_time - start_time).total_seconds() / (resolution_minutes * 60)))
            ],
            summary={"avg": 55.2, "min": 30.1, "max": 89.7, "count": 100}
        )
        metric_series.append(series)
    
    return metric_series

# Resource-specific metrics
@router.get("/resources/{resource_type}/{resource_id}", response_model=ResourceMetrics)
async def get_resource_metrics(
    resource_type: str,
    resource_id: int,
    time_range: TimeRange = Query(TimeRange.LAST_24_HOURS),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get metrics for a specific resource."""
    # Determine resource model class
    resource_model = None
    if resource_type == "data_sources":
        resource_model = DataSource
    elif resource_type == "data_sinks":
        resource_model = DataSink
    elif resource_type == "data_sets":
        resource_model = DataSet
    elif resource_type == "flows":
        resource_model = FlowNode
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid resource type: {resource_type}"
        )
    
    # Get and validate resource access
    resource = db.query(resource_model).filter(resource_model.id == resource_id).first()
    if not resource:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resource not found"
        )
    
    if not resource.accessible_by_(current_user, 'read'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view metrics for this resource"
        )
    
    # Get resource metrics
    metrics = resource.get_metrics_(time_range=time_range.value)
    
    return ResourceMetrics(
        resource_type=resource_type,
        resource_id=resource_id,
        resource_name=resource.name,
        metrics=metrics,
        health_score=resource.health_score_(),
        status=resource.status,
        last_updated=datetime.utcnow()
    )

@router.get("/resources/{resource_type}", response_model=List[ResourceMetrics])
async def get_resource_type_metrics(
    resource_type: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("health_score", regex="^(name|status|health_score|last_updated)$"),
    sort_order: str = Query("desc", regex="^(asc|desc)$"),
    status_filter: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get metrics for all resources of a specific type."""
    # Determine resource model class
    resource_model = None
    if resource_type == "data_sources":
        resource_model = DataSource
    elif resource_type == "data_sinks":
        resource_model = DataSink
    elif resource_type == "data_sets":
        resource_model = DataSet
    elif resource_type == "flows":
        resource_model = FlowNode
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid resource type: {resource_type}"
        )
    
    # Get resources accessible to user
    query = resource_model.accessible_to_user(db, current_user)
    
    if status_filter:
        query = query.filter(resource_model.status == status_filter.upper())
    
    # Apply sorting
    if sort_by == "name":
        order_attr = resource_model.name
    elif sort_by == "status":
        order_attr = resource_model.status
    elif sort_by == "last_updated":
        order_attr = resource_model.updated_at
    else:  # health_score - would need to be computed
        order_attr = resource_model.updated_at  # Fallback
    
    if sort_order == "desc":
        query = query.order_by(order_attr.desc())
    else:
        query = query.order_by(order_attr.asc())
    
    resources = query.offset(offset).limit(limit).all()
    
    # Get metrics for each resource
    resource_metrics = []
    for resource in resources:
        metrics = resource.get_metrics_(time_range="24h")
        resource_metric = ResourceMetrics(
            resource_type=resource_type,
            resource_id=resource.id,
            resource_name=resource.name,
            metrics=metrics,
            health_score=resource.health_score_(),
            status=resource.status,
            last_updated=getattr(resource, 'updated_at', resource.created_at)
        )
        resource_metrics.append(resource_metric)
    
    return resource_metrics

# Organization metrics
@router.get("/organization", response_model=OrganizationMetrics)
async def get_organization_metrics(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get metrics for the current user's organization."""
    org = current_user.org
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User is not associated with an organization"
        )
    
    # Calculate organization metrics
    org_metrics = org.calculate_metrics_()
    
    return OrganizationMetrics(
        org_id=org.id,
        org_name=org.name,
        total_users=org_metrics.get("total_users", 0),
        active_users_24h=org_metrics.get("active_users_24h", 0),
        total_data_sources=org_metrics.get("total_data_sources", 0),
        active_data_sources=org_metrics.get("active_data_sources", 0),
        total_data_sinks=org_metrics.get("total_data_sinks", 0),
        active_data_sinks=org_metrics.get("active_data_sinks", 0),
        total_flows=org_metrics.get("total_flows", 0),
        running_flows=org_metrics.get("running_flows", 0),
        data_processed_gb_24h=org_metrics.get("data_processed_gb_24h", 0.0),
        api_requests_24h=org_metrics.get("api_requests_24h", 0),
        error_count_24h=org_metrics.get("error_count_24h", 0),
        success_rate_percent=org_metrics.get("success_rate_percent", 100.0),
        cost_usd_24h=org_metrics.get("cost_usd_24h")
    )

@router.get("/organization/{org_id}", response_model=OrganizationMetrics)
async def get_specific_organization_metrics(
    org_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get metrics for a specific organization (admin only)."""
    if not current_user.is_super_admin_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only super administrators can view other organization metrics"
        )
    
    org = db.query(Org).filter(Org.id == org_id).first()
    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )
    
    # Calculate organization metrics
    org_metrics = org.calculate_metrics_()
    
    return OrganizationMetrics(
        org_id=org.id,
        org_name=org.name,
        total_users=org_metrics.get("total_users", 0),
        active_users_24h=org_metrics.get("active_users_24h", 0),
        total_data_sources=org_metrics.get("total_data_sources", 0),
        active_data_sources=org_metrics.get("active_data_sources", 0),
        total_data_sinks=org_metrics.get("total_data_sinks", 0),
        active_data_sinks=org_metrics.get("active_data_sinks", 0),
        total_flows=org_metrics.get("total_flows", 0),
        running_flows=org_metrics.get("running_flows", 0),
        data_processed_gb_24h=org_metrics.get("data_processed_gb_24h", 0.0),
        api_requests_24h=org_metrics.get("api_requests_24h", 0),
        error_count_24h=org_metrics.get("error_count_24h", 0),
        success_rate_percent=org_metrics.get("success_rate_percent", 100.0),
        cost_usd_24h=org_metrics.get("cost_usd_24h")
    )

# Custom metrics query
@router.post("/query", response_model=List[MetricSeries])
async def query_metrics(
    query: str = Query(..., description="Metrics query expression"),
    start_time: datetime = Query(...),
    end_time: datetime = Query(...),
    step_seconds: int = Query(60, ge=1, le=3600),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Execute a custom metrics query."""
    if not current_user.can_execute_custom_queries_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to execute custom metrics queries"
        )
    
    # In production, this would execute the query against a metrics database
    # For now, return a placeholder response
    
    # Validate time range
    if end_time <= start_time:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="End time must be after start time"
        )
    
    if (end_time - start_time).total_seconds() > 86400 * 90:  # 90 days
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Time range cannot exceed 90 days"
        )
    
    # Mock query execution
    return [
        MetricSeries(
            metric_name="query_result",
            metric_type=MetricType.GAUGE,
            labels={"query": query},
            points=[
                MetricPoint(
                    timestamp=start_time + timedelta(seconds=i * step_seconds),
                    value=100.0 + (i % 50),
                    labels={}
                ) for i in range(int((end_time - start_time).total_seconds() / step_seconds))
            ],
            summary={"avg": 125.0, "min": 100.0, "max": 149.0, "count": 100}
        )
    ]

# Health and status endpoints
@router.get("/health", response_model=Dict[str, Any])
async def get_health_status(
    include_details: bool = Query(False),
    current_user: User = Depends(get_current_user)
):
    """Get overall system health status."""
    if not current_user.can_view_system_health_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view system health"
        )
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "uptime_seconds": 86400,  # Mock 24 hours
        "version": "1.0.0"
    }
    
    if include_details:
        health_status.update({
            "services": {
                "database": {"status": "healthy", "response_time_ms": 12},
                "redis": {"status": "healthy", "response_time_ms": 5},
                "storage": {"status": "healthy", "disk_usage_percent": 23.1},
                "queue": {"status": "healthy", "depth": 45}
            },
            "metrics": {
                "requests_per_second": 150.3,
                "error_rate_percent": 0.12,
                "avg_response_time_ms": 89.3
            }
        })
    
    return health_status

@router.get("/status", response_model=Dict[str, Any])
async def get_system_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get current system status and key metrics."""
    if not current_user.can_view_system_status_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view system status"
        )
    
    # Get counts of various resources
    org_id = current_user.org_id
    
    total_data_sources = db.query(DataSource).filter(DataSource.org_id == org_id).count()
    active_data_sources = db.query(DataSource).filter(
        DataSource.org_id == org_id,
        DataSource.status == 'ACTIVE'
    ).count()
    
    total_data_sinks = db.query(DataSink).filter(DataSink.org_id == org_id).count()
    active_data_sinks = db.query(DataSink).filter(
        DataSink.org_id == org_id,
        DataSink.status == 'ACTIVE'
    ).count()
    
    total_flows = db.query(FlowNode).filter(FlowNode.org_id == org_id).count()
    running_flows = db.query(FlowNode).filter(
        FlowNode.org_id == org_id,
        FlowNode.status == 'RUNNING'
    ).count()
    
    return {
        "timestamp": datetime.utcnow(),
        "organization": {
            "id": org_id,
            "name": current_user.org.name if current_user.org else "Unknown"
        },
        "resources": {
            "data_sources": {"total": total_data_sources, "active": active_data_sources},
            "data_sinks": {"total": total_data_sinks, "active": active_data_sinks},
            "flows": {"total": total_flows, "running": running_flows}
        },
        "system": {
            "status": "operational",
            "load": "normal",
            "maintenance_mode": False
        }
    }

# Alerting endpoints
@router.get("/alerts", response_model=List[AlertStatus])
async def get_active_alerts(
    severity: Optional[str] = Query(None, regex="^(low|medium|high|critical)$"),
    resource_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get active alerts."""
    if not current_user.can_view_alerts_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view alerts"
        )
    
    # In production, this would query an alerting system
    # For now, return mock alerts
    mock_alerts = [
        AlertStatus(
            alert_id="alert_001",
            rule_id="high_cpu_usage",
            resource_type="system",
            resource_id=1,
            status="firing",
            current_value=85.2,
            threshold=80.0,
            severity="high",
            triggered_at=datetime.utcnow() - timedelta(minutes=15),
            resolved_at=None,
            duration_minutes=15,
            message="CPU usage is above 80% threshold"
        ),
        AlertStatus(
            alert_id="alert_002",
            rule_id="flow_failure",
            resource_type="flows",
            resource_id=123,
            status="firing",
            current_value=1.0,
            threshold=0.0,
            severity="critical",
            triggered_at=datetime.utcnow() - timedelta(minutes=5),
            resolved_at=None,
            duration_minutes=5,
            message="Flow execution failed"
        )
    ]
    
    # Apply filters
    filtered_alerts = mock_alerts
    if severity:
        filtered_alerts = [alert for alert in filtered_alerts if alert.severity == severity]
    if resource_type:
        filtered_alerts = [alert for alert in filtered_alerts if alert.resource_type == resource_type]
    
    # Apply pagination
    return filtered_alerts[offset:offset + limit]

@router.get("/alerts/rules", response_model=List[AlertRule])
async def get_alert_rules(
    enabled_only: bool = Query(True),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get configured alert rules."""
    if not current_user.can_manage_alerts_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view alert rules"
        )
    
    # In production, this would query alert rule configuration
    mock_rules = [
        AlertRule(
            rule_id="high_cpu_usage",
            name="High CPU Usage",
            description="Alert when CPU usage exceeds 80%",
            metric_name="cpu_usage_percent",
            threshold=80.0,
            operator="gte",
            severity="high",
            enabled=True,
            notification_channels=["email", "slack"]
        ),
        AlertRule(
            rule_id="flow_failure",
            name="Flow Failure",
            description="Alert when flow execution fails",
            metric_name="flow_status",
            threshold=0.0,
            operator="eq",
            severity="critical",
            enabled=True,
            notification_channels=["email", "pager"]
        )
    ]
    
    if enabled_only:
        mock_rules = [rule for rule in mock_rules if rule.enabled]
    
    return mock_rules