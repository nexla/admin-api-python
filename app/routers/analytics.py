from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field, validator

from ..database import get_db
from ..auth import get_current_user
from ..models.user import User
from ..models.analytics import (
    MetricDefinition, MetricValue, AlertRule, AlertInstance,
    Dashboard, AnalyticsReport, MetricType, AlertSeverity
)
from ..services.analytics_service import AnalyticsService

router = APIRouter()

# Request/Response Models
class MetricDefinitionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    metric_type: MetricType
    unit: Optional[str] = None
    labels: Optional[Dict[str, str]] = {}
    collection_interval: int = Field(default=60, ge=1)
    retention_days: int = Field(default=90, ge=1)
    aggregation_rules: Optional[Dict[str, Any]] = {}

class MetricDefinitionResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    metric_type: str
    unit: Optional[str]
    labels: Dict[str, str]
    collection_interval: int
    retention_days: int
    enabled: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class MetricValueCreate(BaseModel):
    metric_name: str
    value: float
    labels: Optional[Dict[str, str]] = {}
    timestamp: Optional[datetime] = None

class MetricValueResponse(BaseModel):
    id: int
    value: float
    labels: Dict[str, str]
    timestamp: datetime

    class Config:
        from_attributes = True

class AlertRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    metric_definition_id: int
    condition: str = Field(..., min_length=1)
    threshold: float
    comparison_operator: str = Field(default=">")
    evaluation_window: int = Field(default=300, ge=1)
    severity: AlertSeverity
    notification_channels: List[Dict[str, Any]] = []
    escalation_rules: Optional[Dict[str, Any]] = {}

class AlertRuleResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    condition: str
    threshold: float
    comparison_operator: str
    evaluation_window: int
    severity: str
    enabled: bool
    last_evaluation: Optional[datetime]
    last_triggered: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True

class AlertInstanceResponse(BaseModel):
    id: int
    status: str
    severity: str
    triggered_value: float
    triggered_labels: Dict[str, str]
    message: str
    triggered_at: datetime
    acknowledged_at: Optional[datetime]
    resolved_at: Optional[datetime]
    resolution_reason: Optional[str]

    class Config:
        from_attributes = True

class DashboardCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    widgets: List[Dict[str, Any]]
    layout: Optional[Dict[str, Any]] = {}
    filters: Optional[Dict[str, Any]] = {}
    refresh_interval: int = Field(default=300, ge=1)
    is_public: bool = False
    tags: Optional[List[str]] = []

class DashboardResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    widgets: List[Dict[str, Any]]
    layout: Dict[str, Any]
    filters: Dict[str, Any]
    refresh_interval: int
    is_public: bool
    tags: List[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class AnalyticsReportCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    report_type: str = Field(..., pattern="^(metric_summary|alert_summary|custom_query)$")
    query: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = {}
    schedule: Optional[Dict[str, Any]] = {}
    format: str = Field(default="json")
    delivery_channels: List[Dict[str, Any]] = []

class AnalyticsReportResponse(BaseModel):
    id: int
    name: str
    report_type: str
    parameters: Dict[str, Any]
    schedule: Dict[str, Any]
    format: str
    enabled: bool
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True

class MetricDataQuery(BaseModel):
    metric_name: str
    start_time: datetime
    end_time: datetime
    labels: Optional[Dict[str, str]] = {}
    aggregation: Optional[str] = None
    interval: str = Field(default="5m")

# Metric Definition Endpoints
@router.post("/metrics/definitions", response_model=MetricDefinitionResponse, status_code=status.HTTP_201_CREATED)
async def create_metric_definition(
    definition: MetricDefinitionCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    # Check if metric name already exists
    existing = db.query(MetricDefinition).filter(
        MetricDefinition.name == definition.name
    ).first()
    
    if existing:
        raise HTTPException(status_code=400, detail="Metric definition with this name already exists")
    
    metric_def = MetricDefinition(
        name=definition.name,
        description=definition.description,
        metric_type=definition.metric_type,
        unit=definition.unit,
        labels=definition.labels,
        collection_interval=definition.collection_interval,
        retention_days=definition.retention_days,
        aggregation_rules=definition.aggregation_rules,
        org_id=current_user.default_org_id
    )
    
    db.add(metric_def)
    db.commit()
    db.refresh(metric_def)
    
    return metric_def

@router.get("/metrics/definitions", response_model=List[MetricDefinitionResponse])
async def list_metric_definitions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    definitions = db.query(MetricDefinition).filter(
        MetricDefinition.org_id == current_user.default_org_id
    ).offset(skip).limit(limit).all()
    
    return definitions

@router.get("/metrics/definitions/{definition_id}", response_model=MetricDefinitionResponse)
async def get_metric_definition(
    definition_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    definition = db.query(MetricDefinition).filter(
        MetricDefinition.id == definition_id,
        MetricDefinition.org_id == current_user.default_org_id
    ).first()
    
    if not definition:
        raise HTTPException(status_code=404, detail="Metric definition not found")
    
    return definition

# Metric Value Endpoints
@router.post("/metrics/values", response_model=MetricValueResponse, status_code=status.HTTP_201_CREATED)
async def record_metric_value(
    metric_data: MetricValueCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    analytics_service = AnalyticsService(db)
    
    try:
        metric_value = await analytics_service.collect_metric(
            metric_data.metric_name,
            metric_data.value,
            metric_data.labels,
            metric_data.timestamp
        )
        return metric_value
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/metrics/query", response_model=List[Dict[str, Any]])
async def query_metric_data(
    query: MetricDataQuery,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    analytics_service = AnalyticsService(db)
    
    try:
        data = await analytics_service.get_metric_data(
            query.metric_name,
            query.start_time,
            query.end_time,
            query.labels,
            query.aggregation,
            query.interval
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# Alert Rule Endpoints
@router.post("/alerts/rules", response_model=AlertRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_alert_rule(
    rule: AlertRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    # Verify metric definition exists and belongs to user's org
    metric_def = db.query(MetricDefinition).filter(
        MetricDefinition.id == rule.metric_definition_id,
        MetricDefinition.org_id == current_user.default_org_id
    ).first()
    
    if not metric_def:
        raise HTTPException(status_code=400, detail="Metric definition not found")
    
    alert_rule = AlertRule(
        name=rule.name,
        description=rule.description,
        metric_definition_id=rule.metric_definition_id,
        condition=rule.condition,
        threshold=rule.threshold,
        comparison_operator=rule.comparison_operator,
        evaluation_window=rule.evaluation_window,
        severity=rule.severity,
        notification_channels=rule.notification_channels,
        escalation_rules=rule.escalation_rules,
        org_id=current_user.default_org_id
    )
    
    db.add(alert_rule)
    db.commit()
    db.refresh(alert_rule)
    
    return alert_rule

@router.get("/alerts/rules", response_model=List[AlertRuleResponse])
async def list_alert_rules(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    rules = db.query(AlertRule).filter(
        AlertRule.org_id == current_user.default_org_id
    ).offset(skip).limit(limit).all()
    
    return rules

@router.get("/alerts/instances", response_model=List[AlertInstanceResponse])
async def list_alert_instances(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    status_filter: Optional[str] = Query(None),
    severity_filter: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    query = db.query(AlertInstance).join(AlertRule).filter(
        AlertRule.org_id == current_user.default_org_id
    )
    
    if status_filter:
        query = query.filter(AlertInstance.status == status_filter)
    
    if severity_filter:
        query = query.filter(AlertInstance.severity == severity_filter)
    
    instances = query.order_by(AlertInstance.triggered_at.desc()).offset(skip).limit(limit).all()
    
    return instances

@router.post("/alerts/instances/{instance_id}/acknowledge")
async def acknowledge_alert(
    instance_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    analytics_service = AnalyticsService(db)
    
    try:
        alert = await analytics_service.acknowledge_alert(instance_id, current_user.id)
        return {"message": "Alert acknowledged successfully", "alert_id": alert.id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/alerts/instances/{instance_id}/resolve")
async def resolve_alert(
    instance_id: int,
    reason: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    analytics_service = AnalyticsService(db)
    
    try:
        alert = await analytics_service.resolve_alert(instance_id, current_user.id, reason)
        return {"message": "Alert resolved successfully", "alert_id": alert.id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# Dashboard Endpoints
@router.post("/dashboards", response_model=DashboardResponse, status_code=status.HTTP_201_CREATED)
async def create_dashboard(
    dashboard: DashboardCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    analytics_service = AnalyticsService(db)
    
    created_dashboard = await analytics_service.create_dashboard(
        dashboard.name,
        dashboard.widgets,
        current_user.default_org_id,
        current_user.id,
        dashboard.description,
        dashboard.layout
    )
    
    return created_dashboard

@router.get("/dashboards", response_model=List[DashboardResponse])
async def list_dashboards(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    dashboards = db.query(Dashboard).filter(
        Dashboard.org_id == current_user.default_org_id
    ).offset(skip).limit(limit).all()
    
    return dashboards

@router.get("/dashboards/{dashboard_id}", response_model=DashboardResponse)
async def get_dashboard(
    dashboard_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    dashboard = db.query(Dashboard).filter(
        Dashboard.id == dashboard_id,
        Dashboard.org_id == current_user.default_org_id
    ).first()
    
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    
    return dashboard

# Report Endpoints
@router.post("/reports", response_model=AnalyticsReportResponse, status_code=status.HTTP_201_CREATED)
async def create_report(
    report: AnalyticsReportCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    analytics_report = AnalyticsReport(
        name=report.name,
        report_type=report.report_type,
        query=report.query,
        parameters=report.parameters,
        schedule=report.schedule,
        format=report.format,
        delivery_channels=report.delivery_channels,
        org_id=current_user.default_org_id,
        created_by=current_user.id
    )
    
    db.add(analytics_report)
    db.commit()
    db.refresh(analytics_report)
    
    return analytics_report

@router.get("/reports", response_model=List[AnalyticsReportResponse])
async def list_reports(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    if not current_user.default_org:
        raise HTTPException(status_code=400, detail="User must belong to an organization")
    
    reports = db.query(AnalyticsReport).filter(
        AnalyticsReport.org_id == current_user.default_org_id
    ).offset(skip).limit(limit).all()
    
    return reports

@router.post("/reports/{report_id}/generate")
async def generate_report(
    report_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # Verify report belongs to user's org
    report = db.query(AnalyticsReport).filter(
        AnalyticsReport.id == report_id,
        AnalyticsReport.org_id == current_user.default_org_id
    ).first()
    
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    analytics_service = AnalyticsService(db)
    
    try:
        report_run = await analytics_service.generate_report(report_id, current_user.id)
        return {
            "message": "Report generation initiated",
            "run_id": report_run.id,
            "status": report_run.status
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# System Health Endpoints
@router.get("/system/health")
async def get_system_health(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Super user access required")
    
    # Basic system health metrics
    total_metrics = db.query(MetricDefinition).count()
    active_alerts = db.query(AlertInstance).filter(
        AlertInstance.status == "active"
    ).count()
    
    recent_cutoff = datetime.utcnow() - timedelta(hours=1)
    recent_metric_values = db.query(MetricValue).filter(
        MetricValue.timestamp >= recent_cutoff
    ).count()
    
    return {
        "status": "healthy",
        "metrics": {
            "total_metric_definitions": total_metrics,
            "active_alerts": active_alerts,
            "recent_metric_values": recent_metric_values
        },
        "timestamp": datetime.utcnow().isoformat()
    }