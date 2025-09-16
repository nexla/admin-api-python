from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from app.database import get_db
from app.models.user import User
from app.auth.dependencies import get_current_user, require_permissions
from app.services.audit_service import AuditService
from pydantic import BaseModel

router = APIRouter(prefix="/alerts", tags=["alerts"])

class AlertRule(BaseModel):
    name: str
    description: Optional[str] = None
    metric_name: str
    operator: str  # gt, lt, eq, gte, lte
    threshold: float
    duration_minutes: int = 5
    severity: str = "medium"  # low, medium, high, critical
    is_active: bool = True
    notification_channels: List[str] = []

class AlertRuleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    threshold: Optional[float] = None
    duration_minutes: Optional[int] = None
    severity: Optional[str] = None
    is_active: Optional[bool] = None
    notification_channels: Optional[List[str]] = None

class AlertInstance(BaseModel):
    rule_id: int
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    current_value: float
    message: str
    severity: str
    status: str = "active"  # active, acknowledged, resolved

class AlertResponse(BaseModel):
    id: int
    rule_name: str
    metric_name: str
    current_value: float
    threshold: float
    operator: str
    severity: str
    status: str
    triggered_at: datetime
    resolved_at: Optional[datetime]
    message: str
    duration_minutes: Optional[int]

# In-memory storage for demo (would use database in production)
alert_rules = {}
alert_instances = {}
alert_counter = 1

@router.get("/rules")
async def get_alert_rules(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    is_active: Optional[bool] = Query(None),
    severity: Optional[str] = Query(None),
    current_user: User = Depends(require_permissions(["alert.read"]))
):
    """Get alert rules with filtering"""
    rules = list(alert_rules.values())
    
    if is_active is not None:
        rules = [rule for rule in rules if rule.get("is_active") == is_active]
    
    if severity:
        rules = [rule for rule in rules if rule.get("severity") == severity]
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.list_rules",
        resource_type="alert_rule",
        details={"count": len(rules), "filters": {"is_active": is_active, "severity": severity}}
    )
    
    return rules[skip:skip + limit]

@router.post("/rules")
async def create_alert_rule(
    rule_data: AlertRule,
    current_user: User = Depends(require_permissions(["alert.create"]))
):
    """Create a new alert rule"""
    global alert_counter
    
    rule_id = alert_counter
    alert_counter += 1
    
    rule = {
        "id": rule_id,
        "name": rule_data.name,
        "description": rule_data.description,
        "metric_name": rule_data.metric_name,
        "operator": rule_data.operator,
        "threshold": rule_data.threshold,
        "duration_minutes": rule_data.duration_minutes,
        "severity": rule_data.severity,
        "is_active": rule_data.is_active,
        "notification_channels": rule_data.notification_channels,
        "created_by": current_user.id,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    
    alert_rules[rule_id] = rule
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.create_rule",
        resource_type="alert_rule",
        resource_id=rule_id,
        details={
            "name": rule_data.name,
            "metric": rule_data.metric_name,
            "threshold": rule_data.threshold,
            "severity": rule_data.severity
        }
    )
    
    return rule

@router.put("/rules/{rule_id}")
async def update_alert_rule(
    rule_id: int,
    rule_data: AlertRuleUpdate,
    current_user: User = Depends(require_permissions(["alert.update"]))
):
    """Update an alert rule"""
    if rule_id not in alert_rules:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    
    rule = alert_rules[rule_id]
    update_data = rule_data.dict(exclude_unset=True)
    
    for field, value in update_data.items():
        rule[field] = value
    
    rule["updated_at"] = datetime.utcnow()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.update_rule",
        resource_type="alert_rule",
        resource_id=rule_id,
        details={"updated_fields": list(update_data.keys())}
    )
    
    return rule

@router.delete("/rules/{rule_id}")
async def delete_alert_rule(
    rule_id: int,
    current_user: User = Depends(require_permissions(["alert.delete"]))
):
    """Delete an alert rule"""
    if rule_id not in alert_rules:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    
    rule = alert_rules.pop(rule_id)
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.delete_rule",
        resource_type="alert_rule",
        resource_id=rule_id,
        details={"name": rule.get("name")}
    )
    
    return {"message": "Alert rule deleted"}

@router.get("/instances", response_model=List[AlertResponse])
async def get_alert_instances(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    rule_id: Optional[int] = Query(None),
    current_user: User = Depends(require_permissions(["alert.read"]))
):
    """Get alert instances with filtering"""
    instances = list(alert_instances.values())
    
    if status:
        instances = [alert for alert in instances if alert.get("status") == status]
    
    if severity:
        instances = [alert for alert in instances if alert.get("severity") == severity]
    
    if rule_id:
        instances = [alert for alert in instances if alert.get("rule_id") == rule_id]
    
    # Sort by triggered_at descending
    instances.sort(key=lambda x: x.get("triggered_at", datetime.min), reverse=True)
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.list_instances",
        resource_type="alert_instance",
        details={
            "count": len(instances),
            "filters": {"status": status, "severity": severity, "rule_id": rule_id}
        }
    )
    
    return instances[skip:skip + limit]

@router.post("/instances/{instance_id}/acknowledge")
async def acknowledge_alert(
    instance_id: int,
    current_user: User = Depends(require_permissions(["alert.acknowledge"]))
):
    """Acknowledge an alert instance"""
    if instance_id not in alert_instances:
        raise HTTPException(status_code=404, detail="Alert instance not found")
    
    alert = alert_instances[instance_id]
    alert["status"] = "acknowledged"
    alert["acknowledged_by"] = current_user.id
    alert["acknowledged_at"] = datetime.utcnow()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.acknowledge",
        resource_type="alert_instance",
        resource_id=instance_id,
        details={"rule_name": alert.get("rule_name")}
    )
    
    return {"message": "Alert acknowledged"}

@router.post("/instances/{instance_id}/resolve")
async def resolve_alert(
    instance_id: int,
    current_user: User = Depends(require_permissions(["alert.resolve"]))
):
    """Resolve an alert instance"""
    if instance_id not in alert_instances:
        raise HTTPException(status_code=404, detail="Alert instance not found")
    
    alert = alert_instances[instance_id]
    alert["status"] = "resolved"
    alert["resolved_by"] = current_user.id
    alert["resolved_at"] = datetime.utcnow()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.resolve",
        resource_type="alert_instance",
        resource_id=instance_id,
        details={"rule_name": alert.get("rule_name")}
    )
    
    return {"message": "Alert resolved"}

@router.get("/dashboard")
async def get_alert_dashboard(
    current_user: User = Depends(require_permissions(["alert.read"]))
):
    """Get alert dashboard statistics"""
    instances = list(alert_instances.values())
    rules = list(alert_rules.values())
    
    # Calculate statistics
    active_alerts = len([a for a in instances if a.get("status") == "active"])
    acknowledged_alerts = len([a for a in instances if a.get("status") == "acknowledged"])
    resolved_alerts = len([a for a in instances if a.get("status") == "resolved"])
    
    severity_counts = {
        "critical": len([a for a in instances if a.get("severity") == "critical"]),
        "high": len([a for a in instances if a.get("severity") == "high"]),
        "medium": len([a for a in instances if a.get("severity") == "medium"]),
        "low": len([a for a in instances if a.get("severity") == "low"])
    }
    
    active_rules = len([r for r in rules if r.get("is_active")])
    inactive_rules = len([r for r in rules if not r.get("is_active")])
    
    # Recent alerts (last 24 hours)
    recent_cutoff = datetime.utcnow() - timedelta(hours=24)
    recent_alerts = [
        a for a in instances 
        if a.get("triggered_at", datetime.min) > recent_cutoff
    ]
    
    dashboard = {
        "summary": {
            "active_alerts": active_alerts,
            "acknowledged_alerts": acknowledged_alerts,
            "resolved_alerts": resolved_alerts,
            "total_rules": len(rules),
            "active_rules": active_rules,
            "inactive_rules": inactive_rules
        },
        "severity_breakdown": severity_counts,
        "recent_alerts_24h": len(recent_alerts),
        "top_triggered_rules": _get_top_triggered_rules(instances)
    }
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.view_dashboard",
        resource_type="alert_dashboard"
    )
    
    return dashboard

@router.post("/test-rule/{rule_id}")
async def test_alert_rule(
    rule_id: int,
    test_value: float,
    current_user: User = Depends(require_permissions(["alert.test"]))
):
    """Test an alert rule with a given value"""
    if rule_id not in alert_rules:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    
    rule = alert_rules[rule_id]
    threshold = rule["threshold"]
    operator = rule["operator"]
    
    # Evaluate condition
    triggered = False
    if operator == "gt" and test_value > threshold:
        triggered = True
    elif operator == "lt" and test_value < threshold:
        triggered = True
    elif operator == "gte" and test_value >= threshold:
        triggered = True
    elif operator == "lte" and test_value <= threshold:
        triggered = True
    elif operator == "eq" and test_value == threshold:
        triggered = True
    
    result = {
        "rule_name": rule["name"],
        "test_value": test_value,
        "threshold": threshold,
        "operator": operator,
        "triggered": triggered,
        "message": f"Test value {test_value} {operator} {threshold}: {'TRIGGERED' if triggered else 'OK'}"
    }
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="alert.test_rule",
        resource_type="alert_rule",
        resource_id=rule_id,
        details={"test_value": test_value, "triggered": triggered}
    )
    
    return result

def _get_top_triggered_rules(instances, limit=5):
    """Get the most frequently triggered rules"""
    rule_counts = {}
    for instance in instances:
        rule_id = instance.get("rule_id")
        if rule_id:
            rule_counts[rule_id] = rule_counts.get(rule_id, 0) + 1
    
    # Sort by count and get top rules
    sorted_rules = sorted(rule_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    result = []
    for rule_id, count in sorted_rules:
        if rule_id in alert_rules:
            rule = alert_rules[rule_id]
            result.append({
                "rule_id": rule_id,
                "rule_name": rule.get("name"),
                "trigger_count": count
            })
    
    return result