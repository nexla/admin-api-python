from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field, validator

from ..database import get_db
from ..auth import get_current_user
from ..models.user import User
from ..models.security import (
    SecurityRole, RoleAssignment, SecurityPolicy, AccessControlEntry,
    SecurityAuditLog, SecurityRule, SecurityIncident, DataClassification,
    ResourceType, ActionType, RoleType
)
from ..services.security_service import SecurityService, RBACService

router = APIRouter()

# Request/Response Models
class RoleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    display_name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    role_type: RoleType = RoleType.CUSTOM
    permissions: List[str] = []
    parent_role_id: Optional[int] = None

class RoleResponse(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str]
    role_type: str
    permissions: List[str]
    parent_role_id: Optional[int]
    level: int
    enabled: bool
    created_at: datetime

    class Config:
        from_attributes = True

class RoleAssignmentCreate(BaseModel):
    user_id: int
    role_id: int
    resource_type: Optional[str] = None
    resource_id: Optional[int] = None
    expires_at: Optional[datetime] = None
    conditions: Optional[Dict[str, Any]] = {}

class RoleAssignmentResponse(BaseModel):
    id: int
    user_id: int
    role_id: int
    resource_type: Optional[str]
    resource_id: Optional[int]
    granted_at: datetime
    expires_at: Optional[datetime]
    conditions: Dict[str, Any]

    class Config:
        from_attributes = True

class PermissionCheck(BaseModel):
    action: str
    resource_type: str
    resource_id: Optional[int] = None
    context: Optional[Dict[str, Any]] = {}

class SecurityRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    rule_type: str = Field(..., regex="^(rate_limit|ip_whitelist|geo_restriction|threat_intelligence)$")
    conditions: Dict[str, Any]
    actions: List[str]
    threshold_value: Optional[int] = None
    time_window: Optional[int] = None
    priority: int = Field(default=100, ge=1, le=1000)

class SecurityRuleResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    rule_type: str
    conditions: Dict[str, Any]
    actions: List[str]
    threshold_value: Optional[int]
    time_window: Optional[int]
    enabled: bool
    priority: int
    created_at: datetime
    last_triggered: Optional[datetime]

    class Config:
        from_attributes = True

class SecurityIncidentCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: str
    incident_type: str = Field(..., min_length=1, max_length=50)
    severity: str = Field(..., regex="^(low|medium|high|critical)$")
    affected_users: List[int] = []
    affected_resources: List[Dict[str, Any]] = []

class SecurityIncidentResponse(BaseModel):
    id: int
    title: str
    description: str
    incident_type: str
    severity: str
    status: str
    affected_users: List[int]
    affected_resources: List[Dict[str, Any]]
    detected_at: datetime
    reported_at: Optional[datetime]
    resolved_at: Optional[datetime]

    class Config:
        from_attributes = True

class DataClassificationCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    level: int = Field(..., ge=1, le=4)
    detection_rules: List[Dict[str, Any]] = []
    handling_requirements: Dict[str, Any] = {}
    retention_policy: Dict[str, Any] = {}
    default_permissions: Dict[str, Any] = {}
    encryption_required: bool = False
    compliance_frameworks: List[str] = []

class DataClassificationResponse(BaseModel):
    id: int
    name: str
    level: int
    detection_rules: List[Dict[str, Any]]
    handling_requirements: Dict[str, Any]
    retention_policy: Dict[str, Any]
    default_permissions: Dict[str, Any]
    encryption_required: bool
    compliance_frameworks: List[str]
    active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class AuditLogResponse(BaseModel):
    id: int
    event_type: str
    action: str
    result: str
    user_id: Optional[int]
    user_email: Optional[str]
    user_ip: Optional[str]
    resource_type: Optional[str]
    resource_id: Optional[int]
    details: Dict[str, Any]
    timestamp: datetime

    class Config:
        from_attributes = True

# Role Management Endpoints
@router.post("/roles", response_model=RoleResponse, status_code=status.HTTP_201_CREATED)
async def create_role(
    role: RoleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Super user access required")
    
    rbac_service = RBACService(db)
    
    try:
        created_role = rbac_service.create_role(
            name=role.name,
            display_name=role.display_name,
            permissions=role.permissions,
            org_id=current_user.default_org_id,
            role_type=role.role_type,
            parent_role_id=role.parent_role_id,
            created_by=current_user.id
        )
        return created_role
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/roles", response_model=List[RoleResponse])
async def list_roles(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    org_id: Optional[int] = Query(None),
    role_type: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    query = db.query(SecurityRole)
    
    # Filter by org if specified or user's default org
    if org_id:
        if not current_user.super_user_():
            raise HTTPException(status_code=403, detail="Cannot access other organization's roles")
        query = query.filter(SecurityRole.org_id == org_id)
    else:
        query = query.filter(SecurityRole.org_id == current_user.default_org_id)
    
    if role_type:
        query = query.filter(SecurityRole.role_type == role_type)
    
    roles = query.offset(skip).limit(limit).all()
    return roles

@router.get("/roles/{role_id}", response_model=RoleResponse)
async def get_role(
    role_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    role = db.query(SecurityRole).filter(SecurityRole.id == role_id).first()
    
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    
    # Check access
    if role.org_id != current_user.default_org_id and not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Access denied")
    
    return role

# Role Assignment Endpoints
@router.post("/role-assignments", response_model=RoleAssignmentResponse, status_code=status.HTTP_201_CREATED)
async def assign_role(
    assignment: RoleAssignmentCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    rbac_service = RBACService(db)
    
    # Check if current user can assign this role
    role = db.query(SecurityRole).filter(SecurityRole.id == assignment.role_id).first()
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    
    if role.org_id != current_user.default_org_id and not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Cannot assign role from different organization")
    
    try:
        created_assignment = rbac_service.assign_role(
            user_id=assignment.user_id,
            role_id=assignment.role_id,
            resource_type=assignment.resource_type,
            resource_id=assignment.resource_id,
            expires_at=assignment.expires_at,
            conditions=assignment.conditions,
            granted_by=current_user.id
        )
        return created_assignment
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/role-assignments", response_model=List[RoleAssignmentResponse])
async def list_role_assignments(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    user_id: Optional[int] = Query(None),
    role_id: Optional[int] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    query = db.query(RoleAssignment)
    
    if user_id:
        query = query.filter(RoleAssignment.user_id == user_id)
    
    if role_id:
        query = query.filter(RoleAssignment.role_id == role_id)
    
    # Filter by organization access
    if not current_user.super_user_():
        query = query.join(SecurityRole).filter(
            SecurityRole.org_id == current_user.default_org_id
        )
    
    assignments = query.offset(skip).limit(limit).all()
    return assignments

@router.delete("/role-assignments/{assignment_id}")
async def revoke_role_assignment(
    assignment_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    assignment = db.query(RoleAssignment).filter(RoleAssignment.id == assignment_id).first()
    
    if not assignment:
        raise HTTPException(status_code=404, detail="Role assignment not found")
    
    # Check access
    role = assignment.role
    if role.org_id != current_user.default_org_id and not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Access denied")
    
    db.delete(assignment)
    db.commit()
    
    return {"message": "Role assignment revoked successfully"}

# Permission Checking Endpoints
@router.post("/check-permission")
async def check_permission(
    permission_check: PermissionCheck,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    rbac_service = RBACService(db)
    
    has_permission = rbac_service.check_permission(
        user_id=current_user.id,
        action=permission_check.action,
        resource_type=permission_check.resource_type,
        resource_id=permission_check.resource_id,
        context=permission_check.context
    )
    
    return {"has_permission": has_permission}

@router.get("/my-permissions")
async def get_my_permissions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    rbac_service = RBACService(db)
    
    # Get user's active role assignments
    assignments = rbac_service._get_user_active_assignments(current_user.id)
    
    permissions = []
    for assignment in assignments:
        role = assignment.role
        for permission in role.permissions:
            permissions.append({
                "permission": permission,
                "role": role.name,
                "resource_type": assignment.resource_type,
                "resource_id": assignment.resource_id
            })
    
    return {"permissions": permissions}

# Security Rules Endpoints
@router.post("/rules", response_model=SecurityRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_security_rule(
    rule: SecurityRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Super user access required")
    
    security_rule = SecurityRule(
        name=rule.name,
        description=rule.description,
        rule_type=rule.rule_type,
        conditions=rule.conditions,
        actions=rule.actions,
        threshold_value=rule.threshold_value,
        time_window=rule.time_window,
        priority=rule.priority,
        org_id=current_user.default_org_id,
        created_by=current_user.id
    )
    
    db.add(security_rule)
    db.commit()
    db.refresh(security_rule)
    
    return security_rule

@router.get("/rules", response_model=List[SecurityRuleResponse])
async def list_security_rules(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    rule_type: Optional[str] = Query(None),
    enabled: Optional[bool] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    if not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Super user access required")
    
    query = db.query(SecurityRule)
    
    if rule_type:
        query = query.filter(SecurityRule.rule_type == rule_type)
    
    if enabled is not None:
        query = query.filter(SecurityRule.enabled == enabled)
    
    rules = query.offset(skip).limit(limit).all()
    return rules

# Security Incidents Endpoints
@router.post("/incidents", response_model=SecurityIncidentResponse, status_code=status.HTTP_201_CREATED)
async def create_security_incident(
    incident: SecurityIncidentCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    security_service = SecurityService(db)
    
    created_incident = security_service.create_security_incident(
        title=incident.title,
        description=incident.description,
        incident_type=incident.incident_type,
        severity=incident.severity,
        affected_users=incident.affected_users,
        affected_resources=incident.affected_resources,
        org_id=current_user.default_org_id,
        reported_by=current_user.id
    )
    
    return created_incident

@router.get("/incidents", response_model=List[SecurityIncidentResponse])
async def list_security_incidents(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    status_filter: Optional[str] = Query(None),
    severity_filter: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    query = db.query(SecurityIncident)
    
    # Filter by organization
    if not current_user.super_user_():
        query = query.filter(SecurityIncident.org_id == current_user.default_org_id)
    
    if status_filter:
        query = query.filter(SecurityIncident.status == status_filter)
    
    if severity_filter:
        query = query.filter(SecurityIncident.severity == severity_filter)
    
    incidents = query.order_by(SecurityIncident.detected_at.desc()).offset(skip).limit(limit).all()
    return incidents

# Data Classification Endpoints
@router.post("/data-classifications", response_model=DataClassificationResponse, status_code=status.HTTP_201_CREATED)
async def create_data_classification(
    classification: DataClassificationCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Super user access required")
    
    data_classification = DataClassification(
        name=classification.name,
        level=classification.level,
        detection_rules=classification.detection_rules,
        handling_requirements=classification.handling_requirements,
        retention_policy=classification.retention_policy,
        default_permissions=classification.default_permissions,
        encryption_required=classification.encryption_required,
        compliance_frameworks=classification.compliance_frameworks,
        org_id=current_user.default_org_id,
        created_by=current_user.id
    )
    
    db.add(data_classification)
    db.commit()
    db.refresh(data_classification)
    
    return data_classification

@router.get("/data-classifications", response_model=List[DataClassificationResponse])
async def list_data_classifications(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    active_only: bool = Query(True),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    query = db.query(DataClassification)
    
    # Filter by organization
    if not current_user.super_user_():
        query = query.filter(DataClassification.org_id == current_user.default_org_id)
    
    if active_only:
        query = query.filter(DataClassification.active == True)
    
    classifications = query.order_by(DataClassification.level.desc()).offset(skip).limit(limit).all()
    return classifications

# Audit Log Endpoints
@router.get("/audit-logs", response_model=List[AuditLogResponse])
async def list_audit_logs(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    event_type: Optional[str] = Query(None),
    user_id: Optional[int] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    if not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Super user access required")
    
    query = db.query(SecurityAuditLog)
    
    if event_type:
        query = query.filter(SecurityAuditLog.event_type == event_type)
    
    if user_id:
        query = query.filter(SecurityAuditLog.user_id == user_id)
    
    if start_date:
        query = query.filter(SecurityAuditLog.timestamp >= start_date)
    
    if end_date:
        query = query.filter(SecurityAuditLog.timestamp <= end_date)
    
    audit_logs = query.order_by(SecurityAuditLog.timestamp.desc()).offset(skip).limit(limit).all()
    return audit_logs

# Security Dashboard Endpoints
@router.get("/dashboard")
async def get_security_dashboard(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not current_user.super_user_():
        raise HTTPException(status_code=403, detail="Super user access required")
    
    # Get security metrics
    recent_cutoff = datetime.utcnow() - timedelta(days=7)
    
    total_incidents = db.query(SecurityIncident).count()
    active_incidents = db.query(SecurityIncident).filter(
        SecurityIncident.status == "open"
    ).count()
    
    recent_violations = db.query(SecurityRule).count()  # This would be rule violations
    
    # Authentication events
    recent_auth_events = db.query(SecurityAuditLog).filter(
        and_(
            SecurityAuditLog.event_type == "authentication",
            SecurityAuditLog.timestamp >= recent_cutoff
        )
    ).count()
    
    failed_auth_events = db.query(SecurityAuditLog).filter(
        and_(
            SecurityAuditLog.event_type == "authentication",
            SecurityAuditLog.result == "failure",
            SecurityAuditLog.timestamp >= recent_cutoff
        )
    ).count()
    
    return {
        "incidents": {
            "total": total_incidents,
            "active": active_incidents
        },
        "recent_violations": recent_violations,
        "authentication": {
            "recent_attempts": recent_auth_events,
            "failed_attempts": failed_auth_events,
            "success_rate": ((recent_auth_events - failed_auth_events) / max(recent_auth_events, 1)) * 100
        },
        "timestamp": datetime.utcnow().isoformat()
    }