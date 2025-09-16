"""
Audit Logs Router - API endpoints for audit log querying and management
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.audit_log_enhanced import AuditLogEnhanced, AuditAction, AuditSeverity

router = APIRouter()

# Pydantic models
class AuditLogResponse(BaseModel):
    id: int
    action: str
    display_action: str
    severity: str
    display_severity: str
    auditable_type: str
    auditable_id: int
    user_id: Optional[int]
    org_id: Optional[int]
    ip_address: Optional[str]
    endpoint: Optional[str]
    method: Optional[str]
    comment: Optional[str]
    tags: List[str]
    is_sensitive: bool
    created_at: str
    expires_at: Optional[str]
    change_summary: str

class AuditLogDetailResponse(AuditLogResponse):
    audited_changes: Optional[Dict[str, Any]]
    old_values: Optional[Dict[str, Any]]
    new_values: Optional[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]]
    request_id: Optional[str]
    session_id: Optional[str]
    user_agent: Optional[str]
    retention_days: int
    is_exported: bool
    export_batch_id: Optional[str]
    error_code: Optional[str]
    error_message: Optional[str]

class AuditLogStatsResponse(BaseModel):
    period_days: int
    total_events: int
    security_events: int
    destructive_events: int
    critical_events: int
    average_events_per_day: float
    security_percentage: float

class AuditLogCreateRequest(BaseModel):
    action: AuditAction
    auditable_type: str = Field(..., min_length=1, max_length=100)
    auditable_id: int = Field(..., gt=0)
    audited_changes: Optional[Dict[str, Any]] = None
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    severity: AuditSeverity = AuditSeverity.INFO
    comment: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    extra_metadata: Dict[str, Any] = Field(default_factory=dict)
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    endpoint: Optional[str] = None
    method: Optional[str] = None
    retention_days: Optional[int] = Field(None, ge=1, le=3650)

class AuditLogSearchRequest(BaseModel):
    actions: Optional[List[AuditAction]] = None
    auditable_types: Optional[List[str]] = None
    user_ids: Optional[List[int]] = None
    severities: Optional[List[AuditSeverity]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    search_term: Optional[str] = None
    tags: Optional[List[str]] = None
    include_sensitive: bool = False
    security_events_only: bool = False
    destructive_actions_only: bool = False
    with_errors_only: bool = False

class AuditLogExportRequest(BaseModel):
    actions: Optional[List[AuditAction]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    batch_size: int = Field(default=10000, ge=1, le=50000)
    format: str = Field(default="json", regex="^(json|csv)$")

# Audit log querying endpoints
@router.get("/", response_model=List[AuditLogResponse])
async def list_audit_logs(
    actions: Optional[str] = Query(None, description="Comma-separated list of actions"),
    auditable_type: Optional[str] = Query(None),
    user_id: Optional[int] = Query(None),
    severity: Optional[AuditSeverity] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    security_events_only: bool = Query(False),
    destructive_actions_only: bool = Query(False),
    with_errors_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List audit logs with filtering options"""
    try:
        query = db.query(AuditLogEnhanced)
        
        # Filter by organization
        query = query.filter(AuditLogEnhanced.org_id == current_user.default_org_id)
        
        # Apply filters
        if actions:
            action_list = [AuditAction(action.strip()) for action in actions.split(",")]
            query = query.filter(AuditLogEnhanced.action.in_(action_list))
        
        if auditable_type:
            query = query.filter(AuditLogEnhanced.auditable_type == auditable_type)
        
        if user_id:
            query = query.filter(AuditLogEnhanced.user_id == user_id)
        
        if severity:
            query = query.filter(AuditLogEnhanced.severity == severity)
        
        if start_date:
            query = query.filter(AuditLogEnhanced.created_at >= start_date)
        
        if end_date:
            query = query.filter(AuditLogEnhanced.created_at <= end_date)
        
        if security_events_only:
            security_actions = [action for action in AuditAction if action.is_security_related]
            query = query.filter(AuditLogEnhanced.action.in_(security_actions))
        
        if destructive_actions_only:
            destructive_actions = [action for action in AuditAction if action.is_destructive]
            query = query.filter(AuditLogEnhanced.action.in_(destructive_actions))
        
        if with_errors_only:
            query = query.filter(
                (AuditLogEnhanced.error_code.isnot(None)) | 
                (AuditLogEnhanced.error_message.isnot(None))
            )
        
        # Order by creation time (newest first)
        query = query.order_by(AuditLogEnhanced.created_at.desc())
        
        # Apply pagination
        audit_logs = query.offset(offset).limit(limit).all()
        
        return [AuditLogResponse(**log.to_dict()) for log in audit_logs]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list audit logs: {str(e)}"
        )

@router.post("/search", response_model=List[AuditLogResponse])
async def search_audit_logs(
    search_request: AuditLogSearchRequest,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Advanced search for audit logs"""
    try:
        query = db.query(AuditLogEnhanced)
        
        # Filter by organization
        query = query.filter(AuditLogEnhanced.org_id == current_user.default_org_id)
        
        # Apply search filters
        if search_request.actions:
            query = query.filter(AuditLogEnhanced.action.in_(search_request.actions))
        
        if search_request.auditable_types:
            query = query.filter(AuditLogEnhanced.auditable_type.in_(search_request.auditable_types))
        
        if search_request.user_ids:
            query = query.filter(AuditLogEnhanced.user_id.in_(search_request.user_ids))
        
        if search_request.severities:
            query = query.filter(AuditLogEnhanced.severity.in_(search_request.severities))
        
        if search_request.start_date:
            query = query.filter(AuditLogEnhanced.created_at >= search_request.start_date)
        
        if search_request.end_date:
            query = query.filter(AuditLogEnhanced.created_at <= search_request.end_date)
        
        if search_request.tags:
            # Filter by tags (JSON array contains any of the specified tags)
            for tag in search_request.tags:
                query = query.filter(AuditLogEnhanced.tags.op('JSON_CONTAINS')(f'"{tag}"'))
        
        if search_request.search_term:
            # Search in comment, endpoint, and metadata
            search_pattern = f"%{search_request.search_term}%"
            query = query.filter(
                (AuditLogEnhanced.comment.like(search_pattern)) |
                (AuditLogEnhanced.endpoint.like(search_pattern)) |
                (AuditLogEnhanced.extra_metadata.op('JSON_SEARCH')('one', '$', search_pattern).isnot(None))
            )
        
        if search_request.security_events_only:
            security_actions = [action for action in AuditAction if action.is_security_related]
            query = query.filter(AuditLogEnhanced.action.in_(security_actions))
        
        if search_request.destructive_actions_only:
            destructive_actions = [action for action in AuditAction if action.is_destructive]
            query = query.filter(AuditLogEnhanced.action.in_(destructive_actions))
        
        if search_request.with_errors_only:
            query = query.filter(
                (AuditLogEnhanced.error_code.isnot(None)) | 
                (AuditLogEnhanced.error_message.isnot(None))
            )
        
        # Filter sensitive data based on permissions
        if not search_request.include_sensitive:
            query = query.filter(AuditLogEnhanced.is_sensitive == False)
        
        # Order by creation time (newest first)
        query = query.order_by(AuditLogEnhanced.created_at.desc())
        
        # Apply pagination
        audit_logs = query.offset(offset).limit(limit).all()
        
        return [AuditLogResponse(**log.to_dict(include_sensitive=search_request.include_sensitive)) for log in audit_logs]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search audit logs: {str(e)}"
        )

@router.get("/{audit_log_id}", response_model=AuditLogDetailResponse)
async def get_audit_log(
    audit_log_id: int,
    include_sensitive: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get detailed information about a specific audit log"""
    audit_log = db.query(AuditLogEnhanced).filter(
        AuditLogEnhanced.id == audit_log_id,
        AuditLogEnhanced.org_id == current_user.default_org_id
    ).first()
    
    if not audit_log:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Audit log not found"
        )
    
    # Check permissions for sensitive data
    if audit_log.is_sensitive and not include_sensitive:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access to sensitive audit log data requires additional permissions"
        )
    
    return AuditLogDetailResponse(**audit_log.to_dict(include_sensitive=include_sensitive))

@router.post("/", response_model=AuditLogDetailResponse, status_code=status.HTTP_201_CREATED)
async def create_audit_log(
    audit_data: AuditLogCreateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new audit log entry"""
    try:
        audit_log = AuditLogEnhanced.create_audit_log(
            action=audit_data.action,
            auditable_type=audit_data.auditable_type,
            auditable_id=audit_data.auditable_id,
            user_id=current_user.id,
            org_id=current_user.default_org_id,
            audited_changes=audit_data.audited_changes,
            old_values=audit_data.old_values,
            new_values=audit_data.new_values,
            severity=audit_data.severity,
            comment=audit_data.comment,
            tags=audit_data.tags,
            extra_metadata=audit_data.extra_metadata,
            request_id=audit_data.request_id,
            session_id=audit_data.session_id,
            ip_address=audit_data.ip_address,
            user_agent=audit_data.user_agent,
            endpoint=audit_data.endpoint,
            method=audit_data.method,
            retention_days=audit_data.retention_days
        )
        
        db.add(audit_log)
        db.commit()
        db.refresh(audit_log)
        
        return AuditLogDetailResponse(**audit_log.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create audit log: {str(e)}"
        )

# Analytics and reporting endpoints
@router.get("/stats/summary", response_model=AuditLogStatsResponse)
async def get_audit_stats(
    days: int = Query(30, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get audit log statistics summary"""
    try:
        stats = AuditLogEnhanced.get_audit_statistics(
            org_id=current_user.default_org_id,
            days=days
        )
        return AuditLogStatsResponse(**stats)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get audit stats: {str(e)}"
        )

@router.get("/stats/actions", response_model=Dict[str, int])
async def get_action_stats(
    days: int = Query(30, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get audit log statistics by action type"""
    try:
        since = datetime.now() - timedelta(days=days)
        
        query = db.query(AuditLogEnhanced).filter(
            AuditLogEnhanced.org_id == current_user.default_org_id,
            AuditLogEnhanced.created_at >= since
        )
        
        # Count by action
        action_counts = {}
        for action in AuditAction:
            count = query.filter(AuditLogEnhanced.action == action).count()
            if count > 0:
                action_counts[action.value] = count
        
        return action_counts
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get action stats: {str(e)}"
        )

@router.get("/stats/users", response_model=Dict[str, int])
async def get_user_stats(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(10, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get audit log statistics by user"""
    try:
        since = datetime.now() - timedelta(days=days)
        
        # Get top users by audit log count
        from sqlalchemy import func
        results = db.query(
            AuditLogEnhanced.user_id,
            func.count(AuditLogEnhanced.id).label('count')
        ).filter(
            AuditLogEnhanced.org_id == current_user.default_org_id,
            AuditLogEnhanced.created_at >= since,
            AuditLogEnhanced.user_id.isnot(None)
        ).group_by(
            AuditLogEnhanced.user_id
        ).order_by(
            func.count(AuditLogEnhanced.id).desc()
        ).limit(limit).all()
        
        user_stats = {}
        for user_id, count in results:
            user_stats[f"user_{user_id}"] = count
        
        return user_stats
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user stats: {str(e)}"
        )

@router.get("/security-events", response_model=List[AuditLogResponse])
async def get_security_events(
    hours: int = Query(24, ge=1, le=168),  # Last 24 hours by default, max 1 week
    severity: Optional[AuditSeverity] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get recent security-related audit events"""
    try:
        since = datetime.now() - timedelta(hours=hours)
        
        # Get security-related actions
        security_actions = [action for action in AuditAction if action.is_security_related]
        
        query = db.query(AuditLogEnhanced).filter(
            AuditLogEnhanced.org_id == current_user.default_org_id,
            AuditLogEnhanced.action.in_(security_actions),
            AuditLogEnhanced.created_at >= since
        )
        
        if severity:
            query = query.filter(AuditLogEnhanced.severity == severity)
        
        query = query.order_by(AuditLogEnhanced.created_at.desc())
        audit_logs = query.offset(offset).limit(limit).all()
        
        return [AuditLogResponse(**log.to_dict()) for log in audit_logs]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get security events: {str(e)}"
        )

@router.get("/resource/{auditable_type}/{auditable_id}", response_model=List[AuditLogResponse])
async def get_resource_audit_trail(
    auditable_type: str,
    auditable_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get audit trail for a specific resource"""
    try:
        query = db.query(AuditLogEnhanced).filter(
            AuditLogEnhanced.org_id == current_user.default_org_id,
            AuditLogEnhanced.auditable_type == auditable_type,
            AuditLogEnhanced.auditable_id == auditable_id
        )
        
        query = query.order_by(AuditLogEnhanced.created_at.desc())
        audit_logs = query.offset(offset).limit(limit).all()
        
        return [AuditLogResponse(**log.to_dict()) for log in audit_logs]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get resource audit trail: {str(e)}"
        )

# Export and compliance endpoints
@router.post("/export", response_model=Dict[str, Any])
async def export_audit_logs(
    export_request: AuditLogExportRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Export audit logs for compliance"""
    try:
        query = db.query(AuditLogEnhanced).filter(
            AuditLogEnhanced.org_id == current_user.default_org_id,
            AuditLogEnhanced.is_exported == False
        )
        
        # Apply export filters
        if export_request.actions:
            query = query.filter(AuditLogEnhanced.action.in_(export_request.actions))
        
        if export_request.start_date:
            query = query.filter(AuditLogEnhanced.created_at >= export_request.start_date)
        
        if export_request.end_date:
            query = query.filter(AuditLogEnhanced.created_at <= export_request.end_date)
        
        # Get logs to export
        logs_to_export = query.order_by(AuditLogEnhanced.created_at).limit(export_request.batch_size).all()
        
        if not logs_to_export:
            return {
                "message": "No audit logs available for export",
                "exported_count": 0,
                "batch_id": None
            }
        
        # Generate batch ID
        batch_id = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Mark logs as exported
        exported_data = []
        for log in logs_to_export:
            log.mark_exported_(batch_id)
            exported_data.append(log.to_dict(include_sensitive=True))
        
        db.commit()
        
        return {
            "message": f"Successfully exported {len(logs_to_export)} audit logs",
            "exported_count": len(logs_to_export),
            "batch_id": batch_id,
            "format": export_request.format,
            "data": exported_data if export_request.format == "json" else None
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export audit logs: {str(e)}"
        )

@router.delete("/cleanup-expired", response_model=Dict[str, Any])
async def cleanup_expired_logs(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Clean up expired audit logs"""
    try:
        # Only allow org admins to perform cleanup
        if not current_user.is_admin():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only administrators can perform audit log cleanup"
            )
        
        # Get expired logs for this organization
        expired_logs = db.query(AuditLogEnhanced).filter(
            AuditLogEnhanced.org_id == current_user.default_org_id,
            AuditLogEnhanced.expires_at < datetime.now()
        ).all()
        
        # Count logs to be deleted
        deletable_count = 0
        for log in expired_logs:
            # Only delete non-sensitive or already exported logs
            if not log.is_sensitive or log.is_exported:
                deletable_count += 1
        
        # Perform deletion
        deleted_count = 0
        for log in expired_logs:
            if not log.is_sensitive or log.is_exported:
                db.delete(log)
                deleted_count += 1
        
        db.commit()
        
        return {
            "message": f"Cleaned up {deleted_count} expired audit logs",
            "deleted_count": deleted_count,
            "total_expired": len(expired_logs),
            "skipped_sensitive": len(expired_logs) - deleted_count
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup expired logs: {str(e)}"
        )

@router.patch("/{audit_log_id}/retention", response_model=AuditLogDetailResponse)
async def extend_retention(
    audit_log_id: int,
    days: int = Query(..., ge=1, le=3650),
    reason: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Extend retention period for an audit log"""
    # Only allow org admins to extend retention
    if not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only administrators can extend audit log retention"
        )
    
    audit_log = db.query(AuditLogEnhanced).filter(
        AuditLogEnhanced.id == audit_log_id,
        AuditLogEnhanced.org_id == current_user.default_org_id
    ).first()
    
    if not audit_log:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Audit log not found"
        )
    
    try:
        audit_log.extend_retention_(days, reason)
        db.commit()
        db.refresh(audit_log)
        
        return AuditLogDetailResponse(**audit_log.to_dict(include_sensitive=True))
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to extend retention: {str(e)}"
        )