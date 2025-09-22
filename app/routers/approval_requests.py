"""
Approval Requests Router - API endpoints for approval workflow management
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.approval_request import ApprovalRequest, ApprovalStatus, ApprovalType, ApprovalPriority, ApprovalAction, ApprovalComment

router = APIRouter()

# Pydantic models
class ApprovalRequestCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    approval_type: ApprovalType
    priority: ApprovalPriority = ApprovalPriority.NORMAL
    resource_type: Optional[str] = None
    resource_id: Optional[int] = None
    requested_permissions: Optional[List[str]] = None
    justification: Optional[str] = None
    requires_multiple_approvers: bool = False
    required_approver_count: int = 1
    auto_approve_conditions: Optional[Dict[str, Any]] = None
    escalation_rules: Optional[Dict[str, Any]] = None
    expires_at: Optional[datetime] = None
    deadline: Optional[datetime] = None
    estimated_duration: Optional[int] = None
    business_context: Optional[Dict[str, Any]] = None
    compliance_notes: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    assigned_approver_id: Optional[int] = None

class ApprovalRequestUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    priority: Optional[ApprovalPriority] = None
    justification: Optional[str] = None
    deadline: Optional[datetime] = None
    estimated_duration: Optional[int] = None
    compliance_notes: Optional[str] = None
    assigned_approver_id: Optional[int] = None

class ApprovalActionRequest(BaseModel):
    action: str = Field(..., pattern="^(approve|reject|escalate)$")
    comments: Optional[str] = None
    reason: Optional[str] = None

class CommentRequest(BaseModel):
    comment: str = Field(..., min_length=1)
    is_internal: bool = False

class ApprovalRequestResponse(BaseModel):
    id: int
    title: str
    description: Optional[str]
    approval_type: str
    status: str
    priority: str
    resource_type: Optional[str]
    resource_id: Optional[int]
    requested_permissions: Optional[List[str]]
    justification: Optional[str]
    requires_multiple_approvers: bool
    required_approver_count: int
    expires_at: Optional[str]
    deadline: Optional[str]
    estimated_duration: Optional[int]
    compliance_notes: Optional[str]
    created_at: str
    updated_at: Optional[str]
    submitted_at: Optional[str]
    resolved_at: Optional[str]
    requester_id: int
    org_id: int
    assigned_approver_id: Optional[int]
    resolved_by_id: Optional[int]
    time_metrics: Dict[str, Any]

class ApprovalActionResponse(BaseModel):
    id: int
    request_id: int
    user_id: int
    action_type: str
    comments: Optional[str]
    created_at: str

class ApprovalCommentResponse(BaseModel):
    id: int
    request_id: int
    user_id: int
    comment: str
    is_internal: bool
    created_at: str
    updated_at: Optional[str]

# Approval request management endpoints
@router.post("/", response_model=ApprovalRequestResponse, status_code=status.HTTP_201_CREATED)
async def create_approval_request(
    request_data: ApprovalRequestCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new approval request"""
    try:
        # Validate assigned approver if specified
        if request_data.assigned_approver_id:
            approver = db.query(User).filter(
                User.id == request_data.assigned_approver_id,
                User.default_org_id == current_user.default_org_id
            ).first()
            
            if not approver:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Assigned approver not found"
                )
        
        approval_request = ApprovalRequest(
            title=request_data.title,
            description=request_data.description,
            approval_type=request_data.approval_type.value,
            priority=request_data.priority.value,
            resource_type=request_data.resource_type,
            resource_id=request_data.resource_id,
            requested_permissions=request_data.requested_permissions,
            justification=request_data.justification,
            requires_multiple_approvers=request_data.requires_multiple_approvers,
            required_approver_count=request_data.required_approver_count,
            auto_approve_conditions=request_data.auto_approve_conditions,
            escalation_rules=request_data.escalation_rules,
            expires_at=request_data.expires_at,
            deadline=request_data.deadline,
            estimated_duration=request_data.estimated_duration,
            business_context=request_data.business_context,
            compliance_notes=request_data.compliance_notes,
            metadata=request_data.metadata,
            requester_id=current_user.id,
            org_id=current_user.default_org_id,
            assigned_approver_id=request_data.assigned_approver_id,
            submitted_at=datetime.now()
        )
        
        # Check for auto-approval
        if approval_request.can_auto_approve_():
            approval_request.approve_(current_user.id, "Auto-approved based on conditions")
        
        db.add(approval_request)
        db.commit()
        db.refresh(approval_request)
        
        return ApprovalRequestResponse(**approval_request.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create approval request: {str(e)}"
        )

@router.get("/", response_model=List[ApprovalRequestResponse])
async def list_approval_requests(
    status_filter: Optional[ApprovalStatus] = Query(None, alias="status"),
    approval_type: Optional[ApprovalType] = Query(None),
    priority: Optional[ApprovalPriority] = Query(None),
    assigned_to_me: bool = Query(False),
    requested_by_me: bool = Query(False),
    overdue_only: bool = Query(False),
    urgent_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List approval requests with filtering options"""
    try:
        query = db.query(ApprovalRequest).filter(
            ApprovalRequest.org_id == current_user.default_org_id
        )
        
        # Apply filters
        if status_filter:
            query = query.filter(ApprovalRequest.status == status_filter.value)
        
        if approval_type:
            query = query.filter(ApprovalRequest.approval_type == approval_type.value)
        
        if priority:
            query = query.filter(ApprovalRequest.priority == priority.value)
        
        if assigned_to_me:
            query = query.filter(ApprovalRequest.assigned_approver_id == current_user.id)
        
        if requested_by_me:
            query = query.filter(ApprovalRequest.requester_id == current_user.id)
        
        if overdue_only:
            query = query.filter(
                ApprovalRequest.deadline < datetime.now(),
                ApprovalRequest.status == ApprovalStatus.PENDING.value
            )
        
        if urgent_only:
            query = query.filter(
                ApprovalRequest.priority.in_([
                    ApprovalPriority.URGENT.value,
                    ApprovalPriority.CRITICAL.value
                ])
            )
        
        # Order by priority and creation time
        query = query.order_by(
            ApprovalRequest.priority.desc(),
            ApprovalRequest.created_at.desc()
        )
        
        # Apply pagination
        requests = query.offset(offset).limit(limit).all()
        
        return [ApprovalRequestResponse(**req.to_dict()) for req in requests]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list approval requests: {str(e)}"
        )

@router.get("/{request_id}", response_model=ApprovalRequestResponse)
async def get_approval_request(
    request_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get approval request details"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    return ApprovalRequestResponse(**request.to_dict(include_sensitive=True))

@router.put("/{request_id}", response_model=ApprovalRequestResponse)
async def update_approval_request(
    request_id: int,
    request_data: ApprovalRequestUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update approval request"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    # Only requester can update pending requests
    if request.requester_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only the requester can update this request"
        )
    
    if not request.pending_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot update resolved requests"
        )
    
    try:
        # Update fields
        update_data = request_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            if hasattr(request, field):
                if field == 'priority' and hasattr(value, 'value'):
                    setattr(request, field, value.value)
                else:
                    setattr(request, field, value)
        
        request.updated_at = datetime.now()
        
        db.commit()
        db.refresh(request)
        
        return ApprovalRequestResponse(**request.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update approval request: {str(e)}"
        )

@router.delete("/{request_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_approval_request(
    request_id: int,
    reason: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Cancel approval request"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    # Only requester can cancel their requests
    if request.requester_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only the requester can cancel this request"
        )
    
    if request.resolved_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot cancel resolved requests"
        )
    
    try:
        request.cancel_(current_user.id, reason)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel approval request: {str(e)}"
        )

# Approval actions endpoints
@router.post("/{request_id}/actions", response_model=ApprovalRequestResponse)
async def take_approval_action(
    request_id: int,
    action_data: ApprovalActionRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Take approval action (approve/reject/escalate)"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    if not request.pending_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Can only act on pending requests"
        )
    
    # Check if user can approve this request
    if (request.assigned_approver_id and 
        request.assigned_approver_id != current_user.id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to approve this request"
        )
    
    try:
        if action_data.action == "approve":
            request.approve_(current_user.id, action_data.comments)
        elif action_data.action == "reject":
            if not action_data.reason:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Reason is required for rejection"
                )
            request.reject_(current_user.id, action_data.reason)
        elif action_data.action == "escalate":
            escalation_reason = action_data.reason or "Escalated for further review"
            request.escalate_(current_user.id, escalation_reason)
        
        db.commit()
        db.refresh(request)
        
        return ApprovalRequestResponse(**request.to_dict(include_sensitive=True))
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process approval action: {str(e)}"
        )

@router.get("/{request_id}/actions", response_model=List[ApprovalActionResponse])
async def get_approval_history(
    request_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get approval history for a request"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    try:
        actions = db.query(ApprovalAction).filter(
            ApprovalAction.request_id == request_id
        ).order_by(ApprovalAction.created_at).all()
        
        return [
            ApprovalActionResponse(
                id=action.id,
                request_id=action.request_id,
                user_id=action.user_id,
                action_type=action.action_type,
                comments=action.comments,
                created_at=action.created_at.isoformat()
            )
            for action in actions
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get approval history: {str(e)}"
        )

# Comments endpoints
@router.post("/{request_id}/comments", response_model=ApprovalCommentResponse, status_code=status.HTTP_201_CREATED)
async def add_comment(
    request_id: int,
    comment_data: CommentRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Add comment to approval request"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    try:
        request.add_comment_(
            current_user.id,
            comment_data.comment,
            comment_data.is_internal
        )
        
        db.commit()
        
        # Get the newly created comment
        comment = db.query(ApprovalComment).filter(
            ApprovalComment.request_id == request_id,
            ApprovalComment.user_id == current_user.id
        ).order_by(ApprovalComment.created_at.desc()).first()
        
        return ApprovalCommentResponse(**comment.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add comment: {str(e)}"
        )

@router.get("/{request_id}/comments", response_model=List[ApprovalCommentResponse])
async def get_comments(
    request_id: int,
    include_internal: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get comments for approval request"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    try:
        query = db.query(ApprovalComment).filter(
            ApprovalComment.request_id == request_id
        )
        
        # Filter internal comments based on permissions
        if not include_internal or request.requester_id == current_user.id:
            query = query.filter(ApprovalComment.is_internal == False)
        
        comments = query.order_by(ApprovalComment.created_at).all()
        
        return [ApprovalCommentResponse(**comment.to_dict()) for comment in comments]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get comments: {str(e)}"
        )

# Analytics and reporting endpoints
@router.get("/analytics/summary", response_model=Dict[str, Any])
async def get_approval_analytics(
    days: int = Query(30, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get approval request analytics"""
    try:
        since = datetime.now() - timedelta(days=days)
        
        base_query = db.query(ApprovalRequest).filter(
            ApprovalRequest.org_id == current_user.default_org_id,
            ApprovalRequest.created_at >= since
        )
        
        analytics = {
            "total_requests": base_query.count(),
            "pending_requests": base_query.filter(
                ApprovalRequest.status == ApprovalStatus.PENDING.value
            ).count(),
            "approved_requests": base_query.filter(
                ApprovalRequest.status == ApprovalStatus.APPROVED.value
            ).count(),
            "rejected_requests": base_query.filter(
                ApprovalRequest.status == ApprovalStatus.REJECTED.value
            ).count(),
            "overdue_requests": base_query.filter(
                ApprovalRequest.deadline < datetime.now(),
                ApprovalRequest.status == ApprovalStatus.PENDING.value
            ).count(),
            "urgent_requests": base_query.filter(
                ApprovalRequest.priority.in_([
                    ApprovalPriority.URGENT.value,
                    ApprovalPriority.CRITICAL.value
                ])
            ).count()
        }
        
        # Calculate approval rate
        resolved_requests = analytics["approved_requests"] + analytics["rejected_requests"]
        if resolved_requests > 0:
            analytics["approval_rate"] = (analytics["approved_requests"] / resolved_requests) * 100
        else:
            analytics["approval_rate"] = 0
        
        # Get average resolution time
        resolved = base_query.filter(
            ApprovalRequest.resolved_at.isnot(None)
        ).all()
        
        if resolved:
            total_resolution_time = sum(
                (req.resolved_at - req.created_at).total_seconds()
                for req in resolved
            )
            analytics["avg_resolution_time_hours"] = (total_resolution_time / len(resolved)) / 3600
        else:
            analytics["avg_resolution_time_hours"] = 0
        
        return analytics
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get approval analytics: {str(e)}"
        )

@router.patch("/{request_id}/deadline", response_model=ApprovalRequestResponse)
async def extend_deadline(
    request_id: int,
    new_deadline: datetime,
    reason: str = Query(..., min_length=1),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Extend approval request deadline"""
    request = db.query(ApprovalRequest).filter(
        ApprovalRequest.id == request_id,
        ApprovalRequest.org_id == current_user.default_org_id
    ).first()
    
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Approval request not found"
        )
    
    # Only requester or assigned approver can extend deadline
    if (request.requester_id != current_user.id and 
        request.assigned_approver_id != current_user.id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to extend deadline"
        )
    
    if request.resolved_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot extend deadline for resolved requests"
        )
    
    try:
        request.extend_deadline_(new_deadline, reason)
        db.commit()
        db.refresh(request)
        
        return ApprovalRequestResponse(**request.to_dict(include_sensitive=True))
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to extend deadline: {str(e)}"
        )