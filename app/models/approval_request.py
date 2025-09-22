"""
Approval Request Model - Workflow management for resource access and approval processes
"""

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum as PyEnum

from app.database import Base


class ApprovalStatus(PyEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    ESCALATED = "escalated"


class ApprovalType(PyEnum):
    RESOURCE_ACCESS = "resource_access"
    DATA_SOURCE_ACCESS = "data_source_access"
    PROJECT_ACCESS = "project_access"
    MARKETPLACE_ITEM = "marketplace_item"
    ORG_MEMBERSHIP = "org_membership"
    ROLE_ELEVATION = "role_elevation"
    DATA_EXPORT = "data_export"
    SENSITIVE_DATA_ACCESS = "sensitive_data_access"
    CUSTOM = "custom"


class ApprovalPriority(PyEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"
    CRITICAL = "critical"


class ApprovalRequest(Base):
    """Approval request for various system actions"""
    
    __tablename__ = "approval_requests"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    approval_type = Column(String(50), nullable=False, index=True)
    status = Column(String(50), default=ApprovalStatus.PENDING.value, index=True)
    priority = Column(String(20), default=ApprovalPriority.NORMAL.value, index=True)
    
    # Request details
    resource_type = Column(String(100))  # Type of resource being requested
    resource_id = Column(Integer)  # ID of specific resource
    requested_permissions = Column(JSON)  # List of permissions requested
    justification = Column(Text)  # Business justification
    
    # Workflow configuration
    requires_multiple_approvers = Column(Boolean, default=False)
    required_approver_count = Column(Integer, default=1)
    auto_approve_conditions = Column(JSON)  # Conditions for auto-approval
    escalation_rules = Column(JSON)  # Escalation configuration
    
    # Time management
    expires_at = Column(DateTime, index=True)  # When request expires
    deadline = Column(DateTime)  # Business deadline
    estimated_duration = Column(Integer)  # Estimated access duration in hours
    
    # Request context
    request_metadata = Column(JSON, default=dict)  # Additional request context
    business_context = Column(JSON)  # Business context and impact
    compliance_notes = Column(Text)  # Compliance considerations
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    submitted_at = Column(DateTime(timezone=True))
    resolved_at = Column(DateTime(timezone=True))
    
    # Foreign keys
    requester_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    assigned_approver_id = Column(Integer, ForeignKey("users.id"))
    resolved_by_id = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    requester = relationship("User", foreign_keys=[requester_id], back_populates="created_approval_requests")
    assigned_approver = relationship("User", foreign_keys=[assigned_approver_id])
    resolved_by = relationship("User", foreign_keys=[resolved_by_id])
    org = relationship("Org", back_populates="approval_requests")
    
    approval_actions = relationship("ApprovalAction", back_populates="request", cascade="all, delete-orphan")
    approval_comments = relationship("ApprovalComment", back_populates="request", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<ApprovalRequest(id={self.id}, title='{self.title}', status='{self.status}')>"
    
    # Rails-style predicate methods
    def pending_(self) -> bool:
        """Check if request is pending"""
        return self.status == ApprovalStatus.PENDING.value
    
    def approved_(self) -> bool:
        """Check if request is approved"""
        return self.status == ApprovalStatus.APPROVED.value
    
    def rejected_(self) -> bool:
        """Check if request is rejected"""
        return self.status == ApprovalStatus.REJECTED.value
    
    def cancelled_(self) -> bool:
        """Check if request is cancelled"""
        return self.status == ApprovalStatus.CANCELLED.value
    
    def expired_(self) -> bool:
        """Check if request is expired"""
        return (self.status == ApprovalStatus.EXPIRED.value or 
                (self.expires_at and self.expires_at < datetime.now()))
    
    def escalated_(self) -> bool:
        """Check if request is escalated"""
        return self.status == ApprovalStatus.ESCALATED.value
    
    def resolved_(self) -> bool:
        """Check if request is resolved (approved/rejected/cancelled)"""
        return self.status in [ApprovalStatus.APPROVED.value, ApprovalStatus.REJECTED.value, 
                              ApprovalStatus.CANCELLED.value]
    
    def overdue_(self) -> bool:
        """Check if request is overdue"""
        return self.deadline and self.deadline < datetime.now() and not self.resolved_()
    
    def urgent_(self) -> bool:
        """Check if request has urgent priority"""
        return self.priority in [ApprovalPriority.URGENT.value, ApprovalPriority.CRITICAL.value]
    
    def requires_multiple_approvals_(self) -> bool:
        """Check if multiple approvals are required"""
        return self.requires_multiple_approvers and self.required_approver_count > 1
    
    def can_auto_approve_(self) -> bool:
        """Check if request meets auto-approval conditions"""
        if not self.auto_approve_conditions:
            return False
        
        # Implement auto-approval logic based on conditions
        # This would be customized based on business rules
        return False
    
    def should_escalate_(self) -> bool:
        """Check if request should be escalated"""
        if not self.escalation_rules:
            return False
        
        # Check escalation conditions
        rules = self.escalation_rules
        
        # Time-based escalation
        if 'escalate_after_hours' in rules:
            hours_since_created = (datetime.now() - self.created_at).total_seconds() / 3600
            if hours_since_created > rules['escalate_after_hours']:
                return True
        
        # Priority-based escalation
        if 'escalate_urgent' in rules and self.urgent_():
            return True
        
        return False
    
    # Rails helper methods
    def approve_(self, approver_id: int, comments: Optional[str] = None) -> None:
        """Approve the request"""
        if not self.pending_():
            raise ValueError("Can only approve pending requests")
        
        self.status = ApprovalStatus.APPROVED.value
        self.resolved_at = datetime.now()
        self.resolved_by_id = approver_id
        self.updated_at = datetime.now()
        
        # Create approval action record
        action = ApprovalAction(
            request_id=self.id,
            user_id=approver_id,
            action_type="approve",
            comments=comments
        )
        self.approval_actions.append(action)
    
    def reject_(self, approver_id: int, reason: str) -> None:
        """Reject the request"""
        if not self.pending_():
            raise ValueError("Can only reject pending requests")
        
        self.status = ApprovalStatus.REJECTED.value
        self.resolved_at = datetime.now()
        self.resolved_by_id = approver_id
        self.updated_at = datetime.now()
        
        # Create rejection action record
        action = ApprovalAction(
            request_id=self.id,
            user_id=approver_id,
            action_type="reject",
            comments=reason
        )
        self.approval_actions.append(action)
    
    def cancel_(self, user_id: int, reason: Optional[str] = None) -> None:
        """Cancel the request"""
        if self.resolved_():
            raise ValueError("Cannot cancel resolved requests")
        
        self.status = ApprovalStatus.CANCELLED.value
        self.resolved_at = datetime.now()
        self.resolved_by_id = user_id
        self.updated_at = datetime.now()
        
        # Create cancellation action record
        action = ApprovalAction(
            request_id=self.id,
            user_id=user_id,
            action_type="cancel",
            comments=reason
        )
        self.approval_actions.append(action)
    
    def escalate_(self, user_id: int, escalation_reason: str) -> None:
        """Escalate the request"""
        if not self.pending_():
            raise ValueError("Can only escalate pending requests")
        
        self.status = ApprovalStatus.ESCALATED.value
        self.updated_at = datetime.now()
        
        # Create escalation action record
        action = ApprovalAction(
            request_id=self.id,
            user_id=user_id,
            action_type="escalate",
            comments=escalation_reason
        )
        self.approval_actions.append(action)
    
    def extend_deadline_(self, new_deadline: datetime, reason: str) -> None:
        """Extend the request deadline"""
        self.deadline = new_deadline
        self.updated_at = datetime.now()
        
        # Add to metadata
        if not self.request_metadata:
            self.request_metadata = {}
        
        extensions = self.request_metadata.get('deadline_extensions', [])
        extensions.append({
            'new_deadline': new_deadline.isoformat(),
            'reason': reason,
            'extended_at': datetime.now().isoformat()
        })
        self.request_metadata['deadline_extensions'] = extensions
    
    def add_comment_(self, user_id: int, comment: str, is_internal: bool = False) -> None:
        """Add a comment to the request"""
        comment_obj = ApprovalComment(
            request_id=self.id,
            user_id=user_id,
            comment=comment,
            is_internal=is_internal
        )
        self.approval_comments.append(comment_obj)
        self.updated_at = datetime.now()
    
    def get_approval_history(self) -> List[Dict[str, Any]]:
        """Get chronological approval history"""
        history = []
        
        for action in sorted(self.approval_actions, key=lambda x: x.created_at):
            history.append({
                'action': action.action_type,
                'user_id': action.user_id,
                'comments': action.comments,
                'timestamp': action.created_at.isoformat()
            })
        
        return history
    
    def get_time_metrics(self) -> Dict[str, Any]:
        """Get timing metrics for the request"""
        now = datetime.now()
        
        metrics = {
            'age_hours': (now - self.created_at).total_seconds() / 3600,
            'is_overdue': self.overdue_(),
            'is_expired': self.expired_()
        }
        
        if self.deadline:
            metrics['hours_until_deadline'] = (self.deadline - now).total_seconds() / 3600
        
        if self.resolved_at:
            metrics['resolution_time_hours'] = (self.resolved_at - self.created_at).total_seconds() / 3600
        
        return metrics
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        result = {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "approval_type": self.approval_type,
            "status": self.status,
            "priority": self.priority,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "requested_permissions": self.requested_permissions,
            "justification": self.justification,
            "requires_multiple_approvers": self.requires_multiple_approvers,
            "required_approver_count": self.required_approver_count,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "estimated_duration": self.estimated_duration,
            "compliance_notes": self.compliance_notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "requester_id": self.requester_id,
            "org_id": self.org_id,
            "assigned_approver_id": self.assigned_approver_id,
            "resolved_by_id": self.resolved_by_id,
            "time_metrics": self.get_time_metrics()
        }
        
        if include_sensitive:
            result.update({
                "metadata": self.request_metadata,
                "business_context": self.business_context,
                "auto_approve_conditions": self.auto_approve_conditions,
                "escalation_rules": self.escalation_rules,
                "approval_history": self.get_approval_history()
            })
        
        return result


class ApprovalAction(Base):
    """Individual actions taken on approval requests"""
    
    __tablename__ = "approval_actions"
    
    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(Integer, ForeignKey("approval_requests.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    action_type = Column(String(50), nullable=False)  # approve, reject, cancel, escalate, comment
    comments = Column(Text)
    action_metadata = Column(JSON)  # Additional action-specific data
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Relationships
    request = relationship("ApprovalRequest", back_populates="approval_actions")
    user = relationship("User")
    
    def __repr__(self):
        return f"<ApprovalAction(id={self.id}, request_id={self.request_id}, action='{self.action_type}')>"


class ApprovalComment(Base):
    """Comments on approval requests"""
    
    __tablename__ = "approval_comments"
    
    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(Integer, ForeignKey("approval_requests.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    comment = Column(Text, nullable=False)
    is_internal = Column(Boolean, default=False)  # Internal comments not visible to requester
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    request = relationship("ApprovalRequest", back_populates="approval_comments")
    user = relationship("User")
    
    def __repr__(self):
        return f"<ApprovalComment(id={self.id}, request_id={self.request_id})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "request_id": self.request_id,
            "user_id": self.user_id,
            "comment": self.comment,
            "is_internal": self.is_internal,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }