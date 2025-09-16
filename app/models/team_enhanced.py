from datetime import datetime, timedelta
from enum import Enum as PyEnum
import json
from typing import Dict, List, Optional, Any, Union, Set
import uuid

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class TeamType(PyEnum):
    GENERAL = "GENERAL"
    PROJECT = "PROJECT"
    DEPARTMENT = "DEPARTMENT"
    CROSS_FUNCTIONAL = "CROSS_FUNCTIONAL"
    TEMPORARY = "TEMPORARY"
    EXTERNAL = "EXTERNAL"
    VENDOR = "VENDOR"
    
    @property
    def display_name(self) -> str:
        return {
            self.GENERAL: "General Team",
            self.PROJECT: "Project Team",
            self.DEPARTMENT: "Department Team",
            self.CROSS_FUNCTIONAL: "Cross-Functional Team",
            self.TEMPORARY: "Temporary Team",
            self.EXTERNAL: "External Team",
            self.VENDOR: "Vendor Team"
        }.get(self, self.value)
    
    @property
    def is_permanent(self) -> bool:
        return self not in [self.TEMPORARY, self.PROJECT]


class TeamStatus(PyEnum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ARCHIVED = "ARCHIVED"
    SUSPENDED = "SUSPENDED"
    DISBANDED = "DISBANDED"
    PENDING = "PENDING"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.ARCHIVED: "Archived",
            self.SUSPENDED: "Suspended",
            self.DISBANDED: "Disbanded",
            self.PENDING: "Pending"
        }.get(self, self.value)
    
    @property
    def is_operational(self) -> bool:
        return self in [self.ACTIVE, self.PENDING]


class TeamVisibility(PyEnum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    SECRET = "SECRET"
    INTERNAL = "INTERNAL"
    
    @property
    def display_name(self) -> str:
        return {
            self.PUBLIC: "Public",
            self.PRIVATE: "Private", 
            self.SECRET: "Secret",
            self.INTERNAL: "Internal Only"
        }.get(self, self.value)


class TeamJoinPolicy(PyEnum):
    OPEN = "OPEN"
    REQUEST_APPROVAL = "REQUEST_APPROVAL"
    INVITE_ONLY = "INVITE_ONLY"
    CLOSED = "CLOSED"
    
    @property
    def display_name(self) -> str:
        return {
            self.OPEN: "Open - Anyone Can Join",
            self.REQUEST_APPROVAL: "Request Approval Required",
            self.INVITE_ONLY: "Invite Only",
            self.CLOSED: "Closed - No New Members"
        }.get(self, self.value)


class TeamEnhanced(Base):
    __tablename__ = 'teams_enhanced'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    parent_team_id = Column(Integer, ForeignKey('teams_enhanced.id'), nullable=True)
    
    name = Column(String(255), nullable=False)
    slug = Column(String(100), nullable=False)
    description = Column(Text)
    
    team_type = Column(SQLEnum(TeamType), nullable=False, default=TeamType.GENERAL)
    status = Column(SQLEnum(TeamStatus), nullable=False, default=TeamStatus.ACTIVE)
    visibility = Column(SQLEnum(TeamVisibility), nullable=False, default=TeamVisibility.PRIVATE)
    join_policy = Column(SQLEnum(TeamJoinPolicy), nullable=False, default=TeamJoinPolicy.REQUEST_APPROVAL)
    
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    
    max_members = Column(Integer, default=100)
    member_count = Column(Integer, default=0, nullable=False)
    active_member_count = Column(Integer, default=0, nullable=False)
    
    avatar_url = Column(String(512))
    banner_url = Column(String(512))
    
    tags = Column(JSON, default=list)
    skills_required = Column(JSON, default=list)
    tools_used = Column(JSON, default=list)
    
    location = Column(String(255))
    timezone = Column(String(100))
    
    active = Column(Boolean, default=True, nullable=False)
    archived = Column(Boolean, default=False, nullable=False)
    
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)
    
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    
    budget_allocated = Column(Float)
    budget_spent = Column(Float, default=0.0)
    
    performance_score = Column(Float, default=0.0)
    collaboration_score = Column(Float, default=0.0)
    productivity_score = Column(Float, default=0.0)
    
    last_activity_at = Column(DateTime)
    
    settings = Column(JSON, default=dict)
    extra_metadata = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    archived_at = Column(DateTime)
    disbanded_at = Column(DateTime)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    org = relationship("Org", back_populates="teams_enhanced")
    owner = relationship("User", foreign_keys=[owner_id])
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    project = relationship("Project", back_populates="teams_enhanced")
    
    parent_team = relationship("TeamEnhanced", remote_side=[id], backref="sub_teams")
    memberships = relationship("TeamMembershipEnhanced", back_populates="team", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_team_enhanced_org_id', 'org_id'),
        Index('idx_team_enhanced_status', 'status'),
        Index('idx_team_enhanced_visibility', 'visibility'),
        Index('idx_team_enhanced_type', 'team_type'),
        Index('idx_team_enhanced_owner_id', 'owner_id'),
        Index('idx_team_enhanced_project_id', 'project_id'),
        Index('idx_team_enhanced_parent_team_id', 'parent_team_id'),
        Index('idx_team_enhanced_active', 'active'),
        Index('idx_team_enhanced_archived', 'archived'),
        Index('idx_team_enhanced_last_activity', 'last_activity_at'),
        Index('idx_team_enhanced_performance', 'performance_score'),
        UniqueConstraint('org_id', 'slug', name='uq_team_enhanced_org_slug'),
        CheckConstraint('max_members > 0', name='ck_team_enhanced_max_members_positive'),
        CheckConstraint('member_count >= 0', name='ck_team_enhanced_member_count_non_negative'),
        CheckConstraint('active_member_count >= 0', name='ck_team_enhanced_active_member_count_non_negative'),
        CheckConstraint('active_member_count <= member_count', name='ck_team_enhanced_active_member_count_valid'),
        CheckConstraint('budget_spent >= 0', name='ck_team_enhanced_budget_spent_non_negative'),
        CheckConstraint('performance_score >= 0 AND performance_score <= 10', name='ck_team_enhanced_performance_score_range'),
    )
    
    HIGH_PERFORMANCE_THRESHOLD = 8.0
    LOW_PERFORMANCE_THRESHOLD = 5.0
    MAX_INACTIVE_DAYS = 30
    OVER_CAPACITY_THRESHOLD = 0.9
    
    def __repr__(self):
        return f"<TeamEnhanced(id={self.id}, name='{self.name}', org_id={self.org_id}, status='{self.status.value}')>"
    
    def active_(self) -> bool:
        """Check if team is active (Rails pattern)"""
        return (self.active and 
                self.status == TeamStatus.ACTIVE and
                not self.archived and
                not self.disbanded_())
    
    def operational_(self) -> bool:
        """Check if team is operational (Rails pattern)"""
        return self.status.is_operational and not self.archived
    
    def archived_(self) -> bool:
        """Check if team is archived (Rails pattern)"""
        return self.archived or self.status == TeamStatus.ARCHIVED
    
    def disbanded_(self) -> bool:
        """Check if team is disbanded (Rails pattern)"""
        return self.status == TeamStatus.DISBANDED
    
    def suspended_(self) -> bool:
        """Check if team is suspended (Rails pattern)"""
        return self.status == TeamStatus.SUSPENDED
    
    def pending_(self) -> bool:
        """Check if team is pending activation (Rails pattern)"""
        return self.status == TeamStatus.PENDING
    
    def public_(self) -> bool:
        """Check if team is public (Rails pattern)"""
        return self.visibility == TeamVisibility.PUBLIC
    
    def private_(self) -> bool:
        """Check if team is private (Rails pattern)"""
        return self.visibility == TeamVisibility.PRIVATE
    
    def secret_(self) -> bool:
        """Check if team is secret (Rails pattern)"""
        return self.visibility == TeamVisibility.SECRET
    
    def temporary_(self) -> bool:
        """Check if team is temporary (Rails pattern)"""
        return self.team_type == TeamType.TEMPORARY
    
    def project_team_(self) -> bool:
        """Check if team is project-based (Rails pattern)"""
        return self.team_type == TeamType.PROJECT and self.project_id is not None
    
    def has_sub_teams_(self) -> bool:
        """Check if team has sub-teams (Rails pattern)"""
        return len(self.sub_teams) > 0
    
    def is_sub_team_(self) -> bool:
        """Check if team is a sub-team (Rails pattern)"""
        return self.parent_team_id is not None
    
    def full_(self) -> bool:
        """Check if team is at capacity (Rails pattern)"""
        return self.member_count >= self.max_members
    
    def near_capacity_(self) -> bool:
        """Check if team is near capacity (Rails pattern)"""
        return self.member_count >= (self.max_members * self.OVER_CAPACITY_THRESHOLD)
    
    def high_performing_(self) -> bool:
        """Check if team is high performing (Rails pattern)"""
        return self.performance_score >= self.HIGH_PERFORMANCE_THRESHOLD
    
    def low_performing_(self) -> bool:
        """Check if team is low performing (Rails pattern)"""
        return self.performance_score <= self.LOW_PERFORMANCE_THRESHOLD
    
    def inactive_(self) -> bool:
        """Check if team has been inactive (Rails pattern)"""
        if not self.last_activity_at:
            return True
        cutoff = datetime.now() - timedelta(days=self.MAX_INACTIVE_DAYS)
        return self.last_activity_at < cutoff
    
    def over_budget_(self) -> bool:
        """Check if team is over budget (Rails pattern)"""
        return (self.budget_allocated and 
                self.budget_spent and
                self.budget_spent > self.budget_allocated)
    
    def join_request_open_(self) -> bool:
        """Check if team accepts join requests (Rails pattern)"""
        return self.join_policy in [TeamJoinPolicy.OPEN, TeamJoinPolicy.REQUEST_APPROVAL]
    
    def invite_only_(self) -> bool:
        """Check if team is invite only (Rails pattern)"""
        return self.join_policy == TeamJoinPolicy.INVITE_ONLY
    
    def closed_(self) -> bool:
        """Check if team is closed to new members (Rails pattern)"""
        return self.join_policy == TeamJoinPolicy.CLOSED
    
    def expired_(self) -> bool:
        """Check if team has expired (Rails pattern)"""
        return self.end_date and self.end_date < datetime.now()
    
    def needs_attention_(self) -> bool:
        """Check if team needs attention (Rails pattern)"""
        return (self.low_performing_() or 
                self.inactive_() or
                self.over_budget_() or
                self.suspended_() or
                self.expired_())
    
    def activate_(self) -> None:
        """Activate team (Rails bang method pattern)"""
        self.active = True
        self.status = TeamStatus.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate team (Rails bang method pattern)"""
        self.active = False
        self.status = TeamStatus.INACTIVE
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deactivation_reason'] = reason
    
    def archive_(self, reason: str = None) -> None:
        """Archive team (Rails bang method pattern)"""
        self.archived = True
        self.archived_at = datetime.now()
        self.status = TeamStatus.ARCHIVED
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['archive_reason'] = reason
    
    def unarchive_(self) -> None:
        """Unarchive team (Rails bang method pattern)"""
        self.archived = False
        self.archived_at = None
        self.status = TeamStatus.ACTIVE
        self.active = True
        self.updated_at = datetime.now()
    
    def suspend_(self, reason: str) -> None:
        """Suspend team (Rails bang method pattern)"""
        self.status = TeamStatus.SUSPENDED
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['suspension_reason'] = reason
        self.extra_metadata['suspended_at'] = datetime.now().isoformat()
    
    def unsuspend_(self) -> None:
        """Unsuspend team (Rails bang method pattern)"""
        if self.status == TeamStatus.SUSPENDED:
            self.status = TeamStatus.ACTIVE
            self.updated_at = datetime.now()
    
    def disband_(self, reason: str = None) -> None:
        """Disband team permanently (Rails bang method pattern)"""
        self.status = TeamStatus.DISBANDED
        self.disbanded_at = datetime.now()
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['disband_reason'] = reason
        
        for membership in self.memberships:
            membership.deactivate_("Team disbanded")
    
    def change_visibility_(self, visibility: TeamVisibility) -> None:
        """Change team visibility (Rails bang method pattern)"""
        self.visibility = visibility
        self.updated_at = datetime.now()
    
    def change_join_policy_(self, policy: TeamJoinPolicy) -> None:
        """Change join policy (Rails bang method pattern)"""
        self.join_policy = policy
        self.updated_at = datetime.now()
    
    def update_member_counts_(self) -> None:
        """Update member counts (Rails bang method pattern)"""
        active_memberships = [m for m in self.memberships if m.active_()]
        self.member_count = len(self.memberships)
        self.active_member_count = len(active_memberships)
        self.updated_at = datetime.now()
    
    def add_skill_(self, skill: str) -> None:
        """Add required skill (Rails bang method pattern)"""
        skills = list(self.skills_required or [])
        if skill not in skills:
            skills.append(skill)
            self.skills_required = skills
            self.updated_at = datetime.now()
    
    def remove_skill_(self, skill: str) -> None:
        """Remove required skill (Rails bang method pattern)"""
        skills = list(self.skills_required or [])
        if skill in skills:
            skills.remove(skill)
            self.skills_required = skills
            self.updated_at = datetime.now()
    
    def add_tool_(self, tool: str) -> None:
        """Add tool used (Rails bang method pattern)"""
        tools = list(self.tools_used or [])
        if tool not in tools:
            tools.append(tool)
            self.tools_used = tools
            self.updated_at = datetime.now()
    
    def remove_tool_(self, tool: str) -> None:
        """Remove tool used (Rails bang method pattern)"""
        tools = list(self.tools_used or [])
        if tool in tools:
            tools.remove(tool)
            self.tools_used = tools
            self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def increase_budget_(self, amount: float) -> None:
        """Increase budget allocation (Rails bang method pattern)"""
        self.budget_allocated = (self.budget_allocated or 0) + amount
        self.updated_at = datetime.now()
    
    def spend_budget_(self, amount: float, description: str = None) -> None:
        """Record budget spending (Rails bang method pattern)"""
        self.budget_spent = (self.budget_spent or 0) + amount
        self.updated_at = datetime.now()
        
        if description:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata.setdefault('budget_spending', []).append({
                'amount': amount,
                'description': description,
                'timestamp': datetime.now().isoformat()
            })
    
    def update_performance_score_(self, score: float) -> None:
        """Update performance score (Rails bang method pattern)"""
        if 0 <= score <= 10:
            self.performance_score = score
            self.updated_at = datetime.now()
    
    def record_activity_(self, activity_type: str = None) -> None:
        """Record team activity (Rails bang method pattern)"""
        self.last_activity_at = datetime.now()
        self.updated_at = datetime.now()
        
        if activity_type:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata.setdefault('recent_activities', []).append({
                'type': activity_type,
                'timestamp': datetime.now().isoformat()
            })
            
            activities = self.extra_metadata['recent_activities']
            if len(activities) > 50:
                self.extra_metadata['recent_activities'] = activities[-50:]
    
    def calculate_collaboration_score(self) -> float:
        """Calculate collaboration score based on member interactions (Rails pattern)"""
        if not self.active_member_count or self.active_member_count < 2:
            return 0.0
        
        active_ratio = self.active_member_count / max(self.member_count, 1)
        activity_factor = 1.0 if self.last_activity_at and (datetime.now() - self.last_activity_at).days < 7 else 0.5
        
        return min(10.0, active_ratio * activity_factor * 10)
    
    def calculate_productivity_score(self) -> float:
        """Calculate productivity score based on team metrics (Rails pattern)"""
        base_score = self.performance_score or 0
        
        if self.project_team_() and self.project:
            base_score += 2.0 if self.project.completed_() else 1.0
        
        if not self.inactive_():
            base_score += 1.0
        
        if not self.over_budget_():
            base_score += 1.0
        
        return min(10.0, base_score)
    
    def update_all_scores_(self) -> None:
        """Update all performance scores (Rails bang method pattern)"""
        self.collaboration_score = self.calculate_collaboration_score()
        self.productivity_score = self.calculate_productivity_score()
        self.updated_at = datetime.now()
    
    def get_members(self, status: str = 'active') -> List['TeamMembershipEnhanced']:
        """Get team members by status (Rails pattern)"""
        if status == 'active':
            return [m for m in self.memberships if m.active_()]
        elif status == 'all':
            return list(self.memberships)
        elif status == 'inactive':
            return [m for m in self.memberships if not m.active_()]
        else:
            return []
    
    def get_owners(self) -> List['TeamMembershipEnhanced']:
        """Get team owners (Rails pattern)"""
        return [m for m in self.memberships if m.is_owner_()]
    
    def get_admins(self) -> List['TeamMembershipEnhanced']:
        """Get team admins (Rails pattern)"""
        return [m for m in self.memberships if m.is_admin_()]
    
    def member_skills(self) -> Set[str]:
        """Get all member skills (Rails pattern)"""
        skills = set()
        for membership in self.get_members('active'):
            if membership.user and hasattr(membership.user, 'skills'):
                skills.update(membership.user.skills or [])
        return skills
    
    def skill_coverage(self) -> Dict[str, float]:
        """Calculate skill coverage percentage (Rails pattern)"""
        required_skills = set(self.skills_required or [])
        member_skills = self.member_skills()
        
        if not required_skills:
            return {}
        
        coverage = {}
        for skill in required_skills:
            members_with_skill = sum(1 for m in self.get_members('active') 
                                   if m.user and hasattr(m.user, 'skills') and 
                                   skill in (m.user.skills or []))
            coverage[skill] = members_with_skill / max(self.active_member_count, 1)
        
        return coverage
    
    def budget_remaining(self) -> Optional[float]:
        """Calculate remaining budget (Rails pattern)"""
        if self.budget_allocated is None:
            return None
        return self.budget_allocated - (self.budget_spent or 0)
    
    def budget_utilization(self) -> Optional[float]:
        """Calculate budget utilization percentage (Rails pattern)"""
        if not self.budget_allocated or self.budget_allocated == 0:
            return None
        return (self.budget_spent or 0) / self.budget_allocated
    
    def days_since_activity(self) -> int:
        """Calculate days since last activity (Rails pattern)"""
        if not self.last_activity_at:
            return (datetime.now() - self.created_at).days
        return (datetime.now() - self.last_activity_at).days
    
    def hierarchy_path(self) -> List[str]:
        """Get team hierarchy path (Rails pattern)"""
        path = [self.name]
        current = self.parent_team
        while current:
            path.insert(0, current.name)
            current = current.parent_team
        return path
    
    def all_sub_teams(self) -> List['TeamEnhanced']:
        """Get all sub-teams recursively (Rails pattern)"""
        all_subs = []
        for sub_team in self.sub_teams:
            all_subs.append(sub_team)
            all_subs.extend(sub_team.all_sub_teams())
        return all_subs
    
    def health_report(self) -> Dict[str, Any]:
        """Generate team health report (Rails pattern)"""
        return {
            'team_id': self.team_id,
            'name': self.name,
            'active': self.active_(),
            'status': self.status.value,
            'member_count': self.member_count,
            'active_member_count': self.active_member_count,
            'capacity_utilization': self.member_count / self.max_members,
            'performance_score': self.performance_score,
            'collaboration_score': self.collaboration_score,
            'productivity_score': self.productivity_score,
            'high_performing': self.high_performing_(),
            'inactive': self.inactive_(),
            'over_budget': self.over_budget_(),
            'needs_attention': self.needs_attention_(),
            'days_since_activity': self.days_since_activity(),
            'skill_coverage': self.skill_coverage(),
            'budget_utilization': self.budget_utilization()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'team_id': self.team_id,
            'name': self.name,
            'slug': self.slug,
            'description': self.description,
            'team_type': self.team_type.value,
            'status': self.status.value,
            'visibility': self.visibility.value,
            'join_policy': self.join_policy.value,
            'member_count': self.member_count,
            'active_member_count': self.active_member_count,
            'max_members': self.max_members,
            'performance_score': self.performance_score,
            'tags': self.tags,
            'skills_required': self.skills_required,
            'tools_used': self.tools_used,
            'active': self.active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'budget_allocated': self.budget_allocated,
                'budget_spent': self.budget_spent,
                'settings': self.settings,
                'metadata': self.extra_metadata
            })
        
        return result