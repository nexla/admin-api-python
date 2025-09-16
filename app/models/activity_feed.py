"""
ActivityFeed Model - User activity streams and social features.
Tracks user activities across the platform for activity feeds, notifications, and social interactions.
Implements Rails activity stream patterns for comprehensive user engagement tracking.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Index
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum as PyEnum
import json
import logging
from ..database import Base

logger = logging.getLogger(__name__)

class ActivityType(PyEnum):
    """Activity type enumeration"""
    # Content activities
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    PUBLISHED = "published"
    ARCHIVED = "archived"
    
    # Collaboration activities
    SHARED = "shared"
    COMMENTED = "commented"
    MENTIONED = "mentioned"
    INVITED = "invited"
    JOINED = "joined"
    LEFT = "left"
    
    # Data activities
    UPLOADED = "uploaded"
    DOWNLOADED = "downloaded"
    PROCESSED = "processed"
    EXPORTED = "exported"
    IMPORTED = "imported"
    
    # System activities
    LOGGED_IN = "logged_in"
    LOGGED_OUT = "logged_out"
    PASSWORD_CHANGED = "password_changed"
    PROFILE_UPDATED = "profile_updated"
    
    # Project activities
    PROJECT_CREATED = "project_created"
    PROJECT_SHARED = "project_shared"
    PROJECT_COMPLETED = "project_completed"
    
    # Flow activities
    FLOW_STARTED = "flow_started"
    FLOW_COMPLETED = "flow_completed"
    FLOW_FAILED = "flow_failed"
    
    # Social activities
    FOLLOWED = "followed"
    UNFOLLOWED = "unfollowed"
    LIKED = "liked"
    UNLIKED = "unliked"
    
    @property
    def display_name(self) -> str:
        return {
            self.CREATED: "Created",
            self.UPDATED: "Updated",
            self.DELETED: "Deleted", 
            self.PUBLISHED: "Published",
            self.ARCHIVED: "Archived",
            self.SHARED: "Shared",
            self.COMMENTED: "Commented on",
            self.MENTIONED: "Mentioned",
            self.INVITED: "Invited",
            self.JOINED: "Joined",
            self.LEFT: "Left",
            self.UPLOADED: "Uploaded",
            self.DOWNLOADED: "Downloaded",
            self.PROCESSED: "Processed",
            self.EXPORTED: "Exported",
            self.IMPORTED: "Imported",
            self.LOGGED_IN: "Logged in",
            self.LOGGED_OUT: "Logged out",
            self.PASSWORD_CHANGED: "Changed password",
            self.PROFILE_UPDATED: "Updated profile",
            self.PROJECT_CREATED: "Created project",
            self.PROJECT_SHARED: "Shared project",
            self.PROJECT_COMPLETED: "Completed project",
            self.FLOW_STARTED: "Started flow",
            self.FLOW_COMPLETED: "Completed flow",
            self.FLOW_FAILED: "Flow failed",
            self.FOLLOWED: "Followed",
            self.UNFOLLOWED: "Unfollowed",
            self.LIKED: "Liked",
            self.UNLIKED: "Unliked"
        }.get(self, "Unknown Activity")
    
    @property
    def is_social(self) -> bool:
        """Check if activity is social interaction"""
        return self in [self.SHARED, self.COMMENTED, self.MENTIONED, self.FOLLOWED, 
                       self.UNFOLLOWED, self.LIKED, self.UNLIKED]
    
    @property
    def is_content(self) -> bool:
        """Check if activity is content-related"""
        return self in [self.CREATED, self.UPDATED, self.DELETED, self.PUBLISHED, self.ARCHIVED]

class ActivityVisibility(PyEnum):
    """Activity visibility levels"""
    PUBLIC = "public"        # Visible to everyone in org
    PRIVATE = "private"      # Visible only to actor
    TEAM = "team"           # Visible to team members
    FOLLOWERS = "followers"  # Visible to followers
    MENTIONED = "mentioned"  # Visible to mentioned users
    
    @property
    def display_name(self) -> str:
        return {
            self.PUBLIC: "Public",
            self.PRIVATE: "Private",
            self.TEAM: "Team",
            self.FOLLOWERS: "Followers",
            self.MENTIONED: "Mentioned Users"
        }.get(self, "Unknown Visibility")

class ActivityFeed(Base):
    __tablename__ = "activity_feeds"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    activity_type = Column(SQLEnum(ActivityType), nullable=False, index=True)
    visibility = Column(SQLEnum(ActivityVisibility), default=ActivityVisibility.PUBLIC, index=True)
    
    # Actor (who performed the activity)
    actor_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    actor_type = Column(String(50), default="User")  # For future polymorphic actors
    
    # Target (what was acted upon) - polymorphic association
    target_type = Column(String(100), nullable=False, index=True)  # 'Project', 'DataSource', etc.
    target_id = Column(Integer, nullable=False, index=True)
    
    # Secondary target (for activities involving two objects)
    secondary_target_type = Column(String(100), index=True)
    secondary_target_id = Column(Integer, index=True)
    
    # Content and context
    content = Column(Text)               # Activity description/content
    summary = Column(String(500))       # Short summary for notifications
    extra_metadata = Column(JSON)             # Additional activity data
    tags = Column(JSON)                 # Activity tags for filtering
    
    # Organization and project context
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    
    # Social features
    mention_user_ids = Column(JSON)     # Users mentioned in activity
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    share_count = Column(Integer, default=0)
    
    # Engagement tracking
    view_count = Column(Integer, default=0)
    last_viewed_at = Column(DateTime)
    is_pinned = Column(Boolean, default=False, index=True)
    is_trending = Column(Boolean, default=False, index=True)
    
    # Grouping and threading
    group_key = Column(String(255), index=True)  # For grouping related activities
    parent_activity_id = Column(Integer, ForeignKey("activity_feeds.id"), index=True)
    thread_root_id = Column(Integer, ForeignKey("activity_feeds.id"), index=True)
    
    # External links and attachments
    external_url = Column(String(500))
    attachment_urls = Column(JSON)
    thumbnail_url = Column(String(500))
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    actor = relationship("User", foreign_keys=[actor_id])
    org = relationship("Org", foreign_keys=[org_id])
    project = relationship("Project", foreign_keys=[project_id])
    team = relationship("Team", foreign_keys=[team_id])
    parent_activity = relationship("ActivityFeed", remote_side=[id], foreign_keys=[parent_activity_id])
    child_activities = relationship("ActivityFeed", remote_side=[parent_activity_id])
    thread_root = relationship("ActivityFeed", remote_side=[id], foreign_keys=[thread_root_id])
    
    # Enhanced database indexes
    __table_args__ = (
        Index('idx_activity_feeds_target', 'target_type', 'target_id'),
        Index('idx_activity_feeds_actor_type', 'actor_id', 'activity_type', 'created_at'),
        Index('idx_activity_feeds_org_type', 'org_id', 'activity_type', 'created_at'),
        Index('idx_activity_feeds_project_type', 'project_id', 'activity_type', 'created_at'),
        Index('idx_activity_feeds_visibility_org', 'visibility', 'org_id', 'created_at'),
        Index('idx_activity_feeds_group_key', 'group_key', 'created_at'),
        Index('idx_activity_feeds_trending', 'is_trending', 'created_at'),
        Index('idx_activity_feeds_engagement', 'like_count', 'comment_count', 'created_at'),
        Index('idx_activity_feeds_thread', 'thread_root_id', 'created_at'),
        Index('idx_activity_feeds_secondary_target', 'secondary_target_type', 'secondary_target_id'),
    )
    
    # Rails constants
    MAX_CONTENT_LENGTH = 2000
    MAX_SUMMARY_LENGTH = 500
    TRENDING_THRESHOLD_HOURS = 24
    TRENDING_MIN_ENGAGEMENT = 10
    GROUP_SIMILAR_ACTIVITIES_WINDOW_MINUTES = 30
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Auto-generate summary if not provided
        if not self.summary and self.content:
            self.summary = self._generate_summary()
        
        # Auto-detect mentions
        if self.content and not self.mention_user_ids:
            self.mention_user_ids = self._extract_mentions()
    
    # Rails-style predicate methods
    def public_(self) -> bool:
        """Check if activity is public (Rails pattern)"""
        return self.visibility == ActivityVisibility.PUBLIC
    
    def private_(self) -> bool:
        """Check if activity is private (Rails pattern)"""
        return self.visibility == ActivityVisibility.PRIVATE
    
    def team_visible_(self) -> bool:
        """Check if activity is team visible (Rails pattern)"""
        return self.visibility == ActivityVisibility.TEAM
    
    def social_activity_(self) -> bool:
        """Check if activity is social interaction (Rails pattern)"""
        return self.activity_type.is_social
    
    def content_activity_(self) -> bool:
        """Check if activity is content-related (Rails pattern)"""
        return self.activity_type.is_content
    
    def has_engagement_(self) -> bool:
        """Check if activity has social engagement (Rails pattern)"""
        return (self.like_count > 0 or self.comment_count > 0 or 
                self.share_count > 0 or self.view_count > 0)
    
    def trending_(self) -> bool:
        """Check if activity is trending (Rails pattern)"""
        if not self.recent_(hours=self.TRENDING_THRESHOLD_HOURS):
            return False
        
        engagement_score = (self.like_count * 3 + self.comment_count * 2 + 
                          self.share_count * 5 + self.view_count)
        return engagement_score >= self.TRENDING_MIN_ENGAGEMENT
    
    def pinned_(self) -> bool:
        """Check if activity is pinned (Rails pattern)"""
        return self.is_pinned
    
    def recent_(self, hours: int = 24) -> bool:
        """Check if activity is recent (Rails pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.created_at >= cutoff
    
    def has_mentions_(self) -> bool:
        """Check if activity mentions users (Rails pattern)"""
        return bool(self.mention_user_ids)
    
    def mentions_user_(self, user_id: int) -> bool:
        """Check if activity mentions specific user (Rails pattern)"""
        return bool(self.mention_user_ids and user_id in self.mention_user_ids)
    
    def has_replies_(self) -> bool:
        """Check if activity has replies (Rails pattern)"""
        return len(self.child_activities) > 0 if self.child_activities else False
    
    def is_reply_(self) -> bool:
        """Check if activity is a reply (Rails pattern)"""
        return self.parent_activity_id is not None
    
    def threaded_(self) -> bool:
        """Check if activity is part of a thread (Rails pattern)"""
        return self.thread_root_id is not None
    
    def has_attachments_(self) -> bool:
        """Check if activity has attachments (Rails pattern)"""
        return bool(self.attachment_urls)
    
    def visible_to_user_(self, user_id: int) -> bool:
        """Check if activity is visible to specific user (Rails pattern)"""
        # Actor can always see their own activities
        if self.actor_id == user_id:
            return True
        
        # Check visibility rules
        if self.visibility == ActivityVisibility.PUBLIC:
            return True
        elif self.visibility == ActivityVisibility.PRIVATE:
            return False
        elif self.visibility == ActivityVisibility.MENTIONED:
            return self.mentions_user_(user_id)
        elif self.visibility == ActivityVisibility.TEAM:
            # Would need to check team membership
            return True  # Simplified for now
        elif self.visibility == ActivityVisibility.FOLLOWERS:
            # Would need to check follower relationship
            return True  # Simplified for now
        
        return False
    
    def editable_by_user_(self, user_id: int) -> bool:
        """Check if activity can be edited by user (Rails pattern)"""
        return self.actor_id == user_id
    
    def deletable_by_user_(self, user_id: int) -> bool:
        """Check if activity can be deleted by user (Rails pattern)"""
        return self.actor_id == user_id
    
    # Rails bang methods
    def like_(self, user_id: int) -> None:
        """Add like to activity (Rails bang method pattern)"""
        # In a full implementation, would check if user already liked
        self.like_count += 1
        self._track_engagement('like', user_id)
    
    def unlike_(self, user_id: int) -> None:
        """Remove like from activity (Rails bang method pattern)"""
        if self.like_count > 0:
            self.like_count -= 1
            self._track_engagement('unlike', user_id)
    
    def add_comment_(self, user_id: int, comment_content: str) -> 'ActivityFeed':
        """Add comment to activity (Rails bang method pattern)"""
        comment_activity = ActivityFeed(
            activity_type=ActivityType.COMMENTED,
            actor_id=user_id,
            target_type=self.target_type,
            target_id=self.target_id,
            parent_activity_id=self.id,
            thread_root_id=self.thread_root_id or self.id,
            content=comment_content,
            org_id=self.org_id,
            project_id=self.project_id,
            visibility=self.visibility
        )
        
        self.comment_count += 1
        self._track_engagement('comment', user_id)
        
        return comment_activity
    
    def share_(self, user_id: int, share_content: str = None) -> 'ActivityFeed':
        """Share activity (Rails bang method pattern)"""
        share_activity = ActivityFeed(
            activity_type=ActivityType.SHARED,
            actor_id=user_id,
            target_type='ActivityFeed',
            target_id=self.id,
            content=share_content or f"Shared: {self.summary}",
            org_id=self.org_id,
            project_id=self.project_id,
            visibility=ActivityVisibility.PUBLIC
        )
        
        self.share_count += 1
        self._track_engagement('share', user_id)
        
        return share_activity
    
    def pin_(self) -> None:
        """Pin activity (Rails bang method pattern)"""
        self.is_pinned = True
        self.updated_at = datetime.now()
    
    def unpin_(self) -> None:
        """Unpin activity (Rails bang method pattern)"""
        self.is_pinned = False
        self.updated_at = datetime.now()
    
    def mark_trending_(self) -> None:
        """Mark activity as trending (Rails bang method pattern)"""
        self.is_trending = True
        self.updated_at = datetime.now()
    
    def unmark_trending_(self) -> None:
        """Remove trending status (Rails bang method pattern)"""
        self.is_trending = False
        self.updated_at = datetime.now()
    
    def record_view_(self, user_id: int = None) -> None:
        """Record activity view (Rails bang method pattern)"""
        self.view_count += 1
        self.last_viewed_at = datetime.now()
        
        if user_id:
            self._track_engagement('view', user_id)
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to activity (Rails bang method pattern)"""
        if not self.tags:
            self.tags = []
        if tag not in self.tags:
            self.tags.append(tag)
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from activity (Rails bang method pattern)"""
        if self.tags and tag in self.tags:
            self.tags.remove(tag)
    
    def has_tag_(self, tag: str) -> bool:
        """Check if activity has specific tag (Rails pattern)"""
        return bool(self.tags and tag in self.tags)
    
    # Rails helper methods
    def _generate_summary(self) -> str:
        """Generate activity summary (Rails private pattern)"""
        if not self.content:
            return f"{self.activity_type.display_name} {self.target_type}"
        
        # Truncate content for summary
        if len(self.content) <= self.MAX_SUMMARY_LENGTH:
            return self.content
        
        return self.content[:self.MAX_SUMMARY_LENGTH - 3] + "..."
    
    def _extract_mentions(self) -> List[int]:
        """Extract user mentions from content (Rails private pattern)"""
        if not self.content:
            return []
        
        # Simple regex to find @username patterns
        import re
        mention_pattern = r'@(\w+)'
        mentions = re.findall(mention_pattern, self.content)
        
        # In a full implementation, would resolve usernames to user IDs
        # For now, return empty list
        return []
    
    def _track_engagement(self, action: str, user_id: int) -> None:
        """Track engagement metrics (Rails private pattern)"""
        if not self.extra_metadata:
            self.extra_metadata = {}
        
        engagement = self.extra_metadata.get('engagement_history', [])
        engagement.append({
            'action': action,
            'user_id': user_id,
            'timestamp': datetime.now().isoformat()
        })
        
        # Keep last 100 engagement events
        self.extra_metadata['engagement_history'] = engagement[-100:]
    
    def engagement_score(self) -> int:
        """Calculate engagement score (Rails pattern)"""
        return (self.like_count * 3 + self.comment_count * 2 + 
                self.share_count * 5 + self.view_count)
    
    def mentioned_users(self) -> List[int]:
        """Get list of mentioned user IDs (Rails pattern)"""
        return self.mention_user_ids or []
    
    def activity_summary(self) -> str:
        """Get formatted activity summary (Rails pattern)"""
        actor_name = self.actor.name if self.actor else f"User {self.actor_id}"
        action = self.activity_type.display_name.lower()
        target = self.target_type.lower()
        
        if self.secondary_target_type:
            secondary = self.secondary_target_type.lower()
            return f"{actor_name} {action} {target} in {secondary}"
        else:
            return f"{actor_name} {action} {target}"
    
    # Rails class methods and scopes
    @classmethod
    def by_actor(cls, actor_id: int):
        """Scope for activities by specific actor (Rails scope pattern)"""
        return cls.query.filter_by(actor_id=actor_id)
    
    @classmethod
    def by_org(cls, org_id: int):
        """Scope for activities in organization (Rails scope pattern)"""
        return cls.query.filter_by(org_id=org_id)
    
    @classmethod
    def by_project(cls, project_id: int):
        """Scope for activities in project (Rails scope pattern)"""
        return cls.query.filter_by(project_id=project_id)
    
    @classmethod
    def by_activity_type(cls, activity_type: ActivityType):
        """Scope for specific activity type (Rails scope pattern)"""
        return cls.query.filter_by(activity_type=activity_type)
    
    @classmethod
    def public_activities(cls):
        """Scope for public activities (Rails scope pattern)"""
        return cls.query.filter_by(visibility=ActivityVisibility.PUBLIC)
    
    @classmethod
    def recent_activities(cls, hours: int = 24):
        """Scope for recent activities (Rails scope pattern)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return cls.query.filter(cls.created_at >= cutoff)
    
    @classmethod
    def trending_activities(cls):
        """Scope for trending activities (Rails scope pattern)"""
        return cls.query.filter_by(is_trending=True)
    
    @classmethod
    def pinned_activities(cls):
        """Scope for pinned activities (Rails scope pattern)"""
        return cls.query.filter_by(is_pinned=True)
    
    @classmethod
    def social_activities(cls):
        """Scope for social activities (Rails scope pattern)"""
        social_types = [activity_type for activity_type in ActivityType if activity_type.is_social]
        return cls.query.filter(cls.activity_type.in_(social_types))
    
    @classmethod
    def content_activities(cls):
        """Scope for content activities (Rails scope pattern)"""
        content_types = [activity_type for activity_type in ActivityType if activity_type.is_content]
        return cls.query.filter(cls.activity_type.in_(content_types))
    
    @classmethod
    def with_engagement(cls):
        """Scope for activities with engagement (Rails scope pattern)"""
        return cls.query.filter(
            (cls.like_count > 0) | (cls.comment_count > 0) | 
            (cls.share_count > 0) | (cls.view_count > 0)
        )
    
    @classmethod
    def mentioning_user(cls, user_id: int):
        """Scope for activities mentioning user (Rails scope pattern)"""
        return cls.query.filter(cls.mention_user_ids.contains([user_id]))
    
    @classmethod
    def visible_to_user(cls, user_id: int, org_id: int = None):
        """Scope for activities visible to user (Rails scope pattern)"""
        query = cls.query.filter(
            (cls.actor_id == user_id) |  # Own activities
            (cls.visibility == ActivityVisibility.PUBLIC) |  # Public activities
            (cls.mention_user_ids.contains([user_id]))  # Mentioned activities
        )
        
        if org_id:
            query = query.filter_by(org_id=org_id)
        
        return query
    
    @classmethod
    def create_activity(cls, activity_type: ActivityType, actor_id: int, 
                       target_type: str, target_id: int, org_id: int, **kwargs):
        """Factory method to create activity (Rails pattern)"""
        activity_data = {
            'activity_type': activity_type,
            'actor_id': actor_id,
            'target_type': target_type,
            'target_id': target_id,
            'org_id': org_id,
            **kwargs
        }
        
        return cls(**activity_data)
    
    @classmethod
    def update_trending_activities(cls) -> int:
        """Update trending status for activities (Rails pattern)"""
        # Reset all trending flags
        cls.query.update({cls.is_trending: False})
        
        # Find activities that should be trending
        cutoff = datetime.now() - timedelta(hours=cls.TRENDING_THRESHOLD_HOURS)
        trending_activities = cls.query.filter(
            cls.created_at >= cutoff
        ).all()
        
        updated_count = 0
        for activity in trending_activities:
            if activity.trending_():
                activity.mark_trending_()
                updated_count += 1
        
        return updated_count
    
    @classmethod
    def cleanup_old_activities(cls, days: int = 90) -> int:
        """Clean up old activities (Rails pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        old_activities = cls.query.filter(cls.created_at < cutoff).all()
        
        count = len(old_activities)
        for activity in old_activities:
            # Archive instead of delete to preserve references
            activity.visibility = ActivityVisibility.PRIVATE
        
        return count
    
    @classmethod
    def get_activity_statistics(cls, org_id: int = None, days: int = 30) -> Dict[str, Any]:
        """Get activity statistics (Rails class method pattern)"""
        cutoff = datetime.now() - timedelta(days=days)
        query = cls.query.filter(cls.created_at >= cutoff)
        
        if org_id:
            query = query.filter_by(org_id=org_id)
        
        total_activities = query.count()
        social_activities = query.filter(
            cls.activity_type.in_([t for t in ActivityType if t.is_social])
        ).count()
        
        content_activities = query.filter(
            cls.activity_type.in_([t for t in ActivityType if t.is_content])
        ).count()
        
        return {
            'period_days': days,
            'total_activities': total_activities,
            'social_activities': social_activities,
            'content_activities': content_activities,
            'average_per_day': round(total_activities / days, 2) if days > 0 else 0,
            'social_percentage': round((social_activities / total_activities * 100), 2) if total_activities > 0 else 0
        }
    
    # Display and serialization methods
    def display_activity_type(self) -> str:
        """Get human-readable activity type (Rails pattern)"""
        return self.activity_type.display_name if self.activity_type else "Unknown Activity"
    
    def display_visibility(self) -> str:
        """Get human-readable visibility (Rails pattern)"""
        return self.visibility.display_name if self.visibility else "Unknown Visibility"
    
    def to_dict(self, include_engagement: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for API responses (Rails pattern)"""
        result = {
            'id': self.id,
            'activity_type': self.activity_type.value,
            'display_activity_type': self.display_activity_type(),
            'visibility': self.visibility.value,
            'display_visibility': self.display_visibility(),
            'actor_id': self.actor_id,
            'target_type': self.target_type,
            'target_id': self.target_id,
            'content': self.content,
            'summary': self.summary,
            'like_count': self.like_count,
            'comment_count': self.comment_count,
            'share_count': self.share_count,
            'view_count': self.view_count,
            'is_pinned': self.is_pinned,
            'is_trending': self.is_trending,
            'has_mentions': self.has_mentions_(),
            'has_replies': self.has_replies_(),
            'engagement_score': self.engagement_score(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'org_id': self.org_id,
            'project_id': self.project_id,
            'tags': self.tags or []
        }
        
        if include_engagement and self.extra_metadata:
            result['engagement_history'] = self.extra_metadata.get('engagement_history', [])
        
        return result
    
    def __repr__(self) -> str:
        return f"<ActivityFeed(id={self.id}, type='{self.activity_type.value}', actor_id={self.actor_id}, target='{self.target_type}:{self.target_id}')>"
    
    def __str__(self) -> str:
        return self.activity_summary()