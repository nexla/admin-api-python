from datetime import datetime, timedelta
from enum import Enum as PyEnum
import json
import re
from typing import Dict, List, Optional, Any, Union, Set
import uuid

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, JSON, Enum as SQLEnum, Index, UniqueConstraint,
    Float, CheckConstraint, BigInteger
)
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.database import Base


class CommentStatus(PyEnum):
    ACTIVE = "ACTIVE"
    HIDDEN = "HIDDEN"
    DELETED = "DELETED"
    PENDING_MODERATION = "PENDING_MODERATION"
    FLAGGED = "FLAGGED"
    SPAM = "SPAM"
    
    @property
    def display_name(self) -> str:
        return {
            self.ACTIVE: "Active",
            self.HIDDEN: "Hidden",
            self.DELETED: "Deleted",
            self.PENDING_MODERATION: "Pending Moderation",
            self.FLAGGED: "Flagged",
            self.SPAM: "Spam"
        }.get(self, self.value)
    
    @property
    def is_visible(self) -> bool:
        return self == self.ACTIVE


class CommentType(PyEnum):
    COMMENT = "COMMENT"
    REVIEW = "REVIEW"
    FEEDBACK = "FEEDBACK"
    QUESTION = "QUESTION"
    SUGGESTION = "SUGGESTION"
    BUG_REPORT = "BUG_REPORT"
    APPROVAL = "APPROVAL"
    REJECTION = "REJECTION"
    
    @property
    def display_name(self) -> str:
        return {
            self.COMMENT: "Comment",
            self.REVIEW: "Review",
            self.FEEDBACK: "Feedback",
            self.QUESTION: "Question",
            self.SUGGESTION: "Suggestion",
            self.BUG_REPORT: "Bug Report",
            self.APPROVAL: "Approval",
            self.REJECTION: "Rejection"
        }.get(self, self.value)


class CommentFormat(PyEnum):
    PLAIN_TEXT = "PLAIN_TEXT"
    MARKDOWN = "MARKDOWN"
    HTML = "HTML"
    
    @property
    def display_name(self) -> str:
        return {
            self.PLAIN_TEXT: "Plain Text",
            self.MARKDOWN: "Markdown",
            self.HTML: "HTML"
        }.get(self, self.value)


class ModerationAction(PyEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    HIDDEN = "HIDDEN"
    FLAGGED = "FLAGGED"
    MARKED_SPAM = "MARKED_SPAM"
    
    @property
    def display_name(self) -> str:
        return {
            self.APPROVED: "Approved",
            self.REJECTED: "Rejected",
            self.HIDDEN: "Hidden",
            self.FLAGGED: "Flagged",
            self.MARKED_SPAM: "Marked as Spam"
        }.get(self, self.value)


class Comment(Base):
    __tablename__ = 'comments'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    # Polymorphic association - can be attached to any entity
    commentable_type = Column(String(100), nullable=False, index=True)
    commentable_id = Column(Integer, nullable=False, index=True)
    
    author_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    
    # Hierarchical structure for threaded comments
    parent_comment_id = Column(Integer, ForeignKey('comments.id'), index=True)
    thread_id = Column(CHAR(36), index=True)  # Root comment ID for the thread
    depth = Column(Integer, default=0)
    path = Column(String(500))  # Materialized path for efficient queries
    
    # Comment content
    content = Column(Text, nullable=False)
    content_format = Column(SQLEnum(CommentFormat), default=CommentFormat.PLAIN_TEXT)
    rendered_content = Column(Text)  # Processed/sanitized content
    
    comment_type = Column(SQLEnum(CommentType), default=CommentType.COMMENT)
    status = Column(SQLEnum(CommentStatus), default=CommentStatus.ACTIVE)
    
    # Metadata
    title = Column(String(255))
    summary = Column(String(500))
    
    # Engagement
    upvotes = Column(Integer, default=0)
    downvotes = Column(Integer, default=0)
    reaction_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    
    # Flags and moderation
    flag_count = Column(Integer, default=0)
    spam_score = Column(Float, default=0.0)
    sentiment_score = Column(Float, default=0.0)  # -1 to 1
    toxicity_score = Column(Float, default=0.0)   # 0 to 1
    
    moderated = Column(Boolean, default=False)
    moderated_at = Column(DateTime)
    moderated_by = Column(Integer, ForeignKey('users.id'))
    moderation_action = Column(SQLEnum(ModerationAction))
    moderation_reason = Column(String(500))
    
    # Mentions and notifications
    mentions = Column(JSON, default=list)  # List of mentioned user IDs
    mentioned_users_notified = Column(Boolean, default=False)
    
    # Attachments and media
    attachments = Column(JSON, default=list)  # List of attachment IDs
    has_attachments = Column(Boolean, default=False)
    
    # Edit history
    edited = Column(Boolean, default=False)
    edit_count = Column(Integer, default=0)
    last_edited_at = Column(DateTime)
    last_edited_by = Column(Integer, ForeignKey('users.id'))
    edit_reason = Column(String(255))
    
    # Visibility and permissions
    is_public = Column(Boolean, default=True)
    is_anonymous = Column(Boolean, default=False)
    requires_approval = Column(Boolean, default=False)
    approved = Column(Boolean, default=True)
    approved_at = Column(DateTime)
    approved_by = Column(Integer, ForeignKey('users.id'))
    
    # Analytics
    view_count = Column(BigInteger, default=0)
    unique_view_count = Column(BigInteger, default=0)
    last_viewed_at = Column(DateTime)
    
    # Pinning and featuring
    pinned = Column(Boolean, default=False)
    pinned_at = Column(DateTime)
    pinned_by = Column(Integer, ForeignKey('users.id'))
    featured = Column(Boolean, default=False)
    
    # Resolution (for questions/issues)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime)
    resolved_by = Column(Integer, ForeignKey('users.id'))
    resolution_comment = Column(Text)
    
    active = Column(Boolean, default=True, nullable=False)
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    reactions = Column(JSON, default=dict)  # {"emoji": count}
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    deleted_at = Column(DateTime)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    author = relationship("User", foreign_keys=[author_id])
    org = relationship("Org", back_populates="comments")
    parent_comment = relationship("Comment", remote_side=[id], backref="replies")
    moderator = relationship("User", foreign_keys=[moderated_by])
    editor = relationship("User", foreign_keys=[last_edited_by])
    approver = relationship("User", foreign_keys=[approved_by])
    pinner = relationship("User", foreign_keys=[pinned_by])
    resolver = relationship("User", foreign_keys=[resolved_by])
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    
    __table_args__ = (
        Index('idx_comment_commentable', 'commentable_type', 'commentable_id'),
        Index('idx_comment_author_id', 'author_id'),
        Index('idx_comment_org_id', 'org_id'),
        Index('idx_comment_parent_id', 'parent_comment_id'),
        Index('idx_comment_thread_id', 'thread_id'),
        Index('idx_comment_status', 'status'),
        Index('idx_comment_type', 'comment_type'),
        Index('idx_comment_depth', 'depth'),
        Index('idx_comment_pinned', 'pinned'),
        Index('idx_comment_featured', 'featured'),
        Index('idx_comment_resolved', 'resolved'),
        Index('idx_comment_moderated', 'moderated'),
        Index('idx_comment_created_at', 'created_at'),
        CheckConstraint('upvotes >= 0', name='ck_comment_upvotes_non_negative'),
        CheckConstraint('downvotes >= 0', name='ck_comment_downvotes_non_negative'),
        CheckConstraint('depth >= 0', name='ck_comment_depth_non_negative'),
        CheckConstraint('sentiment_score >= -1 AND sentiment_score <= 1', name='ck_comment_sentiment_range'),
        CheckConstraint('toxicity_score >= 0 AND toxicity_score <= 1', name='ck_comment_toxicity_range'),
    )
    
    MAX_CONTENT_LENGTH = 50000
    MAX_DEPTH = 10
    HIGH_ENGAGEMENT_THRESHOLD = 20
    HIGH_TOXICITY_THRESHOLD = 0.7
    SPAM_THRESHOLD = 0.8
    
    def __init__(self, **kwargs):
        if 'thread_id' not in kwargs:
            kwargs['thread_id'] = kwargs.get('comment_id', str(uuid.uuid4()))
        super().__init__(**kwargs)
        self._update_path()
    
    def __repr__(self):
        return f"<Comment(id={self.id}, author_id={self.author_id}, commentable='{self.commentable_type}:{self.commentable_id}')>"
    
    def active_(self) -> bool:
        """Check if comment is active (Rails pattern)"""
        return (self.active and 
                self.status.is_visible and
                not self.deleted_() and
                not self.spam_())
    
    def visible_(self) -> bool:
        """Check if comment is visible (Rails pattern)"""
        return self.status.is_visible and not self.deleted_()
    
    def deleted_(self) -> bool:
        """Check if comment is deleted (Rails pattern)"""
        return self.status == CommentStatus.DELETED or self.deleted_at is not None
    
    def hidden_(self) -> bool:
        """Check if comment is hidden (Rails pattern)"""
        return self.status == CommentStatus.HIDDEN
    
    def pending_moderation_(self) -> bool:
        """Check if comment is pending moderation (Rails pattern)"""
        return self.status == CommentStatus.PENDING_MODERATION
    
    def flagged_(self) -> bool:
        """Check if comment is flagged (Rails pattern)"""
        return self.status == CommentStatus.FLAGGED or self.flag_count > 0
    
    def spam_(self) -> bool:
        """Check if comment is spam (Rails pattern)"""
        return (self.status == CommentStatus.SPAM or 
                self.spam_score >= self.SPAM_THRESHOLD)
    
    def moderated_(self) -> bool:
        """Check if comment has been moderated (Rails pattern)"""
        return self.moderated and self.moderated_at is not None
    
    def approved_(self) -> bool:
        """Check if comment is approved (Rails pattern)"""
        return self.approved and (not self.requires_approval or self.approved_at is not None)
    
    def edited_(self) -> bool:
        """Check if comment has been edited (Rails pattern)"""
        return self.edited and self.edit_count > 0
    
    def root_comment_(self) -> bool:
        """Check if comment is a root comment (Rails pattern)"""
        return self.parent_comment_id is None and self.depth == 0
    
    def reply_(self) -> bool:
        """Check if comment is a reply (Rails pattern)"""
        return not self.root_comment_()
    
    def has_replies_(self) -> bool:
        """Check if comment has replies (Rails pattern)"""
        return self.reply_count > 0
    
    def pinned_(self) -> bool:
        """Check if comment is pinned (Rails pattern)"""
        return self.pinned
    
    def featured_(self) -> bool:
        """Check if comment is featured (Rails pattern)"""
        return self.featured
    
    def resolved_(self) -> bool:
        """Check if comment is resolved (Rails pattern)"""
        return self.resolved
    
    def public_(self) -> bool:
        """Check if comment is public (Rails pattern)"""
        return self.is_public
    
    def anonymous_(self) -> bool:
        """Check if comment is anonymous (Rails pattern)"""
        return self.is_anonymous
    
    def high_engagement_(self) -> bool:
        """Check if comment has high engagement (Rails pattern)"""
        total_engagement = self.upvotes + self.downvotes + self.reply_count
        return total_engagement >= self.HIGH_ENGAGEMENT_THRESHOLD
    
    def toxic_(self) -> bool:
        """Check if comment is toxic (Rails pattern)"""
        return self.toxicity_score >= self.HIGH_TOXICITY_THRESHOLD
    
    def positive_sentiment_(self) -> bool:
        """Check if comment has positive sentiment (Rails pattern)"""
        return self.sentiment_score > 0.1
    
    def negative_sentiment_(self) -> bool:
        """Check if comment has negative sentiment (Rails pattern)"""
        return self.sentiment_score < -0.1
    
    def needs_attention_(self) -> bool:
        """Check if comment needs attention (Rails pattern)"""
        return (self.flagged_() or 
                self.toxic_() or
                self.spam_() or
                self.pending_moderation_())
    
    def activate_(self) -> None:
        """Activate comment (Rails bang method pattern)"""
        self.active = True
        self.status = CommentStatus.ACTIVE
        self.updated_at = datetime.now()
    
    def hide_(self, reason: str = None) -> None:
        """Hide comment (Rails bang method pattern)"""
        self.status = CommentStatus.HIDDEN
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['hide_reason'] = reason
    
    def delete_(self, reason: str = None) -> None:
        """Soft delete comment (Rails bang method pattern)"""
        self.status = CommentStatus.DELETED
        self.deleted_at = datetime.now()
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deletion_reason'] = reason
    
    def restore_(self) -> None:
        """Restore deleted comment (Rails bang method pattern)"""
        if self.deleted_():
            self.status = CommentStatus.ACTIVE
            self.deleted_at = None
            self.active = True
            self.updated_at = datetime.now()
    
    def flag_(self, reason: str = None, flagger_id: int = None) -> None:
        """Flag comment (Rails bang method pattern)"""
        self.flag_count += 1
        self.status = CommentStatus.FLAGGED
        self.updated_at = datetime.now()
        
        flag_data = {
            'reason': reason,
            'flagger_id': flagger_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata.setdefault('flags', []).append(flag_data)
    
    def unflag_(self) -> None:
        """Unflag comment (Rails bang method pattern)"""
        if self.status == CommentStatus.FLAGGED:
            self.status = CommentStatus.ACTIVE
            self.updated_at = datetime.now()
    
    def mark_spam_(self, reason: str = None) -> None:
        """Mark comment as spam (Rails bang method pattern)"""
        self.status = CommentStatus.SPAM
        self.spam_score = 1.0
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['spam_reason'] = reason
    
    def moderate_(self, action: ModerationAction, moderator_id: int, reason: str = None) -> None:
        """Moderate comment (Rails bang method pattern)"""
        self.moderated = True
        self.moderated_at = datetime.now()
        self.moderated_by = moderator_id
        self.moderation_action = action
        self.moderation_reason = reason
        
        if action == ModerationAction.APPROVED:
            self.status = CommentStatus.ACTIVE
            self.approved = True
            self.approved_at = datetime.now()
            self.approved_by = moderator_id
        elif action == ModerationAction.REJECTED:
            self.status = CommentStatus.HIDDEN
        elif action == ModerationAction.HIDDEN:
            self.status = CommentStatus.HIDDEN
        elif action == ModerationAction.MARKED_SPAM:
            self.status = CommentStatus.SPAM
        
        self.updated_at = datetime.now()
    
    def pin_(self, pinner_id: int) -> None:
        """Pin comment (Rails bang method pattern)"""
        self.pinned = True
        self.pinned_at = datetime.now()
        self.pinned_by = pinner_id
        self.updated_at = datetime.now()
    
    def unpin_(self) -> None:
        """Unpin comment (Rails bang method pattern)"""
        self.pinned = False
        self.pinned_at = None
        self.pinned_by = None
        self.updated_at = datetime.now()
    
    def feature_(self) -> None:
        """Feature comment (Rails bang method pattern)"""
        self.featured = True
        self.updated_at = datetime.now()
    
    def unfeature_(self) -> None:
        """Unfeature comment (Rails bang method pattern)"""
        self.featured = False
        self.updated_at = datetime.now()
    
    def resolve_(self, resolver_id: int, resolution_note: str = None) -> None:
        """Resolve comment (Rails bang method pattern)"""
        self.resolved = True
        self.resolved_at = datetime.now()
        self.resolved_by = resolver_id
        self.resolution_comment = resolution_note
        self.updated_at = datetime.now()
    
    def unresolve_(self) -> None:
        """Unresolve comment (Rails bang method pattern)"""
        self.resolved = False
        self.resolved_at = None
        self.resolved_by = None
        self.resolution_comment = None
        self.updated_at = datetime.now()
    
    def edit_content_(self, new_content: str, editor_id: int, reason: str = None) -> None:
        """Edit comment content (Rails bang method pattern)"""
        self.content = new_content
        self.edited = True
        self.edit_count += 1
        self.last_edited_at = datetime.now()
        self.last_edited_by = editor_id
        self.edit_reason = reason
        self.updated_at = datetime.now()
        
        # Reprocess content
        self._process_content()
    
    def upvote_(self) -> None:
        """Add upvote to comment (Rails bang method pattern)"""
        self.upvotes += 1
        self.updated_at = datetime.now()
    
    def downvote_(self) -> None:
        """Add downvote to comment (Rails bang method pattern)"""
        self.downvotes += 1
        self.updated_at = datetime.now()
    
    def add_reaction_(self, emoji: str) -> None:
        """Add reaction to comment (Rails bang method pattern)"""
        reactions = dict(self.reactions or {})
        reactions[emoji] = reactions.get(emoji, 0) + 1
        self.reactions = reactions
        self.reaction_count = sum(reactions.values())
        self.updated_at = datetime.now()
    
    def remove_reaction_(self, emoji: str) -> None:
        """Remove reaction from comment (Rails bang method pattern)"""
        reactions = dict(self.reactions or {})
        if emoji in reactions and reactions[emoji] > 0:
            reactions[emoji] -= 1
            if reactions[emoji] == 0:
                del reactions[emoji]
            self.reactions = reactions
            self.reaction_count = sum(reactions.values())
            self.updated_at = datetime.now()
    
    def increment_view_(self, unique: bool = False) -> None:
        """Increment view count (Rails bang method pattern)"""
        self.view_count += 1
        if unique:
            self.unique_view_count += 1
        self.last_viewed_at = datetime.now()
        # Don't update updated_at for view tracking
    
    def add_mention_(self, user_id: int) -> None:
        """Add user mention (Rails bang method pattern)"""
        mentions = list(self.mentions or [])
        if user_id not in mentions:
            mentions.append(user_id)
            self.mentions = mentions
            self.updated_at = datetime.now()
    
    def remove_mention_(self, user_id: int) -> None:
        """Remove user mention (Rails bang method pattern)"""
        mentions = list(self.mentions or [])
        if user_id in mentions:
            mentions.remove(user_id)
            self.mentions = mentions
            self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to comment (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from comment (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def _update_path(self) -> None:
        """Update materialized path for hierarchical queries (private helper)"""
        if self.parent_comment_id:
            # Would need to query parent to build path
            # For now, simplified implementation
            self.path = f"{self.parent_comment_id}.{self.id}"
        else:
            self.path = str(self.id)
    
    def _process_content(self) -> None:
        """Process and sanitize content (private helper)"""
        if self.content_format == CommentFormat.MARKDOWN:
            # Would use markdown processor here
            self.rendered_content = self.content  # Simplified
        elif self.content_format == CommentFormat.HTML:
            # Would sanitize HTML here
            self.rendered_content = self.content  # Simplified
        else:
            self.rendered_content = self.content
        
        # Extract mentions
        self._extract_mentions()
    
    def _extract_mentions(self) -> None:
        """Extract user mentions from content (private helper)"""
        # Simple regex to find @username mentions
        mention_pattern = r'@(\w+)'
        mentions = re.findall(mention_pattern, self.content)
        
        # Would need to resolve usernames to user IDs
        # For now, simplified implementation
        self.mentions = mentions[:10]  # Limit mentions
    
    def score(self) -> int:
        """Calculate comment score (Rails pattern)"""
        return self.upvotes - self.downvotes
    
    def engagement_score(self) -> float:
        """Calculate engagement score (Rails pattern)"""
        return (self.upvotes * 1.0 + 
                self.reply_count * 2.0 + 
                self.reaction_count * 0.5 + 
                self.view_count * 0.01)
    
    def age_in_hours(self) -> float:
        """Calculate comment age in hours (Rails pattern)"""
        return (datetime.now() - self.created_at).total_seconds() / 3600
    
    def thread_comments(self) -> List['Comment']:
        """Get all comments in the same thread (Rails pattern)"""
        # Would query database for comments with same thread_id
        return []
    
    def child_comments(self) -> List['Comment']:
        """Get direct child comments (Rails pattern)"""
        return list(self.replies)
    
    def all_descendants(self) -> List['Comment']:
        """Get all descendant comments recursively (Rails pattern)"""
        # Would use materialized path or recursive query
        return []
    
    def ancestors(self) -> List['Comment']:
        """Get ancestor comments up to root (Rails pattern)"""
        ancestors = []
        current = self.parent_comment
        while current:
            ancestors.insert(0, current)
            current = current.parent_comment
        return ancestors
    
    def root_comment(self) -> Optional['Comment']:
        """Get root comment of the thread (Rails pattern)"""
        if self.root_comment_():
            return self
        ancestors = self.ancestors()
        return ancestors[0] if ancestors else None
    
    def mentioned_users(self) -> List[int]:
        """Get list of mentioned user IDs (Rails pattern)"""
        return list(self.mentions or [])
    
    def word_count(self) -> int:
        """Calculate word count (Rails pattern)"""
        return len(self.content.split()) if self.content else 0
    
    def reading_time_minutes(self) -> int:
        """Estimate reading time in minutes (Rails pattern)"""
        words = self.word_count()
        return max(1, words // 200)  # Assuming 200 words per minute
    
    def engagement_metrics(self) -> Dict[str, Any]:
        """Get engagement metrics (Rails pattern)"""
        return {
            'comment_id': self.comment_id,
            'score': self.score(),
            'upvotes': self.upvotes,
            'downvotes': self.downvotes,
            'reply_count': self.reply_count,
            'reaction_count': self.reaction_count,
            'view_count': self.view_count,
            'unique_view_count': self.unique_view_count,
            'engagement_score': self.engagement_score(),
            'high_engagement': self.high_engagement_(),
            'age_hours': self.age_in_hours()
        }
    
    def moderation_summary(self) -> Dict[str, Any]:
        """Get moderation summary (Rails pattern)"""
        return {
            'comment_id': self.comment_id,
            'moderated': self.moderated_(),
            'flagged': self.flagged_(),
            'flag_count': self.flag_count,
            'spam': self.spam_(),
            'spam_score': self.spam_score,
            'toxic': self.toxic_(),
            'toxicity_score': self.toxicity_score,
            'sentiment_score': self.sentiment_score,
            'needs_attention': self.needs_attention_(),
            'moderation_action': self.moderation_action.value if self.moderation_action else None,
            'moderated_at': self.moderated_at.isoformat() if self.moderated_at else None
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate comment health report (Rails pattern)"""
        return {
            'comment_id': self.comment_id,
            'healthy': not self.needs_attention_(),
            'active': self.active_(),
            'visible': self.visible_(),
            'status': self.status.value,
            'approved': self.approved_(),
            'flagged': self.flagged_(),
            'spam': self.spam_(),
            'toxic': self.toxic_(),
            'needs_attention': self.needs_attention_(),
            'engagement_metrics': self.engagement_metrics(),
            'moderation_summary': self.moderation_summary()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'comment_id': self.comment_id,
            'commentable_type': self.commentable_type,
            'commentable_id': self.commentable_id,
            'author_id': self.author_id,
            'parent_comment_id': self.parent_comment_id,
            'thread_id': self.thread_id,
            'depth': self.depth,
            'content': self.content,
            'content_format': self.content_format.value,
            'comment_type': self.comment_type.value,
            'status': self.status.value,
            'title': self.title,
            'summary': self.summary,
            'upvotes': self.upvotes,
            'downvotes': self.downvotes,
            'score': self.score(),
            'reply_count': self.reply_count,
            'reaction_count': self.reaction_count,
            'reactions': self.reactions,
            'pinned': self.pinned,
            'featured': self.featured,
            'resolved': self.resolved,
            'edited': self.edited,
            'tags': self.tags,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'rendered_content': self.rendered_content,
                'mentions': self.mentions,
                'attachments': self.attachments,
                'flag_count': self.flag_count,
                'spam_score': self.spam_score,
                'sentiment_score': self.sentiment_score,
                'toxicity_score': self.toxicity_score,
                'view_count': self.view_count,
                'metadata': self.extra_metadata,
                'moderation_reason': self.moderation_reason,
                'edit_reason': self.edit_reason
            })
        
        return result