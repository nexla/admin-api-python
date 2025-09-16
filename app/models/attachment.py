from datetime import datetime, timedelta
from enum import Enum as PyEnum
import hashlib
import json
import mimetypes
import os
from typing import Dict, List, Optional, Any, Union
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


class AttachmentStatus(PyEnum):
    UPLOADING = "UPLOADING"
    ACTIVE = "ACTIVE"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"
    DELETED = "DELETED"
    ARCHIVED = "ARCHIVED"
    QUARANTINED = "QUARANTINED"
    
    @property
    def display_name(self) -> str:
        return {
            self.UPLOADING: "Uploading",
            self.ACTIVE: "Active",
            self.PROCESSING: "Processing",
            self.PROCESSED: "Processed",
            self.FAILED: "Failed",
            self.DELETED: "Deleted",
            self.ARCHIVED: "Archived",
            self.QUARANTINED: "Quarantined"
        }.get(self, self.value)
    
    @property
    def is_available(self) -> bool:
        return self in [self.ACTIVE, self.PROCESSED]


class AttachmentType(PyEnum):
    IMAGE = "IMAGE"
    DOCUMENT = "DOCUMENT"
    SPREADSHEET = "SPREADSHEET"
    PRESENTATION = "PRESENTATION"
    VIDEO = "VIDEO"
    AUDIO = "AUDIO"
    ARCHIVE = "ARCHIVE"
    CODE = "CODE"
    DATA = "DATA"
    OTHER = "OTHER"
    
    @property
    def display_name(self) -> str:
        return {
            self.IMAGE: "Image",
            self.DOCUMENT: "Document",
            self.SPREADSHEET: "Spreadsheet",
            self.PRESENTATION: "Presentation",
            self.VIDEO: "Video",
            self.AUDIO: "Audio",
            self.ARCHIVE: "Archive",
            self.CODE: "Code",
            self.DATA: "Data File",
            self.OTHER: "Other"
        }.get(self, self.value)


class AttachmentScope(PyEnum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    INTERNAL = "INTERNAL"
    RESTRICTED = "RESTRICTED"
    
    @property
    def display_name(self) -> str:
        return {
            self.PUBLIC: "Public",
            self.PRIVATE: "Private",
            self.INTERNAL: "Internal Only",
            self.RESTRICTED: "Restricted Access"
        }.get(self, self.value)


class StorageProvider(PyEnum):
    LOCAL = "LOCAL"
    S3 = "S3"
    GCS = "GCS"
    AZURE = "AZURE"
    CDN = "CDN"
    
    @property
    def display_name(self) -> str:
        return {
            self.LOCAL: "Local Storage",
            self.S3: "Amazon S3",
            self.GCS: "Google Cloud Storage",
            self.AZURE: "Azure Blob Storage",
            self.CDN: "Content Delivery Network"
        }.get(self, self.value)


class Attachment(Base):
    __tablename__ = 'attachments'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    attachment_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    filename = Column(String(255), nullable=False)
    original_filename = Column(String(255), nullable=False)
    file_extension = Column(String(20))
    mime_type = Column(String(100))
    
    attachment_type = Column(SQLEnum(AttachmentType), nullable=False)
    status = Column(SQLEnum(AttachmentStatus), nullable=False, default=AttachmentStatus.UPLOADING)
    scope = Column(SQLEnum(AttachmentScope), nullable=False, default=AttachmentScope.PRIVATE)
    
    file_size = Column(BigInteger, nullable=False)
    file_hash_md5 = Column(String(32))
    file_hash_sha256 = Column(String(64))
    
    storage_provider = Column(SQLEnum(StorageProvider), nullable=False, default=StorageProvider.LOCAL)
    storage_path = Column(String(1024), nullable=False)
    storage_bucket = Column(String(255))
    storage_region = Column(String(100))
    
    url = Column(String(2048))
    cdn_url = Column(String(2048))
    thumbnail_url = Column(String(2048))
    
    org_id = Column(Integer, ForeignKey('orgs.id'))
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    project_id = Column(Integer, ForeignKey('projects.id'))
    
    # Polymorphic associations
    attachable_type = Column(String(100))
    attachable_id = Column(Integer)
    
    title = Column(String(255))
    description = Column(Text)
    alt_text = Column(String(500))
    
    # Image/Video specific fields
    width = Column(Integer)
    height = Column(Integer)
    duration_seconds = Column(Float)
    
    # Processing information
    processing_started_at = Column(DateTime)
    processing_completed_at = Column(DateTime)
    processing_error = Column(Text)
    
    # Virus scan information
    virus_scanned = Column(Boolean, default=False)
    virus_scan_result = Column(String(50))
    virus_scanned_at = Column(DateTime)
    
    # Access tracking
    download_count = Column(Integer, default=0, nullable=False)
    view_count = Column(Integer, default=0, nullable=False)
    last_accessed_at = Column(DateTime)
    
    # Lifecycle management
    expires_at = Column(DateTime)
    archived_at = Column(DateTime)
    deleted_at = Column(DateTime)
    
    active = Column(Boolean, default=True, nullable=False)
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    exif_data = Column(JSON, default=dict)
    processing_options = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    org = relationship("Org", back_populates="attachments")
    user = relationship("User", foreign_keys=[user_id])
    project = relationship("Project", back_populates="attachments")
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    
    __table_args__ = (
        Index('idx_attachment_status', 'status'),
        Index('idx_attachment_type', 'attachment_type'),
        Index('idx_attachment_scope', 'scope'),
        Index('idx_attachment_org_id', 'org_id'),
        Index('idx_attachment_user_id', 'user_id'),
        Index('idx_attachment_project_id', 'project_id'),
        Index('idx_attachment_attachable', 'attachable_type', 'attachable_id'),
        Index('idx_attachment_mime_type', 'mime_type'),
        Index('idx_attachment_file_hash', 'file_hash_sha256'),
        Index('idx_attachment_active', 'active'),
        Index('idx_attachment_expires_at', 'expires_at'),
        Index('idx_attachment_last_accessed', 'last_accessed_at'),
        CheckConstraint('file_size > 0', name='ck_attachment_file_size_positive'),
        CheckConstraint('download_count >= 0', name='ck_attachment_download_count_non_negative'),
        CheckConstraint('view_count >= 0', name='ck_attachment_view_count_non_negative'),
    )
    
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
    ALLOWED_EXTENSIONS = {
        'IMAGE': {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg'},
        'DOCUMENT': {'.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'},
        'SPREADSHEET': {'.xls', '.xlsx', '.csv', '.ods'},
        'PRESENTATION': {'.ppt', '.pptx', '.odp'},
        'VIDEO': {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm'},
        'AUDIO': {'.mp3', '.wav', '.flac', '.aac', '.ogg'},
        'ARCHIVE': {'.zip', '.rar', '.7z', '.tar', '.gz'},
        'CODE': {'.py', '.js', '.html', '.css', '.sql', '.json', '.xml'},
        'DATA': {'.json', '.xml', '.yaml', '.csv', '.tsv'}
    }
    
    def __repr__(self):
        return f"<Attachment(id={self.id}, filename='{self.filename}', status='{self.status.value}')>"
    
    def active_(self) -> bool:
        """Check if attachment is active (Rails pattern)"""
        return (self.active and 
                self.status.is_available and
                not self.deleted_() and
                not self.expired_())
    
    def available_(self) -> bool:
        """Check if attachment is available for use (Rails pattern)"""
        return self.status.is_available and not self.deleted_()
    
    def uploading_(self) -> bool:
        """Check if attachment is uploading (Rails pattern)"""
        return self.status == AttachmentStatus.UPLOADING
    
    def processing_(self) -> bool:
        """Check if attachment is processing (Rails pattern)"""
        return self.status == AttachmentStatus.PROCESSING
    
    def processed_(self) -> bool:
        """Check if attachment is processed (Rails pattern)"""
        return self.status == AttachmentStatus.PROCESSED
    
    def failed_(self) -> bool:
        """Check if attachment processing failed (Rails pattern)"""
        return self.status == AttachmentStatus.FAILED
    
    def deleted_(self) -> bool:
        """Check if attachment is deleted (Rails pattern)"""
        return self.status == AttachmentStatus.DELETED or self.deleted_at is not None
    
    def archived_(self) -> bool:
        """Check if attachment is archived (Rails pattern)"""
        return self.status == AttachmentStatus.ARCHIVED
    
    def quarantined_(self) -> bool:
        """Check if attachment is quarantined (Rails pattern)"""
        return self.status == AttachmentStatus.QUARANTINED
    
    def expired_(self) -> bool:
        """Check if attachment has expired (Rails pattern)"""
        return self.expires_at and self.expires_at < datetime.now()
    
    def public_(self) -> bool:
        """Check if attachment is public (Rails pattern)"""
        return self.scope == AttachmentScope.PUBLIC
    
    def private_(self) -> bool:
        """Check if attachment is private (Rails pattern)"""
        return self.scope == AttachmentScope.PRIVATE
    
    def restricted_(self) -> bool:
        """Check if attachment is restricted (Rails pattern)"""
        return self.scope == AttachmentScope.RESTRICTED
    
    def image_(self) -> bool:
        """Check if attachment is an image (Rails pattern)"""
        return self.attachment_type == AttachmentType.IMAGE
    
    def video_(self) -> bool:
        """Check if attachment is a video (Rails pattern)"""
        return self.attachment_type == AttachmentType.VIDEO
    
    def document_(self) -> bool:
        """Check if attachment is a document (Rails pattern)"""
        return self.attachment_type == AttachmentType.DOCUMENT
    
    def has_thumbnail_(self) -> bool:
        """Check if attachment has thumbnail (Rails pattern)"""
        return self.thumbnail_url is not None
    
    def virus_scanned_(self) -> bool:
        """Check if attachment was virus scanned (Rails pattern)"""
        return self.virus_scanned and self.virus_scan_result is not None
    
    def virus_clean_(self) -> bool:
        """Check if attachment is virus-free (Rails pattern)"""
        return self.virus_scanned_() and self.virus_scan_result == 'clean'
    
    def virus_infected_(self) -> bool:
        """Check if attachment contains virus (Rails pattern)"""
        return self.virus_scanned_() and self.virus_scan_result != 'clean'
    
    def large_file_(self) -> bool:
        """Check if attachment is large (Rails pattern)"""
        return self.file_size > (10 * 1024 * 1024)  # > 10MB
    
    def needs_attention_(self) -> bool:
        """Check if attachment needs attention (Rails pattern)"""
        return (self.failed_() or 
                self.quarantined_() or
                self.virus_infected_() or
                self.expired_())
    
    def activate_(self) -> None:
        """Activate attachment (Rails bang method pattern)"""
        self.active = True
        self.status = AttachmentStatus.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate attachment (Rails bang method pattern)"""
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deactivation_reason'] = reason
    
    def mark_processed_(self) -> None:
        """Mark attachment as processed (Rails bang method pattern)"""
        self.status = AttachmentStatus.PROCESSED
        self.processing_completed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def mark_failed_(self, error_message: str) -> None:
        """Mark attachment processing as failed (Rails bang method pattern)"""
        self.status = AttachmentStatus.FAILED
        self.processing_error = error_message
        self.processing_completed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def start_processing_(self) -> None:
        """Start processing attachment (Rails bang method pattern)"""
        self.status = AttachmentStatus.PROCESSING
        self.processing_started_at = datetime.now()
        self.updated_at = datetime.now()
    
    def quarantine_(self, reason: str) -> None:
        """Quarantine attachment (Rails bang method pattern)"""
        self.status = AttachmentStatus.QUARANTINED
        self.active = False
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['quarantine_reason'] = reason
        self.extra_metadata['quarantined_at'] = datetime.now().isoformat()
    
    def archive_(self, reason: str = None) -> None:
        """Archive attachment (Rails bang method pattern)"""
        self.status = AttachmentStatus.ARCHIVED
        self.archived_at = datetime.now()
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['archive_reason'] = reason
    
    def soft_delete_(self, reason: str = None) -> None:
        """Soft delete attachment (Rails bang method pattern)"""
        self.status = AttachmentStatus.DELETED
        self.deleted_at = datetime.now()
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deletion_reason'] = reason
    
    def restore_(self) -> None:
        """Restore deleted attachment (Rails bang method pattern)"""
        if self.deleted_():
            self.status = AttachmentStatus.ACTIVE
            self.deleted_at = None
            self.active = True
            self.updated_at = datetime.now()
    
    def set_expiry_(self, expires_at: datetime) -> None:
        """Set attachment expiry (Rails bang method pattern)"""
        self.expires_at = expires_at
        self.updated_at = datetime.now()
    
    def extend_expiry_(self, days: int) -> None:
        """Extend attachment expiry (Rails bang method pattern)"""
        if self.expires_at:
            self.expires_at = self.expires_at + timedelta(days=days)
        else:
            self.expires_at = datetime.now() + timedelta(days=days)
        self.updated_at = datetime.now()
    
    def record_view_(self) -> None:
        """Record attachment view (Rails bang method pattern)"""
        self.view_count += 1
        self.last_accessed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def record_download_(self) -> None:
        """Record attachment download (Rails bang method pattern)"""
        self.download_count += 1
        self.last_accessed_at = datetime.now()
        self.updated_at = datetime.now()
    
    def set_virus_scan_result_(self, result: str) -> None:
        """Set virus scan result (Rails bang method pattern)"""
        self.virus_scanned = True
        self.virus_scan_result = result
        self.virus_scanned_at = datetime.now()
        
        if result != 'clean':
            self.quarantine_(f"Virus scan failed: {result}")
        
        self.updated_at = datetime.now()
    
    def generate_thumbnail_(self, thumbnail_url: str) -> None:
        """Generate thumbnail for attachment (Rails bang method pattern)"""
        self.thumbnail_url = thumbnail_url
        self.updated_at = datetime.now()
    
    def set_dimensions_(self, width: int, height: int) -> None:
        """Set image/video dimensions (Rails bang method pattern)"""
        self.width = width
        self.height = height
        self.updated_at = datetime.now()
    
    def set_duration_(self, duration_seconds: float) -> None:
        """Set media duration (Rails bang method pattern)"""
        self.duration_seconds = duration_seconds
        self.updated_at = datetime.now()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to attachment (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from attachment (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def set_metadata_(self, key: str, value: Any) -> None:
        """Set metadata value (Rails bang method pattern)"""
        metadata = dict(self.extra_metadata or {})
        metadata[key] = value
        self.extra_metadata = metadata
        self.updated_at = datetime.now()
    
    @classmethod
    def detect_type_from_extension(cls, extension: str) -> AttachmentType:
        """Detect attachment type from file extension (Rails pattern)"""
        ext_lower = extension.lower()
        
        for attachment_type, extensions in cls.ALLOWED_EXTENSIONS.items():
            if ext_lower in extensions:
                return AttachmentType[attachment_type]
        
        return AttachmentType.OTHER
    
    @classmethod
    def detect_type_from_mime(cls, mime_type: str) -> AttachmentType:
        """Detect attachment type from MIME type (Rails pattern)"""
        if mime_type.startswith('image/'):
            return AttachmentType.IMAGE
        elif mime_type.startswith('video/'):
            return AttachmentType.VIDEO
        elif mime_type.startswith('audio/'):
            return AttachmentType.AUDIO
        elif mime_type in ['application/pdf', 'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document']:
            return AttachmentType.DOCUMENT
        elif mime_type in ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'text/csv']:
            return AttachmentType.SPREADSHEET
        elif mime_type in ['application/vnd.ms-powerpoint', 'application/vnd.openxmlformats-officedocument.presentationml.presentation']:
            return AttachmentType.PRESENTATION
        elif mime_type in ['application/zip', 'application/x-rar-compressed', 'application/x-7z-compressed']:
            return AttachmentType.ARCHIVE
        elif mime_type in ['application/json', 'application/xml', 'text/plain']:
            return AttachmentType.DATA
        else:
            return AttachmentType.OTHER
    
    def calculate_file_hash(self, file_path: str) -> None:
        """Calculate file hashes (Rails bang method pattern)"""
        hash_md5 = hashlib.md5()
        hash_sha256 = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
                hash_sha256.update(chunk)
        
        self.file_hash_md5 = hash_md5.hexdigest()
        self.file_hash_sha256 = hash_sha256.hexdigest()
        self.updated_at = datetime.now()
    
    def file_size_human(self) -> str:
        """Get human-readable file size (Rails pattern)"""
        bytes_size = self.file_size
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        
        return f"{bytes_size:.1f} PB"
    
    def processing_duration(self) -> Optional[float]:
        """Calculate processing duration in seconds (Rails pattern)"""
        if not self.processing_started_at or not self.processing_completed_at:
            return None
        return (self.processing_completed_at - self.processing_started_at).total_seconds()
    
    def age_in_days(self) -> int:
        """Calculate attachment age in days (Rails pattern)"""
        return (datetime.now() - self.created_at).days
    
    def days_until_expiry(self) -> Optional[int]:
        """Calculate days until expiry (Rails pattern)"""
        if not self.expires_at:
            return None
        delta = self.expires_at - datetime.now()
        return max(0, delta.days)
    
    def access_frequency(self) -> float:
        """Calculate access frequency per day (Rails pattern)"""
        age_days = max(1, self.age_in_days())
        return (self.view_count + self.download_count) / age_days
    
    def duplicate_attachments(self) -> List['Attachment']:
        """Find duplicate attachments by hash (Rails pattern)"""
        if not self.file_hash_sha256:
            return []
        
        # This would query the database for other attachments with same hash
        # For now, return empty list as placeholder
        return []
    
    def similar_attachments(self) -> List['Attachment']:
        """Find similar attachments (Rails pattern)"""
        # This would use various similarity metrics like filename, size, type
        # For now, return empty list as placeholder
        return []
    
    def security_report(self) -> Dict[str, Any]:
        """Generate security report (Rails pattern)"""
        return {
            'attachment_id': self.attachment_id,
            'virus_scanned': self.virus_scanned_(),
            'virus_clean': self.virus_clean_(),
            'quarantined': self.quarantined_(),
            'file_hash_sha256': self.file_hash_sha256,
            'scope': self.scope.value,
            'access_count': self.view_count + self.download_count,
            'age_days': self.age_in_days(),
            'expired': self.expired_(),
            'needs_attention': self.needs_attention_()
        }
    
    def usage_statistics(self) -> Dict[str, Any]:
        """Get usage statistics (Rails pattern)"""
        return {
            'attachment_id': self.attachment_id,
            'filename': self.filename,
            'file_size_human': self.file_size_human(),
            'view_count': self.view_count,
            'download_count': self.download_count,
            'total_access_count': self.view_count + self.download_count,
            'access_frequency_per_day': self.access_frequency(),
            'last_accessed_at': self.last_accessed_at.isoformat() if self.last_accessed_at else None,
            'age_days': self.age_in_days(),
            'processing_duration_seconds': self.processing_duration()
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate attachment health report (Rails pattern)"""
        return {
            'attachment_id': self.attachment_id,
            'healthy': not self.needs_attention_(),
            'active': self.active_(),
            'available': self.available_(),
            'status': self.status.value,
            'virus_clean': self.virus_clean_(),
            'expired': self.expired_(),
            'quarantined': self.quarantined_(),
            'needs_attention': self.needs_attention_(),
            'file_size_human': self.file_size_human(),
            'age_days': self.age_in_days(),
            'usage_statistics': self.usage_statistics(),
            'security_report': self.security_report()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'attachment_id': self.attachment_id,
            'filename': self.filename,
            'original_filename': self.original_filename,
            'file_extension': self.file_extension,
            'mime_type': self.mime_type,
            'attachment_type': self.attachment_type.value,
            'status': self.status.value,
            'scope': self.scope.value,
            'file_size': self.file_size,
            'file_size_human': self.file_size_human(),
            'url': self.url,
            'thumbnail_url': self.thumbnail_url,
            'title': self.title,
            'description': self.description,
            'width': self.width,
            'height': self.height,
            'duration_seconds': self.duration_seconds,
            'download_count': self.download_count,
            'view_count': self.view_count,
            'active': self.active,
            'tags': self.tags,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'storage_provider': self.storage_provider.value,
                'storage_path': self.storage_path,
                'storage_bucket': self.storage_bucket,
                'file_hash_sha256': self.file_hash_sha256,
                'virus_scan_result': self.virus_scan_result,
                'metadata': self.extra_metadata,
                'exif_data': self.exif_data,
                'processing_error': self.processing_error
            })
        
        return result