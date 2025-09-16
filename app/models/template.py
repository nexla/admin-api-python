from datetime import datetime, timedelta
from enum import Enum as PyEnum
import hashlib
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


class TemplateType(PyEnum):
    EMAIL = "EMAIL"
    NOTIFICATION = "NOTIFICATION"
    REPORT = "REPORT"
    DOCUMENT = "DOCUMENT"
    WORKFLOW = "WORKFLOW"
    DASHBOARD = "DASHBOARD"
    FORM = "FORM"
    API_RESPONSE = "API_RESPONSE"
    WEBHOOK_PAYLOAD = "WEBHOOK_PAYLOAD"
    CUSTOM = "CUSTOM"
    
    @property
    def display_name(self) -> str:
        return {
            self.EMAIL: "Email Template",
            self.NOTIFICATION: "Notification Template",
            self.REPORT: "Report Template",
            self.DOCUMENT: "Document Template",
            self.WORKFLOW: "Workflow Template",
            self.DASHBOARD: "Dashboard Template",
            self.FORM: "Form Template",
            self.API_RESPONSE: "API Response Template",
            self.WEBHOOK_PAYLOAD: "Webhook Payload Template",
            self.CUSTOM: "Custom Template"
        }.get(self, self.value)


class TemplateStatus(PyEnum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ARCHIVED = "ARCHIVED"
    DEPRECATED = "DEPRECATED"
    PUBLISHED = "PUBLISHED"
    
    @property
    def display_name(self) -> str:
        return {
            self.DRAFT: "Draft",
            self.ACTIVE: "Active",
            self.INACTIVE: "Inactive",
            self.ARCHIVED: "Archived",
            self.DEPRECATED: "Deprecated",
            self.PUBLISHED: "Published"
        }.get(self, self.value)
    
    @property
    def is_usable(self) -> bool:
        return self in [self.ACTIVE, self.PUBLISHED]


class TemplateScope(PyEnum):
    GLOBAL = "GLOBAL"
    ORG = "ORG"
    PROJECT = "PROJECT"
    USER = "USER"
    
    @property
    def display_name(self) -> str:
        return {
            self.GLOBAL: "Global",
            self.ORG: "Organization",
            self.PROJECT: "Project",
            self.USER: "User"
        }.get(self, self.value)


class TemplateEngine(PyEnum):
    JINJA2 = "JINJA2"
    MUSTACHE = "MUSTACHE"
    HANDLEBARS = "HANDLEBARS"
    PLAIN_TEXT = "PLAIN_TEXT"
    MARKDOWN = "MARKDOWN"
    HTML = "HTML"
    JSON = "JSON"
    
    @property
    def display_name(self) -> str:
        return {
            self.JINJA2: "Jinja2",
            self.MUSTACHE: "Mustache",
            self.HANDLEBARS: "Handlebars",
            self.PLAIN_TEXT: "Plain Text",
            self.MARKDOWN: "Markdown",
            self.HTML: "HTML",
            self.JSON: "JSON"
        }.get(self, self.value)


class Template(Base):
    __tablename__ = 'templates'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    template_id = Column(CHAR(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    name = Column(String(255), nullable=False)
    slug = Column(String(100), nullable=False)
    description = Column(Text)
    
    template_type = Column(SQLEnum(TemplateType), nullable=False)
    status = Column(SQLEnum(TemplateStatus), nullable=False, default=TemplateStatus.DRAFT)
    scope = Column(SQLEnum(TemplateScope), nullable=False, default=TemplateScope.ORG)
    engine = Column(SQLEnum(TemplateEngine), nullable=False, default=TemplateEngine.JINJA2)
    
    org_id = Column(Integer, ForeignKey('orgs.id'))
    project_id = Column(Integer, ForeignKey('projects.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    
    # Template content
    content = Column(Text, nullable=False)
    subject_template = Column(String(500))  # For email templates
    preheader = Column(String(255))  # For email templates
    
    # Alternative formats
    html_content = Column(Text)
    text_content = Column(Text)
    json_schema = Column(JSON)  # For API/JSON templates
    
    # Template variables and validation
    variables = Column(JSON, default=list)  # List of expected variables
    required_variables = Column(JSON, default=list)
    default_values = Column(JSON, default=dict)
    variable_validation = Column(JSON, default=dict)
    
    # Styling and assets
    css_styles = Column(Text)
    inline_styles = Column(Boolean, default=False)
    assets = Column(JSON, default=list)  # Links to images, fonts, etc.
    
    # Usage and versioning
    version = Column(String(20), default='1.0.0')
    parent_template_id = Column(Integer, ForeignKey('templates.id'))
    usage_count = Column(BigInteger, default=0)
    last_used_at = Column(DateTime)
    
    # Categories and organization
    category = Column(String(100))
    subcategory = Column(String(100))
    language = Column(String(10), default='en')
    locale = Column(String(10))
    
    # Access control
    is_public = Column(Boolean, default=False)
    requires_approval = Column(Boolean, default=False)
    approved = Column(Boolean, default=False)
    approved_at = Column(DateTime)
    approved_by = Column(Integer, ForeignKey('users.id'))
    
    # Template settings
    allow_html = Column(Boolean, default=True)
    sanitize_input = Column(Boolean, default=True)
    cache_enabled = Column(Boolean, default=True)
    cache_ttl_seconds = Column(Integer, default=3600)
    
    # Performance and analytics
    render_time_ms = Column(Float, default=0.0)
    compile_time_ms = Column(Float, default=0.0)
    size_bytes = Column(Integer)
    complexity_score = Column(Float, default=0.0)
    
    # Validation and testing
    validation_errors = Column(JSON, default=list)
    test_data = Column(JSON, default=dict)
    last_validated_at = Column(DateTime)
    
    active = Column(Boolean, default=True, nullable=False)
    
    tags = Column(JSON, default=list)
    extra_metadata = Column(JSON, default=dict)
    usage_log = Column(JSON, default=list)
    
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    published_at = Column(DateTime)
    archived_at = Column(DateTime)
    
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_by = Column(Integer, ForeignKey('users.id'))
    
    org = relationship("Org", back_populates="templates")
    project = relationship("Project", back_populates="templates")
    user = relationship("User", foreign_keys=[user_id])
    parent_template = relationship("Template", remote_side=[id], backref="child_templates")
    approver = relationship("User", foreign_keys=[approved_by])
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    
    notifications = relationship("Notification", back_populates="template")
    
    __table_args__ = (
        Index('idx_template_org_id', 'org_id'),
        Index('idx_template_project_id', 'project_id'),
        Index('idx_template_user_id', 'user_id'),
        Index('idx_template_type', 'template_type'),
        Index('idx_template_status', 'status'),
        Index('idx_template_scope', 'scope'),
        Index('idx_template_category', 'category'),
        Index('idx_template_language', 'language'),
        Index('idx_template_active', 'active'),
        Index('idx_template_public', 'is_public'),
        Index('idx_template_usage_count', 'usage_count'),
        UniqueConstraint('org_id', 'slug', name='uq_template_org_slug'),
        CheckConstraint('usage_count >= 0', name='ck_template_usage_count_non_negative'),
        CheckConstraint('render_time_ms >= 0', name='ck_template_render_time_non_negative'),
        CheckConstraint('complexity_score >= 0', name='ck_template_complexity_score_non_negative'),
    )
    
    MAX_CONTENT_SIZE = 1024 * 1024  # 1MB
    MAX_VARIABLES = 100
    MAX_USAGE_LOG_ENTRIES = 1000
    HIGH_COMPLEXITY_THRESHOLD = 7.0
    POPULAR_USAGE_THRESHOLD = 100
    
    def __repr__(self):
        return f"<Template(id={self.id}, name='{self.name}', type='{self.template_type.value}', status='{self.status.value}')>"
    
    def active_(self) -> bool:
        """Check if template is active (Rails pattern)"""
        return (self.active and 
                self.status.is_usable and
                not self.archived_())
    
    def usable_(self) -> bool:
        """Check if template is usable (Rails pattern)"""
        return self.status.is_usable and not self.archived_()
    
    def draft_(self) -> bool:
        """Check if template is draft (Rails pattern)"""
        return self.status == TemplateStatus.DRAFT
    
    def published_(self) -> bool:
        """Check if template is published (Rails pattern)"""
        return self.status == TemplateStatus.PUBLISHED
    
    def archived_(self) -> bool:
        """Check if template is archived (Rails pattern)"""
        return self.status == TemplateStatus.ARCHIVED
    
    def deprecated_(self) -> bool:
        """Check if template is deprecated (Rails pattern)"""
        return self.status == TemplateStatus.DEPRECATED
    
    def public_(self) -> bool:
        """Check if template is public (Rails pattern)"""
        return self.is_public
    
    def private_(self) -> bool:
        """Check if template is private (Rails pattern)"""
        return not self.is_public
    
    def approved_(self) -> bool:
        """Check if template is approved (Rails pattern)"""
        return self.approved and self.approved_at is not None
    
    def pending_approval_(self) -> bool:
        """Check if template is pending approval (Rails pattern)"""
        return self.requires_approval and not self.approved_()
    
    def global_scope_(self) -> bool:
        """Check if template has global scope (Rails pattern)"""
        return self.scope == TemplateScope.GLOBAL
    
    def org_scope_(self) -> bool:
        """Check if template has org scope (Rails pattern)"""
        return self.scope == TemplateScope.ORG
    
    def project_scope_(self) -> bool:
        """Check if template has project scope (Rails pattern)"""
        return self.scope == TemplateScope.PROJECT
    
    def user_scope_(self) -> bool:
        """Check if template has user scope (Rails pattern)"""
        return self.scope == TemplateScope.USER
    
    def has_parent_(self) -> bool:
        """Check if template has parent (Rails pattern)"""
        return self.parent_template_id is not None
    
    def has_children_(self) -> bool:
        """Check if template has children (Rails pattern)"""
        return len(self.child_templates or []) > 0
    
    def complex_(self) -> bool:
        """Check if template is complex (Rails pattern)"""
        return self.complexity_score >= self.HIGH_COMPLEXITY_THRESHOLD
    
    def popular_(self) -> bool:
        """Check if template is popular (Rails pattern)"""
        return self.usage_count >= self.POPULAR_USAGE_THRESHOLD
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if template was recently used (Rails pattern)"""
        if not self.last_used_at:
            return False
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.last_used_at > cutoff
    
    def valid_(self) -> bool:
        """Check if template is valid (Rails pattern)"""
        return not bool(self.validation_errors)
    
    def needs_attention_(self) -> bool:
        """Check if template needs attention (Rails pattern)"""
        return (not self.valid_() or 
                self.deprecated_() or
                self.pending_approval_())
    
    def activate_(self) -> None:
        """Activate template (Rails bang method pattern)"""
        self.active = True
        self.status = TemplateStatus.ACTIVE
        self.updated_at = datetime.now()
    
    def deactivate_(self, reason: str = None) -> None:
        """Deactivate template (Rails bang method pattern)"""
        self.active = False
        self.status = TemplateStatus.INACTIVE
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['deactivation_reason'] = reason
    
    def publish_(self) -> None:
        """Publish template (Rails bang method pattern)"""
        if self.pending_approval_():
            raise ValueError("Template requires approval before publishing")
        
        self.status = TemplateStatus.PUBLISHED
        self.published_at = datetime.now()
        self.active = True
        self.updated_at = datetime.now()
    
    def archive_(self, reason: str = None) -> None:
        """Archive template (Rails bang method pattern)"""
        self.status = TemplateStatus.ARCHIVED
        self.archived_at = datetime.now()
        self.active = False
        self.updated_at = datetime.now()
        
        if reason:
            self.extra_metadata = self.extra_metadata or {}
            self.extra_metadata['archive_reason'] = reason
    
    def deprecate_(self, reason: str = None, replacement_template_id: int = None) -> None:
        """Deprecate template (Rails bang method pattern)"""
        self.status = TemplateStatus.DEPRECATED
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['deprecation_reason'] = reason
        self.extra_metadata['deprecated_at'] = datetime.now().isoformat()
        if replacement_template_id:
            self.extra_metadata['replacement_template_id'] = replacement_template_id
    
    def approve_(self, approver_user_id: int) -> None:
        """Approve template (Rails bang method pattern)"""
        self.approved = True
        self.approved_at = datetime.now()
        self.approved_by = approver_user_id
        self.updated_at = datetime.now()
    
    def reject_approval_(self, reason: str) -> None:
        """Reject template approval (Rails bang method pattern)"""
        self.approved = False
        self.approved_at = None
        self.approved_by = None
        self.updated_at = datetime.now()
        
        self.extra_metadata = self.extra_metadata or {}
        self.extra_metadata['approval_rejection_reason'] = reason
    
    def increment_usage_(self, render_time_ms: float = None) -> None:
        """Increment usage count (Rails bang method pattern)"""
        self.usage_count += 1
        self.last_used_at = datetime.now()
        
        if render_time_ms is not None:
            # Update rolling average render time
            if self.usage_count == 1:
                self.render_time_ms = render_time_ms
            else:
                self.render_time_ms = (self.render_time_ms * 0.9) + (render_time_ms * 0.1)
        
        # Log usage
        usage_entry = {
            'timestamp': datetime.now().isoformat(),
            'render_time_ms': render_time_ms
        }
        
        self.usage_log = self.usage_log or []
        self.usage_log.append(usage_entry)
        
        # Keep only recent entries
        if len(self.usage_log) > self.MAX_USAGE_LOG_ENTRIES:
            self.usage_log = self.usage_log[-self.MAX_USAGE_LOG_ENTRIES:]
        
        self.updated_at = datetime.now()
    
    def update_content_(self, new_content: str) -> None:
        """Update template content (Rails bang method pattern)"""
        self.content = new_content
        self.size_bytes = len(new_content.encode('utf-8'))
        self.complexity_score = self._calculate_complexity()
        self.last_validated_at = None  # Reset validation
        self.validation_errors = []
        self.updated_at = datetime.now()
    
    def add_variable_(self, variable_name: str, required: bool = False, 
                     default_value: Any = None, validation_rule: str = None) -> None:
        """Add template variable (Rails bang method pattern)"""
        variables = list(self.variables or [])
        if variable_name not in variables:
            variables.append(variable_name)
            self.variables = variables
        
        if required:
            required_vars = list(self.required_variables or [])
            if variable_name not in required_vars:
                required_vars.append(variable_name)
                self.required_variables = required_vars
        
        if default_value is not None:
            defaults = dict(self.default_values or {})
            defaults[variable_name] = default_value
            self.default_values = defaults
        
        if validation_rule:
            validations = dict(self.variable_validation or {})
            validations[variable_name] = validation_rule
            self.variable_validation = validations
        
        self.updated_at = datetime.now()
    
    def remove_variable_(self, variable_name: str) -> None:
        """Remove template variable (Rails bang method pattern)"""
        variables = list(self.variables or [])
        if variable_name in variables:
            variables.remove(variable_name)
            self.variables = variables
        
        # Remove from required variables
        required_vars = list(self.required_variables or [])
        if variable_name in required_vars:
            required_vars.remove(variable_name)
            self.required_variables = required_vars
        
        # Remove default value
        defaults = dict(self.default_values or {})
        if variable_name in defaults:
            del defaults[variable_name]
            self.default_values = defaults
        
        # Remove validation
        validations = dict(self.variable_validation or {})
        if variable_name in validations:
            del validations[variable_name]
            self.variable_validation = validations
        
        self.updated_at = datetime.now()
    
    def validate_template_(self) -> List[str]:
        """Validate template syntax and variables (Rails bang method pattern)"""
        errors = []
        
        try:
            # Basic validation
            if not self.content or len(self.content.strip()) == 0:
                errors.append("Template content cannot be empty")
            
            if self.size_bytes and self.size_bytes > self.MAX_CONTENT_SIZE:
                errors.append(f"Template size exceeds maximum limit of {self.MAX_CONTENT_SIZE} bytes")
            
            # Engine-specific validation
            if self.engine == TemplateEngine.JINJA2:
                errors.extend(self._validate_jinja2())
            elif self.engine == TemplateEngine.JSON:
                errors.extend(self._validate_json())
            
            # Variable validation
            errors.extend(self._validate_variables())
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
        
        self.validation_errors = errors
        self.last_validated_at = datetime.now()
        self.updated_at = datetime.now()
        
        return errors
    
    def render(self, variables: Dict[str, Any] = None) -> str:
        """Render template with variables (Rails pattern)"""
        if not self.usable_():
            raise ValueError(f"Template is not usable (status: {self.status.value})")
        
        variables = variables or {}
        
        # Add default values
        render_vars = dict(self.default_values or {})
        render_vars.update(variables)
        
        # Validate required variables
        missing_vars = []
        for required_var in (self.required_variables or []):
            if required_var not in render_vars:
                missing_vars.append(required_var)
        
        if missing_vars:
            raise ValueError(f"Missing required variables: {', '.join(missing_vars)}")
        
        # Render based on engine
        start_time = datetime.now()
        
        try:
            if self.engine == TemplateEngine.JINJA2:
                rendered = self._render_jinja2(render_vars)
            elif self.engine == TemplateEngine.PLAIN_TEXT:
                rendered = self._render_plain_text(render_vars)
            elif self.engine == TemplateEngine.JSON:
                rendered = self._render_json(render_vars)
            else:
                rendered = self.content  # Fallback
            
            render_time = (datetime.now() - start_time).total_seconds() * 1000
            self.increment_usage_(render_time)
            
            return rendered
            
        except Exception as e:
            raise ValueError(f"Template rendering failed: {str(e)}")
    
    def clone_(self, new_name: str, user_id: int = None) -> 'Template':
        """Clone template (Rails bang method pattern)"""
        clone_data = {
            'name': new_name,
            'slug': new_name.lower().replace(' ', '_'),
            'description': f"Clone of {self.name}",
            'template_type': self.template_type,
            'scope': self.scope,
            'engine': self.engine,
            'org_id': self.org_id,
            'project_id': self.project_id,
            'user_id': user_id or self.user_id,
            'content': self.content,
            'subject_template': self.subject_template,
            'html_content': self.html_content,
            'text_content': self.text_content,
            'variables': self.variables.copy() if self.variables else [],
            'required_variables': self.required_variables.copy() if self.required_variables else [],
            'default_values': self.default_values.copy() if self.default_values else {},
            'css_styles': self.css_styles,
            'category': self.category,
            'language': self.language,
            'parent_template_id': self.id,
            'created_by': user_id
        }
        
        return self.__class__(**clone_data)
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to template (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from template (Rails bang method pattern)"""
        tags = list(self.tags or [])
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.now()
    
    def _calculate_complexity(self) -> float:
        """Calculate template complexity score (private helper)"""
        if not self.content:
            return 0.0
        
        score = 0.0
        content = self.content
        
        # Size factor
        score += min(len(content) / 1000, 3.0)
        
        # Variable count
        score += min(len(self.variables or []) / 10, 2.0)
        
        # Control structures (Jinja2)
        if self.engine == TemplateEngine.JINJA2:
            control_patterns = [r'\{%\s*if\s', r'\{%\s*for\s', r'\{%\s*macro\s', r'\{%\s*call\s']
            for pattern in control_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                score += len(matches) * 0.5
        
        # Nested structures
        open_braces = content.count('{')
        close_braces = content.count('}')
        score += abs(open_braces - close_braces) * 0.1
        
        return min(score, 10.0)
    
    def _validate_jinja2(self) -> List[str]:
        """Validate Jinja2 template syntax (private helper)"""
        errors = []
        try:
            import jinja2
            env = jinja2.Environment()
            env.parse(self.content)
        except ImportError:
            errors.append("Jinja2 not available for validation")
        except Exception as e:
            errors.append(f"Jinja2 syntax error: {str(e)}")
        
        return errors
    
    def _validate_json(self) -> List[str]:
        """Validate JSON template syntax (private helper)"""
        errors = []
        try:
            json.loads(self.content)
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON syntax: {str(e)}")
        
        return errors
    
    def _validate_variables(self) -> List[str]:
        """Validate template variables (private helper)"""
        errors = []
        
        if len(self.variables or []) > self.MAX_VARIABLES:
            errors.append(f"Too many variables (max: {self.MAX_VARIABLES})")
        
        # Check for unused variables
        content = self.content.lower()
        for variable in (self.variables or []):
            if variable.lower() not in content:
                errors.append(f"Variable '{variable}' defined but not used in template")
        
        return errors
    
    def _render_jinja2(self, variables: Dict[str, Any]) -> str:
        """Render Jinja2 template (private helper)"""
        try:
            import jinja2
            template = jinja2.Template(self.content)
            return template.render(**variables)
        except ImportError:
            raise ValueError("Jinja2 not available for rendering")
    
    def _render_plain_text(self, variables: Dict[str, Any]) -> str:
        """Render plain text template with variable substitution (private helper)"""
        content = self.content
        for key, value in variables.items():
            content = content.replace(f"{{{key}}}", str(value))
        return content
    
    def _render_json(self, variables: Dict[str, Any]) -> str:
        """Render JSON template (private helper)"""
        try:
            template_dict = json.loads(self.content)
            # Simple variable substitution in JSON values
            rendered_dict = self._substitute_json_variables(template_dict, variables)
            return json.dumps(rendered_dict, indent=2)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON template")
    
    def _substitute_json_variables(self, obj: Any, variables: Dict[str, Any]) -> Any:
        """Recursively substitute variables in JSON structure (private helper)"""
        if isinstance(obj, dict):
            return {k: self._substitute_json_variables(v, variables) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._substitute_json_variables(item, variables) for item in obj]
        elif isinstance(obj, str):
            for key, value in variables.items():
                obj = obj.replace(f"{{{key}}}", str(value))
            return obj
        else:
            return obj
    
    def usage_in_period(self, hours: int = 24) -> int:
        """Get usage count in time period (Rails pattern)"""
        if not self.usage_log:
            return 0
        
        cutoff = datetime.now() - timedelta(hours=hours)
        return len([log for log in self.usage_log 
                   if 'timestamp' in log and 
                   datetime.fromisoformat(log['timestamp']) > cutoff])
    
    def average_render_time(self) -> float:
        """Calculate average render time (Rails pattern)"""
        return self.render_time_ms
    
    def performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics (Rails pattern)"""
        return {
            'template_id': self.template_id,
            'usage_count': self.usage_count,
            'usage_last_24h': self.usage_in_period(24),
            'usage_last_7d': self.usage_in_period(24 * 7),
            'avg_render_time_ms': self.average_render_time(),
            'complexity_score': self.complexity_score,
            'size_bytes': self.size_bytes,
            'variable_count': len(self.variables or []),
            'popular': self.popular_(),
            'complex': self.complex_()
        }
    
    def health_report(self) -> Dict[str, Any]:
        """Generate template health report (Rails pattern)"""
        return {
            'template_id': self.template_id,
            'healthy': not self.needs_attention_(),
            'active': self.active_(),
            'usable': self.usable_(),
            'status': self.status.value,
            'valid': self.valid_(),
            'approved': self.approved_(),
            'pending_approval': self.pending_approval_(),
            'deprecated': self.deprecated_(),
            'needs_attention': self.needs_attention_(),
            'validation_errors_count': len(self.validation_errors or []),
            'performance_metrics': self.performance_metrics()
        }
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary (Rails pattern)"""
        result = {
            'id': self.id,
            'template_id': self.template_id,
            'name': self.name,
            'slug': self.slug,
            'description': self.description,
            'template_type': self.template_type.value,
            'status': self.status.value,
            'scope': self.scope.value,
            'engine': self.engine.value,
            'version': self.version,
            'category': self.category,
            'language': self.language,
            'is_public': self.is_public,
            'approved': self.approved,
            'usage_count': self.usage_count,
            'complexity_score': self.complexity_score,
            'size_bytes': self.size_bytes,
            'tags': self.tags,
            'active': self.active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_sensitive:
            result.update({
                'content': self.content,
                'html_content': self.html_content,
                'variables': self.variables,
                'required_variables': self.required_variables,
                'default_values': self.default_values,
                'validation_errors': self.validation_errors,
                'test_data': self.test_data,
                'metadata': self.extra_metadata,
                'usage_log': self.usage_log[-10:] if self.usage_log else []  # Last 10 usage entries
            })
        
        return result