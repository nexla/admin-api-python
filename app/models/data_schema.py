from enum import Enum as PyEnum
from typing import Optional, Dict, List, Any, Union, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from dataclasses import dataclass
import json
import hashlib
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Index, Enum as SQLEnum, Numeric
from sqlalchemy.orm import relationship, Session, validates
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property

from ..database import Base


logger = logging.getLogger(__name__)


class DataSchemaStatuses(PyEnum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"
    ARCHIVED = "ARCHIVED"
    INVALID = "INVALID"
    TESTING = "TESTING"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class SchemaTypes(PyEnum):
    JSON_SCHEMA = "JSON_SCHEMA"
    AVRO = "AVRO"
    PROTOBUF = "PROTOBUF"
    XML_SCHEMA = "XML_SCHEMA"
    PARQUET = "PARQUET"
    CSV = "CSV"
    CUSTOM = "CUSTOM"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.replace('_', ' ').title()


class SchemaComplexity(PyEnum):
    SIMPLE = "SIMPLE"        # Few fields, basic types
    MODERATE = "MODERATE"    # Nested objects, multiple types
    COMPLEX = "COMPLEX"      # Deep nesting, arrays, unions
    ENTERPRISE = "ENTERPRISE" # Highly complex, multiple schemas
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.title()


class ValidationLevels(PyEnum):
    NONE = "NONE"
    BASIC = "BASIC"
    STRICT = "STRICT"
    ENTERPRISE = "ENTERPRISE"
    
    def get_display_name(self) -> str:
        """Get user-friendly display name"""
        return self.value.title()


@dataclass
class SchemaValidationResult:
    """Result of schema validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    field_count: int
    complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'field_count': self.field_count,
            'complexity_score': self.complexity_score
        }


@dataclass
class SchemaMetrics:
    """Metrics for schema usage and performance"""
    total_usage_count: int = 0
    active_datasets: int = 0
    validation_success_rate: float = 0.0
    average_processing_time_ms: float = 0.0
    last_used_at: Optional[datetime] = None
    error_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_usage_count': self.total_usage_count,
            'active_datasets': self.active_datasets,
            'validation_success_rate': float(self.validation_success_rate),
            'average_processing_time_ms': float(self.average_processing_time_ms),
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'error_count': self.error_count
        }


class DataSchema(Base):
    __tablename__ = "data_schemas"
    
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Core identification
    name = Column(String(255), nullable=False, index=True)
    display_name = Column(String(255))
    description = Column(Text)
    version = Column(String(50), default="1.0.0")
    
    # Schema classification
    schema_type = Column(SQLEnum(SchemaTypes), nullable=False, default=SchemaTypes.JSON_SCHEMA)
    status = Column(SQLEnum(DataSchemaStatuses), nullable=False, default=DataSchemaStatuses.DRAFT)
    complexity = Column(SQLEnum(SchemaComplexity), nullable=False, default=SchemaComplexity.SIMPLE)
    validation_level = Column(SQLEnum(ValidationLevels), nullable=False, default=ValidationLevels.BASIC)
    
    # State flags
    is_active = Column(Boolean, nullable=False, default=True)
    is_deprecated = Column(Boolean, nullable=False, default=False)
    is_template = Column(Boolean, nullable=False, default=False)  # Template schema
    is_public = Column(Boolean, nullable=False, default=False)    # Public schema
    is_detected = Column(Boolean, nullable=False, default=False)  # Auto-detected
    is_managed = Column(Boolean, nullable=False, default=False)   # System-managed
    is_validated = Column(Boolean, nullable=False, default=False)
    
    # Schema data stored as JSON/Text
    schema = Column(Text, nullable=False)        # Main schema definition (JSON)
    annotations = Column(Text)                   # Schema annotations (JSON)
    validations = Column(Text)                   # Validation rules (JSON)
    data_samples = Column(Text)                  # Sample data (JSON array)
    transformations = Column(Text)               # Schema transformations (JSON)
    
    # Schema metadata
    field_count = Column(Integer, default=0)
    nested_depth = Column(Integer, default=1)
    schema_hash = Column(String(255))  # Hash for change detection
    
    # Usage tracking
    usage_count = Column(Integer, default=0)
    validation_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_used_at = Column(DateTime)
    last_validated_at = Column(DateTime)
    
    # Performance metrics
    average_processing_time_ms = Column(Numeric(10, 2), default=0.0)
    max_processing_time_ms = Column(Numeric(10, 2), default=0.0)
    min_processing_time_ms = Column(Numeric(10, 2), default=0.0)
    
    # Lifecycle management
    tested_at = Column(DateTime)
    deprecated_at = Column(DateTime)
    archived_at = Column(DateTime)
    
    # Documentation
    documentation_url = Column(String(500))
    help_text = Column(Text)
    examples = Column(Text)  # JSON examples
    
    # Metadata
    tags = Column(JSON)  # List of tags for categorization
    extra_metadata = Column(JSON)  # Additional metadata
    
    # Relationships
    copied_from_id = Column(Integer, ForeignKey("data_schemas.id"))  # If copied from another schema
    
    # Foreign keys
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"))
    data_credentials_id = Column(Integer, ForeignKey("data_credentials.id"))
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    owner = relationship("User")
    org = relationship("Org")
    data_credentials = relationship("DataCredentials")
    
    # Indexes for performance - defined after columns
    __table_args__ = (
        Index('idx_data_schema_name', 'name'),
        Index('idx_data_schema_status', 'status'),
        Index('idx_data_schema_type', 'schema_type'),
        Index('idx_data_schema_owner', 'owner_id', 'status'),
        Index('idx_data_schema_org', 'org_id', 'status'),
        Index('idx_data_schema_active', 'status', 'is_active'),
    )
    copied_from = relationship("DataSchema", remote_side=[id])
    data_sets = relationship("DataSet", back_populates="data_schema")
    
    # Constants
    MAX_SCHEMA_SIZE = 100000  # Maximum schema size in characters
    MAX_NESTED_DEPTH = 10
    MAX_FIELD_COUNT = 1000
    
    def __repr__(self) -> str:
        return f"<DataSchema(id={self.id}, name='{self.name}', type='{self.schema_type.value}', status='{self.status.value}')>"
    
    def __str__(self) -> str:
        return f"{self.display_name or self.name} ({self.schema_type.get_display_name()})"
    
    # === Rails-style Predicate Methods ===
    
    def active_(self) -> bool:
        """Check if schema is active and available"""
        return self.status == DataSchemaStatuses.ACTIVE and self.is_active and not self.is_deprecated
    
    def inactive_(self) -> bool:
        """Check if schema is inactive"""
        return not self.active_()
    
    def draft_(self) -> bool:
        """Check if schema is in draft status"""
        return self.status == DataSchemaStatuses.DRAFT
    
    def deprecated_(self) -> bool:
        """Check if schema is deprecated"""
        return self.is_deprecated or self.status == DataSchemaStatuses.DEPRECATED
    
    def archived_(self) -> bool:
        """Check if schema is archived"""
        return self.status == DataSchemaStatuses.ARCHIVED or self.archived_at is not None
    
    def invalid_(self) -> bool:
        """Check if schema is invalid"""
        return self.status == DataSchemaStatuses.INVALID
    
    def testing_(self) -> bool:
        """Check if schema is in testing status"""
        return self.status == DataSchemaStatuses.TESTING
    
    def template_(self) -> bool:
        """Check if schema is a template"""
        return self.is_template and self.active_()
    
    def public_(self) -> bool:
        """Check if schema is public"""
        return self.is_public and self.active_()
    
    def detected_(self) -> bool:
        """Check if schema was auto-detected"""
        return self.is_detected
    
    def managed_(self) -> bool:
        """Check if schema is system-managed"""
        return self.is_managed
    
    def validated_(self) -> bool:
        """Check if schema is validated"""
        return self.is_validated and self.last_validated_at is not None
    
    def json_schema_(self) -> bool:
        """Check if schema type is JSON Schema"""
        return self.schema_type == SchemaTypes.JSON_SCHEMA
    
    def avro_(self) -> bool:
        """Check if schema type is Avro"""
        return self.schema_type == SchemaTypes.AVRO
    
    def protobuf_(self) -> bool:
        """Check if schema type is Protobuf"""
        return self.schema_type == SchemaTypes.PROTOBUF
    
    def simple_complexity_(self) -> bool:
        """Check if schema has simple complexity"""
        return self.complexity == SchemaComplexity.SIMPLE
    
    def complex_complexity_(self) -> bool:
        """Check if schema has complex complexity"""
        return self.complexity in [SchemaComplexity.COMPLEX, SchemaComplexity.ENTERPRISE]
    
    def enterprise_complexity_(self) -> bool:
        """Check if schema has enterprise complexity"""
        return self.complexity == SchemaComplexity.ENTERPRISE
    
    def strict_validation_(self) -> bool:
        """Check if schema uses strict validation"""
        return self.validation_level in [ValidationLevels.STRICT, ValidationLevels.ENTERPRISE]
    
    def recently_used_(self, hours: int = 24) -> bool:
        """Check if schema was used recently"""
        if not self.last_used_at:
            return False
        threshold = datetime.utcnow() - timedelta(hours=hours)
        return self.last_used_at > threshold
    
    def has_errors_(self) -> bool:
        """Check if schema has recorded errors"""
        return self.error_count > 0
    
    def well_tested_(self, min_usage: int = 10) -> bool:
        """Check if schema is well tested"""
        return self.usage_count >= min_usage and self.tested_at is not None
    
    def high_usage_(self, threshold: int = 100) -> bool:
        """Check if schema has high usage"""
        return self.usage_count >= threshold
    
    def has_samples_(self) -> bool:
        """Check if schema has data samples"""
        return bool(self.data_samples)
    
    def has_annotations_(self) -> bool:
        """Check if schema has annotations"""
        return bool(self.annotations)
    
    def has_validations_(self) -> bool:
        """Check if schema has validation rules"""
        return bool(self.validations)
    
    def has_transformations_(self) -> bool:
        """Check if schema has transformations"""
        return bool(self.transformations)
    
    def has_documentation_(self) -> bool:
        """Check if schema has documentation"""
        return bool(self.documentation_url or self.help_text or self.examples)
    
    def large_schema_(self, field_threshold: int = 50) -> bool:
        """Check if schema is large (many fields)"""
        return self.field_count >= field_threshold
    
    def deeply_nested_(self, depth_threshold: int = 5) -> bool:
        """Check if schema is deeply nested"""
        return self.nested_depth >= depth_threshold
    
    def schema_changed_(self) -> bool:
        """Check if schema has changed since last hash"""
        if not self.schema_hash:
            return True
        current_hash = self._calculate_schema_hash()
        return current_hash != self.schema_hash
    
    def ready_for_production_(self) -> bool:
        """Check if schema is ready for production use"""
        return (self.active_() and 
                self.validated_() and 
                self.has_samples_() and
                not self.has_errors_())
    
    def copied_schema_(self) -> bool:
        """Check if schema was copied from another schema"""
        return self.copied_from_id is not None
    
    # === Rails-style Bang Methods ===
    
    def activate_(self) -> None:
        """Activate the schema"""
        if self.active_():
            return
        if self.archived_():
            raise ValueError("Cannot activate archived schema")
        if not self.validated_():
            logger.warning(f"Activating unvalidated schema: {self.name}")
        
        self.status = DataSchemaStatuses.ACTIVE
        self.is_active = True
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Activated schema: {self.name}")
    
    def deactivate_(self) -> None:
        """Deactivate the schema"""
        if not self.active_():
            return
        
        self.status = DataSchemaStatuses.DRAFT
        self.is_active = False
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Deactivated schema: {self.name}")
    
    def deprecate_(self, reason: str = None) -> None:
        """Deprecate the schema"""
        if self.deprecated_():
            return
        
        self.is_deprecated = True
        self.status = DataSchemaStatuses.DEPRECATED
        self.deprecated_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Deprecated schema {self.name}: {reason or 'No reason specified'}")
    
    def archive_(self) -> None:
        """Archive the schema"""
        if self.archived_():
            return
        
        self.status = DataSchemaStatuses.ARCHIVED
        self.is_active = False
        self.archived_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Archived schema: {self.name}")
    
    def mark_invalid_(self, reason: str = None) -> None:
        """Mark schema as invalid"""
        if self.invalid_():
            return
        
        self.status = DataSchemaStatuses.INVALID
        self.is_validated = False
        self.updated_at = datetime.utcnow()
        
        logger.warning(f"Marked schema as invalid {self.name}: {reason or 'No reason specified'}")
    
    def mark_as_testing_(self) -> None:
        """Mark schema as in testing"""
        if self.testing_():
            return
        
        self.status = DataSchemaStatuses.TESTING
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Marked schema as testing: {self.name}")
    
    def validate_(self) -> SchemaValidationResult:
        """Validate the schema"""
        result = self._perform_validation()
        
        if result.is_valid:
            self.is_validated = True
            self.last_validated_at = datetime.utcnow()
            self.field_count = result.field_count
            self._update_complexity_from_validation(result)
            self.updated_at = datetime.utcnow()
            logger.info(f"Validated schema: {self.name}")
        else:
            self.is_validated = False
            logger.warning(f"Validation failed for schema {self.name}: {result.errors}")
        
        self.validation_count = (self.validation_count or 0) + 1
        return result
    
    def make_template_(self) -> None:
        """Make schema available as template"""
        if self.template_():
            return
        if not self.validated_():
            raise ValueError("Schema must be validated before making it a template")
        if hasattr(self, 'data_sets') and self.data_sets:
            raise ValueError("Schema associated with data sets cannot be made into template")
        
        self.is_template = True
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Made schema template: {self.name}")
    
    def remove_template_(self) -> None:
        """Remove template status from schema"""
        if not self.template_():
            return
        
        self.is_template = False
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Removed template status from schema: {self.name}")
    
    def make_public_(self) -> None:
        """Make schema public"""
        if self.public_():
            return
        if not self.ready_for_production_():
            raise ValueError("Schema must be ready for production before making it public")
        
        self.is_public = True
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Made schema public: {self.name}")
    
    def make_private_(self) -> None:
        """Make schema private"""
        if not self.public_():
            return
        
        self.is_public = False
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Made schema private: {self.name}")
    
    def increment_usage_(self) -> None:
        """Increment usage counter"""
        self.usage_count = (self.usage_count or 0) + 1
        self.last_used_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def increment_error_(self) -> None:
        """Increment error counter"""
        self.error_count = (self.error_count or 0) + 1
        self.updated_at = datetime.utcnow()
    
    def reset_counters_(self) -> None:
        """Reset usage and error counters"""
        self.usage_count = 0
        self.validation_count = 0
        self.error_count = 0
        self.updated_at = datetime.utcnow()
        
        logger.info(f"Reset counters for schema: {self.name}")
    
    def update_schema_(self, new_schema: str, validate: bool = True) -> None:
        """Update schema definition"""
        if validate:
            # Store old schema in case validation fails
            old_schema = self.schema
            old_is_validated = self.is_validated
            
            self.schema = new_schema
            validation_result = self.validate_()
            
            if not validation_result.is_valid:
                # Restore old schema
                self.schema = old_schema
                self.is_validated = old_is_validated
                raise ValueError(f"Schema validation failed: {validation_result.errors}")
        else:
            self.schema = new_schema
            self.is_validated = False
        
        self._update_schema_hash()
        self.updated_at = datetime.utcnow()
    
    def update_performance_metrics_(self, processing_time_ms: float) -> None:
        """Update performance metrics with new processing time"""
        if self.average_processing_time_ms == 0:
            self.average_processing_time_ms = processing_time_ms
            self.min_processing_time_ms = processing_time_ms
            self.max_processing_time_ms = processing_time_ms
        else:
            # Simple moving average
            count = self.usage_count or 1
            self.average_processing_time_ms = ((self.average_processing_time_ms * (count - 1)) + processing_time_ms) / count
            
            if processing_time_ms < self.min_processing_time_ms:
                self.min_processing_time_ms = processing_time_ms
            if processing_time_ms > self.max_processing_time_ms:
                self.max_processing_time_ms = processing_time_ms
        
        self.updated_at = datetime.utcnow()
    
    def add_tag_(self, tag: str) -> None:
        """Add tag to schema"""
        tags = self.get_tags()
        if tag not in tags:
            tags.append(tag)
            self.tags = tags
            self.updated_at = datetime.utcnow()
    
    def remove_tag_(self, tag: str) -> None:
        """Remove tag from schema"""
        tags = self.get_tags()
        if tag in tags:
            tags.remove(tag)
            self.tags = tags
            self.updated_at = datetime.utcnow()
    
    # === Helper Methods ===
    
    def _calculate_schema_hash(self) -> str:
        """Calculate hash of current schema"""
        if not self.schema:
            return ""
        return hashlib.sha256(self.schema.encode()).hexdigest()
    
    def _update_schema_hash(self) -> None:
        """Update the schema hash"""
        self.schema_hash = self._calculate_schema_hash()
    
    def _perform_validation(self) -> SchemaValidationResult:
        """Perform comprehensive schema validation"""
        errors = []
        warnings = []
        field_count = 0
        complexity_score = 0
        
        # Basic validation
        if not self.schema:
            errors.append("Schema definition is required")
            return SchemaValidationResult(False, errors, warnings, field_count, complexity_score)
        
        # Schema size validation
        if len(self.schema) > self.MAX_SCHEMA_SIZE:
            errors.append(f"Schema size exceeds maximum of {self.MAX_SCHEMA_SIZE} characters")
        
        try:
            # Parse schema based on type
            if self.schema_type == SchemaTypes.JSON_SCHEMA:
                schema_obj = json.loads(self.schema)
                field_count, complexity_score = self._analyze_json_schema(schema_obj)
            else:
                # Basic analysis for other schema types
                field_count = self.schema.count('field') + self.schema.count('property')
                complexity_score = min(len(self.schema) // 1000, 100)
        
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON schema: {e}")
        except Exception as e:
            errors.append(f"Schema parsing error: {e}")
        
        # Field count validation
        if field_count > self.MAX_FIELD_COUNT:
            errors.append(f"Schema has too many fields ({field_count}), maximum is {self.MAX_FIELD_COUNT}")
        
        # Nested depth validation (simplified)
        if complexity_score > 50:  # High complexity score indicates deep nesting
            warnings.append("Schema appears to be deeply nested, consider simplifying")
        
        # Documentation warnings
        if not self.has_documentation_():
            warnings.append("Schema should have documentation for better maintainability")
        
        if not self.has_samples_():
            warnings.append("Schema should have data samples for better understanding")
        
        is_valid = len(errors) == 0
        
        return SchemaValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            field_count=field_count,
            complexity_score=complexity_score
        )
    
    def _analyze_json_schema(self, schema_obj: Dict) -> Tuple[int, int]:
        """Analyze JSON schema and return field count and complexity score"""
        field_count = 0
        complexity_score = 0
        
        def count_fields(obj, depth=0):
            nonlocal field_count, complexity_score
            
            if depth > self.MAX_NESTED_DEPTH:
                complexity_score += 10  # Penalty for deep nesting
                return
            
            if isinstance(obj, dict):
                if 'properties' in obj:
                    properties = obj['properties']
                    field_count += len(properties)
                    complexity_score += depth * 2  # Nested complexity
                    
                    for prop in properties.values():
                        count_fields(prop, depth + 1)
                
                if 'items' in obj:
                    complexity_score += 5  # Array complexity
                    count_fields(obj['items'], depth + 1)
                
                if 'anyOf' in obj or 'oneOf' in obj or 'allOf' in obj:
                    complexity_score += 10  # Union complexity
                    for schema in obj.get('anyOf', []) + obj.get('oneOf', []) + obj.get('allOf', []):
                        count_fields(schema, depth)
        
        count_fields(schema_obj)
        return field_count, min(complexity_score, 100)
    
    def _update_complexity_from_validation(self, validation_result: SchemaValidationResult) -> None:
        """Update complexity based on validation result"""
        if validation_result.complexity_score < 20:
            self.complexity = SchemaComplexity.SIMPLE
        elif validation_result.complexity_score < 50:
            self.complexity = SchemaComplexity.MODERATE
        elif validation_result.complexity_score < 80:
            self.complexity = SchemaComplexity.COMPLEX
        else:
            self.complexity = SchemaComplexity.ENTERPRISE
    
    def get_tags(self) -> List[str]:
        """Get list of tags"""
        if not self.tags:
            return []
        try:
            return json.loads(self.tags) if isinstance(self.tags, str) else self.tags
        except (json.JSONDecodeError, TypeError):
            return []
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata as dictionary"""
        if not self.extra_metadata:
            return {}
        try:
            return json.loads(self.extra_metadata) if isinstance(self.extra_metadata, str) else self.extra_metadata
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata value"""
        current_metadata = self.get_metadata()
        current_metadata[key] = value
        self.extra_metadata = current_metadata
        self.updated_at = datetime.utcnow()
    
    def get_schema_dict(self) -> Dict[str, Any]:
        """Get schema as dictionary"""
        if not self.schema:
            return {}
        try:
            return json.loads(self.schema)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def get_annotations_dict(self) -> Dict[str, Any]:
        """Get annotations as dictionary"""
        if not self.annotations:
            return {}
        try:
            return json.loads(self.annotations)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def get_validations_dict(self) -> Dict[str, Any]:
        """Get validations as dictionary"""
        if not self.validations:
            return {}
        try:
            return json.loads(self.validations)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def get_samples_list(self) -> List[Dict[str, Any]]:
        """Get data samples as list"""
        if not self.data_samples:
            return []
        try:
            return json.loads(self.data_samples)
        except (json.JSONDecodeError, TypeError):
            return []
    
    def get_success_rate(self) -> float:
        """Calculate validation success rate"""
        if not self.validation_count:
            return 0.0
        error_rate = self.error_count / self.validation_count
        return max(0.0, 1.0 - error_rate)
    
    def get_metrics(self) -> SchemaMetrics:
        """Get comprehensive metrics"""
        active_datasets = len(self.data_sets) if self.data_sets else 0
        
        return SchemaMetrics(
            total_usage_count=self.usage_count or 0,
            active_datasets=active_datasets,
            validation_success_rate=self.get_success_rate(),
            average_processing_time_ms=float(self.average_processing_time_ms or 0),
            last_used_at=self.last_used_at,
            error_count=self.error_count or 0
        )
    
    # === Class Methods (Rails-style Scopes) ===
    
    @classmethod
    def active(cls, session: Session):
        """Get all active schemas"""
        return session.query(cls).filter(
            cls.status == DataSchemaStatuses.ACTIVE,
            cls.is_active == True,
            cls.is_deprecated == False
        )
    
    @classmethod
    def templates(cls, session: Session):
        """Get all template schemas"""
        return cls.active(session).filter(cls.is_template == True)
    
    @classmethod
    def public_schemas(cls, session: Session):
        """Get all public schemas"""
        return cls.active(session).filter(cls.is_public == True)
    
    @classmethod
    def validated_schemas(cls, session: Session):
        """Get all validated schemas"""
        return cls.active(session).filter(cls.is_validated == True)
    
    @classmethod
    def by_schema_type(cls, session: Session, schema_type: SchemaTypes):
        """Get schemas by type"""
        return cls.active(session).filter(cls.schema_type == schema_type)
    
    @classmethod
    def by_complexity(cls, session: Session, complexity: SchemaComplexity):
        """Get schemas by complexity"""
        return cls.active(session).filter(cls.complexity == complexity)
    
    @classmethod
    def by_owner(cls, session: Session, owner_id: int):
        """Get schemas by owner"""
        return cls.active(session).filter(cls.owner_id == owner_id)
    
    @classmethod
    def by_org(cls, session: Session, org_id: int):
        """Get schemas by organization"""
        return cls.active(session).filter(cls.org_id == org_id)
    
    @classmethod
    def recently_used(cls, session: Session, hours: int = 24):
        """Get recently used schemas"""
        threshold = datetime.utcnow() - timedelta(hours=hours)
        return cls.active(session).filter(cls.last_used_at > threshold)
    
    @classmethod
    def high_usage(cls, session: Session, threshold: int = 100):
        """Get high usage schemas"""
        return cls.active(session).filter(cls.usage_count >= threshold)
    
    @classmethod
    def with_errors(cls, session: Session):
        """Get schemas with errors"""
        return cls.active(session).filter(cls.error_count > 0)
    
    @classmethod
    def find_by_name(cls, session: Session, name: str):
        """Find schema by name"""
        return session.query(cls).filter(cls.name == name).first()
    
    @classmethod
    def search_by_name_or_description(cls, session: Session, query: str):
        """Search schemas by name or description"""
        search_term = f"%{query.lower()}%"
        return cls.active(session).filter(
            (func.lower(cls.name).like(search_term)) |
            (func.lower(cls.display_name).like(search_term)) |
            (func.lower(cls.description).like(search_term))
        )
    
    @classmethod
    def build_from_input(cls, api_user_info, input_data: dict):
        """Create a new DataSchema from input data (Rails pattern)"""
        if not input_data.get('name'):
            raise ValueError("Schema name is required")
        if not input_data.get('schema'):
            raise ValueError("Schema definition is required")
        
        input_data = dict(input_data)  # Make a copy
        
        # Set defaults
        defaults = {
            'schema_type': SchemaTypes.JSON_SCHEMA,
            'status': DataSchemaStatuses.DRAFT,
            'complexity': SchemaComplexity.SIMPLE,
            'validation_level': ValidationLevels.BASIC,
            'version': '1.0.0',
            'is_active': True,
            'is_deprecated': False,
            'is_template': bool(input_data.get('template', False)),
            'is_public': False,
            'is_detected': False,
            'is_managed': False,
            'is_validated': False,
            'field_count': 0,
            'nested_depth': 1,
            'usage_count': 0,
            'validation_count': 0,
            'error_count': 0
        }
        
        # Merge with input data
        schema_data = {**defaults, **input_data}
        
        # Handle enum conversions
        if isinstance(schema_data.get('schema_type'), str):
            schema_data['schema_type'] = SchemaTypes(schema_data['schema_type'])
        if isinstance(schema_data.get('status'), str):
            schema_data['status'] = DataSchemaStatuses(schema_data['status'])
        if isinstance(schema_data.get('complexity'), str):
            schema_data['complexity'] = SchemaComplexity(schema_data['complexity'])
        if isinstance(schema_data.get('validation_level'), str):
            schema_data['validation_level'] = ValidationLevels(schema_data['validation_level'])
        
        # Remove tags for separate processing
        tags = schema_data.pop('tags', None)
        
        # Handle cloning from existing data schema
        if schema_data.get('data_schema_id'):
            # Would need to implement cloning logic
            raise NotImplementedError("Cloning from data schema not implemented yet")
        
        # Handle cloning from data set
        if schema_data.get('data_set_id'):
            # Would need to implement cloning logic
            raise NotImplementedError("Cloning from data set not implemented yet")
        
        # Set ownership from API user info
        schema_data['owner_id'] = api_user_info.input_owner.id
        schema_data['org_id'] = api_user_info.input_org.id if api_user_info.input_org else None
        
        schema = cls(**schema_data)
        schema._update_schema_hash()
        
        # Tags would be handled separately in a real implementation
        # ResourceTagging.add_owned_tags(schema, tags, api_user_info.input_owner)
        
        return schema
    
    @classmethod
    def clone_from_data_set(cls, data_set, template: bool = False):
        """Clone schema from a data set"""
        return cls(
            owner_id=data_set.owner_id,
            org_id=data_set.org_id,
            name=f"Copied Schema{data_set.name if data_set.name else ''}",
            display_name=f"Schema from {data_set.name or 'Dataset'}",
            description=f"Data schema copied from data set {data_set.id}",
            is_template=template,
            schema=data_set.output_schema,
            annotations=data_set.output_schema_annotations,
            validations=data_set.output_validation_schema,
            data_samples=data_set.data_samples
        )
    
    def latest_version(self) -> int:
        """Get the latest version number for this schema"""
        # This would query DataSchemaVersion table
        # For now, return 1 as default
        return 1
    
    def update_mutable(self, api_user_info, input_data: dict, request=None) -> None:
        """Update mutable fields from input data"""
        if not input_data or not api_user_info:
            return
        
        # Check if trying to make template when associated with data sets
        if input_data.get('template') and hasattr(self, 'data_sets') and self.data_sets:
            raise ValueError("Data schema associated with a data set cannot be marked as a template")
        
        # Handle public field (super user only)
        if 'public' in input_data:
            if not hasattr(api_user_info.user, 'super_user') or not api_user_info.user.super_user:
                raise PermissionError("Only super users can modify public attribute")
            self.is_public = bool(input_data['public'])
        
        # Remove tags for separate processing
        tags = input_data.pop('tags', None)
        
        # Update ownership if changed
        if hasattr(api_user_info, 'input_owner') and self.owner_id != api_user_info.input_owner.id:
            self.owner_id = api_user_info.input_owner.id
        if hasattr(api_user_info, 'input_org') and api_user_info.input_org:
            if self.org_id != api_user_info.input_org.id:
                self.org_id = api_user_info.input_org.id
        
        # Update mutable fields
        mutable_fields = {
            'display_name', 'description', 'version', 'schema_type', 'status',
            'complexity', 'validation_level', 'is_active', 'is_deprecated',
            'is_template', 'is_detected', 'is_managed', 'schema', 'annotations',
            'validations', 'data_samples', 'transformations', 'documentation_url',
            'help_text', 'examples'
        }
        
        for field, value in input_data.items():
            if field in mutable_fields and hasattr(self, field):
                # Handle enum conversions
                if field == 'schema_type' and isinstance(value, str):
                    value = SchemaTypes(value)
                elif field == 'status' and isinstance(value, str):
                    value = DataSchemaStatuses(value)
                elif field == 'complexity' and isinstance(value, str):
                    value = SchemaComplexity(value)
                elif field == 'validation_level' and isinstance(value, str):
                    value = ValidationLevels(value)
                
                setattr(self, field, value)
        
        if 'schema' in input_data:
            self._update_schema_hash()
            self.is_validated = False  # Need revalidation after schema change
        
        self.updated_at = datetime.utcnow()
        
        # Tags would be handled separately in a real implementation
        # ResourceTagging.add_owned_tags(self, tags, api_user_info.input_owner)
    
    # Backward compatibility methods (keeping original method names)
    def is_template(self) -> bool:
        """Check if this is a template schema"""
        return self.template_()
    
    def is_public(self) -> bool:
        """Check if this is a public schema"""
        return self.public_()
    
    def is_detected(self) -> bool:
        """Check if this schema was auto-detected"""
        return self.detected_()
    
    def is_managed(self) -> bool:
        """Check if this schema is system-managed"""
        return self.managed_()
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert schema to dictionary for API responses"""
        data = {
            'id': self.id,
            'name': self.name,
            'display_name': self.display_name,
            'description': self.description,
            'version': self.version,
            'schema_type': self.schema_type.value,
            'status': self.status.value,
            'complexity': self.complexity.value,
            'validation_level': self.validation_level.value,
            'is_active': self.is_active,
            'is_deprecated': self.is_deprecated,
            'is_template': self.is_template,
            'is_public': self.is_public,
            'is_detected': self.is_detected,
            'is_managed': self.is_managed,
            'is_validated': self.is_validated,
            'field_count': self.field_count,
            'nested_depth': self.nested_depth,
            'usage_count': self.usage_count,
            'validation_count': self.validation_count,
            'error_count': self.error_count,
            'success_rate': self.get_success_rate(),
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'last_validated_at': self.last_validated_at.isoformat() if self.last_validated_at else None,
            'tested_at': self.tested_at.isoformat() if self.tested_at else None,
            'documentation_url': self.documentation_url,
            'help_text': self.help_text,
            'tags': self.get_tags(),
            'metadata': self.get_metadata(),
            'copied_from_id': self.copied_from_id,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'data_credentials_id': self.data_credentials_id,
            'latest_version': self.latest_version(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            # Computed properties
            'active': self.active_(),
            'ready_for_production': self.ready_for_production_(),
            'has_documentation': self.has_documentation_(),
            'has_samples': self.has_samples_(),
            'well_tested': self.well_tested_(),
            'metrics': self.get_metrics().to_dict()
        }
        
        if include_sensitive:
            data.update({
                'schema': self.schema,
                'schema_dict': self.get_schema_dict(),
                'annotations': self.annotations,
                'annotations_dict': self.get_annotations_dict(),
                'validations': self.validations,
                'validations_dict': self.get_validations_dict(),
                'data_samples': self.data_samples,
                'samples_list': self.get_samples_list(),
                'transformations': self.transformations,
                'examples': self.examples,
                'schema_hash': self.schema_hash,
                'average_processing_time_ms': float(self.average_processing_time_ms or 0),
                'max_processing_time_ms': float(self.max_processing_time_ms or 0),
                'min_processing_time_ms': float(self.min_processing_time_ms or 0),
                'deprecated_at': self.deprecated_at.isoformat() if self.deprecated_at else None,
                'archived_at': self.archived_at.isoformat() if self.archived_at else None,
                'schema_changed': self.schema_changed_(),
                'validation_result': self._perform_validation().to_dict() if self.schema else None
            })
        else:
            # Public schema view (no sensitive data)
            if self.has_samples_():
                # Include first sample for preview
                samples = self.get_samples_list()
                data['sample_preview'] = samples[0] if samples else None
        
        return data
    
    def to_json(self, include_sensitive: bool = False) -> str:
        """Convert schema to JSON string"""
        return json.dumps(self.to_dict(include_sensitive=include_sensitive), indent=2)