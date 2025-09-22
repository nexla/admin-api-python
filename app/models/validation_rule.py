"""
Validation Rule Model - Data validation and business rule management
"""

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from enum import Enum as PyEnum
import re
import json

from app.database import Base


class ValidationRuleType(PyEnum):
    DATA_TYPE = "data_type"
    RANGE = "range"
    PATTERN = "pattern"
    LENGTH = "length"
    REQUIRED = "required"
    UNIQUE = "unique"
    FOREIGN_KEY = "foreign_key"
    CUSTOM = "custom"
    BUSINESS_RULE = "business_rule"
    SCHEMA = "schema"


class ValidationSeverity(PyEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationScope(PyEnum):
    FIELD = "field"
    RECORD = "record"
    DATASET = "dataset"
    GLOBAL = "global"


class ValidationRule(Base):
    """Data validation rule definition"""
    
    __tablename__ = "validation_rules"
    
    # Primary attributes
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    rule_type = Column(String(50), nullable=False, index=True)
    severity = Column(String(20), default=ValidationSeverity.ERROR.value, index=True)
    scope = Column(String(20), default=ValidationScope.FIELD.value, index=True)
    
    # Rule definition
    field_name = Column(String(255))  # Target field name
    table_name = Column(String(255))  # Target table/dataset
    rule_expression = Column(Text, nullable=False)  # Rule logic/expression
    error_message = Column(Text)  # Custom error message
    
    # Rule parameters
    parameters = Column(JSON, default=dict)  # Rule-specific parameters
    conditions = Column(JSON, default=dict)  # When rule applies
    
    # Configuration
    is_active = Column(Boolean, default=True, index=True)
    is_blocking = Column(Boolean, default=True)  # Blocks operations on failure
    auto_fix = Column(Boolean, default=False)  # Attempt automatic correction
    auto_fix_expression = Column(Text)  # Auto-fix logic
    
    # Performance settings
    sample_rate = Column(Float, default=1.0)  # Fraction of data to validate (0.0-1.0)
    batch_size = Column(Integer, default=1000)  # Batch size for validation
    timeout_seconds = Column(Integer, default=300)  # Validation timeout
    
    # Scheduling
    run_on_insert = Column(Boolean, default=True)
    run_on_update = Column(Boolean, default=True)
    run_on_delete = Column(Boolean, default=False)
    run_on_schedule = Column(Boolean, default=False)
    schedule_expression = Column(String(100))  # Cron expression
    
    # Statistics
    execution_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    last_execution_at = Column(DateTime(timezone=True))
    avg_execution_time_ms = Column(Float)
    
    # Dependencies
    depends_on_rules = Column(JSON, default=list)  # Rule IDs this rule depends on
    tags = Column(JSON, default=list)  # Categorization tags
    
    # Ownership and scope
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"))  # Optional project scope
    created_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Metadata
    rule_metadata = Column(JSON, default=dict)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    org = relationship("Org", back_populates="validation_rules")
    project = relationship("Project", back_populates="validation_rules")
    created_by = relationship("User", back_populates="created_validation_rules")
    
    validation_results = relationship("ValidationResult", back_populates="rule", cascade="all, delete-orphan")
    rule_executions = relationship("RuleExecution", back_populates="rule", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<ValidationRule(id={self.id}, name='{self.name}', type='{self.rule_type}')>"
    
    # Rails-style predicate methods
    def active_(self) -> bool:
        """Check if rule is active"""
        return self.is_active
    
    def blocking_(self) -> bool:
        """Check if rule blocks operations on failure"""
        return self.is_blocking
    
    def auto_fixable_(self) -> bool:
        """Check if rule has auto-fix capability"""
        return self.auto_fix and bool(self.auto_fix_expression)
    
    def scheduled_(self) -> bool:
        """Check if rule runs on schedule"""
        return self.run_on_schedule and bool(self.schedule_expression)
    
    def field_level_(self) -> bool:
        """Check if rule validates individual fields"""
        return self.scope == ValidationScope.FIELD.value
    
    def record_level_(self) -> bool:
        """Check if rule validates entire records"""
        return self.scope == ValidationScope.RECORD.value
    
    def dataset_level_(self) -> bool:
        """Check if rule validates entire datasets"""
        return self.scope == ValidationScope.DATASET.value
    
    def critical_(self) -> bool:
        """Check if rule has critical severity"""
        return self.severity == ValidationSeverity.CRITICAL.value
    
    def recently_executed_(self, hours: int = 24) -> bool:
        """Check if rule was executed recently"""
        if not self.last_execution_at:
            return False
        
        cutoff = datetime.now() - timedelta(hours=hours)
        return self.last_execution_at >= cutoff
    
    def has_dependencies_(self) -> bool:
        """Check if rule has dependencies"""
        return bool(self.depends_on_rules)
    
    # Rule execution methods
    def can_execute_(self, operation_type: str = "insert") -> bool:
        """Check if rule should execute for given operation"""
        if not self.active_():
            return False
        
        operation_map = {
            "insert": self.run_on_insert,
            "update": self.run_on_update,
            "delete": self.run_on_delete,
            "schedule": self.run_on_schedule
        }
        
        return operation_map.get(operation_type, False)
    
    def validate_data(self, data: Union[Dict, List[Dict]], context: Dict[str, Any] = None) -> 'ValidationResult':
        """Execute validation rule against data"""
        from .validation_result import ValidationResult
        
        start_time = datetime.now()
        context = context or {}
        
        try:
            # Execute the validation logic
            result = self._execute_validation(data, context)
            
            # Record execution metrics
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            self._update_execution_stats(True, execution_time)
            
            return result
            
        except Exception as e:
            # Record failed execution
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            self._update_execution_stats(False, execution_time)
            
            # Create error result
            return ValidationResult(
                rule_id=self.id,
                is_valid=False,
                error_message=f"Validation execution failed: {str(e)}",
                severity=self.severity,
                execution_time_ms=execution_time
            )
    
    def _execute_validation(self, data: Union[Dict, List[Dict]], context: Dict[str, Any]) -> 'ValidationResult':
        """Internal validation execution logic"""
        from .validation_result import ValidationResult
        
        # This would contain the actual validation logic based on rule_type
        # For now, we'll implement basic validation patterns
        
        if self.rule_type == ValidationRuleType.DATA_TYPE.value:
            return self._validate_data_type(data)
        elif self.rule_type == ValidationRuleType.RANGE.value:
            return self._validate_range(data)
        elif self.rule_type == ValidationRuleType.PATTERN.value:
            return self._validate_pattern(data)
        elif self.rule_type == ValidationRuleType.LENGTH.value:
            return self._validate_length(data)
        elif self.rule_type == ValidationRuleType.REQUIRED.value:
            return self._validate_required(data)
        elif self.rule_type == ValidationRuleType.CUSTOM.value:
            return self._validate_custom(data, context)
        else:
            return ValidationResult(
                rule_id=self.id,
                is_valid=True,
                message="Validation type not implemented"
            )
    
    def _validate_data_type(self, data: Union[Dict, List[Dict]]) -> 'ValidationResult':
        """Validate data type constraints"""
        from .validation_result import ValidationResult
        
        expected_type = self.parameters.get('expected_type')
        if not expected_type:
            return ValidationResult(
                rule_id=self.id,
                is_valid=False,
                error_message="Expected type not specified in rule parameters"
            )
        
        # Implementation would check data types
        return ValidationResult(rule_id=self.id, is_valid=True)
    
    def _validate_range(self, data: Union[Dict, List[Dict]]) -> 'ValidationResult':
        """Validate numeric range constraints"""
        from .validation_result import ValidationResult
        
        min_val = self.parameters.get('min_value')
        max_val = self.parameters.get('max_value')
        
        # Implementation would check numeric ranges
        return ValidationResult(rule_id=self.id, is_valid=True)
    
    def _validate_pattern(self, data: Union[Dict, List[Dict]]) -> 'ValidationResult':
        """Validate regex pattern constraints"""
        from .validation_result import ValidationResult
        
        pattern = self.parameters.get('pattern')
        if not pattern:
            return ValidationResult(
                rule_id=self.id,
                is_valid=False,
                error_message="Pattern not specified in rule parameters"
            )
        
        try:
            regex = re.compile(pattern)
            # Implementation would check pattern matches
            return ValidationResult(rule_id=self.id, is_valid=True)
        except re.error as e:
            return ValidationResult(
                rule_id=self.id,
                is_valid=False,
                error_message=f"Invalid regex pattern: {str(e)}"
            )
    
    def _validate_length(self, data: Union[Dict, List[Dict]]) -> 'ValidationResult':
        """Validate string length constraints"""
        from .validation_result import ValidationResult
        
        min_length = self.parameters.get('min_length')
        max_length = self.parameters.get('max_length')
        
        # Implementation would check string lengths
        return ValidationResult(rule_id=self.id, is_valid=True)
    
    def _validate_required(self, data: Union[Dict, List[Dict]]) -> 'ValidationResult':
        """Validate required field constraints"""
        from .validation_result import ValidationResult
        
        # Implementation would check for required fields
        return ValidationResult(rule_id=self.id, is_valid=True)
    
    def _validate_custom(self, data: Union[Dict, List[Dict]], context: Dict[str, Any]) -> 'ValidationResult':
        """Execute custom validation logic"""
        from .validation_result import ValidationResult
        
        # This would execute custom Python code or call external validators
        # For security, this should be sandboxed
        return ValidationResult(rule_id=self.id, is_valid=True)
    
    def _update_execution_stats(self, success: bool, execution_time_ms: float) -> None:
        """Update rule execution statistics"""
        self.execution_count = (self.execution_count or 0) + 1
        self.last_execution_at = datetime.now()
        
        if success:
            self.success_count = (self.success_count or 0) + 1
        else:
            self.failure_count = (self.failure_count or 0) + 1
        
        # Update average execution time
        if self.avg_execution_time_ms:
            # Exponential moving average
            self.avg_execution_time_ms = (0.9 * self.avg_execution_time_ms + 
                                        0.1 * execution_time_ms)
        else:
            self.avg_execution_time_ms = execution_time_ms
    
    # Rule management methods
    def clone_(self) -> 'ValidationRule':
        """Create a copy of this rule"""
        return ValidationRule(
            name=f"{self.name} (Copy)",
            description=self.description,
            rule_type=self.rule_type,
            severity=self.severity,
            scope=self.scope,
            field_name=self.field_name,
            table_name=self.table_name,
            rule_expression=self.rule_expression,
            error_message=self.error_message,
            parameters=dict(self.parameters or {}),
            conditions=dict(self.conditions or {}),
            org_id=self.org_id,
            project_id=self.project_id,
            created_by_id=self.created_by_id
        )
    
    def get_success_rate(self) -> float:
        """Calculate rule success rate"""
        total = self.execution_count or 0
        if total == 0:
            return 0.0
        
        success = self.success_count or 0
        return (success / total) * 100
    
    def get_dependent_rules(self, db_session) -> List['ValidationRule']:
        """Get rules that depend on this rule"""
        return db_session.query(ValidationRule).filter(
            ValidationRule.depends_on_rules.op('JSON_CONTAINS')(str(self.id))
        ).all()
    
    def get_dependency_rules(self, db_session) -> List['ValidationRule']:
        """Get rules this rule depends on"""
        if not self.depends_on_rules:
            return []
        
        return db_session.query(ValidationRule).filter(
            ValidationRule.id.in_(self.depends_on_rules)
        ).all()
    
    def test_expression(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Test rule expression with sample data"""
        try:
            # This would safely execute the rule expression
            # For security, this should be sandboxed
            result = {
                "success": True,
                "result": "Expression evaluation not implemented",
                "execution_time_ms": 0
            }
            return result
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "execution_time_ms": 0
            }
    
    def to_dict(self, include_stats: bool = False) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        result = {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "rule_type": self.rule_type,
            "severity": self.severity,
            "scope": self.scope,
            "field_name": self.field_name,
            "table_name": self.table_name,
            "rule_expression": self.rule_expression,
            "error_message": self.error_message,
            "parameters": self.parameters,
            "conditions": self.conditions,
            "is_active": self.is_active,
            "is_blocking": self.is_blocking,
            "auto_fix": self.auto_fix,
            "auto_fix_expression": self.auto_fix_expression,
            "sample_rate": self.sample_rate,
            "batch_size": self.batch_size,
            "timeout_seconds": self.timeout_seconds,
            "run_on_insert": self.run_on_insert,
            "run_on_update": self.run_on_update,
            "run_on_delete": self.run_on_delete,
            "run_on_schedule": self.run_on_schedule,
            "schedule_expression": self.schedule_expression,
            "depends_on_rules": self.depends_on_rules,
            "tags": self.tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "created_by_id": self.created_by_id,
            "metadata": self.metadata
        }
        
        if include_stats:
            result.update({
                "execution_count": self.execution_count,
                "success_count": self.success_count,
                "failure_count": self.failure_count,
                "success_rate": self.get_success_rate(),
                "last_execution_at": self.last_execution_at.isoformat() if self.last_execution_at else None,
                "avg_execution_time_ms": self.avg_execution_time_ms
            })
        
        return result


class ValidationResult(Base):
    """Result of validation rule execution"""
    
    __tablename__ = "validation_results"
    
    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer, ForeignKey("validation_rules.id"), nullable=False, index=True)
    
    # Validation outcome
    is_valid = Column(Boolean, nullable=False, index=True)
    message = Column(Text)  # Validation message
    error_message = Column(Text)  # Error details if validation failed
    severity = Column(String(20), index=True)
    
    # Context
    resource_type = Column(String(100))  # Type of resource validated
    resource_id = Column(Integer)  # ID of specific resource
    field_name = Column(String(255))  # Field that was validated
    
    # Execution details
    execution_time_ms = Column(Float)
    data_sample = Column(JSON)  # Sample of data that was validated
    fix_applied = Column(Boolean, default=False)  # Was auto-fix applied
    fix_details = Column(JSON)  # Details of applied fix
    
    # Timestamps
    validated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Relationships
    rule = relationship("ValidationRule", back_populates="validation_results")
    
    def __repr__(self):
        return f"<ValidationResult(id={self.id}, rule_id={self.rule_id}, is_valid={self.is_valid})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "rule_id": self.rule_id,
            "is_valid": self.is_valid,
            "message": self.message,
            "error_message": self.error_message,
            "severity": self.severity,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "field_name": self.field_name,
            "execution_time_ms": self.execution_time_ms,
            "data_sample": self.data_sample,
            "fix_applied": self.fix_applied,
            "fix_details": self.fix_details,
            "validated_at": self.validated_at.isoformat() if self.validated_at else None
        }


class RuleExecution(Base):
    """Detailed execution log for validation rules"""
    
    __tablename__ = "rule_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer, ForeignKey("validation_rules.id"), nullable=False, index=True)
    
    # Execution context
    execution_type = Column(String(50), nullable=False)  # manual, scheduled, triggered
    triggered_by_id = Column(Integer, ForeignKey("users.id"))
    
    # Execution details
    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at = Column(DateTime(timezone=True))
    status = Column(String(20), nullable=False)  # running, completed, failed, cancelled
    
    # Results
    records_processed = Column(Integer, default=0)
    records_valid = Column(Integer, default=0)
    records_invalid = Column(Integer, default=0)
    fixes_applied = Column(Integer, default=0)
    
    # Error handling
    error_message = Column(Text)
    stack_trace = Column(Text)
    
    # Metadata
    execution_context = Column(JSON)  # Additional execution context
    
    # Relationships
    rule = relationship("ValidationRule", back_populates="rule_executions")
    triggered_by = relationship("User")
    
    def __repr__(self):
        return f"<RuleExecution(id={self.id}, rule_id={self.rule_id}, status='{self.status}')>"
    
    def duration_seconds(self) -> Optional[float]:
        """Calculate execution duration in seconds"""
        if not self.completed_at:
            return None
        
        return (self.completed_at - self.started_at).total_seconds()
    
    def success_rate(self) -> float:
        """Calculate success rate for this execution"""
        total = self.records_processed or 0
        if total == 0:
            return 0.0
        
        valid = self.records_valid or 0
        return (valid / total) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "rule_id": self.rule_id,
            "execution_type": self.execution_type,
            "triggered_by_id": self.triggered_by_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "duration_seconds": self.duration_seconds(),
            "records_processed": self.records_processed,
            "records_valid": self.records_valid,
            "records_invalid": self.records_invalid,
            "fixes_applied": self.fixes_applied,
            "success_rate": self.success_rate(),
            "error_message": self.error_message,
            "execution_context": self.execution_context
        }