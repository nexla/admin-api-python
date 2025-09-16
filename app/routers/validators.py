"""
Validators Router - API endpoints for validation rule management
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.validation_rule import ValidationRule, ValidationRuleType, ValidationSeverity, ValidationScope, ValidationResult, RuleExecution

router = APIRouter()

# Pydantic models
class ValidationRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    rule_type: ValidationRuleType
    severity: ValidationSeverity = ValidationSeverity.ERROR
    scope: ValidationScope = ValidationScope.FIELD
    field_name: Optional[str] = None
    table_name: Optional[str] = None
    rule_expression: str = Field(..., min_length=1)
    error_message: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    conditions: Dict[str, Any] = Field(default_factory=dict)
    is_blocking: bool = True
    auto_fix: bool = False
    auto_fix_expression: Optional[str] = None
    sample_rate: float = Field(1.0, ge=0.0, le=1.0)
    batch_size: int = Field(1000, gt=0)
    timeout_seconds: int = Field(300, gt=0)
    run_on_insert: bool = True
    run_on_update: bool = True
    run_on_delete: bool = False
    run_on_schedule: bool = False
    schedule_expression: Optional[str] = None
    depends_on_rules: List[int] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    project_id: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class ValidationRuleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    severity: Optional[ValidationSeverity] = None
    rule_expression: Optional[str] = None
    error_message: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    conditions: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    is_blocking: Optional[bool] = None
    auto_fix: Optional[bool] = None
    auto_fix_expression: Optional[str] = None
    sample_rate: Optional[float] = Field(None, ge=0.0, le=1.0)
    batch_size: Optional[int] = Field(None, gt=0)
    timeout_seconds: Optional[int] = Field(None, gt=0)
    run_on_insert: Optional[bool] = None
    run_on_update: Optional[bool] = None
    run_on_delete: Optional[bool] = None
    run_on_schedule: Optional[bool] = None
    schedule_expression: Optional[str] = None
    depends_on_rules: Optional[List[int]] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None

class ValidationRuleResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    rule_type: str
    severity: str
    scope: str
    field_name: Optional[str]
    table_name: Optional[str]
    rule_expression: str
    error_message: Optional[str]
    parameters: Dict[str, Any]
    conditions: Dict[str, Any]
    is_active: bool
    is_blocking: bool
    auto_fix: bool
    auto_fix_expression: Optional[str]
    sample_rate: float
    batch_size: int
    timeout_seconds: int
    run_on_insert: bool
    run_on_update: bool
    run_on_delete: bool
    run_on_schedule: bool
    schedule_expression: Optional[str]
    depends_on_rules: List[int]
    tags: List[str]
    created_at: str
    updated_at: Optional[str]
    org_id: int
    project_id: Optional[int]
    created_by_id: int
    metadata: Dict[str, Any]

class ValidationRuleStatsResponse(ValidationRuleResponse):
    execution_count: int
    success_count: int
    failure_count: int
    success_rate: float
    last_execution_at: Optional[str]
    avg_execution_time_ms: Optional[float]

class ValidationExecuteRequest(BaseModel):
    data: Union[Dict[str, Any], List[Dict[str, Any]]]
    context: Dict[str, Any] = Field(default_factory=dict)

class ValidationResultResponse(BaseModel):
    id: int
    rule_id: int
    is_valid: bool
    message: Optional[str]
    error_message: Optional[str]
    severity: Optional[str]
    resource_type: Optional[str]
    resource_id: Optional[int]
    field_name: Optional[str]
    execution_time_ms: Optional[float]
    data_sample: Optional[Dict[str, Any]]
    fix_applied: bool
    fix_details: Optional[Dict[str, Any]]
    validated_at: str

class RuleExecutionResponse(BaseModel):
    id: int
    rule_id: int
    execution_type: str
    triggered_by_id: Optional[int]
    started_at: str
    completed_at: Optional[str]
    status: str
    duration_seconds: Optional[float]
    records_processed: int
    records_valid: int
    records_invalid: int
    fixes_applied: int
    success_rate: float
    error_message: Optional[str]
    execution_context: Optional[Dict[str, Any]]

class RuleTestRequest(BaseModel):
    test_data: Dict[str, Any]

# Validation rule management endpoints
@router.post("/", response_model=ValidationRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_validation_rule(
    rule_data: ValidationRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new validation rule"""
    try:
        # Validate dependencies
        if rule_data.depends_on_rules:
            dependency_count = db.query(ValidationRule).filter(
                ValidationRule.id.in_(rule_data.depends_on_rules),
                ValidationRule.org_id == current_user.default_org_id
            ).count()
            
            if dependency_count != len(rule_data.depends_on_rules):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="One or more dependency rules not found"
                )
        
        validation_rule = ValidationRule(
            name=rule_data.name,
            description=rule_data.description,
            rule_type=rule_data.rule_type.value,
            severity=rule_data.severity.value,
            scope=rule_data.scope.value,
            field_name=rule_data.field_name,
            table_name=rule_data.table_name,
            rule_expression=rule_data.rule_expression,
            error_message=rule_data.error_message,
            parameters=rule_data.parameters,
            conditions=rule_data.conditions,
            is_blocking=rule_data.is_blocking,
            auto_fix=rule_data.auto_fix,
            auto_fix_expression=rule_data.auto_fix_expression,
            sample_rate=rule_data.sample_rate,
            batch_size=rule_data.batch_size,
            timeout_seconds=rule_data.timeout_seconds,
            run_on_insert=rule_data.run_on_insert,
            run_on_update=rule_data.run_on_update,
            run_on_delete=rule_data.run_on_delete,
            run_on_schedule=rule_data.run_on_schedule,
            schedule_expression=rule_data.schedule_expression,
            depends_on_rules=rule_data.depends_on_rules,
            tags=rule_data.tags,
            metadata=rule_data.metadata,
            org_id=current_user.default_org_id,
            project_id=rule_data.project_id,
            created_by_id=current_user.id
        )
        
        db.add(validation_rule)
        db.commit()
        db.refresh(validation_rule)
        
        return ValidationRuleResponse(**validation_rule.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create validation rule: {str(e)}"
        )

@router.get("/", response_model=List[ValidationRuleResponse])
async def list_validation_rules(
    rule_type: Optional[ValidationRuleType] = Query(None),
    severity: Optional[ValidationSeverity] = Query(None),
    scope: Optional[ValidationScope] = Query(None),
    active_only: bool = Query(True),
    table_name: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
    tags: Optional[str] = Query(None, description="Comma-separated list of tags"),
    search: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List validation rules with filtering options"""
    try:
        query = db.query(ValidationRule).filter(
            ValidationRule.org_id == current_user.default_org_id
        )
        
        # Apply filters
        if rule_type:
            query = query.filter(ValidationRule.rule_type == rule_type.value)
        
        if severity:
            query = query.filter(ValidationRule.severity == severity.value)
        
        if scope:
            query = query.filter(ValidationRule.scope == scope.value)
        
        if active_only:
            query = query.filter(ValidationRule.is_active == True)
        
        if table_name:
            query = query.filter(ValidationRule.table_name == table_name)
        
        if project_id:
            query = query.filter(
                (ValidationRule.project_id == project_id) |
                (ValidationRule.project_id.is_(None))
            )
        
        if tags:
            tag_list = [tag.strip() for tag in tags.split(",")]
            for tag in tag_list:
                query = query.filter(ValidationRule.tags.op('JSON_CONTAINS')(f'"{tag}"'))
        
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (ValidationRule.name.like(search_pattern)) |
                (ValidationRule.description.like(search_pattern))
            )
        
        # Order by name
        query = query.order_by(ValidationRule.name)
        
        # Apply pagination
        rules = query.offset(offset).limit(limit).all()
        
        return [ValidationRuleResponse(**rule.to_dict()) for rule in rules]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list validation rules: {str(e)}"
        )

@router.get("/{rule_id}", response_model=ValidationRuleStatsResponse)
async def get_validation_rule(
    rule_id: int,
    include_stats: bool = Query(True),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get validation rule details"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    return ValidationRuleStatsResponse(**rule.to_dict(include_stats=include_stats))

@router.put("/{rule_id}", response_model=ValidationRuleResponse)
async def update_validation_rule(
    rule_id: int,
    rule_data: ValidationRuleUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update validation rule"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    try:
        # Validate dependencies if updated
        if rule_data.depends_on_rules is not None:
            dependency_count = db.query(ValidationRule).filter(
                ValidationRule.id.in_(rule_data.depends_on_rules),
                ValidationRule.org_id == current_user.default_org_id
            ).count()
            
            if dependency_count != len(rule_data.depends_on_rules):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="One or more dependency rules not found"
                )
        
        # Update fields
        update_data = rule_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            if hasattr(rule, field):
                if field in ['severity', 'scope'] and hasattr(value, 'value'):
                    setattr(rule, field, value.value)
                else:
                    setattr(rule, field, value)
        
        rule.updated_at = datetime.now()
        
        db.commit()
        db.refresh(rule)
        
        return ValidationRuleResponse(**rule.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update validation rule: {str(e)}"
        )

@router.delete("/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_validation_rule(
    rule_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete validation rule"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    # Check if other rules depend on this rule
    dependent_rules = rule.get_dependent_rules(db)
    if dependent_rules:
        rule_names = [dep_rule.name for dep_rule in dependent_rules]
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete rule with dependencies: {', '.join(rule_names)}"
        )
    
    try:
        db.delete(rule)
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete validation rule: {str(e)}"
        )

# Rule execution endpoints
@router.post("/{rule_id}/execute", response_model=ValidationResultResponse)
async def execute_validation_rule(
    rule_id: int,
    execute_data: ValidationExecuteRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Execute validation rule against provided data"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    if not rule.active_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot execute inactive rule"
        )
    
    try:
        # Execute validation
        result = rule.validate_data(execute_data.data, execute_data.context)
        
        # Save result to database
        db.add(result)
        db.commit()
        db.refresh(result)
        
        return ValidationResultResponse(**result.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute validation rule: {str(e)}"
        )

@router.post("/{rule_id}/test", response_model=Dict[str, Any])
async def test_validation_rule(
    rule_id: int,
    test_data: RuleTestRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Test validation rule expression with sample data"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    try:
        test_result = rule.test_expression(test_data.test_data)
        return test_result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test validation rule: {str(e)}"
        )

@router.post("/{rule_id}/clone", response_model=ValidationRuleResponse, status_code=status.HTTP_201_CREATED)
async def clone_validation_rule(
    rule_id: int,
    new_name: str = Query(..., min_length=1),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Clone validation rule"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    try:
        cloned_rule = rule.clone_()
        cloned_rule.name = new_name
        cloned_rule.created_by_id = current_user.id
        
        db.add(cloned_rule)
        db.commit()
        db.refresh(cloned_rule)
        
        return ValidationRuleResponse(**cloned_rule.to_dict())
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clone validation rule: {str(e)}"
        )

# Rule results and execution history
@router.get("/{rule_id}/results", response_model=List[ValidationResultResponse])
async def get_validation_results(
    rule_id: int,
    is_valid: Optional[bool] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get validation results for a rule"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    try:
        query = db.query(ValidationResult).filter(
            ValidationResult.rule_id == rule_id
        )
        
        if is_valid is not None:
            query = query.filter(ValidationResult.is_valid == is_valid)
        
        query = query.order_by(ValidationResult.validated_at.desc())
        results = query.offset(offset).limit(limit).all()
        
        return [ValidationResultResponse(**result.to_dict()) for result in results]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get validation results: {str(e)}"
        )

@router.get("/{rule_id}/executions", response_model=List[RuleExecutionResponse])
async def get_rule_executions(
    rule_id: int,
    execution_type: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get execution history for a rule"""
    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id,
        ValidationRule.org_id == current_user.default_org_id
    ).first()
    
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found"
        )
    
    try:
        query = db.query(RuleExecution).filter(
            RuleExecution.rule_id == rule_id
        )
        
        if execution_type:
            query = query.filter(RuleExecution.execution_type == execution_type)
        
        if status_filter:
            query = query.filter(RuleExecution.status == status_filter)
        
        query = query.order_by(RuleExecution.started_at.desc())
        executions = query.offset(offset).limit(limit).all()
        
        return [RuleExecutionResponse(**execution.to_dict()) for execution in executions]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get rule executions: {str(e)}"
        )

# Analytics and reporting
@router.get("/analytics/summary", response_model=Dict[str, Any])
async def get_validation_analytics(
    days: int = Query(30, ge=1, le=365),
    project_id: Optional[int] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get validation analytics summary"""
    try:
        since = datetime.now() - timedelta(days=days)
        
        base_query = db.query(ValidationRule).filter(
            ValidationRule.org_id == current_user.default_org_id
        )
        
        if project_id:
            base_query = base_query.filter(
                (ValidationRule.project_id == project_id) |
                (ValidationRule.project_id.is_(None))
            )
        
        analytics = {
            "total_rules": base_query.count(),
            "active_rules": base_query.filter(ValidationRule.is_active == True).count(),
            "blocking_rules": base_query.filter(ValidationRule.is_blocking == True).count(),
            "scheduled_rules": base_query.filter(ValidationRule.run_on_schedule == True).count()
        }
        
        # Get execution statistics
        results_query = db.query(ValidationResult).join(ValidationRule).filter(
            ValidationRule.org_id == current_user.default_org_id,
            ValidationResult.validated_at >= since
        )
        
        if project_id:
            results_query = results_query.filter(
                (ValidationRule.project_id == project_id) |
                (ValidationRule.project_id.is_(None))
            )
        
        total_validations = results_query.count()
        successful_validations = results_query.filter(ValidationResult.is_valid == True).count()
        
        analytics.update({
            "total_validations": total_validations,
            "successful_validations": successful_validations,
            "failed_validations": total_validations - successful_validations,
            "success_rate": (successful_validations / total_validations * 100) if total_validations > 0 else 0
        })
        
        # Get rule type distribution
        rule_types = {}
        for rule_type in ValidationRuleType:
            count = base_query.filter(ValidationRule.rule_type == rule_type.value).count()
            if count > 0:
                rule_types[rule_type.value] = count
        
        analytics["rule_type_distribution"] = rule_types
        
        return analytics
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get validation analytics: {str(e)}"
        )