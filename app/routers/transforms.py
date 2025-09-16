"""
Data Transformation Router - Manage data transformations and field-level transforms
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.database import get_db
from app.models.user import User
from app.auth.dependencies import get_current_user, require_permissions
from app.services.audit_service import AuditService
from app.services.transform_service import TransformService
from pydantic import BaseModel

router = APIRouter(prefix="/transforms", tags=["data-transformations"])

class TransformCreate(BaseModel):
    name: str
    description: Optional[str] = None
    transform_type: str  # "field", "record", "batch", "stream"
    source_schema: Dict[str, Any]
    target_schema: Dict[str, Any]
    transform_config: Dict[str, Any]
    is_active: bool = True

class TransformUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    transform_config: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None

class TransformResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    transform_type: str
    source_schema: Dict[str, Any]
    target_schema: Dict[str, Any]
    transform_config: Dict[str, Any]
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime]
    created_by_id: int
    org_id: int

class AttributeTransformCreate(BaseModel):
    field_name: str
    source_type: str
    target_type: str
    transform_function: str
    transform_params: Optional[Dict[str, Any]] = None
    validation_rules: Optional[List[Dict[str, Any]]] = None
    is_required: bool = True

class AttributeTransformResponse(BaseModel):
    id: int
    transform_id: int
    field_name: str
    source_type: str
    target_type: str
    transform_function: str
    transform_params: Optional[Dict[str, Any]]
    validation_rules: Optional[List[Dict[str, Any]]]
    is_required: bool
    created_at: datetime

class TransformExecutionRequest(BaseModel):
    input_data: List[Dict[str, Any]]
    validate_output: bool = True
    dry_run: bool = False

class TransformExecutionResult(BaseModel):
    success: bool
    transformed_data: Optional[List[Dict[str, Any]]] = None
    validation_results: Optional[List[Dict[str, Any]]] = None
    execution_stats: Dict[str, Any]
    errors: Optional[List[Dict[str, Any]]] = None

@router.get("/", response_model=List[TransformResponse])
async def list_transforms(
    skip: int = 0,
    limit: int = 100,
    transform_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List data transforms for the organization"""
    
    # Import model here to avoid circular imports
    from app.models.transform import Transform
    
    query = db.query(Transform).filter(Transform.org_id == current_user.org_id)
    
    if transform_type:
        query = query.filter(Transform.transform_type == transform_type)
    
    if is_active is not None:
        query = query.filter(Transform.is_active == is_active)
    
    transforms = query.offset(skip).limit(limit).all()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="transform.list",
        resource_type="transform",
        details={"count": len(transforms), "filters": {"type": transform_type, "active": is_active}}
    )
    
    return transforms

@router.post("/", response_model=TransformResponse)
async def create_transform(
    transform_data: TransformCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["transform.create"]))
):
    """Create a new data transform"""
    
    from app.models.transform import Transform
    
    # Validate transform configuration
    transform_service = TransformService()
    validation_result = transform_service.validate_transform_config(
        transform_data.transform_type,
        transform_data.transform_config,
        transform_data.source_schema,
        transform_data.target_schema
    )
    
    if not validation_result.get("valid"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid transform configuration: {validation_result.get('errors')}"
        )
    
    # Create transform
    transform = Transform(
        name=transform_data.name,
        description=transform_data.description,
        transform_type=transform_data.transform_type,
        source_schema=transform_data.source_schema,
        target_schema=transform_data.target_schema,
        transform_config=transform_data.transform_config,
        is_active=transform_data.is_active,
        created_by_id=current_user.id,
        org_id=current_user.org_id
    )
    
    db.add(transform)
    db.commit()
    db.refresh(transform)
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="transform.create",
        resource_type="transform",
        resource_id=transform.id,
        details={
            "name": transform.name,
            "type": transform.transform_type,
            "active": transform.is_active
        }
    )
    
    return transform

@router.get("/{transform_id}", response_model=TransformResponse)
async def get_transform(
    transform_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific transform"""
    
    from app.models.transform import Transform
    
    transform = db.query(Transform).filter(
        Transform.id == transform_id,
        Transform.org_id == current_user.org_id
    ).first()
    
    if not transform:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    return transform

@router.put("/{transform_id}", response_model=TransformResponse)
async def update_transform(
    transform_id: int,
    transform_data: TransformUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["transform.update"]))
):
    """Update a transform"""
    
    from app.models.transform import Transform
    
    transform = db.query(Transform).filter(
        Transform.id == transform_id,
        Transform.org_id == current_user.org_id
    ).first()
    
    if not transform:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    # Update fields
    update_data = transform_data.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(transform, field, value)
    
    transform.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(transform)
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="transform.update",
        resource_type="transform",
        resource_id=transform.id,
        details={"updated_fields": list(update_data.keys())}
    )
    
    return transform

@router.delete("/{transform_id}")
async def delete_transform(
    transform_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["transform.delete"]))
):
    """Delete a transform"""
    
    from app.models.transform import Transform
    
    transform = db.query(Transform).filter(
        Transform.id == transform_id,
        Transform.org_id == current_user.org_id
    ).first()
    
    if not transform:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    # Check if transform is being used
    # TODO: Add check for transform usage in flows/pipelines
    
    db.delete(transform)
    db.commit()
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="transform.delete",
        resource_type="transform",
        resource_id=transform_id,
        details={"name": transform.name}
    )
    
    return {"message": "Transform deleted successfully"}

@router.post("/{transform_id}/execute", response_model=TransformExecutionResult)
async def execute_transform(
    transform_id: int,
    execution_request: TransformExecutionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["transform.execute"]))
):
    """Execute a transform against input data"""
    
    from app.models.transform import Transform
    
    transform = db.query(Transform).filter(
        Transform.id == transform_id,
        Transform.org_id == current_user.org_id
    ).first()
    
    if not transform:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    if not transform.is_active:
        raise HTTPException(status_code=400, detail="Transform is not active")
    
    try:
        transform_service = TransformService()
        
        # Execute transform
        result = await transform_service.execute_transform(
            transform=transform,
            input_data=execution_request.input_data,
            validate_output=execution_request.validate_output,
            dry_run=execution_request.dry_run
        )
        
        # Log execution
        background_tasks.add_task(
            _log_transform_execution,
            user_id=current_user.id,
            transform_id=transform_id,
            input_count=len(execution_request.input_data),
            output_count=len(result.get("transformed_data", [])),
            success=result.get("success", False),
            dry_run=execution_request.dry_run
        )
        
        return TransformExecutionResult(
            success=result.get("success", False),
            transformed_data=result.get("transformed_data"),
            validation_results=result.get("validation_results"),
            execution_stats=result.get("execution_stats", {}),
            errors=result.get("errors")
        )
        
    except Exception as e:
        await AuditService.log_action(
            user_id=current_user.id,
            action="transform.execute_failed",
            resource_type="transform",
            resource_id=transform_id,
            details={"error": str(e), "input_count": len(execution_request.input_data)},
            status="error"
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Transform execution failed: {str(e)}"
        )

@router.get("/{transform_id}/attributes", response_model=List[AttributeTransformResponse])
async def list_attribute_transforms(
    transform_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List attribute transforms for a specific transform"""
    
    from app.models.transform import Transform
    from app.models.attribute_transform import AttributeTransform
    
    # Verify transform exists and user has access
    transform = db.query(Transform).filter(
        Transform.id == transform_id,
        Transform.org_id == current_user.org_id
    ).first()
    
    if not transform:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    # Get attribute transforms
    attribute_transforms = db.query(AttributeTransform).filter(
        AttributeTransform.transform_id == transform_id
    ).all()
    
    return attribute_transforms

@router.post("/{transform_id}/attributes", response_model=AttributeTransformResponse)
async def create_attribute_transform(
    transform_id: int,
    attribute_data: AttributeTransformCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["transform.update"]))
):
    """Create an attribute transform for a specific transform"""
    
    from app.models.transform import Transform
    from app.models.attribute_transform import AttributeTransform
    
    # Verify transform exists and user has access
    transform = db.query(Transform).filter(
        Transform.id == transform_id,
        Transform.org_id == current_user.org_id
    ).first()
    
    if not transform:
        raise HTTPException(status_code=404, detail="Transform not found")
    
    # Create attribute transform
    attribute_transform = AttributeTransform(
        transform_id=transform_id,
        field_name=attribute_data.field_name,
        source_type=attribute_data.source_type,
        target_type=attribute_data.target_type,
        transform_function=attribute_data.transform_function,
        transform_params=attribute_data.transform_params,
        validation_rules=attribute_data.validation_rules,
        is_required=attribute_data.is_required
    )
    
    db.add(attribute_transform)
    db.commit()
    db.refresh(attribute_transform)
    
    await AuditService.log_action(
        user_id=current_user.id,
        action="transform.create_attribute",
        resource_type="attribute_transform",
        resource_id=attribute_transform.id,
        details={
            "transform_id": transform_id,
            "field_name": attribute_data.field_name,
            "function": attribute_data.transform_function
        }
    )
    
    return attribute_transform

@router.get("/functions/available")
async def get_available_transform_functions(
    category: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    """Get available transform functions"""
    
    transform_service = TransformService()
    functions = transform_service.get_available_functions(category)
    
    return {"functions": functions}

@router.post("/validate-config")
async def validate_transform_config(
    transform_type: str,
    transform_config: Dict[str, Any],
    source_schema: Dict[str, Any],
    target_schema: Dict[str, Any],
    current_user: User = Depends(get_current_user)
):
    """Validate a transform configuration"""
    
    transform_service = TransformService()
    validation_result = transform_service.validate_transform_config(
        transform_type, transform_config, source_schema, target_schema
    )
    
    return validation_result

@router.post("/preview")
async def preview_transform(
    transform_config: Dict[str, Any],
    source_schema: Dict[str, Any],
    target_schema: Dict[str, Any],
    sample_data: List[Dict[str, Any]],
    current_user: User = Depends(get_current_user)
):
    """Preview transform results with sample data"""
    
    if len(sample_data) > 10:
        raise HTTPException(
            status_code=400,
            detail="Preview limited to 10 records maximum"
        )
    
    try:
        transform_service = TransformService()
        preview_result = await transform_service.preview_transform(
            transform_config=transform_config,
            source_schema=source_schema,
            target_schema=target_schema,
            sample_data=sample_data
        )
        
        await AuditService.log_action(
            user_id=current_user.id,
            action="transform.preview",
            resource_type="transform",
            details={"sample_size": len(sample_data)}
        )
        
        return preview_result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Transform preview failed: {str(e)}"
        )

# Helper functions
async def _log_transform_execution(
    user_id: int,
    transform_id: int,
    input_count: int,
    output_count: int,
    success: bool,
    dry_run: bool
):
    """Log transform execution for analytics"""
    await AuditService.log_action(
        user_id=user_id,
        action="transform.execute",
        resource_type="transform",
        resource_id=transform_id,
        details={
            "input_count": input_count,
            "output_count": output_count,
            "success": success,
            "dry_run": dry_run
        },
        status="success" if success else "error"
    )