from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from datetime import datetime

from app.database import get_db
from app.auth import get_current_user
from app.models.user import User
from app.models.data_schema import DataSchema, DataSchemaStatuses, SchemaTypes, SchemaComplexity, ValidationLevels

router = APIRouter()

# Pydantic models for request/response validation
class DataSchemaBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    display_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = Field(None, max_length=10000)
    version: Optional[str] = Field("1.0.0", max_length=50)
    schema_type: Optional[SchemaTypes] = SchemaTypes.JSON_SCHEMA
    complexity: Optional[SchemaComplexity] = SchemaComplexity.SIMPLE
    validation_level: Optional[ValidationLevels] = ValidationLevels.BASIC
    is_template: bool = Field(False)
    is_public: bool = Field(False)
    schema: str = Field(..., min_length=1)
    annotations: Optional[str] = None
    validations: Optional[str] = None
    data_samples: Optional[str] = None
    transformations: Optional[str] = None
    documentation_url: Optional[str] = Field(None, max_length=500)
    help_text: Optional[str] = None
    examples: Optional[str] = None
    tags: Optional[List[str]] = None

class DataSchemaCreate(DataSchemaBase):
    data_credentials_id: Optional[int] = None

class DataSchemaUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    display_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = Field(None, max_length=10000)
    version: Optional[str] = Field(None, max_length=50)
    schema_type: Optional[SchemaTypes] = None
    complexity: Optional[SchemaComplexity] = None
    validation_level: Optional[ValidationLevels] = None
    is_template: Optional[bool] = None
    is_public: Optional[bool] = None
    schema: Optional[str] = None
    annotations: Optional[str] = None
    validations: Optional[str] = None
    data_samples: Optional[str] = None
    transformations: Optional[str] = None
    documentation_url: Optional[str] = Field(None, max_length=500)
    help_text: Optional[str] = None
    examples: Optional[str] = None
    tags: Optional[List[str]] = None

class DataSchemaResponse(DataSchemaBase):
    id: int
    status: DataSchemaStatuses
    is_active: bool
    is_deprecated: bool
    is_detected: bool
    is_managed: bool
    is_validated: bool
    field_count: int
    nested_depth: int
    usage_count: int
    validation_count: int
    error_count: int
    schema_hash: Optional[str]
    last_used_at: Optional[datetime]
    last_validated_at: Optional[datetime]
    tested_at: Optional[datetime]
    deprecated_at: Optional[datetime]
    archived_at: Optional[datetime]
    owner_id: int
    org_id: Optional[int]
    data_credentials_id: Optional[int]
    copied_from_id: Optional[int]
    created_at: datetime
    updated_at: Optional[datetime]
    
    # Computed properties
    active: bool
    ready_for_production: bool
    has_documentation: bool
    has_samples: bool
    well_tested: bool
    
    class Config:
        from_attributes = True

# Core CRUD operations
@router.get("/", response_model=List[DataSchemaResponse])
async def list_schemas(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    status: Optional[DataSchemaStatuses] = Query(None),
    schema_type: Optional[SchemaTypes] = Query(None),
    complexity: Optional[SchemaComplexity] = Query(None),
    templates_only: bool = Query(False),
    public_only: bool = Query(False),
    validated_only: bool = Query(False),
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of data schemas accessible to the current user."""
    # Start with base query for user's org
    query = db.query(DataSchema).filter(DataSchema.org_id == current_user.org_id)
    
    # Apply filters
    if status:
        query = query.filter(DataSchema.status == status)
    if schema_type:
        query = query.filter(DataSchema.schema_type == schema_type)
    if complexity:
        query = query.filter(DataSchema.complexity == complexity)
    if templates_only:
        query = query.filter(DataSchema.is_template == True)
    if public_only:
        query = query.filter(DataSchema.is_public == True)
    if validated_only:
        query = query.filter(DataSchema.is_validated == True)
    
    # Search functionality
    if search:
        search_term = f"%{search.lower()}%"
        query = query.filter(
            (DataSchema.name.ilike(search_term)) |
            (DataSchema.display_name.ilike(search_term)) |
            (DataSchema.description.ilike(search_term))
        )
    
    schemas = query.offset(offset).limit(limit).all()
    
    # Add computed properties
    for schema in schemas:
        schema.active = schema.active_()
        schema.ready_for_production = schema.ready_for_production_()
        schema.has_documentation = schema.has_documentation_()
        schema.has_samples = schema.has_samples_()
        schema.well_tested = schema.well_tested_()
    
    return schemas

@router.post("/", response_model=DataSchemaResponse, status_code=status.HTTP_201_CREATED)
async def create_schema(
    schema_data: DataSchemaCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create new data schema."""
    # Create schema
    schema = DataSchema(
        name=schema_data.name,
        display_name=schema_data.display_name,
        description=schema_data.description,
        version=schema_data.version,
        schema_type=schema_data.schema_type or SchemaTypes.JSON_SCHEMA,
        complexity=schema_data.complexity or SchemaComplexity.SIMPLE,
        validation_level=schema_data.validation_level or ValidationLevels.BASIC,
        is_template=schema_data.is_template,
        is_public=schema_data.is_public,
        schema=schema_data.schema,
        annotations=schema_data.annotations,
        validations=schema_data.validations,
        data_samples=schema_data.data_samples,
        transformations=schema_data.transformations,
        documentation_url=schema_data.documentation_url,
        help_text=schema_data.help_text,
        examples=schema_data.examples,
        owner_id=current_user.id,
        org_id=current_user.org_id,
        data_credentials_id=schema_data.data_credentials_id
    )
    
    # Set tags if provided
    if schema_data.tags:
        schema.tags = schema_data.tags
    
    # Validate schema
    try:
        validation_result = schema.validate_()
        if not validation_result.is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Schema validation failed: {validation_result.errors}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Schema validation error: {str(e)}"
        )
    
    db.add(schema)
    db.commit()
    db.refresh(schema)
    
    # Add computed properties
    schema.active = schema.active_()
    schema.ready_for_production = schema.ready_for_production_()
    schema.has_documentation = schema.has_documentation_()
    schema.has_samples = schema.has_samples_()
    schema.well_tested = schema.well_tested_()
    
    return schema

@router.get("/{schema_id}", response_model=DataSchemaResponse)
async def get_schema(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get specific data schema by ID."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check access permissions
    if schema.org_id != current_user.org_id and not schema.is_public:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to access this schema"
        )
    
    # Add computed properties
    schema.active = schema.active_()
    schema.ready_for_production = schema.ready_for_production_()
    schema.has_documentation = schema.has_documentation_()
    schema.has_samples = schema.has_samples_()
    schema.well_tested = schema.well_tested_()
    
    return schema

@router.put("/{schema_id}", response_model=DataSchemaResponse)
async def update_schema(
    schema_id: int,
    schema_data: DataSchemaUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update data schema."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.owner_id != current_user.id and schema.org_id != current_user.org_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this schema"
        )
    
    # Update fields
    update_data = schema_data.dict(exclude_unset=True)
    for field, value in update_data.items():
        if hasattr(schema, field):
            setattr(schema, field, value)
    
    # If schema content changed, invalidate validation and update hash
    if 'schema' in update_data:
        schema.is_validated = False
        schema._update_schema_hash()
    
    db.commit()
    db.refresh(schema)
    
    # Add computed properties
    schema.active = schema.active_()
    schema.ready_for_production = schema.ready_for_production_()
    schema.has_documentation = schema.has_documentation_()
    schema.has_samples = schema.has_samples_()
    schema.well_tested = schema.well_tested_()
    
    return schema

@router.delete("/{schema_id}")
async def delete_schema(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete data schema."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this schema"
        )
    
    # Check if schema is in use
    if hasattr(schema, 'data_sets') and schema.data_sets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete schema that is associated with data sets"
        )
    
    db.delete(schema)
    db.commit()
    
    return {"message": "Schema deleted successfully"}

# Schema operations
@router.post("/{schema_id}/validate")
async def validate_schema(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Validate schema definition."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.org_id != current_user.org_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to validate this schema"
        )
    
    try:
        validation_result = schema.validate_()
        db.commit()  # Save validation results
        return validation_result.to_dict()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation error: {str(e)}"
        )

@router.post("/{schema_id}/activate")
async def activate_schema(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Activate schema."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to activate this schema"
        )
    
    try:
        schema.activate_()
        db.commit()
        return {"message": "Schema activated successfully"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{schema_id}/deactivate")
async def deactivate_schema(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deactivate schema."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to deactivate this schema"
        )
    
    schema.deactivate_()
    db.commit()
    return {"message": "Schema deactivated successfully"}

@router.post("/{schema_id}/deprecate")
async def deprecate_schema(
    schema_id: int,
    reason: str = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deprecate schema."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to deprecate this schema"
        )
    
    schema.deprecate_(reason)
    db.commit()
    return {"message": "Schema deprecated successfully"}

@router.post("/{schema_id}/make-template")
async def make_template(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Make schema available as template."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.owner_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to modify this schema"
        )
    
    try:
        schema.make_template_()
        db.commit()
        return {"message": "Schema converted to template successfully"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# Template operations
@router.get("/templates/", response_model=List[DataSchemaResponse])
async def list_templates(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    schema_type: Optional[SchemaTypes] = Query(None),
    public_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of schema templates."""
    query = db.query(DataSchema).filter(DataSchema.is_template == True)
    
    if public_only:
        query = query.filter(DataSchema.is_public == True)
    else:
        # Include user's org templates
        query = query.filter(
            (DataSchema.is_public == True) |
            (DataSchema.org_id == current_user.org_id)
        )
    
    if schema_type:
        query = query.filter(DataSchema.schema_type == schema_type)
    
    templates = query.offset(offset).limit(limit).all()
    
    # Add computed properties
    for template in templates:
        template.active = template.active_()
        template.ready_for_production = template.ready_for_production_()
        template.has_documentation = template.has_documentation_()
        template.has_samples = template.has_samples_()
        template.well_tested = template.well_tested_()
    
    return templates

@router.post("/{schema_id}/copy")
async def copy_schema(
    schema_id: int,
    new_name: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Copy schema from template or existing schema."""
    source_schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not source_schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source schema not found"
        )
    
    # Check if user can access source schema
    if source_schema.org_id != current_user.org_id and not source_schema.is_public:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to copy this schema"
        )
    
    # Create new schema as copy
    new_schema = DataSchema(
        name=new_name or f"Copy of {source_schema.name}",
        display_name=source_schema.display_name,
        description=source_schema.description,
        version="1.0.0",  # Reset version for copy
        schema_type=source_schema.schema_type,
        complexity=source_schema.complexity,
        validation_level=source_schema.validation_level,
        schema=source_schema.schema,
        annotations=source_schema.annotations,
        validations=source_schema.validations,
        data_samples=source_schema.data_samples,
        transformations=source_schema.transformations,
        documentation_url=source_schema.documentation_url,
        help_text=source_schema.help_text,
        examples=source_schema.examples,
        is_template=False,  # Copies are not templates by default
        is_public=False,    # Copies are private by default
        owner_id=current_user.id,
        org_id=current_user.org_id,
        copied_from_id=source_schema.id
    )
    
    # Copy tags
    if source_schema.tags:
        new_schema.tags = source_schema.tags
    
    db.add(new_schema)
    db.commit()
    db.refresh(new_schema)
    
    # Add computed properties
    new_schema.active = new_schema.active_()
    new_schema.ready_for_production = new_schema.ready_for_production_()
    new_schema.has_documentation = new_schema.has_documentation_()
    new_schema.has_samples = new_schema.has_samples_()
    new_schema.well_tested = new_schema.well_tested_()
    
    return new_schema

# Metrics and monitoring
@router.get("/{schema_id}/metrics")
async def get_schema_metrics(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get schema usage metrics."""
    schema = db.query(DataSchema).filter(DataSchema.id == schema_id).first()
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schema not found"
        )
    
    # Check permissions
    if schema.org_id != current_user.org_id and not schema.is_public:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view this schema's metrics"
        )
    
    metrics = schema.get_metrics()
    return metrics.to_dict()