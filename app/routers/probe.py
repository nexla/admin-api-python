"""
Data Source Probing Router - Test and validate data source connections
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
import json

from app.database import get_db
from app.models.user import User
from app.models.data_source import DataSource
from app.models.data_credentials import DataCredentials
from app.auth.dependencies import get_current_user, require_permissions
from app.services.audit_service import AuditService
from app.services.probe_service import ProbeService
from pydantic import BaseModel

router = APIRouter(prefix="/probe", tags=["data-source-probing"])

class ProbeRequest(BaseModel):
    connector_type: str
    connection_config: Dict[str, Any]
    credentials_id: Optional[int] = None
    test_query: Optional[str] = None
    timeout_seconds: Optional[int] = 30

class ProbeResult(BaseModel):
    success: bool
    connection_status: str
    response_time_ms: float
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    schema_info: Optional[Dict[str, Any]] = None
    sample_data: Optional[List[Dict[str, Any]]] = None

class DataSourceProbeRequest(BaseModel):
    test_query: Optional[str] = None
    include_schema: bool = True
    include_sample: bool = False
    sample_limit: int = 10

class SchemaProbeResult(BaseModel):
    tables: List[Dict[str, Any]]
    views: Optional[List[Dict[str, Any]]] = None
    procedures: Optional[List[Dict[str, Any]]] = None
    total_tables: int
    database_info: Dict[str, Any]

@router.post("/test-connection", response_model=ProbeResult)
async def test_connection(
    probe_request: ProbeRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["probe.test_connection"]))
):
    """Test a data source connection with given configuration"""
    
    try:
        start_time = datetime.utcnow()
        
        # Get credentials if provided
        credentials = None
        if probe_request.credentials_id:
            credentials = db.query(DataCredentials).filter(
                DataCredentials.id == probe_request.credentials_id,
                DataCredentials.org_id == current_user.org_id
            ).first()
            
            if not credentials:
                raise HTTPException(status_code=404, detail="Credentials not found")
        
        # Initialize probe service
        probe_service = ProbeService()
        
        # Perform connection test
        result = await probe_service.test_connection(
            connector_type=probe_request.connector_type,
            config=probe_request.connection_config,
            credentials=credentials,
            test_query=probe_request.test_query,
            timeout=probe_request.timeout_seconds
        )
        
        # Calculate response time
        end_time = datetime.utcnow()
        response_time = (end_time - start_time).total_seconds() * 1000
        
        # Log the probe attempt
        background_tasks.add_task(
            _log_probe_attempt,
            user_id=current_user.id,
            connector_type=probe_request.connector_type,
            success=result.get("success", False),
            response_time=response_time
        )
        
        return ProbeResult(
            success=result.get("success", False),
            connection_status=result.get("status", "unknown"),
            response_time_ms=response_time,
            error_message=result.get("error_message"),
            metadata=result.get("metadata", {}),
            schema_info=result.get("schema_info"),
            sample_data=result.get("sample_data")
        )
        
    except Exception as e:
        await AuditService.log_action(
            user_id=current_user.id,
            action="probe.test_connection_failed",
            resource_type="probe",
            details={
                "connector_type": probe_request.connector_type,
                "error": str(e)
            },
            status="error"
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Connection test failed: {str(e)}"
        )

@router.get("/data-source/{data_source_id}/test", response_model=ProbeResult)
async def test_existing_data_source(
    data_source_id: int,
    background_tasks: BackgroundTasks,
    test_request: DataSourceProbeRequest = Depends(),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["probe.test_data_source"]))
):
    """Test an existing data source connection"""
    
    # Get data source
    data_source = db.query(DataSource).filter(
        DataSource.id == data_source_id,
        DataSource.org_id == current_user.org_id
    ).first()
    
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    try:
        start_time = datetime.utcnow()
        
        # Initialize probe service
        probe_service = ProbeService()
        
        # Get data source credentials
        credentials = None
        if data_source.credentials_id:
            credentials = db.query(DataCredentials).filter(
                DataCredentials.id == data_source.credentials_id
            ).first()
        
        # Perform connection test
        result = await probe_service.test_data_source(
            data_source=data_source,
            credentials=credentials,
            test_query=test_request.test_query,
            include_schema=test_request.include_schema,
            include_sample=test_request.include_sample,
            sample_limit=test_request.sample_limit
        )
        
        # Calculate response time
        end_time = datetime.utcnow()
        response_time = (end_time - start_time).total_seconds() * 1000
        
        # Update data source last tested timestamp
        data_source.last_tested_at = datetime.utcnow()
        data_source.test_status = "success" if result.get("success") else "failed"
        db.commit()
        
        # Log the probe attempt
        background_tasks.add_task(
            _log_probe_attempt,
            user_id=current_user.id,
            connector_type=data_source.connector_type,
            success=result.get("success", False),
            response_time=response_time,
            data_source_id=data_source_id
        )
        
        return ProbeResult(
            success=result.get("success", False),
            connection_status=result.get("status", "unknown"),
            response_time_ms=response_time,
            error_message=result.get("error_message"),
            metadata=result.get("metadata", {}),
            schema_info=result.get("schema_info"),
            sample_data=result.get("sample_data")
        )
        
    except Exception as e:
        # Update data source test status
        data_source.test_status = "failed"
        data_source.last_tested_at = datetime.utcnow()
        db.commit()
        
        await AuditService.log_action(
            user_id=current_user.id,
            action="probe.test_data_source_failed",
            resource_type="data_source",
            resource_id=data_source_id,
            details={"error": str(e)},
            status="error"
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Data source test failed: {str(e)}"
        )

@router.get("/data-source/{data_source_id}/schema", response_model=SchemaProbeResult)
async def probe_data_source_schema(
    data_source_id: int,
    include_views: bool = True,
    include_procedures: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["probe.schema"]))
):
    """Probe and retrieve schema information from a data source"""
    
    # Get data source
    data_source = db.query(DataSource).filter(
        DataSource.id == data_source_id,
        DataSource.org_id == current_user.org_id
    ).first()
    
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    try:
        # Initialize probe service
        probe_service = ProbeService()
        
        # Get credentials
        credentials = None
        if data_source.credentials_id:
            credentials = db.query(DataCredentials).filter(
                DataCredentials.id == data_source.credentials_id
            ).first()
        
        # Probe schema
        schema_info = await probe_service.probe_schema(
            data_source=data_source,
            credentials=credentials,
            include_views=include_views,
            include_procedures=include_procedures
        )
        
        await AuditService.log_action(
            user_id=current_user.id,
            action="probe.schema",
            resource_type="data_source",
            resource_id=data_source_id,
            details={
                "tables_count": len(schema_info.get("tables", [])),
                "include_views": include_views,
                "include_procedures": include_procedures
            }
        )
        
        return SchemaProbeResult(
            tables=schema_info.get("tables", []),
            views=schema_info.get("views") if include_views else None,
            procedures=schema_info.get("procedures") if include_procedures else None,
            total_tables=len(schema_info.get("tables", [])),
            database_info=schema_info.get("database_info", {})
        )
        
    except Exception as e:
        await AuditService.log_action(
            user_id=current_user.id,
            action="probe.schema_failed",
            resource_type="data_source",
            resource_id=data_source_id,
            details={"error": str(e)},
            status="error"
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Schema probing failed: {str(e)}"
        )

@router.post("/data-source/{data_source_id}/query")
async def execute_test_query(
    data_source_id: int,
    query: str,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions(["probe.execute_query"]))
):
    """Execute a test query against a data source"""
    
    # Get data source
    data_source = db.query(DataSource).filter(
        DataSource.id == data_source_id,
        DataSource.org_id == current_user.org_id
    ).first()
    
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    try:
        # Validate query (basic safety checks)
        if not _is_safe_query(query):
            raise HTTPException(
                status_code=400, 
                detail="Query contains potentially unsafe operations"
            )
        
        # Initialize probe service
        probe_service = ProbeService()
        
        # Get credentials
        credentials = None
        if data_source.credentials_id:
            credentials = db.query(DataCredentials).filter(
                DataCredentials.id == data_source.credentials_id
            ).first()
        
        # Execute query
        result = await probe_service.execute_query(
            data_source=data_source,
            credentials=credentials,
            query=query,
            limit=limit
        )
        
        await AuditService.log_action(
            user_id=current_user.id,
            action="probe.execute_query",
            resource_type="data_source",
            resource_id=data_source_id,
            details={
                "query": query[:200] + "..." if len(query) > 200 else query,
                "rows_returned": len(result.get("data", [])),
                "limit": limit
            }
        )
        
        return {
            "success": True,
            "data": result.get("data", []),
            "columns": result.get("columns", []),
            "row_count": len(result.get("data", [])),
            "execution_time_ms": result.get("execution_time_ms", 0)
        }
        
    except Exception as e:
        await AuditService.log_action(
            user_id=current_user.id,
            action="probe.execute_query_failed",
            resource_type="data_source",
            resource_id=data_source_id,
            details={
                "query": query[:200] + "..." if len(query) > 200 else query,
                "error": str(e)
            },
            status="error"
        )
        
        raise HTTPException(
            status_code=500,
            detail=f"Query execution failed: {str(e)}"
        )

@router.get("/supported-connectors")
async def get_supported_connectors(
    current_user: User = Depends(get_current_user)
):
    """Get list of supported connector types and their capabilities"""
    
    connectors = {
        "mysql": {
            "name": "MySQL",
            "description": "MySQL database connector",
            "capabilities": ["schema_probe", "query_execution", "data_sampling"],
            "required_config": ["host", "port", "database"],
            "optional_config": ["ssl_mode", "timeout"]
        },
        "postgresql": {
            "name": "PostgreSQL", 
            "description": "PostgreSQL database connector",
            "capabilities": ["schema_probe", "query_execution", "data_sampling"],
            "required_config": ["host", "port", "database"],
            "optional_config": ["ssl_mode", "timeout"]
        },
        "snowflake": {
            "name": "Snowflake",
            "description": "Snowflake data warehouse connector",
            "capabilities": ["schema_probe", "query_execution", "data_sampling"],
            "required_config": ["account", "database", "warehouse"],
            "optional_config": ["role", "timeout"]
        },
        "bigquery": {
            "name": "Google BigQuery",
            "description": "Google BigQuery connector",
            "capabilities": ["schema_probe", "query_execution", "data_sampling"],
            "required_config": ["project_id", "dataset"],
            "optional_config": ["location", "timeout"]
        },
        "redshift": {
            "name": "Amazon Redshift",
            "description": "Amazon Redshift connector", 
            "capabilities": ["schema_probe", "query_execution", "data_sampling"],
            "required_config": ["host", "port", "database"],
            "optional_config": ["ssl_mode", "timeout"]
        }
    }
    
    return {"supported_connectors": connectors}

# Helper functions
async def _log_probe_attempt(
    user_id: int,
    connector_type: str,
    success: bool,
    response_time: float,
    data_source_id: Optional[int] = None
):
    """Log probe attempt for analytics"""
    await AuditService.log_action(
        user_id=user_id,
        action="probe.connection_test",
        resource_type="data_source" if data_source_id else "probe",
        resource_id=data_source_id,
        details={
            "connector_type": connector_type,
            "success": success,
            "response_time_ms": response_time
        },
        status="success" if success else "error"
    )

def _is_safe_query(query: str) -> bool:
    """Basic query safety validation"""
    query_lower = query.lower().strip()
    
    # Block potentially dangerous operations
    dangerous_keywords = [
        "drop", "delete", "insert", "update", "create", "alter", 
        "truncate", "grant", "revoke", "exec", "execute", "call"
    ]
    
    # Allow only SELECT statements and basic functions
    if not query_lower.startswith("select"):
        return False
    
    # Check for dangerous keywords
    for keyword in dangerous_keywords:
        if f" {keyword} " in f" {query_lower} ":
            return False
    
    return True