"""
Flow Nodes API endpoints - Python equivalent of Rails flow_nodes controller.
Handles flow node management, configuration, connections, and node-level operations.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, Field
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

from ...database import get_db
from ...auth.jwt_auth import get_current_user, get_current_active_user
from ...auth.rbac import (
    RBACService, SystemPermissions, check_admin_permission
)
from ...models.user import User
from ...models.flow_node import FlowNode
from ...models.flow import Flow
from ...models.data_source import DataSource
from ...models.data_set import DataSet
from ...models.data_sink import DataSink
from ...models.org import Org

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flow_nodes", tags=["Flow Nodes"])


# Pydantic models for request/response
class FlowNodeCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    flow_type: str = Field(..., max_length=50)
    ingestion_mode: str = Field(default="BATCH", pattern="^(BATCH|STREAMING|MICRO_BATCH)$")
    flow_id: Optional[int] = None
    origin_node_id: Optional[int] = None
    shared_origin_node_id: Optional[int] = None
    data_source_id: Optional[int] = None
    data_set_id: Optional[int] = None
    data_sink_id: Optional[int] = None
    org_id: int
    configuration: Optional[Dict[str, Any]] = None

class FlowNodeUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    flow_type: Optional[str] = Field(None, max_length=50)
    ingestion_mode: Optional[str] = Field(None, pattern="^(BATCH|STREAMING|MICRO_BATCH)$")
    origin_node_id: Optional[int] = None
    shared_origin_node_id: Optional[int] = None
    data_source_id: Optional[int] = None
    data_set_id: Optional[int] = None
    data_sink_id: Optional[int] = None
    configuration: Optional[Dict[str, Any]] = None

class FlowNodeConnection(BaseModel):
    source_node_id: int
    target_node_id: int
    connection_type: str = Field(default="data_flow", pattern="^(data_flow|control_flow|error_flow)$")
    configuration: Optional[Dict[str, Any]] = None

class FlowNodeResponse(BaseModel):
    id: int
    name: str
    flow_type: str
    ingestion_mode: str
    flow_id: Optional[int]
    origin_node_id: Optional[int]
    shared_origin_node_id: Optional[int]
    data_source_id: Optional[int]
    data_set_id: Optional[int]
    data_sink_id: Optional[int]
    owner_id: int
    org_id: int
    configuration: Optional[Dict[str, Any]]
    status: str
    last_run_at: Optional[datetime]
    last_run_status: Optional[str]
    created_at: datetime
    updated_at: datetime

class FlowNodeStats(BaseModel):
    total_nodes: int
    nodes_by_type: Dict[str, int]
    nodes_by_status: Dict[str, int]
    connected_nodes: int
    orphaned_nodes: int

class FlowNodeValidationResult(BaseModel):
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    recommendations: List[str]

class FlowNodeExecutionResult(BaseModel):
    node_id: int
    status: str
    execution_time_ms: int
    records_processed: int
    records_output: int
    error_message: Optional[str]
    output_preview: Optional[List[Dict[str, Any]]]


def flow_node_to_response(node: FlowNode) -> FlowNodeResponse:
    """Convert FlowNode model to response"""
    return FlowNodeResponse(
        id=node.id,
        name=node.name,
        flow_type=node.flow_type,
        ingestion_mode=node.ingestion_mode,
        flow_id=None,  # Will be implemented when relationships are enabled
        origin_node_id=node.origin_node_id,
        shared_origin_node_id=node.shared_origin_node_id,
        data_source_id=node.data_source_id,
        data_set_id=node.data_set_id,
        data_sink_id=node.data_sink_id,
        owner_id=node.owner_id,
        org_id=node.org_id,
        configuration={},  # Will be implemented with configuration storage
        status="active",  # Placeholder status
        last_run_at=None,  # Will be implemented with execution tracking
        last_run_status=None,  # Will be implemented with execution tracking
        created_at=node.created_at,
        updated_at=node.updated_at
    )


# Flow Node CRUD operations
@router.get("/", response_model=List[FlowNodeResponse], summary="List Flow Nodes")
async def list_flow_nodes(
    org_id: Optional[int] = Query(None, description="Filter by organization"),
    flow_id: Optional[int] = Query(None, description="Filter by flow"),
    flow_type: Optional[str] = Query(None, description="Filter by node type"),
    data_source_id: Optional[int] = Query(None, description="Filter by data source"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    List flow nodes accessible to the current user.
    
    Equivalent to Rails FlowNodesController#index
    """
    try:
        query = db.query(FlowNode)
        
        # Apply filters
        if org_id:
            query = query.filter(FlowNode.org_id == org_id)
        
        if flow_type:
            query = query.filter(FlowNode.flow_type == flow_type)
        
        if data_source_id:
            query = query.filter(FlowNode.data_source_id == data_source_id)
        
        # TODO: Add flow filtering when relationships are enabled
        # TODO: Filter by user permissions
        
        nodes = query.offset(skip).limit(limit).all()
        
        return [flow_node_to_response(node) for node in nodes]
    
    except Exception as e:
        logger.error(f"List flow nodes error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve flow nodes"
        )


@router.get("/stats", response_model=FlowNodeStats, summary="Get Flow Node Statistics")
async def get_flow_node_stats(
    org_id: Optional[int] = Query(None, description="Filter by organization"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get flow node statistics and analytics.
    
    Equivalent to Rails FlowNodesController#stats
    """
    try:
        # Base query
        query = db.query(FlowNode)
        if org_id:
            query = query.filter(FlowNode.org_id == org_id)
        
        # Calculate statistics
        total_nodes = query.count()
        
        # Nodes by type
        nodes_by_type = {}
        # TODO: Implement actual type aggregation
        
        # Nodes by status
        nodes_by_status = {
            "active": total_nodes,  # Placeholder
            "inactive": 0,
            "error": 0
        }
        
        # Connection statistics
        connected_nodes = query.filter(FlowNode.origin_node_id.isnot(None)).count()
        orphaned_nodes = total_nodes - connected_nodes
        
        return FlowNodeStats(
            total_nodes=total_nodes,
            nodes_by_type=nodes_by_type,
            nodes_by_status=nodes_by_status,
            connected_nodes=connected_nodes,
            orphaned_nodes=orphaned_nodes
        )
    
    except Exception as e:
        logger.error(f"Get flow node stats error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve flow node statistics"
        )


@router.get("/{node_id}", response_model=FlowNodeResponse, summary="Get Flow Node")
async def get_flow_node(
    node_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get a specific flow node by ID.
    
    Equivalent to Rails FlowNodesController#show
    """
    try:
        node = db.query(FlowNode).filter(FlowNode.id == node_id).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow node not found"
            )
        
        # TODO: Check if user has access to this node
        
        return flow_node_to_response(node)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get flow node error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve flow node"
        )


@router.post("/", response_model=FlowNodeResponse, summary="Create Flow Node")
async def create_flow_node(
    node_data: FlowNodeCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new flow node.
    
    Equivalent to Rails FlowNodesController#create
    """
    try:
        # Verify organization exists
        org = db.query(Org).filter(Org.id == node_data.org_id).first()
        if not org:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Verify related resources exist if provided
        if node_data.flow_id:
            flow = db.query(Flow).filter(Flow.id == node_data.flow_id).first()
            if not flow:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Flow not found"
                )
        
        if node_data.origin_node_id:
            origin_node = db.query(FlowNode).filter(FlowNode.id == node_data.origin_node_id).first()
            if not origin_node:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Origin node not found"
                )
        
        if node_data.data_source_id:
            data_source = db.query(DataSource).filter(DataSource.id == node_data.data_source_id).first()
            if not data_source:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Data source not found"
                )
        
        if node_data.data_set_id:
            data_set = db.query(DataSet).filter(DataSet.id == node_data.data_set_id).first()
            if not data_set:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Data set not found"
                )
        
        if node_data.data_sink_id:
            data_sink = db.query(DataSink).filter(DataSink.id == node_data.data_sink_id).first()
            if not data_sink:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Data sink not found"
                )
        
        # Create new flow node
        node = FlowNode(
            name=node_data.name,
            flow_type=node_data.flow_type,
            ingestion_mode=node_data.ingestion_mode,
            origin_node_id=node_data.origin_node_id,
            shared_origin_node_id=node_data.shared_origin_node_id,
            data_source_id=node_data.data_source_id,
            data_set_id=node_data.data_set_id,
            data_sink_id=node_data.data_sink_id,
            owner_id=current_user.id,
            org_id=node_data.org_id,
            created_at=func.now(),
            updated_at=func.now()
        )
        
        db.add(node)
        db.commit()
        db.refresh(node)
        
        logger.info(f"Flow node created: {node.id} by user {current_user.id}")
        return flow_node_to_response(node)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create flow node error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create flow node"
        )


@router.put("/{node_id}", response_model=FlowNodeResponse, summary="Update Flow Node")
async def update_flow_node(
    node_id: int,
    node_data: FlowNodeUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update flow node information.
    
    Equivalent to Rails FlowNodesController#update
    """
    try:
        node = db.query(FlowNode).filter(FlowNode.id == node_id).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow node not found"
            )
        
        # TODO: Check if user has permission to update this node
        
        # Update fields
        update_fields = [
            'name', 'flow_type', 'ingestion_mode', 'origin_node_id',
            'shared_origin_node_id', 'data_source_id', 'data_set_id', 'data_sink_id'
        ]
        
        for field in update_fields:
            value = getattr(node_data, field)
            if value is not None:
                setattr(node, field, value)
        
        node.updated_at = func.now()
        
        db.commit()
        db.refresh(node)
        
        logger.info(f"Flow node updated: {node.id} by user {current_user.id}")
        return flow_node_to_response(node)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update flow node error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update flow node"
        )


@router.delete("/{node_id}", summary="Delete Flow Node")
async def delete_flow_node(
    node_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete a flow node.
    
    Equivalent to Rails FlowNodesController#destroy
    """
    try:
        node = db.query(FlowNode).filter(FlowNode.id == node_id).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow node not found"
            )
        
        # TODO: Check if user has permission to delete this node
        # TODO: Check for dependent nodes
        # TODO: Handle cleanup of connections
        
        db.delete(node)
        db.commit()
        
        logger.info(f"Flow node deleted: {node.id} by user {current_user.id}")
        return {"message": "Flow node deleted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete flow node error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete flow node"
        )


# Flow Node operations
@router.post("/{node_id}/validate", response_model=FlowNodeValidationResult, summary="Validate Flow Node")
async def validate_flow_node(
    node_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Validate a flow node configuration.
    
    Equivalent to Rails FlowNodesController#validate
    """
    try:
        node = db.query(FlowNode).filter(FlowNode.id == node_id).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow node not found"
            )
        
        # TODO: Implement actual validation logic
        errors = []
        warnings = []
        recommendations = []
        
        # Basic validation checks
        if not node.name:
            errors.append("Node name is required")
        
        if not node.flow_type:
            errors.append("Flow type is required")
        
        # Check resource references
        if node.data_source_id:
            data_source = db.query(DataSource).filter(DataSource.id == node.data_source_id).first()
            if not data_source:
                errors.append("Referenced data source not found")
        
        if node.data_set_id:
            data_set = db.query(DataSet).filter(DataSet.id == node.data_set_id).first()
            if not data_set:
                errors.append("Referenced data set not found")
        
        if node.data_sink_id:
            data_sink = db.query(DataSink).filter(DataSink.id == node.data_sink_id).first()
            if not data_sink:
                errors.append("Referenced data sink not found")
        
        # Add recommendations
        if not node.origin_node_id and node.flow_type != "source":
            recommendations.append("Consider connecting this node to a source node")
        
        is_valid = len(errors) == 0
        
        logger.info(f"Flow node validated: {node.id} - valid: {is_valid}")
        return FlowNodeValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            recommendations=recommendations
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Validate flow node error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to validate flow node"
        )


@router.post("/{node_id}/execute", response_model=FlowNodeExecutionResult, summary="Execute Flow Node")
async def execute_flow_node(
    node_id: int,
    parameters: Optional[Dict[str, Any]] = None,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Execute a single flow node for testing.
    
    Equivalent to Rails FlowNodesController#execute
    """
    try:
        node = db.query(FlowNode).filter(FlowNode.id == node_id).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow node not found"
            )
        
        # TODO: Check if user has permission to execute this node
        # TODO: Implement actual node execution logic
        
        # Placeholder execution result
        execution_result = FlowNodeExecutionResult(
            node_id=node_id,
            status="success",
            execution_time_ms=100,
            records_processed=0,
            records_output=0,
            error_message=None,
            output_preview=[]
        )
        
        logger.info(f"Flow node executed: {node.id} by user {current_user.id}")
        return execution_result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Execute flow node error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to execute flow node"
        )


@router.post("/connect", summary="Connect Flow Nodes")
async def connect_flow_nodes(
    connection: FlowNodeConnection,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a connection between two flow nodes.
    
    Equivalent to Rails FlowNodesController#connect
    """
    try:
        # Verify both nodes exist
        source_node = db.query(FlowNode).filter(FlowNode.id == connection.source_node_id).first()
        if not source_node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Source node not found"
            )
        
        target_node = db.query(FlowNode).filter(FlowNode.id == connection.target_node_id).first()
        if not target_node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Target node not found"
            )
        
        # TODO: Check if user has permission to modify these nodes
        # TODO: Validate connection compatibility
        # TODO: Prevent circular dependencies
        
        # Update target node to point to source node
        if connection.connection_type == "data_flow":
            target_node.origin_node_id = connection.source_node_id
        elif connection.connection_type == "control_flow":
            # TODO: Implement control flow connections
            pass
        
        target_node.updated_at = func.now()
        db.commit()
        
        logger.info(f"Flow nodes connected: {connection.source_node_id} -> {connection.target_node_id} by user {current_user.id}")
        return {"message": "Flow nodes connected successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Connect flow nodes error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to connect flow nodes"
        )


@router.delete("/{node_id}/connections", summary="Disconnect Flow Node")
async def disconnect_flow_node(
    node_id: int,
    disconnect_type: str = Query("all", pattern="^(incoming|outgoing|all)$"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Disconnect a flow node from its connections.
    
    Equivalent to Rails FlowNodesController#disconnect
    """
    try:
        node = db.query(FlowNode).filter(FlowNode.id == node_id).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow node not found"
            )
        
        # TODO: Check if user has permission to modify this node
        
        if disconnect_type in ["incoming", "all"]:
            # Remove incoming connections
            node.origin_node_id = None
            node.shared_origin_node_id = None
        
        if disconnect_type in ["outgoing", "all"]:
            # Remove outgoing connections
            downstream_nodes = db.query(FlowNode).filter(FlowNode.origin_node_id == node_id).all()
            for downstream_node in downstream_nodes:
                downstream_node.origin_node_id = None
                downstream_node.updated_at = func.now()
        
        node.updated_at = func.now()
        db.commit()
        
        logger.info(f"Flow node disconnected: {node.id} ({disconnect_type}) by user {current_user.id}")
        return {"message": f"Flow node {disconnect_type} connections removed successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Disconnect flow node error: {str(e)}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to disconnect flow node"
        )


@router.get("/{node_id}/preview", summary="Preview Flow Node Data")
async def preview_flow_node_data(
    node_id: int,
    limit: int = Query(10, ge=1, le=100),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Preview data that would be processed by this flow node.
    
    Equivalent to Rails FlowNodesController#preview
    """
    try:
        node = db.query(FlowNode).filter(FlowNode.id == node_id).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flow node not found"
            )
        
        # TODO: Check if user has permission to preview this node
        # TODO: Implement actual data preview logic
        
        # Placeholder preview data
        preview_data = {
            "node_id": node_id,
            "sample_records": [],
            "schema": {},
            "record_count_estimate": 0
        }
        
        logger.info(f"Flow node data previewed: {node.id} by user {current_user.id}")
        return preview_data
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Preview flow node data error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to preview flow node data"
        )