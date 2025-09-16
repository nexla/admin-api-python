import json
import uuid
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
import logging
from concurrent.futures import ThreadPoolExecutor
import networkx as nx

from ..models.orchestration import (
    Pipeline, PipelineNode, PipelineEdge, PipelineExecution, NodeExecution,
    PipelineDependency, PipelineSchedule, PipelineAlert, DataLineage,
    PipelineMetric, PipelineTemplate, PipelineStatus, ExecutionStatus,
    TriggerType, NodeType
)
from ..models.user import User
from ..models.org import Org

logger = logging.getLogger(__name__)

class OrchestrationService:
    """Advanced data pipeline orchestration service"""
    
    def __init__(self, db: Session):
        self.db = db
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.running_executions = {}
    
    async def create_pipeline(
        self,
        name: str,
        description: str,
        pipeline_config: Dict[str, Any],
        org_id: int,
        created_by: int,
        project_id: Optional[int] = None
    ) -> Pipeline:
        """Create a new data pipeline"""
        
        # Validate pipeline configuration
        self._validate_pipeline_config(pipeline_config)
        
        pipeline = Pipeline(
            name=name,
            description=description,
            pipeline_config=pipeline_config,
            org_id=org_id,
            project_id=project_id,
            created_by=created_by
        )
        
        self.db.add(pipeline)
        self.db.commit()
        self.db.refresh(pipeline)
        
        # Create nodes and edges from config
        await self._create_pipeline_structure(pipeline, pipeline_config)
        
        return pipeline
    
    def _validate_pipeline_config(self, config: Dict[str, Any]):
        """Validate pipeline configuration"""
        required_fields = ["nodes", "edges"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate nodes
        nodes = config["nodes"]
        if not isinstance(nodes, list) or len(nodes) == 0:
            raise ValueError("Pipeline must have at least one node")
        
        node_ids = set()
        for node in nodes:
            if "id" not in node or "type" not in node:
                raise ValueError("Each node must have 'id' and 'type' fields")
            
            if node["id"] in node_ids:
                raise ValueError(f"Duplicate node ID: {node['id']}")
            
            node_ids.add(node["id"])
        
        # Validate edges
        edges = config["edges"]
        if not isinstance(edges, list):
            raise ValueError("Edges must be a list")
        
        for edge in edges:
            if "source" not in edge or "target" not in edge:
                raise ValueError("Each edge must have 'source' and 'target' fields")
            
            if edge["source"] not in node_ids or edge["target"] not in node_ids:
                raise ValueError("Edge references non-existent node")
        
        # Check for cycles (basic validation)
        if self._has_cycles(nodes, edges):
            raise ValueError("Pipeline contains cycles")
    
    def _has_cycles(self, nodes: List[Dict], edges: List[Dict]) -> bool:
        """Check if pipeline graph has cycles"""
        graph = nx.DiGraph()
        
        # Add nodes
        for node in nodes:
            graph.add_node(node["id"])
        
        # Add edges
        for edge in edges:
            graph.add_edge(edge["source"], edge["target"])
        
        return not nx.is_directed_acyclic_graph(graph)
    
    async def _create_pipeline_structure(self, pipeline: Pipeline, config: Dict[str, Any]):
        """Create pipeline nodes and edges from configuration"""
        
        # Create nodes
        node_map = {}
        for node_config in config["nodes"]:
            node = PipelineNode(
                pipeline_id=pipeline.id,
                node_id=node_config["id"],
                name=node_config.get("name", node_config["id"]),
                node_type=node_config["type"],
                config=node_config.get("config", {}),
                input_schema=node_config.get("input_schema"),
                output_schema=node_config.get("output_schema"),
                timeout_seconds=node_config.get("timeout_seconds", 300),
                retry_attempts=node_config.get("retry_attempts", 3),
                position_x=node_config.get("position", {}).get("x", 0),
                position_y=node_config.get("position", {}).get("y", 0)
            )
            
            self.db.add(node)
            node_map[node_config["id"]] = node
        
        self.db.commit()
        
        # Create edges
        for edge_config in config["edges"]:
            source_node = node_map[edge_config["source"]]
            target_node = node_map[edge_config["target"]]
            
            edge = PipelineEdge(
                pipeline_id=pipeline.id,
                edge_id=f"{edge_config['source']}->{edge_config['target']}",
                source_node_id=source_node.id,
                target_node_id=target_node.id,
                source_port=edge_config.get("source_port", "output"),
                target_port=edge_config.get("target_port", "input"),
                transformation_config=edge_config.get("transformation"),
                condition_config=edge_config.get("condition")
            )
            
            self.db.add(edge)
        
        self.db.commit()
    
    async def execute_pipeline(
        self,
        pipeline_id: int,
        trigger_type: TriggerType,
        trigger_data: Optional[Dict[str, Any]] = None,
        triggered_by: Optional[int] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> PipelineExecution:
        """Execute a pipeline"""
        
        pipeline = self.db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        if not pipeline:
            raise ValueError("Pipeline not found")
        
        if pipeline.status != PipelineStatus.ACTIVE:
            raise ValueError(f"Pipeline is not active (status: {pipeline.status})")
        
        # Check concurrent execution limit
        active_executions = self.db.query(PipelineExecution).filter(
            and_(
                PipelineExecution.pipeline_id == pipeline_id,
                PipelineExecution.status.in_([ExecutionStatus.PENDING, ExecutionStatus.RUNNING])
            )
        ).count()
        
        if active_executions >= pipeline.max_concurrent_executions:
            raise ValueError("Maximum concurrent executions reached")
        
        # Create execution record
        execution_id = str(uuid.uuid4())
        execution = PipelineExecution(
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            status=ExecutionStatus.PENDING,
            trigger_type=trigger_type,
            trigger_data=trigger_data or {},
            triggered_by=triggered_by,
            pipeline_config_snapshot=pipeline.pipeline_config
        )
        
        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)
        
        # Start execution asynchronously
        asyncio.create_task(self._execute_pipeline_async(execution, parameters or {}))
        
        return execution
    
    async def _execute_pipeline_async(
        self,
        execution: PipelineExecution,
        parameters: Dict[str, Any]
    ):
        """Execute pipeline asynchronously"""
        
        try:
            execution.status = ExecutionStatus.RUNNING
            execution.started_at = datetime.utcnow()
            self.db.commit()
            
            # Track running execution
            self.running_executions[execution.execution_id] = execution
            
            # Build execution graph
            execution_graph = await self._build_execution_graph(execution)
            
            # Execute nodes in topological order
            await self._execute_pipeline_graph(execution, execution_graph, parameters)
            
            # Mark execution as completed
            execution.status = ExecutionStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            execution.duration_seconds = int(
                (execution.completed_at - execution.started_at).total_seconds()
            )
            
        except Exception as e:
            logger.error(f"Pipeline execution {execution.execution_id} failed: {e}")
            execution.status = ExecutionStatus.FAILED
            execution.completed_at = datetime.utcnow()
            execution.error_message = str(e)
            
            if execution.started_at:
                execution.duration_seconds = int(
                    (execution.completed_at - execution.started_at).total_seconds()
                )
        
        finally:
            # Remove from running executions
            self.running_executions.pop(execution.execution_id, None)
            
            # Update pipeline last execution
            pipeline = execution.pipeline
            pipeline.last_execution_id = execution.id
            pipeline.last_executed_at = execution.completed_at
            
            self.db.commit()
            
            # Check alerts
            await self._check_pipeline_alerts(execution)
    
    async def _build_execution_graph(self, execution: PipelineExecution) -> nx.DiGraph:
        """Build execution graph from pipeline structure"""
        
        pipeline = execution.pipeline
        graph = nx.DiGraph()
        
        # Add nodes
        for node in pipeline.nodes:
            if node.enabled:
                graph.add_node(node.id, node=node)
        
        # Add edges
        for edge in pipeline.edges:
            if edge.enabled and edge.source_node.enabled and edge.target_node.enabled:
                graph.add_edge(edge.source_node_id, edge.target_node_id, edge=edge)
        
        return graph
    
    async def _execute_pipeline_graph(
        self,
        execution: PipelineExecution,
        graph: nx.DiGraph,
        parameters: Dict[str, Any]
    ):
        """Execute pipeline graph in topological order"""
        
        # Get nodes in topological order
        try:
            node_order = list(nx.topological_sort(graph))
        except nx.NetworkXError:
            raise ValueError("Pipeline contains cycles")
        
        node_results = {}
        
        # Execute nodes in batches (parallel execution where possible)
        while node_order:
            # Find nodes that can be executed in parallel
            ready_nodes = []
            for node_id in node_order:
                # Check if all dependencies are completed
                predecessors = list(graph.predecessors(node_id))
                if all(pred_id in node_results for pred_id in predecessors):
                    ready_nodes.append(node_id)
            
            if not ready_nodes:
                raise ValueError("No ready nodes found (possible circular dependency)")
            
            # Remove ready nodes from order
            for node_id in ready_nodes:
                node_order.remove(node_id)
            
            # Execute ready nodes in parallel
            tasks = []
            for node_id in ready_nodes:
                node = graph.nodes[node_id]["node"]
                input_data = self._prepare_node_input(node_id, graph, node_results)
                
                task = self._execute_node_async(execution, node, input_data, parameters)
                tasks.append((node_id, task))
            
            # Wait for all tasks to complete
            for node_id, task in tasks:
                try:
                    result = await task
                    node_results[node_id] = result
                except Exception as e:
                    logger.error(f"Node {node_id} execution failed: {e}")
                    raise e
    
    def _prepare_node_input(
        self,
        node_id: int,
        graph: nx.DiGraph,
        node_results: Dict[int, Any]
    ) -> Dict[str, Any]:
        """Prepare input data for node execution"""
        
        input_data = {}
        
        # Get data from predecessor nodes
        for pred_id in graph.predecessors(node_id):
            edge_data = graph.edges[pred_id, node_id]["edge"]
            pred_result = node_results.get(pred_id)
            
            if pred_result:
                # Apply edge transformation if configured
                transformed_data = self._apply_edge_transformation(
                    pred_result, edge_data
                )
                
                # Map to target port
                target_port = edge_data.target_port
                input_data[target_port] = transformed_data
        
        return input_data
    
    def _apply_edge_transformation(
        self,
        data: Any,
        edge: PipelineEdge
    ) -> Any:
        """Apply transformation defined on edge"""
        
        if not edge.transformation_config:
            return data
        
        transformation = edge.transformation_config
        transform_type = transformation.get("type")
        
        if transform_type == "field_mapping":
            # Map fields from source to target
            mapping = transformation.get("mapping", {})
            if isinstance(data, dict):
                result = {}
                for target_field, source_field in mapping.items():
                    if source_field in data:
                        result[target_field] = data[source_field]
                return result
        
        elif transform_type == "filter":
            # Filter data based on condition
            condition = transformation.get("condition")
            if condition and isinstance(data, list):
                return [item for item in data if self._evaluate_condition(item, condition)]
        
        elif transform_type == "aggregation":
            # Aggregate data
            agg_type = transformation.get("aggregation_type")
            if agg_type == "sum" and isinstance(data, list):
                return sum(data)
            elif agg_type == "count" and isinstance(data, list):
                return len(data)
        
        return data
    
    def _evaluate_condition(self, item: Any, condition: Dict[str, Any]) -> bool:
        """Evaluate condition for filtering"""
        
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")
        
        if not field or not operator:
            return True
        
        item_value = item.get(field) if isinstance(item, dict) else item
        
        if operator == "equals":
            return item_value == value
        elif operator == "not_equals":
            return item_value != value
        elif operator == "greater_than":
            return item_value > value
        elif operator == "less_than":
            return item_value < value
        elif operator == "contains":
            return value in str(item_value)
        
        return True
    
    async def _execute_node_async(
        self,
        execution: PipelineExecution,
        node: PipelineNode,
        input_data: Dict[str, Any],
        parameters: Dict[str, Any]
    ) -> Any:
        """Execute a single node asynchronously"""
        
        node_execution = NodeExecution(
            pipeline_execution_id=execution.id,
            node_id=node.id,
            execution_id=f"{execution.execution_id}-{node.node_id}",
            status=ExecutionStatus.RUNNING,
            started_at=datetime.utcnow(),
            input_data=input_data
        )
        
        self.db.add(node_execution)
        self.db.commit()
        
        try:
            # Execute node based on type
            result = await self._execute_node_by_type(
                node, input_data, parameters, node_execution
            )
            
            node_execution.status = ExecutionStatus.COMPLETED
            node_execution.completed_at = datetime.utcnow()
            node_execution.output_data = result
            node_execution.duration_seconds = int(
                (node_execution.completed_at - node_execution.started_at).total_seconds()
            )
            
            return result
            
        except Exception as e:
            node_execution.status = ExecutionStatus.FAILED
            node_execution.completed_at = datetime.utcnow()
            node_execution.error_message = str(e)
            
            if node_execution.started_at:
                node_execution.duration_seconds = int(
                    (node_execution.completed_at - node_execution.started_at).total_seconds()
                )
            
            # Retry logic
            if node_execution.attempt_number < node.retry_attempts:
                node_execution.attempt_number += 1
                await asyncio.sleep(node.retry_delay_seconds)
                return await self._execute_node_async(execution, node, input_data, parameters)
            
            raise e
        
        finally:
            self.db.commit()
    
    async def _execute_node_by_type(
        self,
        node: PipelineNode,
        input_data: Dict[str, Any],
        parameters: Dict[str, Any],
        node_execution: NodeExecution
    ) -> Any:
        """Execute node based on its type"""
        
        node_type = NodeType(node.node_type)
        config = node.config
        
        if node_type == NodeType.EXTRACTOR:
            return await self._execute_extractor_node(node, input_data, config)
        
        elif node_type == NodeType.TRANSFORMER:
            return await self._execute_transformer_node(node, input_data, config)
        
        elif node_type == NodeType.LOADER:
            return await self._execute_loader_node(node, input_data, config)
        
        elif node_type == NodeType.VALIDATOR:
            return await self._execute_validator_node(node, input_data, config)
        
        elif node_type == NodeType.AGGREGATOR:
            return await self._execute_aggregator_node(node, input_data, config)
        
        elif node_type == NodeType.CONDITIONAL:
            return await self._execute_conditional_node(node, input_data, config)
        
        else:
            raise ValueError(f"Unknown node type: {node_type}")
    
    async def _execute_extractor_node(
        self,
        node: PipelineNode,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Any:
        """Execute data extractor node"""
        
        source_type = config.get("source_type")
        
        if source_type == "database":
            # Execute database query
            query = config.get("query")
            connection_config = config.get("connection")
            # Implementation would execute actual database query
            return {"data": [], "rows_extracted": 0}
        
        elif source_type == "api":
            # Call external API
            url = config.get("url")
            headers = config.get("headers", {})
            # Implementation would make actual API call
            return {"data": [], "records_fetched": 0}
        
        elif source_type == "file":
            # Read file
            file_path = config.get("file_path")
            file_format = config.get("format", "csv")
            # Implementation would read actual file
            return {"data": [], "records_read": 0}
        
        return {"data": []}
    
    async def _execute_transformer_node(
        self,
        node: PipelineNode,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Any:
        """Execute data transformer node"""
        
        transformation_type = config.get("transformation_type")
        input_records = input_data.get("input", {}).get("data", [])
        
        if transformation_type == "field_mapping":
            mapping = config.get("field_mapping", {})
            transformed_records = []
            
            for record in input_records:
                transformed_record = {}
                for target_field, source_field in mapping.items():
                    if isinstance(source_field, str) and source_field in record:
                        transformed_record[target_field] = record[source_field]
                transformed_records.append(transformed_record)
            
            return {"data": transformed_records, "records_transformed": len(transformed_records)}
        
        elif transformation_type == "aggregation":
            # Perform aggregation
            group_by = config.get("group_by", [])
            aggregations = config.get("aggregations", {})
            # Implementation would perform actual aggregation
            return {"data": [], "groups_created": 0}
        
        elif transformation_type == "filtering":
            # Filter records
            filter_condition = config.get("filter_condition")
            filtered_records = []
            
            for record in input_records:
                if self._evaluate_condition(record, filter_condition):
                    filtered_records.append(record)
            
            return {"data": filtered_records, "records_filtered": len(filtered_records)}
        
        return {"data": input_records}
    
    async def _execute_loader_node(
        self,
        node: PipelineNode,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Any:
        """Execute data loader node"""
        
        target_type = config.get("target_type")
        input_records = input_data.get("input", {}).get("data", [])
        
        if target_type == "database":
            # Load to database
            table_name = config.get("table_name")
            connection_config = config.get("connection")
            # Implementation would load to actual database
            return {"records_loaded": len(input_records)}
        
        elif target_type == "file":
            # Save to file
            file_path = config.get("file_path")
            file_format = config.get("format", "csv")
            # Implementation would save to actual file
            return {"records_saved": len(input_records)}
        
        elif target_type == "api":
            # Send to API
            url = config.get("url")
            headers = config.get("headers", {})
            # Implementation would send to actual API
            return {"records_sent": len(input_records)}
        
        return {"records_loaded": 0}
    
    async def _execute_validator_node(
        self,
        node: PipelineNode,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Any:
        """Execute data validator node"""
        
        validation_rules = config.get("validation_rules", [])
        input_records = input_data.get("input", {}).get("data", [])
        
        valid_records = []
        invalid_records = []
        
        for record in input_records:
            is_valid = True
            validation_errors = []
            
            for rule in validation_rules:
                rule_type = rule.get("type")
                field = rule.get("field")
                
                if rule_type == "required" and field not in record:
                    is_valid = False
                    validation_errors.append(f"Missing required field: {field}")
                
                elif rule_type == "data_type":
                    expected_type = rule.get("data_type")
                    if field in record:
                        value = record[field]
                        if expected_type == "string" and not isinstance(value, str):
                            is_valid = False
                            validation_errors.append(f"Field {field} must be string")
                        elif expected_type == "number" and not isinstance(value, (int, float)):
                            is_valid = False
                            validation_errors.append(f"Field {field} must be number")
            
            if is_valid:
                valid_records.append(record)
            else:
                invalid_record = record.copy()
                invalid_record["_validation_errors"] = validation_errors
                invalid_records.append(invalid_record)
        
        return {
            "valid_data": valid_records,
            "invalid_data": invalid_records,
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records)
        }
    
    async def _execute_aggregator_node(
        self,
        node: PipelineNode,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Any:
        """Execute data aggregator node"""
        
        group_by = config.get("group_by", [])
        aggregations = config.get("aggregations", {})
        input_records = input_data.get("input", {}).get("data", [])
        
        if not group_by:
            # No grouping, aggregate all records
            result = {}
            for agg_field, agg_func in aggregations.items():
                values = [record.get(agg_field) for record in input_records if agg_field in record]
                
                if agg_func == "sum":
                    result[f"{agg_field}_sum"] = sum(values)
                elif agg_func == "avg":
                    result[f"{agg_field}_avg"] = sum(values) / len(values) if values else 0
                elif agg_func == "count":
                    result[f"{agg_field}_count"] = len(values)
                elif agg_func == "max":
                    result[f"{agg_field}_max"] = max(values) if values else None
                elif agg_func == "min":
                    result[f"{agg_field}_min"] = min(values) if values else None
            
            return {"data": [result], "groups_created": 1}
        
        else:
            # Group by specified fields and aggregate
            groups = {}
            
            for record in input_records:
                # Create group key
                group_key = tuple(record.get(field) for field in group_by)
                
                if group_key not in groups:
                    groups[group_key] = []
                groups[group_key].append(record)
            
            # Aggregate each group
            aggregated_results = []
            for group_key, group_records in groups.items():
                result = {}
                
                # Add group by fields
                for i, field in enumerate(group_by):
                    result[field] = group_key[i]
                
                # Add aggregations
                for agg_field, agg_func in aggregations.items():
                    values = [record.get(agg_field) for record in group_records if agg_field in record]
                    
                    if agg_func == "sum":
                        result[f"{agg_field}_sum"] = sum(values)
                    elif agg_func == "avg":
                        result[f"{agg_field}_avg"] = sum(values) / len(values) if values else 0
                    elif agg_func == "count":
                        result[f"{agg_field}_count"] = len(values)
                    elif agg_func == "max":
                        result[f"{agg_field}_max"] = max(values) if values else None
                    elif agg_func == "min":
                        result[f"{agg_field}_min"] = min(values) if values else None
                
                aggregated_results.append(result)
            
            return {"data": aggregated_results, "groups_created": len(aggregated_results)}
    
    async def _execute_conditional_node(
        self,
        node: PipelineNode,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Any:
        """Execute conditional node (routing logic)"""
        
        condition = config.get("condition")
        input_records = input_data.get("input", {}).get("data", [])
        
        if not condition:
            return {"data": input_records, "condition_result": True}
        
        # Evaluate condition on input data
        condition_result = self._evaluate_condition(input_data, condition)
        
        return {
            "data": input_records if condition_result else [],
            "condition_result": condition_result
        }
    
    async def _check_pipeline_alerts(self, execution: PipelineExecution):
        """Check and trigger pipeline alerts"""
        
        alerts = self.db.query(PipelineAlert).filter(
            and_(
                PipelineAlert.pipeline_id == execution.pipeline_id,
                PipelineAlert.enabled == True
            )
        ).all()
        
        for alert in alerts:
            try:
                should_trigger = self._evaluate_alert_condition(alert, execution)
                
                if should_trigger:
                    await self._trigger_pipeline_alert(alert, execution)
                    
            except Exception as e:
                logger.error(f"Error evaluating alert {alert.id}: {e}")
    
    def _evaluate_alert_condition(
        self,
        alert: PipelineAlert,
        execution: PipelineExecution
    ) -> bool:
        """Evaluate alert condition"""
        
        condition = alert.condition_config
        alert_type = alert.alert_type
        
        if alert_type == "execution_failure":
            return execution.status == ExecutionStatus.FAILED
        
        elif alert_type == "execution_duration":
            threshold_seconds = condition.get("threshold_seconds")
            return execution.duration_seconds and execution.duration_seconds > threshold_seconds
        
        elif alert_type == "data_quality":
            # Check data quality metrics from execution
            quality_threshold = condition.get("quality_threshold", 0.95)
            # Implementation would check actual data quality metrics
            return False
        
        return False
    
    async def _trigger_pipeline_alert(
        self,
        alert: PipelineAlert,
        execution: PipelineExecution
    ):
        """Trigger pipeline alert"""
        
        alert.last_triggered_at = datetime.utcnow()
        alert.trigger_count += 1
        
        # Send notifications based on configuration
        notification_config = alert.notification_config
        
        for notification in notification_config:
            notification_type = notification.get("type")
            
            if notification_type == "email":
                # Send email notification
                recipients = notification.get("recipients", [])
                subject = f"Pipeline Alert: {alert.alert_type}"
                message = alert.message_template or f"Pipeline {execution.pipeline.name} triggered alert"
                # Implementation would send actual email
                
            elif notification_type == "webhook":
                # Send webhook notification
                webhook_url = notification.get("url")
                payload = {
                    "alert_type": alert.alert_type,
                    "pipeline_id": execution.pipeline_id,
                    "execution_id": execution.execution_id,
                    "severity": alert.severity,
                    "message": alert.message_template
                }
                # Implementation would send actual webhook
        
        self.db.commit()
    
    async def cancel_pipeline_execution(self, execution_id: str) -> bool:
        """Cancel a running pipeline execution"""
        
        execution = self.db.query(PipelineExecution).filter(
            PipelineExecution.execution_id == execution_id
        ).first()
        
        if not execution:
            return False
        
        if execution.status not in [ExecutionStatus.PENDING, ExecutionStatus.RUNNING]:
            return False
        
        execution.status = ExecutionStatus.CANCELLED
        execution.completed_at = datetime.utcnow()
        
        if execution.started_at:
            execution.duration_seconds = int(
                (execution.completed_at - execution.started_at).total_seconds()
            )
        
        self.db.commit()
        
        # Remove from running executions
        self.running_executions.pop(execution_id, None)
        
        return True
    
    async def get_pipeline_metrics(
        self,
        pipeline_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get pipeline performance metrics"""
        
        executions = self.db.query(PipelineExecution).filter(
            and_(
                PipelineExecution.pipeline_id == pipeline_id,
                PipelineExecution.started_at >= start_time,
                PipelineExecution.started_at <= end_time
            )
        ).all()
        
        total_executions = len(executions)
        successful_executions = len([e for e in executions if e.status == ExecutionStatus.COMPLETED])
        failed_executions = len([e for e in executions if e.status == ExecutionStatus.FAILED])
        
        durations = [e.duration_seconds for e in executions if e.duration_seconds]
        avg_duration = sum(durations) / len(durations) if durations else 0
        max_duration = max(durations) if durations else 0
        min_duration = min(durations) if durations else 0
        
        success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
        
        return {
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate_percent": round(success_rate, 2),
            "average_duration_seconds": round(avg_duration, 2),
            "max_duration_seconds": max_duration,
            "min_duration_seconds": min_duration
        }