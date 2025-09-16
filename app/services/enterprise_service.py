import json
import uuid
import asyncio
import aiohttp
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc
import logging
import networkx as nx
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac

from ..models.enterprise import (
    EnterpriseIntegration, IntegrationLog, APIGateway, GatewayRoute,
    WorkflowDefinition, WorkflowExecution, WorkflowTask, WorkflowTaskExecution,
    EventBus, EventTopic, EventSubscription, Event, ServiceDiscovery,
    IntegrationType, AuthenticationType, IntegrationStatus,
    WorkflowStatus, TaskStatus
)
from ..models.user import User
from ..models.org import Org

logger = logging.getLogger(__name__)

class EnterpriseIntegrationService:
    """Enterprise integration and API gateway service"""
    
    def __init__(self, db: Session):
        self.db = db
        self.session_pool = {}
        self.circuit_breakers = {}
    
    async def create_integration(
        self,
        name: str,
        integration_type: IntegrationType,
        endpoint_url: str,
        authentication_type: AuthenticationType,
        authentication_config: Dict[str, Any],
        connection_config: Dict[str, Any],
        org_id: int,
        created_by: int,
        description: str = None
    ) -> EnterpriseIntegration:
        """Create a new enterprise integration"""
        
        # Validate configuration
        self._validate_integration_config(
            integration_type, authentication_type, authentication_config, connection_config
        )
        
        integration = EnterpriseIntegration(
            name=name,
            description=description,
            integration_type=integration_type,
            endpoint_url=endpoint_url,
            authentication_type=authentication_type,
            authentication_config=authentication_config,
            connection_config=connection_config,
            org_id=org_id,
            created_by=created_by
        )
        
        self.db.add(integration)
        self.db.commit()
        self.db.refresh(integration)
        
        # Test connection
        await self._test_integration_connection(integration)
        
        return integration
    
    def _validate_integration_config(
        self,
        integration_type: IntegrationType,
        authentication_type: AuthenticationType,
        authentication_config: Dict[str, Any],
        connection_config: Dict[str, Any]
    ):
        """Validate integration configuration"""
        
        # Validate authentication config based on type
        if authentication_type == AuthenticationType.API_KEY:
            if "api_key" not in authentication_config:
                raise ValueError("API key is required for API key authentication")
        
        elif authentication_type == AuthenticationType.OAUTH2:
            required_fields = ["client_id", "client_secret", "token_url"]
            for field in required_fields:
                if field not in authentication_config:
                    raise ValueError(f"{field} is required for OAuth2 authentication")
        
        elif authentication_type == AuthenticationType.JWT:
            if "secret" not in authentication_config and "public_key" not in authentication_config:
                raise ValueError("Secret or public key is required for JWT authentication")
        
        # Validate connection config based on integration type
        if integration_type in [IntegrationType.DATABASE]:
            required_fields = ["host", "port", "database"]
            for field in required_fields:
                if field not in connection_config:
                    raise ValueError(f"{field} is required for database integration")
    
    async def _test_integration_connection(self, integration: EnterpriseIntegration):
        """Test integration connection and update status"""
        
        try:
            if integration.integration_type == IntegrationType.REST_API:
                await self._test_rest_api_connection(integration)
            elif integration.integration_type == IntegrationType.DATABASE:
                await self._test_database_connection(integration)
            elif integration.integration_type == IntegrationType.MESSAGE_QUEUE:
                await self._test_message_queue_connection(integration)
            
            integration.status = IntegrationStatus.ACTIVE
            integration.health_status = "healthy"
            
        except Exception as e:
            integration.status = IntegrationStatus.ERROR
            integration.health_status = "unhealthy"
            logger.error(f"Integration connection test failed for {integration.id}: {e}")
        
        integration.last_health_check = datetime.utcnow()
        self.db.commit()
    
    async def _test_rest_api_connection(self, integration: EnterpriseIntegration):
        """Test REST API connection"""
        
        headers = await self._build_authentication_headers(integration)
        
        async with aiohttp.ClientSession() as session:
            # Try a simple GET request or use health check endpoint
            health_check_url = integration.health_check_config.get("url") if integration.health_check_config else integration.endpoint_url
            
            async with session.get(
                health_check_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=integration.timeout_seconds)
            ) as response:
                if response.status >= 400:
                    raise Exception(f"Health check failed with status {response.status}")
    
    async def _test_database_connection(self, integration: EnterpriseIntegration):
        """Test database connection"""
        
        # This would test actual database connection
        # For now, simulate the test
        await asyncio.sleep(0.1)
    
    async def _test_message_queue_connection(self, integration: EnterpriseIntegration):
        """Test message queue connection"""
        
        # This would test actual message queue connection
        # For now, simulate the test
        await asyncio.sleep(0.1)
    
    async def execute_integration_request(
        self,
        integration_id: int,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        user_id: Optional[int] = None,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute request through integration"""
        
        integration = self.db.query(EnterpriseIntegration).filter(
            EnterpriseIntegration.id == integration_id
        ).first()
        
        if not integration:
            raise ValueError("Integration not found")
        
        if integration.status != IntegrationStatus.ACTIVE:
            raise ValueError(f"Integration is not active: {integration.status}")
        
        # Check circuit breaker
        if self._is_circuit_breaker_open(integration_id):
            raise Exception("Circuit breaker is open for this integration")
        
        # Generate log ID
        log_id = str(uuid.uuid4())
        correlation_id = correlation_id or str(uuid.uuid4())
        
        # Create integration log
        log_entry = IntegrationLog(
            log_id=log_id,
            integration_id=integration_id,
            request_method=method,
            request_url=f"{integration.endpoint_url}{endpoint}",
            correlation_id=correlation_id,
            user_id=user_id
        )
        
        try:
            # Execute request
            start_time = datetime.utcnow()
            
            response_data = await self._execute_request(
                integration, method, endpoint, data, headers, log_entry
            )
            
            # Calculate response time
            response_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            # Update log with success
            log_entry.response_status = 200
            log_entry.response_body = json.dumps(response_data)
            log_entry.response_time_ms = response_time
            
            # Update integration metrics
            integration.total_requests += 1
            integration.successful_requests += 1
            integration.last_used_at = datetime.utcnow()
            
            # Update average response time
            if integration.avg_response_time:
                integration.avg_response_time = (integration.avg_response_time + response_time) / 2
            else:
                integration.avg_response_time = response_time
            
            self._record_circuit_breaker_success(integration_id)
            
            return response_data
            
        except Exception as e:
            # Update log with error
            log_entry.response_status = 500
            log_entry.error_message = str(e)
            
            # Update integration metrics
            integration.total_requests += 1
            integration.failed_requests += 1
            
            self._record_circuit_breaker_failure(integration_id)
            
            raise e
        
        finally:
            self.db.add(log_entry)
            self.db.commit()
    
    async def _execute_request(
        self,
        integration: EnterpriseIntegration,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]],
        log_entry: IntegrationLog
    ) -> Dict[str, Any]:
        """Execute the actual request"""
        
        # Build authentication headers
        auth_headers = await self._build_authentication_headers(integration)
        
        # Merge headers
        request_headers = {**(headers or {}), **auth_headers}
        
        # Apply request mapping
        if integration.request_mapping and data:
            data = self._apply_data_mapping(data, integration.request_mapping)
        
        # Log request details
        log_entry.request_headers = request_headers
        log_entry.request_body = json.dumps(data) if data else None
        
        url = f"{integration.endpoint_url}{endpoint}"
        
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                url,
                json=data,
                headers=request_headers,
                timeout=aiohttp.ClientTimeout(total=integration.timeout_seconds)
            ) as response:
                response_data = await response.json() if response.content_type == 'application/json' else await response.text()
                
                if response.status >= 400:
                    raise Exception(f"Request failed with status {response.status}: {response_data}")
                
                # Apply response mapping
                if integration.response_mapping and isinstance(response_data, dict):
                    response_data = self._apply_data_mapping(response_data, integration.response_mapping)
                
                return response_data
    
    async def _build_authentication_headers(self, integration: EnterpriseIntegration) -> Dict[str, str]:
        """Build authentication headers based on integration configuration"""
        
        headers = {}
        auth_type = AuthenticationType(integration.authentication_type)
        auth_config = integration.authentication_config
        
        if auth_type == AuthenticationType.API_KEY:
            api_key = auth_config["api_key"]
            header_name = auth_config.get("header_name", "X-API-Key")
            headers[header_name] = api_key
        
        elif auth_type == AuthenticationType.BEARER_TOKEN:
            token = auth_config["token"]
            headers["Authorization"] = f"Bearer {token}"
        
        elif auth_type == AuthenticationType.BASIC_AUTH:
            import base64
            username = auth_config["username"]
            password = auth_config["password"]
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"
        
        elif auth_type == AuthenticationType.OAUTH2:
            # This would implement OAuth2 token refresh logic
            token = await self._get_oauth2_token(auth_config)
            headers["Authorization"] = f"Bearer {token}"
        
        elif auth_type == AuthenticationType.JWT:
            # This would implement JWT token generation
            token = await self._generate_jwt_token(auth_config)
            headers["Authorization"] = f"Bearer {token}"
        
        return headers
    
    async def _get_oauth2_token(self, auth_config: Dict[str, Any]) -> str:
        """Get OAuth2 access token"""
        
        # This would implement actual OAuth2 flow
        # For now, return a mock token
        return "mock_oauth2_token"
    
    async def _generate_jwt_token(self, auth_config: Dict[str, Any]) -> str:
        """Generate JWT token"""
        
        # This would implement actual JWT generation
        # For now, return a mock token
        return "mock_jwt_token"
    
    def _apply_data_mapping(self, data: Dict[str, Any], mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Apply data mapping transformation"""
        
        mapped_data = {}
        
        for target_field, source_path in mapping.items():
            if isinstance(source_path, str):
                # Simple field mapping
                if source_path in data:
                    mapped_data[target_field] = data[source_path]
            elif isinstance(source_path, dict):
                # Complex mapping with transformations
                source_field = source_path.get("field")
                transformation = source_path.get("transformation")
                
                if source_field in data:
                    value = data[source_field]
                    
                    if transformation == "upper":
                        value = str(value).upper()
                    elif transformation == "lower":
                        value = str(value).lower()
                    elif transformation == "strip":
                        value = str(value).strip()
                    
                    mapped_data[target_field] = value
        
        return mapped_data
    
    def _is_circuit_breaker_open(self, integration_id: int) -> bool:
        """Check if circuit breaker is open for integration"""
        
        breaker = self.circuit_breakers.get(integration_id)
        if not breaker:
            return False
        
        # Simple circuit breaker logic
        failure_rate = breaker.get("failures", 0) / max(breaker.get("requests", 1), 1)
        failure_threshold = 0.5  # 50% failure rate
        
        if failure_rate > failure_threshold and breaker.get("requests", 0) >= 10:
            # Check if cool-down period has passed
            last_failure = breaker.get("last_failure")
            if last_failure and datetime.utcnow() - last_failure < timedelta(minutes=5):
                return True
        
        return False
    
    def _record_circuit_breaker_success(self, integration_id: int):
        """Record successful request for circuit breaker"""
        
        if integration_id not in self.circuit_breakers:
            self.circuit_breakers[integration_id] = {"requests": 0, "failures": 0}
        
        self.circuit_breakers[integration_id]["requests"] += 1
    
    def _record_circuit_breaker_failure(self, integration_id: int):
        """Record failed request for circuit breaker"""
        
        if integration_id not in self.circuit_breakers:
            self.circuit_breakers[integration_id] = {"requests": 0, "failures": 0}
        
        breaker = self.circuit_breakers[integration_id]
        breaker["requests"] += 1
        breaker["failures"] += 1
        breaker["last_failure"] = datetime.utcnow()

class WorkflowAutomationService:
    """Advanced workflow automation engine"""
    
    def __init__(self, db: Session):
        self.db = db
        self.executor = ThreadPoolExecutor(max_workers=20)
        self.running_workflows = {}
    
    async def create_workflow(
        self,
        name: str,
        workflow_config: Dict[str, Any],
        triggers: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]],
        org_id: int,
        created_by: int,
        description: str = None
    ) -> WorkflowDefinition:
        """Create a new workflow definition"""
        
        # Validate workflow configuration
        self._validate_workflow_config(workflow_config, triggers, tasks)
        
        # Build task dependencies
        dependencies = self._build_task_dependencies(tasks)
        
        workflow = WorkflowDefinition(
            name=name,
            description=description,
            workflow_config=workflow_config,
            triggers=triggers,
            tasks=tasks,
            dependencies=dependencies,
            org_id=org_id,
            created_by=created_by
        )
        
        self.db.add(workflow)
        self.db.commit()
        self.db.refresh(workflow)
        
        # Create task records
        await self._create_workflow_tasks(workflow, tasks)
        
        return workflow
    
    def _validate_workflow_config(
        self,
        workflow_config: Dict[str, Any],
        triggers: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]]
    ):
        """Validate workflow configuration"""
        
        if not triggers:
            raise ValueError("Workflow must have at least one trigger")
        
        if not tasks:
            raise ValueError("Workflow must have at least one task")
        
        # Validate task IDs are unique
        task_ids = [task.get("id") for task in tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("Task IDs must be unique")
        
        # Validate task dependencies
        for task in tasks:
            depends_on = task.get("depends_on", [])
            for dep_id in depends_on:
                if dep_id not in task_ids:
                    raise ValueError(f"Task dependency '{dep_id}' not found")
    
    def _build_task_dependencies(self, tasks: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Build task dependency graph"""
        
        dependencies = {}
        
        for task in tasks:
            task_id = task["id"]
            depends_on = task.get("depends_on", [])
            dependencies[task_id] = depends_on
        
        # Validate no circular dependencies
        graph = nx.DiGraph()
        for task_id, deps in dependencies.items():
            graph.add_node(task_id)
            for dep_id in deps:
                graph.add_edge(dep_id, task_id)
        
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("Workflow contains circular dependencies")
        
        return dependencies
    
    async def _create_workflow_tasks(
        self,
        workflow: WorkflowDefinition,
        tasks_config: List[Dict[str, Any]]
    ):
        """Create workflow task records"""
        
        for task_config in tasks_config:
            task = WorkflowTask(
                workflow_id=workflow.id,
                task_id=task_config["id"],
                name=task_config.get("name", task_config["id"]),
                description=task_config.get("description"),
                task_type=task_config["type"],
                task_config=task_config.get("config", {}),
                input_mapping=task_config.get("input_mapping"),
                output_mapping=task_config.get("output_mapping"),
                timeout_minutes=task_config.get("timeout_minutes", 30),
                retry_attempts=task_config.get("retry_attempts", 3),
                execution_conditions=task_config.get("conditions"),
                error_handling=task_config.get("error_handling"),
                integration_id=task_config.get("integration_id")
            )
            
            self.db.add(task)
        
        self.db.commit()
    
    async def execute_workflow(
        self,
        workflow_id: int,
        trigger_data: Dict[str, Any],
        triggered_by: Optional[int] = None,
        input_data: Optional[Dict[str, Any]] = None
    ) -> WorkflowExecution:
        """Execute a workflow"""
        
        workflow = self.db.query(WorkflowDefinition).filter(
            WorkflowDefinition.id == workflow_id
        ).first()
        
        if not workflow:
            raise ValueError("Workflow not found")
        
        if workflow.status != WorkflowStatus.ACTIVE:
            raise ValueError(f"Workflow is not active: {workflow.status}")
        
        # Create execution record
        execution_id = str(uuid.uuid4())
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_id,
            trigger_data=trigger_data,
            input_data=input_data or {},
            triggered_by=triggered_by
        )
        
        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)
        
        # Start workflow execution asynchronously
        asyncio.create_task(self._execute_workflow_async(execution))
        
        return execution
    
    async def _execute_workflow_async(self, execution: WorkflowExecution):
        """Execute workflow asynchronously"""
        
        try:
            # Track running workflow
            self.running_workflows[execution.execution_id] = execution
            
            # Build execution graph
            workflow = execution.workflow
            execution_graph = self._build_execution_graph(workflow)
            
            # Execute tasks in topological order
            execution_context = {
                "workflow_id": workflow.id,
                "execution_id": execution.execution_id,
                "input_data": execution.input_data,
                "trigger_data": execution.trigger_data
            }
            
            await self._execute_workflow_graph(execution, execution_graph, execution_context)
            
            # Mark execution as completed
            execution.status = WorkflowStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            execution.duration_minutes = int(
                (execution.completed_at - execution.started_at).total_seconds() / 60
            )
            
        except Exception as e:
            logger.error(f"Workflow execution {execution.execution_id} failed: {e}")
            execution.status = WorkflowStatus.FAILED
            execution.completed_at = datetime.utcnow()
            execution.error_message = str(e)
            
            if execution.started_at:
                execution.duration_minutes = int(
                    (execution.completed_at - execution.started_at).total_seconds() / 60
                )
        
        finally:
            # Remove from running workflows
            self.running_workflows.pop(execution.execution_id, None)
            self.db.commit()
    
    def _build_execution_graph(self, workflow: WorkflowDefinition) -> nx.DiGraph:
        """Build workflow execution graph"""
        
        graph = nx.DiGraph()
        
        # Add task nodes
        for task in workflow.tasks:
            task_id = task["id"]
            graph.add_node(task_id, task=task)
        
        # Add dependency edges
        for task_id, dependencies in workflow.dependencies.items():
            for dep_id in dependencies:
                graph.add_edge(dep_id, task_id)
        
        return graph
    
    async def _execute_workflow_graph(
        self,
        execution: WorkflowExecution,
        graph: nx.DiGraph,
        context: Dict[str, Any]
    ):
        """Execute workflow graph in topological order"""
        
        # Get tasks in topological order
        task_order = list(nx.topological_sort(graph))
        
        task_results = {}
        
        # Execute tasks in batches (parallel execution where possible)
        while task_order:
            # Find tasks that can be executed in parallel
            ready_tasks = []
            for task_id in task_order:
                # Check if all dependencies are completed
                predecessors = list(graph.predecessors(task_id))
                if all(pred_id in task_results for pred_id in predecessors):
                    ready_tasks.append(task_id)
            
            if not ready_tasks:
                raise ValueError("No ready tasks found (possible circular dependency)")
            
            # Remove ready tasks from order
            for task_id in ready_tasks:
                task_order.remove(task_id)
            
            # Execute ready tasks in parallel
            tasks = []
            for task_id in ready_tasks:
                task_config = graph.nodes[task_id]["task"]
                input_data = self._prepare_task_input(task_id, graph, task_results, context)
                
                task_coroutine = self._execute_task_async(
                    execution, task_id, task_config, input_data, context
                )
                tasks.append((task_id, task_coroutine))
            
            # Wait for all tasks to complete
            for task_id, task_coroutine in tasks:
                try:
                    result = await task_coroutine
                    task_results[task_id] = result
                except Exception as e:
                    logger.error(f"Task {task_id} execution failed: {e}")
                    raise e
        
        # Store final results
        execution.output_data = task_results
        execution.execution_summary = {
            "total_tasks": len(task_results),
            "successful_tasks": len([r for r in task_results.values() if r.get("status") == "completed"]),
            "failed_tasks": len([r for r in task_results.values() if r.get("status") == "failed"])
        }
    
    def _prepare_task_input(
        self,
        task_id: str,
        graph: nx.DiGraph,
        task_results: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Prepare input data for task execution"""
        
        input_data = {
            "context": context,
            "dependencies": {}
        }
        
        # Get data from predecessor tasks
        for pred_id in graph.predecessors(task_id):
            pred_result = task_results.get(pred_id, {})
            input_data["dependencies"][pred_id] = pred_result.get("output", {})
        
        return input_data
    
    async def _execute_task_async(
        self,
        execution: WorkflowExecution,
        task_id: str,
        task_config: Dict[str, Any],
        input_data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a single workflow task"""
        
        # Get task record
        task = self.db.query(WorkflowTask).filter(
            and_(
                WorkflowTask.workflow_id == execution.workflow_id,
                WorkflowTask.task_id == task_id
            )
        ).first()
        
        if not task:
            raise ValueError(f"Task {task_id} not found")
        
        # Create task execution record
        task_execution = WorkflowTaskExecution(
            execution_id=f"{execution.execution_id}-{task_id}",
            workflow_execution_id=execution.id,
            task_id=task.id,
            status=TaskStatus.RUNNING,
            started_at=datetime.utcnow(),
            input_data=input_data
        )
        
        self.db.add(task_execution)
        self.db.commit()
        
        try:
            # Execute task based on type
            result = await self._execute_task_by_type(task, input_data, context)
            
            task_execution.status = TaskStatus.COMPLETED
            task_execution.completed_at = datetime.utcnow()
            task_execution.output_data = result
            task_execution.duration_seconds = int(
                (task_execution.completed_at - task_execution.started_at).total_seconds()
            )
            
            return {
                "status": "completed",
                "output": result,
                "duration_seconds": task_execution.duration_seconds
            }
            
        except Exception as e:
            task_execution.status = TaskStatus.FAILED
            task_execution.completed_at = datetime.utcnow()
            task_execution.error_message = str(e)
            
            if task_execution.started_at:
                task_execution.duration_seconds = int(
                    (task_execution.completed_at - task_execution.started_at).total_seconds()
                )
            
            # Retry logic
            if task_execution.attempt_number < task.retry_attempts:
                await asyncio.sleep(task.retry_delay_seconds)
                # Would implement retry logic here
            
            return {
                "status": "failed",
                "error": str(e),
                "duration_seconds": task_execution.duration_seconds
            }
        
        finally:
            self.db.commit()
    
    async def _execute_task_by_type(
        self,
        task: WorkflowTask,
        input_data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute task based on its type"""
        
        task_type = task.task_type
        task_config = task.task_config
        
        if task_type == "integration_call":
            return await self._execute_integration_call_task(task, input_data, task_config)
        
        elif task_type == "data_transformation":
            return await self._execute_data_transformation_task(task, input_data, task_config)
        
        elif task_type == "conditional":
            return await self._execute_conditional_task(task, input_data, task_config)
        
        elif task_type == "notification":
            return await self._execute_notification_task(task, input_data, task_config)
        
        elif task_type == "delay":
            return await self._execute_delay_task(task, input_data, task_config)
        
        else:
            raise ValueError(f"Unknown task type: {task_type}")
    
    async def _execute_integration_call_task(
        self,
        task: WorkflowTask,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute integration call task"""
        
        if not task.integration_id:
            raise ValueError("Integration ID is required for integration call task")
        
        integration_service = EnterpriseIntegrationService(self.db)
        
        method = config.get("method", "GET")
        endpoint = config.get("endpoint", "/")
        data = config.get("data", {})
        
        # Apply input mapping
        if task.input_mapping:
            mapped_data = {}
            for target_field, source_path in task.input_mapping.items():
                if source_path in input_data:
                    mapped_data[target_field] = input_data[source_path]
            data.update(mapped_data)
        
        result = await integration_service.execute_integration_request(
            task.integration_id,
            method,
            endpoint,
            data
        )
        
        # Apply output mapping
        if task.output_mapping:
            mapped_result = {}
            for target_field, source_path in task.output_mapping.items():
                if source_path in result:
                    mapped_result[target_field] = result[source_path]
            return mapped_result
        
        return result
    
    async def _execute_data_transformation_task(
        self,
        task: WorkflowTask,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute data transformation task"""
        
        transformations = config.get("transformations", [])
        data = input_data.get("dependencies", {})
        
        for transformation in transformations:
            transform_type = transformation.get("type")
            
            if transform_type == "filter":
                condition = transformation.get("condition")
                # Apply filter logic
                
            elif transform_type == "map":
                mapping = transformation.get("mapping")
                # Apply field mapping
                
            elif transform_type == "aggregate":
                aggregation = transformation.get("aggregation")
                # Apply aggregation logic
        
        return {"transformed_data": data}
    
    async def _execute_conditional_task(
        self,
        task: WorkflowTask,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute conditional task"""
        
        condition = config.get("condition")
        
        # Evaluate condition
        condition_result = self._evaluate_condition(input_data, condition)
        
        return {
            "condition_result": condition_result,
            "branch_taken": "true" if condition_result else "false"
        }
    
    async def _execute_notification_task(
        self,
        task: WorkflowTask,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute notification task"""
        
        notification_type = config.get("type", "email")
        recipients = config.get("recipients", [])
        message = config.get("message", "")
        
        # Send notification
        # This would integrate with actual notification service
        
        return {
            "notification_sent": True,
            "recipients": recipients,
            "type": notification_type
        }
    
    async def _execute_delay_task(
        self,
        task: WorkflowTask,
        input_data: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute delay task"""
        
        delay_seconds = config.get("delay_seconds", 60)
        
        await asyncio.sleep(delay_seconds)
        
        return {
            "delayed_seconds": delay_seconds,
            "completed_at": datetime.utcnow().isoformat()
        }
    
    def _evaluate_condition(self, data: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """Evaluate conditional expression"""
        
        if not condition:
            return True
        
        operator = condition.get("operator")
        field = condition.get("field")
        value = condition.get("value")
        
        if field not in data:
            return False
        
        field_value = data[field]
        
        if operator == "equals":
            return field_value == value
        elif operator == "not_equals":
            return field_value != value
        elif operator == "greater_than":
            return field_value > value
        elif operator == "less_than":
            return field_value < value
        elif operator == "contains":
            return value in str(field_value)
        elif operator == "exists":
            return field in data
        
        return True
    
    async def cancel_workflow_execution(self, execution_id: str) -> bool:
        """Cancel a running workflow execution"""
        
        execution = self.db.query(WorkflowExecution).filter(
            WorkflowExecution.execution_id == execution_id
        ).first()
        
        if not execution:
            return False
        
        if execution.status not in [WorkflowStatus.ACTIVE]:
            return False
        
        execution.status = WorkflowStatus.CANCELLED
        execution.completed_at = datetime.utcnow()
        
        if execution.started_at:
            execution.duration_minutes = int(
                (execution.completed_at - execution.started_at).total_seconds() / 60
            )
        
        self.db.commit()
        
        # Remove from running workflows
        self.running_workflows.pop(execution_id, None)
        
        return True