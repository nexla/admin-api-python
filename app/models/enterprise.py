from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, BigInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class IntegrationType(str, Enum):
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    WEBHOOK = "webhook"
    MESSAGE_QUEUE = "message_queue"
    DATABASE = "database"
    FILE_SYSTEM = "file_system"
    SFTP = "sftp"
    S3 = "s3"
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"

class AuthenticationType(str, Enum):
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    JWT = "jwt"
    BASIC_AUTH = "basic_auth"
    BEARER_TOKEN = "bearer_token"
    CERTIFICATE = "certificate"
    CUSTOM = "custom"

class IntegrationStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    TESTING = "testing"
    DEPRECATED = "deprecated"

class WorkflowStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

class EnterpriseIntegration(Base):
    __tablename__ = "enterprise_integrations"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Integration configuration
    integration_type = Column(String(50), nullable=False)
    endpoint_url = Column(String(1000))
    authentication_type = Column(String(50), nullable=False)
    authentication_config = Column(JSON, nullable=False)
    
    # Connection settings
    connection_config = Column(JSON, nullable=False)
    timeout_seconds = Column(Integer, default=30)
    retry_config = Column(JSON)
    
    # Rate limiting
    rate_limit_config = Column(JSON)
    
    # Data mapping and transformation
    request_mapping = Column(JSON)
    response_mapping = Column(JSON)
    data_transformation = Column(JSON)
    
    # Monitoring and health
    health_check_config = Column(JSON)
    monitoring_config = Column(JSON)
    
    # Status and metrics
    status = Column(String(20), nullable=False, default=IntegrationStatus.INACTIVE)
    last_health_check = Column(DateTime)
    health_status = Column(String(20), default="unknown")
    
    # Usage statistics
    total_requests = Column(BigInteger, default=0)
    successful_requests = Column(BigInteger, default=0)
    failed_requests = Column(BigInteger, default=0)
    avg_response_time = Column(Float)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    last_used_at = Column(DateTime)
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    integration_logs = relationship("IntegrationLog", back_populates="integration")
    workflow_tasks = relationship("WorkflowTask", back_populates="integration")

class IntegrationLog(Base):
    __tablename__ = "integration_logs"
    
    id = Column(BigInteger, primary_key=True, index=True)
    log_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Request details
    request_method = Column(String(10))
    request_url = Column(String(1000))
    request_headers = Column(JSON)
    request_body = Column(Text)
    
    # Response details
    response_status = Column(Integer)
    response_headers = Column(JSON)
    response_body = Column(Text)
    response_time_ms = Column(Integer)
    
    # Error handling
    error_message = Column(Text)
    error_code = Column(String(50))
    retry_attempt = Column(Integer, default=0)
    
    # Context
    correlation_id = Column(String(100))
    user_id = Column(Integer, ForeignKey("users.id"))
    workflow_execution_id = Column(String(100))
    
    # Timestamps
    timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    
    # Foreign keys
    integration_id = Column(Integer, ForeignKey("enterprise_integrations.id"), nullable=False)
    
    # Relationships
    integration = relationship("EnterpriseIntegration", back_populates="integration_logs")
    user = relationship("User")

class APIGateway(Base):
    __tablename__ = "api_gateways"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Gateway configuration
    gateway_url = Column(String(500), nullable=False)
    version = Column(String(20), default="v1")
    
    # Routing configuration
    routing_rules = Column(JSON, nullable=False)
    load_balancing_config = Column(JSON)
    
    # Security configuration
    authentication_required = Column(Boolean, default=True)
    authorization_config = Column(JSON)
    cors_config = Column(JSON)
    
    # Rate limiting and throttling
    rate_limiting_config = Column(JSON)
    throttling_config = Column(JSON)
    
    # Caching configuration
    caching_config = Column(JSON)
    
    # Monitoring and logging
    logging_config = Column(JSON)
    metrics_config = Column(JSON)
    
    # Circuit breaker configuration
    circuit_breaker_config = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    gateway_routes = relationship("GatewayRoute", back_populates="gateway")

class GatewayRoute(Base):
    __tablename__ = "gateway_routes"
    
    id = Column(Integer, primary_key=True, index=True)
    route_name = Column(String(255), nullable=False)
    
    # Route configuration
    path_pattern = Column(String(500), nullable=False)
    http_methods = Column(JSON, nullable=False)
    
    # Target configuration
    target_type = Column(String(50), nullable=False)  # service, integration, function
    target_config = Column(JSON, nullable=False)
    
    # Middleware configuration
    middleware_chain = Column(JSON, default=list)
    
    # Request/Response transformation
    request_transformation = Column(JSON)
    response_transformation = Column(JSON)
    
    # Security configuration
    authentication_required = Column(Boolean, default=True)
    authorization_rules = Column(JSON)
    
    # Performance configuration
    timeout_seconds = Column(Integer, default=30)
    retry_config = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    priority = Column(Integer, default=100)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    gateway_id = Column(Integer, ForeignKey("api_gateways.id"), nullable=False)
    integration_id = Column(Integer, ForeignKey("enterprise_integrations.id"))
    
    # Relationships
    gateway = relationship("APIGateway", back_populates="gateway_routes")
    integration = relationship("EnterpriseIntegration")

class WorkflowDefinition(Base):
    __tablename__ = "workflow_definitions"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Workflow configuration
    workflow_config = Column(JSON, nullable=False)
    version = Column(String(20), default="1.0")
    
    # Trigger configuration
    triggers = Column(JSON, nullable=False)
    
    # Tasks and dependencies
    tasks = Column(JSON, nullable=False)
    dependencies = Column(JSON, default=dict)
    
    # Execution settings
    timeout_minutes = Column(Integer, default=60)
    max_retries = Column(Integer, default=3)
    parallel_execution = Column(Boolean, default=False)
    
    # Error handling
    error_handling_config = Column(JSON)
    rollback_config = Column(JSON)
    
    # Monitoring and notifications
    monitoring_config = Column(JSON)
    notification_config = Column(JSON)
    
    # Status
    status = Column(String(20), nullable=False, default=WorkflowStatus.DRAFT)
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    executions = relationship("WorkflowExecution", back_populates="workflow")

class WorkflowExecution(Base):
    __tablename__ = "workflow_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Execution details
    status = Column(String(20), nullable=False, default=WorkflowStatus.ACTIVE)
    trigger_type = Column(String(50))
    trigger_data = Column(JSON)
    
    # Input and context
    input_data = Column(JSON)
    execution_context = Column(JSON)
    
    # Timing
    started_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime)
    duration_minutes = Column(Integer)
    
    # Results
    output_data = Column(JSON)
    execution_summary = Column(JSON)
    
    # Error handling
    error_message = Column(Text)
    failed_task_id = Column(String(100))
    rollback_executed = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    workflow_id = Column(Integer, ForeignKey("workflow_definitions.id"), nullable=False)
    triggered_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    workflow = relationship("WorkflowDefinition", back_populates="executions")
    trigger_user = relationship("User")
    task_executions = relationship("WorkflowTaskExecution", back_populates="workflow_execution")

class WorkflowTask(Base):
    __tablename__ = "workflow_tasks"
    
    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String(100), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Task configuration
    task_type = Column(String(50), nullable=False)
    task_config = Column(JSON, nullable=False)
    
    # Input/Output configuration
    input_mapping = Column(JSON)
    output_mapping = Column(JSON)
    
    # Execution settings
    timeout_minutes = Column(Integer, default=30)
    retry_attempts = Column(Integer, default=3)
    retry_delay_seconds = Column(Integer, default=60)
    
    # Conditions
    execution_conditions = Column(JSON)
    skip_conditions = Column(JSON)
    
    # Error handling
    error_handling = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    workflow_id = Column(Integer, ForeignKey("workflow_definitions.id"), nullable=False)
    integration_id = Column(Integer, ForeignKey("enterprise_integrations.id"))
    
    # Relationships
    workflow = relationship("WorkflowDefinition")
    integration = relationship("EnterpriseIntegration", back_populates="workflow_tasks")
    task_executions = relationship("WorkflowTaskExecution", back_populates="task")

class WorkflowTaskExecution(Base):
    __tablename__ = "workflow_task_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(100), nullable=False, index=True)
    
    # Execution details
    status = Column(String(20), nullable=False, default=TaskStatus.PENDING)
    attempt_number = Column(Integer, default=1)
    
    # Input and output
    input_data = Column(JSON)
    output_data = Column(JSON)
    
    # Timing
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Error handling
    error_message = Column(Text)
    error_details = Column(JSON)
    
    # Logs
    execution_logs = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    workflow_execution_id = Column(Integer, ForeignKey("workflow_executions.id"), nullable=False)
    task_id = Column(Integer, ForeignKey("workflow_tasks.id"), nullable=False)
    
    # Relationships
    workflow_execution = relationship("WorkflowExecution", back_populates="task_executions")
    task = relationship("WorkflowTask", back_populates="task_executions")

class EventBus(Base):
    __tablename__ = "event_buses"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Event bus configuration
    bus_type = Column(String(50), nullable=False)  # internal, kafka, rabbitmq, redis
    connection_config = Column(JSON, nullable=False)
    
    # Message configuration
    serialization_format = Column(String(20), default="json")
    compression_enabled = Column(Boolean, default=False)
    
    # Delivery guarantees
    delivery_guarantee = Column(String(20), default="at_least_once")
    dead_letter_queue = Column(Boolean, default=True)
    
    # Monitoring
    monitoring_enabled = Column(Boolean, default=True)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    topics = relationship("EventTopic", back_populates="event_bus")

class EventTopic(Base):
    __tablename__ = "event_topics"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Topic configuration
    topic_config = Column(JSON)
    schema_definition = Column(JSON)
    
    # Partitioning
    partition_count = Column(Integer, default=1)
    partition_key_field = Column(String(100))
    
    # Retention
    retention_policy = Column(JSON)
    
    # Access control
    access_control = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    event_bus_id = Column(Integer, ForeignKey("event_buses.id"), nullable=False)
    
    # Relationships
    event_bus = relationship("EventBus", back_populates="topics")
    subscriptions = relationship("EventSubscription", back_populates="topic")
    events = relationship("Event", back_populates="topic")

class EventSubscription(Base):
    __tablename__ = "event_subscriptions"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    
    # Subscription configuration
    subscription_type = Column(String(50), nullable=False)  # push, pull
    endpoint_config = Column(JSON)
    
    # Filtering
    filter_config = Column(JSON)
    
    # Delivery configuration
    delivery_config = Column(JSON)
    retry_config = Column(JSON)
    
    # Dead letter handling
    dead_letter_config = Column(JSON)
    
    # Consumer group
    consumer_group = Column(String(255))
    
    # Status
    enabled = Column(Boolean, default=True)
    last_consumed_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    topic_id = Column(Integer, ForeignKey("event_topics.id"), nullable=False)
    subscriber_integration_id = Column(Integer, ForeignKey("enterprise_integrations.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    topic = relationship("EventTopic", back_populates="subscriptions")
    subscriber_integration = relationship("EnterpriseIntegration")
    creator = relationship("User")

class Event(Base):
    __tablename__ = "events"
    
    id = Column(BigInteger, primary_key=True, index=True)
    event_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Event details
    event_type = Column(String(100), nullable=False, index=True)
    event_version = Column(String(20), default="1.0")
    
    # Event data
    event_data = Column(JSON, nullable=False)
    event_metadata = Column(JSON)
    
    # Routing
    partition_key = Column(String(255))
    routing_key = Column(String(255))
    
    # Source information
    source_service = Column(String(100))
    source_user_id = Column(Integer, ForeignKey("users.id"))
    correlation_id = Column(String(100), index=True)
    
    # Delivery tracking
    published_at = Column(DateTime, nullable=False, default=func.now(), index=True)
    delivery_count = Column(Integer, default=0)
    last_delivered_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    topic_id = Column(Integer, ForeignKey("event_topics.id"), nullable=False)
    
    # Relationships
    topic = relationship("EventTopic", back_populates="events")
    source_user = relationship("User")

class ServiceDiscovery(Base):
    __tablename__ = "service_discovery"
    
    id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String(255), nullable=False, index=True)
    service_version = Column(String(20), default="1.0")
    
    # Service configuration
    service_type = Column(String(50), nullable=False)
    endpoint_urls = Column(JSON, nullable=False)
    health_check_url = Column(String(500))
    
    # Service metadata
    service_metadata = Column(JSON)
    tags = Column(JSON, default=list)
    
    # Load balancing
    load_balancing_weight = Column(Integer, default=100)
    
    # Health status
    health_status = Column(String(20), default="unknown")
    last_health_check = Column(DateTime)
    
    # Registration details
    registered_at = Column(DateTime, nullable=False, default=func.now())
    last_heartbeat = Column(DateTime)
    ttl_seconds = Column(Integer, default=60)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    registered_by = Column(Integer, ForeignKey("users.id"))
    
    # Relationships
    org = relationship("Org")
    registrar = relationship("User")