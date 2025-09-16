from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, BigInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class NodeStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DRAINING = "draining"
    FAILED = "failed"
    MAINTENANCE = "maintenance"

class JobStatus(str, Enum):
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"

class JobPriority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

class ScalingDirection(str, Enum):
    UP = "up"
    DOWN = "down"

class ClusterNode(Base):
    __tablename__ = "cluster_nodes"
    
    id = Column(Integer, primary_key=True, index=True)
    node_id = Column(String(100), nullable=False, unique=True, index=True)
    node_name = Column(String(255), nullable=False)
    
    # Node configuration
    node_type = Column(String(50), nullable=False)  # worker, master, edge
    hostname = Column(String(255), nullable=False)
    ip_address = Column(String(45), nullable=False)
    port = Column(Integer, default=8080)
    
    # Resource allocation
    cpu_cores = Column(Integer, nullable=False)
    memory_gb = Column(Float, nullable=False)
    storage_gb = Column(Float)
    gpu_count = Column(Integer, default=0)
    
    # Resource usage
    cpu_usage_percent = Column(Float, default=0.0)
    memory_usage_percent = Column(Float, default=0.0)
    storage_usage_percent = Column(Float, default=0.0)
    network_usage_mbps = Column(Float, default=0.0)
    
    # Node status
    status = Column(String(20), nullable=False, default=NodeStatus.INACTIVE)
    health_score = Column(Float, default=100.0)
    
    # Capacity and scheduling
    max_concurrent_jobs = Column(Integer, default=10)
    current_job_count = Column(Integer, default=0)
    job_queue_size = Column(Integer, default=0)
    
    # Network and communication
    api_endpoint = Column(String(500))
    heartbeat_interval = Column(Integer, default=30)
    last_heartbeat = Column(DateTime)
    
    # Node metadata
    labels = Column(JSON, default=dict)
    annotations = Column(JSON, default=dict)
    node_metadata = Column(JSON, default=dict)
    
    # Geographic location
    region = Column(String(100))
    availability_zone = Column(String(100))
    datacenter = Column(String(100))
    
    # Timestamps
    registered_at = Column(DateTime, nullable=False, default=func.now())
    last_seen_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    cluster_id = Column(Integer, ForeignKey("clusters.id"), nullable=False)
    
    # Relationships
    cluster = relationship("Cluster")
    jobs = relationship("DistributedJob", back_populates="assigned_node")
    node_metrics = relationship("NodeMetric", back_populates="node")

class DistributedJob(Base):
    __tablename__ = "distributed_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(100), nullable=False, unique=True, index=True)
    job_name = Column(String(255), nullable=False)
    
    # Job configuration
    job_type = Column(String(50), nullable=False)
    job_config = Column(JSON, nullable=False)
    
    # Resource requirements
    cpu_cores_required = Column(Float, nullable=False)
    memory_gb_required = Column(Float, nullable=False)
    storage_gb_required = Column(Float, default=0)
    gpu_required = Column(Boolean, default=False)
    
    # Scheduling configuration
    priority = Column(String(20), default=JobPriority.NORMAL)
    scheduling_constraints = Column(JSON, default=dict)
    preferred_nodes = Column(JSON, default=list)
    node_affinity = Column(JSON, default=dict)
    
    # Execution configuration
    max_retries = Column(Integer, default=3)
    timeout_minutes = Column(Integer, default=60)
    environment_vars = Column(JSON, default=dict)
    
    # Input/Output
    input_data = Column(JSON)
    output_data = Column(JSON)
    input_files = Column(JSON, default=list)
    output_files = Column(JSON, default=list)
    
    # Job status and progress
    status = Column(String(20), nullable=False, default=JobStatus.PENDING)
    progress_percent = Column(Float, default=0.0)
    current_step = Column(String(255))
    
    # Execution tracking
    scheduled_at = Column(DateTime)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Error handling
    error_message = Column(Text)
    error_details = Column(JSON)
    retry_count = Column(Integer, default=0)
    last_retry_at = Column(DateTime)
    
    # Dependencies
    depends_on_jobs = Column(JSON, default=list)
    dependency_satisfied = Column(Boolean, default=True)
    
    # Resource usage tracking
    actual_cpu_usage = Column(Float)
    actual_memory_usage = Column(Float)
    actual_storage_usage = Column(Float)
    
    # Logs and monitoring
    log_file_path = Column(String(500))
    monitoring_data = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    cluster_id = Column(Integer, ForeignKey("clusters.id"), nullable=False)
    assigned_node_id = Column(Integer, ForeignKey("cluster_nodes.id"))
    submitted_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    cluster = relationship("Cluster")
    assigned_node = relationship("ClusterNode", back_populates="jobs")
    submitter = relationship("User")
    org = relationship("Org")
    job_steps = relationship("JobStep", back_populates="job")

class JobStep(Base):
    __tablename__ = "job_steps"
    
    id = Column(Integer, primary_key=True, index=True)
    step_id = Column(String(100), nullable=False, index=True)
    step_name = Column(String(255), nullable=False)
    
    # Step configuration
    step_type = Column(String(50), nullable=False)
    step_config = Column(JSON, nullable=False)
    step_order = Column(Integer, nullable=False)
    
    # Step status
    status = Column(String(20), default=JobStatus.PENDING)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Step results
    output_data = Column(JSON)
    error_message = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    job_id = Column(Integer, ForeignKey("distributed_jobs.id"), nullable=False)
    
    # Relationships
    job = relationship("DistributedJob", back_populates="job_steps")

class LoadBalancer(Base):
    __tablename__ = "load_balancers"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Load balancer configuration
    load_balancer_type = Column(String(50), nullable=False)  # round_robin, weighted, least_connections
    algorithm_config = Column(JSON, default=dict)
    
    # Health check configuration
    health_check_config = Column(JSON, nullable=False)
    health_check_interval = Column(Integer, default=30)
    
    # Targets and routing
    target_nodes = Column(JSON, nullable=False)
    routing_rules = Column(JSON, default=list)
    
    # Performance settings
    connection_timeout = Column(Integer, default=30)
    request_timeout = Column(Integer, default=60)
    max_connections = Column(Integer, default=1000)
    
    # SSL/TLS configuration
    ssl_enabled = Column(Boolean, default=False)
    ssl_config = Column(JSON)
    
    # Monitoring and metrics
    metrics_enabled = Column(Boolean, default=True)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    cluster_id = Column(Integer, ForeignKey("clusters.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    cluster = relationship("Cluster")
    creator = relationship("User")

class AutoScaler(Base):
    __tablename__ = "auto_scalers"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Scaling configuration
    min_nodes = Column(Integer, nullable=False, default=1)
    max_nodes = Column(Integer, nullable=False, default=10)
    target_cpu_utilization = Column(Float, default=70.0)
    target_memory_utilization = Column(Float, default=80.0)
    
    # Scaling metrics
    scaling_metrics = Column(JSON, nullable=False)
    
    # Scaling behavior
    scale_up_cooldown = Column(Integer, default=300)  # seconds
    scale_down_cooldown = Column(Integer, default=600)  # seconds
    scale_up_step_size = Column(Integer, default=1)
    scale_down_step_size = Column(Integer, default=1)
    
    # Node template for scaling
    node_template = Column(JSON, nullable=False)
    
    # Status
    enabled = Column(Boolean, default=True)
    last_scaling_action = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    cluster_id = Column(Integer, ForeignKey("clusters.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    cluster = relationship("Cluster")
    creator = relationship("User")
    scaling_events = relationship("ScalingEvent", back_populates="auto_scaler")

class ScalingEvent(Base):
    __tablename__ = "scaling_events"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Event details
    scaling_direction = Column(String(10), nullable=False)
    trigger_metric = Column(String(100), nullable=False)
    trigger_value = Column(Float, nullable=False)
    threshold_value = Column(Float, nullable=False)
    
    # Scaling action
    nodes_added = Column(Integer, default=0)
    nodes_removed = Column(Integer, default=0)
    target_node_count = Column(Integer, nullable=False)
    
    # Event status
    status = Column(String(20), default="initiated")
    started_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime)
    
    # Results
    success = Column(Boolean)
    error_message = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    auto_scaler_id = Column(Integer, ForeignKey("auto_scalers.id"), nullable=False)
    
    # Relationships
    auto_scaler = relationship("AutoScaler", back_populates="scaling_events")

class NodeMetric(Base):
    __tablename__ = "node_metrics"
    
    id = Column(BigInteger, primary_key=True, index=True)
    
    # Metric details
    metric_name = Column(String(100), nullable=False, index=True)
    metric_value = Column(Float, nullable=False)
    metric_unit = Column(String(20))
    
    # Resource metrics
    cpu_usage_percent = Column(Float)
    memory_usage_percent = Column(Float)
    storage_usage_percent = Column(Float)
    network_io_mbps = Column(Float)
    disk_io_mbps = Column(Float)
    
    # System metrics
    load_average = Column(Float)
    process_count = Column(Integer)
    connection_count = Column(Integer)
    
    # Application metrics
    active_jobs = Column(Integer)
    queued_jobs = Column(Integer)
    completed_jobs_hour = Column(Integer)
    error_rate_percent = Column(Float)
    
    # Timestamps
    timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    
    # Foreign keys
    node_id = Column(Integer, ForeignKey("cluster_nodes.id"), nullable=False)
    
    # Relationships
    node = relationship("ClusterNode", back_populates="node_metrics")

class TaskQueue(Base):
    __tablename__ = "task_queues"
    
    id = Column(Integer, primary_key=True, index=True)
    queue_name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)
    
    # Queue configuration
    queue_type = Column(String(50), default="fifo")  # fifo, priority, delayed
    max_size = Column(Integer, default=10000)
    max_retries = Column(Integer, default=3)
    
    # Processing configuration
    batch_size = Column(Integer, default=1)
    processing_timeout = Column(Integer, default=300)
    
    # Dead letter queue
    dead_letter_enabled = Column(Boolean, default=True)
    dead_letter_max_age = Column(Integer, default=604800)  # 7 days
    
    # Queue metrics
    total_enqueued = Column(BigInteger, default=0)
    total_processed = Column(BigInteger, default=0)
    total_failed = Column(BigInteger, default=0)
    current_size = Column(Integer, default=0)
    
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
    tasks = relationship("QueuedTask", back_populates="queue")

class QueuedTask(Base):
    __tablename__ = "queued_tasks"
    
    id = Column(BigInteger, primary_key=True, index=True)
    task_id = Column(String(100), nullable=False, unique=True, index=True)
    
    # Task details
    task_type = Column(String(100), nullable=False)
    task_data = Column(JSON, nullable=False)
    
    # Priority and scheduling
    priority = Column(Integer, default=100)
    scheduled_at = Column(DateTime)
    delay_until = Column(DateTime)
    
    # Processing tracking
    status = Column(String(20), default="pending")
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    
    # Execution details
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    processing_time_ms = Column(Integer)
    
    # Results
    result_data = Column(JSON)
    error_message = Column(Text)
    error_details = Column(JSON)
    
    # Worker information
    worker_node_id = Column(Integer, ForeignKey("cluster_nodes.id"))
    worker_process_id = Column(String(100))
    
    # Timestamps
    enqueued_at = Column(DateTime, nullable=False, default=func.now(), index=True)
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    queue_id = Column(Integer, ForeignKey("task_queues.id"), nullable=False)
    
    # Relationships
    queue = relationship("TaskQueue", back_populates="tasks")
    worker_node = relationship("ClusterNode")

class DistributedCache(Base):
    __tablename__ = "distributed_caches"
    
    id = Column(Integer, primary_key=True, index=True)
    cache_name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)
    
    # Cache configuration
    cache_type = Column(String(50), default="redis")  # redis, memcached, hazelcast
    connection_config = Column(JSON, nullable=False)
    
    # Cache settings
    default_ttl = Column(Integer, default=3600)  # seconds
    max_memory_mb = Column(Integer, default=1024)
    eviction_policy = Column(String(50), default="lru")
    
    # Clustering configuration
    cluster_enabled = Column(Boolean, default=False)
    replication_factor = Column(Integer, default=1)
    consistency_level = Column(String(20), default="eventual")
    
    # Performance settings
    connection_pool_size = Column(Integer, default=10)
    read_timeout = Column(Integer, default=5)
    write_timeout = Column(Integer, default=5)
    
    # Monitoring
    metrics_enabled = Column(Boolean, default=True)
    
    # Status
    enabled = Column(Boolean, default=True)
    
    # Cache statistics
    total_keys = Column(BigInteger, default=0)
    cache_hits = Column(BigInteger, default=0)
    cache_misses = Column(BigInteger, default=0)
    memory_usage_mb = Column(Float, default=0.0)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    creator = relationship("User")
    cache_entries = relationship("CacheEntry", back_populates="cache")

class CacheEntry(Base):
    __tablename__ = "cache_entries"
    
    id = Column(BigInteger, primary_key=True, index=True)
    cache_key = Column(String(500), nullable=False, index=True)
    
    # Entry data
    cache_value = Column(Text)
    value_type = Column(String(50), default="string")
    compressed = Column(Boolean, default=False)
    
    # Entry metadata
    size_bytes = Column(Integer)
    access_count = Column(Integer, default=0)
    
    # TTL and expiration
    ttl_seconds = Column(Integer)
    expires_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    last_accessed_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    cache_id = Column(Integer, ForeignKey("distributed_caches.id"), nullable=False)
    
    # Relationships
    cache = relationship("DistributedCache", back_populates="cache_entries")