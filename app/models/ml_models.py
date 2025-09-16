from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON, Float, BigInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from ..database import Base

class ModelType(str, Enum):
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    ANOMALY_DETECTION = "anomaly_detection"
    TIME_SERIES = "time_series"
    NLP = "nlp"
    COMPUTER_VISION = "computer_vision"

class ModelStatus(str, Enum):
    DRAFT = "draft"
    TRAINING = "training"
    TRAINED = "trained"
    DEPLOYED = "deployed"
    DEPRECATED = "deprecated"
    FAILED = "failed"

class DeploymentStatus(str, Enum):
    PENDING = "pending"
    DEPLOYING = "deploying"
    ACTIVE = "active"
    INACTIVE = "inactive"
    FAILED = "failed"
    SCALING = "scaling"

class ExperimentStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class MLModel(Base):
    __tablename__ = "ml_models"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Model metadata
    model_type = Column(String(50), nullable=False)
    framework = Column(String(50))  # tensorflow, pytorch, scikit-learn, etc.
    version = Column(String(20), default="1.0")
    
    # Model configuration
    model_config = Column(JSON, nullable=False)
    hyperparameters = Column(JSON)
    feature_schema = Column(JSON)
    target_schema = Column(JSON)
    
    # Training data
    training_dataset_id = Column(Integer, ForeignKey("data_sets.id"))
    validation_dataset_id = Column(Integer, ForeignKey("data_sets.id"))
    test_dataset_id = Column(Integer, ForeignKey("data_sets.id"))
    
    # Model artifacts
    model_artifact_path = Column(String(500))
    model_size_bytes = Column(BigInteger)
    model_checksum = Column(String(64))
    
    # Performance metrics
    training_metrics = Column(JSON)
    validation_metrics = Column(JSON)
    test_metrics = Column(JSON)
    
    # Status and lifecycle
    status = Column(String(20), nullable=False, default=ModelStatus.DRAFT)
    training_started_at = Column(DateTime)
    training_completed_at = Column(DateTime)
    training_duration_seconds = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    parent_model_id = Column(Integer, ForeignKey("ml_models.id"))
    
    # Relationships
    org = relationship("Org")
    project = relationship("Project")
    creator = relationship("User")
    parent_model = relationship("MLModel", remote_side=[id])
    child_models = relationship("MLModel")
    training_dataset = relationship("DataSet", foreign_keys=[training_dataset_id])
    validation_dataset = relationship("DataSet", foreign_keys=[validation_dataset_id])
    test_dataset = relationship("DataSet", foreign_keys=[test_dataset_id])
    experiments = relationship("MLExperiment", back_populates="model")
    deployments = relationship("ModelDeployment", back_populates="model")
    predictions = relationship("ModelPrediction", back_populates="model")

class MLExperiment(Base):
    __tablename__ = "ml_experiments"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Experiment configuration
    experiment_config = Column(JSON, nullable=False)
    hyperparameter_space = Column(JSON)
    optimization_metric = Column(String(100))
    optimization_direction = Column(String(10), default="maximize")  # maximize or minimize
    
    # Experiment execution
    status = Column(String(20), nullable=False, default=ExperimentStatus.RUNNING)
    max_trials = Column(Integer, default=100)
    current_trial = Column(Integer, default=0)
    
    # Results
    best_trial_id = Column(Integer)
    best_score = Column(Float)
    best_hyperparameters = Column(JSON)
    
    # Timing
    started_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    model_id = Column(Integer, ForeignKey("ml_models.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    model = relationship("MLModel", back_populates="experiments")
    org = relationship("Org")
    creator = relationship("User")
    trials = relationship("ExperimentTrial", back_populates="experiment")

class ExperimentTrial(Base):
    __tablename__ = "experiment_trials"
    
    id = Column(Integer, primary_key=True, index=True)
    trial_number = Column(Integer, nullable=False)
    
    # Trial configuration
    hyperparameters = Column(JSON, nullable=False)
    
    # Trial execution
    status = Column(String(20), nullable=False, default="pending")
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Results
    metrics = Column(JSON)
    score = Column(Float)
    artifacts = Column(JSON)
    
    # Error handling
    error_message = Column(Text)
    logs = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    experiment_id = Column(Integer, ForeignKey("ml_experiments.id"), nullable=False)
    
    # Relationships
    experiment = relationship("MLExperiment", back_populates="trials")

class ModelDeployment(Base):
    __tablename__ = "model_deployments"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Deployment configuration
    deployment_config = Column(JSON, nullable=False)
    environment = Column(String(50), default="production")  # development, staging, production
    endpoint_url = Column(String(500))
    
    # Scaling configuration
    min_instances = Column(Integer, default=1)
    max_instances = Column(Integer, default=10)
    target_cpu_utilization = Column(Integer, default=70)
    
    # Resource requirements
    cpu_request = Column(Float)
    memory_request = Column(Integer)  # MB
    cpu_limit = Column(Float)
    memory_limit = Column(Integer)  # MB
    
    # Status and health
    status = Column(String(20), nullable=False, default=DeploymentStatus.PENDING)
    health_check_url = Column(String(500))
    last_health_check = Column(DateTime)
    health_status = Column(String(20), default="unknown")
    
    # Monitoring
    request_count = Column(BigInteger, default=0)
    error_count = Column(BigInteger, default=0)
    average_response_time = Column(Float)
    
    # Timestamps
    deployed_at = Column(DateTime)
    last_updated_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    model_id = Column(Integer, ForeignKey("ml_models.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    deployed_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    model = relationship("MLModel", back_populates="deployments")
    org = relationship("Org")
    deployer = relationship("User")
    predictions = relationship("ModelPrediction", back_populates="deployment")

class ModelPrediction(Base):
    __tablename__ = "model_predictions"
    
    id = Column(BigInteger, primary_key=True, index=True)
    prediction_id = Column(String(100), unique=True, index=True)
    
    # Request data
    input_data = Column(JSON, nullable=False)
    prediction_result = Column(JSON, nullable=False)
    confidence_score = Column(Float)
    
    # Request metadata
    request_id = Column(String(100))
    user_id = Column(Integer, ForeignKey("users.id"))
    client_ip = Column(String(45))
    user_agent = Column(String(500))
    
    # Performance metrics
    processing_time_ms = Column(Integer)
    model_version = Column(String(20))
    
    # Feedback and monitoring
    feedback_score = Column(Float)
    feedback_label = Column(String(100))
    is_correct = Column(Boolean)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now(), index=True)
    
    # Foreign keys
    model_id = Column(Integer, ForeignKey("ml_models.id"), nullable=False)
    deployment_id = Column(Integer, ForeignKey("model_deployments.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    
    # Relationships
    model = relationship("MLModel", back_populates="predictions")
    deployment = relationship("ModelDeployment", back_populates="predictions")
    org = relationship("Org")
    user = relationship("User")

class FeatureStore(Base):
    __tablename__ = "feature_stores"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Feature group metadata
    feature_group_name = Column(String(255), nullable=False)
    version = Column(String(20), default="1.0")
    schema_definition = Column(JSON, nullable=False)
    
    # Data source configuration
    data_source_config = Column(JSON, nullable=False)
    update_frequency = Column(String(50))  # hourly, daily, weekly, etc.
    
    # Feature engineering
    transformation_config = Column(JSON)
    validation_rules = Column(JSON)
    
    # Statistics and quality
    feature_statistics = Column(JSON)
    data_quality_metrics = Column(JSON)
    last_updated_at = Column(DateTime)
    
    # Access control
    is_public = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"))
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    data_source = relationship("DataSource")
    creator = relationship("User")
    features = relationship("Feature", back_populates="feature_store")

class Feature(Base):
    __tablename__ = "features"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    
    # Feature metadata
    data_type = Column(String(50), nullable=False)  # numerical, categorical, text, etc.
    feature_type = Column(String(50))  # raw, derived, aggregated
    
    # Feature definition
    definition = Column(Text)  # SQL query or transformation logic
    dependencies = Column(JSON)  # List of dependent features
    
    # Feature statistics
    statistics = Column(JSON)
    importance_score = Column(Float)
    usage_count = Column(Integer, default=0)
    
    # Data quality
    null_percentage = Column(Float)
    unique_values_count = Column(Integer)
    min_value = Column(Float)
    max_value = Column(Float)
    mean_value = Column(Float)
    std_deviation = Column(Float)
    
    # Status
    is_active = Column(Boolean, default=True)
    last_computed_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    feature_store_id = Column(Integer, ForeignKey("feature_stores.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    feature_store = relationship("FeatureStore", back_populates="features")
    creator = relationship("User")

class ModelRegistry(Base):
    __tablename__ = "model_registry"
    
    id = Column(Integer, primary_key=True, index=True)
    model_name = Column(String(255), nullable=False, index=True)
    model_version = Column(String(20), nullable=False)
    
    # Model metadata
    model_type = Column(String(50), nullable=False)
    framework = Column(String(50), nullable=False)
    framework_version = Column(String(20))
    
    # Model artifacts
    artifact_uri = Column(String(500), nullable=False)
    model_size_bytes = Column(BigInteger)
    signature = Column(JSON)  # Input/output schema
    
    # Model lineage
    training_dataset_uri = Column(String(500))
    training_code_uri = Column(String(500))
    training_metrics = Column(JSON)
    
    # Lifecycle
    stage = Column(String(20), default="staging")  # staging, production, archived
    
    # Tags and metadata
    tags = Column(JSON, default=dict)
    metadata = Column(JSON, default=dict)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    model_id = Column(Integer, ForeignKey("ml_models.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    registered_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    model = relationship("MLModel")
    org = relationship("Org")
    registrar = relationship("User")

class AutoMLJob(Base):
    __tablename__ = "automl_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Job configuration
    task_type = Column(String(50), nullable=False)  # classification, regression, etc.
    target_column = Column(String(255), nullable=False)
    feature_columns = Column(JSON)
    
    # AutoML settings
    max_runtime_minutes = Column(Integer, default=60)
    max_models = Column(Integer, default=10)
    optimization_metric = Column(String(100))
    
    # Data configuration
    train_data_config = Column(JSON, nullable=False)
    validation_split = Column(Float, default=0.2)
    
    # Job execution
    status = Column(String(20), nullable=False, default="pending")
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_minutes = Column(Integer)
    
    # Results
    best_model_id = Column(Integer, ForeignKey("ml_models.id"))
    best_score = Column(Float)
    leaderboard = Column(JSON)
    
    # Error handling
    error_message = Column(Text)
    logs = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    dataset_id = Column(Integer, ForeignKey("data_sets.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    org = relationship("Org")
    dataset = relationship("DataSet")
    creator = relationship("User")
    best_model = relationship("MLModel")

class ModelMonitor(Base):
    __tablename__ = "model_monitors"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Monitor configuration
    monitor_type = Column(String(50), nullable=False)  # drift, performance, data_quality
    threshold_config = Column(JSON, nullable=False)
    
    # Monitoring schedule
    check_frequency = Column(String(50), default="daily")  # hourly, daily, weekly
    
    # Alert configuration
    alert_enabled = Column(Boolean, default=True)
    alert_channels = Column(JSON)
    
    # Status
    enabled = Column(Boolean, default=True)
    last_check_at = Column(DateTime)
    last_alert_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Foreign keys
    model_id = Column(Integer, ForeignKey("ml_models.id"), nullable=False)
    deployment_id = Column(Integer, ForeignKey("model_deployments.id"))
    org_id = Column(Integer, ForeignKey("orgs.id"), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Relationships
    model = relationship("MLModel")
    deployment = relationship("ModelDeployment")
    org = relationship("Org")
    creator = relationship("User")
    monitor_results = relationship("ModelMonitorResult", back_populates="monitor")

class ModelMonitorResult(Base):
    __tablename__ = "model_monitor_results"
    
    id = Column(BigInteger, primary_key=True, index=True)
    
    # Check results
    check_timestamp = Column(DateTime, nullable=False, default=func.now(), index=True)
    status = Column(String(20), nullable=False)  # passed, failed, warning
    
    # Metrics and scores
    drift_score = Column(Float)
    performance_metrics = Column(JSON)
    data_quality_score = Column(Float)
    
    # Detailed results
    results_data = Column(JSON)
    anomalies_detected = Column(JSON)
    
    # Alert status
    alert_triggered = Column(Boolean, default=False)
    alert_message = Column(Text)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Foreign keys
    monitor_id = Column(Integer, ForeignKey("model_monitors.id"), nullable=False)
    
    # Relationships
    monitor = relationship("ModelMonitor", back_populates="monitor_results")