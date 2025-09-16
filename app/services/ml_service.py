import json
import uuid
import pickle
import joblib
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc
import logging
import asyncio
from pathlib import Path
import hashlib

from ..models.ml_models import (
    MLModel, MLExperiment, ExperimentTrial, ModelDeployment, ModelPrediction,
    FeatureStore, Feature, ModelRegistry, AutoMLJob, ModelMonitor, 
    ModelMonitorResult, ModelType, ModelStatus, DeploymentStatus
)
from ..models.user import User
from ..models.org import Org
from ..models.data_set import DataSet

logger = logging.getLogger(__name__)

class MLService:
    """Machine Learning model lifecycle management service"""
    
    def __init__(self, db: Session, model_storage_path: str = "/tmp/ml_models"):
        self.db = db
        self.model_storage_path = Path(model_storage_path)
        self.model_storage_path.mkdir(parents=True, exist_ok=True)
    
    async def create_model(
        self,
        name: str,
        model_type: ModelType,
        model_config: Dict[str, Any],
        org_id: int,
        created_by: int,
        description: str = None,
        project_id: Optional[int] = None,
        framework: str = "scikit-learn"
    ) -> MLModel:
        """Create a new ML model"""
        
        # Validate model configuration
        self._validate_model_config(model_type, model_config)
        
        model = MLModel(
            name=name,
            description=description,
            model_type=model_type,
            framework=framework,
            model_config=model_config,
            org_id=org_id,
            project_id=project_id,
            created_by=created_by
        )
        
        self.db.add(model)
        self.db.commit()
        self.db.refresh(model)
        
        return model
    
    def _validate_model_config(self, model_type: ModelType, config: Dict[str, Any]):
        """Validate model configuration based on type"""
        
        required_fields = {
            ModelType.CLASSIFICATION: ["algorithm", "features"],
            ModelType.REGRESSION: ["algorithm", "features", "target"],
            ModelType.CLUSTERING: ["algorithm", "features", "n_clusters"],
            ModelType.ANOMALY_DETECTION: ["algorithm", "features"],
            ModelType.TIME_SERIES: ["algorithm", "features", "target", "time_column"]
        }
        
        if model_type in required_fields:
            for field in required_fields[model_type]:
                if field not in config:
                    raise ValueError(f"Missing required field '{field}' for {model_type} model")
    
    async def train_model(
        self,
        model_id: int,
        training_dataset_id: int,
        hyperparameters: Optional[Dict[str, Any]] = None,
        validation_dataset_id: Optional[int] = None
    ) -> MLModel:
        """Train an ML model"""
        
        model = self.db.query(MLModel).filter(MLModel.id == model_id).first()
        if not model:
            raise ValueError("Model not found")
        
        if model.status not in [ModelStatus.DRAFT, ModelStatus.FAILED]:
            raise ValueError(f"Model cannot be trained in status: {model.status}")
        
        # Get training dataset
        training_dataset = self.db.query(DataSet).filter(
            DataSet.id == training_dataset_id
        ).first()
        
        if not training_dataset:
            raise ValueError("Training dataset not found")
        
        # Update model status
        model.status = ModelStatus.TRAINING
        model.training_started_at = datetime.utcnow()
        model.training_dataset_id = training_dataset_id
        model.validation_dataset_id = validation_dataset_id
        
        if hyperparameters:
            model.hyperparameters = hyperparameters
        
        self.db.commit()
        
        try:
            # Start training asynchronously
            asyncio.create_task(self._train_model_async(model))
            
        except Exception as e:
            model.status = ModelStatus.FAILED
            logger.error(f"Failed to start training for model {model_id}: {e}")
            self.db.commit()
            raise e
        
        return model
    
    async def _train_model_async(self, model: MLModel):
        """Train model asynchronously"""
        
        try:
            # Load training data
            training_data = await self._load_dataset(model.training_dataset_id)
            
            validation_data = None
            if model.validation_dataset_id:
                validation_data = await self._load_dataset(model.validation_dataset_id)
            
            # Train the model
            trained_model, training_metrics, validation_metrics = await self._execute_training(
                model, training_data, validation_data
            )
            
            # Save model artifacts
            model_path = await self._save_model_artifacts(model, trained_model)
            
            # Update model record
            model.status = ModelStatus.TRAINED
            model.training_completed_at = datetime.utcnow()
            model.training_duration_seconds = int(
                (model.training_completed_at - model.training_started_at).total_seconds()
            )
            model.model_artifact_path = str(model_path)
            model.training_metrics = training_metrics
            model.validation_metrics = validation_metrics
            
            # Calculate model checksum
            model.model_checksum = self._calculate_file_checksum(model_path)
            model.model_size_bytes = model_path.stat().st_size
            
            self.db.commit()
            
        except Exception as e:
            model.status = ModelStatus.FAILED
            model.training_completed_at = datetime.utcnow()
            
            if model.training_started_at:
                model.training_duration_seconds = int(
                    (model.training_completed_at - model.training_started_at).total_seconds()
                )
            
            logger.error(f"Model training failed for model {model.id}: {e}")
            self.db.commit()
    
    async def _load_dataset(self, dataset_id: int) -> pd.DataFrame:
        """Load dataset for training"""
        
        dataset = self.db.query(DataSet).filter(DataSet.id == dataset_id).first()
        if not dataset:
            raise ValueError(f"Dataset {dataset_id} not found")
        
        # This would load data from the actual data source
        # For now, return a mock DataFrame
        return pd.DataFrame({
            'feature1': np.random.randn(1000),
            'feature2': np.random.randn(1000),
            'target': np.random.randint(0, 2, 1000)
        })
    
    async def _execute_training(
        self,
        model: MLModel,
        training_data: pd.DataFrame,
        validation_data: Optional[pd.DataFrame] = None
    ) -> Tuple[Any, Dict[str, Any], Dict[str, Any]]:
        """Execute model training"""
        
        config = model.model_config
        hyperparams = model.hyperparameters or {}
        
        # Extract features and target
        feature_columns = config.get("features", [])
        target_column = config.get("target")
        
        X_train = training_data[feature_columns]
        y_train = training_data[target_column] if target_column else None
        
        X_val = None
        y_val = None
        if validation_data is not None:
            X_val = validation_data[feature_columns]
            y_val = validation_data[target_column] if target_column else None
        
        # Create and train model based on algorithm
        algorithm = config.get("algorithm")
        trained_model = None
        training_metrics = {}
        validation_metrics = {}
        
        if model.framework == "scikit-learn":
            trained_model, training_metrics, validation_metrics = await self._train_sklearn_model(
                algorithm, X_train, y_train, X_val, y_val, hyperparams, model.model_type
            )
        
        elif model.framework == "tensorflow":
            trained_model, training_metrics, validation_metrics = await self._train_tensorflow_model(
                algorithm, X_train, y_train, X_val, y_val, hyperparams, model.model_type
            )
        
        else:
            raise ValueError(f"Unsupported framework: {model.framework}")
        
        return trained_model, training_metrics, validation_metrics
    
    async def _train_sklearn_model(
        self,
        algorithm: str,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame],
        y_val: Optional[pd.Series],
        hyperparams: Dict[str, Any],
        model_type: ModelType
    ) -> Tuple[Any, Dict[str, Any], Dict[str, Any]]:
        """Train scikit-learn model"""
        
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        from sklearn.linear_model import LogisticRegression, LinearRegression
        from sklearn.svm import SVC, SVR
        from sklearn.cluster import KMeans
        from sklearn.metrics import accuracy_score, mean_squared_error, r2_score
        
        # Create model based on algorithm
        if algorithm == "random_forest":
            if model_type == ModelType.CLASSIFICATION:
                model = RandomForestClassifier(**hyperparams)
            else:
                model = RandomForestRegressor(**hyperparams)
        
        elif algorithm == "logistic_regression":
            model = LogisticRegression(**hyperparams)
        
        elif algorithm == "linear_regression":
            model = LinearRegression(**hyperparams)
        
        elif algorithm == "svm":
            if model_type == ModelType.CLASSIFICATION:
                model = SVC(**hyperparams)
            else:
                model = SVR(**hyperparams)
        
        elif algorithm == "kmeans":
            model = KMeans(**hyperparams)
        
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        # Train the model
        if model_type in [ModelType.CLASSIFICATION, ModelType.REGRESSION]:
            model.fit(X_train, y_train)
            
            # Calculate training metrics
            train_pred = model.predict(X_train)
            training_metrics = self._calculate_metrics(y_train, train_pred, model_type)
            
            # Calculate validation metrics
            validation_metrics = {}
            if X_val is not None and y_val is not None:
                val_pred = model.predict(X_val)
                validation_metrics = self._calculate_metrics(y_val, val_pred, model_type)
        
        elif model_type == ModelType.CLUSTERING:
            model.fit(X_train)
            training_metrics = {
                "inertia": model.inertia_,
                "n_clusters": model.n_clusters
            }
            validation_metrics = {}
        
        return model, training_metrics, validation_metrics
    
    async def _train_tensorflow_model(
        self,
        algorithm: str,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame],
        y_val: Optional[pd.Series],
        hyperparams: Dict[str, Any],
        model_type: ModelType
    ) -> Tuple[Any, Dict[str, Any], Dict[str, Any]]:
        """Train TensorFlow model"""
        
        # This would implement TensorFlow model training
        # For now, return mock results
        return None, {"loss": 0.1}, {"val_loss": 0.15}
    
    def _calculate_metrics(
        self,
        y_true: pd.Series,
        y_pred: np.ndarray,
        model_type: ModelType
    ) -> Dict[str, float]:
        """Calculate performance metrics"""
        
        from sklearn.metrics import (
            accuracy_score, precision_score, recall_score, f1_score,
            mean_squared_error, r2_score, mean_absolute_error
        )
        
        metrics = {}
        
        if model_type == ModelType.CLASSIFICATION:
            metrics["accuracy"] = accuracy_score(y_true, y_pred)
            metrics["precision"] = precision_score(y_true, y_pred, average="weighted")
            metrics["recall"] = recall_score(y_true, y_pred, average="weighted")
            metrics["f1_score"] = f1_score(y_true, y_pred, average="weighted")
        
        elif model_type == ModelType.REGRESSION:
            metrics["mse"] = mean_squared_error(y_true, y_pred)
            metrics["rmse"] = np.sqrt(mean_squared_error(y_true, y_pred))
            metrics["mae"] = mean_absolute_error(y_true, y_pred)
            metrics["r2_score"] = r2_score(y_true, y_pred)
        
        return metrics
    
    async def _save_model_artifacts(self, model: MLModel, trained_model: Any) -> Path:
        """Save model artifacts to storage"""
        
        model_dir = self.model_storage_path / str(model.org_id) / str(model.id)
        model_dir.mkdir(parents=True, exist_ok=True)
        
        model_file = model_dir / f"model_v{model.version}.pkl"
        
        # Save model based on framework
        if model.framework == "scikit-learn":
            joblib.dump(trained_model, model_file)
        
        elif model.framework == "tensorflow":
            # Save TensorFlow model
            trained_model.save(str(model_dir / f"model_v{model.version}"))
        
        else:
            # Default pickle save
            with open(model_file, 'wb') as f:
                pickle.dump(trained_model, f)
        
        return model_file
    
    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate file checksum"""
        
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    async def deploy_model(
        self,
        model_id: int,
        deployment_name: str,
        deployment_config: Dict[str, Any],
        environment: str = "production",
        deployed_by: int = None
    ) -> ModelDeployment:
        """Deploy a model to a serving environment"""
        
        model = self.db.query(MLModel).filter(MLModel.id == model_id).first()
        if not model:
            raise ValueError("Model not found")
        
        if model.status != ModelStatus.TRAINED:
            raise ValueError(f"Model must be trained before deployment (status: {model.status})")
        
        # Create deployment record
        deployment = ModelDeployment(
            name=deployment_name,
            model_id=model_id,
            deployment_config=deployment_config,
            environment=environment,
            org_id=model.org_id,
            deployed_by=deployed_by,
            status=DeploymentStatus.DEPLOYING
        )
        
        self.db.add(deployment)
        self.db.commit()
        self.db.refresh(deployment)
        
        try:
            # Start deployment process
            endpoint_url = await self._deploy_model_async(deployment)
            
            # Update deployment status
            deployment.status = DeploymentStatus.ACTIVE
            deployment.endpoint_url = endpoint_url
            deployment.deployed_at = datetime.utcnow()
            
            self.db.commit()
            
        except Exception as e:
            deployment.status = DeploymentStatus.FAILED
            logger.error(f"Model deployment failed: {e}")
            self.db.commit()
            raise e
        
        return deployment
    
    async def _deploy_model_async(self, deployment: ModelDeployment) -> str:
        """Deploy model asynchronously"""
        
        # This would implement actual model deployment
        # For now, return a mock endpoint URL
        return f"https://api.example.com/models/{deployment.id}/predict"
    
    async def predict(
        self,
        model_id: int,
        input_data: Dict[str, Any],
        deployment_id: Optional[int] = None,
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Make prediction using a model"""
        
        model = self.db.query(MLModel).filter(MLModel.id == model_id).first()
        if not model:
            raise ValueError("Model not found")
        
        # Load the trained model
        trained_model = await self._load_trained_model(model)
        
        # Prepare input data
        input_df = pd.DataFrame([input_data])
        feature_columns = model.model_config.get("features", [])
        X = input_df[feature_columns]
        
        # Make prediction
        prediction = trained_model.predict(X)
        
        # Calculate confidence if available
        confidence = None
        if hasattr(trained_model, "predict_proba"):
            probabilities = trained_model.predict_proba(X)
            confidence = float(np.max(probabilities))
        
        # Prepare result
        result = {
            "prediction": prediction.tolist(),
            "confidence": confidence,
            "model_version": model.version
        }
        
        # Record prediction
        prediction_record = ModelPrediction(
            prediction_id=str(uuid.uuid4()),
            model_id=model_id,
            deployment_id=deployment_id,
            input_data=input_data,
            prediction_result=result,
            confidence_score=confidence,
            model_version=model.version,
            user_id=user_id,
            org_id=model.org_id
        )
        
        self.db.add(prediction_record)
        self.db.commit()
        
        return result
    
    async def _load_trained_model(self, model: MLModel) -> Any:
        """Load trained model from storage"""
        
        if not model.model_artifact_path:
            raise ValueError("Model has no saved artifacts")
        
        model_path = Path(model.model_artifact_path)
        
        if not model_path.exists():
            raise ValueError("Model artifact file not found")
        
        # Load model based on framework
        if model.framework == "scikit-learn":
            return joblib.load(model_path)
        
        elif model.framework == "tensorflow":
            # Load TensorFlow model
            import tensorflow as tf
            return tf.keras.models.load_model(str(model_path.parent))
        
        else:
            # Default pickle load
            with open(model_path, 'rb') as f:
                return pickle.load(f)
    
    async def create_experiment(
        self,
        model_id: int,
        name: str,
        experiment_config: Dict[str, Any],
        hyperparameter_space: Dict[str, Any],
        optimization_metric: str,
        created_by: int,
        max_trials: int = 100
    ) -> MLExperiment:
        """Create hyperparameter optimization experiment"""
        
        model = self.db.query(MLModel).filter(MLModel.id == model_id).first()
        if not model:
            raise ValueError("Model not found")
        
        experiment = MLExperiment(
            name=name,
            model_id=model_id,
            experiment_config=experiment_config,
            hyperparameter_space=hyperparameter_space,
            optimization_metric=optimization_metric,
            max_trials=max_trials,
            org_id=model.org_id,
            created_by=created_by
        )
        
        self.db.add(experiment)
        self.db.commit()
        self.db.refresh(experiment)
        
        # Start experiment asynchronously
        asyncio.create_task(self._run_experiment_async(experiment))
        
        return experiment
    
    async def _run_experiment_async(self, experiment: MLExperiment):
        """Run hyperparameter optimization experiment"""
        
        try:
            for trial_num in range(1, experiment.max_trials + 1):
                # Generate hyperparameters for this trial
                hyperparameters = self._generate_trial_hyperparameters(
                    experiment.hyperparameter_space
                )
                
                # Create trial record
                trial = ExperimentTrial(
                    experiment_id=experiment.id,
                    trial_number=trial_num,
                    hyperparameters=hyperparameters,
                    started_at=datetime.utcnow()
                )
                
                self.db.add(trial)
                self.db.commit()
                
                try:
                    # Train model with these hyperparameters
                    score = await self._evaluate_trial(experiment, hyperparameters)
                    
                    trial.status = "completed"
                    trial.completed_at = datetime.utcnow()
                    trial.score = score
                    trial.metrics = {"score": score}
                    
                    # Update best trial if this is better
                    if (experiment.best_score is None or 
                        (experiment.optimization_direction == "maximize" and score > experiment.best_score) or
                        (experiment.optimization_direction == "minimize" and score < experiment.best_score)):
                        
                        experiment.best_trial_id = trial.id
                        experiment.best_score = score
                        experiment.best_hyperparameters = hyperparameters
                
                except Exception as e:
                    trial.status = "failed"
                    trial.completed_at = datetime.utcnow()
                    trial.error_message = str(e)
                
                finally:
                    trial.duration_seconds = int(
                        (trial.completed_at - trial.started_at).total_seconds()
                    )
                    experiment.current_trial = trial_num
                    self.db.commit()
            
            # Mark experiment as completed
            experiment.status = "completed"
            experiment.completed_at = datetime.utcnow()
            experiment.duration_seconds = int(
                (experiment.completed_at - experiment.started_at).total_seconds()
            )
            
        except Exception as e:
            experiment.status = "failed"
            experiment.completed_at = datetime.utcnow()
            logger.error(f"Experiment {experiment.id} failed: {e}")
        
        finally:
            self.db.commit()
    
    def _generate_trial_hyperparameters(
        self,
        hyperparameter_space: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate hyperparameters for a trial"""
        
        import random
        
        hyperparams = {}
        
        for param_name, param_config in hyperparameter_space.items():
            param_type = param_config.get("type")
            
            if param_type == "int":
                min_val = param_config.get("min", 1)
                max_val = param_config.get("max", 100)
                hyperparams[param_name] = random.randint(min_val, max_val)
            
            elif param_type == "float":
                min_val = param_config.get("min", 0.0)
                max_val = param_config.get("max", 1.0)
                hyperparams[param_name] = random.uniform(min_val, max_val)
            
            elif param_type == "choice":
                choices = param_config.get("choices", [])
                hyperparams[param_name] = random.choice(choices)
        
        return hyperparams
    
    async def _evaluate_trial(
        self,
        experiment: MLExperiment,
        hyperparameters: Dict[str, Any]
    ) -> float:
        """Evaluate a single trial"""
        
        # This would train and evaluate the model with given hyperparameters
        # For now, return a mock score
        return random.uniform(0.7, 0.95)
    
    async def create_feature_store(
        self,
        name: str,
        feature_group_name: str,
        schema_definition: Dict[str, Any],
        data_source_config: Dict[str, Any],
        org_id: int,
        created_by: int,
        data_source_id: Optional[int] = None
    ) -> FeatureStore:
        """Create a feature store"""
        
        feature_store = FeatureStore(
            name=name,
            feature_group_name=feature_group_name,
            schema_definition=schema_definition,
            data_source_config=data_source_config,
            org_id=org_id,
            data_source_id=data_source_id,
            created_by=created_by
        )
        
        self.db.add(feature_store)
        self.db.commit()
        self.db.refresh(feature_store)
        
        return feature_store
    
    async def register_model(
        self,
        model_id: int,
        model_name: str,
        model_version: str,
        artifact_uri: str,
        registered_by: int,
        stage: str = "staging"
    ) -> ModelRegistry:
        """Register model in model registry"""
        
        model = self.db.query(MLModel).filter(MLModel.id == model_id).first()
        if not model:
            raise ValueError("Model not found")
        
        registry_entry = ModelRegistry(
            model_name=model_name,
            model_version=model_version,
            model_type=model.model_type,
            framework=model.framework,
            artifact_uri=artifact_uri,
            model_size_bytes=model.model_size_bytes,
            training_metrics=model.training_metrics,
            stage=stage,
            model_id=model_id,
            org_id=model.org_id,
            registered_by=registered_by
        )
        
        self.db.add(registry_entry)
        self.db.commit()
        self.db.refresh(registry_entry)
        
        return registry_entry
    
    async def get_model_metrics(
        self,
        model_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get model performance metrics"""
        
        if not start_time:
            start_time = datetime.utcnow() - timedelta(days=30)
        
        if not end_time:
            end_time = datetime.utcnow()
        
        # Get prediction statistics
        predictions = self.db.query(ModelPrediction).filter(
            and_(
                ModelPrediction.model_id == model_id,
                ModelPrediction.created_at >= start_time,
                ModelPrediction.created_at <= end_time
            )
        ).all()
        
        total_predictions = len(predictions)
        avg_confidence = np.mean([p.confidence_score for p in predictions if p.confidence_score])
        avg_processing_time = np.mean([p.processing_time_ms for p in predictions if p.processing_time_ms])
        
        # Get feedback statistics
        feedback_predictions = [p for p in predictions if p.feedback_score is not None]
        avg_feedback = np.mean([p.feedback_score for p in feedback_predictions]) if feedback_predictions else None
        
        return {
            "total_predictions": total_predictions,
            "average_confidence": float(avg_confidence) if not np.isnan(avg_confidence) else None,
            "average_processing_time_ms": float(avg_processing_time) if not np.isnan(avg_processing_time) else None,
            "average_feedback_score": float(avg_feedback) if avg_feedback is not None else None,
            "feedback_count": len(feedback_predictions)
        }