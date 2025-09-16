import json
import uuid
import asyncio
import aioredis
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc, asc
import logging
import heapq
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import psutil
import socket

from ..models.distributed import (
    ClusterNode, DistributedJob, JobStep, LoadBalancer, AutoScaler,
    ScalingEvent, NodeMetric, TaskQueue, QueuedTask, DistributedCache,
    CacheEntry, NodeStatus, JobStatus, JobPriority, ScalingDirection
)
from ..models.user import User
from ..models.org import Org
from ..models.cluster import Cluster

logger = logging.getLogger(__name__)

class DistributedComputingService:
    """Distributed computing and scaling framework"""
    
    def __init__(self, db: Session):
        self.db = db
        self.executor = ThreadPoolExecutor(max_workers=50)
        self.process_executor = ProcessPoolExecutor(max_workers=10)
        self.running_jobs = {}
        self.node_heartbeats = {}
        
    async def register_node(
        self,
        node_name: str,
        node_type: str,
        hostname: str,
        ip_address: str,
        cpu_cores: int,
        memory_gb: float,
        cluster_id: int,
        port: int = 8080,
        storage_gb: Optional[float] = None,
        gpu_count: int = 0,
        labels: Optional[Dict[str, str]] = None
    ) -> ClusterNode:
        """Register a new node in the cluster"""
        
        # Generate unique node ID
        node_id = f"{node_type}-{hostname}-{int(datetime.utcnow().timestamp())}"
        
        # Check if node already exists
        existing_node = self.db.query(ClusterNode).filter(
            and_(
                ClusterNode.hostname == hostname,
                ClusterNode.cluster_id == cluster_id
            )
        ).first()
        
        if existing_node:
            # Update existing node
            existing_node.status = NodeStatus.ACTIVE
            existing_node.cpu_cores = cpu_cores
            existing_node.memory_gb = memory_gb
            existing_node.storage_gb = storage_gb
            existing_node.gpu_count = gpu_count
            existing_node.labels = labels or {}
            existing_node.last_heartbeat = datetime.utcnow()
            existing_node.api_endpoint = f"http://{ip_address}:{port}"
            
            self.db.commit()
            return existing_node
        
        node = ClusterNode(
            node_id=node_id,
            node_name=node_name,
            node_type=node_type,
            hostname=hostname,
            ip_address=ip_address,
            port=port,
            cpu_cores=cpu_cores,
            memory_gb=memory_gb,
            storage_gb=storage_gb,
            gpu_count=gpu_count,
            cluster_id=cluster_id,
            labels=labels or {},
            status=NodeStatus.ACTIVE,
            api_endpoint=f"http://{ip_address}:{port}",
            last_heartbeat=datetime.utcnow()
        )
        
        self.db.add(node)
        self.db.commit()
        self.db.refresh(node)
        
        # Start heartbeat monitoring
        asyncio.create_task(self._monitor_node_heartbeat(node))
        
        return node
    
    async def submit_job(
        self,
        job_name: str,
        job_type: str,
        job_config: Dict[str, Any],
        cpu_cores_required: float,
        memory_gb_required: float,
        cluster_id: int,
        submitted_by: int,
        org_id: int,
        priority: JobPriority = JobPriority.NORMAL,
        timeout_minutes: int = 60,
        storage_gb_required: float = 0,
        gpu_required: bool = False,
        scheduling_constraints: Optional[Dict[str, Any]] = None
    ) -> DistributedJob:
        """Submit a job for distributed execution"""
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        job = DistributedJob(
            job_id=job_id,
            job_name=job_name,
            job_type=job_type,
            job_config=job_config,
            cpu_cores_required=cpu_cores_required,
            memory_gb_required=memory_gb_required,
            storage_gb_required=storage_gb_required,
            gpu_required=gpu_required,
            priority=priority,
            timeout_minutes=timeout_minutes,
            scheduling_constraints=scheduling_constraints or {},
            cluster_id=cluster_id,
            submitted_by=submitted_by,
            org_id=org_id
        )
        
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        
        # Schedule job for execution
        asyncio.create_task(self._schedule_job(job))
        
        return job
    
    async def _schedule_job(self, job: DistributedJob):
        """Schedule job on an appropriate node"""
        
        try:
            # Find suitable node
            suitable_node = await self._find_suitable_node(job)
            
            if not suitable_node:
                # No suitable node available, keep job in pending state
                logger.warning(f"No suitable node found for job {job.job_id}")
                return
            
            # Assign job to node
            job.assigned_node_id = suitable_node.id
            job.status = JobStatus.SCHEDULED
            job.scheduled_at = datetime.utcnow()
            
            # Update node job count
            suitable_node.current_job_count += 1
            suitable_node.job_queue_size += 1
            
            self.db.commit()
            
            # Execute job on node
            await self._execute_job_on_node(job, suitable_node)
            
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            self.db.commit()
            logger.error(f"Job scheduling failed for {job.job_id}: {e}")
    
    async def _find_suitable_node(self, job: DistributedJob) -> Optional[ClusterNode]:
        """Find a suitable node for job execution"""
        
        # Get active nodes in the cluster
        active_nodes = self.db.query(ClusterNode).filter(
            and_(
                ClusterNode.cluster_id == job.cluster_id,
                ClusterNode.status == NodeStatus.ACTIVE
            )
        ).all()
        
        if not active_nodes:
            return None
        
        suitable_nodes = []
        
        for node in active_nodes:
            # Check resource requirements
            if (node.cpu_cores >= job.cpu_cores_required and
                node.memory_gb >= job.memory_gb_required and
                node.current_job_count < node.max_concurrent_jobs):
                
                # Check GPU requirement
                if job.gpu_required and node.gpu_count == 0:
                    continue
                
                # Check storage requirement
                if job.storage_gb_required > 0 and (not node.storage_gb or node.storage_gb < job.storage_gb_required):
                    continue
                
                # Check scheduling constraints
                if self._node_matches_constraints(node, job.scheduling_constraints):
                    suitable_nodes.append(node)
        
        if not suitable_nodes:
            return None
        
        # Select best node based on priority and resource utilization
        return self._select_best_node(suitable_nodes, job)
    
    def _node_matches_constraints(
        self,
        node: ClusterNode,
        constraints: Dict[str, Any]
    ) -> bool:
        """Check if node matches scheduling constraints"""
        
        if not constraints:
            return True
        
        # Check label constraints
        required_labels = constraints.get("labels", {})
        for key, value in required_labels.items():
            if node.labels.get(key) != value:
                return False
        
        # Check node type constraint
        if "node_type" in constraints:
            if node.node_type != constraints["node_type"]:
                return False
        
        # Check region/zone constraints
        if "region" in constraints:
            if node.region != constraints["region"]:
                return False
        
        if "availability_zone" in constraints:
            if node.availability_zone != constraints["availability_zone"]:
                return False
        
        return True
    
    def _select_best_node(
        self,
        nodes: List[ClusterNode],
        job: DistributedJob
    ) -> ClusterNode:
        """Select the best node from suitable candidates"""
        
        # Score nodes based on various factors
        node_scores = []
        
        for node in nodes:
            score = 0
            
            # Factor 1: Resource utilization (prefer less utilized nodes)
            cpu_util_score = (100 - node.cpu_usage_percent) / 100 * 30
            memory_util_score = (100 - node.memory_usage_percent) / 100 * 30
            
            # Factor 2: Current job load (prefer nodes with fewer jobs)
            job_load_score = (node.max_concurrent_jobs - node.current_job_count) / node.max_concurrent_jobs * 20
            
            # Factor 3: Health score
            health_score = node.health_score / 100 * 10
            
            # Factor 4: Priority boost for preferred nodes
            preferred_boost = 0
            if job.preferred_nodes and node.node_id in job.preferred_nodes:
                preferred_boost = 10
            
            total_score = cpu_util_score + memory_util_score + job_load_score + health_score + preferred_boost
            
            node_scores.append((total_score, node))
        
        # Sort by score (highest first)
        node_scores.sort(key=lambda x: x[0], reverse=True)
        
        return node_scores[0][1]
    
    async def _execute_job_on_node(self, job: DistributedJob, node: ClusterNode):
        """Execute job on the assigned node"""
        
        try:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            node.job_queue_size -= 1
            self.db.commit()
            
            # Track running job
            self.running_jobs[job.job_id] = job
            
            # Execute job based on type
            if job.job_type == "data_processing":
                result = await self._execute_data_processing_job(job, node)
            elif job.job_type == "machine_learning":
                result = await self._execute_ml_job(job, node)
            elif job.job_type == "batch_computation":
                result = await self._execute_batch_job(job, node)
            else:
                result = await self._execute_generic_job(job, node)
            
            # Mark job as completed
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.output_data = result
            job.duration_seconds = int(
                (job.completed_at - job.started_at).total_seconds()
            )
            
        except asyncio.TimeoutError:
            job.status = JobStatus.FAILED
            job.error_message = f"Job timed out after {job.timeout_minutes} minutes"
            job.completed_at = datetime.utcnow()
            
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            logger.error(f"Job execution failed for {job.job_id}: {e}")
        
        finally:
            # Update node job count
            node.current_job_count -= 1
            
            # Remove from running jobs
            self.running_jobs.pop(job.job_id, None)
            
            self.db.commit()
    
    async def _execute_data_processing_job(
        self,
        job: DistributedJob,
        node: ClusterNode
    ) -> Dict[str, Any]:
        """Execute data processing job"""
        
        config = job.job_config
        
        # Simulate data processing
        await asyncio.sleep(config.get("processing_time", 10))
        
        return {
            "processed_records": config.get("record_count", 1000),
            "processing_time": config.get("processing_time", 10),
            "output_location": f"/data/output/{job.job_id}",
            "status": "completed"
        }
    
    async def _execute_ml_job(
        self,
        job: DistributedJob,
        node: ClusterNode
    ) -> Dict[str, Any]:
        """Execute machine learning job"""
        
        config = job.job_config
        
        # Simulate ML training/inference
        training_time = config.get("training_time", 300)  # 5 minutes
        await asyncio.sleep(min(training_time, 30))  # Cap simulation time
        
        return {
            "model_accuracy": 0.92,
            "training_time": training_time,
            "model_path": f"/models/{job.job_id}/model.pkl",
            "status": "completed"
        }
    
    async def _execute_batch_job(
        self,
        job: DistributedJob,
        node: ClusterNode
    ) -> Dict[str, Any]:
        """Execute batch computation job"""
        
        config = job.job_config
        
        # Simulate batch processing
        batch_size = config.get("batch_size", 100)
        await asyncio.sleep(batch_size / 10)  # Simulate processing time
        
        return {
            "batches_processed": config.get("batch_count", 10),
            "total_items": batch_size * config.get("batch_count", 10),
            "status": "completed"
        }
    
    async def _execute_generic_job(
        self,
        job: DistributedJob,
        node: ClusterNode
    ) -> Dict[str, Any]:
        """Execute generic job"""
        
        config = job.job_config
        
        # Simulate generic processing
        await asyncio.sleep(config.get("execution_time", 5))
        
        return {
            "execution_time": config.get("execution_time", 5),
            "status": "completed"
        }
    
    async def _monitor_node_heartbeat(self, node: ClusterNode):
        """Monitor node heartbeat"""
        
        while True:
            try:
                await asyncio.sleep(node.heartbeat_interval)
                
                # Check if node is still sending heartbeats
                time_since_heartbeat = datetime.utcnow() - (node.last_heartbeat or node.created_at)
                
                if time_since_heartbeat.total_seconds() > node.heartbeat_interval * 3:
                    # Node is considered failed
                    node.status = NodeStatus.FAILED
                    node.health_score = 0.0
                    self.db.commit()
                    
                    # Reschedule jobs from failed node
                    await self._reschedule_jobs_from_failed_node(node)
                    break
                
            except Exception as e:
                logger.error(f"Error monitoring node {node.node_id}: {e}")
                break
    
    async def _reschedule_jobs_from_failed_node(self, failed_node: ClusterNode):
        """Reschedule jobs from a failed node"""
        
        # Get running jobs on the failed node
        running_jobs = self.db.query(DistributedJob).filter(
            and_(
                DistributedJob.assigned_node_id == failed_node.id,
                DistributedJob.status == JobStatus.RUNNING
            )
        ).all()
        
        for job in running_jobs:
            # Reset job for rescheduling
            job.status = JobStatus.PENDING
            job.assigned_node_id = None
            job.retry_count += 1
            job.last_retry_at = datetime.utcnow()
            
            if job.retry_count <= job.max_retries:
                # Reschedule job
                asyncio.create_task(self._schedule_job(job))
            else:
                # Mark job as failed after max retries
                job.status = JobStatus.FAILED
                job.error_message = f"Job failed after {job.max_retries} retries due to node failures"
                job.completed_at = datetime.utcnow()
        
        self.db.commit()
    
    async def update_node_metrics(
        self,
        node_id: str,
        cpu_usage: float,
        memory_usage: float,
        storage_usage: Optional[float] = None,
        network_io: Optional[float] = None
    ):
        """Update node resource metrics"""
        
        node = self.db.query(ClusterNode).filter(ClusterNode.node_id == node_id).first()
        if not node:
            return
        
        # Update node metrics
        node.cpu_usage_percent = cpu_usage
        node.memory_usage_percent = memory_usage
        if storage_usage is not None:
            node.storage_usage_percent = storage_usage
        if network_io is not None:
            node.network_usage_mbps = network_io
        
        node.last_heartbeat = datetime.utcnow()
        
        # Calculate health score
        node.health_score = self._calculate_node_health_score(node)
        
        # Store detailed metrics
        metric = NodeMetric(
            node_id=node.id,
            metric_name="resource_usage",
            metric_value=cpu_usage,
            cpu_usage_percent=cpu_usage,
            memory_usage_percent=memory_usage,
            storage_usage_percent=storage_usage,
            network_io_mbps=network_io,
            active_jobs=node.current_job_count,
            queued_jobs=node.job_queue_size
        )
        
        self.db.add(metric)
        self.db.commit()
    
    def _calculate_node_health_score(self, node: ClusterNode) -> float:
        """Calculate node health score based on various metrics"""
        
        score = 100.0
        
        # Reduce score based on high resource usage
        if node.cpu_usage_percent > 90:
            score -= 20
        elif node.cpu_usage_percent > 80:
            score -= 10
        
        if node.memory_usage_percent > 90:
            score -= 20
        elif node.memory_usage_percent > 80:
            score -= 10
        
        # Reduce score if node is overloaded with jobs
        job_utilization = node.current_job_count / max(node.max_concurrent_jobs, 1)
        if job_utilization > 0.9:
            score -= 15
        elif job_utilization > 0.8:
            score -= 10
        
        return max(score, 0.0)
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a running or pending job"""
        
        job = self.db.query(DistributedJob).filter(
            DistributedJob.job_id == job_id
        ).first()
        
        if not job:
            return False
        
        if job.status not in [JobStatus.PENDING, JobStatus.SCHEDULED, JobStatus.RUNNING]:
            return False
        
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.utcnow()
        
        if job.started_at:
            job.duration_seconds = int(
                (job.completed_at - job.started_at).total_seconds()
            )
        
        # Update node job count if job was assigned
        if job.assigned_node_id:
            node = job.assigned_node
            if node:
                node.current_job_count = max(0, node.current_job_count - 1)
        
        self.db.commit()
        
        # Remove from running jobs
        self.running_jobs.pop(job_id, None)
        
        return True
    
    async def get_cluster_status(self, cluster_id: int) -> Dict[str, Any]:
        """Get comprehensive cluster status"""
        
        # Get cluster nodes
        nodes = self.db.query(ClusterNode).filter(
            ClusterNode.cluster_id == cluster_id
        ).all()
        
        # Get cluster jobs
        jobs = self.db.query(DistributedJob).filter(
            DistributedJob.cluster_id == cluster_id
        ).all()
        
        # Calculate cluster metrics
        total_nodes = len(nodes)
        active_nodes = len([n for n in nodes if n.status == NodeStatus.ACTIVE])
        failed_nodes = len([n for n in nodes if n.status == NodeStatus.FAILED])
        
        total_jobs = len(jobs)
        running_jobs = len([j for j in jobs if j.status == JobStatus.RUNNING])
        completed_jobs = len([j for j in jobs if j.status == JobStatus.COMPLETED])
        failed_jobs = len([j for j in jobs if j.status == JobStatus.FAILED])
        
        # Calculate resource utilization
        total_cpu_cores = sum(n.cpu_cores for n in nodes)
        total_memory_gb = sum(n.memory_gb for n in nodes)
        avg_cpu_usage = sum(n.cpu_usage_percent for n in nodes) / max(total_nodes, 1)
        avg_memory_usage = sum(n.memory_usage_percent for n in nodes) / max(total_nodes, 1)
        
        return {
            "cluster_id": cluster_id,
            "nodes": {
                "total": total_nodes,
                "active": active_nodes,
                "failed": failed_nodes,
                "utilization": {
                    "cpu_percent": round(avg_cpu_usage, 2),
                    "memory_percent": round(avg_memory_usage, 2)
                }
            },
            "jobs": {
                "total": total_jobs,
                "running": running_jobs,
                "completed": completed_jobs,
                "failed": failed_jobs,
                "success_rate": round((completed_jobs / max(total_jobs, 1)) * 100, 2)
            },
            "resources": {
                "total_cpu_cores": total_cpu_cores,
                "total_memory_gb": total_memory_gb,
                "avg_cpu_usage_percent": round(avg_cpu_usage, 2),
                "avg_memory_usage_percent": round(avg_memory_usage, 2)
            },
            "timestamp": datetime.utcnow().isoformat()
        }

class AutoScalingService:
    """Auto-scaling service for distributed clusters"""
    
    def __init__(self, db: Session):
        self.db = db
        self.scaling_in_progress = set()
    
    async def create_auto_scaler(
        self,
        name: str,
        cluster_id: int,
        min_nodes: int,
        max_nodes: int,
        target_cpu_utilization: float,
        target_memory_utilization: float,
        scaling_metrics: Dict[str, Any],
        node_template: Dict[str, Any],
        created_by: int
    ) -> AutoScaler:
        """Create auto-scaler configuration"""
        
        auto_scaler = AutoScaler(
            name=name,
            cluster_id=cluster_id,
            min_nodes=min_nodes,
            max_nodes=max_nodes,
            target_cpu_utilization=target_cpu_utilization,
            target_memory_utilization=target_memory_utilization,
            scaling_metrics=scaling_metrics,
            node_template=node_template,
            created_by=created_by
        )
        
        self.db.add(auto_scaler)
        self.db.commit()
        self.db.refresh(auto_scaler)
        
        # Start auto-scaling monitoring
        asyncio.create_task(self._monitor_auto_scaler(auto_scaler))
        
        return auto_scaler
    
    async def _monitor_auto_scaler(self, auto_scaler: AutoScaler):
        """Monitor auto-scaler and trigger scaling events"""
        
        while auto_scaler.enabled:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Refresh auto_scaler from database
                self.db.refresh(auto_scaler)
                
                if not auto_scaler.enabled:
                    break
                
                # Check if scaling is needed
                scaling_decision = await self._evaluate_scaling_decision(auto_scaler)
                
                if scaling_decision["action"] != "none":
                    await self._execute_scaling_action(auto_scaler, scaling_decision)
                
            except Exception as e:
                logger.error(f"Error in auto-scaler monitoring {auto_scaler.id}: {e}")
                await asyncio.sleep(60)
    
    async def _evaluate_scaling_decision(self, auto_scaler: AutoScaler) -> Dict[str, Any]:
        """Evaluate whether scaling is needed"""
        
        cluster_id = auto_scaler.cluster_id
        
        # Get current cluster metrics
        active_nodes = self.db.query(ClusterNode).filter(
            and_(
                ClusterNode.cluster_id == cluster_id,
                ClusterNode.status == NodeStatus.ACTIVE
            )
        ).all()
        
        if not active_nodes:
            return {"action": "none", "reason": "No active nodes"}
        
        current_node_count = len(active_nodes)
        
        # Calculate average resource utilization
        avg_cpu_usage = sum(n.cpu_usage_percent for n in active_nodes) / len(active_nodes)
        avg_memory_usage = sum(n.memory_usage_percent for n in active_nodes) / len(active_nodes)
        
        # Check scaling conditions
        scale_up_needed = False
        scale_down_needed = False
        trigger_metric = ""
        trigger_value = 0.0
        
        # Scale up conditions
        if avg_cpu_usage > auto_scaler.target_cpu_utilization:
            scale_up_needed = True
            trigger_metric = "cpu_utilization"
            trigger_value = avg_cpu_usage
        
        elif avg_memory_usage > auto_scaler.target_memory_utilization:
            scale_up_needed = True
            trigger_metric = "memory_utilization"
            trigger_value = avg_memory_usage
        
        # Scale down conditions
        elif (avg_cpu_usage < auto_scaler.target_cpu_utilization * 0.5 and
              avg_memory_usage < auto_scaler.target_memory_utilization * 0.5):
            scale_down_needed = True
            trigger_metric = "low_utilization"
            trigger_value = max(avg_cpu_usage, avg_memory_usage)
        
        # Check constraints
        if scale_up_needed and current_node_count >= auto_scaler.max_nodes:
            return {"action": "none", "reason": "Max nodes reached"}
        
        if scale_down_needed and current_node_count <= auto_scaler.min_nodes:
            return {"action": "none", "reason": "Min nodes reached"}
        
        # Check cooldown periods
        if auto_scaler.last_scaling_action:
            time_since_last_scaling = datetime.utcnow() - auto_scaler.last_scaling_action
            
            if scale_up_needed and time_since_last_scaling.total_seconds() < auto_scaler.scale_up_cooldown:
                return {"action": "none", "reason": "Scale up cooldown"}
            
            if scale_down_needed and time_since_last_scaling.total_seconds() < auto_scaler.scale_down_cooldown:
                return {"action": "none", "reason": "Scale down cooldown"}
        
        # Determine scaling action
        if scale_up_needed:
            target_node_count = min(
                current_node_count + auto_scaler.scale_up_step_size,
                auto_scaler.max_nodes
            )
            return {
                "action": "scale_up",
                "current_nodes": current_node_count,
                "target_nodes": target_node_count,
                "trigger_metric": trigger_metric,
                "trigger_value": trigger_value,
                "threshold": auto_scaler.target_cpu_utilization if trigger_metric == "cpu_utilization" else auto_scaler.target_memory_utilization
            }
        
        elif scale_down_needed:
            target_node_count = max(
                current_node_count - auto_scaler.scale_down_step_size,
                auto_scaler.min_nodes
            )
            return {
                "action": "scale_down",
                "current_nodes": current_node_count,
                "target_nodes": target_node_count,
                "trigger_metric": trigger_metric,
                "trigger_value": trigger_value,
                "threshold": auto_scaler.target_cpu_utilization * 0.5
            }
        
        return {"action": "none", "reason": "No scaling needed"}
    
    async def _execute_scaling_action(
        self,
        auto_scaler: AutoScaler,
        scaling_decision: Dict[str, Any]
    ):
        """Execute scaling action"""
        
        if auto_scaler.id in self.scaling_in_progress:
            return
        
        self.scaling_in_progress.add(auto_scaler.id)
        
        try:
            # Create scaling event
            event_id = str(uuid.uuid4())
            scaling_event = ScalingEvent(
                event_id=event_id,
                auto_scaler_id=auto_scaler.id,
                scaling_direction=ScalingDirection.UP if scaling_decision["action"] == "scale_up" else ScalingDirection.DOWN,
                trigger_metric=scaling_decision["trigger_metric"],
                trigger_value=scaling_decision["trigger_value"],
                threshold_value=scaling_decision["threshold"],
                target_node_count=scaling_decision["target_nodes"]
            )
            
            self.db.add(scaling_event)
            self.db.commit()
            self.db.refresh(scaling_event)
            
            if scaling_decision["action"] == "scale_up":
                success = await self._scale_up_cluster(auto_scaler, scaling_decision, scaling_event)
            else:
                success = await self._scale_down_cluster(auto_scaler, scaling_decision, scaling_event)
            
            # Update scaling event
            scaling_event.status = "completed" if success else "failed"
            scaling_event.completed_at = datetime.utcnow()
            scaling_event.success = success
            
            if success:
                auto_scaler.last_scaling_action = datetime.utcnow()
            
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Scaling action failed for auto-scaler {auto_scaler.id}: {e}")
        
        finally:
            self.scaling_in_progress.discard(auto_scaler.id)
    
    async def _scale_up_cluster(
        self,
        auto_scaler: AutoScaler,
        scaling_decision: Dict[str, Any],
        scaling_event: ScalingEvent
    ) -> bool:
        """Scale up cluster by adding nodes"""
        
        nodes_to_add = scaling_decision["target_nodes"] - scaling_decision["current_nodes"]
        node_template = auto_scaler.node_template
        
        nodes_added = 0
        
        for i in range(nodes_to_add):
            try:
                # Create new node from template
                # This would integrate with cloud provider APIs
                # For now, simulate node creation
                
                new_node = ClusterNode(
                    node_id=f"auto-{auto_scaler.id}-{int(datetime.utcnow().timestamp())}-{i}",
                    node_name=f"auto-node-{i}",
                    node_type=node_template.get("node_type", "worker"),
                    hostname=f"auto-node-{auto_scaler.id}-{i}.cluster.local",
                    ip_address=f"10.0.{auto_scaler.id}.{100 + i}",
                    cpu_cores=node_template.get("cpu_cores", 2),
                    memory_gb=node_template.get("memory_gb", 4),
                    storage_gb=node_template.get("storage_gb", 20),
                    cluster_id=auto_scaler.cluster_id,
                    status=NodeStatus.ACTIVE,
                    labels=node_template.get("labels", {}),
                    last_heartbeat=datetime.utcnow()
                )
                
                self.db.add(new_node)
                nodes_added += 1
                
            except Exception as e:
                logger.error(f"Failed to create node {i}: {e}")
                break
        
        scaling_event.nodes_added = nodes_added
        self.db.commit()
        
        return nodes_added > 0
    
    async def _scale_down_cluster(
        self,
        auto_scaler: AutoScaler,
        scaling_decision: Dict[str, Any],
        scaling_event: ScalingEvent
    ) -> bool:
        """Scale down cluster by removing nodes"""
        
        nodes_to_remove = scaling_decision["current_nodes"] - scaling_decision["target_nodes"]
        
        # Get nodes suitable for removal (least utilized, no running jobs)
        candidate_nodes = self.db.query(ClusterNode).filter(
            and_(
                ClusterNode.cluster_id == auto_scaler.cluster_id,
                ClusterNode.status == NodeStatus.ACTIVE,
                ClusterNode.current_job_count == 0
            )
        ).order_by(
            ClusterNode.cpu_usage_percent.asc(),
            ClusterNode.memory_usage_percent.asc()
        ).limit(nodes_to_remove).all()
        
        nodes_removed = 0
        
        for node in candidate_nodes:
            try:
                # Drain node and remove
                node.status = NodeStatus.DRAINING
                
                # Wait for any remaining jobs to complete (with timeout)
                # In production, this would be more sophisticated
                
                # Remove node
                node.status = NodeStatus.INACTIVE
                nodes_removed += 1
                
            except Exception as e:
                logger.error(f"Failed to remove node {node.node_id}: {e}")
        
        scaling_event.nodes_removed = nodes_removed
        self.db.commit()
        
        return nodes_removed > 0