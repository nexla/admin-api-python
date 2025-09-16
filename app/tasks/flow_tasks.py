"""
Flow execution tasks - Handle data flow processing and execution.
Replaces Rails background job system for flow operations.
"""

from celery import current_task
from sqlalchemy.orm import Session
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta
import json

from ..celery_app import celery_app
from ..database import SessionLocal
from ..models.flow import Flow, FlowRun
from ..models.flow_node import FlowNode
from ..models.user import User

logger = logging.getLogger(__name__)


class FlowExecutor:
    """Flow execution engine for processing data flows"""
    
    @staticmethod
    def get_flow_with_nodes(db: Session, flow_id: int):
        """Get flow with its nodes (placeholder until relationships are enabled)"""
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            return None, []
        
        # TODO: Get actual nodes when relationships are enabled
        nodes = db.query(FlowNode).filter(FlowNode.org_id == flow.org_id).all()
        return flow, nodes
    
    @staticmethod
    def execute_node(db: Session, node: FlowNode, input_data: Any = None) -> Dict[str, Any]:
        """Execute a single flow node"""
        try:
            # Placeholder node execution logic
            result = {
                "node_id": node.id,
                "status": "success",
                "records_processed": 100,
                "records_output": 95,
                "execution_time_ms": 1500,
                "output_data": {"sample": "data"},
                "error_message": None
            }
            
            logger.info(f"Node {node.id} executed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Node {node.id} execution failed: {str(e)}")
            return {
                "node_id": node.id,
                "status": "failed",
                "records_processed": 0,
                "records_output": 0,
                "execution_time_ms": 0,
                "output_data": None,
                "error_message": str(e)
            }


@celery_app.task(bind=True, name='flow.execute')
def execute_flow_task(self, flow_run_id: int, parameters: Optional[Dict[str, Any]] = None):
    """
    Execute a complete data flow.
    
    Args:
        flow_run_id: ID of the FlowRun to execute
        parameters: Optional execution parameters
    """
    db = SessionLocal()
    start_time = datetime.utcnow()
    
    try:
        # Get flow run
        flow_run = db.query(FlowRun).filter(FlowRun.id == flow_run_id).first()
        if not flow_run:
            raise Exception(f"FlowRun {flow_run_id} not found")
        
        # Update run status
        flow_run.status = "running"
        flow_run.started_at = start_time
        db.commit()
        
        # Get flow and nodes
        flow, nodes = FlowExecutor.get_flow_with_nodes(db, flow_run.flow_id)
        if not flow:
            raise Exception(f"Flow {flow_run.flow_id} not found")
        
        # Update flow status
        flow.last_run_status = "running"
        db.commit()
        
        # Execute nodes in order (simplified execution)
        total_records_processed = 0
        total_records_success = 0
        total_records_failed = 0
        node_results = []
        
        for i, node in enumerate(nodes[:5]):  # Limit to 5 nodes for demo
            # Update task progress
            self.update_state(
                state='PROGRESS',
                meta={
                    'current_node': i + 1,
                    'total_nodes': len(nodes),
                    'node_name': node.name,
                    'flow_id': flow.id
                }
            )
            
            # Execute node
            result = FlowExecutor.execute_node(db, node)
            node_results.append(result)
            
            if result["status"] == "success":
                total_records_processed += result["records_processed"]
                total_records_success += result["records_output"]
            else:
                total_records_failed += result["records_processed"]
                # Stop execution on node failure if retry_count is 0
                if flow.retry_count == 0:
                    break
        
        # Determine overall status
        failed_nodes = [r for r in node_results if r["status"] == "failed"]
        overall_status = "failed" if failed_nodes else "success"
        
        # Update flow run with results
        end_time = datetime.utcnow()
        duration = int((end_time - start_time).total_seconds())
        
        flow_run.status = overall_status
        flow_run.completed_at = end_time
        flow_run.duration_seconds = duration
        flow_run.records_processed = total_records_processed
        flow_run.records_success = total_records_success
        flow_run.records_failed = total_records_failed
        flow_run.log_data = {"node_results": node_results}
        
        if failed_nodes:
            flow_run.error_message = f"Failed nodes: {[r['node_id'] for r in failed_nodes]}"
        
        # Update flow statistics
        flow.last_run_at = end_time
        flow.last_run_status = overall_status
        
        if overall_status == "success":
            flow.success_count = (flow.success_count or 0) + 1
        else:
            flow.failure_count = (flow.failure_count or 0) + 1
        
        db.commit()
        
        logger.info(f"Flow {flow.id} execution completed: {overall_status}")
        
        return {
            "flow_id": flow.id,
            "flow_run_id": flow_run_id,
            "status": overall_status,
            "duration_seconds": duration,
            "records_processed": total_records_processed,
            "records_success": total_records_success,
            "records_failed": total_records_failed,
            "node_results": node_results
        }
        
    except Exception as e:
        # Update flow run with error
        error_msg = str(e)
        logger.error(f"Flow execution failed: {error_msg}")
        
        if 'flow_run' in locals():
            flow_run.status = "failed"
            flow_run.completed_at = datetime.utcnow()
            flow_run.error_message = error_msg
            if 'start_time' in locals():
                duration = int((datetime.utcnow() - start_time).total_seconds())
                flow_run.duration_seconds = duration
            db.commit()
        
        if 'flow' in locals():
            flow.last_run_status = "failed"
            flow.failure_count = (flow.failure_count or 0) + 1
            db.commit()
        
        raise
        
    finally:
        db.close()


@celery_app.task(name='flow.stop')
def stop_flow_task(flow_id: int, user_id: int):
    """
    Stop a running flow.
    
    Args:
        flow_id: ID of the flow to stop
        user_id: ID of the user stopping the flow
    """
    db = SessionLocal()
    
    try:
        # Get flow
        flow = db.query(Flow).filter(Flow.id == flow_id).first()
        if not flow:
            raise Exception(f"Flow {flow_id} not found")
        
        # Get running flow run
        running_run = db.query(FlowRun).filter(
            FlowRun.flow_id == flow_id,
            FlowRun.status == "running"
        ).first()
        
        if running_run:
            running_run.status = "cancelled"
            running_run.completed_at = datetime.utcnow()
            if running_run.started_at:
                duration = int((datetime.utcnow() - running_run.started_at).total_seconds())
                running_run.duration_seconds = duration
        
        # Update flow status
        flow.last_run_status = "cancelled"
        
        db.commit()
        
        logger.info(f"Flow {flow_id} stopped by user {user_id}")
        
        return {
            "flow_id": flow_id,
            "status": "stopped",
            "stopped_by": user_id,
            "stopped_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to stop flow {flow_id}: {str(e)}")
        raise
        
    finally:
        db.close()


@celery_app.task(name='flow.schedule_check')
def check_scheduled_flows():
    """
    Check for flows that need to be executed based on schedule.
    This task runs periodically to handle scheduled flow execution.
    """
    db = SessionLocal()
    
    try:
        # Get flows that are scheduled and due for execution
        now = datetime.utcnow()
        scheduled_flows = db.query(Flow).filter(
            Flow.is_active == True,
            Flow.schedule_type.in_(["cron", "event_driven"]),
            Flow.next_run_at <= now
        ).all()
        
        executed_count = 0
        
        for flow in scheduled_flows:
            try:
                # Create new flow run
                last_run = db.query(FlowRun).filter(
                    FlowRun.flow_id == flow.id
                ).order_by(FlowRun.run_number.desc()).first()
                
                run_number = (last_run.run_number + 1) if last_run else 1
                
                flow_run = FlowRun(
                    flow_id=flow.id,
                    run_number=run_number,
                    status="queued",
                    trigger_type="scheduled",
                    created_at=now
                )
                
                db.add(flow_run)
                db.commit()
                db.refresh(flow_run)
                
                # Queue flow execution
                execute_flow_task.delay(flow_run.id)
                
                # Update next run time (simplified - would use actual cron logic)
                if flow.schedule_type == "cron":
                    flow.next_run_at = now + timedelta(hours=1)  # Simplified
                
                executed_count += 1
                logger.info(f"Scheduled flow {flow.id} queued for execution")
                
            except Exception as e:
                logger.error(f"Failed to schedule flow {flow.id}: {str(e)}")
                continue
        
        db.commit()
        
        logger.info(f"Scheduled {executed_count} flows for execution")
        
        return {
            "scheduled_count": executed_count,
            "checked_at": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to check scheduled flows: {str(e)}")
        raise
        
    finally:
        db.close()


@celery_app.task(name='flow.cleanup_old_runs')
def cleanup_old_flow_runs(days_to_keep: int = 30):
    """
    Clean up old flow runs to manage database size.
    
    Args:
        days_to_keep: Number of days of flow runs to keep
    """
    db = SessionLocal()
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        # Get old flow runs
        old_runs = db.query(FlowRun).filter(
            FlowRun.created_at < cutoff_date
        )
        
        count = old_runs.count()
        
        # Delete old runs
        old_runs.delete(synchronize_session=False)
        db.commit()
        
        logger.info(f"Cleaned up {count} old flow runs")
        
        return {
            "deleted_count": count,
            "cutoff_date": cutoff_date.isoformat(),
            "cleaned_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to cleanup old flow runs: {str(e)}")
        raise
        
    finally:
        db.close()


@celery_app.task(name='flow.validate_all')
def validate_all_flows():
    """
    Validate all flows in the system for health monitoring.
    """
    db = SessionLocal()
    
    try:
        flows = db.query(Flow).filter(Flow.is_active == True).all()
        
        validation_results = []
        
        for flow in flows:
            try:
                # Basic validation
                errors = []
                warnings = []
                
                if not flow.name:
                    errors.append("Flow name is required")
                
                # TODO: Add more validation logic
                
                validation_results.append({
                    "flow_id": flow.id,
                    "flow_name": flow.name,
                    "is_valid": len(errors) == 0,
                    "errors": errors,
                    "warnings": warnings
                })
                
            except Exception as e:
                validation_results.append({
                    "flow_id": flow.id,
                    "flow_name": flow.name,
                    "is_valid": False,
                    "errors": [f"Validation error: {str(e)}"],
                    "warnings": []
                })
        
        # Count results
        total_flows = len(validation_results)
        valid_flows = len([r for r in validation_results if r["is_valid"]])
        invalid_flows = total_flows - valid_flows
        
        logger.info(f"Validated {total_flows} flows: {valid_flows} valid, {invalid_flows} invalid")
        
        return {
            "total_flows": total_flows,
            "valid_flows": valid_flows,
            "invalid_flows": invalid_flows,
            "validation_results": validation_results,
            "validated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to validate flows: {str(e)}")
        raise
        
    finally:
        db.close()