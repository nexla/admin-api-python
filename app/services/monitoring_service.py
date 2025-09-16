import asyncio
import json
import time
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine, and_
import redis
import logging
from ..database import get_db, engine
from ..models.analytics import MetricDefinition, MetricValue, AlertRule, AlertInstance
from ..services.analytics_service import AnalyticsService

logger = logging.getLogger(__name__)

class RealTimeMonitoringService:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_client = redis.from_url(redis_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self.is_running = False
        self.monitoring_tasks = {}
        self.alert_evaluators = {}
        
    async def start_monitoring(self):
        """Start the real-time monitoring system"""
        if self.is_running:
            logger.warning("Monitoring service is already running")
            return
        
        self.is_running = True
        logger.info("Starting real-time monitoring service")
        
        # Start background tasks
        await asyncio.gather(
            self._metric_collector_task(),
            self._alert_evaluator_task(),
            self._system_health_monitor_task(),
            self._notification_dispatcher_task()
        )
    
    async def stop_monitoring(self):
        """Stop the real-time monitoring system"""
        self.is_running = False
        
        # Cancel all monitoring tasks
        for task in self.monitoring_tasks.values():
            task.cancel()
        
        self.monitoring_tasks.clear()
        logger.info("Real-time monitoring service stopped")
    
    async def _metric_collector_task(self):
        """Background task to collect system metrics"""
        while self.is_running:
            try:
                await self._collect_system_metrics()
                await asyncio.sleep(30)  # Collect every 30 seconds
            except Exception as e:
                logger.error(f"Error in metric collector: {e}")
                await asyncio.sleep(30)
    
    async def _collect_system_metrics(self):
        """Collect various system metrics"""
        db = self.SessionLocal()
        try:
            analytics_service = AnalyticsService(db)
            
            # System resource metrics
            await self._collect_resource_metrics(analytics_service)
            
            # Application metrics
            await self._collect_application_metrics(analytics_service)
            
            # Database metrics
            await self._collect_database_metrics(analytics_service)
            
        finally:
            db.close()
    
    async def _collect_resource_metrics(self, analytics_service: AnalyticsService):
        """Collect system resource metrics"""
        import psutil
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        await analytics_service.collect_metric(
            "system.cpu.usage_percent",
            cpu_percent,
            {"host": "localhost"}
        )
        
        # Memory usage
        memory = psutil.virtual_memory()
        await analytics_service.collect_metric(
            "system.memory.usage_percent",
            memory.percent,
            {"host": "localhost"}
        )
        
        await analytics_service.collect_metric(
            "system.memory.available_bytes",
            memory.available,
            {"host": "localhost"}
        )
        
        # Disk usage
        disk = psutil.disk_usage('/')
        await analytics_service.collect_metric(
            "system.disk.usage_percent",
            (disk.used / disk.total) * 100,
            {"host": "localhost", "mount": "/"}
        )
        
        # Network I/O
        network = psutil.net_io_counters()
        await analytics_service.collect_metric(
            "system.network.bytes_sent",
            network.bytes_sent,
            {"host": "localhost"}
        )
        
        await analytics_service.collect_metric(
            "system.network.bytes_recv",
            network.bytes_recv,
            {"host": "localhost"}
        )
    
    async def _collect_application_metrics(self, analytics_service: AnalyticsService):
        """Collect application-specific metrics"""
        # Active connections
        try:
            active_connections = len(self.redis_client.client_list())
            await analytics_service.collect_metric(
                "app.redis.active_connections",
                active_connections,
                {"service": "redis"}
            )
        except Exception as e:
            logger.error(f"Error collecting Redis metrics: {e}")
        
        # API request metrics (would be collected from middleware)
        # These would typically be incremented by request middleware
        request_count = self.redis_client.get("metrics:api:request_count") or 0
        await analytics_service.collect_metric(
            "app.api.request_count",
            float(request_count),
            {"service": "api"}
        )
        
        # Error rate
        error_count = self.redis_client.get("metrics:api:error_count") or 0
        total_requests = max(float(request_count), 1)
        error_rate = (float(error_count) / total_requests) * 100
        
        await analytics_service.collect_metric(
            "app.api.error_rate_percent",
            error_rate,
            {"service": "api"}
        )
    
    async def _collect_database_metrics(self, analytics_service: AnalyticsService):
        """Collect database metrics"""
        db = analytics_service.db
        
        # Table row counts
        try:
            from ..models.user import User
            from ..models.org import Org
            from ..models.project import Project
            
            user_count = db.query(User).count()
            await analytics_service.collect_metric(
                "db.table.row_count",
                user_count,
                {"table": "users"}
            )
            
            org_count = db.query(Org).count()
            await analytics_service.collect_metric(
                "db.table.row_count",
                org_count,
                {"table": "orgs"}
            )
            
            project_count = db.query(Project).count()
            await analytics_service.collect_metric(
                "db.table.row_count",
                project_count,
                {"table": "projects"}
            )
            
        except Exception as e:
            logger.error(f"Error collecting database metrics: {e}")
    
    async def _alert_evaluator_task(self):
        """Background task to evaluate alert rules"""
        while self.is_running:
            try:
                await self._evaluate_all_alert_rules()
                await asyncio.sleep(60)  # Evaluate every minute
            except Exception as e:
                logger.error(f"Error in alert evaluator: {e}")
                await asyncio.sleep(60)
    
    async def _evaluate_all_alert_rules(self):
        """Evaluate all active alert rules"""
        db = self.SessionLocal()
        try:
            # Get all enabled alert rules
            alert_rules = db.query(AlertRule).filter(
                AlertRule.enabled == True
            ).all()
            
            analytics_service = AnalyticsService(db)
            
            for rule in alert_rules:
                try:
                    # Get latest metric value
                    latest_value = db.query(MetricValue).filter(
                        MetricValue.metric_definition_id == rule.metric_definition_id
                    ).order_by(MetricValue.timestamp.desc()).first()
                    
                    if latest_value:
                        # Check if rule should trigger
                        await analytics_service._evaluate_alert_rules(
                            rule.metric_definition, latest_value
                        )
                    
                    # Update last evaluation time
                    rule.last_evaluation = datetime.utcnow()
                    
                except Exception as e:
                    logger.error(f"Error evaluating alert rule {rule.id}: {e}")
            
            db.commit()
            
        finally:
            db.close()
    
    async def _system_health_monitor_task(self):
        """Monitor overall system health"""
        while self.is_running:
            try:
                await self._check_system_health()
                await asyncio.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.error(f"Error in system health monitor: {e}")
                await asyncio.sleep(300)
    
    async def _check_system_health(self):
        """Check overall system health and generate alerts if needed"""
        db = self.SessionLocal()
        try:
            analytics_service = AnalyticsService(db)
            
            # Check for critical alerts
            critical_alerts = db.query(AlertInstance).filter(
                and_(
                    AlertInstance.status == "active",
                    AlertInstance.severity == "critical"
                )
            ).count()
            
            if critical_alerts > 0:
                await analytics_service.collect_metric(
                    "system.health.critical_alerts",
                    critical_alerts,
                    {"severity": "critical"}
                )
            
            # Check database connectivity
            try:
                db.execute("SELECT 1")
                await analytics_service.collect_metric(
                    "system.health.database_up",
                    1,
                    {"component": "database"}
                )
            except Exception:
                await analytics_service.collect_metric(
                    "system.health.database_up",
                    0,
                    {"component": "database"}
                )
            
            # Check Redis connectivity
            try:
                self.redis_client.ping()
                await analytics_service.collect_metric(
                    "system.health.redis_up",
                    1,
                    {"component": "redis"}
                )
            except Exception:
                await analytics_service.collect_metric(
                    "system.health.redis_up",
                    0,
                    {"component": "redis"}
                )
            
        finally:
            db.close()
    
    async def _notification_dispatcher_task(self):
        """Dispatch pending notifications"""
        while self.is_running:
            try:
                await self._dispatch_pending_notifications()
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Error in notification dispatcher: {e}")
                await asyncio.sleep(30)
    
    async def _dispatch_pending_notifications(self):
        """Dispatch pending alert notifications"""
        db = self.SessionLocal()
        try:
            from ..models.analytics import AlertNotification
            
            # Get pending notifications
            pending_notifications = db.query(AlertNotification).filter(
                AlertNotification.status == "pending"
            ).limit(100).all()
            
            for notification in pending_notifications:
                try:
                    await self._dispatch_notification(notification)
                    notification.status = "sent"
                    notification.sent_at = datetime.utcnow()
                except Exception as e:
                    notification.status = "failed"
                    notification.error_message = str(e)
                    notification.retry_count += 1
                    logger.error(f"Failed to dispatch notification {notification.id}: {e}")
            
            db.commit()
            
        finally:
            db.close()
    
    async def _dispatch_notification(self, notification):
        """Dispatch a single notification"""
        if notification.channel == "email":
            await self._send_email_notification(notification)
        elif notification.channel == "slack":
            await self._send_slack_notification(notification)
        elif notification.channel == "webhook":
            await self._send_webhook_notification(notification)
        else:
            raise ValueError(f"Unknown notification channel: {notification.channel}")
    
    async def _send_email_notification(self, notification):
        """Send email notification"""
        # Email sending implementation
        logger.info(f"Sending email notification to {notification.recipient}")
        # Would integrate with email service here
    
    async def _send_slack_notification(self, notification):
        """Send Slack notification"""
        # Slack API implementation
        logger.info(f"Sending Slack notification to {notification.recipient}")
        # Would integrate with Slack API here
    
    async def _send_webhook_notification(self, notification):
        """Send webhook notification"""
        # Webhook implementation
        logger.info(f"Sending webhook notification to {notification.recipient}")
        # Would send HTTP POST to webhook URL here
    
    async def register_metric_stream(self, metric_name: str, callback: Callable):
        """Register a callback for real-time metric updates"""
        # Store callback for metric updates
        if metric_name not in self.monitoring_tasks:
            self.monitoring_tasks[metric_name] = []
        
        self.monitoring_tasks[metric_name].append(callback)
    
    async def publish_metric_update(self, metric_name: str, value: float, labels: Dict[str, str] = None):
        """Publish real-time metric update"""
        # Publish to Redis pub/sub for real-time updates
        update_data = {
            "metric_name": metric_name,
            "value": value,
            "labels": labels or {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.redis_client.publish(
            f"metrics:{metric_name}",
            json.dumps(update_data)
        )
        
        # Also publish to general metrics channel
        self.redis_client.publish(
            "metrics:all",
            json.dumps(update_data)
        )
    
    async def get_real_time_metrics(self, metric_names: List[str] = None) -> Dict[str, Any]:
        """Get current real-time metric values"""
        db = self.SessionLocal()
        try:
            query = db.query(MetricValue).join(MetricDefinition)
            
            if metric_names:
                query = query.filter(MetricDefinition.name.in_(metric_names))
            
            # Get latest value for each metric
            latest_metrics = {}
            
            for metric_name in metric_names or []:
                latest_value = query.filter(
                    MetricDefinition.name == metric_name
                ).order_by(MetricValue.timestamp.desc()).first()
                
                if latest_value:
                    latest_metrics[metric_name] = {
                        "value": latest_value.value,
                        "labels": latest_value.labels,
                        "timestamp": latest_value.timestamp.isoformat()
                    }
            
            return latest_metrics
            
        finally:
            db.close()
    
    async def create_alert_escalation(self, alert_id: int, escalation_level: int):
        """Create alert escalation"""
        db = self.SessionLocal()
        try:
            alert = db.query(AlertInstance).filter(AlertInstance.id == alert_id).first()
            
            if not alert:
                raise ValueError("Alert not found")
            
            # Create escalation notification
            escalation_data = {
                "alert_id": alert_id,
                "escalation_level": escalation_level,
                "escalated_at": datetime.utcnow().isoformat(),
                "message": f"Alert {alert.alert_rule.name} has been escalated to level {escalation_level}"
            }
            
            # Publish escalation event
            self.redis_client.publish(
                "alerts:escalations",
                json.dumps(escalation_data)
            )
            
            return escalation_data
            
        finally:
            db.close()

# Global monitoring service instance
monitoring_service = RealTimeMonitoringService()

async def start_monitoring_service():
    """Start the global monitoring service"""
    await monitoring_service.start_monitoring()

async def stop_monitoring_service():
    """Stop the global monitoring service"""
    await monitoring_service.stop_monitoring()