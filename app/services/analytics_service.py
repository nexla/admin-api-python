from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc, asc
import json
import asyncio
from ..models.analytics import (
    MetricDefinition, MetricValue, AlertRule, AlertInstance, 
    AlertNotification, Dashboard, AnalyticsReport, AnalyticsReportRun
)
from ..models.user import User
from ..models.org import Org
from .prometheus_metric_service import PrometheusMetricService

class AnalyticsService:
    def __init__(self, db: Session):
        self.db = db
        self.prometheus_service = PrometheusMetricService()
    
    async def collect_metric(
        self, 
        metric_name: str, 
        value: float, 
        labels: Dict[str, str] = None,
        timestamp: datetime = None
    ) -> MetricValue:
        definition = self.db.query(MetricDefinition).filter(
            MetricDefinition.name == metric_name
        ).first()
        
        if not definition:
            raise ValueError(f"Metric definition not found: {metric_name}")
        
        metric_value = MetricValue(
            metric_definition_id=definition.id,
            value=value,
            labels=labels or {},
            timestamp=timestamp or datetime.utcnow()
        )
        
        self.db.add(metric_value)
        self.db.commit()
        
        # Send to Prometheus if configured
        await self.prometheus_service.record_metric(
            metric_name, value, labels
        )
        
        # Check alert rules
        await self._evaluate_alert_rules(definition, metric_value)
        
        return metric_value
    
    async def _evaluate_alert_rules(
        self, 
        definition: MetricDefinition, 
        latest_value: MetricValue
    ):
        active_rules = self.db.query(AlertRule).filter(
            and_(
                AlertRule.metric_definition_id == definition.id,
                AlertRule.enabled == True
            )
        ).all()
        
        for rule in active_rules:
            try:
                should_trigger = await self._evaluate_rule_condition(
                    rule, latest_value
                )
                
                if should_trigger:
                    await self._trigger_alert(rule, latest_value)
                    
            except Exception as e:
                print(f"Error evaluating alert rule {rule.id}: {e}")
    
    async def _evaluate_rule_condition(
        self, 
        rule: AlertRule, 
        latest_value: MetricValue
    ) -> bool:
        condition = rule.condition.lower()
        threshold = rule.threshold
        value = latest_value.value
        
        # Simple threshold-based conditions
        if condition == "greater_than" or condition == ">":
            return value > threshold
        elif condition == "less_than" or condition == "<":
            return value < threshold
        elif condition == "equals" or condition == "==":
            return value == threshold
        elif condition == "not_equals" or condition == "!=":
            return value != threshold
        elif condition == "greater_than_or_equal" or condition == ">=":
            return value >= threshold
        elif condition == "less_than_or_equal" or condition == "<=":
            return value <= threshold
        
        # Time-based aggregation conditions
        elif condition.startswith("avg_"):
            period = int(condition.split("_")[1])
            return await self._evaluate_aggregation_condition(
                rule, "avg", period, threshold
            )
        elif condition.startswith("sum_"):
            period = int(condition.split("_")[1])
            return await self._evaluate_aggregation_condition(
                rule, "sum", period, threshold
            )
        elif condition.startswith("max_"):
            period = int(condition.split("_")[1])
            return await self._evaluate_aggregation_condition(
                rule, "max", period, threshold
            )
        elif condition.startswith("min_"):
            period = int(condition.split("_")[1])
            return await self._evaluate_aggregation_condition(
                rule, "min", period, threshold
            )
        
        return False
    
    async def _evaluate_aggregation_condition(
        self, 
        rule: AlertRule, 
        aggregation: str, 
        period_minutes: int, 
        threshold: float
    ) -> bool:
        cutoff_time = datetime.utcnow() - timedelta(minutes=period_minutes)
        
        query = self.db.query(MetricValue).filter(
            and_(
                MetricValue.metric_definition_id == rule.metric_definition_id,
                MetricValue.timestamp >= cutoff_time
            )
        )
        
        if aggregation == "avg":
            result = query.with_entities(func.avg(MetricValue.value)).scalar()
        elif aggregation == "sum":
            result = query.with_entities(func.sum(MetricValue.value)).scalar()
        elif aggregation == "max":
            result = query.with_entities(func.max(MetricValue.value)).scalar()
        elif aggregation == "min":
            result = query.with_entities(func.min(MetricValue.value)).scalar()
        else:
            return False
        
        if result is None:
            return False
        
        comparison = rule.comparison_operator or ">"
        if comparison == ">":
            return result > threshold
        elif comparison == "<":
            return result < threshold
        elif comparison == ">=":
            return result >= threshold
        elif comparison == "<=":
            return result <= threshold
        elif comparison == "==":
            return result == threshold
        elif comparison == "!=":
            return result != threshold
        
        return False
    
    async def _trigger_alert(self, rule: AlertRule, metric_value: MetricValue):
        # Check if there's already an active alert for this rule
        existing_alert = self.db.query(AlertInstance).filter(
            and_(
                AlertInstance.alert_rule_id == rule.id,
                AlertInstance.status == "active"
            )
        ).first()
        
        if existing_alert:
            return existing_alert
        
        # Create new alert instance
        alert_instance = AlertInstance(
            alert_rule_id=rule.id,
            severity=rule.severity,
            triggered_value=metric_value.value,
            triggered_labels=metric_value.labels,
            message=f"Alert {rule.name}: {metric_value.value} {rule.comparison_operator or '>'} {rule.threshold}"
        )
        
        self.db.add(alert_instance)
        self.db.commit()
        
        # Send notifications
        await self._send_alert_notifications(alert_instance)
        
        # Update rule last triggered
        rule.last_triggered = datetime.utcnow()
        self.db.commit()
        
        return alert_instance
    
    async def _send_alert_notifications(self, alert_instance: AlertInstance):
        rule = alert_instance.alert_rule
        channels = rule.notification_channels or []
        
        for channel_config in channels:
            channel_type = channel_config.get("type")
            
            notification = AlertNotification(
                alert_instance_id=alert_instance.id,
                channel=channel_type,
                status="pending",
                recipient=channel_config.get("recipient"),
                subject=f"ALERT: {rule.name}",
                message=alert_instance.message
            )
            
            self.db.add(notification)
            
            try:
                if channel_type == "email":
                    await self._send_email_notification(notification, channel_config)
                elif channel_type == "slack":
                    await self._send_slack_notification(notification, channel_config)
                elif channel_type == "webhook":
                    await self._send_webhook_notification(notification, channel_config)
                
                notification.status = "sent"
                notification.sent_at = datetime.utcnow()
                
            except Exception as e:
                notification.status = "failed"
                notification.error_message = str(e)
        
        self.db.commit()
    
    async def _send_email_notification(self, notification: AlertNotification, config: Dict):
        # Email notification implementation
        pass
    
    async def _send_slack_notification(self, notification: AlertNotification, config: Dict):
        # Slack notification implementation
        pass
    
    async def _send_webhook_notification(self, notification: AlertNotification, config: Dict):
        # Webhook notification implementation
        pass
    
    async def acknowledge_alert(
        self, 
        alert_id: int, 
        user_id: int, 
        comment: str = None
    ) -> AlertInstance:
        alert = self.db.query(AlertInstance).filter(AlertInstance.id == alert_id).first()
        
        if not alert:
            raise ValueError("Alert not found")
        
        if alert.status != "active":
            raise ValueError("Alert is not active")
        
        alert.status = "acknowledged"
        alert.acknowledged_by = user_id
        alert.acknowledged_at = datetime.utcnow()
        
        self.db.commit()
        return alert
    
    async def resolve_alert(
        self, 
        alert_id: int, 
        user_id: int, 
        reason: str = None
    ) -> AlertInstance:
        alert = self.db.query(AlertInstance).filter(AlertInstance.id == alert_id).first()
        
        if not alert:
            raise ValueError("Alert not found")
        
        alert.status = "resolved"
        alert.resolved_by = user_id
        alert.resolved_at = datetime.utcnow()
        alert.resolution_reason = reason
        
        self.db.commit()
        return alert
    
    async def get_metric_data(
        self,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        labels: Dict[str, str] = None,
        aggregation: str = None,
        interval: str = "5m"
    ) -> List[Dict[str, Any]]:
        definition = self.db.query(MetricDefinition).filter(
            MetricDefinition.name == metric_name
        ).first()
        
        if not definition:
            raise ValueError(f"Metric definition not found: {metric_name}")
        
        query = self.db.query(MetricValue).filter(
            and_(
                MetricValue.metric_definition_id == definition.id,
                MetricValue.timestamp >= start_time,
                MetricValue.timestamp <= end_time
            )
        )
        
        if labels:
            for key, value in labels.items():
                query = query.filter(
                    MetricValue.labels[key].astext == value
                )
        
        values = query.order_by(MetricValue.timestamp).all()
        
        if aggregation and interval:
            return self._aggregate_metric_data(values, aggregation, interval)
        
        return [
            {
                "timestamp": value.timestamp.isoformat(),
                "value": value.value,
                "labels": value.labels
            }
            for value in values
        ]
    
    def _aggregate_metric_data(
        self, 
        values: List[MetricValue], 
        aggregation: str, 
        interval: str
    ) -> List[Dict[str, Any]]:
        # Simple time-based aggregation
        interval_seconds = self._parse_interval(interval)
        aggregated = {}
        
        for value in values:
            # Round timestamp to interval boundary
            bucket = int(value.timestamp.timestamp() // interval_seconds) * interval_seconds
            bucket_time = datetime.fromtimestamp(bucket)
            
            if bucket_time not in aggregated:
                aggregated[bucket_time] = []
            aggregated[bucket_time].append(value.value)
        
        result = []
        for bucket_time, bucket_values in sorted(aggregated.items()):
            if aggregation == "avg":
                agg_value = sum(bucket_values) / len(bucket_values)
            elif aggregation == "sum":
                agg_value = sum(bucket_values)
            elif aggregation == "max":
                agg_value = max(bucket_values)
            elif aggregation == "min":
                agg_value = min(bucket_values)
            elif aggregation == "count":
                agg_value = len(bucket_values)
            else:
                agg_value = bucket_values[-1]  # last value
            
            result.append({
                "timestamp": bucket_time.isoformat(),
                "value": agg_value,
                "sample_count": len(bucket_values)
            })
        
        return result
    
    def _parse_interval(self, interval: str) -> int:
        # Parse interval string like "5m", "1h", "30s"
        if interval.endswith("s"):
            return int(interval[:-1])
        elif interval.endswith("m"):
            return int(interval[:-1]) * 60
        elif interval.endswith("h"):
            return int(interval[:-1]) * 3600
        elif interval.endswith("d"):
            return int(interval[:-1]) * 86400
        else:
            return 60  # default 1 minute
    
    async def create_dashboard(
        self, 
        name: str, 
        widgets: List[Dict], 
        org_id: int, 
        user_id: int,
        description: str = None,
        layout: Dict = None
    ) -> Dashboard:
        dashboard = Dashboard(
            name=name,
            description=description,
            widgets=widgets,
            layout=layout or {},
            org_id=org_id,
            created_by=user_id
        )
        
        self.db.add(dashboard)
        self.db.commit()
        return dashboard
    
    async def generate_report(
        self, 
        report_id: int, 
        user_id: int = None
    ) -> AnalyticsReportRun:
        report = self.db.query(AnalyticsReport).filter(
            AnalyticsReport.id == report_id
        ).first()
        
        if not report:
            raise ValueError("Report not found")
        
        run = AnalyticsReportRun(
            report_id=report_id,
            triggered_by=user_id,
            status="running"
        )
        
        self.db.add(run)
        self.db.commit()
        
        try:
            # Execute report query
            output_data = await self._execute_report_query(report)
            
            run.status = "completed"
            run.completed_at = datetime.utcnow()
            run.duration_seconds = int(
                (run.completed_at - run.started_at).total_seconds()
            )
            run.output_data = output_data
            run.row_count = len(output_data) if isinstance(output_data, list) else 1
            
        except Exception as e:
            run.status = "failed"
            run.error_message = str(e)
            run.completed_at = datetime.utcnow()
        
        self.db.commit()
        return run
    
    async def _execute_report_query(self, report: AnalyticsReport) -> Any:
        # Execute report query based on report type
        if report.report_type == "metric_summary":
            return await self._generate_metric_summary_report(report)
        elif report.report_type == "alert_summary":
            return await self._generate_alert_summary_report(report)
        elif report.report_type == "custom_query":
            return await self._execute_custom_query(report)
        else:
            raise ValueError(f"Unknown report type: {report.report_type}")
    
    async def _generate_metric_summary_report(self, report: AnalyticsReport) -> List[Dict]:
        # Generate metric summary report
        params = report.parameters or {}
        days = params.get("days", 7)
        
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        metrics = self.db.query(MetricDefinition).filter(
            MetricDefinition.org_id == report.org_id
        ).all()
        
        summary = []
        for metric in metrics:
            values_count = self.db.query(MetricValue).filter(
                and_(
                    MetricValue.metric_definition_id == metric.id,
                    MetricValue.timestamp >= cutoff_date
                )
            ).count()
            
            latest_value = self.db.query(MetricValue).filter(
                MetricValue.metric_definition_id == metric.id
            ).order_by(desc(MetricValue.timestamp)).first()
            
            summary.append({
                "metric_name": metric.name,
                "metric_type": metric.metric_type,
                "values_count": values_count,
                "latest_value": latest_value.value if latest_value else None,
                "latest_timestamp": latest_value.timestamp.isoformat() if latest_value else None
            })
        
        return summary
    
    async def _generate_alert_summary_report(self, report: AnalyticsReport) -> List[Dict]:
        # Generate alert summary report
        params = report.parameters or {}
        days = params.get("days", 7)
        
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        alerts = self.db.query(AlertInstance).filter(
            AlertInstance.triggered_at >= cutoff_date
        ).all()
        
        summary = []
        for alert in alerts:
            summary.append({
                "alert_rule_name": alert.alert_rule.name,
                "severity": alert.severity,
                "status": alert.status,
                "triggered_at": alert.triggered_at.isoformat(),
                "triggered_value": alert.triggered_value,
                "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None
            })
        
        return summary
    
    async def _execute_custom_query(self, report: AnalyticsReport) -> Any:
        # Execute custom SQL query - would need proper query validation
        # This is a simplified implementation
        query = report.query
        
        if not query:
            raise ValueError("No query specified for custom report")
        
        # Would execute the custom query here with proper security measures
        # For now, return empty result
        return []