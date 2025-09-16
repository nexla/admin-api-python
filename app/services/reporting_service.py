import json
import uuid
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc, asc, text
import logging
import asyncio
from pathlib import Path
import io
import base64

from ..models.reporting import (
    Report, ReportExecution, Dashboard, Widget, DashboardShare,
    ReportSubscription, ReportTemplate, DataVisualization,
    ReportingMetric, AlertRule, AlertInstance,
    ReportType, ReportStatus, VisualizationType, DashboardType
)
from ..models.user import User
from ..models.org import Org
from ..models.data_set import DataSet
from ..models.data_source import DataSource

logger = logging.getLogger(__name__)

class ReportingService:
    """Advanced reporting and dashboard service"""
    
    def __init__(self, db: Session, storage_path: str = "/tmp/reports"):
        self.db = db
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    async def create_report(
        self,
        name: str,
        report_type: ReportType,
        data_sources: List[Dict[str, Any]],
        query_config: Dict[str, Any],
        org_id: int,
        created_by: int,
        description: str = None,
        project_id: Optional[int] = None,
        visualization_config: Optional[Dict[str, Any]] = None
    ) -> Report:
        """Create a new report"""
        
        # Validate data sources and query configuration
        self._validate_report_config(data_sources, query_config)
        
        report = Report(
            name=name,
            description=description,
            report_type=report_type,
            data_sources=data_sources,
            query_config=query_config,
            visualization_config=visualization_config or {},
            org_id=org_id,
            project_id=project_id,
            created_by=created_by
        )
        
        self.db.add(report)
        self.db.commit()
        self.db.refresh(report)
        
        return report
    
    def _validate_report_config(
        self,
        data_sources: List[Dict[str, Any]],
        query_config: Dict[str, Any]
    ):
        """Validate report configuration"""
        
        if not data_sources:
            raise ValueError("At least one data source is required")
        
        for ds in data_sources:
            if "type" not in ds or "config" not in ds:
                raise ValueError("Each data source must have 'type' and 'config' fields")
        
        if "query" not in query_config:
            raise ValueError("Query configuration must include 'query' field")
    
    async def execute_report(
        self,
        report_id: int,
        parameters: Optional[Dict[str, Any]] = None,
        filters: Optional[Dict[str, Any]] = None,
        triggered_by: Optional[int] = None,
        output_formats: Optional[List[str]] = None
    ) -> ReportExecution:
        """Execute a report"""
        
        report = self.db.query(Report).filter(Report.id == report_id).first()
        if not report:
            raise ValueError("Report not found")
        
        # Check if report is cached and still valid
        if self._is_report_cached(report):
            cached_execution = self._create_cached_execution(
                report, parameters, filters, triggered_by
            )
            return cached_execution
        
        # Create execution record
        execution_id = str(uuid.uuid4())
        execution = ReportExecution(
            execution_id=execution_id,
            report_id=report_id,
            parameters=parameters or {},
            filters=filters or {},
            triggered_by=triggered_by,
            trigger_type="manual" if triggered_by else "api"
        )
        
        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)
        
        # Execute report asynchronously
        asyncio.create_task(
            self._execute_report_async(execution, output_formats or [])
        )
        
        return execution
    
    def _is_report_cached(self, report: Report) -> bool:
        """Check if report has valid cached results"""
        
        if not report.cached_result or not report.cached_at:
            return False
        
        cache_ttl = report.cache_ttl_minutes or 60
        cache_expiry = report.cached_at + timedelta(minutes=cache_ttl)
        
        return datetime.utcnow() < cache_expiry
    
    def _create_cached_execution(
        self,
        report: Report,
        parameters: Optional[Dict[str, Any]],
        filters: Optional[Dict[str, Any]],
        triggered_by: Optional[int]
    ) -> ReportExecution:
        """Create execution record for cached result"""
        
        execution_id = str(uuid.uuid4())
        execution = ReportExecution(
            execution_id=execution_id,
            report_id=report.id,
            status=ReportStatus.COMPLETED,
            parameters=parameters or {},
            filters=filters or {},
            triggered_by=triggered_by,
            trigger_type="cached",
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            duration_seconds=0,
            result_data=report.cached_result
        )
        
        self.db.add(execution)
        self.db.commit()
        
        return execution
    
    async def _execute_report_async(
        self,
        execution: ReportExecution,
        output_formats: List[str]
    ):
        """Execute report asynchronously"""
        
        try:
            execution.status = ReportStatus.RUNNING
            self.db.commit()
            
            # Get report configuration
            report = execution.report
            
            # Execute queries and collect data
            result_data = await self._execute_report_queries(
                report, execution.parameters, execution.filters
            )
            
            # Apply transformations and aggregations
            processed_data = await self._process_report_data(
                result_data, report.query_config
            )
            
            # Generate visualizations if configured
            visualizations = await self._generate_visualizations(
                processed_data, report.visualization_config
            )
            
            # Generate output files
            output_files = await self._generate_output_files(
                processed_data, visualizations, output_formats, execution
            )
            
            # Update execution with results
            execution.status = ReportStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            execution.duration_seconds = int(
                (execution.completed_at - execution.started_at).total_seconds()
            )
            execution.result_data = {
                "data": processed_data,
                "visualizations": visualizations,
                "metadata": {
                    "rows_count": len(processed_data) if isinstance(processed_data, list) else 0,
                    "execution_time": execution.duration_seconds
                }
            }
            execution.output_files = output_files
            execution.rows_processed = len(processed_data) if isinstance(processed_data, list) else 0
            
            # Cache the result
            report.cached_result = execution.result_data
            report.cached_at = datetime.utcnow()
            report.last_run_at = execution.completed_at
            
        except Exception as e:
            execution.status = ReportStatus.FAILED
            execution.completed_at = datetime.utcnow()
            execution.error_message = str(e)
            
            if execution.started_at:
                execution.duration_seconds = int(
                    (execution.completed_at - execution.started_at).total_seconds()
                )
            
            logger.error(f"Report execution {execution.execution_id} failed: {e}")
        
        finally:
            self.db.commit()
    
    async def _execute_report_queries(
        self,
        report: Report,
        parameters: Dict[str, Any],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute report queries against data sources"""
        
        results = []
        
        for data_source in report.data_sources:
            source_type = data_source["type"]
            source_config = data_source["config"]
            
            if source_type == "database":
                data = await self._execute_database_query(
                    source_config, report.query_config, parameters, filters
                )
            
            elif source_type == "dataset":
                data = await self._query_dataset(
                    source_config, report.query_config, parameters, filters
                )
            
            elif source_type == "api":
                data = await self._query_api(
                    source_config, report.query_config, parameters, filters
                )
            
            else:
                raise ValueError(f"Unsupported data source type: {source_type}")
            
            results.extend(data)
        
        return results
    
    async def _execute_database_query(
        self,
        source_config: Dict[str, Any],
        query_config: Dict[str, Any],
        parameters: Dict[str, Any],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute database query"""
        
        # Build parameterized query
        query = query_config["query"]
        
        # Apply parameters
        for param_name, param_value in parameters.items():
            query = query.replace(f"${{{param_name}}}", str(param_value))
        
        # Apply filters
        filter_clauses = []
        for filter_name, filter_value in filters.items():
            if isinstance(filter_value, list):
                values = "','".join(str(v) for v in filter_value)
                filter_clauses.append(f"{filter_name} IN ('{values}')")
            else:
                filter_clauses.append(f"{filter_name} = '{filter_value}'")
        
        if filter_clauses:
            if "WHERE" in query.upper():
                query += " AND " + " AND ".join(filter_clauses)
            else:
                query += " WHERE " + " AND ".join(filter_clauses)
        
        # Execute query (mock implementation)
        # In real implementation, this would connect to actual database
        mock_data = [
            {"id": i, "value": np.random.randint(1, 100), "category": f"Category {i % 5}"}
            for i in range(100)
        ]
        
        return mock_data
    
    async def _query_dataset(
        self,
        source_config: Dict[str, Any],
        query_config: Dict[str, Any],
        parameters: Dict[str, Any],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Query dataset"""
        
        dataset_id = source_config.get("dataset_id")
        
        dataset = self.db.query(DataSet).filter(DataSet.id == dataset_id).first()
        if not dataset:
            raise ValueError(f"Dataset {dataset_id} not found")
        
        # Mock dataset query
        mock_data = [
            {"timestamp": datetime.utcnow() - timedelta(days=i), 
             "metric": np.random.uniform(10, 100)}
            for i in range(30)
        ]
        
        return mock_data
    
    async def _query_api(
        self,
        source_config: Dict[str, Any],
        query_config: Dict[str, Any],
        parameters: Dict[str, Any],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Query external API"""
        
        # Mock API query
        mock_data = [
            {"date": (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d"),
             "users": np.random.randint(100, 1000),
             "revenue": np.random.uniform(1000, 10000)}
            for i in range(7)
        ]
        
        return mock_data
    
    async def _process_report_data(
        self,
        raw_data: List[Dict[str, Any]],
        query_config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Process and transform report data"""
        
        df = pd.DataFrame(raw_data)
        
        # Apply transformations
        transformations = query_config.get("transformations", [])
        
        for transform in transformations:
            transform_type = transform.get("type")
            
            if transform_type == "aggregation":
                group_by = transform.get("group_by", [])
                aggregations = transform.get("aggregations", {})
                
                if group_by:
                    df = df.groupby(group_by).agg(aggregations).reset_index()
            
            elif transform_type == "filter":
                condition = transform.get("condition")
                # Apply filter condition
                pass
            
            elif transform_type == "sort":
                sort_by = transform.get("sort_by")
                ascending = transform.get("ascending", True)
                df = df.sort_values(sort_by, ascending=ascending)
        
        # Convert back to list of dictionaries
        return df.to_dict('records')
    
    async def _generate_visualizations(
        self,
        data: List[Dict[str, Any]],
        visualization_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate visualizations for report data"""
        
        visualizations = {}
        
        charts = visualization_config.get("charts", [])
        
        for chart_config in charts:
            chart_type = chart_config.get("type")
            chart_name = chart_config.get("name", f"chart_{len(visualizations)}")
            
            if chart_type == "bar_chart":
                chart_data = self._create_bar_chart_data(data, chart_config)
            
            elif chart_type == "line_chart":
                chart_data = self._create_line_chart_data(data, chart_config)
            
            elif chart_type == "pie_chart":
                chart_data = self._create_pie_chart_data(data, chart_config)
            
            elif chart_type == "table":
                chart_data = self._create_table_data(data, chart_config)
            
            else:
                chart_data = {"type": chart_type, "data": data}
            
            visualizations[chart_name] = chart_data
        
        return visualizations
    
    def _create_bar_chart_data(
        self,
        data: List[Dict[str, Any]],
        chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create bar chart data"""
        
        x_field = chart_config.get("x_field")
        y_field = chart_config.get("y_field")
        
        chart_data = {
            "type": "bar_chart",
            "data": data,
            "config": {
                "x_field": x_field,
                "y_field": y_field,
                "title": chart_config.get("title", "Bar Chart")
            }
        }
        
        return chart_data
    
    def _create_line_chart_data(
        self,
        data: List[Dict[str, Any]],
        chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create line chart data"""
        
        x_field = chart_config.get("x_field")
        y_field = chart_config.get("y_field")
        
        chart_data = {
            "type": "line_chart",
            "data": data,
            "config": {
                "x_field": x_field,
                "y_field": y_field,
                "title": chart_config.get("title", "Line Chart")
            }
        }
        
        return chart_data
    
    def _create_pie_chart_data(
        self,
        data: List[Dict[str, Any]],
        chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create pie chart data"""
        
        label_field = chart_config.get("label_field")
        value_field = chart_config.get("value_field")
        
        chart_data = {
            "type": "pie_chart",
            "data": data,
            "config": {
                "label_field": label_field,
                "value_field": value_field,
                "title": chart_config.get("title", "Pie Chart")
            }
        }
        
        return chart_data
    
    def _create_table_data(
        self,
        data: List[Dict[str, Any]],
        chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create table data"""
        
        columns = chart_config.get("columns", [])
        
        if not columns and data:
            columns = list(data[0].keys())
        
        chart_data = {
            "type": "table",
            "data": data,
            "config": {
                "columns": columns,
                "title": chart_config.get("title", "Data Table")
            }
        }
        
        return chart_data
    
    async def _generate_output_files(
        self,
        data: List[Dict[str, Any]],
        visualizations: Dict[str, Any],
        output_formats: List[str],
        execution: ReportExecution
    ) -> List[Dict[str, str]]:
        """Generate output files in requested formats"""
        
        output_files = []
        
        for format_type in output_formats:
            if format_type == "csv":
                file_info = await self._generate_csv_file(data, execution)
            
            elif format_type == "excel":
                file_info = await self._generate_excel_file(data, visualizations, execution)
            
            elif format_type == "pdf":
                file_info = await self._generate_pdf_file(data, visualizations, execution)
            
            elif format_type == "json":
                file_info = await self._generate_json_file(data, execution)
            
            else:
                continue
            
            output_files.append(file_info)
        
        return output_files
    
    async def _generate_csv_file(
        self,
        data: List[Dict[str, Any]],
        execution: ReportExecution
    ) -> Dict[str, str]:
        """Generate CSV file"""
        
        df = pd.DataFrame(data)
        
        filename = f"report_{execution.execution_id}.csv"
        file_path = self.storage_path / filename
        
        df.to_csv(file_path, index=False)
        
        return {
            "format": "csv",
            "filename": filename,
            "path": str(file_path),
            "size_bytes": file_path.stat().st_size
        }
    
    async def _generate_excel_file(
        self,
        data: List[Dict[str, Any]],
        visualizations: Dict[str, Any],
        execution: ReportExecution
    ) -> Dict[str, str]:
        """Generate Excel file"""
        
        filename = f"report_{execution.execution_id}.xlsx"
        file_path = self.storage_path / filename
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # Write data
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name='Data', index=False)
            
            # Write visualization configs
            if visualizations:
                viz_df = pd.DataFrame([
                    {"name": name, "type": viz.get("type"), "config": json.dumps(viz.get("config", {}))}
                    for name, viz in visualizations.items()
                ])
                viz_df.to_excel(writer, sheet_name='Visualizations', index=False)
        
        return {
            "format": "excel",
            "filename": filename,
            "path": str(file_path),
            "size_bytes": file_path.stat().st_size
        }
    
    async def _generate_pdf_file(
        self,
        data: List[Dict[str, Any]],
        visualizations: Dict[str, Any],
        execution: ReportExecution
    ) -> Dict[str, str]:
        """Generate PDF file"""
        
        # This would use a PDF generation library like ReportLab
        # For now, create a mock PDF file
        filename = f"report_{execution.execution_id}.pdf"
        file_path = self.storage_path / filename
        
        # Mock PDF content
        with open(file_path, 'w') as f:
            f.write("Mock PDF Report Content")
        
        return {
            "format": "pdf",
            "filename": filename,
            "path": str(file_path),
            "size_bytes": file_path.stat().st_size
        }
    
    async def _generate_json_file(
        self,
        data: List[Dict[str, Any]],
        execution: ReportExecution
    ) -> Dict[str, str]:
        """Generate JSON file"""
        
        filename = f"report_{execution.execution_id}.json"
        file_path = self.storage_path / filename
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        return {
            "format": "json",
            "filename": filename,
            "path": str(file_path),
            "size_bytes": file_path.stat().st_size
        }
    
    async def create_dashboard(
        self,
        name: str,
        dashboard_type: DashboardType,
        layout_config: Dict[str, Any],
        widgets: List[Dict[str, Any]],
        org_id: int,
        created_by: int,
        description: str = None,
        project_id: Optional[int] = None
    ) -> Dashboard:
        """Create a new dashboard"""
        
        dashboard = Dashboard(
            name=name,
            description=description,
            dashboard_type=dashboard_type,
            layout_config=layout_config,
            widgets=widgets,
            org_id=org_id,
            project_id=project_id,
            created_by=created_by
        )
        
        self.db.add(dashboard)
        self.db.commit()
        self.db.refresh(dashboard)
        
        # Create widget records
        await self._create_dashboard_widgets(dashboard, widgets)
        
        return dashboard
    
    async def _create_dashboard_widgets(
        self,
        dashboard: Dashboard,
        widgets_config: List[Dict[str, Any]]
    ):
        """Create widget records for dashboard"""
        
        for widget_config in widgets_config:
            widget = Widget(
                dashboard_id=dashboard.id,
                widget_id=widget_config.get("id", str(uuid.uuid4())),
                name=widget_config.get("name", "Untitled Widget"),
                description=widget_config.get("description"),
                widget_type=widget_config.get("widget_type", "chart"),
                visualization_type=widget_config.get("visualization_type", "bar_chart"),
                data_source_config=widget_config.get("data_source_config", {}),
                query_config=widget_config.get("query_config", {}),
                visual_config=widget_config.get("visual_config", {}),
                position_config=widget_config.get("position_config", {}),
                refresh_interval=widget_config.get("refresh_interval", 300)
            )
            
            self.db.add(widget)
        
        self.db.commit()
    
    async def get_dashboard_data(
        self,
        dashboard_id: int,
        parameters: Optional[Dict[str, Any]] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get data for all widgets in a dashboard"""
        
        dashboard = self.db.query(Dashboard).filter(Dashboard.id == dashboard_id).first()
        if not dashboard:
            raise ValueError("Dashboard not found")
        
        # Update view count
        dashboard.view_count += 1
        dashboard.last_viewed_at = datetime.utcnow()
        self.db.commit()
        
        # Get widget data
        widget_data = {}
        
        for widget in dashboard.widgets_rel:
            if not widget.enabled:
                continue
            
            # Check if widget data is cached
            if self._is_widget_cached(widget):
                widget_data[widget.widget_id] = widget.cached_data
            else:
                # Execute widget query
                data = await self._execute_widget_query(widget, parameters, filters)
                
                # Cache the data
                widget.cached_data = data
                widget.cached_at = datetime.utcnow()
                
                widget_data[widget.widget_id] = data
        
        self.db.commit()
        
        return {
            "dashboard": {
                "id": dashboard.id,
                "name": dashboard.name,
                "layout_config": dashboard.layout_config,
                "theme_config": dashboard.theme_config
            },
            "widgets": widget_data,
            "filters": filters or {},
            "parameters": parameters or {}
        }
    
    def _is_widget_cached(self, widget: Widget) -> bool:
        """Check if widget has valid cached data"""
        
        if not widget.cached_data or not widget.cached_at:
            return False
        
        cache_ttl = widget.cache_ttl_minutes or 30
        cache_expiry = widget.cached_at + timedelta(minutes=cache_ttl)
        
        return datetime.utcnow() < cache_expiry
    
    async def _execute_widget_query(
        self,
        widget: Widget,
        parameters: Optional[Dict[str, Any]],
        filters: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute query for a widget"""
        
        data_source_config = widget.data_source_config
        query_config = widget.query_config
        
        # Execute query based on data source type
        source_type = data_source_config.get("type", "mock")
        
        if source_type == "mock":
            # Generate mock data based on visualization type
            raw_data = self._generate_mock_widget_data(widget.visualization_type)
        else:
            # Execute real query
            raw_data = await self._execute_report_queries(
                type('Report', (), {
                    'data_sources': [data_source_config],
                    'query_config': query_config
                })(),
                parameters or {},
                filters or {}
            )
        
        # Apply widget-specific transformations
        processed_data = await self._process_widget_data(raw_data, widget)
        
        return {
            "data": processed_data,
            "visualization_type": widget.visualization_type,
            "visual_config": widget.visual_config,
            "last_updated": datetime.utcnow().isoformat()
        }
    
    def _generate_mock_widget_data(self, visualization_type: str) -> List[Dict[str, Any]]:
        """Generate mock data for widget based on visualization type"""
        
        if visualization_type == "bar_chart":
            return [
                {"category": f"Category {i}", "value": np.random.randint(10, 100)}
                for i in range(5)
            ]
        
        elif visualization_type == "line_chart":
            return [
                {"date": (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d"),
                 "value": np.random.uniform(10, 100)}
                for i in range(30)
            ]
        
        elif visualization_type == "pie_chart":
            return [
                {"label": f"Segment {i}", "value": np.random.randint(10, 100)}
                for i in range(4)
            ]
        
        elif visualization_type == "metric":
            return [{"value": np.random.uniform(50, 100), "label": "KPI"}]
        
        elif visualization_type == "table":
            return [
                {"id": i, "name": f"Item {i}", "value": np.random.randint(1, 100)}
                for i in range(10)
            ]
        
        else:
            return [{"value": np.random.uniform(0, 100)} for _ in range(10)]
    
    async def _process_widget_data(
        self,
        raw_data: List[Dict[str, Any]],
        widget: Widget
    ) -> List[Dict[str, Any]]:
        """Process data specific to widget requirements"""
        
        # Apply widget-specific transformations
        transformation_config = widget.transformation_config or {}
        
        df = pd.DataFrame(raw_data)
        
        # Apply filters
        filters = transformation_config.get("filters", [])
        for filter_config in filters:
            field = filter_config.get("field")
            operator = filter_config.get("operator")
            value = filter_config.get("value")
            
            if field in df.columns:
                if operator == "equals":
                    df = df[df[field] == value]
                elif operator == "greater_than":
                    df = df[df[field] > value]
                elif operator == "less_than":
                    df = df[df[field] < value]
        
        # Apply sorting
        sort_config = transformation_config.get("sort")
        if sort_config:
            field = sort_config.get("field")
            ascending = sort_config.get("ascending", True)
            if field in df.columns:
                df = df.sort_values(field, ascending=ascending)
        
        # Apply limit
        limit = transformation_config.get("limit")
        if limit:
            df = df.head(limit)
        
        return df.to_dict('records')
    
    async def create_alert_rule(
        self,
        name: str,
        rule_type: str,
        condition_config: Dict[str, Any],
        data_source_config: Dict[str, Any],
        query_config: Dict[str, Any],
        notification_config: Dict[str, Any],
        org_id: int,
        created_by: int,
        threshold_value: Optional[float] = None,
        comparison_operator: str = ">",
        severity: str = "medium"
    ) -> AlertRule:
        """Create a new alert rule"""
        
        alert_rule = AlertRule(
            name=name,
            rule_type=rule_type,
            condition_config=condition_config,
            data_source_config=data_source_config,
            query_config=query_config,
            notification_config=notification_config,
            threshold_value=threshold_value,
            comparison_operator=comparison_operator,
            severity=severity,
            org_id=org_id,
            created_by=created_by
        )
        
        self.db.add(alert_rule)
        self.db.commit()
        self.db.refresh(alert_rule)
        
        return alert_rule
    
    async def evaluate_alert_rules(self):
        """Evaluate all active alert rules"""
        
        active_rules = self.db.query(AlertRule).filter(
            AlertRule.enabled == True
        ).all()
        
        for rule in active_rules:
            try:
                await self._evaluate_alert_rule(rule)
            except Exception as e:
                logger.error(f"Error evaluating alert rule {rule.id}: {e}")
    
    async def _evaluate_alert_rule(self, rule: AlertRule):
        """Evaluate a single alert rule"""
        
        # Execute query to get current value
        current_value = await self._get_alert_rule_value(rule)
        
        if current_value is None:
            return
        
        # Check if threshold is breached
        should_trigger = self._check_threshold(
            current_value, rule.threshold_value, rule.comparison_operator
        )
        
        if should_trigger:
            # Check if there's already an active alert
            existing_alert = self.db.query(AlertInstance).filter(
                and_(
                    AlertInstance.alert_rule_id == rule.id,
                    AlertInstance.status == "active"
                )
            ).first()
            
            if not existing_alert:
                # Create new alert instance
                alert_instance = AlertInstance(
                    alert_rule_id=rule.id,
                    severity=rule.severity,
                    triggered_value=current_value,
                    message=f"Alert {rule.name}: {current_value} {rule.comparison_operator} {rule.threshold_value}"
                )
                
                self.db.add(alert_instance)
                
                # Send notifications
                await self._send_alert_notifications(alert_instance, rule)
        
        # Update rule evaluation timestamp
        rule.last_evaluated_at = datetime.utcnow()
        self.db.commit()
    
    async def _get_alert_rule_value(self, rule: AlertRule) -> Optional[float]:
        """Get current value for alert rule evaluation"""
        
        # Execute query from rule configuration
        # This would execute the actual query against the data source
        # For now, return a mock value
        return np.random.uniform(0, 100)
    
    def _check_threshold(
        self,
        value: float,
        threshold: float,
        operator: str
    ) -> bool:
        """Check if value breaches threshold"""
        
        if operator == ">":
            return value > threshold
        elif operator == "<":
            return value < threshold
        elif operator == ">=":
            return value >= threshold
        elif operator == "<=":
            return value <= threshold
        elif operator == "==":
            return value == threshold
        elif operator == "!=":
            return value != threshold
        else:
            return False
    
    async def _send_alert_notifications(
        self,
        alert_instance: AlertInstance,
        rule: AlertRule
    ):
        """Send notifications for alert"""
        
        notification_config = rule.notification_config
        
        for notification in notification_config:
            notification_type = notification.get("type")
            
            if notification_type == "email":
                await self._send_email_alert(alert_instance, notification)
            
            elif notification_type == "webhook":
                await self._send_webhook_alert(alert_instance, notification)
            
            elif notification_type == "slack":
                await self._send_slack_alert(alert_instance, notification)
    
    async def _send_email_alert(
        self,
        alert_instance: AlertInstance,
        notification_config: Dict[str, Any]
    ):
        """Send email alert notification"""
        # Implementation would send actual email
        logger.info(f"Email alert sent for alert {alert_instance.id}")
    
    async def _send_webhook_alert(
        self,
        alert_instance: AlertInstance,
        notification_config: Dict[str, Any]
    ):
        """Send webhook alert notification"""
        # Implementation would send HTTP POST to webhook URL
        logger.info(f"Webhook alert sent for alert {alert_instance.id}")
    
    async def _send_slack_alert(
        self,
        alert_instance: AlertInstance,
        notification_config: Dict[str, Any]
    ):
        """Send Slack alert notification"""
        # Implementation would send to Slack API
        logger.info(f"Slack alert sent for alert {alert_instance.id}")
    
    async def get_reporting_metrics(
        self,
        org_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Get reporting system usage metrics"""
        
        # Report execution metrics
        executions = self.db.query(ReportExecution).filter(
            and_(
                ReportExecution.started_at >= start_date,
                ReportExecution.started_at <= end_date
            )
        ).join(Report).filter(Report.org_id == org_id).all()
        
        total_executions = len(executions)
        successful_executions = len([e for e in executions if e.status == ReportStatus.COMPLETED])
        failed_executions = len([e for e in executions if e.status == ReportStatus.FAILED])
        
        success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
        
        # Dashboard view metrics
        dashboards = self.db.query(Dashboard).filter(Dashboard.org_id == org_id).all()
        total_dashboard_views = sum(d.view_count for d in dashboards)
        
        # Average execution time
        completed_executions = [e for e in executions if e.duration_seconds]
        avg_execution_time = np.mean([e.duration_seconds for e in completed_executions]) if completed_executions else 0
        
        return {
            "reports": {
                "total_executions": total_executions,
                "successful_executions": successful_executions,
                "failed_executions": failed_executions,
                "success_rate_percent": round(success_rate, 2),
                "average_execution_time_seconds": round(avg_execution_time, 2)
            },
            "dashboards": {
                "total_dashboards": len(dashboards),
                "total_views": total_dashboard_views
            },
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        }