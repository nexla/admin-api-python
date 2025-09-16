"""
Tests for Flow model.
Migrated from Rails spec patterns for flow functionality.
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.flow import Flow, FlowRun, FlowPermission, FlowTemplate
from tests.factories import (
    create_flow, create_user, create_org, create_project, create_team
)


@pytest.mark.unit
class TestFlow:
    """Test Flow model functionality"""
    
    def test_create_flow(self, db_session: Session):
        """Test creating a basic flow"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        project = create_project(db=db_session, owner=user, org=org)
        
        flow = create_flow(
            db=db_session,
            name="Test Flow",
            description="A test data pipeline flow",
            owner=user,
            org=org,
            project=project,
            flow_type="data_pipeline",
            status="draft",
            priority=5
        )
        
        assert flow.name == "Test Flow"
        assert flow.description == "A test data pipeline flow"
        assert flow.flow_type == "data_pipeline"
        assert flow.status == "draft"
        assert flow.schedule_type == "manual"
        assert flow.priority == 5
        assert flow.owner == user
        assert flow.org == org
        assert flow.project == project
        assert flow.is_active is True
        assert flow.is_template is False
    
    def test_flow_types(self, db_session: Session):
        """Test different flow types"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Data pipeline flow
        data_flow = create_flow(
            db=db_session,
            flow_type="data_pipeline",
            owner=user,
            org=org
        )
        assert data_flow.flow_type == "data_pipeline"
        
        # Analytics flow
        analytics_flow = create_flow(
            db=db_session,
            flow_type="analytics",
            owner=user,
            org=org
        )
        assert analytics_flow.flow_type == "analytics"
        
        # ETL flow
        etl_flow = create_flow(
            db=db_session,
            flow_type="etl",
            owner=user,
            org=org
        )
        assert etl_flow.flow_type == "etl"
        
        # ML pipeline flow
        ml_flow = create_flow(
            db=db_session,
            flow_type="ml_pipeline",
            owner=user,
            org=org
        )
        assert ml_flow.flow_type == "ml_pipeline"
    
    def test_flow_status_management(self, db_session: Session):
        """Test flow status transitions"""
        flow = create_flow(db=db_session, status="draft")
        
        # Test status transitions
        assert flow.status == "draft"
        
        # Activate flow
        flow.status = "active"
        db_session.commit()
        assert flow.status == "active"
        
        # Pause flow
        flow.status = "paused"
        db_session.commit()
        assert flow.status == "paused"
        
        # Stop flow
        flow.status = "stopped"
        db_session.commit()
        assert flow.status == "stopped"
    
    def test_flow_schedule_types(self, db_session: Session):
        """Test flow schedule types"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Manual flow
        manual_flow = create_flow(
            db=db_session,
            schedule_type="manual",
            owner=user,
            org=org
        )
        assert manual_flow.schedule_type == "manual"
        
        # Cron scheduled flow
        cron_config = {
            "cron": "0 2 * * *",  # Daily at 2 AM
            "timezone": "UTC"
        }
        cron_flow = create_flow(
            db=db_session,
            schedule_type="cron",
            schedule_config=cron_config,
            owner=user,
            org=org
        )
        assert cron_flow.schedule_type == "cron"
        assert cron_flow.schedule_config["cron"] == "0 2 * * *"
        
        # Event-driven flow
        event_config = {
            "trigger": "file_arrival",
            "source": "s3://bucket/path/",
            "file_pattern": "*.csv"
        }
        event_flow = create_flow(
            db=db_session,
            schedule_type="event_driven",
            schedule_config=event_config,
            owner=user,
            org=org
        )
        assert event_flow.schedule_type == "event_driven"
        assert event_flow.schedule_config["trigger"] == "file_arrival"
    
    def test_flow_relationships(self, db_session: Session):
        """Test flow relationships"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        project = create_project(db=db_session, owner=user, org=org)
        team = create_team(db=db_session, organization=org, owner=user, created_by=user)
        
        flow = create_flow(
            db=db_session,
            name="Relationship Test Flow",
            owner=user,
            org=org,
            project=project,
            team=team
        )
        
        # Test relationships
        assert flow.owner == user
        assert flow.org == org
        assert flow.project == project
        assert flow.team == team
        assert flow.owner_id == user.id
        assert flow.org_id == org.id
        assert flow.project_id == project.id
        assert flow.team_id == team.id
    
    def test_flow_priority_settings(self, db_session: Session):
        """Test flow priority settings"""
        # High priority flow
        high_priority = create_flow(db=db_session, priority=1)
        assert high_priority.priority == 1
        
        # Normal priority flow
        normal_priority = create_flow(db=db_session, priority=5)
        assert normal_priority.priority == 5
        
        # Low priority flow
        low_priority = create_flow(db=db_session, priority=10)
        assert low_priority.priority == 10
    
    def test_flow_settings(self, db_session: Session):
        """Test flow configuration settings"""
        flow = create_flow(
            db=db_session,
            is_active=True,
            is_template=False,
            auto_start=True,
            retry_count=5,
            timeout_minutes=120
        )
        
        assert flow.is_active is True
        assert flow.is_template is False
        assert flow.auto_start is True
        assert flow.retry_count == 5
        assert flow.timeout_minutes == 120


@pytest.mark.unit
class TestFlowBusinessLogic:
    """Test Flow business logic methods"""
    
    def test_get_node_count_placeholder(self, db_session: Session):
        """Test get_node_count method (placeholder implementation)"""
        flow = create_flow(db=db_session)
        
        # Currently returns 0 as placeholder
        assert flow.get_node_count() == 0
    
    def test_get_latest_run_status(self, db_session: Session):
        """Test get_latest_run_status method"""
        # Flow with no runs
        new_flow = create_flow(db=db_session, last_run_status=None)
        assert new_flow.get_latest_run_status() == "never_run"
        
        # Flow with successful run
        success_flow = create_flow(db=db_session, last_run_status="success")
        assert success_flow.get_latest_run_status() == "success"
        
        # Flow with failed run
        failed_flow = create_flow(db=db_session, last_run_status="failed")
        assert failed_flow.get_latest_run_status() == "failed"
    
    def test_is_running(self, db_session: Session):
        """Test is_running method"""
        # Running flow
        running_flow = create_flow(db=db_session, last_run_status="running")
        assert running_flow.is_running() is True
        
        # Non-running flow
        idle_flow = create_flow(db=db_session, last_run_status="success")
        assert idle_flow.is_running() is False
        
        # Never run flow
        new_flow = create_flow(db=db_session, last_run_status=None)
        assert new_flow.is_running() is False
    
    def test_can_be_started(self, db_session: Session):
        """Test can_be_started method"""
        # Active, non-running flow can be started
        startable_flow = create_flow(
            db=db_session,
            is_active=True,
            last_run_status="success"
        )
        assert startable_flow.can_be_started() is True
        
        # Inactive flow cannot be started
        inactive_flow = create_flow(
            db=db_session,
            is_active=False,
            last_run_status="success"
        )
        assert inactive_flow.can_be_started() is False
        
        # Running flow cannot be started
        running_flow = create_flow(
            db=db_session,
            is_active=True,
            last_run_status="running"
        )
        assert running_flow.can_be_started() is False
    
    def test_flow_execution_tracking(self, db_session: Session):
        """Test flow execution tracking fields"""
        flow = create_flow(
            db=db_session,
            last_run_at=datetime.utcnow() - timedelta(hours=1),
            last_run_status="success",
            next_run_at=datetime.utcnow() + timedelta(hours=23),
            run_count=10,
            success_count=8,
            failure_count=2
        )
        
        assert flow.last_run_at is not None
        assert flow.last_run_status == "success"
        assert flow.next_run_at is not None
        assert flow.run_count == 10
        assert flow.success_count == 8
        assert flow.failure_count == 2


@pytest.mark.unit
class TestFlowValidation:
    """Test Flow model validation"""
    
    def test_flow_name_required(self, db_session: Session):
        """Test that flow name is required"""
        with pytest.raises((ValueError, IntegrityError)):
            flow = Flow(
                description="No name flow",
                owner_id=1,
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(flow)
            db_session.commit()
    
    def test_flow_owner_required(self, db_session: Session):
        """Test that flow owner is required"""
        with pytest.raises((ValueError, IntegrityError)):
            flow = Flow(
                name="No Owner Flow",
                description="Missing owner",
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(flow)
            db_session.commit()
    
    def test_flow_org_required(self, db_session: Session):
        """Test that flow org is required"""
        with pytest.raises((ValueError, IntegrityError)):
            flow = Flow(
                name="No Org Flow",
                description="Missing org",
                owner_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(flow)
            db_session.commit()
    
    def test_flow_defaults(self, db_session: Session):
        """Test flow default values"""
        flow = create_flow(db=db_session)
        
        assert flow.flow_type == "data_pipeline"
        assert flow.status == "draft"
        assert flow.schedule_type == "manual"
        assert flow.version == "1.0"
        assert flow.priority == 5
        assert flow.is_active is True
        assert flow.is_template is False
        assert flow.auto_start is False
        assert flow.retry_count == 3
        assert flow.timeout_minutes == 60
        assert flow.run_count == 0
        assert flow.success_count == 0
        assert flow.failure_count == 0


@pytest.mark.unit
class TestFlowRun:
    """Test FlowRun model functionality"""
    
    def test_create_flow_run(self, db_session: Session):
        """Test creating a flow run"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        flow = create_flow(db=db_session, owner=user, org=org)
        
        flow_run = FlowRun(
            flow_id=flow.id,
            run_number=1,
            status="queued",
            trigger_type="manual",
            triggered_by=user.id,
            records_processed=0,
            records_success=0,
            records_failed=0,
            bytes_processed=0
        )
        
        db_session.add(flow_run)
        db_session.commit()
        
        assert flow_run.flow_id == flow.id
        assert flow_run.run_number == 1
        assert flow_run.status == "queued"
        assert flow_run.trigger_type == "manual"
        assert flow_run.triggered_by == user.id
    
    def test_flow_run_status_tracking(self, db_session: Session):
        """Test flow run status tracking"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        flow = create_flow(db=db_session, owner=user, org=org)
        
        flow_run = FlowRun(
            flow_id=flow.id,
            run_number=1,
            status="running",
            started_at=datetime.utcnow(),
            trigger_type="scheduled",
            triggered_by=user.id
        )
        
        db_session.add(flow_run)
        db_session.commit()
        
        # Complete the run
        flow_run.status = "success"
        flow_run.completed_at = datetime.utcnow()
        flow_run.duration_seconds = 120
        flow_run.records_processed = 1000
        flow_run.records_success = 950
        flow_run.records_failed = 50
        
        db_session.commit()
        
        assert flow_run.status == "success"
        assert flow_run.completed_at is not None
        assert flow_run.duration_seconds == 120
        assert flow_run.records_processed == 1000
    
    def test_flow_run_duration_display(self, db_session: Session):
        """Test flow run duration display formatting"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        flow = create_flow(db=db_session, owner=user, org=org)
        
        # Test different duration formats
        flow_run = FlowRun(
            flow_id=flow.id,
            run_number=1,
            triggered_by=user.id
        )
        
        # No duration
        flow_run.duration_seconds = None
        assert flow_run.get_duration_display() == "N/A"
        
        # Seconds only
        flow_run.duration_seconds = 45
        assert flow_run.get_duration_display() == "45s"
        
        # Minutes and seconds
        flow_run.duration_seconds = 125  # 2m 5s
        assert flow_run.get_duration_display() == "2m 5s"
        
        # Hours, minutes, and seconds
        flow_run.duration_seconds = 3665  # 1h 1m 5s
        assert flow_run.get_duration_display() == "1h 1m 5s"


@pytest.mark.unit
class TestFlowPermission:
    """Test FlowPermission model functionality"""
    
    def test_create_flow_permission(self, db_session: Session):
        """Test creating flow permission"""
        org = create_org(db=db_session)
        owner = create_user(db=db_session, email="owner@example.com")
        viewer = create_user(db=db_session, email="viewer@example.com")
        flow = create_flow(db=db_session, owner=owner, org=org)
        
        permission = FlowPermission(
            flow_id=flow.id,
            user_id=viewer.id,
            permission_type="view",
            granted_by=owner.id
        )
        
        db_session.add(permission)
        db_session.commit()
        
        assert permission.flow_id == flow.id
        assert permission.user_id == viewer.id
        assert permission.permission_type == "view"
        assert permission.granted_by == owner.id
        assert permission.granted_at is not None
    
    def test_flow_permission_types(self, db_session: Session):
        """Test different flow permission types"""
        org = create_org(db=db_session)
        owner = create_user(db=db_session, email="owner@example.com")
        user = create_user(db=db_session, email="user@example.com")
        flow = create_flow(db=db_session, owner=owner, org=org)
        
        permission_types = ["view", "edit", "execute", "admin"]
        
        for perm_type in permission_types:
            permission = FlowPermission(
                flow_id=flow.id,
                user_id=user.id,
                permission_type=perm_type,
                granted_by=owner.id
            )
            db_session.add(permission)
        
        db_session.commit()
        
        permissions = db_session.query(FlowPermission).filter_by(flow_id=flow.id).all()
        granted_types = {p.permission_type for p in permissions}
        assert granted_types == set(permission_types)


@pytest.mark.unit
class TestFlowTemplate:
    """Test FlowTemplate model functionality"""
    
    def test_create_flow_template(self, db_session: Session):
        """Test creating flow template"""
        org = create_org(db=db_session)
        creator = create_user(db=db_session, email="creator@example.com")
        
        template_config = {
            "flow": {
                "name": "{{ flow_name }}",
                "description": "Template for {{ purpose }}",
                "nodes": [
                    {"type": "source", "config": {"connection": "{{ source_connection }}"}},
                    {"type": "transform", "config": {"rules": "{{ transform_rules }}"}},
                    {"type": "sink", "config": {"destination": "{{ destination }}"}}
                ]
            }
        }
        
        parameter_schema = {
            "properties": {
                "flow_name": {"type": "string", "required": True},
                "purpose": {"type": "string", "required": True},
                "source_connection": {"type": "string", "required": True},
                "transform_rules": {"type": "array", "required": False},
                "destination": {"type": "string", "required": True}
            }
        }
        
        template = FlowTemplate(
            name="ETL Pipeline Template",
            description="Template for basic ETL pipelines",
            category="ETL",
            template_config=template_config,
            parameter_schema=parameter_schema,
            is_public=True,
            is_verified=False,
            created_by=creator.id,
            org_id=org.id
        )
        
        db_session.add(template)
        db_session.commit()
        
        assert template.name == "ETL Pipeline Template"
        assert template.category == "ETL"
        assert template.is_public is True
        assert template.is_verified is False
        assert template.usage_count == 0
        assert template.rating == 0
        assert len(template.template_config["flow"]["nodes"]) == 3


@pytest.mark.unit
class TestFlowScenarios:
    """Test realistic Flow usage scenarios"""
    
    def test_flow_hierarchy_scenario(self, db_session: Session):
        """Test flow parent-child hierarchy"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create parent flow
        parent_flow = create_flow(
            db=db_session,
            name="Master Data Pipeline",
            description="Parent flow that orchestrates child flows",
            owner=user,
            org=org,
            flow_type="data_pipeline"
        )
        
        # Create child flows
        child_flow1 = create_flow(
            db=db_session,
            name="Ingestion Flow",
            description="Child flow for data ingestion",
            owner=user,
            org=org,
            parent_flow_id=parent_flow.id
        )
        
        child_flow2 = create_flow(
            db=db_session,
            name="Transformation Flow", 
            description="Child flow for data transformation",
            owner=user,
            org=org,
            parent_flow_id=parent_flow.id
        )
        
        # Verify hierarchy
        assert child_flow1.parent_flow_id == parent_flow.id
        assert child_flow2.parent_flow_id == parent_flow.id
        
        # Query child flows
        child_flows = db_session.query(Flow).filter_by(parent_flow_id=parent_flow.id).all()
        assert len(child_flows) >= 2
        
        child_names = {f.name for f in child_flows}
        assert "Ingestion Flow" in child_names
        assert "Transformation Flow" in child_names