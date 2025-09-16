"""
Flows API Tests.
Migrated from Rails flows_spec.rb - comprehensive flow management tests.

This test suite covers:
- Flow CRUD operations
- Flow execution and runs
- Flow ownership and sharing
- Flow status management
- Flow metrics and statistics
- Flow templates and copying
- Flow orchestration and triggers
- Project-based flow organization
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.project import Project
from app.models.flow import Flow, FlowRun
from app.models.data_source import DataSource
from app.models.data_set import DataSet
from app.models.data_sink import DataSink
from tests.factories import (
    create_user, create_org, create_org_membership, create_project,
    create_flow, create_data_source, create_data_set, create_data_sink
)
from tests.utils import (
    TestAuthHelper, TestResponseHelper, TestDataHelper,
    status_ok, status_created, status_bad_request, status_forbidden, 
    status_not_found, status_unauthorized
)


@pytest.mark.api
@pytest.mark.flows
class TestFlowsAPI:
    """Test flows API endpoints"""
    
    def setup_method(self):
        """Set up test data for each test"""
        pass
    
    def test_create_flow_success(self, client: TestClient, db_session: Session):
        """Test successful flow creation"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create flow
        flow_data = {
            "name": "TEST FLOW",
            "description": "TEST FLOW description",
            "flow_type": "data_pipeline",
            "project_id": project.id,
            "schedule_type": "manual",
            "priority": 5
        }
        
        response = client.post("/api/v1/flows", headers=headers, json=flow_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "TEST FLOW"
        assert data["description"] == "TEST FLOW description"
        assert data["flow_type"] == "data_pipeline"
        assert data["project_id"] == project.id
        assert data["schedule_type"] == "manual"
        assert data["priority"] == 5
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
        assert data["status"] == "draft"
        assert data["is_active"] is True
    
    def test_create_flow_with_template(self, client: TestClient, db_session: Session):
        """Test creating flow from template"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        # Create template flow
        template = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project,
            name="Template Flow",
            is_template=True
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        flow_data = {
            "name": "Flow from Template",
            "description": "Created from template",
            "template_id": template.id,
            "project_id": project.id
        }
        
        response = client.post("/api/v1/flows", headers=headers, json=flow_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "Flow from Template"
        assert data["template_id"] == template.id
        assert data["is_template"] is False
    
    def test_get_flows_list(self, client: TestClient, db_session: Session):
        """Test getting list of flows"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        # Create test flows
        flow1 = create_flow(db=db_session, owner=user, org=org, project=project, name="Flow 1")
        flow2 = create_flow(db=db_session, owner=user, org=org, project=project, name="Flow 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/flows", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert isinstance(data, list)
        assert len(data) >= 2
        
        # Verify data structure
        flow_ids = [f["id"] for f in data]
        assert flow1.id in flow_ids
        assert flow2.id in flow_ids
        
        # Check response structure
        first_flow = data[0]
        assert "id" in first_flow
        assert "name" in first_flow
        assert "description" in first_flow
        assert "flow_type" in first_flow
        assert "status" in first_flow
        assert "owner" in first_flow
        assert "org" in first_flow
        assert "project" in first_flow
        assert "created_at" in first_flow
        assert "updated_at" in first_flow
    
    def test_get_flow_by_id(self, client: TestClient, db_session: Session):
        """Test getting specific flow by ID"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project,
            name="Target Flow",
            description="Target description"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get(f"/api/v1/flows/{flow.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["id"] == flow.id
        assert data["name"] == "Target Flow"
        assert data["description"] == "Target description"
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
        assert data["project"]["id"] == project.id
    
    def test_get_nonexistent_flow(self, client: TestClient, db_session: Session):
        """Test getting non-existent flow returns 404"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/flows/99999", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_update_flow(self, client: TestClient, db_session: Session):
        """Test updating a flow"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project,
            name="Original Name",
            description="Original description",
            priority=3
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        update_data = {
            "name": "Updated Name",
            "description": "Updated description",
            "priority": 8,
            "schedule_type": "cron",
            "cron_expression": "0 0 * * *"
        }
        
        response = client.put(f"/api/v1/flows/{flow.id}", headers=headers, json=update_data)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["name"] == "Updated Name"
        assert data["description"] == "Updated description"
        assert data["priority"] == 8
        assert data["schedule_type"] == "cron"
        assert data["cron_expression"] == "0 0 * * *"
        assert data["id"] == flow.id
    
    def test_delete_flow(self, client: TestClient, db_session: Session):
        """Test deleting a flow"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(db=db_session, owner=user, org=org, project=project, name="To Delete")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.delete(f"/api/v1/flows/{flow.id}", headers=headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify it's gone
        response = client.get(f"/api/v1/flows/{flow.id}", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_activate_flow(self, client: TestClient, db_session: Session):
        """Test activating a flow"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project,
            name="To Activate",
            status="draft"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.post(f"/api/v1/flows/{flow.id}/activate", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["status"] == "active"
        assert data["is_active"] is True
    
    def test_deactivate_flow(self, client: TestClient, db_session: Session):
        """Test deactivating a flow"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project,
            name="To Deactivate",
            status="active"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.post(f"/api/v1/flows/{flow.id}/deactivate", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["status"] == "paused"
        assert data["is_active"] is False
    
    def test_run_flow_manually(self, client: TestClient, db_session: Session):
        """Test manually triggering a flow run"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project,
            name="Manual Run Flow",
            status="active"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        run_data = {
            "trigger_type": "manual",
            "run_parameters": {
                "batch_size": 1000,
                "debug_mode": True
            }
        }
        
        response = client.post(f"/api/v1/flows/{flow.id}/run", headers=headers, json=run_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["flow_id"] == flow.id
        assert data["trigger_type"] == "manual"
        assert data["triggered_by"]["id"] == user.id
        assert data["status"] == "queued"
        assert "run_number" in data
        assert data["run_parameters"]["batch_size"] == 1000
        assert data["run_parameters"]["debug_mode"] is True
    
    def test_get_flow_runs(self, client: TestClient, db_session: Session):
        """Test getting flow runs history"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(db=db_session, owner=user, org=org, project=project, name="Test Flow")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create some test runs
        for i in range(3):
            run_data = {"trigger_type": "manual"}
            response = client.post(f"/api/v1/flows/{flow.id}/run", headers=headers, json=run_data)
            TestResponseHelper.assert_success_response(response, status_created())
        
        # Get flow runs
        response = client.get(f"/api/v1/flows/{flow.id}/runs", headers=headers)
        runs = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert isinstance(runs, list)
        assert len(runs) >= 3
        
        # Verify run structure
        first_run = runs[0]
        assert "id" in first_run
        assert "run_number" in first_run
        assert "status" in first_run
        assert "trigger_type" in first_run
        assert "triggered_by" in first_run
        assert "created_at" in first_run
        assert "flow_id" in first_run
        assert first_run["flow_id"] == flow.id
    
    def test_get_flow_run_by_id(self, client: TestClient, db_session: Session):
        """Test getting specific flow run details"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(db=db_session, owner=user, org=org, project=project, name="Test Flow")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create a run
        run_data = {
            "trigger_type": "manual",
            "run_parameters": {"test_param": "test_value"}
        }
        
        response = client.post(f"/api/v1/flows/{flow.id}/run", headers=headers, json=run_data)
        run_creation = TestResponseHelper.assert_success_response(response, status_created())
        run_id = run_creation["id"]
        
        # Get run details
        response = client.get(f"/api/v1/flows/{flow.id}/runs/{run_id}", headers=headers)
        run_details = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert run_details["id"] == run_id
        assert run_details["flow_id"] == flow.id
        assert run_details["trigger_type"] == "manual"
        assert run_details["run_parameters"]["test_param"] == "test_value"
    
    def test_copy_flow(self, client: TestClient, db_session: Session):
        """Test copying a flow"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        original_flow = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project,
            name="Original Flow",
            description="Original description",
            flow_type="data_pipeline"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        copy_data = {
            "name": "Copied Flow",
            "description": "Copy of original flow",
            "project_id": project.id
        }
        
        response = client.post(f"/api/v1/flows/{original_flow.id}/copy", headers=headers, json=copy_data)
        copied_flow = TestResponseHelper.assert_success_response(response, status_created())
        
        assert copied_flow["name"] == "Copied Flow"
        assert copied_flow["description"] == "Copy of original flow"
        assert copied_flow["flow_type"] == original_flow.flow_type
        assert copied_flow["id"] != original_flow.id
        assert copied_flow["owner"]["id"] == user.id
        assert copied_flow["project_id"] == project.id
    
    def test_get_flows_by_project(self, client: TestClient, db_session: Session):
        """Test filtering flows by project"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        # Create two projects
        project1 = create_project(db=db_session, owner=user, org=org, name="Project 1")
        project2 = create_project(db=db_session, owner=user, org=org, name="Project 2")
        
        # Create flows in each project
        flow1 = create_flow(db=db_session, owner=user, org=org, project=project1, name="Flow in Project 1")
        flow2 = create_flow(db=db_session, owner=user, org=org, project=project2, name="Flow in Project 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Filter by project 1
        response = client.get(f"/api/v1/flows?project_id={project1.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Should only see flows from project 1
        flow_ids = [f["id"] for f in data]
        assert flow1.id in flow_ids
        assert flow2.id not in flow_ids
        
        # All returned flows should belong to project 1
        assert all(f["project"]["id"] == project1.id for f in data)
    
    def test_flow_search(self, client: TestClient, db_session: Session):
        """Test flow search functionality"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        # Create flows with searchable names
        flow1 = create_flow(db=db_session, owner=user, org=org, project=project, name="ETL Pipeline Flow")
        flow2 = create_flow(db=db_session, owner=user, org=org, project=project, name="Data Ingestion Flow") 
        flow3 = create_flow(db=db_session, owner=user, org=org, project=project, name="Analytics Report Flow")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        search_data = {
            "query": "Pipeline",
            "filters": [
                {
                    "field": "flow_type",
                    "operator": "equals",
                    "value": "data_pipeline"
                }
            ]
        }
        
        response = client.post("/api/v1/flows/search", headers=headers, json=search_data)
        results = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Should find flow1 (contains "Pipeline")
        assert len(results) >= 1
        assert any(f["id"] == flow1.id for f in results)
        assert any("Pipeline" in f["name"] for f in results)
    
    def test_flow_ownership_and_sharing(self, client: TestClient, db_session: Session):
        """Test flow ownership and cross-org sharing"""
        # Set up two orgs with users
        user1 = create_user(db=db_session, email="user1@org1.com")
        org1 = create_org(db=db_session, name="Org 1", owner=user1)
        create_org_membership(db=db_session, user=user1, org=org1)
        project1 = create_project(db=db_session, owner=user1, org=org1, name="Org 1 Project")
        
        user2 = create_user(db=db_session, email="user2@org2.com")
        org2 = create_org(db=db_session, name="Org 2", owner=user2)
        create_org_membership(db=db_session, user=user2, org=org2)
        
        # Create flow in org1
        flow = create_flow(db=db_session, owner=user1, org=org1, project=project1, name="Org 1 Flow")
        
        headers1 = TestAuthHelper.get_auth_headers(user1, org1)
        headers2 = TestAuthHelper.get_auth_headers(user2, org2)
        
        # User1 can access their flow
        response = client.get(f"/api/v1/flows/{flow.id}", headers=headers1)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # User2 cannot access org1's flow initially
        response = client.get(f"/api/v1/flows/{flow.id}", headers=headers2)
        TestResponseHelper.assert_forbidden(response)
        
        # Share flow with org2 (this would be implemented in sharing endpoints)
        # For now, just test access control works
        
    def test_flow_access_control(self, client: TestClient, db_session: Session):
        """Test that users can only access their org's flows"""
        # Set up two separate orgs with users
        user1 = create_user(db=db_session, email="user1@org1.com")
        org1 = create_org(db=db_session, name="Org 1", owner=user1)
        create_org_membership(db=db_session, user=user1, org=org1)
        project1 = create_project(db=db_session, owner=user1, org=org1, name="Org 1 Project")
        
        user2 = create_user(db=db_session, email="user2@org2.com")
        org2 = create_org(db=db_session, name="Org 2", owner=user2)
        create_org_membership(db=db_session, user=user2, org=org2)
        project2 = create_project(db=db_session, owner=user2, org=org2, name="Org 2 Project")
        
        # Create flows in each org
        flow1 = create_flow(db=db_session, owner=user1, org=org1, project=project1, name="Org 1 Flow")
        flow2 = create_flow(db=db_session, owner=user2, org=org2, project=project2, name="Org 2 Flow")
        
        # User 1 should only see org 1's flows
        headers1 = TestAuthHelper.get_auth_headers(user1, org1)
        response = client.get("/api/v1/flows", headers=headers1)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        flow_ids = [f["id"] for f in data]
        assert flow1.id in flow_ids
        assert flow2.id not in flow_ids
        
        # User 2 should only see org 2's flows
        headers2 = TestAuthHelper.get_auth_headers(user2, org2)
        response = client.get("/api/v1/flows", headers=headers2)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        flow_ids = [f["id"] for f in data]
        assert flow2.id in flow_ids
        assert flow1.id not in flow_ids
    
    def test_flow_ownership_validation(self, client: TestClient, db_session: Session):
        """Test that only owners can modify flows"""
        owner = create_user(db=db_session, email="owner@test.com")
        other_user = create_user(db=db_session, email="other@test.com")
        org = create_org(db=db_session, name="Test Org", owner=owner)
        
        create_org_membership(db=db_session, user=owner, org=org)
        create_org_membership(db=db_session, user=other_user, org=org)
        project = create_project(db=db_session, owner=owner, org=org, name="Test Project")
        
        # Create flow owned by owner
        flow = create_flow(db=db_session, owner=owner, org=org, project=project, name="Owner's Flow")
        
        # Other user tries to modify it
        other_headers = TestAuthHelper.get_auth_headers(other_user, org)
        update_data = {"name": "Hacked Name"}
        
        response = client.put(f"/api/v1/flows/{flow.id}", headers=other_headers, json=update_data)
        TestResponseHelper.assert_forbidden(response)
        
        # Other user tries to delete it
        response = client.delete(f"/api/v1/flows/{flow.id}", headers=other_headers)
        TestResponseHelper.assert_forbidden(response)
    
    def test_unauthorized_access_denied(self, client: TestClient):
        """Test that unauthenticated requests are denied"""
        # Try to access flows without authentication
        response = client.get("/api/v1/flows")
        TestResponseHelper.assert_unauthorized(response)
        
        # Try to create flow without authentication
        flow_data = {
            "name": "Unauthorized Flow",
            "flow_type": "data_pipeline"
        }
        
        response = client.post("/api/v1/flows", json=flow_data)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_flow_metrics_and_statistics(self, client: TestClient, db_session: Session):
        """Test flow metrics and statistics endpoints"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        flow = create_flow(
            db=db_session, 
            owner=user, 
            org=org, 
            project=project, 
            name="Metrics Test Flow"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Get flow metrics
        response = client.get(f"/api/v1/flows/{flow.id}/metrics", headers=headers)
        metrics = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify metrics structure
        assert "run_count" in metrics
        assert "success_count" in metrics
        assert "failure_count" in metrics
        assert "average_runtime" in metrics
        assert "last_run_at" in metrics
        assert "next_scheduled_run" in metrics
        
        # Get flow statistics
        response = client.get(f"/api/v1/flows/{flow.id}/stats", headers=headers)
        stats = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify stats structure
        assert "total_runs" in stats
        assert "successful_runs" in stats
        assert "failed_runs" in stats
        assert "success_rate" in stats
        assert "average_processing_time" in stats
    
    def test_flow_pagination_and_filtering(self, client: TestClient, db_session: Session):
        """Test flow list pagination and filtering"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        # Create multiple flows with different statuses
        flows = []
        for i in range(15):
            status = "active" if i % 2 == 0 else "draft"
            flow = create_flow(
                db=db_session, 
                owner=user, 
                org=org, 
                project=project,
                name=f"Flow {i:02d}",
                status=status
            )
            flows.append(flow)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Test pagination
        response = client.get("/api/v1/flows?limit=10&offset=0", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert len(data) == 10
        
        # Test filtering by status
        response = client.get("/api/v1/flows?status=active", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # All returned flows should be active
        assert all(f["status"] == "active" for f in data)
        
        # Test filtering by owner
        response = client.get(f"/api/v1/flows?owner_id={user.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # All returned flows should belong to the user
        assert all(f["owner"]["id"] == user.id for f in data)
    
    def test_flow_templates_management(self, client: TestClient, db_session: Session):
        """Test flow templates creation and usage"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create template flow
        template_data = {
            "name": "ETL Template",
            "description": "Reusable ETL template",
            "flow_type": "data_pipeline",
            "project_id": project.id,
            "is_template": True,
            "template_config": {
                "default_settings": {
                    "batch_size": 1000,
                    "retry_count": 3
                },
                "configurable_parameters": [
                    "source_connection",
                    "destination_table",
                    "schedule"
                ]
            }
        }
        
        response = client.post("/api/v1/flows", headers=headers, json=template_data)
        template = TestResponseHelper.assert_success_response(response, status_created())
        
        assert template["is_template"] is True
        assert template["template_config"]["default_settings"]["batch_size"] == 1000
        
        # Get templates list
        response = client.get("/api/v1/flows/templates", headers=headers)
        templates = TestResponseHelper.assert_success_response(response, status_ok())
        
        template_ids = [t["id"] for t in templates]
        assert template["id"] in template_ids
        
        # All returned items should be templates
        assert all(t["is_template"] is True for t in templates)
    
    def test_flow_validation_errors(self, client: TestClient, db_session: Session):
        """Test flow creation validation errors"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Missing required fields
        invalid_flow_data = {
            "description": "Missing name and project"
        }
        
        response = client.post("/api/v1/flows", headers=headers, json=invalid_flow_data)
        TestResponseHelper.assert_error_response(response, status_bad_request())
        
        # Invalid flow type
        invalid_type_data = {
            "name": "Invalid Flow",
            "flow_type": "invalid_type"
        }
        
        response = client.post("/api/v1/flows", headers=headers, json=invalid_type_data)
        TestResponseHelper.assert_error_response(response, status_bad_request())
        
        # Invalid priority
        invalid_priority_data = {
            "name": "Invalid Priority Flow",
            "flow_type": "data_pipeline",
            "priority": 20  # Should be 1-10
        }
        
        response = client.post("/api/v1/flows", headers=headers, json=invalid_priority_data)
        TestResponseHelper.assert_error_response(response, status_bad_request())