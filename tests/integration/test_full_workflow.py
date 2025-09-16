"""
Integration tests for complete data pipeline workflows.
These tests verify end-to-end functionality across multiple components.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from tests.utils import TestAuthHelper, TestResponseHelper, status_ok, status_created


@pytest.mark.integration
class TestDataPipelineWorkflow:
    """Test complete data pipeline creation and management"""
    
    def test_complete_data_pipeline_creation(self, client: TestClient, db_session: Session):
        """Test creating a complete data pipeline from source to sink"""
        from tests.factories import create_user, create_org, create_org_membership
        
        # Set up test organization and user
        org = create_org(db=db_session, name="Pipeline Test Org")
        user = create_user(db=db_session, email="pipeline@test.com")
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # 1. Create a data source
        data_source_data = {
            "name": "Test S3 Source",
            "description": "Integration test data source",
            "connection_type": "s3",
            "config": {
                "bucket": "test-bucket",
                "prefix": "data/"
            }
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=data_source_data)
        source_data = TestResponseHelper.assert_success_response(response, status_created())
        source_id = source_data["id"]
        
        # 2. Create a data set from the source
        data_set_data = {
            "name": "Processed Data Set",
            "description": "Data set created from source",
            "data_source_id": source_id,
            "transform_config": {
                "operations": ["clean", "validate"]
            }
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=data_set_data)
        set_data = TestResponseHelper.assert_success_response(response, status_created())
        set_id = set_data["id"]
        
        # 3. Create a data sink for the data set
        data_sink_data = {
            "name": "Output Sink",
            "description": "Final destination for processed data",
            "connection_type": "postgresql",
            "data_set_id": set_id,
            "config": {
                "host": "localhost",
                "database": "output_db",
                "table": "processed_data"
            }
        }
        
        response = client.post("/api/v1/data_sinks", headers=headers, json=data_sink_data)
        sink_data = TestResponseHelper.assert_success_response(response, status_created())
        
        # 4. Verify the complete pipeline exists
        response = client.get(f"/api/v1/data_sources/{source_id}", headers=headers)
        source = TestResponseHelper.assert_success_response(response)
        assert source["name"] == "Test S3 Source"
        
        response = client.get(f"/api/v1/data_sets/{set_id}", headers=headers)
        dataset = TestResponseHelper.assert_success_response(response)
        assert dataset["data_source_id"] == source_id
        
        response = client.get("/api/v1/data_sinks", headers=headers)
        sinks = TestResponseHelper.assert_success_response(response)
        assert len(sinks) >= 1
        assert any(sink["data_set_id"] == set_id for sink in sinks)
    
    def test_flow_execution_workflow(self, client: TestClient, db_session: Session):
        """Test creating and executing a complete flow"""
        from tests.factories import create_user, create_org, create_org_membership, create_project
        
        # Set up test data
        org = create_org(db=db_session, name="Flow Test Org")
        user = create_user(db=db_session, email="flow@test.com")
        create_org_membership(db=db_session, user=user, org=org)
        project = create_project(db=db_session, owner=user, org=org, name="Test Project")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # 1. Create a flow
        flow_data = {
            "name": "Integration Test Flow",
            "description": "End-to-end flow test",
            "flow_type": "data_pipeline",
            "project_id": project.id,
            "schedule_type": "manual",
            "is_active": True
        }
        
        response = client.post("/api/v1/flows", headers=headers, json=flow_data)
        flow = TestResponseHelper.assert_success_response(response, status_created())
        flow_id = flow["id"]
        
        # 2. Start the flow
        response = client.post(f"/api/v1/flows/{flow_id}/start", headers=headers)
        run_data = TestResponseHelper.assert_success_response(response)
        assert run_data["status"] in ["queued", "running"]
        
        # 3. Check flow status
        response = client.get(f"/api/v1/flows/{flow_id}/status", headers=headers)
        status_data = TestResponseHelper.assert_success_response(response)
        assert "last_run_status" in status_data
        
        # 4. Get flow runs
        response = client.get(f"/api/v1/flows/{flow_id}/runs", headers=headers)
        runs = TestResponseHelper.assert_success_response(response)
        assert len(runs) >= 1
        assert runs[0]["flow_id"] == flow_id


@pytest.mark.integration
class TestUserOrganizationWorkflow:
    """Test user and organization management workflows"""
    
    def test_complete_user_onboarding(self, client: TestClient, db_session: Session):
        """Test complete user onboarding process"""
        from tests.factories import create_org, create_user, create_org_membership
        
        # 1. Create organization
        org = create_org(db=db_session, name="Onboarding Test Org")
        admin = create_user(db=db_session, email="admin@onboarding.com")
        create_org_membership(db=db_session, user=admin, org=org)
        
        admin_headers = TestAuthHelper.get_auth_headers(admin, org)
        
        # 2. Admin creates a new user
        user_data = {
            "email": "newuser@onboarding.com",
            "full_name": "New Test User",
            "password": "securepassword123"
        }
        
        response = client.post("/api/v1/users", headers=admin_headers, json=user_data)
        new_user = TestResponseHelper.assert_success_response(response, status_created())
        user_id = new_user["id"]
        
        # 3. Add user to organization
        membership_data = {
            "user_id": user_id,
            "role": "member"
        }
        
        response = client.post(f"/api/v1/orgs/{org.id}/members", headers=admin_headers, json=membership_data)
        TestResponseHelper.assert_success_response(response, status_created())
        
        # 4. User logs in
        login_data = {
            "email": "newuser@onboarding.com",
            "password": "securepassword123"
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        login_response = TestResponseHelper.assert_success_response(response)
        assert "access_token" in login_response
        
        # 5. User accesses their profile
        user_token = login_response["access_token"]
        user_headers = {
            "Authorization": f"Bearer {user_token}",
            "Content-Type": "application/json"
        }
        
        response = client.get("/api/v1/users/me", headers=user_headers)
        profile = TestResponseHelper.assert_success_response(response)
        assert profile["email"] == "newuser@onboarding.com"
        assert profile["full_name"] == "New Test User"
    
    def test_team_collaboration_workflow(self, client: TestClient, db_session: Session):
        """Test team creation and collaboration"""
        from tests.factories import create_org, create_user, create_org_membership
        
        # Set up organization and users
        org = create_org(db=db_session, name="Team Test Org")
        team_lead = create_user(db=db_session, email="lead@team.com")
        member1 = create_user(db=db_session, email="member1@team.com")
        member2 = create_user(db=db_session, email="member2@team.com")
        
        create_org_membership(db=db_session, user=team_lead, org=org)
        create_org_membership(db=db_session, user=member1, org=org)
        create_org_membership(db=db_session, user=member2, org=org)
        
        lead_headers = TestAuthHelper.get_auth_headers(team_lead, org)
        
        # 1. Create a team
        team_data = {
            "name": "Data Science Team",
            "description": "Team for data science projects",
            "team_type": "project",
            "is_private": False
        }
        
        response = client.post("/api/v1/teams", headers=lead_headers, json=team_data)
        team = TestResponseHelper.assert_success_response(response, status_created())
        team_id = team["id"]
        
        # 2. Add members to the team
        member_data = {"user_id": member1.id, "role": "member"}
        response = client.post(f"/api/v1/teams/{team_id}/members", headers=lead_headers, json=member_data)
        TestResponseHelper.assert_success_response(response, status_created())
        
        member_data = {"user_id": member2.id, "role": "member"}
        response = client.post(f"/api/v1/teams/{team_id}/members", headers=lead_headers, json=member_data)
        TestResponseHelper.assert_success_response(response, status_created())
        
        # 3. Create a team project
        project_data = {
            "name": "Team Data Project",
            "description": "Collaborative data project",
            "team_id": team_id
        }
        
        response = client.post("/api/v1/projects", headers=lead_headers, json=project_data)
        project = TestResponseHelper.assert_success_response(response, status_created())
        
        # 4. Verify team members can access the project
        member1_headers = TestAuthHelper.get_auth_headers(member1, org)
        response = client.get(f"/api/v1/projects/{project['id']}", headers=member1_headers)
        TestResponseHelper.assert_success_response(response)


@pytest.mark.integration
class TestAuthenticationIntegration:
    """Test authentication integration across different scenarios"""
    
    def test_api_key_to_jwt_workflow(self, client: TestClient, db_session: Session):
        """Test API key authentication converting to JWT tokens"""
        from tests.factories import create_user, create_org, create_org_membership
        
        # Set up test data
        user = create_user(db=db_session, email="apikey@test.com")
        org = create_org(db=db_session, owner=user)
        membership = create_org_membership(
            db=db_session, 
            user=user, 
            org=org, 
            api_key="INTEGRATION_TEST_API_KEY"
        )
        
        # 1. Use API key to get JWT token
        api_headers = TestAuthHelper.get_api_key_headers(membership.api_key)
        response = client.post("/api/v1/auth/token", headers=api_headers)
        token_data = TestResponseHelper.assert_success_response(response)
        
        access_token = token_data["access_token"]
        assert access_token is not None
        
        # 2. Use JWT token to access protected endpoints
        jwt_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        response = client.get("/api/v1/users/me", headers=jwt_headers)
        user_data = TestResponseHelper.assert_success_response(response)
        assert user_data["email"] == "apikey@test.com"
        
        # 3. Refresh the JWT token
        response = client.post("/api/v1/auth/refresh", headers=jwt_headers)
        refresh_data = TestResponseHelper.assert_success_response(response)
        
        new_token = refresh_data["access_token"]
        assert new_token != access_token
        
        # 4. Use new token
        new_headers = {
            "Authorization": f"Bearer {new_token}",
            "Content-Type": "application/json"
        }
        
        response = client.get("/api/v1/users/me", headers=new_headers)
        TestResponseHelper.assert_success_response(response)


@pytest.mark.integration
@pytest.mark.slow
class TestSystemMonitoringIntegration:
    """Test system monitoring and admin functionality integration"""
    
    def test_monitoring_and_admin_workflow(self, client: TestClient, db_session: Session):
        """Test system monitoring and administration"""
        from tests.factories import create_user, create_org, create_org_membership
        
        # Set up admin user
        org = create_org(db=db_session, name="Admin Test Org")
        admin = create_user(db=db_session, email="admin@system.com")
        create_org_membership(db=db_session, user=admin, org=org)
        
        headers = TestAuthHelper.get_auth_headers(admin, org)
        
        # 1. Check system health
        response = client.get("/api/v1/monitoring/health", headers=headers)
        health = TestResponseHelper.assert_success_response(response)
        assert "status" in health
        assert "uptime" in health
        
        # 2. Get system overview
        response = client.get("/api/v1/monitoring/overview", headers=headers)
        overview = TestResponseHelper.assert_success_response(response)
        assert "users" in overview
        assert "flows" in overview
        assert "data_sources" in overview
        
        # 3. Check performance metrics
        response = client.get("/api/v1/monitoring/performance", headers=headers)
        performance = TestResponseHelper.assert_success_response(response)
        assert "response_times" in performance
        assert "throughput" in performance
        
        # 4. Admin configuration access
        response = client.get("/api/v1/admin/config", headers=headers)
        config = TestResponseHelper.assert_success_response(response)
        assert "features" in config
        assert "limits" in config