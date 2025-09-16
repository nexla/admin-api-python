"""
Data Sources API Tests.
Migrated from Rails data_sources_controller_spec.rb - comprehensive data source management tests.

This test suite covers:
- Data source CRUD operations
- Validation (name length, connector types)
- Data source activation/deactivation
- Search functionality
- Ownership and authorization
- Status management
- Connector type validation
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.data_source import DataSource
from tests.factories import (
    create_user, create_org, create_org_membership, 
    create_data_source, create_data_set
)
from tests.utils import (
    TestAuthHelper, TestResponseHelper, TestDataHelper,
    status_ok, status_created, status_bad_request, status_forbidden, 
    status_not_found, status_unauthorized
)


@pytest.mark.api
@pytest.mark.data_sources
class TestDataSourcesAPI:
    """Test data sources API endpoints"""
    
    def setup_method(self):
        """Set up test data for each test"""
        pass
    
    def test_create_data_source_success(self, client: TestClient, db_session: Session):
        """Test successful data source creation"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create data source
        source_data = {
            "name": "TEST DATA SOURCE",
            "description": "TEST DATA SOURCE description", 
            "source_type": "s3"
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=source_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "TEST DATA SOURCE"
        assert data["description"] == "TEST DATA SOURCE description"
        assert data["source_type"] == "s3"
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
        assert data["status"] == "DRAFT"  # Default status
    
    def test_create_data_source_active(self, client: TestClient, db_session: Session):
        """Test creating data source in active state"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        source_data = {
            "name": "ACTIVE DATA SOURCE",
            "description": "Active data source description",
            "source_type": "s3",
            "activate_now": True
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=source_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["status"] == "ACTIVE"
    
    def test_create_data_source_name_too_long(self, client: TestClient, db_session: Session):
        """Test data source creation fails with name too long"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Name too long (over 255 characters)
        long_name = "TEST DATA SOURCE" * 100
        source_data = {
            "name": long_name,
            "description": "Valid description",
            "source_type": "s3"
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=source_data)
        error_data = TestResponseHelper.assert_error_response(response, status_bad_request())
        
        assert "maximum string length of 255" in error_data["message"]
    
    def test_create_data_source_description_too_long(self, client: TestClient, db_session: Session):
        """Test data source creation fails with description too long"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Description too long
        long_description = "TEST DATA SOURCE description" * 100
        source_data = {
            "name": "Valid name",
            "description": long_description,
            "source_type": "s3"
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=source_data)
        error_data = TestResponseHelper.assert_error_response(response, status_bad_request())
        
        assert "maximum string length of 255" in error_data["message"]
    
    def test_create_data_source_invalid_connector_type(self, client: TestClient, db_session: Session):
        """Test data source creation fails with invalid connector type"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        source_data = {
            "name": "TEST DATA SOURCE",
            "description": "TEST DATA SOURCE description",
            "source_type": "invalid_type"
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=source_data)
        error_data = TestResponseHelper.assert_error_response(response, status_bad_request())
        
        assert "Unsupported source connector type" in error_data["message"]
    
    def test_get_data_sources_list(self, client: TestClient, db_session: Session):
        """Test getting list of data sources"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        # Create test data sources
        ds1 = create_data_source(db=db_session, owner=user, org=org, name="Source 1")
        ds2 = create_data_source(db=db_session, owner=user, org=org, name="Source 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/data_sources", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert isinstance(data, list)
        assert len(data) >= 2
        
        # Verify data structure
        source_ids = [ds["id"] for ds in data]
        assert ds1.id in source_ids
        assert ds2.id in source_ids
        
        # Check response structure
        first_source = data[0]
        assert "id" in first_source
        assert "name" in first_source
        assert "description" in first_source
        assert "source_type" in first_source
        assert "status" in first_source
        assert "owner" in first_source
        assert "org" in first_source
        assert "created_at" in first_source
        assert "updated_at" in first_source
    
    def test_get_data_source_by_id(self, client: TestClient, db_session: Session):
        """Test getting specific data source by ID"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(
            db=db_session, 
            owner=user, 
            org=org, 
            name="Target Source",
            description="Target description"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get(f"/api/v1/data_sources/{data_source.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["id"] == data_source.id
        assert data["name"] == "Target Source"
        assert data["description"] == "Target description"
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
    
    def test_get_nonexistent_data_source(self, client: TestClient, db_session: Session):
        """Test getting non-existent data source returns 404"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/data_sources/99999", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_update_data_source(self, client: TestClient, db_session: Session):
        """Test updating a data source"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(
            db=db_session, 
            owner=user, 
            org=org, 
            name="Original Name",
            description="Original description"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        update_data = {
            "name": "Updated Name",
            "description": "Updated description"
        }
        
        response = client.put(f"/api/v1/data_sources/{data_source.id}", headers=headers, json=update_data)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["name"] == "Updated Name"
        assert data["description"] == "Updated description"
        assert data["id"] == data_source.id
    
    def test_delete_data_source(self, client: TestClient, db_session: Session):
        """Test deleting a data source"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="To Delete")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.delete(f"/api/v1/data_sources/{data_source.id}", headers=headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify it's gone
        response = client.get(f"/api/v1/data_sources/{data_source.id}", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_activate_data_source(self, client: TestClient, db_session: Session):
        """Test activating a data source"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(
            db=db_session, 
            owner=user, 
            org=org, 
            name="To Activate",
            status="DRAFT"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.post(f"/api/v1/data_sources/{data_source.id}/activate", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["status"] == "ACTIVE"
    
    def test_deactivate_data_source(self, client: TestClient, db_session: Session):
        """Test deactivating a data source"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(
            db=db_session, 
            owner=user, 
            org=org, 
            name="To Deactivate",
            status="ACTIVE"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.post(f"/api/v1/data_sources/{data_source.id}/deactivate", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["status"] == "DEACTIVATED"
    
    def test_data_source_search_no_filters(self, client: TestClient, db_session: Session):
        """Test data source search with no filters"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        # Create test data sources
        create_data_source(db=db_session, owner=user, org=org, name="Search Test 1")
        create_data_source(db=db_session, owner=user, org=org, name="Search Test 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Search with empty filters
        search_data = {"filters": []}
        
        response = client.post("/api/v1/data_sources/search", headers=headers, json=search_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Search with no filters at all
        empty_search = {}
        
        response = client.post("/api/v1/data_sources/search", headers=headers, json=empty_search)
        TestResponseHelper.assert_success_response(response, status_ok())
    
    def test_data_source_search_with_name_filter(self, client: TestClient, db_session: Session):
        """Test data source search with name filter"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        # Create test data sources with specific names
        target_source = create_data_source(
            db=db_session, 
            owner=user, 
            org=org, 
            name="UNIQUE_SEARCH_TARGET"
        )
        create_data_source(db=db_session, owner=user, org=org, name="OTHER_SOURCE")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        search_data = {
            "filters": [
                {
                    "field": "name",
                    "operator": "contains",
                    "value": "UNIQUE_SEARCH"
                }
            ]
        }
        
        response = client.post("/api/v1/data_sources/search", headers=headers, json=search_data)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert len(data) >= 1
        assert any(ds["id"] == target_source.id for ds in data)
        assert any(ds["name"] == "UNIQUE_SEARCH_TARGET" for ds in data)
    
    def test_data_source_search_with_status_filter(self, client: TestClient, db_session: Session):
        """Test data source search with status filter"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        # Create data sources with different statuses
        active_source = create_data_source(
            db=db_session, 
            owner=user, 
            org=org, 
            name="Active Source",
            status="ACTIVE"
        )
        create_data_source(
            db=db_session, 
            owner=user, 
            org=org, 
            name="Draft Source",
            status="DRAFT"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        search_data = {
            "filters": [
                {
                    "field": "status",
                    "operator": "equals",
                    "value": "ACTIVE"
                }
            ]
        }
        
        response = client.post("/api/v1/data_sources/search", headers=headers, json=search_data)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # All returned sources should be ACTIVE
        assert all(ds["status"] == "ACTIVE" for ds in data)
        assert any(ds["id"] == active_source.id for ds in data)
    
    def test_data_source_access_control(self, client: TestClient, db_session: Session):
        """Test that users can only access their org's data sources"""
        # Set up two separate orgs with users
        user1 = create_user(db=db_session, email="user1@org1.com")
        org1 = create_org(db=db_session, name="Org 1", owner=user1)
        create_org_membership(db=db_session, user=user1, org=org1)
        
        user2 = create_user(db=db_session, email="user2@org2.com")
        org2 = create_org(db=db_session, name="Org 2", owner=user2)
        create_org_membership(db=db_session, user=user2, org=org2)
        
        # Create data sources in each org
        ds1 = create_data_source(db=db_session, owner=user1, org=org1, name="Org 1 Source")
        ds2 = create_data_source(db=db_session, owner=user2, org=org2, name="Org 2 Source")
        
        # User 1 should only see org 1's data sources
        headers1 = TestAuthHelper.get_auth_headers(user1, org1)
        response = client.get("/api/v1/data_sources", headers=headers1)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        source_ids = [ds["id"] for ds in data]
        assert ds1.id in source_ids
        assert ds2.id not in source_ids
        
        # User 2 should only see org 2's data sources
        headers2 = TestAuthHelper.get_auth_headers(user2, org2)
        response = client.get("/api/v1/data_sources", headers=headers2)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        source_ids = [ds["id"] for ds in data]
        assert ds2.id in source_ids
        assert ds1.id not in source_ids
    
    def test_data_source_ownership_validation(self, client: TestClient, db_session: Session):
        """Test that only owners can modify data sources"""
        owner = create_user(db=db_session, email="owner@test.com")
        other_user = create_user(db=db_session, email="other@test.com")
        org = create_org(db=db_session, name="Test Org", owner=owner)
        
        create_org_membership(db=db_session, user=owner, org=org)
        create_org_membership(db=db_session, user=other_user, org=org)
        
        # Create data source owned by owner
        data_source = create_data_source(db=db_session, owner=owner, org=org, name="Owner's Source")
        
        # Other user tries to modify it
        other_headers = TestAuthHelper.get_auth_headers(other_user, org)
        update_data = {"name": "Hacked Name"}
        
        response = client.put(f"/api/v1/data_sources/{data_source.id}", headers=other_headers, json=update_data)
        TestResponseHelper.assert_forbidden(response)
        
        # Other user tries to delete it
        response = client.delete(f"/api/v1/data_sources/{data_source.id}", headers=other_headers)
        TestResponseHelper.assert_forbidden(response)
    
    def test_unauthorized_access_denied(self, client: TestClient):
        """Test that unauthenticated requests are denied"""
        # Try to access data sources without authentication
        response = client.get("/api/v1/data_sources")
        TestResponseHelper.assert_unauthorized(response)
        
        # Try to create data source without authentication
        source_data = {
            "name": "Unauthorized Source",
            "source_type": "s3"
        }
        
        response = client.post("/api/v1/data_sources", json=source_data)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_data_source_with_custom_connection_config(self, client: TestClient, db_session: Session):
        """Test creating data source with custom connection configuration"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        source_data = {
            "name": "S3 Source with Config",
            "description": "S3 data source with configuration",
            "source_type": "s3",
            "connection_config": {
                "bucket_name": "test-bucket",
                "region": "us-east-1",
                "access_key_id": "test-access-key"
            }
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=source_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "S3 Source with Config"
        assert data["source_type"] == "s3"
        assert data["connection_config"]["bucket_name"] == "test-bucket"
        assert data["connection_config"]["region"] == "us-east-1"
        # Sensitive data should be masked or excluded
        assert "access_key_id" not in str(data["connection_config"]) or "***" in str(data["connection_config"])
    
    def test_data_source_ingestion_mode_options(self, client: TestClient, db_session: Session):
        """Test data source creation with different ingestion modes"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Test batch ingestion mode
        batch_source_data = {
            "name": "Batch Source",
            "description": "Batch ingestion data source",
            "source_type": "s3",
            "ingestion_mode": "BATCH"
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=batch_source_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        assert data["ingestion_mode"] == "BATCH"
        
        # Test streaming ingestion mode
        streaming_source_data = {
            "name": "Streaming Source",
            "description": "Streaming ingestion data source",
            "source_type": "kafka",
            "ingestion_mode": "STREAMING"
        }
        
        response = client.post("/api/v1/data_sources", headers=headers, json=streaming_source_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        assert data["ingestion_mode"] == "STREAMING"
    
    def test_data_source_pagination(self, client: TestClient, db_session: Session):
        """Test data source list pagination"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        # Create multiple data sources
        sources = []
        for i in range(15):
            source = create_data_source(
                db=db_session, 
                owner=user, 
                org=org, 
                name=f"Source {i:02d}"
            )
            sources.append(source)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Test first page
        response = client.get("/api/v1/data_sources?limit=10&offset=0", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert len(data) == 10
        
        # Test second page
        response = client.get("/api/v1/data_sources?limit=10&offset=10", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert len(data) >= 5  # At least the remaining sources
    
    def test_data_source_filtering_by_owner(self, client: TestClient, db_session: Session):
        """Test filtering data sources by owner"""
        user1 = create_user(db=db_session, email="user1@test.com")
        user2 = create_user(db=db_session, email="user2@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user1)
        
        create_org_membership(db=db_session, user=user1, org=org)
        create_org_membership(db=db_session, user=user2, org=org)
        
        # Create data sources for both users
        ds1 = create_data_source(db=db_session, owner=user1, org=org, name="User 1 Source")
        ds2 = create_data_source(db=db_session, owner=user2, org=org, name="User 2 Source")
        
        headers = TestAuthHelper.get_auth_headers(user1, org)
        
        # Filter by owner
        response = client.get(f"/api/v1/data_sources?owner_id={user1.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Should only see user1's sources
        source_ids = [ds["id"] for ds in data]
        assert ds1.id in source_ids
        assert ds2.id not in source_ids
        
        # All sources should belong to user1
        assert all(ds["owner"]["id"] == user1.id for ds in data)