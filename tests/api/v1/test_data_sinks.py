"""
Data Sinks API Tests.
Migrated from Rails data_sink_spec.rb - comprehensive data sink management tests.

This test suite covers:
- Data sink CRUD operations
- Connection type validation
- Data set associations
- Ownership and authorization  
- Status management
- Configuration validation
- Data sink filters and transformations
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.data_source import DataSource
from app.models.data_set import DataSet
from app.models.data_sink import DataSink
from tests.factories import (
    create_user, create_org, create_org_membership, 
    create_data_source, create_data_set, create_data_sink
)
from tests.utils import (
    TestAuthHelper, TestResponseHelper, TestDataHelper,
    status_ok, status_created, status_bad_request, status_forbidden, 
    status_not_found, status_unauthorized
)


@pytest.mark.api
@pytest.mark.data_sinks
class TestDataSinksAPI:
    """Test data sinks API endpoints"""
    
    def setup_method(self):
        """Set up test data for each test"""
        pass
    
    def test_create_data_sink_success(self, client: TestClient, db_session: Session):
        """Test successful data sink creation"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create data sink
        sink_data = {
            "name": "TEST DATA SINK",
            "description": "TEST DATA SINK description",
            "connection_type": "s3",
            "data_set_id": data_set.id
        }
        
        response = client.post("/api/v1/data_sinks", headers=headers, json=sink_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "TEST DATA SINK"
        assert data["description"] == "TEST DATA SINK description"
        assert data["connection_type"] == "s3"
        assert data["data_set_id"] == data_set.id
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
        assert data["status"] == "ACTIVE"
    
    def test_create_data_sink_with_config(self, client: TestClient, db_session: Session):
        """Test creating data sink with connection configuration"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        sink_data = {
            "name": "S3 Sink with Config",
            "description": "S3 data sink with configuration",
            "connection_type": "s3",
            "data_set_id": data_set.id,
            "connection_config": {
                "bucket_name": "test-output-bucket",
                "region": "us-west-2",
                "path_prefix": "processed-data/",
                "file_format": "parquet"
            }
        }
        
        response = client.post("/api/v1/data_sinks", headers=headers, json=sink_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "S3 Sink with Config"
        assert data["connection_type"] == "s3"
        assert data["connection_config"]["bucket_name"] == "test-output-bucket"
        assert data["connection_config"]["region"] == "us-west-2"
        assert data["connection_config"]["path_prefix"] == "processed-data/"
        assert data["connection_config"]["file_format"] == "parquet"
    
    def test_create_data_sink_invalid_connection_type(self, client: TestClient, db_session: Session):
        """Test data sink creation fails with invalid connection type"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        sink_data = {
            "name": "Invalid Sink",
            "connection_type": "invalid_type",
            "data_set_id": data_set.id
        }
        
        response = client.post("/api/v1/data_sinks", headers=headers, json=sink_data)
        error_data = TestResponseHelper.assert_error_response(response, status_bad_request())
        
        assert "Unsupported sink connector type" in error_data["message"]
    
    def test_create_data_sink_missing_data_set(self, client: TestClient, db_session: Session):
        """Test data sink creation fails without data set"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        sink_data = {
            "name": "Missing Data Set Sink",
            "connection_type": "s3"
            # Missing data_set_id
        }
        
        response = client.post("/api/v1/data_sinks", headers=headers, json=sink_data)
        TestResponseHelper.assert_error_response(response, status_bad_request())
    
    def test_get_data_sinks_list(self, client: TestClient, db_session: Session):
        """Test getting list of data sinks"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        # Create test data sinks
        sink1 = create_data_sink(db=db_session, owner=user, org=org, data_set=data_set, name="Sink 1")
        sink2 = create_data_sink(db=db_session, owner=user, org=org, data_set=data_set, name="Sink 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/data_sinks", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert isinstance(data, list)
        assert len(data) >= 2
        
        # Verify data structure
        sink_ids = [ds["id"] for ds in data]
        assert sink1.id in sink_ids
        assert sink2.id in sink_ids
        
        # Check response structure
        first_sink = data[0]
        assert "id" in first_sink
        assert "name" in first_sink
        assert "description" in first_sink
        assert "connection_type" in first_sink
        assert "status" in first_sink
        assert "owner" in first_sink
        assert "org" in first_sink
        assert "data_set_id" in first_sink
        assert "created_at" in first_sink
        assert "updated_at" in first_sink
    
    def test_get_data_sink_by_id(self, client: TestClient, db_session: Session):
        """Test getting specific data sink by ID"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        data_sink = create_data_sink(
            db=db_session, 
            owner=user, 
            org=org, 
            data_set=data_set,
            name="Target Sink",
            description="Target description"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get(f"/api/v1/data_sinks/{data_sink.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["id"] == data_sink.id
        assert data["name"] == "Target Sink"
        assert data["description"] == "Target description"
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
    
    def test_get_nonexistent_data_sink(self, client: TestClient, db_session: Session):
        """Test getting non-existent data sink returns 404"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/data_sinks/99999", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_update_data_sink(self, client: TestClient, db_session: Session):
        """Test updating a data sink"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        data_sink = create_data_sink(
            db=db_session, 
            owner=user, 
            org=org, 
            data_set=data_set,
            name="Original Name",
            description="Original description"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        update_data = {
            "name": "Updated Name",
            "description": "Updated description",
            "connection_config": {
                "bucket_name": "updated-bucket",
                "region": "eu-west-1"
            }
        }
        
        response = client.put(f"/api/v1/data_sinks/{data_sink.id}", headers=headers, json=update_data)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["name"] == "Updated Name"
        assert data["description"] == "Updated description"
        assert data["connection_config"]["bucket_name"] == "updated-bucket"
        assert data["connection_config"]["region"] == "eu-west-1"
        assert data["id"] == data_sink.id
    
    def test_delete_data_sink(self, client: TestClient, db_session: Session):
        """Test deleting a data sink"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        data_sink = create_data_sink(db=db_session, owner=user, org=org, data_set=data_set, name="To Delete")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.delete(f"/api/v1/data_sinks/{data_sink.id}", headers=headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify it's gone
        response = client.get(f"/api/v1/data_sinks/{data_sink.id}", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_activate_data_sink(self, client: TestClient, db_session: Session):
        """Test activating a data sink"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        data_sink = create_data_sink(
            db=db_session, 
            owner=user, 
            org=org, 
            data_set=data_set,
            name="To Activate",
            status="INACTIVE"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.post(f"/api/v1/data_sinks/{data_sink.id}/activate", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["status"] == "ACTIVE"
    
    def test_deactivate_data_sink(self, client: TestClient, db_session: Session):
        """Test deactivating a data sink"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        data_sink = create_data_sink(
            db=db_session, 
            owner=user, 
            org=org, 
            data_set=data_set,
            name="To Deactivate",
            status="ACTIVE"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.post(f"/api/v1/data_sinks/{data_sink.id}/deactivate", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["status"] == "INACTIVE"
    
    def test_data_sink_by_data_set_filter(self, client: TestClient, db_session: Session):
        """Test filtering data sinks by data set"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        # Create two data sets
        data_set1 = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Set 1")
        data_set2 = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Set 2")
        
        # Create data sinks for each set
        sink1 = create_data_sink(db=db_session, owner=user, org=org, data_set=data_set1, name="Sink for Set 1")
        sink2 = create_data_sink(db=db_session, owner=user, org=org, data_set=data_set2, name="Sink for Set 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Filter by data set 1
        response = client.get(f"/api/v1/data_sinks?data_set_id={data_set1.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Should only see sinks for data set 1
        sink_ids = [ds["id"] for ds in data]
        assert sink1.id in sink_ids
        assert sink2.id not in sink_ids
        
        # All returned sinks should belong to data set 1
        assert all(ds["data_set_id"] == data_set1.id for ds in data)
    
    def test_data_sink_connection_types(self, client: TestClient, db_session: Session):
        """Test creating data sinks with different connection types"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Test different connection types
        connection_types = [
            {
                "type": "s3",
                "config": {"bucket_name": "test-bucket", "region": "us-east-1"}
            },
            {
                "type": "redshift", 
                "config": {"cluster": "test-cluster", "database": "test-db", "table": "test-table"}
            },
            {
                "type": "snowflake",
                "config": {"account": "test-account", "database": "test-db", "schema": "test-schema"}
            },
            {
                "type": "bigquery",
                "config": {"project_id": "test-project", "dataset": "test-dataset", "table": "test-table"}
            }
        ]
        
        for conn in connection_types:
            sink_data = {
                "name": f"{conn['type'].upper()} Sink",
                "connection_type": conn["type"],
                "data_set_id": data_set.id,
                "connection_config": conn["config"]
            }
            
            response = client.post("/api/v1/data_sinks", headers=headers, json=sink_data)
            data = TestResponseHelper.assert_success_response(response, status_created())
            
            assert data["connection_type"] == conn["type"]
            assert data["name"] == f"{conn['type'].upper()} Sink"
    
    def test_data_sink_access_control(self, client: TestClient, db_session: Session):
        """Test that users can only access their org's data sinks"""
        # Set up two separate orgs with users
        user1 = create_user(db=db_session, email="user1@org1.com")
        org1 = create_org(db=db_session, name="Org 1", owner=user1)
        create_org_membership(db=db_session, user=user1, org=org1)
        
        user2 = create_user(db=db_session, email="user2@org2.com")
        org2 = create_org(db=db_session, name="Org 2", owner=user2)
        create_org_membership(db=db_session, user=user2, org=org2)
        
        # Create data sources, sets, and sinks in each org
        ds1 = create_data_source(db=db_session, owner=user1, org=org1, name="Org 1 Source")
        dset1 = create_data_set(db=db_session, owner=user1, org=org1, data_source=ds1, name="Org 1 Set")
        dsink1 = create_data_sink(db=db_session, owner=user1, org=org1, data_set=dset1, name="Org 1 Sink")
        
        ds2 = create_data_source(db=db_session, owner=user2, org=org2, name="Org 2 Source")
        dset2 = create_data_set(db=db_session, owner=user2, org=org2, data_source=ds2, name="Org 2 Set")
        dsink2 = create_data_sink(db=db_session, owner=user2, org=org2, data_set=dset2, name="Org 2 Sink")
        
        # User 1 should only see org 1's data sinks
        headers1 = TestAuthHelper.get_auth_headers(user1, org1)
        response = client.get("/api/v1/data_sinks", headers=headers1)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        sink_ids = [ds["id"] for ds in data]
        assert dsink1.id in sink_ids
        assert dsink2.id not in sink_ids
        
        # User 2 should only see org 2's data sinks
        headers2 = TestAuthHelper.get_auth_headers(user2, org2)
        response = client.get("/api/v1/data_sinks", headers=headers2)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        sink_ids = [ds["id"] for ds in data]
        assert dsink2.id in sink_ids
        assert dsink1.id not in sink_ids
    
    def test_data_sink_ownership_validation(self, client: TestClient, db_session: Session):
        """Test that only owners can modify data sinks"""
        owner = create_user(db=db_session, email="owner@test.com")
        other_user = create_user(db=db_session, email="other@test.com")
        org = create_org(db=db_session, name="Test Org", owner=owner)
        
        create_org_membership(db=db_session, user=owner, org=org)
        create_org_membership(db=db_session, user=other_user, org=org)
        
        # Create data source, set, and sink owned by owner
        data_source = create_data_source(db=db_session, owner=owner, org=org, name="Owner's Source")
        data_set = create_data_set(db=db_session, owner=owner, org=org, data_source=data_source, name="Owner's Set")
        data_sink = create_data_sink(db=db_session, owner=owner, org=org, data_set=data_set, name="Owner's Sink")
        
        # Other user tries to modify it
        other_headers = TestAuthHelper.get_auth_headers(other_user, org)
        update_data = {"name": "Hacked Name"}
        
        response = client.put(f"/api/v1/data_sinks/{data_sink.id}", headers=other_headers, json=update_data)
        TestResponseHelper.assert_forbidden(response)
        
        # Other user tries to delete it
        response = client.delete(f"/api/v1/data_sinks/{data_sink.id}", headers=other_headers)
        TestResponseHelper.assert_forbidden(response)
    
    def test_unauthorized_access_denied(self, client: TestClient):
        """Test that unauthenticated requests are denied"""
        # Try to access data sinks without authentication
        response = client.get("/api/v1/data_sinks")
        TestResponseHelper.assert_unauthorized(response)
        
        # Try to create data sink without authentication
        sink_data = {
            "name": "Unauthorized Sink",
            "connection_type": "s3",
            "data_set_id": 1
        }
        
        response = client.post("/api/v1/data_sinks", json=sink_data)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_data_sink_filters_and_transformations(self, client: TestClient, db_session: Session):
        """Test data sink with filters and transformations"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        sink_data = {
            "name": "Filtered Sink",
            "connection_type": "s3",
            "data_set_id": data_set.id,
            "filters": [
                {
                    "field": "status",
                    "operator": "equals",
                    "value": "active"
                },
                {
                    "field": "created_at",
                    "operator": "greater_than",
                    "value": "2023-01-01T00:00:00Z"
                }
            ],
            "transformations": [
                {
                    "type": "column_rename",
                    "from": "old_name",
                    "to": "new_name"
                },
                {
                    "type": "data_type_cast",
                    "field": "price",
                    "to_type": "decimal"
                }
            ]
        }
        
        response = client.post("/api/v1/data_sinks", headers=headers, json=sink_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert len(data["filters"]) == 2
        assert data["filters"][0]["field"] == "status"
        assert data["filters"][0]["operator"] == "equals"
        assert data["filters"][0]["value"] == "active"
        
        assert len(data["transformations"]) == 2
        assert data["transformations"][0]["type"] == "column_rename"
        assert data["transformations"][0]["from"] == "old_name"
        assert data["transformations"][0]["to"] == "new_name"
    
    def test_data_sink_pagination(self, client: TestClient, db_session: Session):
        """Test data sink list pagination"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        # Create multiple data sinks
        sinks = []
        for i in range(15):
            sink = create_data_sink(
                db=db_session, 
                owner=user, 
                org=org, 
                data_set=data_set,
                name=f"Sink {i:02d}"
            )
            sinks.append(sink)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Test first page
        response = client.get("/api/v1/data_sinks?limit=10&offset=0", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert len(data) == 10
        
        # Test second page
        response = client.get("/api/v1/data_sinks?limit=10&offset=10", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert len(data) >= 5  # At least the remaining sinks
    
    def test_data_sink_status_filtering(self, client: TestClient, db_session: Session):
        """Test filtering data sinks by status"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        
        # Create data sinks with different statuses
        active_sink = create_data_sink(
            db=db_session, 
            owner=user, 
            org=org, 
            data_set=data_set,
            name="Active Sink",
            status="ACTIVE"
        )
        inactive_sink = create_data_sink(
            db=db_session, 
            owner=user, 
            org=org, 
            data_set=data_set,
            name="Inactive Sink",
            status="INACTIVE"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Filter by ACTIVE status
        response = client.get("/api/v1/data_sinks?status=ACTIVE", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # All returned sinks should be ACTIVE
        assert all(ds["status"] == "ACTIVE" for ds in data)
        assert any(ds["id"] == active_sink.id for ds in data)
        assert not any(ds["id"] == inactive_sink.id for ds in data)
        
        # Filter by INACTIVE status
        response = client.get("/api/v1/data_sinks?status=INACTIVE", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # All returned sinks should be INACTIVE
        assert all(ds["status"] == "INACTIVE" for ds in data)
        assert any(ds["id"] == inactive_sink.id for ds in data)
        assert not any(ds["id"] == active_sink.id for ds in data)
    
    def test_data_sink_metrics_and_statistics(self, client: TestClient, db_session: Session):
        """Test data sink metrics and statistics endpoints"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Test Set")
        data_sink = create_data_sink(
            db=db_session, 
            owner=user, 
            org=org, 
            data_set=data_set, 
            name="Metrics Test Sink"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Get data sink metrics
        response = client.get(f"/api/v1/data_sinks/{data_sink.id}/metrics", headers=headers)
        metrics = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify metrics structure
        assert "records_written" in metrics
        assert "records_success" in metrics
        assert "records_failed" in metrics
        assert "bytes_written" in metrics
        assert "last_write_at" in metrics
        
        # Get data sink statistics
        response = client.get(f"/api/v1/data_sinks/{data_sink.id}/stats", headers=headers)
        stats = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify stats structure
        assert "total_writes" in stats
        assert "successful_writes" in stats
        assert "failed_writes" in stats
        assert "average_write_time" in stats