"""
Data Sets API Tests.
Migrated from Rails data_sets_spec.rb - comprehensive data set management tests.

This test suite covers:
- Data set CRUD operations  
- Parent-child data set relationships
- Data set summaries and statistics
- Schema management
- Data source associations
- Ownership and authorization
- Status management
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.data_source import DataSource
from app.models.data_set import DataSet
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
@pytest.mark.data_sets
class TestDataSetsAPI:
    """Test data sets API endpoints"""
    
    def setup_method(self):
        """Set up test data for each test"""
        pass
    
    def test_create_data_set_success(self, client: TestClient, db_session: Session):
        """Test successful data set creation with valid data_source_id"""
        # Set up test data
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create data set
        dataset_data = {
            "data_source_id": data_source.id,
            "source_schema": {
                "$schema-id": "012"
            }
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=dataset_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["data_source_id"] == data_source.id
        assert data["source_schema"]["$schema-id"] == "012"
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
        assert data["status"] == "ACTIVE"
    
    def test_create_data_set_with_name(self, client: TestClient, db_session: Session):
        """Test creating data set with custom name"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        dataset_data = {
            "name": "Custom Data Set Name",
            "description": "Custom description",
            "data_source_id": data_source.id,
            "source_schema": {
                "$schema-id": "012"
            }
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=dataset_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "Custom Data Set Name"
        assert data["description"] == "Custom description"
    
    def test_create_child_data_set(self, client: TestClient, db_session: Session):
        """Test creating child data set with parent_data_set_id"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        parent_dataset = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Parent Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        child_data = {
            "name": "Child Data Set",
            "description": "Child of parent data set",
            "parent_data_set_id": parent_dataset.id
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=child_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["name"] == "Child Data Set"
        assert data["parent_data_set_id"] == parent_dataset.id
        assert data["owner"]["id"] == user.id
    
    def test_get_data_sets_list(self, client: TestClient, db_session: Session):
        """Test getting list of data sets"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        # Create test data sets
        ds1 = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Set 1")
        ds2 = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Set 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/data_sets", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert isinstance(data, list)
        assert len(data) >= 2
        
        # Verify data structure
        dataset_ids = [ds["id"] for ds in data]
        assert ds1.id in dataset_ids
        assert ds2.id in dataset_ids
        
        # Check response structure
        first_dataset = data[0]
        assert "id" in first_dataset
        assert "name" in first_dataset
        assert "description" in first_dataset
        assert "status" in first_dataset
        assert "owner" in first_dataset
        assert "org" in first_dataset
        assert "data_source_id" in first_dataset
        assert "created_at" in first_dataset
        assert "updated_at" in first_dataset
    
    def test_get_data_sets_by_data_source(self, client: TestClient, db_session: Session):
        """Test filtering data sets by data source"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        # Create two data sources
        source1 = create_data_source(db=db_session, owner=user, org=org, name="Source 1")
        source2 = create_data_source(db=db_session, owner=user, org=org, name="Source 2")
        
        # Create data sets for each source
        ds1 = create_data_set(db=db_session, owner=user, org=org, data_source=source1, name="Set from Source 1")
        ds2 = create_data_set(db=db_session, owner=user, org=org, data_source=source2, name="Set from Source 2")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Filter by data source 1
        response = client.get(f"/api/v1/data_sets?data_source_id={source1.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Should only see data sets from source 1
        dataset_ids = [ds["id"] for ds in data]
        assert ds1.id in dataset_ids
        assert ds2.id not in dataset_ids
        
        # All returned data sets should belong to source 1
        assert all(ds["data_source_id"] == source1.id for ds in data)
    
    def test_get_data_set_by_id(self, client: TestClient, db_session: Session):
        """Test getting specific data set by ID"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(
            db=db_session, 
            owner=user, 
            org=org, 
            data_source=data_source,
            name="Target Set",
            description="Target description"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get(f"/api/v1/data_sets/{data_set.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["id"] == data_set.id
        assert data["name"] == "Target Set"
        assert data["description"] == "Target description"
        assert data["owner"]["id"] == user.id
        assert data["org"]["id"] == org.id
    
    def test_get_data_set_with_summary(self, client: TestClient, db_session: Session):
        """Test getting data set with summary information"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        parent_dataset = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Parent Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Get data set without summary initially
        response = client.get(f"/api/v1/data_sets/{parent_dataset.id}?include_summary=1", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Should have summary with 0 child data sets
        assert "summary" in data
        assert data["summary"]["data_sets"]["total"] == 0
        
        # Create a child data set
        child_data = {
            "name": "Test summary child data set",
            "parent_data_set_id": parent_dataset.id
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=child_data)
        TestResponseHelper.assert_success_response(response, status_created())
        
        # Get parent with summary again
        response = client.get(f"/api/v1/data_sets/{parent_dataset.id}?include_summary=1", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Summary should now show 1 child data set
        assert data["summary"]["data_sets"]["total"] == 1
    
    def test_get_data_set_summary_endpoint(self, client: TestClient, db_session: Session):
        """Test dedicated summary endpoint for data set"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        parent_dataset = create_data_set(db=db_session, owner=user, org=org, data_source=data_source, name="Parent Set")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create child data set
        child_data = {
            "name": "Child for summary test",
            "parent_data_set_id": parent_dataset.id
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=child_data)
        TestResponseHelper.assert_success_response(response, status_created())
        
        # Get summary via dedicated endpoint
        response = client.get(f"/api/v1/data_sets/{parent_dataset.id}/summary", headers=headers)
        summary = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert summary["id"] == parent_dataset.id
        assert summary["data_sets"]["total"] == 1
    
    def test_update_data_set(self, client: TestClient, db_session: Session):
        """Test updating a data set"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(
            db=db_session, 
            owner=user, 
            org=org, 
            data_source=data_source,
            name="Original Name",
            description="Original description"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        update_data = {
            "name": "Updated Name",
            "description": "Updated description",
            "output_schema_locked": True
        }
        
        response = client.put(f"/api/v1/data_sets/{data_set.id}", headers=headers, json=update_data)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert data["name"] == "Updated Name"
        assert data["description"] == "Updated description"
        assert data["output_schema_locked"] is True
        assert data["id"] == data_set.id
    
    def test_delete_data_set(self, client: TestClient, db_session: Session):
        """Test deleting a data set"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(
            db=db_session, 
            owner=user, 
            org=org, 
            data_source=data_source, 
            name="To Delete"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.delete(f"/api/v1/data_sets/{data_set.id}", headers=headers)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify it's gone
        response = client.get(f"/api/v1/data_sets/{data_set.id}", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_get_nonexistent_data_set(self, client: TestClient, db_session: Session):
        """Test getting non-existent data set returns 404"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        response = client.get("/api/v1/data_sets/99999", headers=headers)
        TestResponseHelper.assert_not_found(response)
    
    def test_data_set_schema_management(self, client: TestClient, db_session: Session):
        """Test data set schema operations"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create data set with complex schema
        dataset_data = {
            "name": "Schema Test Set",
            "data_source_id": data_source.id,
            "source_schema": {
                "$schema-id": "complex_schema_001",
                "fields": [
                    {
                        "name": "id",
                        "type": "integer",
                        "required": True
                    },
                    {
                        "name": "name", 
                        "type": "string",
                        "required": True,
                        "max_length": 255
                    },
                    {
                        "name": "created_at",
                        "type": "timestamp",
                        "required": False
                    }
                ]
            },
            "output_schema": {
                "$schema-id": "output_schema_001",
                "fields": [
                    {
                        "name": "processed_id",
                        "type": "integer"
                    },
                    {
                        "name": "processed_name",
                        "type": "string"
                    }
                ]
            }
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=dataset_data)
        data = TestResponseHelper.assert_success_response(response, status_created())
        
        assert data["source_schema"]["$schema-id"] == "complex_schema_001"
        assert len(data["source_schema"]["fields"]) == 3
        assert data["output_schema"]["$schema-id"] == "output_schema_001"
        assert len(data["output_schema"]["fields"]) == 2
    
    def test_data_set_access_control(self, client: TestClient, db_session: Session):
        """Test that users can only access their org's data sets"""
        # Set up two separate orgs with users
        user1 = create_user(db=db_session, email="user1@org1.com")
        org1 = create_org(db=db_session, name="Org 1", owner=user1)
        create_org_membership(db=db_session, user=user1, org=org1)
        
        user2 = create_user(db=db_session, email="user2@org2.com")
        org2 = create_org(db=db_session, name="Org 2", owner=user2)
        create_org_membership(db=db_session, user=user2, org=org2)
        
        # Create data sources and sets in each org
        ds1 = create_data_source(db=db_session, owner=user1, org=org1, name="Org 1 Source")
        ds2 = create_data_source(db=db_session, owner=user2, org=org2, name="Org 2 Source")
        
        dset1 = create_data_set(db=db_session, owner=user1, org=org1, data_source=ds1, name="Org 1 Set")
        dset2 = create_data_set(db=db_session, owner=user2, org=org2, data_source=ds2, name="Org 2 Set")
        
        # User 1 should only see org 1's data sets
        headers1 = TestAuthHelper.get_auth_headers(user1, org1)
        response = client.get("/api/v1/data_sets", headers=headers1)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        dataset_ids = [ds["id"] for ds in data]
        assert dset1.id in dataset_ids
        assert dset2.id not in dataset_ids
        
        # User 2 should only see org 2's data sets
        headers2 = TestAuthHelper.get_auth_headers(user2, org2)
        response = client.get("/api/v1/data_sets", headers=headers2)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        dataset_ids = [ds["id"] for ds in data]
        assert dset2.id in dataset_ids
        assert dset1.id not in dataset_ids
    
    def test_data_set_ownership_validation(self, client: TestClient, db_session: Session):
        """Test that only owners can modify data sets"""
        owner = create_user(db=db_session, email="owner@test.com")
        other_user = create_user(db=db_session, email="other@test.com")
        org = create_org(db=db_session, name="Test Org", owner=owner)
        
        create_org_membership(db=db_session, user=owner, org=org)
        create_org_membership(db=db_session, user=other_user, org=org)
        
        # Create data source and set owned by owner
        data_source = create_data_source(db=db_session, owner=owner, org=org, name="Owner's Source")
        data_set = create_data_set(
            db=db_session, 
            owner=owner, 
            org=org, 
            data_source=data_source, 
            name="Owner's Set"
        )
        
        # Other user tries to modify it
        other_headers = TestAuthHelper.get_auth_headers(other_user, org)
        update_data = {"name": "Hacked Name"}
        
        response = client.put(f"/api/v1/data_sets/{data_set.id}", headers=other_headers, json=update_data)
        TestResponseHelper.assert_forbidden(response)
        
        # Other user tries to delete it
        response = client.delete(f"/api/v1/data_sets/{data_set.id}", headers=other_headers)
        TestResponseHelper.assert_forbidden(response)
    
    def test_unauthorized_access_denied(self, client: TestClient):
        """Test that unauthenticated requests are denied"""
        # Try to access data sets without authentication
        response = client.get("/api/v1/data_sets")
        TestResponseHelper.assert_unauthorized(response)
        
        # Try to create data set without authentication
        dataset_data = {
            "name": "Unauthorized Set",
            "data_source_id": 1
        }
        
        response = client.post("/api/v1/data_sets", json=dataset_data)
        TestResponseHelper.assert_unauthorized(response)
    
    def test_data_set_parent_child_hierarchy(self, client: TestClient, db_session: Session):
        """Test complex parent-child data set hierarchies"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Create parent data set
        parent_data = {
            "name": "Parent Data Set",
            "data_source_id": data_source.id,
            "source_schema": {"$schema-id": "parent_schema"}
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=parent_data)
        parent = TestResponseHelper.assert_success_response(response, status_created())
        parent_id = parent["id"]
        
        # Create multiple child data sets
        child_names = ["Child 1", "Child 2", "Child 3"]
        child_ids = []
        
        for child_name in child_names:
            child_data = {
                "name": child_name,
                "parent_data_set_id": parent_id
            }
            
            response = client.post("/api/v1/data_sets", headers=headers, json=child_data)
            child = TestResponseHelper.assert_success_response(response, status_created())
            child_ids.append(child["id"])
        
        # Create grandchild data set
        grandchild_data = {
            "name": "Grandchild Data Set",
            "parent_data_set_id": child_ids[0]
        }
        
        response = client.post("/api/v1/data_sets", headers=headers, json=grandchild_data)
        grandchild = TestResponseHelper.assert_success_response(response, status_created())
        
        # Verify parent summary includes all children
        response = client.get(f"/api/v1/data_sets/{parent_id}/summary", headers=headers)
        summary = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert summary["data_sets"]["total"] >= 3  # At least 3 children
        
        # Verify child summary includes grandchild
        response = client.get(f"/api/v1/data_sets/{child_ids[0]}/summary", headers=headers)
        child_summary = TestResponseHelper.assert_success_response(response, status_ok())
        
        assert child_summary["data_sets"]["total"] >= 1  # At least 1 grandchild
    
    def test_data_set_filtering_and_pagination(self, client: TestClient, db_session: Session):
        """Test data set filtering and pagination"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        # Create multiple data sets with different statuses
        datasets = []
        for i in range(15):
            status = "ACTIVE" if i % 2 == 0 else "INACTIVE"
            ds = create_data_set(
                db=db_session, 
                owner=user, 
                org=org, 
                data_source=data_source,
                name=f"Dataset {i:02d}",
                status=status
            )
            datasets.append(ds)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Test pagination
        response = client.get("/api/v1/data_sets?limit=10&offset=0", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        assert len(data) == 10
        
        # Test filtering by status
        response = client.get("/api/v1/data_sets?status=ACTIVE", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # All returned data sets should be ACTIVE
        assert all(ds["status"] == "ACTIVE" for ds in data)
        
        # Test filtering by owner
        response = client.get(f"/api/v1/data_sets?owner_id={user.id}", headers=headers)
        data = TestResponseHelper.assert_success_response(response, status_ok())
        
        # All returned data sets should belong to the user
        assert all(ds["owner"]["id"] == user.id for ds in data)
    
    def test_data_set_batch_operations(self, client: TestClient, db_session: Session):
        """Test batch operations on data sets"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        
        # Create multiple data sets
        dataset_ids = []
        for i in range(5):
            ds = create_data_set(
                db=db_session, 
                owner=user, 
                org=org, 
                data_source=data_source,
                name=f"Batch Dataset {i}",
                status="ACTIVE"
            )
            dataset_ids.append(ds.id)
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Batch update status
        batch_data = {
            "data_set_ids": dataset_ids[:3],  # Update first 3
            "updates": {
                "status": "INACTIVE",
                "description": "Batch updated description"
            }
        }
        
        response = client.put("/api/v1/data_sets/batch", headers=headers, json=batch_data)
        TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify updates were applied
        for dataset_id in dataset_ids[:3]:
            response = client.get(f"/api/v1/data_sets/{dataset_id}", headers=headers)
            data = TestResponseHelper.assert_success_response(response, status_ok())
            
            assert data["status"] == "INACTIVE"
            assert data["description"] == "Batch updated description"
        
        # Verify non-updated data sets remain unchanged
        for dataset_id in dataset_ids[3:]:
            response = client.get(f"/api/v1/data_sets/{dataset_id}", headers=headers)
            data = TestResponseHelper.assert_success_response(response, status_ok())
            
            assert data["status"] == "ACTIVE"  # Unchanged
    
    def test_data_set_metrics_and_statistics(self, client: TestClient, db_session: Session):
        """Test data set metrics and statistics endpoints"""
        user = create_user(db=db_session, email="user@test.com")
        org = create_org(db=db_session, name="Test Org", owner=user)
        create_org_membership(db=db_session, user=user, org=org)
        
        data_source = create_data_source(db=db_session, owner=user, org=org, name="Test Source")
        data_set = create_data_set(
            db=db_session, 
            owner=user, 
            org=org, 
            data_source=data_source, 
            name="Metrics Test Set"
        )
        
        headers = TestAuthHelper.get_auth_headers(user, org)
        
        # Get data set metrics
        response = client.get(f"/api/v1/data_sets/{data_set.id}/metrics", headers=headers)
        metrics = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify metrics structure
        assert "records_processed" in metrics
        assert "records_success" in metrics
        assert "records_failed" in metrics
        assert "bytes_processed" in metrics
        assert "last_run_at" in metrics
        
        # Get data set statistics
        response = client.get(f"/api/v1/data_sets/{data_set.id}/stats", headers=headers)
        stats = TestResponseHelper.assert_success_response(response, status_ok())
        
        # Verify stats structure
        assert "total_runs" in stats
        assert "successful_runs" in stats
        assert "failed_runs" in stats
        assert "average_processing_time" in stats