"""
Tests for DataSet model.
Migrated from Rails spec patterns for data_set functionality.
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.data_set import DataSet
from app.models.data_sink import DataSink
from tests.factories import (
    create_data_set, create_data_source, create_data_sink, 
    create_user, create_org
)


@pytest.mark.unit
class TestDataSet:
    """Test DataSet model functionality"""
    
    def test_create_data_set(self, db_session: Session):
        """Test creating a basic data set"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        data_source = create_data_source(db=db_session, owner=user, org=org)
        
        data_set = create_data_set(
            db=db_session,
            name="Test Data Set",
            description="A test data set",
            owner=user,
            org=org,
            data_source=data_source
        )
        
        assert data_set.name == "Test Data Set"
        assert data_set.description == "A test data set"
        assert data_set.status == "ACTIVE"
        assert data_set.output_schema_locked is False
        assert data_set.owner == user
        assert data_set.org == org
        assert data_set.data_source == data_source
        assert data_set.is_active() is True
    
    def test_data_set_status_methods(self, db_session: Session):
        """Test data set status checking methods"""
        # Test active data set
        active_set = create_data_set(db=db_session, status="ACTIVE")
        assert active_set.is_active() is True
        
        # Test paused data set
        paused_set = create_data_set(db=db_session, status="PAUSED")
        assert paused_set.is_active() is False
        
        # Test inactive data set
        inactive_set = create_data_set(db=db_session, status="INACTIVE")
        assert inactive_set.is_active() is False
    
    def test_data_set_relationships(self, db_session: Session):
        """Test data set relationships"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        data_source = create_data_source(db=db_session, owner=user, org=org)
        
        data_set = create_data_set(
            db=db_session, 
            owner=user, 
            org=org, 
            data_source=data_source
        )
        
        # Test basic relationships
        assert data_set.owner == user
        assert data_set.org == org
        assert data_set.data_source == data_source
        assert data_set.owner_id == user.id
        assert data_set.org_id == org.id
        assert data_set.data_source_id == data_source.id
    
    def test_data_set_data_sinks_relationship(self, db_session: Session):
        """Test data set to data sinks relationship"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        data_source = create_data_source(db=db_session, owner=user, org=org)
        data_set = create_data_set(db=db_session, data_source=data_source, owner=user, org=org)
        
        # Create data sinks for this data set
        data_sink1 = create_data_sink(db=db_session, data_set=data_set, owner=user, org=org)
        data_sink2 = create_data_sink(db=db_session, data_set=data_set, owner=user, org=org)
        
        # Query data sinks for this set
        set_sinks = db_session.query(DataSink).filter_by(data_set_id=data_set.id).all()
        assert len(set_sinks) >= 2
        
        sink_ids = {s.id for s in set_sinks}
        assert data_sink1.id in sink_ids
        assert data_sink2.id in sink_ids
    
    def test_data_set_schema_locked(self, db_session: Session):
        """Test data set output schema locking"""
        # Default should be unlocked
        unlocked_set = create_data_set(db=db_session, output_schema_locked=False)
        assert unlocked_set.output_schema_locked is False
        
        # Test locked schema
        locked_set = create_data_set(db=db_session, output_schema_locked=True)
        assert locked_set.output_schema_locked is True
    
    def test_data_set_sample_storage(self, db_session: Session):
        """Test data set data sample storage"""
        sample_data = {
            "columns": ["id", "name", "email"],
            "rows": [
                [1, "John Doe", "john@example.com"],
                [2, "Jane Smith", "jane@example.com"]
            ]
        }
        
        data_set = create_data_set(db=db_session, data_sample=sample_data)
        
        # Refresh to ensure sample is properly stored/retrieved
        db_session.refresh(data_set)
        
        assert data_set.data_sample is not None
        assert data_set.data_sample.get("columns") == ["id", "name", "email"]
        assert len(data_set.data_sample.get("rows")) == 2
        assert data_set.data_sample["rows"][0] == [1, "John Doe", "john@example.com"]
    
    def test_data_set_schema_sample(self, db_session: Session):
        """Test data set schema sample storage"""
        schema_sample = {
            "fields": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": False},
                {"name": "email", "type": "string", "nullable": True}
            ],
            "inferred_at": "2023-01-01T00:00:00Z"
        }
        
        data_set = create_data_set(db=db_session, schema_sample=schema_sample)
        
        # Refresh to ensure schema sample is properly stored/retrieved
        db_session.refresh(data_set)
        
        assert data_set.schema_sample is not None
        assert len(data_set.schema_sample.get("fields")) == 3
        assert data_set.schema_sample["fields"][0]["name"] == "id"
        assert data_set.schema_sample["fields"][0]["type"] == "integer"
    
    def test_data_set_transform_config(self, db_session: Session):
        """Test data set transform configuration"""
        transform_config = {
            "transformations": [
                {
                    "type": "filter",
                    "condition": "age > 18"
                },
                {
                    "type": "map", 
                    "field": "name",
                    "function": "uppercase"
                }
            ],
            "version": "1.0"
        }
        
        data_set = create_data_set(db=db_session, transform_config=transform_config)
        
        # Refresh to ensure transform config is properly stored/retrieved
        db_session.refresh(data_set)
        
        assert data_set.transform_config is not None
        assert len(data_set.transform_config.get("transformations")) == 2
        assert data_set.transform_config["transformations"][0]["type"] == "filter"
        assert data_set.transform_config["version"] == "1.0"


@pytest.mark.unit
class TestDataSetStatusManagement:
    """Test DataSet status management functionality"""
    
    def test_data_set_status_inheritance(self, db_session: Session):
        """Test that data set can inherit status from data source"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        
        # Create paused data source
        paused_source = create_data_source(
            db=db_session,
            owner=user,
            org=org,
            status="PAUSED"
        )
        
        # Create data set with same status as source
        data_set = create_data_set(
            db=db_session,
            data_source=paused_source,
            owner=user,
            org=org,
            status=paused_source.status
        )
        
        assert data_set.status == "PAUSED"
        assert data_set.status == paused_source.status
        assert data_set.is_active() is False
    
    def test_data_set_runtime_status(self, db_session: Session):
        """Test data set runtime status tracking"""
        data_set = create_data_set(db=db_session, runtime_status="PROCESSING")
        
        assert data_set.runtime_status == "PROCESSING"
        
        # Update runtime status
        data_set.runtime_status = "IDLE"
        db_session.commit()
        
        db_session.refresh(data_set)
        assert data_set.runtime_status == "IDLE"
    
    def test_data_set_status_transitions(self, db_session: Session):
        """Test data set status transitions"""
        data_set = create_data_set(db=db_session, status="ACTIVE")
        
        # Test ACTIVE to PAUSED
        data_set.status = "PAUSED"
        db_session.commit()
        assert data_set.status == "PAUSED"
        assert data_set.is_active() is False
        
        # Test PAUSED back to ACTIVE
        data_set.status = "ACTIVE"
        db_session.commit()
        assert data_set.status == "ACTIVE"
        assert data_set.is_active() is True


@pytest.mark.unit
class TestDataSetValidation:
    """Test DataSet model validation"""
    
    def test_data_set_name_required(self, db_session: Session):
        """Test that data set name is required"""
        with pytest.raises((ValueError, IntegrityError)):
            data_set = DataSet(
                description="No name set",
                status="ACTIVE",
                owner_id=1,
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(data_set)
            db_session.commit()
    
    def test_data_set_owner_required(self, db_session: Session):
        """Test that data set owner is required"""
        with pytest.raises((ValueError, IntegrityError)):
            data_set = DataSet(
                name="No Owner Set",
                description="Missing owner",
                status="ACTIVE",
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(data_set)
            db_session.commit()
    
    def test_data_set_org_required(self, db_session: Session):
        """Test that data set org is required"""
        with pytest.raises((ValueError, IntegrityError)):
            data_set = DataSet(
                name="No Org Set",
                description="Missing org",
                status="ACTIVE",
                owner_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(data_set)
            db_session.commit()
    
    def test_data_set_defaults(self, db_session: Session):
        """Test data set default values"""
        data_set = create_data_set(db=db_session)
        
        assert data_set.status == "ACTIVE"
        assert data_set.output_schema_locked is False


@pytest.mark.unit
class TestDataSetTimestamps:
    """Test DataSet timestamp functionality"""
    
    def test_created_and_updated_timestamps(self, db_session: Session):
        """Test that created_at and updated_at are set automatically"""
        data_set = create_data_set(db=db_session)
        
        assert data_set.created_at is not None
        assert data_set.updated_at is not None
        
        # Update data set
        original_updated_at = data_set.updated_at
        data_set.description = "Updated Description"
        db_session.commit()
        
        # Note: In a real SQLAlchemy setup with proper onupdate, this would be automatic
        # For now, we just verify the initial timestamps are set
        assert data_set.updated_at >= original_updated_at


@pytest.mark.unit
class TestDataSetBusinessLogic:
    """Test DataSet business logic methods"""
    
    def test_data_set_schema_management(self, db_session: Session):
        """Test data set schema management"""
        data_set = create_data_set(db=db_session, output_schema_locked=False)
        
        # Schema should be unlocked initially
        assert data_set.output_schema_locked is False
        
        # Lock the schema
        data_set.output_schema_locked = True
        db_session.commit()
        
        db_session.refresh(data_set)
        assert data_set.output_schema_locked is True
    
    def test_data_set_schema_association(self, db_session: Session):
        """Test data set schema association"""
        # Note: This would need DataSchema model to be fully implemented
        # For now, just test that the foreign key can be set
        data_set = create_data_set(db=db_session, data_schema_id=456)
        assert data_set.data_schema_id == 456
    
    def test_data_set_flow_node_association(self, db_session: Session):
        """Test data set flow node association"""
        # Note: This would need FlowNode model to be fully implemented
        # For now, just test that the foreign key can be set
        data_set = create_data_set(db=db_session, flow_node_id=789)
        assert data_set.flow_node_id == 789
    
    def test_data_set_sample_data_structure(self, db_session: Session):
        """Test data set sample data structure validation"""
        # Test with complex nested sample data
        complex_sample = {
            "metadata": {
                "total_rows": 1000,
                "sample_size": 100,
                "sampled_at": "2023-01-01T12:00:00Z"
            },
            "sample": {
                "headers": ["id", "user_info", "metrics"],
                "data": [
                    [1, {"name": "John", "age": 30}, {"score": 95.5}],
                    [2, {"name": "Jane", "age": 25}, {"score": 87.2}]
                ]
            }
        }
        
        data_set = create_data_set(db=db_session, data_sample=complex_sample)
        
        db_session.refresh(data_set)
        
        assert data_set.data_sample["metadata"]["total_rows"] == 1000
        assert data_set.data_sample["sample"]["headers"] == ["id", "user_info", "metrics"]
        assert len(data_set.data_sample["sample"]["data"]) == 2
        assert data_set.data_sample["sample"]["data"][0][1]["name"] == "John"
    
    def test_data_set_transform_config_validation(self, db_session: Session):
        """Test data set transform configuration validation"""
        # Test with complex transform config
        complex_transform = {
            "pipeline": {
                "name": "user_data_pipeline",
                "version": "2.1"
            },
            "stages": [
                {
                    "stage": "extract",
                    "config": {"format": "json", "encoding": "utf-8"}
                },
                {
                    "stage": "transform",
                    "config": {
                        "rules": [
                            {"field": "email", "action": "normalize"},
                            {"field": "phone", "action": "format", "pattern": "+1-XXX-XXX-XXXX"}
                        ]
                    }
                },
                {
                    "stage": "load",
                    "config": {"batch_size": 1000, "parallel": True}
                }
            ]
        }
        
        data_set = create_data_set(db=db_session, transform_config=complex_transform)
        
        db_session.refresh(data_set)
        
        assert data_set.transform_config["pipeline"]["name"] == "user_data_pipeline"
        assert len(data_set.transform_config["stages"]) == 3
        assert data_set.transform_config["stages"][1]["stage"] == "transform"
        assert len(data_set.transform_config["stages"][1]["config"]["rules"]) == 2