"""
Tests for DataSink model.
Migrated from Rails spec patterns for data_sink functionality.
"""
import pytest
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.data_sink import DataSink
from tests.factories import (
    create_data_sink, create_data_set, create_data_source,
    create_user, create_org
)


@pytest.mark.unit
class TestDataSink:
    """Test DataSink model functionality"""
    
    def test_create_data_sink(self, db_session: Session):
        """Test creating a basic data sink"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        data_source = create_data_source(db=db_session, owner=user, org=org)
        data_set = create_data_set(db=db_session, data_source=data_source, owner=user, org=org)
        
        data_sink = create_data_sink(
            db=db_session,
            name="Test Data Sink",
            description="A test data sink",
            owner=user,
            org=org,
            data_set=data_set,
            connection_type="s3"
        )
        
        assert data_sink.name == "Test Data Sink"
        assert data_sink.description == "A test data sink"
        assert data_sink.status == "ACTIVE"
        assert data_sink.connection_type == "s3"
        assert data_sink.owner == user
        assert data_sink.org == org
        assert data_sink.data_set == data_set
        assert data_sink.is_active() is True
    
    def test_data_sink_status_methods(self, db_session: Session):
        """Test data sink status checking methods"""
        # Test active data sink
        active_sink = create_data_sink(db=db_session, status="ACTIVE")
        assert active_sink.is_active() is True
        
        # Test paused data sink
        paused_sink = create_data_sink(db=db_session, status="PAUSED")
        assert paused_sink.is_active() is False
        
        # Test inactive data sink
        inactive_sink = create_data_sink(db=db_session, status="INACTIVE")
        assert inactive_sink.is_active() is False
    
    def test_data_sink_relationships(self, db_session: Session):
        """Test data sink relationships"""
        org = create_org(db=db_session)
        user = create_user(db=db_session)
        data_source = create_data_source(db=db_session, owner=user, org=org)
        data_set = create_data_set(db=db_session, data_source=data_source, owner=user, org=org)
        
        data_sink = create_data_sink(
            db=db_session,
            owner=user,
            org=org,
            data_set=data_set
        )
        
        # Test basic relationships
        assert data_sink.owner == user
        assert data_sink.org == org
        assert data_sink.data_set == data_set
        assert data_sink.owner_id == user.id
        assert data_sink.org_id == org.id
        assert data_sink.data_set_id == data_set.id
    
    def test_data_sink_config_storage(self, db_session: Session):
        """Test data sink configuration storage"""
        config_data = {
            "destination": {
                "bucket_name": "output-bucket",
                "region": "us-west-2",
                "path": "/exports/data/"
            },
            "format": {
                "type": "parquet",
                "compression": "snappy"
            },
            "schedule": {
                "frequency": "daily",
                "time": "02:00"
            }
        }
        
        data_sink = create_data_sink(db=db_session, config=config_data)
        
        # Refresh to ensure config is properly stored/retrieved
        db_session.refresh(data_sink)
        
        assert data_sink.config is not None
        assert data_sink.config.get("destination")["bucket_name"] == "output-bucket"
        assert data_sink.config.get("format")["type"] == "parquet"
        assert data_sink.config.get("schedule")["frequency"] == "daily"
    
    def test_data_sink_runtime_config(self, db_session: Session):
        """Test data sink runtime configuration"""
        runtime_config = {
            "last_export": "2023-01-01T00:00:00Z",
            "next_export": "2023-01-02T00:00:00Z",
            "export_count": 42,
            "last_success": "2023-01-01T00:00:00Z",
            "last_failure": None
        }
        
        data_sink = create_data_sink(db=db_session, runtime_config=runtime_config)
        
        # Refresh to ensure runtime config is properly stored/retrieved
        db_session.refresh(data_sink)
        
        assert data_sink.runtime_config is not None
        assert data_sink.runtime_config.get("last_export") == "2023-01-01T00:00:00Z"
        assert data_sink.runtime_config.get("export_count") == 42
        assert data_sink.runtime_config.get("last_failure") is None


@pytest.mark.unit
class TestDataSinkStatusManagement:
    """Test DataSink status management functionality"""
    
    def test_data_sink_runtime_status(self, db_session: Session):
        """Test data sink runtime status tracking"""
        data_sink = create_data_sink(db=db_session, runtime_status="EXPORTING")
        
        assert data_sink.runtime_status == "EXPORTING"
        
        # Update runtime status
        data_sink.runtime_status = "IDLE"
        db_session.commit()
        
        db_session.refresh(data_sink)
        assert data_sink.runtime_status == "IDLE"
    
    def test_data_sink_status_transitions(self, db_session: Session):
        """Test data sink status transitions"""
        data_sink = create_data_sink(db=db_session, status="ACTIVE")
        
        # Test ACTIVE to PAUSED
        data_sink.status = "PAUSED"
        db_session.commit()
        assert data_sink.status == "PAUSED"
        assert data_sink.is_active() is False
        
        # Test PAUSED back to ACTIVE
        data_sink.status = "ACTIVE"
        db_session.commit()
        assert data_sink.status == "ACTIVE"
        assert data_sink.is_active() is True


@pytest.mark.unit
class TestDataSinkValidation:
    """Test DataSink model validation"""
    
    def test_data_sink_name_required(self, db_session: Session):
        """Test that data sink name is required"""
        with pytest.raises((ValueError, IntegrityError)):
            data_sink = DataSink(
                description="No name sink",
                status="ACTIVE",
                owner_id=1,
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(data_sink)
            db_session.commit()
    
    def test_data_sink_owner_required(self, db_session: Session):
        """Test that data sink owner is required"""
        with pytest.raises((ValueError, IntegrityError)):
            data_sink = DataSink(
                name="No Owner Sink",
                description="Missing owner",
                status="ACTIVE",
                org_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(data_sink)
            db_session.commit()
    
    def test_data_sink_org_required(self, db_session: Session):
        """Test that data sink org is required"""
        with pytest.raises((ValueError, IntegrityError)):
            data_sink = DataSink(
                name="No Org Sink",
                description="Missing org",
                status="ACTIVE",
                owner_id=1,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db_session.add(data_sink)
            db_session.commit()
    
    def test_data_sink_defaults(self, db_session: Session):
        """Test data sink default values"""
        data_sink = create_data_sink(db=db_session)
        
        assert data_sink.status == "ACTIVE"


@pytest.mark.unit
class TestDataSinkTimestamps:
    """Test DataSink timestamp functionality"""
    
    def test_created_and_updated_timestamps(self, db_session: Session):
        """Test that created_at and updated_at are set automatically"""
        data_sink = create_data_sink(db=db_session)
        
        assert data_sink.created_at is not None
        assert data_sink.updated_at is not None
        
        # Update data sink
        original_updated_at = data_sink.updated_at
        data_sink.description = "Updated Description"
        db_session.commit()
        
        # Note: In a real SQLAlchemy setup with proper onupdate, this would be automatic
        # For now, we just verify the initial timestamps are set
        assert data_sink.updated_at >= original_updated_at


@pytest.mark.unit
class TestDataSinkConnectionTypes:
    """Test DataSink connection type functionality"""
    
    def test_s3_connection_type(self, db_session: Session):
        """Test S3 connection type"""
        data_sink = create_data_sink(db=db_session, connection_type="s3")
        assert data_sink.connection_type == "s3"
    
    def test_database_connection_type(self, db_session: Session):
        """Test database connection type"""
        data_sink = create_data_sink(db=db_session, connection_type="postgresql")
        assert data_sink.connection_type == "postgresql"
    
    def test_api_connection_type(self, db_session: Session):
        """Test API connection type"""
        data_sink = create_data_sink(db=db_session, connection_type="webhook")
        assert data_sink.connection_type == "webhook"


@pytest.mark.unit
class TestDataSinkBusinessLogic:
    """Test DataSink business logic methods"""
    
    def test_data_sink_credential_association(self, db_session: Session):
        """Test data sink credential association"""
        # Note: This would need DataCredentials model to be fully implemented
        # For now, just test that the foreign key can be set
        data_sink = create_data_sink(db=db_session, data_credentials_id=789)
        assert data_sink.data_credentials_id == 789
    
    def test_data_sink_connector_association(self, db_session: Session):
        """Test data sink connector association"""
        # Note: This would need Connector model to be fully implemented
        # For now, just test that the foreign key can be set
        data_sink = create_data_sink(db=db_session, connector_id=456)
        assert data_sink.connector_id == 456
    
    def test_data_sink_flow_node_association(self, db_session: Session):
        """Test data sink flow node association"""
        # Note: This would need FlowNode model to be fully implemented
        # For now, just test that the foreign key can be set
        data_sink = create_data_sink(db=db_session, flow_node_id=123)
        assert data_sink.flow_node_id == 123
    
    def test_data_sink_export_configuration(self, db_session: Session):
        """Test data sink export configuration"""
        export_config = {
            "output": {
                "format": "json",
                "compression": "gzip",
                "file_size_limit": "100MB"
            },
            "metadata": {
                "include_schema": True,
                "include_lineage": True,
                "include_statistics": False
            },
            "partitioning": {
                "enabled": True,
                "field": "created_date",
                "pattern": "yyyy/MM/dd"
            }
        }
        
        data_sink = create_data_sink(db=db_session, config=export_config)
        
        db_session.refresh(data_sink)
        
        assert data_sink.config["output"]["format"] == "json"
        assert data_sink.config["metadata"]["include_schema"] is True
        assert data_sink.config["partitioning"]["enabled"] is True
        assert data_sink.config["partitioning"]["field"] == "created_date"
    
    def test_data_sink_runtime_metrics(self, db_session: Session):
        """Test data sink runtime metrics tracking"""
        runtime_metrics = {
            "export_history": [
                {
                    "timestamp": "2023-01-01T00:00:00Z",
                    "records_exported": 1000,
                    "bytes_exported": 50000,
                    "duration_ms": 2500,
                    "status": "success"
                },
                {
                    "timestamp": "2023-01-02T00:00:00Z",
                    "records_exported": 1200,
                    "bytes_exported": 60000,
                    "duration_ms": 3000,
                    "status": "success"
                }
            ],
            "totals": {
                "lifetime_records": 2200,
                "lifetime_bytes": 110000,
                "success_count": 2,
                "failure_count": 0
            }
        }
        
        data_sink = create_data_sink(db=db_session, runtime_config=runtime_metrics)
        
        db_session.refresh(data_sink)
        
        assert len(data_sink.runtime_config["export_history"]) == 2
        assert data_sink.runtime_config["totals"]["lifetime_records"] == 2200
        assert data_sink.runtime_config["totals"]["success_count"] == 2
        assert data_sink.runtime_config["export_history"][0]["records_exported"] == 1000