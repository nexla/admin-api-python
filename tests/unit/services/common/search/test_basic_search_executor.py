"""
Tests for BasicSearchExecutor service.
Tests search functionality, filtering, and validation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.orm import Session

from app.services.common.search.basic_search_executor import BasicSearchExecutor, ArgumentError
from app.models.user import User
from app.models.org import Org
from app.models.data_set import DataSet
from app.models.data_source import DataSource


class TestBasicSearchExecutor:
    """Test BasicSearchExecutor functionality"""
    
    @pytest.fixture
    def mock_db_session(self):
        """Mock database session"""
        mock_session = Mock(spec=Session)
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.union.return_value = mock_query
        mock_query.all.return_value = []
        mock_session.query.return_value = mock_query
        return mock_session
    
    @pytest.fixture
    def sample_user(self):
        """Create sample user"""
        return User(
            id=123,
            email="test@example.com"
        )
    
    @pytest.fixture
    def sample_org(self):
        """Create sample organization"""
        return Org(
            id=456,
            name="Test Org"
        )
    
    @pytest.fixture
    def sample_datasets(self):
        """Create sample datasets"""
        return [
            DataSet(
                id=1,
                name="Test Dataset 1",
                org_id=456,
                output_schema={"properties": {"Age": {"type": "number"}, "City": {"type": "string"}}}
            ),
            DataSet(
                id=2,
                name="Test Dataset 2",
                org_id=456,
                source_schema={"properties": {"Salary": {"type": "number"}, "Company": {"type": "string"}}}
            )
        ]
    
    def test_initialization(self, sample_user, sample_org):
        """Test BasicSearchExecutor initialization"""
        filter_dict = {"field": "name", "operator": "contains", "value": "test"}
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict,
            include_public=True
        )
        
        assert executor.user == sample_user
        assert executor.org == sample_org
        assert executor.model_class == DataSet
        assert executor.filter_dict == filter_dict
        assert executor.include_public is True
        assert executor.model_name == "DataSet"
    
    def test_call_basic_search(self, mock_db_session, sample_user, sample_org, sample_datasets):
        """Test basic search execution"""
        # Setup
        mock_db_session.query.return_value.filter.return_value.all.return_value = sample_datasets
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Execute
        results = executor.call(mock_db_session)
        
        # Verify
        assert results == sample_datasets
        mock_db_session.query.assert_called_once_with(DataSet)
    
    def test_call_with_name_contains_filter(self, mock_db_session, sample_user, sample_org):
        """Test search with name contains filter"""
        # Setup
        filter_dict = {"field": "name", "operator": "contains", "value": "test"}
        mock_db_session.query.return_value.filter.return_value.all.return_value = []
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        results = executor.call(mock_db_session)
        
        # Verify filter was applied
        mock_query = mock_db_session.query.return_value
        assert mock_query.filter.call_count >= 2  # org filter + name filter
        assert results == []
    
    def test_call_with_schema_properties_filter(self, mock_db_session, sample_user, sample_org):
        """Test search with schema properties filter"""
        # Setup
        filter_dict = {"field": "output_schema_properties", "operator": "contains", "value": "city"}
        mock_db_session.query.return_value.filter.return_value.all.return_value = []
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        results = executor.call(mock_db_session)
        
        # Verify filter was applied
        mock_query = mock_db_session.query.return_value
        assert mock_query.filter.call_count >= 2  # org filter + schema filter
    
    def test_call_with_include_public(self, mock_db_session, sample_user, sample_org):
        """Test search including public resources"""
        # Setup
        filter_dict = {"field": "name", "operator": "contains", "value": "test"}
        mock_db_session.query.return_value.filter.return_value.all.return_value = []
        mock_db_session.query.return_value.union.return_value.all.return_value = []
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict,
            include_public=True
        )
        
        # Execute
        results = executor.call(mock_db_session)
        
        # Verify union was called for public resources
        mock_query = mock_db_session.query.return_value
        mock_query.union.assert_called_once()
    
    def test_ids_method(self, mock_db_session, sample_user, sample_org, sample_datasets):
        """Test IDs-only search"""
        # Setup
        mock_db_session.query.return_value.filter.return_value.all.return_value = sample_datasets
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Execute
        ids = executor.ids(mock_db_session)
        
        # Verify
        assert ids == [1, 2]
    
    def test_validate_filter_string_field_valid_operator(self, sample_user, sample_org):
        """Test filter validation for valid string field operator"""
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Should not raise exception
        executor._validate_filter("name", "contains", "test")
    
    def test_validate_filter_string_field_invalid_operator(self, sample_user, sample_org):
        """Test filter validation for invalid string field operator"""
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Should raise ArgumentError
        with pytest.raises(ArgumentError, match="Invalid filter operator"):
            executor._validate_filter("name", "gt", "test")
    
    def test_validate_filter_numeric_field_valid_operator(self, sample_user, sample_org):
        """Test filter validation for valid numeric field operator"""
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Should not raise exception
        executor._validate_filter("id", "eq", "123")
    
    def test_validate_filter_numeric_field_invalid_operator(self, sample_user, sample_org):
        """Test filter validation for invalid numeric field operator"""
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Should raise ArgumentError
        with pytest.raises(ArgumentError, match="Invalid filter operator"):
            executor._validate_filter("id", "contains", "123")
    
    def test_validate_filter_numeric_field_invalid_value(self, sample_user, sample_org):
        """Test filter validation for invalid numeric value"""
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Should raise ArgumentError
        with pytest.raises(ArgumentError, match="Invalid value"):
            executor._validate_filter("id", "eq", "ABC!")
    
    def test_validate_filter_invalid_field(self, sample_user, sample_org):
        """Test filter validation for invalid field"""
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Should raise ArgumentError
        with pytest.raises(ArgumentError, match="Invalid field"):
            executor._validate_filter("invalid_field", "eq", "test")
    
    def test_apply_filters_equality(self, mock_db_session, sample_user, sample_org):
        """Test applying equality filter"""
        # Setup
        filter_dict = {"field": "id", "operator": "eq", "value": "123"}
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        result = executor._apply_filters(mock_query)
        
        # Verify filter was applied
        mock_query.filter.assert_called()
        assert result == mock_query
    
    def test_apply_filters_greater_than(self, mock_db_session, sample_user, sample_org):
        """Test applying greater than filter"""
        # Setup
        filter_dict = {"field": "id", "operator": "gt", "value": "100"}
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        result = executor._apply_filters(mock_query)
        
        # Verify filter was applied
        mock_query.filter.assert_called()
    
    def test_apply_filters_contains(self, mock_db_session, sample_user, sample_org):
        """Test applying contains filter"""
        # Setup
        filter_dict = {"field": "name", "operator": "contains", "value": "test"}
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        result = executor._apply_filters(mock_query)
        
        # Verify filter was applied
        mock_query.filter.assert_called()
    
    def test_apply_filters_starts_with(self, mock_db_session, sample_user, sample_org):
        """Test applying starts_with filter"""
        # Setup
        filter_dict = {"field": "name", "operator": "starts_with", "value": "test"}
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        result = executor._apply_filters(mock_query)
        
        # Verify filter was applied
        mock_query.filter.assert_called()
    
    def test_apply_filters_ends_with(self, mock_db_session, sample_user, sample_org):
        """Test applying ends_with filter"""
        # Setup
        filter_dict = {"field": "name", "operator": "ends_with", "value": "test"}
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        result = executor._apply_filters(mock_query)
        
        # Verify filter was applied
        mock_query.filter.assert_called()
    
    def test_apply_filters_not_equal(self, mock_db_session, sample_user, sample_org):
        """Test applying not equal filter"""
        # Setup
        filter_dict = {"field": "status", "operator": "ne", "value": "inactive"}
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        result = executor._apply_filters(mock_query)
        
        # Verify filter was applied
        mock_query.filter.assert_called()
    
    def test_apply_filters_incomplete_filter(self, mock_db_session, sample_user, sample_org):
        """Test applying incomplete filter (missing required fields)"""
        # Setup
        filter_dict = {"field": "name"}  # Missing operator and value
        mock_query = Mock()
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet,
            filter_dict=filter_dict
        )
        
        # Execute
        result = executor._apply_filters(mock_query)
        
        # Should return query unchanged
        assert result == mock_query
        mock_query.filter.assert_not_called()
    
    @patch('app.services.common.search.basic_search_executor.text')
    def test_apply_schema_properties_filter_source_schema(self, mock_text, sample_user, sample_org):
        """Test applying source schema properties filter"""
        # Setup
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.params.return_value = mock_query
        mock_text.return_value.params.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Execute
        result = executor._apply_schema_properties_filter(
            mock_query, 
            "source_schema_properties", 
            "contains", 
            "salary"
        )
        
        # Verify filter was applied
        mock_query.filter.assert_called()
    
    @patch('app.services.common.search.basic_search_executor.text')
    def test_apply_schema_properties_filter_output_schema(self, mock_text, sample_user, sample_org):
        """Test applying output schema properties filter"""
        # Setup
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.params.return_value = mock_query
        mock_text.return_value.params.return_value = mock_query
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Execute
        result = executor._apply_schema_properties_filter(
            mock_query, 
            "output_schema_properties", 
            "contains", 
            "city"
        )
        
        # Verify filter was applied
        mock_query.filter.assert_called()
    
    def test_apply_schema_properties_filter_invalid_operator(self, sample_user, sample_org):
        """Test schema properties filter with invalid operator"""
        # Setup
        mock_query = Mock()
        
        executor = BasicSearchExecutor(
            user=sample_user,
            org=sample_org,
            model_class=DataSet
        )
        
        # Execute and verify exception
        with pytest.raises(ArgumentError, match="Only 'contains' operator is supported"):
            executor._apply_schema_properties_filter(
                mock_query, 
                "source_schema_properties", 
                "eq", 
                "salary"
            )
    
    def test_field_type_mappings(self):
        """Test that field type mappings are properly defined"""
        field_types = BasicSearchExecutor.FIELD_TYPES
        
        # Verify DataSet fields
        assert 'DataSet' in field_types
        dataset_fields = field_types['DataSet']
        assert dataset_fields['id'] == 'numeric'
        assert dataset_fields['name'] == 'string'
        assert dataset_fields['source_schema_properties'] == 'string'
        assert dataset_fields['output_schema_properties'] == 'string'
        
        # Verify DataSource fields
        assert 'DataSource' in field_types
        datasource_fields = field_types['DataSource']
        assert datasource_fields['id'] == 'numeric'
        assert datasource_fields['name'] == 'string'
        assert datasource_fields['connector_type'] == 'string'
    
    def test_operator_lists(self):
        """Test that operator lists are properly defined"""
        assert 'contains' in BasicSearchExecutor.STRING_OPERATORS
        assert 'eq' in BasicSearchExecutor.STRING_OPERATORS
        assert 'starts_with' in BasicSearchExecutor.STRING_OPERATORS
        assert 'ends_with' in BasicSearchExecutor.STRING_OPERATORS
        
        assert 'eq' in BasicSearchExecutor.NUMERIC_OPERATORS
        assert 'gt' in BasicSearchExecutor.NUMERIC_OPERATORS
        assert 'gte' in BasicSearchExecutor.NUMERIC_OPERATORS
        assert 'lt' in BasicSearchExecutor.NUMERIC_OPERATORS
        assert 'lte' in BasicSearchExecutor.NUMERIC_OPERATORS