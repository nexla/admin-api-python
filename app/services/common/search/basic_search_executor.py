"""
Basic Search Executor - Execute filtered searches with validation.
Provides search capabilities with field filtering and validation.
"""

import logging
from typing import Optional, List, Dict, Any, Union, Type
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, text
import re

from ....models.user import User
from ....models.org import Org
from ....models.data_set import DataSet
from ....models.data_source import DataSource

logger = logging.getLogger(__name__)


class BasicSearchExecutor:
    """Execute basic searches with filtering and validation"""
    
    # Valid operators for different field types
    STRING_OPERATORS = ['contains', 'eq', 'ne', 'starts_with', 'ends_with']
    NUMERIC_OPERATORS = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte']
    
    # Field type mappings for validation
    FIELD_TYPES = {
        'DataSet': {
            'id': 'numeric',
            'name': 'string',
            'description': 'string',
            'created_at': 'datetime',
            'updated_at': 'datetime',
            'source_schema_properties': 'string',
            'output_schema_properties': 'string',
            'status': 'string',
            'public': 'boolean'
        },
        'DataSource': {
            'id': 'numeric',
            'name': 'string',
            'description': 'string',
            'created_at': 'datetime',
            'updated_at': 'datetime',
            'status': 'string',
            'connector_type': 'string'
        }
    }
    
    def __init__(
        self,
        user: User,
        org: Org,
        model_class: Type,
        filter_dict: Optional[Dict[str, Any]] = None,
        include_public: bool = False
    ):
        """
        Initialize search executor.
        
        Args:
            user: User performing the search
            org: Organization context
            model_class: Model class to search (DataSet, DataSource, etc.)
            filter_dict: Filter parameters
            include_public: Whether to include public resources
        """
        self.user = user
        self.org = org
        self.model_class = model_class
        self.filter_dict = filter_dict or {}
        self.include_public = include_public
        self.model_name = model_class.__name__
    
    def call(self, db: Session) -> List[Any]:
        """
        Execute the search and return results.
        
        Args:
            db: Database session
            
        Returns:
            List of model instances matching the search criteria
        """
        try:
            # Build base query
            query = db.query(self.model_class)
            
            # Apply organization filter
            query = query.filter(self.model_class.org_id == self.org.id)
            
            # Apply search filters
            if self.filter_dict:
                query = self._apply_filters(query)
            
            # Include public resources if requested
            if self.include_public:
                public_query = db.query(self.model_class).filter(
                    and_(
                        self.model_class.public == True,
                        self.model_class.org_id != self.org.id
                    )
                )
                
                # Apply same filters to public resources
                if self.filter_dict:
                    public_query = self._apply_filters(public_query)
                
                # Combine queries
                query = query.union(public_query)
            
            # Execute query
            results = query.all()
            
            logger.info(f"Search executed: {len(results)} results for {self.model_name}")
            return results
            
        except Exception as e:
            logger.error(f"Search execution failed: {str(e)}")
            raise
    
    def ids(self, db: Session) -> List[int]:
        """
        Execute the search and return only IDs.
        
        Args:
            db: Database session
            
        Returns:
            List of IDs matching the search criteria
        """
        try:
            results = self.call(db)
            return [result.id for result in results]
            
        except Exception as e:
            logger.error(f"ID search execution failed: {str(e)}")
            raise
    
    def _apply_filters(self, query):
        """Apply filter dictionary to query"""
        try:
            field = self.filter_dict.get('field')
            operator = self.filter_dict.get('operator')
            value = self.filter_dict.get('value')
            
            if not all([field, operator, value is not None]):
                return query
            
            # Validate filter
            self._validate_filter(field, operator, value)
            
            # Apply filter based on field type and operator
            if field in ['source_schema_properties', 'output_schema_properties']:
                # Special handling for schema properties search
                query = self._apply_schema_properties_filter(query, field, operator, value)
            elif operator == 'contains':
                # String contains search
                column = getattr(self.model_class, field)
                query = query.filter(column.ilike(f'%{value}%'))
            elif operator == 'eq':
                # Equality
                column = getattr(self.model_class, field)
                query = query.filter(column == value)
            elif operator == 'ne':
                # Not equal
                column = getattr(self.model_class, field)
                query = query.filter(column != value)
            elif operator == 'gt':
                # Greater than
                column = getattr(self.model_class, field)
                query = query.filter(column > value)
            elif operator == 'gte':
                # Greater than or equal
                column = getattr(self.model_class, field)
                query = query.filter(column >= value)
            elif operator == 'lt':
                # Less than
                column = getattr(self.model_class, field)
                query = query.filter(column < value)
            elif operator == 'lte':
                # Less than or equal
                column = getattr(self.model_class, field)
                query = query.filter(column <= value)
            elif operator == 'starts_with':
                # String starts with
                column = getattr(self.model_class, field)
                query = query.filter(column.ilike(f'{value}%'))
            elif operator == 'ends_with':
                # String ends with
                column = getattr(self.model_class, field)
                query = query.filter(column.ilike(f'%{value}'))
            
            return query
            
        except Exception as e:
            logger.error(f"Filter application failed: {str(e)}")
            raise
    
    def _apply_schema_properties_filter(self, query, field, operator, value):
        """Apply filtering for schema properties fields"""
        try:
            if operator != 'contains':
                raise ArgumentError(f"Only 'contains' operator is supported for {field}")
            
            # Use JSON search for schema properties
            if field == 'source_schema_properties':
                # Search in source schema properties
                if hasattr(self.model_class, 'source_schema'):
                    query = query.filter(
                        text(f"LOWER(JSON_EXTRACT({self.model_class.__tablename__}.source_schema, '$.properties')) LIKE LOWER(:search_term)")
                    ).params(search_term=f'%{value}%')
            elif field == 'output_schema_properties':
                # Search in output schema properties
                if hasattr(self.model_class, 'output_schema'):
                    query = query.filter(
                        text(f"LOWER(JSON_EXTRACT({self.model_class.__tablename__}.output_schema, '$.properties')) LIKE LOWER(:search_term)")
                    ).params(search_term=f'%{value}%')
            
            return query
            
        except Exception as e:
            logger.error(f"Schema properties filter failed: {str(e)}")
            raise
    
    def _validate_filter(self, field: str, operator: str, value: Any):
        """Validate filter parameters"""
        try:
            # Check if field is valid for the model
            model_fields = self.FIELD_TYPES.get(self.model_name, {})
            if field not in model_fields:
                raise ArgumentError(f"Invalid field '{field}' for {self.model_name}")
            
            field_type = model_fields[field]
            
            # Validate operator for field type
            if field_type == 'string' and operator not in self.STRING_OPERATORS:
                raise ArgumentError(f"Invalid filter operator '{operator}' for string field '{field}'")
            elif field_type == 'numeric' and operator not in self.NUMERIC_OPERATORS:
                raise ArgumentError(f"Invalid filter operator '{operator}' for numeric field '{field}'")
            
            # Validate value for numeric fields
            if field_type == 'numeric':
                try:
                    float(value)
                except (ValueError, TypeError):
                    raise ArgumentError(f"Invalid value '{value}' for numeric field '{field}'")
            
        except Exception as e:
            logger.error(f"Filter validation failed: {str(e)}")
            raise


class ArgumentError(Exception):
    """Custom exception for argument validation errors"""
    pass