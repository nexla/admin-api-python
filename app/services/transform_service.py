"""
Transform Service - Handle data transformation logic and execution
"""

import asyncio
import re
import json
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger(__name__)


class TransformService:
    """Service for data transformation operations"""
    
    def __init__(self):
        self.transform_functions = self._load_transform_functions()
    
    def validate_transform_config(
        self,
        transform_type: str,
        transform_config: Dict[str, Any],
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate a transform configuration"""
        
        errors = []
        warnings = []
        
        try:
            # Validate transform type
            valid_types = ["field", "record", "batch", "stream"]
            if transform_type not in valid_types:
                errors.append(f"Invalid transform type. Must be one of: {valid_types}")
            
            # Validate required config fields
            required_fields = self._get_required_config_fields(transform_type)
            for field in required_fields:
                if field not in transform_config:
                    errors.append(f"Missing required config field: {field}")
            
            # Validate source and target schemas
            if not source_schema.get("fields"):
                errors.append("Source schema must define fields")
            
            if not target_schema.get("fields"):
                errors.append("Target schema must define fields")
            
            # Validate field mappings
            if "field_mappings" in transform_config:
                mapping_errors = self._validate_field_mappings(
                    transform_config["field_mappings"],
                    source_schema,
                    target_schema
                )
                errors.extend(mapping_errors)
            
            # Validate transform functions
            if "functions" in transform_config:
                function_errors = self._validate_transform_functions(
                    transform_config["functions"]
                )
                errors.extend(function_errors)
            
            # Check for unused source fields
            if "field_mappings" in transform_config:
                unused_fields = self._check_unused_source_fields(
                    transform_config["field_mappings"],
                    source_schema
                )
                if unused_fields:
                    warnings.append(f"Unused source fields: {unused_fields}")
            
            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings
            }
            
        except Exception as e:
            logger.error(f"Transform config validation error: {str(e)}")
            return {
                "valid": False,
                "errors": [f"Validation error: {str(e)}"],
                "warnings": warnings
            }
    
    async def execute_transform(
        self,
        transform,
        input_data: List[Dict[str, Any]],
        validate_output: bool = True,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Execute a transform against input data"""
        
        start_time = datetime.utcnow()
        transformed_data = []
        validation_results = []
        errors = []
        
        try:
            for i, record in enumerate(input_data):
                try:
                    # Transform the record
                    transformed_record = await self._transform_record(
                        record,
                        transform.transform_config,
                        transform.source_schema,
                        transform.target_schema
                    )
                    
                    # Validate output if requested
                    if validate_output:
                        validation_result = self._validate_record(
                            transformed_record,
                            transform.target_schema
                        )
                        validation_results.append({
                            "record_index": i,
                            "valid": validation_result["valid"],
                            "errors": validation_result.get("errors", [])
                        })
                        
                        if not validation_result["valid"]:
                            errors.append({
                                "record_index": i,
                                "type": "validation_error",
                                "errors": validation_result.get("errors", [])
                            })
                    
                    if not dry_run:
                        transformed_data.append(transformed_record)
                        
                except Exception as e:
                    errors.append({
                        "record_index": i,
                        "type": "transform_error",
                        "error": str(e)
                    })
            
            # Calculate execution stats
            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds() * 1000
            
            execution_stats = {
                "input_count": len(input_data),
                "output_count": len(transformed_data),
                "error_count": len(errors),
                "execution_time_ms": execution_time,
                "success_rate": (len(input_data) - len(errors)) / len(input_data) if input_data else 0
            }
            
            return {
                "success": len(errors) == 0,
                "transformed_data": transformed_data if not dry_run else None,
                "validation_results": validation_results if validate_output else None,
                "execution_stats": execution_stats,
                "errors": errors if errors else None
            }
            
        except Exception as e:
            logger.error(f"Transform execution error: {str(e)}")
            return {
                "success": False,
                "execution_stats": {"error": str(e)},
                "errors": [{"type": "execution_error", "error": str(e)}]
            }
    
    async def preview_transform(
        self,
        transform_config: Dict[str, Any],
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any],
        sample_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Preview transform results with sample data"""
        
        try:
            preview_results = []
            
            for record in sample_data:
                try:
                    transformed_record = await self._transform_record(
                        record,
                        transform_config,
                        source_schema,
                        target_schema
                    )
                    
                    preview_results.append({
                        "input": record,
                        "output": transformed_record,
                        "success": True
                    })
                    
                except Exception as e:
                    preview_results.append({
                        "input": record,
                        "output": None,
                        "success": False,
                        "error": str(e)
                    })
            
            return {
                "success": True,
                "preview_results": preview_results,
                "summary": {
                    "total_records": len(sample_data),
                    "successful_transforms": sum(1 for r in preview_results if r["success"]),
                    "failed_transforms": sum(1 for r in preview_results if not r["success"])
                }
            }
            
        except Exception as e:
            logger.error(f"Transform preview error: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_available_functions(self, category: Optional[str] = None) -> Dict[str, Any]:
        """Get available transform functions"""
        
        functions = self.transform_functions.copy()
        
        if category:
            functions = {
                name: func for name, func in functions.items()
                if func.get("category") == category
            }
        
        return functions
    
    async def _transform_record(
        self,
        record: Dict[str, Any],
        transform_config: Dict[str, Any],
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform a single record"""
        
        transformed_record = {}
        
        # Handle field mappings
        field_mappings = transform_config.get("field_mappings", {})
        
        for target_field, mapping in field_mappings.items():
            try:
                if isinstance(mapping, str):
                    # Simple field mapping
                    if mapping in record:
                        transformed_record[target_field] = record[mapping]
                elif isinstance(mapping, dict):
                    # Complex field mapping with transformation
                    value = self._apply_field_mapping(record, mapping)
                    transformed_record[target_field] = value
                    
            except Exception as e:
                logger.warning(f"Field mapping error for {target_field}: {str(e)}")
                transformed_record[target_field] = None
        
        # Apply transform functions
        functions = transform_config.get("functions", [])
        for function_config in functions:
            try:
                transformed_record = await self._apply_transform_function(
                    transformed_record,
                    function_config
                )
            except Exception as e:
                logger.warning(f"Transform function error: {str(e)}")
        
        return transformed_record
    
    def _apply_field_mapping(self, record: Dict[str, Any], mapping: Dict[str, Any]) -> Any:
        """Apply a complex field mapping"""
        
        source_field = mapping.get("source_field")
        default_value = mapping.get("default_value")
        transform_function = mapping.get("function")
        
        # Get source value
        if source_field and source_field in record:
            value = record[source_field]
        else:
            value = default_value
        
        # Apply transform function if specified
        if transform_function and value is not None:
            value = self._execute_transform_function(transform_function, value, mapping.get("params", {}))
        
        return value
    
    async def _apply_transform_function(
        self,
        record: Dict[str, Any],
        function_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply a transform function to a record"""
        
        function_name = function_config.get("function")
        target_field = function_config.get("target_field")
        source_fields = function_config.get("source_fields", [])
        params = function_config.get("params", {})
        
        if not function_name or not target_field:
            return record
        
        try:
            # Get source values
            source_values = []
            for field in source_fields:
                if field in record:
                    source_values.append(record[field])
                else:
                    source_values.append(None)
            
            # Execute function
            result = self._execute_transform_function(function_name, source_values, params)
            record[target_field] = result
            
        except Exception as e:
            logger.warning(f"Transform function {function_name} error: {str(e)}")
            record[target_field] = None
        
        return record
    
    def _execute_transform_function(
        self,
        function_name: str,
        value: Any,
        params: Dict[str, Any]
    ) -> Any:
        """Execute a specific transform function"""
        
        function_def = self.transform_functions.get(function_name)
        if not function_def:
            raise ValueError(f"Unknown transform function: {function_name}")
        
        function_impl = function_def.get("implementation")
        if not function_impl:
            raise ValueError(f"No implementation for function: {function_name}")
        
        return function_impl(value, params)
    
    def _validate_record(
        self,
        record: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate a record against target schema"""
        
        errors = []
        
        # Check required fields
        required_fields = [
            field["name"] for field in target_schema.get("fields", [])
            if field.get("required", False)
        ]
        
        for field in required_fields:
            if field not in record or record[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate field types
        for field_def in target_schema.get("fields", []):
            field_name = field_def["name"]
            field_type = field_def.get("type")
            
            if field_name in record and record[field_name] is not None:
                if not self._validate_field_type(record[field_name], field_type):
                    errors.append(f"Invalid type for field {field_name}: expected {field_type}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    def _validate_field_type(self, value: Any, expected_type: str) -> bool:
        """Validate that a value matches the expected type"""
        
        type_validators = {
            "string": lambda v: isinstance(v, str),
            "integer": lambda v: isinstance(v, int),
            "float": lambda v: isinstance(v, (int, float)),
            "boolean": lambda v: isinstance(v, bool),
            "datetime": lambda v: isinstance(v, (str, datetime)),
            "date": lambda v: isinstance(v, (str, datetime)),
            "decimal": lambda v: isinstance(v, (int, float, Decimal))
        }
        
        validator = type_validators.get(expected_type.lower())
        if not validator:
            return True  # Unknown type, assume valid
        
        return validator(value)
    
    def _get_required_config_fields(self, transform_type: str) -> List[str]:
        """Get required configuration fields for a transform type"""
        
        required_fields_map = {
            "field": ["field_mappings"],
            "record": ["field_mappings"],
            "batch": ["field_mappings", "batch_size"],
            "stream": ["field_mappings", "stream_config"]
        }
        
        return required_fields_map.get(transform_type, ["field_mappings"])
    
    def _validate_field_mappings(
        self,
        field_mappings: Dict[str, Any],
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> List[str]:
        """Validate field mappings"""
        
        errors = []
        
        source_fields = {field["name"] for field in source_schema.get("fields", [])}
        target_fields = {field["name"] for field in target_schema.get("fields", [])}
        
        for target_field, mapping in field_mappings.items():
            if target_field not in target_fields:
                errors.append(f"Target field '{target_field}' not found in target schema")
            
            if isinstance(mapping, str):
                if mapping not in source_fields:
                    errors.append(f"Source field '{mapping}' not found in source schema")
            elif isinstance(mapping, dict):
                source_field = mapping.get("source_field")
                if source_field and source_field not in source_fields:
                    errors.append(f"Source field '{source_field}' not found in source schema")
        
        return errors
    
    def _validate_transform_functions(self, functions: List[Dict[str, Any]]) -> List[str]:
        """Validate transform functions"""
        
        errors = []
        
        for func_config in functions:
            function_name = func_config.get("function")
            if not function_name:
                errors.append("Transform function missing 'function' field")
                continue
            
            if function_name not in self.transform_functions:
                errors.append(f"Unknown transform function: {function_name}")
        
        return errors
    
    def _check_unused_source_fields(
        self,
        field_mappings: Dict[str, Any],
        source_schema: Dict[str, Any]
    ) -> List[str]:
        """Check for unused source fields"""
        
        source_fields = {field["name"] for field in source_schema.get("fields", [])}
        used_fields = set()
        
        for mapping in field_mappings.values():
            if isinstance(mapping, str):
                used_fields.add(mapping)
            elif isinstance(mapping, dict):
                source_field = mapping.get("source_field")
                if source_field:
                    used_fields.add(source_field)
        
        return list(source_fields - used_fields)
    
    def _load_transform_functions(self) -> Dict[str, Dict[str, Any]]:
        """Load available transform functions"""
        
        return {
            # String functions
            "upper_case": {
                "category": "string",
                "description": "Convert string to uppercase",
                "params": [],
                "implementation": lambda value, params: value.upper() if isinstance(value, str) else value
            },
            "lower_case": {
                "category": "string",
                "description": "Convert string to lowercase",
                "params": [],
                "implementation": lambda value, params: value.lower() if isinstance(value, str) else value
            },
            "trim": {
                "category": "string",
                "description": "Remove leading and trailing whitespace",
                "params": [],
                "implementation": lambda value, params: value.strip() if isinstance(value, str) else value
            },
            "replace": {
                "category": "string",
                "description": "Replace substring with another string",
                "params": ["search", "replace"],
                "implementation": lambda value, params: value.replace(params.get("search", ""), params.get("replace", "")) if isinstance(value, str) else value
            },
            "regex_replace": {
                "category": "string",
                "description": "Replace using regular expression",
                "params": ["pattern", "replacement"],
                "implementation": lambda value, params: re.sub(params.get("pattern", ""), params.get("replacement", ""), value) if isinstance(value, str) else value
            },
            
            # Numeric functions
            "round": {
                "category": "numeric",
                "description": "Round number to specified decimal places",
                "params": ["decimals"],
                "implementation": lambda value, params: round(float(value), params.get("decimals", 0)) if isinstance(value, (int, float)) else value
            },
            "abs": {
                "category": "numeric",
                "description": "Get absolute value",
                "params": [],
                "implementation": lambda value, params: abs(value) if isinstance(value, (int, float)) else value
            },
            "multiply": {
                "category": "numeric",
                "description": "Multiply by a factor",
                "params": ["factor"],
                "implementation": lambda value, params: value * params.get("factor", 1) if isinstance(value, (int, float)) else value
            },
            
            # Date functions
            "format_date": {
                "category": "date",
                "description": "Format date string",
                "params": ["format"],
                "implementation": lambda value, params: datetime.strptime(str(value), "%Y-%m-%d").strftime(params.get("format", "%Y-%m-%d")) if value else value
            },
            
            # Type conversion functions
            "to_string": {
                "category": "conversion",
                "description": "Convert value to string",
                "params": [],
                "implementation": lambda value, params: str(value) if value is not None else None
            },
            "to_integer": {
                "category": "conversion",
                "description": "Convert value to integer",
                "params": [],
                "implementation": lambda value, params: int(float(value)) if value is not None else None
            },
            "to_float": {
                "category": "conversion",
                "description": "Convert value to float",
                "params": [],
                "implementation": lambda value, params: float(value) if value is not None else None
            },
            
            # Conditional functions
            "default_if_null": {
                "category": "conditional",
                "description": "Use default value if input is null",
                "params": ["default"],
                "implementation": lambda value, params: params.get("default") if value is None else value
            },
            "conditional": {
                "category": "conditional",
                "description": "Return value based on condition",
                "params": ["condition", "true_value", "false_value"],
                "implementation": lambda value, params: params.get("true_value") if value else params.get("false_value")
            }
        }