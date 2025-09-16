"""
Numeric Params Service - Convert string parameters to numeric types.
Provides parameter type conversion and validation.
"""

import logging
from typing import Dict, Any, Union

logger = logging.getLogger(__name__)


class NumericParamsService:
    """Convert string parameters to numeric types where appropriate"""
    
    def __init__(self, hash_data: Dict[str, Any]):
        """
        Initialize numeric params converter.
        
        Args:
            hash_data: Dictionary containing parameters to convert
        """
        self.hash_data = hash_data or {}
    
    def to_h(self) -> Dict[str, Any]:
        """
        Convert string parameters to numeric types where possible.
        
        Returns:
            Dictionary with converted numeric values
        """
        try:
            converted = {}
            
            for key, value in self.hash_data.items():
                converted[key] = self._convert_value(value)
            
            return converted
            
        except Exception as e:
            logger.error(f"Error converting numeric params: {str(e)}")
            return self.hash_data.copy()
    
    def _convert_value(self, value: Any) -> Any:
        """
        Convert a single value to numeric type if appropriate.
        
        Args:
            value: Value to convert
            
        Returns:
            Converted value or original value if conversion not possible
        """
        try:
            # Skip if already numeric
            if isinstance(value, (int, float)):
                return value
            
            # Skip if not string
            if not isinstance(value, str):
                return value
            
            # Skip empty strings
            if not value:
                return value
            
            # Try to convert to integer first
            if self._is_integer_string(value):
                return int(value)
            
            # Try to convert to float
            if self._is_float_string(value):
                return float(value)
            
            # Return original value if no conversion possible
            return value
            
        except Exception as e:
            logger.debug(f"Could not convert value '{value}': {str(e)}")
            return value
    
    def _is_integer_string(self, value: str) -> bool:
        """
        Check if string represents an integer.
        
        Args:
            value: String to check
            
        Returns:
            True if string represents an integer
        """
        try:
            # Check for pure numeric string
            if value.isdigit():
                return True
            
            # Check for negative integer
            if value.startswith('-') and value[1:].isdigit():
                return True
            
            # Try parsing as integer
            int(value)
            
            # Check that it doesn't contain decimal point or other characters
            return '.' not in value and value.replace('-', '').isdigit()
            
        except (ValueError, AttributeError):
            return False
    
    def _is_float_string(self, value: str) -> bool:
        """
        Check if string represents a float.
        
        Args:
            value: String to check
            
        Returns:
            True if string represents a float
        """
        try:
            # Must contain decimal point for float
            if '.' not in value:
                return False
            
            # Try parsing as float
            float(value)
            return True
            
        except (ValueError, AttributeError):
            return False
    
    @classmethod
    def convert(cls, hash_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convenience class method to convert parameters.
        
        Args:
            hash_data: Dictionary to convert
            
        Returns:
            Dictionary with converted numeric values
        """
        converter = cls(hash_data)
        return converter.to_h()


# Compatibility alias for Rails naming
class NumericParams(NumericParamsService):
    """Alias for Rails compatibility"""
    pass