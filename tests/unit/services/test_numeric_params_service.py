"""
Tests for NumericParamsService.
Tests parameter type conversion and validation functionality.
"""

import pytest
from app.services.numeric_params_service import NumericParamsService, NumericParams


class TestNumericParamsService:
    """Test NumericParamsService functionality"""
    
    def test_empty_hash(self):
        """Test conversion of empty hash"""
        service = NumericParamsService({})
        result = service.to_h()
        
        assert result == {}
    
    def test_none_hash(self):
        """Test conversion of None hash"""
        service = NumericParamsService(None)
        result = service.to_h()
        
        assert result == {}
    
    def test_mixed_parameter_conversion(self):
        """Test conversion matching Rails spec example"""
        hash_data = {
            'a': 123,           # Already integer, should stay the same
            'b': '456',         # String integer, should convert to int
            'c': '456notstring', # String with non-numeric suffix, should stay string
            'd': '987'          # String integer, should convert to int
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        expected = {
            'a': 123,
            'b': 456,
            'c': '456notstring',
            'd': 987
        }
        
        assert result == expected
    
    def test_integer_string_conversion(self):
        """Test conversion of various integer string formats"""
        hash_data = {
            'positive': '123',
            'negative': '-456',
            'zero': '0',
            'large': '999999999'
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        expected = {
            'positive': 123,
            'negative': -456,
            'zero': 0,
            'large': 999999999
        }
        
        assert result == expected
    
    def test_float_string_conversion(self):
        """Test conversion of float strings"""
        hash_data = {
            'simple_float': '3.14',
            'negative_float': '-2.5',
            'zero_float': '0.0',
            'scientific': '1.23e-4'
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        expected = {
            'simple_float': 3.14,
            'negative_float': -2.5,
            'zero_float': 0.0,
            'scientific': 1.23e-4
        }
        
        assert result == expected
    
    def test_non_numeric_strings_unchanged(self):
        """Test that non-numeric strings remain unchanged"""
        hash_data = {
            'text': 'hello',
            'mixed': '123abc',
            'symbol': '$100',
            'empty': '',
            'space': ' ',
            'percentage': '50%'
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        # All should remain unchanged
        assert result == hash_data
    
    def test_already_numeric_values_unchanged(self):
        """Test that already numeric values remain unchanged"""
        hash_data = {
            'int': 42,
            'float': 3.14159,
            'negative_int': -10,
            'negative_float': -2.5,
            'zero_int': 0,
            'zero_float': 0.0
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        assert result == hash_data
    
    def test_non_string_non_numeric_values_unchanged(self):
        """Test that non-string, non-numeric values remain unchanged"""
        hash_data = {
            'list': [1, 2, 3],
            'dict': {'key': 'value'},
            'none': None,
            'bool_true': True,
            'bool_false': False
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        assert result == hash_data
    
    def test_edge_case_strings(self):
        """Test edge case string values"""
        hash_data = {
            'leading_zero': '007',        # Should convert to 7
            'decimal_no_fraction': '10.', # Should convert to 10.0
            'leading_decimal': '.5',      # Should convert to 0.5
            'plus_sign': '+123',          # Should convert to 123
            'spaces': ' 456 ',            # Should stay as string (has spaces)
            'multiple_dots': '1.2.3'     # Should stay as string (invalid float)
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        expected = {
            'leading_zero': 7,
            'decimal_no_fraction': 10.0,
            'leading_decimal': 0.5,
            'plus_sign': 123,
            'spaces': ' 456 ',
            'multiple_dots': '1.2.3'
        }
        
        assert result == expected
    
    def test_is_integer_string_method(self):
        """Test _is_integer_string method"""
        service = NumericParamsService({})
        
        # Should return True for valid integers
        assert service._is_integer_string('123') is True
        assert service._is_integer_string('-456') is True
        assert service._is_integer_string('0') is True
        
        # Should return False for non-integers
        assert service._is_integer_string('3.14') is False
        assert service._is_integer_string('abc') is False
        assert service._is_integer_string('123abc') is False
        assert service._is_integer_string('') is False
        assert service._is_integer_string(' ') is False
    
    def test_is_float_string_method(self):
        """Test _is_float_string method"""
        service = NumericParamsService({})
        
        # Should return True for valid floats
        assert service._is_float_string('3.14') is True
        assert service._is_float_string('-2.5') is True
        assert service._is_float_string('0.0') is True
        assert service._is_float_string('.5') is True
        assert service._is_float_string('10.') is True
        
        # Should return False for non-floats or integers
        assert service._is_float_string('123') is False
        assert service._is_float_string('abc') is False
        assert service._is_float_string('') is False
        assert service._is_float_string('1.2.3') is False
    
    def test_convert_value_error_handling(self):
        """Test error handling in _convert_value method"""
        service = NumericParamsService({})
        
        # Test with values that might cause errors
        assert service._convert_value(None) is None
        assert service._convert_value([1, 2, 3]) == [1, 2, 3]
        assert service._convert_value({'key': 'value'}) == {'key': 'value'}
    
    def test_class_method_convert(self):
        """Test convenience class method"""
        hash_data = {
            'a': '123',
            'b': '456notstring',
            'c': 789
        }
        
        result = NumericParamsService.convert(hash_data)
        
        expected = {
            'a': 123,
            'b': '456notstring',
            'c': 789
        }
        
        assert result == expected
    
    def test_to_h_error_handling(self):
        """Test error handling in to_h method"""
        # Create a service with valid data
        service = NumericParamsService({'valid': '123'})
        
        # Mock _convert_value to raise an exception
        original_method = service._convert_value
        service._convert_value = lambda x: exec('raise Exception("Test error")')
        
        # Should return copy of original data on error
        result = service.to_h()
        
        # Restore original method
        service._convert_value = original_method
        
        assert result == {'valid': '123'}
    
    def test_large_numbers(self):
        """Test conversion of large numbers"""
        hash_data = {
            'large_int': '999999999999999999',
            'large_float': '999999999.999999999',
            'scientific': '1.23e10',
            'negative_scientific': '-4.56e-8'
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        assert isinstance(result['large_int'], int)
        assert isinstance(result['large_float'], float)
        assert isinstance(result['scientific'], float)
        assert isinstance(result['negative_scientific'], float)
        
        assert result['large_int'] == 999999999999999999
        assert result['large_float'] == 999999999.999999999
        assert result['scientific'] == 1.23e10
        assert result['negative_scientific'] == -4.56e-8
    
    def test_unicode_strings(self):
        """Test handling of unicode strings"""
        hash_data = {
            'unicode_number': '１２３',  # Full-width numbers
            'unicode_text': '测试',      # Chinese characters
            'mixed': '123测试'           # Mixed
        }
        
        service = NumericParamsService(hash_data)
        result = service.to_h()
        
        # Unicode should remain unchanged (not converted)
        assert result == hash_data


class TestNumericParamsCompatibility:
    """Test NumericParams compatibility class"""
    
    def test_compatibility_alias(self):
        """Test that NumericParams is a proper alias"""
        hash_data = {'a': '123', 'b': '456'}
        
        # Both should work the same way
        service_result = NumericParamsService(hash_data).to_h()
        compat_result = NumericParams(hash_data).to_h()
        
        assert service_result == compat_result
        assert service_result == {'a': 123, 'b': 456}
    
    def test_inheritance(self):
        """Test that NumericParams inherits from NumericParamsService"""
        assert issubclass(NumericParams, NumericParamsService)
        
        instance = NumericParams({'test': '123'})
        assert isinstance(instance, NumericParamsService)
        assert hasattr(instance, 'to_h')
        assert hasattr(instance, '_convert_value')