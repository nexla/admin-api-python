"""
Tests for TemplateVariableExtractor service.
Tests template parsing and variable extraction functionality.
"""

import pytest
from app.services.template_variable_extractor import TemplateVariableExtractor


class TestTemplateVariableExtractor:
    """Test TemplateVariableExtractor functionality"""
    
    def test_empty_template(self):
        """Test extraction from empty template returns empty array"""
        extractor = TemplateVariableExtractor('')
        result = extractor.get()
        
        assert result == []
    
    def test_none_template(self):
        """Test extraction from None template returns empty array"""
        extractor = TemplateVariableExtractor(None)
        result = extractor.get()
        
        assert result == []
    
    def test_template_without_variables(self):
        """Test extraction from template without variables"""
        extractor = TemplateVariableExtractor('just plain text')
        result = extractor.get()
        
        assert result == []
    
    def test_incomplete_template(self):
        """Test extraction from incomplete template returns empty array"""
        extractor = TemplateVariableExtractor('some_string{abc')
        result = extractor.get()
        
        assert result == []
    
    def test_simple_variables(self):
        """Test extraction of simple variables"""
        extractor = TemplateVariableExtractor('Hello {name}, welcome to {place}!')
        result = extractor.get()
        
        assert result == ['name', 'place']
    
    def test_complex_variables_with_json(self):
        """Test extraction of complex variables with JSON content"""
        template = 'some_string{abc}jkl{test={"123": [{"v": {}}, 123, {}]}}something'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['abc', 'test={"123": [{"v": {}}, 123, {}]}']
    
    def test_variables_with_incomplete_last_bracket(self):
        """Test extraction with incomplete last bracket"""
        template = 'some_string{abc}{def}jkl{test={"123": [{"v": {}}, 123, {}]}}something{failing_bracket'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['abc', 'def', 'test={"123": [{"v": {}}, 123, {}]}']
    
    def test_tricky_json_objects(self):
        """Test handling of tricky JSON objects"""
        template = '{"json_value": [{}, {variable={"key": "value", "another": [{}]}}, {123}]}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['variable={"key": "value", "another": [{}]}']
    
    def test_nested_braces(self):
        """Test handling of deeply nested braces"""
        template = '{outer{inner{deep}inner}outer}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['outer{inner{deep}inner}outer']
    
    def test_multiple_levels_of_nesting(self):
        """Test multiple levels of brace nesting"""
        template = 'start{level1{level2{level3}level2}level1}end{simple}finish'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['level1{level2{level3}level2}level1', 'simple']
    
    def test_empty_variables(self):
        """Test handling of empty variables"""
        template = 'text{}more{valid}text{}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        # Empty variables should be excluded
        assert result == ['valid']
    
    def test_adjacent_variables(self):
        """Test handling of adjacent variables"""
        template = '{first}{second}{third}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['first', 'second', 'third']
    
    def test_variables_with_special_characters(self):
        """Test variables containing special characters"""
        template = '{var_with_underscore}{var-with-dash}{var.with.dots}{var:with:colons}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['var_with_underscore', 'var-with-dash', 'var.with.dots', 'var:with:colons']
    
    def test_variables_with_spaces(self):
        """Test variables containing spaces"""
        template = '{variable with spaces}{another variable}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['variable with spaces', 'another variable']
    
    def test_mixed_content_template(self):
        """Test template with mixed content"""
        template = 'prefix{var1}middle{var2={"key": "value"}}suffix{var3}end'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['var1', 'var2={"key": "value"}', 'var3']
    
    def test_single_brace_characters(self):
        """Test template with single brace characters that don't form variables"""
        template = 'text with } single { braces } scattered around'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == [' single { braces ']
    
    def test_malformed_json_in_variable(self):
        """Test variable containing malformed JSON"""
        template = '{malformed={"key": value, "incomplete": [}}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['malformed={"key": value, "incomplete": [']
    
    def test_find_matching_brace_invalid_start(self):
        """Test _find_matching_brace with invalid start position"""
        extractor = TemplateVariableExtractor('test{var}')
        
        # Test with position not pointing to '{'
        result = extractor._find_matching_brace(0)
        assert result == -1
        
        # Test with position beyond string length
        result = extractor._find_matching_brace(100)
        assert result == -1
    
    def test_find_matching_brace_no_closing(self):
        """Test _find_matching_brace when no closing brace exists"""
        extractor = TemplateVariableExtractor('test{no_closing_brace')
        result = extractor._find_matching_brace(4)
        
        assert result == -1
    
    def test_find_matching_brace_simple(self):
        """Test _find_matching_brace with simple case"""
        extractor = TemplateVariableExtractor('test{simple}end')
        result = extractor._find_matching_brace(4)
        
        assert result == 11  # Position of closing brace
    
    def test_find_matching_brace_nested(self):
        """Test _find_matching_brace with nested braces"""
        extractor = TemplateVariableExtractor('test{outer{inner}outer}end')
        result = extractor._find_matching_brace(4)
        
        assert result == 22  # Position of final closing brace
    
    def test_class_method_extract_variables(self):
        """Test class method convenience function"""
        template = 'test{var1}and{var2}'
        result = TemplateVariableExtractor.extract_variables(template)
        
        assert result == ['var1', 'var2']
    
    def test_unicode_content(self):
        """Test handling of unicode content in variables"""
        template = '{variable_with_unicode_🚀}{another_var_with_中文}'
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        assert result == ['variable_with_unicode_🚀', 'another_var_with_中文']
    
    def test_very_long_template(self):
        """Test handling of very long templates"""
        # Create a template with many variables
        template = ''.join([f'prefix{i}{{{f"var_{i}"}}}suffix{i}' for i in range(100)])
        extractor = TemplateVariableExtractor(template)
        result = extractor.get()
        
        expected = [f'var_{i}' for i in range(100)]
        assert result == expected
    
    def test_error_handling(self):
        """Test error handling in get method"""
        # Create an extractor that might cause errors
        extractor = TemplateVariableExtractor('normal template {var}')
        
        # Mock an error in the method
        with pytest.mock.patch.object(extractor, '_find_matching_brace', side_effect=Exception("Test error")):
            result = extractor.get()
            
            # Should return empty list on error
            assert result == []