"""
Template Variable Extractor - Extract variables from template strings.
Provides template parsing and variable extraction capabilities.
"""

import logging
import re
from typing import List, Optional

logger = logging.getLogger(__name__)


class TemplateVariableExtractor:
    """Extract variables from template strings with curly brace syntax"""
    
    def __init__(self, template: str):
        """
        Initialize template variable extractor.
        
        Args:
            template: Template string containing variables in {variable} format
        """
        self.template = template or ""
    
    def get(self) -> List[str]:
        """
        Extract variables from the template.
        
        Returns:
            List of variable names/expressions found in the template
        """
        try:
            if not self.template:
                return []
            
            variables = []
            i = 0
            
            while i < len(self.template):
                # Find opening brace
                start = self.template.find('{', i)
                if start == -1:
                    break
                
                # Find matching closing brace
                end = self._find_matching_brace(start)
                if end == -1:
                    # No matching brace found, move to next character
                    i = start + 1
                    continue
                
                # Extract variable content
                variable_content = self.template[start + 1:end]
                if variable_content:  # Only add non-empty variables
                    variables.append(variable_content)
                
                # Move past this variable
                i = end + 1
            
            return variables
            
        except Exception as e:
            logger.error(f"Error extracting template variables: {str(e)}")
            return []
    
    def _find_matching_brace(self, start: int) -> int:
        """
        Find the matching closing brace for an opening brace.
        Handles nested braces correctly.
        
        Args:
            start: Index of the opening brace
            
        Returns:
            Index of matching closing brace, or -1 if not found
        """
        try:
            if start >= len(self.template) or self.template[start] != '{':
                return -1
            
            brace_count = 1
            i = start + 1
            
            while i < len(self.template) and brace_count > 0:
                char = self.template[i]
                
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                
                i += 1
            
            # If brace_count is 0, we found the matching brace
            return i - 1 if brace_count == 0 else -1
            
        except Exception as e:
            logger.error(f"Error finding matching brace: {str(e)}")
            return -1
    
    @classmethod
    def extract_variables(cls, template: str) -> List[str]:
        """
        Convenience class method to extract variables from a template.
        
        Args:
            template: Template string
            
        Returns:
            List of extracted variables
        """
        extractor = cls(template)
        return extractor.get()