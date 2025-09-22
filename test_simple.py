#!/usr/bin/env python3
"""
Simple test that doesn't import all models to verify pytest works
"""
import pytest

def test_basic_functionality():
    """Test basic Python functionality"""
    assert 1 + 1 == 2
    assert "hello" == "hello"
    assert [1, 2, 3] == [1, 2, 3]

def test_imports():
    """Test that we can import basic packages"""
    import json
    import datetime
    import os
    
    # Test basic functionality
    data = {"test": "value"}
    json_str = json.dumps(data)
    parsed = json.loads(json_str)
    assert parsed["test"] == "value"
    
    now = datetime.datetime.now()
    assert isinstance(now, datetime.datetime)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])