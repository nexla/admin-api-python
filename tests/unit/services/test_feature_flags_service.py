"""
Tests for FeatureFlagsService.
Tests feature flag evaluation, caching, and thread safety.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from threading import current_thread

from app.services.feature_flags_service import FeatureFlagsService, FeatureFlags


class TestFeatureFlagsService:
    """Test FeatureFlagsService functionality"""
    
    def setup_method(self):
        """Setup for each test method"""
        # Clear thread contexts between tests
        FeatureFlagsService._thread_contexts.clear()
    
    def test_feature_on_existing_feature(self):
        """Test feature_on for existing feature"""
        # Execute
        result = FeatureFlagsService.feature_on('test-feature')
        
        # Verify
        assert result is True
    
    def test_feature_on_nonexistent_feature(self):
        """Test feature_on for non-existent feature"""
        # Execute
        result = FeatureFlagsService.feature_on('nonexistent-feature')
        
        # Verify
        assert result is False
    
    def test_feature_on_with_attributes(self):
        """Test feature_on with matching attributes"""
        # Execute
        result = FeatureFlagsService.feature_on(
            'test-feature-flow-id',
            attributes={'flow_id': 1000}
        )
        
        # Verify
        assert result is True
    
    def test_feature_on_with_non_matching_attributes(self):
        """Test feature_on with non-matching attributes"""
        # Execute
        result = FeatureFlagsService.feature_on(
            'test-feature-flow-id',
            attributes={'flow_id': 1001}
        )
        
        # Verify
        assert result is False
    
    def test_feature_value_existing_feature(self):
        """Test feature_value for existing feature"""
        # Execute
        result = FeatureFlagsService.feature_value('test-feature-default', 'bye')
        
        # Verify
        assert result == 'hello'
    
    def test_feature_value_nonexistent_feature(self):
        """Test feature_value for non-existent feature returns default"""
        # Execute
        result = FeatureFlagsService.feature_value('fake-feature', 'bye')
        
        # Verify
        assert result == 'bye'
    
    def test_eval_feature_existing(self):
        """Test eval_feature for existing feature"""
        # Execute
        result = FeatureFlagsService.eval_feature('test-feature')
        
        # Verify
        assert result is True
    
    def test_eval_feature_with_attributes(self):
        """Test eval_feature with attributes"""
        # Execute
        result = FeatureFlagsService.eval_feature(
            'test-feature-flow-id',
            attributes={'flow_id': 1000}
        )
        
        # Verify
        assert result is True
    
    def test_eval_feature_nonexistent(self):
        """Test eval_feature for non-existent feature"""
        # Execute
        result = FeatureFlagsService.eval_feature('nonexistent-feature')
        
        # Verify
        assert result is None
    
    def test_thread_context_creation(self):
        """Test thread context is created and reused"""
        # First call creates context
        FeatureFlagsService.feature_on('test-feature')
        
        thread_id = current_thread().ident
        assert thread_id in FeatureFlagsService._thread_contexts
        
        first_context = FeatureFlagsService._thread_contexts[thread_id]
        
        # Second call reuses context
        FeatureFlagsService.feature_on('test-feature')
        second_context = FeatureFlagsService._thread_contexts[thread_id]
        
        assert first_context is second_context
    
    def test_thread_context_attribute_update(self):
        """Test thread context attributes are updated"""
        # First call with attributes
        FeatureFlagsService.feature_on('test-feature', attributes={'key1': 'value1'})
        
        thread_id = current_thread().ident
        context = FeatureFlagsService._thread_contexts[thread_id]
        assert context['attributes']['key1'] == 'value1'
        
        # Second call with additional attributes
        FeatureFlagsService.feature_on('test-feature', attributes={'key2': 'value2'})
        
        # Verify both attributes are present
        updated_context = FeatureFlagsService._thread_contexts[thread_id]
        assert updated_context['attributes']['key1'] == 'value1'
        assert updated_context['attributes']['key2'] == 'value2'
    
    def test_clear_thread_context(self):
        """Test clearing thread context"""
        # Create context
        FeatureFlagsService.feature_on('test-feature')
        
        thread_id = current_thread().ident
        assert thread_id in FeatureFlagsService._thread_contexts
        
        # Clear context
        FeatureFlagsService.clear_thread_context()
        
        # Verify context is cleared
        assert thread_id not in FeatureFlagsService._thread_contexts
    
    @patch('app.services.feature_flags_service.logger')
    def test_feature_usage_tracking_with_user_info(self, mock_logger):
        """Test feature usage tracking with user info"""
        # Setup mock user info
        mock_user = Mock()
        mock_user.id = 123
        mock_org = Mock()
        mock_org.id = 321
        mock_api_user_info = Mock()
        mock_api_user_info.user = mock_user
        mock_api_user_info.org = mock_org
        
        with patch.object(FeatureFlagsService, '_is_production', return_value=False):
            # Execute
            FeatureFlagsService.feature_on('test-feature', api_user_info=mock_api_user_info)
            
            # Verify logging was called with correct user info
            mock_logger.info.assert_called()
            log_call = mock_logger.info.call_args[0][0]
            assert 'user: 123' in log_call
            assert 'org: 321' in log_call
    
    @patch('app.services.feature_flags_service.logger')
    def test_feature_usage_tracking_without_user_info(self, mock_logger):
        """Test feature usage tracking without user info"""
        with patch.object(FeatureFlagsService, '_is_production', return_value=False):
            # Execute
            FeatureFlagsService.feature_on('test-feature')
            
            # Verify logging was called with system user
            mock_logger.info.assert_called()
            log_call = mock_logger.info.call_args[0][0]
            assert 'user: system_user' in log_call
            assert 'org: system_org' in log_call
    
    @patch('app.services.feature_flags_service.logger')
    def test_feature_usage_not_tracked_in_production(self, mock_logger):
        """Test feature usage is not tracked in production"""
        with patch.object(FeatureFlagsService, '_is_production', return_value=True):
            # Execute
            FeatureFlagsService.feature_on('test-feature')
            
            # Verify tracking was not called
            mock_logger.info.assert_not_called()
    
    def test_evaluate_rule_with_matching_condition(self):
        """Test rule evaluation with matching condition"""
        rule = {
            'condition': {'flow_id': 1000},
            'force': True
        }
        attributes = {'flow_id': 1000}
        
        result = FeatureFlagsService._evaluate_rule(rule, attributes)
        
        assert result is True
    
    def test_evaluate_rule_with_non_matching_condition(self):
        """Test rule evaluation with non-matching condition"""
        rule = {
            'condition': {'flow_id': 1000},
            'force': True
        }
        attributes = {'flow_id': 1001}
        
        result = FeatureFlagsService._evaluate_rule(rule, attributes)
        
        assert result is False
    
    def test_evaluate_rule_with_coverage(self):
        """Test rule evaluation with coverage"""
        rule = {
            'coverage': 0.5,
            'hashAttribute': 'id',
            'force': True
        }
        
        # Test with different IDs to check coverage
        # Note: This test is somewhat probabilistic due to hashing
        results = []
        for i in range(100):
            attributes = {'id': str(i)}
            result = FeatureFlagsService._evaluate_rule(rule, attributes)
            results.append(result)
        
        # Should have roughly 50% true results (within reasonable range)
        true_count = sum(results)
        assert 30 <= true_count <= 70  # Allow for hash distribution variance
    
    def test_evaluate_rule_no_conditions(self):
        """Test rule evaluation with no conditions always returns True"""
        rule = {'force': True}
        attributes = {'any': 'value'}
        
        result = FeatureFlagsService._evaluate_rule(rule, attributes)
        
        assert result is True
    
    def test_hash_attribute_consistency(self):
        """Test hash attribute produces consistent results"""
        value = "test_id_123"
        
        hash1 = FeatureFlagsService._hash_attribute(value)
        hash2 = FeatureFlagsService._hash_attribute(value)
        
        assert hash1 == hash2
        assert 0.0 <= hash1 <= 1.0
    
    def test_hash_attribute_different_values(self):
        """Test hash attribute produces different results for different values"""
        value1 = "test_id_123"
        value2 = "test_id_456"
        
        hash1 = FeatureFlagsService._hash_attribute(value1)
        hash2 = FeatureFlagsService._hash_attribute(value2)
        
        assert hash1 != hash2
    
    @patch.object(FeatureFlagsService, '_get_features')
    def test_error_handling_get_features_failure(self, mock_get_features):
        """Test error handling when getting features fails"""
        mock_get_features.side_effect = Exception("Service unavailable")
        
        # Should return False and not raise exception
        result = FeatureFlagsService.feature_on('test-feature')
        
        assert result is False
    
    @patch.object(FeatureFlagsService, '_evaluate_feature')
    def test_error_handling_evaluate_failure(self, mock_evaluate):
        """Test error handling when feature evaluation fails"""
        mock_evaluate.side_effect = Exception("Evaluation error")
        
        # Should return False and not raise exception
        result = FeatureFlagsService.feature_on('test-feature')
        
        assert result is False
    
    def test_evaluate_feature_no_rules(self):
        """Test feature evaluation with no rules returns default value"""
        feature_config = {
            'defaultValue': 'default_result',
            'rules': []
        }
        
        result = FeatureFlagsService._evaluate_feature(feature_config, {})
        
        assert result == 'default_result'
    
    def test_evaluate_feature_with_matching_rule(self):
        """Test feature evaluation with matching rule"""
        feature_config = {
            'defaultValue': False,
            'rules': [
                {
                    'condition': {'env': 'test'},
                    'force': True
                }
            ]
        }
        attributes = {'env': 'test'}
        
        result = FeatureFlagsService._evaluate_feature(feature_config, attributes)
        
        assert result is True
    
    def test_evaluate_feature_with_non_matching_rule(self):
        """Test feature evaluation with non-matching rule returns default"""
        feature_config = {
            'defaultValue': 'default_value',
            'rules': [
                {
                    'condition': {'env': 'production'},
                    'force': True
                }
            ]
        }
        attributes = {'env': 'test'}
        
        result = FeatureFlagsService._evaluate_feature(feature_config, attributes)
        
        assert result == 'default_value'


class TestFeatureFlags:
    """Test FeatureFlags compatibility class"""
    
    def test_feature_on_alias(self):
        """Test that FeatureFlags.feature_on calls FeatureFlagsService.feature_on"""
        with patch.object(FeatureFlagsService, 'feature_on', return_value=True) as mock_method:
            result = FeatureFlags.feature_on('test-feature')
            
            mock_method.assert_called_once_with('test-feature')
            assert result is True
    
    def test_feature_value_alias(self):
        """Test that FeatureFlags.feature_value calls FeatureFlagsService.feature_value"""
        with patch.object(FeatureFlagsService, 'feature_value', return_value='test') as mock_method:
            result = FeatureFlags.feature_value('test-feature', 'default')
            
            mock_method.assert_called_once_with('test-feature', 'default')
            assert result == 'test'
    
    def test_eval_feature_alias(self):
        """Test that FeatureFlags.eval_feature calls FeatureFlagsService.eval_feature"""
        with patch.object(FeatureFlagsService, 'eval_feature', return_value=True) as mock_method:
            result = FeatureFlags.eval_feature('test-feature')
            
            mock_method.assert_called_once_with('test-feature')
            assert result is True