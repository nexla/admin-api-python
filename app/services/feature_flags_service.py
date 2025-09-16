"""
Feature Flags Service - Manage feature flags and toggles.
Provides feature flag evaluation, caching, and thread-safe access.
"""

import logging
import json
from typing import Optional, Dict, Any, Union
from threading import current_thread
import hashlib

from ..config import settings

logger = logging.getLogger(__name__)


class FeatureFlagsService:
    """Service for feature flag management and evaluation"""
    
    # Thread-local storage for feature flag contexts
    _thread_contexts = {}
    
    @classmethod
    def feature_on(
        cls,
        feature_key: str,
        attributes: Optional[Dict[str, Any]] = None,
        api_user_info: Optional[Any] = None
    ) -> bool:
        """
        Check if a feature flag is enabled.
        
        Args:
            feature_key: The feature flag key to check
            attributes: Optional attributes for feature evaluation
            api_user_info: Optional user info for tracking
            
        Returns:
            True if feature is enabled, False otherwise
        """
        try:
            # Get or create feature context for this thread
            context = cls._get_or_create_context(attributes, api_user_info)
            
            # Get feature configuration
            features = cls._get_features()
            feature_config = features.get(feature_key)
            
            if not feature_config:
                logger.warning(f"Feature flag '{feature_key}' not found, using default value")
                return False
            
            # Evaluate feature
            result = cls._evaluate_feature(feature_config, attributes)
            
            # Track feature usage in non-production environments
            if not cls._is_production():
                cls._track_feature_usage(feature_key, result, api_user_info)
            
            return result
            
        except Exception as e:
            logger.error(f"Error evaluating feature flag '{feature_key}': {str(e)}")
            return False
    
    @classmethod
    def feature_value(
        cls,
        feature_key: str,
        default_value: Any,
        attributes: Optional[Dict[str, Any]] = None,
        api_user_info: Optional[Any] = None
    ) -> Any:
        """
        Get the value of a feature flag.
        
        Args:
            feature_key: The feature flag key to get
            default_value: Default value if feature not found
            attributes: Optional attributes for feature evaluation
            api_user_info: Optional user info for tracking
            
        Returns:
            Feature value or default_value if not found
        """
        try:
            # Get feature configuration
            features = cls._get_features()
            feature_config = features.get(feature_key)
            
            if not feature_config:
                logger.info(f"Feature flag '{feature_key}' not found, using default value")
                return default_value
            
            # Get default value from config
            return feature_config.get('defaultValue', default_value)
            
        except Exception as e:
            logger.error(f"Error getting feature value '{feature_key}': {str(e)}")
            return default_value
    
    @classmethod
    def eval_feature(
        cls,
        feature_key: str,
        attributes: Optional[Dict[str, Any]] = None,
        api_user_info: Optional[Any] = None
    ) -> Any:
        """
        Evaluate a feature flag and return its value.
        
        Args:
            feature_key: The feature flag key to evaluate
            attributes: Optional attributes for feature evaluation
            api_user_info: Optional user info for tracking
            
        Returns:
            Evaluated feature value
        """
        try:
            # Get feature configuration
            features = cls._get_features()
            feature_config = features.get(feature_key)
            
            if not feature_config:
                logger.warning(f"Feature flag '{feature_key}' not found")
                return None
            
            # Evaluate feature with rules
            return cls._evaluate_feature(feature_config, attributes)
            
        except Exception as e:
            logger.error(f"Error evaluating feature '{feature_key}': {str(e)}")
            return None
    
    @classmethod
    def _get_or_create_context(cls, attributes: Optional[Dict[str, Any]], api_user_info: Optional[Any]) -> Dict:
        """Get or create feature evaluation context for current thread"""
        thread_id = current_thread().ident
        
        if thread_id not in cls._thread_contexts:
            cls._thread_contexts[thread_id] = {
                'attributes': attributes or {},
                'api_user_info': api_user_info
            }
        else:
            # Update attributes if they've changed
            if attributes:
                cls._thread_contexts[thread_id]['attributes'].update(attributes)
        
        return cls._thread_contexts[thread_id]
    
    @classmethod
    def _get_features(cls) -> Dict[str, Any]:
        """Get feature configuration from cache or source"""
        try:
            # In a real implementation, this would fetch from GrowthBook or similar service
            # For now, return mock configuration
            return cls._get_mock_features()
            
        except Exception as e:
            logger.error(f"Failed to get feature configuration: {str(e)}")
            return {}
    
    @classmethod
    def _get_mock_features(cls) -> Dict[str, Any]:
        """Get mock feature configuration for testing"""
        return {
            "test-feature": {
                "defaultValue": True,
                "rules": [
                    {
                        "id": "fr_19g62cm8emnlfc",
                        "force": True,
                        "coverage": 0.5,
                        "hashAttribute": "id"
                    }
                ]
            },
            "test-feature-flow-id": {
                "defaultValue": False,
                "rules": [
                    {
                        "id": "fr_19g61wm8eqosel",
                        "condition": {
                            "flow_id": 1000
                        },
                        "force": True
                    }
                ]
            },
            "test-feature-default": {
                "defaultValue": "hello",
                "rules": []
            }
        }
    
    @classmethod
    def _evaluate_feature(cls, feature_config: Dict[str, Any], attributes: Optional[Dict[str, Any]]) -> Any:
        """Evaluate a feature based on its configuration and attributes"""
        try:
            default_value = feature_config.get('defaultValue', False)
            rules = feature_config.get('rules', [])
            
            # If no rules, return default value
            if not rules:
                return default_value
            
            # Evaluate rules in order
            for rule in rules:
                if cls._evaluate_rule(rule, attributes):
                    return rule.get('force', default_value)
            
            # No rules matched, return default
            return default_value
            
        except Exception as e:
            logger.error(f"Error evaluating feature: {str(e)}")
            return feature_config.get('defaultValue', False)
    
    @classmethod
    def _evaluate_rule(cls, rule: Dict[str, Any], attributes: Optional[Dict[str, Any]]) -> bool:
        """Evaluate a single feature rule"""
        try:
            # Check conditions if present
            condition = rule.get('condition')
            if condition and attributes:
                for key, value in condition.items():
                    if attributes.get(key) != value:
                        return False
            
            # Check coverage if present
            coverage = rule.get('coverage')
            if coverage is not None and attributes:
                hash_attribute = rule.get('hashAttribute', 'id')
                if hash_attribute in attributes:
                    # Simple hash-based coverage check
                    hash_value = cls._hash_attribute(str(attributes[hash_attribute]))
                    if hash_value > coverage:
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error evaluating rule: {str(e)}")
            return False
    
    @classmethod
    def _hash_attribute(cls, value: str) -> float:
        """Hash an attribute value to a float between 0 and 1"""
        try:
            # Simple hash function for coverage testing
            hash_obj = hashlib.md5(value.encode())
            hash_int = int(hash_obj.hexdigest()[:8], 16)
            return hash_int / 0xFFFFFFFF
        except Exception:
            return 0.5
    
    @classmethod
    def _track_feature_usage(cls, feature_key: str, result: Any, api_user_info: Optional[Any]):
        """Track feature usage for analytics"""
        try:
            if api_user_info:
                user_id = getattr(api_user_info.user, 'id', None) if hasattr(api_user_info, 'user') else None
                org_id = getattr(api_user_info.org, 'id', None) if hasattr(api_user_info, 'org') else None
            else:
                user_id = 'system_user'
                org_id = 'system_org'
            
            logger.info(
                f"Feature usage: {feature_key} = {result} "
                f"(user: {user_id}, org: {org_id})"
            )
            
            # In a real implementation, this would send to analytics service
            
        except Exception as e:
            logger.error(f"Error tracking feature usage: {str(e)}")
    
    @classmethod
    def _is_production(cls) -> bool:
        """Check if running in production environment"""
        try:
            return getattr(settings, 'ENVIRONMENT', 'development').lower() == 'production'
        except Exception:
            return False
    
    @classmethod
    def clear_thread_context(cls):
        """Clear feature flag context for current thread"""
        thread_id = current_thread().ident
        if thread_id in cls._thread_contexts:
            del cls._thread_contexts[thread_id]


# Convenience aliases for Rails compatibility
class FeatureFlags:
    """Alias for FeatureFlagsService for Rails compatibility"""
    
    @staticmethod
    def feature_on(*args, **kwargs):
        return FeatureFlagsService.feature_on(*args, **kwargs)
    
    @staticmethod
    def feature_value(*args, **kwargs):
        return FeatureFlagsService.feature_value(*args, **kwargs)
    
    @staticmethod
    def eval_feature(*args, **kwargs):
        return FeatureFlagsService.eval_feature(*args, **kwargs)