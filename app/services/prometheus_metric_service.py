"""
Prometheus Metric Service - Collect and expose metrics for monitoring.
Provides metric collection, registration, and Prometheus integration.
"""

import logging
from typing import Dict, Any, Optional, Union
from threading import Lock
from collections import defaultdict
import threading

from ..config import settings

logger = logging.getLogger(__name__)


class PrometheusMetricService:
    """Singleton service for Prometheus metrics collection"""
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(PrometheusMetricService, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._metrics = {}
        self._metric_locks = defaultdict(Lock)
        self._client = None
        self._enabled = getattr(settings, 'PROMETHEUS_ENABLED', True)
        self._initialized = True
        
        if self._enabled:
            self._initialize_client()
    
    @classmethod
    def instance(cls) -> 'PrometheusMetricService':
        """Get singleton instance"""
        return cls()
    
    def _initialize_client(self):
        """Initialize Prometheus client"""
        try:
            # In a real implementation, this would initialize PrometheusExporter::Client
            # For now, create a mock client
            self._client = MockPrometheusClient()
            logger.info("Prometheus metrics client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Prometheus client: {str(e)}")
            self._client = None
            self._enabled = False
    
    def counter(self, metric_name: str, description: Optional[str] = None) -> 'MetricCounter':
        """
        Get or create a counter metric.
        
        Args:
            metric_name: Name of the metric
            description: Optional description of the metric
            
        Returns:
            MetricCounter instance
        """
        if not self._enabled or not self._client:
            return NoOpMetricCounter()
        
        with self._metric_locks[metric_name]:
            if metric_name not in self._metrics:
                # Register new counter with client
                self._client.register(
                    metric_type='counter',
                    name=metric_name,
                    description=description or f"{metric_name} metric"
                )
                
                self._metrics[metric_name] = MetricCounter(
                    name=metric_name,
                    client=self._client
                )
                
                logger.debug(f"Registered new counter metric: {metric_name}")
            
            return self._metrics[metric_name]
    
    def histogram(self, metric_name: str, description: Optional[str] = None) -> 'MetricHistogram':
        """
        Get or create a histogram metric.
        
        Args:
            metric_name: Name of the metric
            description: Optional description of the metric
            
        Returns:
            MetricHistogram instance
        """
        if not self._enabled or not self._client:
            return NoOpMetricHistogram()
        
        with self._metric_locks[metric_name]:
            if metric_name not in self._metrics:
                # Register new histogram with client
                self._client.register(
                    metric_type='histogram',
                    name=metric_name,
                    description=description or f"{metric_name} metric"
                )
                
                self._metrics[metric_name] = MetricHistogram(
                    name=metric_name,
                    client=self._client
                )
                
                logger.debug(f"Registered new histogram metric: {metric_name}")
            
            return self._metrics[metric_name]
    
    def gauge(self, metric_name: str, description: Optional[str] = None) -> 'MetricGauge':
        """
        Get or create a gauge metric.
        
        Args:
            metric_name: Name of the metric
            description: Optional description of the metric
            
        Returns:
            MetricGauge instance
        """
        if not self._enabled or not self._client:
            return NoOpMetricGauge()
        
        with self._metric_locks[metric_name]:
            if metric_name not in self._metrics:
                # Register new gauge with client
                self._client.register(
                    metric_type='gauge',
                    name=metric_name,
                    description=description or f"{metric_name} metric"
                )
                
                self._metrics[metric_name] = MetricGauge(
                    name=metric_name,
                    client=self._client
                )
                
                logger.debug(f"Registered new gauge metric: {metric_name}")
            
            return self._metrics[metric_name]
    
    @classmethod
    def observe(
        cls,
        metric_name: str,
        metric_type: str = 'counter',
        value: Union[int, float] = 1,
        labels: Optional[Dict[str, str]] = None
    ):
        """
        Convenience method to observe a metric value.
        
        Args:
            metric_name: Name of the metric
            metric_type: Type of metric (counter, histogram, gauge)
            value: Value to observe
            labels: Optional labels for the metric
        """
        try:
            instance = cls.instance()
            
            if metric_type == 'counter':
                metric = instance.counter(metric_name)
                metric.observe(value, labels)
            elif metric_type == 'histogram':
                metric = instance.histogram(metric_name)
                metric.observe(value, labels)
            elif metric_type == 'gauge':
                metric = instance.gauge(metric_name)
                metric.set(value, labels)
            else:
                logger.warning(f"Unknown metric type: {metric_type}")
                
        except Exception as e:
            logger.error(f"Failed to observe metric {metric_name}: {str(e)}")


class MetricCounter:
    """Counter metric implementation"""
    
    def __init__(self, name: str, client: Any):
        self.name = name
        self.client = client
        self._value = 0
        self._lock = Lock()
    
    def observe(self, value: Union[int, float] = 1, labels: Optional[Dict[str, str]] = None):
        """Increment counter by value"""
        try:
            with self._lock:
                self._value += value
                if self.client:
                    self.client.counter_observe(self.name, value, labels)
        except Exception as e:
            logger.error(f"Failed to observe counter {self.name}: {str(e)}")
    
    def increment(self, labels: Optional[Dict[str, str]] = None):
        """Increment counter by 1"""
        self.observe(1, labels)
    
    def get_value(self) -> Union[int, float]:
        """Get current counter value"""
        return self._value


class MetricHistogram:
    """Histogram metric implementation"""
    
    def __init__(self, name: str, client: Any):
        self.name = name
        self.client = client
        self._observations = []
        self._lock = Lock()
    
    def observe(self, value: Union[int, float], labels: Optional[Dict[str, str]] = None):
        """Record a histogram observation"""
        try:
            with self._lock:
                self._observations.append(value)
                if self.client:
                    self.client.histogram_observe(self.name, value, labels)
        except Exception as e:
            logger.error(f"Failed to observe histogram {self.name}: {str(e)}")
    
    def get_observations(self) -> list:
        """Get all observations"""
        return self._observations.copy()


class MetricGauge:
    """Gauge metric implementation"""
    
    def __init__(self, name: str, client: Any):
        self.name = name
        self.client = client
        self._value = 0
        self._lock = Lock()
    
    def set(self, value: Union[int, float], labels: Optional[Dict[str, str]] = None):
        """Set gauge value"""
        try:
            with self._lock:
                self._value = value
                if self.client:
                    self.client.gauge_set(self.name, value, labels)
        except Exception as e:
            logger.error(f"Failed to set gauge {self.name}: {str(e)}")
    
    def increment(self, value: Union[int, float] = 1, labels: Optional[Dict[str, str]] = None):
        """Increment gauge value"""
        self.set(self._value + value, labels)
    
    def decrement(self, value: Union[int, float] = 1, labels: Optional[Dict[str, str]] = None):
        """Decrement gauge value"""
        self.set(self._value - value, labels)
    
    def get_value(self) -> Union[int, float]:
        """Get current gauge value"""
        return self._value


# No-op implementations for when metrics are disabled
class NoOpMetricCounter:
    def observe(self, value=1, labels=None): pass
    def increment(self, labels=None): pass
    def get_value(self): return 0

class NoOpMetricHistogram:
    def observe(self, value, labels=None): pass
    def get_observations(self): return []

class NoOpMetricGauge:
    def set(self, value, labels=None): pass
    def increment(self, value=1, labels=None): pass
    def decrement(self, value=1, labels=None): pass
    def get_value(self): return 0


class MockPrometheusClient:
    """Mock Prometheus client for testing and development"""
    
    def __init__(self):
        self._registered_metrics = {}
    
    def register(self, metric_type: str, name: str, description: str):
        """Register a metric with the client"""
        self._registered_metrics[name] = {
            'type': metric_type,
            'description': description
        }
        logger.debug(f"Mock client registered {metric_type} metric: {name}")
    
    def counter_observe(self, name: str, value: Union[int, float], labels: Optional[Dict[str, str]]):
        """Record counter observation"""
        logger.debug(f"Counter {name}: +{value} {labels or ''}")
    
    def histogram_observe(self, name: str, value: Union[int, float], labels: Optional[Dict[str, str]]):
        """Record histogram observation"""
        logger.debug(f"Histogram {name}: {value} {labels or ''}")
    
    def gauge_set(self, name: str, value: Union[int, float], labels: Optional[Dict[str, str]]):
        """Set gauge value"""
        logger.debug(f"Gauge {name}: {value} {labels or ''}")
    
    def get_registered_metrics(self) -> Dict[str, Dict[str, str]]:
        """Get all registered metrics (for testing)"""
        return self._registered_metrics.copy()


# Compatibility alias for Rails naming
class PrometheusMetric:
    """Compatibility class for Rails naming"""
    
    @classmethod
    def instance(cls):
        return PrometheusMetricService.instance()
    
    @classmethod
    def observe(cls, metric_name: str, metric_type: str = 'counter', value: Union[int, float] = 1):
        PrometheusMetricService.observe(metric_name, metric_type, value)


# Global instance for easy access
prometheus_metrics = PrometheusMetricService.instance()