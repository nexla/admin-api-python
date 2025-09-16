"""
Tests for PrometheusMetricService.
Tests metric collection, registration, and Prometheus integration.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from app.services.prometheus_metric_service import (
    PrometheusMetricService, 
    PrometheusMetric,
    MetricCounter, 
    MetricHistogram, 
    MetricGauge,
    MockPrometheusClient,
    NoOpMetricCounter,
    NoOpMetricHistogram,
    NoOpMetricGauge
)


class TestPrometheusMetricService:
    """Test PrometheusMetricService functionality"""
    
    def setup_method(self):
        """Setup for each test method"""
        # Clear singleton instance between tests
        PrometheusMetricService._instance = None
    
    def test_singleton_pattern(self):
        """Test singleton pattern works correctly"""
        instance1 = PrometheusMetricService.instance()
        instance2 = PrometheusMetricService.instance()
        
        assert instance1 is instance2
    
    def test_initialization_enabled(self):
        """Test service initialization when enabled"""
        with patch('app.services.prometheus_metric_service.settings') as mock_settings:
            mock_settings.PROMETHEUS_ENABLED = True
            
            service = PrometheusMetricService()
            
            assert service._enabled is True
            assert service._client is not None
    
    def test_initialization_disabled(self):
        """Test service initialization when disabled"""
        with patch('app.services.prometheus_metric_service.settings') as mock_settings:
            mock_settings.PROMETHEUS_ENABLED = False
            
            service = PrometheusMetricService()
            
            assert service._enabled is False
    
    def test_counter_metric_registration(self):
        """Test counter metric registration"""
        service = PrometheusMetricService()
        service._enabled = True
        service._client = MockPrometheusClient()
        
        # Get counter metric
        counter = service.counter('test_counter', 'Test counter metric')
        
        # Verify metric was registered
        assert 'test_counter' in service._metrics
        assert isinstance(counter, MetricCounter)
        assert counter.name == 'test_counter'
        
        # Verify client registration
        registered = service._client.get_registered_metrics()
        assert 'test_counter' in registered
        assert registered['test_counter']['type'] == 'counter'
    
    def test_counter_metric_reuse(self):
        """Test that counter metrics are reused, not recreated"""
        service = PrometheusMetricService()
        service._enabled = True
        service._client = MockPrometheusClient()
        
        # Get same counter twice
        counter1 = service.counter('test_counter')
        counter2 = service.counter('test_counter')
        
        # Should be same instance
        assert counter1 is counter2
    
    def test_histogram_metric_registration(self):
        """Test histogram metric registration"""
        service = PrometheusMetricService()
        service._enabled = True
        service._client = MockPrometheusClient()
        
        # Get histogram metric
        histogram = service.histogram('test_histogram', 'Test histogram metric')
        
        # Verify metric was registered
        assert 'test_histogram' in service._metrics
        assert isinstance(histogram, MetricHistogram)
        assert histogram.name == 'test_histogram'
        
        # Verify client registration
        registered = service._client.get_registered_metrics()
        assert 'test_histogram' in registered
        assert registered['test_histogram']['type'] == 'histogram'
    
    def test_gauge_metric_registration(self):
        """Test gauge metric registration"""
        service = PrometheusMetricService()
        service._enabled = True
        service._client = MockPrometheusClient()
        
        # Get gauge metric
        gauge = service.gauge('test_gauge', 'Test gauge metric')
        
        # Verify metric was registered
        assert 'test_gauge' in service._metrics
        assert isinstance(gauge, MetricGauge)
        assert gauge.name == 'test_gauge'
        
        # Verify client registration
        registered = service._client.get_registered_metrics()
        assert 'test_gauge' in registered
        assert registered['test_gauge']['type'] == 'gauge'
    
    def test_metrics_when_disabled(self):
        """Test that no-op metrics are returned when service is disabled"""
        service = PrometheusMetricService()
        service._enabled = False
        service._client = None
        
        counter = service.counter('test_counter')
        histogram = service.histogram('test_histogram')
        gauge = service.gauge('test_gauge')
        
        assert isinstance(counter, NoOpMetricCounter)
        assert isinstance(histogram, NoOpMetricHistogram)
        assert isinstance(gauge, NoOpMetricGauge)
    
    def test_observe_class_method_counter(self):
        """Test observe class method for counter"""
        with patch.object(PrometheusMetricService, 'instance') as mock_instance:
            mock_service = Mock()
            mock_counter = Mock()
            mock_service.counter.return_value = mock_counter
            mock_instance.return_value = mock_service
            
            # Execute
            PrometheusMetricService.observe('test_counter', 'counter', 5)
            
            # Verify
            mock_service.counter.assert_called_once_with('test_counter')
            mock_counter.observe.assert_called_once_with(5, None)
    
    def test_observe_class_method_histogram(self):
        """Test observe class method for histogram"""
        with patch.object(PrometheusMetricService, 'instance') as mock_instance:
            mock_service = Mock()
            mock_histogram = Mock()
            mock_service.histogram.return_value = mock_histogram
            mock_instance.return_value = mock_service
            
            # Execute
            PrometheusMetricService.observe('test_histogram', 'histogram', 1.5)
            
            # Verify
            mock_service.histogram.assert_called_once_with('test_histogram')
            mock_histogram.observe.assert_called_once_with(1.5, None)
    
    def test_observe_class_method_gauge(self):
        """Test observe class method for gauge"""
        with patch.object(PrometheusMetricService, 'instance') as mock_instance:
            mock_service = Mock()
            mock_gauge = Mock()
            mock_service.gauge.return_value = mock_gauge
            mock_instance.return_value = mock_service
            
            # Execute
            PrometheusMetricService.observe('test_gauge', 'gauge', 100)
            
            # Verify
            mock_service.gauge.assert_called_once_with('test_gauge')
            mock_gauge.set.assert_called_once_with(100, None)
    
    @patch('app.services.prometheus_metric_service.logger')
    def test_observe_unknown_metric_type(self, mock_logger):
        """Test observe with unknown metric type"""
        with patch.object(PrometheusMetricService, 'instance') as mock_instance:
            mock_instance.return_value = Mock()
            
            # Execute
            PrometheusMetricService.observe('test_metric', 'unknown_type', 1)
            
            # Verify warning was logged
            mock_logger.warning.assert_called_once()


class TestMetricCounter:
    """Test MetricCounter functionality"""
    
    def test_counter_observe(self):
        """Test counter observation"""
        mock_client = Mock()
        counter = MetricCounter('test_counter', mock_client)
        
        # Execute
        counter.observe(5)
        
        # Verify
        assert counter.get_value() == 5
        mock_client.counter_observe.assert_called_once_with('test_counter', 5, None)
    
    def test_counter_observe_with_labels(self):
        """Test counter observation with labels"""
        mock_client = Mock()
        counter = MetricCounter('test_counter', mock_client)
        labels = {'method': 'GET', 'status': '200'}
        
        # Execute
        counter.observe(3, labels)
        
        # Verify
        mock_client.counter_observe.assert_called_once_with('test_counter', 3, labels)
    
    def test_counter_increment(self):
        """Test counter increment"""
        mock_client = Mock()
        counter = MetricCounter('test_counter', mock_client)
        
        # Execute
        counter.increment()
        
        # Verify
        assert counter.get_value() == 1
        mock_client.counter_observe.assert_called_once_with('test_counter', 1, None)
    
    def test_counter_multiple_observations(self):
        """Test multiple counter observations accumulate"""
        mock_client = Mock()
        counter = MetricCounter('test_counter', mock_client)
        
        # Execute
        counter.observe(5)
        counter.observe(3)
        counter.increment()
        
        # Verify total
        assert counter.get_value() == 9
    
    @patch('app.services.prometheus_metric_service.logger')
    def test_counter_error_handling(self, mock_logger):
        """Test counter error handling"""
        mock_client = Mock()
        mock_client.counter_observe.side_effect = Exception("Client error")
        counter = MetricCounter('test_counter', mock_client)
        
        # Execute - should not raise exception
        counter.observe(1)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()


class TestMetricHistogram:
    """Test MetricHistogram functionality"""
    
    def test_histogram_observe(self):
        """Test histogram observation"""
        mock_client = Mock()
        histogram = MetricHistogram('test_histogram', mock_client)
        
        # Execute
        histogram.observe(1.5)
        
        # Verify
        observations = histogram.get_observations()
        assert observations == [1.5]
        mock_client.histogram_observe.assert_called_once_with('test_histogram', 1.5, None)
    
    def test_histogram_observe_with_labels(self):
        """Test histogram observation with labels"""
        mock_client = Mock()
        histogram = MetricHistogram('test_histogram', mock_client)
        labels = {'endpoint': '/api/test'}
        
        # Execute
        histogram.observe(2.3, labels)
        
        # Verify
        mock_client.histogram_observe.assert_called_once_with('test_histogram', 2.3, labels)
    
    def test_histogram_multiple_observations(self):
        """Test multiple histogram observations"""
        mock_client = Mock()
        histogram = MetricHistogram('test_histogram', mock_client)
        
        # Execute
        histogram.observe(1.0)
        histogram.observe(2.0)
        histogram.observe(1.5)
        
        # Verify all observations are stored
        observations = histogram.get_observations()
        assert observations == [1.0, 2.0, 1.5]
    
    @patch('app.services.prometheus_metric_service.logger')
    def test_histogram_error_handling(self, mock_logger):
        """Test histogram error handling"""
        mock_client = Mock()
        mock_client.histogram_observe.side_effect = Exception("Client error")
        histogram = MetricHistogram('test_histogram', mock_client)
        
        # Execute - should not raise exception
        histogram.observe(1.0)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()


class TestMetricGauge:
    """Test MetricGauge functionality"""
    
    def test_gauge_set(self):
        """Test gauge set operation"""
        mock_client = Mock()
        gauge = MetricGauge('test_gauge', mock_client)
        
        # Execute
        gauge.set(42)
        
        # Verify
        assert gauge.get_value() == 42
        mock_client.gauge_set.assert_called_once_with('test_gauge', 42, None)
    
    def test_gauge_set_with_labels(self):
        """Test gauge set with labels"""
        mock_client = Mock()
        gauge = MetricGauge('test_gauge', mock_client)
        labels = {'instance': 'web-1'}
        
        # Execute
        gauge.set(100, labels)
        
        # Verify
        mock_client.gauge_set.assert_called_once_with('test_gauge', 100, labels)
    
    def test_gauge_increment(self):
        """Test gauge increment"""
        mock_client = Mock()
        gauge = MetricGauge('test_gauge', mock_client)
        
        # Execute
        gauge.set(10)
        gauge.increment(5)
        
        # Verify
        assert gauge.get_value() == 15
        # Should have called set twice
        assert mock_client.gauge_set.call_count == 2
    
    def test_gauge_decrement(self):
        """Test gauge decrement"""
        mock_client = Mock()
        gauge = MetricGauge('test_gauge', mock_client)
        
        # Execute
        gauge.set(20)
        gauge.decrement(7)
        
        # Verify
        assert gauge.get_value() == 13
        assert mock_client.gauge_set.call_count == 2
    
    @patch('app.services.prometheus_metric_service.logger')
    def test_gauge_error_handling(self, mock_logger):
        """Test gauge error handling"""
        mock_client = Mock()
        mock_client.gauge_set.side_effect = Exception("Client error")
        gauge = MetricGauge('test_gauge', mock_client)
        
        # Execute - should not raise exception
        gauge.set(50)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()


class TestNoOpMetrics:
    """Test no-op metric implementations"""
    
    def test_noop_counter(self):
        """Test no-op counter does nothing"""
        counter = NoOpMetricCounter()
        
        # Should not raise exceptions
        counter.observe(5)
        counter.increment()
        
        # Always returns 0
        assert counter.get_value() == 0
    
    def test_noop_histogram(self):
        """Test no-op histogram does nothing"""
        histogram = NoOpMetricHistogram()
        
        # Should not raise exceptions
        histogram.observe(1.5)
        
        # Always returns empty list
        assert histogram.get_observations() == []
    
    def test_noop_gauge(self):
        """Test no-op gauge does nothing"""
        gauge = NoOpMetricGauge()
        
        # Should not raise exceptions
        gauge.set(100)
        gauge.increment(5)
        gauge.decrement(3)
        
        # Always returns 0
        assert gauge.get_value() == 0


class TestMockPrometheusClient:
    """Test MockPrometheusClient functionality"""
    
    def test_register_metric(self):
        """Test metric registration"""
        client = MockPrometheusClient()
        
        # Execute
        client.register('counter', 'test_counter', 'Test counter description')
        
        # Verify
        registered = client.get_registered_metrics()
        assert 'test_counter' in registered
        assert registered['test_counter']['type'] == 'counter'
        assert registered['test_counter']['description'] == 'Test counter description'
    
    @patch('app.services.prometheus_metric_service.logger')
    def test_counter_observe_logging(self, mock_logger):
        """Test counter observe logging"""
        client = MockPrometheusClient()
        
        # Execute
        client.counter_observe('test_counter', 5, {'label': 'value'})
        
        # Verify debug log
        mock_logger.debug.assert_called_once()
        log_message = mock_logger.debug.call_args[0][0]
        assert 'Counter test_counter: +5' in log_message
    
    @patch('app.services.prometheus_metric_service.logger')
    def test_histogram_observe_logging(self, mock_logger):
        """Test histogram observe logging"""
        client = MockPrometheusClient()
        
        # Execute
        client.histogram_observe('test_histogram', 1.5, None)
        
        # Verify debug log
        mock_logger.debug.assert_called_once()
        log_message = mock_logger.debug.call_args[0][0]
        assert 'Histogram test_histogram: 1.5' in log_message
    
    @patch('app.services.prometheus_metric_service.logger')
    def test_gauge_set_logging(self, mock_logger):
        """Test gauge set logging"""
        client = MockPrometheusClient()
        
        # Execute
        client.gauge_set('test_gauge', 100, {'env': 'prod'})
        
        # Verify debug log
        mock_logger.debug.assert_called_once()
        log_message = mock_logger.debug.call_args[0][0]
        assert 'Gauge test_gauge: 100' in log_message


class TestPrometheusMetricCompatibility:
    """Test PrometheusMetric compatibility class"""
    
    def test_instance_method(self):
        """Test PrometheusMetric.instance() returns service instance"""
        result = PrometheusMetric.instance()
        
        assert isinstance(result, PrometheusMetricService)
    
    def test_observe_method(self):
        """Test PrometheusMetric.observe() calls service method"""
        with patch.object(PrometheusMetricService, 'observe') as mock_observe:
            # Execute
            PrometheusMetric.observe('test_metric', 'counter', 5)
            
            # Verify
            mock_observe.assert_called_once_with('test_metric', 'counter', 5)