"""
Integration Coordinator - Manages integration between all services
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from .prometheus_metric_service import PrometheusMetricService
from .feature_flags_service import FeatureFlagsService
from .audit_service import AuditService
from .encryption_service import EncryptionService
from .request_logger_service import RequestLoggerService
from .validation_service import ValidationService

logger = logging.getLogger(__name__)


class IntegrationCoordinator:
    """Coordinates integration between all application services"""
    
    def __init__(self):
        self._initialized = False
        self._services = {}
        self._health_status = {}
        
    async def initialize(self):
        """Initialize all services and their integrations"""
        if self._initialized:
            return
            
        logger.info("Initializing service integrations...")
        
        try:
            # Initialize core services
            await self._initialize_core_services()
            
            # Initialize monitoring services
            await self._initialize_monitoring_services()
            
            # Initialize security services
            await self._initialize_security_services()
            
            # Perform health checks
            await self._perform_health_checks()
            
            self._initialized = True
            logger.info("Service integrations initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize service integrations: {str(e)}")
            raise
    
    async def _initialize_core_services(self):
        """Initialize core application services"""
        try:
            # Initialize audit service
            self._services['audit'] = AuditService()
            logger.info("Audit service initialized")
            
            # Initialize validation service
            self._services['validation'] = ValidationService()
            logger.info("Validation service initialized")
            
            # Initialize feature flags service
            self._services['feature_flags'] = FeatureFlagsService()
            logger.info("Feature flags service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize core services: {str(e)}")
            raise
    
    async def _initialize_monitoring_services(self):
        """Initialize monitoring and metrics services"""
        try:
            # Initialize Prometheus metrics
            self._services['prometheus'] = PrometheusMetricService.instance()
            logger.info("Prometheus metrics service initialized")
            
            # Initialize request logger
            self._services['request_logger'] = RequestLoggerService()
            logger.info("Request logger service initialized")
            
            # Set up service integration metrics
            await self._setup_integration_metrics()
            
        except Exception as e:
            logger.error(f"Failed to initialize monitoring services: {str(e)}")
            raise
    
    async def _initialize_security_services(self):
        """Initialize security-related services"""
        try:
            # Initialize encryption service
            self._services['encryption'] = EncryptionService()
            logger.info("Encryption service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize security services: {str(e)}")
            raise
    
    async def _setup_integration_metrics(self):
        """Set up metrics for service integrations"""
        try:
            prometheus = self._services['prometheus']
            
            # Service health metrics
            prometheus.gauge("service_health_status", "Health status of services")
            prometheus.gauge("service_integration_status", "Integration status between services")
            
            # Performance metrics
            prometheus.histogram("service_operation_duration", "Duration of service operations")
            prometheus.counter("service_operations_total", "Total service operations")
            prometheus.counter("service_errors_total", "Total service errors")
            
            logger.info("Integration metrics configured")
            
        except Exception as e:
            logger.error(f"Failed to setup integration metrics: {str(e)}")
    
    async def _perform_health_checks(self):
        """Perform health checks on all services"""
        logger.info("Performing service health checks...")
        
        for service_name, service in self._services.items():
            try:
                if hasattr(service, 'health_check'):
                    status = await service.health_check()
                else:
                    # Basic health check
                    status = {'status': 'healthy', 'service': service_name}
                
                self._health_status[service_name] = {
                    'status': 'healthy',
                    'last_check': datetime.utcnow(),
                    'details': status
                }
                
                # Record health metric
                if 'prometheus' in self._services:
                    self._services['prometheus'].gauge("service_health_status").set(
                        1.0, {'service': service_name}
                    )
                
                logger.info(f"Service {service_name} health check: OK")
                
            except Exception as e:
                self._health_status[service_name] = {
                    'status': 'unhealthy',
                    'last_check': datetime.utcnow(),
                    'error': str(e)
                }
                
                # Record unhealthy metric
                if 'prometheus' in self._services:
                    self._services['prometheus'].gauge("service_health_status").set(
                        0.0, {'service': service_name}
                    )
                
                logger.error(f"Service {service_name} health check failed: {str(e)}")
    
    def get_service(self, service_name: str) -> Any:
        """Get a service by name"""
        return self._services.get(service_name)
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of all services"""
        return {
            'overall_status': 'healthy' if all(
                status['status'] == 'healthy' 
                for status in self._health_status.values()
            ) else 'degraded',
            'services': self._health_status,
            'last_updated': datetime.utcnow()
        }
    
    async def validate_integrations(self) -> Dict[str, Any]:
        """Validate that all service integrations are working correctly"""
        validation_results = {}
        
        try:
            # Test audit service integration
            audit_result = await self._test_audit_integration()
            validation_results['audit_service'] = audit_result
            
            # Test metrics integration
            metrics_result = await self._test_metrics_integration()
            validation_results['metrics_service'] = metrics_result
            
            # Test feature flags integration
            flags_result = await self._test_feature_flags_integration()
            validation_results['feature_flags_service'] = flags_result
            
            # Test encryption integration
            encryption_result = await self._test_encryption_integration()
            validation_results['encryption_service'] = encryption_result
            
            overall_status = all(
                result.get('status') == 'ok' 
                for result in validation_results.values()
            )
            
            return {
                'overall_status': 'ok' if overall_status else 'error',
                'validations': validation_results,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to validate integrations: {str(e)}")
            return {
                'overall_status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow()
            }
    
    async def _test_audit_integration(self) -> Dict[str, Any]:
        """Test audit service integration"""
        try:
            audit_service = self._services.get('audit')
            if not audit_service:
                return {'status': 'error', 'message': 'Audit service not available'}
            
            # Test audit logging
            await audit_service.log_action(
                user_id=1,
                action="integration.test",
                resource_type="test",
                details={'test': True}
            )
            
            return {'status': 'ok', 'message': 'Audit integration working'}
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def _test_metrics_integration(self) -> Dict[str, Any]:
        """Test metrics service integration"""
        try:
            prometheus = self._services.get('prometheus')
            if not prometheus:
                return {'status': 'error', 'message': 'Prometheus service not available'}
            
            # Test metric recording
            prometheus.counter("integration_test_counter").increment()
            prometheus.gauge("integration_test_gauge").set(1.0)
            
            return {'status': 'ok', 'message': 'Metrics integration working'}
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def _test_feature_flags_integration(self) -> Dict[str, Any]:
        """Test feature flags service integration"""
        try:
            feature_flags = self._services.get('feature_flags')
            if not feature_flags:
                return {'status': 'error', 'message': 'Feature flags service not available'}
            
            # Test feature flag evaluation
            result = feature_flags.feature_on("test-feature")
            
            return {
                'status': 'ok', 
                'message': 'Feature flags integration working',
                'test_result': result
            }
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def _test_encryption_integration(self) -> Dict[str, Any]:
        """Test encryption service integration"""
        try:
            encryption = self._services.get('encryption')
            if not encryption:
                return {'status': 'error', 'message': 'Encryption service not available'}
            
            # Test encryption/decryption
            test_data = "integration_test_data"
            encrypted = encryption.encrypt(test_data)
            decrypted = encryption.decrypt(encrypted)
            
            if decrypted == test_data:
                return {'status': 'ok', 'message': 'Encryption integration working'}
            else:
                return {'status': 'error', 'message': 'Encryption/decryption mismatch'}
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def record_operation(
        self, 
        service_name: str, 
        operation: str, 
        duration: float, 
        success: bool = True,
        details: Optional[Dict[str, Any]] = None
    ):
        """Record service operation metrics"""
        try:
            prometheus = self._services.get('prometheus')
            if prometheus:
                # Record operation duration
                prometheus.histogram("service_operation_duration").observe(
                    duration, {
                        'service': service_name,
                        'operation': operation,
                        'success': str(success)
                    }
                )
                
                # Count operations
                prometheus.counter("service_operations_total").increment({
                    'service': service_name,
                    'operation': operation
                })
                
                # Count errors if operation failed
                if not success:
                    prometheus.counter("service_errors_total").increment({
                        'service': service_name,
                        'operation': operation
                    })
            
            # Log to audit if significant operation
            if details and self._services.get('audit'):
                await self._services['audit'].log_action(
                    user_id=details.get('user_id'),
                    action=f"{service_name}.{operation}",
                    resource_type="service_operation",
                    details={
                        'duration_ms': duration * 1000,
                        'success': success,
                        **details
                    }
                )
                
        except Exception as e:
            logger.error(f"Failed to record operation metrics: {str(e)}")
    
    def is_healthy(self) -> bool:
        """Check if all services are healthy"""
        return all(
            status['status'] == 'healthy' 
            for status in self._health_status.values()
        )


# Global instance
integration_coordinator = IntegrationCoordinator()