"""
Probe Service - Handle data source connection testing and schema probing
"""

import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

from app.models.data_source import DataSource
from app.models.data_credentials import DataCredentials
from app.services.encryption_service import EncryptionService

logger = logging.getLogger(__name__)


class ProbeService:
    """Service for probing and testing data source connections"""
    
    def __init__(self):
        self.encryption_service = EncryptionService()
        self.timeout_default = 30  # seconds
    
    async def test_connection(
        self,
        connector_type: str,
        config: Dict[str, Any],
        credentials: Optional[DataCredentials] = None,
        test_query: Optional[str] = None,
        timeout: int = 30
    ) -> Dict[str, Any]:
        """Test a data source connection with given configuration"""
        
        try:
            # Prepare connection configuration
            connection_config = self._prepare_connection_config(
                connector_type, config, credentials
            )
            
            # Get connector instance
            connector = self._get_connector(connector_type)
            
            # Test connection with timeout
            start_time = time.time()
            result = await asyncio.wait_for(
                connector.test_connection(connection_config, test_query),
                timeout=timeout
            )
            execution_time = (time.time() - start_time) * 1000
            
            # Add execution time to result
            result["execution_time_ms"] = execution_time
            
            logger.info(f"Connection test successful for {connector_type}")
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Connection test timeout for {connector_type}")
            return {
                "success": False,
                "status": "timeout",
                "error_message": f"Connection test timed out after {timeout} seconds"
            }
        except Exception as e:
            logger.error(f"Connection test failed for {connector_type}: {str(e)}")
            return {
                "success": False,
                "status": "error",
                "error_message": str(e)
            }
    
    async def test_data_source(
        self,
        data_source: DataSource,
        credentials: Optional[DataCredentials] = None,
        test_query: Optional[str] = None,
        include_schema: bool = True,
        include_sample: bool = False,
        sample_limit: int = 10
    ) -> Dict[str, Any]:
        """Test an existing data source"""
        
        try:
            # Prepare connection configuration from data source
            connection_config = self._prepare_data_source_config(
                data_source, credentials
            )
            
            # Get connector instance
            connector = self._get_connector(data_source.connector_type)
            
            # Test basic connection
            start_time = time.time()
            result = await connector.test_connection(connection_config, test_query)
            
            if result.get("success") and include_schema:
                # Get schema information
                schema_info = await connector.get_schema(connection_config)
                result["schema_info"] = schema_info
            
            if result.get("success") and include_sample:
                # Get sample data
                sample_data = await connector.get_sample_data(
                    connection_config, limit=sample_limit
                )
                result["sample_data"] = sample_data
            
            execution_time = (time.time() - start_time) * 1000
            result["execution_time_ms"] = execution_time
            
            return result
            
        except Exception as e:
            logger.error(f"Data source test failed for {data_source.id}: {str(e)}")
            return {
                "success": False,
                "status": "error",
                "error_message": str(e)
            }
    
    async def probe_schema(
        self,
        data_source: DataSource,
        credentials: Optional[DataCredentials] = None,
        include_views: bool = True,
        include_procedures: bool = False
    ) -> Dict[str, Any]:
        """Probe schema information from a data source"""
        
        try:
            # Prepare connection configuration
            connection_config = self._prepare_data_source_config(
                data_source, credentials
            )
            
            # Get connector instance
            connector = self._get_connector(data_source.connector_type)
            
            # Get detailed schema information
            schema_info = await connector.get_detailed_schema(
                connection_config,
                include_views=include_views,
                include_procedures=include_procedures
            )
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Schema probing failed for {data_source.id}: {str(e)}")
            raise e
    
    async def execute_query(
        self,
        data_source: DataSource,
        credentials: Optional[DataCredentials] = None,
        query: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute a query against a data source"""
        
        try:
            # Prepare connection configuration
            connection_config = self._prepare_data_source_config(
                data_source, credentials
            )
            
            # Get connector instance
            connector = self._get_connector(data_source.connector_type)
            
            # Execute query with limit
            start_time = time.time()
            result = await connector.execute_query(connection_config, query, limit)
            execution_time = (time.time() - start_time) * 1000
            
            result["execution_time_ms"] = execution_time
            return result
            
        except Exception as e:
            logger.error(f"Query execution failed for {data_source.id}: {str(e)}")
            raise e
    
    def _prepare_connection_config(
        self,
        connector_type: str,
        config: Dict[str, Any],
        credentials: Optional[DataCredentials] = None
    ) -> Dict[str, Any]:
        """Prepare connection configuration with credentials"""
        
        connection_config = config.copy()
        
        if credentials:
            # Decrypt credentials
            decrypted_creds = self._decrypt_credentials(credentials)
            connection_config.update(decrypted_creds)
        
        return connection_config
    
    def _prepare_data_source_config(
        self,
        data_source: DataSource,
        credentials: Optional[DataCredentials] = None
    ) -> Dict[str, Any]:
        """Prepare connection configuration from data source"""
        
        # Start with data source configuration
        connection_config = data_source.source_config.copy() if data_source.source_config else {}
        
        if credentials:
            # Decrypt and add credentials
            decrypted_creds = self._decrypt_credentials(credentials)
            connection_config.update(decrypted_creds)
        
        return connection_config
    
    def _decrypt_credentials(self, credentials: DataCredentials) -> Dict[str, Any]:
        """Decrypt credential data"""
        try:
            if credentials.encrypted_credentials:
                return self.encryption_service.decrypt_dict(
                    credentials.encrypted_credentials
                )
            return {}
        except Exception as e:
            logger.error(f"Failed to decrypt credentials {credentials.id}: {str(e)}")
            return {}
    
    def _get_connector(self, connector_type: str):
        """Get connector instance for the given type"""
        connector_map = {
            "mysql": MySQLConnector(),
            "postgresql": PostgreSQLConnector(),
            "snowflake": SnowflakeConnector(),
            "bigquery": BigQueryConnector(),
            "redshift": RedshiftConnector()
        }
        
        connector = connector_map.get(connector_type)
        if not connector:
            raise ValueError(f"Unsupported connector type: {connector_type}")
        
        return connector


class BaseConnector:
    """Base class for data source connectors"""
    
    async def test_connection(
        self, 
        config: Dict[str, Any], 
        test_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test connection to data source"""
        raise NotImplementedError
    
    async def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get basic schema information"""
        raise NotImplementedError
    
    async def get_detailed_schema(
        self, 
        config: Dict[str, Any],
        include_views: bool = True,
        include_procedures: bool = False
    ) -> Dict[str, Any]:
        """Get detailed schema information"""
        raise NotImplementedError
    
    async def get_sample_data(
        self, 
        config: Dict[str, Any], 
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get sample data from data source"""
        raise NotImplementedError
    
    async def execute_query(
        self, 
        config: Dict[str, Any], 
        query: str, 
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute a query against the data source"""
        raise NotImplementedError


class MySQLConnector(BaseConnector):
    """MySQL database connector"""
    
    async def test_connection(
        self, 
        config: Dict[str, Any], 
        test_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test MySQL connection"""
        try:
            # Mock implementation - in real app would use aiomysql or similar
            await asyncio.sleep(0.1)  # Simulate connection time
            
            return {
                "success": True,
                "status": "connected",
                "metadata": {
                    "server_version": "8.0.32",
                    "database": config.get("database"),
                    "host": config.get("host"),
                    "port": config.get("port", 3306)
                }
            }
        except Exception as e:
            return {
                "success": False,
                "status": "error",
                "error_message": str(e)
            }
    
    async def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get MySQL schema"""
        # Mock implementation
        await asyncio.sleep(0.2)
        return {
            "tables": [
                {"name": "users", "rows": 1000, "type": "BASE TABLE"},
                {"name": "orders", "rows": 5000, "type": "BASE TABLE"}
            ],
            "database_info": {
                "name": config.get("database"),
                "charset": "utf8mb4",
                "collation": "utf8mb4_unicode_ci"
            }
        }
    
    async def get_detailed_schema(
        self,
        config: Dict[str, Any],
        include_views: bool = True,
        include_procedures: bool = False
    ) -> Dict[str, Any]:
        """Get detailed MySQL schema"""
        # Mock implementation
        await asyncio.sleep(0.3)
        
        schema = {
            "tables": [
                {
                    "name": "users",
                    "type": "BASE TABLE",
                    "rows": 1000,
                    "columns": [
                        {"name": "id", "type": "int", "nullable": False, "key": "PRI"},
                        {"name": "email", "type": "varchar(255)", "nullable": False, "key": "UNI"},
                        {"name": "created_at", "type": "timestamp", "nullable": False}
                    ]
                }
            ],
            "database_info": {
                "name": config.get("database"),
                "charset": "utf8mb4",
                "collation": "utf8mb4_unicode_ci"
            }
        }
        
        if include_views:
            schema["views"] = [
                {"name": "active_users", "type": "VIEW"}
            ]
        
        if include_procedures:
            schema["procedures"] = [
                {"name": "get_user_stats", "type": "PROCEDURE"}
            ]
        
        return schema
    
    async def get_sample_data(
        self,
        config: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get sample data from MySQL"""
        # Mock implementation
        await asyncio.sleep(0.1)
        return [
            {"id": 1, "email": "user1@example.com", "created_at": "2023-01-01"},
            {"id": 2, "email": "user2@example.com", "created_at": "2023-01-02"}
        ][:limit]
    
    async def execute_query(
        self,
        config: Dict[str, Any],
        query: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute query against MySQL"""
        # Mock implementation
        await asyncio.sleep(0.2)
        return {
            "data": [
                {"id": 1, "count": 100},
                {"id": 2, "count": 200}
            ][:limit],
            "columns": ["id", "count"]
        }


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector"""
    
    async def test_connection(
        self,
        config: Dict[str, Any],
        test_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test PostgreSQL connection"""
        try:
            # Mock implementation
            await asyncio.sleep(0.1)
            return {
                "success": True,
                "status": "connected",
                "metadata": {
                    "server_version": "15.2",
                    "database": config.get("database"),
                    "host": config.get("host")
                }
            }
        except Exception as e:
            return {
                "success": False,
                "status": "error", 
                "error_message": str(e)
            }
    
    async def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get PostgreSQL schema"""
        # Mock implementation
        await asyncio.sleep(0.2)
        return {
            "tables": [
                {"name": "customers", "rows": 2000, "type": "BASE TABLE"},
                {"name": "products", "rows": 500, "type": "BASE TABLE"}
            ],
            "database_info": {
                "name": config.get("database"),
                "encoding": "UTF8"
            }
        }
    
    async def get_detailed_schema(
        self,
        config: Dict[str, Any],
        include_views: bool = True,
        include_procedures: bool = False
    ) -> Dict[str, Any]:
        """Get detailed PostgreSQL schema"""
        # Mock implementation - would query information_schema in real implementation
        await asyncio.sleep(0.3)
        return {
            "tables": [
                {
                    "name": "customers",
                    "type": "BASE TABLE",
                    "rows": 2000,
                    "columns": [
                        {"name": "id", "type": "integer", "nullable": False, "key": "PRI"},
                        {"name": "name", "type": "varchar(255)", "nullable": False}
                    ]
                }
            ],
            "database_info": {
                "name": config.get("database"),
                "encoding": "UTF8"
            }
        }
    
    async def get_sample_data(
        self,
        config: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get sample data from PostgreSQL"""
        # Mock implementation
        await asyncio.sleep(0.1)
        return [
            {"id": 1, "name": "Customer 1"},
            {"id": 2, "name": "Customer 2"}
        ][:limit]
    
    async def execute_query(
        self,
        config: Dict[str, Any],
        query: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute query against PostgreSQL"""
        # Mock implementation
        await asyncio.sleep(0.2)
        return {
            "data": [{"result": "success"}],
            "columns": ["result"]
        }


class SnowflakeConnector(BaseConnector):
    """Snowflake connector (mock implementation)"""
    
    async def test_connection(
        self,
        config: Dict[str, Any],
        test_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test Snowflake connection"""
        try:
            await asyncio.sleep(0.5)  # Snowflake typically slower
            return {
                "success": True,
                "status": "connected",
                "metadata": {
                    "account": config.get("account"),
                    "database": config.get("database"),
                    "warehouse": config.get("warehouse")
                }
            }
        except Exception as e:
            return {
                "success": False,
                "status": "error",
                "error_message": str(e)
            }
    
    async def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get Snowflake schema"""
        await asyncio.sleep(0.3)
        return {
            "tables": [
                {"name": "SALES_DATA", "rows": 1000000, "type": "BASE TABLE"},
                {"name": "CUSTOMER_DATA", "rows": 50000, "type": "BASE TABLE"}
            ],
            "database_info": {
                "name": config.get("database"),
                "warehouse": config.get("warehouse")
            }
        }
    
    async def get_detailed_schema(
        self,
        config: Dict[str, Any],
        include_views: bool = True,
        include_procedures: bool = False
    ) -> Dict[str, Any]:
        """Get detailed Snowflake schema"""
        await asyncio.sleep(0.4)
        return {
            "tables": [
                {
                    "name": "SALES_DATA",
                    "type": "BASE TABLE",
                    "rows": 1000000,
                    "columns": [
                        {"name": "ID", "type": "NUMBER", "nullable": False},
                        {"name": "AMOUNT", "type": "NUMBER(10,2)", "nullable": True}
                    ]
                }
            ],
            "database_info": {
                "name": config.get("database"),
                "warehouse": config.get("warehouse")
            }
        }
    
    async def get_sample_data(
        self,
        config: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get sample data from Snowflake"""
        await asyncio.sleep(0.2)
        return [
            {"ID": 1, "AMOUNT": 100.50},
            {"ID": 2, "AMOUNT": 250.75}
        ][:limit]
    
    async def execute_query(
        self,
        config: Dict[str, Any],
        query: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute query against Snowflake"""
        await asyncio.sleep(0.3)
        return {
            "data": [{"COUNT": 1000000}],
            "columns": ["COUNT"]
        }


class BigQueryConnector(BaseConnector):
    """Google BigQuery connector (mock implementation)"""
    
    async def test_connection(
        self,
        config: Dict[str, Any],
        test_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test BigQuery connection"""
        try:
            await asyncio.sleep(0.3)
            return {
                "success": True,
                "status": "connected",
                "metadata": {
                    "project_id": config.get("project_id"),
                    "dataset": config.get("dataset")
                }
            }
        except Exception as e:
            return {
                "success": False,
                "status": "error",
                "error_message": str(e)
            }
    
    async def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get BigQuery schema"""
        await asyncio.sleep(0.2)
        return {
            "tables": [
                {"name": "analytics_events", "rows": 5000000, "type": "BASE TABLE"},
                {"name": "user_sessions", "rows": 100000, "type": "BASE TABLE"}
            ],
            "database_info": {
                "project_id": config.get("project_id"),
                "dataset": config.get("dataset")
            }
        }
    
    async def get_detailed_schema(
        self,
        config: Dict[str, Any],
        include_views: bool = True,
        include_procedures: bool = False
    ) -> Dict[str, Any]:
        """Get detailed BigQuery schema"""
        await asyncio.sleep(0.3)
        return {
            "tables": [
                {
                    "name": "analytics_events",
                    "type": "BASE TABLE",
                    "rows": 5000000,
                    "columns": [
                        {"name": "event_id", "type": "STRING", "nullable": False},
                        {"name": "timestamp", "type": "TIMESTAMP", "nullable": False}
                    ]
                }
            ],
            "database_info": {
                "project_id": config.get("project_id"),
                "dataset": config.get("dataset")
            }
        }
    
    async def get_sample_data(
        self,
        config: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get sample data from BigQuery"""
        await asyncio.sleep(0.2)
        return [
            {"event_id": "evt_1", "timestamp": "2023-01-01T00:00:00Z"},
            {"event_id": "evt_2", "timestamp": "2023-01-01T00:01:00Z"}
        ][:limit]
    
    async def execute_query(
        self,
        config: Dict[str, Any],
        query: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute query against BigQuery"""
        await asyncio.sleep(0.4)
        return {
            "data": [{"total_events": 5000000}],
            "columns": ["total_events"]
        }


class RedshiftConnector(BaseConnector):
    """Amazon Redshift connector (mock implementation)"""
    
    async def test_connection(
        self,
        config: Dict[str, Any],
        test_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test Redshift connection"""
        try:
            await asyncio.sleep(0.2)
            return {
                "success": True,
                "status": "connected",
                "metadata": {
                    "host": config.get("host"),
                    "database": config.get("database"),
                    "port": config.get("port", 5439)
                }
            }
        except Exception as e:
            return {
                "success": False,
                "status": "error",
                "error_message": str(e)
            }
    
    async def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get Redshift schema"""
        await asyncio.sleep(0.3)
        return {
            "tables": [
                {"name": "sales_fact", "rows": 10000000, "type": "BASE TABLE"},
                {"name": "customer_dim", "rows": 100000, "type": "BASE TABLE"}
            ],
            "database_info": {
                "name": config.get("database"),
                "host": config.get("host")
            }
        }
    
    async def get_detailed_schema(
        self,
        config: Dict[str, Any],
        include_views: bool = True,
        include_procedures: bool = False
    ) -> Dict[str, Any]:
        """Get detailed Redshift schema"""
        await asyncio.sleep(0.4)
        return {
            "tables": [
                {
                    "name": "sales_fact",
                    "type": "BASE TABLE",
                    "rows": 10000000,
                    "columns": [
                        {"name": "sale_id", "type": "bigint", "nullable": False},
                        {"name": "amount", "type": "decimal(10,2)", "nullable": True}
                    ]
                }
            ],
            "database_info": {
                "name": config.get("database"),
                "host": config.get("host")
            }
        }
    
    async def get_sample_data(
        self,
        config: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get sample data from Redshift"""
        await asyncio.sleep(0.2)
        return [
            {"sale_id": 1, "amount": 1500.00},
            {"sale_id": 2, "amount": 850.25}
        ][:limit]
    
    async def execute_query(
        self,
        config: Dict[str, Any],
        query: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Execute query against Redshift"""
        await asyncio.sleep(0.3)
        return {
            "data": [{"total_sales": 10000000}],
            "columns": ["total_sales"]
        }