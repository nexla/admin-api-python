"""
Catalog Service - Data catalog integration and metadata management
"""

import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from app.database import SessionLocal
from app.models.data_source import DataSource
from app.models.data_set import DataSet
from app.models.data_schema import DataSchema
from app.models.user import User
from app.models.org import Org
from app.models.marketplace_item import MarketplaceItem

logger = logging.getLogger(__name__)


class CatalogService:
    """Service for data catalog operations and metadata management"""
    
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()
    
    def __del__(self):
        """Cleanup database connection"""
        if hasattr(self, 'db') and self.db:
            self.db.close()
    
    # Catalog discovery methods
    async def discover_datasets(self, org_id: int, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Discover available datasets across all data sources"""
        try:
            filters = filters or {}
            
            # Get all data sources for the organization
            data_sources = self.db.query(DataSource).filter(
                DataSource.org_id == org_id,
                DataSource.status == "ACTIVE"
            ).all()
            
            discovered_datasets = []
            
            for source in data_sources:
                try:
                    # Get datasets for this source
                    datasets = self.db.query(DataSet).filter(
                        DataSet.data_source_id == source.id
                    ).all()
                    
                    for dataset in datasets:
                        catalog_entry = {
                            "dataset_id": dataset.id,
                            "name": dataset.name,
                            "description": dataset.description,
                            "source_name": source.name,
                            "source_type": source.type,
                            "schema_info": self._get_dataset_schema(dataset),
                            "metadata": dataset.metadata,
                            "tags": dataset.tags,
                            "created_at": dataset.created_at.isoformat() if dataset.created_at else None,
                            "updated_at": dataset.updated_at.isoformat() if dataset.updated_at else None,
                            "row_count": dataset.metadata.get('row_count') if dataset.metadata else None,
                            "size_bytes": dataset.metadata.get('size_bytes') if dataset.metadata else None
                        }
                        
                        # Apply filters
                        if self._matches_filters(catalog_entry, filters):
                            discovered_datasets.append(catalog_entry)
                
                except Exception as e:
                    logger.error(f"Failed to discover datasets for source {source.id}: {str(e)}")
                    continue
            
            # Sort by relevance/recency
            discovered_datasets.sort(key=lambda x: x['updated_at'] or x['created_at'], reverse=True)
            
            return discovered_datasets
            
        except Exception as e:
            logger.error(f"Failed to discover datasets: {str(e)}")
            return []
    
    async def search_catalog(self, org_id: int, query: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Search the data catalog with text query and filters"""
        try:
            filters = filters or {}
            search_results = []
            
            # Search datasets
            dataset_query = self.db.query(DataSet).join(DataSource).filter(
                DataSource.org_id == org_id
            )
            
            # Apply text search
            if query:
                search_pattern = f"%{query}%"
                dataset_query = dataset_query.filter(
                    or_(
                        DataSet.name.like(search_pattern),
                        DataSet.description.like(search_pattern),
                        DataSet.tags.op('JSON_SEARCH')('one', '$', search_pattern).isnot(None)
                    )
                )
            
            # Apply filters
            if filters.get('source_type'):
                dataset_query = dataset_query.filter(DataSource.type == filters['source_type'])
            
            if filters.get('tags'):
                for tag in filters['tags']:
                    dataset_query = dataset_query.filter(
                        DataSet.tags.op('JSON_CONTAINS')(f'"{tag}"')
                    )
            
            datasets = dataset_query.limit(50).all()
            
            for dataset in datasets:
                search_results.append({
                    "type": "dataset",
                    "id": dataset.id,
                    "name": dataset.name,
                    "description": dataset.description,
                    "source": dataset.data_source.name,
                    "relevance_score": self._calculate_relevance(dataset, query),
                    "metadata": dataset.metadata,
                    "tags": dataset.tags
                })
            
            # Search marketplace items
            marketplace_query = self.db.query(MarketplaceItem).filter(
                MarketplaceItem.org_id == org_id,
                MarketplaceItem.status == "PUBLISHED"
            )
            
            if query:
                search_pattern = f"%{query}%"
                marketplace_query = marketplace_query.filter(
                    or_(
                        MarketplaceItem.name.like(search_pattern),
                        MarketplaceItem.description.like(search_pattern),
                        MarketplaceItem.tags.op('JSON_SEARCH')('one', '$', search_pattern).isnot(None)
                    )
                )
            
            marketplace_items = marketplace_query.limit(25).all()
            
            for item in marketplace_items:
                search_results.append({
                    "type": "marketplace_item",
                    "id": item.id,
                    "name": item.name,
                    "description": item.description,
                    "category": item.category,
                    "relevance_score": self._calculate_relevance(item, query),
                    "metadata": item.metadata,
                    "tags": item.tags
                })
            
            # Sort by relevance
            search_results.sort(key=lambda x: x['relevance_score'], reverse=True)
            
            return search_results
            
        except Exception as e:
            logger.error(f"Failed to search catalog: {str(e)}")
            return []
    
    async def get_dataset_lineage(self, dataset_id: int) -> Dict[str, Any]:
        """Get data lineage for a dataset"""
        try:
            dataset = self.db.query(DataSet).filter(DataSet.id == dataset_id).first()
            if not dataset:
                return {"error": "Dataset not found"}
            
            lineage = {
                "dataset_id": dataset_id,
                "upstream": [],
                "downstream": [],
                "transformations": []
            }
            
            # Get upstream sources (where this dataset gets its data from)
            # This would typically involve analyzing flow definitions
            upstream_sources = self._get_upstream_sources(dataset)
            lineage["upstream"] = upstream_sources
            
            # Get downstream consumers (what uses this dataset)
            downstream_consumers = self._get_downstream_consumers(dataset)
            lineage["downstream"] = downstream_consumers
            
            # Get transformations applied to this dataset
            transformations = self._get_dataset_transformations(dataset)
            lineage["transformations"] = transformations
            
            return lineage
            
        except Exception as e:
            logger.error(f"Failed to get dataset lineage: {str(e)}")
            return {"error": str(e)}
    
    async def analyze_dataset_quality(self, dataset_id: int) -> Dict[str, Any]:
        """Analyze data quality metrics for a dataset"""
        try:
            dataset = self.db.query(DataSet).filter(DataSet.id == dataset_id).first()
            if not dataset:
                return {"error": "Dataset not found"}
            
            # This would typically involve running data profiling
            quality_metrics = {
                "dataset_id": dataset_id,
                "completeness": 0.0,  # Percentage of non-null values
                "validity": 0.0,      # Percentage of valid values
                "consistency": 0.0,   # Data consistency score
                "accuracy": 0.0,      # Data accuracy score
                "timeliness": 0.0,    # Data freshness score
                "uniqueness": 0.0,    # Percentage of unique values
                "issues": [],         # List of data quality issues
                "recommendations": [] # Recommendations for improvement
            }
            
            # Run quality checks (simplified)
            quality_metrics.update(await self._run_quality_analysis(dataset))
            
            return quality_metrics
            
        except Exception as e:
            logger.error(f"Failed to analyze dataset quality: {str(e)}")
            return {"error": str(e)}
    
    async def get_catalog_statistics(self, org_id: int) -> Dict[str, Any]:
        """Get overall catalog statistics"""
        try:
            stats = {
                "total_datasets": 0,
                "total_sources": 0,
                "total_schemas": 0,
                "data_volume_bytes": 0,
                "recent_additions": 0,
                "top_categories": [],
                "data_freshness": {},
                "quality_score": 0.0
            }
            
            # Count datasets
            stats["total_datasets"] = self.db.query(DataSet).join(DataSource).filter(
                DataSource.org_id == org_id
            ).count()
            
            # Count sources
            stats["total_sources"] = self.db.query(DataSource).filter(
                DataSource.org_id == org_id
            ).count()
            
            # Count schemas
            stats["total_schemas"] = self.db.query(DataSchema).filter(
                DataSchema.org_id == org_id
            ).count()
            
            # Calculate recent additions (last 30 days)
            thirty_days_ago = datetime.now() - timedelta(days=30)
            stats["recent_additions"] = self.db.query(DataSet).join(DataSource).filter(
                DataSource.org_id == org_id,
                DataSet.created_at >= thirty_days_ago
            ).count()
            
            # Get top categories from marketplace items
            marketplace_items = self.db.query(MarketplaceItem).filter(
                MarketplaceItem.org_id == org_id,
                MarketplaceItem.status == "PUBLISHED"
            ).all()
            
            category_counts = {}
            for item in marketplace_items:
                category = item.category
                category_counts[category] = category_counts.get(category, 0) + 1
            
            stats["top_categories"] = sorted(
                category_counts.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5]
            
            # Calculate data volume (approximate)
            total_volume = 0
            datasets = self.db.query(DataSet).join(DataSource).filter(
                DataSource.org_id == org_id
            ).all()
            
            for dataset in datasets:
                if dataset.metadata and 'size_bytes' in dataset.metadata:
                    total_volume += dataset.metadata['size_bytes']
            
            stats["data_volume_bytes"] = total_volume
            
            # Data freshness analysis
            fresh_count = 0
            stale_count = 0
            seven_days_ago = datetime.now() - timedelta(days=7)
            
            for dataset in datasets:
                if dataset.updated_at and dataset.updated_at >= seven_days_ago:
                    fresh_count += 1
                else:
                    stale_count += 1
            
            stats["data_freshness"] = {
                "fresh": fresh_count,
                "stale": stale_count,
                "freshness_percentage": (fresh_count / len(datasets) * 100) if datasets else 0
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get catalog statistics: {str(e)}")
            return {"error": str(e)}
    
    async def suggest_related_datasets(self, dataset_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Suggest related datasets based on various factors"""
        try:
            dataset = self.db.query(DataSet).filter(DataSet.id == dataset_id).first()
            if not dataset:
                return []
            
            suggestions = []
            
            # Find datasets with similar tags
            if dataset.tags:
                similar_datasets = self.db.query(DataSet).join(DataSource).filter(
                    DataSource.org_id == dataset.data_source.org_id,
                    DataSet.id != dataset_id
                ).all()
                
                for similar_dataset in similar_datasets:
                    if similar_dataset.tags:
                        # Calculate tag similarity
                        common_tags = set(dataset.tags) & set(similar_dataset.tags)
                        if common_tags:
                            similarity_score = len(common_tags) / len(set(dataset.tags) | set(similar_dataset.tags))
                            
                            suggestions.append({
                                "dataset_id": similar_dataset.id,
                                "name": similar_dataset.name,
                                "description": similar_dataset.description,
                                "similarity_score": similarity_score,
                                "similarity_reason": f"Shares {len(common_tags)} tags: {', '.join(common_tags)}",
                                "source": similar_dataset.data_source.name
                            })
            
            # Find datasets from the same source
            same_source_datasets = self.db.query(DataSet).filter(
                DataSet.data_source_id == dataset.data_source_id,
                DataSet.id != dataset_id
            ).limit(5).all()
            
            for same_source_dataset in same_source_datasets:
                suggestions.append({
                    "dataset_id": same_source_dataset.id,
                    "name": same_source_dataset.name,
                    "description": same_source_dataset.description,
                    "similarity_score": 0.7,  # Fixed score for same source
                    "similarity_reason": f"From the same data source: {dataset.data_source.name}",
                    "source": same_source_dataset.data_source.name
                })
            
            # Sort by similarity score and remove duplicates
            unique_suggestions = {s["dataset_id"]: s for s in suggestions}
            sorted_suggestions = sorted(
                unique_suggestions.values(), 
                key=lambda x: x["similarity_score"], 
                reverse=True
            )
            
            return sorted_suggestions[:limit]
            
        except Exception as e:
            logger.error(f"Failed to suggest related datasets: {str(e)}")
            return []
    
    # Helper methods
    def _get_dataset_schema(self, dataset: DataSet) -> Dict[str, Any]:
        """Get schema information for a dataset"""
        schema_info = {
            "columns": [],
            "total_columns": 0,
            "primary_keys": [],
            "foreign_keys": []
        }
        
        # This would typically query the actual data source for schema
        # For now, return basic info from metadata
        if dataset.metadata and 'schema' in dataset.metadata:
            schema_info.update(dataset.metadata['schema'])
        
        return schema_info
    
    def _matches_filters(self, catalog_entry: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Check if catalog entry matches the given filters"""
        if filters.get('source_type') and catalog_entry.get('source_type') != filters['source_type']:
            return False
        
        if filters.get('tags'):
            entry_tags = set(catalog_entry.get('tags', []))
            filter_tags = set(filters['tags'])
            if not entry_tags.intersection(filter_tags):
                return False
        
        if filters.get('min_size') and catalog_entry.get('size_bytes', 0) < filters['min_size']:
            return False
        
        if filters.get('max_size') and catalog_entry.get('size_bytes', 0) > filters['max_size']:
            return False
        
        return True
    
    def _calculate_relevance(self, item, query: str) -> float:
        """Calculate relevance score for search results"""
        if not query:
            return 1.0
        
        query_lower = query.lower()
        score = 0.0
        
        # Name match (highest weight)
        if hasattr(item, 'name') and item.name:
            if query_lower in item.name.lower():
                score += 3.0
            if item.name.lower().startswith(query_lower):
                score += 2.0
        
        # Description match
        if hasattr(item, 'description') and item.description:
            if query_lower in item.description.lower():
                score += 1.0
        
        # Tag match
        if hasattr(item, 'tags') and item.tags:
            for tag in item.tags:
                if query_lower in tag.lower():
                    score += 0.5
        
        return score
    
    def _get_upstream_sources(self, dataset: DataSet) -> List[Dict[str, Any]]:
        """Get upstream data sources for a dataset"""
        # This would analyze flow definitions to find upstream sources
        # For now, return empty list
        return []
    
    def _get_downstream_consumers(self, dataset: DataSet) -> List[Dict[str, Any]]:
        """Get downstream consumers of a dataset"""
        # This would analyze flow definitions to find downstream consumers
        # For now, return empty list
        return []
    
    def _get_dataset_transformations(self, dataset: DataSet) -> List[Dict[str, Any]]:
        """Get transformations applied to a dataset"""
        # This would get transform definitions applied to the dataset
        # For now, return empty list
        return []
    
    async def _run_quality_analysis(self, dataset: DataSet) -> Dict[str, Any]:
        """Run data quality analysis on a dataset"""
        # This would perform actual data profiling
        # For now, return mock metrics
        return {
            "completeness": 95.5,
            "validity": 92.3,
            "consistency": 87.8,
            "accuracy": 90.1,
            "timeliness": 85.2,
            "uniqueness": 98.7,
            "issues": [
                {
                    "type": "missing_values",
                    "column": "email",
                    "severity": "medium",
                    "description": "4.5% of email values are missing"
                },
                {
                    "type": "format_inconsistency",
                    "column": "phone_number",
                    "severity": "low",
                    "description": "Multiple phone number formats detected"
                }
            ],
            "recommendations": [
                "Add validation rules for email field",
                "Standardize phone number format",
                "Consider adding data freshness monitoring"
            ]
        }
    
    # Catalog management methods
    async def sync_catalog_metadata(self, org_id: int) -> Dict[str, Any]:
        """Synchronize catalog metadata with external systems"""
        try:
            sync_results = {
                "sources_synced": 0,
                "datasets_updated": 0,
                "schemas_refreshed": 0,
                "errors": []
            }
            
            # Get all active data sources for the organization
            data_sources = self.db.query(DataSource).filter(
                DataSource.org_id == org_id,
                DataSource.status == "ACTIVE"
            ).all()
            
            for source in data_sources:
                try:
                    # Sync metadata for this source
                    result = await self._sync_source_metadata(source)
                    sync_results["sources_synced"] += 1
                    sync_results["datasets_updated"] += result.get("datasets_updated", 0)
                    sync_results["schemas_refreshed"] += result.get("schemas_refreshed", 0)
                    
                except Exception as e:
                    error_msg = f"Failed to sync source {source.id}: {str(e)}"
                    sync_results["errors"].append(error_msg)
                    logger.error(error_msg)
            
            return sync_results
            
        except Exception as e:
            logger.error(f"Failed to sync catalog metadata: {str(e)}")
            return {"error": str(e)}
    
    async def _sync_source_metadata(self, source: DataSource) -> Dict[str, Any]:
        """Sync metadata for a specific data source"""
        # This would connect to the actual data source and refresh metadata
        # For now, return mock results
        return {
            "datasets_updated": 5,
            "schemas_refreshed": 3
        }
    
    async def index_catalog_for_search(self, org_id: int) -> Dict[str, Any]:
        """Index catalog content for search optimization"""
        try:
            indexing_results = {
                "datasets_indexed": 0,
                "items_indexed": 0,
                "search_terms_extracted": 0,
                "index_size_mb": 0
            }
            
            # This would typically index content in Elasticsearch or similar
            # For now, return mock results
            indexing_results["datasets_indexed"] = self.db.query(DataSet).join(DataSource).filter(
                DataSource.org_id == org_id
            ).count()
            
            indexing_results["items_indexed"] = self.db.query(MarketplaceItem).filter(
                MarketplaceItem.org_id == org_id,
                MarketplaceItem.status == "PUBLISHED"
            ).count()
            
            return indexing_results
            
        except Exception as e:
            logger.error(f"Failed to index catalog for search: {str(e)}")
            return {"error": str(e)}