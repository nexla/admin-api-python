from typing import List, Optional, Dict, Any, Union
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

from app.database import get_db
from app.auth.dependencies import get_current_user, require_permissions
from app.services.audit_service import AuditService
from app.models.user import User
from app.models.data_source import DataSource
from app.models.data_sink import DataSink
from app.models.data_set import DataSet
from app.models.flow_node import FlowNode
from app.models.project import Project

router = APIRouter()

class ResourceType(str, Enum):
    DATA_SOURCES = "data_sources"
    DATA_SINKS = "data_sinks"
    DATA_SETS = "data_sets"
    FLOWS = "flows"
    PROJECTS = "projects"
    ALL = "all"

class SearchScope(str, Enum):
    NAME = "name"
    DESCRIPTION = "description"
    TAGS = "tags"
    METADATA = "metadata"
    CONFIG = "config"
    ALL = "all"

class GlobalSearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500)
    resource_types: List[ResourceType] = Field(default=[ResourceType.ALL])
    search_scopes: List[SearchScope] = Field(default=[SearchScope.ALL])
    limit: int = Field(100, ge=1, le=500)
    offset: int = Query(0, ge=0)
    project_id: Optional[int] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    include_archived: bool = Field(False)
    match_exact: bool = Field(False)
    case_sensitive: bool = Field(False)

class SearchResult(BaseModel):
    resource_type: str
    resource_id: int
    name: str
    description: Optional[str]
    score: float
    matches: List[Dict[str, Any]]
    created_at: datetime
    updated_at: Optional[datetime]
    project_id: Optional[int]
    org_id: int

class GlobalSearchResponse(BaseModel):
    results: List[SearchResult]
    total_results: int
    query_time_ms: float
    resource_counts: Dict[str, int]
    suggestions: Optional[List[str]] = None

@router.post("/", response_model=GlobalSearchResponse)
async def global_search(
    search_request: GlobalSearchRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Perform global search across all resources."""
    start_time = datetime.utcnow()
    all_results = []
    resource_counts = {}
    
    # Determine which resources to search
    search_types = search_request.resource_types
    if ResourceType.ALL in search_types:
        search_types = [ResourceType.DATA_SOURCES, ResourceType.DATA_SINKS, 
                       ResourceType.DATA_SETS, ResourceType.FLOWS, ResourceType.PROJECTS]
    
    # Search each resource type
    for resource_type in search_types:
        try:
            results = await _search_resource_type(
                resource_type, search_request, db, current_user
            )
            all_results.extend(results)
            resource_counts[resource_type.value] = len(results)
        except Exception as e:
            resource_counts[resource_type.value] = 0
    
    # Sort results by score (descending)
    all_results.sort(key=lambda x: x.score, reverse=True)
    
    # Apply pagination
    paginated_results = all_results[search_request.offset:search_request.offset + search_request.limit]
    
    # Calculate query time
    end_time = datetime.utcnow()
    query_time_ms = (end_time - start_time).total_seconds() * 1000
    
    # Generate search suggestions
    suggestions = _generate_search_suggestions(search_request.query, all_results)
    
    return GlobalSearchResponse(
        results=paginated_results,
        total_results=len(all_results),
        query_time_ms=query_time_ms,
        resource_counts=resource_counts,
        suggestions=suggestions
    )

@router.get("/{resource_type}/suggest", response_model=List[str])
async def get_search_suggestions(
    resource_type: ResourceType,
    query: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get search suggestions for a specific resource type."""
    model_class = _get_model_class(resource_type)
    if not model_class:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid resource type: {resource_type}"
        )
    
    # Get base query with user access
    base_query = model_class.accessible_to_user(db, current_user)
    
    # Search for names that start with or contain the query
    suggestions = []
    
    # Names that start with the query (higher priority)
    start_matches = base_query.filter(
        model_class.name.ilike(f"{query}%")
    ).limit(limit // 2).all()
    suggestions.extend([item.name for item in start_matches])
    
    # Names that contain the query (lower priority)
    if len(suggestions) < limit:
        contain_matches = base_query.filter(
            model_class.name.ilike(f"%{query}%"),
            ~model_class.name.ilike(f"{query}%")  # Exclude already included
        ).limit(limit - len(suggestions)).all()
        suggestions.extend([item.name for item in contain_matches])
    
    return suggestions[:limit]

@router.get("/{resource_type}/tags", response_model=List[str])
async def get_available_tags(
    resource_type: ResourceType,
    query: Optional[str] = Query(None, min_length=1, max_length=100),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get available tags for a specific resource type."""
    model_class = _get_model_class(resource_type)
    if not model_class:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid resource type: {resource_type}"
        )
    
    # Get all tags from accessible resources
    accessible_resources = model_class.accessible_to_user(db, current_user).all()
    all_tags = set()
    
    for resource in accessible_resources:
        if hasattr(resource, 'tags') and resource.tags:
            all_tags.update(resource.tags)
    
    # Filter tags if query is provided
    if query:
        filtered_tags = [tag for tag in all_tags if query.lower() in tag.lower()]
    else:
        filtered_tags = list(all_tags)
    
    # Sort and limit results
    filtered_tags.sort()
    return filtered_tags[:limit]

@router.post("/advanced", response_model=GlobalSearchResponse)
async def advanced_search(
    filters: Dict[str, Any],
    resource_types: List[ResourceType] = [ResourceType.ALL],
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("updated_at", pattern="^(name|created_at|updated_at|score)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Perform advanced search with complex filters."""
    start_time = datetime.utcnow()
    all_results = []
    resource_counts = {}
    
    # Determine which resources to search
    search_types = resource_types
    if ResourceType.ALL in search_types:
        search_types = [ResourceType.DATA_SOURCES, ResourceType.DATA_SINKS, 
                       ResourceType.DATA_SETS, ResourceType.FLOWS, ResourceType.PROJECTS]
    
    # Search each resource type with advanced filters
    for resource_type in search_types:
        try:
            results = await _advanced_search_resource_type(
                resource_type, filters, db, current_user
            )
            all_results.extend(results)
            resource_counts[resource_type.value] = len(results)
        except Exception as e:
            resource_counts[resource_type.value] = 0
    
    # Apply sorting
    if sort_by == "name":
        all_results.sort(key=lambda x: x.name, reverse=(sort_order == "desc"))
    elif sort_by == "created_at":
        all_results.sort(key=lambda x: x.created_at, reverse=(sort_order == "desc"))
    elif sort_by == "updated_at":
        all_results.sort(key=lambda x: x.updated_at or x.created_at, reverse=(sort_order == "desc"))
    else:  # score
        all_results.sort(key=lambda x: x.score, reverse=(sort_order == "desc"))
    
    # Apply pagination
    paginated_results = all_results[offset:offset + limit]
    
    # Calculate query time
    end_time = datetime.utcnow()
    query_time_ms = (end_time - start_time).total_seconds() * 1000
    
    return GlobalSearchResponse(
        results=paginated_results,
        total_results=len(all_results),
        query_time_ms=query_time_ms,
        resource_counts=resource_counts
    )

@router.get("/recent", response_model=List[SearchResult])
async def get_recent_resources(
    resource_types: List[ResourceType] = Query([ResourceType.ALL]),
    limit: int = Query(20, ge=1, le=100),
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get recently created or updated resources."""
    cutoff_date = datetime.utcnow() - timedelta(days=days)
    all_results = []
    
    # Determine which resources to search
    search_types = resource_types
    if ResourceType.ALL in search_types:
        search_types = [ResourceType.DATA_SOURCES, ResourceType.DATA_SINKS, 
                       ResourceType.DATA_SETS, ResourceType.FLOWS, ResourceType.PROJECTS]
    
    for resource_type in search_types:
        model_class = _get_model_class(resource_type)
        if not model_class:
            continue
        
        # Get recent resources
        recent_resources = model_class.accessible_to_user(db, current_user).filter(
            model_class.updated_at >= cutoff_date
        ).order_by(model_class.updated_at.desc()).limit(limit).all()
        
        # Convert to search results
        for resource in recent_resources:
            result = SearchResult(
                resource_type=resource_type.value,
                resource_id=resource.id,
                name=resource.name,
                description=getattr(resource, 'description', None),
                score=1.0,  # Not applicable for recent search
                matches=[],
                created_at=resource.created_at,
                updated_at=resource.updated_at,
                project_id=getattr(resource, 'project_id', None),
                org_id=resource.org_id
            )
            all_results.append(result)
    
    # Sort by updated_at descending and limit
    all_results.sort(key=lambda x: x.updated_at or x.created_at, reverse=True)
    return all_results[:limit]

# Helper functions
async def _search_resource_type(
    resource_type: ResourceType,
    search_request: GlobalSearchRequest,
    db: Session,
    current_user: User
) -> List[SearchResult]:
    """Search within a specific resource type."""
    model_class = _get_model_class(resource_type)
    if not model_class:
        return []
    
    # Get base query with user access
    query = model_class.accessible_to_user(db, current_user)
    
    # Apply filters
    if search_request.project_id:
        if hasattr(model_class, 'project_id'):
            query = query.filter(model_class.project_id == search_request.project_id)
    
    if search_request.created_after:
        query = query.filter(model_class.created_at >= search_request.created_after)
    
    if search_request.created_before:
        query = query.filter(model_class.created_at <= search_request.created_before)
    
    if not search_request.include_archived:
        if hasattr(model_class, 'archived'):
            query = query.filter(model_class.archived == False)
    
    # Execute search within scopes
    results = []
    search_term = search_request.query
    if not search_request.case_sensitive:
        search_term = search_term.lower()
    
    search_scopes = search_request.search_scopes
    if SearchScope.ALL in search_scopes:
        search_scopes = [SearchScope.NAME, SearchScope.DESCRIPTION, SearchScope.TAGS, SearchScope.METADATA]
    
    resources = query.all()
    
    for resource in resources:
        matches = []
        total_score = 0.0
        
        # Search in name
        if SearchScope.NAME in search_scopes or SearchScope.ALL in search_scopes:
            name_score, name_matches = _search_in_field(
                getattr(resource, 'name', ''), search_term, search_request.match_exact, search_request.case_sensitive
            )
            if name_score > 0:
                matches.extend([{"field": "name", "matches": name_matches, "score": name_score}])
                total_score += name_score * 2.0  # Name matches weighted higher
        
        # Search in description
        if SearchScope.DESCRIPTION in search_scopes or SearchScope.ALL in search_scopes:
            desc_score, desc_matches = _search_in_field(
                getattr(resource, 'description', '') or '', search_term, search_request.match_exact, search_request.case_sensitive
            )
            if desc_score > 0:
                matches.extend([{"field": "description", "matches": desc_matches, "score": desc_score}])
                total_score += desc_score
        
        # Search in tags
        if SearchScope.TAGS in search_scopes or SearchScope.ALL in search_scopes:
            if hasattr(resource, 'tags') and resource.tags:
                for tag in resource.tags:
                    tag_score, tag_matches = _search_in_field(
                        tag, search_term, search_request.match_exact, search_request.case_sensitive
                    )
                    if tag_score > 0:
                        matches.extend([{"field": "tags", "matches": tag_matches, "score": tag_score}])
                        total_score += tag_score * 1.5  # Tag matches weighted higher
        
        # Search in metadata/config
        if SearchScope.METADATA in search_scopes or SearchScope.ALL in search_scopes:
            metadata_fields = ['metadata', 'source_config', 'sink_config', 'transform_config']
            for field in metadata_fields:
                if hasattr(resource, field):
                    field_value = getattr(resource, field)
                    if field_value and isinstance(field_value, dict):
                        field_str = str(field_value)
                        meta_score, meta_matches = _search_in_field(
                            field_str, search_term, search_request.match_exact, search_request.case_sensitive
                        )
                        if meta_score > 0:
                            matches.extend([{"field": field, "matches": meta_matches, "score": meta_score}])
                            total_score += meta_score * 0.5  # Metadata matches weighted lower
        
        # Add result if matches found
        if matches:
            result = SearchResult(
                resource_type=resource_type.value,
                resource_id=resource.id,
                name=resource.name,
                description=getattr(resource, 'description', None),
                score=total_score,
                matches=matches,
                created_at=resource.created_at,
                updated_at=getattr(resource, 'updated_at', None),
                project_id=getattr(resource, 'project_id', None),
                org_id=resource.org_id
            )
            results.append(result)
    
    return results

async def _advanced_search_resource_type(
    resource_type: ResourceType,
    filters: Dict[str, Any],
    db: Session,
    current_user: User
) -> List[SearchResult]:
    """Perform advanced search within a specific resource type."""
    model_class = _get_model_class(resource_type)
    if not model_class:
        return []
    
    # Get base query with user access
    query = model_class.accessible_to_user(db, current_user)
    
    # Apply advanced filters
    for field, value in filters.items():
        if hasattr(model_class, field):
            if isinstance(value, dict):
                # Handle complex filters like {"gte": 100, "lte": 1000}
                if "gte" in value:
                    query = query.filter(getattr(model_class, field) >= value["gte"])
                if "lte" in value:
                    query = query.filter(getattr(model_class, field) <= value["lte"])
                if "eq" in value:
                    query = query.filter(getattr(model_class, field) == value["eq"])
                if "contains" in value:
                    query = query.filter(getattr(model_class, field).contains(value["contains"]))
                if "in" in value and isinstance(value["in"], list):
                    query = query.filter(getattr(model_class, field).in_(value["in"]))
            else:
                # Simple equality filter
                query = query.filter(getattr(model_class, field) == value)
    
    # Execute query and convert to search results
    resources = query.all()
    results = []
    
    for resource in resources:
        result = SearchResult(
            resource_type=resource_type.value,
            resource_id=resource.id,
            name=resource.name,
            description=getattr(resource, 'description', None),
            score=1.0,  # Advanced search doesn't calculate relevance score
            matches=[],
            created_at=resource.created_at,
            updated_at=getattr(resource, 'updated_at', None),
            project_id=getattr(resource, 'project_id', None),
            org_id=resource.org_id
        )
        results.append(result)
    
    return results

def _get_model_class(resource_type: ResourceType):
    """Get the SQLAlchemy model class for a resource type."""
    mapping = {
        ResourceType.DATA_SOURCES: DataSource,
        ResourceType.DATA_SINKS: DataSink,
        ResourceType.DATA_SETS: DataSet,
        ResourceType.FLOWS: FlowNode,
        ResourceType.PROJECTS: Project
    }
    return mapping.get(resource_type)

def _search_in_field(field_value: str, search_term: str, match_exact: bool, case_sensitive: bool) -> tuple:
    """Search for a term within a field value and return score and matches."""
    if not field_value:
        return 0.0, []
    
    search_value = field_value if case_sensitive else field_value.lower()
    term = search_term if case_sensitive else search_term.lower()
    
    if match_exact:
        if search_value == term:
            return 1.0, [{"text": field_value, "start": 0, "end": len(field_value)}]
        else:
            return 0.0, []
    else:
        # Calculate relevance score based on match type and frequency
        score = 0.0
        matches = []
        
        # Exact match (highest score)
        if search_value == term:
            score = 1.0
            matches = [{"text": field_value, "start": 0, "end": len(field_value)}]
        # Starts with search term
        elif search_value.startswith(term):
            score = 0.8
            matches = [{"text": field_value[:len(term)], "start": 0, "end": len(term)}]
        # Contains search term
        elif term in search_value:
            score = 0.6
            start_idx = search_value.find(term)
            matches = [{"text": field_value[start_idx:start_idx + len(term)], "start": start_idx, "end": start_idx + len(term)}]
        # Fuzzy match (word boundaries)
        else:
            words = term.split()
            word_matches = []
            for word in words:
                if word in search_value:
                    start_idx = search_value.find(word)
                    word_matches.append({"text": field_value[start_idx:start_idx + len(word)], "start": start_idx, "end": start_idx + len(word)})
            
            if word_matches:
                score = 0.3 * (len(word_matches) / len(words))
                matches = word_matches
        
        return score, matches

def _generate_search_suggestions(query: str, results: List[SearchResult]) -> List[str]:
    """Generate search suggestions based on query and results."""
    suggestions = []
    
    # Extract common terms from result names
    all_names = [result.name for result in results]
    
    # Simple suggestion generation (can be enhanced with more sophisticated algorithms)
    query_words = query.lower().split()
    
    for name in all_names:
        name_words = name.lower().split()
        for word in name_words:
            if word not in query_words and len(word) > 3:
                suggestion = f"{query} {word}"
                if suggestion not in suggestions:
                    suggestions.append(suggestion)
    
    return suggestions[:5]  # Return top 5 suggestions