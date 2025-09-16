"""
Test data factories for creating model instances.
This replaces Rails FactoryBot with Python factory_boy.
"""
import factory
from factory import Sequence, SubFactory, LazyAttribute
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.models.user import User
from app.models.org import Org
from app.models.org_membership import OrgMembership
from app.models.team import Team
from app.models.project import Project
from app.models.flow import Flow, FlowRun
from app.models.data_source import DataSource
from app.models.data_set import DataSet
from app.models.data_sink import DataSink
from app.models.user_tier import UserTier
from app.models.org_tier import OrgTier
from app.models.cluster import Cluster

fake = Faker()


class BaseFactory(factory.alchemy.SQLAlchemyModelFactory):
    """Base factory class for SQLAlchemy models"""
    
    class Meta:
        abstract = True
        sqlalchemy_session_persistence = "commit"
        
    @classmethod
    def _setup_next_sequence(cls):
        """Override to avoid issues with sequence generation"""
        return 1


class UserTierFactory(BaseFactory):
    """Factory for UserTier model"""
    
    class Meta:
        model = UserTier
    
    name = "FREE"
    display_name = "Free Tier"
    record_count_limit = 10000
    record_count_limit_time = "DAILY"
    data_source_count_limit = 5


class OrgTierFactory(BaseFactory):
    """Factory for OrgTier model"""
    
    class Meta:
        model = OrgTier
    
    name = "PAID"
    display_name = "Paid Tier"
    record_count_limit = 1000000
    record_count_limit_time = "DAILY"
    data_source_count_limit = 100


class ClusterFactory(BaseFactory):
    """Factory for Cluster model"""
    
    class Meta:
        model = Cluster
    
    name = factory.Sequence(lambda n: f"test-cluster-{n}")
    description = "Test cluster"
    status = "ACTIVE"
    is_default = True
    endpoint_url = "http://localhost:8080"


class UserFactory(BaseFactory):
    """Factory for User model - equivalent to Rails user factory"""
    
    class Meta:
        model = User
    
    email = factory.Sequence(lambda n: f"user{n}@test.com")
    full_name = "Test User"
    password_digest = "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LeVZhDgNZeQ1TvABS"  # "password123"
    status = "ACTIVE"
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    user_tier = SubFactory(UserTierFactory)


class OrgFactory(BaseFactory):
    """Factory for Org model - equivalent to Rails org factory"""
    
    class Meta:
        model = Org
    
    name = factory.Sequence(lambda n: f"Test Org {n}")
    description = "Test organization"
    status = "ACTIVE"
    allow_api_key_access = True
    search_index_name = factory.Sequence(lambda n: f"test_org_{n}")
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    owner = SubFactory(UserFactory)
    org_tier = SubFactory(OrgTierFactory)
    cluster = SubFactory(ClusterFactory)


class OrgMembershipFactory(BaseFactory):
    """Factory for OrgMembership model"""
    
    class Meta:
        model = OrgMembership
    
    status = "ACTIVE"
    api_key = factory.Sequence(lambda n: f"API_KEY_{n}")
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    user = SubFactory(UserFactory)
    org = SubFactory(OrgFactory)


class TeamFactory(BaseFactory):
    """Factory for Team model"""
    
    class Meta:
        model = Team
    
    name = factory.Sequence(lambda n: f"Test Team {n}")
    description = "Test team"
    team_type = "project"
    is_active = True
    is_private = False
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    organization = SubFactory(OrgFactory)
    created_by = SubFactory(UserFactory)
    owner = SubFactory(UserFactory)


class ProjectFactory(BaseFactory):
    """Factory for Project model"""
    
    class Meta:
        model = Project
    
    name = factory.Sequence(lambda n: f"Test Project {n}")
    description = "Test project"
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    owner = SubFactory(UserFactory)
    org = SubFactory(OrgFactory)


class FlowFactory(BaseFactory):
    """Factory for Flow model"""
    
    class Meta:
        model = Flow
    
    name = factory.Sequence(lambda n: f"Test Flow {n}")
    description = "Test flow"
    flow_type = "data_pipeline"
    status = "draft"
    schedule_type = "manual"
    version = "1.0"
    priority = 5
    is_active = True
    is_template = False
    auto_start = False
    retry_count = 3
    timeout_minutes = 60
    run_count = 0
    success_count = 0
    failure_count = 0
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    owner = SubFactory(UserFactory)
    org = SubFactory(OrgFactory)
    project = SubFactory(ProjectFactory)


class FlowRunFactory(BaseFactory):
    """Factory for FlowRun model"""
    
    class Meta:
        model = FlowRun
    
    run_number = factory.Sequence(lambda n: n + 1)
    status = "queued"
    trigger_type = "manual"
    records_processed = 0
    records_success = 0
    records_failed = 0
    bytes_processed = 0
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    flow = SubFactory(FlowFactory)
    triggered_by = SubFactory(UserFactory)


class DataSourceFactory(BaseFactory):
    """Factory for DataSource model"""
    
    class Meta:
        model = DataSource
    
    name = factory.Sequence(lambda n: f"Test Data Source {n}")
    description = "Test data source"
    status = "ACTIVE"
    connection_type = "s3"
    ingestion_mode = "BATCH"
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    owner = SubFactory(UserFactory)
    org = SubFactory(OrgFactory)


class DataSetFactory(BaseFactory):
    """Factory for DataSet model"""
    
    class Meta:
        model = DataSet
    
    name = factory.Sequence(lambda n: f"Test Data Set {n}")
    description = "Test data set"
    status = "ACTIVE"
    output_schema_locked = False
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    owner = SubFactory(UserFactory)
    org = SubFactory(OrgFactory)
    data_source = SubFactory(DataSourceFactory)


class DataSinkFactory(BaseFactory):
    """Factory for DataSink model"""
    
    class Meta:
        model = DataSink
    
    name = factory.Sequence(lambda n: f"Test Data Sink {n}")
    description = "Test data sink"
    status = "ACTIVE"
    connection_type = "s3"
    created_at = factory.LazyFunction(datetime.utcnow)
    updated_at = factory.LazyFunction(datetime.utcnow)
    
    owner = SubFactory(UserFactory)
    org = SubFactory(OrgFactory)
    data_set = SubFactory(DataSetFactory)


# Convenience functions for creating test data
def create_user(db: Session = None, **kwargs) -> User:
    """Create a test user"""
    if db:
        UserFactory._meta.sqlalchemy_session = db
        UserTierFactory._meta.sqlalchemy_session = db
    return UserFactory.create(**kwargs)


def create_org(db: Session = None, **kwargs) -> Org:
    """Create a test organization"""
    if db:
        OrgFactory._meta.sqlalchemy_session = db
        UserFactory._meta.sqlalchemy_session = db
        UserTierFactory._meta.sqlalchemy_session = db
        OrgTierFactory._meta.sqlalchemy_session = db
        ClusterFactory._meta.sqlalchemy_session = db
    return OrgFactory.create(**kwargs)


def create_org_membership(db: Session = None, **kwargs) -> OrgMembership:
    """Create a test org membership"""
    if db:
        OrgMembershipFactory._meta.sqlalchemy_session = db
        UserFactory._meta.sqlalchemy_session = db
        OrgFactory._meta.sqlalchemy_session = db
    return OrgMembershipFactory.create(**kwargs)


def create_team(db: Session = None, **kwargs) -> Team:
    """Create a test team"""
    if db:
        TeamFactory._meta.sqlalchemy_session = db
    return TeamFactory(**kwargs)


def create_project(db: Session = None, **kwargs) -> Project:
    """Create a test project"""
    if db:
        ProjectFactory._meta.sqlalchemy_session = db
    return ProjectFactory(**kwargs)


def create_flow(db: Session = None, **kwargs) -> Flow:
    """Create a test flow"""
    if db:
        FlowFactory._meta.sqlalchemy_session = db
    return FlowFactory(**kwargs)


def create_data_source(db: Session = None, **kwargs) -> DataSource:
    """Create a test data source"""
    if db:
        DataSourceFactory._meta.sqlalchemy_session = db
        UserFactory._meta.sqlalchemy_session = db
        OrgFactory._meta.sqlalchemy_session = db
    return DataSourceFactory.create(**kwargs)


def create_data_set(db: Session = None, **kwargs) -> DataSet:
    """Create a test data set"""
    if db:
        DataSetFactory._meta.sqlalchemy_session = db
        DataSourceFactory._meta.sqlalchemy_session = db
        UserFactory._meta.sqlalchemy_session = db
        OrgFactory._meta.sqlalchemy_session = db
    return DataSetFactory.create(**kwargs)


def create_data_sink(db: Session = None, **kwargs) -> DataSink:
    """Create a test data sink"""
    if db:
        DataSinkFactory._meta.sqlalchemy_session = db
    return DataSinkFactory(**kwargs)


# Admin user creation (equivalent to Rails create_org_admin)
def create_org_admin(org: Org, db: Session = None, email: str = None) -> User:
    """Create an organization admin user"""
    admin_user = create_user(
        db=db,
        email=email or f"admin@{org.name.lower().replace(' ', '')}.test",
        full_name="Admin User"
    )
    
    # Create admin membership
    create_org_membership(
        db=db,
        user=admin_user,
        org=org,
        api_key=f"ADMIN_API_KEY_{org.id}"
    )
    
    return admin_user