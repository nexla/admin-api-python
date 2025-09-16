"""
Test configuration and fixtures for FastAPI Admin API tests.
This is the main pytest configuration file that sets up fixtures and test database.
"""
import os
import pytest
import asyncio
from typing import Generator
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Add app to path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app
from app.database import Base, get_db
from app.config import settings

# Test database URL (SQLite in-memory for fast testing)
SQLALCHEMY_DATABASE_URL = "sqlite:///./test_admin_api.db"

# Create test engine
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """Override database dependency for testing"""
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


# Override the database dependency
app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def db_engine():
    """Create test database engine"""
    return engine


@pytest.fixture(scope="session", autouse=True)
def setup_test_db(db_engine):
    """Create test database tables"""
    Base.metadata.create_all(bind=db_engine)
    yield
    Base.metadata.drop_all(bind=db_engine)


@pytest.fixture
def db_session():
    """Create a fresh database session for each test"""
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)
    
    yield session
    
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def client():
    """Create FastAPI test client"""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def auth_headers():
    """Create authentication headers for API testing"""
    # Will be implemented with actual JWT token creation
    return {
        "Authorization": "Bearer test_token",
        "Content-Type": "application/json"
    }


@pytest.fixture
def api_headers():
    """Create standard API headers"""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


# Test environment setup
@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables"""
    os.environ["TESTING"] = "1"
    os.environ["DATABASE_URL"] = SQLALCHEMY_DATABASE_URL
    yield
    # Cleanup if needed