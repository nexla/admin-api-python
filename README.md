# Admin API - Python/FastAPI

A comprehensive administrative API built with FastAPI, providing full business logic implementation for user management, organization administration, data pipeline management, and system monitoring.

## 🚀 Features

### Core Business Logic
- **User Management**: Complete user lifecycle with authentication, authorization, and audit trails
- **Organization Management**: Multi-tenant organization administration with role-based access control
- **Project Management**: Project creation, collaboration, and team management
- **Data Pipeline Management**: Data sources, sinks, flows, and schema management
- **API Key Management**: Secure API key generation, rotation, and access control

### Advanced Features
- **Search & Discovery**: Global search across all resources with advanced filtering
- **Metrics & Monitoring**: Comprehensive metrics collection with Prometheus integration
- **Notifications & Alerts**: Real-time notification system with configurable alert rules
- **Audit Logging**: Complete audit trail for compliance and security
- **Feature Flags**: Dynamic feature toggling with user-specific targeting

### Security & Performance
- **Role-Based Access Control (RBAC)**: Fine-grained permissions system
- **Rate Limiting**: Configurable rate limiting with burst protection
- **Encryption**: Data encryption at rest and in transit
- **Middleware Stack**: Comprehensive request processing with metrics, logging, and security
- **Production Ready**: Docker, monitoring, and deployment configurations

## 📋 Requirements

- Python 3.11+
- MySQL 8.0+
- Redis 7+
- Elasticsearch 8.11+
- Docker & Docker Compose (for production deployment)

## 🛠️ Installation

### Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd admin-api-python
   ```

2. **Set up Python environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Set up database**
   ```bash
   # Start MySQL (using Docker)
   docker run -d --name mysql \
     -e MYSQL_ROOT_PASSWORD=password \
     -e MYSQL_DATABASE=admin_api \
     -e MYSQL_USER=admin_api \
     -e MYSQL_PASSWORD=password \
     -p 3306:3306 mysql:8.0

   # Run migrations
   alembic upgrade head
   ```

5. **Start the development server**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

### Production Deployment

1. **Configure production environment**
   ```bash
   cp .env.production.example .env.production
   # Edit .env.production with your production settings
   ```

2. **Deploy with Docker Compose**
   ```bash
   docker-compose -f docker-compose.production.yml up -d
   ```

3. **Verify deployment**
   ```bash
   curl https://your-domain.com/health
   ```

## 🏗️ Architecture

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │   Admin API     │    │     MySQL       │
│   (External)    │────│   FastAPI App   │────│    Database     │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                       ┌─────────────────┐    ┌─────────────────┐
                       │     Redis       │    │  Elasticsearch  │
                       │     Cache       │    │   Search Index  │
                       │                 │    │                 │
                       └─────────────────┘    └─────────────────┘
```

### Business Logic Layers

1. **API Layer** (`app/routers/`): FastAPI routers with request/response handling
2. **Service Layer** (`app/services/`): Business logic and external integrations
3. **Model Layer** (`app/models/`): SQLAlchemy ORM models and database operations
4. **Authentication Layer** (`app/auth/`): JWT-based authentication and RBAC
5. **Middleware Layer** (`app/middleware.py`): Request processing pipeline

## 📚 API Documentation

### Core Endpoints

#### Authentication & Users
- `POST /api/v1/auth/login` - User authentication
- `POST /api/v1/auth/logout` - User logout
- `GET /api/v1/users` - List users (with pagination)
- `POST /api/v1/users` - Create new user
- `PUT /api/v1/users/{id}` - Update user
- `DELETE /api/v1/users/{id}` - Deactivate user

#### Organizations & Projects
- `GET /api/v1/organizations` - List organizations
- `POST /api/v1/organizations` - Create organization
- `GET /api/v1/projects` - List projects
- `POST /api/v1/projects` - Create project

#### Data Management
- `GET /api/v1/data-sources` - List data sources
- `POST /api/v1/data-sources` - Create data source
- `GET /api/v1/flows` - List data flows
- `POST /api/v1/flows` - Create data flow

#### Search & Discovery
- `GET /api/v1/search` - Global search
- `GET /api/v1/search/users` - Search users
- `GET /api/v1/search/suggestions` - Get search suggestions

#### Monitoring & Metrics
- `GET /api/v1/metrics/dashboard` - Dashboard metrics
- `GET /api/v1/metrics/usage` - Usage metrics
- `GET /api/v1/alerts` - Active alerts

### Interactive Documentation

When running in development mode, visit:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | MySQL connection string | - |
| `REDIS_URL` | Redis connection string | - |
| `ELASTICSEARCH_URL` | Elasticsearch connection string | - |
| `SECRET_KEY` | Application secret key | - |
| `JWT_SECRET_KEY` | JWT signing key | - |
| `ALLOWED_ORIGINS` | CORS allowed origins | `["*"]` |
| `DEBUG` | Enable debug mode | `false` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Feature Flags

The application supports dynamic feature flags:

```python
from app.services.feature_flags_service import FeatureFlagsService

# Check if feature is enabled
if FeatureFlagsService.feature_on("new_dashboard"):
    # Show new dashboard
    pass
```

## 🔒 Security

### Authentication
- JWT-based authentication with refresh tokens
- Password strength validation and entropy scoring
- Account lockout after failed attempts

### Authorization
- Role-Based Access Control (RBAC)
- Resource-level permissions
- API key authentication for programmatic access

### Data Protection
- Encryption at rest using AES-256
- All sensitive data encrypted in database
- Secure password hashing with bcrypt

## 📊 Monitoring

### Metrics Collection
- Request/response metrics
- Business logic metrics
- System performance metrics
- Custom application metrics

### Alerting
- Configurable alert rules
- Multiple notification channels
- Escalation policies

### Logging
- Structured JSON logging
- Request/response logging
- Audit trail logging
- Centralized log aggregation

## 🧪 Testing

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app

# Run specific test file
pytest tests/test_users.py
```

### Test Categories
- **Unit Tests**: Individual component testing
- **Integration Tests**: Service integration testing
- **End-to-End Tests**: Complete workflow testing
- **Load Tests**: Performance and scalability testing

## 🚀 Deployment

### Docker Deployment
```bash
# Build production image
docker build -f Dockerfile.production -t admin-api:latest .

# Run with docker-compose
docker-compose -f docker-compose.production.yml up -d
```

### Kubernetes Deployment
See `k8s/` directory for Kubernetes manifests.

### Health Checks
- `GET /health` - Basic health check
- `GET /api/v1/status` - Detailed status information

## 📈 Performance

### Optimization Features
- Database connection pooling
- Redis caching for frequently accessed data
- Request/response compression
- Query optimization and indexing
- Background task processing

### Scalability
- Horizontal scaling with load balancing
- Database read replicas support
- Caching layer for reduced database load
- Background worker processes

## 🔄 Development Workflow

### Code Quality
- Black code formatting
- Flake8 linting
- Type hints with mypy
- Pre-commit hooks

### Database Migrations
```bash
# Create new migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1
```

## 📞 Support

### Documentation
- API documentation at `/docs`
- Architecture diagrams in `docs/`
- Deployment guides in `deployment/`

### Monitoring Dashboards
- **Grafana**: System and application metrics
- **Prometheus**: Metrics collection and alerting
- **Logs**: Centralized logging with Loki

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Related Projects

- [Admin Web UI](../admin-web-ui) - React frontend application
- [Data Pipeline Engine](../pipeline-engine) - Core data processing engine
- [Monitoring Stack](../monitoring) - Observability and monitoring setup

