# Admin API - Python/FastAPI Version

Python FastAPI implementation of the admin API, migrated from Rails.

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. Run the application:
   ```bash
   uvicorn app.main:app --reload
   ```

## Features

- FastAPI framework for high performance
- Async/await support
- Automatic API documentation
- Database migrations with Alembic
- Comprehensive test suite

## Docker Support

Run with Docker:
```bash
docker-compose -f docker-compose.python-only.yml up
```

