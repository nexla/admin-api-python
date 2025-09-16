#!/bin/bash

# Production startup script for Admin API

set -e

echo "Starting Admin API in production mode..."

# Wait for database to be ready
echo "Waiting for database connection..."
./wait-for-db.sh

# Run database migrations
echo "Running database migrations..."
alembic upgrade head

# Start the application with Gunicorn
echo "Starting Gunicorn server..."
exec gunicorn app.main:app \
    --bind 0.0.0.0:8000 \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --worker-connections 1000 \
    --max-requests 1000 \
    --max-requests-jitter 100 \
    --timeout 30 \
    --keep-alive 2 \
    --log-level info \
    --access-logfile /app/logs/access.log \
    --error-logfile /app/logs/error.log \
    --capture-output \
    --enable-stdio-inheritance