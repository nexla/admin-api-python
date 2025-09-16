from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
import uvicorn
import os

from .database import get_db
from .config import settings
from .auth import get_current_user
from .routers import (
    auth, users, orgs, projects, data_credentials,
    data_sources, data_sinks, data_sets, flows,
    search, flow_management, metrics, data_schemas, invites,
    billing, custodians
)

app = FastAPI(
    title="Admin API",
    description="FastAPI version of the Admin API",
    version="1.0.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()

# Include routers - Phase 2 & 3 Enhanced API Endpoints
app.include_router(auth.router, prefix="/api/v1/auth", tags=["Authentication & Security"])
app.include_router(users.router, prefix="/api/v1/users", tags=["Users"])
app.include_router(orgs.router, prefix="/api/v1/organizations", tags=["Organizations"])
app.include_router(projects.router, prefix="/api/v1/projects", tags=["Projects"])
app.include_router(data_credentials.router, prefix="/api/v1/credentials", tags=["Data Credentials"])
app.include_router(data_sources.router, prefix="/api/v1/data-sources", tags=["Data Sources"])
app.include_router(data_sinks.router, prefix="/api/v1/data-sinks", tags=["Data Sinks"])
app.include_router(data_sets.router, prefix="/api/v1/data-sets", tags=["Data Sets"])
app.include_router(flows.router, prefix="/api/v1/flows", tags=["Flows"])
app.include_router(flow_management.router, prefix="/api/v1/flow-management", tags=["Flow Management"])
app.include_router(search.router, prefix="/api/v1/search", tags=["Global Search"])
app.include_router(metrics.router, prefix="/api/v1/metrics", tags=["Metrics & Monitoring"])
app.include_router(data_schemas.router, prefix="/api/v1/data-schemas", tags=["Data Schemas"])
app.include_router(invites.router, prefix="/api/v1/invites", tags=["Invitations"])

# New Rails business logic routers  
app.include_router(billing.router, prefix="/api/v1", tags=["Billing & Subscriptions"])
app.include_router(custodians.router, prefix="/api/v1", tags=["Organization Custodians"])

@app.get("/")
async def root():
    return {"status": "ok", "message": "Admin API FastAPI"}

@app.get("/api/v1/status")
async def status():
    return {"status": "ok", "version": "1.0.0"}

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )