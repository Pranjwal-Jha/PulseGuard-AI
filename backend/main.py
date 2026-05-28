"""Main FastAPI application entry point."""

import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime

from backend.config import get_settings
from backend.utils.logging import get_logger, setup_logging
from backend.routes import health, pipelines, documents, decisions, notifications, ws_endpoints
from backend.services.pipeline import get_pipeline, shutdown_pipeline
from backend.utils.observability import setup_observability
from backend.services.vector_db_init import populate_vector_db
from backend.services.vector_db import VectorDatabase
from backend.services.notification import get_notification_service

# Setup logging
setup_logging()
logger = get_logger("main")
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"Debug mode: {settings.debug}")
    
    try:
        # Initialize streaming pipeline
        pipeline = await get_pipeline()
        logger.info("Streaming pipeline initialized")
        
        # Initialize vector DB with historical RCA data
        vector_db = VectorDatabase()
        await populate_vector_db(vector_db)
        logger.info("Vector database populated with historical RCAs")
        
        # Initialize notification service
        notification_service = get_notification_service()
        logger.info("Notification service initialized")
        
    except Exception as e:
        logger.error(f"Error during startup: {e}", exc_info=True)
    
    yield
    
    # Shutdown
    try:
        await shutdown_pipeline()
        logger.info("Streaming pipeline shutdown")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    
    logger.info("Application shutdown complete")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="Production-grade incident mitigation AI with RAG and real-time streaming",
    version=settings.app_version,
    lifespan=lifespan,
)

# Setup Observability (Prometheus + OpenTelemetry)
setup_observability(app)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*", "http://localhost:3000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(health.router, tags=["Health"])
app.include_router(pipelines.router, tags=["Pipelines"])
app.include_router(documents.router, tags=["Documents"])
app.include_router(decisions.router, tags=["Decisions"])
app.include_router(notifications.router, tags=["Notifications"])
app.include_router(ws_endpoints.router, tags=["WebSocket"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "redoc": "/redoc",
        "api_docs": "/docs"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
app.include_router(notifications.router)

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "docs": "/docs",
        "openapi": "/openapi.json"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="debug" if settings.debug else "info"
    )
