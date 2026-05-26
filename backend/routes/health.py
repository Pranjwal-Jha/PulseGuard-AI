"""Health check endpoints."""

from fastapi import APIRouter
from datetime import datetime
from backend.config import get_settings
from backend.models.schemas import HealthCheckResponse

router = APIRouter(prefix="/health", tags=["Health"])
settings = get_settings()


@router.get("", response_model=HealthCheckResponse)
async def health_check():
    """General health check endpoint."""
    return HealthCheckResponse(
        status="healthy",
        version=settings.app_version,
        services={
            "api": "healthy",
            "database": "pending",
            "vector_db": "pending",
            "llm": "pending"
        },
        timestamp=datetime.utcnow()
    )


@router.get("/vector-db")
async def vector_db_health():
    """Check vector database health."""
    try:
        from backend.services.vector_db import get_vector_db
        vector_db = get_vector_db()
        collections = vector_db.list_collections()
        
        return {
            "status": "healthy",
            "service": "ChromaDB",
            "collections": len(collections),
            "embedding_model": vector_db.embedding_model
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "ChromaDB",
            "error": str(e)
        }


@router.get("/llm")
async def llm_health():
    """Check LLM service health."""
    try:
        # Will be implemented with LLM engine in Phase 4
        return {
            "status": "healthy",
            "service": "LLM Engine",
            "message": "Mock mode" if not settings.use_real_llm else "Real API mode"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "LLM Engine",
            "error": str(e)
        }
