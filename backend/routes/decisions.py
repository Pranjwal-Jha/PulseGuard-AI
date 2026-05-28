"""REST API routes for AI decision generation and incident management."""

import time
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, status, Query

from backend.models.schemas import (
    DecisionQuery, DecisionResponse, StreamingAnomalyCreate, StreamingAnomalyResponse,
    RetrievedDocument
)
from backend.services.vector_db import VectorDatabase
from backend.services.llm_engine import LLMEngine
from backend.services.pipeline import get_pipeline
from backend.utils.logging import get_logger
from decimal import Decimal

logger = get_logger("decisions")
router = APIRouter(prefix="/api/v1/decisions", tags=["Decisions"])


# Dependency to get vector DB and LLM engine
async def get_vector_db() -> VectorDatabase:
    """Get vector database instance."""
    return VectorDatabase()


async def get_llm_engine() -> LLMEngine:
    """Get LLM engine instance."""
    return LLMEngine()


@router.post("/analyze", response_model=DecisionResponse)
async def analyze_anomaly(
    query: DecisionQuery,
    vector_db: VectorDatabase = Depends(get_vector_db),
    llm_engine: LLMEngine = Depends(get_llm_engine)
) -> DecisionResponse:
    """
    Analyze a streaming anomaly and generate AI-powered mitigation recommendation.
    """
    try:
        import requests
        import os

        decision_id = str(uuid.uuid4())
        start_time = time.time()

        logger.info(
            f"Analyzing anomaly: {query.error_type} "
            f"(spike: {query.spike_percentage}%) "
            f"for tenant: {query.tenant_id}"
        )

        # ==========================================
        # SEARCH HISTORICAL INCIDENTS
        # ==========================================

        matched_rcas = await vector_db.search_by_metrics(
            error_type=query.error_type,
            spike_percentage=query.spike_percentage,
            k=query.top_k
        )

        logger.info(f"Found {len(matched_rcas)} historical matches")

        # ==========================================
        # PREPARE CONTEXT
        # ==========================================

        anomaly_context = {
            "error_type": query.error_type,
            "metric_name": query.metric_name,
            "spike_percentage": query.spike_percentage,
            "tenant_id": query.tenant_id,
            "service_name": query.service_name,
            "window_minutes": query.window_minutes
        }

        # ==========================================
        # GENERATE AI DECISION
        # ==========================================

        decision_data = await llm_engine.generate_decision(
            anomaly_context=anomaly_context,
            matched_rcas=matched_rcas
        )

        # ==========================================
        # BUILD RETRIEVED DOCUMENTS
        # ==========================================

        retrieved_documents = [
            RetrievedDocument(
                vector_db_id=rca['vector_db_id'],
                incident_id=rca['incident_id'],
                title=rca['title'],
                summary=rca.get('summary', ''),
                error_type=rca['error_type'],
                affected_tenant=rca['affected_tenant'],
                affected_service=rca['affected_service'],
                mitigation_metric=rca['mitigation_metric'],
                mitigation_effectiveness=rca['mitigation_effectiveness'],
                similarity_score=rca['similarity_score'],
                resolution_confidence=rca['resolution_confidence'],
                tags=rca.get('tags', [])
            )
            for rca in matched_rcas[:3]
        ]

        latency_ms = int((time.time() - start_time) * 1000)

        # ==========================================
        # BUILD RESPONSE
        # ==========================================

        response = DecisionResponse(
            id=decision_id,
            matched_incident=decision_data.get(
                'matched_incident',
                'UNKNOWN'
            ),
            symptom=decision_data.get(
                'symptom',
                'Unknown symptom'
            ),
            recommended_action=decision_data.get(
                'recommended_action',
                'Manual review required'
            ),
            confidence_score=min(
                1.0,
                max(
                    0.0,
                    decision_data.get('confidence_score', 0.5)
                )
            ),
            citations=decision_data.get('citations', []),
            retrieved_documents=retrieved_documents,
            llm_response=decision_data.get('llm_response', ''),
            latency_ms=latency_ms,
            generated_at=datetime.utcnow()
        )

        logger.info(
            f"Decision generated: {decision_id} "
            f"(incident: {response.matched_incident}, "
            f"confidence: {response.confidence_score:.2%})"
        )

        # ==========================================
        # SEND SLACK ALERT
        # ==========================================

        webhook = os.getenv("SLACK_WEBHOOK_URL")

        if webhook:
            slack_message = {
                "text": f"""
🚨 AI INCIDENT DETECTED

Incident: {response.matched_incident}

Service: {query.service_name}
Tenant: {query.tenant_id}

Error Type: {query.error_type}
Metric: {query.metric_name}

Spike Percentage: {query.spike_percentage}%

Recommended Action:
{response.recommended_action}

Confidence Score:
{response.confidence_score * 100:.0f}%

Latency:
{latency_ms} ms
"""
            }

            slack_response = requests.post(
                webhook,
                json=slack_message
            )

            logger.info(
                f"Slack notification sent: "
                f"{slack_response.status_code}"
            )

        else:
            logger.warning(
                "SLACK_WEBHOOK_URL not found in environment"
            )

        return response

    except Exception as e:
        logger.error(
            f"Error analyzing anomaly: {e}",
            exc_info=True
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze anomaly: {str(e)}"
        )


@router.post("/stream-anomaly")
async def report_streaming_anomaly(
    anomaly: StreamingAnomalyCreate,
    pipeline = Depends(get_pipeline)
) -> StreamingAnomalyResponse:
    """
    Report a detected streaming anomaly to the pipeline.
    
    This endpoint ingests anomalies detected from the event streaming system
    and feeds them into the anomaly detection pipeline.
    
    Args:
        anomaly: StreamingAnomalyCreate with anomaly details
        pipeline: Streaming pipeline instance
    
    Returns:
        StreamingAnomalyResponse confirming ingestion
    """
    try:
        anomaly_id = str(uuid.uuid4())
        
        # Ingest into pipeline (converted to dict for kafka)
        message = {
            "id": anomaly_id,
            "tenant_id": anomaly.tenant_id,
            "service_name": anomaly.service_name,
            "error_type": anomaly.error_type,
            "metric_name": anomaly.metric_name,
            "baseline_value": anomaly.baseline_value,
            "current_value": anomaly.current_value,
            "spike_percentage": float(anomaly.spike_percentage),
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": anomaly.raw_payload or {}
        }
        
        # Ingest into pipeline
        await pipeline.ingest_telemetry(message)
        
        logger.info(
            f"Anomaly reported: {anomaly_id} "
            f"({anomaly.error_type} @ {anomaly.tenant_id})"
        )
        
        return StreamingAnomalyResponse(
            id=anomaly_id,
            tenant_id=anomaly.tenant_id,
            service_name=anomaly.service_name,
            error_type=anomaly.error_type,
            spike_percentage=float(anomaly.spike_percentage),
            detected_at=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error reporting streaming anomaly: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to report anomaly: {str(e)}"
        )


@router.get("/history")
async def get_decision_history(
    limit: int = 20,
    offset: int = 0,
    error_type: Optional[str] = None
) -> dict:
    """
    Get historical decisions (stub - would query database in production).
    
    Args:
        limit: Number of records to return
        offset: Pagination offset
        error_type: Optional filter by error type
    
    Returns:
        List of decisions with pagination info
    """
    # In production, this would query the database
    logger.info(f"Getting decision history (limit={limit}, offset={offset})")
    
    return {
        "items": [],
        "total": 0,
        "limit": limit,
        "offset": offset,
        "message": "Decision history endpoint - database integration pending"
    }


@router.get("/stats")
async def get_decision_statistics() -> dict:
    """Get statistics about generated decisions."""
    logger.info("Fetching decision statistics")
    
    return {
        "total_decisions": 0,
        "average_confidence": 0.0,
        "average_latency_ms": 0,
        "incidents_by_type": {},
        "last_updated": datetime.utcnow().isoformat(),
        "message": "Statistics endpoint - database integration pending"
    }


@router.get("", response_model=list)
async def get_recent_decisions(limit: int = Query(10, ge=1, le=100)):
    """Get recent decisions."""
    try:
        engine = get_decision_engine()
        decisions = engine.get_recent_decisions(limit=limit)
        return decisions
    except Exception as e:
        logger.error(f"Error retrieving recent decisions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{decision_id}", response_model=dict)
async def get_decision(decision_id: str):
    """Get a specific decision."""
    try:
        engine = get_decision_engine()
        decision = engine.get_decision(decision_id)
        
        if not decision:
            raise HTTPException(status_code=404, detail=f"Decision {decision_id} not found")
        
        return decision
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving decision {decision_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=dict)
async def get_decision_stats():
    """Get decision statistics."""
    try:
        engine = get_decision_engine()
        stats = engine.get_decision_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting decision stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("", response_model=dict)
async def clear_history():
    """Clear decision history."""
    try:
        engine = get_decision_engine()
        engine.clear_history()
        return {
            "status": "success",
            "message": "Decision history cleared"
        }
    except Exception as e:
        logger.error(f"Error clearing history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=dict)
async def decision_system_health():
    """Get health status of decision system."""
    try:
        engine = get_decision_engine()
        provider_info = await engine.llm_engine.get_provider_info()
        stats = engine.get_decision_stats()
        
        return {
            "status": "healthy",
            "llm_provider": provider_info,
            "decision_statistics": stats
        }
    except Exception as e:
        logger.error(f"Error checking decision system health: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }
