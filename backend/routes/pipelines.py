"""Pipeline management API routes."""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from backend.services.stream_processor import get_pipeline_manager
from backend.models.schemas import PipelineResponse, PipelineConfig
from backend.utils.logging import get_logger

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])
logger = get_logger("pipelines_route")


class PipelineCreateRequest(BaseModel):
    """Request model for creating a pipeline."""
    source_topic: str
    window_duration: int = 60


@router.post("", response_model=dict)
async def create_pipeline(request: PipelineCreateRequest):
    """Create a new data pipeline."""
    try:
        manager = get_pipeline_manager()
        pipeline_id = await manager.create_pipeline(
            source_topic=request.source_topic,
            window_duration=request.window_duration
        )
        return {
            "pipeline_id": pipeline_id,
            "source_topic": request.source_topic,
            "window_duration": request.window_duration,
            "status": "created"
        }
    except Exception as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=list)
async def list_pipelines():
    """List all pipelines with their status."""
    try:
        manager = get_pipeline_manager()
        pipelines = await manager.list_pipelines()
        return pipelines
    except Exception as e:
        logger.error(f"Error listing pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pipeline_id}", response_model=dict)
async def get_pipeline(pipeline_id: str):
    """Get pipeline details and metrics."""
    try:
        manager = get_pipeline_manager()
        pipeline = await manager.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        return pipeline.get_metrics()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pipeline_id}/start", response_model=dict)
async def start_pipeline(pipeline_id: str):
    """Start a pipeline."""
    try:
        manager = get_pipeline_manager()
        pipeline = await manager.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        if pipeline.is_running:
            raise HTTPException(status_code=400, detail="Pipeline is already running")
        
        job_id = await manager.start_pipeline(pipeline_id)
        return {
            "pipeline_id": pipeline_id,
            "job_id": job_id,
            "status": "running"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pipeline_id}/stop", response_model=dict)
async def stop_pipeline(pipeline_id: str):
    """Stop a pipeline."""
    try:
        manager = get_pipeline_manager()
        pipeline = await manager.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        if not pipeline.is_running:
            raise HTTPException(status_code=400, detail="Pipeline is not running")
        
        await manager.stop_pipeline(pipeline_id)
        return {
            "pipeline_id": pipeline_id,
            "status": "stopped"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{pipeline_id}", response_model=dict)
async def delete_pipeline(pipeline_id: str):
    """Delete a pipeline."""
    try:
        manager = get_pipeline_manager()
        success = await manager.delete_pipeline(pipeline_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        return {
            "pipeline_id": pipeline_id,
            "status": "deleted"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pipeline_id}/test-event", response_model=dict)
async def publish_test_event(pipeline_id: str, key: str = Query(...), value: dict = None):
    """Publish a test event to the pipeline's source topic."""
    try:
        manager = get_pipeline_manager()
        pipeline = await manager.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        if value is None:
            value = {"test": True}
        
        offset = await pipeline.publish_test_event(key, value)
        return {
            "pipeline_id": pipeline_id,
            "key": key,
            "offset": offset,
            "status": "published"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error publishing test event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=dict)
async def get_pipeline_stats():
    """Get overall pipeline system statistics."""
    try:
        manager = get_pipeline_manager()
        stats = manager.get_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
