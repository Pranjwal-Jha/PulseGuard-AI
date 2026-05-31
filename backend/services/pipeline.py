import logging
import asyncio
import json
from typing import Optional
from backend.services.kafka_client import kafka_client
from backend.models.schemas import StreamingAnomalyCreate

logger = logging.getLogger(__name__)

class PipelineService:
    def __init__(self):
        self.is_running = False

    async def start(self):
        self.is_running = True
        try:
            await kafka_client.start_producer()
            await kafka_client.start_consumer(
                topic="telemetry_anomalies",
                group_id="fastapi_backend",
                callback=self.handle_anomaly
            )
            logger.info("Pipeline service started with Kafka Integration.")
        except Exception as e:
            logger.warning(f"Kafka unavailable, running without it. Error: {e}")

    async def stop(self):
        self.is_running = False
        await kafka_client.stop_producer()
        logger.info("Pipeline service stopped.")

    async def ingest_telemetry(self, telemetry: dict):
        """Sends raw telemetry to Kafka for Flink to process."""
        await kafka_client.send_message("telemetry_raw", telemetry)

    async def handle_anomaly(self, anomaly_data: dict):
        """Called when Flink detects an anomaly and produces to Kafka."""
        logger.warning(f"Anomaly detected from Flink: {anomaly_data}")
        # Here we would integrate with the decision engine
        from backend.services.decision_engine import get_decision_engine
        
        # Construct StreamingAnomalyCreate from Flink output
        anomaly_model = StreamingAnomalyCreate(
            tenant_id=anomaly_data.get("tenant_id", "default"),
            service_name=anomaly_data.get("service_name", "unknown"),
            error_type="Spike Detected",
            metric_name=anomaly_data.get("metric_name", "unknown"),
            baseline_value=0.0, # Not passed by minimal Flink job above
            current_value=0.0,  
            spike_percentage=anomaly_data.get("spike_percentage", 0.0),
            window_minutes=1
        )
        
        # Analyze using real LLM and VectorDB
        engine = get_decision_engine()
        # Build query for the decision engine
        await engine.evaluate(
            query=f"{anomaly_model.error_type} spike {anomaly_model.spike_percentage}% on {anomaly_model.service_name}",
            collection_name="historical_rcas"
        )

_pipeline_instance: Optional[PipelineService] = None

async def get_pipeline() -> PipelineService:
    global _pipeline_instance
    if not _pipeline_instance:
        _pipeline_instance = PipelineService()
        await _pipeline_instance.start()
    return _pipeline_instance

async def shutdown_pipeline():
    global _pipeline_instance
    if _pipeline_instance:
        await _pipeline_instance.stop()
        _pipeline_instance = None
