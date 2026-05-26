"""Stream processor orchestrator connecting Kafka topics to Flink jobs."""

from typing import Dict, List, Callable, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import asyncio
import uuid
from backend.services.kafka_stub import get_kafka_broker, KafkaMessage
from backend.services.flink_stub import get_flink_environment, StreamRecord, StreamProcessor
from backend.utils.logging import get_logger

logger = get_logger("stream_processor")


@dataclass
class PipelineMetrics:
    """Metrics for a running pipeline."""
    pipeline_id: str
    source_topic: str
    job_id: str
    messages_processed: int = 0
    records_in_flight: int = 0
    window_results_emitted: int = 0
    errors: int = 0
    started_at: datetime = None
    last_update: datetime = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        running_time = (datetime.utcnow() - self.started_at).total_seconds() if self.started_at else 0
        return {
            "pipeline_id": self.pipeline_id,
            "source_topic": self.source_topic,
            "job_id": self.job_id,
            "messages_processed": self.messages_processed,
            "records_in_flight": self.records_in_flight,
            "window_results_emitted": self.window_results_emitted,
            "errors": self.errors,
            "running_time_seconds": running_time,
            "last_update": self.last_update.isoformat() if self.last_update else None
        }


class StreamPipeline:
    """Orchestrates data flow from Kafka topics through Flink processors."""
    
    def __init__(self, pipeline_id: str, source_topic: str):
        """Initialize pipeline."""
        self.pipeline_id = pipeline_id
        self.source_topic = source_topic
        self.kafka_broker = get_kafka_broker()
        self.flink_env = get_flink_environment()
        
        self.flink_job: Optional[StreamProcessor] = None
        self.is_running = False
        self.consumer_task: Optional[asyncio.Task] = None
        
        # Consumer configuration
        self.consumer_group = f"pipeline_{pipeline_id}"
        
        # Processing configuration
        self.window_duration = 60
        self.output_callback: Optional[Callable] = None
        
        # Metrics
        self.metrics = PipelineMetrics(
            pipeline_id=pipeline_id,
            source_topic=source_topic,
            job_id="",
            started_at=datetime.utcnow()
        )
    
    async def configure(self, window_duration: int = 60, output_callback: Optional[Callable] = None):
        """Configure pipeline parameters."""
        self.window_duration = window_duration
        self.output_callback = output_callback
        logger.info(f"Pipeline {self.pipeline_id} configured: window={window_duration}s")
    
    async def start(self) -> str:
        """Start the pipeline."""
        try:
            # Create Flink job
            self.flink_job = await self.flink_env.create_stream_job(f"pipeline_{self.pipeline_id}")
            self.metrics.job_id = self.flink_job.job_id
            
            # Configure Flink job
            await self.flink_job.set_window("tumbling", self.window_duration)
            await self.flink_job.start()
            
            # Start Kafka consumer
            self.is_running = True
            self.consumer_task = asyncio.create_task(self._consume_and_process())
            
            logger.info(f"Pipeline {self.pipeline_id} started with Flink job {self.flink_job.job_id}")
            return self.flink_job.job_id
        
        except Exception as e:
            logger.error(f"Failed to start pipeline {self.pipeline_id}: {e}")
            self.metrics.errors += 1
            raise
    
    async def stop(self):
        """Stop the pipeline."""
        try:
            self.is_running = False
            
            if self.consumer_task:
                self.consumer_task.cancel()
                try:
                    await self.consumer_task
                except asyncio.CancelledError:
                    pass
            
            if self.flink_job:
                await self.flink_job.stop()
            
            logger.info(f"Pipeline {self.pipeline_id} stopped")
        except Exception as e:
            logger.error(f"Error stopping pipeline {self.pipeline_id}: {e}")
            self.metrics.errors += 1
    
    async def _consume_and_process(self):
        """Consumer loop that pulls messages from Kafka and processes them."""
        kafka_consumer = self.kafka_broker.create_consumer(
            [self.source_topic],
            self.consumer_group
        )
        
        try:
            while self.is_running:
                # Get batch of messages
                messages = await kafka_consumer.consume_batch(max_records=100, timeout_ms=1000)
                
                if not messages:
                    await asyncio.sleep(0.5)
                    continue
                
                # Process each message through Flink
                for msg in messages:
                    try:
                        # Convert Kafka message to stream record
                        record = StreamRecord(
                            key=msg.key,
                            value=msg.value,
                            timestamp=msg.timestamp
                        )
                        
                        # Process through Flink job
                        await self.flink_job.process_record(record)
                        self.metrics.messages_processed += 1
                        self.metrics.last_update = datetime.utcnow()
                        
                        # Commit offset
                        await kafka_consumer.commit(self.source_topic, msg.offset)
                    
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        self.metrics.errors += 1
        
        finally:
            await kafka_consumer.close()
            logger.debug(f"Consumer for pipeline {self.pipeline_id} closed")
    
    async def publish_test_event(self, key: str, value: Dict[str, Any]):
        """Publish test event to source topic (for testing)."""
        offset = await self.kafka_broker.publish(self.source_topic, key, value)
        logger.debug(f"Published test event to {self.source_topic}: {key}")
        return offset
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline metrics."""
        return self.metrics.to_dict()


class PipelineManager:
    """Manages multiple stream pipelines."""
    
    def __init__(self):
        """Initialize manager."""
        self.pipelines: Dict[str, StreamPipeline] = {}
        self.kafka_broker = get_kafka_broker()
        self.flink_env = get_flink_environment()
        self.lock = asyncio.Lock()
    
    async def create_pipeline(self, source_topic: str, window_duration: int = 60) -> str:
        """Create a new pipeline."""
        async with self.lock:
            pipeline_id = str(uuid.uuid4())[:8]
            pipeline = StreamPipeline(pipeline_id, source_topic)
            await pipeline.configure(window_duration=window_duration)
            self.pipelines[pipeline_id] = pipeline
            logger.info(f"Pipeline {pipeline_id} created for topic {source_topic}")
            return pipeline_id
    
    async def start_pipeline(self, pipeline_id: str) -> str:
        """Start a pipeline."""
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        
        job_id = await pipeline.start()
        return job_id
    
    async def stop_pipeline(self, pipeline_id: str) -> bool:
        """Stop a pipeline."""
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            return False
        
        await pipeline.stop()
        return True
    
    async def delete_pipeline(self, pipeline_id: str) -> bool:
        """Delete a pipeline."""
        async with self.lock:
            pipeline = self.pipelines.get(pipeline_id)
            if not pipeline:
                return False
            
            await pipeline.stop()
            del self.pipelines[pipeline_id]
            logger.info(f"Pipeline {pipeline_id} deleted")
            return True
    
    async def get_pipeline(self, pipeline_id: str) -> Optional[StreamPipeline]:
        """Get pipeline by ID."""
        return self.pipelines.get(pipeline_id)
    
    async def list_pipelines(self) -> List[Dict[str, Any]]:
        """List all pipelines with metrics."""
        return [pipeline.get_metrics() for pipeline in self.pipelines.values()]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get manager statistics."""
        return {
            "total_pipelines": len(self.pipelines),
            "running_pipelines": sum(1 for p in self.pipelines.values() if p.is_running),
            "total_messages_processed": sum(p.metrics.messages_processed for p in self.pipelines.values()),
            "total_errors": sum(p.metrics.errors for p in self.pipelines.values()),
            "kafka_broker": self.kafka_broker.get_stats(),
            "flink_environment": self.flink_env.get_stats()
        }


# Global pipeline manager
_manager: Optional[PipelineManager] = None


def get_pipeline_manager() -> PipelineManager:
    """Get or create global pipeline manager instance."""
    global _manager
    if _manager is None:
        _manager = PipelineManager()
    return _manager
