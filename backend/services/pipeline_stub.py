"""
In-memory streaming pipeline simulation using asyncio.Queue.
Mimics Kafka topic buffering and Flink tumbling window aggregation.
"""

import asyncio
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict
import json

from backend.utils.logging import get_logger

logger = get_logger("pipeline_stub")


@dataclass
class StreamMessage:
    """Represents a message in the streaming pipeline."""
    id: str
    tenant_id: str
    service_name: str
    error_type: str
    metric_name: str
    baseline_value: float
    current_value: float
    spike_percentage: Decimal
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data['spike_percentage'] = float(data['spike_percentage'])
        data['timestamp'] = data['timestamp'].isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StreamMessage':
        """Create from dictionary."""
        data_copy = data.copy()
        data_copy['spike_percentage'] = Decimal(str(data_copy['spike_percentage']))
        data_copy['timestamp'] = datetime.fromisoformat(data_copy['timestamp'])
        return cls(**data_copy)


@dataclass
class WindowAggregate:
    """Represents an aggregated window of messages."""
    window_id: str
    window_start: datetime
    window_end: datetime
    tenant_id: str
    service_name: str
    error_type: str
    message_count: int
    max_spike_percentage: Decimal
    avg_spike_percentage: Decimal
    messages: List[StreamMessage] = field(default_factory=list)
    anomaly_detected: bool = False
    detection_reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "window_id": self.window_id,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "tenant_id": self.tenant_id,
            "service_name": self.service_name,
            "error_type": self.error_type,
            "message_count": self.message_count,
            "max_spike_percentage": float(self.max_spike_percentage),
            "avg_spike_percentage": float(self.avg_spike_percentage),
            "anomaly_detected": self.anomaly_detected,
            "detection_reason": self.detection_reason
        }


class StreamingPipelineStub:
    """
    In-memory streaming pipeline simulator.
    Mimics Kafka topics and Flink tumbling window operations.
    """
    
    def __init__(
        self,
        window_size_seconds: int = 300,  # 5-minute tumbling windows
        anomaly_threshold: float = 300.0,  # 300% spike triggers anomaly
        consecutive_windows: int = 1  # Consecutive violations to trigger alert
    ):
        """
        Initialize streaming pipeline stub.
        
        Args:
            window_size_seconds: Tumbling window size in seconds
            anomaly_threshold: Percentage spike threshold to trigger anomaly
            consecutive_windows: Number of consecutive anomalous windows before alert
        """
        self.window_size_seconds = window_size_seconds
        self.anomaly_threshold = anomaly_threshold
        self.consecutive_windows = consecutive_windows
        
        # Async queues simulating Kafka topics
        self.ingestion_topic: asyncio.Queue = asyncio.Queue()  # Raw telemetry
        self.aggregated_topic: asyncio.Queue = asyncio.Queue()  # Windowed aggregates
        self.throttle_candidates_topic: asyncio.Queue = asyncio.Queue()  # Detected anomalies
        
        # State tracking
        self.window_buffers: Dict[str, List[StreamMessage]] = defaultdict(list)
        self.anomaly_history: Dict[str, int] = defaultdict(int)  # Track consecutive violations
        self.is_running = False
        self.tasks: List[asyncio.Task] = []
        
        logger.info(
            f"Pipeline stub initialized: window={window_size_seconds}s, "
            f"threshold={anomaly_threshold}%, consecutive={consecutive_windows}"
        )
    
    async def start(self):
        """Start the streaming pipeline worker tasks."""
        if self.is_running:
            logger.warning("Pipeline already running")
            return
        
        self.is_running = True
        logger.info("Starting streaming pipeline...")
        
        # Start windowing worker
        self.tasks.append(asyncio.create_task(self._windowing_worker()))
        # Start anomaly detection worker
        self.tasks.append(asyncio.create_task(self._anomaly_detection_worker()))
        
        logger.info("Pipeline workers started")
    
    async def stop(self):
        """Stop the streaming pipeline worker tasks."""
        self.is_running = False
        logger.info("Stopping streaming pipeline...")
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for cancellation
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        
        logger.info("Pipeline stopped")
    
    async def ingest_message(self, message: StreamMessage) -> str:
        """
        Ingest a message into the streaming pipeline.
        
        Args:
            message: StreamMessage to ingest
        
        Returns:
            Message ID
        """
        await self.ingestion_topic.put(message)
        logger.debug(f"Ingested message: {message.id} (error: {message.error_type})")
        return message.id
    
    async def get_anomaly(self) -> Optional[Dict[str, Any]]:
        """
        Get next detected anomaly from throttle candidates topic.
        
        Returns:
            Anomaly data or None if queue empty
        """
        try:
            anomaly = self.throttle_candidates_topic.get_nowait()
            logger.info(f"Anomaly detected: {anomaly.error_type} @ {anomaly.tenant_id}")
            return anomaly.to_dict()
        except asyncio.QueueEmpty:
            return None
    
    async def _windowing_worker(self):
        """
        Tumbling window worker: aggregates messages over fixed time windows.
        """
        window_id_counter = 0
        
        while self.is_running:
            try:
                window_id_counter += 1
                window_id = f"window_{window_id_counter}"
                window_start = datetime.utcnow()
                window_end = window_start + timedelta(seconds=self.window_size_seconds)
                
                logger.debug(f"Opening window {window_id}: {window_start.isoformat()}")
                
                # Collect messages for this window
                messages_in_window = []
                start_collect = datetime.utcnow()
                
                while (datetime.utcnow() - start_collect).total_seconds() < self.window_size_seconds:
                    try:
                        # Try to get a message with timeout
                        message = await asyncio.wait_for(
                            self.ingestion_topic.get(),
                            timeout=1.0
                        )
                        messages_in_window.append(message)
                        logger.debug(f"Window {window_id} collected message: {message.error_type}")
                    except asyncio.TimeoutError:
                        # No message available, continue waiting
                        pass
                
                # Aggregate by tenant+service+error_type
                aggregates: Dict[str, WindowAggregate] = {}
                
                for message in messages_in_window:
                    key = f"{message.tenant_id}_{message.service_name}_{message.error_type}"
                    
                    if key not in aggregates:
                        aggregates[key] = WindowAggregate(
                            window_id=window_id,
                            window_start=window_start,
                            window_end=window_end,
                            tenant_id=message.tenant_id,
                            service_name=message.service_name,
                            error_type=message.error_type,
                            message_count=0,
                            max_spike_percentage=Decimal(0),
                            avg_spike_percentage=Decimal(0)
                        )
                    
                    agg = aggregates[key]
                    agg.message_count += 1
                    agg.messages.append(message)
                    
                    # Track max and average spike
                    if message.spike_percentage > agg.max_spike_percentage:
                        agg.max_spike_percentage = message.spike_percentage
                    
                    agg.avg_spike_percentage = (
                        (agg.avg_spike_percentage * (agg.message_count - 1) + message.spike_percentage)
                        / agg.message_count
                    )
                
                # Emit aggregates
                for key, aggregate in aggregates.items():
                    await self.aggregated_topic.put(aggregate)
                    logger.info(
                        f"Window {window_id} emitted: {key} "
                        f"({aggregate.message_count} msgs, spike: {aggregate.max_spike_percentage}%)"
                    )
                
            except Exception as e:
                logger.error(f"Error in windowing worker: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _anomaly_detection_worker(self):
        """
        Anomaly detection worker: identifies anomalies and triggers recommendations.
        """
        while self.is_running:
            try:
                # Get aggregated window
                aggregate = await asyncio.wait_for(
                    self.aggregated_topic.get(),
                    timeout=2.0
                )
                
                # Check if anomaly threshold exceeded
                if aggregate.max_spike_percentage > Decimal(str(self.anomaly_threshold)):
                    # Track consecutive violations
                    key = f"{aggregate.tenant_id}_{aggregate.service_name}_{aggregate.error_type}"
                    self.anomaly_history[key] += 1
                    
                    logger.warning(
                        f"Anomaly threshold exceeded: {key} "
                        f"(spike: {aggregate.max_spike_percentage}%, "
                        f"consecutive: {self.anomaly_history[key]})"
                    )
                    
                    # Check if consecutive threshold met
                    if self.anomaly_history[key] >= self.consecutive_windows:
                        aggregate.anomaly_detected = True
                        aggregate.detection_reason = (
                            f"Spike {aggregate.max_spike_percentage}% exceeds "
                            f"threshold {self.anomaly_threshold}% for "
                            f"{self.anomaly_history[key]} consecutive windows"
                        )
                        
                        # Create throttle candidate message
                        throttle_candidate = aggregate  # Using aggregate as throttle candidate
                        await self.throttle_candidates_topic.put(throttle_candidate)
                        
                        logger.info(f"Throttle candidate emitted: {key}")
                else:
                    # Reset counter if no anomaly
                    key = f"{aggregate.tenant_id}_{aggregate.service_name}_{aggregate.error_type}"
                    self.anomaly_history[key] = 0
                    logger.debug(f"No anomaly for {key}")
                
            except asyncio.TimeoutError:
                # No aggregates available
                pass
            except Exception as e:
                logger.error(f"Error in anomaly detection worker: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get current pipeline statistics."""
        return {
            "is_running": self.is_running,
            "window_size_seconds": self.window_size_seconds,
            "anomaly_threshold": self.anomaly_threshold,
            "ingestion_queue_size": self.ingestion_topic.qsize(),
            "aggregated_queue_size": self.aggregated_topic.qsize(),
            "throttle_candidates_queue_size": self.throttle_candidates_topic.qsize(),
            "anomaly_history": dict(self.anomaly_history)
        }


# Global pipeline instance
_pipeline_instance: Optional[StreamingPipelineStub] = None


async def get_pipeline() -> StreamingPipelineStub:
    """Get or create global pipeline instance."""
    global _pipeline_instance
    if _pipeline_instance is None:
        _pipeline_instance = StreamingPipelineStub()
        await _pipeline_instance.start()
    return _pipeline_instance


async def shutdown_pipeline():
    """Shutdown global pipeline instance."""
    global _pipeline_instance
    if _pipeline_instance:
        await _pipeline_instance.stop()
        _pipeline_instance = None
