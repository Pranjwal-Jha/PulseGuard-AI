"""Flink stub implementation for local development without actual Flink."""

from typing import Dict, List, Callable, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import uuid
import asyncio
from abc import ABC, abstractmethod
from backend.utils.logging import get_logger

logger = get_logger("flink_stub")


class WindowType(str, Enum):
    """Types of windows for stream processing."""
    TUMBLING = "tumbling"  # Fixed, non-overlapping windows
    SLIDING = "sliding"    # Overlapping windows
    SESSION = "session"    # Based on inactivity


@dataclass
class StreamRecord:
    """Represents a single stream record."""
    key: str
    value: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    window_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "window_id": self.window_id
        }


@dataclass
class WindowState:
    """Represents state within a window."""
    window_id: str
    window_type: WindowType
    start_time: datetime
    end_time: datetime
    records: List[StreamRecord] = field(default_factory=list)
    state: Dict[str, Any] = field(default_factory=dict)
    
    def add_record(self, record: StreamRecord):
        """Add record to window."""
        self.records.append(record)
    
    def get_state(self, key: str) -> Any:
        """Get state value."""
        return self.state.get(key)
    
    def set_state(self, key: str, value: Any):
        """Set state value."""
        self.state[key] = value
    
    def is_expired(self) -> bool:
        """Check if window has expired."""
        return datetime.utcnow() > self.end_time


class StreamProcessor:
    """Stateful stream processor (Flink-like)."""
    
    def __init__(self, job_id: str):
        """Initialize processor."""
        self.job_id = job_id
        self.windows: Dict[str, WindowState] = {}
        self.state_store: Dict[str, Any] = {}
        self.is_running = False
        self.lock = asyncio.Lock()
        self.window_duration = timedelta(seconds=60)
        self.window_type = WindowType.TUMBLING
    
    async def set_window(self, window_type: WindowType, duration_seconds: int):
        """Configure window parameters."""
        self.window_type = window_type
        self.window_duration = timedelta(seconds=duration_seconds)
        logger.info(f"Job {self.job_id}: Window set to {window_type} with duration {duration_seconds}s")
    
    async def get_or_create_window(self, key: str, timestamp: datetime) -> WindowState:
        """Get or create window for given key and timestamp."""
        async with self.lock:
            # Calculate window ID based on timestamp and duration
            window_num = int(timestamp.timestamp()) // int(self.window_duration.total_seconds())
            window_id = f"{key}_window_{window_num}"
            
            if window_id not in self.windows:
                start_time = datetime.fromtimestamp(
                    (int(timestamp.timestamp()) // int(self.window_duration.total_seconds())) * int(self.window_duration.total_seconds())
                )
                end_time = start_time + self.window_duration
                
                self.windows[window_id] = WindowState(
                    window_id=window_id,
                    window_type=self.window_type,
                    start_time=start_time,
                    end_time=end_time
                )
            
            return self.windows[window_id]
    
    async def process_record(self, record: StreamRecord) -> Optional[Dict[str, Any]]:
        """Process a single record with stateful operations."""
        # Get or create window
        window = await self.get_or_create_window(record.key, record.timestamp)
        window.add_record(record)
        
        # Update state
        current_count = window.get_state("count") or 0
        window.set_state("count", current_count + 1)
        
        # Example: Aggregate value
        current_sum = window.get_state("sum") or 0
        value_to_add = record.value.get("amount", 0)
        window.set_state("sum", current_sum + value_to_add)
        
        logger.debug(f"Job {self.job_id}: Processed record in window {window.window_id}")
        
        return None  # Result will be emitted when window closes
    
    async def emit_window_result(self, window_id: str) -> Optional[Dict[str, Any]]:
        """Emit result when window closes."""
        async with self.lock:
            window = self.windows.get(window_id)
            if not window:
                return None
            
            result = {
                "window_id": window.window_id,
                "record_count": len(window.records),
                "state": window.state,
                "window_start": window.start_time.isoformat(),
                "window_end": window.end_time.isoformat(),
                "keys": list(set(r.key for r in window.records))
            }
            
            logger.info(f"Job {self.job_id}: Emitting result for {window_id}: {result['record_count']} records")
            return result
    
    async def get_state(self, key: str) -> Any:
        """Get global state value."""
        async with self.lock:
            return self.state_store.get(key)
    
    async def set_state(self, key: str, value: Any):
        """Set global state value."""
        async with self.lock:
            self.state_store[key] = value
    
    async def start(self):
        """Start processing."""
        self.is_running = True
        logger.info(f"Job {self.job_id} started")
    
    async def stop(self):
        """Stop processing."""
        self.is_running = False
        logger.info(f"Job {self.job_id} stopped")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get job metrics."""
        return {
            "job_id": self.job_id,
            "is_running": self.is_running,
            "active_windows": len(self.windows),
            "state_size": len(self.state_store),
            "window_type": self.window_type.value,
            "window_duration_seconds": int(self.window_duration.total_seconds())
        }


class FlinkEnvironment:
    """Flink execution environment."""
    
    def __init__(self):
        """Initialize Flink environment."""
        self.jobs: Dict[str, StreamProcessor] = {}
        self.lock = asyncio.Lock()
    
    async def create_stream_job(self, job_name: str) -> StreamProcessor:
        """Create a new stream processing job."""
        async with self.lock:
            job_id = str(uuid.uuid4())[:8]
            job = StreamProcessor(job_id)
            self.jobs[job_id] = job
            logger.info(f"Created job: {job_name} (ID: {job_id})")
            return job
    
    async def get_job(self, job_id: str) -> Optional[StreamProcessor]:
        """Get job by ID."""
        return self.jobs.get(job_id)
    
    async def list_jobs(self) -> List[Dict[str, Any]]:
        """List all jobs with metrics."""
        return [job.get_metrics() for job in self.jobs.values()]
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job."""
        job = self.jobs.get(job_id)
        if job:
            await job.stop()
            del self.jobs[job_id]
            logger.info(f"Job {job_id} cancelled")
            return True
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get environment statistics."""
        return {
            "total_jobs": len(self.jobs),
            "running_jobs": sum(1 for j in self.jobs.values() if j.is_running),
            "total_windows": sum(len(j.windows) for j in self.jobs.values()),
            "jobs": {job_id: job.get_metrics() for job_id, job in self.jobs.items()}
        }


# Global Flink environment
_flink_env: Optional[FlinkEnvironment] = None


def get_flink_environment() -> FlinkEnvironment:
    """Get or create global Flink environment instance."""
    global _flink_env
    if _flink_env is None:
        _flink_env = FlinkEnvironment()
    return _flink_env
