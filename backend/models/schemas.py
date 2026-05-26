"""Pydantic schemas for request/response validation."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal


class DocumentCreate(BaseModel):
    """Schema for creating a document."""
    title: str = Field(..., min_length=1, max_length=255)
    content: str = Field(..., min_length=1)
    metadata: Optional[Dict[str, Any]] = None


class DocumentResponse(BaseModel):
    """Schema for document response."""
    id: str
    title: str
    content: str
    chunks_count: int
    uploaded_at: datetime
    embedding_model: str
    status: str
    metadata: Optional[Dict[str, Any]] = None


class DocumentChunk(BaseModel):
    """Schema for document chunk."""
    id: str
    document_id: str
    content: str
    chunk_index: int
    embedding: Optional[List[float]] = None
    metadata: Optional[Dict[str, Any]] = None


class DocumentSearchRequest(BaseModel):
    """Schema for document search request."""
    query: str = Field(..., min_length=1, description="Search query text")
    top_k: int = Field(default=5, ge=1, le=100, description="Number of top results to return")


# ============================================================================
# INCIDENT MANAGEMENT SCHEMAS
# ============================================================================

class IncidentIndicatorSchema(BaseModel):
    """Schema for incident indicator/metric."""
    metric_name: str = Field(..., description="Metric identifier (e.g., db_connection_pool_active)")
    baseline_value: float = Field(..., description="Normal baseline value")
    anomaly_value: float = Field(..., description="Observed anomalous value")
    spike_percentage: Decimal = Field(..., description="Percentage change from baseline")
    window_size_minutes: int = Field(..., ge=1, description="Detection window in minutes")


class IncidentRCACreate(BaseModel):
    """Schema for creating incident RCA."""
    incident_id: str = Field(..., max_length=100, description="Unique incident identifier")
    title: str = Field(..., max_length=255, description="Incident title")
    summary: str = Field(..., description="Dense single-line summary for embeddings")
    root_cause: str = Field(..., description="Root cause analysis")
    impact: str = Field(..., description="Business/operational impact")
    resolution_action: str = Field(..., description="Action taken to resolve")
    resolution_confidence: Decimal = Field(..., ge=0, le=100, description="Confidence percentage")
    mitigation_metric: str = Field(..., description="Mitigation action (e.g., 'Throttle to 30%')")
    mitigation_effectiveness: Decimal = Field(..., ge=0, le=100, description="Effectiveness percentage")
    error_type: str = Field(..., description="Error type classification")
    affected_tenant: Optional[str] = Field(None, description="Affected tenant")
    affected_service: str = Field(..., description="Affected service name")
    severity: str = Field(..., description="Severity level")
    incident_date: datetime = Field(..., description="When incident occurred")
    detection_time_seconds: Optional[int] = Field(None, description="Time to detect")
    recovery_time_seconds: Optional[int] = Field(None, description="Time to recover")
    metadata: Optional[Dict[str, Any]] = None


class IncidentRCAResponse(BaseModel):
    """Schema for incident RCA response."""
    id: str
    incident_id: str
    title: str
    root_cause: str
    mitigation_metric: str
    mitigation_effectiveness: float
    resolution_confidence: float
    error_type: str
    affected_service: str
    severity: str
    documented_at: datetime


class RetrievedDocument(BaseModel):
    """Schema for retrieved historical document in decision."""
    vector_db_id: str
    incident_id: str
    title: str
    summary: Optional[str] = None
    error_type: str
    affected_tenant: Optional[str] = None
    affected_service: str
    mitigation_metric: str
    mitigation_effectiveness: float
    similarity_score: float = Field(..., ge=0, le=1)
    resolution_confidence: float = Field(..., ge=0, le=100)
    tags: List[str] = []


# ============================================================================
# STREAMING ANOMALY & DECISION SCHEMAS
# ============================================================================

class StreamingAnomalyCreate(BaseModel):
    """Schema for reporting a streaming anomaly."""
    tenant_id: str = Field(..., max_length=100, description="Tenant identifier")
    service_name: str = Field(..., max_length=100, description="Service name")
    error_type: str = Field(..., max_length=100, description="Error type classification")
    metric_name: str = Field(..., max_length=100, description="Metric name")
    baseline_value: float = Field(..., description="Baseline value")
    current_value: float = Field(..., description="Current observed value")
    spike_percentage: Decimal = Field(..., description="Spike percentage")
    window_minutes: int = Field(..., ge=1, description="Aggregation window in minutes")
    raw_payload: Optional[Dict[str, Any]] = None


class StreamingAnomalyResponse(BaseModel):
    """Schema for streaming anomaly response."""
    id: str
    tenant_id: str
    service_name: str
    error_type: str
    spike_percentage: float
    detected_at: datetime


class DecisionResponse(BaseModel):
    """Schema for AI-generated decision response."""
    id: str
    matched_incident: str = Field(..., description="Matched historical incident ID")
    symptom: str = Field(..., description="Identified symptom")
    recommended_action: str = Field(..., description="Recommended mitigation action")
    confidence_score: float = Field(..., ge=0, le=1, description="Confidence 0-1")
    citations: List[str] = Field(default_factory=list, description="Evidence citations")
    retrieved_documents: List[RetrievedDocument] = Field(default_factory=list)
    llm_response: str = Field(..., description="Full LLM response")
    latency_ms: int = Field(..., description="Generation latency in ms")
    generated_at: datetime


class DecisionQuery(BaseModel):
    """Schema for decision evaluation request."""
    error_type: str = Field(..., description="Type of error detected")
    spike_percentage: float = Field(..., ge=0, description="Spike percentage")
    tenant_id: Optional[str] = Field(None, description="Affected tenant")
    service_name: str = Field(..., description="Affected service")
    metric_name: str = Field(..., description="Problematic metric")
    window_minutes: int = Field(..., ge=1, description="Observation window")
    top_k: int = Field(5, ge=1, le=20, description="Top-k historical matches")
    include_context: bool = Field(True, description="Include full context")


# ============================================================================
# PIPELINE CONFIGURATION SCHEMAS
# ============================================================================

class WindowConfig(BaseModel):
    """Configuration for tumbling window."""
    window_size_seconds: int = Field(..., ge=1, description="Window size in seconds")
    min_threshold: float = Field(..., ge=0, description="Minimum anomaly threshold")
    consecutive_violations: int = Field(1, ge=1, description="Violations to trigger alert")


class PipelineConfig(BaseModel):
    """Schema for pipeline configuration."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    source_topic: str = Field(..., description="Kafka topic to read from")
    window_config: WindowConfig = Field(..., description="Tumbling window configuration")
    enable_notifications: bool = True


class PipelineResponse(BaseModel):
    """Schema for pipeline response."""
    id: str
    name: str
    status: str
    config: PipelineConfig
    created_at: datetime
    updated_at: datetime
    metrics: Optional[Dict[str, Any]] = None


class PipelineMetrics(BaseModel):
    """Schema for pipeline metrics."""
    message_count: int
    anomalies_detected: int
    decisions_generated: int
    notifications_sent: int
    average_latency_ms: float
    error_rate: float


# ============================================================================
# NOTIFICATION SCHEMAS
# ============================================================================

class NotificationRequest(BaseModel):
    """Schema for sending notification."""
    type: str = Field(..., description="Notification type: email, slack, webhook, websocket")
    target: str = Field(..., description="Destination: email, Slack channel, webhook URL")
    title: str = Field(..., description="Notification title")
    message: str = Field(..., description="Notification message")
    decision_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class NotificationResponse(BaseModel):
    """Schema for notification response."""
    id: str
    type: str
    status: str
    target: str
    sent_at: Optional[datetime] = None
    failed_reason: Optional[str] = None


# ============================================================================
# SYSTEM HEALTH & MONITORING
# ============================================================================

class HealthResponse(BaseModel):
    """Schema for health check response."""
    status: str = Field(..., description="System status: healthy, degraded, unhealthy")
    timestamp: datetime
    components: Dict[str, str] = Field(..., description="Component statuses")
    message: Optional[str] = None


class SystemMetrics(BaseModel):
    """Schema for system-wide metrics."""
    total_incidents_processed: int
    total_decisions_generated: int
    average_decision_latency_ms: float
    vector_db_collection_count: int
    active_pipelines: int
    recent_errors: List[str] = Field(default_factory=list)
    last_updated: datetime
    sent_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class HealthCheckResponse(BaseModel):
    """Schema for health check response."""
    status: str  # healthy, degraded, unhealthy
    version: str
    services: Dict[str, str]
    timestamp: datetime
