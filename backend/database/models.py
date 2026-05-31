"""SQLAlchemy ORM models for database tables."""

from sqlalchemy import (
    Column, String, Text, DateTime, Integer, Float, JSON, ForeignKey, Boolean,
    Table, UniqueConstraint, Index, DECIMAL, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
import enum

Base = declarative_base()


class IncidentSeverity(str, enum.Enum):
    """Incident severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# Association table for many-to-many relationship between decisions and documents
decision_documents = Table(
    "decision_documents",
    Base.metadata,
    Column("decision_id", String(36), ForeignKey("decisions.id"), primary_key=True),
    Column("document_id", String(36), ForeignKey("documents.id"), primary_key=True),
)


class IncidentRCA(Base):
    """Model for Root Cause Analysis documents."""
    __tablename__ = "incident_rcas"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    incident_id = Column(String(100), nullable=False, unique=True, index=True)
    title = Column(String(255), nullable=False, index=True)
    summary = Column(Text, nullable=False)  # Dense, single-line summary for embeddings
    root_cause = Column(Text, nullable=False)
    impact = Column(Text, nullable=False)
    resolution_action = Column(Text, nullable=False)
    resolution_confidence = Column(DECIMAL(5, 2), nullable=False)  # 0-100
    mitigation_metric = Column(String(100), nullable=False)  # e.g., "Throttle to 25%"
    mitigation_effectiveness = Column(DECIMAL(5, 2), nullable=False)  # 0-100
    
    # Metadata for matching
    error_type = Column(String(100), nullable=False, index=True)
    affected_tenant = Column(String(100), nullable=True, index=True)
    affected_service = Column(String(100), nullable=False, index=True)
    severity = Column(Enum(IncidentSeverity), default=IncidentSeverity.MEDIUM)
    
    # Time tracking
    incident_date = Column(DateTime, nullable=False, index=True)
    detection_time_seconds = Column(Integer, nullable=True)  # How long to detect
    recovery_time_seconds = Column(Integer, nullable=True)  # How long to recover
    documented_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Vector DB reference
    vector_db_id = Column(String(255), nullable=True, unique=True)
    
    # Full metadata
    metadata = Column(JSON, nullable=True)
    
    # Relationships
    indicators = relationship("IncidentIndicator", back_populates="rca", cascade="all, delete-orphan")


class IncidentIndicator(Base):
    """Model for metrics/indicators that preceded an incident."""
    __tablename__ = "incident_indicators"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    rca_id = Column(String(36), ForeignKey("incident_rcas.id"), nullable=False, index=True)
    metric_name = Column(String(100), nullable=False)
    baseline_value = Column(Float, nullable=False)
    anomaly_value = Column(Float, nullable=False)
    spike_percentage = Column(DECIMAL(8, 2), nullable=False)  # (anomaly - baseline) / baseline * 100
    window_size_minutes = Column(Integer, nullable=False)  # Detection window
    
    # Relationships
    rca = relationship("IncidentRCA", back_populates="indicators")


class Document(Base):
    """Model for documents stored in the system."""
    __tablename__ = "documents"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(String(255), nullable=False, index=True)
    content = Column(Text, nullable=False)
    chunks_count = Column(Integer, default=0)
    uploaded_at = Column(DateTime, default=datetime.utcnow, index=True)
    embedding_model = Column(String(255), default="sentence-transformers/all-MiniLM-L6-v2")
    status = Column(String(50), default="processed")  # processing, processed, failed
    document_type = Column(String(50), default="general")  # rca, runbook, documentation
    metadata = Column(JSON, nullable=True)
    
    # Relationships
    chunks = relationship("DocumentChunk", back_populates="document", cascade="all, delete-orphan")
    decisions = relationship("Decision", secondary=decision_documents, back_populates="documents")


class DocumentChunk(Base):
    """Model for document chunks."""
    __tablename__ = "document_chunks"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    document_id = Column(String(36), ForeignKey("documents.id"), nullable=False, index=True)
    content = Column(Text, nullable=False)
    chunk_index = Column(Integer, nullable=False)
    embedding_id = Column(String(255), nullable=True, unique=True)  # ChromaDB reference
    metadata = Column(JSON, nullable=True)
    
    # Relationships
    document = relationship("Document", back_populates="chunks")


class StreamingAnomaly(Base):
    """Model for detected streaming anomalies."""
    __tablename__ = "streaming_anomalies"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id = Column(String(100), nullable=False, index=True)
    service_name = Column(String(100), nullable=False)
    error_type = Column(String(100), nullable=False, index=True)
    metric_name = Column(String(100), nullable=False)
    baseline_value = Column(Float, nullable=False)
    current_value = Column(Float, nullable=False)
    spike_percentage = Column(DECIMAL(8, 2), nullable=False)
    window_minutes = Column(Integer, nullable=False)
    detected_at = Column(DateTime, default=datetime.utcnow, index=True)
    raw_payload = Column(JSON, nullable=True)
    
    # Relationships
    decisions = relationship("Decision", back_populates="anomaly")


class Decision(Base):
    """Model for AI-generated decisions."""
    __tablename__ = "decisions"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    anomaly_id = Column(String(36), ForeignKey("streaming_anomalies.id"), nullable=False, index=True)
    query_context = Column(Text, nullable=False)
    llm_response = Column(Text, nullable=False)
    matched_rca_id = Column(String(100), nullable=True, index=True)
    confidence_score = Column(DECIMAL(5, 2), nullable=False)  # 0-100
    recommended_action = Column(Text, nullable=False)
    
    # Citation trail
    citation_data = Column(JSON, nullable=False)  # Stores matched incident info
    latency_ms = Column(Integer, nullable=False)
    generated_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    anomaly = relationship("StreamingAnomaly", back_populates="decisions")
    documents = relationship("Document", secondary=decision_documents, back_populates="decisions")
    notifications = relationship("Notification", back_populates="decision")


class Notification(Base):
    """Model for notifications sent."""
    __tablename__ = "notifications"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    decision_id = Column(String(36), ForeignKey("decisions.id"), nullable=False, index=True)
    notification_type = Column(String(50), nullable=False)  # email, slack, webhook, websocket
    target = Column(String(255), nullable=False)  # email, Slack channel, webhook URL
    status = Column(String(50), default="pending")  # pending, sent, failed, retrying
    message_body = Column(Text, nullable=False)
    response_data = Column(JSON, nullable=True)
    sent_at = Column(DateTime, nullable=True)
    failed_reason = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    
    # Relationships
    decision = relationship("Decision", back_populates="notifications")


class Pipeline(Base):
    """Model for data pipelines."""
    __tablename__ = "pipelines"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    status = Column(String(50), default="active")  # active, paused, completed
    source_topic = Column(String(255), nullable=False)
    window_size_seconds = Column(Integer, default=60)
    min_threshold = Column(Float, default=0.5)
    enable_notifications = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    metadata = Column(JSON, nullable=True)
    
    # Relationships
    logs = relationship("PipelineLog", back_populates="pipeline", cascade="all, delete-orphan")


class PipelineLog(Base):
    """Model for pipeline event logs."""
    __tablename__ = "pipeline_logs"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id = Column(String(36), ForeignKey("pipelines.id"), nullable=False, index=True)
    event_type = Column(String(100), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    data = Column(JSON, nullable=True)
    
    # Relationships
    pipeline = relationship("Pipeline", back_populates="logs")


