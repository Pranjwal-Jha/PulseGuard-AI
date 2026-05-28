"""SQLAlchemy ORM models for database persistence."""
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Float, DateTime, JSON, Integer, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from backend.database.core import Base

class IncidentRecord(Base):
    """Stores generated incident RCAs and outcomes."""
    __tablename__ = "incidents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id = Column(String(100), unique=True, index=True, nullable=False)
    title = Column(String(255), nullable=False)
    summary = Column(Text, nullable=False)
    root_cause = Column(Text, nullable=False)
    impact = Column(Text, nullable=False)
    resolution_action = Column(Text, nullable=False)
    resolution_confidence = Column(Float, nullable=False)
    mitigation_metric = Column(String(255), nullable=False)
    mitigation_effectiveness = Column(Float, nullable=False)
    error_type = Column(String(100), nullable=False)
    affected_tenant = Column(String(100), nullable=True)
    affected_service = Column(String(255), nullable=False)
    severity = Column(String(50), nullable=False)
    incident_date = Column(DateTime, nullable=False)
    detection_time_seconds = Column(Integer, nullable=True)
    recovery_time_seconds = Column(Integer, nullable=True)
    metadata_json = Column(JSONB, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class DecisionRecord(Base):
    """Stores real-time AI decisions made by the engine."""
    __tablename__ = "decisions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    matched_incident = Column(String(100), nullable=False)
    symptom = Column(Text, nullable=False)
    recommended_action = Column(Text, nullable=False)
    confidence_score = Column(Float, nullable=False)
    citations = Column(JSONB, nullable=False, default=list)
    llm_response = Column(Text, nullable=False)
    latency_ms = Column(Integer, nullable=False)
    generated_at = Column(DateTime, default=datetime.utcnow)
