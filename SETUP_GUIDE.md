# RAG of Fire: Production-Grade Incident Mitigation AI System

A real-time, closed-loop AI copilot for automated incident detection, analysis, and mitigation in high-throughput data processing pipelines.

## 🎯 Project Overview

**RAG of Fire** solves the "3 AM on-call memory fail" problem by:
1. **Detecting** anomalies in streaming telemetry (Kafka/Flink simulated)
2. **Querying** a vector database for historical context and Root Cause Analysis (RCA) data
3. **Generating** deterministic, production-ready mitigation recommendations with citation trails
4. **Notifying** engineers via UI/Slack with actionable throttling decisions

## 📋 System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Incoming High-Throughput Message Stream (Kafka)               │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ Filter & Aggregate Engine (Flink Tumbling Window - 5-minute)   │
│ • Filters "bad/slow" messages                                   │
│ • Aggregates by tenant + service + error_type                  │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ Anomaly Detection (>300% spike over N consecutive windows)      │
│ • Evaluates windowed metrics                                    │
│ • Triggers on threshold breaches                               │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ AI Decision Engine (LLM + RAG)                                  │
│ • Queries Error RAG (ChromaDB) for historical context          │
│ • Generates structured mitigation recommendations              │
│ • Includes citations and confidence scores                     │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ Notification Layer (WebSocket + Slack)                         │
│ • Real-time UI dashboard updates                               │
│ • Slack alert channel integration                              │
│ • Audit trail in PostgreSQL                                    │
└─────────────────────────────────────────────────────────────────┘
```

## 🏗️ Directory Structure

```
rag-incident-response/
├── app/                          # Next.js frontend
│   ├── layout.tsx
│   ├── page.tsx
│   └── globals.css
├── components/                   # React components
│   ├── theme-provider.tsx
│   └── ui/                      # Shadcn UI components
├── backend/
│   ├── main.py                  # FastAPI entry point
│   ├── config.py                # Configuration management
│   ├── database/
│   │   ├── db.py               # Database session management
│   │   └── models.py           # SQLAlchemy models (Enhanced: RCA, StreamingAnomaly, Decision)
│   ├── models/
│   │   └── schemas.py          # Pydantic validation schemas
│   ├── services/
│   │   ├── vector_db.py        # ChromaDB wrapper with RCA search
│   │   ├── vector_db_init.py   # Historical RCA data initialization (10 production incidents)
│   │   ├── llm_engine.py       # Hybrid LLM (Mock + OpenAI/Anthropic)
│   │   ├── pipeline_stub.py    # asyncio.Queue Kafka/Flink simulation
│   │   ├── notification.py     # WebSocket + Slack dispatch
│   │   └── stream_processor.py # Stream processing logic
│   ├── routes/
│   │   ├── health.py           # Health check endpoint
│   │   ├── decisions.py        # Decision analysis & anomaly reporting
│   │   ├── documents.py        # RCA document management
│   │   ├── pipelines.py        # Pipeline control & monitoring
│   │   ├── notifications.py    # Notification delivery
│   │   └── ws_endpoints.py     # WebSocket streaming
│   ├── utils/
│   │   └── logging.py          # Structured logging setup
│   └── tests/                   # Unit & integration tests
├── requirements.txt             # Python dependencies
├── package.json                 # Node.js dependencies
├── next.config.mjs              # Next.js configuration
├── tsconfig.json                # TypeScript configuration
└── README.md                    # This file
```

## 🚀 Complete Setup Guide

### Prerequisites
- **Python 3.11+** - [Download](https://www.python.org/downloads/)
- **Node.js 18+** - [Download](https://nodejs.org/)
- **PostgreSQL 14+** - [Download](https://www.postgresql.org/download/) OR use Supabase cloud (free tier)
- **Git** - [Download](https://git-scm.com/)
- **(Optional) OpenAI API key** - For real LLM integration
- **(Optional) Anthropic API key** - For real LLM integration
- **(Optional) Slack Webhook** - For Slack notifications

---

## 📋 Step-by-Step Setup

### Step 1: Set Up Python Environment

**Windows:**
```bash
cd d:\rag-incident-response
python -m venv venv
venv\Scripts\activate
```

**macOS/Linux:**
```bash
cd ~/rag-incident-response
python3 -m venv venv
source venv/bin/activate
```

**Verify activation:**
```bash
python --version  # Should show Python 3.11+
pip --version     # Should show pip version
```

---

### Step 2: Install Python Dependencies

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

**Expected output:**
```
Successfully installed chromadb-0.4.24 fastapi-0.109.0 pydantic-2.6.0 sqlalchemy-2.0.25 ...
```

---

### Step 3: Install Node.js Dependencies

```bash
npm install
# or use pnpm (faster):
npm install -g pnpm
pnpm install
```

**Verify installation:**
```bash
npm --version   # Should show 9+
node --version  # Should show 18+
```

---

### Step 4: Configure Backend Environment Variables

Create `.env` file in the project root (`d:\rag-incident-response\.env`):

```env
# ====================================
# APPLICATION SETTINGS
# ====================================
APP_NAME=RAG of Fire
APP_VERSION=1.0.0
DEBUG=true
LOG_LEVEL=INFO

# ====================================
# DATABASE CONFIGURATION
# ====================================

# Option A: PostgreSQL Local (if installed locally)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/rag_of_fire

# Option B: PostgreSQL on Docker (if using Docker)
# DATABASE_URL=postgresql://postgres:postgres@postgres:5432/rag_of_fire

# Option C: Supabase Cloud (Free tier - Recommended for getting started)
# Sign up at: https://supabase.com
# DATABASE_URL=postgresql://postgres:[YOUR_PASSWORD]@db.[YOUR_PROJECT].supabase.co:5432/postgres

# ====================================
# VECTOR DATABASE CONFIGURATION
# ====================================
VECTOR_DB_PATH=./data/chroma_db
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2

# ====================================
# LLM CONFIGURATION
# ====================================

# Option A: Mock LLM (No API calls - Good for development/testing)
USE_REAL_LLM=false
LLM_PROVIDER=mock

# Option B: OpenAI (GPT-4/GPT-3.5-turbo)
# USE_REAL_LLM=true
# LLM_PROVIDER=openai
# OPENAI_API_KEY=sk-proj-XXXXXXXXXX...
# OPENAI_MODEL=gpt-4-turbo-preview

# Option C: Anthropic Claude
# USE_REAL_LLM=true
# LLM_PROVIDER=anthropic
# ANTHROPIC_API_KEY=sk-ant-XXXXXXXXXX...

# ====================================
# SLACK INTEGRATION (Optional)
# ====================================

# Leave empty to skip Slack notifications
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# ====================================
# STREAMING PIPELINE SETTINGS
# ====================================
STREAMING_WINDOW_SECONDS=300
ANOMALY_THRESHOLD_PERCENTAGE=300
CONSECUTIVE_WINDOWS_THRESHOLD=1

# ====================================
# API SETTINGS
# ====================================
API_HOST=0.0.0.0
API_PORT=8000
CORS_ORIGINS=["*", "http://localhost:3000", "http://localhost:8000"]

# ====================================
# VECTOR DB SEARCH SETTINGS
# ====================================
VECTOR_SEARCH_TOP_K=5
MIN_SIMILARITY_SCORE=0.7
```

---

### Step 5: Set Up PostgreSQL Database

#### Option A: Local PostgreSQL Installation

**Windows:**
1. Download from https://www.postgresql.org/download/windows/
2. Run installer with default settings
3. Note the password you set for `postgres` user
4. Open pgAdmin (comes with PostgreSQL)

**Create database:**
```bash
# Using pgAdmin UI or psql:
psql -U postgres -c "CREATE DATABASE rag_of_fire;"
psql -U postgres -c "CREATE USER rag_user WITH PASSWORD 'secure_password';"
psql -U postgres -c "ALTER ROLE rag_user WITH CREATEDB;"
```

**Update `.env`:**
```env
DATABASE_URL=postgresql://rag_user:secure_password@localhost:5432/rag_of_fire
```

---

#### Option B: Supabase Cloud (Recommended)

**Easiest option - completely free tier:**

1. **Sign up at** https://supabase.com
2. **Create new project:**
   - Click "New Project"
   - Choose region (us-east-1 recommended)
   - Set database password (save this!)
3. **Get connection string:**
   - Go to "Settings" → "Database"
   - Copy "Connection string" (PostgreSQL tab)
   - Format: `postgresql://postgres:[PASSWORD]@db.[PROJECT_ID].supabase.co:5432/postgres`

**Update `.env`:**
```env
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@db.YOUR_PROJECT_ID.supabase.co:5432/postgres
```

**Test connection:**
```bash
psql "postgresql://postgres:YOUR_PASSWORD@db.YOUR_PROJECT_ID.supabase.co:5432/postgres" -c "SELECT version();"
```

---

### Step 6: Initialize Database Schema

```bash
cd backend
python -c "from backend.database.db import Base, engine; Base.metadata.create_all(engine); print('✅ Database schema created')"
```

**Or using Alembic (for migrations):**
```bash
# First time setup
alembic revision --autogenerate -m "Initial schema"
alembic upgrade head
```

---

### Step 7: Configure OpenAI API (Optional - for real LLM)

**If you want to use real LLM instead of mock:**

1. **Create OpenAI account:**
   - Go to https://platform.openai.com/
   - Sign up or log in
   - Click "API keys" in sidebar

2. **Create API key:**
   - Click "Create new secret key"
   - Copy the key (starts with `sk-proj-`)
   - Keep it secure!

3. **Update `.env`:**
   ```env
   USE_REAL_LLM=true
   LLM_PROVIDER=openai
   OPENAI_API_KEY=sk-proj-XXXXXXXXXX...
   OPENAI_MODEL=gpt-4-turbo-preview
   ```

4. **Set up billing:**
   - Add payment method at https://platform.openai.com/account/billing/overview
   - Set usage limits to prevent surprises

---

### Step 8: Configure Anthropic Claude (Optional)

**If you prefer Claude over GPT-4:**

1. **Create Anthropic account:**
   - Go to https://console.anthropic.com/
   - Sign up or log in
   - Click "API keys"

2. **Create API key:**
   - Click "Create Key"
   - Copy the key (starts with `sk-ant-`)

3. **Update `.env`:**
   ```env
   USE_REAL_LLM=true
   LLM_PROVIDER=anthropic
   ANTHROPIC_API_KEY=sk-ant-XXXXXXXXXX...
   ```

4. **Set up billing:**
   - Add payment method
   - Claude 3 costs ~$3 per 1M input tokens, $15 per 1M output tokens

---

### Step 9: Configure Slack Integration (Optional)

**To receive Slack notifications when incidents are detected:**

1. **Create Slack Workspace** (if needed):
   - Go to https://slack.com/create
   - Follow setup wizard

2. **Create Incoming Webhook:**
   - Go to https://api.slack.com/apps
   - Click "Create New App"
   - Choose "From scratch"
   - Name: "RAG of Fire"
   - Select workspace
   - Click "Create App"

3. **Enable Incoming Webhooks:**
   - In left sidebar, click "Incoming Webhooks"
   - Toggle "On"
   - Click "Add New Webhook to Workspace"
   - Select channel (e.g., #incidents or #alerts)
   - Click "Allow"

4. **Copy Webhook URL:**
   - You'll see the webhook URL: `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`
   - Update `.env`:
     ```env
     SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
     ```

5. **Test webhook:**
   ```bash
   curl -X POST -H 'Content-type: application/json' \
     --data '{"text":"Test message from RAG of Fire"}' \
     YOUR_WEBHOOK_URL
   ```

   You should see the message in Slack! ✅

---

### Step 10: Start Backend Server

```bash
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Expected output:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000 [RUNNING]
INFO:     Application startup complete
INFO:     Starting RAG of Fire v1.0.0
INFO:     Streaming pipeline initialized
INFO:     Vector database populated with 10 historical RCAs
INFO:     Notification service initialized
```

**Leave this terminal running!**

---

### Step 11: Start Frontend Server (New Terminal)

```bash
npm run dev
# or
pnpm dev
```

**Expected output:**
```
  ▲ Next.js 16.0.0
  - Local:        http://localhost:3000
  - Environments: .env.local

✓ Ready in 2.4s
```

---

### Step 12: Verify Installation

**Check backend:**
```bash
curl http://localhost:8000/health
# Expected: {"status": "healthy", ...}
```

**Open API docs:**
- http://localhost:8000/docs (Swagger UI)
- http://localhost:8000/redoc (ReDoc)

**Open frontend:**
- http://localhost:3000

**Check WebSocket connection:**
```bash
# In browser console:
ws = new WebSocket('ws://localhost:8000/ws/incidents');
ws.onmessage = (e) => console.log(e.data);
```

---

## 🎮 Quick Test After Setup

```bash
# Test decision generation (from any terminal)
curl -X POST http://localhost:8000/api/v1/decisions/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "error_type": "database_timeout",
    "spike_percentage": 450,
    "tenant_id": "payment",
    "service_name": "payments"
  }'
```

**Expected response:**
```json
{
  "id": "dec_...",
  "matched_incident": "INC-2025-001",
  "recommended_action": "Throttle to 30%",
  "confidence_score": 0.95,
  "latency_ms": 245
}
```

---

## 🔧 Configuration Reference

### Environment Variables Summary

| Variable | Purpose | Options | Default |
|----------|---------|---------|---------|
| `USE_REAL_LLM` | Enable real API calls | `true`/`false` | `false` |
| `LLM_PROVIDER` | Which LLM to use | `mock`, `openai`, `anthropic` | `mock` |
| `DEBUG` | Enable debug logging | `true`/`false` | `true` |
| `LOG_LEVEL` | Logging verbosity | `DEBUG`, `INFO`, `WARNING`, `ERROR` | `INFO` |
| `ANOMALY_THRESHOLD_PERCENTAGE` | Spike threshold | Any number | `300` |

---

### Database URLs Format

**PostgreSQL:**
```
postgresql://username:password@host:port/database
```

**Supabase:**
```
postgresql://postgres:password@db.project-id.supabase.co:5432/postgres
```

**Local (default):**
```
postgresql://postgres:postgres@localhost:5432/rag_of_fire
```

---

## 🚀 First Run Checklist

- [ ] Python 3.11+ installed
- [ ] Node.js 18+ installed
- [ ] PostgreSQL/Supabase configured
- [ ] `.env` file created with database URL
- [ ] `pip install -r requirements.txt` completed
- [ ] `npm install` completed
- [ ] Backend started: `uvicorn main:app --reload`
- [ ] Frontend started: `npm run dev`
- [ ] Visited http://localhost:3000
- [ ] Tested API at http://localhost:8000/docs
- [ ] WebSocket connection verified

---

## 7. **Access the Application**
   - Backend API: http://localhost:8000
   - API Docs: http://localhost:8000/docs
   - Frontend: http://localhost:3000

## 📚 API Documentation

### Decision Analysis

**Analyze an Anomaly and Generate Recommendation**
```http
POST /api/v1/decisions/analyze
Content-Type: application/json

{
  "error_type": "database_timeout",
  "metric_name": "db_query_latency_ms",
  "spike_percentage": 450,
  "tenant_id": "payment_service_prod",
  "service_name": "payments",
  "window_minutes": 5,
  "top_k": 5
}
```

**Response:**
```json
{
  "id": "dec_uuid",
  "matched_incident": "INC-2025-001",
  "symptom": "Database connection pool exhaustion detected",
  "recommended_action": "Throttle tenant traffic to 30% capacity",
  "confidence_score": 0.95,
  "citations": [
    "Historical Incident INC-2025-001: Similar 450% spike resolved with 30% throttle",
    "Baseline response: 200ms | Current: 8500ms"
  ],
  "retrieved_documents": [
    {
      "incident_id": "INC-2025-001",
      "title": "Database Connection Pool Exhaustion",
      "error_type": "database_timeout",
      "mitigation_metric": "Throttle to 30%",
      "similarity_score": 0.95
    }
  ],
  "latency_ms": 245,
  "generated_at": "2025-05-24T10:30:00Z"
}
```

### Report Streaming Anomaly

**Ingest Detected Anomaly**
```http
POST /api/v1/decisions/stream-anomaly
Content-Type: application/json

{
  "tenant_id": "analytics_events",
  "service_name": "event_processing",
  "error_type": "kafka_consumer_lag",
  "metric_name": "kafka_lag_messages",
  "baseline_value": 100,
  "current_value": 15000,
  "spike_percentage": 14900,
  "window_minutes": 5
}
```

### RCA Document Management

**Create RCA Document**
```http
POST /api/v1/documents/rcas
Content-Type: application/json

{
  "incident_id": "INC-2025-001",
  "title": "Database Connection Pool Exhaustion",
  "summary": "Payment service experiencing 450% spike in DB timeouts...",
  "root_cause": "N+1 query patterns in payment processing loop...",
  "impact": "Payment processing latency increased to 8500ms...",
  "resolution_action": "Implemented query batching, throttled traffic...",
  "resolution_confidence": 95,
  "mitigation_metric": "Throttle tenant traffic to 30%",
  "mitigation_effectiveness": 92,
  "error_type": "database_timeout",
  "affected_tenant": "payment_service_prod",
  "affected_service": "payments",
  "severity": "critical",
  "incident_date": "2025-05-24T02:15:00Z"
}
```

**Search RCAs**
```http
GET /api/v1/documents/rcas/search?query=database%20timeout&error_type=database_timeout&top_k=5
```

## 🔬 Historical Incident Data

The system comes pre-populated with 10 realistic historical RCA incidents:

| Incident | Error Type | Spike | Mitigation | Effectiveness |
|----------|-----------|-------|-----------|---|
| INC-2025-001 | database_timeout | 450% | Throttle to 30% | 92% |
| INC-2025-002 | kafka_consumer_lag | 320% | Throttle to 45% | 85% |
| INC-2025-003 | memory_leak | 280% | Throttle to 20% | 88% |
| INC-2025-004 | high_cpu_utilization | 280% | Throttle to 50% | 90% |
| INC-2025-005 | external_api_rate_limit | 620% | Throttle to 60% | 86% |
| INC-2025-006 | distributed_lock_deadlock | 410% | Throttle to 35% | 94% |
| INC-2025-007 | vector_db_timeout | 1300% | Throttle to 40% | 83% |
| INC-2025-008 | database_recursion_limit | 600% | Throttle to 70% | 91% |
| INC-2025-009 | ssl_certificate_expired | 100% | Manual failover | 99% |
| INC-2025-010 | access_control_misconfiguration | 250% | Throttle to 10% | 97% |

## 🏭 Implementation Phases

### ✅ Phase 1-5: Foundation & Core Services
- [x] Configuration management (Pydantic BaseSettings)
- [x] Enhanced database models (RCA, Anomaly, Decision tracking)
- [x] Vector DB schema with historical RCA data (10 production incidents)
- [x] ChromaDB integration with semantic search
- [x] Streaming pipeline stubs (asyncio.Queue + Flink tumbling windows)
- [x] Hybrid LLM engine (Deterministic Mock + Real APIs)

### ✅ Phase 6: REST APIs
- [x] Decision analysis endpoint (`/api/v1/decisions/analyze`)
- [x] Streaming anomaly reporting (`/api/v1/decisions/stream-anomaly`)
- [x] RCA document management (`/api/v1/documents/rcas`)
- [x] Document search and import

### 🚧 Phase 7-8: Notification & Real-time Streaming
- [ ] WebSocket connection manager
- [ ] Slack integration webhook client
- [ ] Real-time dashboard updates via WebSocket
- [ ] Decision broadcast to connected clients

### 📋 Phase 9-10: Testing & Optimization
- [ ] End-to-end integration tests
- [ ] Performance benchmarking
- [ ] Code documentation
- [ ] Deployment guidelines

## 🧪 Testing the System

### Test: Database Timeout Detection

```python
import requests

# 1. Report a database timeout anomaly
response = requests.post(
    "http://localhost:8000/api/v1/decisions/stream-anomaly",
    json={
        "tenant_id": "payment_service_prod",
        "service_name": "payments",
        "error_type": "database_timeout",
        "metric_name": "db_connection_pool_active",
        "baseline_value": 60,
        "current_value": 100,
        "spike_percentage": 66.67,
        "window_minutes": 5
    }
)
print(f"Anomaly reported: {response.json()}")

# 2. Generate recommendation
response = requests.post(
    "http://localhost:8000/api/v1/decisions/analyze",
    json={
        "error_type": "database_timeout",
        "metric_name": "db_query_time_p99_ms",
        "spike_percentage": 450,
        "tenant_id": "payment_service_prod",
        "service_name": "payments",
        "window_minutes": 5,
        "top_k": 5
    }
)

decision = response.json()
print(f"Matched Incident: {decision['matched_incident']}")
print(f"Recommendation: {decision['recommended_action']}")
print(f"Confidence: {decision['confidence_score']:.2%}")
print(f"Citations: {decision['citations']}")
```

### Test: RCA Import

```python
import requests
from backend.services.vector_db_init import HISTORICAL_RCAS

# Import historical data
rcas_to_import = []
for rca in HISTORICAL_RCAS:
    rcas_to_import.append({
        "incident_id": rca["incident_id"],
        "title": rca["title"],
        "summary": rca["summary"],
        "root_cause": rca["root_cause"],
        "impact": rca["impact"],
        "resolution_action": rca["resolution_action"],
        "resolution_confidence": rca["resolution_confidence"],
        "mitigation_metric": rca["mitigation_metric"],
        "mitigation_effectiveness": rca["mitigation_effectiveness"],
        "error_type": rca["error_type"],
        "affected_tenant": rca["affected_tenant"],
        "affected_service": rca["affected_service"],
        "severity": rca["severity"],
        "incident_date": rca["incident_date"].isoformat()
    })

response = requests.post(
    "http://localhost:8000/api/v1/documents/rcas/import-bulk",
    json=rcas_to_import
)

print(f"Import result: {response.json()}")
```

## 🔐 Production Deployment Checklist

- [ ] PostgreSQL database setup and migration
- [ ] ChromaDB persistence directory configured
- [ ] Environment variables properly set
- [ ] OpenAI/Anthropic API keys (if using real LLM)
- [ ] Slack webhook URL configured
- [ ] SSL/TLS certificates configured
- [ ] CORS origins properly restricted
- [ ] Rate limiting implemented
- [ ] Monitoring and alerting setup
- [ ] Backup strategy for vector DB

## 📖 Key Features

✨ **Deterministic Decisions**: Mock LLM generates production-ready recommendations without API calls
🎯 **Historical Context**: 10 realistic incidents with exact metrics and mitigation effectiveness  
📊 **Citation Trail**: Every recommendation includes evidence from historical incidents
⚡ **Real-time Pipeline**: asyncio.Queue simulates Kafka/Flink streaming architecture
🔍 **Semantic Search**: ChromaDB vector DB finds contextually similar incidents
🤖 **Hybrid LLM Support**: Mock provider for dev/test, OpenAI/Anthropic for production
📈 **Structured Output**: JSON schema ensures parsing reliability
🚀 **Zero Setup**: Pre-populated with historical data, runs immediately

## 🛠️ Technology Stack

| Component | Technology |
|-----------|-----------|
| **Backend Framework** | FastAPI + uvicorn |
| **Vector DB** | ChromaDB (local, persistent) |
| **Relational DB** | PostgreSQL (Supabase compatible) |
| **Frontend** | Next.js 16 + TypeScript |
| **Embeddings** | sentence-transformers |
| **LLM Integration** | OpenAI/Anthropic async clients |
| **Streaming** | asyncio.Queue (Kafka/Flink simulation) |
| **Web Server** | Uvicorn ASGI |
| **Validation** | Pydantic v2 |

## 📝 Code Examples

### Accessing the LLM Engine

```python
from backend.services.llm_engine import LLMEngine

llm = LLMEngine()
decision = await llm.generate_decision(
    anomaly_context={
        "error_type": "database_timeout",
        "spike_percentage": 450,
        "tenant_id": "payment_service_prod"
    },
    matched_rcas=[...]
)
```

### Querying the Vector DB

```python
from backend.services.vector_db import VectorDatabase

db = VectorDatabase()
results = await db.search_rcas(
    query="database timeout 300% spike",
    error_type_filter="database_timeout",
    k=5
)
```

### Ingesting Anomalies

```python
from backend.services.pipeline_stub import get_pipeline, StreamMessage
from decimal import Decimal

pipeline = await get_pipeline()
message = StreamMessage(
    id="msg_1",
    tenant_id="tenant_1",
    service_name="payments",
    error_type="database_timeout",
    metric_name="db_latency",
    baseline_value=200,
    current_value=8500,
    spike_percentage=Decimal("4150"),
    timestamp=datetime.utcnow()
)
await pipeline.ingest_message(message)
```

## 🎓 Learning Resources

- **FastAPI Documentation**: https://fastapi.tiangolo.com
- **ChromaDB Docs**: https://docs.trychroma.com
- **Pydantic V2**: https://docs.pydantic.dev/latest/
- **Sentence Transformers**: https://www.sbert.net
- **Next.js 16**: https://nextjs.org/docs

## 📞 Support & Troubleshooting

**ChromaDB Connection Issues:**
```bash
# Clear ChromaDB data and reinitialize
rm -rf ./data/chroma_db/
# Then restart the server
```

**Vector DB Empty:**
Import sample RCA data:
```bash
python -c "from backend.services.vector_db_init import HISTORICAL_RCAS; print(f'Loaded {len(HISTORICAL_RCAS)} RCAs')"
```

**LLM API Errors:**
Check `.env` file for correct API keys and `USE_REAL_LLM=false` to use mock provider.

## 📄 License

This project is provided as-is for educational and production use.

## 🙋 Contributing

Contributions welcome! Please ensure:
- Type hints on all functions
- Comprehensive docstrings
- Error handling with structured logging
- Tests for new features

---

**Built for 3 AM On-Call Clarity** 🚀
