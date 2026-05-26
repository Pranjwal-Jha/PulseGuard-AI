# RAG of Fire - Production-Grade RAG System

A comprehensive Retrieval-Augmented Generation (RAG) system combining streaming data pipelines, vector database integration, and LLM-powered decision engines.

## System Architecture

### Backend Components

#### 1. **FastAPI Application** (`backend/main.py`)
- RESTful API with full OpenAPI documentation
- CORS middleware for cross-origin requests
- Comprehensive logging and error handling

#### 2. **Data Pipeline System**
- **Kafka Stub** (`backend/services/kafka_stub.py`): In-memory Kafka implementation with topics, producers, and consumers
- **Flink Stub** (`backend/services/flink_stub.py`): Stateful stream processor with windowing (tumbling/sliding)
- **Stream Processor Orchestrator** (`backend/services/stream_processor.py`): Coordinates Kafka→Flink data flow

#### 3. **Vector Database**
- **ChromaDB Integration** (`backend/services/vector_db.py`): Semantic search with automatic embedding generation
- **Document Processor** (`backend/services/document_processor.py`): Chunking, embedding, and retrieval

#### 4. **LLM Engine**
- **Mock Provider**: Deterministic responses for development
- **OpenAI Provider**: Integration with GPT-4 and other models
- **Anthropic Provider**: Integration with Claude models
- **Decision Engine** (`backend/services/decision_engine.py`): Hybrid RAG combining retrieval + generation

#### 5. **Notification System**
- Email support (via SMTP)
- Slack webhook integration
- Custom webhooks
- In-app notification storage

### API Routes

#### Health Check (`/health`)
- `GET /health` - General health status
- `GET /health/vector-db` - ChromaDB health
- `GET /health/llm` - LLM engine health

#### Pipelines (`/pipelines`)
- `POST /pipelines` - Create pipeline
- `GET /pipelines` - List all pipelines
- `GET /pipelines/{id}` - Get pipeline details
- `POST /pipelines/{id}/start` - Start pipeline
- `POST /pipelines/{id}/stop` - Stop pipeline
- `DELETE /pipelines/{id}` - Delete pipeline
- `POST /pipelines/{id}/test-event` - Publish test event
- `GET /pipelines/stats` - Get pipeline statistics

#### Documents (`/documents`)
- `POST /documents` - Upload and process document
- `GET /documents` - List collections
- `GET /documents/{id}` - Get document chunks
- `DELETE /documents/{id}` - Delete document
- `POST /documents/search` - Semantic search
- `GET /documents/health` - Document system health

#### Decisions (`/decisions`)
- `POST /decisions/evaluate` - Evaluate single query
- `POST /decisions/evaluate-batch` - Evaluate multiple queries
- `GET /decisions` - Get recent decisions
- `GET /decisions/{id}` - Get specific decision
- `GET /decisions/stats` - Decision statistics
- `DELETE /decisions` - Clear history
- `GET /decisions/health` - Decision system health

#### Notifications (`/notifications`)
- `POST /notifications` - Send notification
- `GET /notifications/{id}` - Get notification
- `GET /notifications` - Get recent notifications
- `GET /notifications/decision/{id}` - Get decision notifications

## Running the Backend

### Installation

```bash
# Install Python dependencies
pip install fastapi uvicorn pydantic python-dotenv psycopg2-binary \
  sqlalchemy alembic chromadb pydantic-settings APScheduler

# Or use the provided installation command
python3 -m pip install --break-system-packages fastapi uvicorn pydantic \
  python-dotenv psycopg2-binary sqlalchemy alembic chromadb \
  pydantic-settings APScheduler
```

### Configuration

Create or edit `.env.local`:

```env
DEBUG=true
DATABASE_URL=sqlite:///./rag_of_fire.db
USE_REAL_LLM=false
LLM_PROVIDER=openai
```

### Start Development Server

```bash
cd /vercel/share/v0-project
python3 -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload
```

Access the API at `http://127.0.0.1:8000`
- Interactive docs: `http://127.0.0.1:8000/docs`
- OpenAPI schema: `http://127.0.0.1:8000/openapi.json`

## Example Workflows

### 1. Document Ingestion & Search

```python
# Upload document
curl -X POST http://localhost:8000/documents \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Safety Guidelines",
    "content": "All equipment must be inspected monthly...",
    "chunking_strategy": "sentences"
  }'

# Search documents
curl -X POST http://localhost:8000/documents/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What are the safety requirements?",
    "top_k": 5
  }'
```

### 2. Decision Evaluation (RAG)

```python
# Evaluate query with RAG
curl -X POST http://localhost:8000/decisions/evaluate \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What are the safety concerns?",
    "top_k": 5,
    "include_context": true
  }'
```

### 3. Pipeline Creation & Monitoring

```python
# Create pipeline
curl -X POST http://localhost:8000/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "source_topic": "safety_events",
    "window_duration": 60
  }'

# Start pipeline
curl -X POST http://localhost:8000/pipelines/{pipeline_id}/start

# Get status
curl http://localhost:8000/pipelines/{pipeline_id}
```

## Core Services

### VectorDatabase (`backend/services/vector_db.py`)

```python
from backend.services.vector_db import get_vector_db

db = get_vector_db()

# Create collection
await db.create_collection("documents")

# Add documents
await db.add_documents(
    collection_name="documents",
    documents=["Document 1 content", "Document 2 content"],
    ids=["doc_1", "doc_2"]
)

# Search
results = await db.search(
    collection_name="documents",
    query="Search query",
    k=5
)
```

### DocumentProcessor (`backend/services/document_processor.py`)

```python
from backend.services.document_processor import get_document_processor

processor = get_document_processor()

# Process document
result = await processor.process_document(
    doc_id="unique_id",
    title="Document Title",
    content="Full document content...",
    chunking_strategy="sentences"
)

# Search documents
results = await processor.search_documents(
    query="Search query",
    top_k=5
)
```

### DecisionEngine (`backend/services/decision_engine.py`)

```python
from backend.services.decision_engine import get_decision_engine

engine = get_decision_engine()

# Evaluate query
decision = await engine.evaluate(
    query="What should I do?",
    top_k=5,
    include_context=True
)

# Get statistics
stats = engine.get_decision_stats()
```

### LLMEngine (`backend/services/llm_engine.py`)

```python
from backend.services.llm_engine import get_llm_engine

engine = get_llm_engine()

# Generate response
result = await engine.generate(
    prompt="Your question here",
    context="Relevant context",
    max_tokens=2000
)

# Get provider info
info = await engine.get_provider_info()
```

### StreamProcessor (`backend/services/stream_processor.py`)

```python
from backend.services.stream_processor import get_pipeline_manager

manager = get_pipeline_manager()

# Create pipeline
pipeline_id = await manager.create_pipeline(
    source_topic="input_topic",
    window_duration=60
)

# Start pipeline
job_id = await manager.start_pipeline(pipeline_id)

# Monitor
stats = manager.get_stats()
```

## Data Flow

```
Input Event
    ↓
Kafka Topic (Stub) ← Producer
    ↓
Consumer Group
    ↓
Flink Job (Stateful Processing)
    ├─ Window Aggregation
    ├─ State Management
    └─ Threshold Detection
    ↓
Document Retrieval (ChromaDB)
    ↓
LLM Generation
    ├─ Mock Provider (Development)
    ├─ OpenAI API (Production)
    └─ Anthropic API (Optional)
    ↓
Decision Output
    ↓
Notifications
    ├─ Email
    ├─ Slack
    ├─ Webhook
    └─ In-App Storage
```

## Configuration

### Environment Variables

```env
# App
DEBUG=true
APP_NAME=RAG of Fire
APP_VERSION=0.1.0

# Database
DATABASE_URL=sqlite:///./rag_of_fire.db

# Vector DB
VECTOR_DB_PATH=./data/chroma_db
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2

# LLM
USE_REAL_LLM=false
LLM_PROVIDER=openai
LLM_MODEL=gpt-4-turbo
OPENAI_API_KEY=your-key-here
ANTHROPIC_API_KEY=your-key-here

# Notifications
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SLACK_WEBHOOK_URL=https://hooks.slack.com/...

# Server
HOST=127.0.0.1
PORT=8000
```

## Development Notes

### Mock vs. Real LLM

By default, the system uses a Mock LLM provider that returns deterministic responses. This is ideal for:
- Development and testing
- Cost control (no API calls)
- Offline operation

To enable real LLM API:
1. Set `USE_REAL_LLM=true` in environment
2. Provide API key (`OPENAI_API_KEY` or `ANTHROPIC_API_KEY`)
3. The system will automatically fall back to mock if API fails

### Kafka/Flink Stubs

The system includes in-memory implementations of Kafka and Flink:
- **No Docker required** for local development
- **Fully functional** for testing pipelines
- **Upgrade path** to real systems by swapping implementations

### ChromaDB Persistence

- Documents and embeddings are persisted in `./data/chroma_db/`
- Supports multiple collections
- Automatic embedding generation using sentence-transformers

## Testing

```python
# Run backend tests
pytest backend/tests/

# Test vector DB
python3 -m pytest backend/tests/test_vector_db.py -v

# Test LLM engine
python3 -m pytest backend/tests/test_llm_engine.py -v

# Test pipelines
python3 -m pytest backend/tests/test_pipelines.py -v
```

## Production Deployment

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY backend/ /app/backend/
EXPOSE 8000
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Environment Setup

1. Use managed database (PostgreSQL, Supabase, etc.)
2. Configure real LLM API keys
3. Set up email/Slack credentials
4. Enable HTTPS
5. Implement authentication/authorization

### Monitoring

- FastAPI OpenAPI docs at `/docs`
- Health checks at `/health` endpoints
- Structured logging with correlation IDs
- Metrics endpoint (extensible)

## Performance Considerations

- **Vector DB**: Automatically optimizes embeddings
- **Streaming**: Async/await throughout for high concurrency
- **LLM**: Caching and rate limiting (optional)
- **Pipelines**: Stateful processing with window optimization

## Future Enhancements

1. **Persistence**: Add database backends for decisions and logs
2. **WebSocket**: Real-time streaming updates to frontend
3. **Authentication**: JWT/OAuth2 for API security
4. **Caching**: Redis integration for multi-instance deployment
5. **Monitoring**: Prometheus metrics and APM integration
6. **Scaling**: Kubernetes deployment manifests

## Support & Documentation

- FastAPI docs: `http://localhost:8000/docs`
- OpenAPI schema: `http://localhost:8000/openapi.json`
- Issue tracking: (To be configured)
- Architecture decision records: `docs/adr/`

## License

MIT License - See LICENSE file for details
