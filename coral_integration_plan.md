# Coral Integration Plan for PulseGuard-AI

## 1. Overview
[Coral](https://withcoral.com/docs) is a local-first SQL runtime that provides a single SQL interface for various APIs, files, and live data sources. It is specifically designed to enhance AI agents by centralizing and simplifying how they access external tools (handling authentication, pagination, and joining across sources).

Currently, **PulseGuard-AI** uses historical RCAs via a Vector Database (`decision_engine.py`). By integrating Coral, PulseGuard can gain **real-time diagnostic capabilities**, allowing the LLM engine to query live external systems (like GitHub, Jira, Datadog, Slack) seamlessly to determine root causes.

## 2. Architecture Updates
Coral can be deployed in two main ways for AI agents:
1. **As an MCP (Model Context Protocol) Server**: This exposes Coral's SQL engine as a tool the LLM can call.
2. **As a Direct Database**: The backend executes SQL against Coral's endpoint, fetching data before feeding it to the LLM.

**Recommendation:** Run Coral as an **MCP Server** within the Docker ecosystem.
*   Update `docker-compose.yml` to include a `coral` service.
*   Update `backend/services/llm_engine.py` to support MCP tool calling, allowing the OpenAI model to dynamically issue SQL queries via Coral when analyzing an anomaly.

## 3. High-Value Use Cases for PulseGuard-AI
*   **Recent Code Changes:** `SELECT * FROM github.commits WHERE repository = 'pulseguard-backend' AND created_at > NOW() - INTERVAL '1 hour'`
*   **Active Incidents:** `SELECT * FROM pagerduty.incidents WHERE status = 'triggered'`
*   **Live Metrics:** Querying Prometheus/Datadog directly via SQL to correlate stream anomalies (from Kafka/Flink) with infrastructure CPU/Memory spikes.
*   **Team Communications:** Checking Slack for recent deployments or mentions of failing services.

## 4. Step-by-Step Implementation

### Phase 1: Infrastructure Setup
1. **Add Coral to Docker:**
   Add a new service in `docker-compose.yml` using the Coral Docker image, exposing the necessary ports (e.g., HTTP/MCP or Postgres wire protocol).
2. **Configure Data Sources:**
   Create a `coral/sources.yml` to define the integrations PulseGuard needs (e.g., GitHub, PagerDuty, Prometheus).
   Provide the API keys in the `.env` file and mount them to the Coral container.

### Phase 2: Backend Integration (`backend/services/llm_engine.py`)
1. **Implement MCP Client:**
   Add an MCP client library to interact with the Coral MCP server.
2. **Enhance LLM Tools:**
   Update `generate_decision` to provide the Coral MCP tools to the OpenAI client (`tools` parameter in `chat.completions.parse`). This allows the LLM to decide if it needs to fetch real-time data before returning the `DecisionStructuredOutput`.

### Phase 3: Update the Decision Engine (`backend/services/decision_engine.py`)
1. **Multi-Step Reasoning:**
   Update the `evaluate` method to support multi-step agentic behavior:
   *   *Step 1:* Retrieve historical RCAs from the vector database.
   *   *Step 2:* Let the LLM Engine use Coral to fetch real-time context (e.g., "Are there any recent PRs touching this service?").
   *   *Step 3:* Synthesize the historical data and live data to produce the final mitigation decision.

### Phase 4: Observability
1. **Tracing:**
   Coral supports OpenTelemetry. Point Coral's OTLP exporter to the existing `jaeger:4317` service in `docker-compose.yml` to maintain end-to-end tracing of all SQL queries executed by the agent.
