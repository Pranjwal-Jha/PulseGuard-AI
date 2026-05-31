# Coral & Gemini 3.5 Integration Update

## What We Did

1. **Coral Core Integration**
   - Wrote `backend/services/coral_client.py`: A lightweight asynchronous Python wrapper that executes Coral SQL queries directly via the CLI and parses the JSON output.
   - Updated `backend.Dockerfile`: Added a step to automatically download and install the Coral CLI binary during the container build process so it runs natively alongside the backend.

2. **Gemini 3.5 Flash Migration**
   - **Replaced OpenAI with Google GenAI SDK**: Stripped out OpenAI entirely and moved to the modern `google-genai` package.
   - **Updated `llm_engine.py`**: Refactored the `LLMEngine` to use `gemini-3.5-flash`. It now uses `response_schema` to guarantee that the LLM continues outputting the strict Pydantic JSON structure (`DecisionStructuredOutput`) the system expects.
   - **Dynamic Context Injection**: Wired the new Coral client directly into the LLM engine. Right before the LLM evaluates an anomaly, the backend queries `pagerduty.incidents` via Coral, grabbing the latest active incidents and seamlessly injecting them into the LLM's system prompt context.

3. **Infrastructure & Configs**
   - Updated `backend/config.py` to add `gemini_api_key` and set `gemini-3.5-flash` as the default model.
   - Updated `requirements.txt` to include `google-genai`.
   - Updated `docker-compose.yml` to correctly pipe the `GEMINI_API_KEY` from the host environment into the `pulseguard-backend` container.

## What's Next

1. **Dynamic SQL Generation**
   - Currently, the LLM engine hardcodes a query to PagerDuty. The next step is letting the AI decide *what* to query based on the anomaly (e.g., querying GitHub if it's a deployment anomaly, or Datadog if it's latency).

2. **Expanding Data Sources**
   - Configure additional local sources in Coral (`~/.coral/sources.yml`) like GitHub, Slack, or Datadog, so the system has a 360-degree view of the infrastructure.

3. **Full MCP Server Migration (Optional Phase)**
   - If the `subprocess` CLI integration ever becomes a bottleneck, we can upgrade to running Coral natively as a persistent MCP (Model Context Protocol) server for even lower latency and bidirectional tool usage.
