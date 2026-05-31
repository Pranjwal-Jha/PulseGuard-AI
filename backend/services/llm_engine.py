import json
import logging
import time
from typing import Dict, List, Any
from google import genai
from google.genai import types
from pydantic import BaseModel
from tenacity import retry, wait_exponential, stop_after_attempt
from backend.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Define the structured output format using Pydantic
class DecisionStructuredOutput(BaseModel):
    matched_incident: str
    symptom: str
    recommended_action: str
    confidence_score: float
    citations: List[str]

class LLMEngine:
    """Production LLM Engine using Gemini (google-genai) with structured outputs and retries."""
    
    def __init__(self):
        self.provider = settings.llm_provider
        if self.provider != 'gemini':
            logger.warning(f"Unsupported provider {self.provider}, falling back to Gemini.")
        
        self.client = genai.Client(api_key=settings.gemini_api_key)
        self.model = settings.llm_model

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
    async def generate_decision(self, anomaly_context: Dict[str, Any], matched_rcas: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate an AI decision using Gemini structured outputs."""
        
        system_prompt = (
            "You are an expert SRE AI assistant (PulseGuard-AI). "
            "Your job is to analyze real-time streaming anomalies and historical Root Cause Analyses (RCAs), "
            "and output a structured mitigation decision."
        )
        
        # Pull live incidents from Coral
        live_incidents_text = ""
        try:
            from backend.services.coral_client import query_coral
            live_incidents = await query_coral("SELECT id, title, status FROM pagerduty.incidents LIMIT 3")
            if live_incidents:
                live_incidents_text = "\n\nLive PagerDuty Incidents:\n"
                for inc in live_incidents:
                    live_incidents_text += f"- [{inc.get('status')}] {inc.get('id')}: {inc.get('title')}\n"
            else:
                live_incidents_text = "\n\nLive PagerDuty Incidents:\nNone right now."
        except Exception as e:
            logger.error(f"Coral integration failed: {e}")

        user_prompt = f"Anomaly Context:\n{json.dumps(anomaly_context, indent=2)}\n\n"
        user_prompt += "Historical Matches:\n"
        for idx, rca in enumerate(matched_rcas):
            user_prompt += f"--- Match {idx + 1} ---\n{json.dumps(rca, indent=2)}\n"
            
        user_prompt += live_incidents_text
            
        try:
            start_time = time.time()
            prompt = f"{system_prompt}\n\n{user_prompt}"
            
            response = await self.client.aio.models.generate_content(
                model=self.model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2,
                    response_mime_type="application/json",
                    response_schema=DecisionStructuredOutput
                )
            )
            
            latency = int((time.time() - start_time) * 1000)
            parsed_decision = DecisionStructuredOutput.model_validate_json(response.text)
            
            return {
                "matched_incident": parsed_decision.matched_incident,
                "symptom": parsed_decision.symptom,
                "recommended_action": parsed_decision.recommended_action,
                "confidence_score": parsed_decision.confidence_score,
                "citations": parsed_decision.citations,
                "llm_response": json.dumps(parsed_decision.model_dump()),
                "latency_ms": latency
            }
            
        except Exception as e:
            logger.error(f"Error in LLM generation: {e}")
            raise

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
    async def generate(self, prompt: str, context: str = "", max_tokens: int = 2000) -> Dict[str, Any]:
        """Generate a free-form LLM response given a prompt and optional context."""
        system_prompt = (
            "You are an expert SRE AI assistant (PulseGuard-AI). "
            "Analyze the provided context and answer the user query accurately."
        )

        user_content = prompt
        if context:
            user_content = f"Context:\n{context}\n\nQuery:\n{prompt}"

        try:
            start_time = time.time()
            prompt_str = f"{system_prompt}\n\n{user_content}"
            response = await self.client.aio.models.generate_content(
                model=self.model,
                contents=prompt_str,
                config=types.GenerateContentConfig(
                    temperature=0.2,
                    max_output_tokens=max_tokens,
                )
            )

            latency = int((time.time() - start_time) * 1000)
            content = response.text or ""
            usage = response.usage_metadata

            return {
                "response": content,
                "model": settings.llm_model,
                "latency_ms": latency,
                "token_estimate": usage.total_token_count if usage else len(content.split()),
            }

        except Exception as e:
            logger.error(f"Error in LLM generate: {e}")
            raise

    async def get_provider_info(self) -> str:
        return f"{self.provider} ({settings.llm_model})"


_llm_engine_instance = None


def get_llm_engine() -> LLMEngine:
    global _llm_engine_instance
    if _llm_engine_instance is None:
        _llm_engine_instance = LLMEngine()
    return _llm_engine_instance
