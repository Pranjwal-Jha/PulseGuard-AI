import json
import logging
import time
from typing import Dict, List, Any
import openai
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
    """Production LLM Engine using OpenAI with structured outputs and retries."""
    
    def __init__(self):
        self.provider = settings.llm_provider
        if self.provider != 'openai':
            logger.warning(f"Unsupported provider {self.provider}, falling back to OpenAI.")
        
        self.client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
        self.model = settings.llm_model

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
    async def generate_decision(self, anomaly_context: Dict[str, Any], matched_rcas: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate an AI decision using OpenAI structured outputs."""
        
        system_prompt = (
            "You are an expert SRE AI assistant (PulseGuard-AI). "
            "Your job is to analyze real-time streaming anomalies and historical Root Cause Analyses (RCAs), "
            "and output a structured mitigation decision."
        )
        
        user_prompt = f"Anomaly Context:\n{json.dumps(anomaly_context, indent=2)}\n\n"
        user_prompt += "Historical Matches:\n"
        for idx, rca in enumerate(matched_rcas):
            user_prompt += f"--- Match {idx + 1} ---\n{json.dumps(rca, indent=2)}\n"
            
        try:
            start_time = time.time()
            response = await self.client.beta.chat.completions.parse(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format=DecisionStructuredOutput,
                temperature=0.2
            )
            
            latency = int((time.time() - start_time) * 1000)
            parsed_decision = response.choices[0].message.parsed
            
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

    async def get_provider_info(self) -> str:
        return f"{self.provider} ({self.model})"
