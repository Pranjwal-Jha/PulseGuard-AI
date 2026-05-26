"""LLM Engine with mock and real API support for incident mitigation decisions."""

import asyncio
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod
import json
import time
from datetime import datetime
from backend.config import get_settings
from backend.utils.logging import get_logger

logger = get_logger("llm_engine")
settings = get_settings()


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    @abstractmethod
    async def generate(self, prompt: str, context: str = "", max_tokens: int = 2000) -> str:
        """Generate response from LLM."""
        pass
    
    @abstractmethod
    async def get_model_name(self) -> str:
        """Get model name."""
        pass


class MockLLMProvider(LLMProvider):
    """
    Deterministic Mock LLM provider for development and testing.
    Returns production-ready mitigation recommendations without API calls.
    """
    
    async def generate(self, prompt: str, context: str = "", max_tokens: int = 2000) -> str:
        """
        Generate deterministic mock response for incident mitigation.
        Simulates real API behavior with accurate throttling recommendations.
        """
        # Simulate processing time (50-200ms)
        await asyncio.sleep(0.1)
        
        prompt_lower = prompt.lower()
        context_lower = context.lower()
        
        # Deterministic responses based on error patterns
        if "database timeout" in prompt_lower or "database_timeout" in prompt_lower:
            return json.dumps({
                "symptom": "Database connection pool exhaustion detected",
                "matched_incident": "INC-2025-001",
                "historical_match": "Payment service experiencing 450% spike in DB timeouts due to connection pool exhaustion",
                "reasoning": "Current spike pattern matches historical incident INC-2025-001 (450% DB timeout increase). Previous resolution used connection pool throttling.",
                "recommended_action": "Throttle tenant traffic to 30% capacity",
                "estimated_recovery_minutes": 8,
                "confidence_score": 0.95,
                "citations": [
                    "Historical Incident INC-2025-001: Similar 450% spike resolved with 30% throttle",
                    "Baseline response: 200ms | Current: 8500ms | Spike: 4150%"
                ]
            })
        
        elif "kafka" in prompt_lower or "consumer lag" in prompt_lower:
            return json.dumps({
                "symptom": "Kafka consumer lag cascade detected",
                "matched_incident": "INC-2025-002",
                "historical_match": "Event processing pipeline experiencing 320% increase in consumer lag",
                "reasoning": "Exponential backlog growth matches INC-2025-002 pattern. Downstream service slowdown detected.",
                "recommended_action": "Throttle event ingestion to 45% capacity",
                "estimated_recovery_minutes": 10,
                "confidence_score": 0.88,
                "citations": [
                    "Historical Incident INC-2025-002: Similar 320% lag spike resolved with 45% throttle",
                    "Current lag: 15000 messages | Baseline: 100 messages"
                ]
            })
        
        elif "memory" in prompt_lower or "gc pause" in prompt_lower:
            return json.dumps({
                "symptom": "Memory leak with elevated GC pause times",
                "matched_incident": "INC-2025-003",
                "historical_match": "Session storage experiencing 280% memory growth and 3.2s GC pauses",
                "reasoning": "Memory usage pattern (2GB->5.6GB) matches session eviction bug in INC-2025-003",
                "recommended_action": "Throttle session creation to 20% during remediation",
                "estimated_recovery_minutes": 15,
                "confidence_score": 0.91,
                "citations": [
                    "Historical Incident INC-2025-003: Memory leak resolved with 20% throttle + fix",
                    "Memory spike: 280% | GC pause: 3.2 seconds (vs 50ms baseline)"
                ]
            })
        
        elif "cpu" in prompt_lower or "utilization" in prompt_lower:
            return json.dumps({
                "symptom": "CPU saturation from unoptimized operations",
                "matched_incident": "INC-2025-004",
                "historical_match": "Search service hitting 95% CPU from catastrophic backtracking regex",
                "reasoning": "CPU spike (25%->95%) matches regex optimization issue from INC-2025-004",
                "recommended_action": "Throttle search requests to 50% capacity",
                "estimated_recovery_minutes": 8,
                "confidence_score": 0.93,
                "citations": [
                    "Historical Incident INC-2025-004: CPU saturation resolved with 50% throttle",
                    "CPU: 95% utilization | Query latency: 10 seconds"
                ]
            })
        
        elif "rate limit" in prompt_lower or "external api" in prompt_lower:
            return json.dumps({
                "symptom": "External API rate limit cascade",
                "matched_incident": "INC-2025-005",
                "historical_match": "Email service hit SendGrid rate limits causing 2.5M email backlog",
                "reasoning": "Immediate API quota exhaustion matches email retry storm in INC-2025-005",
                "recommended_action": "Throttle outbound calls to 60% of normal rate",
                "estimated_recovery_minutes": 30,
                "confidence_score": 0.89,
                "citations": [
                    "Historical Incident INC-2025-005: API rate limit resolved with exponential backoff + 60% throttle",
                    "Quota: 250k/hour | Attempts: 1.8M/hour | Backlog: 2.5M items"
                ]
            })
        
        elif "lock" in prompt_lower or "deadlock" in prompt_lower:
            return json.dumps({
                "symptom": "Distributed lock deadlock detected",
                "matched_incident": "INC-2025-006",
                "historical_match": "Checkout flow experiencing circular lock dependency",
                "reasoning": "Timeout cascade with 28% success rate matches deadlock in INC-2025-006",
                "recommended_action": "Throttle checkout requests to 35% until lock hierarchy deployed",
                "estimated_recovery_minutes": 10,
                "confidence_score": 0.96,
                "citations": [
                    "Historical Incident INC-2025-006: Deadlock resolved with ordered lock acquisition",
                    "Success rate: 28% | Lock timeouts: 410% increase"
                ]
            })
        
        elif "vector" in prompt_lower or "similarity search" in prompt_lower:
            return json.dumps({
                "symptom": "Vector database query timeout spiral",
                "matched_incident": "INC-2025-007",
                "historical_match": "Recommendation engine experiencing 2.8s query latencies",
                "reasoning": "Full-table vector search latency spike matches INC-2025-007",
                "recommended_action": "Throttle vector searches to 40% until indexing optimized",
                "estimated_recovery_minutes": 9,
                "confidence_score": 0.87,
                "citations": [
                    "Historical Incident INC-2025-007: Vector DB optimized with IVF->HNSW indexing",
                    "Query latency: 2.8s vs 200ms baseline | Embeddings: 500M"
                ]
            })
        
        elif "recursive" in prompt_lower or "recursion" in prompt_lower:
            return json.dumps({
                "symptom": "Recursive query limit exceeded",
                "matched_incident": "INC-2025-008",
                "historical_match": "Analytics pipeline hit SQL recursion depth limit",
                "reasoning": "Query timeout (30s) with stale data matches analytics recursion bug",
                "recommended_action": "Throttle analytics to 70% throughput pending fix",
                "estimated_recovery_minutes": 7,
                "confidence_score": 0.94,
                "citations": [
                    "Historical Incident INC-2025-008: Recursion resolved with iterative rewrite",
                    "Query time: 6x increase | Recursion depth: 500"
                ]
            })
        
        elif "ssl" in prompt_lower or "certificate" in prompt_lower:
            return json.dumps({
                "symptom": "SSL certificate expiration causing 100% failures",
                "matched_incident": "INC-2025-009",
                "historical_match": "API Gateway SSL certificate expired",
                "reasoning": "All requests failing with SSL errors matches certificate expiration incident",
                "recommended_action": "Emergency certificate installation + 10% traffic failover",
                "estimated_recovery_minutes": 5,
                "confidence_score": 1.0,
                "citations": [
                    "Historical Incident INC-2025-009: Emergency failover completed in 5 minutes",
                    "Affected requests: 500k+ | Downtime: 5 minutes"
                ]
            })
        
        elif "access" in prompt_lower or "bucket" in prompt_lower or "permissions" in prompt_lower:
            return json.dumps({
                "symptom": "Object storage access control misconfiguration",
                "matched_incident": "INC-2025-010",
                "historical_match": "S3 bucket accidentally set to public-read",
                "reasoning": "Unexpected bandwidth spike with unauthorized access matches public bucket incident",
                "recommended_action": "Throttle object storage reads to 10% + immediately revert to private",
                "estimated_recovery_minutes": 3,
                "confidence_score": 0.98,
                "citations": [
                    "Historical Incident INC-2025-010: Privacy incident - 12GB exposed",
                    "Outbound bandwidth: 250% spike | Unauthorized requests: 50k+"
                ]
            })
        
        else:
            # Default: generic analysis
            return json.dumps({
                "symptom": "Anomaly detected in streaming pipeline",
                "matched_incident": "GENERIC-ANALYSIS",
                "historical_match": "Pattern analysis indicates potential performance degradation",
                "reasoning": "Insufficient specificity for precise matching. Additional context needed.",
                "recommended_action": "Implement 25% traffic throttle as precautionary measure",
                "estimated_recovery_minutes": 15,
                "confidence_score": 0.5,
                "citations": [
                    "Generic analysis - requires more specific error telemetry"
                ]
            })
    
    async def get_model_name(self) -> str:
        """Get model name."""
        return "mock-llm-deterministic-v1"


class OpenAIProvider(LLMProvider):
    """OpenAI LLM provider for real API calls."""
    
    def __init__(self, api_key: str, model: str = "gpt-4-turbo"):
        """Initialize OpenAI provider."""
        self.api_key = api_key
        self.model = model
        self.client = None
        
        try:
            from openai import AsyncOpenAI
            self.client = AsyncOpenAI(api_key=api_key)
            logger.info(f"OpenAI provider initialized with model: {model}")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI: {e}")
            raise RuntimeError(f"OpenAI initialization failed: {e}")
    
    async def generate(self, prompt: str, context: str = "", max_tokens: int = 2000) -> str:
        """Generate response using OpenAI API."""
        if not self.client:
            raise RuntimeError("OpenAI client not initialized")
        
        try:
            system_prompt = (
                "You are an expert incident response AI system for production infrastructure. "
                "Generate ONLY valid JSON responses with deterministic mitigation recommendations. "
                "Always reference historical incidents and provide exact mitigation metrics."
            )
            
            full_prompt = f"{context}\n\nAnalyze this incident and provide mitigation:" if context else prompt
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": full_prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.3  # Low temperature for deterministic output
            )
            
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            raise
    
    async def get_model_name(self) -> str:
        """Get model name."""
        return self.model


class AnthropicProvider(LLMProvider):
    """Anthropic (Claude) LLM provider."""
    
    def __init__(self, api_key: str, model: str = "claude-opus-4.1"):
        """Initialize Anthropic provider."""
        self.api_key = api_key
        self.model = model
        self.client = None
        
        try:
            from anthropic import AsyncAnthropic
            self.client = AsyncAnthropic(api_key=api_key)
            logger.info(f"Anthropic provider initialized with model: {model}")
        except Exception as e:
            logger.error(f"Failed to initialize Anthropic: {e}")
            raise RuntimeError(f"Anthropic initialization failed: {e}")
    
    async def generate(self, prompt: str, context: str = "", max_tokens: int = 2000) -> str:
        """Generate response using Claude API."""
        if not self.client:
            raise RuntimeError("Anthropic client not initialized")
        
        try:
            system_prompt = (
                "You are an expert incident response AI system for production infrastructure. "
                "Generate ONLY valid JSON responses with deterministic mitigation recommendations. "
                "Always reference historical incidents and provide exact mitigation metrics."
            )
            
            full_prompt = f"{context}\n\nAnalyze this incident and provide mitigation:" if context else prompt
            
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": full_prompt}
                ]
            )
            
            return response.content[0].text
        except Exception as e:
            logger.error(f"Anthropic API error: {e}")
            raise
    
    async def get_model_name(self) -> str:
        """Get model name."""
        return self.model


class LLMEngine:
    """Main LLM Engine managing provider selection and decision generation."""
    
    def __init__(self):
        """Initialize LLM engine."""
        self.provider: Optional[LLMProvider] = None
        self._init_provider()
    
    def _init_provider(self):
        """Initialize LLM provider based on configuration."""
        try:
            if settings.use_real_llm:
                if settings.llm_provider == "openai":
                    if not settings.openai_api_key:
                        logger.warning("OpenAI API key not set, falling back to mock")
                        self.provider = MockLLMProvider()
                    else:
                        self.provider = OpenAIProvider(
                            api_key=settings.openai_api_key,
                            model=settings.llm_model
                        )
                        logger.info(f"Using OpenAI provider: {settings.llm_model}")
                
                elif settings.llm_provider == "anthropic":
                    if not settings.anthropic_api_key:
                        logger.warning("Anthropic API key not set, falling back to mock")
                        self.provider = MockLLMProvider()
                    else:
                        self.provider = AnthropicProvider(
                            api_key=settings.anthropic_api_key,
                            model=settings.llm_model
                        )
                        logger.info(f"Using Anthropic provider: {settings.llm_model}")
                else:
                    logger.warning(f"Unknown LLM provider: {settings.llm_provider}")
                    self.provider = MockLLMProvider()
            else:
                self.provider = MockLLMProvider()
                logger.info("Using mock LLM provider (use_real_llm=False)")
        
        except Exception as e:
            logger.error(f"Error initializing LLM provider: {e}")
            logger.info("Falling back to mock provider")
            self.provider = MockLLMProvider()
    
    async def generate_decision(
        self,
        anomaly_context: Dict[str, Any],
        matched_rcas: List[Dict[str, Any]],
        max_tokens: int = 2000
    ) -> Dict[str, Any]:
        """
        Generate AI decision for incident mitigation with strict citation trail.
        
        Args:
            anomaly_context: Current streaming anomaly details
            matched_rcas: Historical RCA matches from vector DB
            max_tokens: Maximum response tokens
        
        Returns:
            Decision payload with recommendation and citations
        """
        try:
            start_time = time.time()
            
            # Build context from matched RCAs
            context_builder = "HISTORICAL INCIDENT CONTEXT:\n"
            for i, rca in enumerate(matched_rcas[:3], 1):  # Top 3 matches
                context_builder += (
                    f"\n{i}. Incident {rca['incident_id']} "
                    f"(Similarity: {rca.get('similarity_score', 0):.2%})\n"
                    f"   Error: {rca.get('error_type', 'unknown')}\n"
                    f"   Mitigation: {rca.get('mitigation_metric', 'unknown')}\n"
                    f"   Effectiveness: {rca.get('mitigation_effectiveness', 0):.0f}%\n"
                    f"   Summary: {rca.get('summary', 'N/A')[:200]}...\n"
                )
            
            # Build prompt
            prompt = (
                f"Current Anomaly:\n"
                f"- Error Type: {anomaly_context.get('error_type', 'unknown')}\n"
                f"- Metric: {anomaly_context.get('metric_name', 'unknown')}\n"
                f"- Spike: {anomaly_context.get('spike_percentage', 0):.2f}%\n"
                f"- Tenant: {anomaly_context.get('tenant_id', 'unknown')}\n"
                f"- Service: {anomaly_context.get('service_name', 'unknown')}\n\n"
                f"{context_builder}\n\n"
                f"Generate a deterministic mitigation recommendation based on historical patterns. "
                f"Return valid JSON only."
            )
            
            # Call LLM provider
            llm_response = await self.provider.generate(
                prompt=prompt,
                context=context_builder,
                max_tokens=max_tokens
            )
            
            # Parse response
            try:
                decision_data = json.loads(llm_response)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse LLM response as JSON, wrapping: {llm_response[:200]}")
                decision_data = {
                    "symptom": "Analysis result",
                    "recommended_action": llm_response[:500],
                    "confidence_score": 0.5
                }
            
            latency_ms = int((time.time() - start_time) * 1000)
            
            # Build final decision payload
            final_decision = {
                "query_context": prompt,
                "llm_response": llm_response,
                "matched_rca_id": matched_rcas[0].get("incident_id") if matched_rcas else None,
                "confidence_score": float(decision_data.get("confidence_score", 0.5)),
                "recommended_action": decision_data.get("recommended_action", "Manual review required"),
                "symptom": decision_data.get("symptom", "Unknown anomaly"),
                "matched_incident": decision_data.get("matched_incident", "No match"),
                "citations": decision_data.get("citations", []),
                "citation_data": {
                    "matched_historical_incidents": [
                        {
                            "id": rca.get("incident_id"),
                            "error_type": rca.get("error_type"),
                            "similarity": rca.get("similarity_score")
                        }
                        for rca in matched_rcas[:3]
                    ]
                },
                "latency_ms": latency_ms,
                "model": await self.provider.get_model_name()
            }
            
            logger.info(
                f"Decision generated: {final_decision.get('matched_incident')} "
                f"(confidence: {final_decision.get('confidence_score'):.2%}, latency: {latency_ms}ms)"
            )
            
            return final_decision
        
        except Exception as e:
            logger.error(f"Error generating decision: {e}", exc_info=True)
            raise
                else:
                    logger.warning(f"Unknown provider: {settings.llm_provider}, using mock")
                    self.provider = MockLLMProvider()
            else:
                self.provider = MockLLMProvider()
                logger.info("Using mock LLM provider")
        except Exception as e:
            logger.error(f"Error initializing LLM provider: {e}, falling back to mock")
            self.provider = MockLLMProvider()
    
    async def generate(
        self,
        prompt: str,
        context: str = "",
        max_tokens: int = 2000
    ) -> Dict[str, Any]:
        """Generate LLM response with metrics."""
        try:
            start_time = time.time()
            
            # Generate response
            response = await self.provider.generate(prompt, context, max_tokens)
            
            # Calculate metrics
            latency_ms = int((time.time() - start_time) * 1000)
            token_estimate = len(response.split()) + len(prompt.split())
            
            return {
                "response": response,
                "model": await self.provider.get_model_name(),
                "latency_ms": latency_ms,
                "token_estimate": token_estimate,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            raise
    
    async def get_provider_info(self) -> Dict[str, Any]:
        """Get current provider information."""
        return {
            "provider_type": type(self.provider).__name__,
            "model": await self.provider.get_model_name() if self.provider else "unknown",
            "use_real_api": settings.use_real_llm,
            "fallback_to_mock": isinstance(self.provider, MockLLMProvider)
        }


import asyncio


# Global LLM engine instance
_engine: Optional[LLMEngine] = None


def get_llm_engine() -> LLMEngine:
    """Get or create global LLM engine instance."""
    global _engine
    if _engine is None:
        _engine = LLMEngine()
    return _engine
