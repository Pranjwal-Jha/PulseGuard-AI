"""Decision Engine - RAG system combining retrieval and generation."""

from typing import Optional, List, Dict, Any
import time
import uuid
from datetime import datetime
from backend.services.vector_db import get_vector_db
from backend.services.document_processor import get_document_processor
from backend.services.llm_engine import get_llm_engine
from backend.utils.logging import get_logger

logger = get_logger("decision_engine")


class DecisionEngine:
    """RAG engine combining document retrieval and LLM generation."""
    
    def __init__(self):
        """Initialize decision engine."""
        self.vector_db = get_vector_db()
        self.document_processor = get_document_processor()
        self.llm_engine = get_llm_engine()
        self.decision_history: Dict[str, Dict] = {}
    
    async def evaluate(
        self,
        query: str,
        collection_name: str = "documents",
        top_k: int = 5,
        include_context: bool = True,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Evaluate a query: retrieve documents and generate response."""
        start_time = time.time()
        decision_id = str(uuid.uuid4())[:8]
        
        try:
            logger.info(f"Decision {decision_id}: Evaluating query: {query}")
            
            # Phase 1: Retrieve relevant documents
            logger.debug(f"Decision {decision_id}: Retrieving documents...")
            retrieved_docs = await self.document_processor.search_documents(
                query=query,
                collection_name=collection_name,
                top_k=top_k
            )
            
            # Phase 2: Build context from retrieved documents
            context_parts = []
            doc_references = []
            
            for i, doc in enumerate(retrieved_docs, 1):
                context_parts.append(f"[Doc {i}] {doc.get('document', '')}")
                doc_references.append({
                    "id": doc['id'],
                    "title": doc.get('document_title', 'Unknown'),
                    "relevance_score": doc.get('relevance_score', 0),
                    "chunk_index": doc.get('chunk_index', 0),
                    "content_preview": doc.get('document', '')[:200]
                })
            
            context = "\n\n".join(context_parts) if context_parts else "No relevant documents found."
            
            # Phase 3: Generate response using LLM
            logger.debug(f"Decision {decision_id}: Generating response...")
            llm_result = await self.llm_engine.generate(
                prompt=query,
                context=context if include_context else "",
                max_tokens=2000
            )
            
            # Calculate confidence based on document relevance
            confidence = 0.0
            if doc_references:
                confidence = sum(d['relevance_score'] for d in doc_references) / len(doc_references)
            confidence = min(confidence, 1.0)
            
            # Total latency
            total_latency_ms = int((time.time() - start_time) * 1000)
            
            # Build decision result
            decision = {
                "id": decision_id,
                "query": query,
                "retrieved_documents": doc_references,
                "llm_response": llm_result['response'],
                "confidence": confidence,
                "triggered_at": datetime.utcnow().isoformat(),
                "latency_ms": total_latency_ms,
                "metadata": {
                    "model": llm_result['model'],
                    "llm_latency_ms": llm_result['latency_ms'],
                    "token_estimate": llm_result['token_estimate'],
                    "retrieval_count": len(doc_references),
                    "include_context": include_context,
                    **(metadata or {})
                }
            }
            
            # Store in history
            self.decision_history[decision_id] = decision
            
            logger.info(f"Decision {decision_id} completed in {total_latency_ms}ms with confidence {confidence:.2f}")
            return decision
        
        except Exception as e:
            logger.error(f"Error in decision {decision_id}: {e}")
            raise
    
    async def batch_evaluate(
        self,
        queries: List[str],
        collection_name: str = "documents",
        top_k: int = 5,
        include_context: bool = True
    ) -> List[Dict[str, Any]]:
        """Evaluate multiple queries."""
        results = []
        for query in queries:
            try:
                result = await self.evaluate(
                    query=query,
                    collection_name=collection_name,
                    top_k=top_k,
                    include_context=include_context
                )
                results.append(result)
            except Exception as e:
                logger.error(f"Error evaluating query '{query}': {e}")
                results.append({
                    "error": str(e),
                    "query": query
                })
        
        return results
    
    def get_decision(self, decision_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a previous decision."""
        return self.decision_history.get(decision_id)
    
    def get_recent_decisions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent decisions."""
        decisions = list(self.decision_history.values())
        # Sort by triggered_at (most recent first)
        decisions.sort(key=lambda d: d['triggered_at'], reverse=True)
        return decisions[:limit]
    
    def get_decision_stats(self) -> Dict[str, Any]:
        """Get decision statistics."""
        decisions = list(self.decision_history.values())
        
        if not decisions:
            return {
                "total_decisions": 0,
                "average_latency_ms": 0,
                "average_confidence": 0
            }
        
        avg_latency = sum(d['latency_ms'] for d in decisions) / len(decisions)
        avg_confidence = sum(d['confidence'] for d in decisions) / len(decisions)
        
        return {
            "total_decisions": len(decisions),
            "average_latency_ms": int(avg_latency),
            "average_confidence": round(avg_confidence, 3),
            "confidence_range": [
                round(min(d['confidence'] for d in decisions), 3),
                round(max(d['confidence'] for d in decisions), 3)
            ],
            "latency_range_ms": [
                min(d['latency_ms'] for d in decisions),
                max(d['latency_ms'] for d in decisions)
            ]
        }
    
    def clear_history(self):
        """Clear decision history."""
        self.decision_history.clear()
        logger.info("Decision history cleared")


# Global decision engine instance
_engine: Optional[DecisionEngine] = None


def get_decision_engine() -> DecisionEngine:
    """Get or create global decision engine instance."""
    global _engine
    if _engine is None:
        _engine = DecisionEngine()
    return _engine
