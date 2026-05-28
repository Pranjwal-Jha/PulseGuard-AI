<<<<<<< HEAD
import logging
import uuid
import time
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.config import Settings
import openai
=======
import os
# Disable ChromaDB anonymized telemetry globally before import
os.environ["ANONYMIZED_TELEMETRY"] = "False"

import chromadb
from chromadb.config import Settings as ChromaSettings
from typing import List, Dict, Any, Optional, Tuple
import json
import asyncio
from decimal import Decimal
>>>>>>> 4331da77534fad0b6a0db7810778b9099ae8751c
from backend.config import get_settings
from backend.database.redis_cache import cache_response

<<<<<<< HEAD
logger = logging.getLogger(__name__)
=======

logger = get_logger("vector_db")
>>>>>>> 4331da77534fad0b6a0db7810778b9099ae8751c
settings = get_settings()

class VectorDatabase:
    """Production Vector DB using ChromaDB and OpenAI Embeddings with Redis Cache."""
    
<<<<<<< HEAD
    def __init__(self):
        self.client = chromadb.PersistentClient(
            path=settings.vector_db_path,
            settings=Settings(anonymized_telemetry=False)
        )
        self.collection_name = "incident_rcas"
=======
    def __init__(self, persist_directory: str = None, embedding_model: str = None):
        """Initialize ChromaDB client."""
        if persist_directory is None:
            persist_directory = settings.vector_db_path
        
        if embedding_model is None:
            embedding_model = settings.embedding_model
        
        # Create directory if it doesn't exist
        os.makedirs(persist_directory, exist_ok=True)
        
        # Initialize ChromaDB with persistent storage
        chroma_settings = ChromaSettings(
            is_persistent=True,
            persist_directory=persist_directory,
            allow_reset=True,
            anonymized_telemetry=False
        )

>>>>>>> 4331da77534fad0b6a0db7810778b9099ae8751c
        
        # Use OpenAI embeddings directly instead of sentence-transformers
        self.openai_client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
        self.embedding_model = settings.embedding_model
        
        self.collection = self.client.get_or_create_collection(
            name=self.collection_name,
            metadata={"hnsw:space": "cosine"}
        )

    async def get_embedding(self, text: str) -> List[float]:
        """Generate embedding using OpenAI API."""
        try:
            response = await self.openai_client.embeddings.create(
                input=text,
                model=self.embedding_model
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise

    async def add_documents(self, documents: List[Dict[str, Any]]):
        """Add documents to Vector DB."""
        if not documents:
            return

        ids = []
        texts = []
        metadatas = []

        for doc in documents:
            doc_id = str(doc.get("id", uuid.uuid4()))
            ids.append(doc_id)
            texts.append(doc["content"])
            # Filter out complex metadata
            meta = {k: v for k, v in doc.get("metadata", {}).items() if isinstance(v, (str, int, float, bool))}
            metadatas.append(meta)

        embeddings = []
        for text in texts:
            emb = await self.get_embedding(text)
            embeddings.append(emb)

        self.collection.add(
            ids=ids,
            embeddings=embeddings,
            documents=texts,
            metadatas=metadatas
        )
        logger.info(f"Added {len(documents)} documents to vector DB")

    @cache_response(ttl_seconds=300)
    async def search_by_metrics(self, error_type: str, spike_percentage: float, k: int = 5) -> List[Dict[str, Any]]:
        """Search similar incidents based on metrics. Cached in Redis."""
        query_text = f"Error Type: {error_type}. Anomaly Spike: {spike_percentage}%"
        query_embedding = await self.get_embedding(query_text)
        
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=k,
            include=["documents", "metadatas", "distances"]
        )

        matches = []
        if results['ids'] and len(results['ids'][0]) > 0:
            for i in range(len(results['ids'][0])):
                meta = results['metadatas'][0][i] or {}
                dist = results['distances'][0][i] if 'distances' in results else 0
                similarity = max(0.0, 1.0 - dist)
                
                matches.append({
                    "vector_db_id": results['ids'][0][i],
                    "incident_id": meta.get("incident_id", "UNKNOWN"),
                    "title": meta.get("title", "Unknown Incident"),
                    "error_type": meta.get("error_type", "Unknown"),
                    "affected_tenant": meta.get("affected_tenant"),
                    "affected_service": meta.get("affected_service", "Unknown"),
                    "mitigation_metric": meta.get("mitigation_metric", "Unknown action"),
                    "mitigation_effectiveness": meta.get("mitigation_effectiveness", 0.0),
                    "resolution_confidence": meta.get("resolution_confidence", 50.0),
                    "similarity_score": similarity,
                    "content": results['documents'][0][i] if results.get('documents') else ""
                })

        return matches
