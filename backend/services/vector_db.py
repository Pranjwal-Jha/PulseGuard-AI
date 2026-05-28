import os
# Disable ChromaDB anonymized telemetry globally before import
os.environ["ANONYMIZED_TELEMETRY"] = "False"

import chromadb
from chromadb.config import Settings as ChromaSettings
from typing import List, Dict, Any, Optional, Tuple
import json
import asyncio
from decimal import Decimal
from backend.config import get_settings
from backend.utils.logging import get_logger


logger = get_logger("vector_db")
settings = get_settings()


class VectorDatabase:
    """Wrapper around ChromaDB for vector operations."""
    
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

        
        self.client = chromadb.Client(chroma_settings)
        self.embedding_model = embedding_model
        self.collections: Dict[str, Any] = {}
        
        logger.info(f"ChromaDB initialized with persistence at {persist_directory}")
    
    async def create_collection(self, collection_name: str, metadata: Optional[Dict] = None) -> str:
        """Create or get a collection for storing embeddings."""
        try:
            # Try to get existing collection
            if collection_name in self.collections:
                return collection_name
            
            collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata=metadata or {"description": f"Collection {collection_name}"}
            )
            self.collections[collection_name] = collection
            logger.info(f"Collection created/retrieved: {collection_name}")
            return collection_name
        except Exception as e:
            logger.error(f"Error creating collection {collection_name}: {e}")
            raise
    
    async def add_documents(
        self,
        collection_name: str,
        documents: List[str],
        ids: List[str],
        metadatas: Optional[List[Dict]] = None
    ) -> List[str]:
        """Add documents to collection with embeddings."""
        try:
            await self.create_collection(collection_name)
            collection = self.collections[collection_name]
            
            # ChromaDB handles embedding automatically
            if metadatas is None:
                metadatas = [{"document_id": id_} for id_ in ids]
            
            collection.add(
                documents=documents,
                ids=ids,
                metadatas=metadatas
            )
            
            logger.info(f"Added {len(documents)} documents to {collection_name}")
            return ids
        except Exception as e:
            logger.error(f"Error adding documents to {collection_name}: {e}")
            raise
    
    async def search(
        self,
        collection_name: str,
        query: str,
        k: int = 5,
        where_filter: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """Search for similar documents using semantic search."""
        try:
            await self.create_collection(collection_name)
            collection = self.collections[collection_name]
            
            # Query the collection
            results = collection.query(
                query_texts=[query],
                n_results=k,
                where=where_filter
            )
            
            # Format results
            formatted_results = []
            if results and results['ids'] and len(results['ids']) > 0:
                for i, doc_id in enumerate(results['ids'][0]):
                    formatted_results.append({
                        "id": doc_id,
                        "document": results['documents'][0][i] if results['documents'] else None,
                        "metadata": results['metadatas'][0][i] if results['metadatas'] else {},
                        "distance": results['distances'][0][i] if results['distances'] else 0,
                        "relevance_score": 1 - (results['distances'][0][i] / 2) if results['distances'] else 0  # Convert distance to similarity
                    })
            
            logger.debug(f"Search in {collection_name} returned {len(formatted_results)} results")
            return formatted_results
        except Exception as e:
            logger.error(f"Error searching in {collection_name}: {e}")
            raise
    
    async def delete_document(self, collection_name: str, doc_id: str) -> bool:
        """Delete a document from collection."""
        try:
            if collection_name not in self.collections:
                return False
            
            collection = self.collections[collection_name]
            collection.delete(ids=[doc_id])
            logger.info(f"Deleted document {doc_id} from {collection_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            raise
    
    async def update_document(
        self,
        collection_name: str,
        doc_id: str,
        document: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        """Update a document in collection."""
        try:
            await self.create_collection(collection_name)
            collection = self.collections[collection_name]
            
            if metadata is None:
                metadata = {"document_id": doc_id}
            
            collection.upsert(
                documents=[document],
                ids=[doc_id],
                metadatas=[metadata]
            )
            logger.info(f"Updated document {doc_id} in {collection_name}")
            return True
        except Exception as e:
            logger.error(f"Error updating document {doc_id}: {e}")
            raise
    
    async def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get statistics about a collection."""
        try:
            if collection_name not in self.collections:
                await self.create_collection(collection_name)
            
            collection = self.collections[collection_name]
            count = collection.count()
            
            return {
                "collection_name": collection_name,
                "document_count": count,
                "embedding_model": self.embedding_model,
                "metadata": collection.metadata if hasattr(collection, 'metadata') else {}
            }
        except Exception as e:
            logger.error(f"Error getting stats for {collection_name}: {e}")
            raise
    
    async def delete_collection(self, collection_name: str) -> bool:
        """Delete entire collection."""
        try:
            if collection_name in self.collections:
                self.client.delete_collection(name=collection_name)
                del self.collections[collection_name]
                logger.info(f"Deleted collection {collection_name}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting collection {collection_name}: {e}")
            raise
    
    def list_collections(self) -> List[str]:
        """List all collections."""
        try:
            collections = self.client.list_collections()
            return [c.name for c in collections]
        except Exception as e:
            logger.error(f"Error listing collections: {e}")
            return []    
    async def upsert_rca(
        self,
        incident_id: str,
        title: str,
        summary_text: str,
        metadata: Dict[str, Any]
    ) -> str:
        """
        Upsert a Root Cause Analysis record into the vector DB.
        
        Args:
            incident_id: Unique incident identifier
            title: Incident title
            summary_text: Dense, single-line summary for embedding
            metadata: Rich metadata for filtering and context
        
        Returns:
            Vector DB ID for the RCA document
        """
        try:
            await self.create_collection("historical_rcas")
            collection = self.collections["historical_rcas"]
            
            # Use incident_id as the document ID for deduplication
            doc_id = f"rca_{incident_id}"
            
            # Upsert with explicit metadata validation
            collection.upsert(
                documents=[summary_text],
                ids=[doc_id],
                metadatas=[{
                    "incident_id": incident_id,
                    "title": title,
                    "error_type": metadata.get("error_type", "unknown"),
                    "affected_tenant": metadata.get("affected_tenant", "unknown"),
                    "affected_service": metadata.get("affected_service", "unknown"),
                    "severity": metadata.get("severity", "medium"),
                    "mitigation_metric": metadata.get("mitigation_metric", ""),
                    "mitigation_effectiveness": str(metadata.get("mitigation_effectiveness", 0)),
                    "resolution_confidence": str(metadata.get("resolution_confidence", 0)),
                    "tags": json.dumps(metadata.get("tags", [])),
                }]
            )
            
            logger.debug(f"Upserted RCA: {incident_id} (doc_id: {doc_id})")
            return doc_id
            
        except Exception as e:
            logger.error(f"Error upserting RCA {incident_id}: {e}", exc_info=True)
            raise
    
    async def search_rcas(
        self,
        query: str,
        k: int = 5,
        error_type_filter: Optional[str] = None,
        tenant_filter: Optional[str] = None,
        min_confidence: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Search for matching historical RCA records with explicit verification.
        
        Args:
            query: Anomaly context query string
            k: Top-k results to return
            error_type_filter: Optional error type to filter by
            tenant_filter: Optional tenant to filter by
            min_confidence: Minimum confidence score threshold
        
        Returns:
            List of matched RCA records with similarity scores and metadata
        """
        try:
            await self.create_collection("historical_rcas")
            collection = self.collections["historical_rcas"]
            
            # Build metadata filter if specified
            where_filter = None
            if error_type_filter or tenant_filter:
                where_filter = {}
                if error_type_filter:
                    where_filter["error_type"] = error_type_filter
                if tenant_filter:
                    where_filter["affected_tenant"] = tenant_filter
            
            # Execute semantic search
            results = collection.query(
                query_texts=[query],
                n_results=k,
                where=where_filter
            )
            
            # Explicit verification and formatting of results
            formatted_results = []
            if results and results.get('ids') and len(results['ids']) > 0:
                for i, doc_id in enumerate(results['ids'][0]):
                    # Extract distance and convert to similarity score (0-1)
                    distance = results.get('distances', [[]])[0][i] if results.get('distances') else 0
                    # Normalize Euclidean distance to similarity (smaller distance = higher similarity)
                    similarity_score = max(0, 1 - (distance / 2))  # Chroma uses L2 distance
                    
                    # Verify confidence meets threshold
                    if similarity_score < min_confidence:
                        logger.debug(f"Skipping match {doc_id} - similarity {similarity_score} below threshold {min_confidence}")
                        continue
                    
                    # Extract and parse metadata
                    metadata = results.get('metadatas', [[]])[0][i] if results.get('metadatas') else {}
                    
                    # Parse JSON fields if present
                    if isinstance(metadata.get('tags'), str):
                        metadata['tags'] = json.loads(metadata['tags'])
                    
                    formatted_result = {
                        "vector_db_id": doc_id,
                        "incident_id": metadata.get("incident_id", "unknown"),
                        "title": metadata.get("title", "unknown"),
                        "summary": results.get('documents', [[]])[0][i] if results.get('documents') else None,
                        "error_type": metadata.get("error_type", "unknown"),
                        "affected_tenant": metadata.get("affected_tenant", "unknown"),
                        "affected_service": metadata.get("affected_service", "unknown"),
                        "severity": metadata.get("severity", "medium"),
                        "mitigation_metric": metadata.get("mitigation_metric", ""),
                        "mitigation_effectiveness": float(metadata.get("mitigation_effectiveness", 0)),
                        "resolution_confidence": float(metadata.get("resolution_confidence", 0)),
                        "similarity_score": round(similarity_score, 4),  # Explicit rounding
                        "distance": round(distance, 4),
                        "tags": metadata.get("tags", []),
                        "metadata": metadata
                    }
                    
                    formatted_results.append(formatted_result)
                    logger.debug(
                        f"Matched RCA: {formatted_result['incident_id']} "
                        f"(similarity: {formatted_result['similarity_score']})"
                    )
            
            logger.info(f"RCA search returned {len(formatted_results)} verified results")
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error searching RCAs: {e}", exc_info=True)
            raise
    
    async def search_by_metrics(
        self,
        error_type: str,
        spike_percentage: float,
        k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Search RCAs based on specific error type and anomaly spike pattern.
        
        Args:
            error_type: Type of error (e.g., 'database_timeout')
            spike_percentage: Percentage spike observed in metrics
            k: Top-k results
        
        Returns:
            Matched RCA records most relevant to the current anomaly
        """
        try:
            # Construct pattern-based query
            query = (
                f"Error type: {error_type.replace('_', ' ')}. "
                f"Anomaly spike: {spike_percentage}% increase over baseline. "
                f"Detection pattern: sudden metric degradation."
            )
            
            # Search with error type filter
            results = await self.search_rcas(
                query=query,
                k=k,
                error_type_filter=error_type,
                min_confidence=0.3  # Lower threshold for metric-based matching
            )
            
            logger.info(f"Found {len(results)} RCA matches for error type '{error_type}'")
            return results
            
        except Exception as e:
            logger.error(f"Error searching by metrics: {e}", exc_info=True)
            raise
    
    async def verify_similarity(
        self,
        query: str,
        candidate_doc_id: str,
        collection_name: str = "historical_rcas"
    ) -> float:
        """
        Verify similarity score for a specific candidate document.
        Explicit verification function for debugging and validation.
        
        Args:
            query: Query string
            candidate_doc_id: Document ID to verify similarity for
            collection_name: Collection to search in
        
        Returns:
            Computed similarity score (0-1)
        """
        try:
            results = await self.search(
                collection_name=collection_name,
                query=query,
                k=100  # Get many results for verification
            )
            
            # Find the specific candidate in results
            for result in results:
                if result["id"] == candidate_doc_id:
                    return result["relevance_score"]
            
            logger.warning(f"Candidate {candidate_doc_id} not found in top 100 results")
            return 0.0
            
        except Exception as e:
            logger.error(f"Error verifying similarity: {e}", exc_info=True)
            raise    
    async def reset(self):
        """Reset entire database (use with caution)."""
        try:
            self.client.reset()
            self.collections.clear()
            logger.warning("Vector database reset")
        except Exception as e:
            logger.error(f"Error resetting database: {e}")
            raise


# Global vector database instance
_vector_db: Optional[VectorDatabase] = None


def get_vector_db() -> VectorDatabase:
    """Get or create global vector database instance."""
    global _vector_db
    if _vector_db is None:
        _vector_db = VectorDatabase()
    return _vector_db
