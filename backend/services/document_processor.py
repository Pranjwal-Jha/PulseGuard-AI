"""Document processing service for ingestion and embedding."""

from typing import List, Dict, Any, Optional
import re
from backend.services.vector_db import get_vector_db
from backend.utils.logging import get_logger

logger = get_logger("document_processor")


class DocumentProcessor:
    """Handles document ingestion, chunking, and embedding."""
    
    def __init__(self, chunk_size: int = 512, chunk_overlap: int = 50):
        """Initialize processor with chunking parameters."""
        self.vector_db = get_vector_db()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def chunk_by_sentences(self, text: str) -> List[str]:
        """Split text into chunks by sentences."""
        # Split by sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            if len(current_chunk) + len(sentence) < self.chunk_size:
                current_chunk += (" " if current_chunk else "") + sentence
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = sentence
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks
    
    def chunk_by_tokens(self, text: str, chunk_size: int = None) -> List[str]:
        """Split text into fixed-size chunks by approximate token count."""
        if chunk_size is None:
            chunk_size = self.chunk_size
        
        # Simple approximation: 1 word ≈ 1.3 tokens, 1 char ≈ 0.25 tokens
        char_limit = int(chunk_size * 4)
        
        words = text.split()
        chunks = []
        current_chunk = []
        current_length = 0
        
        for word in words:
            word_length = len(word) + 1
            if current_length + word_length < char_limit:
                current_chunk.append(word)
                current_length += word_length
            else:
                if current_chunk:
                    chunks.append(" ".join(current_chunk))
                current_chunk = [word]
                current_length = word_length
        
        if current_chunk:
            chunks.append(" ".join(current_chunk))
        
        return chunks
    
    def chunk_by_paragraphs(self, text: str) -> List[str]:
        """Split text by paragraphs, merging small ones."""
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        
        chunks = []
        current_chunk = ""
        
        for para in paragraphs:
            if len(current_chunk) + len(para) < self.chunk_size:
                current_chunk += ("\n\n" if current_chunk else "") + para
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = para
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks
    
    def chunk_document(self, text: str, strategy: str = "sentences") -> List[str]:
        """Chunk document using specified strategy."""
        if strategy == "sentences":
            return self.chunk_by_sentences(text)
        elif strategy == "tokens":
            return self.chunk_by_tokens(text)
        elif strategy == "paragraphs":
            return self.chunk_by_paragraphs(text)
        else:
            logger.warning(f"Unknown chunking strategy: {strategy}, using sentences")
            return self.chunk_by_sentences(text)
    
    async def process_document(
        self,
        doc_id: str,
        title: str,
        content: str,
        collection_name: str = "documents",
        chunking_strategy: str = "sentences",
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process a document: chunk, embed, and store."""
        try:
            logger.info(f"Processing document {doc_id}: {title}")
            
            # Chunk the document
            chunks = self.chunk_document(content, strategy=chunking_strategy)
            logger.info(f"Document {doc_id} chunked into {len(chunks)} chunks")
            
            # Prepare for vector DB
            chunk_ids = [f"{doc_id}_chunk_{i}" for i in range(len(chunks))]
            chunk_metadatas = [
                {
                    "document_id": doc_id,
                    "document_title": title,
                    "chunk_index": i,
                    "chunk_count": len(chunks),
                    "chunking_strategy": chunking_strategy,
                    **(metadata or {})
                }
                for i in range(len(chunks))
            ]
            
            # Add to vector DB (embeddings are created automatically by ChromaDB)
            await self.vector_db.create_collection(collection_name)
            await self.vector_db.add_documents(
                collection_name=collection_name,
                documents=chunks,
                ids=chunk_ids,
                metadatas=chunk_metadatas
            )
            
            logger.info(f"Document {doc_id} processed and stored in vector DB")
            
            return {
                "doc_id": doc_id,
                "title": title,
                "chunk_count": len(chunks),
                "collection": collection_name,
                "chunking_strategy": chunking_strategy,
                "chunk_ids": chunk_ids
            }
        except Exception as e:
            logger.error(f"Error processing document {doc_id}: {e}")
            raise
    
    async def search_documents(
        self,
        query: str,
        collection_name: str = "documents",
        top_k: int = 5,
        filters: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """Search for documents similar to query."""
        try:
            logger.debug(f"Searching for: {query}")
            
            results = await self.vector_db.search(
                collection_name=collection_name,
                query=query,
                k=top_k,
                where_filter=filters
            )
            
            # Enhance results with document info
            enhanced_results = []
            for result in results:
                enhanced_result = result.copy()
                if enhanced_result.get('metadata'):
                    enhanced_result['document_title'] = enhanced_result['metadata'].get('document_title', 'Unknown')
                    enhanced_result['chunk_index'] = enhanced_result['metadata'].get('chunk_index', 0)
                enhanced_results.append(enhanced_result)
            
            logger.debug(f"Search returned {len(enhanced_results)} results")
            return enhanced_results
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            raise
    
    async def delete_document(
        self,
        doc_id: str,
        collection_name: str = "documents"
    ) -> bool:
        """Delete all chunks of a document."""
        try:
            # Get all chunks for this document
            results = await self.vector_db.search(
                collection_name=collection_name,
                query=f"document_id:{doc_id}",
                k=1000
            )
            
            deleted_count = 0
            for result in results:
                if result['metadata'].get('document_id') == doc_id:
                    await self.vector_db.delete_document(collection_name, result['id'])
                    deleted_count += 1
            
            logger.info(f"Deleted document {doc_id}: {deleted_count} chunks removed")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            raise
    
    async def get_document_chunks(
        self,
        doc_id: str,
        collection_name: str = "documents"
    ) -> List[Dict[str, Any]]:
        """Get all chunks for a document."""
        try:
            # Search with a broad query to get document chunks
            results = await self.vector_db.search(
                collection_name=collection_name,
                query="",  # Empty query
                k=1000
            )
            
            doc_chunks = []
            for result in results:
                if result['metadata'].get('document_id') == doc_id:
                    doc_chunks.append({
                        "chunk_id": result['id'],
                        "content": result['document'],
                        "chunk_index": result['metadata'].get('chunk_index', 0),
                        "metadata": result['metadata']
                    })
            
            # Sort by chunk index
            doc_chunks.sort(key=lambda x: x['chunk_index'])
            
            return doc_chunks
        except Exception as e:
            logger.error(f"Error getting chunks for document {doc_id}: {e}")
            raise


# Global document processor instance
_processor: Optional[DocumentProcessor] = None


def get_document_processor() -> DocumentProcessor:
    """Get or create global document processor instance."""
    global _processor
    if _processor is None:
        _processor = DocumentProcessor()
    return _processor
