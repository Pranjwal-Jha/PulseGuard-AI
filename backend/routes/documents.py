"""Document and RCA management API routes."""

import uuid
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Depends, status

from backend.models.schemas import (
    IncidentRCACreate, IncidentRCAResponse, DocumentCreate, DocumentResponse, DocumentSearchRequest
)
from backend.services.vector_db import VectorDatabase
from backend.services.document_processor import get_document_processor
from backend.utils.logging import get_logger

logger = get_logger("documents")
router = APIRouter(prefix="/api/v1/documents", tags=["Documents"])


async def get_vector_db() -> VectorDatabase:
    """Get vector database instance."""
    return VectorDatabase()


@router.post("/rcas", response_model=IncidentRCAResponse)
async def create_rca(
    rca: IncidentRCACreate,
    vector_db: VectorDatabase = Depends(get_vector_db)
) -> IncidentRCAResponse:
    """
    Create a new Root Cause Analysis document.
    
    This endpoint stores historical incident RCA data in both the relational
    database and the vector DB for semantic search matching.
    
    Args:
        rca: IncidentRCACreate with incident details
        vector_db: Vector database instance
    
    Returns:
        IncidentRCAResponse with created RCA ID
    """
    try:
        rca_id = str(uuid.uuid4())
        
        # In production, would store in PostgreSQL first
        logger.info(f"Creating RCA: {rca.incident_id} - {rca.title}")
        
        # Upsert to vector DB for semantic search
        vector_db_id = await vector_db.upsert_rca(
            incident_id=rca.incident_id,
            title=rca.title,
            summary_text=rca.summary,
            metadata={
                "error_type": rca.error_type,
                "affected_tenant": rca.affected_tenant,
                "affected_service": rca.affected_service,
                "severity": rca.severity,
                "mitigation_metric": rca.mitigation_metric,
                "mitigation_effectiveness": float(rca.mitigation_effectiveness),
                "resolution_confidence": float(rca.resolution_confidence),
                "tags": rca.metadata.get("tags", []) if rca.metadata else []
            }
        )
        
        logger.info(f"RCA stored in vector DB: {vector_db_id}")
        
        return IncidentRCAResponse(
            id=rca_id,
            incident_id=rca.incident_id,
            title=rca.title,
            root_cause=rca.root_cause,
            mitigation_metric=rca.mitigation_metric,
            mitigation_effectiveness=float(rca.mitigation_effectiveness),
            resolution_confidence=float(rca.resolution_confidence),
            error_type=rca.error_type,
            affected_service=rca.affected_service,
            severity=rca.severity,
            documented_at=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error creating RCA: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create RCA: {str(e)}"
        )


@router.get("/rcas/search")
async def search_rcas(
    query: str,
    error_type: Optional[str] = None,
    tenant: Optional[str] = None,
    top_k: int = 5,
    vector_db: VectorDatabase = Depends(get_vector_db)
) -> dict:
    """
    Search for RCA documents using semantic similarity.
    
    Args:
        query: Search query
        error_type: Optional error type filter
        tenant: Optional tenant filter
        top_k: Number of top results
        vector_db: Vector database instance
    
    Returns:
        List of matching RCA documents with similarity scores
    """
    try:
        logger.info(f"Searching RCAs: '{query}' (error_type={error_type}, tenant={tenant})")
        
        results = await vector_db.search_rcas(
            query=query,
            k=top_k,
            error_type_filter=error_type,
            tenant_filter=tenant,
            min_confidence=0.2
        )
        
        logger.info(f"Found {len(results)} RCA matches")
        
        return {
            "query": query,
            "filters": {
                "error_type": error_type,
                "tenant": tenant
            },
            "results": results,
            "count": len(results)
        }
        
    except Exception as e:
        logger.error(f"Error searching RCAs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search RCAs: {str(e)}"
        )


@router.get("/rcas/{incident_id}")
async def get_rca(
    incident_id: str,
    vector_db: VectorDatabase = Depends(get_vector_db)
) -> dict:
    """
    Get a specific RCA by incident ID.
    
    Args:
        incident_id: Incident identifier
        vector_db: Vector database instance
    
    Returns:
        RCA document details
    """
    try:
        logger.info(f"Getting RCA: {incident_id}")
        
        # Search for the specific incident
        results = await vector_db.search_rcas(
            query=f"incident_id: {incident_id}",
            k=1
        )
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"RCA not found: {incident_id}"
            )
        
        return {
            "incident_id": incident_id,
            "rca": results[0]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting RCA: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get RCA: {str(e)}"
        )


@router.post("/rcas/import-bulk")
async def import_bulk_rcas(
    rcas: List[IncidentRCACreate],
    vector_db: VectorDatabase = Depends(get_vector_db)
) -> dict:
    """
    Import multiple RCA documents at once.
    
    Args:
        rcas: List of RCAs to import
        vector_db: Vector database instance
    
    Returns:
        Import summary with success count
    """
    try:
        logger.info(f"Importing {len(rcas)} RCA documents")
        
        imported_count = 0
        failed_count = 0
        errors = []
        
        for rca in rcas:
            try:
                await vector_db.upsert_rca(
                    incident_id=rca.incident_id,
                    title=rca.title,
                    summary_text=rca.summary,
                    metadata={
                        "error_type": rca.error_type,
                        "affected_tenant": rca.affected_tenant,
                        "affected_service": rca.affected_service,
                        "severity": rca.severity,
                        "mitigation_metric": rca.mitigation_metric,
                        "mitigation_effectiveness": float(rca.mitigation_effectiveness),
                        "resolution_confidence": float(rca.resolution_confidence),
                    }
                )
                imported_count += 1
            except Exception as e:
                failed_count += 1
                errors.append(f"{rca.incident_id}: {str(e)}")
        
        logger.info(f"Import complete: {imported_count} succeeded, {failed_count} failed")
        
        return {
            "total": len(rcas),
            "imported": imported_count,
            "failed": failed_count,
            "errors": errors if failed_count > 0 else []
        }
        
    except Exception as e:
        logger.error(f"Error in bulk import: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to import RCAs: {str(e)}"
        )


@router.post("/documents")
async def upload_document(
    doc: DocumentCreate,
    vector_db: VectorDatabase = Depends(get_vector_db)
) -> DocumentResponse:
    """
    Upload and process a general document.
    
    Args:
        doc: DocumentCreate with document content
        vector_db: Vector database instance
    
    Returns:
        DocumentResponse with document ID
    """
    try:
        doc_id = str(uuid.uuid4())
        
        logger.info(f"Uploading document: {doc.title}")
        
        # Store in vector DB
        vector_ids = await vector_db.add_documents(
            collection_name="general_documents",
            documents=[doc.content],
            ids=[doc_id],
            metadatas=[{
                "title": doc.title,
                "document_id": doc_id,
                **(doc.metadata or {})
            }]
        )
        
        logger.info(f"Document stored: {doc_id}")
        
        return DocumentResponse(
            id=doc_id,
            title=doc.title,
            content=doc.content[:500],  # Truncate for response
            chunks_count=len(doc.content.split()),
            uploaded_at=datetime.utcnow(),
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            status="processed",
            metadata=doc.metadata
        )
        
    except Exception as e:
        logger.error(f"Error uploading document: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload document: {str(e)}"
        )


@router.get("/stats")
async def get_document_stats(
    vector_db: VectorDatabase = Depends(get_vector_db)
) -> dict:
    """Get statistics about stored documents."""
    try:
        rcas_stats = await vector_db.get_collection_stats("historical_rcas")
        docs_stats = await vector_db.get_collection_stats("general_documents")
        
        return {
            "rcas": {
                "count": rcas_stats.get("document_count", 0),
                "collection": "historical_rcas"
            },
            "documents": {
                "count": docs_stats.get("document_count", 0),
                "collection": "general_documents"
            },
            "total": (rcas_stats.get("document_count", 0) + 
                     docs_stats.get("document_count", 0))
        }
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stats: {str(e)}"
        )


@router.get("", response_model=dict)
async def get_collections():
    """List all document collections."""
    try:
        vector_db = await get_vector_db()
        collections = vector_db.list_collections()
        
        stats = []
        for collection_name in collections:
            try:
                collection_stats = await vector_db.get_collection_stats(collection_name)
                stats.append(collection_stats)
            except:
                pass
        
        return {
            "collections": stats,
            "total_collections": len(stats)
        }
    except Exception as e:
        logger.error(f"Error listing collections: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{doc_id}", response_model=dict)
async def get_document(doc_id: str):
    """Get document chunks."""
    try:
        processor = get_document_processor()
        chunks = await processor.get_document_chunks(doc_id)
        
        if not chunks:
            raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
        
        return {
            "doc_id": doc_id,
            "chunk_count": len(chunks),
            "chunks": chunks
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting document {doc_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{doc_id}", response_model=dict)
async def delete_document(doc_id: str):
    """Delete a document."""
    try:
        processor = get_document_processor()
        success = await processor.delete_document(doc_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
        
        return {
            "doc_id": doc_id,
            "status": "deleted"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting document {doc_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search", response_model=dict)
async def search_documents(request: DocumentSearchRequest):
    """Search for documents by semantic similarity."""
    try:
        processor = get_document_processor()
        results = await processor.search_documents(
            query=request.query,
            top_k=request.top_k
        )
        
        return {
            "query": request.query,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        logger.error(f"Error searching documents: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=dict)
async def document_system_health():
    """Get health status of document system."""
    try:
        vector_db = await get_vector_db()
        collections = vector_db.list_collections()
        
        return {
            "status": "healthy",
            "collections": len(collections),
            "embedding_model": vector_db.embedding_model,
            "collections_list": collections
        }
    except Exception as e:
        logger.error(f"Error checking document system health: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }
