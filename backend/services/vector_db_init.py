"""
Vector Database initialization with production-grade historical RCA data.
This module provides realistic historical incident data for the RAG system.
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
import json


# ============================================================================
# REALISTIC HISTORICAL RCA DATA FOR INCIDENT MATCHING
# ============================================================================

HISTORICAL_RCAS: List[Dict[str, Any]] = [
    {
        "incident_id": "INC-2025-001",
        "title": "Database Connection Pool Exhaustion - Payment Service",
        "summary": "Payment service experiencing 450% spike in DB timeouts due to connection pool exhaustion from N+1 queries in recent deployment. Throttling tenant traffic to 30% resolved issue within 8 minutes.",
        "root_cause": "A new feature deployment introduced N+1 query patterns in the payment processing loop. Each transaction was spawning 50+ additional queries instead of batch operations. The database connection pool (100 connections) was exhausted within 2 minutes under production load.",
        "impact": "Payment processing latency increased from 200ms to 8500ms. Transaction success rate dropped to 45%. Affected 12,000 transactions/minute across all payment processing tenants.",
        "resolution_action": "Implemented query batching, applied 30% tenant throttling via rate limiter, deployed connection pool increase from 100->200. Reverted problematic N+1 queries in deployment.",
        "resolution_confidence": 95.0,
        "mitigation_metric": "Throttle tenant traffic to 30% while deploying fix",
        "mitigation_effectiveness": 92.0,
        "error_type": "database_timeout",
        "affected_tenant": "payment_service_prod",
        "affected_service": "payments",
        "severity": "critical",
        "incident_date": datetime.now() - timedelta(days=45),
        "detection_time_seconds": 120,
        "recovery_time_seconds": 480,
        "metadata": {
            "error_spike": "450%",
            "baseline_latency_ms": 200,
            "peak_latency_ms": 8500,
            "affected_transactions": 12000,
            "success_rate_during": 0.45,
            "tags": ["database", "connection_pool", "n+1_queries", "deployment"]
        }
    },
    {
        "incident_id": "INC-2025-002",
        "title": "Kafka Consumer Lag Cascade - Event Processing",
        "summary": "Event processing pipeline experiencing 320% increase in Kafka consumer lag. Processing 8-second delay per message due to downstream service slowdown. Applied 45% traffic throttle to prevent backlog explosion.",
        "root_cause": "Downstream logging service deployed with inefficient regex pattern matching causing 400ms per log operation. This cascaded back to Kafka consumers which couldn't commit offsets fast enough. Consumer lag grew exponentially from 100 to 15,000 messages within 5 minutes.",
        "impact": "Event processing SLA breached by 2 hours. Real-time fraud detection delayed by 8 seconds. Customer notifications delayed. Revenue impact: ~$45,000.",
        "resolution_action": "Deployed fix to logging regex pattern, applied 45% traffic throttle to stabilize pipeline, increased Kafka consumer thread count from 4->12. Reprocessed 50k backlogged messages.",
        "resolution_confidence": 88.0,
        "mitigation_metric": "Throttle event ingestion to 45% capacity",
        "mitigation_effectiveness": 85.0,
        "error_type": "kafka_consumer_lag",
        "affected_tenant": "analytics_events",
        "affected_service": "event_processing",
        "severity": "critical",
        "incident_date": datetime.now() - timedelta(days=38),
        "detection_time_seconds": 90,
        "recovery_time_seconds": 620,
        "metadata": {
            "lag_spike": "320%",
            "baseline_lag": 100,
            "peak_lag": 15000,
            "processing_delay_seconds": 8,
            "tags": ["kafka", "consumer_lag", "downstream_dependency", "cascading_failure"]
        }
    },
    {
        "incident_id": "INC-2025-003",
        "title": "Memory Leak in Cache Layer - Session Store",
        "summary": "Session storage service experiencing 280% increase in memory usage and GC pauses up to 3.2 seconds. Throttling session creation requests to 20% prevented OOM crash. Memory leak in session eviction policy.",
        "root_cause": "Session eviction policy was retaining references to expired sessions instead of garbage collecting them. LRU cache implementation had a bug where last_access timestamp wasn't being updated correctly, preventing proper eviction. Memory grew from 2GB to 5.6GB in 4 hours.",
        "impact": "P99 latency degraded to 5.2 seconds from 120ms. Session lookup failures spiked to 8%. 50k users experiencing timeouts.",
        "resolution_action": "Applied 20% session creation throttle, deployed fix to eviction policy, restarted service with increased memory allocation. Purged 2 million orphaned session entries.",
        "resolution_confidence": 91.0,
        "mitigation_metric": "Throttle new sessions to 20% during remediation",
        "mitigation_effectiveness": 88.0,
        "error_type": "memory_leak",
        "affected_tenant": "session_store_prod",
        "affected_service": "sessions",
        "severity": "critical",
        "incident_date": datetime.now() - timedelta(days=32),
        "detection_time_seconds": 240,
        "recovery_time_seconds": 900,
        "metadata": {
            "memory_spike": "280%",
            "baseline_memory": "2GB",
            "peak_memory": "5.6GB",
            "gc_pause_seconds": 3.2,
            "session_failures": "8%",
            "tags": ["memory_leak", "cache", "eviction_policy", "garbage_collection"]
        }
    },
    {
        "incident_id": "INC-2025-004",
        "title": "CPU Saturation from Unoptimized Regex - Search Service",
        "summary": "Search service hitting 95% CPU utilization from unoptimized regex patterns in query parsing. Applied 50% search request throttle to prevent cascade failure.",
        "root_cause": "Search query parser was using catastrophic backtracking regex patterns. Complex queries were taking 2-5 seconds of CPU per query. With 5k QPS, CPU became bottleneck immediately. Regex: `(a+)+b` pattern causing exponential backtracking.",
        "impact": "Search response time degraded to 8-12 seconds. Search request success rate dropped to 35%. 200k+ search requests queued.",
        "resolution_action": "Deployed optimized regex patterns and query parser, applied 50% throttle to stabilize CPU, reprocessed queued searches. Added query complexity validation.",
        "resolution_confidence": 93.0,
        "mitigation_metric": "Throttle search requests to 50%",
        "mitigation_effectiveness": 90.0,
        "error_type": "high_cpu_utilization",
        "affected_tenant": "search_prod",
        "affected_service": "search",
        "severity": "critical",
        "incident_date": datetime.now() - timedelta(days=25),
        "detection_time_seconds": 60,
        "recovery_time_seconds": 480,
        "metadata": {
            "cpu_utilization": "95%",
            "baseline_cpu": "25%",
            "query_latency_seconds": 10,
            "success_rate": "35%",
            "queued_requests": 200000,
            "tags": ["cpu_saturation", "regex", "catastrophic_backtracking", "query_parsing"]
        }
    },
    {
        "incident_id": "INC-2025-005",
        "title": "Third-Party API Rate Limit Cascade - Email Service",
        "summary": "Email sending service hit external API rate limits (SendGrid/SES) causing queue backlog of 2.5M emails. Applied 60% email send throttle to stay within API limits.",
        "root_cause": "A new feature was retrying failed email sends immediately without exponential backoff. Single email failures were being retried 10x, causing 10x amplification of API calls. Quota: 250k/hour but seeing 1.8M attempts/hour.",
        "impact": "Email delivery SLA breached. Transactional emails delayed by 2+ hours. Password resets, confirmations failing. User support tickets spiked.",
        "resolution_action": "Implemented exponential backoff (1s, 2s, 4s, 8s, 16s), applied 60% send throttle, increased rate limit quota with provider, processed backlog over 4 hours.",
        "resolution_confidence": 89.0,
        "mitigation_metric": "Throttle email sends to 60% of normal rate",
        "mitigation_effectiveness": 86.0,
        "error_type": "external_api_rate_limit",
        "affected_tenant": "communications",
        "affected_service": "email",
        "severity": "high",
        "incident_date": datetime.now() - timedelta(days=20),
        "detection_time_seconds": 150,
        "recovery_time_seconds": 1800,
        "metadata": {
            "api_quota": "250k/hour",
            "attempted_calls": "1.8M/hour",
            "backlog_emails": 2500000,
            "retry_multiplier": 10,
            "tags": ["external_api", "rate_limit", "retry_storm", "sendgrid"]
        }
    },
    {
        "incident_id": "INC-2025-006",
        "title": "Deadlock in Distributed Lock Service - Checkout Flow",
        "summary": "Checkout service experiencing 410% increase in lock timeouts due to deadlock in distributed lock orchestration. Applied 35% checkout throttle until deadlock cycle identified.",
        "root_cause": "Circular lock dependency: Thread A holds lock_order, waits for lock_inventory; Thread B holds lock_inventory, waits for lock_order. Lock timeout set to 1 second, causing cascading timeout errors. Affected orders weren't being rolled back properly.",
        "impact": "Checkout success rate dropped to 28%. Lost orders: 45k. Revenue impact: $250k. Customer complaints: 8k+.",
        "resolution_action": "Deployed ordered lock acquisition (lock hierarchy: order->payment->inventory), applied 35% checkout throttle, processed stuck orders manually, added deadlock detection monitoring.",
        "resolution_confidence": 96.0,
        "mitigation_metric": "Throttle checkout requests to 35%",
        "mitigation_effectiveness": 94.0,
        "error_type": "distributed_lock_deadlock",
        "affected_tenant": "commerce",
        "affected_service": "checkout",
        "severity": "critical",
        "incident_date": datetime.now() - timedelta(days=15),
        "detection_time_seconds": 45,
        "recovery_time_seconds": 600,
        "metadata": {
            "lock_timeouts": "410%",
            "success_rate": "28%",
            "failed_orders": 45000,
            "revenue_impact": "$250k",
            "tags": ["deadlock", "distributed_locks", "checkout", "circular_dependency"]
        }
    },
    {
        "incident_id": "INC-2025-007",
        "title": "Vector DB Query Timeout Spiral - Recommendation Engine",
        "summary": "Recommendation engine experiencing 2.8s query latencies (vs 200ms baseline) in vector DB. Applied 40% request throttle to prevent timeout cascade.",
        "root_cause": "Vector similarity search was being performed on 500M embeddings without proper indexing. Query filter was full-table scan instead of using metadata indices. IVF index rebuild took 8 hours, not run overnight.",
        "impact": "Recommendation loading time increased to 4 seconds. Recommendation CTR dropped 35%. Session abandonment increased 22%.",
        "resolution_action": "Deployed IVF index optimization, applied 40% request throttle, filtered by tenant before vector search, rebuilt indices with HNSW algorithm.",
        "resolution_confidence": 87.0,
        "mitigation_metric": "Throttle vector searches to 40%",
        "mitigation_effectiveness": 83.0,
        "error_type": "vector_db_timeout",
        "affected_tenant": "recommendations_prod",
        "affected_service": "recommendations",
        "severity": "high",
        "incident_date": datetime.now() - timedelta(days=10),
        "detection_time_seconds": 120,
        "recovery_time_seconds": 540,
        "metadata": {
            "query_latency": "2.8s",
            "baseline_latency": "200ms",
            "embeddings_count": "500M",
            "index_type": "HNSW",
            "tags": ["vector_db", "similarity_search", "indexing", "recommendation_engine"]
        }
    },
    {
        "incident_id": "INC-2025-008",
        "title": "Recursive Stored Procedure Loop - Analytics Pipeline",
        "summary": "Analytics pipeline experiencing 600% increase in query time. Stored procedure hit recursive loop limit. Applied 70% analytics query throttle.",
        "root_cause": "Stored procedure was incorrectly calling itself recursively without proper base case. Each minute running procedure, trying to process 100k rows recursively instead of iteratively. SQL stack depth limit hit after 500 iterations.",
        "impact": "Analytics queries timing out at 30 second limit. Dashboard data stale by 2+ hours. Executive reports delayed.",
        "resolution_action": "Rewrote procedure with iterative approach, applied 70% throttle, rebuilt query execution plan, added query complexity limits.",
        "resolution_confidence": 94.0,
        "mitigation_metric": "Throttle analytics to 70% throughput",
        "mitigation_effectiveness": 91.0,
        "error_type": "database_recursion_limit",
        "affected_tenant": "analytics",
        "affected_service": "analytics",
        "severity": "high",
        "incident_date": datetime.now() - timedelta(days=8),
        "detection_time_seconds": 180,
        "recovery_time_seconds": 420,
        "metadata": {
            "query_time_increase": "600%",
            "recursion_depth": 500,
            "rows_affected": "100k",
            "query_timeout": "30s",
            "tags": ["database", "stored_procedure", "recursion", "analytics"]
        }
    },
    {
        "incident_id": "INC-2025-009",
        "title": "SSL Certificate Expiration Cascade - API Gateway",
        "summary": "API Gateway SSL certificate expired causing 100% request failures. Detected after 15 minutes. Applied emergency throttle to 10% for manual failover.",
        "root_cause": "SSL certificate renewal automation failed silently. Certificate expired at midnight. Monitoring alert configured but routed to deprecated email. No human verification before deployment.",
        "impact": "All API requests returning 503 SSL errors. 500k requests dropped. Integrations timing out. Customer systems experiencing failures.",
        "resolution_action": "Emergency certificate installation, redirected traffic to backup gateway, applied 10% throttle during cutover, updated monitoring to on-call channels.",
        "resolution_confidence": 100.0,
        "mitigation_metric": "Manual failover with 10% throttle during transition",
        "mitigation_effectiveness": 99.0,
        "error_type": "ssl_certificate_expired",
        "affected_tenant": "api_gateway",
        "affected_service": "api_gateway",
        "severity": "critical",
        "incident_date": datetime.now() - timedelta(days=5),
        "detection_time_seconds": 15,
        "recovery_time_seconds": 300,
        "metadata": {
            "detection_delay": "15 minutes",
            "failed_requests": "500k",
            "downtime": "5 minutes",
            "tags": ["ssl", "certificate", "api_gateway", "automation_failure"]
        }
    },
    {
        "incident_id": "INC-2025-010",
        "title": "Object Storage Bucket Access Policy Misconfiguration",
        "summary": "S3/GCS bucket access policy accidentally set to 'public'. Causing 250% increase in outbound bandwidth and exposure of private data. Applied 90% read throttle.",
        "root_cause": "Infrastructure-as-code change accidentally set bucket policy to 'public-read' instead of 'authenticated-users'. Change was auto-deployed by CI/CD without approval. Affected sensitive customer data.",
        "impact": "Unauthorized access to 12GB of private data. Compliance violation. $500k potential liability. Customer breach notification required.",
        "resolution_action": "Reverted bucket policy to private, applied 90% read throttle, audited access logs, notified affected customers, engaged legal/compliance.",
        "resolution_confidence": 98.0,
        "mitigation_metric": "Throttle object storage reads to 10%",
        "mitigation_effectiveness": 97.0,
        "error_type": "access_control_misconfiguration",
        "affected_tenant": "all_tenants",
        "affected_service": "object_storage",
        "severity": "critical",
        "incident_date": datetime.now() - timedelta(days=3),
        "detection_time_seconds": 600,
        "recovery_time_seconds": 180,
        "metadata": {
            "exposure_duration": "10 minutes",
            "data_exposed": "12GB",
            "unauthorized_requests": 50000,
            "tags": ["security", "access_control", "s3", "misconfiguration", "compliance"]
        }
    }
]


INCIDENT_INDICATORS: List[Dict[str, Any]] = [
    {"incident_id": "INC-2025-001", "metric": "db_connection_pool_active", "baseline": 60, "anomaly": 100, "spike": 66.67},
    {"incident_id": "INC-2025-001", "metric": "db_query_time_p99_ms", "baseline": 200, "anomaly": 8500, "spike": 4150},
    {"incident_id": "INC-2025-001", "metric": "transaction_latency_ms", "baseline": 200, "anomaly": 8500, "spike": 4150},
    
    {"incident_id": "INC-2025-002", "metric": "kafka_consumer_lag", "baseline": 100, "anomaly": 15000, "spike": 14900},
    {"incident_id": "INC-2025-002", "metric": "message_processing_latency_ms", "baseline": 50, "anomaly": 8000, "spike": 15900},
    
    {"incident_id": "INC-2025-003", "metric": "heap_memory_gb", "baseline": 2.0, "anomaly": 5.6, "spike": 180},
    {"incident_id": "INC-2025-003", "metric": "gc_pause_time_ms", "baseline": 50, "anomaly": 3200, "spike": 6300},
    
    {"incident_id": "INC-2025-004", "metric": "cpu_utilization_percent", "baseline": 25, "anomaly": 95, "spike": 280},
    {"incident_id": "INC-2025-004", "metric": "query_latency_p99_ms", "baseline": 100, "anomaly": 10000, "spike": 9900},
    
    {"incident_id": "INC-2025-005", "metric": "email_queue_size", "baseline": 1000, "anomaly": 2500000, "spike": 249900},
    {"incident_id": "INC-2025-005", "metric": "sendgrid_api_calls_per_hour", "baseline": 250000, "anomaly": 1800000, "spike": 620},
    
    {"incident_id": "INC-2025-006", "metric": "lock_timeout_errors", "baseline": 10, "anomaly": 4100, "spike": 40900},
    {"incident_id": "INC-2025-006", "metric": "checkout_success_rate_percent", "baseline": 98, "anomaly": 28, "spike": -71.43},
    
    {"incident_id": "INC-2025-007", "metric": "vector_query_latency_ms", "baseline": 200, "anomaly": 2800, "spike": 1300},
    {"incident_id": "INC-2025-007", "metric": "recommendation_requests_per_sec", "baseline": 5000, "anomaly": 1000, "spike": -80},
    
    {"incident_id": "INC-2025-008", "metric": "analytics_query_time_ms", "baseline": 1000, "anomaly": 7000, "spike": 600},
    {"incident_id": "INC-2025-008", "metric": "sql_recursion_depth", "baseline": 1, "anomaly": 500, "spike": 49900},
    
    {"incident_id": "INC-2025-009", "metric": "ssl_error_rate_percent", "baseline": 0, "anomaly": 100, "spike": float('inf')},
    {"incident_id": "INC-2025-009", "metric": "api_requests_failed", "baseline": 0, "anomaly": 500000, "spike": float('inf')},
    
    {"incident_id": "INC-2025-010", "metric": "object_storage_outbound_bandwidth_gb", "baseline": 10, "anomaly": 35, "spike": 250},
    {"incident_id": "INC-2025-010", "metric": "unauthorized_access_attempts", "baseline": 0, "anomaly": 50000, "spike": float('inf')},
]


async def populate_vector_db(vector_db: 'VectorDatabase') -> None:
    """
    Populate the vector database with historical RCA data.
    
    Args:
        vector_db: VectorDatabase instance to populate
    
    Raises:
        Exception: If population fails
    """
    from backend.utils.logging import get_logger
    
    logger = get_logger("vector_db_init")
    
    try:
        # Create RCA collection
        await vector_db.create_collection(
            "historical_rcas",
            metadata={
                "description": "Historical Root Cause Analysis documents for incident matching",
                "purpose": "RAG context retrieval for AI decision making",
                "data_version": "1.0"
            }
        )
        
        logger.info(f"Populating {len(HISTORICAL_RCAS)} historical RCA records...")
        
        # Add each RCA to vector DB
        for rca in HISTORICAL_RCAS:
            # Create dense summary for embedding
            dense_summary = (
                f"{rca['incident_id']}: {rca['error_type'].replace('_', ' ')} - "
                f"{rca['title']}. "
                f"Severity: {rca['severity']}. "
                f"{rca['summary']} "
                f"Root cause: {rca['root_cause'][:100]}... "
                f"Recommended mitigation: {rca['mitigation_metric']}"
            )
            
            # Upsert to vector DB
            await vector_db.upsert_rca(
                incident_id=rca['incident_id'],
                title=rca['title'],
                summary_text=dense_summary,
                metadata={
                    "error_type": rca['error_type'],
                    "affected_tenant": rca['affected_tenant'],
                    "affected_service": rca['affected_service'],
                    "severity": rca['severity'],
                    "mitigation_metric": rca['mitigation_metric'],
                    "mitigation_effectiveness": float(rca['mitigation_effectiveness']),
                    "resolution_confidence": float(rca['resolution_confidence']),
                    "detection_time_seconds": rca['detection_time_seconds'],
                    "recovery_time_seconds": rca['recovery_time_seconds'],
                    "incident_date": rca['incident_date'].isoformat(),
                    "tags": rca['metadata'].get('tags', []),
                    "full_rca": json.dumps(rca, default=str)
                }
            )
            
            logger.debug(f"Indexed RCA: {rca['incident_id']} - {rca['title']}")
        
        logger.info(f"Successfully populated {len(HISTORICAL_RCAS)} RCA records in vector DB")
        
    except Exception as e:
        logger.error(f"Failed to populate vector DB: {str(e)}", exc_info=True)
        raise


async def get_sample_queries() -> List[Dict[str, str]]:
    """Get sample queries for testing the RAG system."""
    return [
        {
            "query": "database timeout 300% spike tenant payment",
            "expected_match": "INC-2025-001"
        },
        {
            "query": "kafka consumer lag high latency event processing",
            "expected_match": "INC-2025-002"
        },
        {
            "query": "memory leak garbage collection pause",
            "expected_match": "INC-2025-003"
        },
        {
            "query": "cpu utilization high search regex catastrophic",
            "expected_match": "INC-2025-004"
        },
        {
            "query": "external API rate limit email queue",
            "expected_match": "INC-2025-005"
        },
        {
            "query": "distributed lock deadlock checkout orders",
            "expected_match": "INC-2025-006"
        },
        {
            "query": "vector database similarity search timeout",
            "expected_match": "INC-2025-007"
        },
        {
            "query": "stored procedure recursion analytics",
            "expected_match": "INC-2025-008"
        },
        {
            "query": "ssl certificate expired api gateway",
            "expected_match": "INC-2025-009"
        },
        {
            "query": "object storage bucket access control",
            "expected_match": "INC-2025-010"
        }
    ]


if __name__ == "__main__":
    """Print RCA data for verification"""
    import json
    print(json.dumps(HISTORICAL_RCAS, indent=2, default=str))
