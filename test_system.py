#!/usr/bin/env python3
"""
Demo script: Test RAG of Fire system end-to-end.
Run this after starting the backend server.
"""

import asyncio
import json
from datetime import datetime
from decimal import Decimal

import requests

BASE_URL = "http://localhost:8000"


def print_section(title: str):
    """Print formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def print_response(label: str, response: dict, indent: int = 0):
    """Print formatted response."""
    prefix = "  " * indent
    print(f"{prefix}{label}:")
    print(json.dumps(response, indent=2, default=str).replace('\n', f'\n{prefix}'))


async def test_decision_analysis():
    """Test 1: Database timeout anomaly analysis."""
    print_section("TEST 1: Database Timeout Anomaly Analysis")
    
    payload = {
        "error_type": "database_timeout",
        "metric_name": "db_query_time_p99_ms",
        "spike_percentage": 450,
        "tenant_id": "payment_service_prod",
        "service_name": "payments",
        "window_minutes": 5,
        "top_k": 3
    }
    
    print("Request:")
    print(json.dumps(payload, indent=2))
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/decisions/analyze",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"\n✅ Decision Generated (Latency: {result['latency_ms']}ms)")
            print(f"   Matched Incident: {result['matched_incident']}")
            print(f"   Symptom: {result['symptom']}")
            print(f"   Recommendation: {result['recommended_action']}")
            print(f"   Confidence: {result['confidence_score']:.2%}")
            print(f"\n   Citations:")
            for i, citation in enumerate(result['citations'][:2], 1):
                print(f"   {i}. {citation}")
            
            print(f"\n   Retrieved Historical Incidents:")
            for doc in result['retrieved_documents'][:2]:
                print(f"   - {doc['incident_id']}: {doc['title']}")
                print(f"     Error: {doc['error_type']}, Similarity: {doc['similarity_score']:.2%}")
            
            return True
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


async def test_kafka_consumer_lag():
    """Test 2: Kafka consumer lag detection."""
    print_section("TEST 2: Kafka Consumer Lag Detection")
    
    payload = {
        "error_type": "kafka_consumer_lag",
        "metric_name": "kafka_consumer_lag",
        "spike_percentage": 320,
        "tenant_id": "analytics_events",
        "service_name": "event_processing",
        "window_minutes": 5,
        "top_k": 3
    }
    
    print("Request:")
    print(json.dumps(payload, indent=2))
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/decisions/analyze",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ Decision Generated")
            print(f"   Matched Incident: {result['matched_incident']}")
            print(f"   Recommendation: {result['recommended_action']}")
            print(f"   Confidence: {result['confidence_score']:.2%}")
            return True
        else:
            print(f"❌ Error {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


async def test_memory_leak():
    """Test 3: Memory leak scenario."""
    print_section("TEST 3: Memory Leak Detection")
    
    payload = {
        "error_type": "memory_leak",
        "metric_name": "heap_memory_gb",
        "spike_percentage": 280,
        "tenant_id": "session_store_prod",
        "service_name": "sessions",
        "window_minutes": 5,
        "top_k": 3
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/decisions/analyze",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Decision Generated")
            print(f"   Matched Incident: {result['matched_incident']}")
            print(f"   Recommendation: {result['recommended_action']}")
            print(f"   Confidence: {result['confidence_score']:.2%}")
            return True
        else:
            print(f"❌ Error {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


async def test_anomaly_reporting():
    """Test 4: Report streaming anomaly."""
    print_section("TEST 4: Report Streaming Anomaly")
    
    payload = {
        "tenant_id": "commerce",
        "service_name": "checkout",
        "error_type": "distributed_lock_deadlock",
        "metric_name": "lock_timeout_errors",
        "baseline_value": 10,
        "current_value": 4100,
        "spike_percentage": 40900,
        "window_minutes": 5,
        "raw_payload": {
            "affected_orders": 45000,
            "revenue_impact": "$250k"
        }
    }
    
    print("Request:")
    print(json.dumps(payload, indent=2, default=str))
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/decisions/stream-anomaly",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ Anomaly Ingested")
            print(f"   ID: {result['id']}")
            print(f"   Error: {result['error_type']}")
            print(f"   Spike: {result['spike_percentage']}%")
            print(f"   Detected: {result['detected_at']}")
            return True
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


async def test_rca_search():
    """Test 5: Search RCA documents."""
    print_section("TEST 5: Search RCA Documents")
    
    params = {
        "query": "database timeout spike connection pool",
        "error_type": "database_timeout",
        "top_k": 3
    }
    
    print("Request:")
    print(f"  Query: {params['query']}")
    print(f"  Filter: {params['error_type']}")
    print(f"  Top K: {params['top_k']}")
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/documents/rcas/search",
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ Found {result['count']} RCA matches")
            
            for i, rca in enumerate(result['results'][:3], 1):
                print(f"\n   {i}. {rca['incident_id']}: {rca['title']}")
                print(f"      Error: {rca['error_type']}")
                print(f"      Mitigation: {rca['mitigation_metric']}")
                print(f"      Effectiveness: {rca['mitigation_effectiveness']:.0f}%")
                print(f"      Similarity: {rca['similarity_score']:.2%}")
            
            return True
        else:
            print(f"❌ Error {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


async def test_document_stats():
    """Test 6: Get document statistics."""
    print_section("TEST 6: Document Statistics")
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/documents/stats",
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Statistics Retrieved")
            print(f"   Total RCAs: {result['rcas']['count']}")
            print(f"   Total Documents: {result['documents']['count']}")
            print(f"   Total Items: {result['total']}")
            return True
        else:
            print(f"❌ Error {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


async def test_health_check():
    """Test 0: Health check."""
    print_section("TEST 0: System Health Check")
    
    try:
        response = requests.get(
            f"{BASE_URL}/health",
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ System Healthy")
            print(f"   Status: {result['status']}")
            print(f"   App: {result['app_name']} v{result['version']}")
            return True
        else:
            print(f"❌ Unhealthy: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Backend not running: {e}")
        return False


async def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("  RAG OF FIRE - SYSTEM DEMONSTRATION")
    print("="*70)
    print("\nThis demo tests the core functionality of RAG of Fire.")
    print("Ensure the backend server is running: uvicorn backend.main:app --reload")
    
    # Test health first
    is_healthy = await test_health_check()
    if not is_healthy:
        print("\n❌ Backend server is not running!")
        print("   Start it with: uvicorn backend.main:app --reload")
        return
    
    # Run all tests
    results = {
        "Database Timeout": await test_decision_analysis(),
        "Kafka Consumer Lag": await test_kafka_consumer_lag(),
        "Memory Leak": await test_memory_leak(),
        "Anomaly Reporting": await test_anomaly_reporting(),
        "RCA Search": await test_rca_search(),
        "Document Stats": await test_document_stats(),
    }
    
    # Summary
    print_section("TEST SUMMARY")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! System is fully operational.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check backend logs.")


if __name__ == "__main__":
    asyncio.run(main())
