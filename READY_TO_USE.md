# 🚀 RAG of Fire - Ready to Use Guide

## What You Have Right Now (Phases 1-8 Complete)

### ✅ Core Incident Response Engine
Your system can now:

1. **Detect Anomalies** in real-time streaming data
2. **Search Historical Context** with semantic matching
3. **Generate Recommendations** with AI-powered decision engine
4. **Broadcast Alerts** via WebSocket to unlimited clients
5. **Notify Teams** automatically via Slack
6. **Track Citations** for every decision

---

## 🎮 Interactive Demo

### Start the Server (30 seconds)
```bash
cd d:\rag-incident-response
pip install -r requirements.txt
cd backend
uvicorn main:app --reload
```

You should see:
```
INFO:     Uvicorn running on http://127.0.0.1:8000
INFO:     Streaming pipeline initialized
INFO:     Vector database populated with 10 historical RCAs
INFO:     Notification service initialized
```

### Open Dashboard
```
http://localhost:8000/docs
```

---

## 💡 Live Example Workflows

### Workflow 1: Database Timeout (60 seconds)

**Step 1**: Send an anomaly alert
```bash
curl -X POST http://localhost:8000/api/v1/decisions/stream-anomaly \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "payment_service",
    "service_name": "checkout",
    "error_type": "database_timeout",
    "metric_name": "db_connection_pool",
    "baseline_value": 60,
    "current_value": 330,
    "spike_percentage": 450,
    "window_minutes": 5
  }'
```

Response: `{"id": "anom_xxx", "detected_at": "2025-05-24T..."}`

**Step 2**: Ask for recommendation
```bash
curl -X POST http://localhost:8000/api/v1/decisions/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "error_type": "database_timeout",
    "metric_name": "db_connection_pool",
    "spike_percentage": 450,
    "tenant_id": "payment_service",
    "service_name": "checkout",
    "window_minutes": 5,
    "top_k": 5
  }'
```

Response:
```json
{
  "id": "dec_abc123",
  "matched_incident": "INC-2025-001",
  "symptom": "Database connection pool exhaustion during peak load",
  "recommended_action": "Reduce to 30% throughput",
  "confidence_score": 0.95,
  "citations": [
    "Historical INC-2025-001: Identical 450% spike resolved with 30% throttle",
    "Effectiveness: 92% - Recovered within 8 minutes"
  ],
  "latency_ms": 245
}
```

**Step 3**: Subscribe to WebSocket for real-time updates
```javascript
// In browser console at http://localhost:8000/docs

const ws = new WebSocket('ws://localhost:8000/ws/incidents');

ws.onmessage = (event) => {
  console.log('Decision broadcast:', JSON.parse(event.data));
};

// Slack receives: "🚨 Incident Decision: INC-2025-001 - Throttle to 30%"
```

---

### Workflow 2: Search Historical Incidents (30 seconds)

**Find similar past incidents**:
```bash
curl "http://localhost:8000/api/v1/documents/rcas/search?query=database%20timeout&error_type=database_timeout&top_k=5"
```

Response:
```json
{
  "count": 1,
  "results": [
    {
      "incident_id": "INC-2025-001",
      "title": "Production Database Connection Pool Exhaustion - May 2025",
      "summary": "During peak traffic hours, database connection pool depleted...",
      "similarity_score": 0.95,
      "error_type": "database_timeout",
      "mitigation_metric": "Throttle to 30%",
      "mitigation_effectiveness": 92.0,
      "incident_date": "2025-05-01T14:30:00"
    }
  ]
}
```

---

### Workflow 3: Monitor Live Connections (20 seconds)

**Check WebSocket connections**:
```bash
curl http://localhost:8000/ws/stats
```

Response:
```json
{
  "active_connections": 3,
  "client_ids": [
    "ws_550e8400",
    "dashboard_6ba7b810",
    "ws_3e5e8400"
  ],
  "slack_enabled": true,
  "timestamp": "2025-05-24T10:30:45.123456"
}
```

---

## 🧠 Understanding the System

### The 3-Phase Detection Flow

```
PHASE 1: ANOMALY DETECTION
  ↓
Raw metrics arrive → Anomaly detector checks spike percentage
  ↓ (If spike > 300% for 5 minutes)

PHASE 2: INTELLIGENT SEARCH
  ↓
Vector DB searches for semantically similar historical incidents
  ↓ (Finds: "INC-2025-001: database_timeout, 450% spike")

PHASE 3: DECISION GENERATION
  ↓
LLM engine combines historical + current context → Recommendation
  ↓ (Returns: "Throttle to 30%, 95% confidence, historical effectiveness 92%")

BROADCAST → WebSocket clients + Slack channels
```

### The 10 Historical Patterns

Your system has learned from these 10 production incidents:

| # | Error Type | Spike | Fix | Works |
|---|-----------|-------|-----|-------|
| 1 | Database Timeout | 450% | Throttle 30% | 92% |
| 2 | Kafka Consumer Lag | 320% | Throttle 45% | 85% |
| 3 | Memory Leak | 280% | Throttle 20% | 88% |
| 4 | High CPU | 280% | Throttle 50% | 90% |
| 5 | External API Rate Limit | 620% | Throttle 60% | 86% |
| 6 | Distributed Lock Deadlock | 410% | Throttle 35% | 94% |
| 7 | Vector DB Timeout | 1300% | Throttle 40% | 83% |
| 8 | Database Recursion Limit | 600% | Throttle 70% | 91% |
| 9 | SSL Certificate Expired | 100% | Manual Failover | 99% |
| 10 | Access Control Misc. | 250% | Throttle 10% | 97% |

When your system detects a new incident, it finds the best match and applies the proven fix.

---

## 📱 Building a Dashboard (Next Step)

Your WebSocket is ready for a frontend. Here's a simple Next.js client:

```javascript
// app/components/IncidentMonitor.tsx
'use client';

import { useEffect, useState } from 'react';

export default function IncidentMonitor() {
  const [decisions, setDecisions] = useState([]);
  const [ws, setWs] = useState(null);

  useEffect(() => {
    const socket = new WebSocket('ws://localhost:8000/ws/incidents');
    
    socket.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === 'decision') {
        setDecisions(prev => [msg.data, ...prev].slice(0, 10));
      }
    };

    setWs(socket);
    return () => socket.close();
  }, []);

  return (
    <div>
      <h1>🚨 Incident Decisions</h1>
      {decisions.map(decision => (
        <div key={decision.id} className="card">
          <h2>{decision.matched_incident}</h2>
          <p>Action: {decision.recommended_action}</p>
          <p>Confidence: {(decision.confidence_score * 100).toFixed(0)}%</p>
          <p>Latency: {decision.latency_ms}ms</p>
        </div>
      ))}
    </div>
  );
}
```

---

## 🔧 Configuration

### Enable Slack Notifications (Optional)

**Step 1**: Create Slack webhook
- Go to: https://api.slack.com/apps/new
- Select workspace → Create app
- Enable "Incoming Webhooks"
- Add webhook URL

**Step 2**: Update `.env`
```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

**Step 3**: Restart server
```bash
cd backend && uvicorn main:app --reload
```

Now alerts go to Slack automatically! 🎉

---

## 📊 Testing with Real Data

### Run the Built-in Demo
```bash
python test_system.py
```

Expected output:
```
✅ TEST 0: System Health Check
✅ TEST 1: Database Timeout Analysis
✅ TEST 2: Kafka Consumer Lag
✅ TEST 3: Memory Leak Detection
✅ TEST 4: Anomaly Reporting
✅ TEST 5: RCA Search
✅ TEST 6: Document Statistics

Total: 6/6 tests passed ✅
```

---

## 🎓 API Reference (Quick)

### Decision Generation
```bash
POST /api/v1/decisions/analyze
{
  "error_type": "database_timeout",
  "spike_percentage": 450,
  "tenant_id": "payment",
  "service_name": "payments"
}
```

### Anomaly Reporting
```bash
POST /api/v1/decisions/stream-anomaly
{
  "tenant_id": "payment",
  "service_name": "payments",
  "error_type": "database_timeout",
  "spike_percentage": 450
}
```

### RCA Search
```bash
GET /api/v1/documents/rcas/search?query=database&top_k=5
```

### WebSocket Streams
```
WS /ws/incidents          # Real-time decisions
WS /ws/dashboard          # System metrics
GET /ws/stats             # Connection count
```

---

## 🎯 What's Next (After You've Tested)

### Priority 1: Build Dashboard (Phase 10)
- Create Next.js app with real-time updates
- Add decision history table
- Show system metrics
- Add RCA search interface

### Priority 2: Add Tests (Phase 9)
- Unit tests for each service
- Integration tests
- Load testing
- Error scenario tests

### Priority 3: Deploy
- Docker container
- Kubernetes ready
- Monitoring integration
- Production database

---

## ❓ Troubleshooting

### WebSocket Connection Refused
```bash
# Check server is running
curl http://localhost:8000/docs

# Look for:
# - "Uvicorn running on http://127.0.0.1:8000"
# - "Streaming pipeline initialized"
```

### No Decision Generated
```bash
# Check vector DB was populated
# Server logs should show:
# "Vector database populated with historical RCAs"

# If not, delete and restart:
rm -rf backend/data/chroma_db
cd backend && uvicorn main:app --reload
```

### Slack Webhook Error
```bash
# Check webhook URL in .env
# Test webhook with curl:
curl -X POST https://hooks.slack.com/services/YOUR/WEBHOOK/URL \
  -d '{"text": "test"}'

# Should return: "1" (success)
```

---

## 🎉 You're Ready!

Your incident mitigation AI is now:
- ✅ Detecting anomalies
- ✅ Searching history
- ✅ Generating recommendations
- ✅ Broadcasting via WebSocket
- ✅ Sending Slack alerts

**Next**: Build a dashboard or add more historical incidents to your knowledge base!

---

**Questions?** Check the docs:
- [QUICKSTART.md](QUICKSTART.md) - 5-minute setup
- [SETUP_GUIDE.md](SETUP_GUIDE.md) - Detailed config
- [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - Full overview
- API Docs: http://localhost:8000/docs

**Happy incident mitigation! 🚀**
