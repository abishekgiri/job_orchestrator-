from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import APIRouter, Response

router = APIRouter()

# Metrics Definitions
QUEUE_DEPTH = Gauge('job_queue_depth', 'Number of jobs in PENDING state', ['tenant_id'])
JOB_FAILURES = Counter('job_failures_total', 'Total job failures', ['tenant_id', 'type']) # type=retryable|final
JOB_LEASE_TIME = Histogram('job_start_delay_seconds', 'Time from available_at to lease', buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0])
JOB_DURATION = Histogram('job_duration_seconds', 'Time from lease to completion', buckets=[1.0, 5.0, 10.0, 60.0, 120.0])

@router.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
