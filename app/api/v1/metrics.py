from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import APIRouter, Response

router = APIRouter()

# Metrics Definitions
# Metrics Definitions
QUEUE_DEPTH = Gauge('job_queue_depth', 'Number of jobs in PENDING state', ['tenant_id'])
JOBS_INFLIGHT = Gauge("jobs_inflight", "Number of jobs currently leased/running")
LEADER_STATUS = Gauge("instance_leader_status", "Whether this instance is currently the leader")

# Counters
JOB_LEASE_TOTAL = Counter('job_lease_total', 'Jobs leased', ['tenant_id', 'worker_type'])
JOB_COMPLETE_TOTAL = Counter('job_complete_total', 'Jobs completed/failed/dlq', ['tenant_id', 'result'])
JOB_ATTEMPTS_TOTAL = Counter('job_attempts_total', 'Job execution attempts (retries)', ['tenant_id'])
JOB_DLQ_TOTAL = Counter('job_dlq_total', 'Jobs moved to DLQ', ['tenant_id'])
JOB_REAPED_TOTAL = Counter('job_reaped_total', 'Jobs recovered by reaper')

# Histograms
JOB_LEASE_TIME = Histogram('job_start_delay_seconds', 'Time from available_at to lease', buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0])
JOB_DURATION = Histogram('job_duration_seconds', 'Time from lease to completion', buckets=[0.1, 1.0, 5.0, 10.0, 60.0, 120.0])

@router.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
