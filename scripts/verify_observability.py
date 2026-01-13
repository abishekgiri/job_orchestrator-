import asyncio
import httpx
import re
from uuid import uuid4
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.db.models import Job, Tenant, JobLease
from app.domain.states import JobStatus
from app.scheduler.service import SchedulerService

def parse_metric(output: str, metric_name: str, labels: dict = None) -> float:
    """
    Parses a Prometheus metric value from the text output.
    Supports basic label matching.
    """
    lines = output.split('\n')
    for line in lines:
        if line.startswith('#'):
            continue
        if not line.strip():
            continue
        
        match = re.match(r'^([a-zA-Z_0-9]+)(\{.*\})?\s+(.+)$', line)
        if match:
            found_name = match.group(1)
            found_labels_str = match.group(2)
            found_value = match.group(3)
            
            if found_name == metric_name:
                if labels is None:
                    return float(found_value)
                
                if found_labels_str:
                    all_matched = True
                    for k, v in labels.items():
                        if f'{k}="{v}"' not in found_labels_str:
                            all_matched = False
                            break
                    if all_matched:
                        return float(found_value)
    return 0.0

async def verify_metrics():
    print("--- Verifying Metrics Behavior (Success, Retry & DLQ) ---")
    
    from datetime import datetime, timedelta, timezone
    from app.scheduler.ticker import run_ticker, run_metrics_tasks
    from app.scheduler.dispatcher import dispatch_lease
    from app.commands.complete_job import complete_job
    from app.commands.fail_job import fail_job
    from prometheus_client import generate_latest

    # CLEANUP all existing data for clean baseline
    async with AsyncSessionLocal() as session:
        from sqlalchemy import delete
        await session.execute(delete(Job))
        await session.execute(delete(JobLease))
        await session.commit()
        
    print("CLEANUP Done")

    # 1. SUCCESS CASE (Verify Delay & Duration)
    t_id_s = str(uuid4())
    async with AsyncSessionLocal() as session:
        session.add(Tenant(id=t_id_s, name=f"tenant-success", weight=10, api_key=f"key-s-{uuid4()}"))
        # 5s offset
        past_time = datetime.now(timezone.utc) - timedelta(seconds=5)
        job_s = Job(id=uuid4(), tenant_id=t_id_s, status=JobStatus.PENDING, available_at=past_time)
        session.add(job_s)
        await session.commit()
        
        # Lease
        lease = await dispatch_lease(session, "worker-metrics", tenant_id=t_id_s)
        job_s, lease_obj_s = lease
        await session.commit()
        
        # In-Lease Inflight Check
        await run_metrics_tasks(session)
        val_inf = parse_metric(generate_latest().decode(), 'jobs_inflight')
        print(f"In-Lease inflight: {val_inf}")

        # Complete
        await complete_job(session, job_s.id, {"status": "ok"}, lease_token=lease_obj_s.lease_token)
        await session.commit()
    print("Success Case Done")

    # 2. RETRY -> DLQ CASE (Verify Attempts & DLQ Counters)
    t_id_f = str(uuid4())
    async with AsyncSessionLocal() as session:
        session.add(Tenant(id=t_id_f, name=f"tenant-fail", weight=10, api_key=f"key-f-{uuid4()}"))
        # Max attempts = 2
        job_f = Job(id=uuid4(), tenant_id=t_id_f, status=JobStatus.PENDING, max_attempts=2)
        session.add(job_f)
        await session.commit()
        job_f_id = job_f.id

        # ATTEMPT 1: Lease -> Fail (Retry)
        lease_1 = await dispatch_lease(session, "worker-f1", tenant_id=t_id_f)
        job_f, l1_obj = lease_1
        await session.commit()
        await fail_job(session, job_f_id, "Error 1", lease_token=l1_obj.lease_token)
        await session.commit()
        print("Attempt 1: RETRY Recorded")

        # BYPASS BACKOFF: Reset available_at to now so it's ready for Attempt 2
        from sqlalchemy import update
        await session.execute(
            update(Job).where(Job.id == job_f_id).values(available_at=datetime.now(timezone.utc))
        )
        await session.commit()

        # ATTEMPT 2: Lease -> Fail (DLQ)
        lease_2 = await dispatch_lease(session, "worker-f2", tenant_id=t_id_f)
        if not lease_2:
            print("ATTEMPT 2: Lease FAILED (Job not available?)")
            exit(1)
        job_f, l2_obj = lease_2
        await session.commit()
        await fail_job(session, job_f_id, "Error 2", lease_token=l2_obj.lease_token)
        await session.commit()
        print("Attempt 2: DLQ Recorded")

    # 3. Final Scrape and Assertions
    await run_metrics_tasks(session)
    output = generate_latest().decode()
    print("\n--- Summary Assertions ---")
    
    def assert_metric(name, expected, labels=None, comparison='eq'):
        val = parse_metric(output, name, labels)
        if comparison == 'eq' and val == expected:
            print(f"PASSED: {name} {labels or ''} is {val}")
            return True
        elif comparison == 'gt' and val > expected:
            print(f"PASSED: {name} {labels or ''} is {val} (> {expected})")
            return True
        else:
            print(f"FAILED: {name} {labels or ''} is {val} (expected {comparison} {expected})")
            return False

    success = True
    # Success tenant
    success &= assert_metric('job_complete_total', 1.0, {'tenant_id': t_id_s, 'result': 'success'})
    success &= assert_metric('job_start_delay_seconds_sum', 5.0, comparison='gt')
    
    # Fail tenant
    success &= assert_metric('job_attempts_total', 2.0, {'tenant_id': t_id_f}) # 1 per lease
    success &= assert_metric('job_complete_total', 1.0, {'tenant_id': t_id_f, 'result': 'retry'})
    success &= assert_metric('job_complete_total', 1.0, {'tenant_id': t_id_f, 'result': 'dlq'})
    success &= assert_metric('job_dlq_total', 1.0, {'tenant_id': t_id_f})
    
    # System
    success &= assert_metric('jobs_inflight', 0.0)

    if success:
         print("\n--- ALL OBSERVABILITY PROOFS PASSED ---")
    else:
         print("\n--- OBSERVABILITY PROOFS FAILED ---")
         exit(1)

if __name__ == "__main__":
    asyncio.run(verify_metrics())
