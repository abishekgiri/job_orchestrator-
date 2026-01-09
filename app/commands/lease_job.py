from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import select, update, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.db.models import Job, JobLease, JobEventLog
from app.domain.states import JobStatus, JobEvent
from app.api.v1.metrics import QUEUE_DEPTH, JOB_LEASE_TIME
from app.settings import settings

async def lease_job(
    session: AsyncSession,
    worker_id: str,
    tenant_id: Optional[str] = None,
    lease_duration: Optional[int] = None
) -> Optional[tuple[Job, JobLease]]:
    """
    Atomically claims a pending job for the given worker.
    
    Strategy:
    1. Find a job that is PENDING and available_at <= now()
    2. Filter by tenant_id if provided (for strict multi-tenant workers)
    3. Order by priority DESC, available_at ASC
    4. Lock the row with FOR UPDATE SKIP LOCKED
    5. Update status to LEASED
    6. create a Lease record
    7. create an Event log
    """
    
    duration = lease_duration if lease_duration is not None else settings.DEFAULT_LEASE_TIMEOUT_SECONDS
        
    now = datetime.now()
    expires_at = now + timedelta(seconds=duration)
    lease_token = uuid4()
    
    # Subquery to find the job ID
    # SELECT * FROM jobs WHERE ... FOR UPDATE SKIP LOCKED LIMIT 1
    subquery = select(Job).where(
        Job.status == JobStatus.PENDING,
        Job.available_at <= now
    )
    
    if tenant_id:
        subquery = subquery.where(Job.tenant_id == tenant_id)
        
    subquery = subquery.order_by(
        Job.priority.desc(),
        Job.available_at.asc()
    ).with_for_update(skip_locked=True).limit(1)
    
    # Execute Selection first to ensure lock
    sel_res = await session.execute(subquery)
    found_job = sel_res.scalar_one_or_none()
    
    if not found_job:
        return None
    
    found_job_id = found_job.id

    # Main Update
    # UPDATE jobs SET status='LEASED', updated_at=now WHERE id = found_id RETURNING *
    stmt = update(Job).where(
        Job.id == found_job_id
    ).values(
        status=JobStatus.LEASED,
        updated_at=now,
        started_at=now  # Reset execution timer
    ).returning(Job)
    
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        return None
        
    # Create the lease
    lease = JobLease(
        job_id=job.id,
        worker_id=worker_id,
        lease_token=lease_token,
        expires_at=expires_at,
        last_heartbeat_at=now
    )
    session.add(lease)
    
    # Metrics
    QUEUE_DEPTH.labels(tenant_id=job.tenant_id).dec()
    if job.available_at:
         # available_at shouldn't be none if it was pending
         delay = (now - job.available_at.replace(tzinfo=None)).total_seconds()
         if delay > 0:
             JOB_LEASE_TIME.observe(delay)
    
    # Audit log
    event = JobEventLog(
        job_id=job.id,
        event_type=JobEvent.LEASED,
        timestamp=now,
        meta={
            "worker_id": worker_id, 
            "lease_token": str(lease_token),
            "expires_at": str(expires_at)
        }
    )
    session.add(event)
    
    # Handle Cron Recurrence
    if job.cron_schedule:
        try:
            from croniter import croniter
            # Calculate next run time
            # base_time: use the job's scheduled available_at to prevent drift, or now if it was immediate
            # Ensure timezone awareness handling
            base_time = job.available_at if job.available_at else now
            iter = croniter(job.cron_schedule, base_time)
            next_run = iter.get_next(datetime)
            
            # Create next job instance
            next_job = Job(
                tenant_id=job.tenant_id,
                payload=job.payload,
                priority=job.priority, # Inherit base priority
                idempotency_key=None, # Reset idempotency for new instance (or generate new one?)
                max_attempts=job.max_attempts,
                execution_timeout=job.execution_timeout,
                status=JobStatus.SCHEDULED,
                available_at=next_run,
                cron_schedule=job.cron_schedule
            )
            session.add(next_job)
            
        except ImportError:
            # In case croniter is missing, we log or ignore? 
            # For this context, we assume it's installed.
            print("Warning: croniter not installed, cannot schedule next recurrence.")
        except Exception as e:
            print(f"Error scheduling next cron job: {e}")

    # We commit in the caller usually, but commands might be self-contained?
    # Usually better to let caller commit to allow composition.
    # But for a "lease" command, it implies a transaction.
    # I will flush to ensure integrity but return objects.
    # Caller responsible for commit to finalize the lease.
    await session.flush()
    
    return job, lease
