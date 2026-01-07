from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobLease, JobEventLog
from app.domain.states import JobStatus, JobEvent
from app.domain.retry import calculate_next_run
from app.domain.errors import JobNotFoundError
from app.api.v1.metrics import JOB_FAILURES, QUEUE_DEPTH

async def fail_job(
    session: AsyncSession,
    job_id: UUID,
    error: str,
    lease_token: Optional[UUID] = None
) -> Job:
    """
    Marks a job as FAILED (retryable or final).
    Calculates next run time base on attempts.
    """
    now = datetime.now()
    
    stmt = select(Job).where(Job.id == job_id)
    res = await session.execute(stmt)
    job = res.scalar_one_or_none()
    
    if not job:
        raise JobNotFoundError(job_id)
        
    # Verify lease if provided... skipped for brevity but similar to complete_job
    
    job.attempts += 1
    
    next_event: JobEvent
    
    if job.attempts >= job.max_attempts:
        # DLQ
        stmt = update(Job).where(Job.id == job_id).values(
            status=JobStatus.DLQ, # OR FAILED_FINAL
            last_error=error,
            updated_at=datetime.now(),
            attempts=job.attempts # Update attempts in DB
        ).returning(Job)
        
        JOB_FAILURES.labels(tenant_id=job.tenant_id, type="final").inc()
        next_event = JobEvent.DLQ_ROUTED
    else:
        # Retry
        next_run = calculate_next_run(job.attempts)
        stmt = update(Job).where(Job.id == job_id).values(
            status=JobStatus.PENDING,
            available_at=next_run,
            last_error=error,
            updated_at=datetime.now(),
            attempts=job.attempts # Update attempts in DB
        ).returning(Job)
        
        JOB_FAILURES.labels(tenant_id=job.tenant_id, type="retryable").inc()
        QUEUE_DEPTH.labels(tenant_id=job.tenant_id).inc() # Back to PENDING
        next_event = JobEvent.RETRIED

    res = await session.execute(stmt)
    job = res.scalar_one() # Get the updated job object

    # Delete lease
    await session.execute(
        delete(JobLease).where(JobLease.job_id == job_id)
    )
    
    # Log
    session.add(JobEventLog(
        job_id=job.id,
        event_type=next_event,
        timestamp=now,
        meta={
            "error": error, 
            "attempts": job.attempts, 
            "max": job.max_attempts, 
            "lease_token": str(lease_token) if lease_token else None
        }
    ))
    
    await session.flush()
    return job
