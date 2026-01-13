from datetime import datetime
from uuid import UUID

from sqlalchemy import select, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobLease, JobEventLog
from app.domain.states import JobStatus, JobEvent

async def requeue_expired_jobs(session: AsyncSession, limit: int = 100) -> int:
    """
    Finds expired leases, removes them, and reverts jobs to PENDING.
    Returns number of jobs recovered.
    """
    now = datetime.now()
    
    # Find expired leases
    # We could do this in one complex UPDATE, but safer to read then update for logic.
    # However, to avoid race conditions, we should lock rows.
    
    # 1. Select expired leases FOR UPDATE
    stmt = select(JobLease).where(
        JobLease.expires_at < now
    ).limit(limit).with_for_update(skip_locked=True)
    
    result = await session.execute(stmt)
    expired_leases = result.scalars().all()
    
    if not expired_leases:
        return 0
        
    count = 0
    for lease in expired_leases:
        count += 1
        job_id = lease.job_id
        
        # Get the job
        job_stmt = select(Job).where(Job.id == job_id).with_for_update()
        j_res = await session.execute(job_stmt)
        job = j_res.scalar_one()
        
        # Logic: Treat expiry as a failure?
        job.attempts += 1
        job.last_error = "Lease expired (worker crash?)"
        job.updated_at = now
        
        if job.attempts >= job.max_attempts:
            job.status = JobStatus.DLQ
            event_type = JobEvent.DLQ_ROUTED
        else:
            job.status = JobStatus.PENDING
            # Retry immediately usually, or backoff?
            # If worker crashed, maybe immediate retry is fine.
            # But if it's a poison pill crashing workers, we want backoff.
            # Let's use logic similar to fail_job but maybe smaller backoff?
            # For simplicity, just available_at = now
            job.available_at = now
            event_type = JobEvent.RETRIED

        # Delete lease
        await session.delete(lease)
        
        # Log
        session.add(JobEventLog(
            job_id=job.id,
            event_type=event_type,
            timestamp=now,
            meta={"reason": "lease_expired", "worker_id": lease.worker_id}
        ))
        
    if count > 0:
        from app.api.v1.metrics import JOB_REAPED_TOTAL
        JOB_REAPED_TOTAL.inc(count)
        
    await session.flush()
    return count
