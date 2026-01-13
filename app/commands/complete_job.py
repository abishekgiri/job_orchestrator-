from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.exc import IntegrityError
from app.db.models import Job, JobLease, JobEventLog, JobCompletion, OutboxEvent
from app.domain.states import JobStatus, JobEvent
from app.api.v1.metrics import JOB_DURATION, JOB_COMPLETE_TOTAL
from app.domain.errors import JobNotFoundError, InvalidJobStateError

async def complete_job(
    session: AsyncSession,
    job_id: UUID,
    result_data: dict[str, Any],
    lease_token: Optional[UUID] = None,
    idempotency_key: Optional[str] = None
) -> Job:
    """
    Marks a job as SUCCEEDED and saves result.
    Verifies lease if provided.
    Releases lease (deletes it).
    """
    now = datetime.now()
    
    # We select Job and Lease to verify
    stmt = select(Job).where(Job.id == job_id)
    res = await session.execute(stmt)
    job = res.scalar_one_or_none()
    
    if not job:
        raise JobNotFoundError(job_id)
        
    # Idempotency Check
    # Idempotency Check
    if idempotency_key:
        # Check if already processed (Avoid rollback/exception path)
        stmt = select(JobCompletion).where(JobCompletion.idempotency_key == idempotency_key)
        existing = await session.scalar(stmt)
        
        if existing:
            # Already processed. Return success.
            if job.status == JobStatus.SUCCEEDED:
                 return job
            
            # If job not succeeded but key exists?
            # Re-fetch job to be sure
            job_refresh = await session.get(Job, job_id)
            if job_refresh.status == JobStatus.SUCCEEDED:
                 return job_refresh
            
            return job_refresh or job

        try:
            # Try to record this completion attempt
            session.add(JobCompletion(job_id=job_id, idempotency_key=idempotency_key))
            await session.flush()
        except IntegrityError:
            # Race condition: duplicate key inserted concurrently
            # Rollback the transaction to clean state
            await session.rollback()
            
            # Start a new transaction by fetching the job again
            job = await session.get(Job, job_id)
            if job and job.status == JobStatus.SUCCEEDED:
                return job
            return job 

    # Validation
    if job.status != JobStatus.LEASED and job.status != JobStatus.RUNNING:
        # Check current status
        if job.status == JobStatus.SUCCEEDED:
             return job
        else:
             raise InvalidJobStateError(job.status, JobStatus.SUCCEEDED)

    # Verify lease if enforced
    lease_obj: Optional[JobLease] = None
    if lease_token:
        # Check lease
        lease_stmt = select(JobLease).where(
             JobLease.job_id == job_id, 
             JobLease.lease_token == lease_token
        )
        l_res = await session.execute(lease_stmt)
        lease_obj = l_res.scalar_one_or_none()
        if not lease_obj:
             # Could be expired or stolen
             # Usually we reject completion if lease is lost
             raise InvalidJobStateError("Lease invalid or lost", JobStatus.SUCCEEDED)

    # Update Job
    job.status = JobStatus.SUCCEEDED
    job.result = result_data
    job.updated_at = now
    
    # Delete Lease
    if lease_obj:
        await session.delete(lease_obj)
    else:
        # If no lease_token was provided, or lease_obj wasn't found (but shouldn't happen if lease_token was provided)
        # we still need to ensure any lease for this job is removed.
        await session.execute(
            delete(JobLease).where(JobLease.job_id == job_id)
        )
    
    # Observe duration
    # We need start time. It's not in JobLease in my model? 
    # JobLease has expires_at. 
    # Job has updated_at? JobLease doesn't have created_at?
    # Actually JobLease only has expires_at and last_heartbeat_at.
    # To measure duration we ideally need lease.created_at or similar.
    # I'll skip accurate duration for now or calculate approx if I can.
    # Job updated_at might be when it was leased?
    # Observe duration
    # We need start time.
    if job.started_at: # Using started_at if available, else updated_at
        # Both aware (UTC)
        duration = (datetime.now(timezone.utc) - job.started_at).total_seconds()
        if duration > 0:
            JOB_DURATION.observe(duration)
    
    # Metrics
    JOB_COMPLETE_TOTAL.labels(tenant_id=job.tenant_id, result="success").inc()

    # Log Event
    session.add(JobEventLog(
        job_id=job.id,
        event_type=JobEvent.COMPLETED,
        timestamp=now,
        meta={"lease_token": str(lease_token) if lease_token else None}
    ))

    # Outbox Event (Transactional Guarantee)
    session.add(OutboxEvent(
        event_type="JOB_COMPLETED",
        payload={
             "job_id": str(job.id),
             "tenant_id": job.tenant_id,
             "result": result_data,
             "completed_at": now.isoformat()
        }
    ))
    
    await session.flush()
    return job
