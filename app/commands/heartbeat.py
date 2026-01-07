from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobLease, JobEventLog, Job
from app.domain.states import JobEvent
from app.domain.errors import LeaseNotFoundError, LeaseExpiredError

async def heartbeat(
    session: AsyncSession,
    job_id: UUID,
    lease_token: UUID,
    extend_seconds: int = 60
) -> datetime:
    """
    Renews the lease for a job.
    Throws error if lease not found or token mismatch or already expired.
    Returns new expires_at.
    """
    now = datetime.now()
    
    # Check if lease exists and token matches
    stmt = select(JobLease).where(
        JobLease.job_id == job_id,
        JobLease.lease_token == lease_token
    )
    result = await session.execute(stmt)
    lease = result.scalar_one_or_none()
    
    if not lease:
        # Check if it expired? Or purely not found (requeued/completed)
        raise LeaseNotFoundError(f"Lease for job {job_id} not found or token mismatch")
        
    
    # Ensure now is timezone aware if comparing with aware DB fields
    # If lease.expires_at is aware, convert now to match or lease to naive.
    # Postgres returns Aware (UTC) if timezone=True.
    if lease.expires_at.tzinfo:
        now = now.astimezone(lease.expires_at.tzinfo)
    
    if lease.expires_at < now:
        # It expired, we can't renew it. It's likely been swept or will be swept.
        # But for correctness, we shouldn't allow renewal.
        raise LeaseExpiredError(f"Lease for job {job_id} expired at {lease.expires_at}")
        
    # Check Execution Timeout
    # optimized: join job? or fetch job? Lease usually doesn't have timeout info.
    # We need to fetch job to check timeout.
    # Cost: extra lookup. Optimization: Store timeout on lease at creation?
    # For now, let's fetch job.
    stmt_job = select(Job).where(Job.id == job_id)
    res_job = await session.execute(stmt_job)
    job = res_job.scalar_one_or_none()
    
    new_expires_at = now + timedelta(seconds=extend_seconds)

    if job and job.execution_timeout and job.started_at:
        # Debugging time
        started_at = job.started_at
        if started_at.tzinfo:
             # Convert to same timezone as now (which we aligned to lease.expires_at)
             # Or convert both to timestamp/naive
             started_at = started_at.astimezone(now.tzinfo)
             
        runtime = (now - started_at).total_seconds()
        
        if runtime > job.execution_timeout:
             raise LeaseExpiredError(f"Execution timeout exceeded ({runtime} > {job.execution_timeout}s)")

        
    new_expires_at = now + timedelta(seconds=extend_seconds)
    
    lease.last_heartbeat_at = now
    lease.expires_at = new_expires_at
    
    # Optional: Log heartbeat event? (Maybe too noisy, only log periodically or debug)
    # Using JobEvent.LEASE_RENEWED
    # event = JobEventLog(
    #     job_id=job_id,
    #     event_type=JobEvent.LEASE_RENEWED,
    #     meta={"new_expires_at": str(new_expires_at)}
    # )
    # session.add(event)
    
    await session.flush()
    return new_expires_at
