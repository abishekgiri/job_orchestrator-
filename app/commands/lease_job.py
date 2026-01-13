from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID, uuid4
import random
import asyncio
import logging

logger = logging.getLogger(__name__)

from sqlalchemy import select, update, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.db.models import Job, JobLease, JobEventLog, Tenant
from app.domain.states import JobStatus, JobEvent
# We can't import QUEUE_DEPTH here directly if we're changing metrics.py structure.
# But assuming metrics.py will be updated or exists, let's defer metrics updates slightly or assume they work.
# Actually, the user asked to ADD labels. So I will update metrics logic here.
from app.api.v1.metrics import QUEUE_DEPTH, JOB_LEASE_TIME, JOB_LEASE_TOTAL
from app.settings import settings

async def lease_job(
    session: AsyncSession,
    worker_id: str,
    tenant_id: Optional[str] = None,
    lease_duration: Optional[int] = None
) -> Optional[tuple[Job, JobLease]]:
    """
    Atomically claims a pending job for the given worker.
    
    Strategies:
    Case A (Pinned Worker): tenant_id is provided. Lease best eligible job for that tenant.
    Case B (Shared Worker): tenant_id is None. Two-Step Fairness:
        1. Find "active tenants" (have PENDING jobs eligible now).
        2. Weighted Random Selection of one tenant.
        3. Lease best job for that tenant.
    """
    
    duration = lease_duration if lease_duration is not None else settings.DEFAULT_LEASE_TIMEOUT_SECONDS
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=duration)
    lease_token = uuid4()
    
    found_job = None
    
    # --- Case A: Pinned Worker ---
    if tenant_id:
        subquery = _build_job_query(tenant_id, now)
        sel_res = await session.execute(subquery)
        found_job = sel_res.scalar_one_or_none()
        
    # --- Case B: Shared Worker ---
    else:
        # Retry loop for race conditions (Step 1 -> Step 2 race)
        for _ in range(3):
            # Step 1: Find Active Tenants
            # Optimization: Use the index 'ix_jobs_poll' implicitly by filtering status='pending'
            # We need tenants that have at least one job ready.
            # SELECT DISTINCT tenant_id FROM jobs WHERE status='pending' AND available_at <= now
            # But we also need their weight. So we join with Tenants.
            
            # Note: This query might differ in performance with large job counts.
            # Ideally we have a separate "TenantQueue" table, but for now we join.
            # To avoid scanning matching jobs, we can use distinct.
            
            # Step 1: Find Active Tenants that are NOT at their max_inflight cap.
            # We count actual leases for each tenant.
            inflight_counts_q = (
                select(
                    Job.tenant_id,
                    func.count(JobLease.job_id).label("current_inflight")
                )
                .join(JobLease, Job.id == JobLease.job_id)
                .group_by(Job.tenant_id)
            ).subquery()

            active_tenants_q = (
                select(Tenant.id, Tenant.weight)
                .join(Job, Job.tenant_id == Tenant.id)
                .outerjoin(inflight_counts_q, Tenant.id == inflight_counts_q.c.tenant_id)
                .where(
                    Job.status == JobStatus.PENDING,
                    Job.available_at <= now,
                    # Filter: (no inflight yet) OR (inflight < cap)
                    and_(
                        (func.coalesce(inflight_counts_q.c.current_inflight, 0) < Tenant.max_inflight)
                    )
                )
                .group_by(Tenant.id, Tenant.weight, inflight_counts_q.c.current_inflight)
            )
            
            # Current async session execute
            t_res = await session.execute(active_tenants_q)
            active_tenants = t_res.all() # list of (id, weight)
            
            if not active_tenants:
                return None
                
            # Step 2: Weighted Random Selection
            # active_tenants is [(id, weight), ...]
            tenants = [t[0] for t in active_tenants]
            weights = [t[1] for t in active_tenants]
            
            # Python's random.choices selects with replacement, k=1
            chosen_tenant_id = random.choices(tenants, weights=weights, k=1)[0]
            
            # Step 3: Lease from chosen tenant
            subquery = _build_job_query(chosen_tenant_id, now)
            sel_res = await session.execute(subquery)
            found_job = sel_res.scalar_one_or_none()
            
            if found_job:
                break
            # If not found (race condition, occupied between Step 1 and 3), retry loop picks another tenant.
            
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
    # Update logic to handle labels properly
    QUEUE_DEPTH.labels(tenant_id=job.tenant_id).dec()
    JOB_LEASE_TOTAL.labels(tenant_id=job.tenant_id, worker_type="pinned" if tenant_id else "shared").inc()
    from app.api.v1.metrics import JOB_ATTEMPTS_TOTAL
    JOB_ATTEMPTS_TOTAL.labels(tenant_id=job.tenant_id).inc()
    
    if job.available_at:
            # available_at shouldn't be none if it was pending
            # Both should be timezone-aware (UTC)
            delay = (now - job.available_at).total_seconds()
            if delay >= 0:
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
        await _handle_cron_recurrence(session, job, now)

    await session.flush()
    
    return job, lease

def _build_job_query(tenant_id, now):
    return select(Job).where(
        Job.status == JobStatus.PENDING,
        Job.available_at <= now,
        Job.tenant_id == tenant_id
    ).order_by(
        Job.priority.desc(),
        Job.available_at.asc()
    ).with_for_update(skip_locked=True).limit(1)

async def _handle_cron_recurrence(session, job, now):
    try:
        from croniter import croniter
        base_time = job.available_at if job.available_at else now
        iter = croniter(job.cron_schedule, base_time)
        next_run = iter.get_next(datetime)
        
        next_job = Job(
            tenant_id=job.tenant_id,
            payload=job.payload,
            priority=job.priority,
            idempotency_key=None,
            max_attempts=job.max_attempts,
            execution_timeout=job.execution_timeout,
            status=JobStatus.SCHEDULED,
            available_at=next_run,
            cron_schedule=job.cron_schedule
        )
        session.add(next_job)
        
    except ImportError:
        logger.warning("Warning: croniter not installed, cannot schedule next recurrence.")
    except Exception as e:
        logger.error(f"Error scheduling next cron job: {e}")
