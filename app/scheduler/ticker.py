from datetime import datetime, timedelta
from sqlalchemy import select, update, text, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job
from app.domain.states import JobStatus
from app.commands.requeue_expired import requeue_expired_jobs

async def run_leader_tasks(session: AsyncSession):
    """
    Tasks that must only run on ONE instance (The Leader).
    1. Requeue Expired Leases (Reaper)
    2. Advance SCHEDULED jobs (Scheduler)
    3. Priority Aging
    """
    now = datetime.now().astimezone()

    # 0. Requeue Expired Jobs (Reaper)
    await requeue_expired_jobs(session)
    
    # 1. Advance Scheduled Jobs
    stmt_advance = (
        update(Job)
        .where(
            Job.status == JobStatus.SCHEDULED,
            Job.available_at <= now
        )
        .values(status=JobStatus.PENDING)
    )
    await session.execute(stmt_advance)
    
    # 2. Priority Aging
    stmt_aging = text("""
        UPDATE jobs 
        SET priority = priority + 1 
        WHERE status = :status 
        AND priority < 9 
        AND created_at < (NOW() - make_interval(mins := (priority + 1)))
    """)
    await session.execute(stmt_aging, {"status": JobStatus.PENDING})
    
    await session.commit()

async def run_metrics_tasks(session: AsyncSession):
    """
    Tasks that update observability gauges.
    Should run on ALL instances so /metrics is accurate everywhere.
    """
    from app.api.v1.metrics import QUEUE_DEPTH, JOBS_INFLIGHT
    from app.db.models import JobLease
    
    # Inflight
    q_inflight = select(func.count()).select_from(JobLease).where(JobLease.expires_at > func.now())
    inflight_count = (await session.execute(q_inflight)).scalar() or 0
    JOBS_INFLIGHT.set(inflight_count)
    
    # Queue Depth
    q_depth = (
        select(Job.tenant_id, func.count(Job.id))
        .where(Job.status == JobStatus.PENDING)
        .group_by(Job.tenant_id)
    )
    rows = (await session.execute(q_depth)).all()
    
    # Set what we find
    for t_id, count in rows:
        QUEUE_DEPTH.labels(tenant_id=t_id or "default").set(count)
        
    await session.commit()

async def run_ticker(session: AsyncSession):
    """
    Helper for running all leader and metrics tasks in a single call.
    Mainly used in synchronous verification scripts.
    """
    await run_leader_tasks(session)
    await run_metrics_tasks(session)


