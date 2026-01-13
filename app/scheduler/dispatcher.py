import random
from datetime import datetime
from typing import Optional
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobLease, Tenant
from app.commands.lease_job import lease_job
from app.settings import settings

async def dispatch_lease(
    session: AsyncSession,
    worker_id: str,
    tenant_id: Optional[str] = None,
    lease_duration: Optional[int] = None
) -> Optional[tuple[Job, JobLease]]:
    """
    Dispatcher facade with Fairness and Rate Limiting.
    """
    # 1. Global Concurrency Check
    # We count only Valid (non-expired) leases
    # Ideally use a Gauge or fast counter in Redis, but DB count is okay for low scale.
    from app.api.v1.metrics import JOBS_INFLIGHT
    
    # We can check the gauge directly? Or query DB?
    # Trusting DB is safer for "Rate Limiting".
    # But for invalidation we rely on query.
    q_global = select(func.count()).select_from(JobLease).where(JobLease.expires_at > func.now())
    global_count = (await session.execute(q_global)).scalar() or 0
    
    if global_count >= settings.GLOBAL_CONCURRENCY_CAP:
        return None

    # 2. Delegate to lease_job (which handles Fairness and Priority)
    result = await lease_job(session, worker_id, tenant_id, lease_duration=lease_duration)
    
    return result
