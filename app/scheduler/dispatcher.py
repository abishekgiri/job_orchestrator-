from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.db.models import Job, JobLease
from app.commands.lease_job import lease_job

async def dispatch_lease(
    session: AsyncSession,
    worker_id: str,
    tenant_id: Optional[str] = None,
    lease_duration: Optional[int] = None
) -> Optional[tuple[Job, JobLease]]:
    """
    Dispatcher facade. 
    In the future, this can include rate limiting checks (e.g. Redis check)
    before calling the DB lease command.
    """
    # TODO: Add Rate Limiting here
    return await lease_job(session, worker_id, tenant_id, lease_duration=lease_duration)
