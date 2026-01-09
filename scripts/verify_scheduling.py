import asyncio
import sys
import logging
from datetime import datetime, timedelta
from uuid import uuid4

import os
sys.path.append(os.getcwd())

from app.db.session import AsyncSessionLocal
from app.db.models import Job, Tenant
from app.domain.states import JobStatus
from app.commands.lease_job import lease_job
from app.scheduler.ticker import run_ticker
from sqlalchemy import delete

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_verification():
    async with AsyncSessionLocal() as session:
        # cleanup
        await session.execute(delete(Job))
        await session.commit()
        
        tenant_id = f"test_tenant_{uuid4()}"
        session.add(Tenant(id=tenant_id, name="Test Scheduled Tenant"))
        await session.commit()
        
        logger.info("=== TEST 1: Scheduled Job ===")
        future_time = datetime.now().astimezone() + timedelta(seconds=2)
        scheduled_job = Job(
            tenant_id=tenant_id,
            status=JobStatus.SCHEDULED,
            available_at=future_time,
            payload={"msg": "future"}
        )
        session.add(scheduled_job)
        await session.commit()
        
        # 1. Try to lease - Should fail
        res = await lease_job(session, worker_id="w1")
        assert res is None, "Should not lease scheduled job"
        
        # 2. Wait
        logger.info("Waiting 3s...")
        await asyncio.sleep(3)
        
        # 3. Ticker
        await run_ticker(session)
        
        # 4. Lease - Should succeed
        res = await lease_job(session, worker_id="w1")
        assert res is not None, "Should lease after time passed + ticker"
        start_time = datetime.now().astimezone()
        logger.info("Scheduled job leased successfully.")
        
        
        logger.info("=== TEST 2: Cron Job ===")
        # Every minute: "* * * * *"
        # But for test we rely on croniter calculation
        cron_job = Job(
            tenant_id=tenant_id,
            status=JobStatus.PENDING,
            available_at=datetime.now().astimezone(),
            cron_schedule="* * * * *", # Every minute
            payload={"msg": "cron"}
        )
        session.add(cron_job)
        await session.commit()
        
        # Lease the cron job
        res = await lease_job(session, worker_id="w1")
        assert res is not None
        job1, lease1 = res
        logger.info(f"Leased Cron Instance 1: {job1.id}")
        
        # Verify next instance created
        # We need to refresh or query
        # Since lease_job commits (or we force commit in wrapper?) 
        # lease_job calls flush(). Implementation details...
        # The verification script needs session.commit() to see changes if lease_job didn't commit?
        # lease_job in this context handles its own transaction logic implicitly via flush, but we should commit to be safe.
        await session.commit()
        
        # Query for new SCHEDULED job
        from sqlalchemy import select
        stmt = select(Job).where(Job.status == JobStatus.SCHEDULED, Job.cron_schedule == "* * * * *")
        next_job = (await session.execute(stmt)).scalar_one_or_none()
        
        assert next_job is not None, "Next cron job should be created"
        logger.info(f"Next Cron Job Created: {next_job.id}, Available At: {next_job.available_at}")
        
        assert next_job.available_at > start_time, "Next run should be in future"
        
        logger.info("Verification Passed: Scheduled & Cron works!")

if __name__ == "__main__":
    asyncio.run(run_verification())
