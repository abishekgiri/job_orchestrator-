import asyncio
import sys
import logging
from datetime import datetime, timedelta
from uuid import uuid4

# Setup paths
import os
sys.path.append(os.getcwd())

from app.db.session import AsyncSessionLocal
from app.db.models import Job, JobLease, Tenant
from app.domain.states import JobStatus
from app.commands.lease_job import lease_job
from app.scheduler.ticker import run_ticker
from sqlalchemy import delete

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_verification():
    async with AsyncSessionLocal() as session:
        # cleanup
        await session.execute(delete(Job))
        await session.commit()
        
        tenant_id = f"test_tenant_{uuid4()}"
        session.add(Tenant(id=tenant_id, name="Test Tenant", api_key=f"prio-key-{uuid4()}"))
        await session.commit()
        
        logger.info("1. Creating Low Priority Job (Old)")
        # Manually insert to override created_at
        low_prio_job = Job(
            tenant_id=tenant_id,
            priority=0,
            status=JobStatus.PENDING,
            payload={"msg": "I am old but low priority"},
            created_at=datetime.now().astimezone() - timedelta(minutes=60), # 1 hour old
            available_at=datetime.now().astimezone() - timedelta(minutes=60)
        )
        session.add(low_prio_job)
        
        logger.info("2. Creating High Priority Job (New)")
        high_prio_job = Job(
            tenant_id=tenant_id,
            priority=5,
            status=JobStatus.PENDING,
            payload={"msg": "I am new and high priority"},
            available_at=datetime.now().astimezone()
        )
        session.add(high_prio_job)
        await session.commit()
        
        logger.info("3. Running Ticker (Aging)")
        # This should bump the low priority job. 
        # Logic: created_at < now - (priority+1) min. 
        # 60 mins age > (0+1) min -> Bump!
        # It bumps by 1. So 0 -> 1. 
        # Still less than 5.
        await run_ticker(session)
        await session.refresh(low_prio_job)
        logger.info(f"Low Prio Job new priority: {low_prio_job.priority}")
        
        assert low_prio_job.priority == 1, "Aging failed to bump priority"
        
        # Dispatch - High priority should win
        logger.info("4. Dispatching Lease (Expect High Priority)")
        # We pass tenant_id to be explicit since we created jobs for this tenant
        job, lease = await lease_job(session, worker_id="worker_1", tenant_id=tenant_id)
        logger.info(f"Leased Job ID: {job.id}, Priority: {job.priority}")
        
        assert job.id == high_prio_job.id, "High priority job should be leased first"
        
        # Dispatch again - Low priority (now bumped)
        logger.info("5. Dispatching Lease (Expect Low Priority)")
        job2, lease2 = await lease_job(session, worker_id="worker_1", tenant_id=tenant_id)
        if job2:
             logger.info(f"Leased Job ID: {job2.id}, Priority: {job2.priority}")
             assert job2.id == low_prio_job.id
        else:
             logger.error("Failed to lease second job")
             
        logger.info("Verification Passed: Priority & Aging works!")

if __name__ == "__main__":
    asyncio.run(run_verification())
