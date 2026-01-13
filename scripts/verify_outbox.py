import asyncio
import sys
import os
import uuid
from sqlalchemy import select, delete

# Add project root to path
sys.path.append(os.getcwd())

from app.db.session import AsyncSessionLocal
from app.db.models import Job, JobLease, Tenant, OutboxEvent, JobStatus
from app.commands.lease_job import lease_job
from app.commands.complete_job import complete_job


async def verify_outbox():
    print("Starting Outbox Verification...")
    
    async with AsyncSessionLocal() as session:
        # 1. Setup Data
        tenant_id = "outbox-verify-tenant"
        worker_id = "outbox-verifier"
        
        # Ensure tenant exists
        t = await session.get(Tenant, tenant_id)
        if not t:
            session.add(Tenant(id=tenant_id, name="Outbox Verify", api_key=f"outbox-key-{uuid.uuid4()}"))
            await session.commit()
        
        # Cleanup old jobs for this tenant to avoid leasing stale jobs
        await session.execute(delete(Job).where(Job.tenant_id == tenant_id))
        await session.commit()
            
        # Create Job
        job_id = uuid.uuid4()
        job = Job(id=job_id, tenant_id=tenant_id, payload={"msg": "verify"})
        session.add(job)
        await session.commit()
        
        print(f"Created Job {job_id}")
        
        # 2. Lease Job
        res = await lease_job(session, worker_id, tenant_id)
        assert res is not None, "Failed to lease job"
        job, lease = res
        print(f"Leased job with token {lease.lease_token}")
        
        # 3. Complete Job (Should trigger Outbox Event)
        result_payload = {"status": "ok"}
        await complete_job(session, job.id, result_payload, lease_token=lease.lease_token)
        await session.commit()
        print("Job completed.")
        
        stmt = select(OutboxEvent).where(OutboxEvent.payload['job_id'].astext == str(job_id))
        events = (await session.execute(stmt)).scalars().all()
        
        if not events:
             # Fallback check if ASTEXT is issue
             # Check in python
             events = [e for e in all_events if e.payload.get('job_id') == str(job_id)]
        
        assert len(events) == 1, f"Expected 1 outbox event, found {len(events)}"
        event = events[0]
        assert event.status == "PENDING", f"Expected PENDING, got {event.status}"
        assert event.event_type == "JOB_COMPLETED"
        print("Verified Outbox Event is PENDING.")
        
        # 5. Wait for Background Processor (running in Docker API)
        print("Waiting for background OutboxProcessor to publish...")
        
        for _ in range(10): # Wait up to 10-20s
            await asyncio.sleep(2)
            await session.refresh(event)
            if event.status == "PUBLISHED":
                break
        
        # 6. Verify Outbox Event (PUBLISHED)
        assert event.status == "PUBLISHED", f"Expected PUBLISHED, got {event.status}. Is the OutboxProcessor running in the API?"
        assert event.published_at is not None
        print("Verified Outbox Event is PUBLISHED.")
        
        # Cleanup
        await session.execute(delete(Job).where(Job.id == job_id))
        await session.execute(delete(OutboxEvent).where(OutboxEvent.id == event.id))
        await session.commit()
        print("Cleanup done.")
        
    print("SUCCESS: Outbox pattern verified.")

if __name__ == "__main__":
    asyncio.run(verify_outbox())
