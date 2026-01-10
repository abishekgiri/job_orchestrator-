import asyncio
import httpx
from uuid import uuid4
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.db.models import Job, Tenant
from app.domain.states import JobStatus
from app.scheduler.service import SchedulerService

# Mock or real app runner?
# Easier to test via API if the app is running, OR test components directly.
# Since metrics are global, we can test components effectively.

async def verify_metrics():
    print("--- Verifying Metrics ---")
    
    # 1. Setup Tenant and Job
    async with AsyncSessionLocal() as session:
        t_id = str(uuid4())
        session.add(Tenant(id=t_id, name=f"metric-tenant-{uuid4()}", weight=10))
        await session.commit()
        
        # Create a Job
        j_id = uuid4()
        session.add(Job(id=j_id, tenant_id=t_id, status=JobStatus.PENDING, priority=1))
        await session.commit()
        
    print(f"Created Job {j_id} for Tenant {t_id}")
    
    # 2. Run Ticker to update QUEUE_DEPTH
    service = SchedulerService(interval=1)
    # Run one tick logic
    async with AsyncSessionLocal() as session:
        from app.scheduler.ticker import run_ticker
        # We need to simulate leadership?
        # Actually run_ticker does the metric update.
        await run_ticker(session)
        
    print("Ran Ticker (should update Queue Depth)")
    
    # 3. Lease Job to update Dispatch Count & Inflight
    from app.scheduler.dispatcher import dispatch_lease
    async with AsyncSessionLocal() as session:
        lease = await dispatch_lease(session, "worker-metrics", tenant_id=t_id)
        assert lease is not None
        await session.commit()
        print("Leased Job")
        
    # 4. Check Metrics via Endpoint (simulate request)
    # We can just check the registry directly or call the router function
    from app.api.v1.metrics import metrics
    from prometheus_client import generate_latest
    
    # Simulate endpoint call
    output = generate_latest().decode()
    
    print("\n--- Metrics Output ---")
    print(output)
    
    # Assertions
    if 'job_queue_depth' in output:
        print("✅ QUEUE_DEPTH metric found")
    else:
        print("❌ QUEUE_DEPTH metric missing for tenant")
        
    if 'job_dispatch_total' in output:
         print("✅ JOB_DISPATCH_COUNT metric found")
    else:
        print("❌ JOB_DISPATCH_COUNT metric missing")
        
    # Run ticker again to see inflight update
    async with AsyncSessionLocal() as session:
        from app.scheduler.ticker import run_ticker
        await run_ticker(session)
        
    output_after = generate_latest().decode()
    if 'jobs_inflight 1.0' in output_after:
        print("✅ JOBS_INFLIGHT metric correct (1.0)")
    else:
        print(f"❌ JOBS_INFLIGHT metric incorrect? Check output.")
        
    print("--- Verification Complete ---")

if __name__ == "__main__":
    asyncio.run(verify_metrics())
