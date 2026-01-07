#!/usr/bin/env python3
import asyncio
import uuid
import httpx
from httpx import AsyncClient
from worker_sdk.client import WorkerClient

API_URL = "http://localhost:8000"

async def verify_lease_expiry():
    tenant_id = f"tenant-expiry-{uuid.uuid4()}"
    
    # 1. Create a job
    async with AsyncClient(base_url=API_URL) as client:
        print("1. Creating job...")
        resp = await client.post("/api/v1/jobs", json={
            "tenant_id": tenant_id,
            "payload": {"task": "crash_test"}
        })
        resp.raise_for_status()
        job = resp.json()
        job_id = job["id"]
        print(f"   Job created: {job_id}")

    # 2. Worker A leases the job with short duration (2 seconds)
    print("2. Worker A polling (lease_duration=2s)...")
    async with AsyncClient(base_url=API_URL) as http_client:
        poll_resp = await http_client.post("/api/v1/workers/poll", json={
            "worker_id": "worker-A",
            "tenant_id": tenant_id,
            "lease_duration_seconds": 2
        })
        data = poll_resp.json()
        if not data:
            print("   ERROR: Failed to lease job for Worker A")
            return
        
        print(f"   Worker A leased job: {data['job']['id']}")
        
    # 3. Worker A crashes (simulated by doing absolutely nothing)
    print("3. Worker A 'crashes' (closes without completing)")

    # 4. Wait for lease to expire
    print("4. Waiting 4s for lease to expire...")
    await asyncio.sleep(4)
    
    # 5. Trigger Requeue (simulate background reaper)
    print("5. Triggering reaper (requeue_expired)...")
    async with AsyncClient(base_url=API_URL) as client:
        resp = await client.post("/api/v1/admin/requeue_expired")
        resp.raise_for_status()
        count = resp.json()["requeued_count"]
        
        # Verify our specific job was reset
        job_resp = await client.get(f"/api/v1/jobs/{job_id}")
        job_status = job_resp.json()['status']
        requeued = "yes" if job_status == "pending" else "no"
        
        print(f"   Reaped {count} jobs (this test job requeued: {requeued})")
        
        if job_status != "pending":
             print(f"   WARNING: Our job {job_id} is {job_status}, expected pending.")

    # 6. Worker B polls
    client_b = WorkerClient(API_URL, worker_id="worker-B")
    print("6. Worker B polling...")
    # Polling loops until job found or timeout
    found = False
    for _ in range(3):
        job_data = await client_b.poll(tenant_id=tenant_id)
        if job_data:
            if job_data['job']['id'] == job_id:
                print(f"   Worker B got the job: {job_id}")
                found = True
                
                # 7. Complete job
                print("7. Worker B completing job...")
                await client_b.complete(job_data['job']['id'], job_data['lease_token'], {"result": "recovered"})
                print("   Job completed.")
                break
        await asyncio.sleep(0.5)
        
    await client_b.close()
    
    if found:
        # Verify status
        async with httpx.AsyncClient(base_url=API_URL) as client:
            resp = await client.get(f"/api/v1/jobs/{job_id}")
            status = resp.json()['status']
            if status == 'succeeded':
                print("SUCCESS: Job recovered and completed.")
            else:
                print(f"FAILURE: Job status is {status}")
    else:
        print("FAILURE: Worker B did not get the job.")

if __name__ == "__main__":
    asyncio.run(verify_lease_expiry())
