#!/usr/bin/env python3
import asyncio
import httpx
from datetime import datetime, timedelta
from worker_sdk.client import WorkerClient

import uuid

API_URL = "http://localhost:8000"

async def verify_retry_dlq():
    tenant_id = f"tenant-retry-{uuid.uuid4()}"
    api_key = f"key-retry-{uuid.uuid4()}"
    
    # 0. Setup Tenant
    async with httpx.AsyncClient(base_url=API_URL) as client:
        try:
             await client.post("/api/v1/admin/tenants", json={
                "id": tenant_id, 
                "name": "Retry Tenant", 
                "api_key": api_key,
                "max_inflight": 10
            })
        except Exception:
             pass

    # 1. Create a job with max_attempts=2
    async with httpx.AsyncClient(base_url=API_URL) as client:
        print("1. Creating job with max_attempts=2...")
        resp = await client.post("/api/v1/jobs", 
            headers={"X-API-Key": api_key},
            json={
                "tenant_id": tenant_id,
                "max_attempts": 2,
                "payload": {"task": "fail_test"}
            }
        )
        resp.raise_for_status()
        job_id = resp.json()["id"]
        print(f"   Job created: {job_id}")

    client_a = WorkerClient(API_URL, worker_id="worker-fail-test", tenant_id=tenant_id, api_key=api_key)
    
    # 2. Attempt 1
    print("2. Worker attempting lease (Attempt 1)...")
    job_data = await client_a.poll() # Uses tenant_id/api_key from init
    if not job_data or job_data['job']['id'] != job_id:
        print("   FAILURE: Could not lease job for attempt 1")
        return
        
    print(f"   Leased job for attempt 1. Failing it...")
    await client_a.fail(job_id, job_data['lease_token'], "Simulated failure 1")
    
    # 3. Verify state
    async with httpx.AsyncClient(base_url=API_URL) as client:
        resp = await client.get(f"/api/v1/jobs/{job_id}", headers={"X-API-Key": api_key})
        job = resp.json()
        print(f"   Status after fail 1: {job['status']}")
        print(f"   Attempts: {job['attempts']}")
        # available_at should be in the future
        # default base_delay=10. So it should be ~10s in future.
        
        if job['status'] != 'pending': # or failed_retryable, depending on enum
             # In fail_job, we set status=PENDING for retries.
             print(f"   FAILURE: Status should be pending/retryable, got {job['status']}")
        
        if job['attempts'] != 1:
             print(f"   FAILURE: Attempts should be 1, got {job['attempts']}")

    # 4. Attempt 2
    # We need to wait for backoff. Default is 10s * 2^1 = 20s + jitter.
    # Let's wait 25s to be safe.
    print("4. Waiting 25s for backoff...")
    await asyncio.sleep(25)
    
    print("5. Worker attempting lease (Attempt 2)...")
    job_data_2 = await client_a.poll()
    if not job_data_2 or job_data_2['job']['id'] != job_id:
         # Retry might need more time or polling?
         # If not found, maybe available_at is further out?
         # Let's try polling a few times
         print("   Job not immediately available. Polling...")
         for _ in range(5):
             job_data_2 = await client_a.poll()
             if job_data_2 and job_data_2['job']['id'] == job_id:
                 break
             await asyncio.sleep(1)
             
    if not job_data_2 or job_data_2['job']['id'] != job_id:
        print("   FAILURE: Could not lease job for attempt 2 (after backoff)")
        # Debug info
        async with httpx.AsyncClient(base_url=API_URL) as client:
             r = await client.get(f"/api/v1/jobs/{job_id}", headers={"X-API-Key": api_key})
             print(f"   Debug Status: {r.json()}")
        return

    print("   Leased job for attempt 2. Failing it...")
    await client_a.fail(job_id, job_data_2['lease_token'], "Simulated failure 2")

    # 5. Verify DLQ
    async with httpx.AsyncClient(base_url=API_URL) as client:
        resp = await client.get(f"/api/v1/jobs/{job_id}", headers={"X-API-Key": api_key})
        job = resp.json()
        print(f"   Status after fail 2: {job['status']}")
        
        # 'dlq' or 'failed_final'
        if job['status'] in ['dlq', 'failed_final']:
            print("SUCCESS: Job routed to DLQ/Failed Final.")
        else:
            print(f"FAILURE: Job status is {job['status']}, expected dlq/failed_final")

    await client_a.close()

if __name__ == "__main__":
    asyncio.run(verify_retry_dlq())
