#!/usr/bin/env python3
import asyncio
import httpx
import uuid
from random import randint
import time

API_URL = "http://localhost:8000"

async def attempt_lease(worker_id, tenant_id):
    async with httpx.AsyncClient(base_url=API_URL) as client:
        try:
            resp = await client.post("/api/v1/workers/poll", json={
                "worker_id": worker_id,
                "tenant_id": tenant_id
            }, timeout=5.0)
            if resp.status_code == 200:
                result = resp.json()
                result['worker_id'] = worker_id
                return result
        except Exception:
            pass
    return None

async def verify_no_double_claim():
    tenant_id = f"tenant-concurrency-{uuid.uuid4()}"
    # 1. Create 1 job
    async with httpx.AsyncClient(base_url=API_URL) as client:
        print("1. Creating 1 job...")
        resp = await client.post("/api/v1/jobs", json={
            "tenant_id": tenant_id,
            "payload": {"task": "concurrency_test"}
        })
        resp.raise_for_status()
        job_id = resp.json()["id"]
        print(f"   Job created: {job_id}")

    # 2. Spawn 20 concurrent workers trying to lease
    print("2. Spawning 20 concurrent lease attempts...")
    tasks = []
    for i in range(20):
        tasks.append(attempt_lease(f"worker-{i}", tenant_id))
    
    # Run them all at once
    results = await asyncio.gather(*tasks)
    
    # 3. Analyze results
    leases = [r for r in results if r is not None]
    print(f"3. Results: {len(leases)} successful leases.")
    
    if len(leases) == 1:
        if leases[0]['job']['id'] != job_id:
             print(f"FAILURE: Worker claimed WRONG job: {leases[0]['job']['id']}")
        else:
             print("SUCCESS: Exactly one worker claimed the job.")
             print(f"   Winner: {leases[0]['worker_id']} (Token: {leases[0]['lease_token']})")
    elif len(leases) == 0:
        print("FAILURE: No one claimed the job (unexpected).")
    else:
        print(f"FAILURE: {len(leases)} workers claimed the job! Double claim detected.")
        for l in leases:
            print(f"   - Token: {l['lease_token']}")

if __name__ == "__main__":
    asyncio.run(verify_no_double_claim())
