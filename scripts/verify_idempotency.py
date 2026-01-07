#!/usr/bin/env python3
import asyncio
import uuid
import httpx
from worker_sdk.client import WorkerClient

API_URL = "http://localhost:8000"

async def verify_idempotency():
    tenant_id = f"tenant-idempotency-{uuid.uuid4()}"
    idempotency_key = f"req-{uuid.uuid4()}"
    
    # 1. Create a job
    async with httpx.AsyncClient(base_url=API_URL) as client:
        print("1. Creating job...")
        resp = await client.post("/api/v1/jobs", json={
            "tenant_id": tenant_id,
            "payload": {"task": "idempotency_test"}
        })
        resp.raise_for_status()
        job_id = resp.json()["id"]
        print(f"   Job created: {job_id}")

    # 2. Worker leases the job
    print("2. Worker polling...")
    async with httpx.AsyncClient(base_url=API_URL) as http_client:
        poll_resp = await http_client.post("/api/v1/workers/poll", json={
            "worker_id": "worker-idempotent",
            "tenant_id": tenant_id
        })
        job_data = poll_resp.json()
        if not job_data:
            print("   FAILURE: No job leased")
            return
        
        lease_token = job_data['lease_token']
        print(f"   Leased job with token: {lease_token}")

    # 3. Complete with Idempotency Key (Attempt 1)
    print(f"3. Completing job with Key: {idempotency_key}...")
    async with httpx.AsyncClient(base_url=API_URL) as client:
        resp = await client.post(f"/api/v1/workers/{job_id}/complete", json={
            "worker_id": "worker-idempotent",
            "lease_token": lease_token,
            "result": {"run": 1},
            "idempotency_key": idempotency_key
        })
        resp.raise_for_status()
        print("   Success (Run 1)")

    # 4. Complete with SAME Idempotency Key (Attempt 2 - Replay)
    print(f"4. Replaying completion with SAME Key: {idempotency_key}...")
    async with httpx.AsyncClient(base_url=API_URL) as client:
        resp = await client.post(f"/api/v1/workers/{job_id}/complete", json={
            "worker_id": "worker-idempotent",
            "lease_token": lease_token,
            "result": {"run": 2}, # Different payload, should be ignored
            "idempotency_key": idempotency_key
        })
        resp.raise_for_status()
        print("   Success (Run 2 - Replay)")

    # 5. Verify Result is from Run 1
    print("5. Verifying final job result...")
    async with httpx.AsyncClient(base_url=API_URL) as client:
        resp = await client.get(f"/api/v1/jobs/{job_id}")
        job = resp.json()
        result = job.get("result")
        print(f"   Result: {result}")
        
        if result.get("run") == 1:
            print("SUCCESS: Result matches Run 1 (Idempotent)")
        else:
            print(f"FAILURE: Result does not match Run 1. Got: {result}")

if __name__ == "__main__":
    asyncio.run(verify_idempotency())
