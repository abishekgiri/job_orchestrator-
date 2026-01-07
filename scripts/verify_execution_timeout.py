import asyncio
import sys
from uuid import UUID
import httpx
import time

API_URL = "http://localhost:8000/api/v1"

async def verify_execution_timeout():
    unique_tenant = f"verify_timeout_{int(time.time())}"
    print(f"1. Creating job with execution_timeout=2s for tenant {unique_tenant}...")
    async with httpx.AsyncClient(base_url=API_URL) as client:
        # Create Job
        resp = await client.post("/jobs", json={
            "tenant_id": unique_tenant,
            "payload": {"task": "sleepy_job"},
            "execution_timeout": 2 # 2 seconds timeout
        })
        resp.raise_for_status()
        job = resp.json()
        job_id = job["id"]
        print(f"   Job created: {job_id}")

        # 2. Worker Leases Job
        print("2. Worker polling...")
        resp = await client.post("/workers/poll", json={
            "worker_id": "worker-timeout-test",
            "tenant_id": unique_tenant
        })
        resp.raise_for_status()
        poll_result = resp.json()
        
        if not poll_result:
            print("FAILURE: No job leased")
            sys.exit(1)
            
        print(f"   Leased job: {poll_result.get('job', {}).get('id')}")
        lease_token = poll_result["lease_token"]
        
        # 3. Heartbeat immediately (should succeed)
        print("3. Heartbeating immediately (should succeed)...")
        resp = await client.post(f"/workers/{job_id}/heartbeat", json={
            "worker_id": "worker-timeout-test",
            "lease_token": lease_token,
            "extend_seconds": 60
        })
        resp.raise_for_status()
        print("   Heartbeat 1 successful.")
        
        # 4. Sleep to exceed timeout
        print("4. Sleeping 3s (timeout is 2s)...")
        await asyncio.sleep(3)
        
        # 5. Heartbeat again (should fail)
        print("5. Heartbeating again (should FAIL)...")
        resp = await client.post(f"/workers/{job_id}/heartbeat", json={
            "worker_id": "worker-timeout-test",
            "lease_token": lease_token,
            "extend_seconds": 60
        })
        
        if resp.status_code == 409:
            print(f"   SUCCESS: Heartbeat rejected with 409: {resp.text}")
        else:
            print(f"   FAILURE: Heartbeat unexpected status: {resp.status_code} {resp.text}")
            sys.exit(1)

        # 6. Verify job state (optional, or just rely on heartbeat rejection)
        # Eventually the reaper will clean it up. We won't wait for reaper here, 
        # proving the *running* worker is stopped is enough for this test.
        print("SUCCESS: Execution timeout enforced.")

if __name__ == "__main__":
    asyncio.run(verify_execution_timeout())
