import asyncio
import sys
from uuid import UUID
import httpx
import time

API_URL = "http://localhost:8000/api/v1"

import hashlib
import hmac
import json
# ... imports ...

async def verify_execution_timeout():
    unique_tenant = f"verify_timeout_{int(time.time())}"
    api_key = f"key-timeout_{int(time.time())}"
    
    # 0. Create Tenant
    async with httpx.AsyncClient(base_url=API_URL) as client:
        try:
             await client.post("/admin/tenants", json={
                "id": unique_tenant, 
                "name": "Timeout Tenant", 
                "api_key": api_key,
                "max_inflight": 10
            })
        except Exception:
             pass 

    print(f"1. Creating job with execution_timeout=2s for tenant {unique_tenant}...")
    async with httpx.AsyncClient(base_url=API_URL) as client:
        # Create Job
        resp = await client.post("/jobs", 
            headers={"X-API-Key": api_key},
            json={
                "tenant_id": unique_tenant,
                "payload": {"task": "sleepy_job"},
                "execution_timeout": 2 # 2 seconds timeout
            }
        )
        resp.raise_for_status()
        job = resp.json()
        job_id = job["id"]
        print(f"   Job created: {job_id}")

        # 2. Worker Leases Job
        print("2. Worker polling...")
        # Manual Sign Poll
        payload = {"worker_id": "worker-timeout-test", "tenant_id": unique_tenant}
        content = json.dumps(payload).encode("utf-8")
        sig = hmac.new(api_key.encode(), content, hashlib.sha256).hexdigest()

        resp = await client.post("/workers/poll", 
            content=content,
            headers={
                "X-Tenant-ID": unique_tenant,
                "X-Worker-Signature": sig,
                "Content-Type": "application/json"
            }
        )
        resp.raise_for_status()
        poll_result = resp.json()
        
        if not poll_result:
            print("FAILURE: No job leased")
            sys.exit(1)
            
        print(f"   Leased job: {poll_result.get('job', {}).get('id')}")
        lease_token = poll_result["lease_token"]
        
        # 3. Heartbeat immediately (should succeed)
        print("3. Heartbeating immediately (should succeed)...")
        # Manual Sign Heartbeat
        h_payload = {
            "worker_id": "worker-timeout-test",
            "lease_token": lease_token,
            "extend_seconds": 60
        }
        h_content = json.dumps(h_payload).encode("utf-8")
        h_sig = hmac.new(api_key.encode(), h_content, hashlib.sha256).hexdigest()
        
        resp = await client.post(f"/workers/{job_id}/heartbeat", 
            content=h_content,
            headers={
                "X-Tenant-ID": unique_tenant,
                "X-Worker-Signature": h_sig,
                "Content-Type": "application/json"
            }
        )
        resp.raise_for_status()
        print("   Heartbeat 1 successful.")
        
        # 4. Sleep to exceed timeout
        print("4. Sleeping 3s (timeout is 2s)...")
        await asyncio.sleep(3)
        
        # 5. Heartbeat again (should fail)
        print("5. Heartbeating again (should FAIL)...")
        # Reuse payload/sig since nothing changed in payload or we recompute to be safe if content changed?
        # Content is same, sig is same.
        
        resp = await client.post(f"/workers/{job_id}/heartbeat", 
            content=h_content,
            headers={
                "X-Tenant-ID": unique_tenant,
                "X-Worker-Signature": h_sig,
                "Content-Type": "application/json"
            }
        )
        
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
