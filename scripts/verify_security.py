import asyncio
import subprocess
import time
import sys
import os
import signal
import httpx
import uuid
import uuid as uuid_lib

sys.path.append(os.getcwd())

API_PORT = 8003
API_URL = f"http://localhost:{API_PORT}"

async def verify_security():
    # 1. Start Server
    print("Starting API Server...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(API_PORT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        # Wait for health
        async with httpx.AsyncClient() as client:
            start = time.time()
            ready = False
            while time.time() - start < 10:
                try:
                    resp = await client.get(f"{API_URL}/health")
                    if resp.status_code == 200:
                        ready = True
                        break
                except Exception:
                    await asyncio.sleep(0.5)
            
            if not ready:
                print("Failed to start API server")
                sys.exit(1)
                
            print("API Server Ready.")

            # 2. Setup Tenants & Keys
            tenant_a = "sec-tenant-A"
            key_a = "secret-key-A"
            tenant_b = "sec-tenant-B"
            key_b = "secret-key-B"
            
            from app.db.session import AsyncSessionLocal
            from app.db.models import Tenant
            from sqlalchemy import update, delete
            
            async with AsyncSessionLocal() as session:
                # Cleanup
                from app.db.models import Job
                await session.execute(delete(Job).where(Job.tenant_id.in_([tenant_a, tenant_b])))
                await session.execute(delete(Tenant).where(Tenant.id.in_([tenant_a, tenant_b])))
                await session.commit()
                
                # Create with API Keys directly
                session.add(Tenant(id=tenant_a, name="Tenant A", api_key=key_a))
                session.add(Tenant(id=tenant_b, name="Tenant B", api_key=key_b))
                await session.commit()
                print("Tenants created with keys.")

            # 3. Verify Job Creation Auth
            print("Testing Job Creation Auth...")
            
            # Case 1: Valid Key A -> Success
            resp = await client.post(
                f"{API_URL}/api/v1/jobs",
                headers={"X-API-Key": key_a},
                json={"tenant_id": tenant_a, "payload": {}}
            )
            assert resp.status_code == 201, f"Expected 201, got {resp.status_code}"
            
            # Case 2: No Key -> 403
            resp = await client.post(
                f"{API_URL}/api/v1/jobs",
                json={"tenant_id": tenant_a, "payload": {}}
            )
            assert resp.status_code == 403, f"Expected 403 (No Key), got {resp.status_code}"
            
            # Case 3: Key B for Tenant A -> 403 (Assuming get_current_tenant check)
            # Actually get_current_tenant returns Tenant B.
            # create_job logic: if payload.tenant_id != current_tenant.id -> 403.
            resp = await client.post(
                f"{API_URL}/api/v1/jobs",
                headers={"X-API-Key": key_b},
                json={"tenant_id": tenant_a, "payload": {}}
            )
            assert resp.status_code == 403, f"Expected 403 (Wrong Tenant), got {resp.status_code}"

            print("Job Creation Auth: PASS")
            
            # 4. Verify Worker Auth (Poll)
            print("Testing Worker Auth...")
            
            from worker_sdk import Worker
            from worker_sdk.client import WorkerClient
            
            async def verify_signature_flow(tid, key, expect_success=True):
                # Manually use Client to poll
                wc = WorkerClient(API_URL, "sec-worker", tenant_id=tid, api_key=key)
                res = await wc.poll()
                # If success, res is dict or None (if queue empty). If fail, None (but prints error).
                # We need to know if it failed AUTH.
                # WorkerClient swallows errors and returns None.
                # So we can't easily verify auth failure via SDK methods directly unless we inspect logs or modify test.
                # Let's use internal _post or check status manually.
                
                # Construct signed request manually to verify status code
                payload = {"worker_id": "sec-worker", "tenant_id": tid}
                import json, hmac, hashlib
                content = json.dumps(payload).encode("utf-8")
                sig = hmac.new(key.encode(), content, hashlib.sha256).hexdigest()
                
                r = await client.post(
                    f"{API_URL}/api/v1/workers/poll", 
                    content=content,
                    headers={
                        "X-Tenant-ID": tid,
                        "X-Worker-Signature": sig,
                        "Content-Type": "application/json"
                    }
                )
                if expect_success:
                    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
                else:
                    assert r.status_code in [401, 403], f"Expected 401/403, got {r.status_code}"

            # Valid
            await verify_signature_flow(tenant_a, key_a, True)
            
            # Invalid Signature (Wrong key)
            await verify_signature_flow(tenant_a, "wrong-key", False)
            
            # Missing Signature
            r = await client.post(f"{API_URL}/api/v1/workers/poll", json={"worker_id": "x"})
            assert r.status_code in [400, 401, 422], f"Expected 400/401/422, got {r.status_code}"
            
            print("Worker Auth: PASS")
            
    finally:
        print("Stopping API Server...")
        proc.terminate()
        try:
            outs, errs = proc.communicate(timeout=5)
            if outs: print(outs.decode())
            if errs: print(errs.decode())
        except:
            proc.kill()

if __name__ == "__main__":
    asyncio.run(verify_security())
