import asyncio
import subprocess
import time
import sys
import os
import signal
import httpx
import uuid

import logging

logging.basicConfig(level=logging.INFO)

sys.path.append(os.getcwd())

from worker_sdk import Worker

API_PORT = 8002
API_URL = f"http://localhost:{API_PORT}"

async def verify_worker():
    # 1. Start Server
    print("Starting API Server...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(API_PORT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
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
            proc.terminate()
            stdout, stderr = proc.communicate()
            print(stderr.decode())
            sys.exit(1)
            
    print("API Server Ready.")
    
    # DB Cleanup using direct session to ensure clean state
    from app.db.session import AsyncSessionLocal
    from app.db.models import Job
    from sqlalchemy import delete
    
    async with AsyncSessionLocal() as session:
        print("Cleaning up old jobs for tenant...")
        await session.execute(delete(Job).where(Job.tenant_id == "worker-sdk-test"))
        await session.commit()
    
    try:
        async with httpx.AsyncClient() as client:
            # 2. Setup
            tenant_id = "worker-sdk-test"
            api_key = "sdk-verifier-key"
            try:
                await client.post(f"{API_URL}/api/v1/admin/tenants", json={
                    "id": tenant_id, 
                    "name": "Worker SDK Test", 
                    "api_key": api_key,
                    "weight": 10, 
                    "max_inflight": 10
                })
            except Exception:
                pass 
                
            # Create Job
            internal_job_id = str(uuid.uuid4())
            resp = await client.post(f"{API_URL}/api/v1/jobs", 
                headers={"X-API-Key": api_key},
                json={
                    "tenant_id": tenant_id,
                    "payload": {"msg": "hello", "job_id": internal_job_id} 
                }
            )
            if resp.status_code != 201:
                print(f"Failed to create job: {resp.status_code} {resp.text}")
                sys.exit(1)
            
            data = resp.json()
            real_job_id = data["id"]
            print(f"Submitted Job {real_job_id} (Internal Payload ID: {internal_job_id})")
            
            # Verify existence immediately
            resp = await client.get(f"{API_URL}/api/v1/jobs/{real_job_id}", headers={"X-API-Key": api_key})
            if resp.status_code != 200:
                 print(f"Immediate GET failed: {resp.status_code}")
                 sys.exit(1)
            
            # 3. Define Handler and Middleware
            processed_event = asyncio.Event()
            
            async def my_middleware(payload, next_handler):
                payload["middleware_active"] = True
                return await next_handler(payload)
                
            async def my_handler(payload):
                if not payload.get("middleware_active"):
                    raise Exception("Middleware missing!")
                
                if payload.get("job_id") == internal_job_id:
                     print(f"Handler processing target job: {payload}")
                     processed_event.set()
                else:
                     print(f"Handler ignoring non-target job: {payload}")
                     
                return {"status": "ok", "processed": True}
                
            # 4. Start Worker
            worker = Worker(API_URL, "sdk-verifier", my_handler, tenant_id=tenant_id, api_key=api_key)
            worker.add_middleware(my_middleware)
            
            # Run worker in background task
            worker_task = asyncio.create_task(worker.run())
            
            # Wait for processing
            try:
                await asyncio.wait_for(processed_event.wait(), timeout=10.0)
                print("Job Processed Successfully!")
            except asyncio.TimeoutError:
                print("Timeout waiting for job processing")
                proc.terminate()
                worker.stop()
                await worker_task
                sys.exit(1)
                
            # Stop worker
            worker.stop()
            await worker_task
            
            # Verify Job Status in API
            resp = await client.get(f"{API_URL}/api/v1/jobs/{real_job_id}", headers={"X-API-Key": api_key})
            data = resp.json()
            assert data["status"] == "succeeded"
            assert data["result"]["processed"] is True
            print("Verified Job Status: SUCCEEDED")
            
    finally:
        print("Stopping API Server...")
        proc.terminate()
        try:
            outs, errs = proc.communicate(timeout=5)
            if outs:
                print(f"Server STDOUT:\n{outs.decode()}")
            if errs:
                print(f"Server STDERR:\n{errs.decode()}")
        except Exception:
            proc.kill()

if __name__ == "__main__":
    try:
        asyncio.run(verify_worker())
    except KeyboardInterrupt:
        pass
