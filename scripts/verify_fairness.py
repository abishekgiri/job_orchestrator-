#!/usr/bin/env python3
import asyncio
import sys
import os
import httpx
import time

sys.path.append(os.path.join(os.getcwd(), '.lib'))
sys.path.append(os.getcwd())

from worker_sdk.client import WorkerClient
from worker_sdk.runner import WorkerRunner

API_URL = "http://localhost:8000"

async def create_tenants():
    async with httpx.AsyncClient(base_url=API_URL) as client:
        # Create Tenant A (Heavy user)
        # Weight 1, limit 100
        try:
            await client.post("/api/v1/admin/tenants", json={
                "id": "tenant-A", "name": "Heavy Tenant", "weight": 1, "max_inflight": 100
            })
        except Exception:
            pass # Ignore if exists

        # Create Tenant B (Light user)
        # Weight 1, limit 100
        try:
            await client.post("/api/v1/admin/tenants", json={
                "id": "tenant-B", "name": "Light Tenant", "weight": 1, "max_inflight": 100
            })
        except Exception:
            pass

async def submit_jobs(tenant_id, count):
    async with httpx.AsyncClient(base_url=API_URL) as client:
        jobs = []
        # Batch submit simulation
        for i in range(count):
            resp = await client.post("/api/v1/jobs", json={
                "tenant_id": tenant_id,
                "payload": {"msg": f"Job {i}"}
            })
            if resp.status_code == 201:
                jobs.append(resp.json()['id'])
        return jobs

async def worker_handler(payload):
    # Simulate work
    await asyncio.sleep(0.1)
    return {"status": "done"}

async def run_fairness_test():
    print("Creating tenants...")
    await create_tenants()

    print("Submitting 100 jobs for Tenant A (Noisy)...")
    jobs_a = await submit_jobs("tenant-A", 100)
    
    print("Submitting 10 jobs for Tenant B (Victim)...")
    jobs_b = await submit_jobs("tenant-B", 10)
    
    print(f"Submitted {len(jobs_a)} A jobs, {len(jobs_b)} B jobs.")
    
    # Run workers
    # Start 2 concurrent workers to simulate meaningful throughput
    client1 = WorkerClient(API_URL, worker_id="fair-worker-1")
    runner1 = WorkerRunner(client1, worker_handler)
    
    client2 = WorkerClient(API_URL, worker_id="fair-worker-2")
    runner2 = WorkerRunner(client2, worker_handler)
    
    task1 = asyncio.create_task(runner1.run())
    task2 = asyncio.create_task(runner2.run())
    
    # Monitor B jobs
    start_time = time.time()
    
    print("Monitoring Tenant B jobs...")
    async with httpx.AsyncClient(base_url=API_URL) as http_client:
        while True:
            done_b = 0
            for jid in jobs_b:
                resp = await http_client.get(f"/api/v1/jobs/{jid}")
                if resp.status_code == 200 and resp.json()['status'] == 'succeeded':
                    done_b += 1
            
            elapsed = time.time() - start_time
            print(f"[{elapsed:.1f}s] Tenant B progress: {done_b}/10")
            
            if done_b == 10:
                print(f"SUCCESS: All Tenant B jobs finished in {elapsed:.2f}s!")
                break
                
            if elapsed > 60:
                print("FAILURE: Timeout waiting for B.")
                break
                
            await asyncio.sleep(2)

    runner1.running = False
    runner2.running = False
    await task1
    await task2
    await client1.close()
    await client2.close()

if __name__ == "__main__":
    asyncio.run(run_fairness_test())
