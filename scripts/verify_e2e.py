#!/usr/bin/env python3
import asyncio
import uuid
import sys
import os

sys.path.append(os.path.join(os.getcwd(), '.lib'))
sys.path.append(os.getcwd())

import httpx
from worker_sdk.client import WorkerClient
from worker_sdk.runner import WorkerRunner

API_URL = "http://localhost:8000"

async def my_handler(payload: dict):
    print(f"Processing payload: {payload}")
    await asyncio.sleep(0.5) # simulate work
    return {"status": "processed", "input": payload}

async def run_worker():
    client = WorkerClient(API_URL, worker_id="test-worker-1")
    runner = WorkerRunner(client, my_handler)
    
    # Run for a bit then stop
    task = asyncio.create_task(runner.run())
    await asyncio.sleep(10)
    runner.running = False
    await task
    await client.close()

async def verify():
    # 1. Submit Job
    async with httpx.AsyncClient(base_url=API_URL) as client:
        tenant_id = "tenant-1"
        payload = {"msg": "hello world"}
        
        print("Submitting job...")
        resp = await client.post("/api/v1/jobs", json={
            "tenant_id": tenant_id,
            "payload": payload
        })
        if resp.status_code != 201:
            print(f"Failed to create job: {resp.text}")
            return
            
        job = resp.json()
        job_id = job["id"]
        print(f"Job created: {job_id}")
        
        # 2. Run Worker (in background or same loop)
        print("Starting worker...")
        await run_worker()
        
        # 3. Check status
        print("Checking status...")
        resp = await client.get(f"/api/v1/jobs/{job_id}")
        job = resp.json()
        print(f"Job status: {job['status']}")
        print(f"Result: {job.get('result')}")
        
        if job['status'] == 'succeeded':
            print("SUCCESS: Job completed successfully.")
        else:
            print("FAILURE: Job did not complete.")

if __name__ == "__main__":
    asyncio.run(verify())
