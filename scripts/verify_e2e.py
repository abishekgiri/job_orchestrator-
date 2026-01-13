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

async def run_worker(tenant_id, api_key):
    client = WorkerClient(API_URL, worker_id="test-worker-1", tenant_id=tenant_id, api_key=api_key)
    # Runner handles loop
    runner = WorkerRunner(client, my_handler)
    
    # Run for a bit then stop
    task = asyncio.create_task(runner.run())
    await asyncio.sleep(10)
    runner.running = False
    await task
    await client.close()

async def verify():
    # 0. Wait for API Readiness
    print("Waiting for API to be ready...")
    async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as check_client:
        for i in range(30):
            try:
                resp = await check_client.get("/health")
                if resp.status_code == 200:
                    print("API is ready!")
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            print("API failed to become ready.")

    # 1. Setup Tenant
    tenant_id = "tenant-e2e"
    api_key = "secret-e2e"
    
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0) as client:
        # Create tenant via Admin API (which we updated to take api_key)
        try:
            await client.post("/api/v1/admin/tenants", json={
                "id": tenant_id, 
                "name": "E2E Test Tenant", 
                "api_key": api_key
            })
        except Exception:
            pass # Maybe exists

        # 2. Submit Job
        payload = {"msg": "hello world"}
        
        print("Submitting job...")
        resp = await client.post("/api/v1/jobs", 
            headers={"X-API-Key": api_key},
            json={
                "tenant_id": tenant_id,
                "payload": payload
            }
        )
        if resp.status_code != 201:
            print(f"Failed to create job: {resp.text}")
            return
            
        job = resp.json()
        job_id = job["id"]
        print(f"Job created: {job_id}")
        
        # 3. Run Worker
        print("Starting worker...")
        await run_worker(tenant_id, api_key)
        
        # 4. Check status
        print("Checking status...")
        resp = await client.get(f"/api/v1/jobs/{job_id}", headers={"X-API-Key": api_key})
        if resp.status_code != 200:
             print(f"Failed to get job: {resp.status_code}")
             return

        job = resp.json()
        print(f"Job status: {job['status']}")
        print(f"Result: {job.get('result')}")
        
        if job['status'] == 'succeeded':
            print("SUCCESS: Job completed successfully.")
        else:
            print("FAILURE: Job did not complete.")

if __name__ == "__main__":
    asyncio.run(verify())
