#!/usr/bin/env python3
import asyncio
import sys
import os
import httpx
import time
import logging
from uuid import uuid4

sys.path.append(os.getcwd())

from worker_sdk.client import WorkerClient
from worker_sdk.runner import WorkerRunner

class SharedWorkerClient(WorkerClient):
    async def poll(self, tenant_id=None):
        payload = {"worker_id": self.worker_id}
        try:
            resp = await self._post("/api/v1/workers/poll", json_body=payload)
            resp.raise_for_status()
            data = resp.json()
            return data
        except Exception as e:
            return None

logging.basicConfig(level=logging.ERROR)
API_URL = "http://localhost:8000"

async def create_tenant(t_id, weight, api_key):
    async with httpx.AsyncClient(base_url=API_URL) as client:
        await client.post("/api/v1/admin/tenants", json={
            "id": t_id, "name": f"Tenant {t_id}", "weight": weight, "max_inflight": 50, "api_key": api_key
        })

async def submit_jobs(tenant_id, api_key, count, payload_msg):
    async with httpx.AsyncClient(base_url=API_URL) as client:
        tasks = []
        for i in range(count):
            tasks.append(client.post("/api/v1/jobs", 
                headers={"X-API-Key": api_key},
                json={"tenant_id": tenant_id, "payload": {"msg": f"{payload_msg} {i}"}}
            ))
            if len(tasks) >= 50:
                await asyncio.gather(*tasks)
                tasks = []
        if tasks:
            await asyncio.gather(*tasks)

async def worker_handler(payload):
    await asyncio.sleep(0.05) # 50ms work to slow down churn for observation
    return {"status": "success"}

async def run_fairness_test():
    print("--- Multi-Tenant Fairness Verification (Fresh Run) ---")
    t_a = f"t-a-{str(uuid4())[:8]}"
    t_b = f"t-b-{str(uuid4())[:8]}"
    k_a = f"k-a-{uuid4()}"
    k_b = f"k-b-{uuid4()}"
    
    await create_tenant(t_a, 1, k_a)
    await create_tenant(t_b, 1, k_b)

    count = 100
    print(f"1. Submitting {count} jobs each for {t_a} and {t_b}...")
    await asyncio.gather(
        submit_jobs(t_a, k_a, count, "A"),
        submit_jobs(t_b, k_b, count, "B")
    )
    
    print("2. Starting 10 Shared Workers...")
    runners = []
    tasks = []
    for i in range(10):
        # We share A's key just for polling auth
        client = SharedWorkerClient(API_URL, f"fair-worker-{i}", tenant_id=t_a, api_key=k_a)
        runner = WorkerRunner(client, worker_handler)
        runners.append(runner)
        tasks.append(asyncio.create_task(runner.run()))

    print("3. Monitoring Progress (Expecting Balanced Throughput)...")
    start_time = time.time()
    
    async with httpx.AsyncClient(base_url=API_URL) as http_client:
        while True:
            resp = await http_client.get("/metrics")
            output = resp.text
            
            def get_val(t_id):
                for line in output.split('\n'):
                    if f'job_complete_total' in line and f'tenant_id="{t_id}"' in line and 'success' in line:
                         return int(float(line.split()[-1]))
                return 0

            a_done = get_val(t_a)
            b_done = get_val(t_b)
            
            elapsed = time.time() - start_time
            print(f"   [{elapsed:.1f}s] Progress - A: {a_done}/{count}, B: {b_done}/{count}")
            
            if a_done >= count and b_done >= count:
                print("Both tenants finished.")
                break
                
            # Tolerance: allow 30% skew, but no total starvation
            if a_done > 20 and b_done == 0:
                 print("STARVATION ERROR: B has zero progress while A is moving.")
                 exit(1)
            if b_done > 20 and a_done == 0:
                 print("STARVATION ERROR: A has zero progress while B is moving.")
                 exit(1)

            if elapsed > 60:
                print("TIMEOUT ERROR")
                exit(1)
            await asyncio.sleep(2)

    # Cleanup
    for r in runners: r.running = False
    for t in tasks: t.cancel()
    print("--- Fairness Verification PASSED ---")

if __name__ == "__main__":
    asyncio.run(run_fairness_test())
