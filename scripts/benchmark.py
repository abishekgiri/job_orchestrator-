import time
import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import hashlib
import hmac
import json
import uuid

# ... imports ...

import os

# ... imports ...

BASE_URL = os.getenv("JOB_API_URL", "http://localhost:8000/api/v1")
NUM_JOBS = 2000          
CONCURRENT_WORKERS = 20 

TENANT_ID = "bench"
API_KEY = os.getenv("JOB_API_KEY", "bench-key")

def setup_tenant():
    try:
        requests.post(f"{BASE_URL}/admin/tenants", json={
            "id": TENANT_ID,
            "name": "Benchmark Tenant",
            "api_key": API_KEY,
            "max_inflight": 2000
        })
    except Exception:
        pass

def create_jobs_batch(n):
    start = time.time()
    with requests.Session() as s:
        for i in range(n):
            resp = s.post(f"{BASE_URL}/jobs", 
                headers={"X-API-Key": API_KEY},
                json={"tenant_id": TENANT_ID, "payload": {"msg": f"bench_{i}"}}
            )
            if resp.status_code >= 400:
                print(f"Failed to create job {i}: {resp.text}", file=sys.stderr)
    duration = time.time() - start
    rate = n / duration if duration > 0 else 0
    return rate

def worker_routine(worker_id):
    processed = 0
    wid = f"bench_w_{worker_id}"
    with requests.Session() as s:
        while True:
            try:
                # Poll with Signature
                payload = {"worker_id": wid, "tenant_id": TENANT_ID}
                content = json.dumps(payload).encode("utf-8")
                sig = hmac.new(API_KEY.encode(), content, hashlib.sha256).hexdigest()
                
                resp = s.post(f"{BASE_URL}/workers/poll", 
                    data=content,
                    headers={
                        "X-Tenant-ID": TENANT_ID, 
                        "X-Worker-Signature": sig,
                        "Content-Type": "application/json"
                    }
                )
                
                if resp.status_code == 200:
                    data = resp.json()
                    if not data:
                        break
                elif resp.status_code == 204:
                    break
                else:
                    print(f"Poll Error: {resp.status_code}")
                    break
                
                job_id = data.get("job", {}).get("id")
                lease_token = data.get("lease_token")
                
                if not job_id:
                     break
                
                # Complete with Signature
                c_payload = {
                    "worker_id": wid,
                    "lease_token": lease_token,
                    "result": {"bench": "ok"}
                }
                c_content = json.dumps(c_payload).encode("utf-8")
                c_sig = hmac.new(API_KEY.encode(), c_content, hashlib.sha256).hexdigest()
                
                c_resp = s.post(f"{BASE_URL}/workers/{job_id}/complete", 
                    data=c_content,
                    headers={
                        "X-Tenant-ID": TENANT_ID, 
                        "X-Worker-Signature": c_sig,
                        "Content-Type": "application/json"
                    }
                )

                if c_resp.status_code != 200:
                     print(f"Complete Error: {c_resp.status_code}")
                
                processed += 1
                
            except Exception as e:
                print(f"Worker Error: {e}")
                break
    return processed

def run_benchmark():
    print(f"Using API: {BASE_URL}")
    setup_tenant()
    injection_rate = create_jobs_batch(NUM_JOBS)
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        futures = [executor.submit(worker_routine, i) for i in range(CONCURRENT_WORKERS)]
        
        total_processed = 0
        for f in as_completed(futures):
            total_processed += f.result()
            
    total_time = time.time() - start_time
    throughput = total_processed / total_time if total_time > 0 else 0
    
    print("\n" + "="*40)
    print(f"BENCHMARK RESULTS")
    print("="*40)
    print(f"Concurrent Workers:   {CONCURRENT_WORKERS}")
    print(f"Total Jobs Processed: {total_processed}")
    print(f"Processing Time:      {total_time:.2f} seconds")
    print(f"Throughput:           {throughput:.2f} jobs/second")
    print("="*40)

if __name__ == "__main__":
    run_benchmark()
