import time
import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://localhost:8000/api/v1"
NUM_JOBS = 2000          # Total jobs to inject
CONCURRENT_WORKERS = 20  # Number of parallel threads simulating workers

def create_jobs_batch(n):
    start = time.time()
    
    # We could optimize injection with concurrency too, but sequential injection measures API write latency better
    # Let's simple injecting sequentially for now or use session for keep-alive
    with requests.Session() as s:
        for i in range(n):
            # Minimal payload for speed
            resp = s.post(f"{BASE_URL}/jobs", json={"tenant_id": "bench", "payload": {"msg": f"bench_{i}"}})
            if resp.status_code >= 400:
                print(f"Failed to create job {i}: {resp.text}", file=sys.stderr)
                
    duration = time.time() - start
    rate = n / duration if duration > 0 else 0
    return rate

def worker_routine(worker_id):
    processed = 0
    with requests.Session() as s:
        while True:
            try:
                # Poll
                # Note: Our poll endpoint returns empty list [] or null?
                # The script provided by user checks 204.
                # My implementation returns `null` (None) or `[]` depending on implementation.
                # `app/api/v1/workers.py`: `if not result: return None` -> 200 OK with "null" body?
                # Let's check: FastAPI return None usually means null JSON.
                # Previous test checked `if not jobs`.
                
                resp = s.post(f"{BASE_URL}/workers/poll", json={"worker_id": f"bench_w_{worker_id}", "tenant_id": "bench"})
                
                # Handling empty queue
                # API returns 200 OK with `null` or empty list.
                if resp.status_code == 200:
                    data = resp.json()
                    if not data: # None or []
                        break
                elif resp.status_code == 204:
                    break
                else:
                    print(f"Poll Error: {resp.status_code}")
                    break
                
                # Check data format
                # My poll returns `PollResponse` or list.
                # Implementation: `return PollResponse(...)` (Step 955)
                # It returns a single object.
                
                job_id = data.get("job", {}).get("id")
                lease_token = data.get("lease_token")
                
                if not job_id:
                     break
                
                # Complete
                c_resp = s.post(f"{BASE_URL}/workers/{job_id}/complete", json={
                    "worker_id": f"bench_w_{worker_id}",
                    "result": {"bench": "ok"},
                    "lease_token": lease_token
                })
                if c_resp.status_code != 200:
                     print(f"Complete Error: {c_resp.status_code}")
                
                processed += 1
                
            except Exception as e:
                print(f"Worker Error: {e}")
                break
    return processed

def run_benchmark():
    print(f"Using API: {BASE_URL}")
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
