import asyncio
import logging
import multiprocessing
import time
import os
import signal
import sys
from app.scheduler.service import SchedulerService

# Configure logging to stdout so we can capture it
logging.basicConfig(level=logging.INFO, format='[%(process)d] %(message)s')
logger = logging.getLogger("verifier")

def run_service(name: str):
    """
    Runs the scheduler service in a loop.
    """
    # We need to setup event loop for this process
    async def _run():
        print(f"[{name}] Starting...")
        service = SchedulerService(interval=1)
        await service.start()
        
        # Keep running until killed
        while True:
            await asyncio.sleep(1)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass

def main():
    print("--- Verifying Leader Election ---")
    
    processes = []
    for i in range(3):
        p = multiprocessing.Process(target=run_service, args=(f"Worker-{i}",))
        p.start()
        processes.append(p)
        print(f"Started Worker-{i} (PID: {p.pid})")
        
    print("Running for 5 seconds...")
    time.sleep(5)
    
    # Kill workers one by one to ensure we hit the leader
    for i, p in enumerate(processes):
        print(f"\n!!! Killing Worker-{i} (PID: {p.pid}) !!!")
        p.terminate()
        p.join()
        
        if i < len(processes) - 1:
            print("Waiting 5 seconds for failover...")
            time.sleep(5)
            
    print("--- Verification Complete ---")
    print("Check logs above. You should see 'Acquired leadership' initially by one, and then by another after kill.")

if __name__ == "__main__":
    main()
