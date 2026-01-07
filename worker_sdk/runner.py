import asyncio
import logging
import traceback
from typing import Callable, Any, Coroutine
from uuid import UUID

from worker_sdk.client import WorkerClient

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Coroutine[Any, Any, dict]]

class WorkerRunner:
    def __init__(self, client: WorkerClient, handler: Handler):
        self.client = client
        self.handler = handler
        self.running = False
        
    async def run(self):
        self.running = True
        logger.info(f"Worker {self.client.worker_id} started")
        
        while self.running:
            try:
                # Poll
                # Optional: Add backoff on empty poll to avoid hammering
                job_data = await self.client.poll()
                
                if not job_data:
                    await asyncio.sleep(1.0)
                    continue
                    
                logger.info(f"Leased job: {job_data['job']['id']}")
                await self.process_job(job_data)
                
            except Exception as e:
                logger.error(f"Error in runner loop: {e}")
                await asyncio.sleep(5.0)

    async def process_job(self, job_data: dict):
        job = job_data['job']
        job_id = UUID(job['id'])
        lease_token = UUID(job_data['lease_token'])
        
        # Start Heartbeat Task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(job_id, lease_token))
        
        try:
            # Run Handler
            result = await self.handler(job['payload'])
            
            # Complete
            await self.client.complete(job_id, lease_token, result)
            logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            # Fail
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Job {job_id} failed: {error_msg}")
            # traceback.print_exc()
            await self.client.fail(job_id, lease_token, error_msg)
            
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self, job_id: UUID, lease_token: UUID):
        try:
            while True:
                await asyncio.sleep(10) # Simple fixed interval
                logger.debug(f"Sending heartbeat for {job_id}")
                success = await self.client.heartbeat(job_id, lease_token)
                if not success:
                    logger.warning(f"Heartbeat failed for {job_id}, aborting execution?")
                    # In real worker, maybe we should cancel the task?
                    # For now just log.
                    break
        except asyncio.CancelledError:
            pass
