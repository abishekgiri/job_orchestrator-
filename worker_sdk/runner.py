import asyncio
import logging
from typing import Any, Callable, Coroutine
from uuid import UUID

from worker_sdk.client import WorkerClient

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Coroutine[Any, Any, dict]]

class WorkerRunner:
    def __init__(self, client: WorkerClient, handler: Handler):
        self.client = client
        self.handler = handler
        self.running = False
        self._shutdown_event = asyncio.Event()
        
    async def run(self):
        self.running = True
        self._shutdown_event.clear()
        logger.info(f"Worker {self.client.worker_id} started")

        try:
            while self.running:
                try:
                    job_data = await self.client.poll()

                    if not job_data:
                        try:
                            await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                        except asyncio.TimeoutError:
                            pass
                        continue

                    logger.info(f"Leased job: {job_data['job']['id']}")
                    await self.process_job(job_data)

                except Exception as e:
                    logger.exception("Error in runner loop for worker %s: %s", self.client.worker_id, e)
                    try:
                        await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
        finally:
            logger.info("Worker runner stopped")

    def stop(self):
        self.running = False
        self._shutdown_event.set()

    async def process_job(self, job_data: dict):
        try:
            job = job_data["job"]
            job_id = UUID(job["id"])
            lease_token = UUID(job_data["lease_token"])
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Received malformed job payload in runner: %s", e)
            return
        
        # Start Heartbeat Task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(job_id, lease_token))
        
        try:
            # Run Handler
            result = await self.handler(job["payload"])
            
            # Complete
            completed = await self.client.complete(job_id, lease_token, result)
            if completed:
                logger.info(f"Job {job_id} completed successfully")
            else:
                logger.error(
                    "Job %s handler succeeded but completion ACK failed; lease may be retried",
                    job_id,
                )
            
        except Exception as e:
            # Fail
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Job {job_id} failed: {error_msg}")
            failed = await self.client.fail(job_id, lease_token, error_msg)
            if not failed:
                logger.error("Failed to report failure for job %s", job_id)
            
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self, job_id: UUID, lease_token: UUID):
        try:
            while self.running:
                await asyncio.sleep(10)  # Simple fixed interval
                if not self.running:
                    break
                logger.debug(f"Sending heartbeat for {job_id}")
                success = await self.client.heartbeat(job_id, lease_token)
                if not success:
                    logger.warning(f"Heartbeat failed for {job_id}, aborting execution?")
                    # In real worker, maybe we should cancel the task?
                    # For now just log.
                    break
        except asyncio.CancelledError:
            pass
