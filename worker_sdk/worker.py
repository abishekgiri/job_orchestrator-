import asyncio
import logging
import signal
import sys
from typing import Callable, Any, Coroutine, Optional, List
from uuid import UUID

from worker_sdk.client import WorkerClient

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Coroutine[Any, Any, dict]]
Middleware = Callable[[dict, Handler], Coroutine[Any, Any, dict]]

class Worker:
    def __init__(self, base_url: str, worker_id: str, handler: Handler, tenant_id: Optional[str] = None):
        self.client = WorkerClient(base_url, worker_id)
        self.handler = handler
        self.tenant_id = tenant_id
        self.running = False
        self.middlewares: List[Middleware] = []
        self._shutdown_event = asyncio.Event()

    def add_middleware(self, middleware: Middleware):
        self.middlewares.append(middleware)

    async def run(self):
        self.running = True
        
        # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop)
            except NotImplementedError:
                # Windows support
                pass
                
        logger.info(f"Worker {self.client.worker_id} started (Tenant: {self.tenant_id})")
        
        try:
            while self.running:
                # Poll
                try:
                    job_data = await self.client.poll(self.tenant_id)
                    
                    if job_data:
                        await self.process_job(job_data)
                    else:
                        # Wait a bit before retrying
                        try:
                            await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                        except asyncio.TimeoutError:
                            pass
                            
                except Exception as e:
                    logger.error(f"Poll error: {e}")
                    await asyncio.sleep(5.0)
                    
        finally:
            await self.client.close()
            logger.info("Worker stopped")

    def stop(self):
        logger.info("Shutdown signal received")
        self.running = False
        self._shutdown_event.set()

    async def process_job(self, job_data: dict):
        job = job_data['job']
        job_id = UUID(job['id'])
        lease_token = UUID(job_data['lease_token'])
        
        logger.info(f"Processing Job {job_id}")
        
        # Heartbeats
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(job_id, lease_token))
        
        try:
            # Construct handler chain
            async def core_invoker(payload):
                return await self.handler(payload)
                
            chain = core_invoker
            
            # Apply middleware in reverse order (onion)
            for mw in reversed(self.middlewares):
                # Capture current chain and mw in closure
                def make_wrapper(current_mw, current_chain):
                    async def wrapper(payload):
                        return await current_mw(payload, current_chain)
                    return wrapper
                chain = make_wrapper(mw, chain)
            
            # Run
            result = await chain(job['payload'])
            
            # Complete
            await self.client.complete(job_id, lease_token, result)
            logger.info(f"Job {job_id} completed")
            
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            await self.client.fail(job_id, lease_token, str(e))
            
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self, job_id: UUID, lease_token: UUID):
        try:
            while True:
                await asyncio.sleep(10)
                if not await self.client.heartbeat(job_id, lease_token):
                    logger.warning(f"Heartbeat failed for {job_id}")
                    # Should we abort? Probably.
                    break
        except asyncio.CancelledError:
            pass
