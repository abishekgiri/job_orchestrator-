import asyncio
import logging
from app.db.session import AsyncSessionLocal


logger = logging.getLogger(__name__)

from app.utils.locking import try_advisory_lock

class SchedulerService:
    def __init__(self, interval: int = 10):
        self.interval = interval
        self._running = False
        self._task = None
        self._is_leader = False

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("Scheduler service started.")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Scheduler service stopped.")

    async def _loop(self):
        session = None
        while self._running:
            try:
                if not session:
                    session = AsyncSessionLocal()
                
                # Attempt to acquire leader lock (session level)
                # Since it's session level, we can check it every tick.
                # If we already have it, this returns True (re-entrant) or we just assume we have it?
                # Best to ensure we have it.
                is_leader = await try_advisory_lock(session)
                
                from app.scheduler.ticker import run_leader_tasks, run_metrics_tasks
                
                # Execute Leader Tasks
                if is_leader:
                    if not self._is_leader:
                        logger.info("Acquired leadership. Starting scheduler.")
                        self._is_leader = True
                    
                    await run_leader_tasks(session)
                else:
                    if self._is_leader:
                        logger.info("Lost leadership. Stopping scheduler.")
                        self._is_leader = False
                
                # Execute Metrics Tasks (On All Instances)
                # This ensures /metrics endpoint has up-to-date gauges (like jobs_inflight)
                await run_metrics_tasks(session)
                    
            except Exception as e:
                logger.error(f"Error in scheduler ticker: {e}", exc_info=True)
                self._is_leader = False
                
                # If DB error, close session and retry to reconnect
                if session:
                    await session.close()
                    session = None
            
            await asyncio.sleep(self.interval)
            
        if session:
            await session.close()
