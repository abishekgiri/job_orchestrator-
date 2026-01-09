import asyncio
import logging
from app.db.session import AsyncSessionLocal
from app.scheduler.ticker import run_ticker

logger = logging.getLogger(__name__)

class SchedulerService:
    def __init__(self, interval: int = 10):
        self.interval = interval
        self._running = False
        self._task = None

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
        while self._running:
            try:
                async with AsyncSessionLocal() as session:
                    await run_ticker(session)
            except Exception as e:
                logger.error(f"Error in scheduler ticker: {e}", exc_info=True)
            
            await asyncio.sleep(self.interval)
