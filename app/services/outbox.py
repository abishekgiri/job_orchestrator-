import asyncio
import logging
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionLocal
from app.db.models import OutboxEvent

logger = logging.getLogger(__name__)

class OutboxProcessor:
    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.running = False
        self._task = None

    async def start(self):
        self.running = True
        self._task = asyncio.create_task(self.run_loop())
        logger.info("OutboxProcessor started.")

    async def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("OutboxProcessor stopped.")

    async def run_loop(self):
        while self.running:
            try:
                processed_count = await self.process_batch()
                if processed_count == 0:
                    await asyncio.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error in OutboxProcessor: {e}", exc_info=True)
                await asyncio.sleep(self.interval)

    async def process_batch(self, batch_size: int = 50) -> int:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                # 1. Select Pending Events with Lock
                stmt = (
                    select(OutboxEvent)
                    .where(OutboxEvent.status == "PENDING")
                    .order_by(OutboxEvent.created_at.asc())
                    .with_for_update(skip_locked=True)
                    .limit(batch_size)
                )
                result = await session.execute(stmt)
                events = result.scalars().all()
                
                if not events:
                    return 0
                
                # 2. Publish event to downstream systems
                for event in events:
                    try:
                        await self._publish(event)
                        event.status = "PUBLISHED"
                        event.published_at = datetime.now()
                    except Exception as e:
                        logger.error(f"Failed to publish event {event.id}: {e}")
                        # Retry logic or Dead Letter logic could go here.
                        # For now, we leave it as PENDING (it will retry) 
                        # OR mark FAILED to avoid blocking. 
                        # Let's keep PENDING but ideally we need backoff.
                        # user said: "For now, just log it"
                        pass
                
                # 3. Commit (Implicit via context manager if no error)
                return len(events)

    async def _publish(self, event: OutboxEvent):
        """
        Broadcasts the event to the configured message bus.
        Currently logs the event as a demonstration of integration.
        """
        logger.info(f"OUTBOX PUBLISH: ID={event.id}, Type={event.event_type}, Payload={event.payload}")
