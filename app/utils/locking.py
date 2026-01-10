from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# A fixed key for the leader lock. 
# In a real system, this might be configurable or derived from a hash.
# 84728472 is just a random 64-bit integer (Postgres uses 64-bit keys for advisory locks).
LEADER_LOCK_KEY = 84728472

async def try_advisory_lock(session: AsyncSession, key: int = LEADER_LOCK_KEY) -> bool:
    """
    Attempts to acquire a Postgres session-level advisory lock.
    Returns True if acquired, False otherwise.
    
    Note: Session-level locks are released automatically when the session ends.
    """
    # pg_try_advisory_lock is session-scoped.
    result = await session.execute(
        text("SELECT pg_try_advisory_lock(:key)"), 
        {"key": key}
    )
    return result.scalar() is True
