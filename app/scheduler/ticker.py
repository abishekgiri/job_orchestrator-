from datetime import datetime, timedelta
from sqlalchemy import select, update, text, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job
from app.domain.states import JobStatus

async def run_ticker(session: AsyncSession):
    """
    Periodic maintenance tasks:
    1. Advance SCHEDULED jobs to PENDING if available_at <= now
    2. Age priorities of waiting jobs to prevent starvation
    """
    now = datetime.now().astimezone()
    
    # 1. Advance Scheduled Jobs
    # UPDATE jobs SET status='PENDING' WHERE status='SCHEDULED' AND available_at <= now
    stmt_advance = (
        update(Job)
        .where(
            Job.status == JobStatus.SCHEDULED,
            Job.available_at <= now
        )
        .values(status=JobStatus.PENDING)
    )
    await session.execute(stmt_advance)
    
    # 2. Priority Aging
    # Increase priority by 1 (cap at 9) for jobs waiting longer than X minutes
    # Strategy: "If you've been PENDING for > 1 hour, +1 priority"
    # Or simplified: Every tick, increment priority for old jobs?
    # Let's say: Increase priority every 1 minute of waiting?
    # To be safe and controllable: 
    # Let's bump priority for any job pending > 30s? (For demo purposes)
    # Real world might be 1 hour.
    # Let's say: "Jobs created < now - 30s AND priority < 9" -> priority += 1
    # But we don't want to bump them infinitely fast. We need a "last_aged_at" or just rely on the tick interval?
    # If the ticker runs every 1 second, this would skyrocket priority.
    # We should only bump if they haven't been bumped recently?
    # Or, we can just say: Priority = BasePriority + (Now - CreatedAt) / Factor?
    # But we store priority in DB.
    # Let's implement a simple "Bump slightly older jobs" query.
    # "Update priority = priority + 1 where status=PENDING and priority < 9 AND created_at < Now - (Priority+1)*1min"
    # This implies higher priority needs longer wait to bump again?
    
    # Simple Approach for Demo:
    # Use a "last_updated_at" check? No, updated_at changes on other things.
    # Let's just bump everything that is old enough, assuming the ticker runs infrequently OR we use a specific criteria.
    # Let's assume ticker runs every 10s.
    # We want to bump priority maybe once every minute.
    # We can check: created_at + (priority * 1 min) < now?
    # E.g. Priority 0 -> bump if Created < now.
    # Priority 1 -> bump if Created < now - 1 min.
    # Priority 2 -> bump if Created < now - 2 min.
    # This means a job naturally climbs the ladder 1 step per minute.
    
    # SQL: UPDATE jobs SET priority = priority + 1 
    # WHERE status = 'PENDING' 
    # AND priority < 9 
    # AND created_at < now() - (priority * interval '1 minute') - interval '1 minute'
    
    # Using text() for interval logic to be postgres specific/easy
    stmt_aging = text("""
        UPDATE jobs 
        SET priority = priority + 1 
        WHERE status = :status 
        AND priority < 9 
        AND created_at < (NOW() - make_interval(mins := (priority + 1)))
    """)
    
    await session.execute(stmt_aging, {"status": JobStatus.PENDING})
    
    await session.commit()
