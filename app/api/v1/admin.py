from fastapi import APIRouter
from app.api.deps import DbSession
from app.commands.requeue_expired import requeue_expired_jobs

router = APIRouter()

@router.post("/requeue_expired")
async def trigger_requeue_expired(session: DbSession):
    count = await requeue_expired_jobs(session)
    await session.commit()
    return {"requeued_count": count}
