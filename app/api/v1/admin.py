from fastapi import APIRouter
from app.api.deps import DbSession
from app.commands.requeue_expired import requeue_expired_jobs
from app.db.models import Tenant
from pydantic import BaseModel

router = APIRouter()

@router.post("/requeue_expired")
async def trigger_requeue_expired(session: DbSession):
    count = await requeue_expired_jobs(session)
    await session.commit()
    return {"requeued_count": count}

class TenantCreate(BaseModel):
    id: str
    name: str
    weight: int = 1
    max_inflight: int = 100

@router.post("/tenants")
async def create_tenant(payload: TenantCreate, session: DbSession):
    tenant = Tenant(
        id=payload.id,
        name=payload.name,
        weight=payload.weight,
        max_inflight=payload.max_inflight
    )
    session.add(tenant)
    await session.commit()
    return tenant
