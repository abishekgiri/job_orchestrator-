from typing import Any, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict

from app.api.deps import DbSession
from app.db.models import Job
from app.domain.states import JobStatus
from app.scheduler.dispatcher import dispatch_lease
from app.commands.heartbeat import heartbeat
from app.commands.complete_job import complete_job
from app.commands.fail_job import fail_job
from app.domain.errors import JobError, LeaseError

router = APIRouter()

class PollRequest(BaseModel):
    worker_id: str
    tenant_id: Optional[str] = None
    lease_duration_seconds: Optional[int] = None

class PollResponse(BaseModel):
    job: Optional[Any] = None # Using JobResponse model ideally
    lease_token: Optional[UUID] = None
    expires_at: Optional[Any] = None
    
    # We can probably re-use JobResponse or return a specific structure containing payload
    
class JobDTO(BaseModel):
    id: UUID
    tenant_id: str
    status: JobStatus
    payload: dict[str, Any]
    attempts: int
    model_config = ConfigDict(from_attributes=True)

class HeartbeatRequest(BaseModel):
    worker_id: str
    lease_token: UUID
    extend_seconds: int = 60

class CompleteRequest(BaseModel):
    worker_id: str # Not strictly needed if token is unique, but good for validation
    lease_token: UUID
    result: dict[str, Any]
    idempotency_key: Optional[str] = None

class FailRequest(BaseModel):
    worker_id: str
    lease_token: UUID
    error: str

from fastapi import APIRouter, HTTPException, status, Depends
from app.auth.security import SignatureVerifier
from app.db.models import Tenant

@router.post("/poll", response_model=Optional[PollResponse])
async def poll_job(body: PollRequest, session: DbSession, tenant: Tenant = Depends(SignatureVerifier())):
    result = await dispatch_lease(
        session, 
        worker_id=body.worker_id, 
        tenant_id=body.tenant_id,
        lease_duration=body.lease_duration_seconds
    )
    if not result:
        return None
        
    job, lease = result
    # Commit is handled by dispatch_lease/lease_job calling session.flush(), 
    # but we need to commit the transaction here
    await session.commit()
    
    return PollResponse(
        job=JobDTO.model_validate(job),
        lease_token=lease.lease_token,
        expires_at=lease.expires_at
    )

@router.post("/{job_id}/heartbeat")
async def job_heartbeat(job_id: UUID, body: HeartbeatRequest, session: DbSession, tenant: Tenant = Depends(SignatureVerifier())):
    try:
        new_expires_at = await heartbeat(
            session, 
            job_id=job_id, 
            lease_token=body.lease_token, 
            extend_seconds=body.extend_seconds
        )
        await session.commit()
        return {"expires_at": new_expires_at}
    except LeaseError as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.post("/{job_id}/complete")
async def job_complete(job_id: UUID, body: CompleteRequest, session: DbSession, tenant: Tenant = Depends(SignatureVerifier())):
    try:
        job = await complete_job(
            session, 
            job_id=job_id, 
            result_data=body.result, 
            lease_token=body.lease_token,
            idempotency_key=body.idempotency_key
        )
        await session.commit()
        return {"status": "success", "job_status": job.status}
    except JobError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{job_id}/fail")
async def job_fail(job_id: UUID, body: FailRequest, session: DbSession, tenant: Tenant = Depends(SignatureVerifier())):
    try:
        job = await fail_job(
            session, 
            job_id=job_id, 
            error=body.error, 
            lease_token=body.lease_token
        )
        await session.commit()
        return {"status": "failed", "job_status": job.status} # PENDING (retry) or DLQ
    except JobError as e:
        raise HTTPException(status_code=400, detail=str(e))
