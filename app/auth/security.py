import hmac
import hashlib
import logging
from typing import Optional

from fastapi import Security, HTTPException, Request, Header
from fastapi.security import APIKeyHeader
from sqlalchemy import select

from app.api.deps import DbSession
from app.db.models import Tenant

logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

async def get_current_tenant(
    session: DbSession,
    api_key: str = Security(API_KEY_HEADER)
) -> Tenant:
    if not api_key:
        raise HTTPException(status_code=403, detail="Missing API Key")
        
    stmt = select(Tenant).where(Tenant.api_key == api_key)
    tenant = await session.scalar(stmt)
    
    if not tenant:
        raise HTTPException(status_code=403, detail="Invalid API Key")
        
    return tenant

async def verify_request_signature(
    request: Request,
    x_worker_signature: str = Header(None),
    session: DbSession = None # Depends on how we inject dependency
):
    """
    Verifies HMAC signature of the payload using the Tenant's API Key as secret.
    This assumes the worker sends its Tenant ID or we lookup tenant via worker ID?
    
    Challenge: We need to know WHICH tenant (and thus which secret) to use.
    The Worker endpoints payload usually has `worker_id`. 
    But `worker_id` doesn't map to tenant easily (1:N or M:N?).
    Actually `JobLease` maps worker to job, job maps to tenant.
    
    If checking signature on `poll`, we receive `tenant_id` optional.
    If `tenant_id` is provided, we use that tenant's key.
    If not, we can't verify unless we have a global worker key?
    
    Let's simplify:
    For `poll`, if `tenant_id` is present, verify.
    For `complete/heartbeat`, we have `job_id`. We can fetch job -> tenant -> key.
    
    BUT, verifying signature requires reading body.
    FastAPI consumes stream.
    
    Implementation detail:
    For this task "Request signing for workers", usually implies workers have a shared secret or a per-worker secret.
    The prompt says "API keys per tenant".
    So workers running for Tenant A use Tenant A's key.
    
    If I run a worker for Tenant A, I sign with Key A.
    
    Let's handle `poll` authentication first.
    If `tenant_id` is missing in poll, maybe we don't enforce auth (public pool)? 
    Or we require `X-Tenant-ID` header?
    
    Let's assume `X-API-Key` is also used for workers, or signature?
    "Request signing" implies HMAC.
    
    Let's modify `verify_signature` to compute SHA256 HMAC of body.
    We need `api_key` to sign.
    
    How do we get `api_key`?
    The worker must claim identity.
    Passing `X-Tenant-ID` header is standard.
    """
    if not x_worker_signature:
         raise HTTPException(status_code=401, detail="Missing Signature")
         
    tenant_id = request.headers.get("X-Tenant-ID")
    if not tenant_id:
         raise HTTPException(status_code=400, detail="Missing X-Tenant-ID header")
         
    # Need session to fetch tenant
    # dependency injection limitation: verify_request_signature is used as dependency?
    # We need access to DB.
    # In FastAPI, `Depends(verify_request_signature)` can inject other deps.
    pass

# We'll implement class-based dependency to handle db access
class SignatureVerifier:
    def __init__(self):
        pass
        
    async def __call__(self, request: Request, session: DbSession, x_signature: str = Header(alias="X-Worker-Signature")):
        tenant_id = request.headers.get("X-Tenant-ID")
        if not tenant_id:
             raise HTTPException(status_code=400, detail="Missing X-Tenant-ID")
             
        stmt = select(Tenant).where(Tenant.id == tenant_id)
        tenant = await session.scalar(stmt)
        if not tenant or not tenant.api_key:
             raise HTTPException(status_code=403, detail="Tenant not found or no key")
             
        # Compute signature
        body = await request.body()
        computed = hmac.new(
            tenant.api_key.encode(),
            body,
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(computed, x_signature):
             raise HTTPException(status_code=401, detail="Invalid Signature")
             
        return tenant
