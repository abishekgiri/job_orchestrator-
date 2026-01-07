from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any
from uuid import UUID

from app.domain.states import JobStatus

@dataclass
class JobDomain:
    id: UUID
    tenant_id: str
    status: JobStatus
    payload: dict[str, Any]
    
    available_at: datetime
    attempts: int = 0
    max_attempts: int = 3
    
    last_error: Optional[str] = None
    idempotency_key: Optional[str] = None
    
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class JobLeaseDomain:
    job_id: UUID
    worker_id: str
    token: UUID
    expires_at: datetime
