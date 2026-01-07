import httpx
from typing import Optional, Any, Dict
from uuid import UUID

class WorkerClient:
    def __init__(self, base_url: str, worker_id: str):
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        # We use a synchronous client or async?
        # Runner is likely async loop, but to keep SDK simple for users, 
        # let's assume async client.
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    async def poll(self, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Polls for a job.
        Returns dict with keys: job, lease_token, expires_at
        or None.
        """
        payload = {"worker_id": self.worker_id}
        if tenant_id:
            payload["tenant_id"] = tenant_id
            
        try:
            resp = await self.client.post("/api/v1/workers/poll", json=payload)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None
            return data
        except httpx.HTTPError as e:
            # logs?
            print(f"Poll error: {e}")
            return None

    async def heartbeat(self, job_id: UUID, lease_token: UUID) -> bool:
        try:
            resp = await self.client.post(
                f"/api/v1/workers/{job_id}/heartbeat",
                json={
                    "worker_id": self.worker_id,
                    "lease_token": str(lease_token)
                }
            )
            resp.raise_for_status()
            return True
        except httpx.HTTPError:
            return False

    async def complete(self, job_id: UUID, lease_token: UUID, result: Dict[str, Any]) -> bool:
        try:
            resp = await self.client.post(
                f"/api/v1/workers/{job_id}/complete",
                json={
                    "worker_id": self.worker_id,
                    "lease_token": str(lease_token),
                    "result": result
                }
            )
            resp.raise_for_status()
            return True
        except httpx.HTTPError:
            return False

    async def fail(self, job_id: UUID, lease_token: UUID, error: str) -> bool:
        try:
            resp = await self.client.post(
                f"/api/v1/workers/{job_id}/fail",
                json={
                    "worker_id": self.worker_id,
                    "lease_token": str(lease_token),
                    "error": error
                }
            )
            resp.raise_for_status()
            return True
        except httpx.HTTPError:
            return False
            
    async def close(self):
        await self.client.aclose()
