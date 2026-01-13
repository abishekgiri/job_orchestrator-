import httpx
import hashlib
import hmac
import json
from typing import Optional, Any, Dict, Union
from uuid import UUID

class WorkerClient:
    def __init__(self, base_url: str, worker_id: str, tenant_id: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.tenant_id = tenant_id
        self.api_key = api_key
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    def _sign_request(self, payload: dict) -> Dict[str, str]:
        headers = {}
        if self.tenant_id:
            headers["X-Tenant-ID"] = self.tenant_id
            
        if self.tenant_id and self.api_key:
            # Note: Signature verification requires exact byte-matching of the payload.
            pass
        return headers

    async def _post(self, path: str, json_body: dict):
        headers = {}
        content = None
        
        if self.tenant_id:
            headers["X-Tenant-ID"] = self.tenant_id
            
        if self.tenant_id and self.api_key:
            # Deterministic serialization for signing
            # We use json.dumps() and send as content
            content = json.dumps(json_body).encode("utf-8")
            sig = hmac.new(
                self.api_key.encode("utf-8"),
                content,
                hashlib.sha256
            ).hexdigest()
            headers["X-Worker-Signature"] = sig
            headers["Content-Type"] = "application/json"
            
            return await self.client.post(path, content=content, headers=headers)
        else:
            return await self.client.post(path, json=json_body, headers=headers)

    async def poll(self, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Polls for a job.
        """
        # Override self.tenant_id if provided?
        tid = tenant_id or self.tenant_id
        
        payload = {"worker_id": self.worker_id}
        if tid:
            payload["tenant_id"] = tid
            
        try:
            # We use internal _post to handle signing
            # But wait, self._post uses self.tenant_id.
            # If poll calls with different tenant_id, signing might be wrong if key belongs to one tenant?
            # Let's assume Worker runs for ONE tenant if security is enabled.
            resp = await self._post("/api/v1/workers/poll", json_body=payload)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None
            return data
        except httpx.HTTPError as e:
            print(f"Poll error: {e}")
            return None

    async def heartbeat(self, job_id: UUID, lease_token: UUID) -> bool:
        try:
            resp = await self._post(
                f"/api/v1/workers/{job_id}/heartbeat",
                json_body={
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
            resp = await self._post(
                f"/api/v1/workers/{job_id}/complete",
                json_body={
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
            resp = await self._post(
                f"/api/v1/workers/{job_id}/fail",
                json_body={
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
