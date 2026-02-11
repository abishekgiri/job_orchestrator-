import hashlib
import hmac
import json
import logging
from typing import Any, Dict, Optional
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)

class WorkerClient:
    def __init__(self, base_url: str, worker_id: str, tenant_id: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.tenant_id = tenant_id
        self.api_key = api_key
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    @staticmethod
    def _serialize_body(json_body: Dict[str, Any]) -> bytes:
        # Stable encoding keeps signatures deterministic and payloads compact.
        return json.dumps(json_body, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def _build_headers(self, tenant_id: Optional[str], body: Optional[bytes] = None) -> Dict[str, str]:
        headers = {}
        if tenant_id:
            headers["X-Tenant-ID"] = tenant_id

        if self.api_key:
            if not tenant_id:
                raise ValueError("tenant_id is required when api_key is configured")
            if body is None:
                raise ValueError("request body is required for signed worker requests")
            signature = hmac.new(
                self.api_key.encode("utf-8"),
                body,
                hashlib.sha256,
            ).hexdigest()
            headers["X-Worker-Signature"] = signature
            headers["Content-Type"] = "application/json"

        return headers

    async def _post(self, path: str, json_body: Dict[str, Any], tenant_id: Optional[str] = None) -> httpx.Response:
        effective_tenant_id = tenant_id or self.tenant_id

        if self.api_key:
            content = self._serialize_body(json_body)
            headers = self._build_headers(effective_tenant_id, content)
            return await self.client.post(path, content=content, headers=headers)

        headers = self._build_headers(effective_tenant_id)
        return await self.client.post(path, json=json_body, headers=headers)

    async def poll(self, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Polls for a job.
        """
        tid = tenant_id or self.tenant_id

        payload = {"worker_id": self.worker_id}
        if tid:
            payload["tenant_id"] = tid

        try:
            resp = await self._post("/api/v1/workers/poll", json_body=payload, tenant_id=tid)
            resp.raise_for_status()
            data = resp.json()
            return data or None
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            log_fn = logger.info if status_code in (401, 403, 422) else logger.warning
            log_fn(
                "Poll rejected for worker=%s tenant=%s status=%s",
                self.worker_id,
                tid,
                status_code,
            )
            return None
        except (httpx.HTTPError, ValueError) as e:
            logger.error("Poll failed for worker=%s tenant=%s: %s", self.worker_id, tid, e)
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
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Heartbeat failed for worker=%s job=%s: %s", self.worker_id, job_id, e)
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
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Complete failed for worker=%s job=%s: %s", self.worker_id, job_id, e)
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
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Fail request failed for worker=%s job=%s: %s", self.worker_id, job_id, e)
            return False
            
    async def close(self):
        await self.client.aclose()
