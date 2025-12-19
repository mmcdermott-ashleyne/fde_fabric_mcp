from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from azure.core.credentials import TokenCredential
from mcp.server.fastmcp import Context

from ..auth import get_credential, get_credential_for_context
from ..config import settings


@dataclass
class FabricPipelineClient:
    """
    Minimal client for submitting and monitoring Fabric pipeline jobs.

    Auth is provided via azure-identity TokenCredential, which by default
    comes from the shared auth layer (get_credential), so it respects
    FABRIC_AUTH_MODE (auto vs client_secret).
    """

    workspace_id: str
    pipeline_id: str
    base_url: str = settings.fabric_base_url
    scope: str = settings.fabric_scope
    credential: Optional[TokenCredential] = None
    request_timeout_sec: int = 60

    def __post_init__(self) -> None:
        # If no credential was supplied, use the global/shared credential.
        # This uses FABRIC_AUTH_MODE ("auto" or "client_secret") under the hood.
        if self.credential is None:
            self.credential = get_credential()

    # ------------------------------------------------------------------ #
    # Alternate constructor for MCP usage
    # ------------------------------------------------------------------ #
    @classmethod
    def from_context(
        cls,
        ctx: Context,
        *,
        workspace_id: str,
        pipeline_id: str,
    ) -> "FabricPipelineClient":
        """
        Build a FabricPipelineClient using the MCP Context for auth.

        Uses the same per-client credential cache as other clients
        (get_credential_for_context), so each MCP client gets its own
        token instance keyed by ctx.client_id.
        """
        cred = get_credential_for_context(ctx)
        return cls(
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            credential=cred,
        )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _get_token(self) -> str:
        """
        Acquire a bearer token string for the configured Fabric scope.
        """
        if self.credential is None:
            # Defensive: should be set in __post_init__
            self.credential = get_credential()
        token = self.credential.get_token(self.scope)
        return token.token

    def _pipeline_base_url(self) -> str:
        return f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.pipeline_id}"

    # ------------------------------------------------------------------ #
    # Submission
    # ------------------------------------------------------------------ #
    def submit_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Submit a pipeline run:

            POST /workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType=Pipeline

        Returns a dict containing:
            - status: "submitted" | "error"
            - status_code: HTTP status code
            - location: job instance URL (if present)
            - instance_id: parsed from Location (if possible)
            - response_text: raw response body (if any)
        """
        token = self._get_token()
        url = f"{self._pipeline_base_url()}/jobs/instances"
        params = {"jobType": "Pipeline"}

        resp = requests.post(
            url,
            params=params,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data=json.dumps(payload),
            timeout=self.request_timeout_sec,
        )

        location = resp.headers.get("Location")
        instance_id = location.rstrip("/").split("/")[-1] if location else None

        return {
            "status": "submitted" if resp.status_code in (200, 201, 202) else "error",
            "status_code": resp.status_code,
            "location": location,
            "instance_id": instance_id,
            "response_text": (resp.text or "").strip() or None,
        }

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #
    def get_run_status(self, instance_id: str) -> Dict[str, Any]:
        """
        Get the status for a specific pipeline job instance:

            GET /workspaces/{workspaceId}/items/{itemId}/jobs/instances/{instanceId}
        """
        token = self._get_token()
        url = f"{self._pipeline_base_url()}/jobs/instances/{instance_id}"

        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=self.request_timeout_sec,
        )

        try:
            body: Any = resp.json() if resp.text else {}
        except ValueError:
            body = {"raw_text": resp.text}

        return {
            "status_code": resp.status_code,
            "body": body,
        }

    # ------------------------------------------------------------------ #
    # Polling
    # ------------------------------------------------------------------ #
    def wait_for_completion(
        self,
        instance_id: str,
        poll_interval_sec: int = 10,
        timeout_sec: int = 3600,
    ) -> Dict[str, Any]:
        """
        Poll the job instance until it reaches a terminal state or times out.

        Terminal states typically include: "Succeeded", "Failed", "Cancelled", etc.
        Returns the last payload from get_run_status(), plus a 'timeout' flag
        if the overall wait exceeded timeout_sec.
        """
        start = time.time()

        while True:
            status = self.get_run_status(instance_id)
            body = status.get("body") or {}
            run_status = body.get("status") or body.get("state")

            print(f"[{instance_id}] status={run_status} http={status['status_code']}")

            if run_status in {"Succeeded", "Failed", "Cancelled", "Canceled"}:
                return status

            if time.time() - start > timeout_sec:
                status["timeout"] = True
                return status

            time.sleep(poll_interval_sec)
