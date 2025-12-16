from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from mcp.server.fastmcp import Context

from ..auth import get_bearer_token_for_context
from ..config import settings


@dataclass
class FabricClient:
    """
    Minimal REST client for the Fabric API using AAD bearer tokens.
    """

    base_url: str
    token: str

    @classmethod
    def from_context(cls, ctx: Context) -> "FabricClient":
        """
        Construct a FabricClient using the MCP Context for auth.
        """
        token = get_bearer_token_for_context(settings.fabric_scope, ctx)
        return cls(base_url=settings.fabric_base_url, token=token)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def post(
        self,
        path: str,
        payload: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = requests.post(
            url,
            headers=self._headers(),
            params=params,
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()
