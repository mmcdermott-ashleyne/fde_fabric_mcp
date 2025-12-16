from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from mcp.server.fastmcp import Context

from ..clients.fabric import FabricClient
from ..config import settings
from ..core import context_store

WORKSPACE_NAMESPACE = "workspace"


async def list_workspaces_impl(ctx: Context) -> List[Dict[str, Any]]:
    """
    List Fabric workspaces visible to the current identity.

    Async wrapper – currently calls sync FabricClient, but can switch
    to async HTTP in the future with no signature changes.
    """
    client = FabricClient.from_context(ctx)
    # Run sync HTTP in a thread to keep tools async-friendly
    resp = await asyncio.to_thread(client.get, "/workspaces")
    return [
        {
            "id": ws.get("id"),
            "displayName": ws.get("displayName"),
            "type": ws.get("type"),
        }
        for ws in resp.get("value", [])
    ]


async def set_workspace_impl(
    ctx: Context,
    workspace_id: str | None,
    workspace_name: str | None,
) -> Dict[str, Any]:
    """
    Resolve and store the active Fabric workspace for this client.
    """
    if not workspace_id and not workspace_name:
        raise ValueError("Provide workspace_id or workspace_name.")

    all_ws = await list_workspaces_impl(ctx)
    chosen: Dict[str, Any] | None = None

    if workspace_id:
        chosen = next(
            (
                w
                for w in all_ws
                if (w.get("id") or "").lower() == workspace_id.lower()
            ),
            None,
        )

    if not chosen and workspace_name:
        chosen = next(
            (
                w
                for w in all_ws
                if (w.get("displayName") or "").lower()
                == (workspace_name or "").lower()
            ),
            None,
        )

    if not chosen:
        raise ValueError("Workspace not found.")

    context_store.set_value(ctx, WORKSPACE_NAMESPACE, chosen)
    return {"message": "Workspace set", "workspace": chosen}


async def get_current_workspace(
    ctx: Context,
    *,
    allow_default: bool = True,
) -> Dict[str, Any]:
    """
    Retrieve the current workspace for this client.

    If not explicitly set, may fall back to FABRIC_DEFAULT_WORKSPACE_ID if configured.
    """
    ws = context_store.get_value(ctx, WORKSPACE_NAMESPACE)
    if ws:
        return ws

    if allow_default and settings.default_workspace_id:
        ws = {"id": settings.default_workspace_id, "displayName": None}
        context_store.set_value(ctx, WORKSPACE_NAMESPACE, ws)
        return ws

    raise RuntimeError("No workspace set. Use set_workspace().")
