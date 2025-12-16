from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from mcp.server.fastmcp import Context

from ..core import context_store
from ..core.sql_endpoints import get_lakehouse_sql_endpoint, _fabric_get
from . import workspace as WS

LAKEHOUSE_NAMESPACE = "lakehouse"


async def list_lakehouses_impl(ctx: Context) -> List[Dict[str, Any]]:
    """
    List lakehouse items in the currently selected Fabric workspace.
    """
    ws = await WS.get_current_workspace(ctx)
    workspace_id = ws["id"]

    data = await asyncio.to_thread(
        _fabric_get,
        f"/workspaces/{workspace_id}/items",
        {"type": "Lakehouse"},
    )

    items: List[Dict[str, Any]] = []
    for it in data.get("value", []):
        items.append(
            {
                "id": it.get("id"),
                "displayName": it.get("displayName"),
                "type": it.get("type"),
            }
        )
    return items


async def set_lakehouse_impl(ctx: Context, lakehouse: str) -> Dict[str, Any]:
    """
    Resolve the lakehouse in the current workspace and store its SQL endpoint
    (server + database) in the context store, along with metadata.
    """
    ws = await WS.get_current_workspace(ctx)

    # We can pass workspace ID or name; helper handles both.
    info = await asyncio.to_thread(
        get_lakehouse_sql_endpoint,
        workspace=ws["id"],
        lakehouse=lakehouse,
    )

    # info contains: server, database, workspace_id, workspace_name, item_id, item_name, type
    context_store.set_value(ctx, LAKEHOUSE_NAMESPACE, info)

    return {
        "message": "Lakehouse resolved and set from Fabric.",
        **info,
    }


async def get_current_lakehouse(ctx: Context) -> Dict[str, Any]:
    """
    Return the currently configured lakehouse info for this client/session.

    Includes server + database so SQL tools can use it.
    """
    lh = context_store.get_value(ctx, LAKEHOUSE_NAMESPACE)
    if not lh:
        raise RuntimeError("No lakehouse set. Call set_lakehouse() first.")
    return lh
