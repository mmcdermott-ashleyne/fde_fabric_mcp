from __future__ import annotations

import asyncio
from typing import Any, Dict

from mcp.server.fastmcp import Context

from ..clients.sql import get_sql_connection
from ..core import context_store
from ..core.sql_endpoints import get_warehouse_sql_endpoint

WAREHOUSE_NAMESPACE = "warehouse"
LAKEHOUSE_NAMESPACE = "lakehouse"  # same key used in lakehouse.py


async def set_warehouse_impl(
    ctx: Context,
    server: str,
    database: str,
) -> Dict[str, Any]:
    """
    Manually store the active Fabric Warehouse/SQL endpoint (server + database)
    for this client/session.
    """
    payload = {"server": server, "database": database, "type": "Warehouse"}
    context_store.set_value(ctx, WAREHOUSE_NAMESPACE, payload)
    return {"message": "Warehouse set manually.", **payload}


async def set_warehouse_from_fabric_impl(
    ctx: Context,
    workspace: str,
    warehouse: str,
) -> Dict[str, Any]:
    """
    Resolve the SQL endpoint (server + database) for a Warehouse
    by workspace + warehouse name/ID, and store it in context.
    """
    info = await asyncio.to_thread(
        get_warehouse_sql_endpoint,
        workspace=workspace,
        warehouse=warehouse,
    )

    # info already has server, database, workspace/item IDs & names
    context_store.set_value(ctx, WAREHOUSE_NAMESPACE, info)

    return {
        "message": "Warehouse resolved and set from Fabric.",
        **info,
    }


async def get_current_warehouse(ctx: Context) -> Dict[str, Any]:
    """
    Retrieve the currently configured warehouse info for this client/session.
    """
    wh = context_store.get_value(ctx, WAREHOUSE_NAMESPACE)
    if not wh:
        raise RuntimeError(
            "No warehouse set. Call set_warehouse() or set_warehouse_from_fabric() first."
        )
    return wh


def _resolve_sql_target(ctx: Context) -> Dict[str, Any]:
    """
    Internal helper: pick the active SQL target in priority order.

    1. Warehouse (if set)
    2. Lakehouse (if set)
    """
    wh = context_store.get_value(ctx, WAREHOUSE_NAMESPACE)
    if wh:
        return {"kind": "warehouse", **wh}

    lh = context_store.get_value(ctx, LAKEHOUSE_NAMESPACE)
    if lh:
        return {"kind": "lakehouse", **lh}

    raise RuntimeError(
        "No SQL endpoint configured. Set a warehouse via set_warehouse()/"
        "set_warehouse_from_fabric(), or set a lakehouse via set_lakehouse()."
    )


async def run_sql_query_impl(ctx: Context, sql: str) -> Dict[str, Any]:
    """
    Execute a SQL query against the 'active' SQL endpoint for this client:

      - Prefers Warehouse if configured
      - Otherwise uses Lakehouse if configured
    """
    target = _resolve_sql_target(ctx)
    server = target["server"]
    database = target["database"]

    conn = get_sql_connection(server=server, database=database)
    df = await asyncio.to_thread(conn.run_query, sql)

    return {
        "rows": df.to_dict(orient="records"),
        "row_count": len(df),
        "server": server,
        "database": database,
        "target_kind": target.get("kind"),
        "target_name": target.get("item_name"),
        "workspace_name": target.get("workspace_name"),
    }
