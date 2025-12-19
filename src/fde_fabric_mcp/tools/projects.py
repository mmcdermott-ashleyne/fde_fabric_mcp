from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import Context

from ..clients.sql import get_sql_connection
from ..core import context_store

PROJECT_NAMESPACE = "project"
WAREHOUSE_NAMESPACE = "warehouse"
LAKEHOUSE_NAMESPACE = "lakehouse"  # same keys used elsewhere


def _resolve_sql_target(ctx: Context) -> Dict[str, Any]:
    """
    Pick the active SQL target for this client:

    1. Warehouse (if set via set_warehouse_from_fabric / set_warehouse)
    2. Lakehouse (if set via set_lakehouse)

    This is the same idea as warehouse._resolve_sql_target, but local here
    to avoid circular imports.
    """
    wh = context_store.get_value(ctx, WAREHOUSE_NAMESPACE)
    if wh:
        return {"kind": "warehouse", **wh}

    lh = context_store.get_value(ctx, LAKEHOUSE_NAMESPACE)
    if lh:
        return {"kind": "lakehouse", **lh}

    raise RuntimeError(
        "No SQL endpoint configured. Set a warehouse via set_warehouse_from_fabric() "
        "or set a lakehouse via set_lakehouse() first."
    )


async def list_projects_impl(
    ctx: Context,
    search: Optional[str] = None,
    top: int = 50,
) -> List[Dict[str, Any]]:
    """
    List projects from [dbo].[project] in the current SQL endpoint.

    If 'search' is provided, filters by project_alias (LIKE %search%).
    Results are ordered by updated_at DESC, created_at DESC.
    """
    if top <= 0:
        raise ValueError("top must be a positive integer.")

    target = _resolve_sql_target(ctx)
    server = target["server"]
    database = target["database"]

    conn = get_sql_connection(server=server, database=database)

    base_sql = """
        SELECT TOP ({top})
            project_id,
            project_alias,
            project_type,
            project_description,
            stage,
            env,
            is_active,
            cron_expression,
            interval_offset_min,
            updated_at,
            created_at
        FROM [dbo].[project]
    """.format(top=int(top))

    params: List[Any] = []
    if search:
        base_sql += " WHERE project_alias LIKE ?"
        params.append(f"%{search}%")

    base_sql += " ORDER BY updated_at DESC, created_at DESC;"

    df = await asyncio.to_thread(conn.run_query, base_sql, params or None)
    if df is None or df.empty:
        return []

    return df.to_dict(orient="records")


async def set_project_impl(
    ctx: Context,
    project_id: Optional[Any] = None,
    project_alias: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolve a single project row by project_id OR project_alias and store it
    in the per-client context.

    Exactly one of project_id or project_alias must be provided.
    """
    if (project_id is None) == (project_alias is None):
        raise ValueError("Provide exactly one of project_id or project_alias.")

    target = _resolve_sql_target(ctx)
    server = target["server"]
    database = target["database"]

    conn = get_sql_connection(server=server, database=database)

    if project_id is not None:
        sql = """
            SELECT TOP (1)
                project_id,
                project_alias,
                project_type,
                project_description,
                intent_type,
                stage,
                env,
                cron_expression,
                interval_offset_min,
                dependency_rule,
                timeout_min,
                is_active,
                created_at,
                updated_at,
                parameters
            FROM [dbo].[project]
            WHERE project_id = ?
            ORDER BY updated_at DESC, created_at DESC;
        """
        params = [project_id]
    else:
        sql = """
            SELECT TOP (1)
                project_id,
                project_alias,
                project_type,
                project_description,
                intent_type,
                stage,
                env,
                cron_expression,
                interval_offset_min,
                dependency_rule,
                timeout_min,
                is_active,
                created_at,
                updated_at,
                parameters
            FROM [dbo].[project]
            WHERE project_alias = ?
            ORDER BY updated_at DESC, created_at DESC;
        """
        params = [project_alias]

    df = await asyncio.to_thread(conn.run_query, sql, params)
    if df is None or df.empty:
        key_desc = f"project_id={project_id!r}" if project_id is not None else f"project_alias={project_alias!r}"
        raise ValueError(f"No project found for {key_desc} in database '{database}'.")

    row = df.iloc[0].to_dict()
    if "interval_offset_min" in row and row["interval_offset_min"] is None:
        row["interval_offset_min"] = 0

    # Store in context
    context_store.set_value(ctx, PROJECT_NAMESPACE, row)
    return {"message": "Project set", "project": row}


async def get_current_project_impl(ctx: Context) -> Dict[str, Any]:
    """
    Return the currently selected project for this client, or error if none set.
    """
    proj = context_store.get_value(ctx, PROJECT_NAMESPACE)
    if not proj:
        raise RuntimeError("No project set. Call set_project() first.")
    return proj
