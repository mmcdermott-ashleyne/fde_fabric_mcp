from __future__ import annotations

from typing import Any, Dict, List
import sys

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from .core import context_store
from .tools import lakehouse as LH
from .tools import warehouse as WH
from .tools import workspace as WS
from .tools import pipelines as PL
from .tools import projects as PR
from .tools.identity import (
    get_username_short,
    get_username_email,
    AzAccountError,
)
# from .tools import pipelines as PL  # Uncomment when pipeline tools are ready

# -----------------------------------------------------------------------------
# MCP Server Setup
# -----------------------------------------------------------------------------
mcp = FastMCP("FDE Fabric MCP", json_response=True)

# -----------------------------------------------------------------------------
# Utility Tools
# -----------------------------------------------------------------------------
@mcp.tool(description="Health check to verify the MCP server is running.")
async def ping() -> str:
    """
    Simple connectivity test.

    Returns:
        The string "pong" if the MCP server is reachable and responsive.
    """
    return "pong"


@mcp.tool(
    description=(
        "Return the current Azure CLI account username. "
        "Uses `az account show` under the hood."
    )
)
async def whoami() -> Dict[str, Any]:
    """
    Get the current Azure CLI username, using Azure CLI (`az account show`).

    Returns:
        {
          "username": "<short username or null>",
          "email": "<full email or null>",
          "error": "<error message if something went wrong, else null>"
        }
    """
    try:
        short = get_username_short()
        email = get_username_email()
        return {
            "username": short,
            "email": email,
            "error": None,
        }
    except AzAccountError as e:
        # Detailed error surfaced to the MCP caller
        return {
            "username": None,
            "email": None,
            "error": str(e),
        }


@mcp.tool(
    description=(
        "Clear all saved context (workspace, lakehouse, warehouse, etc.) "
        "for the current MCP client."
    )
)
async def reset_context(ctx: Context) -> Dict[str, Any]:
    """
    Clear all namespaced values for this MCP client.
    """
    context_store.clear_client(ctx)
    return {"message": "Context cleared for this client."}


# -----------------------------------------------------------------------------
# Workspace Tools
# -----------------------------------------------------------------------------
@mcp.tool(
    description=(
        "List all Fabric workspaces accessible to the current identity. "
        "Use this to discover workspace IDs and names before selecting one."
    )
)
async def list_workspaces(
    ctx: Context,
) -> List[Dict[str, Any]]:
    """
    List all available Fabric workspaces for this MCP client context.
    """
    return await WS.list_workspaces_impl(ctx)


@mcp.tool(
    description=(
        "Set the active Fabric workspace for subsequent tools such as lakehouse and "
        "warehouse/SQL operations. Accepts either workspace_id (preferred) or "
        "workspace_name (case-insensitive)."
    )
)
async def set_workspace(
    ctx: Context,
    workspace_id: str | None = Field(
        default=None,
        description=(
            "Workspace GUID to select. Preferred for deterministic behavior. "
            "If provided, this is used even if workspace_name is also set."
        ),
    ),
    workspace_name: str | None = Field(
        default=None,
        description=(
            "Workspace display name (case-insensitive). Used only if workspace_id "
            "is not provided."
        ),
    ),
) -> Dict[str, Any]:
    """
    Resolve and store the active Fabric workspace for this client.
    """
    return await WS.set_workspace_impl(ctx, workspace_id, workspace_name)


@mcp.tool(
    description=(
        "Get the currently selected Fabric workspace for this client. "
        "May fall back to FABRIC_DEFAULT_WORKSPACE_ID if configured."
    )
)
async def get_current_workspace(
    ctx: Context,
) -> Dict[str, Any]:
    """
    Retrieve the active workspace for the current client.

    Returns:
        A workspace dict containing at least:
        - id: Workspace GUID
        - displayName: May be None if coming from default workspace ID only.
    """
    return await WS.get_current_workspace(ctx)


# -----------------------------------------------------------------------------
# Lakehouse Tools
# -----------------------------------------------------------------------------
@mcp.tool(
    description=(
        "List Lakehouse items in the currently selected Fabric workspace for this client. "
        "Requires set_workspace to have been called first or a default workspace "
        "to be configured."
    )
)
async def list_lakehouses(
    ctx: Context,
) -> List[Dict[str, Any]]:
    """
    List all lakehouses in the active workspace for this client.
    """
    return await LH.list_lakehouses_impl(ctx)


@mcp.tool(
    description=(
        "Set the current lakehouse for this client session by name or ID. "
        "Subsequent lakehouse-related tools can use this selection."
    )
)
async def set_lakehouse(
    ctx: Context,
    lakehouse: str = Field(
        description="Lakehouse name or ID to select in the current workspace."
    ),
) -> Dict[str, Any]:
    """
    Resolve and store the active lakehouse for this client.
    """
    return await LH.set_lakehouse_impl(ctx, lakehouse)


@mcp.tool(
    description=(
        "Get the currently configured lakehouse for this client session, including "
        "its resolved SQL endpoint (server + database) and workspace/lakehouse IDs."
    )
)
async def get_current_lakehouse(
    ctx: Context,
) -> Dict[str, Any]:
    return await LH.get_current_lakehouse(ctx)

# -----------------------------------------------------------------------------
# Warehouse / SQL Tools
# -----------------------------------------------------------------------------
@mcp.tool(
    description=(
        "Resolve a Fabric Warehouse SQL endpoint from workspace + warehouse name/ID "
        "and set it as the active SQL target for this client."
    )
)
async def set_warehouse_from_fabric(
    ctx: Context,
    workspace: str = Field(
        description="Workspace name or GUID where the warehouse lives."
    ),
    warehouse: str = Field(
        description="Warehouse name or GUID to select."
    ),
) -> Dict[str, Any]:
    return await WH.set_warehouse_from_fabric_impl(ctx, workspace, warehouse)

@mcp.tool(
    description=(
        "Get the currently configured Fabric Warehouse/SQL endpoint for this client, "
        "including server, database, workspace and warehouse identifiers."
    )
)
async def get_current_warehouse(
    ctx: Context,
) -> Dict[str, Any]:
    return await WH.get_current_warehouse(ctx)


@mcp.tool(
    description=(
        "Execute a SQL query against the currently configured Fabric Warehouse "
        "for this client. Use set_warehouse before this to select the server and "
        "database."
    )
)
async def run_sql_query(
    ctx: Context,
    sql: str = Field(
        description=(
            "SQL query text to execute. Typically a read-only SELECT statement. "
            "Results are returned as JSON rows."
        ),
    ),
) -> Dict[str, Any]:
    """
    Run a SQL query against the active warehouse for this client.
    """
    return await WH.run_sql_query_impl(ctx, sql)

# -----------------------------------------------------------------------------
# Project Tools
# -----------------------------------------------------------------------------
@mcp.tool(
    description=(
        "List projects from dbo.project in the currently configured SQL endpoint "
        "(warehouse or lakehouse). Optionally filter by project_alias substring."
    )
)
async def list_projects(
    ctx: Context,
    search: str | None = Field(
        default=None,
        description=(
            "Optional substring filter applied to project_alias "
            "(uses SQL LIKE %search%)."
        ),
    ),
    top: int = Field(
        default=50,
        description="Maximum number of projects to return (TOP N).",
    ),
) -> List[Dict[str, Any]]:
    """
    List projects visible in the current warehouse/lakehouse.
    """
    return await PR.list_projects_impl(ctx, search=search, top=top)


@mcp.tool(
    description=(
        "Set the current project for this client by project_id or project_alias. "
        "The project is loaded from dbo.project in the active SQL endpoint and "
        "stored in per-client context."
    )
)
async def set_project(
    ctx: Context,
    project_id: str | None = Field(
        default=None,
        description="Project ID to select (GUID/string/int). Use either this or project_alias.",
    ),
    project_alias: str | None = Field(
        default=None,
        description="Project alias to select. Use either this or project_id.",
    ),
) -> Dict[str, Any]:
    """
    Resolve and store the active project for this client.
    """
    return await PR.set_project_impl(ctx, project_id=project_id, project_alias=project_alias)


@mcp.tool(
    description=(
        "Get the currently selected project for this client. "
        "Requires set_project to have been called first."
    )
)
async def get_current_project(
    ctx: Context,
) -> Dict[str, Any]:
    """
    Retrieve the active project for the current client.
    """
    return await PR.get_current_project_impl(ctx)

# ---------------------------------------------------------------------------
# Pipeline Tools
# ---------------------------------------------------------------------------
@mcp.tool(
    description=(
        "Submit a Fabric pipeline job for the currently selected project. "
        "Use set_project() first so the project metadata is available."
    )
)
async def run_project_pipeline(
    ctx: Context,
    start_date: str = Field(
        description="ISO date (YYYY-MM-DD) for the first interval.",
    ),
    time_utc: str = Field(
        default="cron",
        description="UTC time (HH:MM) for each interval, or 'cron' to derive from project's cron_expression in America/New_York.",
    ),
    end_date: str | None = Field(
        default=None,
        description="Optional end date (YYYY-MM-DD). Defaults to start_date.",
    ),
    workspace_id: str | None = Field(
        default=None,
        description="Optional workspace ID. Falls back to the project's workspace_id or FABRIC_DEFAULT_WORKSPACE_ID.",
    ),
    pipeline_id: str | None = Field(
        default=None,
        description="Optional pipeline item GUID. Falls back to the project's parameters.",
    ),
    pipeline_name: str | None = Field(
        default=None,
        description="Optional pipeline name to include in executionData.pipelineName.",
    ),
    env: Dict[str, Any] | None = Field(
        default=None,
        description="Optional environment dictionary to merge with any env data stored on the project.",
    ),
    parameters_override: Dict[str, Any] | None = Field(
        default=None,
        description="Override for the project's parameters payload.",
    ),
    dry_run: bool = Field(
        default=False,
        description="If true, the payload is built but not submitted.",
    ),
) -> List[Dict[str, Any]]:
    return await PL.run_project_pipeline_impl(
        ctx,
        start_date=start_date,
        end_date=end_date,
        time_utc=time_utc,
        workspace_id=workspace_id,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        env=env,
        parameters_override=parameters_override,
        dry_run=dry_run,
    )


@mcp.tool(
    description=(
        "Build the pipeline payload for the current project using the shared "
        "environment_config row for the active stage (fde_core_data_{stage}). "
        "Returns the payload(s) without submitting the run."
    )
)
async def preview_project_pipeline_payload(
    ctx: Context,
    start_date: str = Field(
        description="ISO date (YYYY-MM-DD) for the first interval.",
    ),
    time_utc: str = Field(
        default="cron",
        description="UTC time (HH:MM) for each interval, or 'cron' to derive from project's cron_expression in America/New_York.",
    ),
    end_date: str | None = Field(
        default=None,
        description="Optional end date (YYYY-MM-DD). Defaults to start_date.",
    ),
    env_override: Dict[str, Any] | None = Field(
        default=None,
        description="Additional environment values to merge on top of the config row.",
    ),
    parameters_override: Dict[str, Any] | None = Field(
        default=None,
        description="Override for the project's parameters payload.",
    ),
) -> List[Dict[str, Any]]:
    return await PL.build_project_pipeline_payload_impl(
        ctx,
        start_date=start_date,
        time_utc=time_utc,
        end_date=end_date,
        env_override=env_override,
        parameters_override=parameters_override,
    )

# -----------------------------------------------------------------------------
# Entrypoint (stdio)
# -----------------------------------------------------------------------------
def run() -> None:
    """
    Start the FDE Fabric MCP server over stdio.
    This will block and serve tool requests until terminated.
    """
    print("Starting FDE Fabric MCP server...", file=sys.stderr, flush=True)
    mcp.run()  # stdio transport by default


if __name__ == "__main__":
    run()
