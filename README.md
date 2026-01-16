# FDE Fabric MCP

An MCP server that exposes Microsoft Fabric workspaces, Lakehouses, Warehouses, SQL endpoints, and FDE-specific project orchestration as MCP tools (e.g. Claude Desktop).

It handles:

- Azure AD auth via `azure-identity` (DefaultAzureCredential or client secret)
- Fabric workspace/item discovery
- Lakehouse/Warehouse SQL endpoint resolution
- SQL execution against Fabric Lakehouse/Warehouse
- **Project-driven pipeline submission via a custom "Project Orchestrator" pattern**

---

---

## Features

- Health & identity helpers (`ping`, `whoami`, `reset_context`)
- Workspace discovery/selection, plus Lakehouse/Warehouse resolution
- SQL execution against the active endpoint (warehouse preferred, lakehouse fallback)
- Per-client in-memory context store for workspaces, lakehouses, warehouses, and projects
- Project tools (`list_projects`, `set_project`, `get_current_project`)
- **Custom orchestrator-based pipeline submission**
  - `run_project_pipeline` (runs a single project's routed processor pipeline)
  - `preview_project_pipeline_payload` (builds payloads without submission)
  - `run_all_jobs_for_day` (orchestrator runs per distinct UTC interval)

---

## Installation

```bash
# create & activate a venv (required so Windows auth works predictably)
python -m venv .venv
.venv\Scripts\activate

# install the MCP client/programming helpers so the MCP runtime is available
pip install mcp

# install the project dependencies
pip install -r requirements.txt

# install this package in editable mode so imports work from the repo root
pip install -e .
```


## Project Structure (simplified)

```text
fde_fabric_mcp/
  src/fde_fabric_mcp/
    clients/
      fabric.py            # REST client for Fabric API
      pipeline.py          # Pipeline submit/monitor client
      sql.py               # Fabric SQL connector via Azure AD token
    core/
      context_store.py     # Per-MCP-client in-memory context
      sql_endpoints.py     # Resolve Lakehouse/Warehouse SQL endpoints
    tools/
      identity.py          # Azure CLI identity helpers (`whoami`)
      lakehouse.py         # List/set/get current lakehouse
      notebooks.py         # Notebook list/create helpers
      pipelines.py         # Project pipeline + orchestrator helpers
      projects.py          # Project lookup/store helpers
      warehouse.py         # Set/get warehouse & run SQL queries
      workspace.py         # List/set/get current workspace
      notebook_templates/  # Built-in notebook templates (see list_notebook_templates)
    auth.py                # Azure credential + bearer token helpers
    config.py              # Settings from environment variables
    server.py              # MCP server definition & stdio entrypoint
    __init__.py            # Package metadata (__version__)
```

---

## Requirements

* Python 3.10+ (see `pyproject.toml`)
* Access to a Microsoft Fabric / Power BI tenant
* Azure AD application or interactive login for `DefaultAzureCredential`
* ODBC driver for SQL Server, e.g.:

  * **ODBC Driver 18 for SQL Server** (preferred)
  * **ODBC Driver 17 for SQL Server**

Core runtime dependencies (non-exhaustive):

* `azure-identity`, `azure-core`
* `mcp`
* `requests`, `pyodbc`, `pandas`, `croniter`

---

## Configuration

Configuration is driven by environment variables via `config.Settings` and `auth.AuthConfig`.

### Auth / Fabric API

```text
FABRIC_AUTH_MODE              # "auto" (default) or "client_secret"
FABRIC_TENANT_ID              # Required when FABRIC_AUTH_MODE="client_secret"
FABRIC_CLIENT_ID              # Required when FABRIC_AUTH_MODE="client_secret"
FABRIC_CLIENT_SECRET          # Required when FABRIC_AUTH_MODE="client_secret"
FABRIC_SCOPE                  # Fabric API scope for AAD tokens (default https://api.fabric.microsoft.com/.default)
FABRIC_BASE_URL               # Fabric REST base URL (default https://api.fabric.microsoft.com/v1)
FABRIC_DEFAULT_WORKSPACE_ID   # Optional default workspace GUID
FABRIC_SQL_DRIVER             # Optional ODBC driver override, default "{ODBC Driver 17 for SQL Server}"
```

**Auth modes:**

* `FABRIC_AUTH_MODE=auto`
  Uses `DefaultAzureCredential`, which can chain:

  * `az login`
  * Managed Identity
  * Environment credentials
  * VSCode / Azure workload identity
* `FABRIC_AUTH_MODE=client_secret`
  Uses `ClientSecretCredential` with `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`.

### Pipeline helper configuration

The project/orchestrator helpers use a shared environment config row pulled from:

* `settings.config_workspace` (default `"fde_core_data_config"`)
* `settings.config_warehouse` / `settings.config_database` (default `"core_dw_config"`)
* `settings.config_table` (default `"dbo.environment_config"`)

These are currently set in `src/fde_fabric_mcp/config.py`. Adjust there if your config warehouse/table live elsewhere.

---

### SQL guardrails (optional)

The MCP server can enforce read-only SQL for specific workspaces to prevent
accidental write operations from `run_sql_query`.

```text
FABRIC_READ_ONLY_SQL_ENABLED     # default true
FABRIC_READ_ONLY_WORKSPACE_NAMES # comma-separated workspace names
FABRIC_READ_ONLY_WORKSPACE_IDS   # comma-separated workspace GUIDs
```

Defaults are defined in `src/fde_fabric_mcp/config.py`.

---

## Running the MCP Server

You can start the server over stdio using:

```bash
python -m fde_fabric_mcp.server
```

`server.py` exposes a `run()` function that initializes a `FastMCP` instance and calls `mcp.run()` (stdio transport).

### Example MCP config snippet

For an MCP client that uses a simple command-based transport (pseudo-config):

```jsonc
{
  "name": "fde-fabric-mcp",
  "command": ["python", "-m", "fde_fabric_mcp.server"],
  "env": {
    "FABRIC_AUTH_MODE": "auto",
    "FABRIC_SCOPE": "https://api.fabric.microsoft.com/.default",
    "FABRIC_BASE_URL": "https://api.fabric.microsoft.com/v1",
    "FABRIC_DEFAULT_WORKSPACE_ID": "<your-workspace-guid>"
  }
}
```

---

## Exposed MCP Tools

### Utility

* **`ping()`**
  Health check. Returns `"pong"` if the MCP server is responding.

* **`whoami()`**
  Uses `az account show` to return:

  ```json
  {
    "username": "<short username>",
    "email": "user@domain.com",
    "error": null
  }
  ```

  or an `error` string if Azure CLI isn't available / configured.

* **`reset_context(ctx)`**
  Clears all namespaced values (workspace, lakehouse, warehouse, project) for the current MCP client.

---

### Workspace Tools

* **`list_workspaces(ctx)`**
  Lists all Fabric workspaces visible to the current identity.

* **`set_workspace(ctx, workspace_id=None, workspace_name=None)`**
  Resolve and persist the active workspace in the context store.

  * Prefer `workspace_id` for deterministic behavior.
  * Falls back to a case-insensitive match on `workspace_name`.

* **`get_current_workspace(ctx)`**
  Returns the active workspace.
  If none is explicitly set but `FABRIC_DEFAULT_WORKSPACE_ID` is configured, it will use that.

---

### Lakehouse Tools

* **`list_lakehouses(ctx)`**
  Lists all Lakehouse items in the current workspace.

* **`set_lakehouse(ctx, lakehouse: str)`**
  Resolves a lakehouse by name or ID in the active workspace and stores:

  * `server`
  * `database`
  * `workspace_id`, `workspace_name`
  * `item_id`, `item_name`
  * `type: "Lakehouse"`

* **`get_current_lakehouse(ctx)`**
  Returns the stored lakehouse info for the current client.
  Errors if nothing is set.

---

### Warehouse / SQL Tools

* **`set_warehouse_from_fabric(ctx, workspace: str, warehouse: str)`**
  Resolve a Warehouse by workspace + warehouse (name or GUID) using Fabric REST, then store:

  * `server`
  * `database`
  * `workspace_id`, `workspace_name`
  * `item_id`, `item_name`
  * `type: "Warehouse"`

* **`get_current_warehouse(ctx)`**
  Returns the stored warehouse info for the current client.
  Errors if nothing is set.

* **`run_sql_query(ctx, sql: str)`**
  Executes a SQL query against the "active" SQL endpoint, resolved by:

  1. Warehouse (preferred)
  2. Lakehouse (fallback)

  Returns JSON like:

  ```json
  {
    "rows": [ { "col": "value", ... } ],
    "row_count": 42,
    "server": "<server>",
    "database": "<database>",
    "target_kind": "warehouse" | "lakehouse",
    "target_name": "<item_name>",
    "workspace_name": "<workspace_name>"
  }
  ```

---

### Notebook Tools

* **`list_notebooks(ctx, workspace=None)`**
  Lists Notebook items in a workspace (or the active workspace).

* **`get_notebook(ctx, workspace, notebook_id)`**
  Returns a Notebook item by ID.

* **`create_notebook(ctx, workspace, notebook_name, content, folder_path=None, folder_id=None, create_missing_folders=False, ipynb_name=None)`**
  Creates a new Notebook from a base64-encoded `.ipynb` payload.

  * Use `folder_path` (e.g., `"Shared/Team Notebooks/DataOps"`) to target a folder.
  * Use `folder_id` to target a specific folder item ID.
  * Set `create_missing_folders=True` to create any missing folders along the path.

* **`list_notebook_templates(ctx)`**
  Lists the built-in template names and descriptions.
  Built-in templates are included in the repo under `tools/notebook_templates`.

* **`create_template_notebook(ctx, workspace, notebook_name, template_name, folder_path=None, folder_id=None, create_missing_folders=False, ipynb_name=None)`**
  Creates a new Notebook from a built-in template and places it in an optional
  folder.

Available templates:

* `basic`
* `python_file_to_delta_template`


---

### Project Tools

* **`list_projects(ctx, search=None, top=50)`**
  Lists projects from `dbo.project` in the active SQL endpoint (warehouse preferred, lakehouse fallback).

* **`set_project(ctx, project_id=None, project_alias=None)`**
  Loads a project by ID or alias and stores it in the per-client context.

* **`get_current_project(ctx)`**
  Returns the stored project info for the current client.

---

## Pipeline Tools (Custom Project Orchestrator)

The pipeline helpers in this repo are **not generic "run any Fabric pipeline" helpers**. They implement a **custom Project Orchestrator pattern** used by FDE:

* Each project stores a `cron_expression` (evaluated in `America/New_York` unless `TZ=`/`CRON_TZ=` is included).
* Projects are grouped into distinct UTC `run_interval_utc` values per day.
* A **single orchestrator pipeline** is invoked once per interval to dispatch the appropriate project workload(s).

### Required env_config keys

The orchestrator pipeline ID is pulled from your shared environment config row:

* `env_config["project_orchestrator_id"]` (pipelineId)
* `env_config["workspace_id"]` (workspaceId)

These are passed into the orchestrator invoke payload along with:

* `interval` (UTC minute string)
* `is_current` (flag for orchestrator logic)
* `env` (the full env_config block)

### Tools

* **`run_project_pipeline(...)`**
  Submits a pipeline run for the currently selected project.
  If `time_utc="cron"` (default), the interval time is derived from the project's `cron_expression` in ET and converted to UTC.

* **`preview_project_pipeline_payload(...)`**
  Builds payload(s) for the current project and returns them without submission.

* **`run_all_jobs_for_day(run_date, ...)`**
  Computes distinct `run_interval_utc` values for `run_date`, then invokes the **Project Orchestrator** once per interval.
  The first call returns a `confirmation_required` payload with a suggested next step.

> Note: Because these tools depend on your **custom orchestrator pipeline contract**, they assume:
>
> * a `dbo.project` table with `cron_expression`
> * an environment config row that contains `project_orchestrator_id`

---

## Example Prompts

Use these with your MCP client to drive the tools:

```text
List all Fabric workspaces I can access.

Set the active workspace to "fde_core_data_dev" and list its lakehouses.

Set the lakehouse to "SalesLakehouse" and show the current SQL endpoint.

Use the warehouse "CoreDW" in workspace "fde_core_data_prod" and run:
SELECT TOP 10 * FROM dbo.project ORDER BY updated_at DESC;

List projects where the alias contains "sftp", then set the first one.

Preview the pipeline payload for project "daily_sales" for 2025-01-15 (use cron time).

Run the project pipeline for "daily_sales" on 2025-01-15 at 05:30 UTC (dry run).

Run the orchestrator for 2025-01-15 and show me the confirmation details.

Reset my session context so I can start over.
```

---

## How the Auth & SQL Pieces Fit Together

* `auth.py` builds an Azure `TokenCredential` using either:

  * `DefaultAzureCredential` (`FABRIC_AUTH_MODE=auto`), or
  * `ClientSecretCredential` (`FABRIC_AUTH_MODE=client_secret`).
* `clients.fabric.FabricClient` uses that credential to obtain bearer tokens for Fabric REST calls.
* `core.sql_endpoints` uses Fabric REST to resolve Lakehouse/Warehouse SQL endpoints.
* `clients.sql.SQLServerConnection` uses an access token for the SQL scope and passes it into `pyodbc` via `SQL_COPT_SS_ACCESS_TOKEN` (1256).
* `tools.warehouse.run_sql_query_impl` binds all this together and returns JSON rows to the MCP client.

---

## Development Notes

* Context is in-memory only (`core.context_store`) and keyed by `ctx.client_id`.
  This means:

  * Each MCP client gets its own workspace/lakehouse/warehouse/project selection.
  * Context is lost when the MCP server process restarts.

* HTTP calls are currently synchronous (`requests`) but wrapped with `asyncio.to_thread` so they can be safely used from async tools.
