# FDE Fabric MCP

An MCP server that exposes Microsoft Fabric workspaces, Lakehouses, Warehouses, and SQL endpoints as tools for MCP clients (e.g. Claude Desktop).  

It handles:

- Azure AD auth via `azure-identity` (DefaultAzureCredential or client secret)
- Discovering Fabric workspaces and items
- Resolving Lakehouse / Warehouse SQL endpoints
- Running SQL queries against Fabric Warehouse/Lakehouse
- Project-driven pipeline submission via `run_project_pipeline`

---

## Features

- Health & identity helpers (`ping`, `whoami`, `reset_context`)
- Workspace discovery and selection tools
- Lakehouse discovery plus SQL endpoint resolution
- Warehouse selection combined with SQL execution helpers
- Per-client in-memory context store for workspaces, lakehouses, warehouses, and projects
- Project-driven Fabric pipeline submission (`run_project_pipeline`)

---

## Project Structure (simplified)

```text
fde_fabric_mcp/
├── clients/
│   ├── fabric.py          # Simple REST client for Fabric API
│   └── sql.py             # Fabric SQL/Warehouse connector via Azure AD token
├── core/
│   ├── context_store.py   # Per-MCP-client in-memory context
│   ├── pipeline_runner.py # (stub / wrapper for your pipeline runner impl)
│   ├── projects.py        # Reserved for future project helpers
│   └── sql_endpoints.py   # Resolve Lakehouse/Warehouse SQL endpoints via REST
├── tools/
│   ├── identity.py        # Azure CLI-based identity helpers (`whoami`)
│   ├── lakehouse.py       # List/set/get current Lakehouse
│   ├── pipelines.py       # Pipeline rerun implementations (sync, no MCP glue)
│   ├── warehouse.py       # Set/get Warehouse & run SQL queries
│   └── workspace.py       # List/set/get current workspace
├── auth.py                # Azure credential + bearer token helpers
├── config.py              # Settings from environment variables
├── server.py              # MCP server definition & stdio entrypoint
└── __init__.py            # Package metadata (__version__)
````

---

## Requirements

* Python 3.11+ (tested with 3.13 in `__pycache__` paths)
* Access to Microsoft Fabric / Power BI tenant
* Azure AD application or a valid interactive login (for `DefaultAzureCredential`)
* ODBC driver for SQL Server, e.g.:

  * **ODBC Driver 18 for SQL Server** (preferred)
  * **ODBC Driver 17 for SQL Server**

Python dependencies (non-exhaustive, from imports):

* `azure-identity`
* `azure-core`
* `cachetools`
* `mcp-server` (or equivalent providing `mcp.server.fastmcp`)
* `pydantic`
* `requests`
* `pyodbc`
* `pandas`

---

## Installation

```bash
# clone your repo
git clone <this-repo-url>
cd fde_fabric_mcp

# create & activate a venv (optional but recommended)
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# install package + deps
pip install -r requirements.txt
```

---

## Configuration

All configuration is driven by environment variables and read via `config.Settings` and `auth.AuthConfig`.

### Auth / Fabric API

```text
FABRIC_AUTH_MODE         # "auto" (default) or "client_secret"
FABRIC_TENANT_ID         # Required when FABRIC_AUTH_MODE="client_secret"
FABRIC_CLIENT_ID         # Required when FABRIC_AUTH_MODE="client_secret"
FABRIC_CLIENT_SECRET     # Required when FABRIC_AUTH_MODE="client_secret"
FABRIC_SCOPE             # Fabric API scope for AAD tokens
FABRIC_BASE_URL          # Fabric REST base URL
FABRIC_DEFAULT_WORKSPACE_ID  # Optional default workspace GUID
FABRIC_SQL_DRIVER        # Optional ODBC driver override, default "{ODBC Driver 17 for SQL Server}"
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

### Optional pipeline runner env (if you wire in `FabricPipelineRunner`)

If you uncomment and plug in your `FabricPipelineRunner` in `core/pipeline_runner.py`, it expects:

```text
FABRIC_TENANT_ID
FABRIC_CLIENT_ID
FABRIC_CLIENT_SECRET
FABRIC_DEFAULT_WORKSPACE_ID
FABRIC_SQL_USER
FABRIC_SQL_PASSWORD
FABRIC_SQL_DRIVER          # (optional override)
FABRIC_CONFIG_DB           # e.g. "core_dw_config"
FABRIC_PROJECT_DB          # e.g. "core_dw"
FABRIC_SQL_PROFILE_CONFIG  # e.g. "config"
```

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
    "FABRIC_SCOPE": "https://analysis.windows.net/powerbi/api/.default",
    "FABRIC_BASE_URL": "https://api.fabric.microsoft.com/v1",
    "FABRIC_DEFAULT_WORKSPACE_ID": "<your-workspace-guid>"
  }
}
```

Adjust the scope and base URL to whatever you actually use in your environment.

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

  or an `error` string if Azure CLI isn’t available / configured.

* **`reset_context(ctx)`**
  Clears all namespaced values (workspace, lakehouse, warehouse, etc.) for the current MCP client from the in-memory `context_store`.

---

### Workspace Tools

* **`list_workspaces(ctx)`**
  Lists all Fabric workspaces visible to the identity represented by the current MCP context.

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
  Executes a SQL query against the “active” SQL endpoint, resolved by:

  1. Warehouse (if `WAREHOUSE_NAMESPACE` is set)
  2. Lakehouse (if `LAKEHOUSE_NAMESPACE` is set)

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

### (Optional) Pipeline Rerun Tools

In `server.py` these are currently commented out but fully implemented in `tools/pipelines.py`:

* `rerun_pipeline_by_alias(...)`
* `rerun_pipeline_last_days(...)`

To enable:

1. Implement/plug in your real `FabricPipelineRunner` in `core/pipeline_runner.py`.
2. Uncomment the imports and tool definitions in `server.py`.
3. Ensure all pipeline-related env vars are set (see **Configuration**).

These helpers are designed for re-running Fabric pipelines over:

* an explicit `start_date` → `end_date` range, or
* the last `N` days.

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

  * Each MCP client gets its own workspace/lakehouse/warehouse selection.
  * Context is lost when the MCP server process restarts.

* HTTP calls are currently synchronous (`requests`) but wrapped with `asyncio.to_thread` so they can be safely used from async tools.

* `core/projects.py` is intentionally empty for now — a stub for future project-related helpers.

---

## License

Add your license information here (e.g. MIT, proprietary, etc.).
