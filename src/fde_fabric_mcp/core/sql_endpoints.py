from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

import requests

from ..auth import get_credential

FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def _get_bearer() -> str:
    """
    Acquire a Fabric REST API bearer token using the same Azure credential
    you already use for SQL (DefaultAzureCredential / ClientSecretCredential, etc.).
    """
    cred = get_credential()
    token = cred.get_token(FABRIC_SCOPE)
    return token.token


def _fabric_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{FABRIC_BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {_get_bearer()}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    if resp.text:
        try:
            return resp.json()
        except ValueError:
            return {}
    return {}


def _looks_like_guid(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-fA-F-]{36}", s.strip()))


def resolve_workspace_name_and_id(workspace: str) -> Tuple[str, str]:
    """
    Resolve a workspace by name or ID.

    - If 'workspace' looks like a GUID, treat it as the ID and fetch name.
    - Otherwise, search by displayName.
    """
    w = workspace.strip()
    if _looks_like_guid(w):
        ws_id = w
        data = _fabric_get(f"/workspaces/{ws_id}")
        name = data.get("displayName") or data.get("name") or ws_id
        return name, ws_id

    # search by name
    all_ws = _fabric_get("/workspaces").get("value", [])
    for ws in all_ws:
        if str(ws.get("displayName", "")).lower() == w.lower():
            return ws.get("displayName"), ws.get("id")

    raise ValueError(f"Workspace '{workspace}' not found.")


def resolve_item_name_and_id(
    *,
    workspace_id: str,
    item: str,
    item_type: str,
) -> Tuple[str, str]:
    """
    Resolve a Lakehouse or Warehouse by name or ID within the given workspace.
    """
    items = _fabric_get(
        f"/workspaces/{workspace_id}/items",
        params={"type": item_type},
    ).get("value", [])

    it = item.strip()
    # If looks like GUID, try direct ID first
    if _looks_like_guid(it):
        for obj in items:
            if str(obj.get("id", "")).lower() == it.lower():
                return obj.get("displayName") or obj.get("id"), obj.get("id")

    # Otherwise match by displayName
    for obj in items:
        if str(obj.get("displayName", "")).lower() == it.lower():
            return obj.get("displayName"), obj.get("id")

    raise ValueError(f"{item_type} '{item}' not found in workspace {workspace_id}.")


def get_lakehouse_sql_endpoint(
    *,
    workspace: str,
    lakehouse: str,
) -> Dict[str, str]:
    """
    Resolve the SQL endpoint (server + database) for a Lakehouse.

    Uses:
      GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}
    and reads properties.sqlEndpointProperties.connectionString as the host.
    The database name is assumed to be the lakehouse displayName.
    """
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    lh_name, lh_id = resolve_item_name_and_id(
        workspace_id=workspace_id, item=lakehouse, item_type="Lakehouse"
    )

    lh = _fabric_get(f"/workspaces/{workspace_id}/lakehouses/{lh_id}")
    props = lh.get("properties", {}) or {}
    sql_props = props.get("sqlEndpointProperties", {}) or {}
    conn_str = sql_props.get("connectionString")

    if not conn_str:
        raise RuntimeError(
            f"No SQL endpoint connectionString found for lakehouse '{lh_name}' "
            f"in workspace '{workspace_name}'."
        )

    server = conn_str.strip()
    database = lh_name

    return {
        "workspace_name": workspace_name,
        "workspace_id": workspace_id,
        "item_name": lh_name,
        "item_id": lh_id,
        "server": server,
        "database": database,
        "type": "Lakehouse",
    }


def get_warehouse_sql_endpoint(
    *,
    workspace: str,
    warehouse: str,
) -> Dict[str, str]:
    """
    Resolve the SQL endpoint (server + database) for a Warehouse.

    Uses:
      GET /v1/workspaces/{workspaceId}/warehouses/{warehouseId}/connectionString
    which returns { "connectionString": "host" }.
    The database name is assumed to be the warehouse displayName.
    """
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    wh_name, wh_id = resolve_item_name_and_id(
        workspace_id=workspace_id, item=warehouse, item_type="Warehouse"
    )

    data = _fabric_get(
        f"/workspaces/{workspace_id}/warehouses/{wh_id}/connectionString"
    )
    conn_str = data.get("connectionString")
    if not conn_str:
        raise RuntimeError(
            f"No connectionString returned for warehouse '{wh_name}' "
            f"in workspace '{workspace_name}'."
        )

    server = conn_str.strip()
    database = wh_name

    return {
        "workspace_name": workspace_name,
        "workspace_id": workspace_id,
        "item_name": wh_name,
        "item_id": wh_id,
        "server": server,
        "database": database,
        "type": "Warehouse",
    }
