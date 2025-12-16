from __future__ import annotations

from typing import Any, Dict

from mcp.server.fastmcp import Context

# Global in-memory context store, shared by all tools.
# Structure:
#   _CTX_STORE[client_key] = { namespace: value, ... }
_CTX_STORE: Dict[str, Dict[str, Any]] = {}


def _client_key(ctx: Context) -> str:
    """
    Compute the key for a given MCP client from Context.
    """
    client_id = getattr(ctx, "client_id", None) or "anonymous"
    return str(client_id)


def set_value(ctx: Context, namespace: str, value: Any) -> None:
    """
    Set a namespaced value for this MCP client.
    """
    key = _client_key(ctx)
    ns = _CTX_STORE.setdefault(key, {})
    ns[namespace] = value


def get_value(ctx: Context, namespace: str) -> Any | None:
    """
    Get a namespaced value for this MCP client.
    Returns None if not set.
    """
    key = _client_key(ctx)
    return _CTX_STORE.get(key, {}).get(namespace)


def get_all_for_client(ctx: Context) -> Dict[str, Any]:
    """
    Get a shallow copy of all namespaced values for this MCP client.
    """
    key = _client_key(ctx)
    return dict(_CTX_STORE.get(key, {}))


def clear_client(ctx: Context) -> None:
    """
    Clear all context for this MCP client.
    """
    key = _client_key(ctx)
    _CTX_STORE.pop(key, None)


def clear_all() -> None:
    """
    Clear context for all clients.
    """
    _CTX_STORE.clear()
