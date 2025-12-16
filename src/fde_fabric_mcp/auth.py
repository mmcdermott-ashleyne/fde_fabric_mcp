from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Optional, Union

from azure.core.credentials import TokenCredential
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from cachetools import TTLCache

from .config import settings

try:
    # When running under FastMCP
    from mcp.server.fastmcp import Context  # type: ignore
except Exception:  # pragma: no cover
    Context = object  # type: ignore


ScopeArg = Union[str, Iterable[str]]


@dataclass(frozen=True)
class AuthConfig:
    """
    Static auth configuration derived from environment variables.

    auth_mode:
        - "auto"         -> DefaultAzureCredential
        - "client_secret" -> ClientSecretCredential
    """

    auth_mode: str = settings.auth_mode
    tenant_id: str | None = os.getenv("FABRIC_TENANT_ID")
    client_id: str | None = os.getenv("FABRIC_CLIENT_ID")
    client_secret: str | None = os.getenv("FABRIC_CLIENT_SECRET")


_auth_config = AuthConfig()

# -----------------------------------------------------------------------------
# Global / per-client credential cache
# -----------------------------------------------------------------------------
_credential: TokenCredential | None = None  # legacy global cache

# Per-client credential cache. Keyed by "global" or ctx.client_id.
_CRED_CACHE: TTLCache[str, TokenCredential] = TTLCache(maxsize=128, ttl=3600)


def _cache_key_for_client(client_id: Optional[str]) -> str:
    """
    Build a cache key for a given client_id. If client_id is None, use 'global'.
    """
    return client_id or "global"


def _build_credential() -> TokenCredential:
    """
    Build an azure-identity credential based on FABRIC_AUTH_MODE and env vars.
    """
    if _auth_config.auth_mode == "client_secret":
        if not (
            _auth_config.tenant_id
            and _auth_config.client_id
            and _auth_config.client_secret
        ):
            raise RuntimeError(
                "FABRIC_AUTH_MODE=client_secret requires "
                "FABRIC_TENANT_ID, FABRIC_CLIENT_ID and FABRIC_CLIENT_SECRET."
            )
        return ClientSecretCredential(
            tenant_id=_auth_config.tenant_id,
            client_id=_auth_config.client_id,
            client_secret=_auth_config.client_secret,
        )

    # Default is "auto": use DefaultAzureCredential chain.
    return DefaultAzureCredential(exclude_interactive_browser_credential=False)


# -----------------------------------------------------------------------------
# Legacy global helpers (process-wide, non-context-aware)
# -----------------------------------------------------------------------------
def get_credential() -> TokenCredential:
    """
    Return a cached TokenCredential instance (process-global).
    """
    global _credential
    if _credential is None:
        _credential = _build_credential()
    return _credential


def _normalize_scope(scope: ScopeArg) -> str:
    if isinstance(scope, str):
        return scope
    return " ".join(scope)


def get_bearer_token(scope: ScopeArg) -> str:
    """
    Acquire a bearer token string for the given scope(s), using the global
    cached credential.
    """
    token = get_credential().get_token(_normalize_scope(scope))
    return token.token


# -----------------------------------------------------------------------------
# New per-client / per-context helpers
# -----------------------------------------------------------------------------
def get_credential_for_client(client_id: Optional[str]) -> TokenCredential:
    """
    Get a TokenCredential instance for a specific client_id, using a shared TTL cache.
    """
    key = _cache_key_for_client(client_id)
    cred = _CRED_CACHE.get(key)
    if cred is not None:
        return cred

    cred = _build_credential()
    _CRED_CACHE[key] = cred
    return cred


def get_credential_for_context(ctx: Context) -> TokenCredential: # type: ignore
    """
    Get a TokenCredential for a given FastMCP Context.
    """
    client_id = getattr(ctx, "client_id", None)
    return get_credential_for_client(client_id)


def get_bearer_token_for_client(scope: ScopeArg, client_id: Optional[str]) -> str:
    """
    Acquire a bearer token string for the given scope(s) using a per-client credential.
    """
    cred = get_credential_for_client(client_id)
    token = cred.get_token(_normalize_scope(scope))
    return token.token


def get_bearer_token_for_context(scope: ScopeArg, ctx: Context) -> str: # type: ignore
    """
    Acquire a bearer token string for the given scope(s) using a credential
    derived from the MCP Context (i.e., keyed by ctx.client_id).
    """
    cred = get_credential_for_context(ctx)
    token = cred.get_token(_normalize_scope(scope))
    return token.token
