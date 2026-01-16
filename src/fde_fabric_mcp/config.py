from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_csv(name: str, default: str) -> tuple[str, ...]:
    raw = os.getenv(name, default)
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return tuple(items)


@dataclass(frozen=True)
class Settings:
    """
    Package-wide configuration loaded from environment variables.
    """

    # Auth mode for azure-identity
    # "auto"         → DefaultAzureCredential (az login, MI, env, VSCode, etc.)
    # "client_secret" → ClientSecretCredential (FABRIC_TENANT_ID / CLIENT_ID / CLIENT_SECRET)
    auth_mode: str = os.getenv("FABRIC_AUTH_MODE", "auto")

    # Fabric API scope (for AAD tokens)
    fabric_scope: str = os.getenv(
        "FABRIC_SCOPE",
        "https://api.fabric.microsoft.com/.default",
    )

    # Fabric REST base URL
    fabric_base_url: str = os.getenv(
        "FABRIC_BASE_URL",
        "https://api.fabric.microsoft.com/v1",
    )

    # Default workspace if user does not set one
    default_workspace_id: str | None = os.getenv("FABRIC_DEFAULT_WORKSPACE_ID")

    # Optional SQL driver override
    sql_default_driver: str = os.getenv(
        "FABRIC_SQL_DRIVER",
        "{ODBC Driver 17 for SQL Server}",
    )

    # Optional configuration database settings (used by pipeline helpers)
    config_workspace: str = "fde_core_data_config"
    config_warehouse: str = "core_dw_config"
    config_database: str = "core_dw_config"
    config_table: str = "dbo.environment_config"

    # Optional SQL guardrails for MCP callers
    read_only_sql_enabled: bool = _env_bool("FABRIC_READ_ONLY_SQL_ENABLED", True)
    read_only_workspace_names: tuple[str, ...] = _env_csv(
        "FABRIC_READ_ONLY_WORKSPACE_NAMES",
        "fde_core_data_prod,fde_core_data_stg,fde_core_data_dev",
    )
    read_only_workspace_ids: tuple[str, ...] = _env_csv(
        "FABRIC_READ_ONLY_WORKSPACE_IDS",
        "",
    )


settings = Settings()
