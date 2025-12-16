from __future__ import annotations

import os
from dataclasses import dataclass


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


settings = Settings()
