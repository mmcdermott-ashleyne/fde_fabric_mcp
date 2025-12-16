from __future__ import annotations

import json
import subprocess
from functools import lru_cache
from shutil import which
from typing import Any, Dict


class AzAccountError(RuntimeError):
    """Raised when Azure CLI account information cannot be retrieved."""


@lru_cache(maxsize=1)
def _raw_az_account() -> Dict[str, Any]:
    """
    Call `az account show --output json` and return the parsed JSON.

    Raises:
        AzAccountError if:
        - Azure CLI is not found on PATH, or
        - `az account show` exits with a non-zero code, or
        - the output is not valid JSON.
    """
    az_path = which("az")
    if not az_path:
        raise AzAccountError(
            "Azure CLI executable 'az' was not found on PATH. "
            "Make sure Azure CLI is installed and that the MCP server "
            "is started from a shell where `az account show` works."
        )

    # Run `az account show`
    result = subprocess.run(
        [az_path, "account", "show", "--output", "json"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise AzAccountError(
            f"`az account show` failed with exit code {result.returncode}. "
            f"Stderr: {stderr}"
        )

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise AzAccountError(
            f"Failed to parse JSON from `az account show`: {e}"
        ) from e


def get_username_email() -> str | None:
    """
    Return the Azure account email (user.name) from `az account show`, or None
    if it cannot be determined.
    """
    try:
        return _raw_az_account().get("user", {}).get("name")
    except AzAccountError:
        return None


def get_username_short() -> str | None:
    """
    Return the short username (before '@') from the Azure account email,
    or None if it cannot be determined.
    """
    email = get_username_email()
    return email.split("@")[0] if email else None
