from __future__ import annotations

import asyncio
import base64
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from mcp.server.fastmcp import Context

from ..clients.fabric import FabricClient
from .workspace import get_current_workspace, list_workspaces_impl


def _normalize_ipynb_name(ipynb_name: str, notebook_name: str) -> str:
    name = (ipynb_name or notebook_name or "").strip()
    if not name:
        raise ValueError("notebook_name must be provided.")
    return name if name.lower().endswith(".ipynb") else f"{name}.ipynb"


def _normalize_folder_path(folder_path: str | None) -> List[str]:
    if not folder_path:
        return []
    parts = [part for part in re.split(r"[\\/]+", folder_path.strip()) if part]
    return parts


def _get_item_parent_id(item: Dict[str, Any]) -> str | None:
    return (
        item.get("parentId")
        or item.get("parent_id")
        or item.get("folderId")
        or item.get("parentFolderId")
    )


def _matches_parent(item: Dict[str, Any], parent_id: str | None) -> bool:
    item_parent = _get_item_parent_id(item)
    if parent_id:
        return (item_parent or "").lower() == parent_id.lower()
    return not item_parent


_TEMPLATE_DIR = Path(__file__).resolve().parent / "notebook_templates"
_DEFAULT_NOTEBOOK_METADATA = {"language_info": {"name": "python"}}


def _normalize_source_lines(source: Any) -> List[str]:
    if isinstance(source, str):
        return source.splitlines()
    if isinstance(source, list):
        return [str(line) for line in source]
    return []


def _derive_description_from_cells(cells: List[Dict[str, Any]]) -> str | None:
    for cell in cells:
        if cell.get("cell_type") != "markdown":
            continue
        lines = _normalize_source_lines(cell.get("source"))
        for line in lines:
            text = line.strip()
            if not text:
                continue
            text = re.sub(r"^#+\s*", "", text)
            if text:
                return text
    return None


def _extract_description(
    template_data: Dict[str, Any], cells: List[Dict[str, Any]]
) -> str:
    description = template_data.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()

    metadata = template_data.get("metadata")
    if isinstance(metadata, dict):
        metadata_description = metadata.get("description")
        if isinstance(metadata_description, str) and metadata_description.strip():
            return metadata_description.strip()

    derived = _derive_description_from_cells(cells)
    return derived or "Notebook template."


def _parse_template_data(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Template {path.name} must be a JSON object.")

    cells = data.get("cells")
    if not isinstance(cells, list):
        raise ValueError(f"Template {path.name} must define cells.")
    if not all(isinstance(cell, dict) for cell in cells):
        raise ValueError(f"Template {path.name} has invalid cell data.")

    template: Dict[str, Any] = {
        "description": _extract_description(data, cells),
        "cells": cells,
    }

    metadata = data.get("metadata")
    if isinstance(metadata, dict):
        template["metadata"] = metadata

    nbformat = data.get("nbformat")
    if isinstance(nbformat, int):
        template["nbformat"] = nbformat

    nbformat_minor = data.get("nbformat_minor")
    if isinstance(nbformat_minor, int):
        template["nbformat_minor"] = nbformat_minor

    return template


def _load_notebook_templates() -> Dict[str, Dict[str, Any]]:
    if not _TEMPLATE_DIR.is_dir():
        raise RuntimeError(
            f"Notebook template folder not found: {_TEMPLATE_DIR}"
        )

    templates: Dict[str, Dict[str, Any]] = {}
    paths = sorted(
        list(_TEMPLATE_DIR.glob("*.json"))
        + list(_TEMPLATE_DIR.glob("*.ipynb")),
        key=lambda item: item.name.lower(),
    )
    for path in paths:
        template_name = path.stem
        if template_name in templates:
            raise ValueError(
                f"Template name '{template_name}' is duplicated."
            )
        templates[template_name] = _parse_template_data(path)

    if not templates:
        raise RuntimeError(f"No notebook templates found in {_TEMPLATE_DIR}")

    return templates


_NOTEBOOK_TEMPLATES = _load_notebook_templates()


def _build_notebook_payload(template_name: str) -> Dict[str, Any]:
    template = _NOTEBOOK_TEMPLATES.get(template_name)
    if not template:
        available = ", ".join(sorted(_NOTEBOOK_TEMPLATES.keys()))
        raise ValueError(f"Unknown template '{template_name}'. Available: {available}")
    metadata = template.get("metadata")
    if not isinstance(metadata, dict):
        metadata = _DEFAULT_NOTEBOOK_METADATA
    nbformat = template.get("nbformat")
    if not isinstance(nbformat, int):
        nbformat = 4
    nbformat_minor = template.get("nbformat_minor")
    if not isinstance(nbformat_minor, int):
        nbformat_minor = 5
    return {
        "nbformat": nbformat,
        "nbformat_minor": nbformat_minor,
        "metadata": metadata,
        "cells": template["cells"],
    }


def _encode_ipynb(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, indent=2)
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


async def _resolve_workspace(ctx: Context, workspace: str) -> Dict[str, Any]:
    if not workspace:
        raise ValueError("workspace is required.")

    all_ws = await list_workspaces_impl(ctx)
    chosen = next(
        (w for w in all_ws if (w.get("id") or "").lower() == workspace.lower()),
        None,
    )

    if not chosen:
        chosen = next(
            (
                w
                for w in all_ws
                if (w.get("displayName") or "").lower() == workspace.lower()
            ),
            None,
        )

    if not chosen:
        raise ValueError(f"Workspace '{workspace}' not found.")

    return chosen


async def _list_folder_items(
    ctx: Context,
    workspace_id: str,
) -> List[Dict[str, Any]]:
    client = FabricClient.from_context(ctx)
    data = await asyncio.to_thread(
        client.get,
        f"/workspaces/{workspace_id}/items",
        {"type": "Folder"},
    )
    return data.get("value", [])


async def _create_folder_item(
    ctx: Context,
    workspace_id: str,
    folder_name: str,
    parent_id: str | None,
) -> Dict[str, Any]:
    client = FabricClient.from_context(ctx)
    payload: Dict[str, Any] = {
        "displayName": folder_name,
        "type": "Folder",
    }
    if parent_id:
        payload["parentId"] = parent_id
    return await asyncio.to_thread(
        client.post, f"/workspaces/{workspace_id}/items", payload
    )


async def _resolve_folder_id(
    ctx: Context,
    workspace_id: str,
    folder_path: str,
    create_missing_folders: bool,
) -> str | None:
    parts = _normalize_folder_path(folder_path)
    if not parts:
        return None

    folder_items = await _list_folder_items(ctx, workspace_id)
    parent_id: str | None = None

    for part in parts:
        match = next(
            (
                item
                for item in folder_items
                if (item.get("displayName") or "").lower() == part.lower()
                and _matches_parent(item, parent_id)
            ),
            None,
        )

        if not match:
            if not create_missing_folders:
                raise ValueError(
                    f"Folder path '{folder_path}' not found in workspace."
                )
            created = await _create_folder_item(
                ctx, workspace_id, part, parent_id
            )
            match = {
                "id": created.get("id"),
                "displayName": created.get("displayName") or part,
                "parentId": parent_id,
            }
            folder_items.append(match)

        parent_id = match.get("id")
        if not parent_id:
            raise RuntimeError(f"Unable to resolve folder id for '{part}'.")

    return parent_id


async def list_notebooks_impl(
    ctx: Context, workspace: str | None = None
) -> List[Dict[str, Any]]:
    """
    List notebooks in a workspace (by name or ID).
    """
    if workspace:
        ws = await _resolve_workspace(ctx, workspace)
    else:
        ws = await get_current_workspace(ctx)
    client = FabricClient.from_context(ctx)
    data = await asyncio.to_thread(
        client.get,
        f"/workspaces/{ws['id']}/items",
        {"type": "Notebook"},
    )

    return [
        {
            "id": item.get("id"),
            "displayName": item.get("displayName"),
            "type": item.get("type"),
        }
        for item in data.get("value", [])
    ]


async def get_notebook_impl(
    ctx: Context,
    workspace: str | None,
    notebook_id: str,
) -> Dict[str, Any]:
    """
    Get a specific notebook by ID in a workspace.
    """
    if not notebook_id:
        raise ValueError("notebook_id is required.")

    if workspace:
        ws = await _resolve_workspace(ctx, workspace)
    else:
        ws = await get_current_workspace(ctx)
    client = FabricClient.from_context(ctx)
    return await asyncio.to_thread(
        client.get, f"/workspaces/{ws['id']}/items/{notebook_id}"
    )


async def create_notebook_impl(
    ctx: Context,
    workspace: str | None,
    notebook_name: str,
    content: str,
    ipynb_name: str | None = None,
    folder_path: str | None = None,
    folder_id: str | None = None,
    create_missing_folders: bool = False,
) -> Dict[str, Any]:
    """
    Create a new notebook in the given workspace.

    The content should be a base64-encoded .ipynb payload.
    """
    if workspace:
        ws = await _resolve_workspace(ctx, workspace)
    else:
        ws = await get_current_workspace(ctx)
    client = FabricClient.from_context(ctx)

    file_name = _normalize_ipynb_name(ipynb_name or "", notebook_name)

    resolved_folder_id = folder_id
    if not resolved_folder_id and folder_path:
        resolved_folder_id = await _resolve_folder_id(
            ctx,
            workspace_id=ws["id"],
            folder_path=folder_path,
            create_missing_folders=create_missing_folders,
        )

    payload = {
        "displayName": notebook_name,
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": file_name,
                    "payload": content,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }
    if resolved_folder_id:
        payload["parentId"] = resolved_folder_id

    return await asyncio.to_thread(
        client.post, f"/workspaces/{ws['id']}/items", payload
    )


async def list_notebook_templates_impl() -> List[Dict[str, str]]:
    return [
        {"name": name, "description": template["description"]}
        for name, template in _NOTEBOOK_TEMPLATES.items()
    ]


async def create_template_notebook_impl(
    ctx: Context,
    workspace: str | None,
    notebook_name: str,
    template_name: str,
    ipynb_name: str | None = None,
    folder_path: str | None = None,
    folder_id: str | None = None,
    create_missing_folders: bool = False,
) -> Dict[str, Any]:
    """
    Create a new notebook from a built-in template.
    """
    payload = _build_notebook_payload(template_name)
    content = _encode_ipynb(payload)
    return await create_notebook_impl(
        ctx,
        workspace=workspace,
        notebook_name=notebook_name,
        content=content,
        ipynb_name=ipynb_name,
        folder_path=folder_path,
        folder_id=folder_id,
        create_missing_folders=create_missing_folders,
    )
