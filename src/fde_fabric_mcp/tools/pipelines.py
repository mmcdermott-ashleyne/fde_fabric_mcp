from __future__ import annotations

from dataclasses import dataclass
import asyncio
import json
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from croniter import croniter
from mcp.server.fastmcp import Context
from zoneinfo import ZoneInfo
import re

from ..clients.pipeline import FabricPipelineClient
from ..clients.sql import get_sql_connection
from ..config import settings
from ..core import context_store
from ..core.sql_endpoints import get_warehouse_sql_endpoint
from .projects import get_current_project_impl
from .workspace import (
    get_current_workspace as _get_current_workspace,
    list_workspaces_impl as _list_workspaces_impl,
)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
WAREHOUSE_NAMESPACE = "warehouse"
LAKEHOUSE_NAMESPACE = "lakehouse"

_TZ_FALLBACK = "America/New_York"
_CRON_TZ_RE = re.compile(r"^\s*(?:CRON_TZ|TZ)\s*=\s*([A-Za-z_/\-+]+)\s+(.+)$")

_FABRIC_MONITOR_BASE = "https://app.fabric.microsoft.com/workloads/data-pipeline/monitoring"

# -----------------------------------------------------------------------------
# Cron helpers
# -----------------------------------------------------------------------------
def _parse_cron_tz(cron_expr_raw: str) -> Tuple[str, str]:
    raw = (cron_expr_raw or "").strip()
    m = _CRON_TZ_RE.match(raw)
    if m:
        return m.group(1), m.group(2).strip()
    return _TZ_FALLBACK, raw


def _scheduled_utc_for_day(cron_expr_raw: str, day: date) -> datetime:
    """
    Returns the FIRST cron occurrence on the given *local day* (cron tz),
    converted to UTC (aware datetime).
    """
    tz_name, cron_expr = _parse_cron_tz(cron_expr_raw)
    if not cron_expr:
        raise ValueError("cron_expression is empty.")

    tz = ZoneInfo(tz_name)

    # Start-of-day local
    local_start = datetime(day.year, day.month, day.day, 0, 0, tzinfo=tz)

    # Get first occurrence on/after local_start (croniter uses base time)
    it = croniter(cron_expr, local_start - timedelta(minutes=1))
    next_local = it.get_next(datetime).replace(second=0, microsecond=0)

    # Ensure it actually lands on this local calendar day
    if next_local.date() != local_start.date():
        raise ValueError(
            f"No cron occurrence on local date {day.isoformat()} for cron {cron_expr_raw!r}"
        )

    return next_local.astimezone(timezone.utc)


# -----------------------------------------------------------------------------
# Simple conversions / formatting
# -----------------------------------------------------------------------------
def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _to_time(value: str | dtime) -> dtime:
    if isinstance(value, dtime):
        return value
    return datetime.strptime(value, "%H:%M").time()


def _fmt_min(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


def _fmt_sec(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# -----------------------------------------------------------------------------
# JSON / env helpers
# -----------------------------------------------------------------------------
def _json_maybe_stringify(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            obj = json.loads(stripped)
            return json.dumps(obj, separators=(",", ":"))
        except json.JSONDecodeError:
            return json.dumps({"raw": stripped}, separators=(",", ":"))

    return json.dumps(value, separators=(",", ":"))


def _dict_from_json(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _merge_env_blocks(
    base: Optional[Dict[str, Any]],
    override: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not base and not override:
        return None
    merged: Dict[str, Any] = {}
    if base:
        merged.update(base)
    if override:
        merged.update(override)
    return merged or None


def _parameters_lookup(value: Any) -> Dict[str, Any]:
    return _dict_from_json(value) or {}


def _fabric_monitor_url(workspace_id: str, instance_id: str) -> str:
    return f"{_FABRIC_MONITOR_BASE}/workspaces/{workspace_id}/pipelines/{instance_id}"


# -----------------------------------------------------------------------------
# SQL target resolution (warehouse preferred)
# -----------------------------------------------------------------------------
def _resolve_sql_target(ctx: Context) -> Dict[str, Any]:
    """
    Pick the active SQL target for this client:
    1) Warehouse (preferred)
    2) Lakehouse
    """
    wh = context_store.get_value(ctx, WAREHOUSE_NAMESPACE)
    if wh:
        return {"kind": "warehouse", **wh}
    lh = context_store.get_value(ctx, LAKEHOUSE_NAMESPACE)
    if lh:
        return {"kind": "lakehouse", **lh}
    raise RuntimeError(
        "No SQL endpoint configured. Set a warehouse via set_warehouse_from_fabric() "
        "or set a lakehouse via set_lakehouse() first."
    )


# -----------------------------------------------------------------------------
# Routing / environment config
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PipelineRoute:
    pipeline_name: str
    item_id_env_key: str
    workspace_id_env_key: Optional[str] = None


DEFAULT_ROUTES: Dict[str, PipelineRoute] = {
    "sftp": PipelineRoute("core_copy_sftp_processor", "copy_sftp_processor"),
    "notebook": PipelineRoute("core_notebook_processor", "notebook_processor_id"),
    "pbi_refresh": PipelineRoute("core_pbi_refresh_processor", "pbi_refresh_processor_id"),
    "lh_to_sql": PipelineRoute("lh_to_sql_processor", "lh_to_sql_processor"),
}

_WORKSPACE_STAGE_PREFIX = "fde_core_data_"
_STAGE_LOOKUP = {"dev", "stg", "prod"}


def _format_table_identifier(table_name: str) -> str:
    normalized = (table_name or "").strip()
    if not normalized:
        raise ValueError("config_table must be set and non-empty.")
    parts = [p.strip(" []") for p in normalized.split(".") if p.strip()]
    if not parts:
        raise ValueError("config_table must contain at least one identifier.")
    return ".".join(f"[{part}]" for part in parts)


def _parse_stage_from_workspace_name(display_name: str) -> str:
    if not display_name:
        raise ValueError("Workspace displayName is required to derive the stage name.")
    normalized = display_name.strip().lower()
    if not normalized.startswith(_WORKSPACE_STAGE_PREFIX):
        raise ValueError(
            "Workspace displayName must start with "
            f"`{_WORKSPACE_STAGE_PREFIX}` to derive the stage."
        )
    stage = normalized[len(_WORKSPACE_STAGE_PREFIX) :]
    if stage not in _STAGE_LOOKUP:
        raise ValueError(
            f"Workspace stage '{stage}' is not one of the supported stages: {sorted(_STAGE_LOOKUP)}."
        )
    return stage


async def _resolve_stage_from_workspace(ctx: Context) -> str:
    workspace = await _get_current_workspace(ctx)
    display_name = workspace.get("displayName")

    if not display_name:
        # Fall back to listing & matching by id
        all_workspaces = await _list_workspaces_impl(ctx)
        workspace_id = workspace.get("id")
        matched = next(
            (w for w in all_workspaces if (w.get("id") or "").lower() == (workspace_id or "").lower()),
            None,
        )
        display_name = matched.get("displayName") if matched else None

    if not display_name:
        raise RuntimeError("Could not resolve the workspace display name to derive the stage.")

    return _parse_stage_from_workspace_name(display_name)


async def _load_environment_config(ctx: Context, stage: str) -> Dict[str, Any]:
    # Always use stable config workspace, never the active warehouse's server.
    if not settings.config_workspace:
        raise RuntimeError(
            "FABRIC_CONFIG_WORKSPACE_ID is not set. "
            "It must point to the workspace that contains the config warehouse."
        )

    config_wh = settings.config_warehouse or settings.config_database

    info = await asyncio.to_thread(
        get_warehouse_sql_endpoint,
        workspace=settings.config_workspace,
        warehouse=config_wh,
    )

    server = info.get("server")
    database = info.get("database")
    if not server or not database:
        raise RuntimeError("Could not resolve config warehouse SQL endpoint (server/database).")

    table_ident = _format_table_identifier(settings.config_table)

    sql = (
        f"SELECT TOP (1) * "
        f"FROM {table_ident} "
        f"WHERE LOWER([stage]) = ? "
        f"ORDER BY 1;"
    )

    conn = get_sql_connection(server=server, database=database)
    df = await asyncio.to_thread(conn.run_query, sql, [stage.lower()])

    if df.empty:
        raise RuntimeError(f"No environment_config row found for stage '{stage}'.")

    row = df.iloc[0]
    env_config: Dict[str, Any] = {}

    for key, value in row.items():
        if pd.isna(value):
            env_config[key] = None
        elif isinstance(value, (bytes, bytearray)):
            env_config[key] = value.decode("utf-8", errors="ignore")
        elif hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
            try:
                env_config[key] = value.item()
            except Exception:
                env_config[key] = str(value)
        else:
            env_config[key] = value

    return env_config


def _resolve_pipeline_route(project_meta: Dict[str, Any]) -> PipelineRoute:
    project_type = (project_meta.get("project_type") or "").strip().lower()
    route = DEFAULT_ROUTES.get(project_type)
    if not route:
        raise RuntimeError(f"No pipeline route configured for project_type '{project_type}'.")
    return route


def _resolve_route_workspace_id(route: PipelineRoute, env_config: Dict[str, Any]) -> str:
    key = route.workspace_id_env_key or "workspace_id"
    value = env_config.get(key)
    if not value:
        raise RuntimeError(f"Environment config is missing the workspace id for key '{key}'.")
    return str(value)


def _resolve_route_pipeline_id(route: PipelineRoute, env_config: Dict[str, Any]) -> str:
    value = env_config.get(route.item_id_env_key)
    if not value:
        raise RuntimeError(
            f"Environment config is missing the pipeline id for key '{route.item_id_env_key}'."
        )
    return str(value)


# -----------------------------------------------------------------------------
# Project pipeline: build payload
# -----------------------------------------------------------------------------
def _build_payload(
    *,
    project_meta: Dict[str, Any],
    env_block: Optional[Dict[str, Any]],
    interval_dt: datetime,
    pipeline_name: Optional[str],
    parameters_override: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    offset_min = int(project_meta.get("interval_offset_min") or 0)
    interval_dt_utc = interval_dt.astimezone(timezone.utc)
    data_interval = interval_dt_utc - timedelta(minutes=offset_min)

    project_payload: Dict[str, Any] = {
        "project_id": str(project_meta["project_id"]),
        "project_type": str(project_meta.get("project_type") or "").lower(),
        "interval_offset_min": offset_min,
        "data_interval_utc": _fmt_sec(data_interval),
    }

    params_value = parameters_override if parameters_override is not None else project_meta.get("parameters")
    params_str = _json_maybe_stringify(params_value)
    if params_str is not None:
        project_payload["parameters"] = params_str

    payload: Dict[str, Any] = {
        "executionData": {
            "parameters": {
                "project": project_payload,
                "interval": _fmt_min(interval_dt),
            }
        }
    }

    if pipeline_name:
        payload["executionData"]["pipelineName"] = pipeline_name

    if env_block:
        payload["executionData"]["parameters"]["env"] = env_block

    return payload


# -----------------------------------------------------------------------------
# Public: Build payload (dry_run) for the "current project"
# -----------------------------------------------------------------------------
async def build_project_pipeline_payload_impl(
    ctx: Context,
    *,
    start_date: str,
    time_utc: str = "cron",
    end_date: str | None = None,
    env_override: Optional[Dict[str, Any]] = None,
    parameters_override: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Build the Fabric pipeline payload for the current project using the
    environment_config row that matches the active stage.
    """
    stage = await _resolve_stage_from_workspace(ctx)
    env_config = await _load_environment_config(ctx, stage)

    project_meta = await get_current_project_impl(ctx)
    route = _resolve_pipeline_route(project_meta)

    resolved_workspace_id = _resolve_route_workspace_id(route, env_config)
    resolved_pipeline_id = _resolve_route_pipeline_id(route, env_config)
    final_env = _merge_env_blocks(env_config, env_override)

    return await run_project_pipeline_impl(
        ctx,
        start_date=start_date,
        time_utc=time_utc,
        end_date=end_date,
        workspace_id=resolved_workspace_id,
        pipeline_id=resolved_pipeline_id,
        pipeline_name=route.pipeline_name,
        env=final_env,
        parameters_override=parameters_override,
        dry_run=True,
    )


# -----------------------------------------------------------------------------
# Public: Run project pipeline (supports cron-derived times)
# -----------------------------------------------------------------------------
async def run_project_pipeline_impl(
    ctx: Context,
    *,
    start_date: str,
    time_utc: str,
    end_date: str | None = None,
    workspace_id: str | None = None,
    pipeline_id: str | None = None,
    pipeline_name: str | None = None,
    env: Optional[Dict[str, Any]] = None,
    parameters_override: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """
    Submit a Fabric pipeline job for the current project stored in context.

    If time_utc == "cron" (or blank), derive the day's time from project's cron_expression
    (evaluated in America/New_York unless TZ/CRON_TZ is provided in the expression).
    """
    project_meta = await get_current_project_impl(ctx)
    project_params = _parameters_lookup(project_meta.get("parameters"))

    stage = await _resolve_stage_from_workspace(ctx)
    env_config = await _load_environment_config(ctx, stage)

    route = _resolve_pipeline_route(project_meta)
    route_workspace_id = _resolve_route_workspace_id(route, env_config)
    route_pipeline_id = _resolve_route_pipeline_id(route, env_config)

    resolved_workspace_id = workspace_id or project_params.get("workspace_id") or project_meta.get("workspace_id") or settings.default_workspace_id or route_workspace_id
    resolved_pipeline_id = pipeline_id or project_params.get("pipeline_id") or project_params.get("pipeline_item_id") or project_params.get("item_id") or project_meta.get("pipeline_id") or project_meta.get("pipeline_item_id") or project_meta.get("item_id") or route_pipeline_id

    if not resolved_workspace_id:
        raise ValueError("Could not determine workspace_id (set on project or pass workspace_id).")
    if not resolved_pipeline_id:
        raise ValueError("Could not determine pipeline_id (set on project parameters or pass pipeline_id).")

    start = _to_date(start_date)
    end = _to_date(end_date) if end_date else start
    if end < start:
        raise ValueError("end_date must be the same or after start_date")

    cron_expr = (project_meta.get("cron_expression") or "").strip()
    use_cron = (not time_utc) or (str(time_utc).strip().lower() == "cron")
    timeslot = None if use_cron else _to_time(time_utc)

    project_env = _dict_from_json(project_meta.get("env"))
    env_block = _merge_env_blocks(_merge_env_blocks(env_config, project_env), env)

    client = FabricPipelineClient.from_context(ctx, workspace_id=str(resolved_workspace_id), pipeline_id=str(resolved_pipeline_id))

    results: List[Dict[str, Any]] = []
    current = start

    while current <= end:
        if use_cron:
            if not cron_expr:
                raise RuntimeError("time_utc='cron' but project has no cron_expression.")
            interval_dt = _scheduled_utc_for_day(cron_expr, current)  # aware UTC
        else:
            interval_dt = datetime.combine(current, timeslot, tzinfo=timezone.utc)

        payload = _build_payload(
            project_meta=project_meta,
            env_block=env_block,
            interval_dt=interval_dt,
            pipeline_name=pipeline_name,
            parameters_override=parameters_override,
        )

        entry: Dict[str, Any] = {
            "interval": _fmt_min(interval_dt),
            "workspace_id": str(resolved_workspace_id),
            "pipeline_id": str(resolved_pipeline_id),
            "project_alias": project_meta.get("project_alias"),
            "dry_run": dry_run,
            "payload": payload,
        }

        if dry_run:
            entry["status"] = "dry_run"
        else:
            resp = await asyncio.to_thread(client.submit_run, payload)
            entry.update(resp)
            instance_id = entry.get("instance_id")
            if instance_id:
                entry["monitor_url"] = _fabric_monitor_url(str(resolved_workspace_id), str(instance_id))

        results.append(entry)
        current += timedelta(days=1)

    return results


# -----------------------------------------------------------------------------
# Orchestrator helpers
# -----------------------------------------------------------------------------
def _build_orchestrator_invoke_payload(
    *,
    interval_utc: str,
    env_block: Dict[str, Any],
    is_current: int = 1,
) -> Dict[str, Any]:
    return {
        "waitOnCompletion": False,
        "operationType": "InvokeFabricPipeline",
        "pipelineId": str(env_block["project_orchestrator_id"]),
        "workspaceId": str(env_block["workspace_id"]),
        "parameters": {
            "interval": interval_utc,
            "is_current": is_current,
            "env": env_block,
        },
    }


# -----------------------------------------------------------------------------
# Public: list run intervals for day + mapping for logging
# -----------------------------------------------------------------------------
async def list_run_intervals_for_day_impl(ctx: Context, run_date: str) -> Dict[str, Any]:
    """
    Return distinct UTC run intervals for a given date based on active projects'
    cron_expression. Also returns interval -> projects mapping for logging.

    Cron expressions are evaluated in America/New_York unless TZ/CRON_TZ is provided.
    """
    d = _to_date(run_date)

    target = _resolve_sql_target(ctx)
    server = target["server"]
    database = target["database"]
    conn = get_sql_connection(server=server, database=database)

    sql = """
    SELECT
        project_id,
        project_alias,
        cron_expression
    FROM [dbo].[project]
    WHERE is_active = 1
    """

    df = await asyncio.to_thread(conn.run_query, sql)

    if df is None or df.empty:
        return {
            "run_date": d.isoformat(),
            "run_intervals_utc": [],
            "interval_projects": {},
            "project_count": 0,
            "errors": [],
            "sql_target": {"kind": target.get("kind"), "server": server, "database": database},
        }

    interval_projects: Dict[str, List[Dict[str, Any]]] = {}
    errors: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        project_id = row.get("project_id")
        project_alias = row.get("project_alias")
        cron_expr = (row.get("cron_expression") or "").strip()

        if not cron_expr:
            errors.append(
                {"project_id": project_id, "project_alias": project_alias, "error": "missing_cron_expression"}
            )
            continue

        try:
            dt_utc = _scheduled_utc_for_day(cron_expr, d)
            interval_str = _fmt_min(dt_utc)
            interval_projects.setdefault(interval_str, []).append(
                {"project_id": project_id, "project_alias": project_alias}
            )
        except Exception as e:
            errors.append({"project_id": project_id, "project_alias": project_alias, "error": str(e)})

    run_intervals_sorted = sorted(interval_projects.keys())

    return {
        "run_date": d.isoformat(),
        "run_intervals_utc": run_intervals_sorted,
        "interval_projects": interval_projects,
        "project_count": int(len(df)),
        "errors": errors,
        "sql_target": {"kind": target.get("kind"), "server": server, "database": database},
    }


# -----------------------------------------------------------------------------
# Public: run orchestrator for day with TWO-PHASE CONFIRMATION
# -----------------------------------------------------------------------------
def _normalize_interval_utc(interval_utc: str) -> str:
    """
    Accepts "YYYY-MM-DD HH:MM" (optionally with seconds) and returns "YYYY-MM-DD HH:MM".
    """
    s = (interval_utc or "").strip()
    if not s:
        raise ValueError("interval_utc is empty.")
    # Try with seconds then without
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return _fmt_min(dt)
        except ValueError:
            continue
    raise ValueError("interval_utc must be in format 'YYYY-MM-DD HH:MM' (UTC).")

async def run_project_orchestrator_for_day_impl(
    ctx: Context,
    run_date: str,
    *,
    interval_utc: str | None = None,   # NEW (optional)
    confirm: bool = False,
    dry_run: bool = False,
    is_current: int = 1,
    allow_missing_interval: bool = False,  # NEW (optional safety valve)
) -> Dict[str, Any]:
    """
    If interval_utc is provided, only runs the orchestrator for that single interval.
    Otherwise runs for all intervals for the day.
    """
    step2 = await list_run_intervals_for_day_impl(ctx, run_date)
    run_intervals: List[str] = step2.get("run_intervals_utc") or []
    interval_projects: Dict[str, List[Dict[str, Any]]] = step2.get("interval_projects") or {}

    # Filter to a single interval if requested
    selected_intervals = run_intervals
    selected_interval_projects = interval_projects

    if interval_utc:
        norm = _normalize_interval_utc(interval_utc)

        if (norm not in run_intervals) and (not allow_missing_interval):
            return {
                "status": "invalid_interval",
                "message": (
                    f"interval_utc '{norm}' is not a scheduled interval for {run_date}. "
                    "Pass allow_missing_interval=true to force a run anyway."
                ),
                "run_date": run_date,
                "requested_interval_utc": norm,
                "available_intervals_utc": run_intervals,
                "step2": step2,
            }

        selected_intervals = [norm]
        # If forcing a missing interval, expected projects will be empty
        selected_interval_projects = {norm: interval_projects.get(norm, [])}

    stage = await _resolve_stage_from_workspace(ctx)
    env_config = await _load_environment_config(ctx, stage)

    for k in ("workspace_id", "project_orchestrator_id"):
        if not env_config.get(k):
            raise RuntimeError(f"Environment config missing required key '{k}'.")

    orchestrator_workspace_id = str(env_config["workspace_id"])
    orchestrator_pipeline_id = str(env_config["project_orchestrator_id"])

    total_projects = sum(len(v) for v in selected_interval_projects.values())

    # Confirmation gate
    if not confirm:
        return {
            "status": "confirmation_required",
            "message": (
                f"This will submit {len(selected_intervals)} orchestrator run(s) for {run_date} "
                f"({total_projects} expected projects). Re-run with confirm=true to proceed."
            ),
            "run_date": run_date,
            "stage": stage,
            "run_intervals_utc": selected_intervals,
            "interval_projects": selected_interval_projects,
            "orchestrator": {
                "workspace_id": orchestrator_workspace_id,
                "pipeline_id": orchestrator_pipeline_id,
            },
            "next_step": {
                "params": {
                    "run_date": run_date,
                    "interval_utc": selected_intervals[0] if interval_utc else None,
                    "confirm": True,
                    "dry_run": dry_run,
                    "is_current": is_current,
                    "allow_missing_interval": allow_missing_interval,
                }
            },
            "step2": step2,
        }

    client = FabricPipelineClient.from_context(
        ctx,
        workspace_id=orchestrator_workspace_id,
        pipeline_id=orchestrator_pipeline_id,
    )

    results: List[Dict[str, Any]] = []

    for interval_str in selected_intervals:
        payload = _build_orchestrator_invoke_payload(
            interval_utc=interval_str,
            env_block=env_config,
            is_current=is_current,
        )

        expected_projects = selected_interval_projects.get(interval_str, [])

        entry: Dict[str, Any] = {
            "run_date": run_date,
            "interval": interval_str,
            "workspace_id": orchestrator_workspace_id,
            "pipeline_id": orchestrator_pipeline_id,
            "confirm": confirm,
            "dry_run": dry_run,
            "payload": payload,
            "expected_projects": expected_projects,
            "expected_project_count": len(expected_projects),
        }

        if dry_run:
            entry["status"] = "dry_run"
        else:
            resp = await asyncio.to_thread(client.submit_run, payload)
            entry.update(resp)
            instance_id = entry.get("instance_id")
            if instance_id:
                entry["monitor_url"] = _fabric_monitor_url(orchestrator_workspace_id, str(instance_id))

        results.append(entry)

    return {
        "status": "submitted" if not dry_run else "dry_run",
        "run_date": run_date,
        "stage": stage,
        "run_intervals_utc": selected_intervals,
        "interval_projects": selected_interval_projects,
        "orchestrator": {
            "workspace_id": orchestrator_workspace_id,
            "pipeline_id": orchestrator_pipeline_id,
        },
        "count": len(results),
        "results": results,
        "step2": step2,
    }