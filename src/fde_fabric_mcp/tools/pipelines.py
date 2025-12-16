from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from ..core.pipeline_runner import build_runner_from_env


def rerun_by_alias_impl(
    profile: str,
    project_alias: str,
    start_date: str,
    end_date: str,
    time_utc: str,
    dry_run: bool,
) -> List[Dict[str, Any]]:
    """
    Rerun a Fabric pipeline over an explicit date range using a project alias.
    """
    runner = build_runner_from_env()
    results = runner.exec_range(
        s=start_date,
        e=end_date,
        t=time_utc,
        profile=profile,
        project_alias=project_alias,
        dry_run=dry_run,
    )

    output: List[Dict[str, Any]] = []
    for r in results:
        proj = r["payload"]["executionData"]["parameters"]["project"]
        output.append(
            {
                "interval": r["interval"],
                "status": r.get("status"),
                "project_alias": project_alias,
                "project_id": proj.get("project_id"),
                "data_interval": proj.get("data_interval_utc"),
                "dry_run": dry_run,
            }
        )
    return output


def rerun_last_days_impl(
    profile: str,
    project_alias: str,
    days: int,
    time_utc: str,
    dry_run: bool,
) -> List[Dict[str, Any]]:
    """
    Rerun a Fabric pipeline for the last N days using a project alias.
    """
    today = datetime.utcnow().date()
    start = today - timedelta(days=days - 1)
    return rerun_by_alias_impl(
        profile=profile,
        project_alias=project_alias,
        start_date=start.isoformat(),
        end_date=today.isoformat(),
        time_utc=time_utc,
        dry_run=dry_run,
    )
