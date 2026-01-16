from __future__ import annotations

import re
from typing import Any, Dict

from ..config import settings

_BLOCKED_KEYWORDS = re.compile(
    r"\b("
    r"INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|DENY|"
    r"EXEC|EXECUTE|CALL|INTO"
    r")\b",
    re.IGNORECASE,
)


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def is_read_only_target(target: Dict[str, Any]) -> bool:
    if not settings.read_only_sql_enabled:
        return False

    workspace_name = _normalize(target.get("workspace_name"))
    workspace_id = _normalize(target.get("workspace_id"))
    names = {_normalize(name) for name in settings.read_only_workspace_names}
    ids = {_normalize(ws_id) for ws_id in settings.read_only_workspace_ids}

    if workspace_name and workspace_name in names:
        return True
    if workspace_id and workspace_id in ids:
        return True
    return False


def _strip_sql_comments_and_literals(sql: str) -> str:
    buf: list[str] = []
    i = 0
    length = len(sql)

    while i < length:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""

        if ch == "-" and nxt == "-":
            buf.append(" ")
            i += 2
            while i < length and sql[i] not in "\r\n":
                i += 1
            continue

        if ch == "/" and nxt == "*":
            buf.append(" ")
            i += 2
            while i < length - 1 and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            if i < length - 1:
                i += 2
            continue

        if ch == "'":
            buf.append(" ")
            i += 1
            while i < length:
                if sql[i] == "'" and i + 1 < length and sql[i + 1] == "'":
                    i += 2
                    continue
                if sql[i] == "'":
                    i += 1
                    break
                i += 1
            continue

        if ch == '"':
            buf.append(" ")
            i += 1
            while i < length:
                if sql[i] == '"' and i + 1 < length and sql[i + 1] == '"':
                    i += 2
                    continue
                if sql[i] == '"':
                    i += 1
                    break
                i += 1
            continue

        if ch == "[":
            buf.append(" ")
            i += 1
            while i < length and sql[i] != "]":
                i += 1
            if i < length and sql[i] == "]":
                i += 1
            continue

        buf.append(ch)
        i += 1

    return "".join(buf)


def is_read_only_sql(sql: str) -> bool:
    cleaned = _strip_sql_comments_and_literals(sql)
    upper = cleaned.upper()

    first_match = re.search(r"[A-Z]+", upper)
    if not first_match:
        return True

    first = first_match.group(0)
    if first not in {"SELECT", "WITH"}:
        return False

    if _BLOCKED_KEYWORDS.search(upper):
        return False

    return True


def validate_read_only_sql(sql: str) -> None:
    if not is_read_only_sql(sql):
        raise RuntimeError(
            "MCP SQL guardrail: write operations are blocked for this workspace. "
            "Only read-only SELECT/CTE queries are allowed."
        )
