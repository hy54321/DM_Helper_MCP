"""
ProtoQuery MCP Server.

Exposes data-migration tools (catalog, profiling, comparison,
reports, SQL preview) via FastMCP (stdio transport).
"""

import json
import os
import hmac
import time
import inspect
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from contextlib import contextmanager
from contextvars import ContextVar

from mcp import types as mcp_types
from mcp.server.fastmcp import FastMCP

from server import db
from server import catalog as cat
from server import profile as prof
from server import comparison as comp
from server import reports as rpt
from server import jobs as job_svc
from server.query_engine import connect, format_results, quote
from server.sql_guard import validate as sql_validate


# PROTOQUERY_MCP_MODE controls MCP tool exposure:
# - "prod" (default): compact pair tools only (list_table_pairs + list_field_pairs)
# - "debug": also exposes legacy list_pairs (full mapping payload)
PROTOQUERY_MCP_MODE = (
    os.getenv("PROTOQUERY_MCP_MODE", "").strip()
    or os.getenv("DMH_MCP_MODE", "prod").strip()
).lower()
DEBUG_MODE = PROTOQUERY_MCP_MODE == "debug"
_EXPOSE_TOOLS_META_BY_SIDE = {
    "source": "expose_source_to_tools",
    "target": "expose_target_to_tools",
    "configurations": "expose_configurations_to_tools",
    "translations": "expose_translations_to_tools",
    "rules": "expose_rules_to_tools",
}
_EXPOSE_TOOLS_DEFAULT_BY_SIDE = {
    "source": True,
    "target": True,
    "configurations": False,
    "translations": False,
    "rules": False,
}
_SETTINGS_TOOL_LOGGING_ENABLED_KEY = "tool_logging_enabled"
_TOOL_LOG_MAX_REQUEST_CHARS = 40_000
_TOOL_LOG_MAX_RESPONSE_CHARS = 120_000
_TOOL_LOG_MAX_ERROR_CHARS = 8_000
_TOOL_LOG_SOURCE_CONTEXT: ContextVar[str] = ContextVar(
    "protoquery_tool_log_source",
    default="mcp_external",
)
_TOOL_LOG_SOURCE_HEADER = "x-protoquery-tool-log-source"
_SENSITIVE_LOG_KEYWORDS = ("api_key", "apikey", "token", "secret", "password", "authorization", "auth")

mcp = FastMCP("ProtoQueryMCP", log_level="ERROR")


def _mcp_auth_mode() -> str:
    raw = (os.getenv("PROTOQUERY_MCP_AUTH_MODE", "") or os.getenv("DMH_MCP_AUTH_MODE", "")).strip().lower()
    if raw in ("api", "api_key", "apikey"):
        return "api"
    return "none"


def _mcp_api_key_header_name() -> str:
    name = (
        os.getenv("PROTOQUERY_MCP_API_KEY_HEADER", "")
        or os.getenv("DMH_MCP_API_KEY_HEADER", "")
        or "x-api-key"
    ).strip()
    return (name or "x-api-key").lower()


def _mcp_api_key_value() -> str:
    return (
        os.getenv("PROTOQUERY_MCP_API_KEY", "")
        or os.getenv("DMH_MCP_API_KEY", "")
    ).strip()


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tool_log_truncate(value: Any, max_chars: int) -> str:
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}... [truncated {len(text) - max_chars} chars]"


def _sanitize_for_tool_log(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: Dict[str, Any] = {}
        for key, inner in value.items():
            key_text = str(key)
            lowered = key_text.lower()
            if any(keyword in lowered for keyword in _SENSITIVE_LOG_KEYWORDS):
                sanitized[key_text] = "***"
            else:
                sanitized[key_text] = _sanitize_for_tool_log(inner)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_for_tool_log(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_for_tool_log(item) for item in value]
    return value


def _tool_logging_enabled(conn) -> bool:
    raw = (db.get_meta(conn, _SETTINGS_TOOL_LOGGING_ENABLED_KEY, "1") or "1").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _tool_log_status_from_result_text(result_text: str) -> str:
    if not result_text:
        return "ok"
    try:
        payload = json.loads(result_text)
    except Exception:
        return "ok"
    if isinstance(payload, dict):
        if payload.get("error"):
            return "error"
        if payload.get("is_error") is True:
            return "error"
    return "ok"


def _tool_log_error_from_result_text(result_text: str) -> str:
    if not result_text:
        return ""
    try:
        payload = json.loads(result_text)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    raw = payload.get("error")
    if raw in (None, "") and payload.get("is_error") is True:
        raw = payload.get("content_text") or "Tool returned error."
    if raw in (None, ""):
        return ""
    return _tool_log_truncate(raw, _TOOL_LOG_MAX_ERROR_CHARS)


def _serialize_tool_result_for_log(result: Any) -> str:
    if isinstance(result, str):
        return _tool_log_truncate(result, _TOOL_LOG_MAX_RESPONSE_CHARS)
    if isinstance(result, mcp_types.CallToolResult):
        content_text = []
        for item in list(getattr(result, "content", []) or []):
            if getattr(item, "type", "") == "text":
                content_text.append(str(getattr(item, "text", "") or ""))
        payload = {
            "is_error": bool(getattr(result, "isError", False)),
            "structured_content": getattr(result, "structuredContent", None),
            "content_text": content_text,
        }
        return _tool_log_truncate(json.dumps(payload, ensure_ascii=False, default=str), _TOOL_LOG_MAX_RESPONSE_CHARS)
    return _tool_log_truncate(json.dumps(result, ensure_ascii=False, default=str), _TOOL_LOG_MAX_RESPONSE_CHARS)


def _bind_tool_call_arguments(fn: Any, args: tuple[Any, ...], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        bound = inspect.signature(fn).bind_partial(*args, **kwargs)
        bound.apply_defaults()
        raw_payload = dict(bound.arguments)
    except Exception:
        raw_payload = {
            "args": list(args),
            "kwargs": dict(kwargs),
        }
    sanitized = _sanitize_for_tool_log(raw_payload)
    payload_json = json.dumps(sanitized, ensure_ascii=False, default=str)
    if len(payload_json) <= _TOOL_LOG_MAX_REQUEST_CHARS:
        return sanitized
    return {
        "_truncated": True,
        "_preview": _tool_log_truncate(payload_json, _TOOL_LOG_MAX_REQUEST_CHARS),
    }


def _persist_tool_call_log(
    *,
    source: str,
    tool_name: str,
    request_payload: Dict[str, Any],
    response_payload: str,
    status: str,
    error_message: str,
    called_at: str,
    responded_at: str,
    duration_ms: int,
) -> None:
    conn = db.get_connection()
    try:
        if not _tool_logging_enabled(conn):
            return
        db.create_tool_call_log(
            conn,
            source=source,
            request_id="",
            tool_name=tool_name,
            status=status,
            request_payload=request_payload,
            response_payload=response_payload,
            error_message=error_message,
            called_at=called_at,
            responded_at=responded_at,
            duration_ms=duration_ms,
        )
    finally:
        conn.close()


@contextmanager
def tool_call_log_source(source: str):
    normalized = (source or "").strip().lower() or "mcp_external"
    token = _TOOL_LOG_SOURCE_CONTEXT.set(normalized)
    try:
        yield
    finally:
        _TOOL_LOG_SOURCE_CONTEXT.reset(token)


def _current_tool_log_source() -> str:
    return _TOOL_LOG_SOURCE_CONTEXT.get()


def _instrument_tool_call_logging() -> None:
    tool_manager = getattr(mcp, "_tool_manager", None)
    tools = getattr(tool_manager, "_tools", {}) if tool_manager is not None else {}
    for tool in list(tools.values()):
        original_fn = getattr(tool, "fn", None)
        if not callable(original_fn):
            continue
        if getattr(original_fn, "_protoquery_tool_log_wrapped", False):
            continue
        tool_name = str(getattr(tool, "name", "") or "")

        def _wrapped_tool_fn(*args, __original=original_fn, __tool_name=tool_name, **kwargs):
            called_at = _iso_utc_now()
            started_at = time.perf_counter()
            request_payload = _bind_tool_call_arguments(__original, args, kwargs)
            response_payload = ""
            status = "ok"
            error_message = ""
            try:
                result = __original(*args, **kwargs)
                response_payload = _serialize_tool_result_for_log(result)
                status = _tool_log_status_from_result_text(response_payload)
                error_message = _tool_log_error_from_result_text(response_payload)
                return result
            except Exception as exc:
                status = "error"
                error_message = _tool_log_truncate(str(exc), _TOOL_LOG_MAX_ERROR_CHARS)
                response_payload = _tool_log_truncate(
                    json.dumps({"error": str(exc)}, ensure_ascii=False),
                    _TOOL_LOG_MAX_RESPONSE_CHARS,
                )
                raise
            finally:
                responded_at = _iso_utc_now()
                duration_ms = int(max(0.0, (time.perf_counter() - started_at) * 1000.0))
                try:
                    _persist_tool_call_log(
                        source=_current_tool_log_source(),
                        tool_name=__tool_name or "unknown_tool",
                        request_payload=request_payload,
                        response_payload=response_payload,
                        status=status,
                        error_message=error_message,
                        called_at=called_at,
                        responded_at=responded_at,
                        duration_ms=duration_ms,
                    )
                except Exception:
                    # Never fail tool execution because logging failed.
                    pass

        setattr(_wrapped_tool_fn, "_protoquery_tool_log_wrapped", True)
        tool.fn = _wrapped_tool_fn


def _split_csv_fields(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _normalize_field_mappings(mappings: Optional[List[Dict[str, Any]]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for mapping in mappings or []:
        source_field = str(mapping.get("source_field") or mapping.get("source") or "").strip()
        target_field = str(mapping.get("target_field") or mapping.get("target") or "").strip()
        if not source_field or not target_field:
            continue
        sig = (source_field, target_field)
        if sig in seen:
            continue
        seen.add(sig)
        normalized.append({"source_field": source_field, "target_field": target_field})
    return normalized


def _resolve_single_mapping(field_name: str, mappings: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    field_name = field_name.strip()
    if not field_name:
        return None

    exact_source = [m for m in mappings if m["source_field"] == field_name]
    if len(exact_source) == 1:
        return exact_source[0]

    exact_target = [m for m in mappings if m["target_field"] == field_name]
    if len(exact_target) == 1:
        return exact_target[0]

    lookup = field_name.lower()
    ci_source = [m for m in mappings if m["source_field"].lower() == lookup]
    if len(ci_source) == 1:
        return ci_source[0]

    ci_target = [m for m in mappings if m["target_field"].lower() == lookup]
    if len(ci_target) == 1:
        return ci_target[0]

    return None


def _resolve_requested_mappings(
    requested_fields: List[str],
    pair_mappings: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    if not pair_mappings:
        return [{"source_field": field, "target_field": field} for field in requested_fields]

    if not requested_fields:
        return pair_mappings

    resolved: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for field in requested_fields:
        mapping = _resolve_single_mapping(field, pair_mappings)
        if not mapping:
            mapping = {"source_field": field, "target_field": field}
        sig = (mapping["source_field"], mapping["target_field"])
        if sig in seen:
            continue
        seen.add(sig)
        resolved.append(mapping)
    return resolved


def _tool_visible_dataset_sides(conn) -> set[str]:
    sides: set[str] = set()
    for side, meta_key in _EXPOSE_TOOLS_META_BY_SIDE.items():
        try:
            default_value = "1" if _EXPOSE_TOOLS_DEFAULT_BY_SIDE.get(side, False) else "0"
            raw = str(db.get_meta(conn, meta_key, default_value) or "").strip().lower()
        except Exception:
            # Keep tool visibility conservative but resilient for lightweight test doubles
            # that do not implement full sqlite connection APIs.
            raw = "1" if _EXPOSE_TOOLS_DEFAULT_BY_SIDE.get(side, False) else "0"
        if raw in ("1", "true", "yes", "on"):
            sides.add(side)
    return sides


def _tool_visible_datasets(conn) -> List[Dict[str, Any]]:
    visible_sides = _tool_visible_dataset_sides(conn)
    return [ds for ds in db.list_datasets(conn) if (ds.get("side") or "source") in visible_sides]


def _tool_get_visible_dataset(dataset_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    conn = db.get_connection()
    try:
        ds = db.get_dataset(conn, dataset_id)
        if not ds:
            return None, f"Dataset '{dataset_id}' not found."
        if (ds.get("side") or "source") not in _tool_visible_dataset_sides(conn):
            return None, f"Dataset '{dataset_id}' is not exposed to MCP tools."
        return ds, None
    finally:
        conn.close()


def _format_export_job_start(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return MCP-friendly accepted payload for queued export jobs."""
    if not isinstance(result, dict) or "error" in result:
        return result

    job_id = str(result.get("job_id") or "").strip()
    state = str(result.get("state") or "").strip().lower()
    if not job_id or state not in ("queued", "running"):
        return result

    return {
        "status": "accepted",
        "state": state,
        "job_id": job_id,
        "message": f"Export started in background ({job_id}). You can keep working; refresh status to track progress.",
        "next": {
            "poll_tool": "get_job_status",
            "poll_arg": job_id,
            "suggested_poll_interval_seconds": 2,
            "completion_tool": "get_job_summary",
        },
    }


def _resolve_pair_context(
    source_dataset_id: str,
    target_dataset_id: str,
    key_fields: List[str],
    compare_fields: Optional[List[str]],
    pair_id: Optional[str] = None,
) -> tuple[
    Optional[str],
    List[str],
    Optional[List[str]],
    Optional[List[Dict[str, str]]],
    Optional[List[Dict[str, str]]],
]:
    pair = cat.get_pair(pair_id) if pair_id else cat.get_pair_by_datasets(source_dataset_id, target_dataset_id)
    if pair_id and not pair:
        return f"Pair '{pair_id}' not found.", key_fields, compare_fields, None, None
    if pair and (
        pair.get("source_dataset") != source_dataset_id or pair.get("target_dataset") != target_dataset_id
    ):
        return (
            (
                f"Pair '{pair.get('id')}' belongs to source '{pair.get('source_dataset')}' and "
                f"target '{pair.get('target_dataset')}', not '{source_dataset_id}' -> '{target_dataset_id}'."
            ),
            key_fields,
            compare_fields,
            None,
            None,
        )

    key_pair_mappings = _normalize_field_mappings((pair or {}).get("key_mappings"))
    compare_pair_mappings = _normalize_field_mappings((pair or {}).get("compare_mappings"))

    resolved_key_mappings: Optional[List[Dict[str, str]]] = None
    resolved_compare_mappings: Optional[List[Dict[str, str]]] = None
    resolved_key_fields = key_fields
    resolved_compare_fields = compare_fields

    if key_pair_mappings:
        resolved_key_mappings = _resolve_requested_mappings(key_fields, key_pair_mappings)
        resolved_key_fields = [m["source_field"] for m in resolved_key_mappings]

    if compare_pair_mappings:
        requested_compare = compare_fields or []
        resolved_compare_mappings = _resolve_requested_mappings(requested_compare, compare_pair_mappings)
        if compare_fields is not None:
            resolved_compare_fields = [m["source_field"] for m in resolved_compare_mappings]

    return None, resolved_key_fields, resolved_compare_fields, resolved_key_mappings, resolved_compare_mappings

# ═══════════════════════════════════════════════════════════════
#  5.1 — Catalog tools
# ═══════════════════════════════════════════════════════════════


@mcp.tool()
def refresh_catalog(
    include_row_counts: bool = False,
) -> str:
    """Refresh catalog metadata from configured folders and auto-pair datasets. Returns a JSON refresh summary (or error JSON). Use after source/target/configuration files change; use `list_datasets` for read-only inspection."""
    result = cat.refresh_catalog(
        include_row_counts=include_row_counts,
    )
    return json.dumps(result, indent=2)


@mcp.tool()
def list_datasets(
    side: Optional[str] = None,
    filter: Optional[str] = None,
) -> str:
    """List datasets currently exposed to MCP tools. Returns a JSON array with dataset IDs, side, file/sheet, column count, and row_count. Use `list_fields` when you already know the dataset and need columns."""
    conn = db.get_connection()
    datasets = _tool_visible_datasets(conn)
    conn.close()
    if side:
        side_norm = side.strip().lower()
        datasets = [d for d in datasets if str(d.get("side", "")).strip().lower() == side_norm]
    if filter:
        needle = filter.strip().lower()
        datasets = [
            d for d in datasets
            if needle in str(d.get("id", "")).lower() or needle in str(d.get("file_name", "")).lower()
        ]
    rows = [
        {
            "id": d["id"],
            "side": d["side"],
            "file_name": d["file_name"],
            "sheet_name": d["sheet_name"],
            "columns": len(d["columns"]),
            "row_count": d["row_count"],
        }
        for d in datasets
    ]
    return json.dumps(rows, indent=2)


@mcp.tool()
def list_fields(dataset_id: str) -> str:
    """List columns for one dataset. Returns JSON with `dataset`, `columns`, and `column_count` (or `error`). Use this before profiling, filtering, SQL, or comparisons."""
    ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    return json.dumps(
        {
            "dataset": dataset_id,
            "columns": ds["columns"],
            "column_count": len(ds["columns"]),
        },
        indent=2,
    )


@mcp.tool()
def preview_dataset(
    dataset_id: str,
    limit: int = 10,
    offset: int = 0,
    fields: Optional[str] = None,
) -> mcp_types.CallToolResult:
    """Preview rows from one dataset with optional field projection. Returns a `CallToolResult` with table text plus structured headers/rows/total metadata (or an error result). Use `run_sql_preview` for custom joins/aggregates."""

    def _error_result(message: str) -> mcp_types.CallToolResult:
        return mcp_types.CallToolResult(
            content=[mcp_types.TextContent(type="text", text=message)],
            structuredContent={"error": message},
            isError=True,
        )

    ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return _error_result(err)

    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    field_list = [f.strip() for f in fields.split(",")] if fields else None

    with connect([ds]) as duck:
        view = quote(dataset_id)
        sel = "*"
        selected_fields: Optional[List[str]] = None
        if field_list:
            selected_fields = [f for f in field_list if f in ds["columns"]]
            if selected_fields:
                sel = ", ".join(quote(f) for f in selected_fields)
        sql = f"SELECT {sel} FROM {view} LIMIT {limit} OFFSET {offset}"
        result = duck.execute(sql)
        headers = [d[0] for d in result.description]
        rows = [list(r) for r in result.fetchall()]
        total = duck.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]

    safe_rows = json.loads(json.dumps(rows, default=str))
    payload: Dict[str, Any] = {
        "dataset": dataset_id,
        "headers": headers,
        "rows": safe_rows,
        "total_rows": total,
        "row_count": len(safe_rows),
        "limit": limit,
        "offset": offset,
        "selected_fields": selected_fields,
    }
    table_text = format_results(headers, safe_rows, total, limit)
    return mcp_types.CallToolResult(
        content=[mcp_types.TextContent(type="text", text=table_text)],
        structuredContent=payload,
        isError=False,
    )


@mcp.tool()
def run_sql_preview(sql: str, limit: int = 10) -> mcp_types.CallToolResult:
    """Run read-only SQL against visible datasets with a capped result size. Returns a `CallToolResult` with executed SQL, headers, rows, and counts (or an error result). Use `preview_dataset` for simple top-N browsing without SQL."""

    def _error_result(message: str) -> mcp_types.CallToolResult:
        return mcp_types.CallToolResult(
            content=[mcp_types.TextContent(type="text", text=message)],
            structuredContent={"error": message},
            isError=True,
        )

    ok, err = sql_validate(sql)
    if not ok:
        return _error_result(err)

    limit = max(1, min(int(limit), 100))
    conn = db.get_connection()
    datasets = _tool_visible_datasets(conn)
    conn.close()

    if not datasets:
        return _error_result("No datasets loaded. Run refresh_catalog first.")

    # Inject LIMIT if not present
    original_sql = sql
    sql_upper = sql.strip().upper()
    if "LIMIT" not in sql_upper:
        sql = f"{sql.rstrip().rstrip(';')} LIMIT {limit}"

    with connect(datasets) as duck:
        try:
            result = duck.execute(sql)
            headers = [d[0] for d in result.description]
            rows = [list(r) for r in result.fetchall()]
        except Exception as exc:
            return _error_result(str(exc))

    safe_rows = json.loads(json.dumps(rows, default=str))
    payload: Dict[str, Any] = {
        "sql": original_sql,
        "executed_sql": sql,
        "headers": headers,
        "rows": safe_rows,
        "row_count": len(safe_rows),
        "total_rows": len(safe_rows),
        "limit_applied": limit,
    }
    table_text = format_results(headers, safe_rows, len(safe_rows), limit)
    return mcp_types.CallToolResult(
        content=[mcp_types.TextContent(type="text", text=table_text)],
        structuredContent=payload,
        isError=False,
    )


@mcp.tool()
def export_query(
    sql: str,
    filename: Optional[str] = None,
    format: str = "xlsx",
    async_job: bool = True,
) -> str:
    """Export read-only SQL results to XLSX, sync or async. Returns accepted job JSON when `async_job=True`, otherwise report metadata JSON, or `error` JSON on failure. Use `run_sql_preview` for interactive non-export queries."""
    if async_job:
        result = _format_export_job_start(job_svc.start_export_query_job(sql=sql, filename=filename))
        return json.dumps(result, indent=2, default=str)

    ok, err = sql_validate(sql)
    if not ok:
        return json.dumps({"error": err})

    conn = db.get_connection()
    datasets = _tool_visible_datasets(conn)
    conn.close()

    if not datasets:
        return json.dumps({"error": "No datasets loaded."})

    with connect(datasets) as duck:
        try:
            result = duck.execute(sql)
            headers = [d[0] for d in result.description]
            report = rpt.export_query_to_xlsx(
                headers=headers,
                rows=result,
                filename=filename,
                sql_query=sql,
            )
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    return json.dumps(report, indent=2)


@mcp.tool()
def row_count_summary(dataset_id: str) -> str:
    """Get row count for one dataset. Returns JSON with `dataset`, `side`, and `row_count` (or `error`). Use `data_profile` when you need column-level statistics, not only table size."""
    ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})

    cached = ds.get("row_count")
    if cached is not None:
        return json.dumps(
            {
                "dataset": ds["id"],
                "side": ds["side"],
                "row_count": cached,
            },
            indent=2,
        )

    with connect([ds]) as duck:
        try:
            n = duck.execute(f"SELECT COUNT(*) FROM {quote(ds['id'])}").fetchone()[0]
        except Exception:
            n = None

    if n is not None:
        conn = db.get_connection()
        try:
            conn.execute(
                "UPDATE datasets SET row_count = ?, updated_at = ? WHERE id = ?",
                (int(n), db.utcnow(), ds["id"]),
            )
            conn.commit()
        finally:
            conn.close()

    return json.dumps(
        {
            "dataset": ds["id"],
            "side": ds["side"],
            "row_count": n,
        },
        indent=2,
    )


# ═══════════════════════════════════════════════════════════════
#  5.2 — Profiling tools
# ═══════════════════════════════════════════════════════════════


@mcp.tool()
def data_profile(dataset_id: str) -> str:
    """Compute per-column profile statistics for a dataset. Returns JSON with total rows plus per-column distinct/min/max/blank_count metrics (or `error`). Use `column_value_summary` for top value frequencies."""
    _ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    result = prof.data_profile(dataset_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def column_value_summary(
    dataset_id: str,
    column: Optional[str] = None,
    top_n: int = 5,
) -> str:
    """Get top-N value frequencies and blank/null counts for one or all columns. Returns JSON summaries per column (or `error`). Use `value_distribution` when you only need one column's distribution."""
    _ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    result = prof.column_value_summary(dataset_id, column=column, top_n=top_n)
    return json.dumps(result, indent=2)


@mcp.tool()
def export_column_value_summary(
    dataset_id: str,
    column: Optional[str] = None,
    top_n: int = 5,
    filename: Optional[str] = None,
) -> str:
    """Export column value summaries to XLSX. Returns report metadata JSON (`report_id`, file path/name, dataset, counts) or `error` JSON. Use `column_value_summary` for in-session JSON results only."""
    _ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    top_n = max(1, min(int(top_n), 100))
    result = prof.column_value_summary(dataset_id, column=column, top_n=top_n)
    if "error" in result:
        return json.dumps(result, indent=2)
    report = rpt.export_column_summary_to_xlsx(result, top_n=top_n, filename=filename)
    return json.dumps(report, indent=2)


@mcp.tool()
def combo_value_summary(
    dataset_id: str,
    columns: str,
    top_n: int = 10,
) -> str:
    """Compute frequency of multi-column value combinations from comma-separated columns. Returns JSON with combo counts and blank/null combo counts (or `error`). Use `column_value_summary` for single-column analysis."""
    _ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    col_list = [c.strip() for c in columns.split(",") if c.strip()]
    result = prof.combo_value_summary(dataset_id, col_list, top_n=top_n)
    return json.dumps(result, indent=2)


@mcp.tool()
def preview_filtered_records(
    dataset_id: str,
    column: str,
    value: Optional[str] = None,
    blanks_only: bool = False,
    limit: int = 10,
) -> str:
    """Preview rows matching an exact value or blanks filter on one column. Returns JSON with headers, rows, row_count, and the applied filter (or `error`). Use `value_distribution` for aggregate counts instead of record samples."""
    _ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    fspec = {"column": column}
    if blanks_only:
        fspec["blanks_only"] = True
    elif value is not None:
        fspec["value"] = value
    result = prof.preview_filtered_records(dataset_id, fspec, limit=limit)
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
def find_duplicates(
    dataset_id: str,
    key_columns: str,
    limit: int = 10,
) -> str:
    """Find duplicate groups by comma-separated key columns. Returns JSON with duplicate groups and totals (or `error`). Use `suggest_keys` first if key columns are not known yet."""
    _ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    keys = [k.strip() for k in key_columns.split(",") if k.strip()]
    result = prof.find_duplicates(dataset_id, keys, limit=limit)
    return json.dumps(result, indent=2)


@mcp.tool()
def value_distribution(
    dataset_id: str,
    column: str,
    limit: int = 20,
) -> str:
    """Get value frequency distribution for a single column. Returns JSON with distribution rows and distinct counts (or `error`). Use `column_value_summary` to scan multiple columns in one call."""
    _ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    result = prof.value_distribution(dataset_id, column, limit=limit)
    return json.dumps(result, indent=2)


# ═══════════════════════════════════════════════════════════════
#  5.3 — Pairing and key tools
# ═══════════════════════════════════════════════════════════════


@mcp.tool()
def list_table_pairs(
    source_dataset_id: Optional[str] = None,
    target_dataset_id: Optional[str] = None,
) -> str:
    """List compact source-target pair metadata without full mappings. Returns JSON array with pair IDs, dataset IDs, and mapping counts. Use `list_field_pairs` to load mappings for a specific pair."""
    pairs = cat.get_pairs()

    filter_ids = {x for x in (source_dataset_id, target_dataset_id) if x}
    if filter_ids:
        pairs = [
            p
            for p in pairs
            if p["source_dataset"] in filter_ids or p["target_dataset"] in filter_ids
        ]

    compact = [
        {
            "pair_id": p["id"],
            "source_dataset": p["source_dataset"],
            "target_dataset": p["target_dataset"],
            "source_file": p["source_file"],
            "source_sheet": p["source_sheet"],
            "target_file": p["target_file"],
            "target_sheet": p["target_sheet"],
            "auto_matched": p["auto_matched"],
            "enabled": p["enabled"],
            "key_mapping_count": len(p.get("key_mappings", [])),
            "compare_mapping_count": len(p.get("compare_mappings", [])),
            "created_at": p["created_at"],
        }
        for p in pairs
    ]
    return json.dumps(compact, indent=2)


@mcp.tool()
def list_field_pairs(pair_id: str) -> str:
    """Get key and compare mappings for one pair. Returns JSON mapping payload for the given `pair_id` (or `error`). Use `list_table_pairs` first when selecting a pair."""
    pair = cat.get_pair(pair_id)
    if not pair:
        return json.dumps({"error": f"Pair '{pair_id}' not found."})
    return json.dumps(
        {
            "id": pair["id"],
            "source_dataset": pair["source_dataset"],
            "target_dataset": pair["target_dataset"],
            "key_mappings": pair.get("key_mappings", []),
            "compare_mappings": pair.get("compare_mappings", []),
            "key_mapping_count": len(pair.get("key_mappings", [])),
            "compare_mapping_count": len(pair.get("compare_mappings", [])),
        },
        indent=2,
    )


if DEBUG_MODE:
    @mcp.tool()
    def list_pairs() -> str:
        """Debug-only full pair listing including complete mapping payloads. Returns JSON array of pair records. Use `list_table_pairs`/`list_field_pairs` in production flows."""
        pairs = cat.get_pairs()
        return json.dumps(pairs, indent=2)

@mcp.tool()
def suggest_keys(pair_id: str) -> str:
    """Suggest likely key columns for a pair using uniqueness, completeness, and overlap heuristics. Returns ranked candidate JSON with scores (or `error`). Use `find_duplicates` to validate a chosen key."""
    result = prof.suggest_keys(pair_id)
    return json.dumps(result, indent=2)


def save_key_preset(
    pair_id: str,
    key_fields: str,
    name: str = "default",
) -> str:
    """Persist a reusable key configuration for a pair. key_fields: comma-separated column names."""
    fields = [f.strip() for f in key_fields.split(",") if f.strip()]
    conn = db.get_connection()
    preset_id = db.save_key_preset(conn, pair_id, name, fields)
    conn.close()
    return json.dumps({"preset_id": preset_id, "pair_id": pair_id, "key_fields": fields, "name": name})


@mcp.tool()
def list_key_presets(pair_id: str) -> str:
    """List saved key presets for a pair. Returns JSON array of preset rows (or empty array). Use `suggest_keys` when no curated presets exist yet."""
    conn = db.get_connection()
    presets = db.list_key_presets(conn, pair_id)
    conn.close()
    return json.dumps(presets, indent=2)


@mcp.tool()
def get_dataset_links(
    dataset_id: str,
) -> str:
    """Return relationship-based join hints for a dataset. Returns JSON relations with linked datasets, field pairs, confidence, and join predicate SQL (or `error`). Use `list_fields` when only schema is needed."""
    conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if not ds:
        conn.close()
        return json.dumps({"error": f"Dataset '{dataset_id}' not found."})
    if ds.get("side") not in _tool_visible_dataset_sides(conn):
        conn.close()
        return json.dumps({"error": f"Dataset '{dataset_id}' is not exposed to MCP tools."})

    rels = db.list_relationships(
        conn,
        side=None,
        dataset_id=dataset_id,
        active_only=True,
        limit=5000,
    )
    conn.close()

    relations: list[dict[str, Any]] = []
    for r in rels:
        left_fields = r.get("left_fields") or ([r["left_field"]] if r.get("left_field") else [])
        right_fields = r.get("right_fields") or ([r["right_field"]] if r.get("right_field") else [])
        if r["left_dataset"] == dataset_id:
            dataset_fields = left_fields
            linked_fields = right_fields
            linked_dataset_id = r["right_dataset"]
        else:
            dataset_fields = right_fields
            linked_fields = left_fields
            linked_dataset_id = r["left_dataset"]

        pair_count = min(len(dataset_fields), len(linked_fields))
        if pair_count <= 0:
            continue
        dataset_fields = dataset_fields[:pair_count]
        linked_fields = linked_fields[:pair_count]
        field_pairs = [
            {"dataset_field": dataset_fields[i], "linked_dataset_field": linked_fields[i]}
            for i in range(pair_count)
        ]
        join_predicate_sql = " AND ".join(
            f'a.{quote(p["dataset_field"])} = b.{quote(p["linked_dataset_field"])}'
            for p in field_pairs
        )

        relations.append(
            {
                "linked_dataset": linked_dataset_id,
                "field_pairs": field_pairs,
                "confidence": r["confidence"],
                "join_predicate_sql": join_predicate_sql,
            }
        )

    relations.sort(key=lambda x: (x["confidence"], x["linked_dataset"]), reverse=True)

    return json.dumps(
        {
            "dataset": dataset_id,
            "relations": relations,
        },
        indent=2,
    )


@mcp.tool()
def schema_diff(
    source_dataset_id: str,
    target_dataset_id: str,
) -> str:
    """Compare schemas between two datasets. Returns JSON drift details such as source-only and target-only columns (or `error`). Use `compare_tables` for data-level differences."""
    result = cat.schema_diff(source_dataset_id, target_dataset_id)
    return json.dumps(result, indent=2)


# ═══════════════════════════════════════════════════════════════
#  5.4 — Comparison and report tools
# ═══════════════════════════════════════════════════════════════


@mcp.tool()
def start_export_query_job(
    sql: str,
    filename: Optional[str] = None,
) -> str:
    """Queue a background query export job. Returns accepted-job JSON with `job_id` and next polling hint (or `error`). Use `export_query(..., async_job=False)` for synchronous export."""
    result = _format_export_job_start(
        job_svc.start_export_query_job(
            sql=sql,
            filename=filename,
        )
    )
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
def start_comparison_job(
    source_dataset_id: str,
    target_dataset_id: str,
    key_fields: str,
    pair_id: Optional[str] = None,
    compare_fields: Optional[str] = None,
) -> str:
    """Queue a background comparison job between source and target datasets. Returns job JSON with IDs/state (or `error`) and supports mapped key/compare fields via `pair_id`. Use `compare_tables` for immediate results."""
    keys = _split_csv_fields(key_fields)
    comp_cols = _split_csv_fields(compare_fields) if compare_fields else None
    err, keys, comp_cols, key_mappings, compare_mappings = _resolve_pair_context(
        source_dataset_id=source_dataset_id,
        target_dataset_id=target_dataset_id,
        key_fields=keys,
        compare_fields=comp_cols,
        pair_id=pair_id,
    )
    if err:
        return json.dumps({"error": err})
    result = job_svc.start_comparison_job(
        source_id=source_dataset_id,
        target_id=target_dataset_id,
        key_columns=keys,
        key_mappings=key_mappings,
        pair_id=pair_id,
        compare_columns=comp_cols,
        compare_mappings=compare_mappings,
        options={
            "key_mappings": key_mappings or [],
            "compare_mappings": compare_mappings or [],
        },
    )
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
def compare_tables(
    source_dataset_id: str,
    target_dataset_id: str,
    key_fields: str,
    pair_id: Optional[str] = None,
    compare_fields: Optional[str] = None,
    sample_limit: int = 10,
) -> str:
    """Run an immediate table comparison. Returns JSON summary plus sample added/removed/changed rows (or `error`) and supports mapped key/compare fields via `pair_id`. Use `start_comparison_job` for long-running comparisons."""
    keys = _split_csv_fields(key_fields)
    comp_cols = _split_csv_fields(compare_fields) if compare_fields else None
    err, keys, comp_cols, key_mappings, compare_mappings = _resolve_pair_context(
        source_dataset_id=source_dataset_id,
        target_dataset_id=target_dataset_id,
        key_fields=keys,
        compare_fields=comp_cols,
        pair_id=pair_id,
    )
    if err:
        return json.dumps({"error": err})
    result = comp.compare_datasets(
        source_id=source_dataset_id,
        target_id=target_dataset_id,
        key_columns=keys,
        compare_columns=comp_cols,
        key_mappings=key_mappings,
        compare_mappings=compare_mappings,
        sample_limit=sample_limit,
    )
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
def compare_field(
    source_dataset_id: str,
    target_dataset_id: str,
    key_columns: str,
    field: str,
    limit: int = 10,
    pair_id: Optional[str] = None,
) -> str:
    """Drill into row-level differences for one field. Returns JSON field-specific diff rows (or `error`) and supports mapped fields via `pair_id`. Use `compare_tables` first for broad triage."""
    keys = _split_csv_fields(key_columns)
    err, keys, _comp_cols, key_mappings, compare_mappings = _resolve_pair_context(
        source_dataset_id=source_dataset_id,
        target_dataset_id=target_dataset_id,
        key_fields=keys,
        compare_fields=[field],
        pair_id=pair_id,
    )
    if err:
        return json.dumps({"error": err})

    field_mapping = compare_mappings[0] if compare_mappings else None
    result = comp.compare_field(
        source_id=source_dataset_id,
        target_id=target_dataset_id,
        key_columns=keys,
        field=field,
        limit=limit,
        key_mappings=key_mappings,
        field_mapping=field_mapping,
    )
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
def get_job_status(job_id: str) -> str:
    """Get current state and progress for a job. Returns JSON status/progress/error details for `job_id` (or `error`). Use `get_job_summary` after completion for final outcomes."""
    result = job_svc.get_job_status(job_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def get_job_summary(job_id: str) -> str:
    """Get final summary for a completed job. Returns JSON completion metrics and linked report metadata when available (or `error`). Use `get_job_status` for active jobs."""
    result = job_svc.get_job_summary(job_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def cancel_job(job_id: str) -> str:
    """Cancel a queued or running job. Returns JSON cancellation result for `job_id` (or `error`). Use `get_job_status` when you only need monitoring."""
    result = job_svc.cancel_job(job_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def list_reports(limit: int = 5) -> str:
    """List recent generated reports. Returns JSON array of report metadata rows, ordered by recency. Use `get_report_metadata` for one specific report."""
    limit = max(1, min(int(limit), 500))
    conn = db.get_connection()
    reports = db.list_reports(conn, limit=limit)
    conn.close()
    return json.dumps(reports, indent=2)


@mcp.tool()
def get_report_metadata(report_id: str) -> str:
    """Get metadata for one report ID. Returns JSON report details including file path/name and summary (or `error`). Use `list_reports` to discover report IDs."""
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    conn.close()
    if not report:
        return json.dumps({"error": f"Report '{report_id}' not found."})
    return json.dumps(report, indent=2)


def delete_report(report_id: str) -> str:
    """Delete a report (file + metadata). Never deletes source/target data."""
    import os

    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    if not report:
        conn.close()
        return json.dumps({"error": f"Report '{report_id}' not found."})

    # Delete file
    try:
        if os.path.exists(report["file_path"]):
            os.remove(report["file_path"])
    except Exception:
        pass

    db.delete_report(conn, report_id)
    conn.close()
    return json.dumps({"deleted": report_id})


# ═══════════════════════════════════════════════════════════════
#  5.6 — MCP resources
# ═══════════════════════════════════════════════════════════════


@mcp.resource("data://datasets")
def resource_datasets() -> str:
    """JSON list of all dataset IDs with side info."""
    conn = db.get_connection()
    datasets = _tool_visible_datasets(conn)
    conn.close()
    return json.dumps(
        [{"id": d["id"], "side": d["side"], "file_name": d["file_name"]} for d in datasets]
    )


@mcp.resource("data://datasets/{dataset_id}/schema")
def resource_dataset_schema(dataset_id: str) -> str:
    """JSON column list for a specific dataset."""
    ds, err = _tool_get_visible_dataset(dataset_id)
    if err:
        return json.dumps({"error": err})
    return json.dumps({"id": ds["id"], "columns": ds["columns"]})


# ═══════════════════════════════════════════════════════════════
#  5.7 — MCP prompts
# ═══════════════════════════════════════════════════════════════


@mcp.prompt()
def compare_data(source: str, target: str) -> str:
    """Guided end-to-end comparison workflow."""
    return f"""I need to compare two datasets:
- Source: {source}
- Target: {target}

Please follow these steps:
1. List fields for both datasets and check schema compatibility (schema_diff)
2. Suggest key columns (suggest_keys if there's a pair, or review the columns)
3. Run a comparison using compare_tables or start_comparison_job
4. Summarize the results: ADDED, REMOVED, CHANGED counts and sample diffs
5. If needed, drill down into specific fields with compare_field"""


@mcp.prompt()
def profile_data(dataset: str) -> str:
    """Guided data-quality profiling."""
    return f"""I need to profile the dataset: {dataset}

Please follow these steps:
1. List all fields (list_fields)
2. Run data_profile to get per-column statistics
3. Run column_value_summary for columns with interesting distributions
4. Check for duplicates on likely key columns (find_duplicates)
5. Summarize data quality: completeness, uniqueness, and any anomalies"""


@mcp.prompt()
def reconcile_data() -> str:
    """Full multi-table reconciliation across all pairs."""
    return """I need to reconcile all paired datasets.

Please follow these steps:
1. List compact pairs (list_table_pairs), then load mappings for chosen pairs (list_field_pairs)
2. For each enabled pair:
   a. Check schema compatibility (schema_diff)
   b. Suggest and confirm key columns (suggest_keys)
   c. Run comparison (start_comparison_job)
   d. Summarize results
3. Provide an overall reconciliation summary across all pairs"""


_instrument_tool_call_logging()


def _run_streamable_http() -> None:
    import uvicorn
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    from starlette.responses import JSONResponse

    auth_mode = _mcp_auth_mode()
    header_name = _mcp_api_key_header_name()
    expected_key = _mcp_api_key_value()
    streamable_path = mcp.settings.streamable_http_path or "/mcp"

    app = mcp.streamable_http_app()

    class ToolLogSourceMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            path = request.url.path or ""
            source = "mcp_external"
            if path == streamable_path or path.startswith(f"{streamable_path}/"):
                hinted_source = (request.headers.get(_TOOL_LOG_SOURCE_HEADER) or "").strip().lower()
                if hinted_source in ("inspector", "claude_chat", "mcp_external"):
                    source = hinted_source
            with tool_call_log_source(source):
                return await call_next(request)

    app.add_middleware(ToolLogSourceMiddleware)

    if auth_mode == "api":
        if not expected_key:
            raise RuntimeError(
                "MCP authentication mode is API but PROTOQUERY_MCP_API_KEY is not set."
            )

        class ApiKeyAuthMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                path = request.url.path or ""
                if path == streamable_path or path.startswith(f"{streamable_path}/"):
                    provided = (request.headers.get(header_name) or "").strip()
                    if not hmac.compare_digest(provided, expected_key):
                        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
                return await call_next(request)

        app.add_middleware(ApiKeyAuthMiddleware)

    config = uvicorn.Config(
        app,
        host=mcp.settings.host,
        port=mcp.settings.port,
        log_level=mcp.settings.log_level.lower(),
    )
    uvicorn.Server(config).run()


if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport == "streamable-http":
        _run_streamable_http()
    else:
        mcp.run(transport=transport)
