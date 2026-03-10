"""
DM Helper MCP Server.

Exposes data-migration tools (catalog, profiling, comparison,
reports, SQL preview) via FastMCP (stdio transport).
"""

import json
import os
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from server import db
from server import catalog as cat
from server import profile as prof
from server import comparison as comp
from server import reports as rpt
from server import jobs as job_svc
from server import relationships as rel
from server.query_engine import connect, format_results, quote
from server.sql_guard import validate as sql_validate


# DMH_MCP_MODE controls MCP tool exposure:
# - "prod" (default): compact pair tools only (list_table_pairs + list_field_pairs)
# - "debug": also exposes legacy list_pairs (full mapping payload)
DMH_MCP_MODE = os.getenv("DMH_MCP_MODE", "prod").strip().lower()
DEBUG_MODE = DMH_MCP_MODE == "debug"

mcp = FastMCP("DMHelperMCP", log_level="ERROR")


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
    """Scan saved source/target folders, register datasets, and auto-pair (name + field overlap fallback)."""
    result = cat.refresh_catalog(
        include_row_counts=include_row_counts,
    )
    return json.dumps(result, indent=2)


@mcp.tool()
def list_datasets(
    side: Optional[str] = None,
    filter: Optional[str] = None,
) -> str:
    """List discovered datasets. Optional filter by side ('source'/'target') or text search."""
    datasets = cat.get_datasets(side=side, filter_text=filter)
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
    """List column names and count for a dataset."""
    ds = cat.get_dataset(dataset_id)
    if not ds:
        return json.dumps({"error": f"Dataset '{dataset_id}' not found."})
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
) -> str:
    """Preview top-N rows from a dataset. Optionally specify comma-separated field names."""
    ds = cat.get_dataset(dataset_id)
    if not ds:
        return json.dumps({"error": f"Dataset '{dataset_id}' not found."})

    limit = min(int(limit), 100)
    field_list = [f.strip() for f in fields.split(",")] if fields else None

    with connect([ds]) as duck:
        view = quote(dataset_id)
        sel = "*"
        if field_list:
            valid = [f for f in field_list if f in ds["columns"]]
            if valid:
                sel = ", ".join(quote(f) for f in valid)
        sql = f"SELECT {sel} FROM {view} LIMIT {limit} OFFSET {int(offset)}"
        result = duck.execute(sql)
        headers = [d[0] for d in result.description]
        rows = result.fetchall()
        total = duck.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]

    return format_results(headers, [list(r) for r in rows], total, limit)


@mcp.tool()
def run_sql_preview(sql: str, limit: int = 10) -> str:
    """Execute a read-only SQL query against loaded datasets and return capped results."""
    ok, err = sql_validate(sql)
    if not ok:
        return json.dumps({"error": err})

    limit = min(int(limit), 100)
    conn = db.get_connection()
    datasets = db.list_datasets(conn)
    conn.close()

    if not datasets:
        return json.dumps({"error": "No datasets loaded. Run refresh_catalog first."})

    # Inject LIMIT if not present
    sql_upper = sql.strip().upper()
    if "LIMIT" not in sql_upper:
        sql = f"{sql.rstrip().rstrip(';')} LIMIT {limit}"

    with connect(datasets) as duck:
        try:
            result = duck.execute(sql)
            headers = [d[0] for d in result.description]
            rows = result.fetchall()
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    return format_results(headers, [list(r) for r in rows], len(rows), limit)


@mcp.tool()
def export_query(
    sql: str,
    filename: Optional[str] = None,
    format: str = "xlsx",
) -> str:
    """Run read-only SQL, save ALL rows to reports/ folder, return file path."""
    ok, err = sql_validate(sql)
    if not ok:
        return json.dumps({"error": err})

    conn = db.get_connection()
    datasets = db.list_datasets(conn)
    conn.close()

    if not datasets:
        return json.dumps({"error": "No datasets loaded."})

    with connect(datasets) as duck:
        try:
            result = duck.execute(sql)
            headers = [d[0] for d in result.description]
            rows = [list(r) for r in result.fetchall()]
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    report = rpt.export_query_to_xlsx(headers, rows, filename=filename, sql_query=sql)
    return json.dumps(report, indent=2)


@mcp.tool()
def row_count_summary() -> str:
    """Return row counts for all loaded datasets."""
    conn = db.get_connection()
    datasets = db.list_datasets(conn)
    conn.close()

    if not datasets:
        return json.dumps({"error": "No datasets loaded."})

    counts = []
    with connect(datasets) as duck:
        for ds in datasets:
            try:
                n = duck.execute(f"SELECT COUNT(*) FROM {quote(ds['id'])}").fetchone()[0]
            except Exception:
                n = None
            counts.append({"dataset": ds["id"], "side": ds["side"], "row_count": n})

    return json.dumps(counts, indent=2)


# ═══════════════════════════════════════════════════════════════
#  5.2 — Profiling tools
# ═══════════════════════════════════════════════════════════════


@mcp.tool()
def data_profile(dataset_id: str) -> str:
    """Profile a dataset: per-column distinct, min, max, blanks."""
    result = prof.data_profile(dataset_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def column_value_summary(
    dataset_id: str,
    column: Optional[str] = None,
    top_n: int = 10,
) -> str:
    """Top-N value frequencies and blank counts for one or all columns."""
    result = prof.column_value_summary(dataset_id, column=column, top_n=top_n)
    return json.dumps(result, indent=2)


@mcp.tool()
def export_column_value_summary(
    dataset_id: str,
    column: Optional[str] = None,
    top_n: int = 5,
    filename: Optional[str] = None,
) -> str:
    """Create an XLSX Top-N value summary report (Column | Top 1..Top N | Blanks) in reports/."""
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
    """Frequency of combined-field value tuples. Columns as comma-separated string."""
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
    """Preview records matching a filter (exact value or blanks)."""
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
    """Find duplicate groups based on comma-separated key columns."""
    keys = [k.strip() for k in key_columns.split(",") if k.strip()]
    result = prof.find_duplicates(dataset_id, keys, limit=limit)
    return json.dumps(result, indent=2)


@mcp.tool()
def value_distribution(
    dataset_id: str,
    column: str,
    limit: int = 20,
) -> str:
    """Frequency counts for a single column, sorted by count descending."""
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
    """List compact pair metadata (no field mappings). Optional filter by source/target dataset IDs."""
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
    """List field mappings for a single pair_id."""
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
        """DEBUG ONLY: list all pairs including full key/compare mappings."""
        pairs = cat.get_pairs()
        return json.dumps(pairs, indent=2)

@mcp.tool()
def upsert_pair_override(
    source_dataset_id: str,
    target_dataset_id: str,
    enabled: bool = True,
) -> str:
    """Create or update a manual pair override."""
    result = cat.upsert_pair_override(source_dataset_id, target_dataset_id, enabled=enabled)
    return json.dumps(result, indent=2)


@mcp.tool()
def suggest_keys(pair_id: str) -> str:
    """Suggest candidate key columns for a pair based on uniqueness, completeness, and overlap."""
    result = prof.suggest_keys(pair_id)
    return json.dumps(result, indent=2)


@mcp.tool()
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
    """List saved key presets for a pair."""
    conn = db.get_connection()
    presets = db.list_key_presets(conn, pair_id)
    conn.close()
    return json.dumps(presets, indent=2)


@mcp.tool()
def link_related_tables(
    side: str = "target",
    min_confidence: float = 0.9,
    suggest_only: bool = False,
) -> str:
    """Discover high-confidence same-side dataset relationships and optionally persist them."""
    result = rel.link_related_tables(
        side=side,
        min_confidence=min_confidence,
        suggest_only=suggest_only,
    )
    return json.dumps(result, indent=2)


@mcp.tool()
def get_dataset_links(
    dataset_id: str,
) -> str:
    """Return compact relationship links for a dataset to guide SQL join construction."""
    conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if not ds:
        conn.close()
        return json.dumps({"error": f"Dataset '{dataset_id}' not found."})

    rels = db.list_relationships(
        conn,
        side=ds["side"],
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
    """Compare column schemas between two datasets: find missing/extra/common columns."""
    result = cat.schema_diff(source_dataset_id, target_dataset_id)
    return json.dumps(result, indent=2)


# ═══════════════════════════════════════════════════════════════
#  5.4 — Comparison and report tools
# ═══════════════════════════════════════════════════════════════


@mcp.tool()
def start_comparison_job(
    source_dataset_id: str,
    target_dataset_id: str,
    key_fields: str,
    pair_id: Optional[str] = None,
    compare_fields: Optional[str] = None,
) -> str:
    """Start a comparison job. key/compare fields can be source or target names when pair mappings exist."""
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
    """Quick ad-hoc comparison. key/compare fields can be source or target names when pair mappings exist."""
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
) -> str:
    """Drill-down: per-row diffs for a single field between source and target."""
    keys = [k.strip() for k in key_columns.split(",") if k.strip()]
    result = comp.compare_field(
        source_id=source_dataset_id,
        target_id=target_dataset_id,
        key_columns=keys,
        field=field,
        limit=limit,
    )
    return json.dumps(result, indent=2, default=str)


@mcp.tool()
def get_job_status(job_id: str) -> str:
    """Get status and progress of a comparison job."""
    result = job_svc.get_job_status(job_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def get_job_summary(job_id: str) -> str:
    """Get detailed summary of a completed job including report info."""
    result = job_svc.get_job_summary(job_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def cancel_job(job_id: str) -> str:
    """Cancel a queued or running job."""
    result = job_svc.cancel_job(job_id)
    return json.dumps(result, indent=2)


@mcp.tool()
def list_reports(limit: int = 5) -> str:
    """List recent generated XLSX reports (default: last 5)."""
    limit = max(1, min(int(limit), 500))
    conn = db.get_connection()
    reports = db.list_reports(conn, limit=limit)
    conn.close()
    return json.dumps(reports, indent=2)


@mcp.tool()
def get_report_metadata(report_id: str) -> str:
    """Get metadata for a specific report."""
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    conn.close()
    if not report:
        return json.dumps({"error": f"Report '{report_id}' not found."})
    return json.dumps(report, indent=2)


@mcp.tool()
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
    datasets = db.list_datasets(conn)
    conn.close()
    return json.dumps(
        [{"id": d["id"], "side": d["side"], "file_name": d["file_name"]} for d in datasets]
    )


@mcp.resource("data://datasets/{dataset_id}/schema")
def resource_dataset_schema(dataset_id: str) -> str:
    """JSON column list for a specific dataset."""
    ds = cat.get_dataset(dataset_id)
    if not ds:
        return json.dumps({"error": f"Dataset '{dataset_id}' not found."})
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


if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)
