"""
ComparisonService.

Diff engine: compares source vs target datasets by key columns.
Produces ADDED (target-only), REMOVED (source-only), and CHANGED
(field-level before/after) results using DuckDB JOINs.
Also detects schema drift and duplicate keys.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from server import db
from server.query_engine import connect, quote

HARD_CAP = 100


def _mapping_label(mapping: Dict[str, str]) -> str:
    if mapping["source_field"] == mapping["target_field"]:
        return mapping["source_field"]
    return f"{mapping['source_field']}->{mapping['target_field']}"


def _normalize_pair_mappings(
    source_columns: List[str],
    target_columns: List[str],
    key_columns: List[str],
    compare_columns: Optional[List[str]],
    key_mappings: Optional[List[Dict[str, str]]] = None,
    compare_mappings: Optional[List[Dict[str, str]]] = None,
) -> tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    src_set = set(source_columns)
    tgt_set = set(target_columns)
    common = sorted(src_set & tgt_set)

    if key_mappings:
        normalized_keys = [
            {
                "source_field": (m.get("source_field") or m.get("source") or "").strip(),
                "target_field": (m.get("target_field") or m.get("target") or "").strip(),
            }
            for m in key_mappings
        ]
    else:
        normalized_keys = [{"source_field": k, "target_field": k} for k in key_columns]
    normalized_keys = [m for m in normalized_keys if m["source_field"] and m["target_field"]]
    if not normalized_keys:
        raise ValueError("No key mappings specified.")

    seen_keys = set()
    for m in normalized_keys:
        if m["source_field"] not in src_set:
            raise ValueError(f"Key source field '{m['source_field']}' not found in source dataset.")
        if m["target_field"] not in tgt_set:
            raise ValueError(f"Key target field '{m['target_field']}' not found in target dataset.")
        sig = (m["source_field"], m["target_field"])
        if sig in seen_keys:
            raise ValueError(f"Duplicate key mapping '{m['source_field']} -> {m['target_field']}'.")
        seen_keys.add(sig)

    key_source_names = {m["source_field"] for m in normalized_keys}

    if compare_mappings:
        normalized_compare = [
            {
                "source_field": (m.get("source_field") or m.get("source") or "").strip(),
                "target_field": (m.get("target_field") or m.get("target") or "").strip(),
            }
            for m in compare_mappings
        ]
        normalized_compare = [m for m in normalized_compare if m["source_field"] and m["target_field"]]
    elif compare_columns:
        normalized_compare = [{"source_field": c, "target_field": c} for c in compare_columns]
    else:
        normalized_compare = [
            {"source_field": c, "target_field": c}
            for c in common
            if c not in key_source_names
        ]

    seen_compare = set()
    for m in normalized_compare:
        if m["source_field"] not in src_set:
            raise ValueError(f"Compare source field '{m['source_field']}' not found in source dataset.")
        if m["target_field"] not in tgt_set:
            raise ValueError(f"Compare target field '{m['target_field']}' not found in target dataset.")
        sig = (m["source_field"], m["target_field"])
        if sig in seen_compare:
            raise ValueError(f"Duplicate compare mapping '{m['source_field']} -> {m['target_field']}'.")
        seen_compare.add(sig)

    labels = [_mapping_label(m) for m in normalized_compare]
    return normalized_keys, normalized_compare, labels


def compare_datasets(
    source_id: str,
    target_id: str,
    key_columns: List[str],
    compare_columns: Optional[List[str]] = None,
    sample_limit: int = 10,
    key_mappings: Optional[List[Dict[str, str]]] = None,
    compare_mappings: Optional[List[Dict[str, str]]] = None,
    conn=None,
) -> Dict[str, Any]:
    """Run a full source-vs-target comparison.

    Returns summary counts and capped sample rows for each category:
    ADDED, REMOVED, CHANGED, plus schema drift info.
    """
    own = conn is None
    if own:
        conn = db.get_connection()
    src = db.get_dataset(conn, source_id)
    tgt = db.get_dataset(conn, target_id)
    if own:
        conn.close()

    if not src:
        return {"error": f"Source dataset '{source_id}' not found."}
    if not tgt:
        return {"error": f"Target dataset '{target_id}' not found."}

    try:
        key_maps, comp_maps, comp_labels = _normalize_pair_mappings(
            source_columns=src["columns"],
            target_columns=tgt["columns"],
            key_columns=key_columns,
            compare_columns=compare_columns,
            key_mappings=key_mappings,
            compare_mappings=compare_mappings,
        )
    except ValueError as exc:
        return {"error": str(exc)}

    # Schema drift remains name-based information.
    src_cols = set(src["columns"])
    tgt_cols = set(tgt["columns"])
    common_cols = sorted(src_cols & tgt_cols)
    source_only_cols = sorted(src_cols - tgt_cols)
    target_only_cols = sorted(tgt_cols - src_cols)

    sample_limit = min(int(sample_limit), HARD_CAP)
    datasets = [src, tgt]

    with connect(datasets) as duck:
        sv = quote(source_id)
        tv = quote(target_id)

        # Materialize external-file views once so repeated queries do not
        # re-read source files for each count/sample computation.
        src_tmp = "__cmp_source"
        tgt_tmp = "__cmp_target"
        duck.execute(f"CREATE TEMP TABLE {quote(src_tmp)} AS SELECT * FROM {sv}")
        duck.execute(f"CREATE TEMP TABLE {quote(tgt_tmp)} AS SELECT * FROM {tv}")
        sv = quote(src_tmp)
        tv = quote(tgt_tmp)

        join_on = " AND ".join(
            f"s.{quote(m['source_field'])} = t.{quote(m['target_field'])}" for m in key_maps
        )
        key_sel_src = ", ".join(
            f"s.{quote(m['source_field'])} AS {quote(m['source_field'])}" for m in key_maps
        )
        src_key_group = ", ".join(quote(m["source_field"]) for m in key_maps)
        tgt_key_group = ", ".join(quote(m["target_field"]) for m in key_maps)

        src_count = duck.execute(f"SELECT COUNT(*) FROM {sv}").fetchone()[0]
        tgt_count = duck.execute(f"SELECT COUNT(*) FROM {tv}").fetchone()[0]

        src_dups = duck.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {src_key_group}
                FROM {sv}
                GROUP BY {src_key_group}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        tgt_dups = duck.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {tgt_key_group}
                FROM {tv}
                GROUP BY {tgt_key_group}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        duck.execute(
            f"""
            CREATE TEMP TABLE "__cmp_added" AS
            SELECT t.* FROM {tv} t
            WHERE NOT EXISTS (
                SELECT 1 FROM {sv} s WHERE {join_on}
            )
            """
        )
        added_count = duck.execute('SELECT COUNT(*) FROM "__cmp_added"').fetchone()[0]

        added_sample = []
        if added_count > 0:
            added_rows = duck.execute(
                f"""
                SELECT * FROM "__cmp_added"
                LIMIT {sample_limit}
                """
            )
            headers = [d[0] for d in added_rows.description]
            added_sample = [dict(zip(headers, row)) for row in added_rows.fetchall()]

        duck.execute(
            f"""
            CREATE TEMP TABLE "__cmp_removed" AS
            SELECT s.* FROM {sv} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {tv} t WHERE {join_on}
            )
            """
        )
        removed_count = duck.execute('SELECT COUNT(*) FROM "__cmp_removed"').fetchone()[0]

        removed_sample = []
        if removed_count > 0:
            removed_rows = duck.execute(
                f"""
                SELECT * FROM "__cmp_removed"
                LIMIT {sample_limit}
                """
            )
            headers = [d[0] for d in removed_rows.description]
            removed_sample = [dict(zip(headers, row)) for row in removed_rows.fetchall()]

        changed_count = 0
        changed_sample: List[Dict[str, Any]] = []
        if comp_maps:
            diff_cond = " OR ".join(
                f"CAST(s.{quote(m['source_field'])} AS VARCHAR) IS DISTINCT FROM "
                f"CAST(t.{quote(m['target_field'])} AS VARCHAR)"
                for m in comp_maps
            )
            diff_cols = ", ".join(
                f"CAST(s.{quote(m['source_field'])} AS VARCHAR) AS src_{i}, "
                f"CAST(t.{quote(m['target_field'])} AS VARCHAR) AS tgt_{i}"
                for i, m in enumerate(comp_maps)
            )
            duck.execute(
                f"""
                CREATE TEMP TABLE "__cmp_changed" AS
                SELECT {key_sel_src}, {diff_cols}
                FROM {sv} s
                INNER JOIN {tv} t ON {join_on}
                WHERE {diff_cond}
                """
            )
            changed_rows = duck.execute(
                f"""
                SELECT * FROM "__cmp_changed"
                LIMIT {sample_limit}
                """
            )
            rows = changed_rows.fetchall()

            changed_count = duck.execute('SELECT COUNT(*) FROM "__cmp_changed"').fetchone()[0]

            key_names = [m["source_field"] for m in key_maps]
            key_len = len(key_names)
            for row in rows:
                key_vals = {key_names[i]: str(row[i]) if row[i] is not None else None for i in range(key_len)}
                diffs = []
                for idx, mapping in enumerate(comp_maps):
                    src_val = row[key_len + idx * 2]
                    tgt_val = row[key_len + idx * 2 + 1]
                    src_text = str(src_val) if src_val is not None else None
                    tgt_text = str(tgt_val) if tgt_val is not None else None
                    if src_text != tgt_text:
                        diffs.append(
                            {
                                "field": _mapping_label(mapping),
                                "source_field": mapping["source_field"],
                                "target_field": mapping["target_field"],
                                "source": src_text,
                                "target": tgt_text,
                            }
                        )
                if diffs:
                    changed_sample.append({"keys": key_vals, "changes": diffs})

        matched_count = duck.execute(
            f"""
            SELECT COUNT(*) FROM {sv} s
            INNER JOIN {tv} t ON {join_on}
            """
        ).fetchone()[0]
        unchanged = matched_count - changed_count

    return {
        "source": source_id,
        "target": target_id,
        "key_columns": [m["source_field"] for m in key_maps],
        "key_mappings": key_maps,
        "compare_columns": comp_labels,
        "compare_mappings": comp_maps,
        "source_rows": src_count,
        "target_rows": tgt_count,
        "source_duplicate_keys": src_dups,
        "target_duplicate_keys": tgt_dups,
        "schema_drift": {
            "source_only_columns": source_only_cols,
            "target_only_columns": target_only_cols,
            "common_columns": len(common_cols),
        },
        "added_count": added_count,
        "removed_count": removed_count,
        "changed_count": changed_count,
        "unchanged_count": unchanged,
        "added_sample": added_sample[:sample_limit],
        "removed_sample": removed_sample[:sample_limit],
        "changed_sample": changed_sample[:sample_limit],
    }

def compare_field(
    source_id: str,
    target_id: str,
    key_columns: List[str],
    field: str,
    limit: int = 10,
    key_mappings: Optional[List[Dict[str, str]]] = None,
    field_mapping: Optional[Dict[str, str]] = None,
    conn=None,
) -> Dict[str, Any]:
    """Per-row diffs for a single field (drill-down tool)."""
    own = conn is None
    if own:
        conn = db.get_connection()
    src = db.get_dataset(conn, source_id)
    tgt = db.get_dataset(conn, target_id)
    if own:
        conn.close()

    if not src:
        return {"error": f"Source '{source_id}' not found."}
    if not tgt:
        return {"error": f"Target '{target_id}' not found."}

    compare_mappings = [field_mapping] if field_mapping else None
    compare_columns = None if compare_mappings else [field]
    try:
        key_maps, comp_maps, _ = _normalize_pair_mappings(
            source_columns=src["columns"],
            target_columns=tgt["columns"],
            key_columns=key_columns,
            compare_columns=compare_columns,
            key_mappings=key_mappings,
            compare_mappings=compare_mappings,
        )
    except ValueError as exc:
        return {"error": str(exc)}

    if not comp_maps:
        return {"error": f"Field '{field}' not in both datasets."}
    resolved_field = comp_maps[0]
    source_field = resolved_field["source_field"]
    target_field = resolved_field["target_field"]

    limit = min(int(limit), HARD_CAP)
    datasets = [src, tgt]

    with connect(datasets) as duck:
        sv = quote(source_id)
        tv = quote(target_id)
        qs = quote(source_field)
        qt = quote(target_field)

        join_on = " AND ".join(
            f"s.{quote(m['source_field'])} = t.{quote(m['target_field'])}" for m in key_maps
        )
        key_sel = ", ".join(
            f"s.{quote(m['source_field'])} AS {quote(m['source_field'])}" for m in key_maps
        )
        key_labels = [m["source_field"] for m in key_maps]

        rows = duck.execute(
            f"""
            WITH diff AS (
                SELECT {key_sel},
                       CAST(s.{qs} AS VARCHAR) AS source_value,
                       CAST(t.{qt} AS VARCHAR) AS target_value
                FROM {sv} s
                INNER JOIN {tv} t ON {join_on}
                WHERE CAST(s.{qs} AS VARCHAR) IS DISTINCT FROM CAST(t.{qt} AS VARCHAR)
            ),
            ranked AS (
                SELECT *,
                       COUNT(*) OVER () AS total_differences,
                       ROW_NUMBER() OVER () AS rn
                FROM diff
            )
            SELECT {", ".join(quote(k) for k in key_labels)},
                   source_value,
                   target_value,
                   total_differences
            FROM ranked
            WHERE rn <= {limit}
            """
        ).fetchall()

        total_diffs = rows[0][len(key_labels) + 2] if rows else 0
        diff_rows = []
        for r in rows:
            entry = {key_labels[i]: r[i] for i in range(len(key_labels))}
            entry["source_value"] = r[len(key_labels)]
            entry["target_value"] = r[len(key_labels) + 1]
            diff_rows.append(entry)

    return {
        "source": source_id,
        "target": target_id,
        "field": field,
        "source_field": source_field,
        "target_field": target_field,
        "field_mapping": _mapping_label(resolved_field),
        "total_differences": total_diffs,
        "showing": len(diff_rows),
        "rows": diff_rows,
    }


def compare_full(
    source_id: str,
    target_id: str,
    key_columns: List[str],
    compare_columns: Optional[List[str]] = None,
    key_mappings: Optional[List[Dict[str, str]]] = None,
    compare_mappings: Optional[List[Dict[str, str]]] = None,
    conn=None,
) -> Dict[str, Any]:
    """Run full comparison returning ALL rows (for XLSX reports).

    Not exposed as an MCP tool - only called by ReportService/JobService.
    """
    own = conn is None
    if own:
        conn = db.get_connection()
    src = db.get_dataset(conn, source_id)
    tgt = db.get_dataset(conn, target_id)
    if own:
        conn.close()

    if not src or not tgt:
        return {"error": "Dataset not found."}

    try:
        key_maps, comp_maps, comp_labels = _normalize_pair_mappings(
            source_columns=src["columns"],
            target_columns=tgt["columns"],
            key_columns=key_columns,
            compare_columns=compare_columns,
            key_mappings=key_mappings,
            compare_mappings=compare_mappings,
        )
    except ValueError as exc:
        return {"error": str(exc)}

    datasets = [src, tgt]

    with connect(datasets) as duck:
        sv = quote(source_id)
        tv = quote(target_id)

        # Materialize external-file views once so full-report queries do not
        # repeatedly scan CSV/XLS(X) sources.
        src_tmp = "__cmp_full_source"
        tgt_tmp = "__cmp_full_target"
        duck.execute(f"CREATE TEMP TABLE {quote(src_tmp)} AS SELECT * FROM {sv}")
        duck.execute(f"CREATE TEMP TABLE {quote(tgt_tmp)} AS SELECT * FROM {tv}")
        sv = quote(src_tmp)
        tv = quote(tgt_tmp)

        join_on = " AND ".join(
            f"s.{quote(m['source_field'])} = t.{quote(m['target_field'])}" for m in key_maps
        )

        added_rows_result = duck.execute(
            f"SELECT t.* FROM {tv} t WHERE NOT EXISTS (SELECT 1 FROM {sv} s WHERE {join_on})"
        )
        added_headers = [d[0] for d in added_rows_result.description]
        added_data = [list(r) for r in added_rows_result.fetchall()]

        removed_rows_result = duck.execute(
            f"SELECT s.* FROM {sv} s WHERE NOT EXISTS (SELECT 1 FROM {tv} t WHERE {join_on})"
        )
        removed_headers = [d[0] for d in removed_rows_result.description]
        removed_data = [list(r) for r in removed_rows_result.fetchall()]

        changed_data: List[List[Any]] = []
        changed_headers: List[str] = []
        changed_field_pairs: List[Dict[str, str]] = []
        if comp_maps:
            diff_cond = " OR ".join(
                f"CAST(s.{quote(m['source_field'])} AS VARCHAR) IS DISTINCT FROM "
                f"CAST(t.{quote(m['target_field'])} AS VARCHAR)"
                for m in comp_maps
            )
            key_select = ", ".join(
                f"s.{quote(m['source_field'])} AS {quote(m['source_field'])}" for m in key_maps
            )
            compare_select_parts = []
            for idx, mapping in enumerate(comp_maps):
                source_alias = f"source_map_{idx}"
                target_alias = f"target_map_{idx}"
                compare_select_parts.append(
                    f"CAST(s.{quote(mapping['source_field'])} AS VARCHAR) AS {quote(source_alias)}, "
                    f"CAST(t.{quote(mapping['target_field'])} AS VARCHAR) AS {quote(target_alias)}"
                )
                changed_field_pairs.append(
                    {
                        "label": _mapping_label(mapping),
                        "source_field": mapping["source_field"],
                        "target_field": mapping["target_field"],
                        "source_header": source_alias,
                        "target_header": target_alias,
                    }
                )
            compare_select = ", ".join(compare_select_parts)
            result = duck.execute(
                f"""
                SELECT {key_select}, {compare_select}
                FROM {sv} s
                INNER JOIN {tv} t ON {join_on}
                WHERE {diff_cond}
                """
            )
            changed_headers = [d[0] for d in result.description]
            changed_data = [list(r) for r in result.fetchall()]

    return {
        "source": source_id,
        "target": target_id,
        "key_columns": [m["source_field"] for m in key_maps],
        "key_mappings": key_maps,
        "compare_columns": comp_labels,
        "compare_mappings": comp_maps,
        "schema_drift": {
            "source_only": sorted(set(src["columns"]) - set(tgt["columns"])),
            "target_only": sorted(set(tgt["columns"]) - set(src["columns"])),
        },
        "added": {"headers": added_headers, "data": added_data},
        "removed": {"headers": removed_headers, "data": removed_data},
        "changed": {
            "headers": changed_headers,
            "data": changed_data,
            "field_pairs": changed_field_pairs,
        },
    }
