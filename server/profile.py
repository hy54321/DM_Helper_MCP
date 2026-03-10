"""
ProfileService.

Per-column data profiling: non-null %, distinct counts, min/max,
top-N value frequencies, blank counts, and duplicate detection.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from server import db
from server.query_engine import connect, format_results, quote

DEFAULT_TOP_N = 10
DEFAULT_LIMIT = 10
HARD_CAP = 100


# ═══════════════════════════════════════════════════════════════
#  Column-level data profile
# ═══════════════════════════════════════════════════════════════

def data_profile(dataset_id: str, conn=None) -> Dict[str, Any]:
    """Return per-column statistics for a dataset.

    For each column: distinct count, min/max values, and blank counts.
    """
    own = conn is None
    if own:
        conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if own:
        conn.close()

    if not ds:
        return {"error": f"Dataset '{dataset_id}' not found."}

    columns = ds["columns"]
    if not columns:
        return {"error": f"Dataset '{dataset_id}' has no columns."}

    datasets = [ds]
    with connect(datasets) as duck:
        view = quote(dataset_id)

        # Build one aggregate query for all columns to avoid repeated full scans.
        exprs: List[str] = []
        for col in columns:
            qc = quote(col)
            exprs.extend(
                [
                    f"COUNT(DISTINCT {qc}) AS {quote(f'{col}__distinct')}",
                    f"MIN({qc}) AS {quote(f'{col}__min')}",
                    f"MAX({qc}) AS {quote(f'{col}__max')}",
                    f"SUM(CASE WHEN TRIM(CAST({qc} AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS {quote(f'{col}__blank')}",
                ]
            )

        sql = (
            f"SELECT COUNT(*) AS total_rows,\n  {',\n  '.join(exprs)}\n"
            f"FROM {view}"
        )
        row = duck.execute(sql).fetchone()
        if row is None:
            return {"error": f"Dataset '{dataset_id}' could not be profiled."}

        total_rows = row[0]
        profile: List[Dict[str, Any]] = []
        idx = 1
        for col in columns:
            distinct_count = row[idx]
            min_val = row[idx + 1]
            max_val = row[idx + 2]
            blank_count = row[idx + 3]
            idx += 4
            profile.append(
                {
                    "column": col,
                    "distinct": distinct_count,
                    "min": str(min_val) if min_val is not None else None,
                    "max": str(max_val) if max_val is not None else None,
                    "blank_count": blank_count or 0,
                }
            )

    return {
        "dataset": dataset_id,
        "total_rows": total_rows,
        "columns": profile,
    }


# ═══════════════════════════════════════════════════════════════
#  Column value summary (top-N frequencies)
# ═══════════════════════════════════════════════════════════════

def column_value_summary(
    dataset_id: str,
    column: Optional[str] = None,
    top_n: int = DEFAULT_TOP_N,
    conn=None,
) -> Dict[str, Any]:
    """Top-N value frequencies + blank count for one or all columns."""
    own = conn is None
    if own:
        conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if own:
        conn.close()

    if not ds:
        return {"error": f"Dataset '{dataset_id}' not found."}

    top_n = max(1, min(int(top_n), HARD_CAP))
    columns = [column] if column else ds["columns"]
    datasets = [ds]

    result: Dict[str, Any] = {"dataset": dataset_id, "summaries": []}

    with connect(datasets) as duck:
        view = quote(dataset_id)
        table_ref = view
        try:
            temp_table = quote("__tmp_column_value_summary")
            duck.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {view}")
            table_ref = temp_table
        except Exception:
            table_ref = view

        for col in columns:
            if col not in ds["columns"]:
                result["summaries"].append({"column": col, "error": "Column not found."})
                continue
            qc = quote(col)
            try:
                rows = duck.execute(
                    f"""
                    WITH grouped AS (
                        SELECT
                            CAST({qc} AS VARCHAR) AS value,
                            COUNT(*) AS cnt
                        FROM {table_ref}
                        GROUP BY {qc}
                    ),
                    ranked AS (
                        SELECT
                            value,
                            cnt,
                            SUM(
                                CASE
                                    WHEN value IS NULL OR TRIM(value) = '' THEN cnt
                                    ELSE 0
                                END
                            ) OVER () AS blank_or_null_count,
                            ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn
                        FROM grouped
                    )
                    SELECT value, cnt, blank_or_null_count
                    FROM ranked
                    WHERE rn <= {top_n}
                    ORDER BY cnt DESC
                    """
                ).fetchall()
                blank_or_null_count = 0
                if rows and rows[0][2] is not None:
                    blank_or_null_count = rows[0][2]
                result["summaries"].append(
                    {
                        "column": col,
                        "top_values": [{"value": r[0], "count": r[1]} for r in rows],
                        "blank_or_null_count": blank_or_null_count,
                    }
                )
            except Exception as exc:
                result["summaries"].append({"column": col, "error": str(exc)})

    return result


# ═══════════════════════════════════════════════════════════════
#  Combo value summary (multi-column frequency)
# ═══════════════════════════════════════════════════════════════

def combo_value_summary(
    dataset_id: str,
    columns: List[str],
    top_n: int = DEFAULT_TOP_N,
    conn=None,
) -> Dict[str, Any]:
    """Frequency of combined-field value tuples."""
    own = conn is None
    if own:
        conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if own:
        conn.close()

    if not ds:
        return {"error": f"Dataset '{dataset_id}' not found."}
    if not columns:
        return {"error": "No columns specified."}

    missing = [c for c in columns if c not in ds["columns"]]
    if missing:
        return {"error": f"Columns not found: {missing}"}
    top_n = max(1, min(int(top_n), HARD_CAP))

    datasets = [ds]
    with connect(datasets) as duck:
        view = quote(dataset_id)
        transformed_aliases = [f"c_{idx}" for idx in range(len(columns))]
        sel = ", ".join(
            f"CASE WHEN {quote(c)} IS NULL OR TRIM(CAST({quote(c)} AS VARCHAR)) = '' THEN '' "
            f"ELSE TRIM(CAST({quote(c)} AS VARCHAR)) END AS {quote(alias)}"
            for c, alias in zip(columns, transformed_aliases)
        )
        grp = ", ".join(quote(alias) for alias in transformed_aliases)
        rows = duck.execute(
            f"""
            WITH normalized AS (
                SELECT {sel}
                FROM {view}
            )
            SELECT {grp}, COUNT(*) AS cnt
            FROM normalized
            GROUP BY {grp}
            """
        ).fetchall()

    counts: Dict[str, int] = {}
    blank_or_null_count = 0
    for r in rows:
        values = [r[i] if r[i] is not None else "" for i in range(len(columns))]
        count = int(r[len(columns)] or 0)
        if not any(v != "" for v in values):
            blank_or_null_count += count
            continue
        combo_key = " - ".join(values)
        counts[combo_key] = counts.get(combo_key, 0) + count

    sorted_items = sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))
    top_items = sorted_items[:top_n]
    result_rows = [{"value": value, "count": cnt} for value, cnt in top_items]

    return {
        "dataset": dataset_id,
        "columns": columns,
        "column": " - ".join(columns),
        "top_values": [{"value": v, "count": c} for v, c in top_items],
        "blank_or_null_count": blank_or_null_count,
        "combos": result_rows,
        "top_n": top_n,
    }


# ═══════════════════════════════════════════════════════════════
#  Filtered preview
# ═══════════════════════════════════════════════════════════════

def preview_filtered_records(
    dataset_id: str,
    filter_spec: Dict[str, Any],
    limit: int = DEFAULT_LIMIT,
    conn=None,
) -> Dict[str, Any]:
    """Preview records matching a filter specification.

    ``filter_spec`` keys:
      - ``column``: column name
      - ``value``: exact value to match (string or None for blanks)
      - ``blanks_only``: True to match NULL / empty string
    """
    own = conn is None
    if own:
        conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if own:
        conn.close()

    if not ds:
        return {"error": f"Dataset '{dataset_id}' not found."}

    limit = min(int(limit), HARD_CAP)
    datasets = [ds]

    col = filter_spec.get("column")
    if col and col not in ds["columns"]:
        return {"error": f"Column '{col}' not found."}

    with connect(datasets) as duck:
        view = quote(dataset_id)

        where_parts: List[str] = []
        if col:
            qc = quote(col)
            if filter_spec.get("blanks_only"):
                where_parts.append(
                    f"(TRIM(CAST({qc} AS VARCHAR)) = '' OR {qc} IS NULL)"
                )
            elif "value" in filter_spec:
                val = str(filter_spec["value"]).replace("'", "''")
                where_parts.append(f"CAST({qc} AS VARCHAR) = '{val}'")

        where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        sql = f"SELECT * FROM {view} {where} LIMIT {limit}"

        result = duck.execute(sql)
        headers = [d[0] for d in result.description]
        rows = result.fetchall()

    return {
        "dataset": dataset_id,
        "filter": filter_spec,
        "row_count": len(rows),
        "headers": headers,
        "rows": [list(r) for r in rows],
    }


# ═══════════════════════════════════════════════════════════════
#  Duplicate detection
# ═══════════════════════════════════════════════════════════════

def find_duplicates(
    dataset_id: str,
    key_columns: List[str],
    limit: int = DEFAULT_LIMIT,
    conn=None,
) -> Dict[str, Any]:
    """Find duplicate groups based on key columns."""
    own = conn is None
    if own:
        conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if own:
        conn.close()

    if not ds:
        return {"error": f"Dataset '{dataset_id}' not found."}
    if not key_columns:
        return {"error": "No key columns specified."}

    missing = [c for c in key_columns if c not in ds["columns"]]
    if missing:
        return {"error": f"Columns not found: {missing}"}

    limit = min(int(limit), HARD_CAP)
    datasets = [ds]

    with connect(datasets) as duck:
        view = quote(dataset_id)
        grp = ", ".join(quote(c) for c in key_columns)
        sel = ", ".join(
            f"CAST({quote(c)} AS VARCHAR) AS {quote(c)}" for c in key_columns
        )
        rows = duck.execute(
            f"""
            WITH dup AS (
                SELECT {sel}, COUNT(*) AS dup_count
                FROM {view}
                GROUP BY {grp}
                HAVING COUNT(*) > 1
            ),
            ranked AS (
                SELECT
                    *,
                    COUNT(*) OVER () AS total_dup_groups,
                    ROW_NUMBER() OVER (ORDER BY dup_count DESC) AS rn
                FROM dup
            )
            SELECT {grp}, dup_count, total_dup_groups
            FROM ranked
            WHERE rn <= {limit}
            ORDER BY dup_count DESC
            """
        ).fetchall()

    dup_rows = []
    total_dup_groups = 0
    if rows:
        total_dup_groups = rows[0][len(key_columns) + 1] or 0

    for r in rows:
        entry = {key_columns[i]: r[i] for i in range(len(key_columns))}
        dup_count = r[len(key_columns)]
        if dup_count is not None:
            entry["duplicate_count"] = dup_count
            dup_rows.append(entry)

    return {
        "dataset": dataset_id,
        "key_columns": key_columns,
        "total_duplicate_groups": total_dup_groups,
        "showing": len(dup_rows),
        "duplicates": dup_rows,
    }


# ═══════════════════════════════════════════════════════════════
#  Value distribution
# ═══════════════════════════════════════════════════════════════

def value_distribution(
    dataset_id: str,
    column: str,
    limit: int = 20,
    conn=None,
) -> Dict[str, Any]:
    """Frequency counts for a single column, sorted by count desc."""
    own = conn is None
    if own:
        conn = db.get_connection()
    ds = db.get_dataset(conn, dataset_id)
    if own:
        conn.close()

    if not ds:
        return {"error": f"Dataset '{dataset_id}' not found."}
    if column not in ds["columns"]:
        return {"error": f"Column '{column}' not found."}

    limit = min(int(limit), HARD_CAP)
    datasets = [ds]

    with connect(datasets) as duck:
        view = quote(dataset_id)
        qc = quote(column)

        total_distinct = duck.execute(
            f"SELECT COUNT(DISTINCT {qc}) FROM {view}"
        ).fetchone()[0]

        rows = duck.execute(
            f"""
            SELECT CAST({qc} AS VARCHAR) AS value, COUNT(*) AS cnt
            FROM {view}
            GROUP BY {qc}
            ORDER BY cnt DESC
            LIMIT {limit}
            """
        ).fetchall()

    return {
        "dataset": dataset_id,
        "column": column,
        "total_distinct_values": total_distinct,
        "showing": len(rows),
        "distribution": [{"value": r[0], "count": r[1]} for r in rows],
    }


# ═══════════════════════════════════════════════════════════════
#  Key suggestion heuristics
# ═══════════════════════════════════════════════════════════════

def suggest_keys(
    pair_id: str,
    conn=None,
) -> Dict[str, Any]:
    """Suggest candidate key columns for a pair based on profiling heuristics.

    Scoring criteria:
    - High uniqueness ratio (distinct / total rows)
    - Low null/blank ratio
    - High overlap between source and target values
    """
    own = conn is None
    if own:
        conn = db.get_connection()
    pair = db.get_pair(conn, pair_id)
    if not pair:
        if own:
            conn.close()
        return {"error": f"Pair '{pair_id}' not found."}

    src = db.get_dataset(conn, pair["source_dataset"])
    tgt = db.get_dataset(conn, pair["target_dataset"])
    if own:
        conn.close()

    if not src or not tgt:
        return {"error": "Source or target dataset not found."}

    # Find common columns
    common = sorted(set(src["columns"]) & set(tgt["columns"]))
    if not common:
        return {"error": "No common columns between source and target."}

    datasets = [src, tgt]
    candidates: List[Dict[str, Any]] = []

    with connect(datasets) as duck:
        src_view = quote(src["id"])
        tgt_view = quote(tgt["id"])
        src_table = src_view
        tgt_table = tgt_view

        # Materialize source/target once to avoid repeatedly scanning files.
        try:
            src_temp = quote("__tmp_suggest_keys_src")
            tgt_temp = quote("__tmp_suggest_keys_tgt")
            duck.execute(f"CREATE TEMP TABLE {src_temp} AS SELECT * FROM {src_view}")
            duck.execute(f"CREATE TEMP TABLE {tgt_temp} AS SELECT * FROM {tgt_view}")
            src_table = src_temp
            tgt_table = tgt_temp
        except Exception:
            src_table = src_view
            tgt_table = tgt_view

        src_total = duck.execute(f"SELECT COUNT(*) FROM {src_table}").fetchone()[0]
        tgt_total = duck.execute(f"SELECT COUNT(*) FROM {tgt_table}").fetchone()[0]
        if src_total == 0 or tgt_total == 0:
            return {
                "pair_id": pair_id,
                "source": src["id"],
                "target": tgt["id"],
                "candidates": [],
            }

        src_exprs: List[str] = []
        tgt_exprs: List[str] = []
        for col in common:
            qc = quote(col)
            src_exprs.append(
                f"SUM(CASE WHEN TRIM(CAST({qc} AS VARCHAR)) <> '' THEN 1 ELSE 0 END) "
                f"AS {quote(f'{col}__non_blank')}"
            )
            src_exprs.append(f"COUNT(DISTINCT {qc}) AS {quote(f'{col}__distinct')}")
            tgt_exprs.append(
                f"SUM(CASE WHEN TRIM(CAST({qc} AS VARCHAR)) <> '' THEN 1 ELSE 0 END) "
                f"AS {quote(f'{col}__non_blank')}"
            )
            tgt_exprs.append(f"COUNT(DISTINCT {qc}) AS {quote(f'{col}__distinct')}")

        src_stats = duck.execute(f"SELECT {', '.join(src_exprs)} FROM {src_table}").fetchone()
        tgt_stats = duck.execute(f"SELECT {', '.join(tgt_exprs)} FROM {tgt_table}").fetchone()

        for i, col in enumerate(common):
            qc = quote(col)
            try:
                src_non_blank = src_stats[i * 2]
                src_dist = src_stats[i * 2 + 1]
                tgt_non_blank = tgt_stats[i * 2]
                tgt_dist = tgt_stats[i * 2 + 1]

                # Uniqueness score (0-1)
                src_uniq = src_dist / src_total if src_total else 0
                tgt_uniq = tgt_dist / tgt_total if tgt_total else 0
                uniq_score = (src_uniq + tgt_uniq) / 2

                # Completeness score (0-1)
                src_comp = src_non_blank / src_total if src_total else 0
                tgt_comp = tgt_non_blank / tgt_total if tgt_total else 0
                comp_score = (src_comp + tgt_comp) / 2

                # Overlap score: intersection of distinct values / union
                overlap_row = duck.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT CAST({qc} AS VARCHAR) FROM {src_table}
                        WHERE {qc} IS NOT NULL
                        INTERSECT
                        SELECT DISTINCT CAST({qc} AS VARCHAR) FROM {tgt_table}
                        WHERE {qc} IS NOT NULL
                    )
                    """
                ).fetchone()
                union_count = (src_dist + tgt_dist - overlap_row[0]) if overlap_row else 0
                overlap_score = (
                    overlap_row[0] / union_count if union_count else 0
                )

                # Combined score
                score = (uniq_score * 0.4) + (comp_score * 0.3) + (overlap_score * 0.3)

                candidates.append(
                    {
                        "column": col,
                        "score": round(score, 3),
                        "uniqueness": round(uniq_score, 3),
                        "completeness": round(comp_score, 3),
                        "overlap": round(overlap_score, 3),
                        "source_distinct": src_dist,
                        "target_distinct": tgt_dist,
                    }
                )
            except Exception:
                continue

    candidates.sort(key=lambda c: c["score"], reverse=True)
    return {
        "pair_id": pair_id,
        "source": src["id"],
        "target": tgt["id"],
        "candidates": candidates[:10],
    }
