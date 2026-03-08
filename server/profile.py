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

    columns = [column] if column else ds["columns"]
    datasets = [ds]

    result: Dict[str, Any] = {"dataset": dataset_id, "summaries": []}

    with connect(datasets) as duck:
        view = quote(dataset_id)

        for col in columns:
            if col not in ds["columns"]:
                result["summaries"].append({"column": col, "error": "Column not found."})
                continue
            qc = quote(col)
            try:
                rows = duck.execute(
                    f"""
                    SELECT CAST({qc} AS VARCHAR) AS value,
                           COUNT(*) AS cnt
                    FROM {view}
                    GROUP BY {qc}
                    ORDER BY cnt DESC
                    LIMIT {int(top_n)}
                    """
                ).fetchall()
                blank_row = duck.execute(
                    f"""
                    SELECT COUNT(*) FROM {view}
                    WHERE TRIM(CAST({qc} AS VARCHAR)) = '' OR {qc} IS NULL
                    """
                ).fetchone()
                result["summaries"].append(
                    {
                        "column": col,
                        "top_values": [{"value": r[0], "count": r[1]} for r in rows],
                        "blank_or_null_count": blank_row[0] if blank_row else 0,
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

    datasets = [ds]
    with connect(datasets) as duck:
        view = quote(dataset_id)
        sel = ", ".join(f"CAST({quote(c)} AS VARCHAR) AS {quote(c)}" for c in columns)
        grp = ", ".join(quote(c) for c in columns)
        rows = duck.execute(
            f"""
            SELECT {sel}, COUNT(*) AS cnt
            FROM {view}
            GROUP BY {grp}
            ORDER BY cnt DESC
            LIMIT {int(top_n)}
            """
        ).fetchall()

    result_rows = []
    for r in rows:
        combo = {columns[i]: r[i] for i in range(len(columns))}
        combo["count"] = r[len(columns)]
        result_rows.append(combo)

    return {
        "dataset": dataset_id,
        "columns": columns,
        "combos": result_rows,
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
            SELECT {sel}, COUNT(*) AS dup_count
            FROM {view}
            GROUP BY {grp}
            HAVING COUNT(*) > 1
            ORDER BY dup_count DESC
            LIMIT {limit}
            """
        ).fetchall()

        total_dup_groups = duck.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {grp} FROM {view}
                GROUP BY {grp}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

    dup_rows = []
    for r in rows:
        entry = {key_columns[i]: r[i] for i in range(len(key_columns))}
        entry["duplicate_count"] = r[len(key_columns)]
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

        for col in common:
            qc = quote(col)
            try:
                # Source stats
                sr = duck.execute(
                    f"""
                    SELECT COUNT(*) AS total,
                           COUNT({qc}) AS non_null,
                           COUNT(DISTINCT {qc}) AS distinct_count
                    FROM {src_view}
                    """
                ).fetchone()

                # Target stats
                tr = duck.execute(
                    f"""
                    SELECT COUNT(*) AS total,
                           COUNT({qc}) AS non_null,
                           COUNT(DISTINCT {qc}) AS distinct_count
                    FROM {tgt_view}
                    """
                ).fetchone()

                src_total, src_nn, src_dist = sr[0], sr[1], sr[2]
                tgt_total, tgt_nn, tgt_dist = tr[0], tr[1], tr[2]

                if src_total == 0 or tgt_total == 0:
                    continue

                # Uniqueness score (0-1)
                src_uniq = src_dist / src_total if src_total else 0
                tgt_uniq = tgt_dist / tgt_total if tgt_total else 0
                uniq_score = (src_uniq + tgt_uniq) / 2

                # Completeness score (0-1)
                src_comp = src_nn / src_total if src_total else 0
                tgt_comp = tgt_nn / tgt_total if tgt_total else 0
                comp_score = (src_comp + tgt_comp) / 2

                # Overlap score: intersection of distinct values / union
                overlap_row = duck.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT CAST({qc} AS VARCHAR) FROM {src_view}
                        WHERE {qc} IS NOT NULL
                        INTERSECT
                        SELECT DISTINCT CAST({qc} AS VARCHAR) FROM {tgt_view}
                        WHERE {qc} IS NOT NULL
                    )
                    """
                ).fetchone()
                union_row = duck.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT CAST({qc} AS VARCHAR) FROM {src_view}
                        WHERE {qc} IS NOT NULL
                        UNION
                        SELECT DISTINCT CAST({qc} AS VARCHAR) FROM {tgt_view}
                        WHERE {qc} IS NOT NULL
                    )
                    """
                ).fetchone()
                overlap_score = (
                    overlap_row[0] / union_row[0] if union_row[0] else 0
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
