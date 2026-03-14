"""
CatalogService.

Scans source and target directories, discovers CSV/Excel files,
reads headers, registers datasets in SQLite, and auto-pairs
source ↔ target datasets by normalised name.
"""

from __future__ import annotations

from difflib import SequenceMatcher
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from server import db
from server.query_engine import (
    connect,
    count_csv_rows,
    count_excel_sheet_rows,
    detect_text_encoding,
    quote,
    read_csv_headers,
    read_excel_sheets,
    sanitize_column_names,
    sanitize_name,
)


# Recognised extensions
CSV_EXTS = {".csv", ".tsv", ".txt"}
EXCEL_EXTS = {".xlsx", ".xlsm", ".xltx", ".xltm", ".xls"}
ALL_EXTS = CSV_EXTS | EXCEL_EXTS
EXTRA_FOLDER_META_BY_SIDE = {
    "configurations": "configurations_folder",
    "translations": "translations_folder",
    "rules": "rules_folder",
}
MANAGED_DATASET_SIDES = ("source", "target", "configurations", "translations", "rules")

# Auto-pair fallback thresholds (for schema/field-based matching)
AUTO_PAIR_MIN_SHARED_FIELDS = 3
AUTO_PAIR_MIN_OVERLAP_RATIO = 0.6

# Mapping suggestion thresholds
CONTENT_MIN_INTERSECTION = 2
LOW_CARDINALITY_DISTINCT = 3
MIN_KEYLIKE_DISTINCT = 3
MIN_KEYLIKE_UNIQUENESS = 0.96
MIN_KEYLIKE_COMPLETENESS = 0.9
MIN_KEYLIKE_CONTAINMENT = 0.8


# ═══════════════════════════════════════════════════════════════
#  Name normalisation for auto-pairing
# ═══════════════════════════════════════════════════════════════

# Prefixes/suffixes commonly used in migration extracts
_STRIP_RE = re.compile(
    r"^(src_?|source_?|tgt_?|target_?|trg_?|exp_?|imp_?)|(\.csv|\.xlsx?|\.xlsm|\.tsv)$",
    re.IGNORECASE,
)


def _normalise_for_matching(name: str) -> str:
    """Normalise a file/sheet name for auto-pair matching.

    Strips common prefixes/suffixes, folds case, and removes underscores.
    """
    name = _STRIP_RE.sub("", name)
    name = _STRIP_RE.sub("", name)  # second pass for suffix after prefix strip
    return re.sub(r"[\s_\-]+", "", name).lower()


def _normalise_col_for_matching(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", str(name or "").strip()).lower()


def _name_similarity(left: str, right: str) -> float:
    ln = _normalise_col_for_matching(left)
    rn = _normalise_col_for_matching(right)
    if not ln or not rn:
        return 0.0
    if ln == rn:
        return 1.0
    if len(ln) >= 4 and (ln in rn or rn in ln):
        return 0.9
    return SequenceMatcher(None, ln, rn).ratio()


def _column_stats(
    duck,
    table: str,
    field: str,
    cache: Dict[Tuple[str, str], Dict[str, float]],
) -> Dict[str, float]:
    key = (table, field)
    cached = cache.get(key)
    if cached is not None:
        return cached

    qf = quote(field)
    row = duck.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN TRIM(CAST({qf} AS VARCHAR)) <> '' THEN 1 ELSE 0 END) AS non_blank,
            COUNT(DISTINCT CASE WHEN TRIM(CAST({qf} AS VARCHAR)) <> '' THEN TRIM(CAST({qf} AS VARCHAR)) END) AS distinct_non_blank
        FROM {table}
        """
    ).fetchone()
    total_rows = int(row[0] or 0)
    non_blank = int(row[1] or 0)
    distinct_non_blank = int(row[2] or 0)
    completeness = (non_blank / total_rows) if total_rows else 0.0
    uniqueness = (distinct_non_blank / non_blank) if non_blank else 0.0
    result = {
        "total_rows": total_rows,
        "non_blank": non_blank,
        "distinct_non_blank": distinct_non_blank,
        "completeness": completeness,
        "uniqueness": uniqueness,
    }
    cache[key] = result
    return result


def _pair_metrics(
    duck,
    source_table: str,
    target_table: str,
    source_field: str,
    target_field: str,
    stats_cache: Dict[Tuple[str, str], Dict[str, float]],
) -> Dict[str, float]:
    src_stats = _column_stats(duck, source_table, source_field, stats_cache)
    tgt_stats = _column_stats(duck, target_table, target_field, stats_cache)
    qs = quote(source_field)
    qt = quote(target_field)
    inter_row = duck.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT TRIM(CAST({qs} AS VARCHAR)) AS v
            FROM {source_table}
            WHERE TRIM(CAST({qs} AS VARCHAR)) <> ''
            INTERSECT
            SELECT DISTINCT TRIM(CAST({qt} AS VARCHAR)) AS v
            FROM {target_table}
            WHERE TRIM(CAST({qt} AS VARCHAR)) <> ''
        )
        """
    ).fetchone()
    intersection = int((inter_row[0] if inter_row else 0) or 0)

    src_distinct = int(src_stats["distinct_non_blank"])
    tgt_distinct = int(tgt_stats["distinct_non_blank"])
    union_count = src_distinct + tgt_distinct - intersection
    jaccard = (intersection / union_count) if union_count > 0 else 0.0
    smaller = min(src_distinct, tgt_distinct)
    containment = (intersection / smaller) if smaller > 0 else 0.0
    overlap = max(jaccard, containment)
    avg_completeness = (float(src_stats["completeness"]) + float(tgt_stats["completeness"])) / 2
    avg_uniqueness = (float(src_stats["uniqueness"]) + float(tgt_stats["uniqueness"])) / 2
    low_cardinality = min(src_distinct, tgt_distinct) <= LOW_CARDINALITY_DISTINCT

    return {
        "intersection": float(intersection),
        "jaccard": jaccard,
        "containment": containment,
        "overlap": overlap,
        "avg_completeness": avg_completeness,
        "avg_uniqueness": avg_uniqueness,
        "src_distinct": float(src_distinct),
        "tgt_distinct": float(tgt_distinct),
        "src_non_blank": float(src_stats["non_blank"]),
        "tgt_non_blank": float(tgt_stats["non_blank"]),
        "src_uniqueness": float(src_stats["uniqueness"]),
        "tgt_uniqueness": float(tgt_stats["uniqueness"]),
        "src_completeness": float(src_stats["completeness"]),
        "tgt_completeness": float(tgt_stats["completeness"]),
        "low_cardinality": 1.0 if low_cardinality else 0.0,
    }


def _is_key_pair_candidate(metrics: Dict[str, float]) -> bool:
    max_uniqueness = max(metrics["src_uniqueness"], metrics["tgt_uniqueness"])
    return bool(
        min(metrics["src_distinct"], metrics["tgt_distinct"]) >= MIN_KEYLIKE_DISTINCT
        and max_uniqueness >= MIN_KEYLIKE_UNIQUENESS
        and min(metrics["src_completeness"], metrics["tgt_completeness"]) >= MIN_KEYLIKE_COMPLETENESS
        and metrics["containment"] >= MIN_KEYLIKE_CONTAINMENT
    )


def _is_relationship_pair_candidate(metrics: Dict[str, float]) -> bool:
    """Relationship candidate heuristic (PK->FK friendly, completeness-agnostic)."""
    max_uniqueness = max(metrics["src_uniqueness"], metrics["tgt_uniqueness"])
    return bool(
        min(metrics["src_distinct"], metrics["tgt_distinct"]) >= MIN_KEYLIKE_DISTINCT
        and max_uniqueness >= MIN_KEYLIKE_UNIQUENESS
        and metrics["containment"] >= MIN_KEYLIKE_CONTAINMENT
    )


def _content_confidence(name_score: float, metrics: Dict[str, float]) -> float:
    is_low_cardinality = metrics["low_cardinality"] >= 0.5
    penalty = 0.25 if is_low_cardinality else 0.0
    raw = (
        (metrics["overlap"] * 0.55)
        + (metrics["jaccard"] * 0.15)
        + (name_score * 0.15)
        + (metrics["avg_uniqueness"] * 0.10)
        + (metrics["avg_completeness"] * 0.05)
        - penalty
    )
    if is_low_cardinality:
        raw = min(raw, 0.58)
    return max(0.0, min(1.0, raw))


def _sheet_match_score(src_ds: Dict[str, Any], tgt_ds: Dict[str, Any]) -> int:
    """Return 1 when source/target sheet names closely match, else 0."""
    src_sheet = _normalise_for_matching(src_ds.get("sheet_name", ""))
    tgt_sheet = _normalise_for_matching(tgt_ds.get("sheet_name", ""))
    return 1 if src_sheet and tgt_sheet and src_sheet == tgt_sheet else 0


# ═══════════════════════════════════════════════════════════════
#  Scanning
# ═══════════════════════════════════════════════════════════════

def _scan_folder(
    folder: str,
    side: str,
    existing_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
    include_row_counts: bool = False,
) -> List[Dict[str, Any]]:
    """Scan a folder and return dataset dicts (not yet persisted)."""
    if not folder or not os.path.isdir(folder):
        return []

    existing_by_id = existing_by_id or {}
    existing_by_file: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for ds in existing_by_id.values():
        if ds.get("side") != side:
            continue
        key = (side, ds.get("file_path", ""))
        existing_by_file.setdefault(key, []).append(ds)

    datasets: List[Dict[str, Any]] = []

    for entry in sorted(os.listdir(folder)):
        full_path = os.path.join(folder, entry)
        if not os.path.isfile(full_path):
            continue
        _, ext = os.path.splitext(entry)
        ext = ext.lower()
        if ext not in ALL_EXTS:
            continue

        base_name = os.path.splitext(entry)[0]
        try:
            st = os.stat(full_path)
            file_size = int(st.st_size)
            file_mtime_ns = int(st.st_mtime_ns)
        except Exception:
            file_size = None
            file_mtime_ns = None

        if ext in CSV_EXTS:
            ds_id = f"{side}_{sanitize_name(base_name)}"
            existing = existing_by_id.get(ds_id)
            unchanged = bool(
                existing
                and existing.get("file_path") == full_path
                and existing.get("file_size") == file_size
                and existing.get("file_mtime_ns") == file_mtime_ns
            )
            if unchanged:
                raw_headers = existing.get("raw_columns", []) or []
                safe_cols = existing.get("columns", []) or []
                col_map = existing.get("column_map", {}) or {}
                row_count = existing.get("row_count")
                csv_encoding = str(existing.get("csv_encoding") or "")
                csv_py_encoding = ""
                if not csv_encoding:
                    csv_py_encoding, csv_encoding = detect_text_encoding(full_path)
                if include_row_counts and row_count is None:
                    if not csv_py_encoding:
                        csv_py_encoding, _ = detect_text_encoding(full_path)
                    row_count = count_csv_rows(full_path, encoding=csv_py_encoding)
            else:
                csv_py_encoding, csv_encoding = detect_text_encoding(full_path)
                raw_headers = read_csv_headers(full_path, encoding=csv_py_encoding)
                safe_cols, col_map = sanitize_column_names(raw_headers)
                row_count = (
                    count_csv_rows(full_path, encoding=csv_py_encoding)
                    if include_row_counts
                    else None
                )
            datasets.append(
                {
                    "id": ds_id,
                    "side": side,
                    "file_name": entry,
                    "file_path": full_path,
                    "file_size": file_size,
                    "file_mtime_ns": file_mtime_ns,
                    "sheet_name": "",
                    "ext": ext,
                    "columns": safe_cols,
                    "raw_columns": raw_headers,
                    "column_map": col_map,
                    "csv_encoding": csv_encoding,
                    "row_count": row_count,
                }
            )
        elif ext in EXCEL_EXTS:
            existing_sheets = [
                d
                for d in existing_by_file.get((side, full_path), [])
                if d.get("sheet_name")
            ]
            unchanged_file = bool(
                existing_sheets
                and all(d.get("file_size") == file_size and d.get("file_mtime_ns") == file_mtime_ns for d in existing_sheets)
            )
            if unchanged_file and len(existing_sheets) > 1:
                # Guard against stale multi-sheet metadata if workbook tabs changed.
                actual_sheet_names = {sheet_name for sheet_name, _headers in read_excel_sheets(full_path)}
                cached_sheet_names = {str(d.get("sheet_name", "")) for d in existing_sheets}
                if actual_sheet_names and actual_sheet_names != cached_sheet_names:
                    unchanged_file = False
            if unchanged_file:
                for old in sorted(existing_sheets, key=lambda d: d["id"]):
                    row_count = old.get("row_count")
                    if include_row_counts and row_count is None:
                        row_count = count_excel_sheet_rows(full_path, old.get("sheet_name", ""))
                    datasets.append(
                        {
                            "id": old["id"],
                            "side": side,
                            "file_name": entry,
                            "file_path": full_path,
                            "file_size": file_size,
                            "file_mtime_ns": file_mtime_ns,
                            "sheet_name": old.get("sheet_name", ""),
                            "ext": ext,
                            "columns": old.get("columns", []) or [],
                            "raw_columns": old.get("raw_columns", []) or [],
                            "column_map": old.get("column_map", {}) or {},
                            "csv_encoding": "",
                            "row_count": row_count,
                        }
                    )
                continue

            sheets = read_excel_sheets(full_path)
            for sheet_name, raw_headers in sheets:
                safe_cols, col_map = sanitize_column_names(raw_headers)
                safe_sheet = sanitize_name(sheet_name)
                ds_id = f"{side}_{sanitize_name(base_name)}_{safe_sheet}"
                row_count = count_excel_sheet_rows(full_path, sheet_name) if include_row_counts else None
                datasets.append(
                    {
                        "id": ds_id,
                        "side": side,
                        "file_name": entry,
                        "file_path": full_path,
                        "file_size": file_size,
                        "file_mtime_ns": file_mtime_ns,
                        "sheet_name": sheet_name,
                        "ext": ext,
                        "columns": safe_cols,
                        "raw_columns": raw_headers,
                        "column_map": col_map,
                        "csv_encoding": "",
                        "row_count": row_count,
                    }
                )
    return datasets


def refresh_catalog(
    source_folder: Optional[str] = None,
    target_folder: Optional[str] = None,
    configurations_folder: Optional[str] = None,
    translations_folder: Optional[str] = None,
    rules_folder: Optional[str] = None,
    include_row_counts: bool = False,
    conn=None,
) -> Dict[str, Any]:
    """Rescan source and target folders, (re)register all datasets.

    Returns a summary dict with counts and lists of new/removed IDs.
    """
    own_conn = conn is None
    if own_conn:
        conn = db.get_connection()

    # Persist folder paths in meta
    if source_folder:
        db.set_meta(conn, "source_folder", source_folder, commit=False)
    else:
        source_folder = db.get_meta(conn, "source_folder")

    if target_folder:
        db.set_meta(conn, "target_folder", target_folder, commit=False)
    else:
        target_folder = db.get_meta(conn, "target_folder")

    extra_folder_values = {
        "configurations": configurations_folder,
        "translations": translations_folder,
        "rules": rules_folder,
    }
    for side, value in extra_folder_values.items():
        meta_key = EXTRA_FOLDER_META_BY_SIDE[side]
        if value is not None:
            db.set_meta(conn, meta_key, value, commit=False)
            extra_folder_values[side] = value
        else:
            extra_folder_values[side] = db.get_meta(conn, meta_key)

    # Existing datasets
    old_datasets = {d["id"]: d for d in db.list_datasets(conn)}
    old_ids = {d["id"] for d in old_datasets.values() if d.get("side") in MANAGED_DATASET_SIDES}

    # Scan
    src_list = (
        _scan_folder(source_folder, "source", existing_by_id=old_datasets, include_row_counts=include_row_counts)
        if source_folder
        else []
    )
    tgt_list = (
        _scan_folder(target_folder, "target", existing_by_id=old_datasets, include_row_counts=include_row_counts)
        if target_folder
        else []
    )
    cfg_list = (
        _scan_folder(
            extra_folder_values["configurations"],
            "configurations",
            existing_by_id=old_datasets,
            include_row_counts=include_row_counts,
        )
        if extra_folder_values["configurations"]
        else []
    )
    trn_list = (
        _scan_folder(
            extra_folder_values["translations"],
            "translations",
            existing_by_id=old_datasets,
            include_row_counts=include_row_counts,
        )
        if extra_folder_values["translations"]
        else []
    )
    rules_list = (
        _scan_folder(
            extra_folder_values["rules"],
            "rules",
            existing_by_id=old_datasets,
            include_row_counts=include_row_counts,
        )
        if extra_folder_values["rules"]
        else []
    )
    new_list = src_list + tgt_list + cfg_list + trn_list + rules_list
    new_ids = {d["id"] for d in new_list}

    # Upsert discovered datasets
    for ds in new_list:
        db.upsert_dataset(conn, ds, commit=False)
    if new_list:
        conn.commit()

    # Remove stale datasets
    removed_ids = old_ids - new_ids
    for rid in removed_ids:
        conn.execute("DELETE FROM datasets WHERE id = ?", (rid,))
    if removed_ids:
        conn.commit()

    # Auto-pair
    pair_summary = _auto_pair(conn, src_list, tgt_list)

    summary = {
        "source_folder": source_folder or "",
        "target_folder": target_folder or "",
        "configurations_folder": extra_folder_values["configurations"] or "",
        "translations_folder": extra_folder_values["translations"] or "",
        "rules_folder": extra_folder_values["rules"] or "",
        "source_datasets": len(src_list),
        "target_datasets": len(tgt_list),
        "configurations_datasets": len(cfg_list),
        "translations_datasets": len(trn_list),
        "rules_datasets": len(rules_list),
        "total_datasets": len(new_list),
        "new_datasets": sorted(new_ids - old_ids),
        "removed_datasets": sorted(removed_ids),
        "row_counts_included": bool(include_row_counts),
        "pairs_created": pair_summary["created"],
        "pairs_created_by_name": pair_summary.get("created_by_name", 0),
        "pairs_created_by_fields": pair_summary.get("created_by_fields", 0),
        "total_pairs": pair_summary["total"],
    }
    if own_conn:
        conn.close()
    return summary


def _auto_pair(
    conn,
    src_list: List[Dict[str, Any]],
    tgt_list: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Auto-match source-target datasets by name, then by field overlap."""
    created = 0
    name_pairs_created = 0
    field_pairs_created = 0

    src_by_id = {d["id"]: d for d in src_list}
    tgt_by_id = {d["id"]: d for d in tgt_list}
    unmatched_src = set(src_by_id.keys())
    unmatched_tgt = set(tgt_by_id.keys())

    # 1) Name-based matching.
    src_norm: Dict[str, List[str]] = {}
    for ds in src_list:
        base = os.path.splitext(ds["file_name"])[0]
        sheet = ds.get("sheet_name", "")
        key = _normalise_for_matching(base + sheet)
        src_norm.setdefault(key, []).append(ds["id"])

    tgt_norm: Dict[str, List[str]] = {}
    for ds in tgt_list:
        base = os.path.splitext(ds["file_name"])[0]
        sheet = ds.get("sheet_name", "")
        key = _normalise_for_matching(base + sheet)
        tgt_norm.setdefault(key, []).append(ds["id"])

    for norm_key in sorted(set(src_norm.keys()) & set(tgt_norm.keys())):
        src_ids = sorted(src_norm[norm_key])
        tgt_ids = sorted(tgt_norm[norm_key])
        for src_id, tgt_id in zip(src_ids, tgt_ids):
            pair_id = f"pair_{uuid.uuid4().hex[:8]}"
            try:
                db.upsert_pair(conn, pair_id, src_id, tgt_id, auto_matched=True, enabled=True, commit=False)
                created += 1
                name_pairs_created += 1
            except Exception:
                pass
            unmatched_src.discard(src_id)
            unmatched_tgt.discard(tgt_id)

    # 2) Field-overlap matching for unmatched datasets.
    if unmatched_src and unmatched_tgt:
        score_matrix: Dict[Tuple[str, str], Tuple[int, float, int, List[Dict[str, str]]]] = {}
        for src_id in sorted(unmatched_src):
            src_cols = src_by_id[src_id].get("columns", []) or []
            src_norm_map = {_normalise_col_for_matching(c): c for c in src_cols if c}
            src_keys = set(src_norm_map.keys())
            if not src_keys:
                continue
            for tgt_id in sorted(unmatched_tgt):
                tgt_cols = tgt_by_id[tgt_id].get("columns", []) or []
                tgt_norm_map = {_normalise_col_for_matching(c): c for c in tgt_cols if c}
                tgt_keys = set(tgt_norm_map.keys())
                if not tgt_keys:
                    continue
                shared_keys = sorted(src_keys & tgt_keys)
                shared_count = len(shared_keys)
                if shared_count < AUTO_PAIR_MIN_SHARED_FIELDS:
                    continue
                overlap_ratio = shared_count / max(1, min(len(src_keys), len(tgt_keys)))
                if overlap_ratio < AUTO_PAIR_MIN_OVERLAP_RATIO:
                    continue
                compare_mappings = [
                    {"source_field": src_norm_map[k], "target_field": tgt_norm_map[k]}
                    for k in shared_keys
                ]
                sheet_score = _sheet_match_score(src_by_id[src_id], tgt_by_id[tgt_id])
                score_matrix[(src_id, tgt_id)] = (shared_count, overlap_ratio, sheet_score, compare_mappings)

        # Keep mutual best matches only to avoid ambiguous links.
        best_tgt_for_src: Dict[str, Tuple[str, int, float, int]] = {}
        for (src_id, tgt_id), (shared_count, ratio, sheet_score, _mappings) in score_matrix.items():
            current = best_tgt_for_src.get(src_id)
            candidate = (tgt_id, shared_count, ratio, sheet_score)
            if current is None or (candidate[1], candidate[2], candidate[3], candidate[0]) > (
                current[1],
                current[2],
                current[3],
                current[0],
            ):
                best_tgt_for_src[src_id] = candidate

        best_src_for_tgt: Dict[str, Tuple[str, int, float, int]] = {}
        for (src_id, tgt_id), (shared_count, ratio, sheet_score, _mappings) in score_matrix.items():
            current = best_src_for_tgt.get(tgt_id)
            candidate = (src_id, shared_count, ratio, sheet_score)
            if current is None or (candidate[1], candidate[2], candidate[3], candidate[0]) > (
                current[1],
                current[2],
                current[3],
                current[0],
            ):
                best_src_for_tgt[tgt_id] = candidate

        for src_id, (tgt_id, _shared_count, _ratio, _sheet_score) in sorted(best_tgt_for_src.items()):
            reverse = best_src_for_tgt.get(tgt_id)
            if not reverse or reverse[0] != src_id:
                continue
            if src_id not in unmatched_src or tgt_id not in unmatched_tgt:
                continue

            pair_id = f"pair_{uuid.uuid4().hex[:8]}"
            _sc, _r, _ss, compare_mappings = score_matrix[(src_id, tgt_id)]
            try:
                db.upsert_pair(
                    conn,
                    pair_id,
                    src_id,
                    tgt_id,
                    auto_matched=True,
                    enabled=True,
                    compare_mappings=compare_mappings,
                    commit=False,
                )
                created += 1
                field_pairs_created += 1
            except Exception:
                pass
            unmatched_src.discard(src_id)
            unmatched_tgt.discard(tgt_id)

    conn.commit()
    total = len(db.list_pairs(conn))
    return {
        "created": created,
        "created_by_name": name_pairs_created,
        "created_by_fields": field_pairs_created,
        "total": total,
    }

# ═══════════════════════════════════════════════════════════════
#  Convenience accessors
# ═══════════════════════════════════════════════════════════════

def get_datasets(
    side: Optional[str] = None,
    filter_text: Optional[str] = None,
    conn=None,
) -> List[Dict[str, Any]]:
    own = conn is None
    if own:
        conn = db.get_connection()
    result = db.list_datasets(conn, side=side, filter_text=filter_text)
    if own:
        conn.close()
    return result


def get_dataset(dataset_id: str, conn=None) -> Optional[Dict[str, Any]]:
    own = conn is None
    if own:
        conn = db.get_connection()
    result = db.get_dataset(conn, dataset_id)
    if own:
        conn.close()
    return result


def get_pairs(conn=None) -> List[Dict[str, Any]]:
    own = conn is None
    if own:
        conn = db.get_connection()
    result = db.list_pairs(conn)
    if own:
        conn.close()
    return result


def get_pair(pair_id: str, conn=None) -> Optional[Dict[str, Any]]:
    own = conn is None
    if own:
        conn = db.get_connection()
    result = db.get_pair(conn, pair_id)
    if own:
        conn.close()
    return result


def get_pair_by_datasets(source_id: str, target_id: str, conn=None) -> Optional[Dict[str, Any]]:
    own = conn is None
    if own:
        conn = db.get_connection()
    result = db.get_pair_by_datasets(conn, source_id, target_id)
    if own:
        conn.close()
    return result


def clear_pair_key_mappings(pair_id: str, conn=None) -> Dict[str, Any]:
    """Clear saved key mappings for a pair while preserving compare mappings and pair metadata."""
    own = conn is None
    if own:
        conn = db.get_connection()
    pair = db.get_pair(conn, pair_id)
    if not pair:
        if own:
            conn.close()
        return {"error": f"Pair '{pair_id}' not found."}

    db.upsert_pair(
        conn,
        pair_id=pair["id"],
        source_dataset=pair["source_dataset"],
        target_dataset=pair["target_dataset"],
        auto_matched=bool(pair.get("auto_matched", True)),
        enabled=bool(pair.get("enabled", True)),
        key_mappings=[],
        compare_mappings=None,
        commit=True,
    )
    updated = db.get_pair(conn, pair_id)
    if own:
        conn.close()
    if not updated:
        return {"error": f"Pair '{pair_id}' not found after update."}
    return {
        "pair_id": updated["id"],
        "source_dataset": updated["source_dataset"],
        "target_dataset": updated["target_dataset"],
        "key_mapping_count": len(updated.get("key_mappings", [])),
        "compare_mapping_count": len(updated.get("compare_mappings", [])),
    }


def delete_pair(pair_id: str, conn=None) -> Dict[str, Any]:
    """Delete a saved source-target pair."""
    own = conn is None
    if own:
        conn = db.get_connection()
    pair = db.get_pair(conn, pair_id)
    if not pair:
        if own:
            conn.close()
        return {"error": f"Pair '{pair_id}' not found."}

    deleted = db.delete_pair(conn, pair_id)
    if own:
        conn.close()
    if deleted <= 0:
        return {"error": f"Pair '{pair_id}' could not be deleted."}
    return {
        "pair_id": pair_id,
        "deleted": True,
        "source_dataset": pair["source_dataset"],
        "target_dataset": pair["target_dataset"],
    }


def upsert_pair_override(
    source_id: str,
    target_id: str,
    enabled: bool = True,
    key_mappings: Optional[List[Dict[str, str]]] = None,
    compare_mappings: Optional[List[Dict[str, str]]] = None,
    conn=None,
) -> Dict[str, Any]:
    """Create or update a manual pair override."""
    own = conn is None
    if own:
        conn = db.get_connection()
    pair_id = f"pair_{uuid.uuid4().hex[:8]}"
    db.upsert_pair(
        conn,
        pair_id,
        source_id,
        target_id,
        auto_matched=False,
        enabled=enabled,
        key_mappings=key_mappings,
        compare_mappings=compare_mappings,
    )
    pair = db.get_pair_by_datasets(conn, source_id, target_id)
    if own:
        conn.close()
    return {
        "pair_id": pair["id"] if pair else pair_id,
        "source": source_id,
        "target": target_id,
        "enabled": enabled,
        "key_mappings": pair.get("key_mappings", []) if pair else (key_mappings or []),
        "compare_mappings": pair.get("compare_mappings", []) if pair else (compare_mappings or []),
    }


def suggest_field_mappings(
    source_id: str,
    target_id: str,
    mode: str = "name",
    min_confidence: float = 0.75,
    max_mappings: int = 200,
    conn=None,
) -> Dict[str, Any]:
    """Suggest source-target field mappings by name and/or content overlap."""
    mode = (mode or "name").strip().lower()
    if mode not in {"name", "content", "hybrid"}:
        return {"error": "mode must be one of: name, content, hybrid."}

    min_confidence = max(0.0, min(float(min_confidence), 1.0))
    max_mappings = max(1, min(int(max_mappings), 1000))

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

    source_cols = src.get("columns", []) or []
    target_cols = tgt.get("columns", []) or []
    if not source_cols or not target_cols:
        return {
            "source_dataset_id": source_id,
            "target_dataset_id": target_id,
            "mode": mode,
            "match_count": 0,
            "compare_mappings": [],
        }

    mappings: List[Dict[str, Any]] = []
    used_source: set[str] = set()
    used_target: set[str] = set()

    datasets = [src, tgt]
    with connect(datasets) as duck:
        src_view = quote(src["id"])
        tgt_view = quote(tgt["id"])
        src_table = src_view
        tgt_table = tgt_view
        try:
            src_tmp = quote("__tmp_suggest_map_src")
            tgt_tmp = quote("__tmp_suggest_map_tgt")
            duck.execute(f"CREATE TEMP TABLE {src_tmp} AS SELECT * FROM {src_view}")
            duck.execute(f"CREATE TEMP TABLE {tgt_tmp} AS SELECT * FROM {tgt_view}")
            src_table = src_tmp
            tgt_table = tgt_tmp
        except Exception:
            src_table = src_view
            tgt_table = tgt_view

        stats_cache: Dict[Tuple[str, str], Dict[str, float]] = {}

        if mode in {"name", "hybrid"}:
            tgt_lookup = {_normalise_col_for_matching(c): c for c in target_cols}
            for s_col in source_cols:
                t_col = tgt_lookup.get(_normalise_col_for_matching(s_col))
                if not t_col:
                    continue
                metrics = _pair_metrics(
                    duck=duck,
                    source_table=src_table,
                    target_table=tgt_table,
                    source_field=s_col,
                    target_field=t_col,
                    stats_cache=stats_cache,
                )
                is_key_pair = _is_key_pair_candidate(metrics)
                is_relationship_pair = _is_relationship_pair_candidate(metrics)
                mappings.append(
                    {
                        "source_field": s_col,
                        "target_field": t_col,
                        "origin_mode": "name",
                        "confidence": None,
                        "use_key": bool(is_key_pair),
                        "use_compare": True,
                        "is_key_pair": bool(is_key_pair),
                        "is_relationship_pair": bool(is_relationship_pair),
                        "low_cardinality": bool(metrics["low_cardinality"] >= 0.5),
                    }
                )
                used_source.add(s_col)
                used_target.add(t_col)

        if mode in {"content", "hybrid"}:
            content_candidates: List[Tuple[float, float, float, str, str, Dict[str, float]]] = []
            for s_col in source_cols:
                if mode == "hybrid" and s_col in used_source:
                    continue
                for t_col in target_cols:
                    if mode == "hybrid" and t_col in used_target:
                        continue
                    try:
                        metrics = _pair_metrics(
                            duck=duck,
                            source_table=src_table,
                            target_table=tgt_table,
                            source_field=s_col,
                            target_field=t_col,
                            stats_cache=stats_cache,
                        )
                    except Exception:
                        continue

                    if metrics["intersection"] < CONTENT_MIN_INTERSECTION:
                        continue
                    name_score = _name_similarity(s_col, t_col)
                    confidence = _content_confidence(name_score, metrics)
                    if confidence < min_confidence:
                        continue
                    content_candidates.append(
                        (
                            confidence,
                            metrics["overlap"],
                            name_score,
                            s_col,
                            t_col,
                            metrics,
                        )
                    )

            content_candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
            for confidence, _overlap, _name_score, s_col, t_col, metrics in content_candidates:
                if len(mappings) >= max_mappings:
                    break
                if s_col in used_source or t_col in used_target:
                    continue
                is_key_pair = _is_key_pair_candidate(metrics)
                is_relationship_pair = _is_relationship_pair_candidate(metrics)
                mappings.append(
                    {
                        "source_field": s_col,
                        "target_field": t_col,
                        "origin_mode": "content",
                        "confidence": round(confidence, 3),
                        "use_key": bool(is_key_pair),
                        "use_compare": True,
                        "is_key_pair": bool(is_key_pair),
                        "is_relationship_pair": bool(is_relationship_pair),
                        "low_cardinality": bool(metrics["low_cardinality"] >= 0.5),
                    }
                )
                used_source.add(s_col)
                used_target.add(t_col)

    return {
        "source_dataset_id": source_id,
        "target_dataset_id": target_id,
        "mode": mode,
        "match_count": len(mappings),
        "compare_mappings": mappings[:max_mappings],
    }


def schema_diff(
    source_id: str,
    target_id: str,
    conn=None,
) -> Dict[str, Any]:
    """Compare column schemas between two datasets."""
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

    src_cols = set(src["columns"])
    tgt_cols = set(tgt["columns"])

    return {
        "source": source_id,
        "target": target_id,
        "source_only": sorted(src_cols - tgt_cols),
        "target_only": sorted(tgt_cols - src_cols),
        "common": sorted(src_cols & tgt_cols),
        "source_column_count": len(src["columns"]),
        "target_column_count": len(tgt["columns"]),
    }

