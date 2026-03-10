"""
Relationship discovery and auto-linking for datasets on the same side.
"""

from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any, Dict, List, Tuple
import re

from server import db
from server.query_engine import connect, quote


AUTO_METHOD = "auto_link_v1"
MIN_KEYLIKE_UNIQUENESS = 0.8
MIN_INTERSECTION_VALUES = 2
MIN_DISTINCT_NON_BLANK = 3
LOW_CARDINALITY_CUTOFF = 2
FLAG_LIKE_MAX_DISTINCT = 5

_ALIAS_GROUPS: Dict[str, set[str]] = {
    "party_number": {
        "partynumber",
        "party_number",
        "partyid",
        "party_id",
        "partynum",
        "partyno",
    },
    "customer_account": {
        "customeraccount",
        "customer_account",
        "custaccount",
        "cust_account",
        "customeraccountnum",
        "customeraccountnumber",
        "accountnum",
        "accountnumber",
        "custaccountnum",
        "custaccountnumber",
        "customer_account_num",
        "customer_account_number",
    },
    "electronic_address_id": {
        "electronicaddressid",
        "electronic_address_id",
        "electronicaddress",
        "electronic_address",
        "emailid",
        "email_id",
    },
    "locator": {
        "locator",
        "locatorextension",
        "locator_extension",
    },
}
_ALIAS_LOOKUP: Dict[str, str] = {
    re.sub(r"[^a-z0-9]", "", alias.lower()): group
    for group, aliases in _ALIAS_GROUPS.items()
    for alias in aliases
}
_FLAG_PREFIXES: Tuple[str, ...] = (
    "is",
    "has",
    "can",
    "allow",
    "enabled",
    "active",
    "flag",
    "use",
)


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def _name_score(left_field: str, right_field: str) -> float:
    left = _norm(left_field)
    right = _norm(right_field)
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0

    left_group = _ALIAS_LOOKUP.get(left)
    right_group = _ALIAS_LOOKUP.get(right)
    if left_group and right_group and left_group == right_group:
        return 0.97

    if len(left) >= 6 and (left in right or right in left):
        return 0.9

    return SequenceMatcher(None, left, right).ratio()


def _looks_flag_field(field: str) -> bool:
    norm = _norm(field)
    if len(norm) < 3:
        return False
    return any(norm.startswith(prefix) for prefix in _FLAG_PREFIXES)


def _best_candidates(
    left_columns: List[str],
    right_columns: List[str],
    min_name_score: float,
    max_candidates: int,
) -> List[Tuple[str, str, float]]:
    candidates: List[Tuple[str, str, float]] = []
    for left_col in left_columns:
        best_score = 0.0
        best_right = ""
        for right_col in right_columns:
            score = _name_score(left_col, right_col)
            if score > best_score:
                best_score = score
                best_right = right_col
        if best_score >= min_name_score and best_right:
            candidates.append((left_col, best_right, best_score))

    # Keep strongest unique right-field matches first.
    candidates.sort(key=lambda x: x[2], reverse=True)
    seen_right: set[str] = set()
    unique: List[Tuple[str, str, float]] = []
    for left_col, right_col, score in candidates:
        if right_col in seen_right:
            continue
        seen_right.add(right_col)
        unique.append((left_col, right_col, score))
        if len(unique) >= max_candidates:
            break
    return unique


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
            SUM(CASE WHEN TRIM(CAST({qf} AS VARCHAR)) <> '' THEN 1 ELSE 0 END) AS non_blank,
            COUNT(DISTINCT CASE WHEN TRIM(CAST({qf} AS VARCHAR)) <> '' THEN CAST({qf} AS VARCHAR) END) AS distinct_non_blank
        FROM {table}
        """
    ).fetchone()

    non_blank = int(row[0] or 0)
    distinct_non_blank = int(row[1] or 0)
    uniqueness = (distinct_non_blank / non_blank) if non_blank else 0.0
    result = {
        "non_blank": non_blank,
        "distinct_non_blank": distinct_non_blank,
        "uniqueness": round(uniqueness, 6),
    }
    cache[key] = result
    return result


def _overlap_metrics(
    duck,
    left_table: str,
    right_table: str,
    left_field: str,
    right_field: str,
) -> Dict[str, float]:
    ql = quote(left_field)
    qr = quote(right_field)

    left_distinct = duck.execute(
        f"SELECT COUNT(DISTINCT CASE WHEN TRIM(CAST({ql} AS VARCHAR)) <> '' THEN CAST({ql} AS VARCHAR) END) FROM {left_table}"
    ).fetchone()[0]
    right_distinct = duck.execute(
        f"SELECT COUNT(DISTINCT CASE WHEN TRIM(CAST({qr} AS VARCHAR)) <> '' THEN CAST({qr} AS VARCHAR) END) FROM {right_table}"
    ).fetchone()[0]
    left_distinct = int(left_distinct or 0)
    right_distinct = int(right_distinct or 0)
    if not left_distinct and not right_distinct:
        return {"jaccard": 0.0, "containment": 0.0, "intersection": 0.0}

    intersection = duck.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT CAST({ql} AS VARCHAR) AS v
            FROM {left_table}
            WHERE TRIM(CAST({ql} AS VARCHAR)) <> ''
            INTERSECT
            SELECT DISTINCT CAST({qr} AS VARCHAR) AS v
            FROM {right_table}
            WHERE TRIM(CAST({qr} AS VARCHAR)) <> ''
        )
        """
    ).fetchone()[0]
    intersection = int(intersection or 0)
    union_count = left_distinct + right_distinct - intersection
    jaccard = (intersection / union_count) if union_count > 0 else 0.0
    smaller_distinct = min(left_distinct, right_distinct)
    containment = (intersection / smaller_distinct) if smaller_distinct > 0 else 0.0
    return {
        "jaccard": jaccard,
        "containment": containment,
        "intersection": float(intersection),
    }


def _is_low_information_candidate(
    left_field: str,
    right_field: str,
    left_stats: Dict[str, float],
    right_stats: Dict[str, float],
) -> bool:
    left_distinct = int(left_stats["distinct_non_blank"])
    right_distinct = int(right_stats["distinct_non_blank"])

    if left_distinct <= LOW_CARDINALITY_CUTOFF or right_distinct <= LOW_CARDINALITY_CUTOFF:
        return True
    if min(left_distinct, right_distinct) < MIN_DISTINCT_NON_BLANK:
        return True

    if (_looks_flag_field(left_field) or _looks_flag_field(right_field)) and (
        max(left_distinct, right_distinct) <= FLAG_LIKE_MAX_DISTINCT
    ):
        return True

    return False


def _sig(side: str, left_ds: str, left_field: str, right_ds: str, right_field: str) -> Tuple[str, str, str, str, str]:
    return (side, left_ds, left_field, right_ds, right_field)


def _pair_sig(left_ds: str, right_ds: str) -> Tuple[str, str]:
    return tuple(sorted((left_ds, right_ds)))


def link_related_tables(
    side: str = "target",
    min_confidence: float = 0.9,
    suggest_only: bool = False,
    min_name_score: float = 0.84,
    max_candidates_per_pair: int = 8,
    max_links: int = 200,
    conn=None,
) -> Dict[str, Any]:
    """Discover high-confidence same-side dataset field relationships and optionally persist them."""
    side = (side or "").strip().lower()
    if side not in ("source", "target"):
        return {"error": "side must be 'source' or 'target'."}

    min_confidence = max(0.0, min(float(min_confidence), 1.0))
    min_name_score = max(0.0, min(float(min_name_score), 1.0))
    max_links = max(1, min(int(max_links), 2000))

    own = conn is None
    if own:
        conn = db.get_connection()
    datasets = db.list_datasets(conn, side=side)
    if len(datasets) < 2:
        if own:
            conn.close()
        return {
            "side": side,
            "suggest_only": suggest_only,
            "min_confidence": min_confidence,
            "datasets_considered": len(datasets),
            "suggested_count": 0,
            "applied_count": 0,
            "relationships": [],
        }

    existing = db.list_relationships(conn, side=side, active_only=True, limit=5000)
    existing_sigs = {
        _sig(r["side"], r["left_dataset"], r["left_field"], r["right_dataset"], r["right_field"])
        for r in existing
    }
    existing_pair_sigs = {_pair_sig(r["left_dataset"], r["right_dataset"]) for r in existing}

    suggestions: List[Dict[str, Any]] = []
    applied: List[Dict[str, Any]] = []

    for i in range(len(datasets)):
        for j in range(i + 1, len(datasets)):
            left_ds = datasets[i]
            right_ds = datasets[j]
            if _pair_sig(left_ds["id"], right_ds["id"]) in existing_pair_sigs:
                continue
            left_cols = left_ds.get("columns", []) or []
            right_cols = right_ds.get("columns", []) or []
            if not left_cols or not right_cols:
                continue

            candidates = _best_candidates(
                left_columns=left_cols,
                right_columns=right_cols,
                min_name_score=min_name_score,
                max_candidates=max_candidates_per_pair,
            )
            if not candidates:
                continue

            with connect([left_ds, right_ds]) as duck:
                left_view = quote(left_ds["id"])
                right_view = quote(right_ds["id"])
                left_table = left_view
                right_table = right_view
                stats_cache: Dict[Tuple[str, str], Dict[str, float]] = {}
                try:
                    left_tmp = quote("__tmp_rel_left")
                    right_tmp = quote("__tmp_rel_right")
                    duck.execute(f"CREATE TEMP TABLE {left_tmp} AS SELECT * FROM {left_view}")
                    duck.execute(f"CREATE TEMP TABLE {right_tmp} AS SELECT * FROM {right_view}")
                    left_table = left_tmp
                    right_table = right_tmp
                except Exception:
                    left_table = left_view
                    right_table = right_view

                for left_field, right_field, name_score in candidates:
                    try:
                        left_stats = _column_stats(
                            duck=duck,
                            table=left_table,
                            field=left_field,
                            cache=stats_cache,
                        )
                        right_stats = _column_stats(
                            duck=duck,
                            table=right_table,
                            field=right_field,
                            cache=stats_cache,
                        )
                        if _is_low_information_candidate(left_field, right_field, left_stats, right_stats):
                            continue

                        key_like_score = max(
                            float(left_stats["uniqueness"]),
                            float(right_stats["uniqueness"]),
                        )
                        if key_like_score < MIN_KEYLIKE_UNIQUENESS:
                            continue

                        overlap_metrics = _overlap_metrics(
                            duck=duck,
                            left_table=left_table,
                            right_table=right_table,
                            left_field=left_field,
                            right_field=right_field,
                        )
                    except Exception:
                        continue

                    if overlap_metrics["intersection"] < MIN_INTERSECTION_VALUES:
                        continue

                    overlap = max(
                        float(overlap_metrics["jaccard"]),
                        float(overlap_metrics["containment"]),
                    )
                    confidence = round(
                        (name_score * 0.55) + (overlap * 0.30) + (key_like_score * 0.15),
                        3,
                    )
                    if confidence < min_confidence:
                        continue

                    sig = _sig(side, left_ds["id"], left_field, right_ds["id"], right_field)
                    reverse_sig = _sig(side, right_ds["id"], right_field, left_ds["id"], left_field)
                    suggestion = {
                        "side": side,
                        "left_dataset": left_ds["id"],
                        "left_field": left_field,
                        "right_dataset": right_ds["id"],
                        "right_field": right_field,
                        "confidence": confidence,
                        "name_score": round(name_score, 3),
                        "overlap_score": round(overlap, 3),
                        "containment_score": round(float(overlap_metrics["containment"]), 3),
                        "jaccard_score": round(float(overlap_metrics["jaccard"]), 3),
                        "left_uniqueness": round(float(left_stats["uniqueness"]), 3),
                        "right_uniqueness": round(float(right_stats["uniqueness"]), 3),
                        "method": AUTO_METHOD,
                        "already_exists": sig in existing_sigs or reverse_sig in existing_sigs,
                    }
                    suggestions.append(suggestion)

                    if suggest_only:
                        continue
                    if len(applied) >= max_links:
                        continue
                    rel = db.upsert_relationship(
                        conn=conn,
                        side=side,
                        left_dataset=left_ds["id"],
                        left_field=left_field,
                        right_dataset=right_ds["id"],
                        right_field=right_field,
                        confidence=confidence,
                        method=AUTO_METHOD,
                        active=True,
                    )
                    applied.append(rel)
                    existing_sigs.add(sig)

    if own:
        conn.close()

    return {
        "side": side,
        "suggest_only": suggest_only,
        "min_confidence": min_confidence,
        "datasets_considered": len(datasets),
        "suggested_count": len(suggestions),
        "applied_count": len(applied),
        "relationships": applied if not suggest_only else suggestions,
    }
