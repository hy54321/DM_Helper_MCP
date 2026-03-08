"""
SQLite metadata database for DM Helper.

Stores catalog snapshots, pair overrides, key presets, job history,
and report manifests.  All operational state lives here – DuckDB is
ephemeral-only.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _app_base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_db_path() -> str:
    return os.path.join(_app_base_dir(), "dm_helper.db")


def get_connection(path: str | None = None) -> sqlite3.Connection:
    db_path = path or _default_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- ── Catalog ────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS datasets (
            id          TEXT PRIMARY KEY,
            side        TEXT NOT NULL CHECK(side IN ('source', 'target')),
            file_name   TEXT NOT NULL,
            file_path   TEXT NOT NULL,
            file_size   INTEGER,
            file_mtime_ns INTEGER,
            sheet_name  TEXT NOT NULL DEFAULT '',
            ext         TEXT NOT NULL DEFAULT '',
            columns_json TEXT NOT NULL DEFAULT '[]',
            raw_columns_json TEXT NOT NULL DEFAULT '[]',
            column_map_json TEXT NOT NULL DEFAULT '{}',
            row_count   INTEGER,
            discovered_at TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_datasets_side
            ON datasets(side);

        -- ── Pairs ──────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS pairs (
            id              TEXT PRIMARY KEY,
            source_dataset  TEXT NOT NULL,
            target_dataset  TEXT NOT NULL,
            auto_matched    INTEGER NOT NULL DEFAULT 1,
            enabled         INTEGER NOT NULL DEFAULT 1,
            key_mappings_json TEXT NOT NULL DEFAULT '[]',
            compare_mappings_json TEXT NOT NULL DEFAULT '[]',
            created_at      TEXT NOT NULL,
            FOREIGN KEY (source_dataset) REFERENCES datasets(id) ON DELETE CASCADE,
            FOREIGN KEY (target_dataset) REFERENCES datasets(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_pairs_unique
            ON pairs(source_dataset, target_dataset);

        -- ── Key presets ────────────────────────────────────

        CREATE TABLE IF NOT EXISTS key_presets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_id     TEXT NOT NULL,
            name        TEXT NOT NULL,
            key_fields_json TEXT NOT NULL DEFAULT '[]',
            created_at  TEXT NOT NULL,
            FOREIGN KEY (pair_id) REFERENCES pairs(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_key_presets_pair
            ON key_presets(pair_id);

        -- ── Jobs ───────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS jobs (
            id          TEXT PRIMARY KEY,
            pair_id     TEXT,
            source_dataset TEXT NOT NULL,
            target_dataset TEXT NOT NULL,
            key_fields_json TEXT NOT NULL DEFAULT '[]',
            options_json TEXT NOT NULL DEFAULT '{}',
            state       TEXT NOT NULL DEFAULT 'queued'
                        CHECK(state IN ('queued','running','succeeded','failed','canceled')),
            progress_json TEXT NOT NULL DEFAULT '{}',
            error_message TEXT,
            started_at  TEXT,
            finished_at TEXT,
            created_at  TEXT NOT NULL
        );

        -- ── Reports ────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS reports (
            id          TEXT PRIMARY KEY,
            job_id      TEXT,
            pair_id     TEXT,
            source_dataset TEXT NOT NULL,
            target_dataset TEXT NOT NULL,
            file_path   TEXT NOT NULL,
            file_name   TEXT NOT NULL,
            summary_json TEXT NOT NULL DEFAULT '{}',
            created_at  TEXT NOT NULL,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE SET NULL
        );

        -- ── Meta ───────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    pair_cols = {r["name"] for r in conn.execute("PRAGMA table_info(pairs)").fetchall()}
    if "key_mappings_json" not in pair_cols:
        conn.execute("ALTER TABLE pairs ADD COLUMN key_mappings_json TEXT NOT NULL DEFAULT '[]'")
    if "compare_mappings_json" not in pair_cols:
        conn.execute("ALTER TABLE pairs ADD COLUMN compare_mappings_json TEXT NOT NULL DEFAULT '[]'")
    ds_cols = {r["name"] for r in conn.execute("PRAGMA table_info(datasets)").fetchall()}
    if "file_size" not in ds_cols:
        conn.execute("ALTER TABLE datasets ADD COLUMN file_size INTEGER")
    if "file_mtime_ns" not in ds_cols:
        conn.execute("ALTER TABLE datasets ADD COLUMN file_mtime_ns INTEGER")
    conn.commit()


# ═══════════════════════════════════════════════════════════════
#  Helper functions
# ═══════════════════════════════════════════════════════════════

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Meta ────────────────────────────────────────────────────────

def get_meta(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_meta(conn: sqlite3.Connection, key: str, value: str, commit: bool = True) -> None:
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


# ── Datasets ────────────────────────────────────────────────────

def upsert_dataset(conn: sqlite3.Connection, ds: Dict[str, Any], commit: bool = True) -> None:
    now = utcnow()
    conn.execute(
        """
        INSERT INTO datasets
            (id, side, file_name, file_path, file_size, file_mtime_ns, sheet_name, ext,
             columns_json, raw_columns_json, column_map_json,
             row_count, discovered_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            file_path = excluded.file_path,
            file_size = excluded.file_size,
            file_mtime_ns = excluded.file_mtime_ns,
            columns_json = excluded.columns_json,
            raw_columns_json = excluded.raw_columns_json,
            column_map_json = excluded.column_map_json,
            row_count = excluded.row_count,
            updated_at = excluded.updated_at
        """,
        (
            ds["id"],
            ds["side"],
            ds["file_name"],
            ds["file_path"],
            ds.get("file_size"),
            ds.get("file_mtime_ns"),
            ds.get("sheet_name", ""),
            ds.get("ext", ""),
            json.dumps(ds.get("columns", [])),
            json.dumps(ds.get("raw_columns", [])),
            json.dumps(ds.get("column_map", {})),
            ds.get("row_count"),
            now,
            now,
        ),
    )
    if commit:
        conn.commit()


def get_dataset(conn: sqlite3.Connection, dataset_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
    if not row:
        return None
    return _row_to_dataset(row)


def list_datasets(
    conn: sqlite3.Connection,
    side: str | None = None,
    filter_text: str | None = None,
) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM datasets"
    params: list = []
    clauses: list[str] = []
    if side and side in ("source", "target"):
        clauses.append("side = ?")
        params.append(side)
    if filter_text:
        clauses.append("(id LIKE ? OR file_name LIKE ?)")
        params.extend([f"%{filter_text}%", f"%{filter_text}%"])
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY side, id"
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_dataset(r) for r in rows]


def delete_datasets_by_side(conn: sqlite3.Connection, side: str) -> int:
    cur = conn.execute("DELETE FROM datasets WHERE side = ?", (side,))
    conn.commit()
    return cur.rowcount


def _row_to_dataset(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "side": row["side"],
        "file_name": row["file_name"],
        "file_path": row["file_path"],
        "file_size": row["file_size"],
        "file_mtime_ns": row["file_mtime_ns"],
        "sheet_name": row["sheet_name"],
        "ext": row["ext"],
        "columns": json.loads(row["columns_json"]),
        "raw_columns": json.loads(row["raw_columns_json"]),
        "column_map": json.loads(row["column_map_json"]),
        "row_count": row["row_count"],
        "discovered_at": row["discovered_at"],
        "updated_at": row["updated_at"],
    }


# ── Pairs ───────────────────────────────────────────────────────

def upsert_pair(
    conn: sqlite3.Connection,
    pair_id: str,
    source_dataset: str,
    target_dataset: str,
    auto_matched: bool = True,
    enabled: bool = True,
    key_mappings: Optional[List[Dict[str, str]]] = None,
    compare_mappings: Optional[List[Dict[str, str]]] = None,
    commit: bool = True,
) -> None:
    key_mappings_json = json.dumps(key_mappings) if key_mappings is not None else None
    compare_mappings_json = json.dumps(compare_mappings) if compare_mappings is not None else None
    conn.execute(
        """
        INSERT INTO pairs (
            id, source_dataset, target_dataset, auto_matched, enabled,
            key_mappings_json, compare_mappings_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, COALESCE(?, '[]'), COALESCE(?, '[]'), ?)
        ON CONFLICT(source_dataset, target_dataset) DO UPDATE SET
            enabled = excluded.enabled,
            auto_matched = excluded.auto_matched,
            key_mappings_json = CASE
                WHEN ? IS NULL THEN pairs.key_mappings_json
                ELSE excluded.key_mappings_json
            END,
            compare_mappings_json = CASE
                WHEN ? IS NULL THEN pairs.compare_mappings_json
                ELSE excluded.compare_mappings_json
            END
        """,
        (
            pair_id,
            source_dataset,
            target_dataset,
            int(auto_matched),
            int(enabled),
            key_mappings_json,
            compare_mappings_json,
            utcnow(),
            key_mappings_json,
            compare_mappings_json,
        ),
    )
    if commit:
        conn.commit()


def list_pairs(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT p.*,
               s.file_name AS source_file, s.sheet_name AS source_sheet,
               t.file_name AS target_file, t.sheet_name AS target_sheet
        FROM pairs p
        JOIN datasets s ON p.source_dataset = s.id
        JOIN datasets t ON p.target_dataset = t.id
        ORDER BY p.id
        """
    ).fetchall()
    return [
        {
            "id": r["id"],
            "source_dataset": r["source_dataset"],
            "target_dataset": r["target_dataset"],
            "source_file": r["source_file"],
            "source_sheet": r["source_sheet"],
            "target_file": r["target_file"],
            "target_sheet": r["target_sheet"],
            "auto_matched": bool(r["auto_matched"]),
            "enabled": bool(r["enabled"]),
            "key_mappings": json.loads(r["key_mappings_json"] or "[]"),
            "compare_mappings": json.loads(r["compare_mappings_json"] or "[]"),
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def get_pair(conn: sqlite3.Connection, pair_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT p.*,
               s.file_name AS source_file, s.sheet_name AS source_sheet,
               t.file_name AS target_file, t.sheet_name AS target_sheet
        FROM pairs p
        JOIN datasets s ON p.source_dataset = s.id
        JOIN datasets t ON p.target_dataset = t.id
        WHERE p.id = ?
        """,
        (pair_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "source_dataset": row["source_dataset"],
        "target_dataset": row["target_dataset"],
        "source_file": row["source_file"],
        "source_sheet": row["source_sheet"],
        "target_file": row["target_file"],
        "target_sheet": row["target_sheet"],
        "auto_matched": bool(row["auto_matched"]),
        "enabled": bool(row["enabled"]),
        "key_mappings": json.loads(row["key_mappings_json"] or "[]"),
        "compare_mappings": json.loads(row["compare_mappings_json"] or "[]"),
        "created_at": row["created_at"],
    }


def get_pair_by_datasets(
    conn: sqlite3.Connection,
    source_dataset: str,
    target_dataset: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT p.*,
               s.file_name AS source_file, s.sheet_name AS source_sheet,
               t.file_name AS target_file, t.sheet_name AS target_sheet
        FROM pairs p
        JOIN datasets s ON p.source_dataset = s.id
        JOIN datasets t ON p.target_dataset = t.id
        WHERE p.source_dataset = ? AND p.target_dataset = ?
        """,
        (source_dataset, target_dataset),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "source_dataset": row["source_dataset"],
        "target_dataset": row["target_dataset"],
        "source_file": row["source_file"],
        "source_sheet": row["source_sheet"],
        "target_file": row["target_file"],
        "target_sheet": row["target_sheet"],
        "auto_matched": bool(row["auto_matched"]),
        "enabled": bool(row["enabled"]),
        "key_mappings": json.loads(row["key_mappings_json"] or "[]"),
        "compare_mappings": json.loads(row["compare_mappings_json"] or "[]"),
        "created_at": row["created_at"],
    }


def delete_all_pairs(conn: sqlite3.Connection) -> int:
    cur = conn.execute("DELETE FROM pairs")
    conn.commit()
    return cur.rowcount


# ── Key presets ─────────────────────────────────────────────────

def save_key_preset(
    conn: sqlite3.Connection,
    pair_id: str,
    name: str,
    key_fields: List[str],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO key_presets (pair_id, name, key_fields_json, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (pair_id, name, json.dumps(key_fields), utcnow()),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def list_key_presets(conn: sqlite3.Connection, pair_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM key_presets WHERE pair_id = ? ORDER BY id",
        (pair_id,),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "pair_id": r["pair_id"],
            "name": r["name"],
            "key_fields": json.loads(r["key_fields_json"]),
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def get_key_preset(conn: sqlite3.Connection, preset_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM key_presets WHERE id = ?", (preset_id,)
    ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "pair_id": row["pair_id"],
        "name": row["name"],
        "key_fields": json.loads(row["key_fields_json"]),
        "created_at": row["created_at"],
    }


# ── Jobs ────────────────────────────────────────────────────────

def create_job(
    conn: sqlite3.Connection,
    job_id: str,
    source_dataset: str,
    target_dataset: str,
    key_fields: List[str],
    pair_id: str | None = None,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    now = utcnow()
    conn.execute(
        """
        INSERT INTO jobs
            (id, pair_id, source_dataset, target_dataset,
             key_fields_json, options_json, state, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'queued', ?)
        """,
        (
            job_id,
            pair_id,
            source_dataset,
            target_dataset,
            json.dumps(key_fields),
            json.dumps(options or {}),
            now,
        ),
    )
    conn.commit()
    return {
        "id": job_id,
        "state": "queued",
        "created_at": now,
    }


def update_job_state(
    conn: sqlite3.Connection,
    job_id: str,
    state: str,
    progress: Dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    now = utcnow()
    updates = ["state = ?"]
    params: list = [state]
    if progress is not None:
        updates.append("progress_json = ?")
        params.append(json.dumps(progress))
    if error_message is not None:
        updates.append("error_message = ?")
        params.append(error_message)
    if state == "running":
        updates.append("started_at = ?")
        params.append(now)
    if state in ("succeeded", "failed", "canceled"):
        updates.append("finished_at = ?")
        params.append(now)
    params.append(job_id)
    conn.execute(
        f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    conn.commit()


def get_job(conn: sqlite3.Connection, job_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return None
    return _row_to_job(row)


def list_jobs(conn: sqlite3.Connection, limit: int = 50) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [_row_to_job(r) for r in rows]


def _row_to_job(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "pair_id": row["pair_id"],
        "source_dataset": row["source_dataset"],
        "target_dataset": row["target_dataset"],
        "key_fields": json.loads(row["key_fields_json"]),
        "options": json.loads(row["options_json"]),
        "state": row["state"],
        "progress": json.loads(row["progress_json"]),
        "error_message": row["error_message"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "created_at": row["created_at"],
    }


# ── Reports ─────────────────────────────────────────────────────

def create_report(
    conn: sqlite3.Connection,
    report_id: str,
    job_id: str | None,
    pair_id: str | None,
    source_dataset: str,
    target_dataset: str,
    file_path: str,
    file_name: str,
    summary: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    now = utcnow()
    conn.execute(
        """
        INSERT INTO reports
            (id, job_id, pair_id, source_dataset, target_dataset,
             file_path, file_name, summary_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            report_id,
            job_id,
            pair_id,
            source_dataset,
            target_dataset,
            file_path,
            file_name,
            json.dumps(summary or {}),
            now,
        ),
    )
    conn.commit()
    return {
        "id": report_id,
        "file_path": file_path,
        "file_name": file_name,
        "created_at": now,
    }


def get_report(conn: sqlite3.Connection, report_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
    if not row:
        return None
    return _row_to_report(row)


def get_report_by_job(conn: sqlite3.Connection, job_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM reports WHERE job_id = ?", (job_id,)
    ).fetchone()
    if not row:
        return None
    return _row_to_report(row)


def list_reports(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM reports ORDER BY created_at DESC"
    ).fetchall()
    return [_row_to_report(r) for r in rows]


def delete_report(conn: sqlite3.Connection, report_id: str) -> bool:
    cur = conn.execute("DELETE FROM reports WHERE id = ?", (report_id,))
    conn.commit()
    return cur.rowcount > 0


def _row_to_report(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "job_id": row["job_id"],
        "pair_id": row["pair_id"],
        "source_dataset": row["source_dataset"],
        "target_dataset": row["target_dataset"],
        "file_path": row["file_path"],
        "file_name": row["file_name"],
        "summary": json.loads(row["summary_json"]),
        "created_at": row["created_at"],
    }
