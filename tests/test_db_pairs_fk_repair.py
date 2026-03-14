import sqlite3
from pathlib import Path

from server import db


def test_get_connection_repairs_pairs_fk_targeting_datasets_old(tmp_path: Path) -> None:
    db_file = tmp_path / "broken_pairs_fk.db"

    conn = db.get_connection(path=str(db_file))
    db.upsert_dataset(
        conn,
        {
            "id": "source_a",
            "side": "source",
            "file_name": "source.csv",
            "file_path": str(tmp_path / "source.csv"),
            "sheet_name": "",
            "ext": ".csv",
            "columns": ["ID"],
            "raw_columns": ["ID"],
            "column_map": {"ID": "ID"},
            "row_count": 1,
        },
    )
    db.upsert_dataset(
        conn,
        {
            "id": "target_b",
            "side": "target",
            "file_name": "target.csv",
            "file_path": str(tmp_path / "target.csv"),
            "sheet_name": "",
            "ext": ".csv",
            "columns": ["ID"],
            "raw_columns": ["ID"],
            "column_map": {"ID": "ID"},
            "row_count": 1,
        },
    )
    db.upsert_pair(conn, "pair_seed", "source_a", "target_b")
    conn.close()

    # Simulate the broken schema observed in production where pairs FK points to datasets__old.
    raw = sqlite3.connect(str(db_file))
    raw.execute("PRAGMA foreign_keys = OFF")
    raw.execute("CREATE TABLE pairs_tmp AS SELECT * FROM pairs")
    raw.execute("DROP TABLE pairs")
    raw.execute(
        """
        CREATE TABLE pairs (
            id              TEXT PRIMARY KEY,
            source_dataset  TEXT NOT NULL,
            target_dataset  TEXT NOT NULL,
            auto_matched    INTEGER NOT NULL DEFAULT 1,
            enabled         INTEGER NOT NULL DEFAULT 1,
            key_mappings_json TEXT NOT NULL DEFAULT '[]',
            compare_mappings_json TEXT NOT NULL DEFAULT '[]',
            created_at      TEXT NOT NULL,
            FOREIGN KEY (source_dataset) REFERENCES datasets__old(id) ON DELETE CASCADE,
            FOREIGN KEY (target_dataset) REFERENCES datasets__old(id) ON DELETE CASCADE
        )
        """
    )
    raw.execute(
        """
        INSERT INTO pairs (
            id, source_dataset, target_dataset, auto_matched, enabled,
            key_mappings_json, compare_mappings_json, created_at
        )
        SELECT
            id, source_dataset, target_dataset, auto_matched, enabled,
            key_mappings_json, compare_mappings_json, created_at
        FROM pairs_tmp
        """
    )
    raw.execute("DROP TABLE pairs_tmp")
    raw.execute("PRAGMA foreign_keys = ON")
    raw.commit()
    raw.close()

    repaired = db.get_connection(path=str(db_file))
    try:
        fk_targets = {
            str(row["table"]).lower()
            for row in repaired.execute("PRAGMA foreign_key_list(pairs)").fetchall()
        }
        assert fk_targets == {"datasets"}

        # Should no longer fail with: "no such table: main.datasets__old"
        db.upsert_pair(repaired, "pair_seed", "source_a", "target_b", enabled=True)
    finally:
        repaired.close()
