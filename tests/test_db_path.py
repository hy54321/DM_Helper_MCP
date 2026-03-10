from pathlib import Path

from server import db


def test_get_connection_uses_env_override(monkeypatch, tmp_path: Path) -> None:
    db_file = tmp_path / "custom-location" / "dm_helper_custom.db"
    monkeypatch.setenv("DMH_DB_PATH", str(db_file))

    conn = db.get_connection()
    conn.close()

    assert db_file.exists()


def test_get_connection_creates_parent_directories(tmp_path: Path) -> None:
    db_file = tmp_path / "nested" / "folder" / "dm_helper.db"

    conn = db.get_connection(path=str(db_file))
    conn.close()

    assert db_file.exists()
