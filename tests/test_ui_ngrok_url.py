import importlib
import io
import json
from pathlib import Path


def _load_ui_api(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "dm_helper.db"
    app_dir = tmp_path / "app_data"
    app_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("DMH_DB_PATH", str(db_path))
    monkeypatch.setenv("DMH_APP_BASE_DIR", str(app_dir))

    import ui.api as ui_api

    return importlib.reload(ui_api)


def test_append_mcp_suffix_adds_suffix(monkeypatch, tmp_path: Path) -> None:
    ui_api = _load_ui_api(monkeypatch, tmp_path)
    assert ui_api._append_mcp_suffix("https://demo.ngrok-free.dev") == "https://demo.ngrok-free.dev/mcp"


def test_append_mcp_suffix_preserves_existing_suffix(monkeypatch, tmp_path: Path) -> None:
    ui_api = _load_ui_api(monkeypatch, tmp_path)
    assert ui_api._append_mcp_suffix("https://demo.ngrok-free.dev/mcp") == "https://demo.ngrok-free.dev/mcp"


def test_discover_ngrok_public_mcp_url_prefers_https(monkeypatch, tmp_path: Path) -> None:
    ui_api = _load_ui_api(monkeypatch, tmp_path)

    class _Resp:
        def __init__(self, payload: dict):
            self._buf = io.BytesIO(json.dumps(payload).encode("utf-8"))

        def read(self):
            return self._buf.read()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(url: str, timeout: float = 1.5):
        return _Resp(
            {
                "tunnels": [
                    {"public_url": "http://demo-http.ngrok-free.dev"},
                    {"public_url": "https://demo-https.ngrok-free.dev"},
                ]
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    result = ui_api._discover_ngrok_public_mcp_url(timeout_seconds=0.2)
    assert result == "https://demo-https.ngrok-free.dev/mcp"
