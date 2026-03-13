import importlib
import subprocess
from pathlib import Path


def _load_ui_api(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "protoquery.db"
    app_dir = tmp_path / "app_data"
    app_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("PROTOQUERY_DB_PATH", str(db_path))
    monkeypatch.setenv("PROTOQUERY_APP_BASE_DIR", str(app_dir))

    import ui.api as ui_api

    return importlib.reload(ui_api)


def test_extract_port_from_url_handles_inspector_token_url(monkeypatch, tmp_path: Path) -> None:
    ui_api = _load_ui_api(monkeypatch, tmp_path)
    url = "http://localhost:6274/?MCP_PROXY_AUTH_TOKEN=abc123"
    assert ui_api._extract_port_from_url(url) == 6274


def test_ports_for_force_stop_prefers_hinted_inspector_port(monkeypatch, tmp_path: Path) -> None:
    ui_api = _load_ui_api(monkeypatch, tmp_path)
    ports = ui_api._ports_for_force_stop("mcp_inspector", hinted_url="http://localhost:6310/?token=x")
    assert ports == [6274, 6277, 6310]


def test_list_listening_pids_on_port_parses_windows_netstat_without_english_state(monkeypatch, tmp_path: Path) -> None:
    ui_api = _load_ui_api(monkeypatch, tmp_path)

    monkeypatch.setattr(ui_api.os, "name", "nt")

    calls = {"count": 0}

    def fake_check_output(cmd, text, stderr):
        calls["count"] += 1
        # First call is PowerShell probe; fail to trigger netstat fallback.
        if calls["count"] == 1:
            raise subprocess.CalledProcessError(returncode=1, cmd=cmd)
        # netstat output with localized listening state token should still parse.
        return "\n".join(
            [
                "  Proto  Local Address          Foreign Address        State           PID",
                "  TCP    127.0.0.1:6274         0.0.0.0:0              ABHOEREN        41200",
            ]
        )

    monkeypatch.setattr(ui_api.subprocess, "check_output", fake_check_output)

    pids = ui_api._list_listening_pids_on_port(6274)
    assert pids == [41200]
