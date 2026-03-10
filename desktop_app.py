from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from server import db


def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _is_port_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def _pick_port(host: str, preferred_port: int) -> int:
    if preferred_port > 0 and _is_port_available(host, preferred_port):
        return preferred_port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _wait_for_healthcheck(url: str, timeout_seconds: float = 15.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.5) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            time.sleep(0.2)
    raise RuntimeError(f"Backend did not become ready within {timeout_seconds:.0f}s.")


class BackendServer:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None

    def start(self) -> None:
        from ui.api import app

        config = uvicorn.Config(
            app=app,
            host=self.host,
            port=self.port,
            log_level=os.getenv("UI_LOG_LEVEL", "warning"),
            access_log=False,
            # In PyInstaller windowed mode, stderr can be unavailable.
            # Disable uvicorn's default logging config to avoid formatter setup errors.
            log_config=None,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5.0)


def _run_service_mode(service_name: str) -> None:
    if service_name != "mcp-server":
        raise RuntimeError(f"Unsupported service mode '{service_name}'")

    os.environ.setdefault("MCP_TRANSPORT", "streamable-http")
    os.environ.setdefault("FASTMCP_HOST", os.getenv("FASTMCP_HOST", "127.0.0.1"))
    os.environ.setdefault("FASTMCP_PORT", os.getenv("DMH_MCP_PORT", "8000"))
    os.environ.setdefault("DMH_MCP_MODE", os.getenv("DMH_MCP_MODE", "prod"))

    from mcp_server import mcp

    mcp.run(transport=os.getenv("MCP_TRANSPORT", "streamable-http"))


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--service", dest="service_name", default="")
    args, _ = parser.parse_known_args()

    base_dir = _base_dir()
    db_path = base_dir / "dm_helper.db"
    os.environ.setdefault("DMH_DESKTOP_MODE", "1")
    os.environ.setdefault("DMH_APP_BASE_DIR", str(base_dir))
    os.environ.setdefault("DMH_MCP_PORT", "8000")
    os.environ.setdefault("DMH_DB_PATH", str(db_path))

    if args.service_name:
        _run_service_mode(args.service_name)
        return

    # Ensure SQLite file is created at startup in the executable directory.
    conn = db.get_connection()
    conn.close()

    host = os.getenv("UI_HOST", "127.0.0.1")
    try:
        preferred_port = int(os.getenv("UI_PORT", "8001"))
    except ValueError:
        preferred_port = 8001
    port = _pick_port(host, preferred_port)
    os.environ["UI_PORT"] = str(port)

    backend = BackendServer(host=host, port=port)
    backend.start()
    _wait_for_healthcheck(f"http://{host}:{port}/api/health")

    try:
        import webview
    except Exception as exc:
        backend.stop()
        raise RuntimeError(
            "pywebview is required for desktop mode. Install it with: uv add pywebview"
        ) from exc

    window_title = os.getenv("DMH_WINDOW_TITLE", "DM Helper")
    webview.create_window(
        title=window_title,
        url=f"http://{host}:{port}",
        width=1440,
        height=920,
        min_size=(1100, 700),
    )

    try:
        webview.start(debug=os.getenv("DMH_WEBVIEW_DEBUG", "0") == "1")
    finally:
        backend.stop()
        try:
            from ui.api import stop_managed_services

            stop_managed_services()
        except Exception:
            pass


if __name__ == "__main__":
    main()
