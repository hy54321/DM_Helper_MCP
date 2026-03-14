from __future__ import annotations

import csv
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import threading
import asyncio
import inspect
import httpx
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, IO, List, Optional
from urllib.parse import urlparse, urlunparse

import logging

from anthropic import Anthropic
from cryptography.fernet import Fernet, InvalidToken
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask
from pydantic import BaseModel, Field

from mcp_server import mcp as _mcp_instance
from mcp_server import tool_call_log_source as _tool_call_log_source
from server import catalog as cat
from server import comparison as comp
from server import db
from server import jobs as job_svc
from server import profile as prof
from server import relationships as rel
from server.query_engine import connect, quote
from server.sql_guard import validate as sql_validate

_log = logging.getLogger(__name__)

# ── MCP tool bridge helpers ───────────────────────────────────────────────────
_MAX_TOOL_ROUNDS = 25  # safeguard against infinite tool loops


def _get_anthropic_tools() -> List[Dict[str, Any]]:
    """Return MCP-registered tools in the Anthropic tool-definition format."""
    tools: List[Dict[str, Any]] = []
    for tool in _mcp_instance._tool_manager._tools.values():
        tools.append({
            "name": tool.name,
            "description": tool.description or "",
            "input_schema": tool.parameters,
        })
    return tools


def _call_mcp_tool(name: str, arguments: Dict[str, Any]) -> str:
    """Invoke a registered MCP tool function by *name* and return its string result."""
    tool = _mcp_instance._tool_manager._tools.get(name)
    if not tool:
        return json.dumps({"error": f"Tool '{name}' not found."})
    try:
        with _tool_call_log_source("claude_chat"):
            return tool.fn(**arguments)
    except Exception as exc:
        return json.dumps({"error": f"Tool '{name}' failed: {exc}"})


def _format_export_job_start(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return UI-friendly accepted payload for queued export jobs."""
    if not isinstance(result, dict) or "error" in result:
        return result

    job_id = str(result.get("job_id") or "").strip()
    state = str(result.get("state") or "").strip().lower()
    if not job_id or state not in ("queued", "running"):
        return result

    return {
        "status": "accepted",
        "state": state,
        "job_id": job_id,
        "message": f"Export started in background ({job_id}). You can keep working; refresh status to track progress.",
        "next": {
            "poll_endpoint": f"/api/jobs/{job_id}",
            "summary_endpoint": f"/api/jobs/{job_id}/summary",
            "suggested_poll_interval_seconds": 2,
        },
    }


class RefreshCatalogRequest(BaseModel):
    source_folder: Optional[str] = None
    target_folder: Optional[str] = None
    configurations_folder: Optional[str] = None
    translations_folder: Optional[str] = None
    rules_folder: Optional[str] = None
    report_folder: Optional[str] = None
    include_row_counts: bool = False


class SaveFoldersRequest(BaseModel):
    source_folder: str = ""
    target_folder: str = ""
    configurations_folder: str = ""
    translations_folder: str = ""
    rules_folder: str = ""
    expose_source_to_tools: bool = True
    expose_target_to_tools: bool = True
    expose_configurations_to_tools: bool = False
    expose_translations_to_tools: bool = False
    expose_rules_to_tools: bool = False
    report_folder: str = ""


class SaveFolderConfigRequest(BaseModel):
    name: str = ""
    source_folder: str = ""
    target_folder: str = ""
    configurations_folder: str = ""
    translations_folder: str = ""
    rules_folder: str = ""
    expose_source_to_tools: bool = True
    expose_target_to_tools: bool = True
    expose_configurations_to_tools: bool = False
    expose_translations_to_tools: bool = False
    expose_rules_to_tools: bool = False
    report_folder: str = ""
    set_active: bool = True


class PairOverrideRequest(BaseModel):
    source_dataset_id: str
    target_dataset_id: str
    enabled: bool = True
    key_mappings: Optional[List[Dict[str, Any]]] = None
    compare_mappings: Optional[List[Dict[str, Any]]] = None


class SaveKeyPresetRequest(BaseModel):
    name: str = Field(default="default")
    key_fields: List[str]


class SqlPreviewRequest(BaseModel):
    sql: str
    limit: int = Field(default=10, ge=1, le=100)
    include_total: bool = False


class SqlExportRequest(BaseModel):
    sql: str
    filename: Optional[str] = None
    async_job: bool = True


class FilteredPreviewRequest(BaseModel):
    filter_spec: Dict[str, Any]
    limit: int = Field(default=10, ge=1, le=100)


class ComboSummaryRequest(BaseModel):
    columns: List[str]
    top_n: int = Field(default=10, ge=1, le=100)


class StartCompareRequest(BaseModel):
    source_dataset_id: str
    target_dataset_id: str
    key_fields: List[str] = Field(default_factory=list)
    pair_id: Optional[str] = None
    compare_fields: Optional[List[str]] = None
    key_mappings: Optional[List[Dict[str, Any]]] = None
    compare_mappings: Optional[List[Dict[str, Any]]] = None


class QuickCompareRequest(BaseModel):
    source_dataset_id: str
    target_dataset_id: str
    key_fields: List[str] = Field(default_factory=list)
    compare_fields: Optional[List[str]] = None
    key_mappings: Optional[List[Dict[str, Any]]] = None
    compare_mappings: Optional[List[Dict[str, Any]]] = None
    sample_limit: int = Field(default=10, ge=1, le=100)


class RelationshipUpsertRequest(BaseModel):
    side: str = Field(default="target")
    left_dataset: str
    left_field: str = ""
    left_fields: Optional[List[str]] = None
    right_dataset: str
    right_field: str = ""
    right_fields: Optional[List[str]] = None
    confidence: float = Field(default=1.0, ge=0, le=1)
    method: str = Field(default="manual")
    active: bool = True


class RelationshipLinkRequest(BaseModel):
    side: str = Field(default="target")
    min_confidence: float = Field(default=0.9, ge=0, le=1)
    suggest_only: bool = False


class RelationshipScopedLinkRequest(BaseModel):
    left_side: str = Field(default="any")
    right_side: str = Field(default="any")
    left_dataset: str = ""
    right_dataset: str = ""
    mode: str = Field(default="content")
    min_confidence: float = Field(default=0.6, ge=0, le=1)
    suggest_only: bool = False
    max_links: int = Field(default=200, ge=1, le=2000)


class SaveAppSettingsRequest(BaseModel):
    theme: str = "light"
    anthropic_api_key: Optional[str] = None
    ngrok_authtoken: Optional[str] = None
    mcp_auth_mode: str = "none"
    tool_logging_enabled: Optional[bool] = None
    model: str = ""
    claude_instructions: str = ""


class ValidateAnthropicKeyRequest(BaseModel):
    api_key: str = ""


class LookupAnthropicModelsRequest(BaseModel):
    api_key: str = ""


class ClaudeChatHistoryMessage(BaseModel):
    role: str = "user"
    content: str = ""


class ClaudeChatRequest(BaseModel):
    message: str = ""
    history: List[ClaudeChatHistoryMessage] = Field(default_factory=list)


class ToolLogCleanupOlderThanRequest(BaseModel):
    days: int = Field(default=7, ge=1, le=3650)


@dataclass
class ManagedService:
    process: Optional[subprocess.Popen]
    log_handle: Optional[IO[str]]
    log_file: Optional[str]
    started_at: str
    service_url: Optional[str] = None
    log_offset: int = 0
    ngrok_listener: Any = None


_DESKTOP_SERVICES = ("mcp_server", "mcp_inspector", "ngrok")
_SERVICE_LOCK = threading.RLock()
_SERVICE_STATE: Dict[str, ManagedService] = {}
_SERVICE_ERRORS: Dict[str, str] = {name: "" for name in _DESKTOP_SERVICES}
_ALLOWED_THEMES = {"light", "dark"}
_ALLOWED_MCP_AUTH_MODES = {"none", "api"}
_SETTINGS_THEME_KEY = "ui_theme"
_SETTINGS_ANTHROPIC_API_KEY = "anthropic_api_key_encrypted"
_SETTINGS_ANTHROPIC_MODEL_KEY = "anthropic_model"
_SETTINGS_ANTHROPIC_MODELS_CACHE_KEY = "anthropic_models_cache_json"
_SETTINGS_ANTHROPIC_ACTIVATED_KEY = "anthropic_api_key_activated"
_SETTINGS_CLAUDE_INSTRUCTIONS_KEY = "claude_system_instructions"
_SETTINGS_NGROK_AUTHTOKEN_KEY = "ngrok_authtoken_encrypted"
_SETTINGS_MCP_AUTH_MODE_KEY = "mcp_auth_mode"
_SETTINGS_MCP_API_KEY_KEY = "mcp_api_key_encrypted"
_SETTINGS_TOOL_LOGGING_ENABLED_KEY = "tool_logging_enabled"
_MCP_API_KEY_HEADER_NAME = "x-api-key"
_SETTINGS_FOLDER_CONFIGS_KEY = "folder_configs_json"
_SETTINGS_ACTIVE_FOLDER_CONFIG_KEY = "active_folder_config_id"
_SETTINGS_ENCRYPTION_KEY_FILE = ".protoquery_settings.key"
_LEGACY_SETTINGS_ENCRYPTION_KEY_FILE = ".dmh_settings.key"
_TOOL_LOG_ALLOWED_STATUSES = {"all", "ok", "error"}
_TOOL_LOG_MAX_QUERY_CHARS = 500
_EXTRA_FOLDER_META_BY_SIDE = {
    "configurations": "configurations_folder",
    "translations": "translations_folder",
    "rules": "rules_folder",
}
_EXPOSE_TOOLS_META_BY_SIDE = {
    "source": "expose_source_to_tools",
    "target": "expose_target_to_tools",
    "configurations": "expose_configurations_to_tools",
    "translations": "expose_translations_to_tools",
    "rules": "expose_rules_to_tools",
}
_FOLDER_CONFIG_EXPOSE_KEYS = {
    "source": "expose_source_to_tools",
    "target": "expose_target_to_tools",
    "configurations": "expose_configurations_to_tools",
    "translations": "expose_translations_to_tools",
    "rules": "expose_rules_to_tools",
}


def _env_with_legacy(primary: str, legacy: str, default: str = "") -> str:
    value = os.getenv(primary, "").strip()
    if value:
        return value
    legacy_value = os.getenv(legacy, "").strip()
    if legacy_value:
        return legacy_value
    return default


def _desktop_mode_enabled() -> bool:
    return _env_with_legacy("PROTOQUERY_DESKTOP_MODE", "DMH_DESKTOP_MODE", "0") == "1"


def _app_base_dir() -> Path:
    raw = _env_with_legacy("PROTOQUERY_APP_BASE_DIR", "DMH_APP_BASE_DIR")
    if raw:
        return Path(raw).resolve()
    return Path.cwd()


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tool_log_truncate(value: Any, max_chars: int) -> str:
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}... [truncated {len(text) - max_chars} chars]"


def _mcp_port() -> int:
    for key in ("PROTOQUERY_MCP_PORT", "DMH_MCP_PORT", "MCP_PORT", "FASTMCP_PORT"):
        raw = os.getenv(key, "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        if 1 <= value <= 65535:
            return value
    return 8000


def _resolve_npx_executable() -> str:
    for name in ("npx.cmd", "npx.exe", "npx"):
        found = shutil.which(name)
        if found:
            return found
    raise RuntimeError("npx not found. Install Node.js or add npx to PATH.")


def _load_ngrok_sdk() -> Any:
    try:
        import ngrok as ngrok_sdk
    except Exception as exc:
        raise RuntimeError("ngrok SDK is not installed. Install dependencies and restart the app.") from exc
    return ngrok_sdk


def _resolve_awaitable_sync(value: Any, operation: str) -> Any:
    if not inspect.isawaitable(value):
        return value
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(value)

    # Sync endpoints should not run inside an event loop, but handle it defensively.
    result_box: Dict[str, Any] = {}
    error_box: Dict[str, Exception] = {}

    def _runner() -> None:
        try:
            result_box["value"] = asyncio.run(value)
        except Exception as run_exc:
            error_box["error"] = run_exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "error" in error_box:
        raise RuntimeError(f"{operation} failed: {error_box['error']}") from error_box["error"]
    return result_box.get("value")


def _ngrok_listener_public_url(listener: Any) -> str:
    if listener is None:
        return ""
    try:
        url_attr = getattr(listener, "url", None)
        if callable(url_attr):
            raw = url_attr()
        else:
            raw = url_attr
    except Exception:
        return ""
    return str(raw or "").strip()


def _ngrok_listener_public_mcp_url(listener: Any) -> str:
    return _append_mcp_suffix(_ngrok_listener_public_url(listener))


def _start_ngrok_listener(authtoken: str, mcp_port: int) -> tuple[Any, str]:
    ngrok_sdk = _load_ngrok_sdk()
    ngrok_sdk.set_auth_token(authtoken)
    listener = _resolve_awaitable_sync(
        ngrok_sdk.forward(f"http://127.0.0.1:{mcp_port}"),
        "ngrok.forward",
    )
    if not listener:
        raise RuntimeError("ngrok.forward() did not return a listener.")
    service_url = _ngrok_listener_public_mcp_url(listener)
    return listener, service_url


def _stop_ngrok_listener(listener: Any) -> None:
    ngrok_sdk = _load_ngrok_sdk()
    try:
        close_fn = getattr(listener, "close", None)
        if callable(close_fn):
            _resolve_awaitable_sync(close_fn(), "listener.close")
    except Exception:
        # Fall back to killing any SDK-managed listeners in this process.
        pass
    kill_fn = getattr(ngrok_sdk, "kill", None)
    if callable(kill_fn):
        _resolve_awaitable_sync(kill_fn(), "ngrok.kill")


def _extract_inspector_url_from_log(log_file: str, min_offset: int = 0) -> Optional[str]:
    try:
        with open(log_file, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            start = max(size - 65536, 0, int(min_offset))
            handle.seek(start, os.SEEK_SET)
            payload = handle.read().decode("utf-8", errors="ignore")
    except Exception:
        return None

    # Prefer URLs explicitly printed by inspector startup output.
    ready_urls = re.findall(
        r"MCP Inspector is up and running at:\s*(https?://(?:localhost|127\.0\.0\.1):\d+(?:/\?[^ \r\n]+)?)",
        payload,
        flags=re.IGNORECASE,
    )
    if ready_urls:
        return ready_urls[-1]

    token_urls = re.findall(
        r"https?://localhost:\d+/\?MCP_PROXY_AUTH_TOKEN=[^ \r\n]+",
        payload,
    )
    if token_urls:
        return token_urls[-1]

    base_urls = re.findall(r"https?://localhost:\d+", payload)
    if base_urls:
        return base_urls[-1]
    return None


def _wait_for_inspector_url(
    log_file: str, timeout_seconds: float = 8.0, min_offset: int = 0
) -> Optional[str]:
    import time

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        url = _extract_inspector_url_from_log(log_file, min_offset=min_offset)
        if url:
            return url
        time.sleep(0.2)
    return None


def _wait_for_http_ready(url: str, timeout_seconds: float = 8.0) -> bool:
    import time
    import urllib.error
    import urllib.request

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=1.5) as response:
                if 200 <= response.status < 500:
                    return True
        except urllib.error.HTTPError as exc:
            if 200 <= int(getattr(exc, "code", 0)) < 500:
                return True
            time.sleep(0.2)
        except (OSError, urllib.error.URLError):
            time.sleep(0.2)
    return False


def _mcp_server_url() -> str:
    return f"http://127.0.0.1:{_mcp_port()}/mcp"


def _ui_port() -> int:
    raw = (os.getenv("UI_PORT", "") or "").strip()
    if raw:
        try:
            value = int(raw)
            if 1 <= value <= 65535:
                return value
        except ValueError:
            pass
    return 8001


def _inspector_mcp_relay_url() -> str:
    return f"http://127.0.0.1:{_ui_port()}/api/inspector/mcp"


def _inspector_base_url() -> str:
    return "http://localhost:6274"


def _append_mcp_suffix(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""

    path = (parsed.path or "").rstrip("/")
    if not path.endswith("/mcp"):
        path = f"{path}/mcp" if path else "/mcp"

    return urlunparse(parsed._replace(path=path))


def _discover_ngrok_public_mcp_url(timeout_seconds: float = 1.5) -> str:
    import urllib.error
    import urllib.request

    for port in (4040, 4041):
        api_url = f"http://127.0.0.1:{port}/api/tunnels"
        try:
            with urllib.request.urlopen(api_url, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8", errors="ignore"))
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError):
            continue

        tunnels = payload.get("tunnels") if isinstance(payload, dict) else None
        if not isinstance(tunnels, list):
            continue

        https_url = ""
        fallback_url = ""
        for item in tunnels:
            if not isinstance(item, dict):
                continue
            public_url = str(item.get("public_url") or "").strip()
            if not public_url:
                continue
            if public_url.startswith("https://"):
                https_url = public_url
                break
            if not fallback_url:
                fallback_url = public_url

        chosen = https_url or fallback_url
        if chosen:
            with_suffix = _append_mcp_suffix(chosen)
            if with_suffix:
                return with_suffix

    return ""


def _is_port_listening(host: str, port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.4)
        return sock.connect_ex((host, port)) == 0


def _service_command(
    service_name: str, mcp_auth: Optional[Dict[str, str]] = None
) -> tuple[List[str], Dict[str, str]]:
    base_dir = _app_base_dir()
    mcp_port = str(_mcp_port())
    env_overrides: Dict[str, str] = {}
    auth_mode = _normalize_mcp_auth_mode((mcp_auth or {}).get("mode", "none"))
    auth_header = (mcp_auth or {}).get("header_name", _MCP_API_KEY_HEADER_NAME).strip() or _MCP_API_KEY_HEADER_NAME
    auth_api_key = (mcp_auth or {}).get("api_key", "").strip()

    if service_name == "mcp_server":
        if getattr(sys, "frozen", False):
            command = [sys.executable, "--service", "mcp-server"]
        else:
            command = [sys.executable, str(base_dir / "mcp_server.py")]
        env_overrides = {
            "MCP_TRANSPORT": "streamable-http",
            "FASTMCP_HOST": os.getenv("FASTMCP_HOST", "127.0.0.1"),
            "FASTMCP_PORT": mcp_port,
            "PROTOQUERY_MCP_MODE": _env_with_legacy("PROTOQUERY_MCP_MODE", "DMH_MCP_MODE", "prod"),
        }
        if auth_mode == "api":
            if not auth_api_key:
                raise RuntimeError("MCP authentication mode is API but no API key is stored. Generate one in Settings.")
            env_overrides["PROTOQUERY_MCP_AUTH_MODE"] = "api"
            env_overrides["PROTOQUERY_MCP_API_KEY"] = auth_api_key
            env_overrides["PROTOQUERY_MCP_API_KEY_HEADER"] = auth_header
        else:
            env_overrides["PROTOQUERY_MCP_AUTH_MODE"] = "none"
        return command, env_overrides

    if service_name == "mcp_inspector":
        npx_exe = _resolve_npx_executable()
        server_url = f"http://127.0.0.1:{mcp_port}/mcp"
        if auth_mode == "api":
            # Use a local relay so inspector does not depend on its own persisted custom headers.
            server_url = _inspector_mcp_relay_url()
        command = [
            npx_exe,
            "@modelcontextprotocol/inspector",
            "--transport",
            "http",
            "--server-url",
            server_url,
        ]
        if auth_mode == "api" and not auth_api_key:
            raise RuntimeError("MCP authentication mode is API but no API key is stored. Generate one in Settings.")
        env_overrides = {
            "MCP_AUTO_OPEN_ENABLED": "false",
        }
        return command, env_overrides

    raise ValueError(f"Unsupported service '{service_name}'")


def _command_for_log(command: List[str]) -> str:
    if not command:
        return ""
    redacted: List[str] = []
    i = 0
    while i < len(command):
        part = command[i]
        if part == "--header" and i + 1 < len(command):
            header_value = command[i + 1]
            if ":" in header_value:
                header_name, _ = header_value.split(":", 1)
                redacted.extend(["--header", f"{header_name}: ***"])
                i += 2
                continue
        redacted.append(part)
        i += 1
    return " ".join(redacted)


def _cleanup_if_exited(service_name: str) -> None:
    service = _SERVICE_STATE.get(service_name)
    if not service:
        return
    if service.process is None:
        return
    return_code = service.process.poll()
    if return_code is None:
        return
    _SERVICE_ERRORS[service_name] = f"Exited with code {return_code}. Check logs."
    try:
        if service.log_handle:
            service.log_handle.close()
    except Exception:
        pass
    _SERVICE_STATE.pop(service_name, None)


def _terminate_process_tree(process: subprocess.Popen) -> None:
    pid = process.pid
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        except Exception:
            pass
    try:
        process.terminate()
        process.wait(timeout=8)
    except Exception:
        try:
            process.kill()
            process.wait(timeout=2)
        except Exception:
            pass


def _list_listening_pids_on_port(port: int) -> List[int]:
    if port < 1 or port > 65535:
        return []

    pids: List[int] = []
    if os.name == "nt":
        # Prefer PowerShell TCP connection API first; it is more robust than netstat parsing.
        ps_script = (
            "$ErrorActionPreference='SilentlyContinue';"
            f"Get-NetTCPConnection -LocalPort {port} -State Listen | "
            "Select-Object -ExpandProperty OwningProcess"
        )
        try:
            output = subprocess.check_output(
                ["powershell", "-NoProfile", "-Command", ps_script],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            for row in output.splitlines():
                row = row.strip()
                if row.isdigit():
                    pids.append(int(row))
            if pids:
                return sorted(set(pids))
        except Exception:
            pass

        try:
            output = subprocess.check_output(
                ["netstat", "-ano", "-p", "tcp"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            return []

        for line in output.splitlines():
            parts = line.split()
            if len(parts) < 5:
                continue
            if parts[0].upper() != "TCP":
                continue
            local_addr = parts[1]
            pid_text = parts[-1]
            match = re.search(r":(\d+)$", local_addr)
            if not match:
                continue
            try:
                line_port = int(match.group(1))
                pid = int(pid_text)
            except Exception:
                continue
            if line_port == port and pid > 0:
                pids.append(pid)
        return sorted(set(pids))

    # Best-effort non-Windows fallback.
    try:
        output = subprocess.check_output(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        for row in output.splitlines():
            row = row.strip()
            if row.isdigit():
                pids.append(int(row))
    except Exception:
        pass
    return sorted(set(pids))


def _list_process_pids_by_name(process_names: List[str]) -> List[int]:
    names = {str(name or "").strip().lower() for name in process_names if str(name or "").strip()}
    if not names:
        return []

    pids: List[int] = []
    if os.name == "nt":
        try:
            output = subprocess.check_output(
                ["tasklist", "/FO", "CSV", "/NH"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            return []
        for row in csv.reader(output.splitlines()):
            if len(row) < 2:
                continue
            image_name = str(row[0] or "").strip().lower()
            pid_text = str(row[1] or "").strip()
            if image_name in names and pid_text.isdigit():
                pids.append(int(pid_text))
        return sorted(set(pids))

    try:
        output = subprocess.check_output(
            ["ps", "-eo", "pid=,comm="],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []
    for line in output.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2:
            continue
        pid_text, command_name = parts
        if not pid_text.isdigit():
            continue
        name = command_name.strip().lower()
        if name in names:
            pids.append(int(pid_text))
    return sorted(set(pids))


def _list_ngrok_pids() -> List[int]:
    return _list_process_pids_by_name(["ngrok.exe", "ngrok"])


def _terminate_pid_tree(pid: int) -> bool:
    if pid <= 0 or pid == os.getpid():
        return False

    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except Exception:
            return False

    try:
        os.kill(pid, 15)
        return True
    except Exception:
        return False


def _extract_port_from_url(service_url: Optional[str]) -> Optional[int]:
    raw = (service_url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
        port = parsed.port
        if port and 1 <= int(port) <= 65535:
            return int(port)
    except Exception:
        return None
    return None


def _ports_for_force_stop(service_name: str, hinted_url: Optional[str] = None) -> List[int]:
    if service_name == "mcp_server":
        return [_mcp_port()]
    if service_name == "mcp_inspector":
        ports: List[int] = []
        hinted_port = _extract_port_from_url(hinted_url)
        if hinted_port:
            ports.append(hinted_port)
        ports.extend([6277, 6274])
        return sorted(set(p for p in ports if 1 <= p <= 65535))
    if service_name == "ngrok":
        return [4040, 4041]
    return []


def _force_stop_service(service_name: str) -> Dict[str, Any]:
    if service_name not in _DESKTOP_SERVICES:
        raise ValueError(f"Unsupported service '{service_name}'")

    with _SERVICE_LOCK:
        hinted_url: Optional[str] = None
        if service_name == "mcp_inspector":
            current = _SERVICE_STATE.get(service_name)
            hinted_url = current.service_url if current else None
            if not hinted_url:
                hinted_url = _extract_inspector_url_from_log(
                    str(_app_base_dir() / "logs" / "mcp_inspector.log")
                ) or _inspector_base_url()

        # Stop managed process first (if this app started it).
        _stop_service(service_name)

        killed_pids: List[int] = []
        checked_ports = _ports_for_force_stop(service_name, hinted_url=hinted_url)
        for port in checked_ports:
            port_pids = _list_listening_pids_on_port(port)
            for pid in port_pids:
                if _terminate_pid_tree(pid):
                    killed_pids.append(pid)
        if service_name == "ngrok":
            for pid in _list_ngrok_pids():
                if _terminate_pid_tree(pid):
                    killed_pids.append(pid)

        # Re-evaluate and refresh state after force kill attempts.
        snapshot = _service_snapshot(service_name)
        if killed_pids:
            _SERVICE_ERRORS[service_name] = ""
            snapshot["last_error"] = ""
        return {
            "service": snapshot,
            "killed_pids": sorted(set(killed_pids)),
            "checked_ports": checked_ports,
        }


def _service_snapshot(service_name: str) -> Dict[str, Any]:
    if service_name not in _DESKTOP_SERVICES:
        raise ValueError(f"Unsupported service '{service_name}'")
    with _SERVICE_LOCK:
        _cleanup_if_exited(service_name)
        service = _SERVICE_STATE.get(service_name)
        managed_process_running = bool(service and service.process and service.process.poll() is None)
        managed_ngrok_running = bool(service_name == "ngrok" and service and service.ngrok_listener is not None)
        running = bool(managed_process_running or managed_ngrok_running)
        external_pid: Optional[int] = None
        service_url = service.service_url if service else None
        if running and service_name == "mcp_server":
            service_url = _mcp_server_url()
        if running and service_name == "mcp_inspector" and service and not service_url:
            service_url = _extract_inspector_url_from_log(
                service.log_file or str(_app_base_dir() / "logs" / "mcp_inspector.log"),
                min_offset=service.log_offset,
            )
            if service_url:
                service.service_url = service_url
        if not running and service_name == "mcp_server":
            mcp_url = _mcp_server_url()
            if _wait_for_http_ready(mcp_url, timeout_seconds=0.8):
                running = True
                service_url = mcp_url
                if not _SERVICE_ERRORS.get(service_name):
                    _SERVICE_ERRORS[service_name] = (
                        "MCP server is already running in another process; reusing existing endpoint."
                    )
        if not running and service_name == "mcp_inspector":
            inspector_base = _inspector_base_url()
            if _wait_for_http_ready(inspector_base, timeout_seconds=0.8):
                running = True
                service_url = _extract_inspector_url_from_log(
                    str(_app_base_dir() / "logs" / "mcp_inspector.log")
                ) or inspector_base
                if not _SERVICE_ERRORS.get(service_name):
                    _SERVICE_ERRORS[service_name] = (
                        "MCP inspector is already running in another process; reusing existing endpoint."
                    )
        if not running and service_name == "ngrok":
            ngrok_pids = _list_ngrok_pids()
            if ngrok_pids:
                running = True
                external_pid = ngrok_pids[0]
                if not _SERVICE_ERRORS.get(service_name):
                    _SERVICE_ERRORS[service_name] = (
                        "ngrok is already running in another process; reusing existing tunnel process."
                    )
        if running and service_name == "ngrok":
            ngrok_url = ""
            if managed_ngrok_running and service:
                ngrok_url = _ngrok_listener_public_mcp_url(service.ngrok_listener)
            if not ngrok_url:
                ngrok_url = _discover_ngrok_public_mcp_url(timeout_seconds=1.0)
            if ngrok_url:
                service_url = ngrok_url
                if service and (managed_process_running or managed_ngrok_running):
                    service.service_url = ngrok_url
        if service_name == "mcp_inspector" and not service_url:
            service_url = "http://localhost:6274"
        reported_port: Optional[int] = None
        if service_name == "mcp_server":
            reported_port = _mcp_port()
        elif service_name == "ngrok":
            reported_port = _mcp_port()
        elif service_name == "mcp_inspector":
            reported_port = _extract_port_from_url(service_url) or 6274
        return {
            "name": service_name,
            "running": running,
            "pid": service.process.pid if running and service and service.process else external_pid,
            "started_at": service.started_at if running and service else None,
            "log_file": service.log_file if service and service.log_file else None,
            "last_error": _SERVICE_ERRORS.get(service_name, ""),
            "port": reported_port,
            "service_url": service_url,
        }


def _start_service(service_name: str) -> Dict[str, Any]:
    if service_name not in _DESKTOP_SERVICES:
        raise ValueError(f"Unsupported service '{service_name}'")

    with _SERVICE_LOCK:
        _cleanup_if_exited(service_name)
        current = _SERVICE_STATE.get(service_name)
        current_running = bool(
            current
            and (
                (current.process and current.process.poll() is None)
                or (service_name == "ngrok" and current.ngrok_listener is not None)
            )
        )
        if current_running:
            return _service_snapshot(service_name)

        if service_name == "mcp_server":
            mcp_url = _mcp_server_url()
            if _wait_for_http_ready(mcp_url, timeout_seconds=1.0):
                return _service_snapshot(service_name)
            mcp_port = _mcp_port()
            if _is_port_listening("127.0.0.1", mcp_port):
                raise RuntimeError(
                    f"Port {mcp_port} is already in use by another process; unable to start managed MCP server."
                )

        if service_name == "mcp_inspector":
            inspector_base = _inspector_base_url()
            if _wait_for_http_ready(inspector_base, timeout_seconds=1.0):
                return _service_snapshot(service_name)
            if _is_port_listening("127.0.0.1", 6277):
                raise RuntimeError(
                    "Inspector proxy port 6277 is already in use by another process; unable to start managed inspector."
                )
            _start_service("mcp_server")
            mcp_url = _mcp_server_url()
            if not _wait_for_http_ready(mcp_url, timeout_seconds=10.0):
                raise RuntimeError(f"MCP server did not become ready at {mcp_url}.")
        if service_name == "ngrok":
            if _list_ngrok_pids():
                return _service_snapshot(service_name)
            conn = db.get_connection()
            try:
                authtoken = _require_stored_ngrok_authtoken(conn)
            finally:
                conn.close()
            listener, service_url = _start_ngrok_listener(authtoken=authtoken, mcp_port=_mcp_port())
            _SERVICE_STATE[service_name] = ManagedService(
                process=None,
                log_handle=None,
                log_file=None,
                started_at=_iso_utc_now(),
                service_url=service_url or None,
                log_offset=0,
                ngrok_listener=listener,
            )
            _SERVICE_ERRORS[service_name] = ""
            return _service_snapshot(service_name)

        mcp_auth: Optional[Dict[str, str]] = None
        if service_name in ("mcp_server", "mcp_inspector"):
            conn = db.get_connection()
            try:
                mcp_auth = _mcp_auth_runtime_config(conn)
            finally:
                conn.close()

        command, env_overrides = _service_command(service_name, mcp_auth=mcp_auth)
        base_dir = _app_base_dir()
        log_dir = base_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{service_name}.log"
        log_handle = open(log_file, "a", encoding="utf-8")
        log_offset = log_handle.tell()
        log_handle.write(f"\n[{_iso_utc_now()}] START {_command_for_log(command)}\n")
        log_handle.flush()

        creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        env = os.environ.copy()
        env.update(env_overrides)
        try:
            process = subprocess.Popen(
                command,
                cwd=str(base_dir),
                env=env,
                stdout=log_handle,
                stderr=log_handle,
                creationflags=creation_flags,
            )
        except Exception:
            try:
                log_handle.close()
            except Exception:
                pass
            raise
        service = ManagedService(
            process=process,
            log_handle=log_handle,
            log_file=str(log_file),
            started_at=_iso_utc_now(),
            service_url=None,
            log_offset=log_offset,
            ngrok_listener=None,
        )
        _SERVICE_STATE[service_name] = service
        if service_name == "mcp_server":
            mcp_url = _mcp_server_url()
            if not _wait_for_http_ready(mcp_url, timeout_seconds=10.0):
                _cleanup_if_exited(service_name)
                raise RuntimeError(f"MCP server did not become ready at {mcp_url}. Check logs.")
        if service_name == "mcp_inspector":
            inspector_url = (
                _wait_for_inspector_url(str(log_file), min_offset=log_offset) or "http://localhost:6274"
            )
            if not _wait_for_http_ready(inspector_url, timeout_seconds=10.0):
                _cleanup_if_exited(service_name)
                raise RuntimeError(f"MCP inspector did not become ready at {inspector_url}. Check logs.")
            service.service_url = inspector_url
        _SERVICE_ERRORS[service_name] = ""
        return _service_snapshot(service_name)


def _stop_service(service_name: str) -> Dict[str, Any]:
    if service_name not in _DESKTOP_SERVICES:
        raise ValueError(f"Unsupported service '{service_name}'")

    with _SERVICE_LOCK:
        if service_name == "mcp_server":
            _stop_service("mcp_inspector")

        _cleanup_if_exited(service_name)
        service = _SERVICE_STATE.get(service_name)
        if not service:
            if service_name == "mcp_inspector":
                inspector_base = _inspector_base_url()
                if _wait_for_http_ready(inspector_base, timeout_seconds=1.0):
                    _SERVICE_ERRORS[service_name] = (
                        "MCP inspector is running externally and cannot be stopped from this app instance."
                    )
            if service_name == "ngrok":
                if _list_ngrok_pids():
                    _SERVICE_ERRORS[service_name] = (
                        "ngrok is running externally and cannot be stopped from this app instance."
                    )
            if service_name == "mcp_server":
                mcp_url = _mcp_server_url()
                if _wait_for_http_ready(mcp_url, timeout_seconds=1.0):
                    _SERVICE_ERRORS[service_name] = (
                        "MCP server is running externally and cannot be stopped from this app instance."
                    )
            return _service_snapshot(service_name)

        try:
            if service_name == "ngrok":
                _stop_ngrok_listener(service.ngrok_listener)
            elif service.process:
                _terminate_process_tree(service.process)
        finally:
            try:
                if service.log_handle:
                    service.log_handle.write(f"[{_iso_utc_now()}] STOP\n")
                    service.log_handle.flush()
            except Exception:
                pass
            try:
                if service.log_handle:
                    service.log_handle.close()
            except Exception:
                pass
            _SERVICE_STATE.pop(service_name, None)

        return _service_snapshot(service_name)


def stop_managed_services() -> None:
    for service_name in list(_DESKTOP_SERVICES):
        try:
            _stop_service(service_name)
        except Exception:
            pass


def _clean_field_mappings(
    mappings: Optional[List[Dict[str, Any]]],
    preserve_metadata: bool = False,
) -> Optional[List[Dict[str, Any]]]:
    if not mappings:
        return None
    cleaned: List[Dict[str, Any]] = []
    for m in mappings:
        src = (m.get("source_field") or m.get("source") or "").strip()
        tgt = (m.get("target_field") or m.get("target") or "").strip()
        if src and tgt:
            row: Dict[str, Any] = {"source_field": src, "target_field": tgt}
            if preserve_metadata:
                origin_mode = str(m.get("origin_mode") or "manual").strip().lower() or "manual"
                row["origin_mode"] = origin_mode
                conf = m.get("confidence")
                try:
                    conf_value = float(conf) if conf is not None else None
                except Exception:
                    conf_value = None
                row["confidence"] = conf_value
                row["is_key_pair"] = bool(m.get("is_key_pair", False))
                row["low_cardinality"] = bool(m.get("low_cardinality", False))
                row["use_key"] = bool(m.get("use_key", False))
                row["use_compare"] = bool(m.get("use_compare", True))
            cleaned.append(row)
    return cleaned or None


def _datasets_or_404() -> List[Dict[str, Any]]:
    conn = db.get_connection()
    datasets = db.list_datasets(conn)
    conn.close()
    if not datasets:
        raise HTTPException(status_code=400, detail="No datasets loaded. Run catalog refresh first.")
    return datasets


def _meta_bool(conn, key: str, default: bool = False) -> bool:
    raw = (db.get_meta(conn, key, "1" if default else "0") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _save_meta_bool(conn, key: str, value: bool) -> None:
    db.set_meta(conn, key, "1" if value else "0", commit=False)


def _get_saved_folders_from_conn(conn) -> Dict[str, Any]:
    source = (db.get_meta(conn, "source_folder", "") or "").strip()
    target = (db.get_meta(conn, "target_folder", "") or "").strip()
    configurations = (db.get_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["configurations"], "") or "").strip()
    translations = (db.get_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["translations"], "") or "").strip()
    rules = (db.get_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["rules"], "") or "").strip()
    report = (db.get_meta(conn, "report_folder", "") or "").strip()
    return {
        "source_folder": source,
        "target_folder": target,
        "configurations_folder": configurations,
        "translations_folder": translations,
        "rules_folder": rules,
        "report_folder": report,
        "expose_source_to_tools": _meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["source"], default=True),
        "expose_target_to_tools": _meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["target"], default=True),
        "expose_configurations_to_tools": _meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["configurations"], default=False),
        "expose_translations_to_tools": _meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["translations"], default=False),
        "expose_rules_to_tools": _meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["rules"], default=False),
    }


def _get_saved_folders() -> Dict[str, Any]:
    conn = db.get_connection()
    try:
        return _get_saved_folders_from_conn(conn)
    finally:
        conn.close()


def _normalize_folder_config_name(value: str) -> str:
    name = re.sub(r"\s+", " ", str(value or "").strip())
    if not name:
        raise HTTPException(status_code=400, detail="Folder configuration name is required.")
    if len(name) > 80:
        raise HTTPException(status_code=400, detail="Folder configuration name is too long (max 80 characters).")
    return name


def _load_folder_configs_from_conn(conn) -> List[Dict[str, Any]]:
    raw = (db.get_meta(conn, _SETTINGS_FOLDER_CONFIGS_KEY, "") or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []

    configs: List[Dict[str, Any]] = []
    seen_ids = set()
    for item in parsed:
        if not isinstance(item, dict):
            continue
        config_id = str(item.get("id") or "").strip()
        name = str(item.get("name") or "").strip()
        if not config_id or not name or config_id in seen_ids:
            continue
        configs.append(
            {
                "id": config_id,
                "name": name,
                "source_folder": str(item.get("source_folder") or "").strip(),
                "target_folder": str(item.get("target_folder") or "").strip(),
                "configurations_folder": str(item.get("configurations_folder") or "").strip(),
                "translations_folder": str(item.get("translations_folder") or "").strip(),
                "rules_folder": str(item.get("rules_folder") or "").strip(),
                "report_folder": str(item.get("report_folder") or "").strip(),
                "expose_source_to_tools": bool(item.get("expose_source_to_tools", True)),
                "expose_target_to_tools": bool(item.get("expose_target_to_tools", True)),
                "expose_configurations_to_tools": bool(item.get("expose_configurations_to_tools", False)),
                "expose_translations_to_tools": bool(item.get("expose_translations_to_tools", False)),
                "expose_rules_to_tools": bool(item.get("expose_rules_to_tools", False)),
                "created_at": str(item.get("created_at") or "").strip(),
                "updated_at": str(item.get("updated_at") or "").strip(),
            }
        )
        seen_ids.add(config_id)

    configs.sort(key=lambda c: (c["name"].lower(), c["id"]))
    return configs


def _save_folder_configs_to_conn(conn, configs: List[Dict[str, Any]], active_id: str) -> str:
    valid_ids = {cfg["id"] for cfg in configs}
    next_active = active_id if active_id in valid_ids else ""
    db.set_meta(conn, _SETTINGS_FOLDER_CONFIGS_KEY, json.dumps(configs), commit=False)
    db.set_meta(conn, _SETTINGS_ACTIVE_FOLDER_CONFIG_KEY, next_active, commit=False)
    return next_active


def _find_matching_folder_config_id(
    configs: List[Dict[str, Any]],
    source_folder: str,
    target_folder: str,
    configurations_folder: str,
    translations_folder: str,
    rules_folder: str,
    report_folder: str,
    expose_source_to_tools: bool,
    expose_target_to_tools: bool,
    expose_configurations_to_tools: bool,
    expose_translations_to_tools: bool,
    expose_rules_to_tools: bool,
) -> str:
    for cfg in configs:
        if (
            cfg["source_folder"] == source_folder
            and cfg["target_folder"] == target_folder
            and cfg["configurations_folder"] == configurations_folder
            and cfg["translations_folder"] == translations_folder
            and cfg["rules_folder"] == rules_folder
            and cfg["report_folder"] == report_folder
            and bool(cfg.get("expose_source_to_tools", True)) == bool(expose_source_to_tools)
            and bool(cfg.get("expose_target_to_tools", True)) == bool(expose_target_to_tools)
            and bool(cfg.get("expose_configurations_to_tools", False)) == bool(expose_configurations_to_tools)
            and bool(cfg.get("expose_translations_to_tools", False)) == bool(expose_translations_to_tools)
            and bool(cfg.get("expose_rules_to_tools", False)) == bool(expose_rules_to_tools)
        ):
            return cfg["id"]
    return ""


def _folder_configs_payload(conn) -> Dict[str, Any]:
    configs = _load_folder_configs_from_conn(conn)
    active_id = (db.get_meta(conn, _SETTINGS_ACTIVE_FOLDER_CONFIG_KEY, "") or "").strip()
    next_active = _save_folder_configs_to_conn(conn, configs, active_id)
    return {"configs": configs, "active_id": next_active}


def _normalize_theme(theme: str) -> str:
    val = (theme or "").strip().lower()
    if val in _ALLOWED_THEMES:
        return val
    return "light"


def _normalize_mcp_auth_mode(mode: str) -> str:
    val = (mode or "").strip().lower()
    if val in ("api", "api_key", "apikey"):
        return "api"
    if val in _ALLOWED_MCP_AUTH_MODES:
        return val
    return "none"


def _settings_encryption_key_file_path() -> Path:
    legacy_key = _app_base_dir() / _LEGACY_SETTINGS_ENCRYPTION_KEY_FILE
    if legacy_key.exists():
        return legacy_key
    return _app_base_dir() / _SETTINGS_ENCRYPTION_KEY_FILE


def _settings_cipher() -> Fernet:
    env_key = _env_with_legacy("PROTOQUERY_SETTINGS_ENCRYPTION_KEY", "DMH_SETTINGS_ENCRYPTION_KEY")
    if env_key:
        try:
            return Fernet(env_key.encode("utf-8"))
        except Exception as exc:
            raise RuntimeError("PROTOQUERY_SETTINGS_ENCRYPTION_KEY is invalid.") from exc

    key_file = _settings_encryption_key_file_path()
    key_file.parent.mkdir(parents=True, exist_ok=True)
    if key_file.exists():
        raw_key = key_file.read_text(encoding="utf-8").strip()
        try:
            return Fernet(raw_key.encode("utf-8"))
        except Exception as exc:
            raise RuntimeError(
                f"Settings key file is invalid: {key_file}. Remove it and re-enter the API key."
            ) from exc

    key = Fernet.generate_key()
    key_file.write_text(key.decode("utf-8"), encoding="utf-8")
    if os.name != "nt":
        try:
            os.chmod(key_file, 0o600)
        except Exception:
            pass
    return Fernet(key)


def _encrypt_secret(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    token = _settings_cipher().encrypt(raw.encode("utf-8"))
    return token.decode("utf-8")


def _decrypt_secret(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        token = _settings_cipher().decrypt(raw.encode("utf-8"))
    except InvalidToken as exc:
        raise RuntimeError("Stored secret could not be decrypted with current settings key.") from exc
    return token.decode("utf-8")


def _mask_secret(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if len(raw) <= 8:
        return "*" * len(raw)
    return f"{raw[:6]}...{raw[-4:]}"


def _generate_mcp_api_key() -> str:
    # 32 bytes of entropy, URL-safe for easy copy/paste into MCP client settings.
    return secrets.token_urlsafe(32)


def _read_stored_ngrok_authtoken(conn) -> tuple[str, bool]:
    encrypted = (db.get_meta(conn, _SETTINGS_NGROK_AUTHTOKEN_KEY, "") or "").strip()
    if not encrypted:
        return "", False
    try:
        return _decrypt_secret(encrypted), True
    except RuntimeError:
        return "", True


def _require_stored_ngrok_authtoken(conn) -> str:
    token, configured = _read_stored_ngrok_authtoken(conn)
    if token:
        return token
    if configured:
        raise RuntimeError("Stored ngrok authtoken cannot be decrypted. Please set it again.")
    raise RuntimeError("ngrok authtoken is required. Save it in Settings first.")


def _read_stored_mcp_api_key(conn) -> tuple[str, bool]:
    encrypted = (db.get_meta(conn, _SETTINGS_MCP_API_KEY_KEY, "") or "").strip()
    if not encrypted:
        return "", False
    try:
        return _decrypt_secret(encrypted), True
    except RuntimeError:
        return "", True


def _require_stored_mcp_api_key(conn) -> str:
    key, configured = _read_stored_mcp_api_key(conn)
    if key:
        return key
    if configured:
        raise RuntimeError("Stored MCP API key cannot be decrypted. Generate a new key in Settings.")
    raise RuntimeError("MCP API key is required when MCP authentication mode is API. Generate one in Settings.")


def _mcp_auth_runtime_config(conn) -> Dict[str, str]:
    mode = _normalize_mcp_auth_mode(db.get_meta(conn, _SETTINGS_MCP_AUTH_MODE_KEY, "none") or "none")
    config: Dict[str, str] = {"mode": mode, "header_name": _MCP_API_KEY_HEADER_NAME}
    if mode == "api":
        config["api_key"] = _require_stored_mcp_api_key(conn)
    return config


_HOP_BY_HOP_REQUEST_HEADERS = {
    "host",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
}

_HOP_BY_HOP_RESPONSE_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


def _filtered_proxy_request_headers(request: Request) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in request.headers.items():
        lower_key = key.lower()
        if lower_key in _HOP_BY_HOP_REQUEST_HEADERS:
            continue
        # Inspector's own proxy-auth header is not relevant to upstream MCP server.
        if lower_key == "x-mcp-proxy-auth":
            continue
        headers[key] = value
    return headers


def _filtered_proxy_response_headers(headers: httpx.Headers) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in _HOP_BY_HOP_RESPONSE_HEADERS:
            continue
        result[key] = value
    return result


def _read_stored_anthropic_key(conn) -> tuple[str, bool]:
    encrypted = (db.get_meta(conn, _SETTINGS_ANTHROPIC_API_KEY, "") or "").strip()
    if not encrypted:
        return "", False
    try:
        return _decrypt_secret(encrypted), True
    except RuntimeError:
        return "", True


def _require_stored_anthropic_key(conn) -> str:
    key, configured = _read_stored_anthropic_key(conn)
    if key:
        return key
    if configured:
        raise RuntimeError("Stored Anthropic API key cannot be decrypted. Please set it again.")
    return ""


def _list_anthropic_models(api_key: str, limit: int = 100) -> List[Dict[str, str]]:
    key = (api_key or "").strip()
    if not key:
        raise RuntimeError("Anthropic API key is required.")

    try:
        client = Anthropic(api_key=key, timeout=20.0)
        page = client.models.list(limit=max(1, min(int(limit), 100)))
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch Anthropic models: {exc}") from exc

    raw_items = getattr(page, "data", None)
    if raw_items is None:
        raw_items = list(page)

    models: List[Dict[str, str]] = []
    for item in raw_items or []:
        model_id = str(getattr(item, "id", "") or "").strip()
        if not model_id:
            continue
        display_name = str(getattr(item, "display_name", "") or model_id).strip() or model_id
        models.append({"id": model_id, "display_name": display_name})

    models.sort(key=lambda row: row["id"].lower())
    return models


def _load_cached_models(conn) -> List[Dict[str, str]]:
    raw = (db.get_meta(conn, _SETTINGS_ANTHROPIC_MODELS_CACHE_KEY, "") or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    models: List[Dict[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        model_id = str(item.get("id") or "").strip()
        if not model_id:
            continue
        display_name = str(item.get("display_name") or model_id).strip() or model_id
        models.append({"id": model_id, "display_name": display_name})
    return models


def _save_cached_models(conn, models: List[Dict[str, str]]) -> None:
    db.set_meta(conn, _SETTINGS_ANTHROPIC_MODELS_CACHE_KEY, json.dumps(models), commit=False)


def _is_anthropic_key_activated(conn) -> bool:
    raw = (db.get_meta(conn, _SETTINGS_ANTHROPIC_ACTIVATED_KEY, "0") or "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _set_anthropic_key_activated(conn, activated: bool) -> None:
    db.set_meta(conn, _SETTINGS_ANTHROPIC_ACTIVATED_KEY, "1" if activated else "0", commit=False)


def _prepare_claude_history(history: List[ClaudeChatHistoryMessage]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for item in history or []:
        role = (item.role or "").strip().lower()
        if role not in ("user", "assistant"):
            continue
        content = (item.content or "").strip()
        if not content:
            continue
        normalized.append({"role": role, "content": content})
    return normalized


def _text_from_anthropic_message(message: Any) -> str:
    parts: List[str] = []
    for block in getattr(message, "content", []) or []:
        if getattr(block, "type", "") != "text":
            continue
        text = str(getattr(block, "text", "") or "").strip()
        if text:
            parts.append(text)
    return "\n".join(parts).strip()


def _get_saved_app_settings() -> Dict[str, Any]:
    conn = db.get_connection()
    try:
        theme = _normalize_theme((db.get_meta(conn, _SETTINGS_THEME_KEY, "light") or "light"))
        model = (db.get_meta(conn, _SETTINGS_ANTHROPIC_MODEL_KEY, "") or "").strip()
        claude_instructions = (db.get_meta(conn, _SETTINGS_CLAUDE_INSTRUCTIONS_KEY, "") or "").strip()
        models = _load_cached_models(conn)
        anthropic_key, anthropic_configured = _read_stored_anthropic_key(conn)
        anthropic_needs_reset = bool(anthropic_configured and not anthropic_key)
        anthropic_activated = bool(anthropic_configured and anthropic_key and _is_anthropic_key_activated(conn))
        anthropic_masked = _mask_secret(anthropic_key) if anthropic_key else ("configured" if anthropic_needs_reset else "")
        ngrok_token, ngrok_configured = _read_stored_ngrok_authtoken(conn)
        ngrok_needs_reset = bool(ngrok_configured and not ngrok_token)
        ngrok_masked = _mask_secret(ngrok_token) if ngrok_token else ("configured" if ngrok_needs_reset else "")
        mcp_auth_mode = _normalize_mcp_auth_mode(db.get_meta(conn, _SETTINGS_MCP_AUTH_MODE_KEY, "none") or "none")
        tool_logging_enabled_raw = (db.get_meta(conn, _SETTINGS_TOOL_LOGGING_ENABLED_KEY, "1") or "1").strip().lower()
        tool_logging_enabled = tool_logging_enabled_raw in ("1", "true", "yes", "on")
        mcp_api_key, mcp_api_key_configured = _read_stored_mcp_api_key(conn)
        mcp_api_key_needs_reset = bool(mcp_api_key_configured and not mcp_api_key)
        mcp_api_key_masked = (
            _mask_secret(mcp_api_key)
            if mcp_api_key
            else ("configured" if mcp_api_key_needs_reset else "")
        )
        return {
            "theme": theme,
            "model": model,
            "models": models,
            "anthropic_api_key_set": anthropic_configured,
            "anthropic_api_key_masked": anthropic_masked,
            "anthropic_api_key_needs_reset": anthropic_needs_reset,
            "anthropic_api_key_activated": anthropic_activated,
            "ngrok_authtoken_set": ngrok_configured,
            "ngrok_authtoken_masked": ngrok_masked,
            "ngrok_authtoken_needs_reset": ngrok_needs_reset,
            "mcp_auth_mode": mcp_auth_mode,
            "tool_logging_enabled": tool_logging_enabled,
            "mcp_api_key_header_name": _MCP_API_KEY_HEADER_NAME,
            "mcp_api_key_set": mcp_api_key_configured,
            "mcp_api_key_masked": mcp_api_key_masked,
            "mcp_api_key_needs_reset": mcp_api_key_needs_reset,
            "claude_instructions": claude_instructions,
        }
    finally:
        conn.close()


def _validate_service_start_folders(service_name: str) -> None:
    if service_name not in ("mcp_server", "mcp_inspector"):
        return

    folders = _get_saved_folders()
    source = folders["source_folder"]
    target = folders["target_folder"]
    report = folders["report_folder"]
    if not source or not target or not report:
        raise RuntimeError(
            "Select source, target, and report folders first in Catalog before starting MCP server or inspector."
        )

    invalid: List[str] = []
    if not os.path.isdir(source):
        invalid.append(f"source: {source}")
    if not os.path.isdir(target):
        invalid.append(f"target: {target}")
    if not os.path.isdir(report):
        invalid.append(f"report: {report}")
    if invalid:
        raise RuntimeError("Configured folders do not exist: " + "; ".join(invalid))


def _validate_relationship_payload(conn, payload: RelationshipUpsertRequest) -> tuple[List[str], List[str], str]:
    requested_side = (payload.side or "").strip().lower()
    allowed_requested = set(db.RELATIONSHIP_SIDES) | {"any", "all", "mixed"}
    if requested_side and requested_side not in allowed_requested:
        raise HTTPException(
            status_code=400,
            detail="side must be one of: source, target, configurations, translations, rules, cross, any.",
        )

    left = db.get_dataset(conn, payload.left_dataset)
    right = db.get_dataset(conn, payload.right_dataset)
    if not left:
        raise HTTPException(status_code=404, detail=f"Left dataset '{payload.left_dataset}' not found.")
    if not right:
        raise HTTPException(status_code=404, detail=f"Right dataset '{payload.right_dataset}' not found.")
    natural_side = left["side"] if left["side"] == right["side"] else "cross"

    if requested_side in {"source", "target", "configurations", "translations", "rules"}:
        if left["side"] != requested_side or right["side"] != requested_side:
            raise HTTPException(status_code=400, detail="Both datasets must belong to the selected side.")
        resolved_side = requested_side
    elif requested_side == "cross":
        resolved_side = natural_side
    else:
        resolved_side = natural_side

    left_fields = [f.strip() for f in (payload.left_fields or []) if f and f.strip()]
    right_fields = [f.strip() for f in (payload.right_fields or []) if f and f.strip()]
    if not left_fields and payload.left_field.strip():
        left_fields = [payload.left_field.strip()]
    if not right_fields and payload.right_field.strip():
        right_fields = [payload.right_field.strip()]
    if not left_fields or not right_fields:
        raise HTTPException(status_code=400, detail="At least one left and right field are required.")
    if len(left_fields) != len(right_fields):
        raise HTTPException(status_code=400, detail="left_fields and right_fields must have the same length.")

    for fld in left_fields:
        if fld not in left["columns"]:
            raise HTTPException(
                status_code=400,
                detail=f"Field '{fld}' not found in dataset '{payload.left_dataset}'.",
            )
    for fld in right_fields:
        if fld not in right["columns"]:
            raise HTTPException(
                status_code=400,
                detail=f"Field '{fld}' not found in dataset '{payload.right_dataset}'.",
            )
    return left_fields, right_fields, resolved_side


app = FastAPI(title="ProtoQuery Admin UI", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_STATIC_DIR = Path(__file__).resolve().parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/system/services")
def list_system_services() -> Dict[str, Any]:
    services = {name: _service_snapshot(name) for name in _DESKTOP_SERVICES}
    return {
        "desktop_mode": _desktop_mode_enabled(),
        "services": services,
        "ui": {
            "running": True,
            "host": os.getenv("UI_HOST", "127.0.0.1"),
            "port": os.getenv("UI_PORT", "8001"),
        },
    }


@app.post("/api/system/services/{service_name}/start")
def start_system_service(service_name: str) -> Dict[str, Any]:
    if not _desktop_mode_enabled():
        raise HTTPException(status_code=400, detail="Service controls are available only in desktop mode.")
    if service_name not in _DESKTOP_SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service '{service_name}'.")
    try:
        _validate_service_start_folders(service_name)
    except RuntimeError as exc:
        _SERVICE_ERRORS[service_name] = str(exc)
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        return {"service": _start_service(service_name)}
    except Exception as exc:
        _SERVICE_ERRORS[service_name] = str(exc)
        raise HTTPException(status_code=500, detail=f"Failed to start {service_name}: {exc}")


@app.post("/api/system/services/{service_name}/stop")
def stop_system_service(service_name: str) -> Dict[str, Any]:
    if not _desktop_mode_enabled():
        raise HTTPException(status_code=400, detail="Service controls are available only in desktop mode.")
    if service_name not in _DESKTOP_SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service '{service_name}'.")
    try:
        return {"service": _stop_service(service_name)}
    except Exception as exc:
        _SERVICE_ERRORS[service_name] = str(exc)
        raise HTTPException(status_code=500, detail=f"Failed to stop {service_name}: {exc}")


@app.post("/api/system/services/{service_name}/force-stop")
def force_stop_system_service(service_name: str) -> Dict[str, Any]:
    if not _desktop_mode_enabled():
        raise HTTPException(status_code=400, detail="Service controls are available only in desktop mode.")
    if service_name not in _DESKTOP_SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service '{service_name}'.")
    if service_name not in ("mcp_server", "mcp_inspector", "ngrok"):
        raise HTTPException(status_code=400, detail=f"Force stop is not supported for '{service_name}'.")
    try:
        return _force_stop_service(service_name)
    except Exception as exc:
        _SERVICE_ERRORS[service_name] = str(exc)
        raise HTTPException(status_code=500, detail=f"Failed to force stop {service_name}: {exc}")


@app.get("/api/tool-logs")
def list_tool_logs(
    limit: int = 200,
    offset: int = 0,
    status: str = "all",
    tool_name: str = "",
    contains: str = "",
    since_days: int = 0,
) -> Dict[str, Any]:
    normalized_status = (status or "all").strip().lower()
    if normalized_status not in _TOOL_LOG_ALLOWED_STATUSES:
        raise HTTPException(status_code=400, detail="status must be one of: all, ok, error.")

    safe_limit = max(1, min(int(limit), 1000))
    safe_offset = max(0, int(offset))
    safe_since_days = max(0, min(int(since_days), 3650))
    safe_tool_name = (tool_name or "").strip()
    safe_contains = _tool_log_truncate((contains or "").strip(), _TOOL_LOG_MAX_QUERY_CHARS)
    called_since = ""
    if safe_since_days > 0:
        called_since = (datetime.now(timezone.utc) - timedelta(days=safe_since_days)).isoformat()

    conn = db.get_connection()
    try:
        rows, total = db.list_tool_call_logs(
            conn,
            limit=safe_limit,
            offset=safe_offset,
            status=None if normalized_status == "all" else normalized_status,
            tool_name=safe_tool_name,
            contains=safe_contains,
            called_since=called_since or None,
        )
        tool_names = db.list_tool_call_log_names(conn)
    finally:
        conn.close()

    return {
        "items": rows,
        "total": total,
        "limit": safe_limit,
        "offset": safe_offset,
        "tool_names": tool_names,
        "filters": {
            "status": normalized_status,
            "tool_name": safe_tool_name,
            "contains": safe_contains,
            "since_days": safe_since_days,
        },
    }


@app.delete("/api/tool-logs")
def clear_all_tool_logs() -> Dict[str, Any]:
    conn = db.get_connection()
    try:
        deleted = db.delete_tool_call_logs(conn)
    finally:
        conn.close()
    return {"deleted": deleted}


@app.post("/api/tool-logs/cleanup-older-than")
def clear_tool_logs_older_than(req: ToolLogCleanupOlderThanRequest) -> Dict[str, Any]:
    days = max(1, min(int(req.days), 3650))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn = db.get_connection()
    try:
        deleted = db.delete_tool_call_logs_older_than(conn, cutoff)
    finally:
        conn.close()
    return {"deleted": deleted, "older_than_days": days, "cutoff": cutoff}


@app.get("/api/settings/folders")
def get_folders() -> Dict[str, Any]:
    return _get_saved_folders()


@app.post("/api/settings/folders")
def save_folders(req: SaveFoldersRequest) -> Dict[str, Any]:
    source = (req.source_folder or "").strip()
    target = (req.target_folder or "").strip()
    configurations = (req.configurations_folder or "").strip()
    translations = (req.translations_folder or "").strip()
    rules = (req.rules_folder or "").strip()
    report = (req.report_folder or "").strip()
    expose_source = bool(req.expose_source_to_tools)
    expose_target = bool(req.expose_target_to_tools)
    expose_configurations = bool(req.expose_configurations_to_tools)
    expose_translations = bool(req.expose_translations_to_tools)
    expose_rules = bool(req.expose_rules_to_tools)
    conn = db.get_connection()
    try:
        db.set_meta(conn, "source_folder", source, commit=False)
        db.set_meta(conn, "target_folder", target, commit=False)
        db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["configurations"], configurations, commit=False)
        db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["translations"], translations, commit=False)
        db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["rules"], rules, commit=False)
        db.set_meta(conn, "report_folder", report, commit=False)
        _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["source"], expose_source)
        _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["target"], expose_target)
        _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["configurations"], expose_configurations)
        _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["translations"], expose_translations)
        _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["rules"], expose_rules)
        configs = _load_folder_configs_from_conn(conn)
        matching_id = _find_matching_folder_config_id(
            configs,
            source,
            target,
            configurations,
            translations,
            rules,
            report,
            expose_source,
            expose_target,
            expose_configurations,
            expose_translations,
            expose_rules,
        )
        _save_folder_configs_to_conn(conn, configs, matching_id)
        return _get_saved_folders_from_conn(conn)
    finally:
        conn.close()


@app.get("/api/settings/folder-configs")
def list_folder_configs() -> Dict[str, Any]:
    conn = db.get_connection()
    try:
        return _folder_configs_payload(conn)
    finally:
        conn.close()


@app.post("/api/settings/folder-configs")
def save_folder_config(req: SaveFolderConfigRequest) -> Dict[str, Any]:
    name = _normalize_folder_config_name(req.name)
    source = (req.source_folder or "").strip()
    target = (req.target_folder or "").strip()
    configurations = (req.configurations_folder or "").strip()
    translations = (req.translations_folder or "").strip()
    rules = (req.rules_folder or "").strip()
    report = (req.report_folder or "").strip()
    expose_source = bool(req.expose_source_to_tools)
    expose_target = bool(req.expose_target_to_tools)
    expose_configurations = bool(req.expose_configurations_to_tools)
    expose_translations = bool(req.expose_translations_to_tools)
    expose_rules = bool(req.expose_rules_to_tools)
    set_active = bool(req.set_active)

    conn = db.get_connection()
    try:
        now = _iso_utc_now()
        configs = _load_folder_configs_from_conn(conn)
        existing = next((cfg for cfg in configs if cfg["name"].casefold() == name.casefold()), None)
        created = existing is None
        if existing:
            existing["name"] = name
            existing["source_folder"] = source
            existing["target_folder"] = target
            existing["configurations_folder"] = configurations
            existing["translations_folder"] = translations
            existing["rules_folder"] = rules
            existing["report_folder"] = report
            existing["expose_source_to_tools"] = expose_source
            existing["expose_target_to_tools"] = expose_target
            existing["expose_configurations_to_tools"] = expose_configurations
            existing["expose_translations_to_tools"] = expose_translations
            existing["expose_rules_to_tools"] = expose_rules
            existing["updated_at"] = now
            saved_id = existing["id"]
        else:
            saved_id = f"cfg_{secrets.token_hex(6)}"
            configs.append(
                {
                    "id": saved_id,
                    "name": name,
                    "source_folder": source,
                    "target_folder": target,
                    "configurations_folder": configurations,
                    "translations_folder": translations,
                    "rules_folder": rules,
                    "report_folder": report,
                    "expose_source_to_tools": expose_source,
                    "expose_target_to_tools": expose_target,
                    "expose_configurations_to_tools": expose_configurations,
                    "expose_translations_to_tools": expose_translations,
                    "expose_rules_to_tools": expose_rules,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        configs.sort(key=lambda c: (c["name"].lower(), c["id"]))

        active_id = (db.get_meta(conn, _SETTINGS_ACTIVE_FOLDER_CONFIG_KEY, "") or "").strip()
        if set_active:
            db.set_meta(conn, "source_folder", source, commit=False)
            db.set_meta(conn, "target_folder", target, commit=False)
            db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["configurations"], configurations, commit=False)
            db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["translations"], translations, commit=False)
            db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["rules"], rules, commit=False)
            db.set_meta(conn, "report_folder", report, commit=False)
            _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["source"], expose_source)
            _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["target"], expose_target)
            _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["configurations"], expose_configurations)
            _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["translations"], expose_translations)
            _save_meta_bool(conn, _EXPOSE_TOOLS_META_BY_SIDE["rules"], expose_rules)
            active_id = saved_id
        active_id = _save_folder_configs_to_conn(conn, configs, active_id)

        return {
            "configs": configs,
            "active_id": active_id,
            "saved_id": saved_id,
            "saved_name": name,
            "created": created,
            "folders": _get_saved_folders_from_conn(conn),
        }
    finally:
        conn.close()


@app.post("/api/settings/folder-configs/{config_id}/apply")
def apply_folder_config(config_id: str) -> Dict[str, Any]:
    config_id = (config_id or "").strip()
    conn = db.get_connection()
    try:
        configs = _load_folder_configs_from_conn(conn)
        selected = next((cfg for cfg in configs if cfg["id"] == config_id), None)
        if not selected:
            raise HTTPException(status_code=404, detail=f"Folder configuration '{config_id}' not found.")

        db.set_meta(conn, "source_folder", selected["source_folder"], commit=False)
        db.set_meta(conn, "target_folder", selected["target_folder"], commit=False)
        db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["configurations"], selected.get("configurations_folder", ""), commit=False)
        db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["translations"], selected.get("translations_folder", ""), commit=False)
        db.set_meta(conn, _EXTRA_FOLDER_META_BY_SIDE["rules"], selected.get("rules_folder", ""), commit=False)
        db.set_meta(conn, "report_folder", selected["report_folder"], commit=False)
        _save_meta_bool(
            conn,
            _EXPOSE_TOOLS_META_BY_SIDE["source"],
            bool(selected.get("expose_source_to_tools", True)),
        )
        _save_meta_bool(
            conn,
            _EXPOSE_TOOLS_META_BY_SIDE["target"],
            bool(selected.get("expose_target_to_tools", True)),
        )
        _save_meta_bool(
            conn,
            _EXPOSE_TOOLS_META_BY_SIDE["configurations"],
            bool(selected.get("expose_configurations_to_tools", False)),
        )
        _save_meta_bool(
            conn,
            _EXPOSE_TOOLS_META_BY_SIDE["translations"],
            bool(selected.get("expose_translations_to_tools", False)),
        )
        _save_meta_bool(
            conn,
            _EXPOSE_TOOLS_META_BY_SIDE["rules"],
            bool(selected.get("expose_rules_to_tools", False)),
        )
        active_id = _save_folder_configs_to_conn(conn, configs, selected["id"])
        return {
            "configs": configs,
            "active_id": active_id,
            "applied_id": selected["id"],
            "applied_name": selected["name"],
            "folders": _get_saved_folders_from_conn(conn),
        }
    finally:
        conn.close()


@app.delete("/api/settings/folder-configs/{config_id}")
def delete_folder_config(config_id: str) -> Dict[str, Any]:
    config_id = (config_id or "").strip()
    conn = db.get_connection()
    try:
        configs = _load_folder_configs_from_conn(conn)
        selected = next((cfg for cfg in configs if cfg["id"] == config_id), None)
        if not selected:
            raise HTTPException(status_code=404, detail=f"Folder configuration '{config_id}' not found.")
        configs = [cfg for cfg in configs if cfg["id"] != config_id]
        active_id = (db.get_meta(conn, _SETTINGS_ACTIVE_FOLDER_CONFIG_KEY, "") or "").strip()
        next_active = _save_folder_configs_to_conn(conn, configs, active_id)
        return {
            "configs": configs,
            "active_id": next_active,
            "deleted_id": selected["id"],
            "deleted_name": selected["name"],
        }
    finally:
        conn.close()


@app.get("/api/settings/app")
def get_app_settings() -> Dict[str, Any]:
    return _get_saved_app_settings()


@app.post("/api/settings/app")
def save_app_settings(req: SaveAppSettingsRequest) -> Dict[str, Any]:
    theme = _normalize_theme(req.theme)
    mcp_auth_mode = _normalize_mcp_auth_mode(req.mcp_auth_mode)
    model = (req.model or "").strip()
    claude_instructions = (req.claude_instructions or "").strip()
    api_key_input = req.anthropic_api_key
    ngrok_authtoken_input = req.ngrok_authtoken

    conn = db.get_connection()
    try:
        if req.tool_logging_enabled is None:
            tool_logging_raw = (db.get_meta(conn, _SETTINGS_TOOL_LOGGING_ENABLED_KEY, "1") or "1").strip().lower()
            tool_logging_enabled = tool_logging_raw in ("1", "true", "yes", "on")
        else:
            tool_logging_enabled = bool(req.tool_logging_enabled)
        db.set_meta(conn, _SETTINGS_THEME_KEY, theme, commit=False)
        db.set_meta(conn, _SETTINGS_MCP_AUTH_MODE_KEY, mcp_auth_mode, commit=False)
        db.set_meta(conn, _SETTINGS_TOOL_LOGGING_ENABLED_KEY, "1" if tool_logging_enabled else "0", commit=False)
        db.set_meta(conn, _SETTINGS_ANTHROPIC_MODEL_KEY, model, commit=False)
        db.set_meta(conn, _SETTINGS_CLAUDE_INSTRUCTIONS_KEY, claude_instructions, commit=False)
        if api_key_input is not None:
            encrypted = _encrypt_secret((api_key_input or "").strip())
            db.set_meta(conn, _SETTINGS_ANTHROPIC_API_KEY, encrypted, commit=False)
            _set_anthropic_key_activated(conn, False)
        if ngrok_authtoken_input is not None:
            encrypted_token = _encrypt_secret((ngrok_authtoken_input or "").strip())
            db.set_meta(conn, _SETTINGS_NGROK_AUTHTOKEN_KEY, encrypted_token, commit=False)
    finally:
        conn.close()

    return _get_saved_app_settings()


@app.post("/api/settings/mcp-auth/generate")
def generate_mcp_api_key() -> Dict[str, Any]:
    generated_key = _generate_mcp_api_key()
    conn = db.get_connection()
    try:
        # Generating a key implies API-key auth intent, so persist mode to avoid UI/state reversion.
        db.set_meta(conn, _SETTINGS_MCP_AUTH_MODE_KEY, "api", commit=False)
        db.set_meta(conn, _SETTINGS_MCP_API_KEY_KEY, _encrypt_secret(generated_key), commit=False)
    finally:
        conn.close()

    return {
        "api_key": generated_key,
        "header_name": _MCP_API_KEY_HEADER_NAME,
        "app_settings": _get_saved_app_settings(),
    }


@app.api_route("/api/inspector/mcp", methods=["GET", "POST", "DELETE"])
@app.api_route("/api/inspector/mcp/{subpath:path}", methods=["GET", "POST", "DELETE"])
async def inspector_mcp_relay(request: Request, subpath: str = "") -> StreamingResponse:
    upstream_base = _mcp_server_url().rstrip("/")
    suffix = f"/{subpath.lstrip('/')}" if subpath else ""
    query = request.url.query or ""
    upstream_url = f"{upstream_base}{suffix}"
    if query:
        upstream_url = f"{upstream_url}?{query}"

    forward_headers = _filtered_proxy_request_headers(request)

    conn = db.get_connection()
    try:
        auth_cfg = _mcp_auth_runtime_config(conn)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if auth_cfg.get("mode") == "api":
        header_name = auth_cfg.get("header_name", _MCP_API_KEY_HEADER_NAME)
        api_key = auth_cfg.get("api_key", "")
        if not api_key:
            raise HTTPException(status_code=500, detail="MCP API key is missing for inspector relay.")
        forward_headers[header_name] = api_key
    forward_headers["x-protoquery-tool-log-source"] = "inspector"

    body = await request.body()

    client = httpx.AsyncClient(timeout=None)
    outbound = client.build_request(
        request.method.upper(),
        upstream_url,
        headers=forward_headers,
        content=body,
    )

    try:
        upstream = await client.send(outbound, stream=True)
    except Exception as exc:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Inspector relay failed: {exc}")

    relay_headers = _filtered_proxy_response_headers(upstream.headers)

    async def _stream() -> Any:
        async for chunk in upstream.aiter_raw():
            yield chunk

    async def _cleanup() -> None:
        await upstream.aclose()
        await client.aclose()

    return StreamingResponse(
        _stream(),
        status_code=upstream.status_code,
        headers=relay_headers,
        media_type=upstream.headers.get("content-type"),
        background=BackgroundTask(_cleanup),
    )


@app.post("/api/settings/anthropic/validate")
def validate_anthropic_key(req: ValidateAnthropicKeyRequest) -> Dict[str, Any]:
    key = (req.api_key or "").strip()
    stored_key = ""
    should_activate = False
    conn = db.get_connection()
    try:
        stored_key, _ = _read_stored_anthropic_key(conn)
        if key:
            effective_key = key
            should_activate = bool(stored_key and stored_key == key)
        else:
            effective_key = _require_stored_anthropic_key(conn)
            should_activate = True
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

    if not effective_key:
        raise HTTPException(status_code=400, detail="No Anthropic API key provided or stored.")

    try:
        models = _list_anthropic_models(effective_key, limit=1)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if should_activate:
        conn = db.get_connection()
        try:
            _set_anthropic_key_activated(conn, True)
        finally:
            conn.close()

    activated = bool(should_activate)
    return {
        "valid": True,
        "message": (
            "Anthropic API key is valid and activated."
            if activated
            else "Anthropic API key is valid. Save this key first, then validate again to activate."
        ),
        "activated": activated,
        "sample_model": models[0]["id"] if models else None,
        "app_settings": _get_saved_app_settings(),
    }


@app.post("/api/settings/anthropic/models")
def lookup_anthropic_models(req: LookupAnthropicModelsRequest) -> Dict[str, Any]:
    provided_key = (req.api_key or "").strip()
    selected_model = ""
    conn = db.get_connection()
    try:
        selected_model = (db.get_meta(conn, _SETTINGS_ANTHROPIC_MODEL_KEY, "") or "").strip()
        if provided_key:
            effective_key = provided_key
        else:
            effective_key = _require_stored_anthropic_key(conn)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

    if not effective_key:
        raise HTTPException(status_code=400, detail="No Anthropic API key provided or stored.")

    try:
        models = _list_anthropic_models(effective_key, limit=100)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    conn = db.get_connection()
    try:
        _save_cached_models(conn, models)
    finally:
        conn.close()

    return {"models": models, "selected_model": selected_model}


@app.post("/api/claude/chat")
def claude_chat(req: ClaudeChatRequest) -> Dict[str, Any]:
    user_message = (req.message or "").strip()
    if not user_message:
        raise HTTPException(status_code=400, detail="Message is required.")

    conn = db.get_connection()
    try:
        model = (db.get_meta(conn, _SETTINGS_ANTHROPIC_MODEL_KEY, "") or "").strip()
        api_key = _require_stored_anthropic_key(conn)
        activated = _is_anthropic_key_activated(conn)
        claude_instructions = (db.get_meta(conn, _SETTINGS_CLAUDE_INSTRUCTIONS_KEY, "") or "").strip()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

    if not api_key:
        raise HTTPException(status_code=400, detail="No stored Anthropic API key found. Save one in Settings.")
    if not activated:
        raise HTTPException(status_code=400, detail="Anthropic API key must be validated before using Claude chat.")
    if not model:
        raise HTTPException(status_code=400, detail="No Anthropic model selected. Choose one in Settings.")

    history = _prepare_claude_history(req.history)
    messages: List[Dict[str, Any]] = [*history, {"role": "user", "content": user_message}]
    tools = _get_anthropic_tools()

    try:
        client = Anthropic(api_key=api_key, timeout=120.0)

        for _round in range(_MAX_TOOL_ROUNDS):
            request_payload: Dict[str, Any] = {
                "model": model,
                "max_tokens": 8096,
                "messages": messages,
            }
            if claude_instructions:
                request_payload["system"] = claude_instructions
            if tools:
                request_payload["tools"] = tools

            response = client.messages.create(**request_payload)

            # Append the full assistant message (text + tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason != "tool_use":
                break

            # Execute every tool_use block and build the tool results
            tool_results: List[Dict[str, Any]] = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                tool_name = str(getattr(block, "name", "") or "").strip()
                arguments = getattr(block, "input", {})
                if not isinstance(arguments, dict):
                    arguments = {"value": arguments}

                _log.info("Calling MCP tool: %s(%s)", tool_name, arguments)
                result_text = _call_mcp_tool(tool_name, arguments)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_text,
                })

            messages.append({"role": "user", "content": tool_results})

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Claude chat failed: {exc}")

    assistant_text = _text_from_anthropic_message(response)
    if not assistant_text:
        assistant_text = "(Tools executed but Claude returned no text summary.)"

    return {
        "message": {"role": "assistant", "content": assistant_text},
        "model": model,
    }


@app.get("/api/system/browse-folder")
def browse_folder(initial: Optional[str] = None) -> Dict[str, str]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Folder picker is unavailable: {exc}")

    initial_dir = initial if initial and os.path.isdir(initial) else str(Path.home())
    root = None
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        selected = filedialog.askdirectory(initialdir=initial_dir, mustexist=True) or ""
        return {"folder": selected}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to open folder picker: {exc}")
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:
                pass


@app.post("/api/catalog/refresh")
def refresh_catalog(req: RefreshCatalogRequest) -> Dict[str, Any]:
    conn = db.get_connection()
    try:
        if req.report_folder is not None:
            db.set_meta(conn, "report_folder", (req.report_folder or "").strip(), commit=False)
        result = cat.refresh_catalog(
            source_folder=req.source_folder,
            target_folder=req.target_folder,
            configurations_folder=req.configurations_folder,
            translations_folder=req.translations_folder,
            rules_folder=req.rules_folder,
            include_row_counts=req.include_row_counts,
            conn=conn,
        )
        saved_folders = _get_saved_folders_from_conn(conn)
        configs = _load_folder_configs_from_conn(conn)
        matching_id = _find_matching_folder_config_id(
            configs,
            saved_folders["source_folder"],
            saved_folders["target_folder"],
            saved_folders["configurations_folder"],
            saved_folders["translations_folder"],
            saved_folders["rules_folder"],
            saved_folders["report_folder"],
            bool(saved_folders["expose_source_to_tools"]),
            bool(saved_folders["expose_target_to_tools"]),
            bool(saved_folders["expose_configurations_to_tools"]),
            bool(saved_folders["expose_translations_to_tools"]),
            bool(saved_folders["expose_rules_to_tools"]),
        )
        _save_folder_configs_to_conn(conn, configs, matching_id)
        return result
    finally:
        conn.close()


@app.get("/api/datasets")
def list_datasets(side: Optional[str] = None, filter: Optional[str] = None) -> List[Dict[str, Any]]:
    return cat.get_datasets(side=side, filter_text=filter)


@app.get("/api/datasets/{dataset_id}")
def get_dataset(dataset_id: str) -> Dict[str, Any]:
    ds = cat.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")
    return ds


@app.get("/api/datasets/{dataset_id}/fields")
def get_fields(dataset_id: str) -> Dict[str, Any]:
    ds = cat.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")
    return {"dataset": dataset_id, "columns": ds["columns"], "column_count": len(ds["columns"])}


@app.get("/api/datasets/{dataset_id}/preview")
def preview_dataset(
    dataset_id: str,
    limit: int = 10,
    offset: int = 0,
    fields: Optional[str] = None,
) -> Dict[str, Any]:
    ds = cat.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")

    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    field_list = [f.strip() for f in fields.split(",")] if fields else None

    with connect([ds]) as duck:
        view = quote(dataset_id)
        sel = "*"
        selected_fields: Optional[List[str]] = None
        if field_list:
            selected_fields = [f for f in field_list if f in ds["columns"]]
            if selected_fields:
                sel = ", ".join(quote(f) for f in selected_fields)

        rows_cur = duck.execute(
            f"SELECT {sel} FROM {view} LIMIT {limit} OFFSET {offset}"
        )
        headers = [d[0] for d in rows_cur.description]
        rows = [list(r) for r in rows_cur.fetchall()]
        total = duck.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]

    return {
        "dataset": dataset_id,
        "headers": headers,
        "rows": rows,
        "total_rows": total,
        "limit": limit,
        "offset": offset,
        "selected_fields": selected_fields,
    }


@app.post("/api/sql/preview")
def sql_preview(req: SqlPreviewRequest) -> Dict[str, Any]:
    ok, err = sql_validate(req.sql)
    if not ok:
        raise HTTPException(status_code=400, detail=err)

    datasets = _datasets_or_404()
    clean = req.sql.strip().rstrip(";")
    sql_to_run = clean
    if "LIMIT" not in clean.upper():
        sql_to_run = f"{clean} LIMIT {req.limit}"

    with connect(datasets) as duck:
        total = None
        if req.include_total:
            try:
                total = duck.execute(f"SELECT COUNT(*) FROM ({clean}) _q").fetchone()[0]
            except Exception:
                total = None
        result = duck.execute(sql_to_run)
        headers = [d[0] for d in result.description]
        rows = [list(r) for r in result.fetchall()]

    return {
        "headers": headers,
        "rows": rows,
        "row_count": len(rows),
        "total_rows": total if total is not None else len(rows),
        "total_computed": total is not None,
        "limit_applied": req.limit,
    }


@app.post("/api/sql/export")
def sql_export(req: SqlExportRequest) -> Dict[str, Any]:
    if req.async_job:
        result = _format_export_job_start(
            job_svc.start_export_query_job(
                sql=req.sql,
                filename=req.filename,
            )
        )
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result

    conn = db.get_connection()
    try:
        result = job_svc.start_export_query_job(
            sql=req.sql,
            filename=req.filename,
            conn=conn,
        )
    finally:
        conn.close()
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.get("/api/profile/{dataset_id}")
def data_profile(dataset_id: str) -> Dict[str, Any]:
    return prof.data_profile(dataset_id)


@app.get("/api/summary/column/{dataset_id}")
def column_summary(
    dataset_id: str,
    column: Optional[str] = None,
    top_n: int = 10,
) -> Dict[str, Any]:
    top_n = max(1, min(int(top_n), 100))
    return prof.column_value_summary(dataset_id, column=column, top_n=top_n)


@app.post("/api/summary/combo/{dataset_id}")
def combo_summary(dataset_id: str, req: ComboSummaryRequest) -> Dict[str, Any]:
    return prof.combo_value_summary(dataset_id, req.columns, top_n=req.top_n)


@app.post("/api/preview/filtered/{dataset_id}")
def filtered_preview(dataset_id: str, req: FilteredPreviewRequest) -> Dict[str, Any]:
    return prof.preview_filtered_records(dataset_id, req.filter_spec, limit=req.limit)


@app.get("/api/duplicates/{dataset_id}")
def duplicates(dataset_id: str, key_fields: str, limit: int = 10) -> Dict[str, Any]:
    keys = [k.strip() for k in key_fields.split(",") if k.strip()]
    return prof.find_duplicates(dataset_id, key_columns=keys, limit=limit)


@app.get("/api/value-distribution/{dataset_id}")
def distribution(dataset_id: str, column: str, limit: int = 20) -> Dict[str, Any]:
    return prof.value_distribution(dataset_id, column=column, limit=limit)


@app.get("/api/pairs")
def list_pairs() -> List[Dict[str, Any]]:
    return cat.get_pairs()


@app.post("/api/pairs/override")
def upsert_pair_override(req: PairOverrideRequest) -> Dict[str, Any]:
    return cat.upsert_pair_override(
        source_id=req.source_dataset_id,
        target_id=req.target_dataset_id,
        enabled=req.enabled,
        key_mappings=_clean_field_mappings(req.key_mappings, preserve_metadata=True),
        compare_mappings=_clean_field_mappings(req.compare_mappings, preserve_metadata=True),
    )


@app.get("/api/pairs/resolve")
def resolve_pair(source_dataset_id: str, target_dataset_id: str) -> Dict[str, Any]:
    pair = cat.get_pair_by_datasets(source_dataset_id, target_dataset_id)
    return {"pair": pair}


@app.get("/api/pairs/quick-map")
def quick_map_pair(
    source_dataset_id: str,
    target_dataset_id: str,
    mode: str = "name",
    min_confidence: float = 0.75,
    max_mappings: int = 200,
) -> Dict[str, Any]:
    result = cat.suggest_field_mappings(
        source_id=source_dataset_id,
        target_id=target_dataset_id,
        mode=mode,
        min_confidence=min_confidence,
        max_mappings=max_mappings,
    )
    if "error" in result:
        message = str(result["error"])
        status_code = 400
        if "not found" in message.lower():
            status_code = 404
        raise HTTPException(status_code=status_code, detail=message)
    return result


@app.get("/api/pairs/{pair_id}/suggest-keys")
def suggest_keys(pair_id: str) -> Dict[str, Any]:
    return prof.suggest_keys(pair_id)


@app.delete("/api/pairs/{pair_id}/key-mappings")
def delete_pair_key_mappings(pair_id: str) -> Dict[str, Any]:
    result = cat.clear_pair_key_mappings(pair_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=str(result["error"]))
    return result


@app.delete("/api/pairs/{pair_id}")
def delete_pair(pair_id: str) -> Dict[str, Any]:
    result = cat.delete_pair(pair_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=str(result["error"]))
    return result


@app.get("/api/pairs/{pair_id}/key-presets")
def list_key_presets(pair_id: str) -> List[Dict[str, Any]]:
    conn = db.get_connection()
    presets = db.list_key_presets(conn, pair_id)
    conn.close()
    return presets


@app.post("/api/pairs/{pair_id}/key-presets")
def save_key_preset(pair_id: str, req: SaveKeyPresetRequest) -> Dict[str, Any]:
    fields = [f.strip() for f in req.key_fields if f.strip()]
    if not fields:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    conn = db.get_connection()
    preset_id = db.save_key_preset(conn, pair_id, req.name, fields)
    conn.close()
    return {"preset_id": preset_id, "pair_id": pair_id, "name": req.name, "key_fields": fields}


@app.get("/api/relationships")
def list_relationships(
    side: Optional[str] = None,
    dataset_id: Optional[str] = None,
    active_only: bool = False,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    side_filter = (side or "").strip().lower() or None
    if side_filter in ("any", "all"):
        side_filter = None
    conn = db.get_connection()
    rows = db.list_relationships(
        conn,
        side=side_filter,
        dataset_id=dataset_id,
        active_only=active_only,
        limit=limit,
    )
    conn.close()
    return rows


@app.post("/api/relationships")
def create_relationship(req: RelationshipUpsertRequest) -> Dict[str, Any]:
    conn = db.get_connection()
    left_fields, right_fields, resolved_side = _validate_relationship_payload(conn, req)
    row = db.upsert_relationship(
        conn,
        side=resolved_side,
        left_dataset=req.left_dataset,
        left_field=left_fields[0],
        left_fields=left_fields,
        right_dataset=req.right_dataset,
        right_field=right_fields[0],
        right_fields=right_fields,
        confidence=req.confidence,
        method=req.method.strip() or "manual",
        active=req.active,
    )
    conn.close()
    return row


@app.put("/api/relationships/{relationship_id}")
def update_relationship(relationship_id: int, req: RelationshipUpsertRequest) -> Dict[str, Any]:
    conn = db.get_connection()
    if not db.get_relationship(conn, relationship_id):
        conn.close()
        raise HTTPException(status_code=404, detail=f"Relationship '{relationship_id}' not found.")
    left_fields, right_fields, resolved_side = _validate_relationship_payload(conn, req)
    row = db.update_relationship(
        conn,
        relationship_id=relationship_id,
        side=resolved_side,
        left_dataset=req.left_dataset,
        left_field=left_fields[0],
        right_dataset=req.right_dataset,
        right_field=right_fields[0],
        confidence=req.confidence,
        method=req.method.strip() or "manual",
        active=req.active,
        left_fields=left_fields,
        right_fields=right_fields,
    )
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail=f"Relationship '{relationship_id}' not found.")
    return row


@app.delete("/api/relationships/{relationship_id}")
def delete_relationship(relationship_id: int) -> Dict[str, Any]:
    conn = db.get_connection()
    ok = db.delete_relationship(conn, relationship_id)
    conn.close()
    if not ok:
        raise HTTPException(status_code=404, detail=f"Relationship '{relationship_id}' not found.")
    return {"deleted": relationship_id}


@app.post("/api/relationships/link-related")
def link_related_tables(req: RelationshipLinkRequest) -> Dict[str, Any]:
    return rel.link_related_tables(
        side=req.side,
        min_confidence=req.min_confidence,
        suggest_only=req.suggest_only,
    )


@app.post("/api/relationships/auto-link")
def auto_link_relationships(req: RelationshipScopedLinkRequest) -> Dict[str, Any]:
    result = rel.auto_link_scoped_relationships(
        left_side=req.left_side,
        right_side=req.right_side,
        left_dataset=req.left_dataset,
        right_dataset=req.right_dataset,
        mode=req.mode,
        min_confidence=req.min_confidence,
        suggest_only=req.suggest_only,
        max_links=req.max_links,
    )
    if "error" in result:
        message = str(result["error"])
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message)
    return result


@app.get("/api/schema-diff")
def schema_diff(source_dataset_id: str, target_dataset_id: str) -> Dict[str, Any]:
    return cat.schema_diff(source_dataset_id, target_dataset_id)


@app.post("/api/compare/start")
def start_compare(req: StartCompareRequest) -> Dict[str, Any]:
    key_mappings = _clean_field_mappings(req.key_mappings)
    compare_mappings = _clean_field_mappings(req.compare_mappings)
    keys = [k.strip() for k in req.key_fields if k.strip()]
    effective_keys = keys or [m["source_field"] for m in (key_mappings or [])]
    if not key_mappings and not keys:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    compare_fields = [c.strip() for c in (req.compare_fields or []) if c.strip()] or None
    return job_svc.start_comparison_job(
        source_id=req.source_dataset_id,
        target_id=req.target_dataset_id,
        key_columns=effective_keys,
        key_mappings=key_mappings,
        pair_id=req.pair_id,
        compare_columns=compare_fields,
        compare_mappings=compare_mappings,
        options={
            "key_mappings": key_mappings or [],
            "compare_mappings": compare_mappings or [],
        },
    )


@app.get("/api/compare/quick")
def quick_compare(
    source_dataset_id: str,
    target_dataset_id: str,
    key_fields: str,
    compare_fields: Optional[str] = None,
    sample_limit: int = 10,
) -> Dict[str, Any]:
    keys = [k.strip() for k in key_fields.split(",") if k.strip()]
    if not keys:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    comps = [c.strip() for c in compare_fields.split(",")] if compare_fields else None
    return comp.compare_datasets(
        source_id=source_dataset_id,
        target_id=target_dataset_id,
        key_columns=keys,
        compare_columns=comps,
        sample_limit=sample_limit,
    )


@app.post("/api/compare/quick")
def quick_compare_post(req: QuickCompareRequest) -> Dict[str, Any]:
    key_mappings = _clean_field_mappings(req.key_mappings)
    compare_mappings = _clean_field_mappings(req.compare_mappings)
    keys = [k.strip() for k in req.key_fields if k.strip()]
    effective_keys = keys or [m["source_field"] for m in (key_mappings or [])]
    compare_fields = [c.strip() for c in (req.compare_fields or []) if c.strip()] or None
    if not key_mappings and not keys:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    return comp.compare_datasets(
        source_id=req.source_dataset_id,
        target_id=req.target_dataset_id,
        key_columns=effective_keys,
        compare_columns=compare_fields,
        key_mappings=key_mappings,
        compare_mappings=compare_mappings,
        sample_limit=req.sample_limit,
    )


@app.get("/api/jobs")
def list_jobs(limit: int = 50) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    return job_svc.list_jobs(limit=limit)


@app.get("/api/jobs/{job_id}")
def get_job_status(job_id: str) -> Dict[str, Any]:
    result = job_svc.get_job_status(job_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.get("/api/jobs/{job_id}/summary")
def get_job_summary(job_id: str) -> Dict[str, Any]:
    result = job_svc.get_job_summary(job_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> Dict[str, Any]:
    result = job_svc.cancel_job(job_id)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.get("/api/reports")
def list_reports(limit: int = 200) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 5000))
    conn = db.get_connection()
    rows = db.list_reports(conn, limit=limit)
    conn.close()
    return rows


@app.get("/api/reports/{report_id}")
def report_metadata(report_id: str) -> Dict[str, Any]:
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    conn.close()
    if not report:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")
    return report


@app.get("/api/reports/{report_id}/download")
def download_report(report_id: str):
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    conn.close()
    if not report:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")
    path = report["file_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Report file not found: {path}")
    return FileResponse(path, filename=report["file_name"])


@app.post("/api/reports/{report_id}/open")
def open_report(report_id: str) -> Dict[str, Any]:
    if not _desktop_mode_enabled():
        raise HTTPException(status_code=400, detail="Opening report files is available only in desktop mode.")

    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    conn.close()
    if not report:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")

    path = report["file_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Report file not found: {path}")

    try:
        if os.name == "nt":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.Popen(["xdg-open", path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to open report file: {exc}")

    return {"opened": report_id, "file_path": path}


@app.delete("/api/reports/{report_id}")
def delete_report(report_id: str) -> Dict[str, Any]:
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    if not report:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")

    try:
        if os.path.exists(report["file_path"]):
            os.remove(report["file_path"])
    except Exception:
        pass

    db.delete_report(conn, report_id)
    conn.close()
    return {"deleted": report_id}


@app.get("/")
def serve_ui():
    index_path = _STATIC_DIR / "index.html"
    if not index_path.exists():
        return JSONResponse(
            status_code=200,
            content={
                "message": "UI static files not found.",
                "hint": "Create ui/static/index.html and related assets.",
            },
        )
    return FileResponse(str(index_path))
