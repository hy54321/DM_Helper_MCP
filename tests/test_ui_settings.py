import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
import types

from fastapi.testclient import TestClient

from server import db


def _load_ui_api(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "protoquery.db"
    app_dir = tmp_path / "app_data"
    app_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("PROTOQUERY_DB_PATH", str(db_path))
    monkeypatch.setenv("PROTOQUERY_APP_BASE_DIR", str(app_dir))

    import ui.api as ui_api

    return importlib.reload(ui_api), db_path


def _register_demo_tool(ui_api, tool_name: str = "demo_tool"):
    import mcp_server

    manager = ui_api._mcp_instance._tool_manager

    def _fn(limit: int = 1):
        return '{"ok": true, "limit": %d}' % int(limit)

    manager._tools[tool_name] = types.SimpleNamespace(
        name=tool_name,
        description="Demo wrapped tool",
        parameters={"type": "object", "properties": {"limit": {"type": "integer"}}},
        fn=_fn,
    )
    mcp_server._instrument_tool_call_logging()


def test_save_settings_encrypts_anthropic_key(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    raw_key = "sk-ant-test-1234567890"
    raw_ngrok_token = "2abc1234567890-ngrok-token"
    response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": raw_key,
            "ngrok_authtoken": raw_ngrok_token,
            "model": "claude-test-model",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["theme"] == "light"
    assert payload["anthropic_api_key_set"] is True
    assert payload["anthropic_api_key_masked"]
    assert raw_key not in payload["anthropic_api_key_masked"]

    conn = db.get_connection(path=str(db_path))
    encrypted = db.get_meta(conn, "anthropic_api_key_encrypted", "")
    encrypted_ngrok_token = db.get_meta(conn, "ngrok_authtoken_encrypted", "")
    conn.close()

    assert encrypted
    assert encrypted != raw_key
    assert ui_api._decrypt_secret(encrypted) == raw_key
    assert encrypted_ngrok_token
    assert encrypted_ngrok_token != raw_ngrok_token
    assert ui_api._decrypt_secret(encrypted_ngrok_token) == raw_ngrok_token


def test_generate_mcp_api_key_encrypts_and_returns_plaintext_once(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "mcp_auth_mode": "api",
            "model": "",
        },
    )
    assert save_response.status_code == 200

    generate_response = client.post("/api/settings/mcp-auth/generate", json={})
    assert generate_response.status_code == 200
    payload = generate_response.json()

    generated_key = payload["api_key"]
    assert generated_key
    assert payload["header_name"] == "x-api-key"
    assert payload["app_settings"]["mcp_api_key_set"] is True
    assert payload["app_settings"]["mcp_auth_mode"] == "api"

    conn = db.get_connection(path=str(db_path))
    encrypted_key = db.get_meta(conn, "mcp_api_key_encrypted", "")
    conn.close()

    assert encrypted_key
    assert encrypted_key != generated_key
    assert ui_api._decrypt_secret(encrypted_key) == generated_key


def test_generate_mcp_api_key_forces_api_mode(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "mcp_auth_mode": "none",
            "model": "",
        },
    )
    assert save_response.status_code == 200
    assert save_response.json()["mcp_auth_mode"] == "none"

    generate_response = client.post("/api/settings/mcp-auth/generate", json={})
    assert generate_response.status_code == 200
    payload = generate_response.json()
    assert payload["app_settings"]["mcp_auth_mode"] == "api"


def test_service_command_includes_mcp_api_auth_env(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "mcp_auth_mode": "api",
            "model": "",
        },
    )
    assert save_response.status_code == 200

    generate_response = client.post("/api/settings/mcp-auth/generate", json={})
    assert generate_response.status_code == 200

    conn = db.get_connection()
    try:
        runtime_auth = ui_api._mcp_auth_runtime_config(conn)
    finally:
        conn.close()

    _command, env_overrides = ui_api._service_command("mcp_server", mcp_auth=runtime_auth)
    assert env_overrides["PROTOQUERY_MCP_AUTH_MODE"] == "api"
    assert env_overrides["PROTOQUERY_MCP_API_KEY"] == runtime_auth["api_key"]
    assert env_overrides["PROTOQUERY_MCP_API_KEY_HEADER"] == "x-api-key"


def test_service_command_rejects_api_mode_without_key(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)

    try:
        ui_api._service_command(
            "mcp_server",
            mcp_auth={"mode": "api", "header_name": "x-api-key", "api_key": ""},
        )
        assert False, "Expected RuntimeError for missing API key"
    except RuntimeError as exc:
        assert "no API key is stored" in str(exc)


def test_service_command_uses_local_relay_for_inspector_when_api_auth_enabled(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)
    monkeypatch.setenv("UI_PORT", "18001")

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "mcp_auth_mode": "api",
            "model": "",
        },
    )
    assert save_response.status_code == 200

    generate_response = client.post("/api/settings/mcp-auth/generate", json={})
    assert generate_response.status_code == 200

    conn = db.get_connection()
    try:
        runtime_auth = ui_api._mcp_auth_runtime_config(conn)
    finally:
        conn.close()

    command, _env_overrides = ui_api._service_command("mcp_inspector", mcp_auth=runtime_auth)
    assert "--server-url" in command
    server_url_index = command.index("--server-url")
    assert command[server_url_index + 1] == "http://127.0.0.1:18001/api/inspector/mcp"
    assert "--header" not in command


def test_inspector_relay_injects_saved_mcp_api_key(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "mcp_auth_mode": "api",
            "model": "",
        },
    )
    assert save_response.status_code == 200

    generate_response = client.post("/api/settings/mcp-auth/generate", json={})
    assert generate_response.status_code == 200
    generated_key = generate_response.json()["api_key"]

    captured = {}

    class _FakeUpstreamResponse:
        status_code = 200
        headers = {"content-type": "application/json"}

        async def aiter_raw(self):
            yield b'{"ok":true}'

        async def aclose(self):
            return None

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        def build_request(self, method, url, headers=None, content=None):
            captured["method"] = method
            captured["url"] = url
            captured["headers"] = dict(headers or {})
            captured["content"] = content
            return object()

        async def send(self, request, stream=False):
            captured["stream"] = stream
            return _FakeUpstreamResponse()

        async def aclose(self):
            return None

    monkeypatch.setattr(ui_api.httpx, "AsyncClient", _FakeAsyncClient)
    monkeypatch.setenv("PROTOQUERY_DB_PATH", str(db_path))

    relay_response = client.post(
        "/api/inspector/mcp",
        headers={"mcp-session-id": "session-1"},
        json={"jsonrpc": "2.0", "id": "1", "method": "tools/list"},
    )
    assert relay_response.status_code == 200
    assert relay_response.json() == {"ok": True}
    assert captured["stream"] is True
    assert captured["headers"]["x-api-key"] == generated_key


def test_start_ngrok_service_uses_saved_authtoken(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "ngrok_authtoken": "2abc1234567890-ngrok-token",
            "model": "",
        },
    )
    assert save_response.status_code == 200

    captured = {}

    class _FakeListener:
        def url(self):
            return "https://demo.ngrok-free.app"

        def close(self):
            captured["closed"] = True
            return None

    class _FakeNgrokSdk:
        def set_auth_token(self, token: str):
            captured["token"] = token

        def forward(self, upstream: str):
            captured["forward"] = upstream
            return _FakeListener()

        def kill(self):
            captured["killed"] = True
            return None

    monkeypatch.setattr(ui_api, "_list_ngrok_pids", lambda: [])
    monkeypatch.setattr(ui_api, "_load_ngrok_sdk", lambda: _FakeNgrokSdk())

    snapshot = ui_api._start_service("ngrok")
    assert snapshot["running"] is True
    assert snapshot["service_url"] == "https://demo.ngrok-free.app/mcp"
    assert captured["token"] == "2abc1234567890-ngrok-token"
    assert captured["forward"] == "http://127.0.0.1:8000"

    stopped = ui_api._stop_service("ngrok")
    assert stopped["running"] is False
    assert captured["closed"] is True
    assert captured["killed"] is True


def test_models_lookup_uses_stored_key_when_request_key_is_empty(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": "sk-ant-stored-key",
            "model": "",
        },
    )
    assert response.status_code == 200

    captured = {"key": ""}

    def fake_list_models(api_key: str, limit: int = 100):
        captured["key"] = api_key
        return [{"id": "claude-foo", "display_name": "Claude Foo"}]

    monkeypatch.setattr(ui_api, "_list_anthropic_models", fake_list_models)

    models_response = client.post(
        "/api/settings/anthropic/models",
        json={"api_key": ""},
    )
    assert models_response.status_code == 200
    assert captured["key"] == "sk-ant-stored-key"
    payload = models_response.json()
    assert payload["models"] == [{"id": "claude-foo", "display_name": "Claude Foo"}]

    settings_response = client.get("/api/settings/app")
    assert settings_response.status_code == 200
    settings_payload = settings_response.json()
    assert settings_payload["models"] == [{"id": "claude-foo", "display_name": "Claude Foo"}]


def test_save_settings_persists_claude_instructions(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    instructions = "You are a Data Migration Assistant using MCP server"
    response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": "sk-ant-stored-key",
            "model": "claude-sonnet-4-5",
            "claude_instructions": instructions,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["claude_instructions"] == instructions

    conn = db.get_connection(path=str(db_path))
    stored = db.get_meta(conn, "claude_system_instructions", "")
    conn.close()
    assert stored == instructions


def test_validate_stored_key_activates_claude_tab(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": "sk-ant-stored-key",
            "model": "claude-sonnet-4-5",
        },
    )
    assert save_response.status_code == 200
    assert save_response.json()["anthropic_api_key_activated"] is False

    monkeypatch.setattr(
        ui_api,
        "_list_anthropic_models",
        lambda api_key, limit=1: [{"id": "claude-sonnet-4-5", "display_name": "Claude Sonnet 4.5"}],
    )

    validate_response = client.post("/api/settings/anthropic/validate", json={"api_key": ""})
    assert validate_response.status_code == 200
    payload = validate_response.json()
    assert payload["activated"] is True
    assert payload["app_settings"]["anthropic_api_key_activated"] is True

    settings_response = client.get("/api/settings/app")
    assert settings_response.status_code == 200
    assert settings_response.json()["anthropic_api_key_activated"] is True


def test_claude_chat_requires_activated_key(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": "sk-ant-stored-key",
            "model": "claude-sonnet-4-5",
        },
    )
    assert save_response.status_code == 200

    chat_response = client.post(
        "/api/claude/chat",
        json={"message": "Hello", "history": []},
    )
    assert chat_response.status_code == 400
    assert "must be validated" in chat_response.json()["detail"]


def test_claude_chat_uses_saved_key_and_model(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": "sk-ant-stored-key",
            "model": "claude-sonnet-4-5",
            "claude_instructions": "Use MCP tools first.",
        },
    )
    assert save_response.status_code == 200

    monkeypatch.setattr(
        ui_api,
        "_list_anthropic_models",
        lambda api_key, limit=1: [{"id": "claude-sonnet-4-5", "display_name": "Claude Sonnet 4.5"}],
    )
    validate_response = client.post("/api/settings/anthropic/validate", json={"api_key": ""})
    assert validate_response.status_code == 200
    assert validate_response.json()["activated"] is True

    captured = {}

    class _FakeTextBlock:
        type = "text"
        text = "Hello from Claude"

    class _FakeMessage:
        content = [_FakeTextBlock()]
        stop_reason = "end_turn"

    class _FakeMessagesClient:
        def create(self, **kwargs):
            captured.update(kwargs)
            return _FakeMessage()

    class _FakeAnthropic:
        def __init__(self, api_key: str, timeout: float):
            captured["api_key"] = api_key
            captured["timeout"] = timeout
            self.messages = _FakeMessagesClient()

    monkeypatch.setattr(ui_api, "Anthropic", _FakeAnthropic)

    chat_response = client.post(
        "/api/claude/chat",
        json={
            "message": "How many rows changed?",
            "history": [{"role": "user", "content": "Summary please"}],
        },
    )
    assert chat_response.status_code == 200
    payload = chat_response.json()
    assert payload["model"] == "claude-sonnet-4-5"
    assert payload["message"]["role"] == "assistant"
    assert payload["message"]["content"] == "Hello from Claude"

    assert captured["api_key"] == "sk-ant-stored-key"
    assert captured["model"] == "claude-sonnet-4-5"
    assert captured["system"] == "Use MCP tools first."
    assert captured["messages"][:2] == [
        {"role": "user", "content": "Summary please"},
        {"role": "user", "content": "How many rows changed?"},
    ]
    assert captured["messages"][2]["role"] == "assistant"


def test_claude_chat_logs_tool_calls(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)
    _register_demo_tool(ui_api, "demo_tool")

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": "sk-ant-stored-key",
            "model": "claude-sonnet-4-5",
        },
    )
    assert save_response.status_code == 200

    monkeypatch.setattr(
        ui_api,
        "_list_anthropic_models",
        lambda api_key, limit=1: [{"id": "claude-sonnet-4-5", "display_name": "Claude Sonnet 4.5"}],
    )
    validate_response = client.post("/api/settings/anthropic/validate", json={"api_key": ""})
    assert validate_response.status_code == 200
    assert validate_response.json()["activated"] is True

    class _FakeToolUseBlock:
        def __init__(self):
            self.type = "tool_use"
            self.id = "toolu_123"
            self.name = "demo_tool"
            self.input = {"limit": 1}

    class _FakeTextBlock:
        type = "text"
        text = "Tool call done."

    class _FakeMessagesClient:
        def __init__(self):
            self.calls = 0

        def create(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                class _ToolUseMessage:
                    content = [_FakeToolUseBlock()]
                    stop_reason = "tool_use"

                return _ToolUseMessage()

            class _FinalMessage:
                content = [_FakeTextBlock()]
                stop_reason = "end_turn"

            return _FinalMessage()

    class _FakeAnthropic:
        def __init__(self, api_key: str, timeout: float):
            self.api_key = api_key
            self.timeout = timeout
            self.messages = _FakeMessagesClient()

    monkeypatch.setattr(ui_api, "Anthropic", _FakeAnthropic)

    chat_response = client.post(
        "/api/claude/chat",
        json={"message": "Run the demo tool", "history": []},
    )
    assert chat_response.status_code == 200
    assert chat_response.json()["message"]["content"] == "Tool call done."

    logs_response = client.get("/api/tool-logs?status=all&limit=20")
    assert logs_response.status_code == 200
    logs_payload = logs_response.json()
    assert logs_payload["total"] == 1
    assert len(logs_payload["items"]) == 1
    entry = logs_payload["items"][0]
    assert entry["tool_name"] == "demo_tool"
    assert entry["source"] == "claude_chat"
    assert entry["status"] == "ok"
    assert entry["request_payload"]["limit"] == 1
    assert "\"ok\": true" in entry["response_payload"]
    assert entry["called_at"]
    assert entry["responded_at"]
    assert entry["duration_ms"] >= 0


def test_external_mcp_tool_calls_are_logged(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)
    _register_demo_tool(ui_api, "external_demo_tool")

    # Invoke the wrapped MCP tool function directly (mimics external MCP client execution path).
    tool = ui_api._mcp_instance._tool_manager._tools.get("external_demo_tool")
    assert tool is not None
    output = tool.fn(limit=1)
    assert isinstance(output, str)

    logs_response = client.get("/api/tool-logs?status=all&limit=20")
    assert logs_response.status_code == 200
    payload = logs_response.json()
    assert payload["total"] == 1
    entry = payload["items"][0]
    assert entry["source"] == "mcp_external"
    assert entry["tool_name"] == "external_demo_tool"
    assert entry["request_payload"]["limit"] == 1
    assert entry["status"] == "ok"


def test_tool_logging_can_be_disabled_from_settings(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)
    _register_demo_tool(ui_api, "no_log_tool")

    save_response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "tool_logging_enabled": False,
            "anthropic_api_key": "sk-ant-stored-key",
            "model": "claude-sonnet-4-5",
        },
    )
    assert save_response.status_code == 200
    assert save_response.json()["tool_logging_enabled"] is False

    monkeypatch.setattr(
        ui_api,
        "_list_anthropic_models",
        lambda api_key, limit=1: [{"id": "claude-sonnet-4-5", "display_name": "Claude Sonnet 4.5"}],
    )
    validate_response = client.post("/api/settings/anthropic/validate", json={"api_key": ""})
    assert validate_response.status_code == 200
    assert validate_response.json()["activated"] is True

    class _FakeToolUseBlock:
        def __init__(self):
            self.type = "tool_use"
            self.id = "toolu_456"
            self.name = "no_log_tool"
            self.input = {"limit": 1}

    class _FakeTextBlock:
        type = "text"
        text = "Done."

    class _FakeMessagesClient:
        def __init__(self):
            self.calls = 0

        def create(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                class _ToolUseMessage:
                    content = [_FakeToolUseBlock()]
                    stop_reason = "tool_use"

                return _ToolUseMessage()

            class _FinalMessage:
                content = [_FakeTextBlock()]
                stop_reason = "end_turn"

            return _FinalMessage()

    class _FakeAnthropic:
        def __init__(self, api_key: str, timeout: float):
            self.api_key = api_key
            self.timeout = timeout
            self.messages = _FakeMessagesClient()

    monkeypatch.setattr(ui_api, "Anthropic", _FakeAnthropic)

    chat_response = client.post(
        "/api/claude/chat",
        json={"message": "Run no_log_tool", "history": []},
    )
    assert chat_response.status_code == 200

    logs_response = client.get("/api/tool-logs?status=all&limit=20")
    assert logs_response.status_code == 200
    assert logs_response.json()["total"] == 0


def test_tool_logs_filters_and_cleanup(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    old_called = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    old_responded = (datetime.now(timezone.utc) - timedelta(days=30, seconds=-1)).isoformat()
    new_called = datetime.now(timezone.utc).isoformat()
    new_responded = (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat()

    conn = db.get_connection(path=str(db_path))
    try:
        db.create_tool_call_log(
            conn,
            source="claude_chat",
            request_id="old_req",
            tool_name="old_tool",
            status="error",
            request_payload={"a": 1},
            response_payload='{"error":"old failure"}',
            error_message="old failure",
            called_at=old_called,
            responded_at=old_responded,
            duration_ms=100,
            commit=False,
        )
        db.create_tool_call_log(
            conn,
            source="claude_chat",
            request_id="new_req",
            tool_name="new_tool",
            status="ok",
            request_payload={"b": 2},
            response_payload='{"ok":true}',
            error_message="",
            called_at=new_called,
            responded_at=new_responded,
            duration_ms=50,
            commit=False,
        )
        conn.commit()
    finally:
        conn.close()

    error_only_response = client.get("/api/tool-logs?status=error")
    assert error_only_response.status_code == 200
    error_payload = error_only_response.json()
    assert error_payload["total"] == 1
    assert error_payload["items"][0]["tool_name"] == "old_tool"

    cleanup_response = client.post("/api/tool-logs/cleanup-older-than", json={"days": 7})
    assert cleanup_response.status_code == 200
    assert cleanup_response.json()["deleted"] == 1

    after_cleanup = client.get("/api/tool-logs")
    assert after_cleanup.status_code == 200
    assert after_cleanup.json()["total"] == 1
    assert after_cleanup.json()["items"][0]["tool_name"] == "new_tool"

    clear_all_response = client.delete("/api/tool-logs")
    assert clear_all_response.status_code == 200
    assert clear_all_response.json()["deleted"] == 1

    empty_response = client.get("/api/tool-logs")
    assert empty_response.status_code == 200
    assert empty_response.json()["total"] == 0


def test_folder_configurations_save_list_apply(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_a = client.post(
        "/api/settings/folder-configs",
        json={
            "name": "Client A",
            "source_folder": "C:/data/client-a/source",
            "target_folder": "C:/data/client-a/target",
            "report_folder": "C:/data/client-a/reports",
            "set_active": True,
        },
    )
    assert save_a.status_code == 200
    payload_a = save_a.json()
    assert payload_a["created"] is True
    saved_a_id = payload_a["saved_id"]
    assert payload_a["active_id"] == saved_a_id
    assert payload_a["folders"]["source_folder"] == "C:/data/client-a/source"

    save_b = client.post(
        "/api/settings/folder-configs",
        json={
            "name": "Client B",
            "source_folder": "C:/data/client-b/source",
            "target_folder": "C:/data/client-b/target",
            "report_folder": "C:/data/client-b/reports",
            "set_active": False,
        },
    )
    assert save_b.status_code == 200
    payload_b = save_b.json()
    saved_b_id = payload_b["saved_id"]
    assert payload_b["active_id"] == saved_a_id

    list_response = client.get("/api/settings/folder-configs")
    assert list_response.status_code == 200
    listed = list_response.json()
    assert len(listed["configs"]) == 2
    assert listed["active_id"] == saved_a_id

    apply_b = client.post(f"/api/settings/folder-configs/{saved_b_id}/apply")
    assert apply_b.status_code == 200
    apply_payload = apply_b.json()
    assert apply_payload["active_id"] == saved_b_id
    assert apply_payload["folders"]["source_folder"] == "C:/data/client-b/source"
    assert apply_payload["folders"]["target_folder"] == "C:/data/client-b/target"
    assert apply_payload["folders"]["report_folder"] == "C:/data/client-b/reports"


def test_folder_configurations_active_syncs_with_manual_folder_save(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_cfg = client.post(
        "/api/settings/folder-configs",
        json={
            "name": "QA Env",
            "source_folder": "C:/qa/source",
            "target_folder": "C:/qa/target",
            "report_folder": "C:/qa/reports",
            "set_active": False,
        },
    )
    assert save_cfg.status_code == 200
    cfg_id = save_cfg.json()["saved_id"]

    manual_save = client.post(
        "/api/settings/folders",
        json={
            "source_folder": "C:/qa/source",
            "target_folder": "C:/qa/target",
            "report_folder": "C:/qa/reports",
        },
    )
    assert manual_save.status_code == 200

    list_after_match = client.get("/api/settings/folder-configs")
    assert list_after_match.status_code == 200
    assert list_after_match.json()["active_id"] == cfg_id

    manual_mismatch = client.post(
        "/api/settings/folders",
        json={
            "source_folder": "C:/other/source",
            "target_folder": "C:/other/target",
            "report_folder": "C:/other/reports",
        },
    )
    assert manual_mismatch.status_code == 200

    list_after_mismatch = client.get("/api/settings/folder-configs")
    assert list_after_mismatch.status_code == 200
    assert list_after_mismatch.json()["active_id"] == ""


def test_folder_configurations_delete_removes_active(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_cfg = client.post(
        "/api/settings/folder-configs",
        json={
            "name": "Delete Me",
            "source_folder": "C:/delete/source",
            "target_folder": "C:/delete/target",
            "report_folder": "C:/delete/reports",
            "set_active": True,
        },
    )
    assert save_cfg.status_code == 200
    cfg_id = save_cfg.json()["saved_id"]
    assert save_cfg.json()["active_id"] == cfg_id

    delete_response = client.delete(f"/api/settings/folder-configs/{cfg_id}")
    assert delete_response.status_code == 200
    deleted_payload = delete_response.json()
    assert deleted_payload["deleted_id"] == cfg_id
    assert deleted_payload["active_id"] == ""

    list_response = client.get("/api/settings/folder-configs")
    assert list_response.status_code == 200
    assert list_response.json()["configs"] == []
    assert list_response.json()["active_id"] == ""


def test_folders_expose_source_target_defaults_to_true(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    response = client.get("/api/settings/folders")
    assert response.status_code == 200
    payload = response.json()
    assert payload["expose_source_to_tools"] is True
    assert payload["expose_target_to_tools"] is True


def test_folder_configurations_persist_extra_folders_and_exposure_flags(monkeypatch, tmp_path: Path) -> None:
    ui_api, _ = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    save_response = client.post(
        "/api/settings/folder-configs",
        json={
            "name": "Extended Config",
            "source_folder": "C:/extended/source",
            "target_folder": "C:/extended/target",
            "configurations_folder": "C:/extended/configurations",
            "translations_folder": "C:/extended/translations",
            "rules_folder": "C:/extended/rules",
            "report_folder": "C:/extended/reports",
            "expose_source_to_tools": False,
            "expose_target_to_tools": True,
            "expose_configurations_to_tools": True,
            "expose_translations_to_tools": False,
            "expose_rules_to_tools": True,
            "set_active": True,
        },
    )
    assert save_response.status_code == 200
    payload = save_response.json()
    assert payload["folders"]["configurations_folder"] == "C:/extended/configurations"
    assert payload["folders"]["translations_folder"] == "C:/extended/translations"
    assert payload["folders"]["rules_folder"] == "C:/extended/rules"
    assert payload["folders"]["expose_source_to_tools"] is False
    assert payload["folders"]["expose_target_to_tools"] is True
    assert payload["folders"]["expose_configurations_to_tools"] is True
    assert payload["folders"]["expose_translations_to_tools"] is False
    assert payload["folders"]["expose_rules_to_tools"] is True

    folders_response = client.get("/api/settings/folders")
    assert folders_response.status_code == 200
    folders_payload = folders_response.json()
    assert folders_payload["configurations_folder"] == "C:/extended/configurations"
    assert folders_payload["translations_folder"] == "C:/extended/translations"
    assert folders_payload["rules_folder"] == "C:/extended/rules"
    assert folders_payload["expose_source_to_tools"] is False
    assert folders_payload["expose_target_to_tools"] is True
    assert folders_payload["expose_configurations_to_tools"] is True
    assert folders_payload["expose_translations_to_tools"] is False
    assert folders_payload["expose_rules_to_tools"] is True

    list_response = client.get("/api/settings/folder-configs")
    assert list_response.status_code == 200
    configs = list_response.json()["configs"]
    assert len(configs) == 1
    cfg = configs[0]
    assert cfg["configurations_folder"] == "C:/extended/configurations"
    assert cfg["translations_folder"] == "C:/extended/translations"
    assert cfg["rules_folder"] == "C:/extended/rules"
    assert cfg["expose_source_to_tools"] is False
    assert cfg["expose_target_to_tools"] is True
    assert cfg["expose_configurations_to_tools"] is True
    assert cfg["expose_translations_to_tools"] is False
    assert cfg["expose_rules_to_tools"] is True
