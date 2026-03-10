import importlib
from pathlib import Path

from fastapi.testclient import TestClient

from server import db


def _load_ui_api(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "dm_helper.db"
    app_dir = tmp_path / "app_data"
    app_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("DMH_DB_PATH", str(db_path))
    monkeypatch.setenv("DMH_APP_BASE_DIR", str(app_dir))

    import ui.api as ui_api

    return importlib.reload(ui_api), db_path


def test_save_settings_encrypts_anthropic_key(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    raw_key = "sk-ant-test-1234567890"
    response = client.post(
        "/api/settings/app",
        json={
            "theme": "light",
            "anthropic_api_key": raw_key,
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
    conn.close()

    assert encrypted
    assert encrypted != raw_key
    assert ui_api._decrypt_secret(encrypted) == raw_key


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
    assert captured["messages"] == [
        {"role": "user", "content": "Summary please"},
        {"role": "user", "content": "How many rows changed?"},
    ]
