# ProtoQuery

ProtoQuery is a data migration assistant with:
- an MCP server (`mcp_server.py`)
- a FastAPI admin UI (`run_ui.py`)
- a desktop wrapper (`desktop_app.py`) for a single `.exe` experience

## Prerequisites

- Python 3.10+
- `uv` (recommended)

## Run In Development

Install dependencies:

```powershell
uv sync
```

Run web UI:

```powershell
uv run run_ui.py
```

Run MCP server:

```powershell
$env:MCP_TRANSPORT="streamable-http"; uv run mcp_server.py
```

## Run As Desktop App (from source)

```powershell
uv run protoquery-desktop
```

This starts the local backend and opens a desktop window via `pywebview`.
In desktop mode, open the `Services` tab to start/stop `MCP Server`, `MCP Inspector`, and `ngrok` with slider toggles.
Save your ngrok auth token in `Settings` before starting the ngrok service.
When `MCP Inspector` is running, an `MCP Inspector` tab appears and embeds its web page.

## Build Standalone EXE (PyInstaller)

Install build extras:

```powershell
uv sync --extra desktop-build
```

Build:

```powershell
.\build-desktop.ps1
```

Output:

```text
dist\ProtoQuery.exe
```

## SQLite Location In EXE Mode

When running the packaged `.exe`, the app creates and uses:

```text
<exe-folder>\protoquery.db
```

This is automatic at startup. You can override the path with:

```powershell
$env:PROTOQUERY_DB_PATH="D:\somewhere\protoquery.db"
```

## Useful Environment Variables

- `UI_HOST` (default `127.0.0.1`)
- `UI_PORT` (default `8001`, auto-fallback to free port if busy)
- `PROTOQUERY_DB_PATH` (SQLite file override; `DMH_DB_PATH` still supported)
- `PROTOQUERY_MCP_PORT` (default `8000`, used by MCP server and ngrok; `DMH_MCP_PORT` still supported)
- `npx` is required for MCP Inspector service (install Node.js)
- `PROTOQUERY_WINDOW_TITLE` (desktop window title; `DMH_WINDOW_TITLE` still supported)
- `PROTOQUERY_WEBVIEW_DEBUG` (`1` enables webview debug mode; `DMH_WEBVIEW_DEBUG` still supported)
