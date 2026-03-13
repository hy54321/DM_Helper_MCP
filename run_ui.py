import os

import uvicorn


def main() -> None:
    host = os.getenv("UI_HOST", "127.0.0.1")
    port = int(os.getenv("UI_PORT", "8000"))
    reload = os.getenv("UI_RELOAD", "0") == "1"
    os.environ["UI_HOST"] = host
    os.environ["UI_PORT"] = str(port)
    uvicorn.run("ui.api:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    main()
