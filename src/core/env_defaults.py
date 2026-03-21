from __future__ import annotations

import os
from pathlib import Path


def load_bots_env_defaults() -> dict[str, str]:
    """
    Load missing environment variables from the repo-local `.env` file.

    This keeps manual runs and targeted automation flows consistent with the
    full bots runtime without overwriting shell-provided secrets.
    """

    env_path = Path(__file__).resolve().parents[2] / ".env"
    loaded: dict[str, str] = {}
    if not env_path.exists():
        return loaded

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value
        loaded[key] = value
    return loaded
