import os
import subprocess
import sys
from pathlib import Path
from typing import Dict


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _site_repo() -> Path:
    return Path(os.environ.get("FALCO_SITE_REPO", r"C:\code\falco-site"))


def _load_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    env: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            env[key] = value
    return env


def _run_command(command: list[str], cwd: Path, env: Dict[str, str]) -> Dict[str, object]:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "command": command,
        "cwd": str(cwd),
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "ok": completed.returncode == 0,
    }


def maybe_publish_to_vault(run_id: str) -> Dict[str, object]:
    if not _truthy(os.environ.get("FALCO_AUTO_PUBLISH_VAULT")):
        return {
            "attempted": False,
            "enabled": False,
            "reason": "FALCO_AUTO_PUBLISH_VAULT not enabled",
        }

    bots_repo = _repo_root()
    site_repo = _site_repo()
    site_env = os.environ.copy()
    site_env.update(_load_env_file(site_repo / ".env.local"))

    sync_result = _run_command([sys.executable, str(bots_repo / "sync_to_vault.py")], bots_repo, os.environ.copy())
    if not sync_result["ok"]:
        return {
            "attempted": True,
            "enabled": True,
            "run_id": run_id,
            "sync": sync_result,
            "import": None,
            "ok": False,
        }

    import_result = _run_command(["node", str(site_repo / "scripts" / "import-vault-listings.mjs")], site_repo, site_env)
    return {
        "attempted": True,
        "enabled": True,
        "run_id": run_id,
        "sync": sync_result,
        "import": import_result,
        "ok": bool(sync_result["ok"] and import_result["ok"]),
    }
