"""A0 Scheduler plugin lifecycle hooks.

The Scheduler plugin uses CalDAV-related third-party Python packages from
``requirements.txt``.  Fresh Agent Zero containers may not have those packages
in the runtime virtualenv, so install them during plugin install/update and make
that repair path reusable from ``execute.py``.

Additionally, the in-plugin async runtime (Option B dispatcher) is started
from :func:`post_install` and :func:`post_load` so calendar events actually
fire their ``a0_prompts`` / ``a0_toolcalls`` / ``a0_actions`` blocks at the
right time. The runtime singleton is idempotent and safe to call multiple
times.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

REQUIRED_MODULES = ("caldav", "vobject", "icalendar", "exchangelib", "dateutil")


def _requirements_file() -> Path:
    return Path(__file__).resolve().parent / "requirements.txt"


def missing_modules() -> list[str]:
    """Return required import names that are not available in this runtime."""
    return [name for name in REQUIRED_MODULES if importlib.util.find_spec(name) is None]


def install_requirements(*, quiet: bool = True) -> dict[str, object]:
    """Install Scheduler Python dependencies into the current A0 runtime venv."""
    req_file = _requirements_file()
    if not req_file.exists():
        raise FileNotFoundError(f"requirements.txt not found: {req_file}")

    before = missing_modules()
    command = [sys.executable, "-m", "pip", "install", "-r", str(req_file)]
    if quiet:
        command.append("-q")

    subprocess.check_call(command)

    after = missing_modules()
    return {
        "ok": not after,
        "python": sys.executable,
        "requirements": str(req_file),
        "missing_before": before,
        "missing_after": after,
    }


def _start_runtime_safely() -> None:
    """Best-effort startup of the in-plugin async tick loop."""
    try:
        from usr.plugins.a0_scheduler.helpers import agent_scheduler_runtime as _runtime
        _runtime.start_runtime()
    except Exception:
        # Plugin install/update flows must not fail because the runtime cannot
        # be started yet (for example, when the asyncio loop is not ready).
        # The UI also lazy-starts the runtime via ``runtime_status``.
        pass


def install():
    """Install plugin dependencies from requirements.txt after plugin install."""
    install_requirements(quiet=True)
    _start_runtime_safely()


def pre_update():
    """Ensure dependencies are present before plugin updates run new code."""
    install_requirements(quiet=True)


def post_install():
    """Idempotent runtime start after install (separate hook for clarity)."""
    _start_runtime_safely()


def post_load():
    """Called when the plugin module is loaded into the running process."""
    _start_runtime_safely()
