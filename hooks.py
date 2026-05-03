"""A0 Scheduler plugin lifecycle hooks.

The Scheduler plugin uses CalDAV-related third-party Python packages from
``requirements.txt``.  Fresh Agent Zero containers may not have those packages
in the runtime virtualenv, so install them during plugin install/update and make
that repair path reusable from ``execute.py``.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

REQUIRED_MODULES = ("caldav", "vobject", "icalendar")


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


def install():
    """Install plugin dependencies from requirements.txt after plugin install."""
    install_requirements(quiet=True)


def pre_update():
    """Ensure dependencies are present before plugin updates run new code."""
    install_requirements(quiet=True)
