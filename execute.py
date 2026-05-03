"""Manual setup/repair entry point for A0 Scheduler."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

REQUIRED_MODULES = ("caldav", "vobject", "icalendar")


def _requirements_file() -> Path:
    return Path(__file__).resolve().parent / "requirements.txt"


def _module_status() -> list[str]:
    lines: list[str] = []
    for name in REQUIRED_MODULES:
        spec = importlib.util.find_spec(name)
        lines.append(f"{'✅' if spec else '❌'} {name}: {'available' if spec else 'missing'}")
    return lines


def execute(**_kwargs):
    """Install/repair Scheduler runtime dependencies.

    This is safe to run again after pulling a new Agent Zero container or after
    reinstalling/updating the plugin.
    """
    results: list[str] = []
    req_file = _requirements_file()
    results.append(f"Python: {sys.executable}")
    results.append(f"Requirements: {req_file}")
    results.append("Before:")
    results.extend(_module_status())

    if not req_file.exists():
        results.append(f"❌ requirements.txt not found: {req_file}")
        return "\n".join(results)

    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", str(req_file), "-q"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        results.append(f"❌ Dependency install failed: {exc}")
        return "\n".join(results)

    results.append("After:")
    results.extend(_module_status())
    missing = [name for name in REQUIRED_MODULES if importlib.util.find_spec(name) is None]
    if missing:
        results.append(f"❌ Still missing: {', '.join(missing)}")
    else:
        results.append("✅ Scheduler dependencies are installed")
    return "\n".join(results)


if __name__ == "__main__":
    print(execute())
