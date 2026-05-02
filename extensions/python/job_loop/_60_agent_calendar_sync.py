"""Background CalDAV/local ICS sync scheduler.

Keeps selected CalDAV collections from drifting silently.  The sync helper enforces
a 15-minute normal cadence and exposes stale status after 1 hour; this job-loop
extension simply wakes it periodically without blocking every framework tick.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any


class _FallbackExtension:
    """Minimal extension base used when framework helper imports are unavailable.

    Agent Zero's extension runner only needs an instantiable class with an
    ``execute`` method.  Avoiding a hard import of ``helpers.extension`` here also
    avoids plugin-local ``helpers`` shadowing and optional framework dependency
    failures during plugin discovery.
    """

    def __init__(self, agent: Any | None = None, **kwargs: Any) -> None:
        self.agent = agent
        self.kwargs = kwargs


try:  # Prefer the real base when the framework import path is healthy.
    from helpers.extension import Extension as _FrameworkExtension  # type: ignore
except Exception:  # pragma: no cover - defensive for plugin discovery edge cases
    _FrameworkExtension = _FallbackExtension


class _Print:
    @staticmethod
    def hint(message: str) -> None:
        try:
            from helpers.print_style import PrintStyle  # type: ignore

            PrintStyle.hint(message)
        except Exception:
            print(message)

    @staticmethod
    def error(message: str) -> None:
        try:
            from helpers.print_style import PrintStyle  # type: ignore

            PrintStyle.error(message)
        except Exception:
            print(message)


_LAST_TICK = 0.0
_TASK: asyncio.Task | None = None
TICK_SECONDS = 60


class AgentCalendarSyncLoop(_FrameworkExtension):
    async def execute(self, **kwargs: Any) -> None:
        global _LAST_TICK, _TASK
        now = time.time()
        if _TASK is not None and _TASK.done():
            _TASK = None
        if _TASK is not None:
            return
        if now - _LAST_TICK < TICK_SECONDS:
            return
        _LAST_TICK = now
        _TASK = asyncio.create_task(_run_due_syncs())


async def _run_due_syncs() -> None:
    try:
        from usr.plugins.a0_scheduler.helpers import agent_calendar_sync

        result = await asyncio.to_thread(agent_calendar_sync.sync_due_contexts, max_contexts=2)
        synced = int(result.get("synced") or 0) if isinstance(result, dict) else 0
        if synced:
            _Print.hint(f"Agent Calendar CalDAV sync: synced {synced} due context(s)")
    except Exception as exc:
        _Print.error(f"Agent Calendar CalDAV sync job failed: {exc}")
