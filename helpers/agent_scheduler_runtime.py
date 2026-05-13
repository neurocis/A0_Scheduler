"""A0 Scheduler — in-plugin async runtime (Option B).

This module owns the async tick loop and dispatcher that fires the
``a0_prompts`` / ``a0_toolcalls`` / ``a0_actions`` blocks declared in the
sibling JSON sidecar for each ICS file under
``/a0/usr/chats/<ctxid>/calendar/**``.

ICS + sibling sidecar JSON is the **sole source of truth** — no materialization
into the framework's builtin TaskScheduler. The loop scans calendar dirs every
60s, expands RRULE/RDATE/EXDATE, computes which occurrences crossed start or
end boundaries inside the tick window, and dispatches them. Per-event fired
records are persisted to ``.a0-scheduler-fired.json`` next to the ICS files so
state survives restarts and ``on_miss`` policies can decide what to do about
missed events.

Framework integration points (verified at implementation time):

- ``AgentContext.get(id)`` / ``AgentContext.use(id)`` — ``/a0/agent.py`` (class
  ``AgentContext`` near line 42).
- ``AgentContext.communicate(UserMessage)`` — ``/a0/agent.py`` around line 251.
- ``UserMessage`` dataclass — ``/a0/agent.py`` around line 319; fields:
  ``message``, ``attachments``, ``system_message``, ``id``.
- Tool base class — ``/a0/helpers/tool.py`` (``Tool`` / ``Response``).
- Tool dispatch (per-agent) — ``/a0/agent.py`` around line 1010
  (``Agent.get_tool(name, method, args, message, loop_data)``).
- Chat compaction routine —
  ``/a0/plugins/_chat_compaction/helpers/compactor.py:run_compaction(
  context, use_chat_model=True, preset_name=None)``.
- Chat reset/clear routine — pattern used by ``/a0/api/chat_reset.py``:
  ``context.reset()`` followed by ``helpers.persist_chat.save_tmp_chat(
  context)`` and ``helpers.persist_chat.remove_msg_files(ctxid)``. The
  framework's ``TaskScheduler.cancel_tasks_by_context`` is also invoked.
- Superordinate name → ctxid lookup —
  ``/a0/usr/plugins/a0_superordinates/helpers/name_registry.py:lookup_by_name``.
- Superordinate spawn pattern —
  ``/a0/usr/plugins/a0_superordinates/tools/superordinate_spawn.py``.

All imports of framework modules are performed lazily inside helper functions
to avoid import cycles at plugin load time and so the plugin can still be
imported in test environments where ``initialize_agent`` is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterable, Iterator

logger = logging.getLogger("agent_scheduler_runtime")
if not logger.handlers:
    logger.setLevel(logging.INFO)

CHATS_ROOT = Path("/a0/usr/chats")
CALENDAR_DIRNAME = "calendar"
FIRED_STATE_FILENAME = ".a0-scheduler-fired.json"

# Filenames inside a calendar/ dir that are not real ICS events.
_NON_EVENT_FILES: set[str] = {
    "caldav.json",
    "exchange.json",
    "subscriptions.json",
    ".a0-caldav-sync-state.json",
    ".a0-exchange-sync-state.json",
    FIRED_STATE_FILENAME,
}

# Defaults for a0_runtime block.
DEFAULT_RUNTIME: dict[str, Any] = {
    "target": "self",
    "profile": "agent0",
    "name": "",
    "on_miss": "fire",
    "grace_seconds": 600,
    "order": ["prompt", "toolcall", "action"],
}

# Cap on per-event RRULE expansion per tick to prevent runaway recurrence.
_MAX_OCCURRENCES_PER_TICK = 50

# Concurrency cap for dispatch within a single tick.
_DISPATCH_CONCURRENCY = 4

# How long the loop sleeps between ticks (seconds). Aligns to wall-minute.
_TICK_INTERVAL_SECONDS = 60

# Tick history kept in memory for status display.
_FIRED_RECENT_LIMIT = 50


# ---------------------------------------------------------------------------
# Runtime singleton + status state
# ---------------------------------------------------------------------------


class _RuntimeState:
    """Thread-safe singleton state for the tick loop."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.task: asyncio.Task | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.ready_event: threading.Event | None = None
        self.running: bool = False
        self.last_tick_iso: str = ""
        self.tick_count: int = 0
        self.last_error: str = ""
        self.fired_recent: list[dict[str, Any]] = []
        self.started_at_iso: str = ""
        # Previous tick boundary (UTC datetime). On first tick, this is
        # populated from ``now - grace_seconds`` so missed events can fire.
        self._prev_tick: datetime | None = None

    def record_fired(self, entry: dict[str, Any]) -> None:
        with self._lock:
            self.fired_recent.insert(0, entry)
            if len(self.fired_recent) > _FIRED_RECENT_LIMIT:
                self.fired_recent = self.fired_recent[:_FIRED_RECENT_LIMIT]


_state = _RuntimeState()


# ---------------------------------------------------------------------------
# Datetime / time helpers
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime | None:
    """Parse an ISO-8601 string, returning a timezone-aware datetime or None."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Tolerate trailing Z.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_local_tzinfo())
    return dt


def _local_tzinfo():
    return datetime.now().astimezone().tzinfo


def _ensure_aware(dt: datetime | date) -> datetime:
    """Return a timezone-aware datetime. Date-only values are treated as 00:00
    in the system local timezone, then converted to UTC at the caller."""
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=_local_tzinfo())
        return dt
    # ``date`` (not datetime) — VALUE=DATE all-day event.
    return datetime.combine(dt, dtime(0, 0, 0)).replace(tzinfo=_local_tzinfo())


# ---------------------------------------------------------------------------
# Sidecar / fired-state I/O
# ---------------------------------------------------------------------------


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
        json.dump(data, tmp, indent=2, sort_keys=False, default=str)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _sidecar_path_for(ics_path: Path) -> Path:
    return ics_path.with_suffix(".json")


def _has_runtime_payload(sidecar: dict[str, Any]) -> bool:
    """Return True if the sidecar declares at least one runtime-firable block.

    A sidecar is considered runtime-firable when any of ``a0_prompts``,
    ``a0_toolcalls``, or ``a0_actions`` contains a non-empty ``start`` or
    ``end`` payload.
    """
    for key in ("a0_prompts", "a0_toolcalls", "a0_actions"):
        block = sidecar.get(key)
        if not isinstance(block, dict):
            continue
        for kind in ("start", "end"):
            value = block.get(kind)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if isinstance(value, dict) and not value:
                continue
            return True
    return False


def _load_fired_state(calendar_dir: Path) -> dict[str, Any]:
    return _read_json(calendar_dir / FIRED_STATE_FILENAME)


def _save_fired_state(calendar_dir: Path, state: dict[str, Any]) -> None:
    _atomic_write_json(calendar_dir / FIRED_STATE_FILENAME, state)


def _write_sidecar_last_execution(
    ics_path: Path,
    kind: str,
    fired_at_iso: str,
    result: str,
    error: str,
) -> None:
    sidecar_path = _sidecar_path_for(ics_path)
    if not sidecar_path.is_file():
        return
    try:
        data = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(data, dict):
        return
    last = data.setdefault("a0_last_execution", {})
    if not isinstance(last, dict):
        last = {}
        data["a0_last_execution"] = last
    last[kind] = {
        "fired_at": fired_at_iso,
        "result": result[:2000],
        "error": error[:1000],
    }
    try:
        _atomic_write_json(sidecar_path, data)
    except Exception as exc:
        logger.warning("failed to update sidecar last_execution for %s: %s", sidecar_path, exc)


# ---------------------------------------------------------------------------
# Calendar discovery + occurrence math
# ---------------------------------------------------------------------------


def _iter_calendar_files() -> Iterator[tuple[str, Path, dict[str, Any]]]:
    """Yield ``(ctxid, ics_path, sidecar_dict)`` for every fireable event."""
    if not CHATS_ROOT.is_dir():
        return
    for ctx_dir in CHATS_ROOT.iterdir():
        if not ctx_dir.is_dir():
            continue
        calendar_dir = ctx_dir / CALENDAR_DIRNAME
        if not calendar_dir.is_dir():
            continue
        for ics_path in calendar_dir.rglob("*.ics"):
            if ".conflicts" in ics_path.parts:
                continue
            if ics_path.name in _NON_EVENT_FILES:
                continue
            sidecar = _read_json(_sidecar_path_for(ics_path))
            if not isinstance(sidecar, dict):
                continue
            if not _has_runtime_payload(sidecar):
                continue
            yield ctx_dir.name, ics_path, sidecar


def _event_occurrences(
    ics_path: Path,
    start_window: datetime,
    end_window: datetime,
) -> list[tuple[datetime, datetime]]:
    """Compute (DTSTART, DTEND) pairs whose start OR end falls inside the
    window ``[start_window, end_window]``. Honors RRULE, RDATE, EXDATE.

    All datetimes returned are timezone-aware (UTC).
    """
    try:
        from icalendar import Calendar  # type: ignore
    except Exception as exc:  # pragma: no cover
        logger.warning("icalendar import failed: %s", exc)
        return []
    try:
        from dateutil.rrule import rrulestr  # type: ignore
    except Exception:
        rrulestr = None  # type: ignore[assignment]

    try:
        cal = Calendar.from_ical(ics_path.read_bytes())
    except Exception as exc:
        logger.warning("unable to parse %s: %s", ics_path, exc)
        return []

    pairs: list[tuple[datetime, datetime]] = []
    sw = start_window.astimezone(timezone.utc)
    ew = end_window.astimezone(timezone.utc)

    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue

        dtstart_raw = comp.get("DTSTART")
        dtend_raw = comp.get("DTEND")
        if dtstart_raw is None:
            continue
        dtstart = _ensure_aware(dtstart_raw.dt).astimezone(timezone.utc)
        if dtend_raw is not None:
            dtend = _ensure_aware(dtend_raw.dt).astimezone(timezone.utc)
        else:
            duration_raw = comp.get("DURATION")
            if duration_raw is not None:
                try:
                    dur = duration_raw.dt
                    if isinstance(dur, timedelta):
                        dtend = dtstart + dur
                    else:
                        dtend = dtstart
                except Exception:
                    dtend = dtstart
            else:
                # All-day or zero-duration; treat as instant.
                dtend = dtstart

        rrule_prop = comp.get("RRULE")
        if rrule_prop is None or rrulestr is None:
            occurrences = [dtstart]
        else:
            try:
                rule_text = rrule_prop.to_ical().decode() if hasattr(rrule_prop, "to_ical") else str(rrule_prop)
                rule = rrulestr("RRULE:" + rule_text, dtstart=dtstart)
                # Window expansion: anything overlapping [sw - duration, ew].
                duration = dtend - dtstart
                expansion_start = sw - duration - timedelta(seconds=1)
                occurrences = list(rule.between(expansion_start, ew + timedelta(seconds=1), inc=True))
            except Exception as exc:
                logger.warning("RRULE expansion failed for %s: %s", ics_path, exc)
                occurrences = [dtstart]

        # RDATE additions.
        rdate_prop = comp.get("RDATE")
        if rdate_prop is not None:
            try:
                rdates_iter = rdate_prop if isinstance(rdate_prop, list) else [rdate_prop]
                for rdate_entry in rdates_iter:
                    for d in getattr(rdate_entry, "dts", []) or []:
                        try:
                            occurrences.append(_ensure_aware(d.dt).astimezone(timezone.utc))
                        except Exception:
                            continue
            except Exception:
                pass

        # EXDATE removals.
        exdates: set[datetime] = set()
        ex_prop = comp.get("EXDATE")
        if ex_prop is not None:
            try:
                ex_iter = ex_prop if isinstance(ex_prop, list) else [ex_prop]
                for ex_entry in ex_iter:
                    for d in getattr(ex_entry, "dts", []) or []:
                        try:
                            exdates.add(_ensure_aware(d.dt).astimezone(timezone.utc))
                        except Exception:
                            continue
            except Exception:
                pass

        duration = dtend - dtstart
        kept = 0
        for occ_start in sorted(set(occurrences)):
            if occ_start in exdates:
                continue
            occ_start_utc = occ_start.astimezone(timezone.utc) if occ_start.tzinfo else occ_start.replace(tzinfo=timezone.utc)
            occ_end_utc = occ_start_utc + duration
            # Window test: either boundary inside window.
            if (sw <= occ_start_utc <= ew) or (sw <= occ_end_utc <= ew):
                pairs.append((occ_start_utc, occ_end_utc))
                kept += 1
                if kept >= _MAX_OCCURRENCES_PER_TICK:
                    break
        # Only one VEVENT per A0-scheduler ICS — break after first match.
        break

    return pairs


def _event_uid_from_sidecar(sidecar: dict[str, Any], ics_path: Path) -> str:
    name = sidecar.get("a0_name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    # fallback to extracting UID from ICS
    try:
        text = ics_path.read_text(encoding="utf-8", errors="ignore")
        m = re.search(r"^UID:(.+)$", text, flags=re.MULTILINE)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return ics_path.stem


def _runtime_cfg(sidecar: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(DEFAULT_RUNTIME)
    user_cfg = sidecar.get("a0_runtime")
    if isinstance(user_cfg, dict):
        for key in cfg.keys():
            if key in user_cfg and user_cfg[key] is not None:
                cfg[key] = user_cfg[key]
    # Validate order
    order = cfg.get("order") or DEFAULT_RUNTIME["order"]
    if not isinstance(order, list):
        order = list(DEFAULT_RUNTIME["order"])
    cfg["order"] = [o for o in order if o in {"prompt", "toolcall", "action"}] or list(DEFAULT_RUNTIME["order"])
    try:
        cfg["grace_seconds"] = int(cfg.get("grace_seconds") or DEFAULT_RUNTIME["grace_seconds"])
    except Exception:
        cfg["grace_seconds"] = int(DEFAULT_RUNTIME["grace_seconds"])
    return cfg


# ---------------------------------------------------------------------------
# Should-fire logic
# ---------------------------------------------------------------------------


def _should_fire(
    state: dict[str, Any],
    uid: str,
    occurrence_iso: str,
    kind: str,
    boundary_dt: datetime,
    on_miss: str,
    grace_seconds: int,
    now: datetime,
) -> tuple[bool, str | None]:
    """Return ``(fire, skip_reason)`` for a candidate firing."""
    rec = state.get(uid, {}).get(occurrence_iso, {}).get(kind)
    if isinstance(rec, dict) and rec.get("fired_at"):
        return False, "already_fired"

    # Determine miss policy.
    age_seconds = (now - boundary_dt).total_seconds()
    if age_seconds < 0:
        # Future boundary inside this tick (shouldn't normally happen).
        return True, None

    if age_seconds <= grace_seconds:
        return True, None

    policy = (on_miss or "fire").lower()
    if policy == "fire":
        return True, None
    if policy == "skip":
        return False, "missed_beyond_grace_skip"
    if policy == "notify":
        # We still skip the real dispatch but mark as notify so the dispatcher
        # records a notification entry.
        return False, "missed_beyond_grace_notify"
    return True, None


def _record_fired(
    state: dict[str, Any],
    uid: str,
    occurrence_iso: str,
    kind: str,
    fired_at_iso: str,
    result_summary: str,
    error: str,
) -> None:
    uid_block = state.setdefault(uid, {})
    occ_block = uid_block.setdefault(occurrence_iso, {})
    occ_block[kind] = {
        "fired_at": fired_at_iso,
        "result": result_summary[:2000],
        "error": error[:1000],
    }


# ---------------------------------------------------------------------------
# Variable substitution
# ---------------------------------------------------------------------------

_VAR_PATTERN = re.compile(
    r"\$(?:\{(?P<braced>[A-Za-z_0-9][A-Za-z_0-9]*)(?::-(?P<default>[^}]*))?\}|(?P<bare>[A-Za-z_0-9]+))"
)


def _substitute_vars(value: Any, env: dict[str, str]) -> Any:
    """Recursive variable substitutor.

    Supports:
    - ``$N`` and ``${N}`` — positional from ``a0_toolcall_args``
    - ``${N:-default}`` — positional with default
    - ``${NAME}`` and ``${NAME:-default}`` — named variables
    - bare ``$NAME`` — named lookup
    """
    if isinstance(value, str):
        def _repl(m: re.Match[str]) -> str:
            key = m.group("braced") or m.group("bare") or ""
            default = m.group("default") if m.group("default") is not None else None
            if key in env:
                return str(env[key])
            if default is not None:
                return default
            # Keep the original token if no value and no default.
            return m.group(0)
        return _VAR_PATTERN.sub(_repl, value)
    if isinstance(value, list):
        return [_substitute_vars(v, env) for v in value]
    if isinstance(value, dict):
        return {k: _substitute_vars(v, env) for k, v in value.items()}
    return value


def _build_var_env(
    ctxid: str,
    sidecar: dict[str, Any],
    kind: str,
    occurrence_start: datetime,
) -> dict[str, str]:
    env: dict[str, str] = {}
    # Positional args.
    positional = sidecar.get("a0_toolcall_args") or []
    if isinstance(positional, list):
        for idx, val in enumerate(positional):
            env[str(idx)] = "" if val is None else str(val)
    env["EVENT_UID"] = _event_uid_from_sidecar(sidecar, Path("x.ics"))  # may be overwritten below
    env["EVENT_SUMMARY"] = str(sidecar.get("a0_name") or "")
    env["CTXID"] = ctxid
    env["NOW_ISO"] = _iso(_utc_now())
    env["OCCURRENCE_ISO"] = _iso(occurrence_start)
    env["ACCOUNT_NAME"] = _account_name_for(ctxid)
    env["KIND"] = kind
    return env


def _account_name_for(ctxid: str) -> str:
    """Best-effort lookup of the unified account label for a context."""
    try:
        cal_dir = CHATS_ROOT / ctxid / CALENDAR_DIRNAME
        for fname in ("account.json", "caldav.json", "exchange.json"):
            payload = _read_json(cal_dir / fname)
            if isinstance(payload, dict):
                # account.json shape
                label = payload.get("label")
                if isinstance(label, str) and label.strip():
                    return label.strip()
                # caldav.json/exchange.json list shape
                accounts = payload.get("accounts")
                if isinstance(accounts, list) and accounts:
                    first = accounts[0]
                    if isinstance(first, dict):
                        label = first.get("label") or first.get("username")
                        if isinstance(label, str) and label.strip():
                            return label.strip()
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Target resolution (self / superordinate / spawn)
# ---------------------------------------------------------------------------


def _resolve_target(
    ctxid: str,
    runtime_cfg: dict[str, Any],
) -> tuple[Any, str, str]:
    """Resolve a dispatch target.

    Returns ``(context_or_none, resolved_target_label, warning_message)`` where
    ``warning_message`` is non-empty when a fallback occurred.
    """
    target = (runtime_cfg.get("target") or "self").lower()
    warning = ""

    try:
        from agent import AgentContext  # type: ignore
    except Exception as exc:
        logger.error("AgentContext import failed: %s", exc)
        return None, target, f"AgentContext import failed: {exc}"

    if target == "self":
        ctx = AgentContext.get(ctxid)
        if ctx is None:
            ctx = _try_load_context(ctxid)
        if ctx is None:
            warning = f"context {ctxid} not loaded"
        return ctx, "self", warning

    if target == "superordinate":
        name = (runtime_cfg.get("name") or "").strip()
        try:
            from usr.plugins.a0_superordinates.helpers.name_registry import (  # type: ignore
                lookup_by_name,
            )
        except Exception as exc:
            return _resolve_target(ctxid, {**runtime_cfg, "target": "self"})[:2] + (
                f"superordinate registry unavailable ({exc}); falling back to self",
            )
        target_ctxid = lookup_by_name(name) if name else None
        if not target_ctxid:
            ctx = AgentContext.get(ctxid) or _try_load_context(ctxid)
            return ctx, "self", f"superordinate '{name}' not found; falling back to self"
        ctx = AgentContext.get(target_ctxid) or _try_load_context(target_ctxid)
        if ctx is None:
            return AgentContext.get(ctxid), "self", f"superordinate '{name}' ({target_ctxid}) not loaded; falling back to self"
        return ctx, f"superordinate:{name}", ""

    if target == "spawn":
        profile = (runtime_cfg.get("profile") or "agent0").strip()
        name = (runtime_cfg.get("name") or "").strip()
        ctx, warn = _spawn_persistent(ctxid, profile, name)
        if ctx is None:
            fallback = AgentContext.get(ctxid) or _try_load_context(ctxid)
            return fallback, "self", warn or "spawn failed; falling back to self"
        return ctx, f"spawn:{profile}:{name}", warn

    # Unknown target — fallback to self.
    ctx = AgentContext.get(ctxid) or _try_load_context(ctxid)
    return ctx, "self", f"unknown target '{target}'; falling back to self"


def _try_load_context(ctxid: str):
    """Try to load a chat context that exists on disk but is not yet in memory.

    Returns the loaded AgentContext or None on failure.
    """
    try:
        from helpers import persist_chat  # type: ignore
        from agent import AgentContext  # type: ignore
    except Exception:
        return None
    try:
        # persist_chat.load_chat populates AgentContext._contexts as a side effect.
        if hasattr(persist_chat, "load_chat"):
            persist_chat.load_chat(ctxid)
        return AgentContext.get(ctxid)
    except Exception as exc:
        logger.debug("persist_chat.load_chat(%s) failed: %s", ctxid, exc)
        return None


def _spawn_persistent(parent_ctxid: str, profile: str, name: str):
    """Spawn a persistent superordinate via the a0_superordinates pattern.

    Returns ``(context, warning)``.
    """
    try:
        from agent import AgentContext  # type: ignore
        from initialize import initialize_agent  # type: ignore
        from helpers import guids, projects  # type: ignore
        from helpers.state_monitor_integration import mark_dirty_all  # type: ignore
        from usr.plugins.a0_superordinates.helpers.name_registry import (  # type: ignore
            name_exists,
            register_name,
        )
        from usr.plugins.a0_superordinates.helpers.hierarchy import add_child  # type: ignore
        from usr.plugins.a0_superordinates.helpers.inheritance import ensure_inheritance_file  # type: ignore
    except Exception as exc:
        return None, f"spawn dependencies unavailable: {exc}"

    chosen_name = name or f"Sched-{profile.capitalize()}-{int(time.time()) % 100000}"
    if name_exists(chosen_name):
        # use a numeric suffix to disambiguate
        n = 2
        while name_exists(f"{chosen_name}{n}"):
            n += 1
        chosen_name = f"{chosen_name}{n}"

    try:
        new_ctxid = guids.generate_id()
    except Exception as exc:
        return None, f"guids.generate_id failed: {exc}"

    if not register_name(chosen_name, new_ctxid):
        return None, f"failed to register name {chosen_name}"

    try:
        config = initialize_agent()
        if profile:
            config.profile = profile
        display_name = f"{chosen_name} ({profile.capitalize()})"
        new_context = AgentContext(config=config, id=new_ctxid, name=display_name)
        new_context.data["sup_parent"] = parent_ctxid
        new_context.data["sup_profile"] = profile
        new_context.data["sup_name"] = chosen_name
        new_context.data["sup_msgme_blocked"] = True
        new_context.data["chat_rename_manual_lock"] = True
        try:
            ensure_inheritance_file(new_ctxid)
        except Exception:
            pass
        try:
            add_child(parent_ctxid, new_ctxid, profile, chosen_name)
        except Exception:
            pass
        try:
            mark_dirty_all(reason="a0_scheduler_runtime.spawn")
        except Exception:
            pass
        return new_context, ""
    except Exception as exc:
        return None, f"spawn failed: {exc}"


# ---------------------------------------------------------------------------
# Dispatch primitives
# ---------------------------------------------------------------------------


def _run_prompt(target_ctx: Any, prompt_text: str) -> str:
    """Send a prompt to a target context via AgentContext.communicate."""
    if not prompt_text:
        return "empty_prompt"
    try:
        from agent import UserMessage  # type: ignore
    except Exception as exc:
        return f"UserMessage import failed: {exc}"
    try:
        target_ctx.communicate(UserMessage(message=prompt_text))
        return "prompt_dispatched"
    except Exception as exc:
        return f"prompt_error: {exc}"


async def _run_toolcall(target_ctx: Any, tool_name: str, tool_args: dict[str, Any]) -> str:
    """Resolve a tool from the target agent's registry and execute it."""
    if not tool_name:
        return "empty_toolname"
    try:
        agent = target_ctx.agent0
    except Exception as exc:
        return f"agent0 missing: {exc}"
    try:
        tool = agent.get_tool(
            name=tool_name,
            method=None,
            args=tool_args or {},
            message="",
            loop_data=None,
        )
    except Exception as exc:
        return f"get_tool_error: {exc}"
    if tool is None:
        return f"tool_not_found: {tool_name}"
    try:
        # We deliberately do NOT call before_execution/after_execution because
        # they write to the target agent's chat history via hist_add_tool_result,
        # which would conflict with the runtime's fire-and-forget semantics.
        response = await tool.execute(**(tool_args or {}))
        msg = getattr(response, "message", str(response))
        return f"tool_ok: {msg[:300]}"
    except Exception as exc:
        return f"tool_error: {exc}"


def _run_action(target_ctx: Any, action_name: str) -> str:
    """Implement the named action against the target context."""
    name = (action_name or "").strip().lower()
    if not name:
        return "empty_action"

    if name == "compact":
        try:
            from plugins._chat_compaction.helpers.compactor import run_compaction  # type: ignore
        except Exception as exc:
            return f"compact_import_error: {exc}"
        try:
            # run_compaction is async; schedule it on the target context.
            target_ctx.run_task(_compact_task_wrapper, target_ctx)
            return "compact_dispatched"
        except Exception as exc:
            return f"compact_error: {exc}"

    if name == "clear":
        try:
            from helpers import persist_chat  # type: ignore
            from helpers.task_scheduler import TaskScheduler  # type: ignore
            from helpers.state_monitor_integration import mark_dirty_all  # type: ignore
        except Exception as exc:
            return f"clear_import_error: {exc}"
        try:
            try:
                TaskScheduler.get().cancel_tasks_by_context(target_ctx.id, terminate_thread=True)
            except Exception:
                pass
            target_ctx.reset()
            persist_chat.save_tmp_chat(target_ctx)
            persist_chat.remove_msg_files(target_ctx.id)
            try:
                mark_dirty_all(reason="a0_scheduler_runtime.clear")
            except Exception:
                pass
            return "clear_ok"
        except Exception as exc:
            return f"clear_error: {exc}"

    return f"unknown_action: {name}"


async def _compact_task_wrapper(target_ctx: Any) -> None:
    try:
        from plugins._chat_compaction.helpers.compactor import run_compaction  # type: ignore
        await run_compaction(target_ctx, True, None)
    except Exception as exc:
        logger.warning("compact task failed: %s", exc)


# ---------------------------------------------------------------------------
# Per-event dispatcher
# ---------------------------------------------------------------------------


async def _dispatch_event(
    ctxid: str,
    ics_path: Path,
    sidecar: dict[str, Any],
    kind: str,
    occurrence_start: datetime,
) -> dict[str, Any]:
    runtime_cfg = _runtime_cfg(sidecar)
    target_ctx, target_label, warning = _resolve_target(ctxid, runtime_cfg)
    fired_at_iso = _iso(_utc_now())

    env = _build_var_env(ctxid, sidecar, kind, occurrence_start)
    env["EVENT_UID"] = _event_uid_from_sidecar(sidecar, ics_path)

    results: list[str] = []
    errors: list[str] = []
    if warning:
        errors.append(warning)

    if target_ctx is None:
        errors.append("no_target_context_available")
        return {
            "target": target_label,
            "result": " | ".join(results) or "no_dispatch",
            "error": " | ".join(errors),
            "fired_at_iso": fired_at_iso,
        }

    for kind_token in runtime_cfg["order"]:
        try:
            if kind_token == "prompt":
                prompts = sidecar.get("a0_prompts") or {}
                raw = prompts.get(kind) if isinstance(prompts, dict) else ""
                if isinstance(raw, str) and raw.strip():
                    sub = _substitute_vars(raw, env)
                    results.append(_run_prompt(target_ctx, sub))
            elif kind_token == "toolcall":
                toolcalls = sidecar.get("a0_toolcalls") or {}
                raw = toolcalls.get(kind) if isinstance(toolcalls, dict) else None
                if isinstance(raw, dict) and raw.get("tool_name"):
                    tool_name = str(raw.get("tool_name") or "").strip()
                    tool_args = raw.get("tool_args") or {}
                    if not isinstance(tool_args, dict):
                        tool_args = {}
                    subbed = _substitute_vars(tool_args, env)
                    results.append(await _run_toolcall(target_ctx, tool_name, subbed))
            elif kind_token == "action":
                actions = sidecar.get("a0_actions") or {}
                raw = actions.get(kind) if isinstance(actions, dict) else ""
                if isinstance(raw, str) and raw.strip():
                    results.append(_run_action(target_ctx, raw))
        except Exception as exc:
            errors.append(f"{kind_token}_exception: {exc}")

    result_summary = " | ".join(r for r in results if r) or "no_blocks_defined"
    error_summary = " | ".join(e for e in errors if e)

    _write_sidecar_last_execution(ics_path, kind, fired_at_iso, result_summary, error_summary)

    return {
        "target": target_label,
        "result": result_summary,
        "error": error_summary,
        "fired_at_iso": fired_at_iso,
    }


# ---------------------------------------------------------------------------
# Tick loop
# ---------------------------------------------------------------------------


async def tick_once(now_iso: str | None = None) -> dict[str, Any]:
    """Run a single tick pass and dispatch any due events."""
    parsed_now: datetime | None = _parse_iso(now_iso) if now_iso else None
    now = parsed_now.astimezone(timezone.utc) if parsed_now else _utc_now()
    prev = _state._prev_tick
    if prev is None:
        # First-ever tick: rewind by maximum grace_seconds we expect.
        prev = now - timedelta(seconds=DEFAULT_RUNTIME["grace_seconds"])
    window_start = prev
    window_end = now

    semaphore = asyncio.Semaphore(_DISPATCH_CONCURRENCY)
    tasks: list[asyncio.Task] = []
    fired_summary: list[dict[str, Any]] = []

    async def _gated(coro):
        async with semaphore:
            return await coro

    for ctxid, ics_path, sidecar in _iter_calendar_files():
        try:
            occurrences = _event_occurrences(ics_path, window_start, window_end)
        except Exception as exc:
            logger.warning("occurrence calc failed for %s: %s", ics_path, exc)
            continue
        if not occurrences:
            continue
        calendar_dir = ics_path.parent
        # ensure fired state is per top-level calendar directory
        # We store fired state at the top calendar/ directory of the context.
        ctx_calendar_dir = CHATS_ROOT / ctxid / CALENDAR_DIRNAME
        state = _load_fired_state(ctx_calendar_dir)
        uid = _event_uid_from_sidecar(sidecar, ics_path)
        runtime_cfg = _runtime_cfg(sidecar)
        on_miss = runtime_cfg["on_miss"]
        grace_seconds = runtime_cfg["grace_seconds"]
        state_dirty = False

        for occ_start, occ_end in occurrences:
            occurrence_iso = _iso(occ_start)
            for kind, boundary in (("start", occ_start), ("end", occ_end)):
                if window_start <= boundary <= window_end or (
                    on_miss in ("fire", "notify") and boundary < window_start and (now - boundary).total_seconds() <= grace_seconds
                ):
                    fire, reason = _should_fire(
                        state, uid, occurrence_iso, kind, boundary, on_miss, grace_seconds, now
                    )
                    if not fire:
                        if reason == "missed_beyond_grace_notify":
                            # Mark notified so we don't keep notifying.
                            _record_fired(state, uid, occurrence_iso, kind, _iso(now), "missed_notify", "")
                            state_dirty = True
                            fired_summary.append({
                                "ctxid": ctxid,
                                "ics": ics_path.relative_to(CHATS_ROOT).as_posix(),
                                "uid": uid,
                                "occurrence": occurrence_iso,
                                "kind": kind,
                                "result": "missed_notify",
                                "error": "",
                                "fired_at": _iso(now),
                            })
                        continue

                    async def _do(
                        ctxid=ctxid,
                        ics_path=ics_path,
                        sidecar=sidecar,
                        kind=kind,
                        occurrence_start=occ_start,
                        occurrence_iso=occurrence_iso,
                        uid=uid,
                        state_ref=state,
                    ):
                        try:
                            outcome = await _dispatch_event(
                                ctxid, ics_path, sidecar, kind, occurrence_start
                            )
                        except Exception as exc:
                            outcome = {
                                "target": "self",
                                "result": "dispatch_exception",
                                "error": str(exc),
                                "fired_at_iso": _iso(_utc_now()),
                            }
                        _record_fired(
                            state_ref,
                            uid,
                            occurrence_iso,
                            kind,
                            outcome["fired_at_iso"],
                            outcome["result"],
                            outcome["error"],
                        )
                        entry = {
                            "ctxid": ctxid,
                            "ics": ics_path.relative_to(CHATS_ROOT).as_posix(),
                            "uid": uid,
                            "occurrence": occurrence_iso,
                            "kind": kind,
                            "target": outcome["target"],
                            "result": outcome["result"],
                            "error": outcome["error"],
                            "fired_at": outcome["fired_at_iso"],
                        }
                        fired_summary.append(entry)
                        _state.record_fired(entry)

                    tasks.append(asyncio.create_task(_gated(_do())))
                    state_dirty = True

        if state_dirty:
            try:
                _save_fired_state(ctx_calendar_dir, state)
            except Exception as exc:
                logger.warning("failed to save fired state for %s: %s", ctx_calendar_dir, exc)

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        # Persist updated states again so dispatch results are durable.
        for ctxid in {entry["ctxid"] for entry in fired_summary}:
            ctx_calendar_dir = CHATS_ROOT / ctxid / CALENDAR_DIRNAME
            state = _load_fired_state(ctx_calendar_dir)
            for entry in fired_summary:
                if entry["ctxid"] != ctxid:
                    continue
                _record_fired(
                    state,
                    entry["uid"],
                    entry["occurrence"],
                    entry["kind"],
                    entry["fired_at"],
                    entry["result"],
                    entry["error"],
                )
            try:
                _save_fired_state(ctx_calendar_dir, state)
            except Exception:
                pass

    _state._prev_tick = now
    _state.last_tick_iso = _iso(now)
    _state.tick_count += 1
    return {"fired": fired_summary, "window": [_iso(window_start), _iso(window_end)]}


async def _tick_loop() -> None:
    logger.info("agent_scheduler_runtime tick loop starting")
    _state.running = True
    _state.started_at_iso = _iso(_utc_now())
    try:
        while True:
            try:
                await tick_once()
            except Exception as exc:
                _state.last_error = f"{type(exc).__name__}: {exc}"
                logger.exception("tick failed: %s", exc)
            # Sleep until the next wall-minute boundary.
            now = time.time()
            delay = _TICK_INTERVAL_SECONDS - (now % _TICK_INTERVAL_SECONDS)
            if delay <= 0:
                delay = _TICK_INTERVAL_SECONDS
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
    finally:
        _state.running = False
        logger.info("agent_scheduler_runtime tick loop stopped")


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------


def start_runtime() -> dict[str, Any]:
    """Idempotent — starts the singleton tick loop if not already running.

    In normal WebUI/API usage this function is called from synchronous code with
    no running asyncio loop.  In that case the runtime owns a daemon-thread event
    loop.  We wait briefly for that thread to create the task and let
    ``_tick_loop`` set ``_state.running`` so the immediately returned status does
    not incorrectly say "not started".
    """
    ready: threading.Event | None = None
    with _state._lock:
        if _state.task is not None and not _state.task.done():
            return runtime_status()
        if _state.thread is not None and _state.thread.is_alive() and _state.loop is not None:
            return runtime_status()

        _state.running = False
        _state.last_error = ""
        _state.ready_event = threading.Event()
        ready = _state.ready_event

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        try:
            if running_loop is not None and running_loop.is_running():
                _state.loop = running_loop
                _state.task = running_loop.create_task(_tick_loop())
                running_loop.call_soon(lambda: ready.set() if ready is not None else None)
            else:
                _start_thread_loop_locked()
        except Exception as exc:
            _state.last_error = f"start failed: {type(exc).__name__}: {exc}"
            logger.exception("failed to start runtime: %s", exc)
            if ready is not None:
                ready.set()

    if ready is not None:
        ready.wait(timeout=2.0)
    return runtime_status()


def _start_thread_loop_locked() -> None:
    """Start a dedicated daemon-thread asyncio loop.

    Caller must hold ``_state._lock``.  The thread owns ``_state.loop`` and
    ``_state.task`` so status/stop always point at the actual running loop.
    """
    ready = _state.ready_event

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            task = loop.create_task(_tick_loop())
            with _state._lock:
                _state.loop = loop
                _state.task = task
            # Run the loop briefly enough for _tick_loop to set _state.running.
            loop.call_soon(lambda: ready.set() if ready is not None else None)
            loop.run_forever()
        except Exception as exc:
            _state.last_error = f"thread loop failed: {type(exc).__name__}: {exc}"
            logger.exception("runtime thread loop failed: %s", exc)
            if ready is not None:
                ready.set()
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for pending_task in pending:
                    pending_task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass
            with _state._lock:
                if _state.loop is loop:
                    _state.loop = None
                _state.task = None
                _state.running = False

    thread = threading.Thread(target=_runner, name="a0-scheduler-runtime", daemon=True)
    _state.thread = thread
    thread.start()


def _start_thread_loop() -> None:
    """Backward-compatible internal wrapper for older tests/imports."""
    with _state._lock:
        _start_thread_loop_locked()


def stop_runtime() -> dict[str, Any]:
    """Stop the singleton tick loop. Safe to call when not running."""
    with _state._lock:
        task = _state.task
        loop = _state.loop
    if task is not None and loop is not None:
        try:
            loop.call_soon_threadsafe(task.cancel)
            loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass
    return runtime_status()


def runtime_status() -> dict[str, Any]:
    task = _state.task
    thread = _state.thread
    task_alive = bool(task is not None and not task.done())
    thread_alive = bool(thread is not None and thread.is_alive())
    return {
        "running": bool(task_alive and _state.running),
        "starting": bool((task_alive or thread_alive) and not _state.running),
        "thread_alive": thread_alive,
        "task_alive": task_alive,
        "started_at": _state.started_at_iso,
        "last_tick_iso": _state.last_tick_iso,
        "tick_count": _state.tick_count,
        "last_error": _state.last_error,
        "fired_recent": list(_state.fired_recent[:20]),
    }


def _resolve_ctx_ics(ctxid: str, ics_path: str | Path) -> Path | None:
    """Coerce a path string into an absolute path inside the context's calendar dir."""
    if not ctxid:
        return None
    base = CHATS_ROOT / ctxid / CALENDAR_DIRNAME
    base = base.resolve()
    if not base.is_dir():
        return None
    candidate = Path(str(ics_path))
    if not candidate.is_absolute():
        candidate = (base / candidate).resolve()
    else:
        candidate = candidate.resolve()
    try:
        candidate.relative_to(base)
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return candidate


async def run_event_now(
    ctxid: str,
    ics_path: str | Path,
    kind: str = "start",
    occurrence_iso: str | None = None,
) -> dict[str, Any]:
    """Manually fire one block of an event.

    Note: this bypasses the firing-state check (no idempotency); it is meant
    for the "Run now" UI button.
    """
    path = _resolve_ctx_ics(ctxid, ics_path)
    if path is None:
        return {"ok": False, "error": "ics_path not found inside context calendar dir"}
    sidecar = _read_json(_sidecar_path_for(path))
    if not _has_runtime_payload(sidecar):
        return {"ok": False, "error": "sidecar has no runtime-firable blocks"}
    if kind not in ("start", "end"):
        return {"ok": False, "error": "kind must be 'start' or 'end'"}
    occ_dt = _parse_iso(occurrence_iso) if occurrence_iso else _utc_now()
    if occ_dt is None:
        return {"ok": False, "error": "invalid occurrence_iso"}
    occ_dt_utc = occ_dt.astimezone(timezone.utc)
    outcome = await _dispatch_event(ctxid, path, sidecar, kind, occ_dt_utc)
    # Record in fired history.
    ctx_calendar_dir = CHATS_ROOT / ctxid / CALENDAR_DIRNAME
    state = _load_fired_state(ctx_calendar_dir)
    uid = _event_uid_from_sidecar(sidecar, path)
    iso = _iso(occ_dt_utc)
    _record_fired(state, uid, iso, kind, outcome["fired_at_iso"], outcome["result"], outcome["error"])
    _save_fired_state(ctx_calendar_dir, state)
    _state.record_fired({
        "ctxid": ctxid,
        "ics": path.relative_to(CHATS_ROOT).as_posix(),
        "uid": uid,
        "occurrence": iso,
        "kind": kind,
        "target": outcome["target"],
        "result": outcome["result"],
        "error": outcome["error"],
        "fired_at": outcome["fired_at_iso"],
        "manual": True,
    })
    return {"ok": not outcome.get("error"), **outcome}


def list_fired_history(
    ctxid: str,
    ics_path: str | Path | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return recent firings for an event, newest first."""
    ctx_calendar_dir = CHATS_ROOT / ctxid / CALENDAR_DIRNAME
    state = _load_fired_state(ctx_calendar_dir)
    if not state:
        return []
    uid: str | None = None
    if ics_path is not None:
        path = _resolve_ctx_ics(ctxid, ics_path)
        if path is None:
            return []
        sidecar = _read_json(_sidecar_path_for(path))
        uid = _event_uid_from_sidecar(sidecar, path)

    entries: list[dict[str, Any]] = []
    for u, occurrences in state.items():
        if uid is not None and u != uid:
            continue
        for occ_iso, kinds in occurrences.items():
            for kind, rec in kinds.items():
                if not isinstance(rec, dict):
                    continue
                entries.append({
                    "uid": u,
                    "occurrence": occ_iso,
                    "kind": kind,
                    "fired_at": rec.get("fired_at", ""),
                    "result": rec.get("result", ""),
                    "error": rec.get("error", ""),
                })
    entries.sort(key=lambda e: e.get("fired_at", ""), reverse=True)
    return entries[: max(1, int(limit or 20))]


def clear_fired_history(ctxid: str, ics_path: str | Path | None = None) -> dict[str, Any]:
    ctx_calendar_dir = CHATS_ROOT / ctxid / CALENDAR_DIRNAME
    if not ctx_calendar_dir.is_dir():
        return {"ok": False, "error": "context calendar dir not found"}
    if ics_path is None:
        try:
            (ctx_calendar_dir / FIRED_STATE_FILENAME).unlink(missing_ok=True)
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "cleared": "all"}
    path = _resolve_ctx_ics(ctxid, ics_path)
    if path is None:
        return {"ok": False, "error": "ics_path not found"}
    sidecar = _read_json(_sidecar_path_for(path))
    uid = _event_uid_from_sidecar(sidecar, path)
    state = _load_fired_state(ctx_calendar_dir)
    if uid in state:
        state.pop(uid, None)
        _save_fired_state(ctx_calendar_dir, state)
        return {"ok": True, "cleared": uid}
    return {"ok": True, "cleared": ""}


__all__ = [
    "start_runtime",
    "stop_runtime",
    "runtime_status",
    "tick_once",
    "run_event_now",
    "list_fired_history",
    "clear_fired_history",
]
