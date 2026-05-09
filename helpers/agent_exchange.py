"""Exchange (EWS) calendar helpers mirroring the CalDAV contract.

This module provides the public Exchange equivalents of the helpers in
``agent_caldav.py``. Each Agent context can register one Exchange account,
discover its calendar folders, select a folder, and sync its CalendarItem
objects into local ICS files under the per-context calendar dir.

The singleton account is persisted in
``/a0/usr/chats/<ctxid>/calendar/exchange.json``. Synced ICS files are written
into a dedicated subdirectory so they cannot collide with CalDAV-synced files
in the same context.

# TODO(oauth2): exchangelib supports OAuth2/MSAL via
# ``OAuth2AuthorizationCodeCredentials``. This first pass intentionally limits
# itself to legacy basic-auth Credentials so it works without tenant-side admin
# consent; a future pass should layer OAuth2 on top of this scaffolding.
# TODO(graph): Microsoft Graph (calendars/events) is the modern API for
# Microsoft 365 tenants that have disabled basic auth. Implementing it requires
# OAuth2 first, so it is deferred for the same reason.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

log = logging.getLogger("agent_exchange")

EXCHANGE_FILENAME = "exchange.json"
SYNC_STATE_FILENAME = ".a0-exchange-sync-state.json"
DEFAULT_EXCHANGE_SERVER = "outlook.office365.com"
REGISTRY_VERSION = 1

# Sync window for CalendarItem fetches.
SYNC_PAST_DAYS = 30
SYNC_FUTURE_DAYS = 365


# ---------------------------------------------------------------------------
# Late imports to avoid circular import with agent_calendar.
# ---------------------------------------------------------------------------

def _calendar_helpers():
    from . import agent_calendar  # type: ignore  # late import
    return agent_calendar


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def exchange_path(ctxid: str, create: bool = True) -> Path:
    return _calendar_helpers().context_calendar_dir(ctxid, create=create) / EXCHANGE_FILENAME


def _empty_registry() -> dict[str, Any]:
    return {"version": REGISTRY_VERSION, "account": None}


def _normalize_account(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    if not entry.get("id") or not entry.get("server"):
        return None
    entry.setdefault("label", entry.get("server") or "")
    entry.setdefault("username", "")
    entry.setdefault("password", "")
    entry.setdefault("kind", "exchange")
    entry.setdefault("calendars", [])
    entry.setdefault("selected_calendar_id", "")
    entry.setdefault("selected_calendar_name", "")
    entry.setdefault("status", "unverified")
    entry.setdefault("last_error", "")
    entry.setdefault("last_verified", "")
    entry.setdefault("last_synced", "")
    entry.setdefault("last_item_count", 0)
    return entry


def public_account(account: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return account dict with the password stripped."""
    if not isinstance(account, dict):
        return None
    public = {k: v for k, v in account.items() if k != "password"}
    public["has_password"] = bool(account.get("password"))
    return public


def _first_legacy_account(data: dict[str, Any]) -> dict[str, Any] | None:
    direct = _normalize_account(data.get("account") if isinstance(data, dict) else None)
    if direct is not None:
        return direct
    accounts = data.get("accounts") if isinstance(data, dict) else None
    if isinstance(accounts, list):
        for entry in accounts:
            normalized = _normalize_account(entry)
            if normalized is not None:
                return normalized
    return None


def load_exchange_registry(ctxid: str, create: bool = False) -> dict[str, Any]:
    path = exchange_path(ctxid, create=create)
    if not path.exists():
        return _empty_registry()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {
        "version": int(data.get("version") or REGISTRY_VERSION),
        "account": _first_legacy_account(data),
    }


def save_exchange_registry(ctxid: str, registry: dict[str, Any]) -> None:
    path = exchange_path(ctxid, create=True)
    singleton = {
        "version": int(registry.get("version") or REGISTRY_VERSION),
        "account": _normalize_account(registry.get("account")),
    }
    with NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
        json.dump(singleton, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def get_exchange_account(ctxid: str) -> dict[str, Any] | None:
    return public_account(load_exchange_registry(ctxid).get("account"))


def list_exchange_accounts(ctxid: str) -> list[dict[str, Any]]:
    account = get_exchange_account(ctxid)
    return [account] if account else []


def exchange_account_entry(ctxid: str) -> dict[str, Any] | None:
    try:
        return load_exchange_registry(ctxid, create=False).get("account")
    except Exception:
        return None


def has_active_exchange_source(ctxid: str) -> bool:
    """True if the Exchange account has a selected calendar folder."""
    acc = exchange_account_entry(ctxid)
    return bool(isinstance(acc, dict) and str(acc.get("selected_calendar_id") or "").strip())


def _find_account(ctxid: str, account_id: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    registry = load_exchange_registry(ctxid, create=False)
    account = registry.get("account")
    if not isinstance(account, dict):
        raise ValueError("no Exchange account configured")
    clean_id = str(account_id or "").strip()
    if clean_id and clean_id != str(account.get("id") or ""):
        raise ValueError(f"exchange account not found: {clean_id}")
    return registry, account


def _save_with_indicator(ctxid: str, registry: dict[str, Any]) -> None:
    cal = _calendar_helpers()
    save_exchange_registry(ctxid, registry)
    cal.persist_calendar_indicator(ctxid)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _normalize_server(value: str) -> str:
    clean = str(value or "").strip()
    if not clean:
        raise ValueError("server is required")
    # exchangelib accepts a bare host (e.g. ``outlook.office365.com``); tolerate a
    # full URL by stripping scheme/path so users can paste either form.
    if "://" in clean:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(clean)
            if parsed.netloc:
                clean = parsed.netloc
        except Exception:
            pass
    clean = clean.split("/", 1)[0]
    if not clean:
        raise ValueError("server must include a host")
    return clean


# ---------------------------------------------------------------------------
# Account CRUD
# ---------------------------------------------------------------------------

def add_exchange_account(
    ctxid: str,
    label: str,
    server: str,
    username: str,
    password: str,
) -> dict[str, Any] | None:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    clean_server = _normalize_server(server)
    clean_label = (str(label or "").strip()) or clean_server
    clean_username = str(username or "").strip()
    clean_password = str(password or "")
    previous = exchange_account_entry(ctxid) or {}
    if not clean_password and isinstance(previous, dict):
        clean_password = str(previous.get("password") or "")
    if not clean_username:
        raise ValueError("username is required")
    if not clean_password:
        raise ValueError("password is required")

    registry = load_exchange_registry(ctxid, create=True)
    entry = _normalize_account({
        "id": str(previous.get("id") or uuid.uuid4().hex[:12]),
        "label": clean_label,
        "server": clean_server,
        "username": clean_username,
        "password": clean_password,
        "kind": "exchange",
        "created": previous.get("created") or cal.iso_now(),
        "updated": cal.iso_now(),
        # Replacing the account clears stale discovery/selection from the old
        # provider until the user re-tests and selects a folder again.
        "calendars": [],
        "selected_calendar_id": "",
        "selected_calendar_name": "",
        "status": "unverified",
        "last_error": "",
        "last_verified": "",
        "last_synced": previous.get("last_synced") or "",
        "last_item_count": int(previous.get("last_item_count") or 0),
    })
    assert entry is not None
    registry["account"] = entry
    save_exchange_registry(ctxid, registry)
    cal.persist_calendar_indicator(ctxid)
    return public_account(entry)


def set_exchange_account(
    ctxid: str,
    label: str,
    server: str,
    username: str,
    password: str,
) -> dict[str, Any] | None:
    """Create or replace the singleton Exchange account for a context."""
    return add_exchange_account(ctxid, label, server, username, password)


def remove_exchange_account(ctxid: str, account_id: str | None = None) -> bool:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry = load_exchange_registry(ctxid, create=False)
    account = registry.get("account")
    if not isinstance(account, dict):
        return False
    clean_id = str(account_id or "").strip()
    if clean_id and clean_id != str(account.get("id") or ""):
        return False
    registry["account"] = None
    save_exchange_registry(ctxid, registry)
    cal.persist_calendar_indicator(ctxid)
    return True


# ---------------------------------------------------------------------------
# Network operations
# ---------------------------------------------------------------------------

def _connect_account(account: dict[str, Any]):
    from exchangelib import Account, Configuration, Credentials, DELEGATE  # local import

    credentials = Credentials(
        username=str(account.get("username") or ""),
        password=str(account.get("password") or ""),
    )
    config = Configuration(
        server=str(account.get("server") or ""),
        credentials=credentials,
    )
    return Account(
        primary_smtp_address=str(account.get("username") or ""),
        config=config,
        autodiscover=False,
        access_type=DELEGATE,
    )


def _serialize_calendar_folder(folder, root_url: str = "") -> dict[str, Any]:
    name = str(getattr(folder, "name", "") or "")
    folder_id = ""
    try:
        folder_id = str(getattr(folder, "id", "") or "")
    except Exception:
        folder_id = ""
    return {
        "name": name or folder_id or "Calendar",
        "id": folder_id,
        # exchangelib does not expose a browser URL; use the folder id as a stable
        # "url-equivalent" so callers can pass a single string token around.
        "url": folder_id,
    }


def _enumerate_calendar_folders(account) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    root = None
    try:
        root = account.calendar
    except Exception as exc:
        log.warning("exchange: failed to access account.calendar: %s", exc)
        return out
    if root is None:
        return out
    try:
        out.append(_serialize_calendar_folder(root))
        seen_ids.add(str(getattr(root, "id", "") or ""))
    except Exception:
        pass

    # Walk children that look like calendar folders.
    walker = getattr(root, "walk", None)
    children: list[Any] = []
    if callable(walker):
        try:
            children = list(walker())
        except Exception as exc:
            log.warning("exchange: calendar folder walk failed: %s", exc)
    else:
        try:
            children = list(getattr(root, "children", []) or [])
        except Exception:
            children = []

    for folder in children:
        try:
            fid = str(getattr(folder, "id", "") or "")
            if fid in seen_ids:
                continue
            # Filter to calendar-supporting folders. exchangelib exposes
            # ``CalendarItem`` as the supported_item_models on Calendar folders.
            cls = getattr(folder, "CONTAINER_CLASS", "") or ""
            supported = getattr(folder, "supported_item_models", None)
            is_calendar = False
            if cls and "Appointment" in str(cls):
                is_calendar = True
            else:
                try:
                    from exchangelib.items import CalendarItem  # type: ignore
                    if supported and CalendarItem in supported:
                        is_calendar = True
                except Exception:
                    pass
            if not is_calendar:
                continue
            seen_ids.add(fid)
            out.append(_serialize_calendar_folder(folder))
        except Exception as exc:
            log.warning("exchange: skipping calendar folder due to error: %s", exc)
    return out


def _resolve_selected_folder(account, folder_id: str):
    """Return the folder object matching ``folder_id`` from ``account.calendar``."""
    target = str(folder_id or "").strip()
    if not target:
        return account.calendar
    root = account.calendar
    if str(getattr(root, "id", "") or "") == target:
        return root
    walker = getattr(root, "walk", None)
    if callable(walker):
        try:
            for folder in walker():
                if str(getattr(folder, "id", "") or "") == target:
                    return folder
        except Exception as exc:
            log.warning("exchange: folder lookup walk failed: %s", exc)
    # Fallback to root if id no longer resolves (e.g. folder deleted upstream).
    return root


def test_exchange_account(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    """Verify credentials and discover calendar folders."""
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry, account = _find_account(ctxid, account_id)
    try:
        ews_account = _connect_account(account)
        # Trigger a lightweight call to confirm credentials work.
        try:
            _ = ews_account.root  # type: ignore[attr-defined]
        except Exception:
            # Some installations defer root resolution until first use; fall back
            # to enumerating calendar folders below to verify connectivity.
            pass
        calendars = _enumerate_calendar_folders(ews_account)
        account["calendars"] = calendars
        account["status"] = "ok"
        account["last_error"] = ""
        account["last_verified"] = cal.iso_now()
        if not account.get("selected_calendar_id") and len(calendars) >= 1:
            account["selected_calendar_id"] = calendars[0]["id"]
            account["selected_calendar_name"] = calendars[0]["name"]
        _save_with_indicator(ctxid, registry)
        return {
            "ok": True,
            "account": public_account(account),
            "calendars": calendars,
        }
    except Exception as exc:
        account["status"] = "error"
        account["last_error"] = str(exc)
        save_exchange_registry(ctxid, registry)
        return {"ok": False, "error": str(exc), "account": public_account(account)}


def list_exchange_calendars(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    return test_exchange_account(ctxid, account_id)


def select_exchange_calendar(
    ctxid: str,
    account_id: str | None = None,
    calendar_id: str = "",
) -> dict[str, Any]:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry, account = _find_account(ctxid, account_id)
    clean_id = str(calendar_id or "").strip()
    if not clean_id:
        raise ValueError("calendar_id is required")
    matched = None
    for col in account.get("calendars") or []:
        if str(col.get("id") or "") == clean_id or str(col.get("url") or "") == clean_id:
            matched = col
            break
    if matched is None:
        matched = {"id": clean_id, "url": clean_id, "name": clean_id}
    account["selected_calendar_id"] = clean_id
    account["selected_calendar_name"] = matched.get("name") or clean_id
    _save_with_indicator(ctxid, registry)
    return {"ok": True, "account": public_account(account)}


# ---------------------------------------------------------------------------
# ICS translation and sync
# ---------------------------------------------------------------------------

def _sanitize_label_token(value: str) -> str:
    base = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "")).strip("._-")
    return base[:32] or "exchange"


def _account_sync_subdir_name(account: dict[str, Any]) -> str:
    label = _sanitize_label_token(account.get("label") or account.get("server") or "exchange")
    raw_id = str(account.get("id") or "").strip().replace("-", "")
    short = raw_id[:8] if raw_id else uuid.uuid4().hex[:8]
    return f"{label}-{short}_exchange"


def _account_sync_dir(ctxid: str, account: dict[str, Any], create: bool = True) -> Path:
    cal = _calendar_helpers()
    base = cal.context_calendar_dir(ctxid, create=create)
    sub = base / _account_sync_subdir_name(account)
    if create:
        sub.mkdir(parents=True, exist_ok=True)
    return sub


def _sync_state_path(ctxid: str, account: dict[str, Any]) -> Path:
    return _account_sync_dir(ctxid, account, create=True) / SYNC_STATE_FILENAME


def _load_sync_state(ctxid: str, account: dict[str, Any]) -> dict[str, Any]:
    path = _sync_state_path(ctxid, account)
    if not path.exists():
        return {"version": 1, "items": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    items = data.get("items")
    if not isinstance(items, dict):
        items = {}
    return {"version": int(data.get("version") or 1), "items": items}


def _save_sync_state(ctxid: str, account: dict[str, Any], state: dict[str, Any]) -> None:
    path = _sync_state_path(ctxid, account)
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
        json.dump(state, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _filename_for_uid(uid: str, summary: str = "") -> str:
    label = re.sub(r"[^A-Za-z0-9_. -]+", "_", str(summary or "")).strip(" ._")
    if len(label) > 48:
        label = label[:48].strip(" ._")
    suffix = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(uid or uuid.uuid4().hex)).strip("._-")[:40]
    base = f"{label}-{suffix}" if label and suffix else (suffix or "exchange-item")
    if not base.lower().endswith(".ics"):
        base = f"{base}.ics"
    return base


def _ensure_aware(dt) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def _calendar_item_to_ics(item) -> tuple[str, str, str]:
    """Translate an exchangelib CalendarItem to (uid, ics_text, summary)."""
    from icalendar import Calendar, Event  # local import

    uid_raw = getattr(item, "uid", None) or getattr(item, "item_id", None) or ""
    uid = str(uid_raw) if uid_raw else f"a0-exchange-{uuid.uuid4().hex}"

    cal = Calendar()
    cal.add("prodid", "-//Agent Zero//A0 Scheduler Exchange//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")

    event = Event()
    event.add("uid", uid)

    summary = str(getattr(item, "subject", "") or "") or "(no title)"
    event.add("summary", summary)

    description = str(getattr(item, "text_body", "") or getattr(item, "body", "") or "")
    if description:
        event.add("description", description)

    start = _ensure_aware(getattr(item, "start", None))
    end = _ensure_aware(getattr(item, "end", None))
    now = datetime.now(tz=timezone.utc)
    if start is None:
        start = now
    if end is None:
        end = start + timedelta(hours=1)
    event.add("dtstart", start)
    event.add("dtend", end)
    event.add("dtstamp", now)

    last_modified = _ensure_aware(
        getattr(item, "last_modified_time", None)
        or getattr(item, "datetime_received", None)
        or getattr(item, "datetime_sent", None)
    )
    if last_modified is not None:
        event.add("last-modified", last_modified)

    location = str(getattr(item, "location", "") or "")
    if location:
        event.add("location", location)

    cal.add_component(event)
    raw = cal.to_ical()
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw or "")
    return uid, text, summary


def _fetch_remote_calendar_items(account: dict[str, Any]) -> list[Any]:
    ews_account = _connect_account(account)
    folder = _resolve_selected_folder(ews_account, account.get("selected_calendar_id") or "")
    if folder is None:
        return []
    tz = getattr(ews_account, "default_timezone", None) or timezone.utc
    now = datetime.now(tz=tz) if hasattr(tz, "utcoffset") else datetime.now(tz=timezone.utc)
    start = now - timedelta(days=SYNC_PAST_DAYS)
    end = now + timedelta(days=SYNC_FUTURE_DAYS)
    try:
        # ``view`` expands recurring events to instances; ``filter`` returns the
        # master items. Prefer ``filter`` for simpler 1:1 UID mapping into ICS.
        from exchangelib import Q  # type: ignore
        items = list(
            folder.filter(
                Q(start__gte=start) | Q(end__gte=start)
            ).filter(start__lte=end)
        )
        return items
    except Exception as exc:
        log.warning("exchange: filter failed (%s); falling back to view()", exc)
        try:
            return list(folder.view(start=start, end=end))
        except Exception as exc2:
            log.warning("exchange: view() also failed: %s", exc2)
            return []


def _write_ics_file(path: Path, text: str, modified: datetime | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", dir=str(path.parent), delete=False) as tmp:
        tmp.write(text)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
    if modified is not None:
        try:
            ts = modified.timestamp()
            os.utime(path, (ts, ts))
        except Exception:
            pass


def sync_exchange_ics_files(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    """Fetch CalendarItem objects from Exchange and write them as local ICS.

    Maintains a sidecar JSON state file so subsequent syncs can delete or update
    files for items that disappeared or changed remotely. Files are written into
    a per-account subdirectory so they cannot collide with CalDAV-synced files.
    """
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry, account = _find_account(ctxid, account_id)
    if not account.get("selected_calendar_id"):
        return {"ok": False, "error": "no Exchange calendar selected", "account": public_account(account)}

    sync_dir = _account_sync_dir(ctxid, account, create=True)
    state = _load_sync_state(ctxid, account)
    previous_items: dict[str, Any] = state.get("items") or {}
    new_items: dict[str, Any] = {}

    downloaded = 0
    updated = 0
    deleted = 0
    errors: list[str] = []
    sample: list[dict[str, Any]] = []
    seen_uids: set[str] = set()

    try:
        items = _fetch_remote_calendar_items(account)
    except Exception as exc:
        account["status"] = "error"
        account["last_error"] = str(exc)
        save_exchange_registry(ctxid, registry)
        return {"ok": False, "error": str(exc), "account": public_account(account)}

    for item in items:
        try:
            uid, ics_text, summary = _calendar_item_to_ics(item)
            if uid in seen_uids:
                continue
            seen_uids.add(uid)
            modified = _ensure_aware(getattr(item, "last_modified_time", None))
            previous = previous_items.get(uid) or {}
            filename = previous.get("filename") or _filename_for_uid(uid, summary)
            target = sync_dir / filename
            previous_modified = previous.get("modified") or ""
            current_modified = modified.isoformat().replace("+00:00", "Z") if modified else ""
            if (
                target.exists()
                and previous_modified
                and current_modified
                and previous_modified == current_modified
            ):
                # Unchanged: keep existing file, just refresh state entry.
                new_items[uid] = {
                    "filename": filename,
                    "modified": current_modified,
                    "summary": summary,
                }
                continue
            _write_ics_file(target, ics_text, modified)
            if previous:
                updated += 1
            else:
                downloaded += 1
            new_items[uid] = {
                "filename": filename,
                "modified": current_modified,
                "summary": summary,
            }
            if len(sample) < 8:
                sample.append({"uid": uid, "summary": summary, "filename": filename})
        except Exception as exc:
            errors.append(f"{getattr(item, 'subject', '?')}: {exc}")
            log.warning("exchange sync: item translation failed: %s", exc)
            continue

    # Delete local files for items that disappeared remotely.
    for old_uid, info in previous_items.items():
        if old_uid in new_items:
            continue
        filename = (info or {}).get("filename") or ""
        if not filename:
            continue
        path = sync_dir / filename
        try:
            if path.exists():
                path.unlink()
                deleted += 1
        except Exception as exc:
            errors.append(f"delete {filename}: {exc}")

    state["items"] = new_items
    _save_sync_state(ctxid, account, state)

    account["status"] = "ok"
    account["last_error"] = "; ".join(errors[:4]) if errors else ""
    account["last_synced"] = cal.iso_now()
    account["last_item_count"] = len(new_items)
    save_exchange_registry(ctxid, registry)
    cal.persist_calendar_indicator(ctxid)

    return {
        "ok": True,
        "account": public_account(account),
        "sync": {
            "ok": True,
            "calendar_id": account.get("selected_calendar_id") or "",
            "calendar_name": account.get("selected_calendar_name") or "",
            "sync_dir": str(sync_dir),
            "remote_count": len(seen_uids),
            "downloaded": downloaded,
            "updated": updated,
            "deleted_local": deleted,
            "errors": errors,
            "sample": sample,
        },
        "sync_status": get_exchange_sync_status(ctxid)["sync_status"],
    }


def get_exchange_sync_status(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    """Return persisted Exchange sync status."""
    try:
        _registry, account = _find_account(ctxid, account_id)
    except Exception:
        return {"ok": True, "sync_status": {"state": "empty"}}
    last_synced = str(account.get("last_synced") or "")
    item_count = int(account.get("last_item_count") or 0)
    state = "never" if not last_synced else "ok"
    if account.get("status") == "error":
        state = "error"
    age_seconds: float | None = None
    if last_synced:
        try:
            ts = last_synced.replace("Z", "+00:00")
            then = datetime.fromisoformat(ts)
            if then.tzinfo is None:
                then = then.replace(tzinfo=timezone.utc)
            age_seconds = max(0.0, (datetime.now(timezone.utc) - then).total_seconds())
        except Exception:
            age_seconds = None
    return {
        "ok": True,
        "sync_status": {
            "state": state,
            "last_success_at": last_synced,
            "last_error": account.get("last_error") or "",
            "item_count": item_count,
            "age_seconds": age_seconds,
            "calendar_id": account.get("selected_calendar_id") or "",
            "calendar_name": account.get("selected_calendar_name") or "",
        },
    }
