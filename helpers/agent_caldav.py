"""CalDAV client helpers for per-agent CalDAV accounts.

This module replaces the old ICS-subscription logic.  Each Agent context can
register one CalDAV account, discover its calendar collections, select an
active collection, and read/write events on it via PUT/DELETE.

The singleton account is persisted in ``/a0/usr/chats/<ctxid>/calendar/caldav.json``.
Passwords are stored in plaintext alongside the account record because Agent
Zero does not yet expose a per-context secret store; rotate or remove the
account if this is a concern.  Discovery and event operations use the ``caldav`` PyPI
library, which builds on ``requests``/``niquests``, ``lxml``, ``vobject`` and
``icalendar``.

Provider quirks (kept as caveats, not enforced here):

- **Google**: usually requires app passwords or OAuth; plain Google account
  passwords typically fail.
- **iCloud**: requires app-specific passwords; the server URL is
  ``https://caldav.icloud.com``.
- **Nextcloud / Radicale / SOGo**: usually work with the base URL; Nextcloud's
  canonical entry point is ``https://<host>/remote.php/dav``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import urlparse

log = logging.getLogger("agent_caldav")

CALDAV_FILENAME = "caldav.json"
REGISTRY_VERSION = 1
A0_DESCRIPTION_JSON_MARKER = '!{"a0_name":'
A0_DESCRIPTION_JSON_MARKER_ESCAPED = '!{\\"a0_name\\":'


# ---------------------------------------------------------------------------
# Late imports to avoid circular import with agent_calendar.
# ---------------------------------------------------------------------------

def _calendar_helpers():
    from . import agent_calendar  # type: ignore  # late import
    return agent_calendar


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def caldav_path(ctxid: str, create: bool = True) -> Path:
    return _calendar_helpers().context_calendar_dir(ctxid, create=create) / CALDAV_FILENAME


def _empty_registry() -> dict[str, Any]:
    return {"version": REGISTRY_VERSION, "account": None}


def _normalize_account(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    if not entry.get("id") or not entry.get("server_url"):
        return None
    entry.setdefault("label", entry.get("server_url") or "")
    entry.setdefault("username", "")
    entry.setdefault("password", "")
    entry.setdefault("kind", "caldav")
    entry["webui_calendar_url"] = normalize_webui_calendar_url(
        entry.get("webui_calendar_url")
        if entry.get("webui_calendar_url") is not None
        else entry.get("webuiCalendarUrl") or "",
        reject_unsafe=False,
    )
    entry.setdefault("collections", [])
    entry.setdefault("selected_collection_url", "")
    entry.setdefault("selected_collection_name", "")
    entry.setdefault("status", "unverified")
    entry.setdefault("last_error", "")
    entry.setdefault("last_verified", "")
    return entry


def _first_legacy_account(data: dict[str, Any]) -> dict[str, Any] | None:
    """Return the first valid account from old multi-account data.

    Requirement: per agent/context there is exactly zero or one CalDAV account.
    Older list-shaped data is tolerated defensively and normalized to the first
    valid account so stale/malformed registries do not crash the UI/API.
    """
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


def load_caldav_registry(ctxid: str, create: bool = False) -> dict[str, Any]:
    path = caldav_path(ctxid, create=create)
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


def save_caldav_registry(ctxid: str, registry: dict[str, Any]) -> None:
    path = caldav_path(ctxid, create=True)
    singleton = {
        "version": int(registry.get("version") or REGISTRY_VERSION),
        "account": _normalize_account(registry.get("account")),
    }
    with NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
        json.dump(singleton, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def public_account(account: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return an account dict with the password stripped."""
    if not isinstance(account, dict):
        return None
    public = {k: v for k, v in account.items() if k != "password"}
    public["has_password"] = bool(account.get("password"))
    return public


def get_caldav_account(ctxid: str) -> dict[str, Any] | None:
    return public_account(load_caldav_registry(ctxid).get("account"))


def list_caldav_accounts(ctxid: str) -> list[dict[str, Any]]:
    """Backward-compatible one-item list view for older UI/API callers."""
    account = get_caldav_account(ctxid)
    return [account] if account else []


def caldav_account_entry(ctxid: str) -> dict[str, Any] | None:
    """Internal-use raw entry (with password) for indicator/connection logic."""
    try:
        return load_caldav_registry(ctxid, create=False).get("account")
    except Exception:
        return None


def caldav_account_entries(ctxid: str) -> list[dict[str, Any]]:
    """Backward-compatible raw one-item list for older internal callers."""
    account = caldav_account_entry(ctxid)
    return [account] if account else []


def has_active_caldav_source(ctxid: str) -> bool:
    """True if the singleton account has a selected collection URL."""
    acc = caldav_account_entry(ctxid)
    return bool(isinstance(acc, dict) and str(acc.get("selected_collection_url") or "").strip())


def _find_account(ctxid: str, account_id: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    registry = load_caldav_registry(ctxid, create=False)
    account = registry.get("account")
    if not isinstance(account, dict):
        raise ValueError("no CalDAV account configured")
    clean_id = str(account_id or "").strip()
    # The singleton API does not require an id.  If an old caller supplies one,
    # tolerate it only when it matches the configured singleton.
    if clean_id and clean_id != str(account.get("id") or ""):
        raise ValueError(f"caldav account not found: {clean_id}")
    return registry, account


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def normalize_server_url(url: str) -> str:
    clean = str(url or "").strip()
    if not clean:
        raise ValueError("server URL is required")
    if not (clean.startswith("http://") or clean.startswith("https://")):
        clean = "https://" + clean
    parsed = urlparse(clean)
    if not parsed.netloc:
        raise ValueError("server URL must include a host")
    return clean


def normalize_webui_calendar_url(url: str, *, reject_unsafe: bool = True) -> str:
    """Normalize the optional browser-facing calendar URL.

    Blank is allowed.  Nonblank values must be http(s); unsafe schemes such as
    javascript:, data:, file:, etc. are rejected for user-entered values and
    stripped when defensively reading old/malformed persisted data.
    """
    clean = str(url or "").strip()
    if not clean:
        return ""
    parsed = urlparse(clean)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        if reject_unsafe:
            raise ValueError("WebUI Calendar URL must be http:// or https://")
        return ""
    return clean


# ---------------------------------------------------------------------------
# Account CRUD
# ---------------------------------------------------------------------------

def add_caldav_account(
    ctxid: str,
    label: str,
    server_url: str,
    username: str,
    password: str,
    webui_calendar_url: str = "",
) -> dict[str, Any] | None:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    clean_url = normalize_server_url(server_url)
    clean_webui_url = normalize_webui_calendar_url(webui_calendar_url)
    clean_label = (str(label or "").strip()) or clean_url
    clean_username = str(username or "").strip()
    clean_password = str(password or "")
    previous = caldav_account_entry(ctxid) or {}
    if not clean_password and isinstance(previous, dict):
        # Editing/reconfiguring an existing singleton account should not require
        # re-entering the password just to change metadata such as the optional
        # WebUI Calendar URL.  A blank password is still rejected for first-time
        # account creation below.
        clean_password = str(previous.get("password") or "")
    if not clean_username:
        raise ValueError("username is required")
    if not clean_password:
        raise ValueError("password is required")

    registry = load_caldav_registry(ctxid, create=True)
    entry = _normalize_account({
        "id": str(previous.get("id") or uuid.uuid4().hex[:12]),
        "label": clean_label,
        "server_url": clean_url,
        "username": clean_username,
        "password": clean_password,
        "webui_calendar_url": clean_webui_url,
        "kind": "caldav",
        "created": previous.get("created") or cal.iso_now(),
        "updated": cal.iso_now(),
        # Replacing the account clears stale discovery/selection from the old
        # provider unless the user re-tests and selects a collection again.
        "collections": [],
        "selected_collection_url": "",
        "selected_collection_name": "",
        "status": "unverified",
        "last_error": "",
        "last_verified": "",
    })
    assert entry is not None
    registry["account"] = entry
    save_caldav_registry(ctxid, registry)
    cal.persist_calendar_indicator(ctxid)
    return public_account(entry)


def set_caldav_account(
    ctxid: str,
    label: str,
    server_url: str,
    username: str,
    password: str,
    webui_calendar_url: str = "",
) -> dict[str, Any] | None:
    """Create or replace the singleton CalDAV account for a context."""
    return add_caldav_account(ctxid, label, server_url, username, password, webui_calendar_url)


def remove_caldav_account(ctxid: str, account_id: str | None = None) -> bool:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry = load_caldav_registry(ctxid, create=False)
    account = registry.get("account")
    if not isinstance(account, dict):
        return False
    clean_id = str(account_id or "").strip()
    if clean_id and clean_id != str(account.get("id") or ""):
        return False
    registry["account"] = None
    save_caldav_registry(ctxid, registry)
    cal.persist_calendar_indicator(ctxid)
    return True


def _save_with_indicator(ctxid: str, registry: dict[str, Any]) -> None:
    cal = _calendar_helpers()
    save_caldav_registry(ctxid, registry)
    cal.persist_calendar_indicator(ctxid)


# ---------------------------------------------------------------------------
# Network operations
# ---------------------------------------------------------------------------

def _connect(account: dict[str, Any]):
    import caldav  # local import keeps module load light when unused
    return caldav.DAVClient(
        url=account["server_url"],
        username=account.get("username") or "",
        password=account.get("password") or "",
    )


def _calendar_object(account: dict[str, Any]):
    selected = str(account.get("selected_collection_url") or "").strip()
    if not selected:
        raise ValueError("no CalDAV collection selected for this account")
    client = _connect(account)
    return client.calendar(url=selected)


def _serialize_collections(calendars) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for cal in calendars:
        try:
            url = str(getattr(cal, "url", "") or "")
            display = ""
            if hasattr(cal, "get_display_name"):
                try:
                    display = str(cal.get_display_name() or "") or ""
                except Exception:
                    display = ""
            name = str(getattr(cal, "name", "") or "")
            out.append({
                "url": url,
                "name": name or display or url,
                "display_name": display or name or url,
            })
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("caldav: skipping collection due to error: %s", exc)
    return out


def test_caldav_account(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    """Verify credentials and discover calendar collections."""
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry, account = _find_account(ctxid, account_id)
    try:
        client = _connect(account)
        principal = client.principal()
        calendars = principal.calendars()
        collections = _serialize_collections(calendars)
        account["collections"] = collections
        account["status"] = "ok"
        account["last_error"] = ""
        account["last_verified"] = cal.iso_now()
        if not account.get("selected_collection_url") and len(collections) == 1:
            account["selected_collection_url"] = collections[0]["url"]
            account["selected_collection_name"] = collections[0]["display_name"]
        _save_with_indicator(ctxid, registry)
        return {
            "ok": True,
            "account": public_account(account),
            "collections": collections,
        }
    except Exception as exc:
        account["status"] = "error"
        account["last_error"] = str(exc)
        save_caldav_registry(ctxid, registry)
        return {"ok": False, "error": str(exc), "account": public_account(account)}


def list_caldav_collections(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    return test_caldav_account(ctxid, account_id)


def select_caldav_collection(ctxid: str, account_id: str | None = None, collection_url: str = "") -> dict[str, Any]:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry, account = _find_account(ctxid, account_id)
    clean_url = str(collection_url or "").strip()
    if not clean_url:
        raise ValueError("collection_url is required")
    matched = None
    for col in account.get("collections") or []:
        if str(col.get("url") or "") == clean_url:
            matched = col
            break
    if matched is None:
        matched = {"url": clean_url, "name": clean_url, "display_name": clean_url}
    account["selected_collection_url"] = clean_url
    account["selected_collection_name"] = matched.get("display_name") or matched.get("name") or clean_url
    _save_with_indicator(ctxid, registry)
    return {"ok": True, "account": public_account(account)}


# ---------------------------------------------------------------------------
# Event listing / CRUD
# ---------------------------------------------------------------------------

def _summarize_event(ev) -> dict[str, Any]:
    summary = ""
    dtstart = ""
    dtend = ""
    uid = ""
    rrule = ""
    component_kind = "event"
    try:
        ical = ev.icalendar_instance
        for sub in getattr(ical, "subcomponents", []) or []:
            name = getattr(sub, "name", "") or ""
            if name in ("VEVENT", "VTODO"):
                component_kind = "event" if name == "VEVENT" else "todo"
                summary = str(sub.get("summary") or "") or summary
                uid = str(sub.get("uid") or "") or uid
                ds = sub.get("dtstart")
                de = sub.get("dtend") or sub.get("due")
                rrule_val = sub.get("rrule")
                try:
                    if ds is not None:
                        dtstart = ds.to_ical().decode()
                except Exception:
                    dtstart = str(ds) if ds is not None else ""
                try:
                    if de is not None:
                        dtend = de.to_ical().decode()
                except Exception:
                    dtend = str(de) if de is not None else ""
                try:
                    if rrule_val is not None:
                        rrule = rrule_val.to_ical().decode()
                except Exception:
                    rrule = str(rrule_val) if rrule_val is not None else ""
                break
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("caldav: failed to summarize event: %s", exc)
    return {
        "uid": uid,
        "summary": summary,
        "dtstart": dtstart,
        "dtend": dtend,
        "rrule": rrule,
        "is_recurring": bool(rrule),
        "component_kind": component_kind,
        "href": str(getattr(ev, "url", "") or ""),
        "etag": str(getattr(ev, "etag", "") or ""),
    }


def list_caldav_events(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    registry, account = _find_account(ctxid, account_id)
    try:
        cal_obj = _calendar_object(account)
        events = []
        try:
            iterable = cal_obj.events()
        except Exception:
            iterable = []
        for ev in iterable:
            events.append(_summarize_event(ev))
        try:
            for todo in cal_obj.todos(include_completed=True):
                events.append(_summarize_event(todo))
        except Exception:
            pass
        account["status"] = "ok"
        account["last_error"] = ""
        account["last_verified"] = cal.iso_now()
        save_caldav_registry(ctxid, registry)
        return {
            "ok": True,
            "events": events,
            "account": public_account(account),
        }
    except Exception as exc:
        account["status"] = "error"
        account["last_error"] = str(exc)
        save_caldav_registry(ctxid, registry)
        return {"ok": False, "error": str(exc), "account": public_account(account)}


def get_caldav_event(ctxid: str, account_id: str | None = None, href: str = "") -> dict[str, Any]:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    _registry, account = _find_account(ctxid, account_id)
    clean_href = str(href or "").strip()
    if not clean_href:
        raise ValueError("href is required")
    cal_obj = _calendar_object(account)
    ev = _caldav_object_by_href(cal_obj, clean_href)
    return {
        "ok": True,
        "href": str(getattr(ev, "url", "") or clean_href),
        "etag": str(getattr(ev, "etag", "") or ""),
        "ics": str(getattr(ev, "data", "") or ""),
    }


def _wrap_in_vcalendar(component_block: str) -> str:
    body = (component_block or "").strip()
    if not body:
        raise ValueError("empty component block")
    return (
        "BEGIN:VCALENDAR\r\n"
        "VERSION:2.0\r\n"
        "PRODID:-//Agent Zero//Superordinates Calendar//EN\r\n"
        "CALSCALE:GREGORIAN\r\n"
        + body.replace("\r\n", "\n").replace("\n", "\r\n")
        + "\r\nEND:VCALENDAR\r\n"
    )


def _parse_ics_modified_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1]
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M", "%Y%m%d"):
        try:
            parsed = datetime.strptime(raw[: len(datetime.now().strftime(fmt))], fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _component_modified_datetime(lines: list[str]) -> datetime | None:
    cal = _calendar_helpers()
    # LAST-MODIFIED is the closest iCalendar equivalent to the requested sync key.
    # DTSTAMP and CREATED are fallbacks for imported components that lack it.
    for prop in ("LAST-MODIFIED", "DTSTAMP", "CREATED"):
        found = cal.property_from_event_lines(lines, prop)
        if found:
            parsed = _parse_ics_modified_datetime(found[1])
            if parsed is not None:
                return parsed
    return None


def _remote_object_modified_datetime(ev, component_lines: list[str]) -> datetime | None:
    for attr in ("last_modified", "lastmodified", "modified", "created"):
        value = getattr(ev, attr, None)
        if not value:
            continue
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            parsed = _parse_ics_modified_datetime(value)
            if parsed is not None:
                return parsed
            try:
                parsed_mail = parsedate_to_datetime(value)
                return parsed_mail.astimezone(timezone.utc) if parsed_mail.tzinfo else parsed_mail.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return _component_modified_datetime(component_lines)


def _component_from_ics_text(text: str) -> dict[str, Any] | None:
    cal = _calendar_helpers()
    normalized = cal.normalize_ics_content(text)
    coerced, dropped = cal.enforce_single_component_text(normalized)
    if dropped:
        normalized = coerced
    _skeleton, components = cal.split_calendar_component_blocks(normalized)
    if not components:
        return None
    kind, uid, lines = components[0]
    summary_prop = cal.property_from_event_lines(lines, "SUMMARY")
    summary = cal.unescape_ics_text(summary_prop[1]) if summary_prop else ""
    modified = _component_modified_datetime(lines)
    return {
        "kind": kind,
        "uid": str(uid or "").strip(),
        "summary": summary,
        "lines": lines,
        "ics": normalized,
        "modified": modified,
    }


def _filename_for_component(uid: str, summary: str = "") -> str:
    cal = _calendar_helpers()
    label = re.sub(r"[^A-Za-z0-9_. -]+", "_", str(summary or "")).strip(" ._")
    if len(label) > 48:
        label = label[:48].strip(" ._")
    suffix = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(uid or uuid.uuid4().hex)).strip("._-")[:40]
    base = f"{label}-{suffix}" if label and suffix else (suffix or "caldav-item")
    return cal.sanitize_calendar_filename(base)


def _unique_local_sync_path(calendar_dir: Path, preferred_name: str, existing_path: Path | None = None) -> Path:
    cal = _calendar_helpers()
    safe = cal.sanitize_calendar_filename(preferred_name)
    stem = Path(safe).stem
    suffix = Path(safe).suffix or ".ics"
    candidate = calendar_dir / safe
    if existing_path is not None and candidate.resolve() == existing_path.resolve():
        return candidate
    if not candidate.exists():
        return candidate
    for idx in range(2, 1000):
        candidate = calendar_dir / cal.sanitize_calendar_filename(f"{stem}-{idx}{suffix}")
        if existing_path is not None and candidate.resolve() == existing_path.resolve():
            return candidate
        if not candidate.exists():
            return candidate
    raise ValueError("could not allocate local ICS filename for synced CalDAV item")


def _a0_description_values_from_ics_text(text: str) -> list[str]:
    """Return unescaped DESCRIPTION values from an ICS body."""
    cal = _calendar_helpers()
    values: list[str] = []
    for line in cal.unfold_ics_lines(cal.normalize_ics_content(text)):
        name, _params, value = cal.split_content_line(line)
        if name == "DESCRIPTION":
            values.append(cal.unescape_ics_text(value))
    return values


def _raw_decode_a0_json_payload(candidate: str) -> Any | None:
    decoder = json.JSONDecoder()
    candidates = [candidate]
    seen = {candidate}

    # Some providers/user inputs may preserve JSON as quote-escaped text, e.g.
    # !{\"a0_name\":...}. Local Event/ToDo saves escape DESCRIPTION
    # backslashes again, so the parser may see !{\\"a0_name\\":...}.
    # Try a conservative quote-deescaped variant before giving up.
    deescaped = re.sub(r'\\+"', '"', candidate)
    if deescaped not in seen:
        candidates.append(deescaped)
        seen.add(deescaped)

    for item in candidates:
        try:
            payload, _end = decoder.raw_decode(item)
            return payload
        except json.JSONDecodeError:
            continue
    return None


def extract_a0_description_json_from_ics(text: str) -> Any | None:
    """Extract the first embedded A0 JSON object from any DESCRIPTION field.

    The embedded payload is identified by a sentinel immediately before the JSON
    object, currently ``!{"a0_name":``.  The returned value is the decoded JSON
    object beginning at that sentinel's ``{``; trailing description text is
    ignored by ``JSONDecoder.raw_decode``.
    """
    markers = (A0_DESCRIPTION_JSON_MARKER, A0_DESCRIPTION_JSON_MARKER_ESCAPED)
    for description in _a0_description_values_from_ics_text(text):
        for marker in markers:
            index = description.find(marker)
            if index < 0:
                continue
            candidate = description[index + 1 :]
            payload = _raw_decode_a0_json_payload(candidate)
            if isinstance(payload, dict) and payload.get("a0_name"):
                return payload
    return None


def extract_a0_description_json_sidecar(path: Path, text: str | None = None) -> Path | None:
    """Synchronize the ``<same-stem>.json`` sidecar for embedded A0 DESCRIPTION JSON.

    A valid DESCRIPTION marker writes the decoded JSON object to the sibling
    sidecar.  Missing markers, invalid/truncated JSON, or read/write failures do
    not crash sync; when no valid payload is found, any stale sibling sidecar is
    removed so remote CalDAV edits are reflected locally.
    """
    sidecar = path.with_suffix(".json")
    try:
        source_text = text if text is not None else path.read_text(encoding="utf-8", errors="replace")
        payload = extract_a0_description_json_from_ics(source_text)
        if payload is None:
            try:
                sidecar.unlink(missing_ok=True)
            except Exception as exc:
                log.warning("caldav sync: failed to remove stale A0 JSON sidecar for %s: %s", path, exc)
            return None
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile("w", encoding="utf-8", dir=str(sidecar.parent), delete=False) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n")
            tmp_path = Path(tmp.name)
        tmp_path.replace(sidecar)
        return sidecar
    except Exception as exc:
        log.warning("caldav sync: failed to extract A0 JSON sidecar for %s: %s", path, exc)
        try:
            sidecar.unlink(missing_ok=True)
        except Exception:
            pass
        return None


def _write_local_ics(path: Path, text: str, modified: datetime | None = None) -> None:
    cal = _calendar_helpers()
    final_text, dropped = cal.enforce_single_component_text(cal.normalize_ics_content(text))
    if dropped:
        raise ValueError("synced CalDAV object contains multiple top-level components")
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", dir=str(path.parent), delete=False) as tmp:
        tmp.write(final_text)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
    extract_a0_description_json_sidecar(path, final_text)
    if modified is not None:
        ts = modified.timestamp()
        os.utime(path, (ts, ts))


def _local_sync_items(ctxid: str) -> dict[str, dict[str, Any]]:
    cal = _calendar_helpers()
    calendar_dir = cal.context_calendar_dir(ctxid, create=True)
    out: dict[str, dict[str, Any]] = {}
    for path in cal.local_ics_file_paths(ctxid):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            component = _component_from_ics_text(text)
            if not component or not component.get("uid"):
                continue
            stat = path.stat()
            modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
            uid = str(component["uid"])
            existing = out.get(uid)
            if existing and existing.get("modified") and existing["modified"] >= modified:
                continue
            out[uid] = {
                "uid": uid,
                "kind": component["kind"],
                "summary": component.get("summary") or "",
                "path": path,
                "filename": path.name,
                "ics": cal.normalize_ics_content(text),
                "modified": modified,
            }
        except Exception as exc:
            log.warning("caldav sync: skipping local ICS %s: %s", path, exc)
            continue
    return out


def _call_caldav_date_search(cal_obj, start: datetime, end: datetime) -> list[Any]:
    """Call caldav.Calendar.date_search across known caldav-py signatures.

    Several providers return an empty result for broad ``events()`` enumeration but
    do return VEVENTs when queried with an explicit time range.  caldav-py has
    also changed argument names across versions, so try the common forms.
    """
    method = getattr(cal_obj, "date_search", None)
    if not callable(method):
        return []
    attempts = [
        lambda: method(start=start, end=end, expand=False),
        lambda: method(start=start, end=end, expand=True),
        lambda: method(start=start, end=end),
        lambda: method(start, end, expand=False),
        lambda: method(start, end, expand=True),
        lambda: method(start, end),
        lambda: method(start=start, end=end, comp_class="VEVENT", expand=False),
        lambda: method(start=start, end=end, comp_class="VEVENT", expand=True),
        lambda: method(start=start, end=end, comp_class="VEVENT"),
        lambda: method(start=start, end=end, event=True, expand=False),
        lambda: method(start=start, end=end, event=True),
    ]
    last_error: Exception | None = None
    for attempt in attempts:
        try:
            found = list(attempt() or [])
            if found:
                return found
        except TypeError as exc:
            last_error = exc
            continue
        except Exception as exc:
            # Non-signature failures are often provider/report errors; try the
            # next compatible call shape before giving up.
            last_error = exc
            continue
    if last_error is not None:
        log.warning("caldav sync: date_search failed or returned no objects: %s", last_error)
    return []


def _call_caldav_object_listing(cal_obj) -> tuple[list[Any], list[str], dict[str, int]]:
    """Try broad object-listing methods exposed by different caldav-py versions."""
    objects: list[Any] = []
    errors: list[str] = []
    counts: dict[str, int] = {}
    method_specs = [
        ("objects", ()),
        ("calendar_objects", ()),
        ("children", ()),
    ]
    for method_name, args in method_specs:
        method = getattr(cal_obj, method_name, None)
        if not callable(method):
            continue
        try:
            found = list(method(*args) or [])
            counts[method_name] = len(found)
            objects.extend(found)
        except Exception as exc:
            errors.append(f"{method_name}(): {exc}")
    search = getattr(cal_obj, "search", None)
    if callable(search):
        search_attempts = [
            ("search(event,todo)", lambda: search(event=True, todo=True, expand=False)),
            ("search(event)", lambda: search(event=True, expand=False)),
            ("search()", lambda: search()),
        ]
        for label, attempt in search_attempts:
            try:
                found = list(attempt() or [])
                counts[label] = len(found)
                objects.extend(found)
                if found:
                    break
            except TypeError as exc:
                errors.append(f"{label}: {exc}")
            except Exception as exc:
                errors.append(f"{label}: {exc}")
    return objects, errors, counts


def _caldav_object_text(ev) -> str:
    """Return ICS text from a CalDAV object, loading it if needed."""
    data = getattr(ev, "data", "")
    if callable(data):
        try:
            data = data()
        except Exception:
            data = ""
    if not data:
        loader = getattr(ev, "load", None)
        if callable(loader):
            try:
                loader()
                data = getattr(ev, "data", "")
                if callable(data):
                    data = data()
            except Exception as exc:
                log.warning("caldav sync: failed to load remote object data: %s", exc)
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")
    text = str(data or "")
    if text:
        return text
    ical = getattr(ev, "icalendar_instance", None)
    to_ical = getattr(ical, "to_ical", None)
    if callable(to_ical):
        try:
            raw = to_ical()
            return raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw or "")
        except Exception:
            return ""
    return ""


def _dedupe_caldav_objects(objects: list[Any]) -> list[Any]:
    out: list[Any] = []
    seen: set[str] = set()
    for obj in objects:
        key = str(getattr(obj, "url", "") or getattr(obj, "href", "") or id(obj))
        if key in seen:
            continue
        seen.add(key)
        out.append(obj)
    return out


def _remote_sync_items(cal_obj) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    objects: list[Any] = []
    diagnostics: dict[str, Any] = {
        "collection_url": str(getattr(cal_obj, "url", "") or ""),
        "events_listed": 0,
        "todos_listed": 0,
        "date_search_listed": 0,
        "broad_listed": 0,
        "broad_listing_counts": {},
        "objects_seen": 0,
        "parsed": 0,
        "skipped_without_uid": 0,
        "skipped_parse_errors": 0,
        "listing_errors": [],
        "date_search_range": "",
        "sample_summaries": [],
    }
    try:
        listed_events = list(cal_obj.events())
        diagnostics["events_listed"] = len(listed_events)
        objects.extend(listed_events)
    except Exception as exc:
        diagnostics["listing_errors"].append(f"events(): {exc}")
        log.warning("caldav sync: event listing failed: %s", exc)
    try:
        listed_todos = list(cal_obj.todos(include_completed=True))
        diagnostics["todos_listed"] = len(listed_todos)
        objects.extend(listed_todos)
    except Exception as exc:
        diagnostics["listing_errors"].append(f"todos(): {exc}")
        log.warning("caldav sync: todo listing failed: %s", exc)

    broad_objects, broad_errors, broad_counts = _call_caldav_object_listing(cal_obj)
    diagnostics["broad_listed"] = len(broad_objects)
    diagnostics["broad_listing_counts"] = broad_counts
    diagnostics["listing_errors"].extend(broad_errors)
    objects.extend(broad_objects)

    now = datetime.now(timezone.utc)
    # Wide explicit range: catches today's event and near-future events on
    # providers that do not return anything from broad events() enumeration.
    start = (now - timedelta(days=370)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = (now + timedelta(days=735)).replace(hour=23, minute=59, second=59, microsecond=0)
    diagnostics["date_search_range"] = f"{start.isoformat().replace('+00:00', 'Z')}..{end.isoformat().replace('+00:00', 'Z')}"
    date_objects = _call_caldav_date_search(cal_obj, start, end)
    diagnostics["date_search_listed"] = len(date_objects)
    objects.extend(date_objects)

    objects = _dedupe_caldav_objects(objects)
    diagnostics["objects_seen"] = len(objects)
    for ev in objects:
        try:
            text = _caldav_object_text(ev)
            component = _component_from_ics_text(text)
            if not component or not component.get("uid"):
                diagnostics["skipped_without_uid"] += 1
                continue
            modified = _remote_object_modified_datetime(ev, component["lines"]) or datetime.fromtimestamp(0, timezone.utc)
            uid = str(component["uid"])
            kind = str(component.get("kind") or "VEVENT").upper()
            item_key = f"{uid}::{kind}"
            existing = out.get(item_key)
            if existing and existing.get("modified") and existing["modified"] >= modified:
                continue
            item = {
                "uid": uid,
                "kind": kind,
                "summary": component.get("summary") or "",
                "href": str(getattr(ev, "url", "") or ""),
                "etag": str(getattr(ev, "etag", "") or ""),
                "ics": component["ics"],
                "modified": modified,
            }
            out[item_key] = item
            diagnostics["parsed"] = len(out)
            if len(diagnostics["sample_summaries"]) < 8:
                diagnostics["sample_summaries"].append({
                    "uid": uid,
                    "kind": item["kind"],
                    "summary": item["summary"],
                    "href": item["href"],
                })
        except Exception as exc:
            diagnostics["skipped_parse_errors"] += 1
            log.warning("caldav sync: skipping remote object: %s", exc)
            continue
    diagnostics["remote_uids"] = len({str(item.get("uid") or "") for item in out.values()})
    diagnostics["remote_components"] = len(out)
    return out, diagnostics


def sync_caldav_ics_files(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    """Bidirectionally sync selected CalDAV collection and local one-component ICS files.

    The implementation is ledger-backed in helpers.agent_calendar_sync.  This
    wrapper preserves the existing public helper/API name used by the WebUI.
    """
    from . import agent_calendar_sync

    return agent_calendar_sync.sync_context(ctxid, account_id)


def get_caldav_sync_status(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    """Return persisted CalDAV/local ICS sync status for the selected collection."""
    from . import agent_calendar_sync

    return {"ok": True, "sync_status": agent_calendar_sync.get_status(ctxid, account_id)}


def resolve_caldav_sync_conflict(
    ctxid: str,
    uid: str = "",
    component_kind: str = "",
    strategy: str = "",
) -> dict[str, Any]:
    """Resolve a ledger conflict using a conservative user-selected strategy."""
    from . import agent_calendar_sync

    return agent_calendar_sync.resolve_conflict(ctxid, uid=uid, component_kind=component_kind, strategy=strategy)


def _caldav_object_by_href(cal_obj, href: str, kind: str = ""):
    """Return a CalDAV object by URL using event/todo-capable library methods.

    The caldav package exposes event_by_url() broadly; some versions/providers
    also expose todo_by_url(), object_by_url(), or calendar_object_by_url().
    Try the component-specific method first, then fall back defensively.
    """
    clean_href = str(href or "").strip()
    if not clean_href:
        raise ValueError("href is required")
    normalized_kind = str(kind or "").upper()
    if normalized_kind == "VTODO":
        method_names = ["todo_by_url", "event_by_url", "object_by_url", "calendar_object_by_url"]
    else:
        method_names = ["event_by_url", "todo_by_url", "object_by_url", "calendar_object_by_url"]
    last_error: Exception | None = None
    for method_name in method_names:
        method = getattr(cal_obj, method_name, None)
        if not callable(method):
            continue
        try:
            return method(clean_href)
        except Exception as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    raise AttributeError("CalDAV calendar object does not support URL lookup")


def _build_ics_from_payload(payload: dict[str, Any]) -> str:
    cal = _calendar_helpers()
    raw = str(payload.get("ics") or "").strip()
    if raw:
        return raw if raw.upper().startswith("BEGIN:VCALENDAR") else _wrap_in_vcalendar(raw)

    event = payload.get("event") if isinstance(payload.get("event"), dict) else None
    todo = payload.get("todo") if isinstance(payload.get("todo"), dict) else None
    if event:
        _uid, block = cal.build_vevent(event, existing_lines=None)
        return _wrap_in_vcalendar(block)
    if todo:
        _uid, block = cal.build_vtodo(todo, existing_lines=None)
        return _wrap_in_vcalendar(block)
    raise ValueError("upsert payload must include one of: ics, event, todo")


def upsert_caldav_event(
    ctxid: str,
    account_id: str | None = None,
    payload: dict[str, Any] | None = None,
    href: str | None = None,
) -> dict[str, Any]:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    _registry, account = _find_account(ctxid, account_id)
    payload = payload or {}
    ics_text = _build_ics_from_payload(payload)
    component = _component_from_ics_text(ics_text) or {}
    component_kind = str(component.get("kind") or "VEVENT").upper()
    cal_obj = _calendar_object(account)
    clean_href = str(href or "").strip()
    if clean_href:
        try:
            existing = _caldav_object_by_href(cal_obj, clean_href, component_kind)
            existing.data = ics_text
            existing.save()
            return {
                "ok": True,
                "href": str(getattr(existing, "url", "") or clean_href),
                "etag": str(getattr(existing, "etag", "") or ""),
                "created": False,
            }
        except Exception as exc:
            log.warning("caldav: update via href failed (%s); falling back to create", exc)
    if component_kind == "VTODO" and callable(getattr(cal_obj, "save_todo", None)):
        ev = cal_obj.save_todo(ics_text)
    else:
        ev = cal_obj.save_event(ics_text)
    return {
        "ok": True,
        "href": str(getattr(ev, "url", "") or ""),
        "etag": str(getattr(ev, "etag", "") or ""),
        "created": True,
    }


def delete_caldav_event(ctxid: str, account_id: str | None = None, href: str = "") -> dict[str, Any]:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    _registry, account = _find_account(ctxid, account_id)
    clean_href = str(href or "").strip()
    if not clean_href:
        raise ValueError("href is required")
    cal_obj = _calendar_object(account)
    ev = _caldav_object_by_href(cal_obj, clean_href)
    ev.delete()
    return {"ok": True, "href": clean_href}
