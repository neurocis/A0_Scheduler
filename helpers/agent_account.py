"""Unified account facade for CalDAV and Exchange calendar providers.

The UI exposes one Account configuration.  This module keeps the single unified
``account.json`` source of truth, detects a provider only when creating/updating
credentials, and delegates all protocol-specific work to ``agent_caldav`` or
``agent_exchange``.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable

from usr.plugins.a0_scheduler.helpers import agent_caldav, agent_exchange

log = logging.getLogger("agent_account")

ACCOUNT_FILENAME = "account.json"
REGISTRY_VERSION = 1
PROVIDER_TIMEOUT_SECONDS = 10


def _calendar_helpers():
    from usr.plugins.a0_scheduler.helpers import agent_calendar  # type: ignore
    return agent_calendar


def account_path(ctxid: str, create: bool = True) -> Path:
    return _calendar_helpers().context_calendar_dir(ctxid, create=create) / ACCOUNT_FILENAME


def _empty_registry() -> dict[str, Any]:
    return {"version": REGISTRY_VERSION, "account": None}


def _selected_from_provider(kind: str, provider_account: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(provider_account, dict):
        return None
    if kind == "exchange":
        value = str(provider_account.get("selected_calendar_id") or "").strip()
        if not value:
            return None
        return {"id_or_url": value, "name": provider_account.get("selected_calendar_name") or value}
    value = str(provider_account.get("selected_collection_url") or "").strip()
    if not value:
        return None
    return {"id_or_url": value, "name": provider_account.get("selected_collection_name") or value}


def _provider_public(kind: str, ctxid: str) -> dict[str, Any] | None:
    if kind == "exchange":
        return agent_exchange.get_exchange_account(ctxid)
    if kind == "caldav":
        return agent_caldav.get_caldav_account(ctxid)
    return None


def _normalize_status(value: str) -> str:
    status = str(value or "").strip().lower()
    if status in {"ok", "verified", "connected", "success", "working"}:
        return "verified"
    if status in {"error", "failed", "fail", "invalid"}:
        return "error"
    return "unverified"


def _normalize_account(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    kind = str(entry.get("kind") or "").strip().lower()
    if kind not in {"exchange", "caldav"}:
        return None
    server_url = str(entry.get("server_url") or entry.get("server") or "").strip()
    if not entry.get("id") or not server_url:
        return None
    normalized = dict(entry)
    normalized["kind"] = kind
    normalized["server_url"] = server_url
    normalized.setdefault("label", server_url)
    normalized.setdefault("username", "")
    normalized.setdefault("password", "")
    normalized["webui_calendar_url"] = agent_caldav.normalize_webui_calendar_url(
        normalized.get("webui_calendar_url")
        if normalized.get("webui_calendar_url") is not None
        else normalized.get("webuiCalendarUrl") or "",
        reject_unsafe=False,
    )
    selected = normalized.get("selected_target")
    if not isinstance(selected, dict):
        selected = None
    normalized["selected_target"] = selected
    normalized["status"] = _normalize_status(str(normalized.get("status") or ""))
    normalized.setdefault("last_error", "")
    normalized.setdefault("last_verified", "")
    return normalized


def public_account(account: dict[str, Any] | None) -> dict[str, Any] | None:
    normalized = _normalize_account(account)
    if not normalized:
        return None
    public = {k: v for k, v in normalized.items() if k != "password"}
    public["has_password"] = bool(normalized.get("password"))
    return public


def _provider_to_unified(kind: str, provider_account: dict[str, Any]) -> dict[str, Any]:
    cal = _calendar_helpers()
    if kind == "exchange":
        server_url = str(provider_account.get("server") or provider_account.get("server_url") or "")
    else:
        server_url = str(provider_account.get("server_url") or provider_account.get("server") or "")
    return _normalize_account({
        "id": provider_account.get("id") or "",
        "kind": kind,
        "label": provider_account.get("label") or server_url,
        "server_url": server_url,
        "username": provider_account.get("username") or "",
        "password": provider_account.get("password") or "",
        "webui_calendar_url": provider_account.get("webui_calendar_url") or "",
        "selected_target": _selected_from_provider(kind, provider_account),
        "status": _normalize_status(str(provider_account.get("status") or "")),
        "last_error": provider_account.get("last_error") or "",
        "last_verified": provider_account.get("last_verified") or "",
        "created": provider_account.get("created") or cal.iso_now(),
        "updated": provider_account.get("updated") or cal.iso_now(),
    })


def load_account_registry(ctxid: str, create: bool = False) -> dict[str, Any]:
    _calendar_helpers().validate_context_id(ctxid)
    path = account_path(ctxid, create=create)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        return {
            "version": int(data.get("version") or REGISTRY_VERSION),
            "account": _normalize_account(data.get("account")),
        }
    migrated = _migrate_from_provider_files(ctxid)
    if migrated is not None:
        registry = {"version": REGISTRY_VERSION, "account": migrated}
        save_account_registry(ctxid, registry)
        return registry
    return _empty_registry()


def save_account_registry(ctxid: str, registry: dict[str, Any]) -> None:
    path = account_path(ctxid, create=True)
    singleton = {
        "version": int(registry.get("version") or REGISTRY_VERSION),
        "account": _normalize_account(registry.get("account")),
    }
    with NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
        json.dump(singleton, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _migrate_from_provider_files(ctxid: str) -> dict[str, Any] | None:
    exchange = None
    caldav = None
    try:
        exchange = agent_exchange.load_exchange_registry(ctxid, create=False).get("account")
    except Exception as exc:
        log.warning("account migration: failed to read exchange.json for %s: %s", ctxid, exc)
    try:
        caldav = agent_caldav.load_caldav_registry(ctxid, create=False).get("account")
    except Exception as exc:
        log.warning("account migration: failed to read caldav.json for %s: %s", ctxid, exc)
    if isinstance(exchange, dict) and isinstance(caldav, dict):
        log.warning("account migration: both exchange.json and caldav.json exist for %s; preferring exchange", ctxid)
    if isinstance(exchange, dict):
        migrated = _provider_to_unified("exchange", exchange)
        if migrated:
            return migrated
    if isinstance(caldav, dict):
        migrated = _provider_to_unified("caldav", caldav)
        if migrated:
            return migrated
    return None


def _save_from_provider(ctxid: str, kind: str, provider_account: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if provider_account is None:
        if kind == "exchange":
            provider_account = agent_exchange.exchange_account_entry(ctxid)
        else:
            provider_account = agent_caldav.caldav_account_entry(ctxid)
    if not isinstance(provider_account, dict):
        return None
    unified = _provider_to_unified(kind, provider_account)
    registry = {"version": REGISTRY_VERSION, "account": unified}
    save_account_registry(ctxid, registry)
    try:
        _calendar_helpers().persist_calendar_indicator(ctxid)
    except Exception:
        pass
    return public_account(unified)


def _run_with_timeout(fn: Callable[[], dict[str, Any]], timeout: float = PROVIDER_TIMEOUT_SECONDS) -> dict[str, Any]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fn)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            return {"ok": False, "error": f"connection attempt timed out after {int(timeout)}s"}


def _snapshot_provider(ctxid: str, kind: str) -> dict[str, Any]:
    if kind == "exchange":
        return agent_exchange.load_exchange_registry(ctxid, create=False)
    return agent_caldav.load_caldav_registry(ctxid, create=False)


def _restore_provider(ctxid: str, kind: str, registry: dict[str, Any]) -> None:
    try:
        if kind == "exchange":
            agent_exchange.save_exchange_registry(ctxid, registry)
        else:
            agent_caldav.save_caldav_registry(ctxid, registry)
    except Exception as exc:
        log.warning("account detection: failed to restore %s registry for %s: %s", kind, ctxid, exc)


def _attempt_provider(ctxid: str, kind: str, label: str, server_url: str, username: str, password: str, webui_calendar_url: str) -> dict[str, Any]:
    if kind == "exchange":
        agent_exchange.add_exchange_account(ctxid, label, server_url, username, password)
        result = _run_with_timeout(lambda: agent_exchange.test_exchange_account(ctxid))
        result.setdefault("kind", "exchange")
        result.setdefault("account", agent_exchange.get_exchange_account(ctxid))
        return result
    agent_caldav.add_caldav_account(ctxid, label, server_url, username, password, webui_calendar_url)
    result = _run_with_timeout(lambda: agent_caldav.test_caldav_account(ctxid))
    result.setdefault("kind", "caldav")
    result.setdefault("account", agent_caldav.get_caldav_account(ctxid))
    return result


def _error_text(result: dict[str, Any]) -> str:
    return str(result.get("error") or result.get("last_error") or "").strip()


def add_account(
    ctxid: str,
    label: str,
    server_url: str,
    username: str,
    password: str,
    webui_calendar_url: str = "",
) -> dict[str, Any] | None:
    """Detect and configure a unified account, trying Exchange before CalDAV."""
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    clean_label = str(label or "").strip()
    clean_server = str(server_url or "").strip()
    clean_username = str(username or "").strip()
    clean_password = str(password or "")
    clean_webui = agent_caldav.normalize_webui_calendar_url(webui_calendar_url)
    previous_unified = load_account_registry(ctxid, create=False).get("account") or {}
    if not clean_password and isinstance(previous_unified, dict):
        clean_password = str(previous_unified.get("password") or "")
    if not clean_server:
        raise ValueError("server URL is required")
    if not clean_username:
        raise ValueError("username is required")
    if not clean_password:
        raise ValueError("password is required")

    snapshots = {
        "exchange": _snapshot_provider(ctxid, "exchange"),
        "caldav": _snapshot_provider(ctxid, "caldav"),
    }
    results: dict[str, dict[str, Any]] = {}

    for kind in ("exchange", "caldav"):
        try:
            result = _attempt_provider(ctxid, kind, clean_label, clean_server, clean_username, clean_password, clean_webui)
        except Exception as exc:
            result = {"ok": False, "error": str(exc), "kind": kind}
        results[kind] = result
        if result.get("ok"):
            provider_raw = agent_exchange.exchange_account_entry(ctxid) if kind == "exchange" else agent_caldav.caldav_account_entry(ctxid)
            public = _save_from_provider(ctxid, kind, provider_raw)
            if public is not None:
                return public
        _restore_provider(ctxid, kind, snapshots[kind])

    # Preserve the previous successful provider state if neither detection works.
    _restore_provider(ctxid, "exchange", snapshots["exchange"])
    _restore_provider(ctxid, "caldav", snapshots["caldav"])
    ex_error = _error_text(results.get("exchange", {}))
    cd_error = _error_text(results.get("caldav", {}))
    message = ex_error or cd_error or "account verification failed"
    if ex_error and cd_error:
        message = f"Exchange failed: {ex_error}; CalDAV failed: {cd_error}"
    # Mark unified account as error without changing kind when editing existing.
    if isinstance(previous_unified, dict) and previous_unified.get("kind"):
        previous_unified["status"] = "error"
        previous_unified["last_error"] = message
        save_account_registry(ctxid, {"version": REGISTRY_VERSION, "account": previous_unified})
    raise ValueError(message)


set_account = add_account


def get_account(ctxid: str) -> dict[str, Any] | None:
    registry = load_account_registry(ctxid, create=False)
    account = registry.get("account")
    if not isinstance(account, dict):
        return None
    # Keep selected target current with provider state.
    provider = _provider_public(str(account.get("kind") or ""), ctxid)
    if provider:
        selected = _selected_from_provider(str(account.get("kind") or ""), provider)
        if selected != account.get("selected_target"):
            account["selected_target"] = selected
            save_account_registry(ctxid, registry)
    return public_account(account)


def _raw_account(ctxid: str) -> dict[str, Any]:
    account = load_account_registry(ctxid, create=False).get("account")
    if not isinstance(account, dict):
        raise ValueError("no account configured")
    return account


def remove_account(ctxid: str) -> bool:
    cal = _calendar_helpers()
    cal.validate_context_id(ctxid)
    account = load_account_registry(ctxid, create=False).get("account")
    if not isinstance(account, dict):
        return False
    kind = str(account.get("kind") or "").lower()
    removed = False
    if kind == "exchange":
        removed = agent_exchange.remove_exchange_account(ctxid)
    elif kind == "caldav":
        removed = agent_caldav.remove_caldav_account(ctxid)
    save_account_registry(ctxid, {"version": REGISTRY_VERSION, "account": None})
    cal.persist_calendar_indicator(ctxid)
    return bool(removed or True)


def test_account(ctxid: str) -> dict[str, Any]:
    account = _raw_account(ctxid)
    kind = str(account.get("kind") or "").lower()
    if kind == "exchange":
        result = _run_with_timeout(lambda: agent_exchange.test_exchange_account(ctxid))
        key = "calendars"
        provider_raw = agent_exchange.exchange_account_entry(ctxid)
    elif kind == "caldav":
        result = _run_with_timeout(lambda: agent_caldav.test_caldav_account(ctxid))
        key = "collections"
        provider_raw = agent_caldav.caldav_account_entry(ctxid)
    else:
        raise ValueError("unknown account kind")
    if result.get("ok"):
        account_public = _save_from_provider(ctxid, kind, provider_raw)
    else:
        account["status"] = "error"
        account["last_error"] = _error_text(result)
        save_account_registry(ctxid, {"version": REGISTRY_VERSION, "account": account})
        account_public = public_account(account)
    return {
        "ok": bool(result.get("ok")),
        "error": result.get("error") or "",
        "account": account_public,
        "calendars": result.get(key, []),
        "collections": result.get(key, []),
    }


def list_account_calendars(ctxid: str) -> dict[str, Any]:
    return test_account(ctxid)


def select_account_calendar(ctxid: str, calendar_id_or_url: str) -> dict[str, Any]:
    account = _raw_account(ctxid)
    kind = str(account.get("kind") or "").lower()
    target = str(calendar_id_or_url or "").strip()
    if kind == "exchange":
        result = agent_exchange.select_exchange_calendar(ctxid, calendar_id=target)
        provider_raw = agent_exchange.exchange_account_entry(ctxid)
    elif kind == "caldav":
        result = agent_caldav.select_caldav_collection(ctxid, collection_url=target)
        provider_raw = agent_caldav.caldav_account_entry(ctxid)
    else:
        raise ValueError("unknown account kind")
    public = _save_from_provider(ctxid, kind, provider_raw)
    result["account"] = public
    return result


def sync_account(ctxid: str) -> dict[str, Any]:
    account = _raw_account(ctxid)
    kind = str(account.get("kind") or "").lower()
    if kind == "exchange":
        result = agent_exchange.sync_exchange_ics_files(ctxid)
        provider_raw = agent_exchange.exchange_account_entry(ctxid)
    elif kind == "caldav":
        result = agent_caldav.sync_caldav_ics_files(ctxid)
        provider_raw = agent_caldav.caldav_account_entry(ctxid)
    else:
        raise ValueError("unknown account kind")
    public = _save_from_provider(ctxid, kind, provider_raw)
    if isinstance(result, dict):
        result["account"] = public
    return result


def get_account_sync_status(ctxid: str) -> dict[str, Any]:
    try:
        account = _raw_account(ctxid)
    except Exception:
        return {"ok": True, "sync_status": {"state": "empty"}}
    kind = str(account.get("kind") or "").lower()
    if kind == "exchange":
        return agent_exchange.get_exchange_sync_status(ctxid)
    if kind == "caldav":
        return agent_caldav.get_caldav_sync_status(ctxid)
    return {"ok": True, "sync_status": {"state": "empty"}}


def has_active_account_source(ctxid: str) -> bool:
    try:
        account = _raw_account(ctxid)
    except Exception:
        return False
    kind = str(account.get("kind") or "").lower()
    if kind == "exchange":
        return agent_exchange.has_active_exchange_source(ctxid)
    if kind == "caldav":
        return agent_caldav.has_active_caldav_source(ctxid)
    return False
