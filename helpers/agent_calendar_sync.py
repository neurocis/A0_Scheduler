"""Ledger-backed sync between local per-agent ICS files and a CalDAV collection."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import quote, urljoin

log = logging.getLogger("agent_calendar_sync")

SYNC_STATE_FILENAME = ".a0-caldav-sync-state.json"
SYNC_LOCK_FILENAME = ".a0-caldav-sync.lock"
SYNC_TRASH_DIRNAME = ".trash"
SYNC_CONFLICT_DIRNAME = ".conflicts"
SYNC_VERSION = 1
NORMAL_SYNC_INTERVAL_SECONDS = 15 * 60
STALE_SYNC_SECONDS = 60 * 60
TOMBSTONE_RETENTION_SECONDS = 14 * 24 * 60 * 60


def _calendar_helpers():
    from . import agent_calendar  # type: ignore  # late import avoids circular deps
    return agent_calendar


def _caldav_helpers():
    from . import agent_caldav  # type: ignore  # late import avoids circular deps
    return agent_caldav


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    return (dt or _now()).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _sha256_text(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8", errors="replace")).hexdigest()


def _component_key(uid: str, kind: str) -> str:
    clean_uid = str(uid or "").strip()
    clean_kind = str(kind or "").strip().upper()
    if clean_kind == "VEVENT":
        clean_kind = "event"
    elif clean_kind == "VTODO":
        clean_kind = "todo"
    elif clean_kind not in {"event", "todo"}:
        clean_kind = "todo" if "TODO" in clean_kind.upper() else "event"
    return f"{clean_uid}::{clean_kind}"


def _kind_label(kind: str) -> str:
    clean = str(kind or "").strip().upper()
    if clean in {"VTODO", "TODO"} or "TODO" in clean:
        return "todo"
    return "event"


def _kind_component(kind: str) -> str:
    return "VTODO" if _kind_label(kind) == "todo" else "VEVENT"


def _state_path(ctxid: str) -> Path:
    cal = _calendar_helpers()
    return cal.context_calendar_dir(ctxid, create=True) / SYNC_STATE_FILENAME


def _lock_path(ctxid: str) -> Path:
    cal = _calendar_helpers()
    return cal.context_calendar_dir(ctxid, create=True) / SYNC_LOCK_FILENAME


def _empty_state(ctxid: str, account: dict[str, Any] | None = None) -> dict[str, Any]:
    account = account or {}
    return {
        "version": SYNC_VERSION,
        "ctxid": ctxid,
        "account_id": str(account.get("id") or ""),
        "collection_url": str(account.get("selected_collection_url") or ""),
        "sync_token": "",
        "last_attempt_at": "",
        "last_success_at": "",
        "last_error": "",
        "last_sync_summary": {},
        "objects": {},
    }


def _normalize_state(ctxid: str, data: Any, account: dict[str, Any] | None = None) -> dict[str, Any]:
    state = _empty_state(ctxid, account)
    if isinstance(data, dict):
        state.update({k: v for k, v in data.items() if k in state})
        state["version"] = int(data.get("version") or SYNC_VERSION)
        if isinstance(data.get("objects"), dict):
            state["objects"] = data.get("objects") or {}
        if isinstance(data.get("last_sync_summary"), dict):
            state["last_sync_summary"] = data.get("last_sync_summary") or {}
    if account:
        state["ctxid"] = ctxid
        state["account_id"] = str(account.get("id") or state.get("account_id") or "")
        state["collection_url"] = str(account.get("selected_collection_url") or state.get("collection_url") or "")
    if not isinstance(state.get("objects"), dict):
        state["objects"] = {}
    return state


def load_sync_state(ctxid: str, account: dict[str, Any] | None = None) -> dict[str, Any]:
    cal = _calendar_helpers()
    clean = cal.validate_context_id(ctxid)
    path = _state_path(clean)
    if not path.exists():
        return _empty_state(clean, account)
    try:
        return _normalize_state(clean, json.loads(path.read_text(encoding="utf-8")), account)
    except Exception:
        return _empty_state(clean, account)


def save_sync_state(ctxid: str, state: dict[str, Any]) -> None:
    cal = _calendar_helpers()
    clean = cal.validate_context_id(ctxid)
    path = _state_path(clean)
    path.parent.mkdir(parents=True, exist_ok=True)
    state = _normalize_state(clean, state)
    with NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
        json.dump(state, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


@contextmanager
def _sync_lock(ctxid: str, stale_seconds: int = 30 * 60):
    """Small cross-process lock file.  Avoids overlapping UI/manual/job syncs."""
    path = _lock_path(ctxid)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd: int | None = None
    acquired = False
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(str(path), flags, 0o600)
            acquired = True
        except FileExistsError as exc:
            try:
                age = time.time() - path.stat().st_mtime
                if age > stale_seconds:
                    path.unlink(missing_ok=True)
                    fd = os.open(str(path), flags, 0o600)
                    acquired = True
                else:
                    raise RuntimeError("CalDAV sync is already running") from exc
            except RuntimeError:
                raise
            except Exception as inner:
                raise RuntimeError("CalDAV sync is already running") from inner
        os.write(fd, f"pid={os.getpid()} time={_iso()}\n".encode("utf-8"))
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        if acquired:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


def _local_scan(ctxid: str) -> dict[str, dict[str, Any]]:
    cal = _calendar_helpers()
    clean = cal.validate_context_id(ctxid)
    out: dict[str, dict[str, Any]] = {}
    for path in cal.local_ics_file_paths(clean):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            normalized = cal.normalize_ics_content(text)
            coerced, dropped = cal.enforce_single_component_text(normalized)
            if dropped:
                normalized = coerced
            _skeleton, components = cal.split_calendar_component_blocks(normalized)
            if not components:
                continue
            kind, uid, lines = components[0]
            uid = str(uid or "").strip()
            if not uid:
                continue
            summary_prop = cal.property_from_event_lines(lines, "SUMMARY")
            summary = cal.unescape_ics_text(summary_prop[1]) if summary_prop else ""
            stat = path.stat()
            component_kind = _kind_label(kind)
            key = _component_key(uid, component_kind)
            modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
            existing = out.get(key)
            if existing and existing.get("modified_epoch", 0) >= stat.st_mtime:
                continue
            out[key] = {
                "key": key,
                "uid": uid,
                "component_kind": component_kind,
                "kind": _kind_component(component_kind),
                "summary": summary,
                "path": path,
                "local_path": path.name,
                "filename": path.name,
                "ics": normalized,
                "hash": _sha256_text(normalized),
                "modified": modified,
                "modified_epoch": stat.st_mtime,
            }
        except Exception as exc:
            log.warning("caldav ledger sync: skipping local ICS %s: %s", path, exc)
            continue
    return out


def _remote_scan(account: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, Any], Any]:
    cd = _caldav_helpers()
    cal_obj = cd._calendar_object(account)  # reuse provider-compatible discovery helpers
    remote_by_uid, diagnostics = cd._remote_sync_items(cal_obj)
    out: dict[str, dict[str, Any]] = {}
    for _uid_key, item in remote_by_uid.items():
        uid = str(item.get("uid") or "").strip()
        if not uid:
            continue
        component_kind = _kind_label(str(item.get("kind") or item.get("component_kind") or "VEVENT"))
        text = str(item.get("ics") or "")
        key = _component_key(uid, component_kind)
        existing = out.get(key)
        modified = item.get("modified")
        if isinstance(modified, datetime):
            modified_dt = modified.astimezone(timezone.utc) if modified.tzinfo else modified.replace(tzinfo=timezone.utc)
        else:
            modified_dt = datetime.fromtimestamp(0, timezone.utc)
        if existing and existing.get("modified", datetime.fromtimestamp(0, timezone.utc)) >= modified_dt:
            continue
        out[key] = {
            "key": key,
            "uid": uid,
            "component_kind": component_kind,
            "kind": _kind_component(component_kind),
            "summary": str(item.get("summary") or ""),
            "href": str(item.get("href") or ""),
            "etag": str(item.get("etag") or ""),
            "ics": text,
            "hash": _sha256_text(text),
            "modified": modified_dt,
        }
    diagnostics["remote_component_keys"] = len(out)
    return out, diagnostics, cal_obj


def _safe_filename_piece(value: str, max_len: int = 64) -> str:
    clean = "".join(ch if ch.isalnum() or ch in "._- " else "_" for ch in str(value or "")).strip(" ._")
    return (clean[:max_len].strip(" ._") or "item")


def _filename_for_item(item: dict[str, Any]) -> str:
    cd = _caldav_helpers()
    uid = str(item.get("uid") or "")
    summary = str(item.get("summary") or "")
    try:
        return cd._filename_for_component(uid, summary)
    except Exception:
        cal = _calendar_helpers()
        return cal.sanitize_calendar_filename(f"{_safe_filename_piece(summary, 48)}-{_safe_filename_piece(uid, 40)}.ics")


def _unique_path(calendar_dir: Path, preferred_name: str, existing_path: Path | None = None) -> Path:
    cd = _caldav_helpers()
    return cd._unique_local_sync_path(calendar_dir, preferred_name, existing_path=existing_path)


def _write_local(path: Path, text: str, modified: datetime | None = None) -> None:
    cd = _caldav_helpers()
    cd._write_local_ics(path, text, modified)


def _move_to_trash(path: Path) -> Path:
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    trash = path.parent / SYNC_TRASH_DIRNAME
    trash.mkdir(parents=True, exist_ok=True)
    candidate = trash / f"{path.stem}.deleted-{stamp}{path.suffix or '.ics'}"
    for idx in range(2, 1000):
        if not candidate.exists():
            break
        candidate = trash / f"{path.stem}.deleted-{stamp}-{idx}{path.suffix or '.ics'}"
    shutil.move(str(path), str(candidate))
    return candidate


def _write_conflict_copy(calendar_dir: Path, local_path: str, remote_text: str) -> Path:
    cal = _calendar_helpers()
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    conflict_dir = calendar_dir / SYNC_CONFLICT_DIRNAME
    conflict_dir.mkdir(parents=True, exist_ok=True)
    base = Path(str(local_path or "remote") or "remote.ics")
    preferred = cal.sanitize_calendar_filename(f"{base.stem}.remote-conflict-{stamp}{base.suffix or '.ics'}")
    candidate = conflict_dir / preferred
    for idx in range(2, 1000):
        if not candidate.exists():
            break
        candidate = conflict_dir / cal.sanitize_calendar_filename(f"{base.stem}.remote-conflict-{stamp}-{idx}{base.suffix or '.ics'}")
    _write_local(candidate, remote_text, _now())
    return candidate


def _remote_href_for_create(account: dict[str, Any], uid: str, kind: str = "event") -> str:
    collection = str(account.get("selected_collection_url") or "").strip()
    if not collection:
        raise ValueError("no CalDAV collection selected for this account")
    if not collection.endswith("/"):
        collection += "/"
    clean_uid = quote(_safe_filename_piece(uid or hashlib.sha256(os.urandom(16)).hexdigest(), 120), safe="._-")
    prefix = "todo" if _kind_label(kind) == "todo" else "event"
    return urljoin(collection, f"{prefix}-{clean_uid}.ics")


def _request_auth(account: dict[str, Any]) -> tuple[str, str] | None:
    username = str(account.get("username") or "")
    password = str(account.get("password") or "")
    if username or password:
        return username, password
    return None


def _request_verify(account: dict[str, Any]) -> bool:
    # Leave certificate verification enabled by default; allow an explicit future
    # registry flag to disable it for self-hosted testing without changing code.
    return bool(account.get("verify_ssl", True))


def _remote_put(account: dict[str, Any], href: str, text: str, *, etag: str = "", create: bool = False) -> dict[str, Any]:
    """PUT with preconditions when possible.  Falls back to caldav-py only if requests is unavailable."""
    clean_href = str(href or "").strip()
    if not clean_href:
        clean_href = _remote_href_for_create(account, "", "event")
    try:
        import requests

        headers = {"Content-Type": "text/calendar; charset=utf-8"}
        if create:
            headers["If-None-Match"] = "*"
        elif etag:
            headers["If-Match"] = etag
        response = requests.put(
            clean_href,
            data=str(text or "").encode("utf-8"),
            headers=headers,
            auth=_request_auth(account),
            timeout=30,
            verify=_request_verify(account),
        )
        if response.status_code == 412:
            raise RuntimeError("remote object changed before PUT completed (ETag precondition failed)")
        if response.status_code == 409 and create:
            # Some providers require parent collection canonicalization; surface a clear error.
            raise RuntimeError(f"CalDAV PUT failed with 409 Conflict for {clean_href}")
        if response.status_code >= 400:
            raise RuntimeError(f"CalDAV PUT failed with HTTP {response.status_code}: {response.text[:300]}")
        return {
            "ok": True,
            "href": clean_href,
            "etag": response.headers.get("ETag", ""),
            "created": create,
            "preconditioned": bool(create or etag),
        }
    except ImportError as exc:
        raise RuntimeError("requests is required for ETag-safe CalDAV PUT") from exc


def _remote_delete(account: dict[str, Any], href: str, *, etag: str = "") -> dict[str, Any]:
    clean_href = str(href or "").strip()
    if not clean_href:
        raise ValueError("href is required")
    try:
        import requests

        headers = {"If-Match": etag} if etag else {}
        response = requests.delete(
            clean_href,
            headers=headers,
            auth=_request_auth(account),
            timeout=30,
            verify=_request_verify(account),
        )
        if response.status_code in {404, 410}:
            return {"ok": True, "href": clean_href, "missing": True, "preconditioned": bool(etag)}
        if response.status_code == 412:
            raise RuntimeError("remote object changed before DELETE completed (ETag precondition failed)")
        if response.status_code >= 400:
            raise RuntimeError(f"CalDAV DELETE failed with HTTP {response.status_code}: {response.text[:300]}")
        return {"ok": True, "href": clean_href, "preconditioned": bool(etag)}
    except ImportError as exc:
        raise RuntimeError("requests is required for ETag-safe CalDAV DELETE") from exc


def _update_ledger_entry_from_local_remote(
    state: dict[str, Any],
    key: str,
    local: dict[str, Any] | None,
    remote: dict[str, Any] | None,
    *,
    tombstone: bool = False,
    conflict: bool = False,
) -> None:
    entry = dict(state.setdefault("objects", {}).get(key) or {})
    source = local or remote or {}
    entry.update({
        "uid": str(source.get("uid") or entry.get("uid") or ""),
        "component_kind": _kind_label(str(source.get("component_kind") or source.get("kind") or entry.get("component_kind") or "event")),
        "local_path": str((local or {}).get("local_path") or entry.get("local_path") or ""),
        "remote_href": str((remote or {}).get("href") or entry.get("remote_href") or ""),
        "remote_etag": str((remote or {}).get("etag") or entry.get("remote_etag") or ""),
        "last_synced_hash": str((local or remote or {}).get("hash") or entry.get("last_synced_hash") or ""),
        "last_local_mtime": float((local or {}).get("modified_epoch") or entry.get("last_local_mtime") or 0),
        "deleted_local": False,
        "deleted_remote": False,
        "tombstone": bool(tombstone),
        "tombstone_at": _iso() if tombstone else "",
        "conflict": bool(conflict),
        "updated_at": _iso(),
    })
    if remote and remote.get("href"):
        entry["remote_href"] = str(remote.get("href") or "")
    if remote and remote.get("etag") is not None:
        entry["remote_etag"] = str(remote.get("etag") or "")
    if local and local.get("local_path"):
        entry["local_path"] = str(local.get("local_path") or "")
    state.setdefault("objects", {})[key] = entry


def _entry_clean_local(entry: dict[str, Any], local: dict[str, Any] | None) -> bool:
    if not local:
        return False
    previous_hash = str(entry.get("last_synced_hash") or "")
    return bool(previous_hash and previous_hash == str(local.get("hash") or ""))


def _entry_clean_remote(entry: dict[str, Any], remote: dict[str, Any] | None) -> bool:
    if not remote:
        return False
    previous_etag = str(entry.get("remote_etag") or "")
    previous_hash = str(entry.get("last_synced_hash") or "")
    remote_etag = str(remote.get("etag") or "")
    if previous_etag and remote_etag:
        return previous_etag == remote_etag
    return bool(previous_hash and previous_hash == str(remote.get("hash") or ""))


def _remote_from_put_result(item: dict[str, Any], result: dict[str, Any], fallback_href: str = "") -> dict[str, Any]:
    remote = dict(item)
    remote["href"] = str(result.get("href") or fallback_href or item.get("href") or "")
    if result.get("etag"):
        remote["etag"] = str(result.get("etag") or "")
    remote["hash"] = str(item.get("hash") or _sha256_text(str(item.get("ics") or "")))
    remote["modified"] = _now()
    return remote


def _local_from_path(path: Path, source: dict[str, Any]) -> dict[str, Any]:
    stat = path.stat()
    text = path.read_text(encoding="utf-8", errors="replace")
    item = dict(source)
    item.update({
        "path": path,
        "local_path": path.name,
        "filename": path.name,
        "ics": text,
        "hash": _sha256_text(text),
        "modified": datetime.fromtimestamp(stat.st_mtime, timezone.utc),
        "modified_epoch": stat.st_mtime,
    })
    return item


def _extract_a0_json_sidecars_for_context(ctxid: str) -> list[str]:
    """Scan local ICS files during sync and write A0 JSON sidecars where present."""
    cal = _calendar_helpers()
    cd = _caldav_helpers()
    extracted: list[str] = []
    for path in cal.local_ics_file_paths(ctxid):
        sidecar = cd.extract_a0_description_json_sidecar(path)
        if sidecar is not None:
            try:
                extracted.append(sidecar.relative_to(cal.context_calendar_dir(ctxid, create=True)).as_posix())
            except ValueError:
                extracted.append(sidecar.name)
    return extracted


def _prune_tombstones(state: dict[str, Any]) -> None:
    cutoff = time.time() - TOMBSTONE_RETENTION_SECONDS
    objects = state.setdefault("objects", {})
    for key in list(objects.keys()):
        entry = objects.get(key) or {}
        if not entry.get("tombstone"):
            continue
        tombstone_at = _parse_iso(entry.get("tombstone_at"))
        if tombstone_at and tombstone_at.timestamp() < cutoff:
            objects.pop(key, None)


def sync_context(ctxid: str, account_id: str | None = None, *, force: bool = False) -> dict[str, Any]:
    """Synchronize one context's local ICS files with its selected CalDAV collection.

    The reconciler is ledger-backed and no-data-loss oriented.  It avoids silent
    overwrites when both sides changed since the last clean sync by writing a
    remote conflict copy and marking the object conflicted in the ledger.
    """
    cal = _calendar_helpers()
    cd = _caldav_helpers()
    clean_ctxid = cal.validate_context_id(ctxid)
    registry, account = cd._find_account(clean_ctxid, account_id)
    calendar_dir = cal.context_calendar_dir(clean_ctxid, create=True)
    state = load_sync_state(clean_ctxid, account)
    actions: list[dict[str, Any]] = []
    errors: list[str] = []
    uploaded = downloaded = deleted_local = deleted_remote = conflicts = skipped = 0
    remote_scan: dict[str, Any] = {}
    local_items: dict[str, dict[str, Any]] = {}
    remote_items: dict[str, dict[str, Any]] = {}

    try:
        with _sync_lock(clean_ctxid):
            state = load_sync_state(clean_ctxid, account)
            state["last_attempt_at"] = _iso()
            state["last_error"] = ""
            save_sync_state(clean_ctxid, state)

            local_items = _local_scan(clean_ctxid)
            initial_a0_json_sidecars = _extract_a0_json_sidecars_for_context(clean_ctxid)
            remote_items, remote_scan, _cal_obj = _remote_scan(account)
            objects = state.setdefault("objects", {})
            keys = sorted(set(local_items) | set(remote_items) | set(objects))
            _prune_tombstones(state)

            for key in keys:
                local = local_items.get(key)
                remote = remote_items.get(key)
                entry = dict(objects.get(key) or {})
                if entry.get("conflict"):
                    conflicts += 1
                    skipped += 1
                    actions.append({"key": key, "uid": entry.get("uid") or key, "action": "skipped", "reason": "unresolved conflict"})
                    continue

                try:
                    local_clean = _entry_clean_local(entry, local)
                    remote_clean = _entry_clean_remote(entry, remote)
                    known = bool(entry)
                    tombstone = bool(entry.get("tombstone"))

                    if local and remote:
                        same_hash = str(local.get("hash") or "") == str(remote.get("hash") or "")
                        if same_hash:
                            skipped += 1
                            _update_ledger_entry_from_local_remote(state, key, local, remote)
                            actions.append({"key": key, "uid": local.get("uid") or remote.get("uid") or key, "action": "skipped", "reason": "already identical"})
                            continue

                        if local_clean and not remote_clean:
                            _write_local(Path(local["path"]), str(remote.get("ics") or ""), remote.get("modified") if isinstance(remote.get("modified"), datetime) else None)
                            new_local = _local_from_path(Path(local["path"]), remote)
                            _update_ledger_entry_from_local_remote(state, key, new_local, remote)
                            downloaded += 1
                            actions.append({"key": key, "uid": remote.get("uid") or key, "action": "downloaded", "filename": new_local.get("filename"), "href": remote.get("href") or "", "reason": "remote changed"})
                            continue

                        if remote_clean and not local_clean:
                            result = _remote_put(account, str(remote.get("href") or entry.get("remote_href") or ""), str(local.get("ics") or ""), etag=str(entry.get("remote_etag") or remote.get("etag") or ""), create=False)
                            pushed_remote = _remote_from_put_result(local, result, str(remote.get("href") or ""))
                            _update_ledger_entry_from_local_remote(state, key, local, pushed_remote)
                            uploaded += 1
                            actions.append({"key": key, "uid": local.get("uid") or key, "action": "uploaded", "filename": local.get("filename"), "href": pushed_remote.get("href") or "", "reason": "local changed", "preconditioned": result.get("preconditioned", False)})
                            continue

                        # If neither side had a previous ledger entry, compare mtime as a
                        # bootstrap heuristic.  Otherwise this is a true conflict.
                        if not known:
                            local_modified = local.get("modified") if isinstance(local.get("modified"), datetime) else datetime.fromtimestamp(0, timezone.utc)
                            remote_modified = remote.get("modified") if isinstance(remote.get("modified"), datetime) else datetime.fromtimestamp(0, timezone.utc)
                            if local_modified >= remote_modified:
                                result = _remote_put(account, str(remote.get("href") or ""), str(local.get("ics") or ""), etag=str(remote.get("etag") or ""), create=False)
                                pushed_remote = _remote_from_put_result(local, result, str(remote.get("href") or ""))
                                _update_ledger_entry_from_local_remote(state, key, local, pushed_remote)
                                uploaded += 1
                                actions.append({"key": key, "uid": local.get("uid") or key, "action": "uploaded", "reason": "initial bootstrap newer local"})
                            else:
                                _write_local(Path(local["path"]), str(remote.get("ics") or ""), remote.get("modified") if isinstance(remote.get("modified"), datetime) else None)
                                new_local = _local_from_path(Path(local["path"]), remote)
                                _update_ledger_entry_from_local_remote(state, key, new_local, remote)
                                downloaded += 1
                                actions.append({"key": key, "uid": remote.get("uid") or key, "action": "downloaded", "reason": "initial bootstrap newer remote"})
                            continue

                        conflict_path = _write_conflict_copy(calendar_dir, str(local.get("local_path") or local.get("filename") or key), str(remote.get("ics") or ""))
                        _update_ledger_entry_from_local_remote(state, key, local, remote, conflict=True)
                        state["objects"][key]["conflict_type"] = "local_and_remote_changed"
                        conflicts += 1
                        actions.append({"key": key, "uid": local.get("uid") or remote.get("uid") or key, "action": "conflict", "filename": local.get("filename"), "conflict_file": conflict_path.relative_to(calendar_dir).as_posix(), "reason": "local and remote both changed"})
                        continue

                    if local and not remote:
                        if known and entry.get("remote_href") and not tombstone:
                            if _entry_clean_local(entry, local):
                                # Remote disappeared and local is unchanged since last sync.
                                trashed = _move_to_trash(Path(local["path"]))
                                _update_ledger_entry_from_local_remote(state, key, None, None, tombstone=True)
                                state["objects"][key].update({
                                    "uid": local.get("uid") or entry.get("uid") or "",
                                    "component_kind": local.get("component_kind") or entry.get("component_kind") or "event",
                                    "local_path": str(local.get("local_path") or entry.get("local_path") or ""),
                                    "remote_href": str(entry.get("remote_href") or ""),
                                    "remote_etag": "",
                                })
                                deleted_local += 1
                                actions.append({"key": key, "uid": local.get("uid") or key, "action": "deleted_local", "filename": local.get("filename"), "trash": trashed.relative_to(calendar_dir).as_posix(), "reason": "remote deleted"})
                                continue
                            # Local changed while the remote object disappeared.  Keep the
                            # local file and require explicit user resolution.
                            _update_ledger_entry_from_local_remote(state, key, local, None, conflict=True)
                            state["objects"][key].update({
                                "remote_href": str(entry.get("remote_href") or ""),
                                "remote_etag": str(entry.get("remote_etag") or ""),
                                "remote_deleted_conflict": True,
                                "conflict_type": "local_changed_remote_deleted",
                            })
                            conflicts += 1
                            actions.append({"key": key, "uid": local.get("uid") or key, "action": "conflict", "filename": local.get("filename"), "reason": "local changed and remote deleted"})
                            continue

                        if tombstone:
                            skipped += 1
                            actions.append({"key": key, "uid": local.get("uid") or key, "action": "skipped", "reason": "local file exists for tombstoned object"})
                            continue

                        href = str(entry.get("remote_href") or "") or _remote_href_for_create(account, str(local.get("uid") or ""), str(local.get("component_kind") or "event"))
                        result = _remote_put(account, href, str(local.get("ics") or ""), etag=str(entry.get("remote_etag") or ""), create=not bool(entry.get("remote_href")))
                        pushed_remote = _remote_from_put_result(local, result, href)
                        _update_ledger_entry_from_local_remote(state, key, local, pushed_remote)
                        uploaded += 1
                        actions.append({"key": key, "uid": local.get("uid") or key, "action": "uploaded", "filename": local.get("filename"), "href": pushed_remote.get("href") or "", "reason": "missing on CalDAV", "preconditioned": result.get("preconditioned", False)})
                        continue

                    if remote and not local:
                        local_path_text = str(entry.get("local_path") or "")
                        if known and local_path_text and not tombstone:
                            if _entry_clean_remote(entry, remote):
                                # Local file disappeared and remote is unchanged since last sync.
                                result = _remote_delete(account, str(entry.get("remote_href") or remote.get("href") or ""), etag=str(entry.get("remote_etag") or remote.get("etag") or ""))
                                _update_ledger_entry_from_local_remote(state, key, None, None, tombstone=True)
                                state["objects"][key].update({
                                    "uid": remote.get("uid") or entry.get("uid") or "",
                                    "component_kind": remote.get("component_kind") or entry.get("component_kind") or "event",
                                    "remote_href": str(remote.get("href") or entry.get("remote_href") or ""),
                                    "remote_etag": "",
                                    "local_path": local_path_text,
                                })
                                deleted_remote += 1
                                actions.append({"key": key, "uid": remote.get("uid") or key, "action": "deleted_remote", "href": remote.get("href") or "", "reason": "local deleted", "preconditioned": result.get("preconditioned", False)})
                                continue
                            # Local was deleted while the remote object changed.  Preserve
                            # the remote body as a conflict copy instead of resurrecting it.
                            conflict_path = _write_conflict_copy(calendar_dir, local_path_text or _filename_for_item(remote), str(remote.get("ics") or ""))
                            _update_ledger_entry_from_local_remote(state, key, None, remote, conflict=True)
                            state["objects"][key].update({"local_path": local_path_text, "conflict_type": "local_deleted_remote_changed"})
                            conflicts += 1
                            actions.append({"key": key, "uid": remote.get("uid") or key, "action": "conflict", "href": remote.get("href") or "", "conflict_file": conflict_path.relative_to(calendar_dir).as_posix(), "reason": "local deleted and remote changed"})
                            continue

                        if tombstone:
                            skipped += 1
                            actions.append({"key": key, "uid": remote.get("uid") or key, "action": "skipped", "reason": "remote object exists for tombstoned local deletion"})
                            continue

                        preferred = local_path_text or _filename_for_item(remote)
                        existing_path = (calendar_dir / local_path_text) if local_path_text else None
                        path = _unique_path(calendar_dir, preferred, existing_path=existing_path if existing_path and existing_path.exists() else None)
                        _write_local(path, str(remote.get("ics") or ""), remote.get("modified") if isinstance(remote.get("modified"), datetime) else None)
                        new_local = _local_from_path(path, remote)
                        _update_ledger_entry_from_local_remote(state, key, new_local, remote)
                        downloaded += 1
                        actions.append({"key": key, "uid": remote.get("uid") or key, "action": "downloaded", "filename": path.name, "href": remote.get("href") or "", "reason": "missing locally"})
                        continue

                    if not local and not remote and known:
                        # Both sides gone.  Keep a tombstone for a retention window.
                        if not entry.get("tombstone"):
                            entry["tombstone"] = True
                            entry["tombstone_at"] = _iso()
                            entry["updated_at"] = _iso()
                            objects[key] = entry
                        skipped += 1
                        continue

                except Exception as exc:
                    error = f"{key}: {exc}"
                    errors.append(error)
                    actions.append({"key": key, "uid": (local or remote or entry).get("uid") or key, "action": "error", "error": str(exc)})

            state["last_attempt_at"] = state.get("last_attempt_at") or _iso()
            if errors:
                state["last_error"] = "; ".join(errors)[:1000]
            else:
                state["last_success_at"] = _iso()
                state["last_error"] = ""
            final_a0_json_sidecars = sorted(set(initial_a0_json_sidecars + _extract_a0_json_sidecars_for_context(clean_ctxid)))
            state["last_sync_summary"] = {
                "ok": not errors,
                "uploaded": uploaded,
                "downloaded": downloaded,
                "deleted_local": deleted_local,
                "deleted_remote": deleted_remote,
                "conflicts": conflicts,
                "skipped": skipped,
                "errors": errors,
                "actions": actions[-200:],
                "local_count": len(local_items),
                "remote_count": len(remote_items),
                "a0_json_sidecars": final_a0_json_sidecars,
                "scanned_collection": account.get("selected_collection_name") or account.get("selected_collection_url") or "",
                "remote_scan": remote_scan,
                "finished_at": _iso(),
            }
            save_sync_state(clean_ctxid, state)

            account["status"] = "ok" if not errors else "error"
            account["last_error"] = "; ".join(errors)[:1000]
            account["last_verified"] = _iso()
            cd.save_caldav_registry(clean_ctxid, registry)

    except RuntimeError as exc:
        if "already running" not in str(exc):
            raise
        status = get_status(clean_ctxid, account_id)
        payload = cal.list_calendar_stack(clean_ctxid)
        payload["ok"] = False
        payload["error"] = str(exc)
        payload["sync_status"] = status
        payload["sync"] = {"ok": False, "syncing": True, "errors": [str(exc)], "actions": []}
        return payload
    except Exception as exc:
        state = load_sync_state(clean_ctxid, account)
        state["last_attempt_at"] = state.get("last_attempt_at") or _iso()
        state["last_error"] = str(exc)
        state["last_sync_summary"] = {
            "ok": False,
            "uploaded": uploaded,
            "downloaded": downloaded,
            "deleted_local": deleted_local,
            "deleted_remote": deleted_remote,
            "conflicts": conflicts,
            "skipped": skipped,
            "errors": [str(exc)],
            "actions": actions,
            "local_count": len(local_items),
            "remote_count": len(remote_items),
            "scanned_collection": account.get("selected_collection_name") or account.get("selected_collection_url") or "",
            "remote_scan": remote_scan,
            "finished_at": _iso(),
        }
        save_sync_state(clean_ctxid, state)
        account["status"] = "error"
        account["last_error"] = str(exc)
        cd.save_caldav_registry(clean_ctxid, registry)

    payload = cal.list_calendar_stack(clean_ctxid)
    status = get_status(clean_ctxid, account_id)
    summary = state.get("last_sync_summary") or {}
    payload["ok"] = not bool(summary.get("errors"))
    payload["sync"] = summary
    payload["sync_status"] = status
    if summary.get("errors"):
        payload["error"] = "; ".join(str(e) for e in summary.get("errors") or [])
    return payload


def _conflict_copy_relative_path(calendar_dir: Path, entry: dict[str, Any]) -> str:
    copy = _find_conflict_remote_copy(calendar_dir, entry)
    if copy is None:
        return ""
    try:
        return copy.relative_to(calendar_dir).as_posix()
    except ValueError:
        return copy.name


def _sync_conflict_details(calendar_dir: Path, objects: dict[str, Any]) -> list[dict[str, Any]]:
    """Return UI-safe details for unresolved sync conflicts."""
    conflicts: list[dict[str, Any]] = []
    for key, raw_entry in sorted(objects.items(), key=lambda item: str(item[0])):
        if not isinstance(raw_entry, dict) or not raw_entry.get("conflict"):
            continue
        entry = dict(raw_entry)
        local_path = str(entry.get("local_path") or "")
        local_exists = bool(local_path and (calendar_dir / local_path).exists())
        conflict_copy = _conflict_copy_relative_path(calendar_dir, entry)
        conflict_type = str(entry.get("conflict_type") or "").strip()
        reason_map = {
            "local_and_remote_changed": "Local and remote both changed",
            "local_changed_remote_deleted": "Local changed and remote was deleted",
            "local_deleted_remote_changed": "Local was deleted and remote changed",
        }
        conflicts.append({
            "key": str(key),
            "uid": str(entry.get("uid") or ""),
            "component_kind": _kind_label(str(entry.get("component_kind") or entry.get("kind") or "event")),
            "summary": str(entry.get("summary") or entry.get("last_summary") or ""),
            "local_path": local_path,
            "local_exists": local_exists,
            "remote_href": str(entry.get("remote_href") or ""),
            "remote_etag": str(entry.get("remote_etag") or ""),
            "conflict_type": conflict_type,
            "reason": reason_map.get(conflict_type, conflict_type.replace("_", " ").strip() or "Unresolved sync conflict"),
            "conflict_file": conflict_copy,
            "remote_copy_exists": bool(conflict_copy),
            "updated_at": str(entry.get("updated_at") or ""),
        })
    return conflicts

def get_status(ctxid: str, account_id: str | None = None) -> dict[str, Any]:
    cal = _calendar_helpers()
    cd = _caldav_helpers()
    clean_ctxid = cal.validate_context_id(ctxid)
    account: dict[str, Any] | None = None
    try:
        _registry, account = cd._find_account(clean_ctxid, account_id)
    except Exception:
        account = None
    state = load_sync_state(clean_ctxid, account)
    objects = state.get("objects") if isinstance(state.get("objects"), dict) else {}
    last_success = _parse_iso(state.get("last_success_at"))
    age_seconds = int((_now() - last_success).total_seconds()) if last_success else None
    calendar_dir = cal.context_calendar_dir(clean_ctxid, create=True)
    conflicts = _sync_conflict_details(calendar_dir, objects)
    conflict_count = len(conflicts)
    tombstone_count = sum(1 for entry in objects.values() if isinstance(entry, dict) and entry.get("tombstone"))
    lock_exists = _lock_path(clean_ctxid).exists()
    last_error = str(state.get("last_error") or "")
    if conflict_count:
        status_state = "conflict"
    elif lock_exists:
        status_state = "syncing"
    elif last_error:
        status_state = "error"
    elif age_seconds is None:
        status_state = "never"
    elif age_seconds > STALE_SYNC_SECONDS:
        status_state = "stale"
    elif age_seconds > NORMAL_SYNC_INTERVAL_SECONDS:
        status_state = "warning"
    else:
        status_state = "ok"
    return {
        "ok": not bool(last_error),
        "state": status_state,
        "ctxid": clean_ctxid,
        "account_id": state.get("account_id") or (account or {}).get("id") or "",
        "collection_url": state.get("collection_url") or (account or {}).get("selected_collection_url") or "",
        "last_attempt_at": state.get("last_attempt_at") or "",
        "last_success_at": state.get("last_success_at") or "",
        "age_seconds": age_seconds,
        "stale": bool(age_seconds is None or age_seconds > STALE_SYNC_SECONDS),
        "syncing": lock_exists,
        "conflict_count": conflict_count,
        "conflicts": conflicts,
        "tombstone_count": tombstone_count,
        "tracked_count": len(objects),
        "last_error": last_error,
        "last_sync_summary": state.get("last_sync_summary") or {},
    }


def _replace_ics_uid(text: str, new_uid: str) -> str:
    lines = str(text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: list[str] = []
    replaced = False
    for line in lines:
        name, params, _value = _calendar_helpers().split_content_line(line)
        if name == "UID" and not replaced:
            suffix = f";{params}" if params else ""
            out.append(f"UID{suffix}:{new_uid}")
            replaced = True
        else:
            out.append(line)
    return "\r\n".join(out).rstrip("\r\n") + "\r\n"


def _find_conflict_remote_copy(calendar_dir: Path, entry: dict[str, Any]) -> Path | None:
    local_name = str(entry.get("local_path") or "")
    stem = Path(local_name or "remote").stem
    conflict_dir = calendar_dir / SYNC_CONFLICT_DIRNAME
    if not conflict_dir.exists():
        return None
    candidates = sorted(conflict_dir.glob(f"{stem}.remote-conflict-*.ics"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def resolve_conflict(ctxid: str, uid: str = "", component_kind: str = "", strategy: str = "") -> dict[str, Any]:
    cal = _calendar_helpers()
    cd = _caldav_helpers()
    clean_ctxid = cal.validate_context_id(ctxid)
    clean_strategy = str(strategy or "").strip().lower()
    if clean_strategy not in {"keep_local", "keep_remote", "duplicate_both", "clear"}:
        raise ValueError("strategy must be keep_local, keep_remote, duplicate_both, or clear")
    state = load_sync_state(clean_ctxid)
    objects = state.setdefault("objects", {})
    key = _component_key(uid, component_kind) if uid else ""
    if not key or key not in objects:
        matches = [k for k, v in objects.items() if isinstance(v, dict) and v.get("conflict") and (not uid or str(v.get("uid") or "") == str(uid))]
        if len(matches) == 1:
            key = matches[0]
        elif not matches:
            raise ValueError("conflict not found")
        else:
            raise ValueError("multiple conflicts match; include component_kind")
    entry = dict(objects.get(key) or {})
    if not entry.get("conflict"):
        return {"ok": True, "resolved": False, "reason": "object is not conflicted", "sync_status": get_status(clean_ctxid)}

    calendar_dir = cal.context_calendar_dir(clean_ctxid, create=True)
    conflict_copy = _find_conflict_remote_copy(calendar_dir, entry)
    local_path = calendar_dir / str(entry.get("local_path") or "") if entry.get("local_path") else None

    local_item: dict[str, Any] | None = None
    remote_item: dict[str, Any] | None = None

    if clean_strategy == "keep_local":
        if local_path is None or not local_path.exists():
            raise ValueError("local conflict file is missing")
        _registry, account = cd._find_account(clean_ctxid, str(entry.get("account_id") or ""))
        local_item = _local_from_path(local_path, {
            "uid": entry.get("uid") or uid,
            "component_kind": entry.get("component_kind") or component_kind or "event",
            "kind": _kind_component(str(entry.get("component_kind") or component_kind or "event")),
        })
        href = str(entry.get("remote_href") or "") or _remote_href_for_create(account, str(local_item.get("uid") or ""), str(local_item.get("component_kind") or "event"))
        recreate_deleted_remote = bool(entry.get("remote_deleted_conflict"))
        result = _remote_put(
            account,
            href,
            str(local_item.get("ics") or ""),
            etag="" if recreate_deleted_remote else str(entry.get("remote_etag") or ""),
            create=bool(recreate_deleted_remote or not entry.get("remote_href")),
        )
        remote_item = _remote_from_put_result(local_item, result, href)
        _update_ledger_entry_from_local_remote(state, key, local_item, remote_item, conflict=False)

    elif clean_strategy == "keep_remote":
        if conflict_copy is None or not conflict_copy.exists():
            raise ValueError("remote conflict copy is missing")
        if local_path is None:
            local_path = _unique_path(calendar_dir, _filename_for_item(entry))
        remote_text = conflict_copy.read_text(encoding="utf-8", errors="replace")
        _write_local(local_path, remote_text, _now())
        local_item = _local_from_path(local_path, {
            "uid": entry.get("uid") or uid,
            "component_kind": entry.get("component_kind") or component_kind or "event",
            "kind": _kind_component(str(entry.get("component_kind") or component_kind or "event")),
        })
        remote_item = {
            "uid": entry.get("uid") or local_item.get("uid") or uid,
            "component_kind": entry.get("component_kind") or local_item.get("component_kind") or "event",
            "kind": _kind_component(str(entry.get("component_kind") or local_item.get("component_kind") or "event")),
            "href": str(entry.get("remote_href") or ""),
            "etag": str(entry.get("remote_etag") or ""),
            "ics": remote_text,
            "hash": _sha256_text(remote_text),
            "modified": _now(),
        }
        _update_ledger_entry_from_local_remote(state, key, local_item, remote_item, conflict=False)

    elif clean_strategy == "duplicate_both":
        # Keep the local object as the original and import the remote copy as a
        # new local object with a fresh UID, so two files with the same UID do not
        # fight each other in future scans.
        if conflict_copy is None or not conflict_copy.exists():
            raise ValueError("remote conflict copy is missing")
        old_uid = str(entry.get("uid") or uid or "item")
        stamp = _now().strftime("%Y%m%dT%H%M%SZ")
        new_uid = f"{old_uid}-remote-copy-{stamp}"
        remote_text = _replace_ics_uid(conflict_copy.read_text(encoding="utf-8", errors="replace"), new_uid)
        preferred = _filename_for_item({"uid": new_uid, "summary": f"Remote copy {old_uid}"})
        duplicate_path = _unique_path(calendar_dir, preferred)
        _write_local(duplicate_path, remote_text, _now())
        # Keep the original local side as dirty; a subsequent sync will upload it.
        if local_path is not None and local_path.exists():
            local_item = _local_from_path(local_path, {
                "uid": entry.get("uid") or uid,
                "component_kind": entry.get("component_kind") or component_kind or "event",
                "kind": _kind_component(str(entry.get("component_kind") or component_kind or "event")),
            })
            remote_item = {
                "uid": entry.get("uid") or uid,
                "component_kind": entry.get("component_kind") or component_kind or "event",
                "kind": _kind_component(str(entry.get("component_kind") or component_kind or "event")),
                "href": str(entry.get("remote_href") or ""),
                "etag": str(entry.get("remote_etag") or ""),
                "hash": str(local_item.get("hash") or ""),
            }
            _update_ledger_entry_from_local_remote(state, key, local_item, remote_item, conflict=False)
        entry = dict(objects.get(key) or entry)
        entry["duplicate_remote_path"] = duplicate_path.name
        objects[key] = entry

    else:
        entry["conflict"] = False
        objects[key] = entry

    entry = dict(objects.get(key) or entry)
    entry["conflict"] = False
    entry.pop("remote_deleted_conflict", None)
    entry.pop("conflict_type", None)
    entry["resolved_at"] = _iso()
    entry["resolved_strategy"] = clean_strategy
    objects[key] = entry
    save_sync_state(clean_ctxid, state)
    return {"ok": True, "resolved": True, "key": key, "strategy": clean_strategy, "sync_status": get_status(clean_ctxid)}


def discover_sync_contexts() -> list[str]:
    cal = _calendar_helpers()
    cd = _caldav_helpers()
    root = cal.CHATS_ROOT
    out: list[str] = []
    try:
        for chat_dir in sorted(root.iterdir(), key=lambda p: p.name.lower()):
            if not chat_dir.is_dir():
                continue
            ctxid = chat_dir.name
            try:
                account = cd.caldav_account_entry(ctxid)
                if isinstance(account, dict) and str(account.get("selected_collection_url") or "").strip():
                    out.append(ctxid)
            except Exception:
                continue
    except Exception:
        return []
    return out


def sync_due_contexts(*, max_contexts: int = 3) -> dict[str, Any]:
    """Run due background syncs.  Intended for the global job_loop tick."""
    results: list[dict[str, Any]] = []
    checked = 0
    synced = 0
    for ctxid in discover_sync_contexts():
        checked += 1
        try:
            status = get_status(ctxid)
            last_attempt = _parse_iso(status.get("last_attempt_at"))
            last_success = _parse_iso(status.get("last_success_at"))
            last_error = str(status.get("last_error") or "")
            now = _now()
            due = False
            if status.get("syncing"):
                due = False
            elif last_success is None:
                due = True
            elif (now - last_success).total_seconds() >= NORMAL_SYNC_INTERVAL_SECONDS:
                due = True
            if last_error and last_attempt is not None:
                # Exponential-ish retry based on recent failed attempts, capped by the stale target.
                summary = status.get("last_sync_summary") if isinstance(status.get("last_sync_summary"), dict) else {}
                error_count = len(summary.get("errors") or []) if isinstance(summary, dict) else 1
                retry_after = min(STALE_SYNC_SECONDS, max(5 * 60, (2 ** min(error_count, 4)) * 5 * 60))
                due = (now - last_attempt).total_seconds() >= retry_after
            if not due:
                continue
            result = sync_context(ctxid)
            results.append({
                "ctxid": ctxid,
                "ok": bool(result.get("ok")),
                "sync": result.get("sync") or {},
            })
            synced += 1
            if synced >= max_contexts:
                break
        except Exception as exc:
            results.append({"ctxid": ctxid, "ok": False, "error": str(exc)})
    return {"ok": True, "checked": checked, "synced": synced, "results": results}
