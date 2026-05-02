# A0 Scheduler

A0 Scheduler is an Agent Zero plugin that provides the CalDAVnTasks calendar/task functionality extracted from A0 Superordinates.

## Runtime plugin name

```text
a0_scheduler
```

## Repository name

```text
A0_Scheduler
```

## Features

- Per-chat local `.ics` storage under `/a0/usr/chats/<contextid>/calendar/`
- VEVENT and VTODO creation, edit, delete, and compact card UI
- Collision-safe filenames for newly created ICS files using `-A0_` plus a 24-character lowercase-hex token
- CalDAV account setup, collection discovery/selection, optional WebUI calendar URL, and bidirectional sync
- Ledger-backed local/remote conflict handling with trash and conflict copies
- 15-minute CalDAV sync cadence with stale warnings after 1 hour
- A0 JSON sidecar extraction from `DESCRIPTION` values containing `!{"a0_name":`
- Sidecar cleanup when an ICS description no longer contains an A0 JSON marker
- Read-only calendar indicator helpers that other plugins, such as A0 Superordinates, may consume optionally

## API

```text
POST /api/plugins/a0_scheduler/agent_calendar
```

Supported actions include:

- `list`
- `create_ics`
- `read_ics`
- `save_ics`
- `delete_ics` / `delete_local_ics` / `delete_calendar`
- `upsert_event` / `delete_event`
- `upsert_todo` / `delete_todo`
- `get_caldav_account` / `set_caldav_account`
- `list_caldav_accounts` / `add_caldav_account`
- `remove_caldav_account`
- `test_caldav_account`
- `list_caldav_collections`
- `select_caldav_collection`
- `sync_caldav_ics_files`
- `get_caldav_sync_status` / `sync_caldav_status`
- `resolve_caldav_sync_conflict`
- `list_caldav_events`
- `get_caldav_event`
- `upsert_caldav_event`
- `delete_caldav_event`

## Storage

Local calendar data is stored per context:

```text
/a0/usr/chats/<contextid>/calendar/
```

CalDAV account metadata is stored in the same folder as:

```text
/a0/usr/chats/<contextid>/calendar/caldav.json
```

The plugin continues to use this existing storage location so calendars created before extraction remain discoverable without a data migration.
