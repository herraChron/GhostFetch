# db.py — GhostFetch SQLite persistence layer
# PERF-01: replaces 7 separate JSON files with a single WAL-mode SQLite database.
# All public functions are async (aiosqlite).  Import this module and call init_db()
# before any other function.  Existing JSON files are migrated automatically on first run.
#
# Tables:
#   downloaded_ids  (chat_id TEXT, msg_id INTEGER, PRIMARY KEY (chat_id, msg_id))
#   file_hashes     (hash TEXT PRIMARY KEY)
#   file_unique_ids (unique_id TEXT PRIMARY KEY)
#   chat_names      (folder_id TEXT PRIMARY KEY, title TEXT)
#   sessions        (key TEXT PRIMARY KEY, value TEXT)
#   ledger          (folder_id TEXT PRIMARY KEY, data TEXT)
#   job_history     (id INTEGER PRIMARY KEY AUTOINCREMENT, bytes REAL, duration REAL, ts REAL)

from __future__ import annotations

import json
import logging
import os
from typing import Any

import aiosqlite

log = logging.getLogger(__name__)

DB_PATH = "ghostfetch.db"

# JSON files that may exist from the old persistence layer — migrated then left in place
_LEGACY_FILES = {
    "downloaded_ids": "downloaded_ids.json",
    "file_hashes":    "file_hashes.json",
    "file_unique_ids":"unique_ids.json",
    "chat_names":     "chat_names.json",
    "session":        "session.json",
    "ledger":         "ledger.json",
    "job_history":    "job_history.json",
}

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS downloaded_ids (
    chat_id  TEXT    NOT NULL,
    msg_id   INTEGER NOT NULL,
    PRIMARY KEY (chat_id, msg_id)
);

CREATE TABLE IF NOT EXISTS file_hashes (
    hash TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS file_unique_ids (
    unique_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS chat_names (
    folder_id TEXT PRIMARY KEY,
    title     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger (
    folder_id TEXT PRIMARY KEY,
    data      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_history (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    bytes    REAL    NOT NULL,
    duration REAL    NOT NULL,
    ts       REAL    NOT NULL
);
"""


async def init_db() -> None:
    """Create schema and run one-time JSON migration if needed."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_DDL)
        await db.commit()
    await _migrate_legacy()
    log.info("DB initialised (WAL mode)")


# ───────────────────────────────────────────────────────────────
# Migration: JSON → SQLite (runs only once; harmless if re-run)
# ───────────────────────────────────────────────────────────────

async def _migrate_legacy() -> None:
    migrated: list[str] = []

    # downloaded_ids.json  → downloaded_ids table
    legacy = _LEGACY_FILES["downloaded_ids"]
    if os.path.exists(legacy):
        try:
            with open(legacy, encoding="utf-8") as f:
                ids: list[int] = json.load(f)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT OR IGNORE INTO downloaded_ids (chat_id, msg_id) VALUES (?, ?)",
                    [("legacy", mid) for mid in ids if isinstance(mid, int)],
                )
                await db.commit()
            migrated.append(legacy)
        except Exception as e:
            log.warning(f"Migration failed for {legacy}: {e}")

    # file_hashes.json → file_hashes table
    legacy = _LEGACY_FILES["file_hashes"]
    if os.path.exists(legacy):
        try:
            with open(legacy, encoding="utf-8") as f:
                hashes: list[str] = json.load(f)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT OR IGNORE INTO file_hashes (hash) VALUES (?)",
                    [(h,) for h in hashes if isinstance(h, str)],
                )
                await db.commit()
            migrated.append(legacy)
        except Exception as e:
            log.warning(f"Migration failed for {legacy}: {e}")

    # unique_ids.json → file_unique_ids table
    legacy = _LEGACY_FILES["file_unique_ids"]
    if os.path.exists(legacy):
        try:
            with open(legacy, encoding="utf-8") as f:
                uids: list[str] = json.load(f)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT OR IGNORE INTO file_unique_ids (unique_id) VALUES (?)",
                    [(u,) for u in uids if isinstance(u, str)],
                )
                await db.commit()
            migrated.append(legacy)
        except Exception as e:
            log.warning(f"Migration failed for {legacy}: {e}")

    # chat_names.json → chat_names table
    legacy = _LEGACY_FILES["chat_names"]
    if os.path.exists(legacy):
        try:
            with open(legacy, encoding="utf-8") as f:
                names: dict[str, str] = json.load(f)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT OR REPLACE INTO chat_names (folder_id, title) VALUES (?, ?)",
                    [(k, v) for k, v in names.items()],
                )
                await db.commit()
            migrated.append(legacy)
        except Exception as e:
            log.warning(f"Migration failed for {legacy}: {e}")

    # session.json → sessions table (key="selected_chat")
    legacy = _LEGACY_FILES["session"]
    if os.path.exists(legacy):
        try:
            with open(legacy, encoding="utf-8") as f:
                session_data = json.load(f)
            if isinstance(session_data, dict):
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "INSERT OR REPLACE INTO sessions (key, value) VALUES (?, ?)",
                        ("selected_chat", json.dumps(session_data)),
                    )
                    await db.commit()
            migrated.append(legacy)
        except Exception as e:
            log.warning(f"Migration failed for {legacy}: {e}")

    # ledger.json → ledger table
    legacy = _LEGACY_FILES["ledger"]
    if os.path.exists(legacy):
        try:
            with open(legacy, encoding="utf-8") as f:
                ledger_data: dict[str, Any] = json.load(f)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT OR REPLACE INTO ledger (folder_id, data) VALUES (?, ?)",
                    [(k, json.dumps(v)) for k, v in ledger_data.items()],
                )
                await db.commit()
            migrated.append(legacy)
        except Exception as e:
            log.warning(f"Migration failed for {legacy}: {e}")

    # job_history.json → job_history table
    legacy = _LEGACY_FILES["job_history"]
    if os.path.exists(legacy):
        try:
            with open(legacy, encoding="utf-8") as f:
                history: list[dict] = json.load(f)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT INTO job_history (bytes, duration, ts) VALUES (?, ?, ?)",
                    [
                        (h.get("bytes", 0), h.get("duration", 0), h.get("ts", 0))
                        for h in history
                        if isinstance(h, dict)
                    ],
                )
                await db.commit()
            migrated.append(legacy)
        except Exception as e:
            log.warning(f"Migration failed for {legacy}: {e}")

    if migrated:
        log.info(f"Migrated {len(migrated)} legacy JSON file(s) to SQLite: {migrated}")


# ───────────────────────────────────────────────────────────────
# downloaded_ids
# ───────────────────────────────────────────────────────────────

async def load_downloaded_ids(chat_id: str = "legacy") -> set[int]:
    """Load all downloaded message IDs for a chat (or global 'legacy' bucket)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT msg_id FROM downloaded_ids WHERE chat_id = ?", (chat_id,)
        ) as cur:
            rows = await cur.fetchall()
    return {r[0] for r in rows}


async def load_all_downloaded_ids() -> set[int]:
    """Load every downloaded message ID regardless of chat (for backward compatibility)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT msg_id FROM downloaded_ids") as cur:
            rows = await cur.fetchall()
    return {r[0] for r in rows}


async def persist_downloaded_id(msg_id: int, chat_id: str = "legacy") -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO downloaded_ids (chat_id, msg_id) VALUES (?, ?)",
            (chat_id, msg_id),
        )
        await db.commit()


async def clear_downloaded_ids(chat_id: str | None = None) -> None:
    """Clear IDs for a specific chat, or all IDs if chat_id is None."""
    async with aiosqlite.connect(DB_PATH) as db:
        if chat_id:
            await db.execute("DELETE FROM downloaded_ids WHERE chat_id = ?", (chat_id,))
        else:
            await db.execute("DELETE FROM downloaded_ids")
        await db.commit()


# ───────────────────────────────────────────────────────────────
# file_hashes
# ───────────────────────────────────────────────────────────────

async def load_file_hashes() -> set[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT hash FROM file_hashes") as cur:
            rows = await cur.fetchall()
    return {r[0] for r in rows}


async def add_file_hash(hash_val: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO file_hashes (hash) VALUES (?)", (hash_val,))
        await db.commit()


# ───────────────────────────────────────────────────────────────
# file_unique_ids  (PERF-05)
# ───────────────────────────────────────────────────────────────

async def load_unique_ids() -> set[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT unique_id FROM file_unique_ids") as cur:
            rows = await cur.fetchall()
    return {r[0] for r in rows}


async def add_unique_id(unique_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO file_unique_ids (unique_id) VALUES (?)", (unique_id,)
        )
        await db.commit()


# ───────────────────────────────────────────────────────────────
# chat_names
# ───────────────────────────────────────────────────────────────

async def load_chat_names() -> dict[str, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT folder_id, title FROM chat_names") as cur:
            rows = await cur.fetchall()
    return {r[0]: r[1] for r in rows}


async def save_chat_name(folder_id: str, title: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO chat_names (folder_id, title) VALUES (?, ?)",
            (folder_id, title),
        )
        await db.commit()


# ───────────────────────────────────────────────────────────────
# sessions
# ───────────────────────────────────────────────────────────────

async def load_session(key: str = "selected_chat") -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM sessions WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
    if row:
        try:
            return json.loads(row[0])
        except Exception:
            return None
    return None


async def save_session(data: dict, key: str = "selected_chat") -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO sessions (key, value) VALUES (?, ?)",
            (key, json.dumps(data, ensure_ascii=False)),
        )
        await db.commit()


# ───────────────────────────────────────────────────────────────
# ledger
# ───────────────────────────────────────────────────────────────

async def load_ledger() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT folder_id, data FROM ledger") as cur:
            rows = await cur.fetchall()
    result = {}
    for folder_id, data in rows:
        try:
            result[folder_id] = json.loads(data)
        except Exception:
            pass
    return result


async def save_ledger_entry(folder_id: str, entry: dict) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO ledger (folder_id, data) VALUES (?, ?)",
            (folder_id, json.dumps(entry, ensure_ascii=False)),
        )
        await db.commit()


# ───────────────────────────────────────────────────────────────
# job_history
# ───────────────────────────────────────────────────────────────

async def load_job_history(limit: int = 15) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT bytes, duration, ts FROM job_history ORDER BY id DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
    return [{"bytes": r[0], "duration": r[1], "ts": r[2]} for r in reversed(rows)]


async def append_job_record(bytes_total: float, duration: float, ts: float) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO job_history (bytes, duration, ts) VALUES (?, ?, ?)",
            (bytes_total, duration, ts),
        )
        # Keep only the latest 15 records
        await db.execute(
            "DELETE FROM job_history WHERE id NOT IN "
            "(SELECT id FROM job_history ORDER BY id DESC LIMIT 15)"
        )
        await db.commit()
