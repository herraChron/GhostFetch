# GhostFetch — Telegram restricted media downloader
# herraChron
# Updated: MOD-01 through MOD-28

import os
import json
import shutil
import psutil
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from time import time
from datetime import datetime
from uuid import uuid4                           # MOD-03

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid, BadRequest, MessageNotModified
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from config import PyroConf


# ───────────────────────────────────────────────────────────────
# Constants
# ───────────────────────────────────────────────────────────────

DOWNLOAD_BASE       = "/data/data/com.termux/files/home/GhostFetch/downloads"
SESSION_FILE        = "session.json"
CHAT_NAMES_FILE     = "chat_names.json"
DOWNLOADED_IDS_FILE = "downloaded_ids.json"     # MOD-18
LOG_FILE            = "session.log"
BOT_START_TIME      = time()

MAX_LOG_SIZE      = 5 * 1024 * 1024
RETRY_ATTEMPTS    = 3
PROGRESS_INTERVAL = 2.0
PAGE_SIZE         = 8
FILES_PAGE_SIZE   = 5
SEARCH_LIMIT      = 5000
DOT               = "•"

# MOD-27: authorization whitelist; empty list = allow all users
ALLOWED_USER_IDS: list[int] = getattr(PyroConf, "ALLOWED_USER_IDS", [])


# ───────────────────────────────────────────────────────────────
# Logging
# ───────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
_fh  = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=2, encoding="utf-8")
_fh.setFormatter(_fmt)
_ch  = logging.StreamHandler()
_ch.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_ch)


# ───────────────────────────────────────────────────────────────
# Pyrogram clients  (created inside main() to bind to the loop)
# ───────────────────────────────────────────────────────────────

bot         = None
user_client = None


# ───────────────────────────────────────────────────────────────
# Global state
# ───────────────────────────────────────────────────────────────

dialogs_cache:  list             = []
selected_chat:  dict             = {}
downloaded_ids: set              = set()         # populated from disk on start (MOD-18)
user_state:     dict             = {}            # uid → string state
search_results: dict             = {}            # uid → list of dialog dicts

_job_queue:     list             = []
_current_job:   dict | None      = None
_worker_task:   asyncio.Task | None = None
_bulk_task:     asyncio.Task | None = None
_bulk_state:    dict             = {"cancelled": False}
_files_nav:     dict             = {}
_fname_results: dict             = {}            # uid → [{id, name}]


# ───────────────────────────────────────────────────────────────
# MOD-27: Authorization helper
# ───────────────────────────────────────────────────────────────

def _authorized(uid: int) -> bool:
    """Return True if the user is allowed. Empty whitelist allows everyone."""
    return not ALLOWED_USER_IDS or uid in ALLOWED_USER_IDS


# ───────────────────────────────────────────────────────────────
# Persistence
# ───────────────────────────────────────────────────────────────

def _load_session() -> None:
    global selected_chat
    try:
        with open(SESSION_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and data.get("id"):
            selected_chat = data
            log.info(f"Session loaded: {selected_chat['title']}")
    except FileNotFoundError:
        selected_chat = {}
    except Exception as e:
        log.warning(f"Session load failed: {e}")
        selected_chat = {}


def _save_session() -> None:
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(selected_chat, f, indent=2)
    except Exception as e:
        log.error(f"Session save failed: {e}")


def _load_chat_names() -> dict:
    try:
        with open(CHAT_NAMES_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_chat_name(folder_id: str, title: str) -> None:
    names = _load_chat_names()
    if names.get(folder_id) == title:
        return
    names[folder_id] = title
    try:
        with open(CHAT_NAMES_FILE, "w", encoding="utf-8") as f:
            json.dump(names, f, indent=2)
    except Exception as e:
        log.warning(f"chat_names save failed: {e}")


def _folder_title(folder_id: str) -> str:
    for d in dialogs_cache:
        if str(d["id"]).replace("-100", "") == folder_id:
            return d["title"]
    return _load_chat_names().get(folder_id, f"Chat {folder_id}")


# MOD-18: downloaded_ids persistence helpers
def _load_downloaded_ids() -> None:
    global downloaded_ids
    try:
        with open(DOWNLOADED_IDS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        downloaded_ids = set(data) if isinstance(data, list) else set()
        log.info(f"Loaded {len(downloaded_ids)} downloaded IDs from disk")
    except FileNotFoundError:
        downloaded_ids = set()
    except Exception as e:
        log.warning(f"downloaded_ids load failed: {e}")


def _persist_downloaded_id(msg_id: int) -> None:
    """Add a single ID to the in-memory set and flush the whole set to disk."""
    downloaded_ids.add(msg_id)
    try:
        with open(DOWNLOADED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(downloaded_ids), f)
    except Exception as e:
        log.warning(f"downloaded_ids save failed: {e}")


def _clear_downloaded_ids() -> None:
    """Clear the in-memory set and wipe the on-disk file (MOD-22)."""
    global downloaded_ids
    downloaded_ids = set()
    try:
        with open(DOWNLOADED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)
    except Exception as e:
        log.warning(f"downloaded_ids clear failed: {e}")


# ───────────────────────────────────────────────────────────────
# Formatting helpers
# ───────────────────────────────────────────────────────────────

def _sz(b: int | float) -> str:
    if b == 0:                              # MOD-23: avoid "0.0 B"
        return "0 B"
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _speed(bps: float) -> str:
    return _sz(bps) + "/s"


def _elapsed(start: float) -> str:
    s = int(time() - start)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _fmt_eta(s: int) -> str:
    """Format a number of seconds as a human-readable ETA string."""
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _bar(pct: float, width: int = 10) -> str:
    n = round(width * pct / 100)
    return "█" * n + "░" * (width - n)


def _trunc(text: str, n: int) -> str:
    if n <= 0:                              # MOD-25: guard against n <= 0
        return ""
    if not text:
        return ""
    return text if len(text) <= n else text[:n - 1] + "…"


def _file_emoji(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    return {
        ".jpg": "🖼", ".jpeg": "🖼", ".png": "🖼", ".gif": "🖼",
        ".webp": "🖼", ".bmp": "🖼", ".heic": "🖼",
        ".mp4": "🎬", ".mkv": "🎬", ".avi": "🎬", ".mov": "🎬",
        ".webm": "🎬", ".flv": "🎬", ".ts": "🎬", ".m4v": "🎬", ".3gp": "🎬",
        ".mp3": "🎵", ".ogg": "🎵", ".flac": "🎵", ".wav": "🎵",
        ".m4a": "🎵", ".aac": "🎵", ".opus": "🎵", ".wma": "🎵",
        ".zip": "🗜", ".rar": "🗜", ".7z": "🗜", ".tar": "🗜",
        ".gz": "🗜", ".xz": "🗜", ".bz2": "🗜",
        ".pdf": "📕", ".epub": "📕", ".mobi": "📕",
        ".txt": "📝", ".md": "📝", ".doc": "📄", ".docx": "📄",
        ".xlsx": "📊", ".csv": "📊",
        ".apk": "📱", ".xapk": "📱",
    }.get(ext, "📄")


def _truncate_msg(text: str, limit: int = 4090) -> str:
    if len(text) <= limit:
        return text
    return text[:limit - 15] + "\n\n… truncated"


# ───────────────────────────────────────────────────────────────
# File type definitions
# ───────────────────────────────────────────────────────────────

FILE_TYPES = {
    "🎬 Videos":    [".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".m4v", ".3gp"],
    "🎵 Audio":     [".mp3", ".ogg", ".flac", ".wav", ".m4a", ".aac", ".opus", ".wma"],
    "🖼 Images":    [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic"],
    "📚 Documents": [".pdf", ".epub", ".mobi", ".txt", ".docx", ".xlsx", ".csv"],
    "🗜 Archives":  [".zip", ".rar", ".7z", ".tar", ".gz", ".xz", ".bz2"],
    "📱 Apps":      [".apk", ".xapk", ".apks"],
}


# ───────────────────────────────────────────────────────────────
# Progress rendering
# ───────────────────────────────────────────────────────────────

def _render_entry(e: dict) -> str:
    mid = f"`{e['id']}`"
    if e["status"] == "done":
        return f"✓ {mid} · {_trunc(e.get('name', '?'), 22)} · {_sz(e.get('size', 0))}"
    if e["status"] == "downloading":
        pct = e.get("pct", 0)
        return f"↓ {mid} · {_trunc(e.get('name', '…'), 22)}\n`[{_bar(pct)}] {pct:.0f}%`"
    if e["status"] == "skipped":
        return f"○ {mid} · {e.get('reason', '')}"
    if e["status"] == "failed":
        return f"✗ {mid} · {e.get('reason', '')}"
    return f"· {mid}"


def _get_speed(job: dict) -> float:
    """MOD-13: rolling 5-second window speed in bytes/sec."""
    now        = time()
    done_bytes = sum(e.get("size", 0) for e in job["entries"] if e["status"] == "done")
    last_time  = job.get("_tick_time",  job["start_time"])
    last_bytes = job.get("_tick_bytes", 0)
    dt         = now - last_time
    if dt >= 5.0:
        speed = max((done_bytes - last_bytes) / dt, 0.0)
        job["_tick_time"]     = now
        job["_tick_bytes"]    = done_bytes
        job["_rolling_speed"] = speed
        return speed
    return max(job.get("_rolling_speed", 0.0), 0.0)


def _render_job(job: dict) -> str:
    entries = job["entries"]

    # MOD-12: show only last 5 entries; summarise the rest
    visible = entries[-5:]
    hidden  = len(entries) - len(visible)
    prefix  = f"+{hidden} more completed…\n" if hidden else ""
    rows    = prefix + "\n".join(_render_entry(e) for e in visible)

    # MOD-13/14: speed and ETA
    speed       = _get_speed(job)
    done_bytes  = sum(e.get("size", 0) for e in entries if e["status"] == "done")
    total_bytes = job.get("total_bytes", 0)
    remaining   = max(total_bytes - done_bytes, 0)
    if speed > 0 and remaining > 0:
        eta_str = f"  ·  ETA {_fmt_eta(int(remaining / speed))}"
    else:
        eta_str = ""

    # header
    header = f"📥 **{_trunc(job['chat_title'], 28)}**"

    return _truncate_msg(
        f"{header}\n"
        f"{'━' * 22}\n"
        f"{rows}\n\n"
        f"⏱ {_elapsed(job['start_time'])} · {_speed(speed)}{eta_str}"
    )


def _render_job_summary(job: dict) -> str:
    done     = sum(1 for e in job["entries"] if e["status"] == "done")
    skipped  = sum(1 for e in job["entries"] if e["status"] == "skipped")
    failed   = sum(1 for e in job["entries"] if e["status"] == "failed")
    total_sz = sum(e.get("size", 0) for e in job["entries"] if e["status"] == "done")
    return (
        f"✓ **{_trunc(job['chat_title'], 28)}**\n"
        f"{'━' * 22}\n"
        f"Done {done} · Skipped {skipped} · Failed {failed}\n"
        f"{_sz(total_sz)} · ⏱ {_elapsed(job['start_time'])}"
    )


async def _edit(msg, text: str, reply_markup=None) -> None:
    if not msg:
        return
    try:
        await msg.edit(_truncate_msg(text), reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await _edit(msg, text, reply_markup)
    except Exception as e:
        log.debug(f"Edit failed: {e}")


# ───────────────────────────────────────────────────────────────
# MOD-01/02: Job and bulk controls markup builders
# ───────────────────────────────────────────────────────────────

def _job_controls_markup(job_id: str) -> InlineKeyboardMarkup:
    """Inline keyboard attached to every active job progress message."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel Job", callback_data=f"cb_job_cancel:{job_id}")],
    ])


def _bulk_controls_markup() -> InlineKeyboardMarkup:
    """Inline keyboard attached to the bulk job progress message."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel Download", callback_data="bulk_ctrl:cancel")],
    ])


# ───────────────────────────────────────────────────────────────
# Dialog cache
# ───────────────────────────────────────────────────────────────

async def _load_dialogs() -> None:
    global dialogs_cache
    log.info("Loading dialogs…")
    dialogs_cache.clear()
    try:
        async for d in user_client.get_dialogs():
            c = d.chat
            title = c.title or c.first_name or c.username or str(c.id)
            dialogs_cache.append({
                "id":    c.id,
                "title": title,
                "type":  str(c.type).split(".")[-1].lower(),
            })
        log.info(f"Dialogs loaded: {len(dialogs_cache)}")
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await _load_dialogs()
    except Exception as e:
        log.error(f"Dialog load failed: {e}")


# ───────────────────────────────────────────────────────────────
# File helpers
# ───────────────────────────────────────────────────────────────

def _save_path(chat_id, filename: str) -> str | None:
    folder = os.path.join(DOWNLOAD_BASE, str(chat_id).replace("-100", ""))
    try:
        os.makedirs(folder, exist_ok=True)
    except Exception as e:
        log.error(f"Cannot create folder: {e}")
        return None
    dest = os.path.join(folder, filename)
    if os.path.exists(dest):
        # MOD-20: scan the directory once into a set instead of hitting the FS per iteration
        try:
            existing = set(os.listdir(folder))
        except Exception:
            existing = set()
        base, ext = os.path.splitext(filename)
        i = 1
        while f"{base}_{i}{ext}" in existing:
            i += 1
        dest = os.path.join(folder, f"{base}_{i}{ext}")
    return dest


def _filename(msg) -> str:
    ts = int(msg.date.timestamp()) if msg.date else int(time())
    if msg.document:   return msg.document.file_name or f"doc_{msg.id}_{ts}"
    if msg.video:      return msg.video.file_name    or f"video_{msg.id}_{ts}.mp4"
    if msg.audio:      return msg.audio.file_name    or f"audio_{msg.id}_{ts}.mp3"
    if msg.voice:      return f"voice_{msg.id}_{ts}.ogg"
    if msg.photo:      return f"photo_{msg.id}_{ts}.jpg"
    if msg.animation:  return f"gif_{msg.id}_{ts}.mp4"
    if msg.video_note: return f"vidnote_{msg.id}_{ts}.mp4"
    if msg.sticker:    return f"sticker_{msg.id}_{ts}.webp"
    return f"file_{msg.id}_{ts}"


# ───────────────────────────────────────────────────────────────
# Core download
# ───────────────────────────────────────────────────────────────

async def _download_file(
    msg, chat_id, job: dict, entry: dict, index: int, total: int
) -> tuple[str | None, int]:
    fname = _filename(msg)
    path  = _save_path(chat_id, fname)
    if not path:
        entry["status"] = "failed"
        entry["reason"] = "Path error"
        return None, 0

    _last_update = [0.0]

    async def _progress(current, total_bytes):
        if job.get("cancelled"):
            raise asyncio.CancelledError()
        # MOD-14: track total_bytes on the job dict for ETA calculation
        prev = entry.get("_total_bytes", 0)
        if prev != total_bytes:
            job["total_bytes"] = max(job.get("total_bytes", 0) - prev + total_bytes, 0)
            entry["_total_bytes"] = total_bytes
        now = time()
        if now - _last_update[0] < PROGRESS_INTERVAL:
            return
        _last_update[0] = now
        pct = (current / total_bytes * 100) if total_bytes else 0
        entry["pct"]  = pct
        entry["name"] = fname if total == 1 else f"{fname} [{index+1}/{total}]"
        markup = _job_controls_markup(job["job_id"])
        await _edit(job["progress_msg"], _render_job(job), reply_markup=markup)

    try:
        await msg.download(file_name=path, progress=_progress)
    except asyncio.CancelledError:
        if os.path.exists(path):
            os.remove(path)
        raise
    except Exception as e:
        if os.path.exists(path):
            os.remove(path)
        raise e

    size = os.path.getsize(path) if os.path.exists(path) else 0
    log.info(f"Saved: {fname} ({_sz(size)})")
    return fname, size


async def _process_id(msg_id: int, job: dict, entry: dict) -> None:
    chat_id = selected_chat["id"]
    entry["status"] = "downloading"

    if msg_id in downloaded_ids:
        entry["status"] = "skipped"
        entry["reason"] = "Already downloaded"
        return

    msg = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            fetched = await user_client.get_messages(chat_id=chat_id, message_ids=[msg_id])
            msg = fetched[0] if fetched else None
            break
        except FloodWait as e:
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(e.value + 1)
            else:
                entry["status"] = "failed"
                entry["reason"] = "FloodWait"
                return
        except (PeerIdInvalid, BadRequest) as e:
            entry["status"] = "failed"
            entry["reason"] = str(e)[:30]
            return
        except Exception as e:
            entry["status"] = "failed"
            entry["reason"] = str(e)[:30]
            log.error(f"Fetch error {msg_id}: {e}")
            return

    if not msg or msg.empty:
        entry["status"] = "skipped"
        entry["reason"] = "Not found"
        return

    try:
        if msg.media_group_id:
            group = await user_client.get_media_group(chat_id, msg_id)
            media = [m for m in group if m.media]
            total_size, last_name = 0, ""
            for i, m in enumerate(media):
                if job.get("cancelled"):
                    raise asyncio.CancelledError()
                fname, sz = await _download_file(m, chat_id, job, entry, i, len(media))
                if fname:
                    total_size += sz
                    last_name   = fname
            entry["name"] = f"{len(media)} files" if len(media) > 1 else last_name
            entry["size"] = total_size

        elif msg.media:
            fname, sz = await _download_file(msg, chat_id, job, entry, 0, 1)
            if not fname:
                return
            entry["name"] = fname
            entry["size"] = sz

        else:
            entry["status"] = "skipped"
            entry["reason"] = "No media"
            return

        _persist_downloaded_id(msg_id)     # MOD-18: flush to disk after each success
        entry["status"] = "done"

    except asyncio.CancelledError:
        entry["status"] = "failed"
        entry["reason"] = "Cancelled"
        raise
    except Exception as e:
        entry["status"] = "failed"
        entry["reason"] = str(e)[:30]
        log.error(f"Download error {msg_id}: {e}")


async def _run_job(job: dict) -> None:
    global _current_job, _worker_task, _job_queue

    async def _execute(j: dict) -> None:
        global _current_job
        _current_job = j
        folder_id = str(selected_chat.get("id", "")).replace("-100", "")
        if folder_id and j.get("chat_title"):
            _save_chat_name(folder_id, j["chat_title"])
        try:
            for entry in j["entries"]:
                if j.get("cancelled"):
                    entry["status"] = "failed"
                    entry["reason"] = "Cancelled"
                else:
                    await _process_id(entry["id"], j, entry)
                # MOD-01: attach controls keyboard after each entry update
                markup = _job_controls_markup(j["job_id"])
                await _edit(j["progress_msg"], _render_job(j), reply_markup=markup)
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            for e in j["entries"]:
                if e["status"] not in ("done", "skipped", "failed"):
                    e["status"] = "failed"
                    e["reason"] = "Cancelled"
            raise
        finally:
            folder_id = str(selected_chat["id"]).replace("-100", "")
            folder    = os.path.join(DOWNLOAD_BASE, folder_id)
            q_left    = len(_job_queue)
            q_note    = f"\n{q_left} more job(s) queued" if q_left else ""
            summary   = _render_job_summary(j) + f"\n{folder}" + q_note
            markup    = InlineKeyboardMarkup([[
                InlineKeyboardButton("Open files", callback_data=f"openf:{folder_id}")
            ]])
            await _edit(j["progress_msg"], summary, reply_markup=markup)
            done    = sum(1 for e in j["entries"] if e["status"] == "done")
            skipped = sum(1 for e in j["entries"] if e["status"] == "skipped")
            failed  = sum(1 for e in j["entries"] if e["status"] == "failed")
            log.info(f"Job done: {done}✓ {skipped}— {failed}✗")

    try:
        await _execute(job)
        while _job_queue:
            nxt = _job_queue.pop(0)
            await _edit(nxt["progress_msg"], "starting…")
            await _execute(nxt)
    except asyncio.CancelledError:
        for pending in _job_queue:
            for e in pending["entries"]:
                e["status"] = "failed"
                e["reason"] = "Cancelled"
            await _edit(pending["progress_msg"], "Cancelled")
        _job_queue.clear()
    finally:
        _current_job = None
        _worker_task = None


# ───────────────────────────────────────────────────────────────
# Bulk download
# ───────────────────────────────────────────────────────────────

def _msg_matches_type(msg, file_type: str) -> bool:
    exts = FILE_TYPES.get(file_type, [])
    if file_type == "🎬 Videos" and (msg.video or msg.video_note):
        return True
    if file_type == "🎵 Audio" and (msg.audio or msg.voice):
        return True
    if file_type == "🖼 Images":
        if msg.photo:
            return True
        if msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"):
            return True
    if msg.document:
        return os.path.splitext(msg.document.file_name or "")[1].lower() in exts
    return False


async def _run_bulk_job(chat_id, chat_title: str, file_type: str, progress_msg) -> None:
    global _bulk_task, _bulk_state

    _bulk_state = {"cancelled": False}

    scanned     = 0
    found       = 0
    done        = 0
    failed      = 0
    done_bytes  = 0
    total_bytes = 0
    start_time  = time()
    last_edit   = [0.0]
    folder_id   = str(chat_id).replace("-100", "")
    _save_chat_name(folder_id, chat_title)

    def _meta_size(m) -> int:
        for attr in ("document", "video", "audio", "voice", "animation", "video_note"):
            obj = getattr(m, attr, None)
            if obj and getattr(obj, "file_size", None):
                return obj.file_size
        return 0

    async def _update(final: bool = False):
        now = time()
        if not final and now - last_edit[0] < 2.5:
            return
        last_edit[0] = now
        # MOD-15: hide percentage bar until we have enough data to be meaningful
        if scanned >= 5 and total_bytes > 0:
            pct     = min(done_bytes / total_bytes * 100, 100)
            bar_str = f"`[{_bar(pct)}] {pct:.0f}%  {_sz(done_bytes)}`\n"
        else:
            bar_str = f"scanning…  {scanned} msgs scanned\n"
        prefix = "✓" if final else "↓"
        status = "Complete" if final else "Running"
        markup = None if final else _bulk_controls_markup()
        try:
            await progress_msg.edit(
                f"{prefix} **{file_type}** · {status}\n"
                f"{'━' * 22}\n"
                f"Scanned {scanned} · Matched {found}\n"
                f"Done {done} · Failed {failed}\n\n"
                f"{bar_str}"
                f"⏱ {_elapsed(start_time)}",
                reply_markup=markup,
            )
        except Exception:
            pass

    try:
        async for msg in user_client.get_chat_history(chat_id):
            if asyncio.current_task().cancelled() or _bulk_state.get("cancelled"):
                raise asyncio.CancelledError()
            scanned += 1
            await _update()
            if not _msg_matches_type(msg, file_type):
                continue
            found += 1
            total_bytes += _meta_size(msg)
            if msg.id in downloaded_ids:
                continue
            try:
                fname = _filename(msg)
                path  = _save_path(chat_id, fname)
                if not path:
                    failed += 1
                    continue
                await msg.download(file_name=path)
                sz = os.path.getsize(path) if os.path.exists(path) else 0
                _persist_downloaded_id(msg.id)      # MOD-18
                done_bytes += sz
                done       += 1
                log.info(f"Bulk: {fname} ({_sz(sz)})")
            except asyncio.CancelledError:
                raise
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    await msg.download(file_name=path)
                    sz = os.path.getsize(path) if os.path.exists(path) else 0
                    _persist_downloaded_id(msg.id)
                    done_bytes += sz
                    done       += 1
                except Exception:
                    failed += 1
            except Exception as e:
                log.error(f"Bulk error {msg.id}: {e}")
                failed += 1

    except asyncio.CancelledError:
        pct = min(done_bytes / total_bytes * 100, 100) if total_bytes else 0
        try:
            await progress_msg.edit(
                f"⛔ **{file_type}** · Cancelled\n"
                f"Scanned {scanned} · Done {done} · Failed {failed}\n"
                f"`[{_bar(pct)}] {pct:.0f}%` · {_sz(done_bytes)}\n"
                f"⏱ {_elapsed(start_time)}"
            )
        except Exception:
            pass
        return
    except Exception as e:
        log.error(f"Bulk job error: {e}")
        try:
            await progress_msg.edit(f"✗ Bulk download failed\n`{e}`")
        except Exception:
            pass
        return
    finally:
        _bulk_task  = None
        _bulk_state = {"cancelled": False}

    await _update(final=True)
    pct = min(done_bytes / total_bytes * 100, 100) if total_bytes else 100
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("Open files", callback_data=f"openf:{folder_id}")
    ]])
    try:
        await progress_msg.edit(
            f"✓ **{file_type}** · Complete\n"
            f"{'━' * 22}\n"
            f"Scanned {scanned} · Matched {found}\n"
            f"Done {done} · Failed {failed}\n\n"
            f"`[{_bar(pct)}] {pct:.0f}%`\n"
            f"{_sz(done_bytes)} · ⏱ {_elapsed(start_time)}",
            reply_markup=markup,
        )
    except Exception:
        pass
    log.info(f"Bulk done: scanned={scanned} found={found} done={done} failed={failed}")


# ───────────────────────────────────────────────────────────────
# Filename search
# ───────────────────────────────────────────────────────────────

async def _find_messages_by_name(chat_id: int, query: str) -> list:
    matching = []
    q = query.lower()
    try:
        async for msg in user_client.get_chat_history(chat_id, limit=SEARCH_LIMIT):
            if msg.media:
                fname = _filename(msg)
                if q in fname.lower():
                    matching.append({"id": msg.id, "name": fname})
        matching.sort(key=lambda x: x["name"])
    except Exception as e:
        log.error(f"Name search error: {e}")
    return matching


# ───────────────────────────────────────────────────────────────
# Dialog list UI
# ───────────────────────────────────────────────────────────────

def _dialog_list_markup(uid: int, page: int = 0, query: str = "") -> tuple[str, InlineKeyboardMarkup]:
    pool = (
        [d for d in dialogs_cache if query.lower() in d["title"].lower()]
        if query else dialogs_cache
    )
    search_results[uid] = pool
    total       = len(pool)
    start       = page * PAGE_SIZE
    page_items  = pool[start : start + PAGE_SIZE]
    total_pages = max(1, -(-total // PAGE_SIZE))

    TYPE_ICON = {"group": "👥", "supergroup": "👥", "channel": "📢", "bot": "🤖", "private": "👤"}
    # MOD-10: encode the actual chat_id, not the list index
    buttons = [
        [InlineKeyboardButton(
            f"{TYPE_ICON.get(d['type'], '💬')} {_trunc(d['title'], 28)}",
            callback_data=f"sc:{d['id']}",
        )]
        for d in page_items
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("← Prev", callback_data=f"pg:{page-1}:{query}"))
    if start + PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("Next →", callback_data=f"pg:{page+1}:{query}"))
    if nav:
        buttons.append(nav)

    header = (
        f"**Select a Chat**\n"
        + (f"Filter: `{query}`\n" if query else "")
        + f"{total} chats · Page {page+1}/{total_pages}\n"
        + "Type a name to filter"
    )
    return header, InlineKeyboardMarkup(buttons)


async def _show_dialog_list(target, uid: int, page: int = 0, query: str = "") -> None:
    if not dialogs_cache:
        # MOD-11: informative loading message with retry button
        retry_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Retry", callback_data="welcome:setchat")
        ]])
        text = "Loading chats, please try again in a moment…"
        if hasattr(target, "reply"):
            await target.reply(text, reply_markup=retry_markup)
        else:
            try: await target.message.edit(text, reply_markup=retry_markup)
            except Exception: pass
        return
    text, markup = _dialog_list_markup(uid, page, query)
    if hasattr(target, "reply"):
        await target.reply(text, reply_markup=markup)
    else:
        try: await target.message.edit(text, reply_markup=markup)
        except Exception: pass


def _dl_action_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Manual IDs",        callback_data="dlopt:manual")],
        [InlineKeyboardButton("Search by Filename", callback_data="dlopt:search")],
        [InlineKeyboardButton("Bulk Download",      callback_data="dlopt:bulk")],
    ])


def _bulk_type_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Videos",    callback_data="bulk:🎬 Videos"),
         InlineKeyboardButton("🎵 Audio",     callback_data="bulk:🎵 Audio")],
        [InlineKeyboardButton("🖼 Images",    callback_data="bulk:🖼 Images"),
         InlineKeyboardButton("📚 Documents", callback_data="bulk:📚 Documents")],
        [InlineKeyboardButton("🗜 Archives",  callback_data="bulk:🗜 Archives"),
         InlineKeyboardButton("📱 Apps",      callback_data="bulk:📱 Apps")],
        [InlineKeyboardButton("← Back",       callback_data="dlopt:back")],
    ])


# ───────────────────────────────────────────────────────────────
# Welcome screen builder  (MOD-22: New Session button added)
# ───────────────────────────────────────────────────────────────

def _welcome_markup() -> InlineKeyboardMarkup:
    buttons = []
    if selected_chat.get("id"):
        buttons.append([InlineKeyboardButton(
            f"▶  Resume  {_trunc(selected_chat['title'], 24)}",
            callback_data="welcome:resume",
        )])
    buttons.append([InlineKeyboardButton("Select Chat",  callback_data="welcome:setchat")])
    buttons.append([InlineKeyboardButton("New Session",  callback_data="welcome:new_session")])
    buttons.append([InlineKeyboardButton("Help",         callback_data="welcome:help")])
    return InlineKeyboardMarkup(buttons)


def _welcome_text(status: str) -> str:
    return (
        f"**GhostFetch**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"**{len(dialogs_cache)}** chats · **{len(_job_queue)}** queued · {status}\n"
    )


# ───────────────────────────────────────────────────────────────
# Commands
# ───────────────────────────────────────────────────────────────

async def cmd_start(_, message: Message):
    global _current_job, _worker_task, _job_queue
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")

    # MOD-07: evaluate status BEFORE any cancellation so the value is accurate
    status = "downloading" if _current_job else "idle"

    # MOD-06: warn the user instead of cancelling immediately
    if _worker_task and not _worker_task.done():
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("Yes, reset", callback_data="confirm_reset:yes"),
            InlineKeyboardButton("No",          callback_data="confirm_reset:no"),
        ]])
        return await message.reply(
            "A download is currently running. Reset and discard it?",
            reply_markup=markup,
        )

    # MOD-22: downloaded_ids is NOT cleared here; use "New Session" button for that
    _job_queue.clear()
    user_state[uid] = "idle"

    await message.reply(
        _welcome_text(status),
        reply_markup=_welcome_markup(),
        disable_web_page_preview=True,
    )


async def cmd_setchat(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    user_state[uid] = "selecting"
    await _show_dialog_list(message, uid)


async def cmd_options(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    if not selected_chat.get("id"):
        return await message.reply("⚠️ No chat selected. Use /start or /setchat first.")
    await message.reply(
        f"**{_trunc(selected_chat['title'], 28)}**\nSelect a download mode:",
        reply_markup=_dl_action_markup(),
    )


async def cmd_help(_, message: Message):
    # Auth check is skipped here because cmd_help is called internally from cb_welcome.
    # All external entry points are already guarded.
    await message.reply(
        "**GhostFetch**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Commands**\n"
        "`/start` · Reset session\n"
        "`/setchat` · Pick a source chat\n"
        "`/options` · Download mode picker\n"
        "`/files` · Browse downloads\n"
        "`/queue` · View pending jobs\n"
        "`/killall` · Cancel everything\n"
        "`/stats` · System and session info\n"
        "`/log` · Export session log\n\n"
        "**Download Modes**\n\n"
        "**Manual IDs**\n"
        "Send message IDs separated by spaces\n"
        "`26473` or `26473 26570 26600`\n\n"
        "**Search by Filename**\n"
        "Type a keyword; the bot scans history\n"
        "and shows matches as buttons\n\n"
        "**Bulk Download**\n"
        "Pick a file type; the bot grabs every\n"
        "matching file in the entire chat history\n\n"
        "**Queue**\n"
        "Jobs submitted while a download is active\n"
        "are queued automatically.\n"
        "Use /killall to cancel everything.\n\n"
        f"**Save Path**\n`{DOWNLOAD_BASE}`",
        disable_web_page_preview=True,
    )


async def cmd_queue(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    if not _job_queue and not _current_job:
        return await message.reply("No jobs in queue.")

    # MOD-08: build inline keyboard for per-job cancel actions
    lines   = []
    buttons = []
    if _current_job:
        done   = sum(1 for e in _current_job["entries"] if e["status"] == "done")
        total  = len(_current_job["entries"])
        lines.append(f"▶ **{_trunc(_current_job['chat_title'], 24)}** · {done}/{total}")
    for i, j in enumerate(_job_queue, 1):
        lines.append(f"{i}. **{_trunc(j['chat_title'], 24)}** · {len(j['entries'])} IDs")
        buttons.append([InlineKeyboardButton(
            f"Cancel Job {i}", callback_data=f"qcancel:{j['job_id']}"
        )])
    if _job_queue:
        buttons.append([InlineKeyboardButton("Cancel All", callback_data="qcancelall")])
    markup = InlineKeyboardMarkup(buttons) if buttons else None
    await message.reply(
        "**Queue**\n━━━━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
        reply_markup=markup,
    )


async def cmd_killall(_, message: Message):
    global _worker_task, _current_job, _job_queue, _bulk_task, _bulk_state
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    nothing = (
        not (_worker_task and not _worker_task.done())
        and not _job_queue
        and not (_bulk_task and not _bulk_task.done())
    )
    if nothing:
        return await message.reply("No active downloads or queued jobs.")
    if _current_job:
        _current_job["cancelled"] = True
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
    if _bulk_task and not _bulk_task.done():
        _bulk_state["cancelled"] = True
        _bulk_task.cancel()
    _worker_task = None
    _current_job = None
    _bulk_task   = None
    _job_queue.clear()
    await message.reply("Stopped. All jobs cleared.")
    log.info("killall")


async def cmd_log(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    if os.path.exists(LOG_FILE):
        await message.reply_document(LOG_FILE, caption="session log")
    else:
        await message.reply("No log file found.")


async def cmd_stats(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    dl_root = DOWNLOAD_BASE if os.path.exists(DOWNLOAD_BASE) else "."
    try:
        total, used, free = shutil.disk_usage(dl_root)
        cpu  = await asyncio.to_thread(psutil.cpu_percent, interval=0.5)
        ram  = psutil.virtual_memory().percent
        net  = psutil.net_io_counters()
        proc = psutil.Process(os.getpid())
    except Exception as e:
        return await message.reply(f"⚠️ Stats error: `{e}`")

    chat_line = _trunc(selected_chat["title"], 22) if selected_chat.get("id") else "none"
    job_line  = (
        f"{len(_current_job['ids'])} IDs downloading"
        if _current_job and "ids" in _current_job else "idle"
    )
    await message.reply(
        f"**Stats**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"uptime  {_elapsed(BOT_START_TIME)}\n"
        f"chat    {chat_line}\n"
        f"status  {job_line}\n"
        f"files   {len(downloaded_ids)}\n\n"
        f"disk    {_sz(used)} / {_sz(total)} ({100*used//total}%)\n"
        f"cpu     {cpu:.1f}%\n"
        f"ram     {ram:.1f}%\n"
        f"net  ↑  {_sz(net.bytes_sent)}\n"
        f"     ↓  {_sz(net.bytes_recv)}\n"
        f"proc    {round(proc.memory_info().rss / 1024**2)} MB\n"
        f"```"
    )


# ───────────────────────────────────────────────────────────────
# Callbacks — welcome / chat select / download options
# ───────────────────────────────────────────────────────────────

async def cb_welcome(_, query: CallbackQuery):
    uid    = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    action = query.data.split(":")[1]

    if action == "resume":
        if not selected_chat.get("id"):
            return await query.answer("No previous session.", show_alert=True)
        user_state[uid] = "idle"
        await query.message.edit(
            f"**{_trunc(selected_chat['title'], 28)}**\nSelect a download mode:",
            reply_markup=_dl_action_markup(),
        )
        await query.answer()

    elif action == "setchat":
        user_state[uid] = "selecting"
        await query.answer()
        await _show_dialog_list(query, uid)

    elif action == "help":
        await query.answer()
        await cmd_help(None, query.message)

    elif action == "new_session":   # MOD-22
        _clear_downloaded_ids()
        await query.answer("Session cleared")
        status = "downloading" if _current_job else "idle"
        await query.message.edit(
            _welcome_text(status) + "IDs cleared ✓\n",
            reply_markup=_welcome_markup(),
        )


async def cb_select_chat(_, query: CallbackQuery):
    """MOD-10: decode the actual chat_id from callback data instead of a list index."""
    global selected_chat
    uid     = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    chat_id = int(query.data.split(":")[1])
    chosen  = next((d for d in dialogs_cache if d["id"] == chat_id), None)
    if not chosen:
        return await query.answer("Chat not found. Reload chats.", show_alert=True)
    selected_chat = {"id": chosen["id"], "title": chosen["title"]}
    _save_session()
    user_state[uid] = "idle"
    search_results.pop(uid, None)
    await query.message.edit(
        f"✓ **{_trunc(chosen['title'], 28)}**\n"
        f"ID `{chosen['id']}` · {chosen['type']}\n\n"
        "Select a download mode:",
        reply_markup=_dl_action_markup(),
    )
    await query.answer()
    log.info(f"Chat selected: {chosen['title']}")


async def cb_page(_, query: CallbackQuery):
    uid   = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    parts = query.data.split(":", 2)
    page  = int(parts[1])
    q     = parts[2] if len(parts) > 2 else ""
    await _show_dialog_list(query, uid, page=page, query=q)
    await query.answer()


async def cb_dlopt(_, query: CallbackQuery):
    uid  = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    mode = query.data.split(":")[1]

    if mode == "back":
        chat = _trunc(selected_chat.get("title", ""), 28)
        await query.message.edit(
            f"**{chat}**\nSelect a download mode:",
            reply_markup=_dl_action_markup(),
        )
        await query.answer()
        return

    if mode == "manual":
        # MOD-21: use awaiting_ids state so idle numerics don't accidentally trigger downloads
        user_state[uid] = "awaiting_ids"
        # MOD-04: Back button on the Manual IDs screen
        back_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("← Back", callback_data="dlopt:back")
        ]])
        await query.message.edit(
            "**Manual IDs**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Send one or more message IDs, separated by spaces.\n\n"
            "`26473`\n"
            "`26473 26570 26600`\n\n"
            "Find IDs via the message link or the desktop info panel.",
            reply_markup=back_markup,
        )
        await query.answer()
        return

    if mode == "search":
        user_state[uid] = "download_search"
        # MOD-05: Back button on the Search filename screen
        back_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("← Back", callback_data="dlopt:back")
        ]])
        await query.message.edit(
            "**Search by Filename**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Type part of the filename you are looking for.\n"
            "The bot will scan chat history and return matches.",
            reply_markup=back_markup,
        )
        await query.answer()
        return

    if mode == "bulk":
        await query.message.edit(
            "**Bulk Download**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Select a file type. The bot will download every\n"
            "matching file in the entire chat history.",
            reply_markup=_bulk_type_markup(),
        )
        await query.answer()
        return


async def cb_bulk_select(_, query: CallbackQuery):
    global _bulk_task
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    file_type = query.data.split(":", 1)[1]

    if _bulk_task and not _bulk_task.done():
        return await query.answer(
            "Bulk download already running. Use /killall to stop it.", show_alert=True
        )
    if not selected_chat.get("id"):
        return await query.answer("No chat selected.", show_alert=True)

    await query.answer(f"Starting {file_type}…")
    progress_msg = await query.message.reply(
        f"↓ **{file_type}** · Starting\nScanning chat history…"
    )
    _bulk_task = asyncio.create_task(
        _run_bulk_job(selected_chat["id"], selected_chat["title"], file_type, progress_msg)
    )


# ───────────────────────────────────────────────────────────────
# Per-job inline cancel callback
# ───────────────────────────────────────────────────────────────

async def cb_job_cancel(_, query: CallbackQuery):
    global _worker_task, _current_job
    if not _authorized(query.from_user.id):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    job_id = query.data.split(":")[1]
    if _current_job and _current_job.get("job_id") == job_id:
        _current_job["cancelled"] = True
        if _worker_task and not _worker_task.done():
            _worker_task.cancel()
        return await query.answer("Cancelled")
    to_remove = next((j for j in _job_queue if j.get("job_id") == job_id), None)
    if to_remove:
        _job_queue.remove(to_remove)
        for e in to_remove["entries"]:
            e["status"] = "failed"
            e["reason"] = "Cancelled"
        await _edit(to_remove["progress_msg"], "Cancelled")
        await query.answer("Cancelled")
    else:
        await query.answer("Job not found.", show_alert=True)


# ───────────────────────────────────────────────────────────────
# MOD-02: Bulk controls callback
# ───────────────────────────────────────────────────────────────

async def cb_bulk_ctrl(_, query: CallbackQuery):
    global _bulk_task, _bulk_state
    if not _authorized(query.from_user.id):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    action = query.data.split(":")[1]
    if action == "cancel":
        _bulk_state["cancelled"] = True
        if _bulk_task and not _bulk_task.done():
            _bulk_task.cancel()
        await query.answer("Cancelled")


# ───────────────────────────────────────────────────────────────
# MOD-06: Reset confirmation callbacks
# ───────────────────────────────────────────────────────────────

async def cb_confirm_reset(_, query: CallbackQuery):
    global _current_job, _worker_task, _job_queue
    uid    = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    action = query.data.split(":")[1]
    if action == "no":
        try: await query.message.delete()
        except Exception: pass
        await query.answer()
        return
    # action == "yes": proceed with reset
    if _current_job:
        _current_job["cancelled"] = True
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
    _worker_task = None
    _current_job = None
    _job_queue.clear()
    user_state[uid] = "idle"
    await query.answer()
    status = "idle"
    await query.message.edit(
        _welcome_text(status),
        reply_markup=_welcome_markup(),
    )


# ───────────────────────────────────────────────────────────────
# MOD-08: Queue cancel callbacks
# ───────────────────────────────────────────────────────────────

async def cb_queue_cancel(_, query: CallbackQuery):
    if not _authorized(query.from_user.id):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    job_id    = query.data.split(":")[1]
    to_remove = next((j for j in _job_queue if j.get("job_id") == job_id), None)
    if not to_remove:
        return await query.answer("Job not found or already started.", show_alert=True)
    _job_queue.remove(to_remove)
    for e in to_remove["entries"]:
        e["status"] = "failed"
        e["reason"] = "Cancelled"
    await _edit(to_remove["progress_msg"], "Cancelled")
    await query.answer("Job removed from queue")
    # Refresh the queue message inline
    if not _job_queue and not _current_job:
        await query.message.edit("No jobs in queue.")
    else:
        lines   = []
        buttons = []
        if _current_job:
            done  = sum(1 for e in _current_job["entries"] if e["status"] == "done")
            total = len(_current_job["entries"])
            lines.append(f"▶ **{_trunc(_current_job['chat_title'], 24)}** · {done}/{total}")
        for i, j in enumerate(_job_queue, 1):
            lines.append(f"{i}. **{_trunc(j['chat_title'], 24)}** · {len(j['entries'])} IDs")
            buttons.append([InlineKeyboardButton(
                f"Cancel Job {i}", callback_data=f"qcancel:{j['job_id']}"
            )])
        if _job_queue:
            buttons.append([InlineKeyboardButton("Cancel All", callback_data="qcancelall")])
        try:
            await query.message.edit(
                "**Queue**\n━━━━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
            )
        except Exception:
            pass


async def cb_queue_cancel_all(_, query: CallbackQuery):
    global _worker_task, _current_job, _job_queue
    if not _authorized(query.from_user.id):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    if _current_job:
        _current_job["cancelled"] = True
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
    for j in _job_queue:
        for e in j["entries"]:
            e["status"] = "failed"
            e["reason"] = "Cancelled"
        await _edit(j["progress_msg"], "Cancelled")
    _job_queue.clear()
    await query.answer("All jobs cancelled")
    await query.message.edit("Queue cleared.")


async def cb_open_files_from_job(_, query: CallbackQuery):
    uid       = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    folder_id = query.data.split(":")[1]
    folders   = _scan_folders()
    fi = next((i for i, f in enumerate(folders) if f["folder_id"] == folder_id), None)
    if fi is None:
        return await query.answer("Folder not found.", show_alert=True)
    _files_nav[uid] = {"folders": folders}
    await query.answer()
    await _send_file_view(query.message, uid, fi, page=0, reply=True)


async def cb_back(_, query: CallbackQuery):
    if not _authorized(query.from_user.id):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    action = query.data.split(":")[1]
    if action == "options":
        chat = _trunc(selected_chat.get("title", ""), 28)
        await query.message.edit(
            f"**{chat}**\nSelect a download mode:",
            reply_markup=_dl_action_markup(),
        )
    await query.answer()


# ───────────────────────────────────────────────────────────────
# File browser
# ───────────────────────────────────────────────────────────────

def _scan_folders() -> list[dict]:
    result = []
    if not os.path.exists(DOWNLOAD_BASE):
        return result
    for folder_id in sorted(os.listdir(DOWNLOAD_BASE)):
        full = os.path.join(DOWNLOAD_BASE, folder_id)
        if not os.path.isdir(full):
            continue
        try:
            files = [f for f in os.listdir(full) if os.path.isfile(os.path.join(full, f))]
            total = sum(os.path.getsize(os.path.join(full, f)) for f in files)
            result.append({
                "folder_id":  folder_id,
                "title":      _folder_title(folder_id),
                "file_count": len(files),
                "total_size": total,
                "path":       full,
            })
        except Exception as e:
            log.warning(f"Folder scan error: {e}")
    return result


def _scan_files_in(folder_path: str) -> list[dict]:
    files = []
    if not os.path.isdir(folder_path):
        return files
    for fname in os.listdir(folder_path):
        fpath = os.path.join(folder_path, fname)
        if os.path.isfile(fpath):
            files.append({"path": fpath, "name": fname, "size": os.path.getsize(fpath)})
    files.sort(key=lambda x: os.path.getmtime(x["path"]), reverse=True)
    return files


def _folders_markup(folders: list) -> tuple[str, InlineKeyboardMarkup]:
    total_files = sum(f["file_count"] for f in folders)
    total_size  = sum(f["total_size"] for f in folders)
    buttons = [
        [InlineKeyboardButton(
            f"📁 {_trunc(f['title'], 22)} · {f['file_count']}f · {_sz(f['total_size'])}",
            callback_data=f"fd:{i}",
        )]
        for i, f in enumerate(folders)
    ]
    buttons.append([InlineKeyboardButton("Wipe All Downloads", callback_data="fwipe")])
    header = f"**Downloads**\n{len(folders)} folder(s)  ·  {total_files} file(s)  ·  {_sz(total_size)}"
    return header, InlineKeyboardMarkup(buttons)


def _folder_files_markup(folder: dict, fi: int, files: list, page: int) -> tuple[str, InlineKeyboardMarkup]:
    total       = len(files)
    total_pages = max(1, -(-total // FILES_PAGE_SIZE))
    start       = page * FILES_PAGE_SIZE
    page_items  = files[start : start + FILES_PAGE_SIZE]

    buttons = []
    for i, f in enumerate(page_items):
        actual = start + i
        short  = (f["name"][:48] + "…") if len(f["name"]) > 48 else f["name"]
        emoji  = _file_emoji(f["name"])
        buttons.append([
            InlineKeyboardButton(f"{emoji} {short}", callback_data=f"fl:{fi}:{actual}"),
            InlineKeyboardButton(f"✕  {_sz(f['size'])}", callback_data=f"fdel:{fi}:{actual}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("←", callback_data=f"fp:{fi}:{page-1}"))
    nav.append(InlineKeyboardButton("Back", callback_data="fb"))
    if start + FILES_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("→", callback_data=f"fp:{fi}:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("Delete Folder", callback_data=f"fdeldir:{fi}")])

    header = f"📁  **{_trunc(folder['title'], 22)}**\n{total} file(s)  ·  Page {page+1}/{total_pages}"
    return header, InlineKeyboardMarkup(buttons)


async def _send_folder_view(target, uid: int) -> None:
    folders = _scan_folders()
    _files_nav[uid] = {"folders": folders}
    if not folders:
        text = "No downloads yet."
        if hasattr(target, "reply"):
            await target.reply(text)
        else:
            try: await target.message.edit(text)
            except Exception: pass
        return
    text, markup = _folders_markup(folders)
    if hasattr(target, "reply"):
        await target.reply(text, reply_markup=markup)
    else:
        try: await target.message.edit(text, reply_markup=markup)
        except Exception: pass


async def _send_file_view(target, uid: int, fi: int, page: int = 0, reply: bool = False) -> None:
    nav     = _files_nav.get(uid, {})
    folders = nav.get("folders") or _scan_folders()
    if fi >= len(folders):
        if hasattr(target, "answer"):
            await target.answer("Folder not found.", show_alert=True)
        return
    folder = folders[fi]
    files  = _scan_files_in(folder["path"])
    folder["file_count"] = len(files)
    folder["total_size"] = sum(f["size"] for f in files)
    _files_nav[uid] = {"folders": folders, "fi": fi, "files": files, "page": page}

    if not files:
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="fb")]])
        text   = f"📁  **{_trunc(folder['title'], 22)}**  is empty"
        if reply or hasattr(target, "reply"):
            await target.reply(text, reply_markup=markup)
        else:
            try: await target.message.edit(text, reply_markup=markup)
            except Exception: pass
        return

    text, markup = _folder_files_markup(folder, fi, files, page)
    if reply or (hasattr(target, "reply") and not hasattr(target, "data")):
        await target.reply(text, reply_markup=markup)
    else:
        try: await target.message.edit(text, reply_markup=markup)
        except Exception: pass


async def cmd_files(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    await _send_folder_view(message, uid)


async def cb_open_folder(_, query: CallbackQuery):
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    fi  = int(query.data.split(":")[1])
    await _send_file_view(query, uid, fi)
    await query.answer()


async def cb_files_page(_, query: CallbackQuery):
    uid   = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    parts = query.data.split(":")
    fi    = int(parts[1])
    page  = int(parts[2])
    await _send_file_view(query, uid, fi, page)
    await query.answer()


async def cb_back_to_folders(_, query: CallbackQuery):
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    await _send_folder_view(query, uid)
    await query.answer()


async def cb_send_file(_, query: CallbackQuery):
    uid   = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    parts = query.data.split(":")
    fi    = int(parts[1])
    idx   = int(parts[2])
    nav   = _files_nav.get(uid, {})
    files = nav.get("files")
    if not files or idx >= len(files):
        return await query.answer("File list expired.", show_alert=True)
    entry = files[idx]
    if not os.path.exists(entry["path"]):
        return await query.answer("File no longer exists.", show_alert=True)
    # MOD-09: answer with a toast; do NOT re-render the file view as a new message
    await query.answer("✅ File sent")
    await query.message.reply_document(
        entry["path"],
        caption=f"`{_trunc(entry['name'], 48)}` · {_sz(entry['size'])}",
    )


async def cb_delete_file(_, query: CallbackQuery):
    """MOD-19: first tap shows confirmation; actual deletion is in cb_delete_file_confirm."""
    uid   = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    parts = query.data.split(":")
    fi    = int(parts[1])
    idx   = int(parts[2])
    nav   = _files_nav.get(uid, {})
    files = nav.get("files")
    if not files or idx >= len(files):
        return await query.answer("List expired.", show_alert=True)
    entry  = files[idx]
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("Yes, Delete", callback_data=f"fdelok:{fi}:{idx}"),
        InlineKeyboardButton("Cancel",      callback_data=f"fp:{fi}:{nav.get('page', 0)}"),
    ]])
    await query.message.edit(
        f"Delete `{_trunc(entry['name'], 40)}`?\nThis cannot be undone.",
        reply_markup=markup,
    )
    await query.answer()


async def cb_delete_file_confirm(_, query: CallbackQuery):
    """MOD-19: executes the actual file deletion after user confirmation."""
    uid   = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    parts = query.data.split(":")
    fi    = int(parts[1])
    idx   = int(parts[2])
    nav   = _files_nav.get(uid, {})
    files = nav.get("files")
    if not files or idx >= len(files):
        return await query.answer("List expired.", show_alert=True)
    entry = files[idx]
    if os.path.exists(entry["path"]):
        os.remove(entry["path"])
        log.info(f"Deleted: {entry['path']}")
    await query.answer("Deleted")
    await _send_file_view(query, uid, fi, nav.get("page", 0))


async def cb_delete_folder(_, query: CallbackQuery):
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    fi  = int(query.data.split(":")[1])
    nav = _files_nav.get(uid, {})
    folders = nav.get("folders") or _scan_folders()
    if fi >= len(folders):
        return await query.answer("Not found.", show_alert=True)
    folder = folders[fi]
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("Yes, Delete", callback_data=f"fdeldirok:{fi}"),
        InlineKeyboardButton("Cancel",      callback_data=f"fp:{fi}:0"),
    ]])
    await query.message.edit(
        f"**Delete Folder?**\n\n"
        f"{_trunc(folder['title'], 28)}\n"
        f"{folder['file_count']} file(s)  ·  {_sz(folder['total_size'])}\n\n"
        f"This cannot be undone.",
        reply_markup=markup,
    )
    await query.answer()


async def cb_delete_folder_confirm(_, query: CallbackQuery):
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    fi  = int(query.data.split(":")[1])
    nav = _files_nav.get(uid, {})
    folders = nav.get("folders") or _scan_folders()
    if fi >= len(folders):
        return await query.answer("Not found.", show_alert=True)
    folder = folders[fi]
    if os.path.exists(folder["path"]):
        shutil.rmtree(folder["path"])
        log.info(f"Deleted folder: {folder['path']}")
    await query.answer("Deleted")
    await _send_folder_view(query, uid)


async def cb_wipe_all(_, query: CallbackQuery):
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("Yes, Wipe Everything", callback_data="fwipeok"),
        InlineKeyboardButton("Cancel",               callback_data="fb"),
    ]])
    await query.message.edit(
        "**Wipe All Downloads?**\n\nEvery file and folder will be permanently deleted.\nThis cannot be undone.",
        reply_markup=markup,
    )
    await query.answer()


async def cb_wipe_all_confirm(_, query: CallbackQuery):
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    if os.path.exists(DOWNLOAD_BASE):
        shutil.rmtree(DOWNLOAD_BASE)
        os.makedirs(DOWNLOAD_BASE, exist_ok=True)
        log.info("Wiped all downloads")
    _files_nav.pop(uid, None)
    await query.answer("Wiped")
    await query.message.edit("All downloads cleared.")


# ───────────────────────────────────────────────────────────────
# Main text handler
# ───────────────────────────────────────────────────────────────

async def handle_text(_, message: Message):
    global _current_job, _worker_task, _job_queue

    uid   = message.from_user.id
    if not _authorized(uid):
        return
    text  = message.text.strip()
    state = user_state.get(uid, "idle")

    if state == "selecting":
        await _show_dialog_list(message, uid, page=0, query=text)
        return

    if state == "download_search":
        user_state[uid] = "idle"
        if not selected_chat.get("id"):
            return await message.reply("⚠️ No chat selected.")
        progress = await message.reply(f"Searching for `{text}`…")
        matching = await _find_messages_by_name(selected_chat["id"], text)
        if not matching:
            await progress.edit(f"No files found matching `{text}`.")
            return
        _fname_results[uid] = matching
        buttons = []
        for i, item in enumerate(matching[:20]):
            short = _trunc(item["name"], 40)
            emoji = _file_emoji(item["name"])
            buttons.append([InlineKeyboardButton(f"{emoji} {short}", callback_data=f"searchdl:{i}")])
        if len(matching) > 1:
            buttons.append([InlineKeyboardButton(
                f"Download All  ({len(matching)} files)", callback_data="searchdl:all"
            )])
        await progress.edit(
            f"**{len(matching)} file(s)** matching `{text}`\nTap a file to download:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    # MOD-21: only accept numeric IDs in the explicit awaiting_ids state
    if state == "awaiting_ids":
        if not selected_chat.get("id"):
            return await message.reply("⚠️ No chat selected. Use /start or /setchat first.")

        tokens  = text.split()
        msg_ids = []
        bad     = []
        for t in tokens:
            try:
                mid = int(t)
                if mid > 0:
                    msg_ids.append(mid)
                else:
                    bad.append(t)
            except ValueError:
                bad.append(t)

        if bad:
            return await message.reply(
                f"Invalid token(s): `{' '.join(bad)}`\n\n"
                "Please send message IDs only (space-separated numbers)."
            )
        if not msg_ids:
            return await message.reply("Send at least one message ID.")

        entries = [{"id": mid, "status": "queued"} for mid in msg_ids]
        job_id  = uuid4().hex[:8]       # MOD-03: unique ID per job

        if _current_job or (_worker_task and not _worker_task.done()):
            pos = len(_job_queue) + 1
            progress_msg = await message.reply(
                f"Queued  #{pos}  ·  {len(msg_ids)} ID(s)\n"
                f"{_trunc(selected_chat['title'], 28)}"
            )
            job = {
                "job_id":       job_id,
                "ids":          msg_ids,
                "entries":      entries,
                "chat_title":   selected_chat["title"],
                "progress_msg": progress_msg,
                "start_time":   time(),
                "cancelled":    False,
                "total_bytes":  0,
            }
            _job_queue.append(job)
            return

        progress_msg = await message.reply(
            f"↓  {len(msg_ids)} ID(s)\n{_trunc(selected_chat['title'], 28)}"
        )
        job = {
            "job_id":       job_id,
            "ids":          msg_ids,
            "entries":      entries,
            "chat_title":   selected_chat["title"],
            "progress_msg": progress_msg,
            "start_time":   time(),
            "cancelled":    False,
            "total_bytes":  0,
        }
        _current_job = job
        _worker_task = asyncio.create_task(_run_job(job))
        return

    # MOD-21: in idle state, numeric input is ignored with a helpful nudge
    if state == "idle" and text.replace(" ", "").isdigit():
        return await message.reply(
            "To download by message ID, open /options and select Manual IDs first."
        )


async def cb_search_download(_, query: CallbackQuery):
    global _current_job, _worker_task, _job_queue
    uid    = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    choice  = query.data.split(":")[1]
    results = _fname_results.get(uid)
    if not results:
        return await query.answer("Search expired. Run again.", show_alert=True)

    if choice == "all":
        msg_ids = [r["id"] for r in results]
    else:
        idx = int(choice)
        if idx >= len(results):
            return await query.answer("Expired.", show_alert=True)
        msg_ids = [results[idx]["id"]]

    if not selected_chat.get("id"):
        return await query.answer("No chat selected.", show_alert=True)

    entries      = [{"id": mid, "status": "queued"} for mid in msg_ids]
    job_id       = uuid4().hex[:8]
    progress_msg = await query.message.reply(
        f"↓  {len(msg_ids)} file(s)\n{_trunc(selected_chat['title'], 28)}"
    )
    await query.answer("Starting…")
    job = {
        "job_id":       job_id,
        "ids":          msg_ids,
        "entries":      entries,
        "chat_title":   selected_chat["title"],
        "progress_msg": progress_msg,
        "start_time":   time(),
        "cancelled":    False,
        "total_bytes":  0,
    }
    if _current_job or (_worker_task and not _worker_task.done()):
        _job_queue.append(job)
    else:
        _current_job = job
        _worker_task = asyncio.create_task(_run_job(job))


# ───────────────────────────────────────────────────────────────
# Startup
# ───────────────────────────────────────────────────────────────

async def _set_bot_commands() -> None:
    try:
        await bot.set_bot_commands([
            BotCommand("start",   "Start / reset session"),
            BotCommand("options", "Download mode picker"),
            BotCommand("files",   "Browse downloads"),
            BotCommand("queue",   "Show pending jobs"),
            BotCommand("killall", "Stop all downloads"),
            BotCommand("stats",   "System & session info"),
            BotCommand("log",     "Send session log"),
            BotCommand("help",    "Help"),
        ])
        log.info("Bot commands registered")
    except Exception as e:
        log.warning(f"Set commands failed: {e}")


async def main() -> None:
    global bot, user_client

    # MOD-28: workers reduced to 4 for single-user Termux deployment.
    # Increase if running on a server with multiple users.
    bot = Client(
        "bot_session",
        api_id=PyroConf.API_ID,
        api_hash=PyroConf.API_HASH,
        bot_token=PyroConf.BOT_TOKEN,
        workers=4,
        parse_mode=ParseMode.MARKDOWN,
        sleep_threshold=30,
    )
    user_client = Client(
        "user_account",
        api_id=PyroConf.API_ID,
        api_hash=PyroConf.API_HASH,
        session_string=PyroConf.SESSION_STRING,
        in_memory=True,
        workers=4,
        sleep_threshold=30,
    )

    bot.add_handler(MessageHandler(cmd_start,   filters.command("start")   & filters.private))
    bot.add_handler(MessageHandler(cmd_setchat, filters.command("setchat") & filters.private))
    bot.add_handler(MessageHandler(cmd_options, filters.command("options") & filters.private))
    bot.add_handler(MessageHandler(cmd_help,    filters.command("help")    & filters.private))
    bot.add_handler(MessageHandler(cmd_files,   filters.command("files")   & filters.private))
    bot.add_handler(MessageHandler(cmd_queue,   filters.command("queue")   & filters.private))
    bot.add_handler(MessageHandler(cmd_killall, filters.command("killall") & filters.private))
    bot.add_handler(MessageHandler(cmd_log,     filters.command("log")     & filters.private))
    bot.add_handler(MessageHandler(cmd_stats,   filters.command("stats")   & filters.private))
    bot.add_handler(MessageHandler(
        handle_text,
        filters.private & filters.text & ~filters.command([
            "start", "setchat", "options", "help", "files",
            "queue", "killall", "log", "stats",
        ]),
    ))

    bot.add_handler(CallbackQueryHandler(cb_welcome,               filters.regex(r"^welcome:")))
    bot.add_handler(CallbackQueryHandler(cb_select_chat,           filters.regex(r"^sc:-?\d+$")))   # MOD-10: allow negative IDs
    bot.add_handler(CallbackQueryHandler(cb_page,                  filters.regex(r"^pg:\d+")))
    bot.add_handler(CallbackQueryHandler(cb_dlopt,                 filters.regex(r"^dlopt:")))
    bot.add_handler(CallbackQueryHandler(cb_bulk_select,           filters.regex(r"^bulk:")))
    bot.add_handler(CallbackQueryHandler(cb_search_download,       filters.regex(r"^searchdl:")))
    bot.add_handler(CallbackQueryHandler(cb_open_files_from_job,   filters.regex(r"^openf:")))
    bot.add_handler(CallbackQueryHandler(cb_back,                  filters.regex(r"^back:")))
    bot.add_handler(CallbackQueryHandler(cb_open_folder,           filters.regex(r"^fd:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_files_page,            filters.regex(r"^fp:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_back_to_folders,       filters.regex(r"^fb$")))
    bot.add_handler(CallbackQueryHandler(cb_send_file,             filters.regex(r"^fl:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_file,           filters.regex(r"^fdel:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_file_confirm,   filters.regex(r"^fdelok:\d+:\d+$")))   # MOD-19
    bot.add_handler(CallbackQueryHandler(cb_delete_folder,         filters.regex(r"^fdeldir:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_folder_confirm, filters.regex(r"^fdeldirok:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_wipe_all,              filters.regex(r"^fwipe$")))
    bot.add_handler(CallbackQueryHandler(cb_wipe_all_confirm,      filters.regex(r"^fwipeok$")))
    # MOD-01/03: per-job inline controls
    bot.add_handler(CallbackQueryHandler(cb_job_cancel,            filters.regex(r"^cb_job_cancel:")))
    # MOD-02: bulk inline controls
    bot.add_handler(CallbackQueryHandler(cb_bulk_ctrl,             filters.regex(r"^bulk_ctrl:")))
    # MOD-06: reset confirmation
    bot.add_handler(CallbackQueryHandler(cb_confirm_reset,         filters.regex(r"^confirm_reset:")))
    # MOD-08: queue cancel buttons
    bot.add_handler(CallbackQueryHandler(cb_queue_cancel,          filters.regex(r"^qcancel:")))
    bot.add_handler(CallbackQueryHandler(cb_queue_cancel_all,      filters.regex(r"^qcancelall$")))

    _load_session()
    _load_downloaded_ids()                  # MOD-18: restore IDs from disk on start
    os.makedirs(DOWNLOAD_BASE, exist_ok=True)

    log.info("Starting clients…")
    await bot.start()
    await user_client.start()
    log.info("Clients started")
    await _load_dialogs()
    await _set_bot_commands()
    log.info("GhostFetch online")

    try:
        await asyncio.Event().wait()
    finally:
        await bot.stop()
        await user_client.stop()
        log.info("Bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
