# GhostFetch — Telegram restricted media downloader
# herraChron

import os
import json
import shutil
import psutil
import asyncio
import logging
import httpx
from time import time
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid, BadRequest, MessageNotModified
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from config import PyroConf


# ═══════════════════════════════════════════════════════════════
# Constants & paths
# ═══════════════════════════════════════════════════════════════

DOWNLOAD_BASE    = "/data/data/com.termux/files/home/GhostFetch/downloads"
SESSION_FILE     = "session.json"
CHAT_NAMES_FILE  = "chat_names.json"
LOG_FILE         = "session.log"
BOT_START_TIME   = time()

# UI Constants
BORDER    = "━━━━━━━━━━━━━━━━━━━━━━━"
SEPARATOR = "──────────────────────"
ICON_DL      = "📥"
ICON_FOLDER  = "📂"
ICON_HELP    = "📖"
ICON_STATS   = "📊"
ICON_SUCCESS = "✅"
ICON_ERROR   = "❌"
ICON_WARN    = "⚠️"
ICON_INFO    = "ℹ️"
ICON_CLOCK   = "⏱"
ICON_DELETE  = "🗑"
ARROW = "▶"
DOT   = "•"


# ═══════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Pyrogram clients
# ═══════════════════════════════════════════════════════════════

bot         = None
user_client = None


# ═══════════════════════════════════════════════════════════════
# Global state
# ═══════════════════════════════════════════════════════════════

dialogs_cache:  list        = []
selected_chat:  dict        = {}
downloaded_ids: set         = set()
user_state:     dict        = {}
search_results: dict        = {}

_job_queue:   list               = []
_current_job: dict | None        = None
_worker_task: asyncio.Task | None = None

_files_nav:   dict               = {}
_bulk_task:   asyncio.Task | None = None   # separate tracker for bulk scan/download


# ═══════════════════════════════════════════════════════════════
# Persistence
# ═══════════════════════════════════════════════════════════════

def _load_session() -> None:
    global selected_chat
    try:
        with open(SESSION_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if data.get("id"):
            selected_chat = data
            log.info(f"Loaded session — chat: {selected_chat['title']} ({selected_chat['id']})")
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
            return json.load(f)
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
    """Return human-readable name for a download folder."""
    for d in dialogs_cache:
        if str(d["id"]).replace("-100", "") == folder_id:
            return d["title"]
    return _load_chat_names().get(folder_id, folder_id)


# ═══════════════════════════════════════════════════════════════
# Formatting helpers
# ═══════════════════════════════════════════════════════════════

def _sz(b: int | float) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _elapsed(start: float) -> str:
    """Human-readable elapsed time from a start timestamp."""
    s = int(time() - start)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _bar(pct: float, width: int = 12) -> str:
    """Text progress bar."""
    n = round(width * pct / 100)
    return "█" * n + "░" * (width - n)


def _file_emoji(filename: str) -> str:
    """Return an emoji that matches the file's extension."""
    ext = os.path.splitext(filename)[1].lower()
    return {
        ".jpg": "🖼", ".jpeg": "🖼", ".png": "🖼", ".gif": "🖼",
        ".webp": "🖼", ".bmp": "🖼", ".tiff": "🖼", ".heic": "🖼",
        ".mp4": "🎬", ".mkv": "🎬", ".avi": "🎬", ".mov": "🎬",
        ".webm": "🎬", ".flv": "🎬", ".ts": "🎬", ".m4v": "🎬",
        ".mp3": "🎵", ".ogg": "🎵", ".flac": "🎵", ".wav": "🎵",
        ".m4a": "🎵", ".aac": "🎵", ".opus": "🎵",
        ".zip": "🗜", ".rar": "🗜", ".7z": "🗜", ".tar": "🗜",
        ".gz": "🗜", ".xz": "🗜", ".bz2": "🗜",
        ".pdf": "📕", ".epub": "📕", ".mobi": "📕", ".djvu": "📕",
        ".txt": "📝", ".md": "📝", ".log": "📝", ".srt": "📝",
        ".ass": "📝", ".sub": "📝",
        ".xlsx": "📊", ".xls": "📊", ".csv": "📊", ".ods": "📊",
        ".apk": "📱", ".xapk": "📱", ".apks": "📱",
        ".exe": "🧩", ".msi": "🧩", ".dmg": "🧩", ".deb": "🧩",
        ".rpm": "🧩",
        ".tgs": "🎭",
    }.get(ext, "📄")


# ═══════════════════════════════════════════════════════════════
# Bulk download — type definitions & filter
# ═══════════════════════════════════════════════════════════════

BULK_TYPES = {
    "all":      ("📥 All Files",    None),
    "video":    ("🎬 Videos",       {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".m4v", ".wmv", ".3gp"}),
    "audio":    ("🎵 Audio",        {".mp3", ".ogg", ".flac", ".wav", ".m4a", ".aac", ".opus", ".wma", ".amr"}),
    "photo":    ("🖼 Photos",       {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".heic"}),
    "doc":      ("📄 Documents",    {".pdf", ".epub", ".mobi", ".djvu", ".txt", ".md", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".csv", ".srt", ".sub", ".ass"}),
    "archive":  ("📦 Archives",     {".zip", ".rar", ".7z", ".tar", ".gz", ".xz", ".bz2", ".zst"}),
    "apk":      ("📱 APKs",         {".apk", ".xapk", ".apks", ".aab"}),
    "sticker":  ("🎭 Stickers",     {".webp", ".tgs"}),
}


def _msg_matches_type(msg, filter_type: str) -> bool:
    """Return True if this message has media matching the given filter_type."""
    if filter_type == "all":
        return bool(msg.media)

    exts = BULK_TYPES[filter_type][1]

    if filter_type == "sticker":
        return bool(msg.sticker)

    if filter_type == "photo":
        if msg.photo:
            return True
        if msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"):
            return True
        return False

    if filter_type == "video":
        if msg.video or msg.video_note:
            return True
        if msg.document:
            ext = os.path.splitext(msg.document.file_name or "")[1].lower()
            return ext in exts
        return False

    if filter_type == "audio":
        if msg.audio or msg.voice:
            return True
        if msg.document:
            ext = os.path.splitext(msg.document.file_name or "")[1].lower()
            return ext in exts
        return False

    # doc, archive, apk — document-based
    if msg.document:
        ext = os.path.splitext(msg.document.file_name or "")[1].lower()
        return ext in exts

    return False




_STATUS_ICON = {
    "queued":      "⏸",
    "downloading": "⏳",
    "done":        "✅",
    "skipped":     "⏭️",
    "failed":      "❌",
}


def _render_entry(e: dict) -> str:
    icon = _STATUS_ICON.get(e["status"], "•")
    mid  = f"`{e['id']}`"

    if e["status"] == "done":
        name  = e.get("name", "?")
        emoji = _file_emoji(name)
        return f"{emoji} {mid} {DOT} {name} **({_sz(e.get('size', 0))})**"

    if e["status"] == "downloading":
        pct  = e.get("pct", 0)
        name = e.get("name", "…")
        return f"{icon} {mid} {DOT} {name}\n    `[{_bar(pct)}]` **{pct:.0f}%**"

    if e["status"] in ("skipped", "failed"):
        return f"{icon} {mid} {DOT} {e.get('reason', '')}"

    return f"{icon} {mid}"


def _render_job(job: dict) -> str:
    header = (
        f"{ICON_DL} **Download Session**\n"
        f"{ICON_FOLDER} `{job['chat_title']}`\n"
        f"{BORDER}\n"
    )
    rows   = "\n".join(_render_entry(e) for e in job["entries"])
    footer = f"\n\n{ICON_CLOCK} `{_elapsed(job['start_time'])}`"
    return header + rows + footer


async def _edit(msg: Message, text: str, reply_markup=None) -> None:
    """Edit a message, suppressing harmless errors."""
    try:
        await msg.edit(text, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await _edit(msg, text, reply_markup)
    except Exception as e:
        log.warning(f"Edit failed: {e}")


# ═══════════════════════════════════════════════════════════════
# Dialog search
# ═══════════════════════════════════════════════════════════════

async def _load_dialogs() -> None:
    global dialogs_cache
    log.info("Loading dialogs cache…")
    dialogs_cache.clear()
    async for d in user_client.get_dialogs():
        c = d.chat
        title = c.title or c.first_name or c.username or str(c.id)
        dialogs_cache.append({
            "id":    c.id,
            "title": title,
            "type":  str(c.type).split(".")[-1].lower(),
        })
    log.info(f"Dialogs cached: {len(dialogs_cache)} entries")


def _search_dialogs(query: str) -> list:
    q = query.lower().strip()
    return [d for d in dialogs_cache if q in d["title"].lower()][:8]


# ═══════════════════════════════════════════════════════════════
# File helpers
# ═══════════════════════════════════════════════════════════════

def _save_path(chat_id, filename: str) -> str:
    """
    Returns a unique absolute path inside DOWNLOAD_BASE/{chat_id}/.
    Appends _1, _2… if the filename already exists.
    """
    folder = os.path.join(DOWNLOAD_BASE, str(chat_id).replace("-100", ""))
    os.makedirs(folder, exist_ok=True)
    dest = os.path.join(folder, filename)
    if os.path.exists(dest):
        base, ext = os.path.splitext(dest)
        i = 1
        while os.path.exists(f"{base}_{i}{ext}"):
            i += 1
        dest = f"{base}_{i}{ext}"
    return dest


def _filename(msg) -> str:
    if msg.document:   return msg.document.file_name   or f"doc_{msg.id}"
    if msg.video:      return msg.video.file_name       or f"video_{msg.id}.mp4"
    if msg.audio:      return msg.audio.file_name       or f"audio_{msg.id}.mp3"
    if msg.voice:      return f"voice_{msg.id}.ogg"
    if msg.photo:      return f"photo_{msg.id}.jpg"
    if msg.animation:  return f"gif_{msg.id}.mp4"
    if msg.video_note: return f"vidnote_{msg.id}.mp4"
    if msg.sticker:    return f"sticker_{msg.id}.webp"
    return f"file_{msg.id}"


# ═══════════════════════════════════════════════════════════════
# Core download
# ═══════════════════════════════════════════════════════════════

async def _download_file(
    msg,
    chat_id,
    job:   dict,
    entry: dict,
    index: int,
    total: int,
) -> tuple[str, int]:
    """Download one media file."""
    fname = _filename(msg)
    path  = _save_path(chat_id, fname)
    _last_update = [0.0]

    async def _progress(current, total_bytes):
        if job.get("cancelled"):
            raise asyncio.CancelledError()
        now = time()
        if now - _last_update[0] < 1.5:
            return
        _last_update[0] = now
        pct  = (current / total_bytes * 100) if total_bytes else 0
        label = fname if total == 1 else f"{fname} [{index + 1}/{total}]"
        entry["pct"]  = pct
        entry["name"] = label
        await _edit(job["progress_msg"], _render_job(job))

    await msg.download(file_name=path, progress=_progress)

    size = os.path.getsize(path) if os.path.exists(path) else 0
    log.info(f"Saved: {path} ({_sz(size)})")
    return fname, size


async def _process_id(msg_id: int, job: dict, entry: dict) -> None:
    """Fetch and download all media for one message ID."""
    chat_id = selected_chat["id"]
    entry["status"] = "downloading"

    if msg_id in downloaded_ids:
        entry["status"] = "skipped"
        entry["reason"] = "Already downloaded"
        return

    msg = None
    for attempt in range(3):
        try:
            fetched = await user_client.get_messages(chat_id=chat_id, message_ids=[msg_id])
            msg = fetched[0] if fetched else None
            break
        except FloodWait as e:
            log.warning(f"FloodWait {e.value}s fetching ID {msg_id} (attempt {attempt+1})")
            if attempt < 2:
                await asyncio.sleep(e.value + 1)
            else:
                entry["status"] = "failed"
                entry["reason"] = f"FloodWait {e.value}s"
                return
        except (PeerIdInvalid, BadRequest) as e:
            entry["status"] = "failed"
            entry["reason"] = str(e)
            return
        except Exception as e:
            entry["status"] = "failed"
            entry["reason"] = str(e)
            log.error(f"Fetch error ID {msg_id}: {e}")
            return

    if not msg or msg.empty:
        entry["status"] = "skipped"
        entry["reason"] = "Not found/deleted"
        return

    try:
        if msg.media_group_id:
            group       = await user_client.get_media_group(chat_id, msg_id)
            media_msgs  = [m for m in group if m.media]
            total_size  = 0
            last_name   = ""

            for i, m in enumerate(media_msgs):
                if job.get("cancelled"):
                    raise asyncio.CancelledError()
                fname, sz = await _download_file(
                    m, chat_id, job, entry, i, len(media_msgs)
                )
                total_size += sz
                last_name   = fname

            count = len(media_msgs)
            entry["name"] = f"{count} files" if count > 1 else last_name
            entry["size"] = total_size

        elif msg.media:
            fname, sz = await _download_file(msg, chat_id, job, entry, 0, 1)
            entry["name"] = fname
            entry["size"] = sz

        else:
            entry["status"] = "skipped"
            entry["reason"] = "No media"
            return

        downloaded_ids.add(msg_id)
        entry["status"] = "done"

    except asyncio.CancelledError:
        entry["status"] = "failed"
        entry["reason"] = "Cancelled"
        raise
    except (PeerIdInvalid, BadRequest) as e:
        entry["status"] = "failed"
        entry["reason"] = str(e)
    except Exception as e:
        log.error(f"Download error ID {msg_id}: {e}")
        entry["status"] = "failed"
        entry["reason"] = str(e)


async def _run_job(job: dict) -> None:
    """Sequential worker — runs the given job."""
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
                await _edit(j["progress_msg"], _render_job(j))
                await asyncio.sleep(0.3)

        except asyncio.CancelledError:
            for e in j["entries"]:
                if e["status"] not in ("done", "skipped", "failed"):
                    e["status"] = "failed"
                    e["reason"] = "Cancelled"
            raise

        finally:
            done    = sum(1 for e in j["entries"] if e["status"] == "done")
            skipped = sum(1 for e in j["entries"] if e["status"] == "skipped")
            failed  = sum(1 for e in j["entries"] if e["status"] == "failed")
            folder_id = str(selected_chat["id"]).replace("-100", "")
            folder    = os.path.join(DOWNLOAD_BASE, folder_id)
            queue_left = len(_job_queue)
            next_note  = f"\n⏭ `{queue_left}` job(s) queued" if queue_left else ""
            summary = (
                _render_job(j) + "\n\n"
                f"{BORDER}\n"
                f"✅ `{done}` done  {DOT}  ⏭️ `{skipped}` skipped  {DOT}  ❌ `{failed}` failed\n"
                f"{ICON_FOLDER} `{folder}`" + next_note
            )
            open_files_markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("📂 Open Files", callback_data=f"openf:{folder_id}")
            ]])
            await _edit(j["progress_msg"], summary, reply_markup=open_files_markup)
            log.info(f"Job finished — done={done} skipped={skipped} failed={failed}")

    try:
        await _execute(job)
        while _job_queue:
            next_job = _job_queue.pop(0)
            await _edit(next_job["progress_msg"],
                        f"▶️ **Starting next…**\n{ICON_FOLDER} `{next_job['chat_title']}`\n"
                        + "\n".join(f"⏸ `{e['id']}`" for e in next_job["entries"]))
            await _execute(next_job)

    except asyncio.CancelledError:
        for pending in _job_queue:
            for e in pending["entries"]:
                e["status"] = "failed"
                e["reason"] = "Cancelled"
            await _edit(pending["progress_msg"], "🛑 **Cancelled** (queue cleared)")
        _job_queue.clear()

    finally:
        _current_job = None
        _worker_task = None


async def _run_bulk_job(chat_id, chat_title: str, filter_type: str, progress_msg) -> None:
    """
    Scan the entire chat history, download every message matching filter_type.
    Runs independently from the normal job queue.
    """
    global _bulk_task

    type_label = BULK_TYPES[filter_type][0]
    scanned    = 0
    found      = 0
    done       = 0
    failed     = 0
    skipped    = 0
    start_time = time()
    last_edit  = [0.0]

    folder_id = str(chat_id).replace("-100", "")
    _save_chat_name(folder_id, chat_title)

    async def _update(final: bool = False):
        now = time()
        if not final and now - last_edit[0] < 2.0:
            return
        last_edit[0] = now
        status = "✅ Done" if final else "⏳ Running"
        text = (
            f"🔍 **Bulk Download** {DOT} {type_label}\n"
            f"{ICON_FOLDER} `{chat_title}`\n"
            f"{BORDER}\n"
            f"📨 Scanned: `{scanned}`\n"
            f"🎯 Matched: `{found}`\n"
            f"✅ Done: `{done}`  ❌ Failed: `{failed}`  ⏭ Skipped: `{skipped}`\n"
            f"{ICON_CLOCK} `{_elapsed(start_time)}`\n\n"
            f"_{status}_"
        )
        try:
            await progress_msg.edit(text)
        except Exception:
            pass

    try:
        async for msg in user_client.get_chat_history(chat_id):
            # Check if cancelled
            if asyncio.current_task().cancelled():
                raise asyncio.CancelledError()

            scanned += 1
            await _update()

            if not _msg_matches_type(msg, filter_type):
                continue

            found += 1

            # Skip already-downloaded in this session
            if msg.id in downloaded_ids:
                skipped += 1
                continue

            try:
                fname = _filename(msg)
                path  = _save_path(chat_id, fname)

                await msg.download(file_name=path)

                size = os.path.getsize(path) if os.path.exists(path) else 0
                downloaded_ids.add(msg.id)
                done += 1
                log.info(f"Bulk saved: {path} ({_sz(size)})")

            except asyncio.CancelledError:
                raise
            except FloodWait as e:
                log.warning(f"Bulk FloodWait {e.value}s")
                await asyncio.sleep(e.value + 1)
                # retry
                try:
                    await msg.download(file_name=path)
                    downloaded_ids.add(msg.id)
                    done += 1
                except Exception as e2:
                    log.error(f"Bulk retry failed: {e2}")
                    failed += 1
            except Exception as e:
                log.error(f"Bulk download error msg {msg.id}: {e}")
                failed += 1

    except asyncio.CancelledError:
        await progress_msg.edit(
            f"🛑 **Bulk Download Cancelled**\n"
            f"{ICON_FOLDER} `{chat_title}` {DOT} {type_label}\n"
            f"{BORDER}\n"
            f"📨 Scanned: `{scanned}` {DOT} 🎯 Matched: `{found}`\n"
            f"✅ `{done}` done  ❌ `{failed}` failed\n"
            f"{ICON_CLOCK} `{_elapsed(start_time)}`"
        )
        return

    except Exception as e:
        log.error(f"Bulk job error: {e}")
        await progress_msg.edit(f"❌ **Bulk Download Error**\n\n`{e}`")
        return

    finally:
        _bulk_task = None

    await _update(final=True)

    # Show open files button
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("📂 Open Files", callback_data=f"openf:{folder_id}")
    ]])
    try:
        await progress_msg.edit(
            f"✅ **Bulk Download Complete**\n"
            f"{ICON_FOLDER} `{chat_title}` {DOT} {type_label}\n"
            f"{BORDER}\n"
            f"📨 Scanned: `{scanned}` {DOT} 🎯 Matched: `{found}`\n"
            f"✅ `{done}` done  ❌ `{failed}` failed  ⏭ `{skipped}` skipped\n"
            f"{ICON_CLOCK} `{_elapsed(start_time)}`",
            reply_markup=markup,
        )
    except Exception:
        pass

    log.info(f"Bulk done — scanned={scanned} found={found} done={done} failed={failed}")


# ═══════════════════════════════════════════════════════════════
# Command handlers
# ═══════════════════════════════════════════════════════════════

PAGE_SIZE = 8


def _dialog_list_markup(uid: int, page: int = 0, query: str = "") -> tuple[str, InlineKeyboardMarkup]:
    """Build text + keyboard for a page of dialogs."""
    pool = (
        [d for d in dialogs_cache if query.lower() in d["title"].lower()]
        if query else dialogs_cache
    )
    search_results[uid] = pool

    total   = len(pool)
    start   = page * PAGE_SIZE
    page_items = pool[start : start + PAGE_SIZE]
    total_pages = max(1, -(-total // PAGE_SIZE))

    TYPE_ICON = {"group": "👥", "supergroup": "👥", "channel": "📢", "bot": "🤖", "private": "👤"}
    buttons = [
        [InlineKeyboardButton(
            f"{TYPE_ICON.get(d['type'], '💬')} {d['title']}",
            callback_data=f"sc:{start + i}",
        )]
        for i, d in enumerate(page_items)
    ]

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pg:{page-1}:{query}"))
    if start + PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pg:{page+1}:{query}"))
    if nav:
        buttons.append(nav)

    header = (
        f"{ICON_FOLDER} **Select a Chat** {DOT} {total} found\n"
        + (f"🔍 `{query}`\n" if query else "")
        + f"{SEPARATOR}\n"
        f"Page `{page+1}/{total_pages}` {DOT} Type to filter"
    )
    return header, InlineKeyboardMarkup(buttons)


async def _show_dialog_list(target, uid: int, page: int = 0, query: str = "") -> None:
    """Send (or edit) the dialog list."""
    if not dialogs_cache:
        text = f"{ICON_WARN} No chats loaded yet. Please wait…"
        if hasattr(target, "reply"):
            await target.reply(text)
        else:
            await target.message.edit(text)
        return

    text, markup = _dialog_list_markup(uid, page, query)
    if hasattr(target, "reply"):
        await target.reply(text, reply_markup=markup)
    else:
        await target.message.edit(text, reply_markup=markup)


async def cmd_start(_, message: Message):
    global downloaded_ids, _current_job, _worker_task, _job_queue

    if _worker_task and not _worker_task.done():
        if _current_job:
            _current_job["cancelled"] = True
        _worker_task.cancel()
        _worker_task = None
        _current_job = None
    _job_queue.clear()
    downloaded_ids = set()

    buttons = []
    if selected_chat.get("id"):
        buttons.append([InlineKeyboardButton(
            f"{ARROW} Continue {ARROW} {selected_chat['title']}",
            callback_data="welcome:resume",
        )])
    buttons.append([
        InlineKeyboardButton("📂 Set Chat", callback_data="welcome:setchat"),
        InlineKeyboardButton("❓ Help", callback_data="welcome:help"),
    ])

    await message.reply(
        "👻 **GhostFetch** >> Secure Media Downloader\n"
        f"{BORDER}\n\n"
        "🎯 **What can you do?**\n\n"
        "✦ Download from any Telegram chat\n"
        "✦ Auto-queue multiple downloads\n"
        "✦ Browse & manage your files\n"
        "✦ Fast & reliable\n\n"
        "🚀 **Get Started**\n"
        "Just pick a chat, send message IDs, and let us do the rest!\n\n"
        "v1.0 by herraChron",
        reply_markup=InlineKeyboardMarkup(buttons),
        disable_web_page_preview=True,
    )


async def cmd_setchat(_, message: Message):
    uid = message.from_user.id
    user_state[uid] = "selecting"
    await _show_dialog_list(message, uid)


async def cb_welcome(_, query: CallbackQuery):
    """Handle welcome buttons."""
    uid    = query.from_user.id
    action = query.data.split(":")[1]

    if action == "resume":
        if not selected_chat.get("id"):
            await query.answer("No previous session.", show_alert=True)
            return
        user_state[uid] = "idle"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Download All",   callback_data="dlmode:all"),
             InlineKeyboardButton("✏️ Manual IDs",     callback_data="dlmode:manual")],
            [InlineKeyboardButton("🎬 Videos",          callback_data="dlmode:video"),
             InlineKeyboardButton("🎵 Audio",           callback_data="dlmode:audio")],
            [InlineKeyboardButton("🖼 Photos",          callback_data="dlmode:photo"),
             InlineKeyboardButton("📄 Documents",       callback_data="dlmode:doc")],
            [InlineKeyboardButton("📦 Archives",        callback_data="dlmode:archive"),
             InlineKeyboardButton("📱 APKs",            callback_data="dlmode:apk")],
            [InlineKeyboardButton("🎭 Stickers",        callback_data="dlmode:sticker")],
        ])
        await query.message.edit(
            f"{ICON_SUCCESS} **Session Resumed**\n\n"
            f"{ICON_FOLDER} `{selected_chat['title']}`\n"
            f"🆔 `{selected_chat['id']}`\n\n"
            "Choose what to download:",
            reply_markup=markup,
        )
        await query.answer()

    elif action == "setchat":
        user_state[uid] = "selecting"
        await query.answer()
        await _show_dialog_list(query, uid)

    elif action == "help":
        await query.answer()
        await query.message.edit(
            f"{ICON_HELP} **GhostFetch Help**\n"
            f"{BORDER}\n\n"
            "**Commands**\n"
            f"/start {DOT} Reset session\n"
            f"/setchat {DOT} Change chat\n"
            f"/files {DOT} Browse downloads\n"
            f"/stats {DOT} System stats\n"
            f"/killall {DOT} Cancel all\n"
            f"/help {DOT} Help\n\n"
            "**How to Use**\n"
            "1. /start\n"
            "2. Pick a chat\n"
            "3. Send message IDs\n\n"
            f"📍 Files: `{DOWNLOAD_BASE}`",
            disable_web_page_preview=True,
        )


async def cmd_help(_, message: Message):
    await message.reply(
        f"{ICON_HELP} **GhostFetch Help**\n"
        f"{BORDER}\n\n"
        "**Commands**\n"
        f"/start {DOT} Reset session\n"
        f"/setchat {DOT} Change chat\n"
        f"/files {DOT} Browse downloads\n"
        f"/stats {DOT} System stats\n"
        f"/killall {DOT} Cancel all\n"
        f"/help {DOT} Help\n\n"
        "**How to Use**\n"
        "1. /start\n"
        "2. Pick a chat\n"
        "3. Send message IDs\n\n"
        f"📍 Files: `{DOWNLOAD_BASE}`",
        disable_web_page_preview=True,
    )


# ═══════════════════════════════════════════════════════════════
# /files — folder-first browser
# ═══════════════════════════════════════════════════════════════

FILES_PAGE_SIZE = 5


def _scan_folders() -> list[dict]:
    """Return sorted list of download folders."""
    result = []
    if not os.path.exists(DOWNLOAD_BASE):
        return result
    for folder_id in sorted(os.listdir(DOWNLOAD_BASE)):
        full = os.path.join(DOWNLOAD_BASE, folder_id)
        if not os.path.isdir(full):
            continue
        files = [f for f in os.listdir(full) if os.path.isfile(os.path.join(full, f))]
        total = sum(os.path.getsize(os.path.join(full, f)) for f in files)
        result.append({
            "folder_id":  folder_id,
            "title":      _folder_title(folder_id),
            "file_count": len(files),
            "total_size": total,
            "path":       full,
        })
    return result


def _scan_files_in(folder_path: str) -> list[dict]:
    """Return files inside a folder."""
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
    buttons = []
    for i, f in enumerate(folders):
        label = f"📁 {f['title']} {DOT} {f['file_count']}f {DOT} {_sz(f['total_size'])}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"fd:{i}")])
    buttons.append([InlineKeyboardButton("🗑 Wipe All", callback_data="fwipe")])
    header = (
        f"📂 **Downloads** {DOT} {len(folders)} folders\n"
        f"📊 {total_files} files {DOT} {_sz(total_size)}\n"
        f"{SEPARATOR}"
    )
    return header, InlineKeyboardMarkup(buttons)


def _folder_files_markup(folder: dict, fi: int, files: list, page: int) -> tuple[str, InlineKeyboardMarkup]:
    total       = len(files)
    total_pages = max(1, -(-total // FILES_PAGE_SIZE))
    start       = page * FILES_PAGE_SIZE
    page_items  = files[start: start + FILES_PAGE_SIZE]

    buttons = []
    for i, f in enumerate(page_items):
        actual = start + i
        short  = (f["name"][:48] + "…") if len(f["name"]) > 48 else f["name"]
        emoji  = _file_emoji(f["name"])
        buttons.append([
            InlineKeyboardButton(f"{emoji} {short}", callback_data=f"fl:{fi}:{actual}"),
            InlineKeyboardButton(f"🗑 {_sz(f['size'])}", callback_data=f"fdel:{fi}:{actual}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"fp:{fi}:{page - 1}"))
    nav.append(InlineKeyboardButton("🔙 Back", callback_data="fb"))
    if start + FILES_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"fp:{fi}:{page + 1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("🗑 Delete all", callback_data=f"fdeldir:{fi}")])

    header = (
        f"📁 **{folder['title']}**\n"
        f"📄 {total} files {DOT} {_sz(folder['total_size'])} {DOT} {page + 1}/{total_pages}"
    )
    return header, InlineKeyboardMarkup(buttons)


async def _send_folder_view(target, uid: int) -> None:
    """Send folder list."""
    folders = _scan_folders()
    _files_nav[uid] = {"view": "folders", "folders": folders}
    if not folders:
        text = "📭 No downloads yet"
        if hasattr(target, "reply"):
            await target.reply(text)
        else:
            await target.message.edit(text)
        return
    text, markup = _folders_markup(folders)
    if hasattr(target, "reply"):
        await target.reply(text, reply_markup=markup)
    else:
        await target.message.edit(text, reply_markup=markup)


async def _send_file_view(target, uid: int, fi: int, page: int = 0, reply: bool = False) -> None:
    """Send file list."""
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
    _files_nav[uid] = {"view": "files", "folders": folders, "fi": fi, "files": files, "page": page}

    if not files:
        text   = f"📭 **{folder['title']}** is empty"
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="fb")]])
        if reply or hasattr(target, "reply"):
            await target.reply(text, reply_markup=markup)
        else:
            await target.message.edit(text, reply_markup=markup)
        return

    text, markup = _folder_files_markup(folder, fi, files, page)
    if reply or (hasattr(target, "reply") and not hasattr(target, "data")):
        await target.reply(text, reply_markup=markup)
    else:
        await target.message.edit(text, reply_markup=markup)


async def cmd_files(_, message: Message):
    await _send_folder_view(message, message.from_user.id)


async def cb_open_folder(_, query: CallbackQuery):
    """Tap a folder to show its files."""
    uid = query.from_user.id
    fi  = int(query.data.split(":")[1])
    await _send_file_view(query, uid, fi)
    await query.answer()


async def cb_files_page(_, query: CallbackQuery):
    """Paginate file list."""
    uid   = query.from_user.id
    parts = query.data.split(":")
    fi    = int(parts[1])
    page  = int(parts[2])
    await _send_file_view(query, uid, fi, page)
    await query.answer()


async def cb_back_to_folders(_, query: CallbackQuery):
    """Back to folder list."""
    await _send_folder_view(query, query.from_user.id)
    await query.answer()


async def cb_send_file(_, query: CallbackQuery):
    """Send file."""
    uid   = query.from_user.id
    parts = query.data.split(":")
    fi    = int(parts[1])
    idx   = int(parts[2])

    nav   = _files_nav.get(uid, {})
    files = nav.get("files")
    if not files or idx >= len(files):
        await query.answer("File list expired.", show_alert=True)
        return

    entry = files[idx]
    if not os.path.exists(entry["path"]):
        await query.answer("File not found.", show_alert=True)
        return

    await query.answer("📤 Sending…")
    await query.message.reply_document(
        entry["path"],
        caption=f"`{entry['name']}`\n{_sz(entry['size'])}",
    )
    page = nav.get("page", 0)
    await _send_file_view(query.message, uid, fi, page, reply=True)


async def cb_delete_file(_, query: CallbackQuery):
    """Delete a file."""
    uid   = query.from_user.id
    parts = query.data.split(":")
    fi    = int(parts[1])
    idx   = int(parts[2])

    nav   = _files_nav.get(uid, {})
    files = nav.get("files")
    if not files or idx >= len(files):
        await query.answer("List expired.", show_alert=True)
        return

    entry = files[idx]
    name  = entry["name"]
    if os.path.exists(entry["path"]):
        os.remove(entry["path"])
        log.info(f"Deleted: {entry['path']}")

    await query.answer("🗑 Deleted")
    page = nav.get("page", 0)
    await _send_file_view(query, uid, fi, page)


async def cb_delete_folder(_, query: CallbackQuery):
    """Confirm delete folder."""
    uid = query.from_user.id
    fi  = int(query.data.split(":")[1])

    nav     = _files_nav.get(uid, {})
    folders = nav.get("folders") or _scan_folders()
    if fi >= len(folders):
        await query.answer("Not found.", show_alert=True)
        return

    folder = folders[fi]
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes", callback_data=f"fdeldirok:{fi}"),
        InlineKeyboardButton("❌ No", callback_data=f"fp:{fi}:0"),
    ]])
    await query.message.edit(
        f"{ICON_WARN} Delete {folder['title']}?\n\n"
        f"`{folder['file_count']} files` {DOT} `{_sz(folder['total_size'])}`",
        reply_markup=markup,
    )
    await query.answer()


async def cb_delete_folder_confirm(_, query: CallbackQuery):
    """Actually delete folder."""
    uid = query.from_user.id
    fi  = int(query.data.split(":")[1])

    nav     = _files_nav.get(uid, {})
    folders = nav.get("folders") or _scan_folders()
    if fi >= len(folders):
        await query.answer("Not found.", show_alert=True)
        return

    folder = folders[fi]
    if os.path.exists(folder["path"]):
        shutil.rmtree(folder["path"])
        log.info(f"Deleted folder: {folder['path']}")

    await query.answer("🗑 Deleted")
    await _send_folder_view(query, uid)


async def cb_wipe_all(_, query: CallbackQuery):
    """Confirm wipe all."""
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("💀 Yes", callback_data="fwipeok"),
        InlineKeyboardButton("❌ No", callback_data="fb"),
    ]])
    await query.message.edit(
        "☢️ Wipe ALL Downloads?\n\nAll files will be deleted",
        reply_markup=markup,
    )
    await query.answer()


async def cb_wipe_all_confirm(_, query: CallbackQuery):
    """Wipe all downloads."""
    uid = query.from_user.id
    if os.path.exists(DOWNLOAD_BASE):
        shutil.rmtree(DOWNLOAD_BASE)
        os.makedirs(DOWNLOAD_BASE, exist_ok=True)
        log.info("Wiped all downloads")
    _files_nav.pop(uid, None)
    await query.answer("💀 Wiped")
    await query.message.edit("📭 All downloads cleared")


async def cmd_killall(_, message: Message):
    global _worker_task, _current_job, _job_queue, _bulk_task

    nothing_running = (
        not (_worker_task and not _worker_task.done())
        and not _job_queue
        and not (_bulk_task and not _bulk_task.done())
    )
    if nothing_running:
        return await message.reply(f"{ICON_INFO} Nothing running")

    if _current_job:
        _current_job["cancelled"] = True
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
    if _bulk_task and not _bulk_task.done():
        _bulk_task.cancel()

    _worker_task = None
    _current_job = None
    _bulk_task   = None
    _job_queue.clear()

    await message.reply(f"🛑 **Stopped** {DOT} Queue cleared")
    log.info("killall")


async def cmd_log(_, message: Message):
    if os.path.exists(LOG_FILE):
        await message.reply_document(LOG_FILE, caption="📋 Session Log")
    else:
        await message.reply("⚠️ No log")


async def cmd_stats(_, message: Message):
    try:
        dl_root = DOWNLOAD_BASE if os.path.exists(DOWNLOAD_BASE) else "."
        total, used, free = shutil.disk_usage(dl_root)
        net  = psutil.net_io_counters()
        proc = psutil.Process(os.getpid())
        cpu  = await asyncio.to_thread(psutil.cpu_percent, interval=0.5)
        ram  = psutil.virtual_memory().percent
        disk = psutil.disk_usage(dl_root).percent
    except Exception as e:
        return await message.reply(f"{ICON_WARN} Stats unavailable: `{e}`")

    chat_line = (
        f"`{selected_chat['title']}`"
        if selected_chat.get("id") else "Not set"
    )
    job_line = (
        f"`{len(_current_job['ids'])} IDs` downloading"
        if _current_job and "ids" in _current_job else "Idle"
    )

    await message.reply(
        f"📊 **Bot Stats**\n"
        f"{SEPARATOR}\n"
        f"⏱ Uptime: `{_elapsed(BOT_START_TIME)}`\n"
        f"📂 Chat: {chat_line}\n"
        f"⚙️ Status: {job_line}\n"
        f"📥 Downloaded: `{len(downloaded_ids)}` IDs\n\n"
        f"💾 Disk: `{_sz(used)}/{_sz(total)}` ({disk:.1f}%)\n"
        f"📡 Network: ↑ `{_sz(net.bytes_sent)}` ↓ `{_sz(net.bytes_recv)}`\n"
        f"🖥 CPU: `{cpu:.1f}%` RAM: `{ram:.1f}%`\n"
        f"🧠 Bot RAM: `{round(proc.memory_info().rss / 1024 ** 2)} MB`"
    )


# ═══════════════════════════════════════════════════════════════
# Main text handler
# ═══════════════════════════════════════════════════════════════

async def handle_text(_, message: Message):
    global _current_job, _worker_task, _job_queue

    uid   = message.from_user.id
    text  = message.text.strip()
    state = user_state.get(uid, "idle")

    if state == "selecting":
        await _show_dialog_list(message, uid, page=0, query=text)
        return

    if not selected_chat.get("id"):
        return await message.reply(
            f"{ICON_WARN} **No chat selected**\n\n"
            "Use /start or /setchat"
        )

    tokens  = text.split()
    msg_ids = []
    bad     = []
    for t in tokens:
        if t.isdigit():
            msg_ids.append(int(t))
        else:
            bad.append(t)

    if bad:
        return await message.reply(
            f"{ICON_ERROR} Invalid: `{' '.join(bad)}`\n\n"
            "Send message IDs only (space-separated)"
        )
    if not msg_ids:
        return await message.reply("Send at least one message ID")

    entries = [{"id": mid, "status": "queued"} for mid in msg_ids]

    if _current_job or (_worker_task and not _worker_task.done()):
        pos = len(_job_queue) + 1
        progress_msg = await message.reply(
            f"📋 **Queued** (#{pos}) {DOT} {len(msg_ids)} ID(s)\n"
            f"{ICON_FOLDER} `{selected_chat['title']}`\n"
            + "\n".join(f"⏸ `{mid}`" for mid in msg_ids)
        )
        job = {
            "ids":          msg_ids,
            "entries":      entries,
            "chat_title":   selected_chat["title"],
            "progress_msg": progress_msg,
            "start_time":   time(),
            "cancelled":    False,
        }
        _job_queue.append(job)
        log.info(f"Queued: {msg_ids}")
        return

    progress_msg = await message.reply(
        f"{ICON_DL} **Starting…** {len(msg_ids)} ID(s)\n"
        f"{ICON_FOLDER} `{selected_chat['title']}`"
    )
    job = {
        "ids":          msg_ids,
        "entries":      entries,
        "chat_title":   selected_chat["title"],
        "progress_msg": progress_msg,
        "start_time":   time(),
        "cancelled":    False,
    }
    _current_job = job
    _worker_task = asyncio.create_task(_run_job(job))


async def cb_dlmode(_, query: CallbackQuery):
    """Handle download mode selection buttons."""
    global _bulk_task

    uid         = query.from_user.id
    filter_type = query.data.split(":")[1]

    if not selected_chat.get("id"):
        await query.answer("No chat selected.", show_alert=True)
        return

    if filter_type == "manual":
        await query.message.edit(
            f"{ICON_SUCCESS} **Ready**\n\n"
            f"{ICON_FOLDER} `{selected_chat['title']}`\n\n"
            "Send me message IDs to download:\n"
            "`26473`  or  `26473 26570 26600`"
        )
        await query.answer()
        return

    # Bulk download — check nothing is already running
    if _bulk_task and not _bulk_task.done():
        await query.answer("⚠️ A bulk download is already running. Use /killall to stop it.", show_alert=True)
        return

    type_label = BULK_TYPES[filter_type][0]
    await query.answer(f"Starting {type_label}…")

    progress_msg = await query.message.reply(
        f"🔍 **Bulk Download Starting…**\n"
        f"{ICON_FOLDER} `{selected_chat['title']}`\n"
        f"Type: {type_label}\n\n"
        "_Scanning chat history…_"
    )

    _bulk_task = asyncio.create_task(
        _run_bulk_job(
            selected_chat["id"],
            selected_chat["title"],
            filter_type,
            progress_msg,
        )
    )


async def cb_select_chat(_, query: CallbackQuery):
    global selected_chat

    uid  = query.from_user.id
    idx  = int(query.data.split(":")[1])
    pool = search_results.get(uid, [])

    if idx >= len(pool):
        return await query.answer("Expired", show_alert=True)

    chosen = pool[idx]
    selected_chat = {"id": chosen["id"], "title": chosen["title"]}
    _save_session()

    user_state[uid] = "idle"
    search_results.pop(uid, None)

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Download All",   callback_data="dlmode:all"),
         InlineKeyboardButton("✏️ Manual IDs",     callback_data="dlmode:manual")],
        [InlineKeyboardButton("🎬 Videos",          callback_data="dlmode:video"),
         InlineKeyboardButton("🎵 Audio",           callback_data="dlmode:audio")],
        [InlineKeyboardButton("🖼 Photos",          callback_data="dlmode:photo"),
         InlineKeyboardButton("📄 Documents",       callback_data="dlmode:doc")],
        [InlineKeyboardButton("📦 Archives",        callback_data="dlmode:archive"),
         InlineKeyboardButton("📱 APKs",            callback_data="dlmode:apk")],
        [InlineKeyboardButton("🎭 Stickers",        callback_data="dlmode:sticker")],
    ])

    await query.message.edit(
        f"{ICON_SUCCESS} **Chat Set**\n\n"
        f"{ICON_FOLDER} `{chosen['title']}`\n"
        f"🆔 `{chosen['id']}`\n\n"
        "Choose what to download:",
        reply_markup=markup,
    )
    await query.answer()
    log.info(f"Chat: {chosen['title']}")


async def cb_page(_, query: CallbackQuery):
    """Handle pagination."""
    uid   = query.from_user.id
    parts = query.data.split(":", 2)
    page  = int(parts[1])
    q     = parts[2] if len(parts) > 2 else ""
    await _show_dialog_list(query, uid, page=page, query=q)
    await query.answer()


async def cb_open_files_from_job(_, query: CallbackQuery):
    """Open files from job summary button."""
    uid       = query.from_user.id
    folder_id = query.data.split(":")[1]
    folders   = _scan_folders()
    fi = next((i for i, f in enumerate(folders) if f["folder_id"] == folder_id), None)
    if fi is None:
        await query.answer("Not found", show_alert=True)
        return
    _files_nav[uid] = {"view": "folders", "folders": folders}
    await query.answer()
    await _send_file_view(query.message, uid, fi, page=0, reply=True)


# ═══════════════════════════════════════════════════════════════
# Bot command menu — uses Bot API directly so commands appear
# in the "/" menu inside the chat, not just in BotFather
# ═══════════════════════════════════════════════════════════════

async def _set_bot_commands() -> None:
    url = f"https://api.telegram.org/bot{PyroConf.BOT_TOKEN}/setMyCommands"
    payload = {
        "commands": [
            {"command": "start",   "description": "Start new session"},
            {"command": "setchat", "description": "Change chat"},
            {"command": "files",   "description": "Browse downloads"},
            {"command": "killall", "description": "Stop all downloads"},
            {"command": "stats",   "description": "System stats"},
            {"command": "log",     "description": "Session log"},
            {"command": "help",    "description": "Help"},
        ]
    }
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(url, json=payload)
        if r.json().get("ok"):
            log.info("Bot commands registered")
        else:
            log.warning(f"Command registration failed: {r.text}")
    except Exception as e:
        log.warning(f"Command registration error: {e}")


# ═══════════════════════════════════════════════════════════════
# Startup & entry point
# ═══════════════════════════════════════════════════════════════

async def _initialize() -> None:
    _load_session()
    os.makedirs(DOWNLOAD_BASE, exist_ok=True)
    log.info("Starting user client…")
    await user_client.start()
    await _load_dialogs()
    log.info("Initialization complete")


async def main() -> None:
    global bot, user_client

    bot = Client(
        "media_bot",
        api_id=PyroConf.API_ID,
        api_hash=PyroConf.API_HASH,
        bot_token=PyroConf.BOT_TOKEN,
        workers=20,
        parse_mode=ParseMode.MARKDOWN,
        sleep_threshold=30,
    )
    user_client = Client(
        "user_account",
        session_string=PyroConf.SESSION_STRING,
        in_memory=True,
        workers=20,
        sleep_threshold=30,
    )

    bot.add_handler(MessageHandler(cmd_start,   filters.command("start")   & filters.private))
    bot.add_handler(MessageHandler(cmd_setchat, filters.command("setchat") & filters.private))
    bot.add_handler(MessageHandler(cmd_help,    filters.command("help")    & filters.private))
    bot.add_handler(MessageHandler(cmd_files,   filters.command("files")   & filters.private))
    bot.add_handler(MessageHandler(cmd_killall, filters.command("killall") & filters.private))
    bot.add_handler(MessageHandler(cmd_log,     filters.command("log")     & filters.private))
    bot.add_handler(MessageHandler(cmd_stats,   filters.command("stats")   & filters.private))
    bot.add_handler(MessageHandler(
        handle_text,
        filters.private & filters.text
        & ~filters.command(["start", "setchat", "help", "killall", "log", "stats", "files"]),
    ))
    bot.add_handler(CallbackQueryHandler(cb_select_chat,           filters.regex(r"^sc:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_page,                  filters.regex(r"^pg:\d+")))
    bot.add_handler(CallbackQueryHandler(cb_welcome,               filters.regex(r"^welcome:")))
    bot.add_handler(CallbackQueryHandler(cb_dlmode,                filters.regex(r"^dlmode:")))
    bot.add_handler(CallbackQueryHandler(cb_open_files_from_job,   filters.regex(r"^openf:")))
    bot.add_handler(CallbackQueryHandler(cb_open_folder,           filters.regex(r"^fd:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_files_page,            filters.regex(r"^fp:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_back_to_folders,       filters.regex(r"^fb$")))
    bot.add_handler(CallbackQueryHandler(cb_send_file,             filters.regex(r"^fl:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_file,           filters.regex(r"^fdel:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_folder,         filters.regex(r"^fdeldir:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_folder_confirm, filters.regex(r"^fdeldirok:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_wipe_all,              filters.regex(r"^fwipe$")))
    bot.add_handler(CallbackQueryHandler(cb_wipe_all_confirm,      filters.regex(r"^fwipeok$")))

    await _initialize()
    await bot.start()
    await _set_bot_commands()
    log.info("Bot is online")
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
