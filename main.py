# GhostFetch — Telegram restricted media downloader
# herraChron

import os
import json
import shutil
import psutil
import asyncio
import logging
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

DEFAULT_DOWNLOAD_BASE = "/data/data/com.termux/files/home/GhostFetch/downloads"
SESSION_FILE          = "session.json"
PATH_FILE             = "path.json"
LOG_FILE              = "session.log"
BOT_START_TIME        = time()

PATH_PRESETS = {
    "termux":   "/data/data/com.termux/files/home/GhostFetch/downloads",
    "internal": "/storage/emulated/0/Download/GhostFetch/downloads",
}

# Loaded at startup from path.json, falls back to DEFAULT_DOWNLOAD_BASE
DOWNLOAD_BASE = DEFAULT_DOWNLOAD_BASE


# ═══════════════════════════════════════════════════════════════
# Logging  (fresh file each run, also prints to console)
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

# Clients are initialized inside main() so they bind to the correct event loop.
# Creating them at module level causes "Future attached to a different loop"
# on Python 3.10+ because Pyrogram grabs the loop at construction time.
bot         = None
user_client = None


# ═══════════════════════════════════════════════════════════════
# Global state
# ═══════════════════════════════════════════════════════════════

dialogs_cache:  list        = []    # [{id, title, type}] — loaded once at startup
selected_chat:  dict        = {}    # {"id": .., "title": ..} — persisted to disk
downloaded_ids: set         = set() # cleared on /start (in-memory dedup)
user_state:     dict        = {}    # uid → "idle" | "selecting"
search_results: dict        = {}    # uid → [dialog, …]  (temp, per search)

_job_queue:   list               = []   # pending jobs (dicts), processed in order
_current_job: dict | None        = None # the job currently being downloaded
_worker_task: asyncio.Task | None = None

user_setpath_state: dict = {}  # uid → "waiting_custom"


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


def _load_path() -> None:
    global DOWNLOAD_BASE
    try:
        with open(PATH_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if data.get("path"):
            DOWNLOAD_BASE = data["path"]
            log.info(f"Loaded download path: {DOWNLOAD_BASE}")
    except FileNotFoundError:
        DOWNLOAD_BASE = DEFAULT_DOWNLOAD_BASE
    except Exception as e:
        log.warning(f"Path load failed: {e}")
        DOWNLOAD_BASE = DEFAULT_DOWNLOAD_BASE


def _save_path_config(path: str) -> None:
    global DOWNLOAD_BASE
    DOWNLOAD_BASE = path
    try:
        with open(PATH_FILE, "w", encoding="utf-8") as f:
            json.dump({"path": path}, f, indent=2)
    except Exception as e:
        log.error(f"Path save failed: {e}")


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


def _bar(pct: float, width: int = 10) -> str:
    """Text progress bar."""
    n = round(width * pct / 100)
    return "█" * n + "░" * (width - n)


# ═══════════════════════════════════════════════════════════════
# Progress renderer
# ═══════════════════════════════════════════════════════════════

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
        return f"{icon} {mid} — {e.get('name', '?')} · **{_sz(e.get('size', 0))}**"

    if e["status"] == "downloading":
        pct  = e.get("pct", 0)
        name = e.get("name", "…")
        return f"{icon} {mid} — {name}\n    `[{_bar(pct)}]` **{pct:.0f}%**"

    if e["status"] in ("skipped", "failed"):
        return f"{icon} {mid} — _{e.get('reason', '')}_"

    # queued
    return f"{icon} {mid}"


def _render_job(job: dict) -> str:
    header = (
        f"📥 **Download** · {len(job['ids'])} ID(s)\n"
        f"📂 `{job['chat_title']}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    rows   = "\n".join(_render_entry(e) for e in job["entries"])
    footer = f"\n\n⏱ `{_elapsed(job['start_time'])}`"
    return header + rows + footer


async def _edit(msg: Message, text: str) -> None:
    """Edit a message, suppressing harmless errors."""
    try:
        await msg.edit(text)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await _edit(msg, text)
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
    """
    Download one media file.
    Returns (filename, size_bytes).
    Raises CancelledError if job is cancelled mid-download.
    """
    fname = _filename(msg)
    path  = _save_path(chat_id, fname)
    _last_update = [0.0]

    async def _progress(current, total_bytes):
        if job.get("cancelled"):
            raise asyncio.CancelledError()
        now = time()
        if now - _last_update[0] < 1.5:   # throttle edits
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
    """
    Fetch and download all media for one message ID.
    Handles media groups, single files, and text-only messages.
    Updates `entry` in-place throughout.
    """
    chat_id = selected_chat["id"]
    entry["status"] = "downloading"

    # In-memory dedup (cleared on /start)
    if msg_id in downloaded_ids:
        entry["status"] = "skipped"
        entry["reason"] = "Already downloaded this session"
        return

    # Fetch message with retry on FloodWait
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
                entry["reason"] = f"FloodWait {e.value}s, giving up"
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
        entry["reason"] = "Message not found or deleted"
        return

    try:
        # ── Media group (multiple files in one post) ──────────
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

        # ── Single media file ─────────────────────────────────
        elif msg.media:
            fname, sz = await _download_file(msg, chat_id, job, entry, 0, 1)
            entry["name"] = fname
            entry["size"] = sz

        # ── Text-only or empty ────────────────────────────────
        else:
            entry["status"] = "skipped"
            entry["reason"] = "No downloadable media"
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
    """
    Sequential worker — runs the given job, then drains _job_queue automatically.
    Each job's progress message is updated live; a summary is posted when it finishes.
    """
    global _current_job, _worker_task, _job_queue

    async def _execute(j: dict) -> None:
        global _current_job
        _current_job = j
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
            folder  = os.path.join(
                DOWNLOAD_BASE,
                str(selected_chat["id"]).replace("-100", "")
            )
            queue_left = len(_job_queue)
            next_note  = f"\n⏭ `{queue_left}` job(s) still queued" if queue_left else ""
            summary = (
                _render_job(j) + "\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ `{done}` done  ·  ⏭️ `{skipped}` skipped  ·  ❌ `{failed}` failed\n"
                f"📁 `{folder}`" + next_note
            )
            await _edit(j["progress_msg"], summary)
            log.info(f"Job finished — done={done} skipped={skipped} failed={failed}")

    try:
        await _execute(job)
        # Drain the queue
        while _job_queue:
            next_job = _job_queue.pop(0)
            await _edit(next_job["progress_msg"],
                        f"▶️ **Starting now…**\n📂 `{next_job['chat_title']}`\n"
                        + "\n".join(f"⏸ `{e['id']}`" for e in next_job["entries"]))
            await _execute(next_job)

    except asyncio.CancelledError:
        # Mark all queued jobs as cancelled too
        for pending in _job_queue:
            for e in pending["entries"]:
                e["status"] = "failed"
                e["reason"] = "Cancelled"
            await _edit(pending["progress_msg"], "🛑 **Cancelled** (queue cleared)")
        _job_queue.clear()

    finally:
        _current_job = None
        _worker_task = None


# ═══════════════════════════════════════════════════════════════
# Command handlers
# ═══════════════════════════════════════════════════════════════

PAGE_SIZE = 10   # dialogs per page


def _dialog_list_markup(uid: int, page: int = 0, query: str = "") -> tuple[str, InlineKeyboardMarkup]:
    """Build text + keyboard for a page of dialogs, optionally filtered by query."""
    pool = (
        [d for d in dialogs_cache if query.lower() in d["title"].lower()]
        if query else dialogs_cache
    )
    search_results[uid] = pool          # store the filtered pool for cb_select_chat

    total   = len(pool)
    start   = page * PAGE_SIZE
    page_items = pool[start : start + PAGE_SIZE]
    total_pages = max(1, -(-total // PAGE_SIZE))   # ceil division

    TYPE_ICON = {"group": "👥", "supergroup": "👥", "channel": "📢", "bot": "🤖", "private": "👤"}
    buttons = [
        [InlineKeyboardButton(
            f"{TYPE_ICON.get(d['type'], '💬')} {d['title']}",
            callback_data=f"sc:{start + i}",
        )]
        for i, d in enumerate(page_items)
    ]

    # Pagination row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pg:{page-1}:{query}"))
    if start + PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pg:{page+1}:{query}"))
    if nav:
        buttons.append(nav)

    header = (
        f"📋 **Select a chat** — {total} found"
        + (f" for `{query}`" if query else "")
        + f"\n_Page {page+1}/{total_pages} · or type a name to filter_\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    return header, InlineKeyboardMarkup(buttons)


async def _show_dialog_list(target, uid: int, page: int = 0, query: str = "") -> None:
    """Send (or edit) the dialog list. target can be a Message or CallbackQuery."""
    if not dialogs_cache:
        text = "⚠️ No dialogs loaded yet. Please wait a moment and try again."
        if hasattr(target, "reply"):
            await target.reply(text)
        else:
            await target.message.edit(text)
        return

    text, markup = _dialog_list_markup(uid, page, query)
    if hasattr(target, "reply"):          # Message — send new
        await target.reply(text, reply_markup=markup)
    else:                                  # CallbackQuery — edit existing
        await target.message.edit(text, reply_markup=markup)


async def cmd_start(_, message: Message):
    global downloaded_ids, _current_job, _worker_task, _job_queue

    # Kill any running job and clear the queue
    if _worker_task and not _worker_task.done():
        if _current_job:
            _current_job["cancelled"] = True
        _worker_task.cancel()
        _worker_task = None
        _current_job = None
    _job_queue.clear()

    downloaded_ids = set()
    uid = message.from_user.id
    user_state[uid] = "selecting"

    prev = (
        f"\n\n_Previous chat: **{selected_chat['title']}**_"
        if selected_chat.get("id") else ""
    )
    await message.reply(
        "👋 **GhostFetch** — session reset." + prev,
        disable_web_page_preview=True,
    )
    await _show_dialog_list(message, uid)


async def cmd_setchat(_, message: Message):
    uid = message.from_user.id
    user_state[uid] = "selecting"
    await _show_dialog_list(message, uid)


async def cmd_help(_, message: Message):
    await message.reply(
        "📖 **GhostFetch — Help**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Getting started**\n"
        "1. Send /start — pick a chat from the list\n"
        "2. Send one or more message IDs to download\n"
        "3. The bot downloads them and saves to your Termux storage\n\n"
        "**Commands**\n"
        "/start — reset session & pick a new chat\n"
        "/setchat — change the active chat without resetting\n"
        "/setpath — change the download folder\n"
        "/killall — cancel the running download & clear queue\n"
        "/stats — show disk, CPU, RAM & network usage\n"
        "/log — send the current session log file\n"
        "/help — show this message\n\n"
        "**Sending IDs**\n"
        "• Single ID: `26473`\n"
        "• Multiple IDs: `26473 26570 26600`\n"
        "• While a download is running, new IDs are **queued** automatically\n\n"
        "**Queue**\n"
        "• Each batch of IDs you send becomes one job\n"
        "• Jobs run one after another, automatically\n"
        "• /killall cancels the current job and clears the entire queue\n\n"
        "**Chat selection**\n"
        "• After /start or /setchat, a full list of your chats appears\n"
        "• Use ⬅️ / ➡️ to page through them\n"
        "• Or just type part of a name to filter the list\n\n"
        "**Files are saved to:**\n"
        f"`{DOWNLOAD_BASE}/<chat_id>/`",
        disable_web_page_preview=True,
    )


async def cmd_setpath(_, message: Message):
    uid = message.from_user.id
    user_setpath_state.pop(uid, None)
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 Internal Downloads", callback_data="path:internal")],
        [InlineKeyboardButton("🖥 Termux Home",        callback_data="path:termux")],
        [InlineKeyboardButton("✏️ Custom path",        callback_data="path:custom")],
    ])
    await message.reply(
        "📁 **Choose a download location:**\n\n"
        f"📱 Internal Downloads:\n`{PATH_PRESETS['internal']}`\n\n"
        f"🖥 Termux Home:\n`{PATH_PRESETS['termux']}`\n\n"
        f"_Current: `{DOWNLOAD_BASE}`_",
        reply_markup=markup,
    )


async def cb_setpath(_, query: CallbackQuery):
    uid    = query.from_user.id
    choice = query.data.split(":")[1]

    if choice == "custom":
        user_setpath_state[uid] = "waiting_custom"
        await query.message.edit(
            "✏️ **Send me the full path you want to use.**\n\n"
            "_Example: `/sdcard/MyFolder/GhostFetch`_"
        )
        await query.answer()
        return

    path = PATH_PRESETS[choice]
    _save_path_config(path)
    os.makedirs(path, exist_ok=True)
    await query.message.edit(
        f"✅ **Download path set!**\n\n`{path}`"
    )
    await query.answer("✅ Path updated!")
    log.info(f"Download path set to: {path}")


async def cmd_killall(_, message: Message):
    global _worker_task, _current_job, _job_queue
    if _worker_task and not _worker_task.done():
        if _current_job:
            _current_job["cancelled"] = True
        _worker_task.cancel()
        _worker_task = None
        _current_job = None
        _job_queue.clear()
        await message.reply("🛑 **Download killed and queue cleared.**")
    else:
        await message.reply("ℹ️ Nothing is currently running.")


async def cmd_log(_, message: Message):
    if os.path.exists(LOG_FILE):
        await message.reply_document(
            LOG_FILE,
            caption=f"📋 **Session Log** · `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        )
    else:
        await message.reply("⚠️ No log file found.")


async def cmd_stats(_, message: Message):
    dl_root = DOWNLOAD_BASE if os.path.exists(DOWNLOAD_BASE) else "."
    total, used, free = shutil.disk_usage(dl_root)
    net  = psutil.net_io_counters()
    proc = psutil.Process(os.getpid())
    cpu  = psutil.cpu_percent(interval=0.5)
    ram  = psutil.virtual_memory().percent
    disk = psutil.disk_usage(dl_root).percent   # "/" is inaccessible on Termux

    chat_line = (
        f"`{selected_chat['title']}` · `{selected_chat['id']}`"
        if selected_chat.get("id") else "not set"
    )
    job_line = (
        f"`{len(_current_job['ids'])} IDs` in progress"
        if _current_job else "idle"
    )

    await message.reply(
        "**≧◉◡◉≦ Bot Stats**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ **Uptime:** `{_elapsed(BOT_START_TIME)}`\n"
        f"📂 **Active Chat:** {chat_line}\n"
        f"⚙️ **Status:** {job_line}\n"
        f"📥 **Downloaded (session):** `{len(downloaded_ids)}` IDs\n\n"
        f"💾 **Disk** — `{_sz(total)}` total · `{_sz(used)}` used · `{_sz(free)}` free\n"
        f"📡 **Network** — ↑ `{_sz(net.bytes_sent)}` · ↓ `{_sz(net.bytes_recv)}`\n"
        f"🖥 **CPU:** `{cpu}%` · **RAM:** `{ram}%` · **Disk:** `{disk}%`\n"
        f"🧠 **Bot RAM:** `{round(proc.memory_info().rss / 1024 ** 2)} MiB`"
    )


# ═══════════════════════════════════════════════════════════════
# Main text handler — chat search query OR message IDs
# ═══════════════════════════════════════════════════════════════

async def handle_text(_, message: Message):
    global _current_job, _worker_task, _job_queue

    uid   = message.from_user.id
    text  = message.text.strip()
    state = user_state.get(uid, "idle")

    # ── State: waiting for custom path ────────────────────────
    if user_setpath_state.get(uid) == "waiting_custom":
        user_setpath_state.pop(uid)
        if not text.startswith("/"):
            return await message.reply("⚠️ Path must start with `/`. Try again with /setpath.")
        _save_path_config(text)
        os.makedirs(text, exist_ok=True)
        return await message.reply(f"✅ **Download path set!**\n\n`{text}`")

    # ── State: selecting a chat — filter the list ──────────────
    if state == "selecting":
        await _show_dialog_list(message, uid, page=0, query=text)
        return

    # ── State: idle — expect message IDs ──────────────────────
    if not selected_chat.get("id"):
        return await message.reply(
            "⚠️ **No chat selected.**\n\n"
            "Use /start or /setchat first."
        )

    # Parse space-separated integers
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
            f"❌ Invalid token(s): `{' '.join(bad)}`\n\n"
            "Send message IDs (numbers only), separated by spaces.\n"
            "_Example: `26473` or `26473 26570 26600`_"
        )
    if not msg_ids:
        return await message.reply(
            "Send at least one message ID.\n"
            "_Example: `26473` or `26473 26570 26600`_"
        )

    entries = [{"id": mid, "status": "queued"} for mid in msg_ids]

    # ── Queue if a job is already running ─────────────────────
    if _current_job or (_worker_task and not _worker_task.done()):
        pos = len(_job_queue) + 1
        progress_msg = await message.reply(
            f"📋 **Queued** (position {pos}) · {len(msg_ids)} ID(s)\n"
            f"📂 `{selected_chat['title']}`\n"
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
        log.info(f"Job queued (pos {pos}): {msg_ids}")
        return

    # ── No job running — start immediately ────────────────────
    progress_msg = await message.reply(
        f"📥 **Starting…** {len(msg_ids)} ID(s)\n"
        f"📂 `{selected_chat['title']}`"
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


# ═══════════════════════════════════════════════════════════════
# Callback — chat selection buttons
# ═══════════════════════════════════════════════════════════════

async def cb_select_chat(_, query: CallbackQuery):
    global selected_chat

    uid  = query.from_user.id
    idx  = int(query.data.split(":")[1])
    pool = search_results.get(uid, [])

    if idx >= len(pool):
        return await query.answer("Selection expired, search again.", show_alert=True)

    chosen = pool[idx]
    selected_chat = {"id": chosen["id"], "title": chosen["title"]}
    _save_session()

    user_state[uid] = "idle"
    search_results.pop(uid, None)

    await query.message.edit(
        f"✅ **Chat set!**\n\n"
        f"📂 **{chosen['title']}**\n"
        f"🆔 `{chosen['id']}`  ·  type: `{chosen['type']}`\n\n"
        "Ready. Send me message IDs.\n"
        "_Example: `26473` or `26473 26570 26600`_"
    )
    await query.answer("✅ Chat selected!")
    log.info(f"Chat selected: {chosen['title']} ({chosen['id']})")


async def cb_page(_, query: CallbackQuery):
    """Handle ⬅️ / ➡️ pagination buttons."""
    uid   = query.from_user.id
    parts = query.data.split(":", 2)          # pg:<page>:<query>
    page  = int(parts[1])
    q     = parts[2] if len(parts) > 2 else ""
    await _show_dialog_list(query, uid, page=page, query=q)
    await query.answer()


# ═══════════════════════════════════════════════════════════════
# Startup & entry point
# ═══════════════════════════════════════════════════════════════

async def _initialize() -> None:
    _load_session()
    _load_path()
    os.makedirs(DOWNLOAD_BASE, exist_ok=True)
    log.info("Starting user client…")
    await user_client.start()
    await _load_dialogs()
    log.info("Initialization complete.")


async def main() -> None:
    global bot, user_client

    # Instantiate clients here so they bind to the running event loop.
    # Creating them at module level causes "Future attached to a different loop"
    # on Python 3.10+ because Pyrogram grabs the loop at construction time.
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

    # Register handlers now that bot exists
    bot.add_handler(MessageHandler(cmd_start,   filters.command("start")   & filters.private))
    bot.add_handler(MessageHandler(cmd_setchat, filters.command("setchat") & filters.private))
    bot.add_handler(MessageHandler(cmd_help,    filters.command("help")    & filters.private))
    bot.add_handler(MessageHandler(cmd_killall, filters.command("killall") & filters.private))
    bot.add_handler(MessageHandler(cmd_log,     filters.command("log")     & filters.private))
    bot.add_handler(MessageHandler(cmd_stats,   filters.command("stats")   & filters.private))
    bot.add_handler(MessageHandler(cmd_setpath, filters.command("setpath") & filters.private))
    bot.add_handler(MessageHandler(
        handle_text,
        filters.private & filters.text
        & ~filters.command(["start", "setchat", "help", "killall", "log", "stats", "setpath"]),
    ))
    bot.add_handler(CallbackQueryHandler(cb_select_chat, filters.regex(r"^sc:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_page,        filters.regex(r"^pg:\d+")))
    bot.add_handler(CallbackQueryHandler(cb_setpath,     filters.regex(r"^path:")))

    await _initialize()
    await bot.start()
    log.info("Bot is online.")
    try:
        await asyncio.Event().wait()   # run forever
    finally:
        await bot.stop()
        await user_client.stop()
        log.info("Bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
