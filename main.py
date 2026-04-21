# GhostFetch — Telegram restricted media downloader
# herraChron
# Updated: MOD-01 through MOD-45 + FIX-01–06, PERF-02/06/07, SEC-01/03/04, ARCH-02/03, UX-01–10

import os
import json
import shutil
import psutil
import asyncio
import hashlib
import logging
from collections import deque
from difflib import SequenceMatcher
from logging.handlers import RotatingFileHandler
from time import time
from datetime import datetime
from uuid import uuid4

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid, BadRequest, MessageNotModified
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from config import PyroConf


# ───────────────────────────────────────────────────────────────
# Constants
# ───────────────────────────────────────────────────────────────

DOWNLOAD_BASE        = os.path.join(os.path.expanduser("~"), "GhostFetch", "downloads")
SESSION_FILE         = "session.json"
CHAT_NAMES_FILE      = "chat_names.json"
DOWNLOADED_IDS_FILE  = "downloaded_ids.json"
FILE_HASHES_FILE     = "file_hashes.json"         # MOD-34
LEDGER_FILE          = "ledger.json"              # MOD-36
AUDIT_FILE           = "audit.log"               # MOD-35
JOB_HISTORY_FILE     = "job_history.json"         # MOD-39
STATE_HASH_FILE      = "state.hash"              # MOD-41
LOG_FILE             = "session.log"
BOT_START_TIME       = time()

MAX_LOG_SIZE         = 5 * 1024 * 1024
RETRY_ATTEMPTS       = 3
PROGRESS_INTERVAL    = 2.0
PAGE_SIZE            = 8
FILES_PAGE_SIZE      = 5
SEARCH_LIMIT         = 5000
BULK_PRESORT_LIMIT   = 8000                       # MOD-29: max msgs buffered for size-sort
DISK_LOW_MB          = 500                        # MOD-40: pause threshold in MB
SPEED_ANOMALY_RATIO  = 0.3                        # MOD-38: alert if speed < 30% of average
FUZZY_THRESHOLD      = 0.35                       # MOD-43: minimum fuzzy score to show
DOT                  = "•"

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
# FIX-01: per-user selected_chat (was single global dict — multi-user safe)
_user_selected_chat: dict[int, dict] = {}
downloaded_ids: set              = set()
user_state:     dict             = {}
search_results: dict             = {}

# PERF-02: deque for O(1) popleft
_job_queue:     deque            = deque()
_current_job:   dict | None      = None
_worker_task:   asyncio.Task | None = None
_bulk_task:     asyncio.Task | None = None
_bulk_state:    dict             = {"cancelled": False}
_files_nav:     dict             = {}
_fname_results: dict             = {}

file_hashes:    set              = set()           # MOD-34
_adaptive_delay: dict[int, float] = {}             # ARCH-02: per-chat adaptive delay
_speed_samples:  deque           = deque(maxlen=30) # PERF-02
_job_history:    deque           = deque(maxlen=15) # PERF-02
_bulk_owner_uid: int | None      = None            # FIX-06
_disk_sentinel_task: asyncio.Task | None = None    # MOD-40
_cached_state_hash: str          = ""              # PERF-07


# ───────────────────────────────────────────────────────────────
# FIX-01: Per-user chat helpers
# ───────────────────────────────────────────────────────────────

def get_selected_chat(uid: int) -> dict:
    return _user_selected_chat.get(uid, {})


def set_selected_chat(uid: int, chat: dict) -> None:
    _user_selected_chat[uid] = chat


# ───────────────────────────────────────────────────────────────
# MOD-27: Authorization helper
# ───────────────────────────────────────────────────────────────

def _authorized(uid: int) -> bool:
    return not ALLOWED_USER_IDS or uid in ALLOWED_USER_IDS


# ───────────────────────────────────────────────────────────────
# Persistence — session / chat names / downloaded IDs
# ───────────────────────────────────────────────────────────────

def _load_session() -> None:
    """Load last-used session into a sentinel uid=0 slot (startup only)."""
    global _user_selected_chat
    try:
        with open(SESSION_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and data.get("id"):
            # Store under uid=0 as a fallback; overwritten when a real user picks a chat
            _user_selected_chat[0] = data
            log.info(f"Session loaded: {data['title']}")
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning(f"Session load failed: {e}")


def _save_session(uid: int) -> None:
    chat = get_selected_chat(uid)
    if not chat:
        return
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(chat, f, indent=2)
        _save_state_hash()                         # MOD-41
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
    downloaded_ids.add(msg_id)
    try:
        with open(DOWNLOADED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(downloaded_ids), f)
        _save_state_hash()                         # MOD-41
    except Exception as e:
        log.warning(f"downloaded_ids save failed: {e}")


def _clear_downloaded_ids() -> None:
    global downloaded_ids
    downloaded_ids = set()
    try:
        with open(DOWNLOADED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)
        _save_state_hash()
    except Exception as e:
        log.warning(f"downloaded_ids clear failed: {e}")


# ── MOD-34: File hash deduplication ──────────────────────────

def _load_file_hashes() -> None:
    global file_hashes
    try:
        with open(FILE_HASHES_FILE, encoding="utf-8") as f:
            data = json.load(f)
        file_hashes = set(data) if isinstance(data, list) else set()
        log.info(f"Loaded {len(file_hashes)} file hashes from disk")
    except FileNotFoundError:
        file_hashes = set()
    except Exception as e:
        log.warning(f"file_hashes load failed: {e}")


def _file_hash(path: str) -> str | None:
    """Blake2b hash of file contents (SEC-01: replaces MD5)."""
    try:
        h = hashlib.blake2b(digest_size=32)
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _register_file_hash(path: str) -> bool:
    """Register file hash. Returns True if new (keep), False if duplicate (delete)."""
    fh = _file_hash(path)
    if not fh:
        return True
    if fh in file_hashes:
        log.info(f"Hash duplicate detected: {os.path.basename(path)}")
        return False
    file_hashes.add(fh)
    try:
        with open(FILE_HASHES_FILE, "w", encoding="utf-8") as f:
            json.dump(list(file_hashes), f)
    except Exception as e:
        log.warning(f"file_hashes save failed: {e}")
    return True


# ── MOD-39: ETA calibration ──────────────────────────────────

def _load_job_history() -> None:
    global _job_history
    try:
        with open(JOB_HISTORY_FILE, encoding="utf-8") as f:
            _job_history = json.load(f)
        log.info(f"Loaded {len(_job_history)} job history entries")
    except FileNotFoundError:
        _job_history = []
    except Exception as e:
        log.warning(f"job_history load failed: {e}")


def _save_job_record(bytes_total: float, duration: float) -> None:
    _job_history.append({"bytes": bytes_total, "duration": duration, "ts": time()})
    # deque(maxlen=15) handles eviction automatically
    try:
        with open(JOB_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(list(_job_history), f)
    except Exception as e:
        log.warning(f"job_history save failed: {e}")


def _calibrated_eta(remaining_bytes: float, current_bps: float) -> int:
    """ETA in seconds, calibrated via EWMA (ARCH-03)."""
    if current_bps <= 0 or remaining_bytes <= 0:
        return 0
    raw_eta = remaining_bytes / current_bps
    if len(_job_history) < 3:
        return int(raw_eta)
    ratios = []
    for h in _job_history:
        if h.get("bytes", 0) > 0 and h.get("duration", 0) > 0 and current_bps > 0:
            predicted = h["bytes"] / current_bps
            if predicted > 0:
                ratios.append(h["duration"] / predicted)
    if not ratios:
        return int(raw_eta)
    # Exponentially weighted moving average (recent samples weigh more)
    alpha = 0.3
    ewma_ratio = ratios[0]
    for r in ratios[1:]:
        ewma_ratio = alpha * r + (1 - alpha) * ewma_ratio
    return int(raw_eta * max(0.5, min(ewma_ratio, 3.0)))


# ── MOD-41: State fingerprint ─────────────────────────────────

def _compute_state_hash() -> str:
    """SEC-03: covers all persistent state files (not just session + downloaded_ids)."""
    h = hashlib.sha256()
    for fname in [SESSION_FILE, DOWNLOADED_IDS_FILE, FILE_HASHES_FILE,
                  LEDGER_FILE, JOB_HISTORY_FILE]:
        try:
            with open(fname, "rb") as f:
                h.update(f.read())
        except FileNotFoundError:
            pass
    return h.hexdigest()[:20]


def _check_state_integrity() -> bool:
    """Returns False if state files look tampered since last save. PERF-07: uses in-memory cache."""
    global _cached_state_hash
    try:
        if _cached_state_hash:
            # Fast path: compare against in-memory cache
            current = _compute_state_hash()
            if _cached_state_hash != current:
                log.warning(f"State fingerprint mismatch (cached vs current={current})")
                return False
            return True
        # Startup path: read from disk
        with open(STATE_HASH_FILE) as f:
            stored = f.read().strip()
        current = _compute_state_hash()
        if stored and stored != current:
            log.warning(f"State fingerprint mismatch (stored={stored}, current={current})")
            return False
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning(f"State integrity check failed: {e}")
    return True


def _save_state_hash() -> None:
    global _cached_state_hash
    try:
        h = _compute_state_hash()
        with open(STATE_HASH_FILE, "w") as f:
            f.write(h)
        _cached_state_hash = h               # PERF-07: update in-memory cache
    except Exception as e:
        log.warning(f"State hash save failed: {e}")


# ── MOD-35: Audit trail ───────────────────────────────────────

def _audit_log(action: str, details: dict) -> None:
    """Append a tamper-evident entry to the audit log (SHA-256 chained)."""
    try:
        prev_hash = "0" * 64
        if os.path.exists(AUDIT_FILE):
            with open(AUDIT_FILE, "rb") as f:
                prev_hash = hashlib.sha256(f.read()).hexdigest()
        entry = {
            "ts":      datetime.utcnow().isoformat(),
            "action":  action,
            "details": details,
            "prev":    prev_hash,
        }
        raw = json.dumps(entry, ensure_ascii=False)
        entry["hash"] = hashlib.sha256((prev_hash + raw).encode()).hexdigest()
        with open(AUDIT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning(f"Audit log failed: {e}")


# ── MOD-36: Per-chat ledger ───────────────────────────────────

def _ledger_load() -> dict:
    try:
        with open(LEDGER_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _ledger_update(folder_id: str, file_count: int, total_bytes: int,
                   success: int, failed: int) -> None:
    ledger = _ledger_load()
    key = str(folder_id).replace("-100", "")
    e = ledger.get(key, {"downloads": 0, "bytes": 0,
                         "success": 0, "failed": 0,
                         "title": _folder_title(key),
                         "first": None, "last": None})
    e["downloads"] += file_count
    e["bytes"]     += total_bytes
    e["success"]   += success
    e["failed"]    += failed
    e["title"]      = _folder_title(key)
    now = datetime.utcnow().isoformat()
    if not e["first"]:
        e["first"] = now
    e["last"] = now
    ledger[key] = e
    try:
        with open(LEDGER_FILE, "w", encoding="utf-8") as f:
            json.dump(ledger, f, indent=2)
    except Exception as ex:
        log.warning(f"Ledger save failed: {ex}")


def _ledger_top(n: int = 3) -> str:
    """Return a formatted string of top-N most downloaded chats."""
    ledger = _ledger_load()
    if not ledger:
        return "No history yet."
    top = sorted(ledger.values(), key=lambda x: x.get("bytes", 0), reverse=True)[:n]
    lines = []
    for e in top:
        title = _trunc(e.get("title", "?"), 20)
        lines.append(
            f"• {title}  {e.get('success', 0)}✓  {_sz(e.get('bytes', 0))}"
        )
    return "\n".join(lines)


# ───────────────────────────────────────────────────────────────
# Formatting helpers
# ───────────────────────────────────────────────────────────────

def _sz(b: int | float) -> str:
    if b == 0:
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
    if n <= 0:
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
# MOD-33: Magic-byte validation
# ───────────────────────────────────────────────────────────────

_MAGIC_MAP: list[tuple[bytes, int, bytes, str]] = [
    # (magic, offset, extra_check_at_8, extension)
    (b'\xff\xd8\xff',      0, b'',         '.jpg'),
    (b'\x89PNG\r\n\x1a\n', 0, b'',         '.png'),
    (b'GIF8',              0, b'',         '.gif'),
    (b'%PDF',              0, b'',         '.pdf'),
    (b'\x1a\x45\xdf\xa3', 0, b'',         '.mkv'),
    (b'ID3',               0, b'',         '.mp3'),
    (b'fLaC',              0, b'',         '.flac'),
    (b'OggS',              0, b'',         '.ogg'),
    (b'\x1f\x8b',          0, b'',         '.gz'),
    (b'Rar!\x1a\x07',      0, b'',         '.rar'),
    (b'7z\xbc\xaf',        0, b'',         '.7z'),
    (b'PK\x03\x04',        0, b'',         '.zip'),  # zip/docx/apk — skip rename
]


def _detect_extension(path: str) -> str | None:
    """Return the correct extension from magic bytes, or None if unknown."""
    try:
        with open(path, "rb") as f:
            header = f.read(16)
        # MP4: 'ftyp' box at offset 4
        if len(header) >= 8 and header[4:8] == b'ftyp':
            return '.mp4'
        # WEBP: RIFF????WEBP
        if header[:4] == b'RIFF' and len(header) >= 12 and header[8:12] == b'WEBP':
            return '.webp'
        for magic, offset, _, ext in _MAGIC_MAP:
            if header[offset:offset + len(magic)] == magic:
                return ext
    except Exception:
        pass
    return None


def _validate_and_rename(path: str) -> str:
    """Check magic bytes, rename if extension is wrong. Returns final path."""
    detected = _detect_extension(path)
    if not detected:
        return path
    current_ext = os.path.splitext(path)[1].lower()
    # Zip family (docx/xlsx/apk) — don't rename, they're all PK
    if detected == '.zip':
        return path
    if current_ext == detected:
        return path
    new_path = os.path.splitext(path)[0] + detected
    if not os.path.exists(new_path):
        try:
            os.rename(path, new_path)
            log.info(f"Magic rename: {os.path.basename(path)} → {os.path.basename(new_path)}")
            return new_path
        except Exception as e:
            log.warning(f"Magic rename failed: {e}")
    return path


# ───────────────────────────────────────────────────────────────
# MOD-37: Adaptive FloodWait throttle
# ───────────────────────────────────────────────────────────────

def _floodwait_hit(wait_seconds: float, chat_id: int = 0) -> None:
    cur = _adaptive_delay.get(chat_id, 0.2)
    _adaptive_delay[chat_id] = min(cur * 1.6 + 0.1, 3.0)
    log.info(f"FloodWait {wait_seconds:.0f}s → adaptive_delay[{chat_id}]={_adaptive_delay[chat_id]:.2f}s")


def _download_success(chat_id: int = 0) -> None:
    cur = _adaptive_delay.get(chat_id, 0.2)
    _adaptive_delay[chat_id] = max(cur * 0.93, 0.2)


def _get_adaptive_delay(chat_id: int = 0) -> float:
    return _adaptive_delay.get(chat_id, 0.2)


# ───────────────────────────────────────────────────────────────
# MOD-38: Speed anomaly detection
# ───────────────────────────────────────────────────────────────

def _record_speed(bps: float) -> None:
    if bps > 0:
        _speed_samples.append(bps)   # deque(maxlen=30) auto-evicts oldest


def _check_speed_anomaly(current_bps: float) -> bool:
    """Returns True if speed has dropped significantly below the session average."""
    if len(_speed_samples) < 6 or current_bps <= 0:
        return False
    avg = sum(_speed_samples) / len(_speed_samples)
    return avg > 0 and (current_bps < avg * SPEED_ANOMALY_RATIO)


# ───────────────────────────────────────────────────────────────
# MOD-42: Chronological gap reporter
# ───────────────────────────────────────────────────────────────

def _gap_report(msg_ids: list[int]) -> str:
    """Analyse a list of message IDs and return a deleted-message estimate."""
    if len(msg_ids) < 2:
        return ""
    sorted_ids = sorted(msg_ids)
    gap_count  = sum(1 for a, b in zip(sorted_ids, sorted_ids[1:]) if b - a > 1)
    gap_msgs   = sum(b - a - 1 for a, b in zip(sorted_ids, sorted_ids[1:]) if b - a > 1)
    if gap_msgs <= 0:
        return ""
    return f"\n~{gap_msgs} likely deleted msg(s) in {gap_count} gap(s)"


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
        return f"○ {mid} · {_friendly_error(e.get('reason', ''))}"   # UX-04
    if e["status"] == "failed":
        return f"✗ {mid} · {_friendly_error(e.get('reason', ''))}"   # UX-04
    return f"· {mid}"


def _get_speed(job: dict) -> float:
    """Rolling 5-second window speed in bytes/sec."""
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
        _record_speed(speed)                       # MOD-38
        return speed
    return max(job.get("_rolling_speed", 0.0), 0.0)


def _render_job(job: dict) -> str:
    entries = job["entries"]
    visible = entries[-5:]
    hidden  = len(entries) - len(visible)
    prefix  = f"+{hidden} more completed…\n" if hidden else ""
    rows    = prefix + "\n".join(_render_entry(e) for e in visible)

    speed       = _get_speed(job)
    done_bytes  = sum(e.get("size", 0) for e in entries if e["status"] == "done")
    total_bytes = job.get("total_bytes", 0)
    remaining   = max(total_bytes - done_bytes, 0)
    if speed > 0 and remaining > 0:
        eta_s   = _calibrated_eta(remaining, speed)   # MOD-39
        eta_str = f"  ·  ETA {_fmt_eta(eta_s)}"
    else:
        eta_str = ""

    # UX-05: show queue depth
    q_note = f"\n📋 {len(_job_queue)} job(s) waiting" if _job_queue else ""

    header = f"📥 **{_trunc(job['chat_title'], 28)}**"
    return _truncate_msg(
        f"{header}\n"
        f"{'━' * 22}\n"
        f"{rows}\n\n"
        f"⏱ {_elapsed(job['start_time'])} · {_speed(speed)}{eta_str}"
        f"{q_note}"
    )


def _render_job_summary(job: dict) -> str:
    done     = sum(1 for e in job["entries"] if e["status"] == "done")
    skipped  = sum(1 for e in job["entries"] if e["status"] == "skipped")
    failed   = sum(1 for e in job["entries"] if e["status"] == "failed")
    total_sz = sum(e.get("size", 0) for e in job["entries"] if e["status"] == "done")
    # UX-09: user-first language
    return (
        f"✓ **{_trunc(job['chat_title'], 28)}**\n"
        f"{'━' * 22}\n"
        f"{done} file(s) saved · {skipped} skipped · {_sz(total_sz)} total\n"
        f"⏱ {_elapsed(job['start_time'])}"
    )


async def _edit(msg, text: str, reply_markup=None) -> None:
    """FIX-02: iterative FloodWait retry (was recursive — could cause RecursionError)."""
    if not msg:
        return
    for attempt in range(3):
        try:
            await msg.edit(_truncate_msg(text), reply_markup=reply_markup)
            return
        except MessageNotModified:
            return
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.debug(f"Edit failed: {e}")
            return


# ───────────────────────────────────────────────────────────────
# Markup builders
# ───────────────────────────────────────────────────────────────

def _job_controls_markup(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel Job", callback_data=f"cb_job_cancel:{job_id}")],
    ])


def _bulk_controls_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel Download", callback_data="bulk_ctrl:cancel")],
    ])


def _home_markup() -> InlineKeyboardMarkup:
    """MOD-NAV: Universal bottom-of-message home navigation."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 Home", callback_data="home:"),
    ]])


def _home_files_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 Home",   callback_data="home:"),
        InlineKeyboardButton("📁 Files", callback_data="home:files"),
    ]])


# ───────────────────────────────────────────────────────────────
# Dialog cache
# ───────────────────────────────────────────────────────────────

async def _load_dialogs() -> None:
    """FIX-03: iterative FloodWait retry (was recursive — could cause RecursionError)."""
    global dialogs_cache
    log.info("Loading dialogs…")
    for attempt in range(3):
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
            return
        except FloodWait as e:
            log.warning(f"FloodWait loading dialogs ({e.value}s), attempt {attempt+1}/3")
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.error(f"Dialog load failed: {e}")
            return


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
# MOD-43: Fuzzy chat search
# ───────────────────────────────────────────────────────────────

def _fuzzy_score(query: str, title: str) -> float:
    q, t = query.lower(), title.lower()
    if q in t:
        return 1.0                              # exact substring → highest priority
    return SequenceMatcher(None, q, t).ratio()


# UX-04: Plain-English error translations
ERROR_MESSAGES = {
    "PeerIdInvalid": "This chat is no longer accessible.",
    "BadRequest":    "Telegram rejected the request.",
    "FloodWait":     "Telegram is rate limiting. Retrying automatically.",
    "Cancelled":     "Download was cancelled.",
    "Not found":     "This message no longer exists.",
    "No media":      "This message has no downloadable file.",
    "Duplicate":     "Already downloaded (duplicate file).",
    "Path error":    "Could not create the download folder.",
}


def _friendly_error(reason: str) -> str:
    for key, msg in ERROR_MESSAGES.items():
        if key.lower() in reason.lower():
            return msg
    return reason


# ───────────────────────────────────────────────────────────────
# Core download  (MOD-33/34/35/37 integrated)
# ───────────────────────────────────────────────────────────────

async def _download_file(
    msg, chat_id, job: dict, entry: dict, index: int, total: int, chat: dict = None
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
        # FIX-04: recompute total_bytes from scratch instead of delta math
        entry["_total_bytes"] = total_bytes
        job["total_bytes"] = sum(e.get("_total_bytes", 0) for e in job["entries"])
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

    if not os.path.exists(path):
        return None, 0

    # MOD-33: validate magic bytes, rename if needed
    final_path = _validate_and_rename(path)
    fname      = os.path.basename(final_path)
    size       = os.path.getsize(final_path)

    # MOD-34: deduplicate by file hash
    if not _register_file_hash(final_path):
        os.remove(final_path)
        entry["status"] = "skipped"
        entry["reason"] = "Duplicate"
        return None, 0

    # MOD-35: audit trail (FIX-01: use passed chat dict, not global selected_chat)
    chat_title = (chat or {}).get("title", str(chat_id))
    _audit_log("download", {
        "chat": chat_title,
        "file": fname,
        "size": size,
    })

    log.info(f"Saved: {fname} ({_sz(size)})")
    return fname, size


async def _process_id(msg_id: int, job: dict, entry: dict, chat: dict) -> None:
    """FIX-01: uses passed chat dict. FIX-05: wraps download in retry loop."""
    chat_id = chat["id"]
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
            _floodwait_hit(e.value, chat_id)       # ARCH-02: per-chat delay
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
                # FIX-05: retry download on transient errors
                for dl_attempt in range(RETRY_ATTEMPTS):
                    try:
                        fname, sz = await _download_file(m, chat_id, job, entry, i, len(media), chat)
                        break
                    except asyncio.CancelledError:
                        raise
                    except FloodWait as e:
                        _floodwait_hit(e.value, chat_id)
                        await asyncio.sleep(e.value + 1)
                    except Exception as e:
                        if dl_attempt == RETRY_ATTEMPTS - 1:
                            fname, sz = None, 0
                            log.error(f"Download retry exhausted {msg_id}: {e}")
                if fname:
                    total_size += sz
                    last_name   = fname
            entry["name"] = f"{len(media)} files" if len(media) > 1 else last_name
            entry["size"] = total_size

        elif msg.media:
            # FIX-05: retry download on transient errors
            fname, sz = None, 0
            for dl_attempt in range(RETRY_ATTEMPTS):
                try:
                    fname, sz = await _download_file(msg, chat_id, job, entry, 0, 1, chat)
                    break
                except asyncio.CancelledError:
                    raise
                except FloodWait as e:
                    _floodwait_hit(e.value, chat_id)
                    await asyncio.sleep(e.value + 1)
                except Exception as e:
                    if dl_attempt == RETRY_ATTEMPTS - 1:
                        log.error(f"Download retry exhausted {msg_id}: {e}")
            if not fname:
                return
            entry["name"] = fname
            entry["size"] = sz

        else:
            entry["status"] = "skipped"
            entry["reason"] = "No media"
            return

        _persist_downloaded_id(msg_id)
        entry["status"] = "done"
        _download_success(chat_id)                 # ARCH-02: per-chat delay

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
        chat = j.get("chat", {})                   # FIX-01: chat stored in job at creation
        folder_id = str(chat.get("id", "")).replace("-100", "")
        if folder_id and j.get("chat_title"):
            _save_chat_name(folder_id, j["chat_title"])
        try:
            for entry in j["entries"]:
                if j.get("cancelled"):
                    entry["status"] = "failed"
                    entry["reason"] = "Cancelled"
                else:
                    await _process_id(entry["id"], j, entry, chat)   # FIX-01

                markup = _job_controls_markup(j["job_id"])
                await _edit(j["progress_msg"], _render_job(j), reply_markup=markup)

                # MOD-38: speed anomaly alert — send to job uid (FIX-06)
                speed = _get_speed(j)
                if speed > 0 and _check_speed_anomaly(speed) and not j.get("_speed_alerted"):
                    j["_speed_alerted"] = True
                    alert_uid = j.get("uid") or _bulk_owner_uid
                    if alert_uid:
                        try:
                            await bot.send_message(
                                alert_uid,
                                f"⚠️ **Speed drop detected**\n"
                                f"Current: {_speed(speed)} — may be throttled.",
                            )
                        except Exception:
                            pass

                await asyncio.sleep(_get_adaptive_delay(chat.get("id", 0)))  # ARCH-02

        except asyncio.CancelledError:
            for e in j["entries"]:
                if e["status"] not in ("done", "skipped", "failed"):
                    e["status"] = "failed"
                    e["reason"] = "Cancelled"
            raise
        finally:
            folder_id = str(chat.get("id", "")).replace("-100", "")
            folder    = os.path.join(DOWNLOAD_BASE, folder_id)
            q_left    = len(_job_queue)
            q_note    = f"\n{q_left} more job(s) queued" if q_left else ""
            summary   = _render_job_summary(j) + f"\n{folder}" + q_note

            # MOD-36: update per-chat ledger
            done_n    = sum(1 for e in j["entries"] if e["status"] == "done")
            failed_n  = sum(1 for e in j["entries"] if e["status"] == "failed")
            done_sz   = sum(e.get("size", 0) for e in j["entries"] if e["status"] == "done")
            _ledger_update(folder_id, len(j["entries"]), done_sz, done_n, failed_n)

            # MOD-39: save job record for ETA calibration
            elapsed = time() - j["start_time"]
            if done_sz > 0 and elapsed > 5:
                _save_job_record(done_sz, elapsed)

            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("Open files", callback_data=f"openf:{folder_id}"),
                InlineKeyboardButton("🏠 Home",    callback_data="home:"),
            ]])
            await _edit(j["progress_msg"], summary, reply_markup=markup)
            log.info(f"Job done: {done_n}✓ failed={failed_n}")

    try:
        await _execute(job)
        while _job_queue:
            nxt = _job_queue.popleft()             # PERF-02: O(1)
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
# Bulk download  (MOD-29 pre-sort, MOD-42 gap, MOD-36 ledger)
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


def _meta_size(m) -> int:
    for attr in ("document", "video", "audio", "voice", "animation", "video_note"):
        obj = getattr(m, attr, None)
        if obj and getattr(obj, "file_size", None):
            return obj.file_size
    return 0


async def _run_bulk_job(uid: int, chat_id, chat_title: str, file_type: str, progress_msg) -> None:
    global _bulk_task, _bulk_state, _bulk_owner_uid

    _bulk_state     = {"cancelled": False}
    _bulk_owner_uid = uid                          # FIX-06

    scanned      = 0
    found        = 0
    done         = 0
    failed       = 0
    done_bytes   = 0
    total_bytes  = 0
    start_time   = time()
    last_edit    = [0.0]
    folder_id    = str(chat_id).replace("-100", "")
    _save_chat_name(folder_id, chat_title)

    all_msg_ids:  list[int] = []          # MOD-42: for gap detection
    pending_msgs: list      = []          # MOD-29: buffered for pre-sort

    # ── Progress helpers ──────────────────────────────────────

    async def _update_scan(final: bool = False):
        now = time()
        if not final and now - last_edit[0] < 2.5:
            return
        last_edit[0] = now
        markup = None if final else _bulk_controls_markup()
        try:
            await progress_msg.edit(
                f"🔍 **{file_type}** · Scanning\n"
                f"{'━' * 22}\n"
                f"Found {found} files to download · ({scanned} messages checked)\n"
                f"⏱ {_elapsed(start_time)}",
                reply_markup=markup,
            )
        except Exception:
            pass

    async def _update_dl(final: bool = False):
        now = time()
        if not final and now - last_edit[0] < 2.5:
            return
        last_edit[0] = now
        pct     = min(done_bytes / total_bytes * 100, 100) if total_bytes else 0
        bar_str = f"`[{_bar(pct)}] {pct:.0f}%  {_sz(done_bytes)}`\n"
        prefix  = "✓" if final else "↓"
        status  = "Complete" if final else "Downloading"
        markup  = None if final else _bulk_controls_markup()
        try:
            await progress_msg.edit(
                f"{prefix} **{file_type}** · {status}\n"
                f"{'━' * 22}\n"
                f"Done {done}/{found} · Failed {failed}\n\n"
                f"{bar_str}"
                f"⏱ {_elapsed(start_time)}",
                reply_markup=markup,
            )
        except Exception:
            pass

    # ── Phase 1: Scan ─────────────────────────────────────────

    try:
        async for msg in user_client.get_chat_history(chat_id):
            if asyncio.current_task().cancelled() or _bulk_state.get("cancelled"):
                raise asyncio.CancelledError()
            scanned += 1
            all_msg_ids.append(msg.id)             # MOD-42
            await _update_scan()
            if not _msg_matches_type(msg, file_type):
                continue
            found += 1
            total_bytes += _meta_size(msg)
            if msg.id in downloaded_ids:
                continue
            if len(pending_msgs) < BULK_PRESORT_LIMIT:
                pending_msgs.append(msg)
            else:
                # Overflow: download immediately in scan order
                try:
                    fname = _filename(msg)
                    path  = _save_path(chat_id, fname)
                    if path:
                        await msg.download(file_name=path)
                        path = _validate_and_rename(path)          # MOD-33
                        sz   = os.path.getsize(path) if os.path.exists(path) else 0
                        if not _register_file_hash(path):          # MOD-34
                            os.remove(path)
                        else:
                            _audit_log("bulk_download", {"chat": chat_title,
                                                          "file": os.path.basename(path),
                                                          "size": sz})
                            done_bytes += sz
                            done       += 1
                        _persist_downloaded_id(msg.id)
                        _download_success(chat_id)                 # ARCH-02
                except asyncio.CancelledError:
                    raise
                except FloodWait as e:
                    _floodwait_hit(e.value, chat_id)               # ARCH-02
                    await asyncio.sleep(e.value + 1)
                    failed += 1
                except Exception as e:
                    log.error(f"Bulk overflow error {msg.id}: {e}")
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
        log.error(f"Bulk scan error: {e}")
        try:
            await progress_msg.edit(f"✗ Bulk scan failed\n`{e}`")
        except Exception:
            pass
        return

    await _update_scan(final=True)

    # ── Phase 2: Sort by file size descending (MOD-29) ────────
    pending_msgs.sort(key=_meta_size, reverse=True)
    log.info(f"Bulk pre-sort: {len(pending_msgs)} msgs sorted by size desc")

    # ── Phase 3: Download ──────────────────────────────────────

    try:
        for msg in pending_msgs:
            if asyncio.current_task().cancelled() or _bulk_state.get("cancelled"):
                raise asyncio.CancelledError()
            await _update_dl()
            try:
                fname = _filename(msg)
                path  = _save_path(chat_id, fname)
                if not path:
                    failed += 1
                    continue
                await msg.download(file_name=path)
                path = _validate_and_rename(path)                  # MOD-33
                sz   = os.path.getsize(path) if os.path.exists(path) else 0
                if not _register_file_hash(path):                  # MOD-34
                    os.remove(path)
                    log.info(f"Bulk: duplicate removed {fname}")
                else:
                    _audit_log("bulk_download", {"chat": chat_title,
                                                  "file": os.path.basename(path),
                                                  "size": sz})
                    done_bytes += sz
                    done       += 1
                _persist_downloaded_id(msg.id)
                _download_success(chat_id)                        # ARCH-02: per-chat
                log.info(f"Bulk: {os.path.basename(path)} ({_sz(sz)})")

            except asyncio.CancelledError:
                raise
            except FloodWait as e:
                _floodwait_hit(e.value, chat_id)                   # ARCH-02: per-chat
                await asyncio.sleep(e.value + 1)
                try:
                    await msg.download(file_name=path)
                    path = _validate_and_rename(path)
                    sz   = os.path.getsize(path) if os.path.exists(path) else 0
                    if not _register_file_hash(path):
                        os.remove(path)
                    else:
                        done_bytes += sz
                        done       += 1
                    _persist_downloaded_id(msg.id)
                except Exception:
                    failed += 1
            except Exception as e:
                log.error(f"Bulk error {msg.id}: {e}")
                failed += 1

            await asyncio.sleep(_get_adaptive_delay(chat_id))     # ARCH-02: per-chat

    except asyncio.CancelledError:
        pct = min(done_bytes / total_bytes * 100, 100) if total_bytes else 0
        try:
            await progress_msg.edit(
                f"⛔ **{file_type}** · Cancelled\n"
                f"Done {done} · Failed {failed}\n"
                f"`[{_bar(pct)}] {pct:.0f}%` · {_sz(done_bytes)}\n"
                f"⏱ {_elapsed(start_time)}"
            )
        except Exception:
            pass
        return
    except Exception as e:
        log.error(f"Bulk download error: {e}")
        try:
            await progress_msg.edit(f"✗ Bulk download failed\n`{e}`")
        except Exception:
            pass
        return
    finally:
        _bulk_task  = None
        _bulk_state = {"cancelled": False}

    # MOD-42: gap report
    gap_note = _gap_report(all_msg_ids)

    # MOD-36: update ledger
    _ledger_update(folder_id, done + failed, done_bytes, done, failed)

    await _update_dl(final=True)
    pct = min(done_bytes / total_bytes * 100, 100) if total_bytes else 100
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("Open files", callback_data=f"openf:{folder_id}"),
        InlineKeyboardButton("🏠 Home",    callback_data="home:"),
    ]])
    try:
        await progress_msg.edit(
            f"✓ **{file_type}** · Complete\n"
            f"{'━' * 22}\n"
            f"Scanned {scanned} · Matched {found}\n"
            f"Done {done} · Failed {failed}\n\n"
            f"`[{_bar(pct)}] {pct:.0f}%`\n"
            f"{_sz(done_bytes)} · ⏱ {_elapsed(start_time)}"
            f"{gap_note}",                                         # MOD-42
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
# Dialog list UI  (MOD-43: fuzzy search)
# ───────────────────────────────────────────────────────────────

def _dialog_list_markup(uid: int, page: int = 0, query: str = "") -> tuple[str, InlineKeyboardMarkup]:
    current_chat = get_selected_chat(uid)          # FIX-01 + UX-08
    current_id   = current_chat.get("id")
    if query:
        # MOD-43: fuzzy scoring — exact substring always wins, then by ratio
        scored = [(d, _fuzzy_score(query, d["title"])) for d in dialogs_cache]
        pool   = [(d, s) for d, s in scored if s >= FUZZY_THRESHOLD]
        pool.sort(key=lambda x: x[1], reverse=True)
        pool = [d for d, _ in pool]
        if not pool:
            # Fall back to substring search if fuzzy finds nothing
            pool = [d for d in dialogs_cache if query.lower() in d["title"].lower()]
    else:
        pool = dialogs_cache

    search_results[uid] = pool
    total       = len(pool)
    start       = page * PAGE_SIZE
    page_items  = pool[start : start + PAGE_SIZE]
    total_pages = max(1, -(-total // PAGE_SIZE))

    TYPE_ICON = {"group": "👥", "supergroup": "👥", "channel": "📢", "bot": "🤖", "private": "👤"}
    buttons = [
        [InlineKeyboardButton(
            f"{'✅ ' if d['id'] == current_id else ''}{TYPE_ICON.get(d['type'], '💬')} {_trunc(d['title'], 28)}",
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
    buttons.append([InlineKeyboardButton("🏠 Home", callback_data="home:")])

    header = (
        f"**Select a Chat**\n"
        + (f"🔍 Fuzzy: `{query}`\n" if query else "")
        + f"{total} chat(s) · Page {page+1}/{total_pages}\n"
        + "Type a name to filter (fuzzy search supported)"
    )
    return header, InlineKeyboardMarkup(buttons)


async def _show_dialog_list(target, uid: int, page: int = 0, query: str = "") -> None:
    if not dialogs_cache:
        retry_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Retry",  callback_data="welcome:setchat"),
            InlineKeyboardButton("🏠 Home",   callback_data="home:"),
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
        [InlineKeyboardButton("Manual IDs",         callback_data="dlopt:manual")],
        [InlineKeyboardButton("Search by Filename", callback_data="dlopt:search")],
        [InlineKeyboardButton("Bulk Download",       callback_data="dlopt:bulk")],
        [InlineKeyboardButton("🏠 Home",             callback_data="home:")],
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
# Welcome screen  (MOD-NAV: expanded with all quick-access buttons)
# ───────────────────────────────────────────────────────────────

def _welcome_markup(uid: int = 0) -> InlineKeyboardMarkup:
    chat = get_selected_chat(uid)
    buttons = []
    if chat.get("id"):
        buttons.append([InlineKeyboardButton(
            f"▶  Resume  {_trunc(chat['title'], 22)}",
            callback_data="welcome:resume",
        )])
    buttons.append([
        InlineKeyboardButton("🔍 Select Chat",  callback_data="welcome:setchat"),
        InlineKeyboardButton("🔄 Refresh",       callback_data="welcome:refresh"),
    ])
    buttons.append([
        InlineKeyboardButton("📁 Files",  callback_data="welcome:files"),
        InlineKeyboardButton("📊 Stats",  callback_data="welcome:stats"),
    ])
    buttons.append([
        InlineKeyboardButton("📋 Queue",  callback_data="welcome:queue"),
        InlineKeyboardButton("📜 Log",    callback_data="welcome:log"),
    ])
    buttons.append([
        InlineKeyboardButton("🆕 New Session", callback_data="welcome:new_session"),
        InlineKeyboardButton("❓ Help",         callback_data="welcome:help"),
    ])
    return InlineKeyboardMarkup(buttons)


def _welcome_text(status: str, uid: int = 0) -> str:
    chat      = get_selected_chat(uid)
    integrity = "" if _check_state_integrity() else "  ⚠️ state modified\n"  # MOD-41
    # UX-10: show active chat
    if chat.get("id"):
        chat_line = f"📌 {_trunc(chat['title'], 30)}\n"
    else:
        chat_line = "⚠️ No chat selected — tap **Select Chat** to begin\n"
    return (
        f"**GhostFetch**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{chat_line}"
        f"**{len(dialogs_cache)}** chats · **{len(_job_queue)}** queued · {status}\n"
        f"{integrity}"
    )


# ───────────────────────────────────────────────────────────────
# Commands
# ───────────────────────────────────────────────────────────────

async def cmd_start(_, message: Message):
    global _current_job, _worker_task, _job_queue
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")

    # PERF-06: prune stale user_state entries older than 1 hour
    now = time()
    for k in list(user_state.keys()):
        if isinstance(user_state[k], dict) and now - user_state[k].get("ts", 0) > 3600:
            del user_state[k]

    status = "downloading" if _current_job else "idle"

    if _worker_task and not _worker_task.done():
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("Yes, reset", callback_data="confirm_reset:yes"),
            InlineKeyboardButton("No",          callback_data="confirm_reset:no"),
        ]])
        return await message.reply(
            "A download is currently running. Reset and discard it?",
            reply_markup=markup,
        )

    _job_queue.clear()
    user_state[uid] = "idle"

    await message.reply(
        _welcome_text(status, uid),
        reply_markup=_welcome_markup(uid),
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
    chat = get_selected_chat(uid)
    if not chat.get("id"):
        return await message.reply(
            "⚠️ No chat selected.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 Select Chat", callback_data="welcome:setchat"),
            ]]),
        )
    await message.reply(
        f"**{_trunc(chat['title'], 28)}**\nSelect a download mode:",
        reply_markup=_dl_action_markup(),
    )


async def cmd_help(_, message: Message):
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 Home", callback_data="home:"),
    ]])
    await message.reply(
        "**GhostFetch**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Commands**\n"
        "`/start` · Home screen\n"
        "`/setchat` · Pick a source chat\n"
        "`/options` · Download mode picker\n"
        "`/files` · Browse downloads\n"
        "`/queue` · View pending jobs\n"
        "`/killall` · Cancel everything\n"
        "`/stats` · System and session info\n"
        "`/log` · Export session log\n\n"
        "**Download Modes**\n\n"
        "**Manual IDs** — send message IDs separated by spaces\n\n"
        "**Search by Filename** — type a keyword; bot scans history and shows matches\n\n"
        "**Bulk Download** — pick a file type; bot grabs every match in chat history\n"
        "*(files sorted largest-first for maximum throughput)*\n\n"
        "**Smart Features**\n"
        "• Fuzzy chat search (typo-tolerant)\n"
        "• Magic-byte validation (auto-rename wrong extensions)\n"
        "• Hash-based deduplication (same file across chats)\n"
        "• Adaptive throttle (learns Telegram rate limits)\n"
        "• Speed anomaly alerts\n"
        "• Gap detection (shows deleted message count)\n"
        "• Per-chat download ledger\n"
        "• Tamper-evident audit log\n"
        "• Calibrated ETA (improves with each job)\n\n"
        f"**Save path**\n`{DOWNLOAD_BASE}`",
        reply_markup=markup,
        disable_web_page_preview=True,
    )


async def cmd_queue(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    if not _job_queue and not _current_job:
        return await message.reply(
            "No jobs in queue.",
            reply_markup=_home_markup(),
        )

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
    buttons.append([InlineKeyboardButton("🏠 Home", callback_data="home:")])
    await message.reply(
        "**Queue**\n━━━━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
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
        return await message.reply(
            "No active downloads or queued jobs.",
            reply_markup=_home_markup(),
        )
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
    await message.reply(
        "Stopped. All jobs cleared.",
        reply_markup=_home_markup(),
    )
    log.info("killall")


async def cmd_log(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    if os.path.exists(LOG_FILE):
        await message.reply_document(LOG_FILE, caption="session log")
        await message.reply("Log sent.", reply_markup=_home_markup())
    else:
        await message.reply("No log file found.", reply_markup=_home_markup())


async def cmd_stats(_, message: Message):
    uid = message.from_user.id
    if not _authorized(uid):
        return await message.reply("⛔ Unauthorized.")
    dl_root = DOWNLOAD_BASE if os.path.exists(DOWNLOAD_BASE) else "."
    try:
        total, used, free = shutil.disk_usage(dl_root)
        proc = psutil.Process(os.getpid())
    except Exception as e:
        return await message.reply(f"⚠️ Stats error: `{e}`")

    chat      = get_selected_chat(uid)
    job_line  = (
        f"{len(_current_job['ids'])} IDs downloading"
        if _current_job and "ids" in _current_job else "idle"
    )
    top       = _ledger_top(3)                     # MOD-36
    uptime    = _elapsed(BOT_START_TIME)
    total_dl  = len(downloaded_ids)
    storage   = _sz(sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, files in os.walk(DOWNLOAD_BASE) if os.path.exists(DOWNLOAD_BASE)
        for f in files
    ) if os.path.exists(DOWNLOAD_BASE) else 0)

    # UX-07: summary view by default
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔍 More Details",  callback_data="home:stats_detail"),
        InlineKeyboardButton("🏠 Home",          callback_data="home:"),
    ], [
        InlineKeyboardButton("📁 Files",  callback_data="home:files"),
        InlineKeyboardButton("📜 Log",    callback_data="welcome:log"),
    ]])
    await message.reply(
        f"**📊 Stats**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Downloads: **{total_dl}** files  ·  Storage: **{storage}**\n"
        f"Active job: **{'yes' if _current_job else 'no'}**  ·  Queue: **{len(_job_queue)}** waiting\n"
        f"⏱ Uptime: **{uptime}**\n\n"
        f"**Top downloads**\n{top}",
        reply_markup=markup,
    )


async def cmd_stats_detail(message: Message, uid: int):
    """UX-07: detailed stats view shown on 'More Details' button."""
    dl_root = DOWNLOAD_BASE if os.path.exists(DOWNLOAD_BASE) else "."
    try:
        total, used, free = shutil.disk_usage(dl_root)
        cpu  = await asyncio.to_thread(psutil.cpu_percent, interval=0.5)
        ram  = psutil.virtual_memory().percent
        net  = psutil.net_io_counters()
        proc = psutil.Process(os.getpid())
    except Exception as e:
        return await message.reply(f"⚠️ Stats error: `{e}`")

    chat      = get_selected_chat(uid)
    chat_line = _trunc(chat["title"], 22) if chat.get("id") else "none"
    job_line  = (
        f"{len(_current_job['ids'])} IDs downloading"
        if _current_job and "ids" in _current_job else "idle"
    )
    integrity = "✓ ok" if _check_state_integrity() else "⚠️ mismatch"
    avg_delay = sum(_adaptive_delay.values()) / len(_adaptive_delay) if _adaptive_delay else 0.2

    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 Home",   callback_data="home:"),
        InlineKeyboardButton("📁 Files",  callback_data="home:files"),
        InlineKeyboardButton("📜 Log",    callback_data="welcome:log"),
    ]])
    await message.reply(
        f"**Stats — Full Details**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"```\n"
        f"uptime     {_elapsed(BOT_START_TIME)}\n"
        f"chat       {chat_line}\n"
        f"status     {job_line}\n"
        f"files      {len(downloaded_ids)}\n"
        f"hashes     {len(file_hashes)}\n"
        f"throttle   {avg_delay:.2f}s (avg)\n"
        f"integrity  {integrity}\n\n"
        f"disk       {_sz(used)} / {_sz(total)} ({100*used//total}%)\n"
        f"disk free  {_sz(free)}\n"
        f"cpu        {cpu:.1f}%\n"
        f"ram        {ram:.1f}%\n"
        f"net  ↑     {_sz(net.bytes_sent)}\n"
        f"     ↓     {_sz(net.bytes_recv)}\n"
        f"proc       {round(proc.memory_info().rss / 1024**2)} MB\n"
        f"```",
        reply_markup=markup,
    )


# ───────────────────────────────────────────────────────────────
# MOD-NAV: Home callback
# ───────────────────────────────────────────────────────────────

async def cb_home(_, query: CallbackQuery):
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)

    action = query.data.split(":", 1)[1] if ":" in query.data else ""

    if action == "files":
        await query.answer()
        await _send_folder_view(query, uid)
        return

    if action == "stats":
        await query.answer()
        await cmd_stats(None, query.message)
        return

    if action == "stats_detail":
        await query.answer()
        await cmd_stats_detail(query.message, uid)
        return

    user_state[uid] = "idle"
    status = "downloading" if _current_job else "idle"
    await query.answer()
    try:
        await query.message.edit(_welcome_text(status, uid), reply_markup=_welcome_markup(uid))
    except Exception:
        await query.message.reply(
            _welcome_text(status, uid),
            reply_markup=_welcome_markup(uid),
            disable_web_page_preview=True,
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
        chat = get_selected_chat(uid)
        if not chat.get("id"):
            return await query.answer("No previous session.", show_alert=True)
        user_state[uid] = "idle"
        await query.message.edit(
            f"**{_trunc(chat['title'], 28)}**\nSelect a download mode:",
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

    elif action == "new_session":
        _clear_downloaded_ids()
        await query.answer("Session cleared")
        status = "downloading" if _current_job else "idle"
        await query.message.edit(
            _welcome_text(status, uid) + "IDs cleared ✓\n",
            reply_markup=_welcome_markup(uid),
        )

    elif action == "refresh":                     # MOD-NAV
        await query.answer("Refreshing chats…")
        await _load_dialogs()
        status = "downloading" if _current_job else "idle"
        await query.message.edit(
            _welcome_text(status, uid) + f"✓ {len(dialogs_cache)} chats loaded\n",
            reply_markup=_welcome_markup(uid),
        )

    elif action == "files":                       # MOD-NAV
        await query.answer()
        await _send_folder_view(query, uid)

    elif action == "stats":                       # MOD-NAV
        await query.answer()
        await cmd_stats(None, query.message)

    elif action == "queue":                       # MOD-NAV
        await query.answer()
        await cmd_queue(None, query.message)

    elif action == "log":                         # MOD-NAV
        await query.answer()
        await cmd_log(None, query.message)


async def cb_select_chat(_, query: CallbackQuery):
    uid     = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    chat_id = int(query.data.split(":")[1])
    chosen  = next((d for d in dialogs_cache if d["id"] == chat_id), None)
    if not chosen:
        return await query.answer("Chat not found. Reload chats.", show_alert=True)
    set_selected_chat(uid, {"id": chosen["id"], "title": chosen["title"]})  # FIX-01
    _save_session(uid)
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
    chat = get_selected_chat(uid)

    if mode == "back":
        title = _trunc(chat.get("title", ""), 28)
        await query.message.edit(
            f"**{title}**\nSelect a download mode:",
            reply_markup=_dl_action_markup(),
        )
        await query.answer()
        return

    if mode == "manual":
        user_state[uid] = "awaiting_ids"
        back_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("← Back", callback_data="dlopt:back"),
            InlineKeyboardButton("🏠 Home", callback_data="home:"),
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
        back_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("← Back", callback_data="dlopt:back"),
            InlineKeyboardButton("🏠 Home", callback_data="home:"),
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
            "matching file in the entire chat history.\n"
            "Files are sorted largest-first for maximum throughput.",
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
    chat = get_selected_chat(uid)
    if not chat.get("id"):
        return await query.answer("No chat selected.", show_alert=True)

    # UX-03: show confirmation screen before starting bulk scan
    await query.answer()
    confirm_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm",  callback_data=f"bulk_confirm:{file_type}"),
        InlineKeyboardButton("✗ Cancel",   callback_data="dlopt:bulk"),
    ]])
    await query.message.edit(
        f"⚠️ **Bulk Download**\n"
        f"{'━' * 22}\n"
        f"Chat: **{_trunc(chat['title'], 28)}**\n"
        f"Type: **{file_type}**\n"
        f"This will scan up to {BULK_PRESORT_LIMIT:,} messages.\n\n"
        "Tap Confirm to begin.",
        reply_markup=confirm_markup,
    )


async def cb_bulk_confirm(_, query: CallbackQuery):
    """UX-03: called when user confirms bulk download."""
    global _bulk_task
    uid = query.from_user.id
    if not _authorized(uid):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    file_type = query.data.split(":", 1)[1]

    if _bulk_task and not _bulk_task.done():
        return await query.answer(
            "Bulk download already running. Use /killall to stop it.", show_alert=True
        )
    chat = get_selected_chat(uid)
    if not chat.get("id"):
        return await query.answer("No chat selected.", show_alert=True)

    await query.answer(f"Starting {file_type}…")
    progress_msg = await query.message.reply(
        f"🔍 **{file_type}** · Scanning\nBuilding file list…"
    )
    _bulk_task = asyncio.create_task(
        _run_bulk_job(uid, chat["id"], chat["title"], file_type, progress_msg)  # FIX-06
    )


# ───────────────────────────────────────────────────────────────
# Per-job inline cancel
# ───────────────────────────────────────────────────────────────

async def cb_job_cancel(_, query: CallbackQuery):
    global _worker_task, _current_job
    if not _authorized(query.from_user.id):
        return await query.answer("⛔ Unauthorized.", show_alert=True)
    job_id = query.data.split(":")[1]
    if _current_job and _current_job.get("job_id") == job_id:
        # UX-02: immediate feedback before actual cancellation
        await _edit(_current_job.get("progress_msg"), "⏳ Cancelling after current file…")
        _current_job["cancelled"] = True
        if _worker_task and not _worker_task.done():
            _worker_task.cancel()
        return await query.answer("Cancelling…")
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
        _welcome_text(status, uid),
        reply_markup=_welcome_markup(uid),
    )


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
    if not _job_queue and not _current_job:
        await query.message.edit("No jobs in queue.", reply_markup=_home_markup())
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
        buttons.append([InlineKeyboardButton("🏠 Home", callback_data="home:")])
        try:
            await query.message.edit(
                "**Queue**\n━━━━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(buttons),
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
    await query.message.edit("Queue cleared.", reply_markup=_home_markup())


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
    uid    = query.from_user.id
    action = query.data.split(":")[1]
    if action == "options":
        chat = get_selected_chat(uid)
        await query.message.edit(
            f"**{_trunc(chat.get('title', ''), 28)}**\nSelect a download mode:",
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
    buttons.append([InlineKeyboardButton("🗑 Wipe All Downloads", callback_data="fwipe")])
    buttons.append([InlineKeyboardButton("🏠 Home", callback_data="home:")])
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
    buttons.append([
        InlineKeyboardButton("Delete Folder", callback_data=f"fdeldir:{fi}"),
        InlineKeyboardButton("🏠 Home",       callback_data="home:"),
    ])

    header = f"📁  **{_trunc(folder['title'], 22)}**\n{total} file(s)  ·  Page {page+1}/{total_pages}"
    return header, InlineKeyboardMarkup(buttons)


async def _send_folder_view(target, uid: int) -> None:
    folders = _scan_folders()
    _files_nav[uid] = {"folders": folders}
    if not folders:
        text   = "No downloads yet."
        markup = _home_markup()
        if hasattr(target, "reply"):
            await target.reply(text, reply_markup=markup)
        else:
            try: await target.message.edit(text, reply_markup=markup)
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
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("Back",    callback_data="fb"),
            InlineKeyboardButton("🏠 Home", callback_data="home:"),
        ]])
        text = f"📁  **{_trunc(folder['title'], 22)}**  is empty"
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
    await query.answer("✅ File sent")
    await query.message.reply_document(
        entry["path"],
        caption=f"`{_trunc(entry['name'], 48)}` · {_sz(entry['size'])}",
    )


async def cb_delete_file(_, query: CallbackQuery):
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
    await query.message.edit("All downloads cleared.", reply_markup=_home_markup())


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

    chat = get_selected_chat(uid)                  # FIX-01

    if state == "download_search":
        user_state[uid] = "idle"
        if not chat.get("id"):
            return await message.reply("⚠️ No chat selected.")
        progress = await message.reply(f"Searching for `{text}`…")
        matching = await _find_messages_by_name(chat["id"], text)
        if not matching:
            await progress.edit(
                f"No files found matching `{text}`.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("← Back", callback_data="dlopt:search"),
                    InlineKeyboardButton("🏠 Home", callback_data="home:"),
                ]]),
            )
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
        buttons.append([InlineKeyboardButton("🏠 Home", callback_data="home:")])
        await progress.edit(
            f"**{len(matching)} file(s)** matching `{text}`\nTap a file to download:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    if state == "awaiting_ids":
        if not chat.get("id"):
            return await message.reply("⚠️ No chat selected.")

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
        job_id  = uuid4().hex[:8]

        if _current_job or (_worker_task and not _worker_task.done()):
            pos = len(_job_queue) + 1
            progress_msg = await message.reply(
                f"Queued  #{pos}  ·  {len(msg_ids)} ID(s)\n"
                f"{_trunc(chat['title'], 28)}"
            )
            job = {
                "job_id":       job_id,
                "ids":          msg_ids,
                "entries":      entries,
                "chat_title":   chat["title"],
                "chat":         chat,              # FIX-01
                "uid":          uid,               # FIX-06
                "progress_msg": progress_msg,
                "start_time":   time(),
                "cancelled":    False,
                "total_bytes":  0,
            }
            _job_queue.append(job)
            return

        progress_msg = await message.reply(
            f"↓  {len(msg_ids)} ID(s)\n{_trunc(chat['title'], 28)}"
        )
        job = {
            "job_id":       job_id,
            "ids":          msg_ids,
            "entries":      entries,
            "chat_title":   chat["title"],
            "chat":         chat,                  # FIX-01
            "uid":          uid,                   # FIX-06
            "progress_msg": progress_msg,
            "start_time":   time(),
            "cancelled":    False,
            "total_bytes":  0,
        }
        _current_job = job
        _worker_task = asyncio.create_task(_run_job(job))
        return

    if state == "idle" and text.replace(" ", "").isdigit():
        return await message.reply(
            "To download by message ID, open /options and select Manual IDs first.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("▶ Open Options", callback_data="welcome:resume"),
                InlineKeyboardButton("🏠 Home",        callback_data="home:"),
            ]]),
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

    chat = get_selected_chat(uid)                  # FIX-01
    if not chat.get("id"):
        return await query.answer("No chat selected.", show_alert=True)

    entries      = [{"id": mid, "status": "queued"} for mid in msg_ids]
    job_id       = uuid4().hex[:8]
    progress_msg = await query.message.reply(
        f"↓  {len(msg_ids)} file(s)\n{_trunc(chat['title'], 28)}"
    )
    await query.answer("Starting…")
    job = {
        "job_id":       job_id,
        "ids":          msg_ids,
        "entries":      entries,
        "chat_title":   chat["title"],
        "chat":         chat,                      # FIX-01
        "uid":          uid,                       # FIX-06
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
# MOD-40: Disk sentinel background task
# ───────────────────────────────────────────────────────────────

async def _disk_sentinel_loop() -> None:
    """Monitor free disk space; pause downloads and alert owner if critically low."""
    global _current_job, _worker_task, _bulk_task, _bulk_state

    _alerted = False
    while True:
        await asyncio.sleep(20)
        try:
            root = DOWNLOAD_BASE if os.path.exists(DOWNLOAD_BASE) else "."
            _, _, free = shutil.disk_usage(root)
            free_mb = free / (1024 * 1024)

            if free_mb < DISK_LOW_MB:
                if not _alerted and bot:
                    _alerted = True
                    log.warning(f"Disk low: {_sz(free)} remaining — pausing downloads")
                    # Pause active downloads
                    if _current_job and not _current_job.get("cancelled"):
                        _current_job["cancelled"] = True
                        if _worker_task and not _worker_task.done():
                            _worker_task.cancel()
                    if _bulk_task and not _bulk_task.done():
                        _bulk_state["cancelled"] = True
                        _bulk_task.cancel()
                    # FIX-06: alert the uid stored in the active job, not a global
                    alert_uid = (_current_job or {}).get("uid") or _bulk_owner_uid
                    if alert_uid:
                        try:
                            await bot.send_message(
                                alert_uid,
                                f"⚠️ **Disk Low — Downloads Paused**\n"
                                f"Only {_sz(free)} remaining on disk.\n"
                                "Free up space, then restart your job.",
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("📁 Files", callback_data="home:files"),
                                    InlineKeyboardButton("🏠 Home",  callback_data="home:"),
                                ]]),
                            )
                        except Exception:
                            pass
            else:
                _alerted = False   # Reset once disk is free again

        except Exception as e:
            log.warning(f"Disk sentinel error: {e}")


# ───────────────────────────────────────────────────────────────
# Startup
# ───────────────────────────────────────────────────────────────

async def _set_bot_commands() -> None:
    try:
        await bot.set_bot_commands([
            BotCommand("start",   "Home screen"),
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
    global bot, user_client, _disk_sentinel_task

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

    # ── Message handlers ──────────────────────────────────────
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

    # ── Callback handlers ─────────────────────────────────────
    bot.add_handler(CallbackQueryHandler(cb_welcome,               filters.regex(r"^welcome:\w+$")))
    bot.add_handler(CallbackQueryHandler(cb_home,                  filters.regex(r"^home:(\w*)$")))
    bot.add_handler(CallbackQueryHandler(cb_select_chat,           filters.regex(r"^sc:-?\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_page,                  filters.regex(r"^pg:\d+")))
    bot.add_handler(CallbackQueryHandler(cb_dlopt,                 filters.regex(r"^dlopt:\w+$")))
    bot.add_handler(CallbackQueryHandler(cb_bulk_select,           filters.regex(r"^bulk:.+$")))
    bot.add_handler(CallbackQueryHandler(cb_bulk_confirm,          filters.regex(r"^bulk_confirm:.+$")))
    bot.add_handler(CallbackQueryHandler(cb_search_download,       filters.regex(r"^searchdl:\w+$")))
    bot.add_handler(CallbackQueryHandler(cb_open_files_from_job,   filters.regex(r"^openf:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_back,                  filters.regex(r"^back:\w+$")))
    bot.add_handler(CallbackQueryHandler(cb_open_folder,           filters.regex(r"^fd:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_files_page,            filters.regex(r"^fp:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_back_to_folders,       filters.regex(r"^fb$")))
    bot.add_handler(CallbackQueryHandler(cb_send_file,             filters.regex(r"^fl:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_file,           filters.regex(r"^fdel:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_file_confirm,   filters.regex(r"^fdelok:\d+:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_folder,         filters.regex(r"^fdeldir:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_delete_folder_confirm, filters.regex(r"^fdeldirok:\d+$")))
    bot.add_handler(CallbackQueryHandler(cb_wipe_all,              filters.regex(r"^fwipe$")))
    bot.add_handler(CallbackQueryHandler(cb_wipe_all_confirm,      filters.regex(r"^fwipeok$")))
    bot.add_handler(CallbackQueryHandler(cb_job_cancel,            filters.regex(r"^cb_job_cancel:[0-9a-f]+$")))
    bot.add_handler(CallbackQueryHandler(cb_bulk_ctrl,             filters.regex(r"^bulk_ctrl:\w+$")))
    bot.add_handler(CallbackQueryHandler(cb_confirm_reset,         filters.regex(r"^confirm_reset:\w+$")))
    bot.add_handler(CallbackQueryHandler(cb_queue_cancel,          filters.regex(r"^qcancel:[0-9a-f]+$")))
    bot.add_handler(CallbackQueryHandler(cb_queue_cancel_all,      filters.regex(r"^qcancelall$")))

    # ── Load persisted state ──────────────────────────────────
    _load_session()
    _load_downloaded_ids()
    _load_file_hashes()                            # MOD-34
    _load_job_history()                            # MOD-39

    # MOD-41: warn on startup if state files were externally modified
    if not _check_state_integrity():
        log.warning("⚠️  State fingerprint mismatch on startup — files may have been modified externally")

    os.makedirs(DOWNLOAD_BASE, exist_ok=True)

    log.info("Starting clients…")
    await bot.start()
    await user_client.start()
    log.info("Clients started")
    await _load_dialogs()
    await _set_bot_commands()

    # MOD-40: start disk sentinel
    _disk_sentinel_task = asyncio.create_task(_disk_sentinel_loop())
    log.info("GhostFetch online — all systems active")

    try:
        await asyncio.Event().wait()
    finally:
        if _disk_sentinel_task:
            _disk_sentinel_task.cancel()
        await bot.stop()
        await user_client.stop()
        log.info("Bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
