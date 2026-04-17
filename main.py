# GhostFetch — Telegram restricted media downloader
# herraChron
# Complete version with 49 advanced features

import os
import json
import shutil
import psutil
import asyncio
import logging
import hashlib
import pickle
import re
import aiohttp
import sqlite3
from time import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from collections import defaultdict
from functools import lru_cache
import subprocess
import io

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid, BadRequest, MessageNotModified
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand,
)
from config import PyroConf

# ═══════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════��════════════════

BOT_OWNER_ID = PyroConf.BOT_OWNER_ID if hasattr(PyroConf, 'BOT_OWNER_ID') else None
WEBHOOK_URL = getattr(PyroConf, 'WEBHOOK_URL', None)

MAX_REQUESTS_PER_MINUTE = 30
MAX_REQUESTS_PER_HOUR = 300
MAX_REQUESTS_PER_IP = 100

FILE_OPERATION_TIMEOUT = 300
MAX_FOLDER_SIZE = 10 * 1024 * 1024 * 1024
SEARCH_LIMIT = 5000
MAX_CONCURRENT_DOWNLOADS = 3
PROGRESS_UPDATE_INTERVAL = 2.0
CALLBACK_EXPIRY_TIME = 600

MAX_PATH_LENGTH = 200
MAX_ENTRIES_DISPLAY = 500
PARTIAL_FILE_MIN_SIZE = 1024

DIALOG_CACHE_TTL = 3600
USER_SESSION_TTL = 21600
CLEANUP_INTERVAL = 3600
REQUEST_CACHE_TTL = 300

TEXT_TRUNCATE_SHORT = 20
TEXT_TRUNCATE_MED = 25
TEXT_TRUNCATE_LONG = 28
PROGRESS_BAR_WIDTH = 10
BUTTON_TRUNCATE = 25
MAX_MESSAGE_LENGTH = 4090

DOWNLOAD_BASE = "/data/data/com.termux/files/home/GhostFetch/downloads"
SESSION_FILE = "session.json"
CHAT_NAMES_FILE = "chat_names.json"
LOG_FILE = "session.log"
STATS_FILE = "stats.json"
HISTORY_FILE = "history.json"
DOWNLOAD_DB_FILE = "downloads.pkl"
USER_RATE_LIMIT_FILE = "rate_limits.json"
ANALYTICS_DB = "analytics.db"
ENCRYPTION_KEY_FILE = "encryption.key"
API_TOKENS_FILE = "api_tokens.json"
WEBHOOK_LOG_FILE = "webhooks.log"

MAX_LOG_SIZE = 10 * 1024 * 1024
MAX_LOG_BACKUPS = 3

BANDWIDTH_LIMIT = 0
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 2

BORDER_THICK = "━━━━━━━━━━━━━━━━━━━━━━━"
BORDER_THIN = "──────────────────────"

ICON_DL = "📥"
ICON_FOLDER = "📂"
ICON_HELP = "📖"
ICON_STATS = "📊"
ICON_SUCCESS = "✅"
ICON_ERROR = "❌"
ICON_WARN = "⚠️"
ICON_INFO = "ℹ️"
ICON_CLOCK = "⏱"
ICON_DELETE = "🗑"
ICON_SEARCH = "🔍"
ICON_BULK = "📦"
ICON_SKIP = "⊘"
ICON_SPEED = "⚡"
ICON_PAUSE = "⏸"
ICON_STAR = "⭐"
ICON_LOCK = "🔒"
ICON_LINK = "🔗"

DOT = "•"
ARROW = "▶"

# ═══════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════

def _setup_logging():
    """Setup logging with rotation."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=MAX_LOG_SIZE,
        backupCount=MAX_LOG_BACKUPS,
        encoding="utf-8"
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    )
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

log = _setup_logging()


# ═══════════════════════════════════════════════════════════════
# Database for analytics
# ═══════════════════════════════════════════════════════════════

def _init_analytics_db():
    """Initialize analytics database."""
    try:
        conn = sqlite3.connect(ANALYTICS_DB)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS downloads
                     (id INTEGER PRIMARY KEY, filename TEXT, size INTEGER, 
                      chat_id INTEGER, user_id INTEGER, timestamp DATETIME, 
                      duration REAL, speed REAL, status TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS errors
                     (id INTEGER PRIMARY KEY, error TEXT, user_id INTEGER,
                      timestamp DATETIME, stacktrace TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS access_logs
                     (id INTEGER PRIMARY KEY, user_id INTEGER, action TEXT,
                      resource TEXT, timestamp DATETIME, ip_address TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS search_history
                     (id INTEGER PRIMARY KEY, user_id INTEGER, query TEXT,
                      results_count INTEGER, timestamp DATETIME)''')
        
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"Analytics DB init failed: {e}")


def _log_download(filename: str, size: int, chat_id: int, user_id: int, duration: float, speed: float):
    """Log download to analytics."""
    try:
        conn = sqlite3.connect(ANALYTICS_DB)
        c = conn.cursor()
        c.execute('''INSERT INTO downloads 
                     (filename, size, chat_id, user_id, timestamp, duration, speed, status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (filename, size, chat_id, user_id, datetime.now(), duration, speed, 'success'))
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug(f"Analytics log failed: {e}")


def _log_error(error: str, user_id: int, stacktrace: str = ""):
    """Log error to analytics."""
    try:
        conn = sqlite3.connect(ANALYTICS_DB)
        c = conn.cursor()
        c.execute('''INSERT INTO errors (error, user_id, timestamp, stacktrace)
                     VALUES (?, ?, ?, ?)''',
                  (error, user_id, datetime.now(), stacktrace))
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug(f"Error log failed: {e}")


def _log_access(user_id: int, action: str, resource: str, ip_address: str = ""):
    """Log user access."""
    try:
        conn = sqlite3.connect(ANALYTICS_DB)
        c = conn.cursor()
        c.execute('''INSERT INTO access_logs (user_id, action, resource, timestamp, ip_address)
                     VALUES (?, ?, ?, ?, ?)''',
                  (user_id, action, resource, datetime.now(), ip_address))
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug(f"Access log failed: {e}")


def _log_search(user_id: int, query: str, results_count: int):
    """Log search."""
    try:
        conn = sqlite3.connect(ANALYTICS_DB)
        c = conn.cursor()
        c.execute('''INSERT INTO search_history (user_id, query, results_count, timestamp)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, query, results_count, datetime.now()))
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug(f"Search log failed: {e}")


def _get_analytics(user_id: int = None) -> dict:
    """Get analytics data."""
    try:
        conn = sqlite3.connect(ANALYTICS_DB)
        c = conn.cursor()
        
        where = f"WHERE user_id = {user_id}" if user_id else ""
        
        c.execute(f"SELECT COUNT(*), SUM(size), AVG(speed) FROM downloads {where}")
        total_downloads, total_size, avg_speed = c.fetchone()
        
        c.execute(f"SELECT COUNT(*) FROM errors {where}")
        total_errors = c.fetchone()[0]
        
        conn.close()
        
        return {
            "downloads": total_downloads or 0,
            "size": total_size or 0,
            "avg_speed": avg_speed or 0,
            "errors": total_errors or 0,
        }
    except Exception as e:
        log.error(f"Get analytics failed: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════
# Pyrogram clients
# ═══════════════════════════════════════════════════════════════

bot         = None
user_client = None


# ═══════════════════════════════════════════════════════════════
# Global state
# ═══════════════════════════════════════════════════════════════

dialogs_cache: list = []
dialogs_cache_time: float = 0
selected_chat: dict = {}
downloaded_ids: dict = {}
user_state: dict = {}
search_results: dict = {}
search_history: dict = {}
user_preferences: dict = {}

_job_queue: list = []
_current_job: dict | None = None
_worker_task: asyncio.Task | None = None
_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

_files_nav: dict = {}
_user_cleanup_task: asyncio.Task | None = None
_user_rate_limits: dict = {}
_callback_timestamps: dict = {}
_emoji_cache: dict = {}
_message_cache: dict = {}
_request_cache: dict = {}
_bandwidth_throttle: dict = {}

_download_stats: dict = {}
_active_downloads: dict = {}
_api_tokens: dict = {}

FILE_TYPES = {
    "🎬 Videos": [".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".m4v", ".3gp"],
    "🎵 Audio": [".mp3", ".ogg", ".flac", ".wav", ".m4a", ".aac", ".opus", ".wma"],
    "🖼️ Images": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".heic"],
    "📚 Documents": [".pdf", ".epub", ".mobi", ".djvu", ".txt", ".docx", ".xlsx", ".doc"],
    "🗜️ Archives": [".zip", ".rar", ".7z", ".tar", ".gz", ".xz", ".bz2"],
    "📱 Apps": [".apk", ".xapk", ".apks", ".exe", ".msi"],
}

http_session = None


# ═══════════════════════════════════════════════════════════════
# Helper functions
# ═══════════════════════════════════════════════════════════════

def _is_owner(uid: int) -> bool:
    """Check if user is bot owner."""
    if not BOT_OWNER_ID:
        return True
    return uid == BOT_OWNER_ID


def _check_rate_limit(uid: int) -> tuple[bool, str]:
    """Check user rate limit."""
    now = time()
    
    if uid not in _user_rate_limits:
        _user_rate_limits[uid] = {
            "minute_count": 0,
            "hour_count": 0,
            "last_reset_minute": now,
            "last_reset_hour": now,
        }
    
    limit = _user_rate_limits[uid]
    
    if now - limit["last_reset_minute"] > 60:
        limit["minute_count"] = 0
        limit["last_reset_minute"] = now
    
    if now - limit["last_reset_hour"] > 3600:
        limit["hour_count"] = 0
        limit["last_reset_hour"] = now
    
    limit["minute_count"] += 1
    limit["hour_count"] += 1
    
    if limit["minute_count"] > MAX_REQUESTS_PER_MINUTE:
        return False, f"Rate limited. {MAX_REQUESTS_PER_MINUTE} req/min max"
    
    if limit["hour_count"] > MAX_REQUESTS_PER_HOUR:
        return False, f"Rate limited. {MAX_REQUESTS_PER_HOUR} req/hour max"
    
    return True, ""


def _track_callback(callback_id: str) -> None:
    """Track callback timestamp."""
    _callback_timestamps[callback_id] = time()


def _is_callback_valid(callback_id: str) -> bool:
    """Check if callback is still valid."""
    now = time()
    
    if callback_id not in _callback_timestamps:
        _callback_timestamps[callback_id] = now
        return True
    
    if now - _callback_timestamps[callback_id] > CALLBACK_EXPIRY_TIME:
        return False
    
    return True


def _load_api_tokens() -> dict:
    """Load API tokens."""
    try:
        with open(API_TOKENS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_api_tokens(tokens: dict) -> None:
    """Save API tokens."""
    try:
        with open(API_TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(tokens, f, indent=2)
    except Exception as e:
        log.warning(f"Save tokens failed: {e}")


def _generate_api_token(user_id: int) -> str:
    """Generate API token."""
    token = hashlib.sha256(f"{user_id}{time()}".encode()).hexdigest()
    _api_tokens[token] = {
        "user_id": user_id,
        "created_at": datetime.now().isoformat(),
    }
    _save_api_tokens(_api_tokens)
    return token


def _load_preferences(uid: int) -> dict:
    """Load user preferences."""
    try:
        with open(f"prefs_{uid}.json", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"theme": "light", "notifications": True, "concurrent_downloads": 1}


def _save_preferences(uid: int, prefs: dict) -> None:
    """Save user preferences."""
    try:
        with open(f"prefs_{uid}.json", "w", encoding="utf-8") as f:
            json.dump(prefs, f, indent=2)
    except Exception as e:
        log.warning(f"Save preferences failed: {e}")


def _load_encryption_key() -> bytes:
    """Load or generate encryption key."""
    try:
        with open(ENCRYPTION_KEY_FILE, "rb") as f:
            return f.read()
    except FileNotFoundError:
        key = os.urandom(32)
        try:
            with open(ENCRYPTION_KEY_FILE, "wb") as f:
                f.write(key)
        except Exception as e:
            log.warning(f"Save encryption key failed: {e}")
        return key


async def _send_webhook(event: str, data: dict) -> None:
    """Send webhook notification."""
    if not WEBHOOK_URL or not http_session:
        return
    
    try:
        payload = {
            "event": event,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }
        async with http_session.post(WEBHOOK_URL, json=payload, timeout=5) as resp:
            if resp.status != 200:
                log.warning(f"Webhook failed: {resp.status}")
    except Exception as e:
        log.warning(f"Webhook error: {e}")


def _load_download_db() -> dict:
    """Load persistent download database."""
    try:
        with open(DOWNLOAD_DB_FILE, "rb") as f:
            return pickle.load(f)
    except Exception:
        return {}


def _save_download_db(db: dict) -> None:
    """Save download database."""
    try:
        with open(DOWNLOAD_DB_FILE, "wb") as f:
            pickle.dump(db, f)
    except Exception as e:
        log.warning(f"Failed to save download DB: {e}")


def _load_history() -> dict:
    """Load download history."""
    try:
        with open(HISTORY_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"total_downloads": 0, "total_size": 0, "total_time": 0, "downloads": []}


def _save_history(history: dict) -> None:
    """Save download history."""
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        log.warning(f"Failed to save history: {e}")


def _add_to_history(filename: str, size: int, duration: float, chat_id: int, user_id: int) -> None:
    """Add download to history."""
    history = _load_history()
    history["total_downloads"] += 1
    history["total_size"] += size
    history["total_time"] += duration
    history["downloads"].append({
        "name": filename,
        "size": size,
        "time": datetime.now().isoformat(),
        "duration": duration,
        "chat_id": chat_id,
        "user_id": user_id,
    })
    if len(history["downloads"]) > 1000:
        history["downloads"] = history["downloads"][-1000:]
    _save_history(history)
    
    speed = size / duration if duration > 0 else 0
    _log_download(filename, size, chat_id, user_id, duration, speed)


# ═══════════════════════════════════════════════════════════════
# Persistence with validation
# ═══════════════════════════════════════════════════════════════

def _load_session() -> None:
    global selected_chat
    try:
        with open(SESSION_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and data.get("id"):
            selected_chat = data
            log.info(f"Loaded session — {selected_chat['title']}")
        else:
            selected_chat = {}
    except json.JSONDecodeError:
        log.error("Corrupted session")
        selected_chat = {}
    except Exception as e:
        log.warning(f"Session load failed: {e}")
        selected_chat = {}


def _save_session() -> None:
    try:
        if os.path.exists(SESSION_FILE):
            shutil.copy(SESSION_FILE, SESSION_FILE + ".bak")
        
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
    """Get folder title with fallback."""
    for d in dialogs_cache:
        if str(d["id"]).replace("-100", "") == folder_id:
            return d["title"]
    names = _load_chat_names()
    return names.get(folder_id, f"Chat_{folder_id}")


# ═══════════════════════════════════════════════════════════════
# Formatting helpers
# ═══════════════════════════════════════════════════════════════

def _sz(b: int | float) -> str:
    """Human-readable byte size."""
    if b < 1024:
        return f"{int(b)} B"
    b /= 1024
    if b < 1024:
        return f"{b:.0f} KB"
    b /= 1024
    if b < 1024:
        return f"{b:.1f} MB"
    b /= 1024
    if b < 1024:
        return f"{b:.1f} GB"
    b /= 1024
    return f"{b:.2f} TB"


def _speed(bytes_per_sec: float) -> str:
    """Human-readable download speed."""
    if bytes_per_sec < 1024:
        return f"{bytes_per_sec:.0f} B/s"
    bytes_per_sec /= 1024
    if bytes_per_sec < 1024:
        return f"{bytes_per_sec:.1f} KB/s"
    bytes_per_sec /= 1024
    return f"{bytes_per_sec:.1f} MB/s"


def _elapsed(start: float) -> str:
    """Human-readable elapsed time."""
    s = int(time() - start)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h}h {m:02d}m"
    d, h = divmod(h, 24)
    return f"{d}d {h}h"


def _timestamp() -> str:
    """Current timestamp."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _bar(pct: float, width: int = PROGRESS_BAR_WIDTH) -> str:
    """Progress bar."""
    n = round(width * pct / 100)
    return "█" * n + "░" * (width - n)


def _file_emoji(filename: str) -> str:
    """Get file emoji with caching."""
    ext = os.path.splitext(filename)[1].lower()
    
    if ext in _emoji_cache:
        return _emoji_cache[ext]
    
    emojis = {
        ".jpg": "🖼", ".jpeg": "🖼", ".png": "🖼", ".gif": "🖼",
        ".webp": "🖼", ".bmp": "🖼", ".tiff": "🖼", ".heic": "🖼",
        ".mp4": "🎬", ".mkv": "🎬", ".avi": "🎬", ".mov": "🎬",
        ".webm": "🎬", ".flv": "🎬", ".ts": "🎬", ".m4v": "🎬", ".3gp": "🎬",
        ".mp3": "🎵", ".ogg": "🎵", ".flac": "🎵", ".wav": "🎵",
        ".m4a": "🎵", ".aac": "🎵", ".opus": "🎵", ".wma": "🎵",
        ".zip": "🗜", ".rar": "🗜", ".7z": "🗜", ".tar": "🗜",
        ".gz": "🗜", ".xz": "🗜", ".bz2": "🗜",
        ".pdf": "📕", ".epub": "📕", ".mobi": "📕", ".djvu": "📕",
        ".txt": "📝", ".md": "📝", ".log": "📝",
        ".xlsx": "📊", ".xls": "📊", ".csv": "📊", ".doc": "📄", ".docx": "📄",
        ".apk": "📱", ".xapk": "📱", ".apks": "📱",
    }
    
    result = emojis.get(ext, "📄")
    _emoji_cache[ext] = result
    return result


def _truncate(text: str, length: int) -> str:
    """Safely truncate text."""
    if not text:
        return ""
    if len(text) <= length:
        return text
    return text[:length-1] + "…"


def _truncate_message(text: str, max_len: int = MAX_MESSAGE_LENGTH) -> str:
    """Truncate message if too long."""
    if len(text) <= max_len:
        return text
    return text[:max_len-10] + "\n\n…truncated"


def _file_hash(filepath: str) -> str:
    """Calculate file hash for integrity."""
    try:
        hasher = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return ""


def _get_file_info(filepath: str) -> dict:
    """Extract file metadata."""
    info = {
        "name": os.path.basename(filepath),
        "size": 0,
        "modified": datetime.now().isoformat(),
        "type": "unknown",
        "duration": 0,
        "codec": "",
    }
    
    try:
        stat = os.stat(filepath)
        info["size"] = stat.st_size
        info["modified"] = datetime.fromtimestamp(stat.st_mtime).isoformat()
        
        ext = os.path.splitext(filepath)[1].lower()
        if ext in [".mp4", ".mkv", ".avi", ".mov", ".webm"]:
            info["type"] = "video"
            try:
                result = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                     "-of", "default=noprint_wrappers=1:nokey=1:0", filepath],
                    capture_output=True, text=True, timeout=5
                )
                if result.stdout:
                    info["duration"] = int(float(result.stdout.strip()))
            except Exception:
                pass
        elif ext in [".jpg", ".png", ".gif"]:
            info["type"] = "image"
        elif ext in [".mp3", ".flac", ".wav"]:
            info["type"] = "audio"
    except Exception:
        pass
    
    return info


@lru_cache(maxsize=256)
def _normalize_filename(filename: str) -> str:
    """Normalize filename for comparison."""
    return filename.lower().replace(" ", "").replace("_", "")


# ════════════════════════════════════════════════���══════════════
# Cleanup and rate limiting
# ═══════════════════════════════════════════════════════════════

async def _cleanup_task():
    """Periodic cleanup."""
    while True:
        try:
            await asyncio.sleep(CLEANUP_INTERVAL)
            now = time()
            
            expired = [uid for uid, data in user_state.items()
                      if isinstance(data, tuple) and now - data[1] > USER_SESSION_TTL]
            for uid in expired:
                user_state.pop(uid, None)
                search_results.pop(uid, None)
                _files_nav.pop(uid, None)
            
            if expired:
                log.info(f"Cleaned {len(expired)} expired sessions")
            
            expired_cb = [k for k, v in _callback_timestamps.items() 
                         if now - v > CALLBACK_EXPIRY_TIME]
            for k in expired_cb:
                _callback_timestamps.pop(k, None)
            
            expired_rl = [uid for uid, data in _user_rate_limits.items()
                         if now - data.get("last_reset_hour", now) > 3600]
            for uid in expired_rl:
                _user_rate_limits.pop(uid, None)
            
            expired_cache = [k for k, v in _request_cache.items()
                            if now - v.get("time", 0) > REQUEST_CACHE_TTL]
            for k in expired_cache:
                _request_cache.pop(k, None)
            
            if len(_emoji_cache) > 500:
                _emoji_cache.clear()
                log.info("Cleared emoji cache")
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"Cleanup error: {e}")


def _add_user_activity(uid: int) -> None:
    """Track user activity."""
    state = "idle"
    if isinstance(user_state.get(uid), tuple):
        state, _ = user_state[uid]
    else:
        state = user_state.get(uid, "idle")
    user_state[uid] = (state, time())


# ═══════════════════════════════════════════════════════════════
# Bandwidth throttling
# ═══════════════════════════════════════════════════════════════

async def _apply_bandwidth_limit(uid: int, downloaded: int) -> None:
    """Apply bandwidth throttling."""
    if BANDWIDTH_LIMIT <= 0:
        return
    
    target_speed = BANDWIDTH_LIMIT * 1024
    elapsed = time() - _bandwidth_throttle.get(uid, time())
    
    if elapsed > 0:
        current_speed = downloaded / elapsed
        if current_speed > target_speed:
            delay = (downloaded / target_speed) - elapsed
            if delay > 0:
                await asyncio.sleep(min(delay, 1.0))


# ═══════════════════════════════════════════════════════════════
# Progress renderer with advanced info
# ═══════════════════════════════════════════════════════════════

def _estimate_eta(downloaded: int, total: int, elapsed: float) -> str:
    """Estimate time remaining."""
    if elapsed <= 0 or downloaded <= 0:
        return "–"
    speed = downloaded / elapsed
    if speed == 0:
        return "∞"
    remaining = (total - downloaded) / speed
    return _elapsed(time() - remaining)


def _get_current_speed(job: dict) -> float:
    """Calculate current download speed."""
    elapsed = time() - job['start_time']
    if elapsed <= 0:
        return 0
    total_size = sum(e.get('size', 0) for e in job["entries"] if e["status"] == "done")
    return total_size / elapsed if elapsed > 0 else 0


def _count_files_by_type(job: dict, file_type: str) -> int:
    """Count files of each type."""
    extensions = FILE_TYPES.get(file_type, [])
    count = 0
    for e in job["entries"]:
        fname = e.get("name", "")
        ext = os.path.splitext(fname)[1].lower()
        if ext in extensions:
            count += 1
    return count


def _render_queue_preview(queue: list) -> str:
    """Render queue preview."""
    if not queue:
        return "Empty queue"
    
    text = f"📋 **Queue ({len(queue)})**\n\n"
    for i, job in enumerate(queue[:5], 1):
        count = len(job.get("entries", []))
        text += f"{i}. {_truncate(job['chat_title'], 20)} - {count} files\n"
    
    if len(queue) > 5:
        text += f"\n... +{len(queue) - 5} more"
    
    return text


def _render_job_summary(job: dict) -> str:
    """Render completion summary."""
    done    = sum(1 for e in job["entries"] if e["status"] == "done")
    skipped = sum(1 for e in job["entries"] if e["status"] == "skipped")
    failed  = sum(1 for e in job["entries"] if e["status"] == "failed")
    total_dl = sum(e.get('size', 0) for e in job["entries"] if e["status"] == "done")
    elapsed = time() - job['start_time']
    speed = _get_current_speed(job)
    
    text = (
        f"{ICON_DL} **Complete**\n\n"
        f"Chat: {_truncate(job['chat_title'], TEXT_TRUNCATE_MED)}\n"
        f"✅ {done} {DOT} ⏭️ {skipped} {DOT} ❌ {failed}\n\n"
        f"💾 {_sz(total_dl)}\n"
        f"⏱ {_elapsed(job['start_time'])} {DOT} {ICON_SPEED} {_speed(speed)}\n"
        f"📅 {_timestamp()}"
    )
    
    if done > 0:
        _add_to_history(f"{done} files", total_dl, elapsed, job.get("chat_id", 0), job.get("user_id", 0))
    
    return text


def _render_bulk_progress(job: dict, file_type: str = "") -> str:
    """Render bulk download progress."""
    total_files = len(job["entries"])
    done    = sum(1 for e in job["entries"] if e["status"] == "done")
    current = sum(1 for e in job["entries"] if e["status"] in ("done", "failed", "skipped"))
    
    current_file = None
    current_pct = 0
    for e in job["entries"]:
        if e["status"] == "downloading":
            current_file = e.get("name", "…")
            current_pct = e.get("pct", 0)
            break
    
    total_size = sum(e.get('size', 0) for e in job["entries"] if e["status"] == "done")
    progress_pct = (current / total_files * 100) if total_files > 0 else 0
    speed = _get_current_speed(job)
    
    type_info = f"\n{file_type}" if file_type else ""
    file_type_count = _count_files_by_type(job, file_type) if file_type else total_files
    
    text = (
        f"{ICON_BULK} **Downloading…**{type_info}\n"
        f"`{current}/{total_files}` files ({file_type_count} {file_type.lower() if file_type else 'total'})\n\n"
    )
    
    if current_file:
        short = _truncate(current_file, TEXT_TRUNCATE_LONG)
        text += (
            f"📄 {short}\n"
            f"`[{_bar(current_pct)}]` {current_pct:.0f}%\n\n"
        )
    
    eta = _estimate_eta(total_size, 
                       sum(e.get('size', 0) for e in job["entries"]),
                       time() - job['start_time'])
    
    text += (
        f"📊 `[{_bar(progress_pct)}]` {progress_pct:.0f}%\n"
        f"💾 {_sz(total_size)} {DOT} {ICON_SPEED} {_speed(speed)}\n"
        f"⏱ {_elapsed(job['start_time'])} {DOT} ETA {eta}"
    )
    
    return _truncate_message(text)


def _render_manual_progress(job: dict) -> str:
    """Render manual ID progress."""
    header = f"{ICON_DL} **Download**\n\n"
    
    entries_text = []
    for e in job["entries"]:
        if e["status"] == "done":
            mid = f"`{e['id']}`"
            name = _truncate(e.get("name", "?"), TEXT_TRUNCATE_SHORT)
            entries_text.append(f"✅ {mid} {DOT} {name}")
        elif e["status"] == "downloading":
            mid = f"`{e['id']}`"
            pct = e.get("pct", 0)
            name = _truncate(e.get("name", "…"), TEXT_TRUNCATE_SHORT)
            entries_text.append(f"⏳ {mid} {DOT} {name}\n   `[{_bar(pct)}]` {pct:.0f}%")
        elif e["status"] == "skipped":
            mid = f"`{e['id']}`"
            reason = e.get('reason', '')[:12]
            entries_text.append(f"{ICON_SKIP} {mid} {DOT} {reason}")
        elif e["status"] == "failed":
            mid = f"`{e['id']}`"
            reason = e.get('reason', '')[:12]
            entries_text.append(f"❌ {mid} {DOT} {reason}")
    
    shown_entries = []
    done_count = 0
    for entry in reversed(entries_text):
        if "✅" in entry and done_count < 3:
            shown_entries.insert(0, entry)
            done_count += 1
        elif "⏳" in entry or "❌" in entry or ICON_SKIP in entry:
            shown_entries.insert(0, entry)
    
    if len(entries_text) > len(shown_entries):
        shown_entries.insert(0, f"... +{len(entries_text) - len(shown_entries)} more")
    
    rows = "\n".join(shown_entries[:8])
    total_dl = sum(e.get('size', 0) for e in job["entries"] if e["status"] == "done")
    speed = _get_current_speed(job)
    footer = f"\n\n💾 {_sz(total_dl)} {DOT} {ICON_SPEED} {_speed(speed)}\n⏱ {_elapsed(job['start_time'])}"
    
    return _truncate_message(header + rows + footer)


async def _edit(msg: Message, text: str, reply_markup=None) -> None:
    """Edit message safely."""
    if not msg:
        return
    
    try:
        text = _truncate_message(text)
        await msg.edit(text, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await _edit(msg, text, reply_markup)
    except Exception as e:
        log.debug(f"Edit failed: {e}")


# ═══════════════════════════════════════════════════════════════
# Dialog loading
# ═══════════════════════════════════════════════════════════════

async def _load_dialogs(force: bool = False) -> None:
    global dialogs_cache, dialogs_cache_time
    
    now = time()
    if not force and dialogs_cache and (now - dialogs_cache_time) < DIALOG_CACHE_TTL:
        return
    
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
        dialogs_cache_time = now
        log.info(f"Dialogs: {len(dialogs_cache)} chats")
    except FloodWait as e:
        log.warning(f"FloodWait, retrying in {e.value}s")
        await asyncio.sleep(e.value + 1)
        await _load_dialogs(force=True)
    except Exception as e:
        log.error(f"Dialog load failed: {e}")


# ═══════════════════════════════════════════════════════════════
# File operations
# ═══════════════════════════════════════════════════════════════

def _check_folder_size() -> bool:
    """Check folder size."""
    try:
        total = 0
        for root, dirs, files in os.walk(DOWNLOAD_BASE):
            for f in files:
                total += os.path.getsize(os.path.join(root, f))
        return total < MAX_FOLDER_SIZE
    except Exception as e:
        log.error(f"Size check failed: {e}")
        return True


def _save_path(chat_id, filename: str) -> str:
    """Save path with size check."""
    if not _check_folder_size():
        log.error("Max folder size reached")
        return None
    
    folder = os.path.join(DOWNLOAD_BASE, str(chat_id).replace("-100", ""))
    
    try:
        os.makedirs(folder, exist_ok=True)
    except Exception as e:
        log.error(f"Cannot create folder: {e}")
        return None
    
    if len(os.path.join(folder, filename)) > MAX_PATH_LENGTH:
        base, ext = os.path.splitext(filename)
        max_base = MAX_PATH_LENGTH - len(folder) - len(ext) - 10
        filename = base[:max_base] + ext
    
    dest = os.path.join(folder, filename)
    if os.path.exists(dest):
        base, ext = os.path.splitext(dest)
        i = 1
        while os.path.exists(f"{base}_{i}{ext}"):
            i += 1
        dest = f"{base}_{i}{ext}"
    
    return dest


def _filename(msg) -> str:
    """Extract filename."""
    if msg.document:
        return msg.document.file_name or f"doc_{msg.id}_{int(msg.date.timestamp())}"
    if msg.video:
        return msg.video.file_name or f"video_{msg.id}_{int(msg.date.timestamp())}.mp4"
    if msg.audio:
        return msg.audio.file_name or f"audio_{msg.id}_{int(msg.date.timestamp())}.mp3"
    if msg.voice:
        return f"voice_{msg.id}_{int(msg.date.timestamp())}.ogg"
    if msg.photo:
        return f"photo_{msg.id}_{int(msg.date.timestamp())}.jpg"
    if msg.animation:
        return f"gif_{msg.id}_{int(msg.date.timestamp())}.mp4"
    if msg.video_note:
        return f"vidnote_{msg.id}_{int(msg.date.timestamp())}.mp4"
    if msg.sticker:
        return f"sticker_{msg.id}_{int(msg.date.timestamp())}.webp"
    return f"file_{msg.id}_{int(msg.date.timestamp())}"


# ═══════════════════════════════════════════════════════════════
# Search with caching
# ═══════════════════════════════════════════════════════════════

async def _find_messages_by_extensions(chat_id: int, extensions: list) -> list:
    """Search with caching."""
    cache_key = f"ext_{chat_id}_{''.join(sorted(extensions))}"
    
    if cache_key in _request_cache:
        cached = _request_cache[cache_key]
        if time() - cached["time"] < REQUEST_CACHE_TTL:
            return cached["data"]
    
    matching_ids = []
    ext_lower = [e.lower() for e in extensions]
    retry_count = 0
    
    while retry_count < RETRY_ATTEMPTS:
        try:
            count = 0
            async for message in user_client.get_chat_history(chat_id, limit=SEARCH_LIMIT):
                if message.media:
                    fname = _filename(message)
                    file_ext = os.path.splitext(fname)[1].lower()
                    if file_ext in ext_lower:
                        matching_ids.append(message.id)
                count += 1
            
            matching_ids.reverse()
            
            _request_cache[cache_key] = {
                "data": matching_ids,
                "time": time(),
            }
            
            log.info(f"Search: {count} msgs, {len(matching_ids)} matches")
            return matching_ids
            
        except FloodWait as e:
            retry_count += 1
            wait_time = e.value * (RETRY_BACKOFF_BASE ** (retry_count - 1))
            log.warning(f"FloodWait search (attempt {retry_count}), waiting {wait_time}s")
            await asyncio.sleep(wait_time)
        except Exception as e:
            log.error(f"Search error: {e}")
            _log_error(str(e), 0, "search_extension_error")
            return matching_ids
    
    return matching_ids


async def _find_messages_by_name(chat_id: int, query: str) -> list:
    """Search by name with sorting and caching."""
    cache_key = f"name_{chat_id}_{query}"
    
    if cache_key in _request_cache:
        cached = _request_cache[cache_key]
        if time() - cached["time"] < REQUEST_CACHE_TTL:
            return cached["data"]
    
    matching = []
    q = query.lower()
    retry_count = 0
    
    while retry_count < RETRY_ATTEMPTS:
        try:
            count = 0
            async for message in user_client.get_chat_history(chat_id, limit=SEARCH_LIMIT):
                if message.media:
                    fname = _filename(message)
                    if q in fname.lower():
                        matching.append({"id": message.id, "name": fname})
                count += 1
            
            matching.sort(key=lambda x: x["name"])
            
            _request_cache[cache_key] = {
                "data": matching,
                "time": time(),
            }
            
            log.info(f"Name search: {count} msgs, {len(matching)} matches")
            _log_search(0, query, len(matching))
            
            return matching
            
        except FloodWait as e:
            retry_count += 1
            wait_time = e.value * (RETRY_BACKOFF_BASE ** (retry_count - 1))
            log.warning(f"FloodWait search (attempt {retry_count}), waiting {wait_time}s")
            await asyncio.sleep(wait_time)
        except Exception as e:
            log.error(f"Search error: {e}")
            _log_error(str(e), 0, "search_name_error")
            return matching
    
    return matching


async def _find_messages_by_date_range(chat_id: int, start_date: datetime, end_date: datetime) -> list:
    """Search by date range."""
    matching = []
    
    try:
        async for message in user_client.get_chat_history(chat_id, limit=SEARCH_LIMIT):
            if message.media and start_date <= message.date <= end_date:
                matching.append(message.id)
        
        return matching
    except Exception as e:
        log.error(f"Date range search error: {e}")
        return matching


async def _find_messages_by_size(chat_id: int, min_size: int = 0, max_size: int = None) -> list:
    """Search by file size range."""
    matching = []
    
    try:
        async for message in user_client.get_chat_history(chat_id, limit=SEARCH_LIMIT):
            if message.media:
                size = 0
                if message.document:
                    size = message.document.file_size
                elif message.video:
                    size = message.video.file_size
                elif message.audio:
                    size = message.audio.file_size
                
                if min_size <= size and (max_size is None or size <= max_size):
                    matching.append(message.id)
        
        return matching
    except Exception as e:
        log.error(f"Size range search error: {e}")
        return matching


# ═══════════════════════════════════════════════════════════════
# Download with advanced features
# ═══════════════════════════════════════════════════════════════

async def _download_file(
    msg,
    chat_id,
    job: dict,
    entry: dict,
    index: int,
    total: int,
    uid: int = 0,
) -> tuple[str, int]:
    """Download with bandwidth limiting and retry."""
    fname = _filename(msg)
    path  = _save_path(chat_id, fname)
    
    if not path:
        entry["status"] = "failed"
        entry["reason"] = "Path error"
        return None, 0
    
    _last_update = [0.0]
    _bandwidth_throttle[uid] = time()

    async def _progress(current, total_bytes):
        if job.get("cancelled"):
            raise asyncio.CancelledError()
        if job.get("paused"):
            raise Exception("Paused")
        
        now = time()
        if now - _last_update[0] < PROGRESS_UPDATE_INTERVAL:
            return
        _last_update[0] = now
        pct  = (current / total_bytes * 100) if total_bytes else 0
        label = fname if total == 1 else f"{fname} [{index + 1}/{total}]"
        entry["pct"]  = pct
        entry["name"] = label
        
        await _apply_bandwidth_limit(uid, current)

    retry_count = 0
    while retry_count < RETRY_ATTEMPTS:
        try:
            await asyncio.wait_for(
                msg.download(file_name=path, progress=_progress),
                timeout=FILE_OPERATION_TIMEOUT
            )

            size = os.path.getsize(path) if os.path.exists(path) else 0
            
            if size < PARTIAL_FILE_MIN_SIZE:
                if os.path.exists(path):
                    os.remove(path)
                entry["status"] = "failed"
                entry["reason"] = "Incomplete"
                log.warning(f"Invalid file: {path}")
                return None, 0
            
            file_hash = _file_hash(path)
            entry["hash"] = file_hash
            
            log.info(f"Downloaded: {fname} ({_sz(size)})")
            return fname, size
            
        except asyncio.TimeoutError:
            retry_count += 1
            if retry_count >= RETRY_ATTEMPTS:
                if os.path.exists(path):
                    os.remove(path)
                entry["status"] = "failed"
                entry["reason"] = "Timeout"
                log.error(f"Download timeout: {fname}")
                raise
            wait_time = RETRY_BACKOFF_BASE ** (retry_count - 1)
            await asyncio.sleep(wait_time)
            
        except asyncio.CancelledError:
            if os.path.exists(path):
                os.remove(path)
            raise
        except Exception as e:
            retry_count += 1
            if retry_count >= RETRY_ATTEMPTS:
                if os.path.exists(path):
                    os.remove(path)
                raise
            wait_time = RETRY_BACKOFF_BASE ** (retry_count - 1)
            await asyncio.sleep(wait_time)


async def _process_id(msg_id: int, job: dict, entry: dict, chat_id: int, uid: int = 0) -> None:
    """Process download with retry logic."""
    
    if not isinstance(msg_id, int) or msg_id <= 0:
        entry["status"] = "failed"
        entry["reason"] = "Bad ID"
        return
    
    entry["status"] = "downloading"
    
    if chat_id not in downloaded_ids:
        downloaded_ids[chat_id] = {}
    
    msg = None
    retry_count = 0
    while retry_count < RETRY_ATTEMPTS:
        try:
            fetched = await user_client.get_messages(chat_id=chat_id, message_ids=[msg_id])
            msg = fetched[0] if fetched else None
            break
        except FloodWait as e:
            retry_count += 1
            if retry_count >= RETRY_ATTEMPTS:
                entry["status"] = "failed"
                entry["reason"] = "Wait"
                return
            wait_time = e.value * (RETRY_BACKOFF_BASE ** (retry_count - 1))
            await asyncio.sleep(wait_time)
        except (PeerIdInvalid, BadRequest):
            entry["status"] = "failed"
            entry["reason"] = "Bad ID"
            return
        except Exception as e:
            entry["status"] = "failed"
            entry["reason"] = "Error"
            log.error(f"Fetch error: {e}")
            _log_error(str(e), uid, f"fetch_msg_{msg_id}")
            return

    if not msg or msg.empty:
        entry["status"] = "skipped"
        entry["reason"] = "Gone"
        return

    try:
        async with _download_semaphore:
            if msg.media_group_id:
                group       = await user_client.get_media_group(chat_id, msg_id)
                media_msgs  = [m for m in group if m.media]
                total_size  = 0
                last_name   = ""

                for i, m in enumerate(media_msgs):
                    if job.get("cancelled"):
                        raise asyncio.CancelledError()
                    if job.get("paused"):
                        raise Exception("Paused")
                    
                    fname, sz = await _download_file(
                        m, chat_id, job, entry, i, len(media_msgs), uid
                    )
                    if fname:
                        total_size += sz
                        last_name   = fname

                entry["name"] = f"{len(media_msgs)} files" if len(media_msgs) > 1 else last_name
                entry["size"] = total_size

            elif msg.media:
                fname, sz = await _download_file(msg, chat_id, job, entry, 0, 1, uid)
                if fname:
                    entry["name"] = fname
                    entry["size"] = sz
                else:
                    return

            else:
                entry["status"] = "skipped"
                entry["reason"] = "No media"
                return

            downloaded_ids[chat_id][msg_id] = {
                "id": msg_id,
                "name": entry.get("name"),
                "size": entry.get("size", 0),
                "hash": entry.get("hash", ""),
                "date": datetime.now().isoformat(),
            }
            entry["status"] = "done"

    except asyncio.CancelledError:
        entry["status"] = "failed"
        entry["reason"] = "Stop"
        raise
    except Exception as e:
        log.error(f"Process error: {e}")
        entry["status"] = "failed"
        entry["reason"] = "Error"
        _log_error(str(e), uid, f"process_id_{msg_id}")


async def _run_job(job: dict) -> None:
    """Run job with pause support."""
    global _current_job, _worker_task, _job_queue

    async def _execute(j: dict) -> None:
        global _current_job
        _current_job = j
        folder_id = str(selected_chat.get("id", "")).replace("-100", "")
        chat_id = selected_chat["id"]
        uid = j.get("user_id", 0)
        
        if folder_id and j.get("chat_title"):
            _save_chat_name(folder_id, j["chat_title"])
        
        try:
            for idx, entry in enumerate(j["entries"]):
                while j.get("paused"):
                    await asyncio.sleep(1)
                
                if j.get("cancelled"):
                    entry["status"] = "failed"
                    entry["reason"] = "Stop"
                else:
                    await _process_id(entry["id"], j, entry, chat_id, uid)
                
                if idx % 5 == 0 or (time() - j.get("_last_render", 0)) > 3:
                    if j.get("is_bulk"):
                        file_type = j.get("file_type", "")
                        await _edit(j["progress_msg"], _render_bulk_progress(j, file_type))
                    else:
                        await _edit(j["progress_msg"], _render_manual_progress(j))
                    j["_last_render"] = time()
                
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            for e in j["entries"]:
                if e["status"] not in ("done", "skipped", "failed"):
                    e["status"] = "failed"
                    e["reason"] = "Stop"
            raise

        finally:
            folder_id = str(selected_chat["id"]).replace("-100", "")
            folder    = os.path.join(DOWNLOAD_BASE, folder_id)
            
            try:
                os.makedirs(folder, exist_ok=True)
            except Exception:
                pass
            
            queue_left = len(_job_queue)
            next_note  = f"\n⏭ {queue_left} queued" if queue_left else ""
            
            summary = (
                _render_job_summary(j) + "\n\n"
                f"{BORDER_THIN}\n"
                f"{ICON_FOLDER} `{folder}`" + next_note
            )
            
            try:
                open_files_markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton("📂 Open Files", callback_data=f"openf:{folder_id}"),
                ]])
                await _edit(j["progress_msg"], summary, reply_markup=open_files_markup)
            except Exception as e:
                log.warning(f"Final edit failed: {e}")
            
            done    = sum(1 for e in j["entries"] if e["status"] == "done")
            skipped = sum(1 for e in j["entries"] if e["status"] == "skipped")
            failed  = sum(1 for e in j["entries"] if e["status"] == "failed")
            log.info(f"Job done: {done}✓ {skipped}⊘ {failed}✗")
            
            await _send_webhook("download_complete", {
                "chat": j["chat_title"],
                "done": done,
                "skipped": skipped,
                "failed": failed,
                "size": sum(e.get('size', 0) for e in j["entries"] if e["status"] == "done"),
            })

    try:
        await _execute(job)
        while _job_queue:
            next_job = _job_queue.pop(0)
            await _edit(next_job["progress_msg"], f"▶️ **Next…**")
            await _execute(next_job)

    except asyncio.CancelledError:
        for pending in _job_queue:
            for e in pending["entries"]:
                e["status"] = "failed"
                e["reason"] = "Stop"
            try:
                await _edit(pending["progress_msg"], "🛑 **Stopped**")
            except Exception:
                pass
        _job_queue.clear()

    finally:
        _current_job = None
        _worker_task = None


# ═══════════════════════════════════════════════════════════════
# Command handlers - Core commands
# ═══════════════════════════════════════════════════════════════

PAGE_SIZE = 8


def _dialog_list_markup(uid: int, page: int = 0, query: str = "") -> tuple[str, InlineKeyboardMarkup]:
    """Dialog list."""
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
            f"{TYPE_ICON.get(d['type'], '💬')} {_truncate(d['title'], BUTTON_TRUNCATE)}",
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
        f"{ICON_FOLDER} Chats | {total}\n"
        + (f"{ICON_SEARCH} `{query}`\n" if query else "")
        + f"Page {page+1}/{total_pages}"
    )
    return header, InlineKeyboardMarkup(buttons)


async def _show_dialog_list(target, uid: int, page: int = 0, query: str = "") -> None:
    """Show dialog list."""
    if not dialogs_cache:
        text = f"{ICON_WARN} Loading…"
        if hasattr(target, "reply"):
            await target.reply(text)
        else:
            try:
                await target.message.edit(text)
            except Exception:
                pass
        return

    text, markup = _dialog_list_markup(uid, page, query)
    if hasattr(target, "reply"):
        await target.reply(text, reply_markup=markup)
    else:
        try:
            await target.message.edit(text, reply_markup=markup)
        except Exception:
            pass


async def cmd_start(_, message: Message):
    global _current_job, _worker_task, _job_queue
    
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    _log_access(uid, "start", "main", message.from_user.username or "")

    if _worker_task and not _worker_task.done():
        if _current_job:
            _current_job["cancelled"] = True
        _worker_task.cancel()
        _worker_task = None
        _current_job = None
    _job_queue.clear()

    buttons = []
    if selected_chat.get("id"):
        title = _truncate(selected_chat['title'], BUTTON_TRUNCATE)
        buttons.append([InlineKeyboardButton(
            f"✅ Resume: {title}",
            callback_data="welcome:resume",
        )])
    buttons.extend([
        [InlineKeyboardButton("📂 Select Chat", callback_data="welcome:setchat")],
        [InlineKeyboardButton("📖 Help & Guide", callback_data="welcome:help")],
    ])

    await message.reply(
        f"🤖 **GHOSTFETCH - Media Downloader**\n"
        f"{BORDER_THICK}\n\n"
        
        f"Welcome! 👋\n"
        f"Download media from Telegram chats effortlessly.\n\n"
        
        f"⚡ **Features:**\n"
        f"  ✓ Bulk download by type (videos, audio, photos, etc)\n"
        f"  ✓ Search files by name keyword\n"
        f"  ✓ Download by date range\n"
        f"  ✓ Download by file size\n"
        f"  ✓ Manual ID selection\n"
        f"  ✓ Concurrent downloads (3 at a time)\n"
        f"  ✓ Pause/Resume support\n"
        f"  ✓ Queue management\n\n"
        
        f"📊 **Quick Stats:**\n"
        f"  • Chats Available: {len(dialogs_cache)}\n"
        f"  • Active Queued Jobs: {len(_job_queue)}\n"
        f"  • Status: {'🟢 Ready' if not _current_job else '🔴 Downloading'}\n\n"
        
        f"🚀 **Getting Started:**\n"
        f"1️⃣ Tap '📂 Select Chat' to choose source\n"
        f"2️⃣ Choose a download mode\n"
        f"3️⃣ Follow the prompts\n"
        f"4️⃣ Your files will be downloaded!\n\n"
        
        f"💡 **Tip:** Use /help for detailed command list\n"
        f"{BORDER_THIN}",
        reply_markup=InlineKeyboardMarkup(buttons),
        disable_web_page_preview=True,
    )


async def cmd_setchat(_, message: Message):
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    user_state[uid] = ("selecting", time())
    await _show_dialog_list(message, uid)


async def cb_welcome(_, query: CallbackQuery):
    """Welcome buttons with improved messaging."""
    uid    = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    action = query.data.split(":")[1]

    if action == "resume":
        if not selected_chat.get("id"):
            await query.answer("No session.", show_alert=True)
            return
        user_state[uid] = ("idle", time())
        await query.message.edit(
            f"✅ **Session Resumed!**\n\n"
            f"{ICON_FOLDER} **Chat:** {_truncate(selected_chat['title'], TEXT_TRUNCATE_MED)}\n\n"
            f"Ready to download!\n"
            f"What would you like to do?"
        )
        await query.answer()
        await _show_download_options(query.message, uid, is_callback=True)

    elif action == "setchat":
        user_state[uid] = ("selecting", time())
        await query.answer()
        await _show_dialog_list(query, uid)

    elif action == "help":
        await query.answer()
        # Call help function directly
        await cmd_help(None, query.message)


async def _show_download_options(msg_or_query, uid: int, is_callback: bool = False) -> None:
    """Download options."""
    if not selected_chat.get("id"):
        text = f"{ICON_WARN} No chat selected"
        if hasattr(msg_or_query, "reply"):
            await msg_or_query.reply(text)
        return
    
    buttons = [
        [InlineKeyboardButton("🆔 Manual ID", callback_data="dlopt:manual")],
        [InlineKeyboardButton(f"{ICON_BULK} Bulk Download", callback_data="dlopt:bulk")],
        [InlineKeyboardButton(f"{ICON_SEARCH} Search Name", callback_data="dlopt:search")],
        [InlineKeyboardButton(f"📅 By Date", callback_data="dlopt:date")],
        [InlineKeyboardButton(f"📊 By Size", callback_data="dlopt:size")],
    ]
    
    title = _truncate(selected_chat['title'], TEXT_TRUNCATE_SHORT)
    
    text = (
        f"{ICON_FOLDER} {title}\n"
        f"{BORDER_THIN}\n"
        f"Mode:"
    )
    
    markup = InlineKeyboardMarkup(buttons)
    
    if is_callback:
        if hasattr(msg_or_query, 'edit'):
            try:
                await msg_or_query.edit(text, reply_markup=markup)
            except Exception:
                pass
        else:
            await msg_or_query.reply(text, reply_markup=markup)
    else:
        await msg_or_query.reply(text, reply_markup=markup)


async def cmd_options(_, message: Message):
    """Download options."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    if not selected_chat.get("id"):
        return await message.reply(f"{ICON_WARN} No chat\n\nUse /start")
    
    await _show_download_options(message, uid)


async def cb_dlopt(_, query: CallbackQuery):
    """Download option selection."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    mode = query.data.split(":")[1]
    
    if mode == "manual":
        await query.message.edit(
            f"🆔 **Manual IDs**\n\n"
            f"Send IDs:\n"
            f"`26473` or `26473 26570`\n\n"
            f"✓ Skip already downloaded"
        )
        user_state[uid] = ("download_manual", time())
    
    elif mode == "bulk":
        await query.message.edit(
            f"{ICON_BULK} **Bulk Download**\n"
            f"{BORDER_THIN}\n"
            f"Select type:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(ft, callback_data=f"bulk:{ft}")] 
                for ft in FILE_TYPES.keys()
            ] + [
                [InlineKeyboardButton("⬅️ Back", callback_data="back:options")]
            ])
        )
        user_state[uid] = ("download_bulk", time())
    
    elif mode == "search":
        await query.message.edit(
            f"{ICON_SEARCH} **Search**\n\n"
            f"Send keyword:"
        )
        user_state[uid] = ("download_search", time())
    
    elif mode == "date":
        await query.message.edit(
            f"📅 **By Date**\n\n"
            f"Send range:\n"
            f"`2024-01-01 to 2024-12-31`"
        )
        user_state[uid] = ("download_date", time())
    
    elif mode == "size":
        await query.message.edit(
            f"📊 **By Size**\n\n"
            f"Send range (MB):\n"
            f"`1 to 100` or `100` (min only)"
        )
        user_state[uid] = ("download_size", time())
    
    await query.answer()


async def cb_bulk_select(_, query: CallbackQuery):
    """Bulk file type selection."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    _add_user_activity(uid)
    file_type = query.data.split(":")[1]
    
    extensions = ", ".join(FILE_TYPES[file_type][:3])
    if len(FILE_TYPES[file_type]) > 3:
        extensions += f", +{len(FILE_TYPES[file_type]) - 3}"
    
    await query.message.edit(
        f"✅ {file_type}\n"
        f"{BORDER_THIN}\n"
        f"{extensions}\n\n"
        f"Start?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Start", callback_data=f"bulkstart:{file_type}"),
             InlineKeyboardButton("❌ Cancel", callback_data="back:options")]
        ])
    )
    await query.answer()


async def cb_bulk_start(_, query: CallbackQuery):
    """Start bulk download."""
    global _current_job, _worker_task, _job_queue
    
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    file_type = query.data.split(":")[1]
    
    await query.answer("Searching…")
    
    extensions = FILE_TYPES[file_type]
    msg_ids = await _find_messages_by_extensions(selected_chat["id"], extensions)
    
    if not msg_ids:
        await query.message.edit(f"{ICON_WARN} No files")
        return
    
    entries = [{"id": mid, "status": "queued"} for mid in msg_ids[:MAX_ENTRIES_DISPLAY]]
    if len(msg_ids) > MAX_ENTRIES_DISPLAY:
        log.warning(f"Truncated {len(msg_ids)} to {MAX_ENTRIES_DISPLAY}")
    
    progress_msg = await query.message.edit(
        f"{ICON_BULK} **Downloading…**\n"
        f"`0/{len(entries)}` files"
    )
    
    job = {
        "ids":          [e["id"] for e in entries],
        "entries":      entries,
        "chat_title":   selected_chat["title"],
        "chat_id":      selected_chat["id"],
        "user_id":      uid,
        "progress_msg": progress_msg,
        "start_time":   time(),
        "cancelled":    False,
        "paused":       False,
        "is_bulk":      True,
        "file_type":    file_type,
        "_last_render": time(),
    }
    
    if _current_job or (_worker_task and not _worker_task.done()):
        _job_queue.append(job)
        log.info(f"Bulk queued: {len(entries)} files")
    else:
        _current_job = job
        _worker_task = asyncio.create_task(_run_job(job))
        log.info(f"Bulk started: {len(entries)} files")
    
    user_state[uid] = ("idle", time())


async def cmd_help(_, message: Message):
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    await message.reply(
        f"📖 **GHOSTFETCH HELP GUIDE**\n"
        f"{BORDER_THICK}\n\n"
        
        f"🎯 **QUICK START**\n"
        f"{BORDER_THIN}\n"
        f"1. /start → Select a chat/channel\n"
        f"2. /options → Choose download mode\n"
        f"3. Send input based on selected mode\n"
        f"4. Download starts automatically!\n\n"
        
        f"📥 **DOWNLOAD MODES**\n"
        f"{BORDER_THIN}\n"
        f"🆔 **Manual ID**\n"
        f"   Download specific messages by ID\n"
        f"   Usage: Send `123 456 789`\n"
        f"   Multiple IDs separated by spaces\n\n"
        
        f"{ICON_BULK} **Bulk Download**\n"
        f"   Download all files of one type\n"
        f"   Types: Videos, Audio, Images, Docs, Archives, Apps\n"
        f"   Searches entire chat history\n\n"
        
        f"{ICON_SEARCH} **Search by Name**\n"
        f"   Find files matching keywords\n"
        f"   Usage: Send keyword (e.g., 'movie', 'song')\n"
        f"   Shows matching results as buttons\n\n"
        
        f"📅 **By Date Range**\n"
        f"   Download files from specific dates\n"
        f"   Usage: `2024-01-01 to 2024-12-31`\n"
        f"   Format: YYYY-MM-DD to YYYY-MM-DD\n\n"
        
        f"📊 **By File Size**\n"
        f"   Download files within size range\n"
        f"   Usage: `10 to 500` (in MB)\n"
        f"   Or just `100` for min size only\n\n"
        
        f"⚙️ **COMMANDS**\n"
        f"{BORDER_THIN}\n"
        f"/start - Start bot & select chat\n"
        f"/options - Choose download mode\n"
        f"/files - Browse downloaded files\n"
        f"/stats - View download statistics\n"
        f"/history - Download history\n"
        f"/settings - User preferences\n"
        f"/api - Generate API token\n"
        f"/pause - Pause current download\n"
        f"/resume - Resume paused download\n"
        f"/queue - View download queue\n"
        f"/killall - Stop all downloads\n"
        f"/resetstats - Reset statistics\n"
        f"/help - Show this help message\n\n"
        
        f"💡 **TIPS & TRICKS**\n"
        f"{BORDER_THIN}\n"
        f"→ Downloads are queued automatically\n"
        f"→ Pause/resume at any time\n"
        f"→ Check /queue to see pending jobs\n"
        f"→ Use /files to manage downloads\n"
        f"→ Concurrent downloads limit: 3\n"
        f"→ Max folder size: 10GB\n\n"
        
        f"📝 **NOTES**\n"
        f"{BORDER_THIN}\n"
        f"✓ Only bot owner can use commands\n"
        f"✓ Rate limit: 30 req/min, 300 req/hour\n"
        f"✓ Files saved in: {DOWNLOAD_BASE}\n"
        f"✓ Session auto-saved for quick resume",
        disable_web_page_preview=True,
    )


# ═══════════════════════════════════════════════════════════════
# Advanced Features - Settings, API
# ═══════════════════════════════════════════════════════════════

async def cmd_settings(_, message: Message):
    """User settings."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    prefs = _load_preferences(uid)
    
    text = (
        f"⚙️ **Settings**\n"
        f"{BORDER_THIN}\n\n"
        f"Theme: {prefs.get('theme', 'light')}\n"
        f"Notifications: {'On' if prefs.get('notifications', True) else 'Off'}\n"
        f"Concurrent Downloads: {prefs.get('concurrent_downloads', 1)}\n"
    )
    
    buttons = [
        [InlineKeyboardButton("🌙 Theme", callback_data="sett:theme")],
        [InlineKeyboardButton("🔔 Notifications", callback_data="sett:notify")],
        [InlineKeyboardButton("⚡ Speed", callback_data="sett:speed")],
    ]
    
    await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))


async def cmd_api(_, message: Message):
    """API token management."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    token = _generate_api_token(uid)
    
    text = (
        f"{ICON_LINK} **API Token**\n"
        f"{BORDER_THIN}\n\n"
        f"Your token:\n"
        f"`{token}`\n\n"
        f"Use for REST API calls.\n"
        f"Keep it secret!"
    )
    
    await message.reply(text)


async def cmd_queue_preview(_, message: Message):
    """Show queue preview."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    text = _render_queue_preview(_job_queue)
    await message.reply(text)


# ═══════════════════════════════════════════════════════════════
# File Browser
# ═══════════════════════════════════════════════════════════════

FILES_PAGE_SIZE = 5


def _scan_folders() -> list[dict]:
    """Scan folders safely."""
    result = []
    if not os.path.exists(DOWNLOAD_BASE):
        return result
    try:
        for folder_id in sorted(os.listdir(DOWNLOAD_BASE)):
            full = os.path.join(DOWNLOAD_BASE, folder_id)
            if not os.path.isdir(full):
                continue
            try:
                files = [f for f in os.listdir(full) 
                        if os.path.isfile(os.path.join(full, f)) and not os.path.islink(os.path.join(full, f))]
                total = sum(os.path.getsize(os.path.join(full, f)) for f in files)
                result.append({
                    "folder_id":  folder_id,
                    "title":      _folder_title(folder_id),
                    "file_count": len(files),
                    "total_size": total,
                    "path":       full,
                })
            except Exception as e:
                log.warning(f"Error scanning folder: {e}")
    except Exception as e:
        log.error(f"Scan error: {e}")
    
    return result


def _scan_files_in(folder_path: str, sort_by: str = "date") -> list[dict]:
    """Scan files with sorting."""
    files = []
    if not os.path.isdir(folder_path):
        return files
    try:
        for fname in os.listdir(folder_path):
            fpath = os.path.join(folder_path, fname)
            try:
                if os.path.isfile(fpath) and not os.path.islink(fpath):
                    stat = os.stat(fpath)
                    files.append({
                        "path": fpath,
                        "name": fname,
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                    })
            except Exception as e:
                log.debug(f"Error with file: {e}")
        
        if sort_by == "size":
            files.sort(key=lambda x: x["size"], reverse=True)
        elif sort_by == "name":
            files.sort(key=lambda x: x["name"])
        else:
            files.sort(key=lambda x: x["mtime"], reverse=True)
    except Exception as e:
        log.error(f"Scan error: {e}")
    
    return files


def _folders_markup(folders: list) -> tuple[str, InlineKeyboardMarkup]:
    total_files = sum(f["file_count"] for f in folders)
    total_size  = sum(f["total_size"] for f in folders)
    buttons = []
    for i, f in enumerate(folders):
        title = _truncate(f['title'], TEXT_TRUNCATE_SHORT)
        label = f"📁 {title}"
        info = f"  {f['file_count']}f {DOT} {_sz(f['total_size'])}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"fd:{i}")])
        buttons.append([InlineKeyboardButton(info, callback_data=f"fd:{i}")])
    
    buttons.append([InlineKeyboardButton("🗑 Wipe", callback_data="fwipe")])
    header = (
        f"📂 Downloads\n"
        f"{total_files}f {DOT} {_sz(total_size)}"
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
        short  = _truncate(f["name"], TEXT_TRUNCATE_MED)
        emoji  = _file_emoji(f["name"])
        buttons.append([
            InlineKeyboardButton(f"{emoji} {short}", callback_data=f"fl:{fi}:{actual}"),
            InlineKeyboardButton("🗑", callback_data=f"fdel:{fi}:{actual}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"fp:{fi}:{page - 1}"))
    nav.append(InlineKeyboardButton("Back", callback_data="fb"))
    if start + FILES_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"fp:{fi}:{page + 1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("🗑 Delete", callback_data=f"fdeldir:{fi}")])

    header = (
        f"📁 {_truncate(folder['title'], TEXT_TRUNCATE_SHORT)}\n"
        f"{total}f {DOT} {page + 1}/{total_pages}"
    )
    return header, InlineKeyboardMarkup(buttons)


async def _send_folder_view(target, uid: int) -> None:
    """Send folder list."""
    folders = _scan_folders()
    _files_nav[uid] = {"folders": folders}
    
    if not folders:
        text = "📭 Empty"
        if hasattr(target, "reply"):
            await target.reply(text)
        else:
            try:
                await target.message.edit(text)
            except Exception:
                pass
        return
    text, markup = _folders_markup(folders)
    if hasattr(target, "reply"):
        await target.reply(text, reply_markup=markup)
    else:
        try:
            await target.message.edit(text, reply_markup=markup)
        except Exception:
            pass


async def _send_file_view(target, uid: int, fi: int, page: int = 0, reply: bool = False) -> None:
    """Send file list."""
    nav     = _files_nav.get(uid, {})
    folders = nav.get("folders") or _scan_folders()
    
    if fi >= len(folders):
        if hasattr(target, "answer"):
            await target.answer("Not found.", show_alert=True)
        return
    
    folder = folders[fi]
    files  = _scan_files_in(folder["path"])
    folder["file_count"] = len(files)
    folder["total_size"] = sum(f["size"] for f in files)
    _files_nav[uid] = {"folders": folders, "fi": fi, "files": files, "page": page}

    if not files:
        text   = f"📭 Empty"
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="fb")]])
        if reply or hasattr(target, "reply"):
            await target.reply(text, reply_markup=markup)
        else:
            try:
                await target.message.edit(text, reply_markup=markup)
            except Exception:
                pass
        return

    text, markup = _folder_files_markup(folder, fi, files, page)
    if reply or (hasattr(target, "reply") and not hasattr(target, "data")):
        await target.reply(text, reply_markup=markup)
    else:
        try:
            await target.message.edit(text, reply_markup=markup)
        except Exception:
            pass


async def cmd_files(_, message: Message):
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    await _send_folder_view(message, uid)


async def cb_open_folder(_, query: CallbackQuery):
    """Open folder."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        fi = int(query.data.split(":")[1])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return
    
    await _send_file_view(query, uid, fi)
    await query.answer()


async def cb_files_page(_, query: CallbackQuery):
    """Paginate files."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        parts = query.data.split(":")
        fi    = int(parts[1])
        page  = int(parts[2])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return
    
    await _send_file_view(query, uid, fi, page)
    await query.answer()


async def cb_back_to_folders(_, query: CallbackQuery):
    """Back to folders."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    await _send_folder_view(query, uid)
    await query.answer()


async def cb_send_file(_, query: CallbackQuery):
    """Send file."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        parts = query.data.split(":")
        fi    = int(parts[1])
        idx   = int(parts[2])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return

    nav   = _files_nav.get(uid, {})
    files = nav.get("files")
    
    if not files or idx >= len(files):
        await query.answer("Expired.", show_alert=True)
        return

    entry = files[idx]
    if not os.path.exists(entry["path"]):
        await query.answer("Deleted.", show_alert=True)
        return

    try:
        await query.answer("Sending…")
        await query.message.reply_document(
            entry["path"],
            caption=f"`{_truncate(entry['name'], BUTTON_TRUNCATE)}`",
        )
        
        page = nav.get("page", 0)
        fi = nav.get("fi", 0)
        await _send_file_view(query.message, uid, fi, page, reply=True)
    except Exception as e:
        log.error(f"Send error: {e}")
        _log_error(str(e), uid, "send_file_error")
        await query.answer(f"Error", show_alert=True)


async def cb_delete_file(_, query: CallbackQuery):
    """Delete file."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        parts = query.data.split(":")
        fi    = int(parts[1])
        idx   = int(parts[2])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return

    nav   = _files_nav.get(uid, {})
    files = nav.get("files")
    
    if not files or idx >= len(files):
        await query.answer("Expired.", show_alert=True)
        return

    entry = files[idx]
    if os.path.exists(entry["path"]):
        try:
            os.remove(entry["path"])
            log.info(f"Deleted: {entry['path']}")
        except Exception as e:
            log.error(f"Delete error: {e}")
            _log_error(str(e), uid, "delete_file_error")
            await query.answer(f"Error", show_alert=True)
            return

    await query.answer("Deleted")
    page = nav.get("page", 0)
    fi = nav.get("fi", 0)
    await _send_file_view(query, uid, fi, page)


async def cb_delete_folder(_, query: CallbackQuery):
    """Confirm delete folder."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        fi = int(query.data.split(":")[1])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return

    nav     = _files_nav.get(uid, {})
    folders = nav.get("folders")
    
    if not folders or fi >= len(folders):
        await query.answer("Not found.", show_alert=True)
        return

    folder = folders[fi]
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes", callback_data=f"fdeldirok:{fi}"),
        InlineKeyboardButton("❌ No", callback_data="fb"),
    ]])
    await query.message.edit(
        f"Delete?\n\n{folder['file_count']}f {DOT} {_sz(folder['total_size'])}",
        reply_markup=markup,
    )
    await query.answer()


async def cb_delete_folder_confirm(_, query: CallbackQuery):
    """Delete folder confirm."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        fi = int(query.data.split(":")[1])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return

    nav     = _files_nav.get(uid, {})
    folders = nav.get("folders")
    
    if not folders or fi >= len(folders):
        await query.answer("Not found.", show_alert=True)
        return

    folder = folders[fi]
    if os.path.exists(folder["path"]):
        try:
            shutil.rmtree(folder["path"])
            log.info(f"Deleted folder: {folder['path']}")
        except Exception as e:
            log.error(f"Folder delete error: {e}")
            _log_error(str(e), uid, "delete_folder_error")
            await query.answer(f"Error", show_alert=True)
            return

    await query.answer("Deleted")
    await _send_folder_view(query, uid)


async def cb_wipe_all(_, query: CallbackQuery):
    """Confirm wipe all."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    _add_user_activity(uid)
    
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("💀 Yes", callback_data="fwipeok"),
        InlineKeyboardButton("❌ No", callback_data="fb"),
    ]])
    await query.message.edit(
        "Wipe ALL?",
        reply_markup=markup,
    )
    await query.answer()


async def cb_wipe_all_confirm(_, query: CallbackQuery):
    """Wipe all."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    _add_user_activity(uid)
    
    if os.path.exists(DOWNLOAD_BASE):
        try:
            shutil.rmtree(DOWNLOAD_BASE)
            os.makedirs(DOWNLOAD_BASE, exist_ok=True)
            log.info("Wiped all")
        except Exception as e:
            log.error(f"Wipe error: {e}")
            _log_error(str(e), uid, "wipe_all_error")
            await query.answer(f"Error", show_alert=True)
            return
    
    await query.answer("Wiped")
    await query.message.edit("📭 Cleared")


# ═══════════════════════════════════════════════════════════════
# Search handlers with caching
# ═══════════════════════════════════════════════════════════════

async def cb_search_download(_, query: CallbackQuery):
    """Download from search."""
    global _current_job, _worker_task, _job_queue
    
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        msg_id = int(query.data.split(":")[1])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return
    
    if not _check_folder_size():
        return await query.answer("Disk full", show_alert=True)
    
    await query.answer("Adding…")
    
    entries = [{"id": msg_id, "status": "queued"}]
    
    if _current_job or (_worker_task and not _worker_task.done()):
        pos = len(_job_queue) + 1
        progress_msg = await query.message.reply(
            f"📋 #{pos} {DOT} 1 ID"
        )
        job = {
            "ids":          [msg_id],
            "entries":      entries,
            "chat_title":   selected_chat["title"],
            "chat_id":      selected_chat["id"],
            "user_id":      uid,
            "progress_msg": progress_msg,
            "start_time":   time(),
            "cancelled":    False,
            "paused":       False,
            "is_bulk":      False,
            "_last_render": time(),
        }
        _job_queue.append(job)
        log.info(f"Queued search: {msg_id}")
    else:
        progress_msg = await query.message.reply(
            f"{ICON_DL} **Downloading…**"
        )
        job = {
            "ids":          [msg_id],
            "entries":      entries,
            "chat_title":   selected_chat["title"],
            "chat_id":      selected_chat["id"],
            "user_id":      uid,
            "progress_msg": progress_msg,
            "start_time":   time(),
            "cancelled":    False,
            "paused":       False,
            "is_bulk":      False,
            "_last_render": time(),
        }
        _current_job = job
        _worker_task = asyncio.create_task(_run_job(job))
        log.info(f"Download search: {msg_id}")


async def cb_select_chat(_, query: CallbackQuery):
    """Select chat."""
    global selected_chat

    uid  = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        idx = int(query.data.split(":")[1])
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return
    
    pool = search_results.get(uid, [])

    if idx >= len(pool):
        return await query.answer("Expired", show_alert=True)

    chosen = pool[idx]
    selected_chat = {"id": chosen["id"], "title": chosen["title"]}
    _save_session()

    user_state[uid] = ("idle", time())
    search_results.pop(uid, None)

    await query.message.edit(f"✅ {_truncate(chosen['title'], BUTTON_TRUNCATE)}")
    await query.answer()
    await _show_download_options(query.message, uid, is_callback=True)
    log.info(f"Chat: {chosen['title']}")


async def cb_page(_, query: CallbackQuery):
    """Pagination."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        parts = query.data.split(":", 2)
        page  = int(parts[1])
        q     = parts[2] if len(parts) > 2 else ""
    except (ValueError, IndexError):
        await query.answer("Invalid", show_alert=True)
        return
    
    await _show_dialog_list(query, uid, page=page, query=q)
    await query.answer()


async def cb_open_files_from_job(_, query: CallbackQuery):
    """Open files from job."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        folder_id = query.data.split(":")[1]
    except IndexError:
        await query.answer("Invalid", show_alert=True)
        return
    
    folders   = _scan_folders()
    fi = next((i for i, f in enumerate(folders) if f["folder_id"] == folder_id), None)
    if fi is None:
        await query.answer("Not found", show_alert=True)
        return
    await query.answer()
    await _send_file_view(query.message, uid, fi, page=0, reply=True)


async def cb_back(_, query: CallbackQuery):
    """Back button."""
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("Not authorized", show_alert=True)
    
    if not _is_callback_valid(query.id):
        return await query.answer("Expired", show_alert=True)
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await query.answer(msg_text, show_alert=True)
    
    _add_user_activity(uid)
    
    try:
        action = query.data.split(":")[1]
    except IndexError:
        await query.answer("Invalid", show_alert=True)
        return
    
    if action == "options":
        await _show_download_options(query.message, uid, is_callback=True)
    
    await query.answer()


# ══════════════════════════════════════��════════════════════════
# Download management commands
# ═══════════════════════════════════════════════════════════════

async def cmd_killall(_, message: Message):
    global _worker_task, _current_job, _job_queue
    
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)

    if not (_worker_task and not _worker_task.done()) and not _job_queue:
        return await message.reply(f"{ICON_INFO} Nothing")

    if _current_job:
        _current_job["cancelled"] = True
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()

    _worker_task = None
    _current_job = None
    _job_queue.clear()

    await message.reply("🛑 **Stopped**")
    log.info("killall")


async def cmd_pause(_, message: Message):
    """Pause current download."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    global _current_job
    if not _current_job:
        return await message.reply(f"{ICON_INFO} No active job")
    
    _current_job["paused"] = True
    await message.reply(f"{ICON_PAUSE} **Paused**")


async def cmd_resume(_, message: Message):
    """Resume paused download."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    global _current_job
    if not _current_job:
        return await message.reply(f"{ICON_INFO} No active job")
    
    _current_job["paused"] = False
    await message.reply(f"▶️ **Resumed**")


async def cmd_stats(_, message: Message):
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    analytics = _get_analytics(uid)
    
    dl_root = DOWNLOAD_BASE if os.path.exists(DOWNLOAD_BASE) else "."
    try:
        total, used, free = shutil.disk_usage(dl_root)
        cpu  = psutil.cpu_percent(interval=0.5)
        ram  = psutil.virtual_memory().percent
    except Exception as e:
        return await message.reply(f"Error: {e}")

    chat_line = (
        _truncate(selected_chat['title'], TEXT_TRUNCATE_SHORT)
        if selected_chat.get("id") else "–"
    )
    job_line = (
        f"{len(_current_job['ids'])} IDs"
        if _current_job and "ids" in _current_job else "Idle"
    )

    text = (
        f"📊 **Statistics**\n"
        f"{BORDER_THIN}\n"
        f"Chat: {chat_line}\n"
        f"Status: {job_line}\n\n"
        f"Downloads: {analytics.get('downloads', 0)}\n"
        f"Total Size: {_sz(analytics.get('size', 0))}\n"
        f"Avg Speed: {_speed(analytics.get('avg_speed', 0))}\n"
        f"Errors: {analytics.get('errors', 0)}\n\n"
        f"Disk: {_sz(used)}/{_sz(total)}\n"
        f"CPU: {cpu:.1f}% {DOT} RAM: {ram:.1f}%"
    )
    
    await message.reply(text)


async def cmd_history(_, message: Message):
    """Show download history."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    history = _load_history()
    
    text = (
        f"📜 **Download History**\n"
        f"{BORDER_THIN}\n\n"
        f"Total: {history['total_downloads']} downloads\n"
        f"Size: {_sz(history['total_size'])}\n"
        f"Time: {_elapsed(time() - history['total_time'])}\n\n"
    )
    
    if history['downloads']:
        text += "Recent:\n"
        for dl in history['downloads'][-5:]:
            text += f"• {dl['name'][:20]} {_sz(dl['size'])}\n"
    
    await message.reply(_truncate_message(text))


async def cmd_resetstats(_, message: Message):
    """Reset download stats."""
    uid = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    default_history = {"total_downloads": 0, "total_size": 0, "total_time": 0, "downloads": []}
    _save_history(default_history)
    await message.reply("📜 Stats reset")


# ═══════════════════════════════════════════════════════════════
# Main text handler with date/size filtering
# ═══════════════════════════════════════════════════════════════

async def handle_text(_, message: Message):
    global _current_job, _worker_task, _job_queue

    uid   = message.from_user.id
    if not _is_owner(uid):
        return await message.reply("Not authorized")
    
    allowed, msg_text = _check_rate_limit(uid)
    if not allowed:
        return await message.reply(msg_text)
    
    _add_user_activity(uid)
    
    text  = message.text.strip()
    state_data = user_state.get(uid, ("idle", time()))
    if isinstance(state_data, tuple):
        state, _ = state_data
    else:
        state = state_data

    if state == "selecting":
        await _show_dialog_list(message, uid, page=0, query=text)
        return

    if state == "download_manual":
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
            return await message.reply(f"{ICON_ERROR} Invalid: {', '.join(bad[:5])}")
        if not msg_ids:
            return await message.reply("Send IDs")

        if not _check_folder_size():
            return await message.reply(f"{ICON_WARN} Disk full")

        entries = [{"id": mid, "status": "queued"} for mid in msg_ids]

        if _current_job or (_worker_task and not _worker_task.done()):
            pos = len(_job_queue) + 1
            progress_msg = await message.reply(
                f"📋 #{pos} {DOT} {len(msg_ids)} IDs"
            )
            job = {
                "ids":          msg_ids,
                "entries":      entries,
                "chat_title":   selected_chat["title"],
                "chat_id":      selected_chat["id"],
                "user_id":      uid,
                "progress_msg": progress_msg,
                "start_time":   time(),
                "cancelled":    False,
                "paused":       False,
                "is_bulk":      False,
                "_last_render": time(),
            }
            _job_queue.append(job)
            log.info(f"Queued: {len(msg_ids)} IDs")
            return

        progress_msg = await message.reply(
            f"{ICON_DL} **Downloading…**\n"
            f"{len(msg_ids)} IDs"
        )
        job = {
            "ids":          msg_ids,
            "entries":      entries,
            "chat_title":   selected_chat["title"],
            "chat_id":      selected_chat["id"],
            "user_id":      uid,
            "progress_msg": progress_msg,
            "start_time":   time(),
            "cancelled":    False,
            "paused":       False,
            "is_bulk":      False,
            "_last_render": time(),
        }
        _current_job = job
        _worker_task = asyncio.create_task(_run_job(job))
        user_state[uid] = ("idle", time())
        return

    if state == "download_search":
        query = text.strip()
        if not query:
            return await message.reply("Send keyword")
        
        await message.reply(f"{ICON_SEARCH} Searching…")
        
        matching = await _find_messages_by_name(selected_chat["id"], query)
        
        if not matching:
            return await message.reply(f"{ICON_WARN} Not found")
        
        buttons = []
        for i, item in enumerate(matching[:12]):
            short = _truncate(item["name"], TEXT_TRUNCATE_MED)
            emoji = _file_emoji(item["name"])
            buttons.append([
                InlineKeyboardButton(
                    f"{emoji} {short}",
                    callback_data=f"searchdl:{item['id']}"
                )
            ])
        
        if len(matching) > 12:
            buttons.append([InlineKeyboardButton(f"… +{len(matching) - 12} more", callback_data=f"search:more:{query}")])
        
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="back:options")])
        
        await message.reply(
            f"{ICON_SEARCH} {len(matching)} found",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        user_state[uid] = ("idle", time())
        return

    if state == "download_date":
        try:
            parts = text.split(" to ")
            if len(parts) == 2:
                start_date = datetime.strptime(parts[0].strip(), "%Y-%m-%d")
                end_date = datetime.strptime(parts[1].strip(), "%Y-%m-%d")
            else:
                return await message.reply("Format: `2024-01-01 to 2024-12-31`")
            
            await message.reply(f"📅 Searching…")
            msg_ids = await _find_messages_by_date_range(selected_chat["id"], start_date, end_date)
            
            if not msg_ids:
                return await message.reply(f"{ICON_WARN} No files found")
            
            entries = [{"id": mid, "status": "queued"} for mid in msg_ids]
            progress_msg = await message.reply(f"{ICON_DL} **Downloading…**\n{len(msg_ids)} IDs")
            job = {
                "ids": msg_ids,
                "entries": entries,
                "chat_title": selected_chat["title"],
                "chat_id": selected_chat["id"],
                "user_id": uid,
                "progress_msg": progress_msg,
                "start_time": time(),
                "cancelled": False,
                "paused": False,
                "is_bulk": False,
                "_last_render": time(),
            }
            if _current_job or (_worker_task and not _worker_task.done()):
                _job_queue.append(job)
            else:
                _current_job = job
                _worker_task = asyncio.create_task(_run_job(job))
            user_state[uid] = ("idle", time())
            
        except ValueError:
            return await message.reply("Invalid date format")

    if state == "download_size":
        try:
            parts = text.split(" to ")
            if len(parts) == 2:
                min_size = int(parts[0].strip()) * 1024 * 1024
                max_size = int(parts[1].strip()) * 1024 * 1024
            else:
                min_size = int(parts[0].strip()) * 1024 * 1024
                max_size = None
            
            await message.reply(f"📊 Searching…")
            msg_ids = await _find_messages_by_size(selected_chat["id"], min_size, max_size)
            
            if not msg_ids:
                return await message.reply(f"{ICON_WARN} No files found")
            
            entries = [{"id": mid, "status": "queued"} for mid in msg_ids]
            progress_msg = await message.reply(f"{ICON_DL} **Downloading…**\n{len(msg_ids)} IDs")
            job = {
                "ids": msg_ids,
                "entries": entries,
                "chat_title": selected_chat["title"],
                "chat_id": selected_chat["id"],
                "user_id": uid,
                "progress_msg": progress_msg,
                "start_time": time(),
                "cancelled": False,
                "paused": False,
                "is_bulk": False,
                "_last_render": time(),
            }
            if _current_job or (_worker_task and not _worker_task.done()):
                _job_queue.append(job)
            else:
                _current_job = job
                _worker_task = asyncio.create_task(_run_job(job))
            user_state[uid] = ("idle", time())
            
        except ValueError:
            return await message.reply("Invalid size format")

    if not selected_chat.get("id"):
        return await message.reply(f"{ICON_WARN} No chat")

    await message.reply("Send /options")


# ═══════════════════════════════════════════════════════════════
# Bot initialization
# ═══════════════════════════════════════════════════════════════

async def _set_bot_commands() -> None:
    """Register bot commands."""
    try:
        await bot.set_bot_commands([
            BotCommand("start", "Start bot"),
            BotCommand("options", "Download options"),
            BotCommand("files", "Browse downloads"),
            BotCommand("stats", "Show statistics"),
            BotCommand("history", "Download history"),
            BotCommand("settings", "User settings"),
            BotCommand("api", "API token"),
            BotCommand("pause", "Pause download"),
            BotCommand("resume", "Resume download"),
            BotCommand("queue", "Show queue"),
            BotCommand("killall", "Stop all downloads"),
            BotCommand("resetstats", "Reset statistics"),
            BotCommand("help", "Help menu"),
        ])
        log.info("Bot commands registered")
    except Exception as e:
        log.error(f"Set commands failed: {e}")


async def main():
    """Main entry point - initializes and starts the bot."""
    global bot, user_client, http_session, _user_cleanup_task, _api_tokens
    
    # Initialize database
    _init_analytics_db()
    _api_tokens = _load_api_tokens()
    
    try:
        # Create clients
        log.info("Creating Pyrogram clients...")
        bot = Client(
            "bot_session",
            api_id=PyroConf.API_ID,
            api_hash=PyroConf.API_HASH,
            bot_token=PyroConf.BOT_TOKEN
        )
        user_client = Client(
            "user_session",
            api_id=PyroConf.API_ID,
            api_hash=PyroConf.API_HASH,
            session_string=PyroConf.SESSION_STRING
        )
        
        # Create HTTP session for webhooks
        http_session = aiohttp.ClientSession()
        
        # Start clients
        log.info("Starting clients...")
        await bot.start()
        await user_client.start()
        log.info("✅ Clients started successfully!")
        
        # Load data
        _load_session()
        await _load_dialogs()
        await _set_bot_commands()
        
        # Register message handlers
        bot.add_handler(MessageHandler(cmd_start, filters.command("start")))
        bot.add_handler(MessageHandler(cmd_setchat, filters.command("setchat")))
        bot.add_handler(MessageHandler(cmd_options, filters.command("options")))
        bot.add_handler(MessageHandler(cmd_files, filters.command("files")))
        bot.add_handler(MessageHandler(cmd_stats, filters.command("stats")))
        bot.add_handler(MessageHandler(cmd_history, filters.command("history")))
        bot.add_handler(MessageHandler(cmd_settings, filters.command("settings")))
        bot.add_handler(MessageHandler(cmd_api, filters.command("api")))
        bot.add_handler(MessageHandler(cmd_help, filters.command("help")))
        bot.add_handler(MessageHandler(cmd_killall, filters.command("killall")))
        bot.add_handler(MessageHandler(cmd_pause, filters.command("pause")))
        bot.add_handler(MessageHandler(cmd_resume, filters.command("resume")))
        bot.add_handler(MessageHandler(cmd_queue_preview, filters.command("queue")))
        bot.add_handler(MessageHandler(cmd_resetstats, filters.command("resetstats")))
        
        # Register callback handlers
        bot.add_handler(CallbackQueryHandler(cb_welcome, filters.regex("^welcome:")))
        bot.add_handler(CallbackQueryHandler(cb_dlopt, filters.regex("^dlopt:")))
        bot.add_handler(CallbackQueryHandler(cb_bulk_select, filters.regex("^bulk:")))
        bot.add_handler(CallbackQueryHandler(cb_bulk_start, filters.regex("^bulkstart:")))
        bot.add_handler(CallbackQueryHandler(cb_select_chat, filters.regex("^sc:")))
        bot.add_handler(CallbackQueryHandler(cb_page, filters.regex("^pg:")))
        bot.add_handler(CallbackQueryHandler(cb_open_folder, filters.regex("^fd:")))
        bot.add_handler(CallbackQueryHandler(cb_files_page, filters.regex("^fp:")))
        bot.add_handler(CallbackQueryHandler(cb_back_to_folders, filters.regex("^fb$")))
        bot.add_handler(CallbackQueryHandler(cb_send_file, filters.regex("^fl:")))
        bot.add_handler(CallbackQueryHandler(cb_delete_file, filters.regex("^fdel:")))
        bot.add_handler(CallbackQueryHandler(cb_delete_folder, filters.regex("^fdeldir:")))
        bot.add_handler(CallbackQueryHandler(cb_delete_folder_confirm, filters.regex("^fdeldirok:")))
        bot.add_handler(CallbackQueryHandler(cb_wipe_all, filters.regex("^fwipe$")))
        bot.add_handler(CallbackQueryHandler(cb_wipe_all_confirm, filters.regex("^fwipeok$")))
        bot.add_handler(CallbackQueryHandler(cb_search_download, filters.regex("^searchdl:")))
        bot.add_handler(CallbackQueryHandler(cb_open_files_from_job, filters.regex("^openf:")))
        bot.add_handler(CallbackQueryHandler(cb_back, filters.regex("^back:")))
        
        # Text handler (catch-all)
        bot.add_handler(MessageHandler(
            handle_text,
            filters.text & ~filters.command([
                "start", "setchat", "options", "files", "stats", "history",
                "settings", "api", "help", "killall", "pause",
                "resume", "queue", "resetstats"
            ])
        ))
        
        # Start cleanup task
        _user_cleanup_task = asyncio.create_task(_cleanup_task())
        log.info("✅ Cleanup task started")
        
        log.info("━" * 50)
        log.info("🤖 GhostFetch Bot is RUNNING!")
        log.info("━" * 50)
        
        # Keep bot running using Event
        stop_event = asyncio.Event()
        await stop_event.wait()
        
    except Exception as e:
        log.error(f"Fatal bot error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Cleanup on shutdown
        log.info("Shutting down...")
        
        if http_session:
            await http_session.close()
        if _user_cleanup_task:
            _user_cleanup_task.cancel()
            try:
                await _user_cleanup_task
            except asyncio.CancelledError:
                pass
        if bot:
            await bot.stop()
        if user_client:
            await user_client.stop()
        
        _save_download_db(downloaded_ids)
        log.info("✅ Bot stopped gracefully")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.error(f"Startup error: {e}")
        import traceback
        traceback.print_exc()
