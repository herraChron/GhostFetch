<div align="center">

# 👻 GhostFetch

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Pyrogram](https://img.shields.io/badge/Pyrogram-2.x-009DFF?style=flat&logo=telegram&logoColor=white)](https://docs.pyrogram.org)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20macOS%20%7C%20Linux%20%7C%20Termux-0078D6?style=flat&logo=windows&logoColor=white)](https://github.com/herraChron/GhostFetch)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat)](./LICENSE)

**Download media from any restricted Telegram chat**
**directly to your local storage.**

*No web scraping. No third-party servers. Your account, your files.*

> 🚧 **This project is still actively being developed. More features and updates are on the way.**

✪ *herraChron*

</div>

---

## How it works

Two Pyrogram clients run side by side:

- **User client** — your personal account session, which has access to the restricted content.
- **Bot client** — a bot you control via @BotFather, used as your command interface.

DM your bot, pick a chat from your dialog list, choose a download mode — files land in your GhostFetch downloads folder.

---

## Features

- 📥 **Download from restricted chats** — channels, groups, bots, DMs
- 🔢 **Manual ID mode** — send one or more message IDs to download specific files
- 🔍 **Search by filename** — type a keyword; the bot scans chat history and shows matches as buttons
- 📦 **Bulk download** — pick a file type (Videos, Audio, Images, Documents, Archives, Apps) and grab everything in the chat history
- 🗂 **File browser** — browse, receive, or delete downloads right from the chat (`/files`)
- 📋 **Job queue** — send multiple batches; jobs run one after another automatically, with per-job cancel buttons
- ⚡ **Live progress** — real-time progress bar with percentage, speed, and ETA per file
- 🔁 **Media groups** — handles multi-file posts in a single ID
- 🛡 **FloodWait handling** — automatic retry on Telegram rate limits
- 🔍 **Chat filter** — search your dialog list by typing a name
- 💾 **Session memory** — last used chat and downloaded IDs are remembered across restarts
- 🔒 **User whitelist** — restrict bot access to specific Telegram user IDs
- 📊 **Stats** — uptime, disk, CPU, RAM, network I/O, and total files downloaded on demand
- 📂 **Quick access** — "Open Files" button appears on every completed download

---

## Requirements

- Python 3.11+
- Git
- A Telegram account
- A bot token from [@BotFather](https://t.me/BotFather)
- API credentials from [my.telegram.org](https://my.telegram.org/apps)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/herraChron/GhostFetch.git
cd GhostFetch
```

---

### 2. Install dependencies

Choose the instructions for your operating system.

---

#### 🤖 Android (Termux)

Install [Termux from F-Droid](https://f-droid.org/packages/com.termux/) (recommended over the Play Store build).

```bash
pkg update && pkg upgrade -y
pkg install python git -y
pip install -r requirements.txt
```

---

#### 🐧 Linux (Debian / Ubuntu)

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3 python3-pip git -y
pip3 install -r requirements.txt
```

For Arch-based distros (Manjaro, EndeavourOS, etc.):

```bash
sudo pacman -Syu
sudo pacman -S python python-pip git
pip install -r requirements.txt
```

For Fedora / RHEL-based distros:

```bash
sudo dnf update -y
sudo dnf install python3 python3-pip git -y
pip3 install -r requirements.txt
```

---

#### 🍎 macOS

Make sure you have [Homebrew](https://brew.sh) installed, then run:

```bash
brew update
brew install python git
pip3 install -r requirements.txt
```

If you do not have Homebrew, install it first:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

---

#### 🪟 Windows

1. Download and install [Python 3.11+](https://www.python.org/downloads/windows/) — make sure to check **"Add Python to PATH"** during setup.
2. Download and install [Git for Windows](https://git-scm.com/download/win).
3. Open **Command Prompt** or **PowerShell** and run:

```powershell
pip install -r requirements.txt
```

> **Tip:** On Windows, it is recommended to run GhostFetch inside [Windows Terminal](https://aka.ms/terminal) or [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) for a better experience.

---

### 3. Get your Telegram API credentials

Go to [my.telegram.org/apps](https://my.telegram.org/apps), log in, and create an app. Copy the **API ID** and **API Hash**.

### 4. Create a bot

Open Telegram, message [@BotFather](https://t.me/BotFather), send `/newbot`, follow the steps. Copy the **bot token**.

### 5. Configure

```bash
cp .env.example .env
```

Open `.env` with any text editor and fill in your credentials — leave `SESSION_STRING` empty for now:

```
API_ID=your_api_id_here
API_HASH=your_api_hash_here
BOT_TOKEN=your_bot_token_here
SESSION_STRING=
```

**Quick editor commands by platform:**

| Platform | Command |
|---|---|
| Termux / Linux / macOS | `nano .env` then **CTRL+X → Y → Enter** to save |
| Windows (Notepad) | `notepad .env` |
| Windows (VS Code) | `code .env` |

### 6. Generate your session string

```bash
python gen_session.py
```

> On Linux/macOS you may need to use `python3` instead of `python`.

This will ask for your phone number and a login code, then print a `SESSION_STRING`. Copy it and paste it into the `SESSION_STRING=` line in your `.env` file, then save.

### 7. Run

```bash
python main.py
```

> On Linux/macOS: `python3 main.py`

Open Telegram, find your bot, and send `/start`.

---

## Usage

### Welcome screen

After `/start`, the welcome screen shows the number of loaded chats and queued jobs, plus:

- **▶ Resume** — jump straight back to the last used chat (shown only if a chat was previously selected)
- **Select Chat** — pick a different source chat from your dialog list
- **New Session** — clears the downloaded-IDs history so previously skipped files will be downloaded again
- **Help** — show the command reference

If a download is already running when you send `/start`, the bot will ask you to confirm before resetting.

### Picking a chat

Tap **Select Chat** (or use `/setchat`) to browse your full dialog list. Type part of a name to filter the list, and page through results with ← / →.

### Download modes

Once a chat is selected, open `/options` (or tap the download mode picker) to choose how to download:

**Manual IDs**

Send one or more space-separated message IDs:

```
26473
```
```
26473 26570 26600
```

Message IDs appear in the Telegram URL (e.g. `t.me/chatname/26473`) or in the message info panel on desktop.

**Search by Filename**

Type a keyword after selecting this mode. The bot scans up to 5,000 recent messages for matching filenames and shows results as inline buttons. Tap a single result or download all matches at once.

**Bulk Download**

Pick a file type from the menu:

| Type | Extensions |
|---|---|
| 🎬 Videos | mp4, mkv, avi, mov, webm, flv, ts, m4v, 3gp |
| 🎵 Audio | mp3, ogg, flac, wav, m4a, aac, opus, wma |
| 🖼 Images | jpg, jpeg, png, gif, webp, bmp, heic |
| 📚 Documents | pdf, epub, mobi, txt, docx, xlsx, csv |
| 🗜 Archives | zip, rar, 7z, tar, gz, xz, bz2 |
| 📱 Apps | apk, xapk, apks |

The bot will scan the entire chat history and download every matching file. A live progress bar shows scanned messages, matched files, bytes downloaded, and elapsed time. A **Cancel Download** button lets you stop at any point.

### Job queue

Each batch you send (Manual IDs or Search result) becomes a job. Jobs queue automatically — you don't have to wait for one to finish before sending the next. Use `/queue` to see pending jobs and cancel individual ones.

### File browser

Use `/files` (or tap **📂 Open Files** after any download) to browse your downloads. From there you can:

- Tap a file to have the bot send it back to you
- Tap 🗑 to delete a single file (with confirmation)
- Delete an entire folder or wipe everything

### Commands

| Command | What it does |
|---|---|
| `/start` | Reset session, clear queue, pick a new source chat |
| `/setchat` | Change the source chat without resetting anything else |
| `/options` | Open the download mode picker for the current chat |
| `/files` | Browse, receive, or delete downloaded files |
| `/queue` | Show pending jobs with per-job cancel buttons |
| `/killall` | Cancel the active download and clear the entire queue |
| `/stats` | Show uptime, disk usage, CPU, RAM, network I/O, and file count |
| `/log` | Send the current session log as a file |
| `/help` | Show the command reference |

> Commands also appear in the Telegram menu bar automatically on first run.

### Where files go

```
GhostFetch/downloads/<chat_id>/<filename>
```

| Platform | Full path example |
|---|---|
| Termux | `~/GhostFetch/downloads/<chat_id>/` |
| Linux / macOS | `~/GhostFetch/downloads/<chat_id>/` |
| Windows | `C:\Users\<YourName>\GhostFetch\downloads\<chat_id>\` |

If a file with the same name already exists, it gets a `_1`, `_2` suffix instead of overwriting.

---

## Project structure

```
GhostFetch/
├── main.py              # bot logic, handlers, download engine
├── config.py            # loads credentials from .env
├── gen_session.py       # one-time session string generator
├── requirements.txt
├── .env.example         # credential template
└── .gitignore
```

---

## Notes

- The session string gives full access to your Telegram account. Keep your `.env` file private and never commit it. The `.gitignore` already excludes it.
- `TgCrypto` is optional but strongly recommended — it speeds up encryption significantly on all platforms.
- If you get `FloodWait` errors during large batches, the bot handles them automatically with retries.
- Downloaded IDs are persisted to disk (`downloaded_ids.json`) and survive restarts, so files already fetched are never re-downloaded. To reset this history, tap **New Session** on the welcome screen.
- To restrict bot access to specific users, add a list of Telegram user IDs to `ALLOWED_USER_IDS` in `config.py`. An empty list (the default) allows any user to interact with the bot.
- The bot is optimised for single-user deployments (4 async workers). If you move it to a server with multiple simultaneous users, increase the `workers` value in `main()`.
- On Windows, if `python` is not recognised in your terminal, try `py` instead, or verify that Python was added to your PATH during installation.

---

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
