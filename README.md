<div align="center">

# 👻 GhostFetch

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Pyrogram](https://img.shields.io/badge/Pyrogram-2.x-009DFF?style=flat&logo=telegram&logoColor=white)](https://docs.pyrogram.org)
[![Platform](https://img.shields.io/badge/Platform-Termux%20%7C%20Android-3DDC84?style=flat&logo=android&logoColor=white)](https://termux.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat)](./LICENSE)

**Download media from any restricted Telegram chat**
**directly to your Termux storage.**

*No web scraping. No third-party servers. Your account, your files.*

— *herraChron*

</div>

---

## How it works

Two Pyrogram clients run side by side:

- **User client** — your personal account session, which has access to the restricted content.
- **Bot client** — a bot you control via @BotFather, used as your command interface.

DM your bot, pick a chat from your dialog list, send message IDs — files land in your Termux home folder.

---

## Features

- 📥 **Download from restricted chats** — channels, groups, bots, DMs
- 🗂 **File browser** — browse, receive, or delete downloads right from the chat (`/files`)
- 🎬 **File-type emoji** — downloads are labelled with matching icons (🖼 🎬 🎵 🗜 📕 ...)
- 📋 **Job queue** — send multiple batches; jobs run one after another automatically
- ⚡ **Live progress** — real-time progress bar with percentage per file
- 🔁 **Media groups** — handles multi-file posts in a single ID
- 🛡 **FloodWait handling** — automatic retry on Telegram rate limits
- 🔍 **Chat filter** — search your dialog list by typing a name
- 💾 **Session memory** — last used chat is remembered across restarts
- 📊 **Stats** — uptime, disk, CPU, RAM, and network I/O on demand
- 📂 **Quick access** — "Open Files" button appears on every completed download

---

## Requirements

- Android with [Termux](https://github.com/termux/termux-app) (F-Droid build recommended)
- Python 3.11+
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

### 2. Install dependencies

```bash
pkg update && pkg upgrade -y
pkg install python git -y
pip install -r requirements.txt
```

### 3. Get your Telegram API credentials

Go to [my.telegram.org/apps](https://my.telegram.org/apps), log in, and create an app. Copy the **API ID** and **API Hash**.

### 4. Create a bot

Open Telegram, message [@BotFather](https://t.me/BotFather), send `/newbot`, follow the steps. Copy the **bot token**.

### 5. Configure

```bash
cp .env.example .env
nano .env
```

Fill in your first three values — leave `SESSION_STRING` empty for now:

```
API_ID=your_api_id_here
API_HASH=your_api_hash_here
BOT_TOKEN=your_bot_token_here
SESSION_STRING=
```

Press **CTRL+X → Y → Enter** to save.

### 6. Generate your session string

```bash
python gen_session.py
```

This will ask for your phone number and a login code, then print a `SESSION_STRING`. Copy it, then open `.env` again:

```bash
nano .env
```

Paste the session string into the `SESSION_STRING=` line, then save with **CTRL+X → Y → Enter**.

### 7. Run

```bash
python main.py
```

Open Telegram, find your bot, and send `/start`.

---

## Usage

### Picking a chat

After `/start`, a clean welcome screen appears with a **Continue** button if you've used the bot before, or tap **Set Chat** to pick from your full dialog list. You can type part of a name to filter the list, and page through with ⬅️ / ➡️.

### Downloading

Once a chat is selected, send one or more message IDs:

```
26473
```
```
26473 26570 26600
```

Message IDs are the numbers in the Telegram URL (e.g. `t.me/chatname/26473`) or visible in the message info on desktop.

Each batch you send becomes a job. Jobs queue automatically — you don't have to wait.

### File browser

Use `/files` (or tap **📂 Open Files** after any download) to browse your downloads. From there you can:

- Tap a file to have the bot send it back to you
- Tap 🗑 to delete a single file
- Delete an entire folder or wipe everything

### Commands

| Command | What it does |
|---|---|
| `/start` | Reset session, clear queue, pick a new source chat |
| `/setchat` | Change the source chat without resetting anything else |
| `/files` | Browse, receive, or delete downloaded files |
| `/killall` | Cancel the active download and clear the entire queue |
| `/stats` | Show uptime, disk usage, CPU, RAM, network I/O |
| `/log` | Send the current session log as a file |
| `/help` | Show the command reference |

> Commands also appear in the Telegram menu bar automatically on first run.

### Where files go

```
~/GhostFetch/downloads/<chat_id>/<filename>
```

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
- `TgCrypto` is optional but strongly recommended — it speeds up encryption significantly on Termux hardware.
- If you get `FloodWait` errors during large batches, the bot handles them automatically with retries.
- Downloaded IDs are deduplicated per session (cleared on `/start`), so re-sending the same ID within a session just skips it.

---

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
