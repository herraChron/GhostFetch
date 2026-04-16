[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

# GhostFetch

A Telegram userbot + bot combo that downloads media from restricted or private chats — channels, groups, bots — directly to your Termux storage. No web scraping, no third-party servers. Everything goes through your own Telegram account.

> Made by **herraChron**

---

## How it works

You run two Pyrogram clients side by side:

* **User client** — your personal account session. This is what actually has access to the restricted content.
* **Bot client** — a regular bot you control via @BotFather. This is your interface for sending commands and message IDs.

You DM your bot, pick a target chat from your dialog list, send message IDs, and the files land in your Termux home folder.

---

## Requirements

* Android with [Termux](https://github.com/termux/termux-app) (F-Droid build recommended)
* Python 3.11+
* A Telegram account
* A bot token from [@BotFather](https://t.me/BotFather)
* API credentials from [my.telegram.org](https://my.telegram.org/apps)

---

## Setup

### 1. Clone the repo

```
git clone https://github.com/herraChron/GhostFetch.git
cd GhostFetch
```

### 2. Install dependencies

```
pkg update && pkg upgrade -y
pkg install python git -y
pip install -r requirements.txt
```

### 3. Grant storage access

Run this once to allow Termux to access your phone's internal storage. You'll need this if you ever want to save downloads to `/storage/emulated/0/` (Internal Downloads).

```
termux-setup-storage
```

Accept the permission popup when it appears. You can skip this step if you only plan to use the Termux Home path.

### 4. Get your Telegram API credentials

Go to [my.telegram.org/apps](https://my.telegram.org/apps), log in, and create an app. Copy the **API ID** and **API Hash**.

### 5. Create a bot

Open Telegram, message [@BotFather](https://t.me/BotFather), send `/newbot`, follow the steps. Copy the **bot token**.

### 6. Configure

```
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

### 7. Generate your session string

```
python gen_session.py
```

This will ask for your phone number and a login code, then print a `SESSION_STRING`. Copy it, then open `.env` again:

```
nano .env
```

Paste the session string into the `SESSION_STRING=` line, then save with **CTRL+X → Y → Enter**.

### 8. Run

```
python main.py
```

Open Telegram, find your bot, and send `/start`.

---

## Usage

### Picking a chat

After `/start`, the bot shows a scrollable list of every chat in your account — groups, channels, bots, DMs. Tap one to set it as the active source. You can also type part of a name to filter the list.

### Downloading

Once a chat is selected, send one or more message IDs:

```
26473
```

```
26473 26570 26600
```

Message IDs are the numbers in the Telegram URL (e.g. `t.me/chatname/26473`) or visible in the message info on desktop.

Each batch you send becomes a job. Jobs queue up automatically — you don't have to wait for one to finish before sending the next.

### Commands

| Command | What it does |
| --- | --- |
| `/start` | Reset session, clear queue, pick a new source chat |
| `/setchat` | Change the source chat without resetting anything else |
| `/setpath` | Change the download folder |
| `/killall` | Cancel the active download and clear the entire queue |
| `/stats` | Show uptime, disk usage, CPU, RAM, network I/O |
| `/log` | Send the current session log as a file |
| `/help` | Show the command reference |

### Download paths

Use `/setpath` to choose where files are saved:

| Option | Path |
| --- | --- |
| 🖥 Termux Home | `/data/data/com.termux/files/home/GhostFetch/downloads` |
| 📱 Internal Downloads | `/storage/emulated/0/Download/GhostFetch/downloads` |
| ✏️ Custom path | Any path you type |

> **Internal Downloads** requires `termux-setup-storage` to have been run first (step 3). If you choose it without storage permission, the bot will show a clear error message instead of crashing.

Files are organised as:

```
<download_path>/<chat_id>/<filename>
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

## Troubleshooting

**`OSError: [Errno 14] Bad address` on startup**

This means a path in `storage/emulated/0/` was saved but Termux can't access it. The bot will now auto-recover and reset to the Termux Home path automatically. To fix it permanently, run `termux-setup-storage` and then use `/setpath` to re-select Internal Downloads.

**`OSError: [Errno 13] Permission denied`**

Same root cause as above. Run `termux-setup-storage` first.

**`FloodWait` errors**

These are Telegram rate limits. The bot handles them automatically with retries — just let it run.

**Bot starts but dialogs list is empty**

The dialog cache loads at startup. Wait a few seconds, then send `/start` again.

---

## Notes

* The session string gives full access to your Telegram account. Keep your `.env` file private and never commit it. The `.gitignore` already excludes it.
* `TgCrypto` is optional but strongly recommended — it speeds up encryption significantly on Termux hardware.
* Downloaded IDs are deduplicated per session (cleared on `/start`), so re-sending the same ID within a session just skips it.

---

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
