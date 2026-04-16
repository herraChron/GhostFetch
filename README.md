# GhostFetch

A Telegram userbot + bot combo that downloads media from restricted or private chats — channels, groups, bots — directly to your Termux storage. No web scraping, no third-party servers. Everything goes through your own Telegram account.

> Made by **herraChron**

---

## How it works

You run two Pyrogram clients side by side:

- **User client** — your personal account session. This is what actually has access to the restricted content.
- **Bot client** — a regular bot you control via @BotFather. This is your interface for sending commands and message IDs.

You DM your bot, pick a target chat from your dialog list, send message IDs, and the files land in your Termux home folder.

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

### 2. Install dependencies in Termux

```bash
pkg update && pkg upgrade -y
pkg install python git -y
pip install -r requirements.txt
```

### 3. Get your Telegram API credentials

Go to [my.telegram.org/apps](https://my.telegram.org/apps), log in, and create an app. Copy the **API ID** and **API Hash**.

### 4. Create a bot

Open Telegram, message [@BotFather](https://t.me/BotFather), send `/newbot`, follow the steps. Copy the **bot token**.

### 5. Generate your session string

```bash
python gen_session.py
```

This will ask for your phone number and a login code, then print a `SESSION_STRING`. Copy it.

### 6. Configure

```bash
nano .env
```

Fill in all four values:

```
API_ID=12345678
API_HASH=your_api_hash_here
BOT_TOKEN=123456:ABCdef...
SESSION_STRING=BQA...long_string...
```

### 7. Run

```bash
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
|---|---|
| `/start` | Reset session, clear queue, pick a new source chat |
| `/setchat` | Change the source chat without resetting anything else |
| `/killall` | Cancel the active download and clear the entire queue |
| `/stats` | Show uptime, disk usage, CPU, RAM, network I/O |
| `/log` | Send the current session log as a file |
| `/help` | Show the command reference |

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
- If you get `FloodWait` errors during large batches, the bot handles them automatically with retries. It won't spam Telegram.
- Downloaded IDs are deduplicated per session (cleared on `/start`), so re-sending the same ID within a session just skips it.

---

## License

MIT
