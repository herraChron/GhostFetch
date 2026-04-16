# config.py — GhostFetch
# herraChron

import os
from dotenv import load_dotenv

load_dotenv()


class PyroConf:
    API_ID         = int(os.environ["API_ID"])
    API_HASH       = os.environ["API_HASH"]
    BOT_TOKEN      = os.environ["BOT_TOKEN"]
    SESSION_STRING = os.environ["SESSION_STRING"]
