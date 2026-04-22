import os
from dotenv import load_dotenv

load_dotenv()


class PyroConf:
    API_ID         = int(os.environ["API_ID"])
    API_HASH       = os.environ["API_HASH"]
    BOT_TOKEN      = os.environ["BOT_TOKEN"]
    SESSION_STRING = os.environ["SESSION_STRING"]
    PHONE          = os.environ["PHONE"]  
    BOT_OWNER_ID   = int(os.environ.get("BOT_OWNER_ID", 0))  
    WEBHOOK_URL    = os.environ.get("WEBHOOK_URL", None) 
