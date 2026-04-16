#!/usr/bin/env python3
# tools/gen_session.py — GhostFetch
# herraChron
#
# Run this once to generate your SESSION_STRING.
# Paste the output into your .env file, then delete this script
# (or at least never commit it while your session is printed on screen).

import asyncio
from pyrogram import Client
from dotenv import load_dotenv
import os

load_dotenv()

API_ID   = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]


async def main():
    async with Client(
        "gen_session",
        api_id=API_ID,
        api_hash=API_HASH,
    ) as app:
        string = await app.export_session_string()
        print("\n" + "=" * 60)
        print("SESSION_STRING (paste this into your .env):")
        print("=" * 60)
        print(string)
        print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
