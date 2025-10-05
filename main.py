import logging
import asyncio
import os
import requests
import tempfile
import json
from bs4 import BeautifulSoup
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

# Telegram (PTB) imports for the main bot
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.request import HTTPXRequest

# Pyrogram imports for the userbot worker
from pyrogram import Client as PyrogramClient, filters as PyrogramFilters
from pyrogram.handlers import MessageHandler as PyrogramMessageHandler

# --- Configuration for Deployment ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Part 1: The Worker Logic (Pyrogram) ---

userbot = PyrogramClient(
    "userbot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=USERBOT_SESSION_STRING
)

async def job_handler(client, message):
    """This function is the userbot worker. It listens to 'Saved Messages'."""
    text = message.text
    if not text or not text.startswith("process|"): return

    logger.info(f"Userbot received job: {text}")
    parts = text.split('|', 3)
    if len(parts) != 4:
        logger.error("Invalid job format.")
        return
    
    await message.delete()
    
    _, original_chat_id, referer_url, media_url = parts
    
    local_filename = media_url.split('/')[-1].split('?')[0]
    if len(local_filename) > 60 or not local_filename:
        file_ext = os.path.splitext(media_url.split('/')[-1].split('?')[0])[1]
        local_filename = f"temp_media{file_ext}" if file_ext else "temp_media"
    
    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, local_filename)

    try:
        logger.info(f"Userbot downloading {media_url} for user {original_chat_id}...")
        headers = {'Referer': referer_url}
        with requests.get(media_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(full_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        logger.info(f"Userbot uploading {full_path} to user {original_chat_id}...")
        if any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm']):
            await client.send_video(int(original_chat_id), full_path)
        else:
            await client.send_photo(int(original_chat_id), full_path)
        logger.info("Userbot job complete.")

    except Exception as e:
        logger.error(f"Userbot failed to process {media_url}: {e}")
        await client.send_message(int(original_chat_id), f"⚠️ Worker failed to download/send file: {media_url}")
    finally:
        if os.path.exists(full_path):
            os.remove(full_path)

userbot.add_handler(PyrogramMessageHandler(job_handler, PyrogramFilters.chat("me") & PyrogramFilters.text))

# --- Part 2: The Manager Logic (python-telegram-bot) ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 All-in-One Bot. Send an album or individual media link.")

async def process_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    initial_url = update.message.text.strip()
    chat_id = update.message.chat_id
    
    await update.message.reply_text("✅ Link received. Scraping and dispatching jobs...")

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(initial_url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        thumbnail_links = soup.select("a.spotlight[data-media]")

        if not thumbnail_links:
            await update.message.reply_text("❌ Could not find any media thumbnails.")
            return
            
        media_urls = []
        for link in thumbnail_links:
            media_type = link.get('data-media')
            if media_type == 'video':
                url = link.get('data-src-mp4')
                if url: media_urls.append(url)
            elif media_type == 'image':
                url = link.get('href')
                if url: media_urls.append(url)
        
        if not media_urls:
            await update.message.reply_text("Found thumbnails, but no media links.")
            return

        media_urls = sorted(list(set(media_urls)))

        for media_url in media_urls:
            job_message = f"process|{chat_id}|{initial_url}|{media_url}"
            await userbot.send_message("me", job_message)
        
        await update.message.reply_text(f"👍 Found {len(media_urls)} files. Jobs dispatched. Your files will be sent shortly.")

    except Exception as e:
        logger.error(f"Manager error: {e}", exc_info=True)
        await context.bot.send_message(chat_id, f"An error occurred: {type(e).__name__}")

# --- Part 3: The Keep-Alive Server and Main Runner ---

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server_address = ('', port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info(f"Keep-alive server running on port {port}")
    httpd.serve_forever()

async def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING]):
        raise ValueError("One or more required environment variables are missing!")

    # Start the keep-alive web server in a separate thread
    web_thread = Thread(target=run_web_server, daemon=True)
    web_thread.start()

    # Setup the main PTB bot
    request = HTTPXRequest(connect_timeout=10.0, read_timeout=30.0)
    ptb_app = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request).build()
    ptb_app.add_handler(CommandHandler("start", start_command))
    ptb_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_url))

    # --- NEW: Correctly run both clients concurrently ---
    # We initialize PTB but don't start polling yet.
    await ptb_app.initialize()
    
    # We start both the userbot and the PTB application in the background.
    # PTB's start() begins fetching updates but doesn't block.
    # userbot.start() connects to Telegram and starts its update loop.
    await userbot.start()
    await ptb_app.start()
    
    # start_polling() is also non-blocking when run this way.
    await ptb_app.updater.start_polling()
    
    logger.info("Both PTB and Pyrogram clients are running concurrently.")
    
    # Keep the script alive indefinitely.
    await asyncio.Event().wait()
    
    # Graceful shutdown (will likely not be reached in a server environment)
    await ptb_app.updater.stop()
    await ptb_app.stop()
    await userbot.stop()

if __name__ == '__main__':
    print("Starting All-in-One Bot...")
    asyncio.run(main())
