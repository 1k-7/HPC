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

# Pyrogram imports for the userbot
from pyrogram import Client as PyrogramClient
from pyrogram.errors import FloodWait

# --- Configuration for Deployment ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- UPLOADER FUNCTION with FloodWait Handling ---
async def download_and_upload_item(app, chat_id, media_url, referer_url):
    """Downloads a single file and uploads it using the provided, active Pyrogram client."""
    local_filename = media_url.split('/')[-1].split('?')[0]
    if len(local_filename) > 60 or not local_filename:
        file_ext = os.path.splitext(media_url.split('/')[-1].split('?')[0])[1]
        local_filename = f"temp_media{file_ext}" if file_ext else "temp_media"
    
    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, local_filename)

    try:
        logger.info(f"Downloading {media_url}...")
        headers = {'Referer': referer_url}
        with requests.get(media_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(full_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
        
        # --- NEW: FloodWait Handling and Retry Logic ---
        while True:
            try:
                logger.info(f"Uploading {full_path} to user {chat_id}...")
                if is_video:
                    await app.send_video(chat_id, full_path)
                else:
                    await app.send_photo(chat_id, full_path)
                logger.info(f"Successfully sent {local_filename}.")
                break # Exit the loop on success
            except FloodWait as e:
                logger.warning(f"FloodWait received. Sleeping for {e.value} seconds.")
                await asyncio.sleep(e.value + 5) # Sleep for the required time + a buffer
        # --- END OF NEW LOGIC ---

    except Exception as e:
        logger.error(f"Failed to process {media_url}: {e}")
    finally:
        if os.path.exists(full_path):
            os.remove(full_path)

# --- MAIN BOT (PTB) LOGIC ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hello! Send me a link to an album or a single media page.")

async def process_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    initial_url = update.message.text.strip()
    chat_id = update.message.chat.id
    
    await update.message.reply_text("✅ Link received. Scraping...")

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

        await update.message.reply_text(f"👍 Found {len(media_urls)} files. Starting batch upload session...")

        # --- NEW: Batch Processing Logic ---
        # Create one userbot client for the entire batch of files
        app = PyrogramClient(
            "userbot_session",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=USERBOT_SESSION_STRING
        )
        async with app:
            # Create a list of tasks to run concurrently
            tasks = [download_and_upload_item(app, chat_id, media_url, initial_url) for media_url in media_urls]
            await asyncio.gather(*tasks)
        # --- END OF NEW LOGIC ---
        
        await update.message.reply_text("✅ All tasks complete.")

    except Exception as e:
        logger.error(f"Manager error: {e}", exc_info=True)
        await context.bot.send_message(chat_id, f"An error occurred: {type(e).__name__}")

# --- Keep-Alive Server and Main Runner ---
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

def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING]):
        raise ValueError("One or more required environment variables are missing!")

    web_thread = Thread(target=run_web_server, daemon=True)
    web_thread.start()

    request = HTTPXRequest(connect_timeout=10.0, read_timeout=30.0)
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_url))
    
    print("Batch Processing Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
