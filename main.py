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
BOT_USERNAME = os.environ.get("BOT_USERNAME") # e.g., "@MyDownloaderBot"
USERBOT_USER_ID = int(os.environ.get("USERBOT_USER_ID")) # Your numerical Telegram ID

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- ON-DEMAND UPLOADER (sends to the bot) ---
async def upload_to_bot(chat_id, media_url, referer_url):
    """Creates a temporary userbot client to download a file and send it TO THE BOT with a caption."""
    local_filename = media_url.split('/')[-1].split('?')[0]
    if len(local_filename) > 60 or not local_filename:
        file_ext = os.path.splitext(media_url.split('/')[-1].split('?')[0])[1]
        local_filename = f"temp_media{file_ext}" if file_ext else "temp_media"
    
    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, local_filename)

    app = PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING)

    try:
        logger.info(f"Downloading {media_url}...")
        headers = {'Referer': referer_url}
        with requests.get(media_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(full_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
        
        # This caption contains the routing information for the main bot
        caption = f"FORWARD_TO::{chat_id}"

        async with app:
            while True:
                try:
                    logger.info(f"Uploading {full_path} to the bot {BOT_USERNAME}...")
                    if is_video:
                        await app.send_video(BOT_USERNAME, full_path, caption=caption)
                    else:
                        await app.send_photo(BOT_USERNAME, full_path, caption=caption)
                    logger.info(f"Successfully sent {local_filename} to bot.")
                    break 
                except FloodWait as e:
                    logger.warning(f"FloodWait received. Sleeping for {e.value + 5} seconds.")
                    await asyncio.sleep(e.value + 5)
    except Exception as e:
        logger.error(f"Failed to process and upload to bot: {e}")
    finally:
        if os.path.exists(full_path):
            os.remove(full_path)

# --- MAIN BOT LOGIC ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hello! Send me a link.")

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
        await update.message.reply_text(f"👍 Found {len(media_urls)} files. Processing will begin shortly...")
        tasks = [upload_to_bot(chat_id, media_url, initial_url) for media_url in media_urls]
        await asyncio.gather(*tasks)
        await update.message.reply_text("✅ All tasks dispatched.")
    except Exception as e:
        logger.error(f"Manager error: {e}", exc_info=True)
        await context.bot.send_message(chat_id, f"An error occurred: {type(e).__name__}")

async def forwarder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """This handler listens for files sent from the userbot and forwards them."""
    if not update.message.caption or not update.message.caption.startswith("FORWARD_TO::"):
        return
    
    try:
        destination_chat_id = int(update.message.caption.split("::")[1])
        logger.info(f"Received file from userbot, forwarding to {destination_chat_id}")
        await update.message.forward(chat_id=destination_chat_id)
    except Exception as e:
        logger.error(f"Failed to forward message: {e}")

# --- Keep-Alive Server and Main Runner ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header('Content-type', 'text/plain'); self.end_headers(); self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000)); server_address = ('', port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info(f"Keep-alive server running on port {port}"); httpd.serve_forever()

def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING, BOT_USERNAME, USERBOT_USER_ID]):
        raise ValueError("One or more required environment variables are missing!")

    web_thread = Thread(target=run_web_server, daemon=True)
    web_thread.start()

    request = HTTPXRequest(connect_timeout=10.0, read_timeout=30.0)
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request).build()
    
    # Handler for user commands
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_url))
    
    # Handler for receiving files from the trusted userbot
    user_filter = filters.User(user_id=USERBOT_USER_ID)
    application.add_handler(MessageHandler(filters.PHOTO & user_filter, forwarder_handler))
    application.add_handler(MessageHandler(filters.VIDEO & user_filter, forwarder_handler))
    
    print("Definitive Relay Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
