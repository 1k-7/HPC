import logging
import asyncio
import os
import requests
import json
from bs4 import BeautifulSoup
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

# Telegram imports
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.request import HTTPXRequest

# Pyrogram import
from pyrogram import Client

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- The Rest of the Bot Code (No Changes) ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Manager Bot. Send a link. I will create jobs for the worker.")

async def process_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    initial_url = update.message.text.strip()
    chat_id = update.message.chat_id
    await update.message.reply_text("✅ Link received. Scraping for media URLs...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(initial_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        thumbnail_links = soup.select("a.spotlight[data-media]")
        if not thumbnail_links:
            await update.message.reply_text("❌ Could not find any media thumbnails on this page.")
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
            await update.message.reply_text("Found thumbnails, but could not extract any media links.")
            return
        media_urls = sorted(list(set(media_urls)))
        await update.message.reply_text(f"👍 Found {len(media_urls)} files. Dispatching jobs to the worker...")
        async with Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING) as app:
            for media_url in media_urls:
                job_message = f"process|{chat_id}|{initial_url}|{media_url}"
                await app.send_message("me", job_message)
        await update.message.reply_text("✅ All jobs dispatched. The worker will send you the files shortly.")
    except Exception as e:
        logger.error(f"A critical error occurred: {e}", exc_info=True)
        await context.bot.send_message(chat_id, f"An unexpected error occurred: {type(e).__name__}")

# --- NEW: Keep-Alive Web Server for Render ---
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
# --- END OF NEW CODE ---

# --- Main Bot Setup ---
def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING]):
        raise ValueError("One or more required environment variables are missing!")

    # Start the keep-alive web server in a separate thread
    web_thread = Thread(target=run_web_server, daemon=True)
    web_thread.start()

    request = HTTPXRequest(connect_timeout=10.0, read_timeout=30.0)
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_url))
    
    print("Manager Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
