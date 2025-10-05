import logging
import asyncio
import os
import requests
import tempfile
import json

from bs4 import BeautifulSoup

# Telegram imports
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.request import HTTPXRequest

# Pyrogram import for the userbot
from pyrogram import Client

# --- Configuration for Deployment ---
# These are read securely from the server's environment variables
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- HELPER: UPLOAD VIA USERBOT AND FORWARD ---
async def upload_with_userbot_and_forward(media_url, original_referer_url, chat_id, context):
    """Downloads a file, uploads it via userbot, and forwards it via the main bot."""
    local_filename = media_url.split('/')[-1].split('?')[0]
    if len(local_filename) > 60 or not local_filename:
        file_ext = os.path.splitext(media_url.split('/')[-1].split('?')[0])[1]
        local_filename = f"temp_media{file_ext}" if file_ext else "temp_media"

    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, local_filename)
    
    app = Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING)

    try:
        logger.info(f"Downloading {media_url}...")
        headers = {'Referer': original_referer_url}
        with requests.get(media_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(full_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        logger.info(f"Uploading {full_path} via userbot...")
        await app.start()
        
        if any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm']):
            sent_message = await app.send_video("me", full_path)
        else:
            sent_message = await app.send_photo("me", full_path)
        
        logger.info("Forwarding message to user...")
        await context.bot.forward_message(
            chat_id=chat_id,
            from_chat_id=sent_message.chat.id,
            message_id=sent_message.id
        )
        
        await app.delete_messages("me", sent_message.id)

    except Exception as e:
        logger.error(f"Failed during userbot upload/forward process: {e}")
        await context.bot.send_message(chat_id, "⚠️ Failed to process and forward the file.")
    finally:
        if os.path.exists(full_path):
            os.remove(full_path)
        if app.is_initialized:
            await app.stop()

# --- Main Bot Logic ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Universal Scraper Bot. Send an album or individual media link.")

async def process_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    initial_url = update.message.text.strip()
    chat_id = update.message.chat_id
    
    if '/album/' in initial_url:
        link_type = "Album"
    elif '/i/' in initial_url or '/post/' in initial_url:
        link_type = "Individual"
    else:
        await update.message.reply_text("❌ This doesn't look like a valid link.")
        return

    await update.message.reply_text(f"✅ {link_type} link detected. Fetching page...")

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(initial_url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        json_ld_script = soup.find('script', type='application/ld+json')

        if not json_ld_script:
            await update.message.reply_text("❌ Could not find the hidden data map.")
            return
            
        data = json.loads(json_ld_script.string, strict=False)
        media_urls = []

        # This flexible parsing logic works for both album and individual pages
        if 'image' in data and isinstance(data.get('image'), list):
            for img_obj in data['image']:
                if 'contentUrl' in img_obj: media_urls.append(img_obj['contentUrl'])
        
        if 'video' in data and isinstance(data.get('video'), list):
            for vid_obj in data['video']:
                if 'contentUrl' in vid_obj: media_urls.append(vid_obj['contentUrl'])
        
        if 'video' in data and isinstance(data.get('video'), dict):
            if 'contentUrl' in data['video']: media_urls.append(data['video']['contentUrl'])

        if 'image' in data and isinstance(data.get('image'), dict):
            if 'contentUrl' in data['image']: media_urls.append(data['image']['contentUrl'])

        if not media_urls:
            await update.message.reply_text("Found data map, but no media links inside.")
            return

        media_urls = sorted(list(set(media_urls)))

        await update.message.reply_text(f"👍 Found {len(media_urls)} file(s). Processing with userbot...")
        for media_url in media_urls:
            await upload_with_userbot_and_forward(media_url, initial_url, chat_id, context)
        
        await update.message.reply_text("✅ All tasks complete.")

    except Exception as e:
        logger.error(f"A critical error occurred: {e}", exc_info=True)
        await context.bot.send_message(chat_id, f"An unexpected error occurred: {type(e).__name__}")

# --- Main Bot Setup ---
def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING]):
        raise ValueError("One or more required environment variables are missing!")

    request = HTTPXRequest(connect_timeout=10.0, read_timeout=30.0)
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_url))
    
    print("Universal Userbot-Powered Bot is running on the server...")
    application.run_polling()

if __name__ == '__main__':
    main()
