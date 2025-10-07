import logging
import asyncio
import os
import requests
import re
import tempfile
import ffmpeg
import shutil
from bs4 import BeautifulSoup
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Telegram (PTB) imports
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Pyrogram imports
from pyrogram import Client as PyrogramClient
from pyrogram.errors import FloodWait

# Local imports
import database

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")
ADMIN_IDS = [int(admin_id) for admin_id in os.environ.get("ADMIN_IDS", "").split()]
BOT_USERNAME = os.environ.get("BOT_USERNAME")
USERBOT_USER_ID = int(os.environ.get("USERBOT_USER_ID"))
CONCURRENCY_LIMIT = 2

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

last_sent_stats = {}
admin_filter = filters.User(user_id=ADMIN_IDS)

# --- CORE TASK LOGIC ---
async def process_single_file(semaphore: asyncio.Semaphore, user_id: int, media_url: str, referer_url: str):
    async with semaphore:
        full_path, thumb_path = "", None
        try:
            target_chat_id = database.get_user_config(user_id).get('target_chat_id', user_id)
            caption = f"FORWARD_TO::{target_chat_id}"
            local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
            temp_dir = tempfile.gettempdir(); full_path = os.path.join(temp_dir, local_filename)
            with requests.get(media_url, headers={'Referer': referer_url}, stream=True) as r:
                r.raise_for_status()
                with open(full_path, 'wb') as f: shutil.copyfileobj(r.raw, f)
            is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
            if is_video:
                duration, width, height = 0,0,0
                try:
                    probe = ffmpeg.probe(full_path)
                    video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
                    if video_stream:
                        duration, width, height = int(float(video_stream.get('duration', 0))), int(video_stream.get('width', 0)), int(video_stream.get('height', 0))
                    thumb_path = os.path.join(temp_dir, f"{os.path.basename(full_path)}.jpg")
                    (ffmpeg.input(full_path, ss=min(1, duration - 0.1) if duration > 1 else 0).output(thumb_path, vframes=1).overwrite_output().run(capture_stdout=True, capture_stderr=True))
                except Exception as e:
                    logger.error(f"Metadata/thumbnail failed: {e}"); thumb_path = None
            async with PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING) as userbot:
                while True:
                    try:
                        if is_video:
                            await userbot.send_video(BOT_USERNAME, full_path, caption=caption, thumb=thumb_path, duration=duration, width=width, height=height)
                        else:
                            await userbot.send_photo(BOT_USERNAME, full_path, caption=caption)
                        database.update_stats('videos_sent' if is_video else 'images_sent')
                        break
                    except FloodWait as e: await asyncio.sleep(e.value + 5)
        except Exception as e:
            logger.error(f"Processing failed for {media_url}: {e}")
        finally:
            if os.path.exists(full_path): os.remove(full_path)
            if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)

# --- Background Worker ---
async def frenzy_worker_loop():
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    while True:
        job = database.get_job()
        if job:
            await process_single_file(semaphore, job['user_id'], job['media_url'], job['referer_url'])
            database.complete_job(job['_id'])
        else:
            await asyncio.sleep(5)

# --- FORWARDER HANDLER ---
async def forwarder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.caption or not update.message.caption.startswith("FORWARD_TO::"): return
    try:
        destination_chat_id = int(update.message.caption.split("::")[1])
        if update.message.video:
            await context.bot.send_video(chat_id=destination_chat_id, video=update.message.video.file_id)
        elif update.message.photo:
            await context.bot.send_photo(chat_id=destination_chat_id, photo=update.message.photo[-1].file_id)
    except Exception as e:
        logger.error(f"Failed to re-send message: {e}")

# --- PTB Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, False)
    await update.message.reply_text("Bot ready. Send a link to process it. Use `/frenzy` to queue multiple links.")

async def frenzy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, True)
    await update.message.reply_text("✅ **Frenzy Mode Activated**. I will now queue all links from your messages.")

async def cf_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, False)
    await update.message.reply_text("⛔ **Normal Mode Activated**. I will process links in small batches.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_in_frenzy = database.get_user_config(user_id).get("frenzy_mode_active", False)
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', update.message.text)
    if not urls: return
    await update.message.reply_text(f"Found {len(urls)} link(s). Scraping for media...")
    scraped_media = []
    for url in urls:
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 ...'})
            for link in BeautifulSoup(response.text, 'html.parser').select("a.spotlight[data-media]"):
                media_url = link.get('data-src-mp4') if link.get('data-media') == 'video' else link.get('href')
                if media_url: scraped_media.append((media_url, url))
        except Exception as e: logger.error(f"Failed to scrape {url}: {e}")
    if not scraped_media:
        await update.message.reply_text("Could not find any media on the page(s)."); return
    if is_in_frenzy:
        for media_url, referer_url in scraped_media:
            database.add_job(user_id, media_url, referer_url)
        await update.message.reply_text(f"✅ Queued {len(scraped_media)} media files. The background worker will process them.")
    else:
        await update.message.reply_text(f"Found {len(scraped_media)} files. Processing in small batches to ensure stability...")
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [process_single_file(semaphore, user_id, media_url, referer_url) for media_url, referer_url in scraped_media]
        await asyncio.gather(*tasks)
        await update.message.reply_text("✅ Task complete.")

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text/plain'); self.end_headers(); self.wfile.write(b"ok")
def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    httpd = HTTPServer(('', port), HealthCheckHandler)
    logger.info(f"Keep-alive server on port {port}"); httpd.serve_forever()

async def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING, BOT_USERNAME, USERBOT_USER_ID]):
        raise ValueError("One or more required environment variables are missing!")
    web_thread = Thread(target=run_web_server, daemon=True); web_thread.start()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # --- THE WORKING HANDLER REGISTRATION ---
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("frenzy", frenzy_command))
    application.add_handler(CommandHandler("cf", cf_command))
    
    user_filter = filters.User(user_id=USERBOT_USER_ID)
    application.add_handler(MessageHandler(filters.PHOTO & user_filter, forwarder_handler))
    application.add_handler(MessageHandler(filters.VIDEO & user_filter, forwarder_handler))
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    scheduler = AsyncIOScheduler()
    async with application:
        scheduler.start()
        worker_task = asyncio.create_task(frenzy_worker_loop())
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
        try: await asyncio.Future()
        except (KeyboardInterrupt, SystemExit): logger.info("Shutdown signal received.")
        finally:
            if scheduler.running: scheduler.shutdown()
            if worker_task: worker_task.cancel(); await asyncio.sleep(1)

if __name__ == '__main__':
    print("Starting Bot in Restored Stable Mode...")
    asyncio.run(main())
