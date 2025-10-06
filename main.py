import logging
import asyncio
import os
import requests
import re
import tempfile
import ffmpeg # <-- New import
import shutil # <-- New import
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

# --- Concurrency Limiter ---
CONCURRENCY_LIMIT = 2

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Global Cache & Filters ---
last_sent_stats = {}
admin_filter = filters.User(user_id=ADMIN_IDS)

# --- REVISED CORE TASK LOGIC ---
async def process_single_file(semaphore: asyncio.Semaphore, user_id: int, media_url: str, referer_url: str):
    """
    Acquires a semaphore, then downloads and sends a file with a generated thumbnail to the main bot for relay.
    """
    async with semaphore:
        full_path = ""
        thumb_path = None
        try:
            user_config = database.get_user_config(user_id) or {}
            target_chat_id = user_config.get('target_chat_id', user_id)

            local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
            temp_dir = tempfile.gettempdir(); full_path = os.path.join(temp_dir, local_filename)

            logger.info(f"Downloading {media_url} for user {user_id}")
            headers = {'Referer': referer_url}
            with requests.get(media_url, headers=headers, stream=True) as r:
                r.raise_for_status()
                with open(full_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192): f.write(chunk)

            is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
            caption = f"FORWARD_TO::{target_chat_id}"

            # --- NEW: THUMBNAIL GENERATION LOGIC ---
            if is_video:
                try:
                    thumb_path = os.path.join(temp_dir, f"{os.path.basename(full_path)}.jpg")
                    probe = ffmpeg.probe(full_path)
                    video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
                    duration = float(video_stream['duration'])
                    
                    (
                        ffmpeg
                        .input(full_path, ss=min(1, duration - 0.1)) # Take thumbnail from 1s mark or earlier
                        .output(thumb_path, vframes=1)
                        .overwrite_output()
                        .run(capture_stdout=True, capture_stderr=True)
                    )
                    logger.info(f"Thumbnail generated for {local_filename} at {thumb_path}")
                except Exception as e:
                    logger.error(f"Failed to generate thumbnail for {local_filename}: {e}")
                    thumb_path = None # Proceed without a thumbnail if generation fails

            async with PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING) as userbot:
                while True:
                    try:
                        logger.info(f"Uploading {local_filename} to bot {BOT_USERNAME} for relay to {target_chat_id}...")
                        if is_video:
                            await userbot.send_video(BOT_USERNAME, full_path, caption=caption, thumb=thumb_path)
                        else:
                            await userbot.send_photo(BOT_USERNAME, full_path, caption=caption)
                        logger.info(f"Successfully sent {local_filename} to bot for relay.")
                        database.update_stats('videos_sent' if is_video else 'images_sent')
                        break
                    except FloodWait as e:
                        logger.warning(f"FloodWait received. Sleeping for {e.value + 5} seconds.")
                        await asyncio.sleep(e.value + 5)

        except Exception as e:
            logger.error(f"Failed to process {media_url} for user {user_id}: {e}")
        finally:
            if os.path.exists(full_path): os.remove(full_path)
            if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)

# --- Background Worker (for Frenzy Mode) ---
async def frenzy_worker_loop():
    logger.info("Frenzy mode background worker started.")
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT) # Frenzy worker also respects the limit
    while True:
        job = database.get_job()
        if job:
            # Note: We now pass the semaphore to the processing function
            await process_single_file(semaphore, job['user_id'], job['media_url'], job['referer_url'])
            database.complete_job(job['_id'])
        else:
            await asyncio.sleep(5)

# (The rest of your main.py file remains the same... paste this function and the new imports at the top,
# and ensure the rest of the file (handlers, main function, etc.) is unchanged from the last version I sent)

# --- NEW FORWARDER HANDLER ---
async def forwarder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.caption or not update.message.caption.startswith("FORWARD_TO::"):
        return
    try:
        destination_chat_id = int(update.message.caption.split("::")[1])
        logger.info(f"Received file from userbot, re-sending to {destination_chat_id}")
        if update.message.video:
            await context.bot.send_video(chat_id=destination_chat_id, video=update.message.video.file_id)
        elif update.message.photo:
            await context.bot.send_photo(chat_id=destination_chat_id, photo=update.message.photo[-1].file_id)
    except Exception as e:
        logger.error(f"Failed to re-send message from userbot: {e}")

# --- PTB Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    database.set_frenzy_mode(user.id, False)
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
    text = update.message.text
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    if not urls: return

    scraped_media = []
    await update.message.reply_text(f"Found {len(urls)} link(s). Scraping for media...")
    for url in urls:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 ...'}
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            thumbnail_links = soup.select("a.spotlight[data-media]")
            for link in thumbnail_links:
                media_url = link.get('data-src-mp4') if link.get('data-media') == 'video' else link.get('href')
                if media_url:
                    scraped_media.append((media_url, url))
        except Exception as e:
            logger.error(f"Failed to scrape {url}: {e}")

    if not scraped_media:
        await update.message.reply_text("Could not find any media on the page(s).")
        return

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

# --- Admin, Stats, and Other Handlers ---
async def target_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(context.args[0]); database.set_user_target(update.effective_user.id, target_id)
        await update.message.reply_text(f"✅ Target chat ID set to `{target_id}`.")
    except (IndexError, ValueError): await update.message.reply_text("Usage: `/target <chat_id>`")
async def cleartarget_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.clear_user_target(update.effective_user.id)
    await update.message.reply_text("✅ Target chat cleared.")
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_config = database.get_user_config(update.effective_user.id)
    frenzy_status = "ON" if user_config.get("frenzy_mode_active", False) else "OFF"
    target_status = user_config.get("target_chat_id", "Default (you)")
    await update.message.reply_text(f"**Your Settings**\n- Frenzy Mode: `{frenzy_status}`\n- Target: `{target_status}`", parse_mode='Markdown')
async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    jobs = database.get_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"You have {len(jobs)} pending jobs.")
async def clear_queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    deleted_count = database.clear_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"✅ Cleared {deleted_count} pending jobs.")
async def set_supergroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.message.chat
    if not chat.is_forum:
        await update.message.reply_text("Must be used in a supergroup with Topics enabled.")
        return
    await update.message.reply_text("Setting up supergroup...")
    try:
        topic_names = ["Jobs", "Logs", "User Activity", "Stats"]; topic_ids = {}
        for name in topic_names:
            topic = await context.bot.create_forum_topic(chat_id=chat.id, name=name)
            topic_ids[name.lower().replace(" ", "_")] = topic.message_thread_id
        database.set_supergroup(chat.id, topic_ids)
        await update.message.reply_text("✅ Supergroup configured.")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}.")
async def check_and_send_stats(bot: Bot):
    global last_sent_stats; current_stats = database.get_stats()
    if current_stats and current_stats != last_sent_stats:
        settings = database.get_settings();
        if not settings: return
        stats_message = "📊 **Bot Stats**\n\n";
        for key, value in current_stats.items():
            if key != '_id': stats_message += f"**{key.replace('_', ' ').title()}**: `{value}`\n"
        await log_to_topic(bot, 'stats', stats_message)
        last_sent_stats = current_stats; logger.info("Sent stats update.")
async def log_to_topic(bot: Bot, topic_key: str, text: str):
    settings = database.get_settings()
    if settings and 'supergroup_id' in settings:
        topic_ids = settings.get('topic_ids', {})
        if topic_key in topic_ids:
            try: await bot.send_message(chat_id=settings['supergroup_id'], message_thread_id=topic_ids[topic_key], text=text, parse_mode='Markdown')
            except Exception as e: logger.error(f"Failed to log to topic '{topic_key}': {e}")

# --- Keep-Alive Server and Main Runner ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text/plain'); self.end_headers(); self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000));
    httpd = HTTPServer(('', port), HealthCheckHandler)
    logger.info(f"Keep-alive server on port {port}"); httpd.serve_forever()

async def main():
    # --- DIAGNOSTIC CHECK ---
    if not shutil.which("ffmpeg"):
        logger.error("CRITICAL: ffmpeg is not installed or not in the system's PATH. Video processing will fail.")
        # Depending on the desired behavior, you might want to exit here
        # return
    else:
        logger.info("ffmpeg installation confirmed.")

    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING, ADMIN_IDS, BOT_USERNAME, USERBOT_USER_ID]):
        raise ValueError("One or more required environment variables are missing!")

    web_thread = Thread(target=run_web_server, daemon=True); web_thread.start()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    # ... (add all other handlers here as before)
    application.add_handler(CommandHandler("setsupergroup", set_supergroup_command, filters=admin_filter))
    application.add_handler(CommandHandler("frenzy", frenzy_command))
    application.add_handler(CommandHandler("cf", cf_command))
    application.add_handler(CommandHandler("target", target_command))
    application.add_handler(CommandHandler("cleartarget", cleartarget_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("tasks", tasks_command))
    application.add_handler(CommandHandler("cc", clear_queue_command))
    application.add_handler(CommandHandler("ce", clear_queue_command))

    user_filter = filters.User(user_id=USERBOT_USER_ID)
    application.add_handler(MessageHandler(filters.PHOTO & user_filter, forwarder_handler))
    application.add_handler(MessageHandler(filters.VIDEO & user_filter, forwarder_handler))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    scheduler = AsyncIOScheduler(); scheduler.add_job(check_and_send_stats, 'interval', minutes=15, args=[application.bot]);

    async with application:
        scheduler.start()
        worker_task = asyncio.create_task(frenzy_worker_loop())
        logger.info("Main bot, scheduler, and worker loop are starting concurrently.")
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
        try:
            await asyncio.Future()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutdown signal received.")
        finally:
            if scheduler.running: scheduler.shutdown()
            if worker_task: worker_task.cancel(); await asyncio.sleep(1)

if __name__ == '__main__':
    print("Starting Dual-Mode Bot with Forced Thumbnail Generation...")
    asyncio.run(main())
