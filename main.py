import logging
import asyncio
import os
import requests
import re
import tempfile
import ffmpeg
import shutil
import time
from functools import wraps
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

# --- NEW: Logging Decorator ---
def log_user_activity(func):
    """Decorator that logs user activity for command handlers."""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if user and update.message.text.startswith('/'):
            log_message = (
                f"👤 **User Activity**\n\n"
                f"**Name:** {user.full_name}\n"
                f"**Username:** @{user.username}\n"
                f"**ID:** `{user.id}`\n"
                f"**Command:** `{update.message.text}`"
            )
            await log_to_topic(context.bot, 'user_activity', log_message)
        return await func(update, context, *args, **kwargs)
    return wrapped

# --- CORE TASK LOGIC ---
async def process_single_file(semaphore: asyncio.Semaphore, user_id: int, media_url: str, referer_url: str, task_id: str):
    async with semaphore:
        full_path, thumb_path = "", None
        try:
            target_chat_id = database.get_user_config(user_id).get('target_chat_id', user_id)
            local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
            temp_dir = tempfile.gettempdir(); full_path = os.path.join(temp_dir, local_filename)

            with requests.get(media_url, headers={'Referer': referer_url}, stream=True) as r:
                r.raise_for_status()
                with open(full_path, 'wb') as f: shutil.copyfileobj(r.raw, f)

            is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
            caption = f"FORWARD_TO::{target_chat_id}::{task_id}"

            duration, width, height = 0, 0, 0
            if is_video:
                try:
                    probe = ffmpeg.probe(full_path)
                    video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
                    if video_stream:
                        duration = int(float(video_stream.get('duration', 0)))
                        width = int(video_stream.get('width', 0)); height = int(video_stream.get('height', 0))
                    thumb_path = os.path.join(temp_dir, f"{os.path.basename(full_path)}.jpg")
                    (ffmpeg.input(full_path, ss=min(1, duration - 0.1) if duration > 1 else 0)
                     .output(thumb_path, vframes=1).overwrite_output()
                     .run(capture_stdout=True, capture_stderr=True))
                except Exception as e:
                    logger.error(f"Metadata/thumbnail failed for {local_filename}: {e}"); thumb_path = None

            async with PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING) as userbot:
                while True:
                    try:
                        if is_video:
                            await userbot.send_video(BOT_USERNAME, full_path, caption=caption, thumb=thumb_path, duration=duration, width=width, height=height)
                        else:
                            await userbot.send_photo(BOT_USERNAME, full_path, caption=caption)
                        database.update_stats('videos_sent' if is_video else 'images_sent')
                        break
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 5)
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
            await process_single_file(semaphore, job['user_id'], job['media_url'], job['referer_url'], job['task_id'])
            database.complete_job(job['_id'])
        else:
            await asyncio.sleep(5)

# --- FORWARDER HANDLER ---
async def forwarder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.caption or not update.message.caption.startswith("FORWARD_TO::"): return
    try:
        parts = update.message.caption.split("::")
        if len(parts) != 3: return
        
        destination_chat_id, task_id = int(parts[1]), parts[2]
        media_type, file_id = "", ""

        if update.message.video:
            media_type, file_id = "video", update.message.video.file_id
            await context.bot.send_video(chat_id=destination_chat_id, video=file_id)
        elif update.message.photo:
            media_type, file_id = "photo", update.message.photo[-1].file_id
            await context.bot.send_photo(chat_id=destination_chat_id, photo=file_id)

        if media_type and file_id:
            database.add_file_detail(task_id, file_id, media_type)
    except Exception as e:
        logger.error(f"Failed to re-send message: {e}")

# --- PTB Handlers ---
@log_user_activity
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, False)
    await update.message.reply_text("✨ **System Online.** Ready for links.\nUse `/frenzy` for bulk processing.")

@log_user_activity
async def frenzy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, True)
    await update.message.reply_text("⛩️ **Frenzy Mode Engaged.** All submitted links will be added to the queue.")

@log_user_activity
async def cf_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, False)
    await update.message.reply_text("⚙️ **Normal Mode Engaged.** Processing links one by one, in real-time.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = update.effective_user
    is_in_frenzy = database.get_user_config(user_id).get("frenzy_mode_active", False)
    text = update.message.text
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    if not urls: return

    task_id = f"{user_id}_{int(time.time())}"
    scraped_media, scraped_count = [], 0
    await update.message.reply_text(f"📡 **Scanning {len(urls)} link(s)...**")
    for url in urls:
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 ...'})
            soup = BeautifulSoup(response.text, 'html.parser')
            for link in soup.select("a.spotlight[data-media]"):
                media_url = link.get('data-src-mp4') if link.get('data-media') == 'video' else link.get('href')
                if media_url:
                    scraped_media.append((media_url, url)); scraped_count += 1
        except Exception as e: logger.error(f"Failed to scrape {url}: {e}")

    if not scraped_media:
        await update.message.reply_text(" ghostly **Scan Complete.** No valid media found on the page(s)."); return
    
    videos_count = sum(1 for media_url, _ in scraped_media if any(ext in media_url.lower() for ext in ['.mp4', '.mov', '.webm']))
    images_count = len(scraped_media) - videos_count

    log_message = (
        f"🔗 **Link Submission**\n\n"
        f"**User:** {user.full_name} (`{user.id}`)\n"
        f"**Link(s):**\n" + "\n".join(f"`{u}`" for u in urls) + "\n\n"
        f"**Media Found:** {scraped_count}\n"
        + (f"**Videos:** {videos_count}\n" if videos_count > 0 else "")
        + (f"**Images:** {images_count}\n" if images_count > 0 else "") +
        f"\n**Fetch Command:** `/fetch_{task_id}`"
    )
    await log_to_topic(context.bot, 'user_activity', log_message)

    if is_in_frenzy:
        for media_url, referer_url in scraped_media:
            database.add_job(user_id, media_url, referer_url, task_id)
        await log_to_topic(context.bot, 'jobs', f"💼 Queued {len(scraped_media)} jobs from task `{task_id}` for user `{user_id}`.")
        await update.message.reply_text(f"⛩️ **Queue Updated.** Added {scraped_count} files to the processing pipeline.")
    else:
        await update.message.reply_text(f"📥 **Scrape Complete.** Found {scraped_count} files. Initiating transfer...")
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [process_single_file(semaphore, user_id, media_url, referer_url, task_id) for media_url, referer_url in scraped_media]
        await asyncio.gather(*tasks)
        await update.message.reply_text(f"✨ **Task `{task_id}` Complete.**")

@log_user_activity
async def fetch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        task_id = update.message.text.split('_')[1]
    except IndexError:
        await update.message.reply_text("🏮 **Invalid Command.** Use the format `/fetch_taskID`."); return

    details = database.get_file_details(task_id)
    if not details or not details.get('files'):
        await update.message.reply_text(f" ghostly **Not Found.** No files are associated with task ID `{task_id}`."); return
    
    await update.message.reply_text(f" Retrieving Archive.** Fetching {len(details['files'])} files for task `{task_id}`...")
    for file in details['files']:
        try:
            if file['media_type'] == 'video':
                await context.bot.send_video(update.effective_chat.id, file['file_id'])
            else:
                await context.bot.send_photo(update.effective_chat.id, file['file_id'])
        except Exception as e:
            logger.error(f"Failed to send file {file['file_id']} for task {task_id}: {e}")
            await update.message.reply_text(f"🏮 **Delivery Error.** Failed to send one of the files from task `{task_id}`.")
        await asyncio.sleep(1)

@log_user_activity
async def target_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(context.args[0])
        database.set_user_target(update.effective_user.id, target_id)
        await update.message.reply_text(f"🎯 **Target Acquired.** All future media will be sent to `{target_id}`.")
    except (IndexError, ValueError):
        await update.message.reply_text("🏮 **Syntax Error.** Please use `/target <chat_id>`.")

@log_user_activity
async def cleartarget_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.clear_user_target(update.effective_user.id)
    await update.message.reply_text("🎯 **Target Cleared.** Media will now be sent to this chat by default.")

@log_user_activity
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_config = database.get_user_config(update.effective_user.id)
    frenzy_mode_active = user_config.get("frenzy_mode_active", False)
    frenzy_status = "⛩️ Frenzy (Queued)" if frenzy_mode_active else "⚙️ Normal (Real-time)"
    target_status = user_config.get("target_chat_id", "This Chat")
    await update.message.reply_text(
        f"📜 **Current Status**\n\n"
        f"∙ **Mode:** `{frenzy_status}`\n"
        f"∙ **Destination:** `{target_status}`",
        parse_mode='Markdown'
    )

@log_user_activity
async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    jobs = database.get_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"⏳ **Queue Status.** There are {len(jobs)} jobs pending.")

@log_user_activity
async def clear_queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    deleted_count = database.clear_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"🗑️ **Queue Purged.** Removed {deleted_count} pending jobs.")

@log_user_activity
async def set_supergroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.message.chat
    if not chat.is_forum:
        await update.message.reply_text("🏮 **Configuration Error.** This command must be used in a supergroup with Topics enabled.")
        return
    await update.message.reply_text("Initializing supergroup integration...")
    try:
        topic_names = ["Jobs", "Logs", "User Activity", "Stats"]; topic_ids = {}
        for name in topic_names:
            topic = await context.bot.create_forum_topic(chat_id=chat.id, name=name)
            topic_ids[name.lower().replace(" ", "_")] = topic.message_thread_id
        database.set_supergroup(chat.id, topic_ids)
        await update.message.reply_text("💠 **Supergroup Linked.** Logging channels are now active.")
    except Exception as e:
        await update.message.reply_text(f"🏮 **Setup Failed:** {e}.")

async def check_and_send_stats(bot: Bot):
    global last_sent_stats; current_stats = database.get_stats()
    if current_stats and current_stats != last_sent_stats:
        settings = database.get_settings()
        if not settings: return
        stats_message = "📊 **Bot Stats**\n\n"
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

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text/plain'); self.end_headers(); self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    httpd = HTTPServer(('', port), HealthCheckHandler)
    logger.info(f"Keep-alive server on port {port}"); httpd.serve_forever()

async def main():
    if not shutil.which("ffmpeg"): logger.error("CRITICAL: ffmpeg is not installed or not in the system's PATH.")
    else: logger.info("ffmpeg installation confirmed.")
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING, ADMIN_IDS, BOT_USERNAME, USERBOT_USER_ID]):
        raise ValueError("One or more required environment variables are missing!")
    web_thread = Thread(target=run_web_server, daemon=True); web_thread.start()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("setsupergroup", set_supergroup_command, filters=admin_filter))
    application.add_handler(CommandHandler("frenzy", frenzy_command))
    application.add_handler(CommandHandler("cf", cf_command))
    application.add_handler(CommandHandler("target", target_command))
    application.add_handler(CommandHandler("cleartarget", cleartarget_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("tasks", tasks_command))
    application.add_handler(CommandHandler("cc", clear_queue_command))
    application.add_handler(CommandHandler("ce", clear_queue_command))

    application.add_handler(MessageHandler(filters.Regex(r'^/fetch_'), fetch_command))

    user_filter = filters.User(user_id=USERBOT_USER_ID)
    application.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO) & user_filter, forwarder_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    scheduler = AsyncIOScheduler(); scheduler.add_job(check_and_send_stats, 'interval', minutes=15, args=[application.bot])

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
    print("Starting Bot with Full Logging and Fetching Capabilities...")
    asyncio.run(main())
