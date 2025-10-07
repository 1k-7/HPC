import logging
import asyncio
import os
import requests
import re
import tempfile
import ffmpeg
import shutil
import time
import html  # <-- THE CRITICAL FIX
from functools import wraps
from bs4 import BeautifulSoup
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Telegram (PTB) imports
from telegram import Update, Bot
from telegram.constants import ParseMode
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

# --- Logging Decorator ---
def log_user_activity(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if user and update.message.text and update.message.text.startswith('/'):
            user_info = f"<b>Name:</b> {html.escape(user.full_name)}\n"
            if user.username:
                user_info += f"<b>Username:</b> @{user.username}\n"
            user_info += f"<b>ID:</b> <code>{user.id}</code>\n"
            log_message = (f"👤 <b>User Activity</b>\n\n{user_info}<b>Command:</b> <code>{html.escape(update.message.text)}</code>")
            await log_to_topic(context.bot, 'user_activity', log_message)
        return await func(update, context, *args, **kwargs)
    return wrapped

# --- CORE TASK LOGIC ---
async def process_single_file(semaphore: asyncio.Semaphore, user_id: int, media_url: str, referer_url: str, task_id: str):
    async with semaphore:
        full_path, thumb_path = "", None
        try:
            target_chat_id = database.get_user_config(user_id).get('target_chat_id', user_id)
            caption = f"FORWARD_TO::{target_chat_id}::{task_id}"
            local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
            temp_dir = tempfile.gettempdir(); full_path = os.path.join(temp_dir, local_filename)
            with requests.get(media_url, headers={'Referer': referer_url}, stream=True) as r:
                r.raise_for_status();
                with open(full_path, 'wb') as f: shutil.copyfileobj(r.raw, f)
            is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
            if is_video:
                duration, width, height = 0,0,0
                try:
                    probe = ffmpeg.probe(full_path)
                    video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
                    if video_stream:
                        duration, width, height = int(float(video_stream.get('duration',0))), int(video_stream.get('width',0)), int(video_stream.get('height',0))
                    thumb_path = os.path.join(temp_dir, f"{os.path.basename(full_path)}.jpg")
                    (ffmpeg.input(full_path, ss=min(1, duration - 0.1) if duration > 1 else 0).output(thumb_path, vframes=1).overwrite_output().run(capture_stdout=True, capture_stderr=True))
                except Exception as e:
                    logger.error(f"Metadata failed: {e}"); thumb_path = None
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
    await update.message.reply_text("✨ <b>System Online.</b>\nReady for links. Use <code>/frenzy</code> for bulk processing.", parse_mode=ParseMode.HTML)

@log_user_activity
async def frenzy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, True)
    await update.message.reply_text("⛩️ <b>Frenzy Mode Engaged.</b>\nAll submitted links will be added to the queue.", parse_mode=ParseMode.HTML)

@log_user_activity
async def cf_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, False)
    await update.message.reply_text("⚙️ <b>Normal Mode Engaged.</b>\nProcessing links one by one, in real-time.", parse_mode=ParseMode.HTML)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user, user_id = update.effective_user, update.effective_user.id
    is_in_frenzy = database.get_user_config(user_id).get("frenzy_mode_active", False)
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', update.message.text)
    if not urls: return
    task_id = f"{user_id}_{int(time.time())}"
    await update.message.reply_text(f"📡 <b>Scanning {len(urls)} link(s)...</b>", parse_mode=ParseMode.HTML)
    scraped_media = []
    for url in urls:
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 ...'})
            for link in BeautifulSoup(response.text, 'html.parser').select("a.spotlight[data-media]"):
                media_url = link.get('data-src-mp4') if link.get('data-media') == 'video' else link.get('href')
                if media_url: scraped_media.append((media_url, url))
        except Exception as e: logger.error(f"Failed to scrape {url}: {e}")
    if not scraped_media:
        await update.message.reply_text("👻 <b>Scan Complete.</b>\nNo valid media found on the page(s).", parse_mode=ParseMode.HTML); return
    videos_count = sum(1 for media_url, _ in scraped_media if any(ext in media_url.lower() for ext in ['.mp4', '.mov', '.webm']))
    images_count = len(scraped_media) - videos_count
    log_message = (f"🔗 <b>Link Submission</b>\n\n<b>User:</b> {html.escape(user.full_name)} (<code>{user.id}</code>)\n<b>Link(s):</b>\n" + "\n".join(f"<code>{html.escape(u)}</code>" for u in urls) + f"\n\n<b>Media Found:</b> {len(scraped_media)}\n" + (f"<b>Videos:</b> {videos_count}\n" if videos_count > 0 else "") + (f"<b>Images:</b> {images_count}\n" if images_count > 0 else "") + f"\n<b>Fetch Command:</b> <code>/fetch_{task_id}</code>")
    await log_to_topic(context.bot, 'user_activity', log_message)
    if is_in_frenzy:
        for media_url, referer_url in scraped_media: database.add_job(user_id, media_url, referer_url, task_id)
        await log_to_topic(context.bot, 'jobs', f"💼 Queued {len(scraped_media)} jobs from task <code>{task_id}</code> for user <code>{user_id}</code>.")
        await update.message.reply_text(f"⛩️ <b>Queue Updated.</b>\nAdded {len(scraped_media)} files to the processing pipeline.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"📥 <b>Scrape Complete.</b>\nFound {len(scraped_media)} files. Initiating transfer...", parse_mode=ParseMode.HTML)
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [process_single_file(semaphore, user_id, media_url, referer_url, task_id) for media_url, referer_url in scraped_media]
        await asyncio.gather(*tasks)
        await update.message.reply_text(f"✨ <b>Task <code>{task_id}</code> Complete.</b>", parse_mode=ParseMode.HTML)

@log_user_activity
async def fetch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text.startswith('/fetch_'):
        await update.message.reply_text("🏮 <b>Invalid Command.</b>\nUse the format <code>/fetch_taskID</code>.", parse_mode=ParseMode.HTML); return
    task_id = update.message.text[len('/fetch_'):]
    details = database.get_file_details(task_id)
    if not details or not details.get('files'):
        await update.message.reply_text(f"👻 <b>Not Found.</b>\nNo files are associated with task ID <code>{task_id}</code>.", parse_mode=ParseMode.HTML); return
    await update.message.reply_text(f"📦 <b>Retrieving Archive.</b>\nFetching {len(details['files'])} files for task <code>{task_id}</code>...", parse_mode=ParseMode.HTML)
    for file in details['files']:
        try:
            if file['media_type'] == 'video': await context.bot.send_video(update.effective_chat.id, file['file_id'])
            else: await context.bot.send_photo(update.effective_chat.id, file['file_id'])
        except Exception as e:
            logger.error(f"Failed to send file {file['file_id']} for task {task_id}: {e}")
            await update.message.reply_text(f"🏮 <b>Delivery Error.</b>\nFailed to send one of the files from task <code>{task_id}</code>.", parse_mode=ParseMode.HTML)
        await asyncio.sleep(1)

@log_user_activity
async def target_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(context.args[0])
        database.set_user_target(update.effective_user.id, target_id)
        await update.message.reply_text(f"🎯 <b>Target Acquired.</b>\nAll future media will be sent to <code>{target_id}</code>.", parse_mode=ParseMode.HTML)
    except (IndexError, ValueError): await update.message.reply_text("🏮 <b>Syntax Error.</b>\nPlease use <code>/target &lt;chat_id&gt;</code>.", parse_mode=ParseMode.HTML)

@log_user_activity
async def cleartarget_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.clear_user_target(update.effective_user.id)
    await update.message.reply_text("🎯 <b>Target Cleared.</b>\nMedia will now be sent to this chat by default.", parse_mode=ParseMode.HTML)

@log_user_activity
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_config = database.get_user_config(update.effective_user.id)
    frenzy_mode_active = user_config.get("frenzy_mode_active", False)
    frenzy_status = "⛩️ Frenzy (Queued)" if frenzy_mode_active else "⚙️ Normal (Real-time)"
    target_status = user_config.get("target_chat_id", "This Chat")
    await update.message.reply_text(f"📜 <b>Current Status</b>\n\n∙ <b>Mode:</b> <code>{frenzy_status}</code>\n∙ <b>Destination:</b> <code>{target_status}</code>", parse_mode=ParseMode.HTML)

@log_user_activity
async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    jobs = database.get_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"⏳ <b>Queue Status.</b>\nThere are {len(jobs)} jobs pending.", parse_mode=ParseMode.HTML)

@log_user_activity
async def clear_queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    deleted_count = database.clear_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"🗑️ <b>Queue Purged.</b>\nRemoved {deleted_count} pending jobs.", parse_mode=ParseMode.HTML)

@log_user_activity
async def set_supergroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.message.chat
    if not chat.is_forum:
        await update.message.reply_text("🏮 <b>Configuration Error.</b>\nThis command must be used in a supergroup with Topics enabled.", parse_mode=ParseMode.HTML); return
    await update.message.reply_text("Initializing supergroup integration...", parse_mode=ParseMode.HTML)
    try:
        topic_names = ["Jobs", "Logs", "User Activity", "Stats"]; topic_ids = {}
        for name in topic_names:
            topic = await context.bot.create_forum_topic(chat_id=chat.id, name=name)
            topic_ids[name.lower().replace(" ", "_")] = topic.message_thread_id
        database.set_supergroup(chat.id, topic_ids)
        await update.message.reply_text("💠 <b>Supergroup Linked.</b>\nLogging channels are now active.", parse_mode=ParseMode.HTML)
    except Exception as e: await update.message.reply_text(f"🏮 <b>Setup Failed:</b> <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)

async def check_and_send_stats(bot: Bot):
    global last_sent_stats; current_stats = database.get_stats()
    if current_stats and current_stats != last_sent_stats:
        settings = database.get_settings();
        if not settings: return
        stats_message = "📊 <b>Bot Stats</b>\n\n"
        for key, value in current_stats.items():
            if key != '_id': stats_message += f"<b>{html.escape(key.replace('_', ' ').title())}:</b> <code>{value}</code>\n"
        await log_to_topic(bot, 'stats', stats_message)
        last_sent_stats = current_stats; logger.info("Sent stats update.")

async def log_to_topic(bot: Bot, topic_key: str, text: str):
    settings = database.get_settings()
    if settings and 'supergroup_id' in settings:
        topic_ids = settings.get('topic_ids', {})
        if topic_key in topic_ids:
            try: await bot.send_message(chat_id=settings['supergroup_id'], message_thread_id=topic_ids[topic_key], text=text, parse_mode=ParseMode.HTML)
            except Exception as e: logger.error(f"Failed to log to topic '{topic_key}': {e}")

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text/plain'); self.end_headers(); self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    httpd = HTTPServer(('', port), HealthCheckHandler)
    logger.info(f"Keep-alive server on port {port}"); httpd.serve_forever()

async def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING, ADMIN_IDS, BOT_USERNAME, USERBOT_USER_ID]):
        raise ValueError("One or more required environment variables are missing!")
    web_thread = Thread(target=run_web_server, daemon=True); web_thread.start()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # --- Simple, linear handler registration ---
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

    user_filter = filters.User(user_id=USERBOT_USER_ID)
    application.add_handler(MessageHandler(filters.Regex(r'^/fetch_'), fetch_command))
    application.add_handler(MessageHandler(filters.PHOTO & user_filter, forwarder_handler))
    application.add_handler(MessageHandler(filters.VIDEO & user_filter, forwarder_handler))
    
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
    print("Starting Bot...")
    asyncio.run(main())
