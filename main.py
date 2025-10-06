import logging
import asyncio
import os
import requests
import re
import tempfile
import json
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

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Global Cache & Filters ---
last_sent_stats = {}
admin_filter = filters.User(user_id=ADMIN_IDS)
worker_bots_cache = {}

# --- Centralized Telegram Logger ---
async def log_to_topic(bot: Bot, topic_key: str, text: str):
    """Fetches settings from DB and sends a log message to the correct topic."""
    settings = database.get_settings()
    if settings and 'supergroup_id' in settings:
        topic_ids = settings.get('topic_ids', {})
        if topic_key in topic_ids:
            try:
                await bot.send_message(
                    chat_id=settings['supergroup_id'],
                    message_thread_id=topic_ids[topic_key],
                    text=text,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Failed to log to topic '{topic_key}': {e}")
        else:
            logger.warning(f"Topic key '{topic_key}' not found in DB settings.")
    # If settings aren't configured, the log will just appear in the console.

# --- Background Worker Logic ---
async def worker_loop(ptb_bot_instance: Bot):
    """Continuously polls the database for new jobs and processes them."""
    logger.info("Background worker loop started.")
    userbot = PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING)
    await userbot.start()
    logger.info("Userbot client started for worker loop.")

    while True:
        job = database.get_job()
        if job:
            job_id = job['_id']
            logger.info(f"Worker picked up job: {job_id}")
            user_id, media_url, referer_url = job['user_id'], job['media_url'], job['referer_url']
            full_path = ""
            try:
                # 1. Download
                local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
                temp_dir = tempfile.gettempdir()
                full_path = os.path.join(temp_dir, local_filename)
                headers = {'Referer': referer_url}
                with requests.get(media_url, headers=headers, stream=True) as r:
                    r.raise_for_status();
                    with open(full_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
                
                is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
                
                # 2. Get user config for this job
                user_config = database.get_user_config(user_id) or {}
                target_chat_id = user_config.get('target_chat_id', user_id)
                worker_tokens = user_config.get('worker_tokens', [])
                
                # 3. Upload and Send
                sent_msg = await userbot.send_video("me", full_path, caption=f"For user {user_id}") if is_video else await userbot.send_photo("me", full_path, caption=f"For user {user_id}")
                
                # The MAIN bot does the forwarding
                await ptb_bot_instance.forward_message(
                    chat_id=target_chat_id,
                    from_chat_id=sent_msg.chat.id,
                    message_id=sent_msg.id
                )
                await sent_msg.delete()
                
                database.update_stats('videos_sent' if is_video else 'images_sent')
                database.complete_job(job_id)
                await log_to_topic(ptb_bot_instance, 'jobs', f"✅ Processed job for user `{user_id}`:\n`{media_url}`")
            except Exception as e:
                logger.error(f"Failed to process job {job_id}: {e}")
                await log_to_topic(ptb_bot_instance, 'logs', f"🔥 **Worker Error** on job `{job_id}`:\n`{e}`")
                database.complete_job(job_id)
            finally:
                if os.path.exists(full_path):
                    os.remove(full_path)
        else:
            await asyncio.sleep(5)

# --- PTB Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    database.set_frenzy_mode(user.id, True)
    database.update_stats('users_started')
    await log_to_topic(context.bot, 'user_activity', f"User started bot: {user.full_name} (`@{user.username}`, ID: `{user.id}`)")
    await update.message.reply_text(
        "Bot ready. I am in **frenzy mode** by default.\n\n"
        "`/cf` - Pause link processing.\n"
        "`/frenzy` - Resume link processing.\n"
        "`/target <chat_id>` - Set a custom destination.\n"
        "`/cleartarget` - Send files back to you.\n"
        "`/status` - View your current settings.\n"
        "`/tasks` - View your pending jobs.\n"
        "`/cc` or `/ce` - Clear all pending jobs."
    )

async def frenzy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, True)
    await update.message.reply_text("✅ Frenzy mode **activated**.")

async def cf_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, False)
    await update.message.reply_text("⛔ Frenzy mode **deactivated**.")

async def target_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(context.args[0]); database.set_user_target(update.effective_user.id, target_id)
        await update.message.reply_text(f"✅ Target chat ID set to `{target_id}`.")
    except (IndexError, ValueError): await update.message.reply_text("Usage: `/target <chat_id>`")

async def cleartarget_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.clear_user_target(update.effective_user.id)
    await update.message.reply_text("✅ Target chat cleared. Files will now be sent back to you.")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_config = database.get_user_config(update.effective_user.id)
    frenzy_status = "ON" if user_config.get("frenzy_mode_active", True) else "OFF"
    target_status = user_config.get("target_chat_id", "Default (you)")
    await update.message.reply_text(f"**Your Current Settings**\n- Frenzy Mode: `{frenzy_status}`\n- Target Chat: `{target_status}`", parse_mode='Markdown')

async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    jobs = database.get_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"You have {len(jobs)} pending jobs in the queue.")

async def clear_queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    deleted_count = database.clear_pending_jobs(update.effective_user.id)
    await update.message.reply_text(f"✅ Cleared {deleted_count} pending jobs.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = database.get_user_config(user_id)
    if not user_config.get("frenzy_mode_active", True): return

    text = update.message.text
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    if not urls: return

    await log_to_topic(context.bot, 'user_activity', f"User `{user_id}` submitted links:\n" + "\n".join(f"`{url}`" for url in urls))
    await update.message.reply_text(f"Found {len(urls)} links. Scraping and adding to queue...")
    
    scraped_count = 0
    for url in urls:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 ...'}
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            thumbnail_links = soup.select("a.spotlight[data-media]")
            for link in thumbnail_links:
                media_url = link.get('data-src-mp4') if link.get('data-media') == 'video' else link.get('href')
                if media_url:
                    database.add_job(user_id, media_url, url)
                    scraped_count += 1
        except Exception as e:
            logger.error(f"Failed to scrape {url}: {e}")
    
    await update.message.reply_text(f"✅ Queued {scraped_count} media files.")
    database.update_stats('links_processed', len(urls))

async def set_supergroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.message.chat
    if not chat.is_forum:
        await update.message.reply_text("This command must be used in a supergroup with Topics enabled.")
        return
    await update.message.reply_text("Setting up this supergroup... this may take a moment.")
    try:
        topic_names = ["Jobs", "Logs", "User Activity", "Stats"]
        topic_ids = {}
        for name in topic_names:
            topic = await context.bot.create_forum_topic(chat_id=chat.id, name=name)
            topic_ids[name.lower().replace(" ", "_")] = topic.message_thread_id
        database.set_supergroup(chat.id, topic_ids)
        await update.message.reply_text("✅ Supergroup configured and topics created successfully!")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}. Is the bot an admin with 'Manage Topics' permission?")

async def check_and_send_stats(bot: Bot):
    global last_sent_stats
    current_stats = database.get_stats()
    if current_stats and current_stats != last_sent_stats:
        stats_message = "📊 **Bot Statistics Update**\n\n";
        for key, value in current_stats.items():
            if key != '_id': stats_message += f"**{key.replace('_', ' ').title()}**: `{value}`\n"
        await log_to_topic(bot, 'stats', stats_message)
        last_sent_stats = current_stats
        logger.info("Sent stats update.")

# --- Keep-Alive Server and Main Runner ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text-plain'); self.end_headers(); self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000));
    httpd = HTTPServer(('', port), HealthCheckHandler)
    logger.info(f"Keep-alive server on port {port}"); httpd.serve_forever()

async def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING, ADMIN_IDS]):
        raise ValueError("Core environment variables are missing! (TOKEN, API_ID, HASH, SESSION, ADMIN_IDS)")

    web_thread = Thread(target=run_web_server, daemon=True)
    web_thread.start()

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add all handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("setsupergroup", set_supergroup_command, filters=admin_filter))
    application.add_handler(CommandHandler("frenzy", frenzy_command))
    application.add_handler(CommandHandler("cf", cf_command))
    application.add_handler(CommandHandler("target", target_command))
    application.add_handler(CommandHandler("cleartarget", cleartarget_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("tasks", tasks_command))
    application.add_handler(CommandHandler("cc", clear_queue_command))
    application.add_handler(CommandHandler("ce", clear_queue_command)) # Alias
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_and_send_stats, 'interval', minutes=15, args=[application.bot])
    
    # Corrected concurrent startup
    worker_task = asyncio.create_task(worker_loop(application.bot))
    
    await application.initialize()
    scheduler.start()
    logger.info("Main bot and stats scheduler are running.")
    await application.run_polling()
    
    worker_task.cancel()

if __name__ == '__main__':
    print("Starting Advanced Bot...")
    asyncio.run(main())

