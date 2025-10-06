import logging
import asyncio
import os
import requests
import re
import tempfile
import json
import itertools
from bs4 import BeautifulSoup
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Telegram (PTB) imports
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.request import HTTPXRequest
from telegram.error import TelegramError

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

# --- Global Cache ---
last_sent_stats = {}
admin_filter = filters.User(user_id=ADMIN_IDS)

# --- Background Worker Logic ---
async def worker_loop():
    """Continuously polls the database for new jobs and processes them."""
    logger.info("Background worker loop started.")
    worker_bots_cache = {}
    userbot = PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING)
    await userbot.start()

    while True:
        job = database.get_job()
        if job:
            logger.info(f"Worker picked up job: {job['_id']}")
            user_id = job['user_id']
            media_url = job['media_url']
            referer_url = job['referer_url']
            full_path = ""

            try:
                # 1. Download
                local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
                temp_dir = tempfile.gettempdir()
                full_path = os.path.join(temp_dir, local_filename)
                headers = {'Referer': referer_url}
                with requests.get(media_url, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    with open(full_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
                
                is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
                
                # 2. Get user config for this job
                user_config = database.get_user_config(user_id)
                target_chat_id = user_config.get('target_chat_id', user_id)
                worker_tokens = user_config.get('worker_tokens', [])
                
                # 3. Upload and Send
                if target_chat_id != user_id and worker_tokens:
                    # Use a worker bot for custom targets
                    if user_id not in worker_bots_cache or worker_bots_cache[user_id]['tokens'] != worker_tokens:
                        bots = [Bot(token) for token in worker_tokens]
                        worker_bots_cache[user_id] = {'tokens': worker_tokens, 'cycle': itertools.cycle(bots)}
                    worker_bot = next(worker_bots_cache[user_id]['cycle'])
                    
                    sent_msg = await userbot.send_video("me", full_path) if is_video else await userbot.send_photo("me", full_path)
                    await worker_bot.forward_message(chat_id=target_chat_id, from_chat_id=sent_msg.chat.id, message_id=sent_msg.id)
                    await sent_msg.delete()
                else:
                    # Default: Userbot sends directly to the user who requested
                    if is_video:
                        await userbot.send_video(user_id, full_path)
                    else:
                        await userbot.send_photo(user_id, full_path)

                database.update_stats('videos_sent' if is_video else 'images_sent')
                database.complete_job(job['_id'])
                logger.info(f"Successfully processed job {job['_id']}")
            
            except Exception as e:
                logger.error(f"Failed to process job {job['_id']}: {e}")
                database.complete_job(job['_id']) # Remove failed job
            finally:
                if os.path.exists(full_path):
                    os.remove(full_path)
        else:
            await asyncio.sleep(5)

# --- PTB Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    database.set_frenzy_mode(user.id, False) # Default to OFF
    database.update_stats('users_started')
    await update.message.reply_text(
        "Bot ready.\n\n"
        "Send a message with a link to process it once.\n"
        "Use `/frenzy` to continuously process all links you send.\n"
        "Use `/cf` to stop frenzy mode."
    )

async def frenzy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    database.set_frenzy_mode(user_id, True)
    await update.message.reply_text("✅ Frenzy mode **activated**. I will now queue links from all your messages.")

async def cf_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    database.set_frenzy_mode(user_id, False)
    await update.message.reply_text("⛔ Frenzy mode **deactivated**. Queued jobs will continue.")

async def target_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        target_id = int(context.args[0]); database.set_user_target(user_id, target_id)
        await update.message.reply_text(f"✅ Target chat ID set to `{target_id}`.")
    except (IndexError, ValueError): await update.message.reply_text("Usage: `/target <chat_id>`")

async def token_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        new_token = context.args[0]; database.add_worker_token(user_id, new_token)
        await update.message.reply_text("✅ Worker bot token added.")
    except IndexError: await update.message.reply_text("Usage: `/token <bot_token>`")

async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    jobs = database.get_pending_jobs(user_id)
    await update.message.reply_text(f"You have {len(jobs)} pending jobs in the queue.")

async def clear_queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    deleted_count = database.clear_pending_jobs(user_id)
    await update.message.reply_text(f"✅ Cleared {deleted_count} pending jobs from your queue.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = database.get_user_config(user_id)
    is_in_frenzy = user_config.get("frenzy_mode_active", False)
    text = update.message.text
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    if not urls: return
    
    # Process if in frenzy mode OR if not in frenzy mode (for one-off link processing)
    if is_in_frenzy or not is_in_frenzy:
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
    if chat.type != 'supergroup' or not chat.is_forum:
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
    settings = database.get_settings()
    if not settings: return
    current_stats = database.get_stats()
    if current_stats and current_stats != last_sent_stats:
        stats_message = "📊 Bot Statistics Update:\n"
        for key, value in current_stats.items():
            if key != '_id': stats_message += f"- **{key.replace('_', ' ').title()}**: {value}\n"
        await bot.send_message(chat_id=settings['supergroup_id'], message_thread_id=settings['topic_ids']['stats'], text=stats_message, parse_mode='Markdown')
        last_sent_stats = current_stats
        logger.info("Sent stats update.")

# --- Keep-Alive Server and Main Runner ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text/plain'); self.end_headers(); self.wfile.write(b"ok")

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
    
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("setsupergroup", set_supergroup_command, filters=admin_filter))
    application.add_handler(CommandHandler("frenzy", frenzy_command))
    application.add_handler(CommandHandler("cf", cf_command))
    application.add_handler(CommandHandler("target", target_command, filters=admin_filter))
    application.add_handler(CommandHandler("token", token_command, filters=admin_filter))
    application.add_handler(CommandHandler("tasks", tasks_command))
    application.add_handler(CommandHandler("cc", clear_queue_command))
    application.add_handler(CommandHandler("ce", clear_queue_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_and_send_stats, 'interval', minutes=15, args=[application.bot])
    
    # Run everything concurrently
    async with application:
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        scheduler.start()
        logger.info("Main bot and scheduler are running.")
        
        # Start the background worker as a concurrent task
        await worker_loop()
        
if __name__ == '__main__':
    print("Starting Advanced Frenzy Bot...")
    asyncio.run(main())
