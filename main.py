import logging
import asyncio
import os
import requests
import re
import tempfile
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

# --- Reusable Core Task Logic ---
async def process_single_file(ptb_bot_instance: Bot, user_id: int, media_url: str, referer_url: str):
    """The core logic for downloading, uploading, and forwarding a single file."""
    full_path = ""
    try:
        user_config = database.get_user_config(user_id) or {}
        target_chat_id = user_config.get('target_chat_id', user_id)

        local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
        temp_dir = tempfile.gettempdir(); full_path = os.path.join(temp_dir, local_filename)
        headers = {'Referer': referer_url}
        with requests.get(media_url, headers=headers, stream=True) as r:
            r.raise_for_status();
            with open(full_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
        
        is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
        
        async with PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING) as userbot:
            sent_msg = await userbot.send_video("me", full_path) if is_video else await userbot.send_photo("me", full_path)
            await ptb_bot_instance.forward_message(chat_id=target_chat_id, from_chat_id=sent_msg.chat.id, message_id=sent_msg.id)
            await sent_msg.delete()
        
        database.update_stats('videos_sent' if is_video else 'images_sent')
        
    except Exception as e:
        logger.error(f"Failed to process {media_url}: {e}")
        await log_to_topic(ptb_bot_instance, 'logs', f"🔥 **Processing Error** for user `{user_id}`:\n`{e}`")
    finally:
        if os.path.exists(full_path): os.remove(full_path)

# --- Background Worker (for Frenzy Mode) ---
async def frenzy_worker_loop(ptb_bot_instance: Bot):
    logger.info("Frenzy mode background worker started.")
    while True:
        job = database.get_job()
        if job:
            await process_single_file(ptb_bot_instance, job['user_id'], job['media_url'], job['referer_url'])
            database.complete_job(job['_id'])
        else:
            await asyncio.sleep(5)

# --- PTB Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    database.set_frenzy_mode(user.id, False) # Default to Normal Mode
    await update.message.reply_text("Bot ready. Send a link to process it. Use `/frenzy` to queue multiple links.")

async def frenzy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, True)
    await update.message.reply_text("✅ **Frenzy Mode Activated**. I will now queue all links from your messages.")

async def cf_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.set_frenzy_mode(update.effective_user.id, False)
    await update.message.reply_text("⛔ **Normal Mode Activated**. I will process one link at a time, and wait until it's finished.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = database.get_user_config(user_id)
    is_in_frenzy = user_config.get("frenzy_mode_active", False)
    
    text = update.message.text
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    if not urls: return

    if is_in_frenzy:
        # FRENZY MODE: Scrape and add to DB queue
        await update.message.reply_text(f"Frenzy Mode: Found {len(urls)} links. Scraping and adding to background queue...")
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
                        database.add_job(user_id, media_url, url); scraped_count += 1
            except Exception as e:
                logger.error(f"Failed to scrape {url}: {e}")
        await update.message.reply_text(f"✅ Queued {scraped_count} media files. The worker will process them in the background.")
    else:
        # NORMAL MODE: Block and process the first link now
        url = urls[0]
        await update.message.reply_text(f"Normal Mode: Processing link: `{url}`. Please wait...")
        try:
            headers = {'User-Agent': 'Mozilla/5.0 ...'}
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            thumbnail_links = soup.select("a.spotlight[data-media]")
            
            media_to_process = []
            for link in thumbnail_links:
                media_url = link.get('data-src-mp4') if link.get('data-media') == 'video' else link.get('href')
                if media_url:
                    media_to_process.append(media_url)
            
            if not media_to_process:
                await update.message.reply_text("Could not find any media on that page.")
                return

            await update.message.reply_text(f"Found {len(media_to_process)} files. Processing now...")
            tasks = [process_single_file(context.bot, user_id, media_url, url) for media_url in media_to_process]
            await asyncio.gather(*tasks)
            await update.message.reply_text("✅ Task complete.")

        except Exception as e:
            logger.error(f"Failed to process link {url} in normal mode: {e}")
            await update.message.reply_text(f"⚠️ Failed to process link: {url}")
            
# --- Admin, Stats, and Other Handlers (Copied from previous version) ---
# ... (insert the full code for target_command, cleartarget_command, status_command, tasks_command, clear_queue_command, set_supergroup_command, check_and_send_stats, and log_to_topic here) ...
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
    global last_sent_stats
    current_stats = database.get_stats()
    if current_stats and current_stats != last_sent_stats:
        settings = database.get_settings()
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
            try:
                await bot.send_message(chat_id=settings['supergroup_id'], message_thread_id=topic_ids[topic_key], text=text, parse_mode='Markdown')
            except Exception as e: logger.error(f"Failed to log to topic '{topic_key}': {e}")


# --- Keep-Alive Server and Main Runner ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text/plain'); self.end_headers(); self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000));
    httpd = HTTPServer(('', port), HealthCheckHandler)
    logger.info(f"Keep-alive server on port {port}"); httpd.serve_forever()

async def main():
    if not all([TELEGRAM_BOT_TOKEN, API_ID, API_HASH, USERBOT_SESSION_STRING, ADMIN_IDS]):
        raise ValueError("Core environment variables are missing!")

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
    application.add_handler(CommandHandler("ce", clear_queue_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    scheduler = AsyncIOScheduler(); scheduler.add_job(check_and_send_stats, 'interval', minutes=15, args=[application.bot]);
    
    try:
        await application.initialize()
        scheduler.start()
        worker_task = asyncio.create_task(frenzy_worker_loop(application.bot))
        
        logger.info("Main bot, scheduler, and worker loop are starting concurrently.")
        await application.run_polling(drop_pending_updates=True)
        
        worker_task.cancel()
    finally:
        await application.shutdown()
        scheduler.shutdown()

if __name__ == '__main__':
    print("Starting Dual-Mode Bot...")
    asyncio.run(main())
