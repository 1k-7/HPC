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

# Pyrogram imports
from pyrogram import Client as PyrogramClient
from pyrogram.errors import FloodWait

# Local imports
import database

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH"))
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")
ADMIN_IDS = [int(admin_id) for admin_id in os.environ.get("ADMIN_IDS", "").split()]

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

last_sent_stats = {}
admin_filter = filters.User(user_id=ADMIN_IDS)

# --- Background Worker Logic ---
async def worker_loop():
    """Continuously polls the database for new jobs and processes them."""
    logger.info("Starting background worker loop...")
    worker_bots_cache = {} # Cache for worker bot instances
    userbot = PyrogramClient("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING)
    await userbot.start()

    while True:
        job = database.get_job()
        if job:
            logger.info(f"Worker picked up job: {job['_id']}")
            user_id = job['user_id']
            media_url = job['media_url']
            referer_url = job['referer_url']

            local_filename = f"temp_{os.path.basename(requests.utils.urlparse(media_url).path)}"
            temp_dir = tempfile.gettempdir()
            full_path = os.path.join(temp_dir, local_filename)

            try:
                # 1. Download
                headers = {'Referer': referer_url}
                with requests.get(media_url, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    with open(full_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
                
                is_video = any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm'])
                
                # 2. Upload and Send
                user_config = database.get_user_config(user_id)
                target_chat_id = user_config.get('target_chat_id', user_id)
                worker_tokens = user_config.get('worker_tokens', [])
                
                # Upload to Saved Messages
                sent_msg = await userbot.send_video("me", full_path) if is_video else await userbot.send_photo("me", full_path)
                
                # Forward/Send to Target
                if target_chat_id != user_id and worker_tokens:
                    # Use a worker bot
                    if user_id not in worker_bots_cache:
                        worker_bots_cache[user_id] = {'bots': [Bot(token) for token in worker_tokens], 'cycle': None}
                        worker_bots_cache[user_id]['cycle'] = itertools.cycle(worker_bots_cache[user_id]['bots'])
                    worker_bot = next(worker_bots_cache[user_id]['cycle'])
                    await worker_bot.forward_message(chat_id=target_chat_id, from_chat_id=sent_msg.chat.id, message_id=sent_msg.id)
                else:
                    # Userbot sends directly to the user who requested
                    await userbot.forward_messages(chat_id=user_id, from_chat_id=sent_msg.chat.id, message_ids=sent_msg.id)
                
                await sent_msg.delete()
                
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
            await asyncio.sleep(5) # Wait if queue is empty

# --- PTB Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (code from previous version)

async def frenzy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (code from previous version)

# ... All other handlers from previous version: cf_command, target_command, token_command, tasks_command, clear_queue_command, handle_links, check_and_send_stats ...

async def set_supergroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Admin Only) Sets the current supergroup as the logging/ops hub."""
    chat_id = update.message.chat.id
    if update.message.chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command can only be used in a supergroup.")
        return
    
    await update.message.reply_text("Setting up this supergroup... Creating topics...")
    try:
        topic_names = ["Jobs", "Logs", "User Activity", "Stats"]
        topic_ids = {}
        for name in topic_names:
            topic = await context.bot.create_forum_topic(chat_id=chat_id, name=name)
            topic_ids[name.lower().replace(" ", "_")] = topic.message_thread_id
        
        database.set_supergroup(chat_id, topic_ids)
        await update.message.reply_text("✅ Supergroup configured and topics created successfully!")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}. Make sure the bot is an admin with 'Manage Topics' permission.")

# --- Keep-Alive Server and Main Runner ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header('Content-type','text/plain'); self.end_headers(); self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000));
    httpd = HTTPServer(('', port), HealthCheckHandler)
    logger.info(f"Keep-alive server on port {port}"); httpd.serve_forever()

async def main():
    # Start web server in a background thread
    web_thread = Thread(target=run_web_server, daemon=True)
    web_thread.start()

    # Setup PTB application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add all handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("setsupergroup", set_supergroup_command, filters=admin_filter))
    # ... add all other command handlers here ...
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_links))
    
    # Setup scheduler
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_and_send_stats, 'interval', minutes=15, args=[application])
    
    # Run PTB, Pyrogram worker, and Scheduler concurrently
    async with application:
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        
        scheduler.start()
        logger.info("Main bot and stats scheduler are running.")
        
        # Start the background worker as a concurrent task
        await worker_loop()
        
if __name__ == '__main__':
    print("Starting Advanced Bot...")
    asyncio.run(main())
