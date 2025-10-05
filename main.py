import logging
import asyncio
import os
import requests
import tempfile
import json
import re
import time
from urllib.parse import urlparse
from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

# Telegram imports
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.request import HTTPXRequest

# Pyrogram import for the userbot
from pyrogram import Client

# --- Main Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")

# --- Proxy Manager Configuration ---
PROXY_LATENCY_THRESHOLD_MS = 100  # Will accept any proxy below this latency
PROXY_TEST_WORKERS = 50           # Number of proxies to test at once
PROXY_BATCH_SIZE = 200            # How many proxies to pull from the queue for a testing round

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Proxy Scraping and Management ---

def scrape_proxies():
    """Scrapes proxies from multiple sources and returns a list."""
    proxies = set()
    logger.info("Scraping proxies from spys.me...")
    try:
        c = requests.get("https://spys.me/proxy.txt", timeout=10)
        regex = r"[0-9]+(?:\.[0-9]+){3}:[0-9]+"
        matches = re.finditer(regex, c.text, re.MULTILINE)
        for match in matches: proxies.add(match.group())
    except Exception as e:
        logger.error(f"Failed to scrape from spys.me: {e}")

    logger.info("Scraping proxies from free-proxy-list.net...")
    try:
        d = requests.get("https://free-proxy-list.net/", timeout=10)
        soup = BeautifulSoup(d.content, 'html.parser')
        td_elements = soup.select('.fpl-list .table tbody tr td')
        for j in range(0, len(td_elements), 8):
            ip = td_elements[j].text.strip()
            port = td_elements[j + 1].text.strip()
            proxies.add(f"{ip}:{port}")
    except Exception as e:
        logger.error(f"Failed to scrape from free-proxy-list.net: {e}")
        
    logger.info(f"Scraped a total of {len(proxies)} unique proxies.")
    return list(proxies)

class ProxyManager:
    def __init__(self):
        self.proxies = Queue()
        self.lock = Lock()
        self.current_proxy = None
        self.reload_proxies()

    def reload_proxies(self):
        with self.lock:
            scraped_proxies = scrape_proxies()
            while not self.proxies.empty():
                self.proxies.get()
            for p in scraped_proxies:
                self.proxies.put(p)
            self.current_proxy = None

    def test_proxy(self, proxy):
        try:
            start_time = time.time()
            requests.get("http://google.com/generate_204", proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"}, timeout=2)
            latency = (time.time() - start_time) * 1000
            return proxy, latency
        except Exception:
            return proxy, float('inf')

    def find_best_proxy(self):
        """Continuously searches for a working proxy and never gives up."""
        with self.lock:
            if self.current_proxy:
                return self.current_proxy

        logger.info("No active proxy. Starting continuous search for a new fast proxy...")
        
        while True: # Never stop searching
            if self.proxies.empty():
                logger.warning("Proxy queue is empty. Re-scraping for a fresh list...")
                self.reload_proxies()
                if self.proxies.empty():
                    logger.error("Re-scraping failed to find any proxies. Retrying in 30 seconds...")
                    time.sleep(30)
                    continue

            best_proxy_in_batch = None
            best_latency_in_batch = float('inf')
            
            with ThreadPoolExecutor(max_workers=PROXY_TEST_WORKERS) as executor:
                proxies_to_test = []
                for _ in range(min(PROXY_BATCH_SIZE, self.proxies.qsize())):
                    proxies_to_test.append(self.proxies.get())
                
                logger.info(f"Testing a batch of {len(proxies_to_test)} proxies...")
                future_to_proxy = {executor.submit(self.test_proxy, p): p for p in proxies_to_test}

                for future in future_to_proxy:
                    proxy, latency = future.result()
                    if latency < PROXY_LATENCY_THRESHOLD_MS and latency < best_latency_in_batch:
                        best_latency_in_batch = latency
                        best_proxy_in_batch = proxy
            
            if best_proxy_in_batch:
                logger.info(f"SUCCESS: Found suitable proxy: {best_proxy_in_batch} ({best_latency_in_batch:.2f}ms)")
                with self.lock:
                    self.current_proxy = best_proxy_in_batch
                return best_proxy_in_batch
            else:
                logger.warning("Batch tested. No suitable proxies found. Continuing search...")

    def report_dead_proxy(self):
        with self.lock:
            dead_proxy = self.current_proxy
            self.current_proxy = None
            logger.warning(f"Proxy {dead_proxy} reported as dead. A new one will be found on the next request.")

# Initialize the proxy manager globally
proxy_manager = ProxyManager()

# --- HELPER: UPLOAD VIA USERBOT AND FORWARD ---
async def upload_with_userbot_and_forward(media_url, original_referer_url, chat_id, context):
    local_filename = media_url.split('/')[-1].split('?')[0]
    if len(local_filename) > 60 or not local_filename:
        file_ext = os.path.splitext(media_url.split('/')[-1].split('?')[0])[1]
        local_filename = f"temp_media{file_ext}" if file_ext else "temp_media"
    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, local_filename)
    
    max_retries = 5
    for attempt in range(max_retries):
        proxy = proxy_manager.find_best_proxy()
        try:
            logger.info(f"Attempt {attempt + 1}: Downloading {media_url} via proxy {proxy}...")
            proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
            headers = {'Referer': original_referer_url}
            with requests.get(media_url, headers=headers, stream=True, proxies=proxies, timeout=30) as r:
                r.raise_for_status()
                with open(full_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed with proxy {proxy}: {e}. Reporting as dead.")
            proxy_manager.report_dead_proxy()
            if attempt == max_retries - 1:
                await context.bot.send_message(chat_id, "⚠️ Failed to download a file after multiple proxy attempts.")
                return
    
    proxy_for_pyrogram = proxy_manager.current_proxy
    pyrogram_proxy = None
    if proxy_for_pyrogram:
        parsed_proxy = urlparse(f"http://{proxy_for_pyrogram}")
        pyrogram_proxy = {"scheme": "http", "hostname": parsed_proxy.hostname, "port": parsed_proxy.port}

    app = Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING, proxy=pyrogram_proxy)
    try:
        await app.start()
        if any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm']):
            sent_message = await app.send_video("me", full_path)
        else:
            sent_message = await app.send_photo("me", full_path)
        await context.bot.forward_message(chat_id=chat_id, from_chat_id=sent_message.chat.id, message_id=sent_message.id)
        await app.delete_messages("me", sent_message.id)
    except Exception as e:
         logger.error(f"Failed during userbot upload/forward: {e}")
         await context.bot.send_message(chat_id, "⚠️ Failed to upload file via userbot.")
    finally:
        if os.path.exists(full_path):
            os.remove(full_path)
        if app.is_initialized:
            await app.stop()

# --- Main Bot Logic ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Self-Healing Proxy Bot. Send a link.")

async def process_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    initial_url = update.message.text.strip()
    chat_id = update.message.chat_id
    await update.message.reply_text("🚀 Fetching page...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        # The initial page scrape can also use a proxy if needed, though often not necessary
        proxy = proxy_manager.find_best_proxy()
        proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"} if proxy else None
        
        response = requests.get(initial_url, headers=headers, proxies=proxies)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        thumbnail_links = soup.select("a.spotlight[data-media]")
        if not thumbnail_links:
            await update.message.reply_text("❌ Could not find any media thumbnails.")
            return
            
        media_urls = []
        for link in thumbnail_links:
            media_type = link.get('data-media')
            if media_type == 'video': url = link.get('data-src-mp4')
            elif media_type == 'image': url = link.get('href')
            else: url = None
            if url: media_urls.append(url)

        if not media_urls:
            await update.message.reply_text("Found thumbnails, but no media links.")
            return

        await update.message.reply_text(f"👍 Found {len(media_urls)} files! Processing with dynamic proxies...")
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
    
    print("Self-Healing Proxy Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
