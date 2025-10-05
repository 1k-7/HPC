import logging
import os
import requests
import tempfile
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler

# --- Configuration ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
USERBOT_SESSION_STRING = os.environ.get("USERBOT_SESSION_STRING")

# --- Setup Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- The Rest of the Bot Code (No Changes) ---
app = Client("userbot_session", api_id=API_ID, api_hash=API_HASH, session_string=USERBOT_SESSION_STRING)

async def job_handler(client, message):
    if not message.from_user or not message.from_user.is_self: return
    text = message.text
    if not text or not text.startswith("process|"): return
    logger.info(f"Received job: {text}")
    parts = text.split('|', 3)
    if len(parts) != 4:
        logger.error("Invalid job format.")
        return
    await message.delete()
    _, original_chat_id, referer_url, media_url = parts
    local_filename = media_url.split('/')[-1].split('?')[0]
    if len(local_filename) > 60 or not local_filename:
        file_ext = os.path.splitext(media_url.split('/')[-1].split('?')[0])[1]
        local_filename = f"temp_media{file_ext}" if file_ext else "temp_media"
    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, local_filename)
    try:
        logger.info(f"Downloading {media_url} for user {original_chat_id}...")
        headers = {'Referer': referer_url}
        with requests.get(media_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(full_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info(f"Uploading {full_path} to user {original_chat_id}...")
        if any(ext in full_path.lower() for ext in ['.mp4', '.mov', '.webm']):
            await client.send_video(int(original_chat_id), full_path)
        else:
            await client.send_photo(int(original_chat_id), full_path)
        logger.info("Job complete.")
    except Exception as e:
        logger.error(f"Failed to process {media_url}: {e}")
        await client.send_message(int(original_chat_id), f"⚠️ Failed to download/send file: {media_url}")
    finally:
        if os.path.exists(full_path):
            os.remove(full_path)

# --- NEW: Keep-Alive Web Server for Render ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server_address = ('', port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info(f"Keep-alive server running on port {port}")
    httpd.serve_forever()
# --- END OF NEW CODE ---

async def run_pyrogram():
    async with app:
        app.add_handler(MessageHandler(job_handler, filters.chat("me") & filters.text))
        print("Userbot Worker is running and listening for jobs in 'Saved Messages'...")
        await asyncio.Event().wait()

if __name__ == "__main__":
    if not all([API_ID, API_HASH, USERBOT_SESSION_STRING]):
        raise ValueError("One or more required environment variables are missing!")
    
    # Start the keep-alive web server in a separate thread
    web_thread = Thread(target=run_web_server, daemon=True)
    web_thread.start()
    
    # Run the main Pyrogram loop
    app.run(run_pyrogram())
      
