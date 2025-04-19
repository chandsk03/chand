import os
import logging
import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from datetime import datetime
import asyncio
import re
import requests
import shutil
from collections import defaultdict
import time
from dotenv import load_dotenv
import signal
import sys
import libtorrent as lt
import aria2p
import subprocess

# Load environment variables
load_dotenv()

# Configuration
API_ID = int(os.getenv("API_ID", "25781839"))
API_HASH = os.getenv("API_HASH", "20a3f2f168739259a180dcdd642e196c")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7614305417:AAFjptKmdgPUN0aeiRSRqNUm2l7KhHj0aFc")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "7584086775").split(",")]
ARIA2_SECRET = os.getenv("ARIA2_SECRET", "mysecret")
DOWNLOAD_DIR = "downloads"
MAX_CONCURRENT = 3
RATE_LIMIT_SECONDS = 60
RATE_LIMIT_REQUESTS = 10
STALL_TIMEOUT = 300
ARIA2_RPC_PORT = 6800
YTS_API_URL = "https://yts.mx/api/v2/list_movies.json"
DEFAULT_ENGINE = "aria2c"

# Hacker aesthetic
HACKER_PREFIX = "💾 [CYBERLINK v4.2] "
HACKER_FOOTER = "🔒 SECURE TRANSMISSION ENDED"
ASCII_ART = """
   ╔════════════════════════════════════╗
   ║  CYBERLINK TORRENT MATRIX v4.2     ║
   ║  INITIALIZING HACKER PROTOCOL...   ║
   ╚════════════════════════════════════╝
"""

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize libtorrent
try:
    settings = {
        'listen_interfaces': '0.0.0.0:6881',
        'allow_multiple_connections_per_ip': True,
        'enable_dht': True,
        'enable_lsd': True,
        'enable_upnp': True,
        'enable_natpmp': True
    }
    ses = lt.session(settings)
except Exception as e:
    logger.error(f"Failed to initialize libtorrent: {e}")
    ses = None

# Initialize aria2c
try:
    aria2_process = subprocess.Popen([
        "aria2c",
        "--enable-rpc",
        f"--rpc-listen-port={ARIA2_RPC_PORT}",
        f"--rpc-secret={ARIA2_SECRET}",
        f"--dir={DOWNLOAD_DIR}",
        "--daemon=false",
        "--quiet"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)
    aria2 = aria2p.API(
        aria2p.Client(
            host="http://localhost",
            port=ARIA2_RPC_PORT,
            secret=ARIA2_SECRET
        )
    )
except Exception as e:
    logger.error(f"Failed to initialize aria2c: {e}")
    aria2 = None
    aria2_process = None

# State
downloads = []
download_queue = []
user_downloads = defaultdict(list)
user_requests = defaultdict(list)
download_start_times = {}
torrent_names = {}

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

async def send_file(update: Update, file_path: str, context: ContextTypes.DEFAULT_TYPE):
    """Send file to user if within Telegram's 2GB limit."""
    try:
        file_size = os.path.getsize(file_path)
        if file_size > 2 * 1024 * 1024 * 1024:
            await update.message.reply_text(
                f"{HACKER_PREFIX}ERROR: File exceeds 2GB limit.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
        with open(file_path, 'rb') as f:
            await update.message.reply_document(
                document=f,
                caption=f"📽️ {os.path.basename(file_path)}",
                parse_mode="Markdown"
            )
        logger.info(f"Sent file {file_path} to user {update.effective_user.id}")
    except Exception as e:
        logger.error(f"Error sending file {file_path}: {e}")
        await update.message.reply_text(
            f"{HACKER_PREFIX}ERROR: Failed to send file: {str(e)}\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )

async def rate_limit_check(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is within rate limits."""
    now = time.time()
    user_requests[user_id] = [t for t in user_requests[user_id] if now - t < RATE_LIMIT_SECONDS]
    if len(user_requests[user_id]) >= RATE_LIMIT_REQUESTS:
        logger.warning(f"Rate limit exceeded for user {user_id}")
        await notify_admins(context, f"User {user_id} exceeded rate limit.")
        await context.bot.send_message(
            chat_id=user_id,
            text=f"{HACKER_PREFIX}ALERT: Rate limit exceeded. Retry later.\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )
        return False
    user_requests[user_id].append(now)
    return True

async def notify_admins(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Notify admins of suspicious activity."""
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"🚨 [ADMIN ALERT] {message}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

async def log_ip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log user IP for security monitoring."""
    user_id = update.effective_user.id
    logger.info(f"Action by user {user_id} from IP: Unknown (Telegram API limitation)")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command with inline buttons."""
    await log_ip(update, context)
    if not await rate_limit_check(update.effective_user.id, context):
        return
    keyboard = [
        [InlineKeyboardButton("🔍 Search Movies", callback_data="search_movie")],
        [InlineKeyboardButton("📥 Send Magnet/Link", callback_data="send_magnet")],
        [InlineKeyboardButton("📂 Upload Torrent File", callback_data="upload_torrent")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"```\n{ASCII_ART}\n```\n"
        f"{HACKER_PREFIX}WELCOME TO CYBERLINK TORRENT MATRIX\n"
        "🌐 Initiate download protocols with buttons below.\n"
        f"{HACKER_FOOTER}",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    await log_ip(update, context)
    if not await rate_limit_check(update.effective_user.id, context):
        return
    await update.message.reply_text(
        f"```\n{ASCII_ART}\n```\n"
        f"{HACKER_PREFIX}CYBERLINK PROTOCOL\n"
        "📟 Use /start to access the main interface.\n"
        "📥 Send magnet links or .torrent files directly.\n"
        "🔍 Click buttons to search movies or manage downloads.\n"
        f"{HACKER_FOOTER}",
        parse_mode="Markdown"
    )

async def search_torrents(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str = None):
    """Handle movie search with inline buttons."""
    await log_ip(update, context)
    if not await rate_limit_check(update.effective_user.id, context):
        return
    if not query:
        await update.callback_query.message.reply_text(
            f"{HACKER_PREFIX}Enter a movie title to scan (e.g., 'Matrix').\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )
        context.user_data["awaiting_search"] = True
        return
    try:
        response = requests.get(YTS_API_URL, params={"query_term": query, "limit": 5})
        response.raise_for_status()
        data = response.json()
        movies = data.get("data", {}).get("movies", [])
        if not movies:
            await update.callback_query.message.reply_text(
                f"{HACKER_PREFIX}SCAN COMPLETE: No torrents found for '{query}'.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
        keyboard = []
        for movie in movies:
            title = movie["title"]
            year = movie["year"]
            for torrent in movie["torrents"]:
                quality = torrent["quality"]
                magnet = torrent["url"].replace("torrent://", "magnet:?")
                keyboard.append([
                    InlineKeyboardButton(
                        f"{title} ({year}, {quality})",
                        callback_data=f"magnet_{magnet}"
                    )
                ])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.message.reply_text(
            f"{HACKER_PREFIX}TORRENT SCAN RESULTS FOR '{query}'\nSelect a stream:\n{HACKER_FOOTER}",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error searching torrents for '{query}': {e}")
        await update.callback_query.message.reply_text(
            f"{HACKER_PREFIX}ERROR: Torrent scan failed: {str(e)}\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )

async def handle_magnet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle magnet links."""
    await log_ip(update, context)
    user_id = update.effective_user.id
    if not await rate_limit_check(user_id, context):
        return
    magnet = update.message.text
    if not re.match(r'^magnet:\?', magnet):
        await update.message.reply_text(
            f"{HACKER_PREFIX}ERROR: Invalid magnet protocol.\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )
        return
    if len(downloads) >= MAX_CONCURRENT:
        download_queue.append((update, {"magnet": magnet}))
        await update.message.reply_text(
            f"{HACKER_PREFIX}Stream queued. Awaiting matrix slot.\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )
        return
    await start_download(update, context, magnet=magnet)

async def handle_torrent_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle .torrent file uploads."""
    await log_ip(update, context)
    user_id = update.effective_user.id
    if not await rate_limit_check(user_id, context):
        return
    file = await update.message.document.get_file()
    file_path = os.path.join(DOWNLOAD_DIR, update.message.document.file_name)
    try:
        await file.download_to_drive(file_path)
        if len(downloads) >= MAX_CONCURRENT:
            download_queue.append((update, {"torrent_file": file_path}))
            await update.message.reply_text(
                f"{HACKER_PREFIX}Stream queued. Awaiting matrix slot.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
        await start_download(update, context, torrent_file=file_path)
    except Exception as e:
        logger.error(f"Error processing torrent file for user {user_id}: {e}")
        await update.message.reply_text(
            f"{HACKER_PREFIX}ERROR: Failed to process torrent file: {str(e)}\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

async def start_download(update: Update, context: ContextTypes.DEFAULT_TYPE, magnet=None, torrent_file=None):
    """Start a torrent download."""
    user_id = update.effective_user.id
    engine = DEFAULT_ENGINE
    try:
        if engine == "libtorrent" and ses:
            if magnet:
                params = lt.parse_magnet_uri(magnet)
                params.save_path = DOWNLOAD_DIR
                download = ses.add_torrent(params)
            else:  # torrent_file
                with open(torrent_file, 'rb') as f:
                    torrent_data = f.read()
                info = lt.torrent_info(lt.bdecode(torrent_data))
                params = {'ti': info, 'save_path': DOWNLOAD_DIR}
                download = ses.add_torrent(params)
            downloads.append(download)
            user_downloads[user_id].append(download)
            download_start_times[download] = time.time()
            name = download.name()
            torrent_names[download] = name
            keyboard = [
                [InlineKeyboardButton("📊 Progress", callback_data=f"progress_{id(download)}")],
                [InlineKeyboardButton("🛑 Cancel", callback_data=f"cancel_{id(download)}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                f"{HACKER_PREFIX}Stream initiated: {name} (libtorrent)\n{HACKER_FOOTER}",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            await file_selection_prompt(update, context, download, engine)
        elif engine == "aria2c" and aria2:
            if magnet:
                download = aria2.add_magnet(magnet, options={"dir": DOWNLOAD_DIR})
            else:  # torrent_file
                download = aria2.add_torrent(torrent_file, options={"dir": DOWNLOAD_DIR})
            downloads.append(download)
            user_downloads[user_id].append(download)
            download_start_times[download] = time.time()
            name = download.name
            torrent_names[download] = name
            keyboard = [
                [InlineKeyboardButton("📊 Progress", callback_data=f"progress_{download.gid}")],
                [InlineKeyboardButton("🛑 Cancel", callback_data=f"cancel_{download.gid}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                f"{HACKER_PREFIX}Stream initiated: {name} (aria2c)\n{HACKER_FOOTER}",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            await file_selection_prompt(update, context, download, engine)
        else:
            await update.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Download engine unavailable.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
    except Exception as e:
        logger.error(f"Error starting download for user {user_id}: {e}")
        await update.message.reply_text(
            f"{HACKER_PREFIX}ERROR: Failed to initiate stream: {str(e)}\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )

async def file_selection_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, download, engine):
    """Prompt user to select files from torrent."""
    try:
        if engine == "libtorrent":
            info = download.torrent_info()
            if not info:
                return
            keyboard = []
            for i, file in enumerate(info.files()):
                keyboard.append([InlineKeyboardButton(
                    file.path,
                    callback_data=f"file_{id(download)}_{i}"
                )])
            keyboard.append([InlineKeyboardButton(
                "Download All",
                callback_data=f"file_{id(download)}_all"
            )])
        else:  # aria2c
            files = download.files
            if not files:
                return
            keyboard = []
            for i, file in enumerate(files):
                keyboard.append([InlineKeyboardButton(
                    file.path,
                    callback_data=f"file_{download.gid}_{i}"
                )])
            keyboard.append([InlineKeyboardButton(
                "Download All",
                callback_data=f"file_{download.gid}_all"
            )])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"{HACKER_PREFIX}Select files to stream:\n{HACKER_FOOTER}",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error creating file selection prompt: {e}")
        await update.message.reply_text(
            f"{HACKER_PREFIX}ERROR: Failed to display file selection: {str(e)}\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data.split("_")
    if data[0] == "search":
        await search_torrents(update, context)
    elif data[0] == "send":
        await query.message.reply_text(
            f"{HACKER_PREFIX}Send a magnet link to initiate stream.\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )
        context.user_data["awaiting_magnet"] = True
    elif data[0] == "upload":
        await query.message.reply_text(
            f"{HACKER_PREFIX}Upload a .torrent file to initiate stream.\n{HACKER_FOOTER}",
            parse_mode="Markdown"
        )
        context.user_data["awaiting_torrent"] = True
    elif data[0] == "magnet":
        magnet = "_".join(data[1:])
        if len(downloads) >= MAX_CONCURRENT:
            download_queue.append((update, {"magnet": magnet}))
            await query.message.reply_text(
                f"{HACKER_PREFIX}Stream queued. Awaiting matrix slot.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
        await start_download(update, context, magnet=magnet)
    elif data[0] == "file":
        download_id = data[1]
        download = next((d for d in downloads if (isinstance(d, lt.torrent_handle) and id(d) == int(download_id)) or (hasattr(d, 'gid') and d.gid == download_id)), None)
        if not download or download_id not in [str(id(d)) if isinstance(d, lt.torrent_handle) else d.gid for d in user_downloads[user_id]]:
            await query.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Stream not found.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
        try:
            if isinstance(download, lt.torrent_handle):
                info = download.torrent_info()
                priorities = [0] * info.num_files()
                if data[2] == "all":
                    priorities = [1] * info.num_files()
                else:
                    file_index = int(data[2])
                    priorities[file_index] = 1
                download.prioritize_files(priorities)
            else:  # aria2c
                files = download.files
                selected = [False] * len(files)
                if data[2] == "all":
                    selected = [True] * len(files)
                else:
                    file_index = int(data[2])
                    selected[file_index] = True
                download.update(options={"select-file": ",".join(str(i + 1) for i, s in enumerate(selected) if s)})
            await query.message.reply_text(
                f"{HACKER_PREFIX}File selection updated. Stream active.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Error processing file selection for user {user_id}: {e}")
            await query.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Failed to update file selection: {str(e)}\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
    elif data[0] == "progress":
        download_id = data[1]
        download = next((d for d in downloads if (isinstance(d, lt.torrent_handle) and id(d) == int(download_id)) or (hasattr(d, 'gid') and d.gid == download_id)), None)
        if not download or download_id not in [str(id(d)) if isinstance(d, lt.torrent_handle) else d.gid for d in user_downloads[user_id]]:
            await query.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Stream not found.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
        try:
            if isinstance(download, lt.torrent_handle):
                status = download.status()
                progress = status.progress * 100
                speed = status.download_rate / 1024
                eta = (status.total_wanted - status.total_wanted_done) / (status.download_rate + 1)
                name = status.name
                status_text = (
                    f"📦 {name} (libtorrent)\n"
                    f"  Progress: {progress:.2f}%\n"
                    f"  Speed: {speed:.2f} KB/s\n"
                    f"  ETA: {int(eta)} seconds\n"
                )
            else:  # aria2c
                status = download.status()
                progress = status.completion * 100
                speed = status.download_rate / 1024
                eta = status.eta.total_seconds() if status.eta else float('inf')
                name = status.name
                status_text = (
                    f"📦 {name} (aria2c)\n"
                    f"  Progress: {progress:.2f}%\n"
                    f"  Speed: {speed:.2f} KB/s\n"
                    f"  ETA: {int(eta)} seconds\n"
                )
            await query.message.reply_text(
                f"{HACKER_PREFIX}STREAM STATUS\n{status_text}{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Error checking progress: {e}")
            await query.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Failed to check progress: {str(e)}\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
    elif data[0] == "cancel":
        download_id = data[1]
        download = next((d for d in downloads if (isinstance(d, lt.torrent_handle) and id(d) == int(download_id)) or (hasattr(d, 'gid') and d.gid == download_id)), None)
        if not download or download_id not in [str(id(d)) if isinstance(d, lt.torrent_handle) else d.gid for d in user_downloads[user_id]]:
            await query.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Stream not found.\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
            return
        try:
            if isinstance(download, lt.torrent_handle):
                ses.remove_torrent(download)
                engine = "libtorrent"
            else:  # aria2c
                download.remove()
                engine = "aria2c"
            downloads.remove(download)
            user_downloads[user_id].remove(download)
            download_start_times.pop(download, None)
            torrent_names.pop(download, None)
            await query.message.reply_text(
                f"{HACKER_PREFIX}Stream terminated: {torrent_names.get(download, 'Unknown')} ({engine})\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Error cancelling download: {e}")
            await query.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Failed to cancel stream: {str(e)}\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )

async def process_queue(context: ContextTypes.DEFAULT_TYPE):
    """Process queued downloads."""
    if download_queue and len(downloads) < MAX_CONCURRENT:
        update, item = download_queue.pop(0)
        try:
            if "magnet" in item:
                await start_download(update, context, magnet=item["magnet"])
            elif "torrent_file" in item:
                await start_download(update, context, torrent_file=item["torrent_file"])
        except Exception as e:
            logger.error(f"Error processing queued download: {e}")
            await update.message.reply_text(
                f"{HACKER_PREFIX}ERROR: Failed to start queued stream: {str(e)}\n{HACKER_FOOTER}",
                parse_mode="Markdown"
            )
        finally:
            if "torrent_file" in item and os.path.exists(item["torrent_file"]):
                os.remove(item["torrent_file"])

async def check_downloads(context: ContextTypes.DEFAULT_TYPE):
    """Check download progress, handle stalls, and send files."""
    global downloads
    completed = []
    stalled = []
    now = time.time()
    for download in downloads:
        try:
            if isinstance(download, lt.torrent_handle):
                if not download.is_valid():
                    continue
                status = download.status()
                if download in download_start_times and (now - download_start_times[download]) > STALL_TIMEOUT:
                    if status.progress == 0 or status.download_rate == 0:
                        stalled.append(download)
                        continue
                if status.is_seeding:
                    completed.append(download)
                    info = download.torrent_info()
                    for i, file in enumerate(info.files()):
                        if download.file_priority(i) == 0:
                            continue
                        file_path = os.path.join(DOWNLOAD_DIR, file.path)
                        for user_id, user_dls in user_downloads.items():
                            if download in user_dls:
                                await context.bot.send_message(
                                    chat_id=user_id,
                                    text=f"{HACKER_PREFIX}Stream completed: {torrent_names.get(download, 'Unknown')} (libtorrent)\n{HACKER_FOOTER}",
                                    parse_mode="Markdown"
                                )
                                await send_file(
                                    Update(0, message=telegram.Message(
                                        message_id=0,
                                        chat=telegram.Chat(id=user_id, type='private'),
                                        date=datetime.now(),
                                        document=None
                                    )),
                                    file_path,
                                    context
                                )
                    ses.remove_torrent(download)
                    for user_id, user_dls in user_downloads.items():
                        if download in user_dls:
                            user_downloads[user_id].remove(download)
                            download_start_times.pop(download, None)
                            torrent_names.pop(download, None)
                            shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)
            else:  # aria2c
                status = download.status()
                if download in download_start_times and (now - download_start_times[download]) > STALL_TIMEOUT:
                    if status.completion == 0 or status.download_rate == 0:
                        stalled.append(download)
                        continue
                if status.is_complete:
                    completed.append(download)
                    for file in download.files:
                        if not file.selected:
                            continue
                        file_path = file.path
                        for user_id, user_dls in user_downloads.items():
                            if download in user_dls:
                                await context.bot.send_message(
                                    chat_id=user_id,
                                    text=f"{HACKER_PREFIX}Stream completed: {torrent_names.get(download, 'Unknown')} (aria2c)\n{HACKER_FOOTER}",
                                    parse_mode="Markdown"
                                )
                                await send_file(
                                    Update(0, message=telegram.Message(
                                        message_id=0,
                                        chat=telegram.Chat(id=user_id, type='private'),
                                        date=datetime.now(),
                                        document=None
                                    )),
                                    file_path,
                                    context
                                )
                    download.remove()
                    for user_id, user_dls in user_downloads.items():
                        if download in user_dls:
                            user_downloads[user_id].remove(download)
                            download_start_times.pop(download, None)
                            torrent_names.pop(download, None)
                            shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)
        except Exception as e:
            logger.error(f"Error checking download: {e}")
    for download in stalled:
        try:
            if isinstance(download, lt.torrent_handle):
                ses.remove_torrent(download)
                engine = "libtorrent"
            else:  # aria2c
                download.remove()
                engine = "aria2c"
            for user_id, user_dls in user_downloads.items():
                if download in user_dls:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"{HACKER_PREFIX}Stream stalled and terminated: {torrent_names.get(download, 'Unknown')} ({engine})\n{HACKER_FOOTER}",
                        parse_mode="Markdown"
                    )
                    logger.info(f"Cancelled stalled download for user {user_id}")
                    user_downloads[user_id].remove(download)
                    download_start_times.pop(download, None)
                    torrent_names.pop(download, None)
        except Exception as e:
            logger.error(f"Error cancelling stalled download: {e}")
    downloads = [d for d in downloads if d not in completed and d not in stalled]

def signal_handler(sig, frame):
    """Handle graceful shutdown."""
    logger.info("Shutting down CYBERLINK matrix...")
    for download in downloads:
        try:
            if isinstance(download, lt.torrent_handle):
                ses.remove_torrent(download)
            else:  # aria2c
                download.remove()
        except Exception:
            pass
    if aria2_process:
        aria2_process.terminate()
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    sys.exit(0)

def main():
    """Start the bot."""
    try:
        application = Application.builder().token(BOT_TOKEN).build()
        
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(MessageHandler(filters.Regex(r'^magnet:\?'), handle_magnet))
        application.add_handler(MessageHandler(filters.Document.FileExtension("torrent"), handle_torrent_file))
        application.add_handler(CallbackQueryHandler(button_callback))
        
        application.job_queue.run_repeating(check_downloads, interval=10)
        application.job_queue.run_repeating(process_queue, interval=5)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("CYBERLINK matrix online.")
        application.run_polling()
    except Exception as e:
        logger.error(f"Failed to initialize matrix: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
