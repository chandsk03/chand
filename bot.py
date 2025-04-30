import asyncio
import json
import os
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.custom import Button
from telethon.errors import (
    ChatAdminRequiredError,
    InviteHashExpiredError,
    FloodWaitError,
    ChannelPrivateError,
    UsernameNotOccupiedError,
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    ChatWriteForbiddenError,
    UserNotParticipantError,
    PeerIdInvalidError,
)
import psutil
from logging.handlers import RotatingFileHandler
import getpass

# Constants
MIN_MESSAGE_INTERVAL = 30
WARNING_INTERVAL = 300
STATS_SAVE_INTERVAL = 300
RECONNECT_INTERVAL = 300
MAX_RETRIES = 3
MAX_FLOOD_WAIT = 300
DIR_PERMISSIONS = 0o700
FILE_PERMISSIONS = 0o600
MAX_CONCURRENT_SENDS = 5
MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10MB
AUTH_TIMEOUT = 300  # 5 minutes

# Initialize directories
CONFIG_DIR = Path("config")
MEDIA_DIR = Path("media")
SESSION_DIR = Path("sessions")

def init_dirs():
    """Initialize required directories with secure permissions."""
    for directory in [CONFIG_DIR, MEDIA_DIR, SESSION_DIR]:
        directory.mkdir(exist_ok=True, parents=True)
        try:
            directory.chmod(DIR_PERMISSIONS)
            if directory.stat().st_mode & 0o777 != DIR_PERMISSIONS:
                logging.warning(f"Failed to set permissions for {directory}")
        except Exception as e:
            logging.error(f"Failed to set permissions for {directory}: {str(e)}")
init_dirs()

# Configure logging
log_level = logging.DEBUG if os.getenv("DEBUG", "0") == "1" else logging.INFO
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(CONFIG_DIR / "bot.log", maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler(sys.stdout)
    ]
)

# Configuration
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")
TARGETS_FILE = CONFIG_DIR / "targets.json"
STATS_FILE = CONFIG_DIR / "stats.json"
IMAGE_PATH = MEDIA_DIR / "bot_image.jpg"
LOCK_FILE = CONFIG_DIR / "bot.lock"
USER_SESSION = SESSION_DIR / "user.session"

# Global state
is_running: bool = False
message_counts: Dict[str, int] = {}
last_warning_time: Dict[str, float] = {}
target_usernames: Dict[str, str] = {}
tasks: List[asyncio.Task] = []
last_message_time: Dict[int, float] = {}
bot_client: Optional[TelegramClient] = None
user_client: Optional[TelegramClient] = None
state_lock = asyncio.Lock()
send_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)

# Validate configuration
def validate_config():
    """Validate critical configuration parameters."""
    errors = []
    if not API_ID or not isinstance(API_ID, int):
        errors.append("API_ID must be a valid integer")
    if not API_HASH or len(API_HASH) < 32:
        errors.append("API_HASH is invalid or not set")
    if not BOT_TOKEN or len(BOT_TOKEN.split(":")) != 2:
        errors.append("Invalid BOT_TOKEN format")
    if not PHONE_NUMBER:
        errors.append("PHONE_NUMBER not set in environment")
    if not ADMIN_ID or not isinstance(ADMIN_ID, int) or ADMIN_ID <= 0:
        errors.append("Invalid ADMIN_ID")
    if errors:
        logging.error("Configuration errors: " + "; ".join(errors))
        sys.exit(1)
    if not IMAGE_PATH.parent.exists():
        logging.error(f"Media directory {IMAGE_PATH.parent} does not exist")
        sys.exit(1)
validate_config()

# Lock file handling
def acquire_lock():
    """Acquire a lock to prevent multiple instances."""
    try:
        if LOCK_FILE.exists():
            with LOCK_FILE.open("r") as f:
                pid = f.read().strip()
            if pid and pid.isdigit():
                try:
                    p = psutil.Process(int(pid))
                    if p.is_running():
                        logging.error(f"Bot already running (PID: {pid})")
                        sys.exit(1)
                except psutil.NoSuchProcess:
                    logging.warning("Removing stale lock file")
                    LOCK_FILE.unlink()
        with LOCK_FILE.open("w") as f:
            f.write(str(os.getpid()))
        try:
            LOCK_FILE.chmod(FILE_PERMISSIONS)
        except Exception as e:
            logging.warning(f"Failed to set lock file permissions: {str(e)}")
    except Exception as e:
        logging.error(f"Failed to acquire lock: {str(e)}")
        sys.exit(1)

def release_lock():
    """Release the lock file."""
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception as e:
        logging.error(f"Failed to release lock: {str(e)}")

# File operations
def load_json(file_path: Path, default: Union[dict, list]) -> Union[dict, list]:
    """Load JSON file with error handling."""
    try:
        if file_path.exists():
            with file_path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error loading {file_path}: {str(e)}")
    return default

def save_json(file_path: Path, data: Union[dict, list]):
    """Save JSON file with secure permissions."""
    try:
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        try:
            file_path.chmod(FILE_PERMISSIONS)
        except Exception as e:
            logging.warning(f"Failed to set permissions for {file_path}: {str(e)}")
    except Exception as e:
        logging.error(f"Error saving {file_path}: {str(e)}")

def load_targets() -> List[int]:
    """Load target IDs and usernames."""
    data = load_json(TARGETS_FILE, {"targets": [], "usernames": {}})
    global target_usernames
    target_usernames = {str(k): v for k, v in data.get("usernames", {}).items()}
    return [int(tid) for tid in data.get("targets", []) if isinstance(tid, (int, str))]

def save_targets(targets: List[int]):
    """Save target IDs and usernames."""
    save_json(TARGETS_FILE, {"targets": targets, "usernames": target_usernames})

def load_config() -> Dict[str, Union[str, int]]:
    """Load bot configuration."""
    return load_json(CONFIG_DIR / "settings.json", {
        "message": "Hello! This is an automated message.",
        "interval": 120
    })

def save_config(config: Dict[str, Union[str, int]]):
    """Save bot configuration."""
    save_json(CONFIG_DIR / "settings.json", config)

def load_stats() -> Dict[str, int]:
    """Load message statistics."""
    return {str(k): v for k, v in load_json(STATS_FILE, {}).items()}

def save_stats(stats: Dict[str, int]):
    """Save message statistics."""
    save_json(STATS_FILE, stats)

targets = load_targets()
config = load_config()
message_counts = load_stats()

# Inline buttons
settings_buttons = [
    [Button.inline("Set Image", b"set_image"), Button.inline("Set Message", b"set_message")],
    [Button.inline("Set Interval", b"set_interval"), Button.inline("Add Target", b"add_target")],
    [Button.inline("Remove Target", b"remove_target"), Button.inline("View Settings", b"view_settings")],
    [Button.inline("Start Attack", b"start_bot"), Button.inline("Stop Attack", b"stop_bot")],
    [Button.inline("Status", b"status"), Button.inline("Validate Targets", b"validate")],
    [Button.inline("Restart Bot", b"restart"), Button.inline("Health Check", b"health")]
]

# Utility functions
async def get_entity_safe(client: TelegramClient, target_input: Union[str, int]) -> Optional[Union[User, Channel, Chat]]:
    """Safely retrieve a Telegram entity with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            entity = await client.get_entity(target_input)
            logging.debug(f"Retrieved entity for {target_input}")
            return entity
        except (ValueError, ChannelPrivateError, UsernameNotOccupiedError, PeerIdInvalidError) as e:
            logging.error(f"Cannot access {target_input}: {str(e)}")
            return None
        except FloodWaitError as e:
            wait_time = min(e.seconds, MAX_FLOOD_WAIT)
            logging.warning(f"FloodWaitError for {target_input}: Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logging.error(f"Error getting entity {target_input}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return None

async def join_chat_safe(client: TelegramClient, target_input: Union[str, int]) -> bool:
    """Safely join a Telegram chat with retries."""
    if isinstance(target_input, str):
        target_input = target_input.strip()
        if not target_input.startswith("@"):
            target_input = f"@{target_input.lstrip('https://t.me/')}"
    for attempt in range(MAX_RETRIES):
        try:
            await client(JoinChannelRequest(target_input))
            logging.info(f"Joined {target_input}")
            return True
        except FloodWaitError as e:
            wait_time = min(e.seconds, MAX_FLOOD_WAIT)
            logging.warning(f"FloodWaitError joining {target_input}: Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
        except (ChatAdminRequiredError, InviteHashExpiredError, ChannelPrivateError, UsernameNotOccupiedError) as e:
            logging.error(f"Cannot join {target_input}: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Error joining {target_input}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False

async def send_message_safe(client: TelegramClient, chat_id: int, message: str, buttons: Optional[list] = None):
    """Safely send a message with error handling."""
    async with send_semaphore:
        try:
            await client.send_message(chat_id, message, buttons=buttons)
            logging.debug(f"Sent message to {chat_id}")
        except FloodWaitError as e:
            wait_time = min(e.seconds, MAX_FLOOD_WAIT)
            logging.warning(f"FloodWaitError sending to {chat_id}: Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
        except (ChatWriteForbiddenError, ChannelPrivateError) as e:
            logging.error(f"Cannot send message to {chat_id}: {str(e)}")
        except Exception as e:
            logging.error(f"Failed to send message to {chat_id}: {str(e)}")

async def reconnect_client(client: TelegramClient, client_name: str = "client", max_attempts: int = 3) -> bool:
    """Reconnect a Telegram client with retry limits."""
    for attempt in range(max_attempts):
        try:
            if client.is_connected():
                await client.disconnect()
            await client.connect()
            if not await client.is_user_authorized():
                logging.error(f"{client_name} not authorized after reconnect")
                return False
            logging.info(f"{client_name} reconnected successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to reconnect {client_name}: {str(e)}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(2 ** attempt)
    return False

async def report_error(message: str):
    """Report critical errors to the admin."""
    if bot_client and bot_client.is_connected():
        await send_message_safe(bot_client, ADMIN_ID, f"Critical Error: {message}")

# User authentication
async def authenticate_user() -> bool:
    """Authenticate the user client."""
    global user_client
    user_client = TelegramClient(str(USER_SESSION), API_ID, API_HASH)
    
    try:
        await user_client.connect()
        if not await user_client.is_user_authorized():
            logging.info("Initiating user authentication")
            code = os.getenv("TELEGRAM_CODE")
            password = os.getenv("TELEGRAM_2FA_PASSWORD")
            
            if not code:
                print(f"Enter the login code for {PHONE_NUMBER}: ", end="")
                try:
                    code = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: getpass.getpass("") or input()
                    )
                except asyncio.TimeoutError:
                    logging.error("Authentication timed out")
                    return False
            
            try:
                await user_client.sign_in(PHONE_NUMBER, code)
            except SessionPasswordNeededError:
                if not password:
                    print("Enter 2FA password: ", end="")
                    try:
                        password = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: getpass.getpass("")
                        )
                    except asyncio.TimeoutError:
                        logging.error("2FA authentication timed out")
                        return False
                await user_client.sign_in(password=password)
            logging.info("User authenticated successfully")
        else:
            logging.info("User session loaded from file")
        
        try:
            USER_SESSION.chmod(FILE_PERMISSIONS)
        except Exception as e:
            logging.warning(f"Failed to set session file permissions: {str(e)}")
        return True
    except PhoneCodeInvalidError:
        logging.error("Invalid login code")
        await report_error("Invalid login code")
        return False
    except Exception as e:
        logging.error(f"Authentication error: {str(e)}")
        await report_error(f"Authentication error: {str(e)}")
        return False

# Command handlers
async def start(event):
    """Handle /start command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Sorry, only my boss can boss me around!")
        return
    await send_message_safe(
        bot_client, event.chat_id,
        "Welcome to the Telegram Bot! Use buttons or /help for commands.",
        buttons=settings_buttons
    )

async def help_command(event):
    """Handle /help command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can see the help menu!")
        return
    help_text = (
        "/start - Show main menu\n"
        "/settings - Show settings menu\n"
        "/setimage - Set image (attach image)\n"
        "/setmessage <text> - Set message\n"
        "/setinterval <seconds> - Set interval (min 30)\n"
        "/addtarget <@username/link> - Add target\n"
        "/removetarget <ID> - Remove target\n"
        "/cleartargets - Clear all targets\n"
        "/startbot - Start attack\n"
        "/stopbot - Stop attack\n"
        "/status - Show status\n"
        "/validate - Validate targets\n"
        "/checksetup - Check setup\n"
        "/reset - Reset stats\n"
        "/restart - Restart bot\n"
        "/health - Check health"
    )
    await send_message_safe(bot_client, event.chat_id, help_text)

async def settings(event):
    """Handle /settings command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Access denied!")
        return
    await send_message_safe(
        bot_client, event.chat_id,
        "Configure bot settings:",
        buttons=settings_buttons
    )

async def set_image(event):
    """Handle /setimage command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can set the image!")
        return
    if not event.message.photo:
        await send_message_safe(bot_client, event.chat_id, "Please attach an image with /setimage")
        return
    try:
        photo = event.message.photo
        if not photo:
            await send_message_safe(bot_client, event.chat_id, "No valid photo found in the message")
            return

        # Get the file size from the photo object
        # Use the largest size available (last in the sizes list)
        if photo.sizes and hasattr(photo.sizes[-1], 'file_size'):
            image_size = photo.sizes[-1].file_size
        else:
            # Fallback: Download the photo temporarily to check size
            temp_path = MEDIA_DIR / f"temp_{photo.id}.jpg"
            try:
                await event.message.download_media(file=str(temp_path))
                image_size = temp_path.stat().st_size
                temp_path.unlink()  # Clean up temporary file
            except Exception as e:
                logging.error(f"Error checking image size: {str(e)}")
                await send_message_safe(bot_client, event.chat_id, f"Failed to check image size: {str(e)}")
                return

        if image_size > MAX_IMAGE_SIZE:
            await send_message_safe(bot_client, event.chat_id, f"Image too large (> {MAX_IMAGE_SIZE // 1024 // 1024}MB)")
            return

        await event.message.download_media(file=str(IMAGE_PATH))
        try:
            IMAGE_PATH.chmod(FILE_PERMISSIONS)
        except Exception as e:
            logging.warning(f"Failed to set image file permissions: {str(e)}")
        await send_message_safe(bot_client, event.chat_id, f"Image set to {IMAGE_PATH}")
    except Exception as e:
        logging.error(f"Error setting image: {str(e)}")
        await send_message_safe(bot_client, event.chat_id, f"Failed to set image: {str(e)}")

async def set_image_button(event):
    """Handle set_image button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await send_message_safe(bot_client, event.chat_id, "Send an image with /setimage")
    await event.answer()

async def set_message(event):
    """Handle /setmessage command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can set the message!")
        return
    try:
        text = event.message.text.split(maxsplit=1)[1] if len(event.message.text.split()) > 1 else None
        if not text or len(text) > 4096:  # Telegram message length limit
            await send_message_safe(bot_client, event.chat_id, "Message must be 1-4096 characters")
            return
        async with state_lock:
            config["message"] = text
            save_config(config)
        await send_message_safe(bot_client, event.chat_id, f"Message set to: {text}")
    except Exception as e:
        logging.error(f"Error setting message: {str(e)}")
        await send_message_safe(bot_client, event.chat_id, f"Failed to set message: {str(e)}")

async def set_message_button(event):
    """Handle set_message button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await send_message_safe(bot_client, event.chat_id, "Usage: /setmessage Your message")
    await event.answer()

async def set_interval(event):
    """Handle /setinterval command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can set the interval!")
        return
    try:
        interval = int(event.message.text.split(maxsplit=1)[1])
        if interval < 30:
            await send_message_safe(bot_client, event.chat_id, "Interval must be >= 30 seconds")
            return
        async with state_lock:
            config["interval"] = interval
            save_config(config)
        await send_message_safe(bot_client, event.chat_id, f"Interval set to {interval} seconds")
    except (IndexError, ValueError):
        await send_message_safe(bot_client, event.chat_id, "Usage: /setinterval 120")
    except Exception as e:
        logging.error(f"Error setting interval: {str(e)}")
        await send_message_safe(bot_client, event.chat_id, f"Failed to set interval: {str(e)}")

async def set_interval_button(event):
    """Handle set_interval button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await send_message_safe(bot_client, event.chat_id, "Usage: /setinterval 120")
    await event.answer()

async def add_target(event):
    """Handle /addtarget command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can add targets!")
        return
    try:
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await send_message_safe(bot_client, event.chat_id, "Usage: /addtarget @username or https://t.me/link")
            return

        target_input = args[1].strip()
        if target_input.startswith("https://t.me/"):
            target_input = f"@{target_input.replace('https://t.me/', '')}"

        entity = await get_entity_safe(user_client, target_input)
        if not entity:
            await send_message_safe(bot_client, event.chat_id, f"Could not find: {target_input}")
            return

        if isinstance(entity, User):
            await send_message_safe(bot_client, event.chat_id, "Target must be a channel/group")
            return

        target_id = entity.id
        async with state_lock:
            if target_id in targets:
                await send_message_safe(bot_client, event.chat_id, f"Target {target_input} already added")
                return

            joined = await join_chat_safe(user_client, target_input)
            if not joined:
                await send_message_safe(bot_client, event.chat_id, f"Could not join {target_input}")
                return

            targets.append(target_id)
            target_usernames[str(target_id)] = target_input
            message_counts[str(target_id)] = 0
            last_message_time[target_id] = 0
            save_targets(targets)
            save_stats(message_counts)
        await send_message_safe(bot_client, event.chat_id, f"Added target: {target_input} (ID: {target_id})")
    except Exception as e:
        logging.error(f"Error adding target: {str(e)}")
        await send_message_safe(bot_client, event.chat_id, f"Error adding target: {str(e)}")

async def add_target_button(event):
    """Handle add_target button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await send_message_safe(bot_client, event.chat_id, "Usage: /addtarget @username")
    await event.answer()

async def remove_target(event):
    """Handle /removetarget command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can remove targets!")
        return
    try:
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await send_message_safe(bot_client, event.chat_id, "Usage: /removetarget 123456789")
            return
        
        target_id = int(args[1])
        async with state_lock:
            if target_id not in targets:
                await send_message_safe(bot_client, event.chat_id, f"Target ID {target_id} not found")
                return

            targets.remove(target_id)
            target_usernames.pop(str(target_id), None)
            message_counts.pop(str(target_id), None)
            last_message_time.pop(target_id, None)
            save_targets(targets)
            save_stats(message_counts)
        await send_message_safe(bot_client, event.chat_id, f"Removed target ID: {target_id}")
    except (ValueError, IndexError):
        await send_message_safe(bot_client, event.chat_id, "Usage: /removetarget 123456789")
    except Exception as e:
        logging.error(f"Error removing target: {str(e)}")
        await send_message_safe(bot_client, event.chat_id, f"Failed to remove target: {str(e)}")

async def remove_target_button(event):
    """Handle remove_target button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await send_message_safe(bot_client, event.chat_id, "Usage: /removetarget 123456789")
    await event.answer()

async def clear_targets(event):
    """Handle /cleartargets command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can clear targets!")
        return
    async with state_lock:
        global targets, message_counts, target_usernames, last_message_time
        targets = []
        message_counts = {}
        target_usernames = {}
        last_message_time = {}
        save_targets(targets)
        save_stats(message_counts)
    await send_message_safe(bot_client, event.chat_id, "All targets cleared")

async def validate_targets(event):
    """Handle /validate command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can validate targets!")
        return
    if not targets:
        await send_message_safe(bot_client, event.chat_id, "No targets configured")
        return

    target_details = []
    invalid_targets = []
    async with state_lock:
        # Batch entity lookups
        entities = await asyncio.gather(*[
            get_entity_safe(user_client, tid) for tid in targets
        ], return_exceptions=True)

        for tid, entity in zip(targets[:], entities):
            try:
                if isinstance(entity, Exception) or not entity:
                    target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')}): Inaccessible")
                    invalid_targets.append(tid)
                    continue

                chat_title = getattr(entity, 'title', 'Unknown')
                is_member = False
                try:
                    await user_client.get_permissions(entity, await user_client.get_me())
                    is_member = True
                except UserNotParticipantError:
                    joined = await join_chat_safe(user_client, target_usernames.get(str(tid), tid))
                    is_member = joined

                status = "Joined" if is_member else "Not Joined"
                target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')} - {chat_title}): {status}")

                if not is_member:
                    invalid_targets.append(tid)
            except Exception as e:
                target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')}): Error: {str(e)}")
                invalid_targets.append(tid)

        if invalid_targets:
            await send_message_safe(bot_client, event.chat_id, f"Found {len(invalid_targets)} invalid targets. Reply with /confirm_remove")
            async with state_lock:
                config["pending_removals"] = invalid_targets
                save_config(config)
        else:
            await send_message_safe(bot_client, event.chat_id, "Target Validation:\n" + "\n".join(target_details) or "No valid targets")

async def confirm_remove(event):
    """Handle /confirm_remove command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can confirm removals!")
        return
    async with state_lock:
        if "pending_removals" not in config:
            await send_message_safe(bot_client, event.chat_id, "No pending removals")
            return
        for tid in config["pending_removals"]:
            if tid in targets:
                targets.remove(tid)
                target_usernames.pop(str(tid), None)
                message_counts.pop(str(tid), None)
                last_message_time.pop(tid, None)
        save_targets(targets)
        save_stats(message_counts)
        del config["pending_removals"]
        save_config(config)
    await send_message_safe(bot_client, event.chat_id, "Invalid targets removed")

async def validate_targets_button(event):
    """Handle validate button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await validate_targets(event)

async def check_setup(event):
    """Handle /checksetup command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can check setup!")
        return
    setup_status = [
        f"Image: {'Set' if IMAGE_PATH.exists() else 'Not set'}",
        f"Targets: {len(targets)} configured" if targets else "Targets: None",
        f"Bot Client: {'Connected' if bot_client and bot_client.is_connected() else 'Disconnected'}",
        f"User Client: {'Connected' if user_client and user_client.is_connected() else 'Disconnected'}",
        f"Attack: {'Running' if is_running else 'Stopped'}"
    ]
    await send_message_safe(bot_client, event.chat_id, "Setup Status:\n" + "\n".join(setup_status))

async def health_check(event):
    """Handle /health command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can check health!")
        return
    health_status = [
        f"Bot Client: {'Connected' if bot_client and bot_client.is_connected() else 'Disconnected'}",
        f"User Client: {'Connected' if user_client and user_client.is_connected() else 'Disconnected'}",
        f"Active Tasks: {len([t for t in tasks if not t.done()])}",
        f"Memory Usage: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB"
    ]
    await send_message_safe(bot_client, event.chat_id, "Health Check:\n" + "\n".join(health_status))

async def health_check_button(event):
    """Handle health button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await health_check(event)

async def view_settings(event):
    """Handle view_settings button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    settings_text = await get_status_text()
    await send_message_safe(bot_client, event.chat_id, settings_text)
    await event.answer()

async def reset(event):
    """Handle /reset command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can reset stats!")
        return
    async with state_lock:
        global message_counts
        message_counts = {}
        save_stats(message_counts)
    await send_message_safe(bot_client, event.chat_id, "Stats reset")

async def restart(event):
    """Handle /restart command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can restart the bot!")
        return
    global is_running
    is_running = False
    await send_message_safe(bot_client, event.chat_id, "Restarting bot...")
    await shutdown()
    
    init_dirs()
    await start_clients()
    is_running = True
    await send_message_safe(bot_client, event.chat_id, "Bot restarted")
    
    if not IMAGE_PATH.exists():
        await send_message_safe(bot_client, ADMIN_ID, "Warning: No image set")
    if not targets:
        await send_message_safe(bot_client, ADMIN_ID, "Warning: No targets configured")

async def restart_button(event):
    """Handle restart button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await restart(event)
    await event.answer()

async def status(event):
    """Handle /status command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can check status!")
        return
    settings_text = await get_status_text()
    await send_message_safe(bot_client, event.chat_id, settings_text)

async def status_button(event):
    """Handle status button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    settings_text = await get_status_text()
    await send_message_safe(bot_client, event.chat_id, settings_text)
    await event.answer()

async def get_status_text() -> str:
    """Generate status text."""
    target_details = []
    async with state_lock:
        for tid in targets:
            try:
                chat = await get_entity_safe(user_client, tid)
                status = "Inaccessible" if not chat else "Joined"
                chat_title = getattr(chat, 'title', 'Unknown') if chat else 'Unknown'
                count = message_counts.get(str(tid), 0)
                target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')} - {chat_title}): {count} messages, {status}")
            except Exception as e:
                target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')}): Error: {str(e)}")

    return (
        f"Attack: {'Running' if is_running else 'Stopped'}\n"
        f"Message: {config['message']}\n"
        f"Interval: {config['interval']} seconds\n"
        f"Image: {'Set' if IMAGE_PATH.exists() else 'Not set'}\n"
        f"Targets: {len(targets)}\n"
        f"Messages Sent:\n" + "\n".join(target_details) or "Messages Sent: None"
    )

async def start_bot(event):
    """Handle /startbot command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can start the attack!")
        return
    global is_running
    if is_running:
        await send_message_safe(bot_client, event.chat_id, "Attack already running")
        return
    is_running = True
    await send_message_safe(bot_client, event.chat_id, "Attack started")
    
    if not IMAGE_PATH.exists():
        await send_message_safe(bot_client, event.chat_id, "Warning: No image set")
    if not targets:
        await send_message_safe(bot_client, event.chat_id, "Warning: No targets configured")

async def start_bot_button(event):
    """Handle start_bot button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await start_bot(event)
    await event.answer()

async def stop_bot(event):
    """Handle /stopbot command."""
    if event.sender_id != ADMIN_ID:
        await event.respond("Only admins can stop the attack!")
        return
    global is_running
    if not is_running:
        await send_message_safe(bot_client, event.chat_id, "Attack already stopped")
        return
    is_running = False
    await send_message_safe(bot_client, event.chat_id, "Attack stopped")

async def stop_bot_button(event):
    """Handle stop_bot button."""
    if event.sender_id != ADMIN_ID:
        await event.answer("Unauthorized")
        return
    await stop_bot(event)
    await event.answer()

async def send_messages():
    """Send messages to target channels."""
    last_stats_save = time.time()
    flood_waits: Dict[int, float] = {}  # Track flood wait expirations per target
    next_send_time: Dict[int, float] = {}  # Track next send time per target

    while True:
        try:
            if not is_running:
                await asyncio.sleep(5)
                continue

            current_time = time.time()
            if not targets or not IMAGE_PATH.exists():
                if not targets and current_time - last_warning_time.get('no_targets', 0) > WARNING_INTERVAL:
                    last_warning_time['no_targets'] = current_time
                    await send_message_safe(bot_client, ADMIN_ID, "No targets configured")
                if not IMAGE_PATH.exists() and current_time - last_warning_time.get('no_image', 0) > WARNING_INTERVAL:
                    last_warning_time['no_image'] = current_time
                    await send_message_safe(bot_client, ADMIN_ID, "No image set")
                await asyncio.sleep(config["interval"])
                continue

            if not user_client.is_connected():
                logging.warning("User client disconnected. Attempting reconnect.")
                if not await reconnect_client(user_client, "User client"):
                    await report_error("User client not authorized")
                    await asyncio.sleep(config["interval"])
                    continue

            async with state_lock:
                target_ids = targets[:]

            # Find the earliest next send time across all targets
            earliest_send_time = min(
                (next_send_time.get(tid, 0) for tid in target_ids),
                default=current_time
            )
            if earliest_send_time > current_time:
                await asyncio.sleep(earliest_send_time - current_time)

            for target_id in target_ids:
                async with send_semaphore:
                    try:
                        current_time = time.time()
                        # Skip if target is in flood wait or not yet time to send
                        if current_time < flood_waits.get(target_id, 0):
                            logging.debug(f"Skipping {target_id}: In flood wait")
                            continue
                        if current_time < next_send_time.get(target_id, 0):
                            logging.debug(f"Skipping {target_id}: Not yet time to send")
                            continue

                        chat = await get_entity_safe(user_client, target_id)
                        if not chat:
                            logging.error(f"Target {target_id} inaccessible")
                            async with state_lock:
                                if target_id in targets:
                                    targets.remove(target_id)
                                    target_usernames.pop(str(target_id), None)
                                    message_counts.pop(str(target_id), None)
                                    last_message_time.pop(target_id, None)
                                    next_send_time.pop(target_id, None)
                                    save_targets(targets)
                                    save_stats(message_counts)
                            await send_message_safe(bot_client, ADMIN_ID, f"Removed target {target_id}: Inaccessible")
                            continue

                        is_member = False
                        try:
                            await user_client.get_permissions(chat, await user_client.get_me())
                            is_member = True
                        except UserNotParticipantError:
                            joined = await join_chat_safe(user_client, target_usernames.get(str(target_id), target_id))
                            is_member = joined

                        if not is_member:
                            logging.error(f"Not a member of {target_id}")
                            async with state_lock:
                                if target_id in targets:
                                    targets.remove(target_id)
                                    target_usernames.pop(str(target_id), None)
                                    message_counts.pop(str(target_id), None)
                                    last_message_time.pop(target_id, None)
                                    next_send_time.pop(target_id, None)
                                    save_targets(targets)
                                    save_stats(message_counts)
                            await send_message_safe(bot_client, ADMIN_ID, f"Removed target {target_id}: Failed to join")
                            continue

                        message_number = message_counts.get(str(target_id), 0) + 1
                        for attempt in range(MAX_RETRIES):
                            try:
                                logging.info(f"Sending message #{message_number} to {target_id}")
                                await user_client.send_file(
                                    target_id,
                                    file=str(IMAGE_PATH),
                                    caption=config['message']
                                )
                                async with state_lock:
                                    message_counts[str(target_id)] = message_number
                                    last_message_time[target_id] = current_time
                                    next_send_time[target_id] = current_time + config["interval"]
                                logging.info(f"Successfully sent message #{message_number} to {target_id}")
                                break
                            except FloodWaitError as e:
                                wait_time = min(e.seconds, MAX_FLOOD_WAIT)
                                flood_waits[target_id] = current_time + wait_time
                                logging.warning(f"FloodWaitError for {target_id}: Waiting {wait_time}s")
                                await asyncio.sleep(wait_time)
                            except (ChatWriteForbiddenError, ChannelPrivateError):
                                logging.error(f"Cannot send to {target_id}: No permission")
                                break
                            except UserNotParticipantError:
                                logging.info(f"Bot not a participant in {target_id}. Attempting to rejoin.")
                                await join_chat_safe(user_client, target_usernames.get(str(target_id), target_id))
                            except Exception as e:
                                logging.error(f"Error sending to {target_id}: {str(e)}")
                                if attempt == MAX_RETRIES - 1:
                                    break
                                await asyncio.sleep(2 ** attempt)

                        if current_time - last_stats_save > STATS_SAVE_INTERVAL:
                            async with state_lock:
                                save_stats(message_counts)
                            last_stats_save = current_time

                    except Exception as e:
                        logging.error(f"Error processing target {target_id}: {str(e)}")
                        if isinstance(e, (ChannelPrivateError, UsernameNotOccupiedError)):
                            async with state_lock:
                                if target_id in targets:
                                    targets.remove(target_id)
                                    target_usernames.pop(str(target_id), None)
                                    message_counts.pop(str(target_id), None)
                                    last_message_time.pop(target_id, None)
                                    next_send_time.pop(target_id, None)
                                    save_targets(targets)
                                    save_stats(message_counts)
                            await send_message_safe(bot_client, ADMIN_ID, f"Removed target {target_id}: {str(e)}")

        except asyncio.CancelledError:
            logging.info("send_messages task cancelled")
            break
        except Exception as e:
            logging.error(f"Critical error in send_messages: {str(e)}")
            await report_error(f"Critical error in message sending: {str(e)}")
            await asyncio.sleep(10)

async def heartbeat():
    """Periodically check client connections."""
    while True:
        try:
            logging.info("Bot heartbeat")
            for client, name in [(bot_client, "Bot client"), (user_client, "User client")]:
                if client and not client.is_connected():
                    logging.warning(f"{name} disconnected. Reconnecting.")
                    await reconnect_client(client, name)
            # Clean up completed tasks
            global tasks
            tasks = [t for t in tasks if not t.done()]
            await asyncio.sleep(RECONNECT_INTERVAL)
        except asyncio.CancelledError:
            logging.info("heartbeat task cancelled")
            break
        except Exception as e:
            logging.error(f"Heartbeat error: {str(e)}")
            await report_error(f"Heartbeat error: {str(e)}")
            await asyncio.sleep(60)

def register_handlers(client: TelegramClient):
    """Register event handlers."""
    commands = [
        ("/start", start),
        ("/help", help_command),
        ("/settings", settings),
        ("/setimage", set_image),
        ("/setmessage", set_message),
        ("/setinterval", set_interval),
        ("/addtarget", add_target),
        ("/removetarget", remove_target),
        ("/cleartargets", clear_targets),
        ("/validate", validate_targets),
        ("/confirm_remove", confirm_remove),
        ("/checksetup", check_setup),
        ("/health", health_check),
        ("/reset", reset),
        ("/restart", restart),
        ("/status", status),
        ("/startbot", start_bot),
        ("/stopbot", stop_bot),
    ]
    for pattern, handler in commands:
        client.on(events.NewMessage(pattern=pattern))(handler)

    buttons = [
        (b"set_image", set_image_button),
        (b"set_message", set_message_button),
        (b"set_interval", set_interval_button),
        (b"add_target", add_target_button),
        (b"remove_target", remove_target_button),
        (b"validate", validate_targets_button),
        (b"health", health_check_button),
        (b"view_settings", view_settings),
        (b"restart", restart_button),
        (b"status", status_button),
        (b"start_bot", start_bot_button),
        (b"stop_bot", stop_bot_button),
    ]
    for data, handler in buttons:
        client.on(events.CallbackQuery(data=data))(handler)

async def start_clients():
    """Start bot and user clients."""
    global bot_client, user_client
    bot_client = TelegramClient(None, API_ID, API_HASH)
    try:
        await bot_client.start(bot_token=BOT_TOKEN)
    except Exception as e:
        logging.error(f"Failed to start bot client: {str(e)}")
        await report_error(f"Failed to start bot client: {str(e)}")
        sys.exit(1)
    
    register_handlers(bot_client)
    
    if not await authenticate_user():
        logging.error("User authentication failed")
        await bot_client.disconnect()
        sys.exit(1)

async def shutdown():
    """Cleanly shut down the bot."""
    global is_running, tasks
    is_running = False
    
    for task in tasks:
        if not task.done():
            task.cancel()
    
    try:
        await asyncio.gather(*[t for t in tasks if not t.done()], return_exceptions=True)
    except asyncio.CancelledError:
        pass
    
    for client, name in [(bot_client, "Bot client"), (user_client, "User client")]:
        if client and client.is_connected():
            try:
                await client.disconnect()
                logging.info(f"{name} disconnected")
            except Exception as e:
                logging.error(f"Error disconnecting {name}: {str(e)}")
    
    release_lock()
    logging.info("Shutdown complete")

async def main():
    """Main bot entry point."""
    global is_running, tasks
    init_dirs()
    acquire_lock()
    
    try:
        await start_clients()
        is_running = True
        
        logging.info(f"Bot started. Targets: {targets}, Image exists: {IMAGE_PATH.exists()}")
        if not IMAGE_PATH.exists():
            await send_message_safe(bot_client, ADMIN_ID, "Warning: No image set")
        if not targets:
            await send_message_safe(bot_client, ADMIN_ID, "Warning: No targets configured")
        else:
            await send_message_safe(bot_client, ADMIN_ID, f"Configured targets: {', '.join(target_usernames.values())}")

        tasks = [
            asyncio.create_task(send_messages()),
            asyncio.create_task(heartbeat()),
            asyncio.create_task(bot_client.run_until_disconnected()),
            asyncio.create_task(user_client.run_until_disconnected())
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")
        await report_error(f"Fatal error: {str(e)}")
    finally:
        await shutdown()

if __name__ == "__main__":
    try:
        import psutil
    except ImportError:
        logging.error("Install psutil: pip install psutil")
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        loop.run_until_complete(shutdown())
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        loop.run_until_complete(shutdown())
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        except Exception as e:
            logging.error(f"Error during final shutdown: {str(e)}")
        loop.close()
        logging.info("Bot stopped")
