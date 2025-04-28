import asyncio
import json
import os
import logging
import sys
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.types import User, Channel, Chat
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.custom import Button
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    ChatAdminRequiredError,
    InviteHashExpiredError,
    FloodWaitError,
    ChannelPrivateError,
    UsernameNotOccupiedError,
)

# Initialize directories
CONFIG_DIR = "config"
MEDIA_DIR = "media"
def init_dirs():
    os.makedirs(CONFIG_DIR, exist_ok=True)
    os.makedirs(MEDIA_DIR, exist_ok=True)
init_dirs()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(CONFIG_DIR, "log.txt")),
        logging.StreamHandler()
    ]
)

# Configuration
API_ID = 29637547
API_HASH = "13e303a526522f741c0680cfc8cd9c00"
BOT_TOKEN = "7547436649:AAG1CoExVXPpace2NxAs70EZ-aa11jIzG24"
ADMIN_ID = 6257711894  # Update this to a valid admin ID
SESSION_FILE = os.path.join(CONFIG_DIR, "user_session")
BOT_SESSION_FILE = os.path.join(CONFIG_DIR, "bot_session")
TARGETS_FILE = os.path.join(CONFIG_DIR, "targets.json")
STATS_FILE = os.path.join(CONFIG_DIR, "stats.json")
IMAGE_PATH = os.path.join(MEDIA_DIR, "bot_image.jpg")
CREDENTIALS_FILE = os.path.join(CONFIG_DIR, "credentials.json")

# Initialize clients
bot = TelegramClient(BOT_SESSION_FILE, API_ID, API_HASH)
user_client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# Global state
is_running = False
message_counts = {}
last_warning_time = {}
target_usernames = {}

# File operations
def load_targets():
    try:
        if os.path.exists(TARGETS_FILE):
            with open(TARGETS_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    global target_usernames
                    target_usernames = data.get("usernames", {})
                    return data.get("targets", [])
                return data
    except Exception as e:
        logging.error(f"Error loading targets: {str(e)}")
        return []
    return []

def save_targets(targets):
    try:
        with open(TARGETS_FILE, "w") as f:
            json.dump({"targets": targets, "usernames": target_usernames}, f)
    except Exception as e:
        logging.error(f"Error saving targets: {str(e)}")

def load_config():
    config_file = os.path.join(CONFIG_DIR, "settings.json")
    default_config = {"message": "Hello! This is an automated message with an image.", "interval": 120}
    try:
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error loading config: {str(e)}")
        return default_config
    return default_config

def save_config(config):
    try:
        with open(os.path.join(CONFIG_DIR, "settings.json"), "w") as f:
            json.dump(config, f)
    except Exception as e:
        logging.error(f"Error saving config: {str(e)}")

def load_stats():
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error loading stats: {str(e)}")
        return {}
    return {}

def save_stats(stats):
    try:
        with open(STATS_FILE, "w") as f:
            json.dump(stats, f)
    except Exception as e:
        logging.error(f"Error saving stats: {str(e)}")

def load_credentials():
    try:
        if os.path.exists(CREDENTIALS_FILE):
            with open(CREDENTIALS_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error loading credentials: {str(e)}")
        return {}
    return {}

def save_credentials(credentials):
    try:
        with open(CREDENTIALS_FILE, "w") as f:
            json.dump(credentials, f)
    except Exception as e:
        logging.error(f"Error saving credentials: {str(e)}")

targets = load_targets()
config = load_config()
message_counts = load_stats()
credentials = load_credentials()

# Inline buttons
settings_buttons = [
    [Button.inline("Set Image", b"set_image"), Button.inline("Set Message", b"set_message")],
    [Button.inline("Set Interval", b"set_interval"), Button.inline("Add Target", b"add_target")],
    [Button.inline("Remove Target", b"remove_target"), Button.inline("View Settings", b"view_settings")],
    [Button.inline("Start Attack", b"start_bot"), Button.inline("Stop Attack", b"stop_bot")],
    [Button.inline("Status", b"status"), Button.inline("Validate Targets", b"validate")]
]

async def get_entity_safe(client, target_input):
    for attempt in range(3):
        try:
            entity = await client.get_entity(target_input)
            return entity
        except (ValueError, ChannelPrivateError, UsernameNotOccupiedError):
            if attempt == 2:
                return None
            await asyncio.sleep(2)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
    return None

async def join_chat_safe(client, target_input):
    for attempt in range(3):
        try:
            if isinstance(target_input, str):
                if target_input.startswith("https://t.me/"):
                    target_input = target_input.replace("https://t.me/", "@")
                if not target_input.startswith("@"):
                    target_input = f"@{target_input}"
            await client(JoinChannelRequest(target_input))
            return True
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except (ChatAdminRequiredError, InviteHashExpiredError, ChannelPrivateError) as e:
            logging.error(f"Cannot join {target_input}: {str(e)}")
            return False
        except Exception as e:
            if attempt == 2:
                logging.error(f"Error joining {target_input}: {str(e)}")
                return False
            await asyncio.sleep(2)
    return False

async def send_message_safe(client, chat_id, message, buttons=None):
    try:
        await client.send_message(chat_id, message, buttons=buttons)
    except Exception as e:
        logging.error(f"Failed to send message to {chat_id}: {str(e)}")

# Commands
@bot.on(events.NewMessage(pattern="/start", from_users=ADMIN_ID))
async def start(event):
    await send_message_safe(
        bot, event.chat_id,
        "Welcome to the Telegram Bot! Use the buttons below to configure settings or send /help for commands.",
        buttons=settings_buttons
    )

@bot.on(events.NewMessage(pattern="/help", from_users=ADMIN_ID))
async def help_command(event):
    help_text = (
        "Available Commands:\n"
        "/start - Show main menu\n"
        "/settings - Show settings menu\n"
        "/setimage - Set image (attach an image)\n"
        "/setmessage <text> - Set message text\n"
        "/setinterval <seconds> - Set send interval (min 30)\n"
        "/addtarget <@username/link> - Add a target (channel or group)\n"
        "/removetarget <ID> - Remove a target by ID\n"
        "/cleartargets - Clear all targets\n"
        "/startbot - Start target attack\n"
        "/stopbot - Stop target attack\n"
        "/status - Show status and message counts\n"
        "/validate - Validate all targets\n"
        "/checksetup - Check setup (image, targets, client)\n"
        "/reset - Reset message counts or session"
    )
    await send_message_safe(bot, event.chat_id, help_text)

@bot.on(events.NewMessage(pattern="/settings", from_users=ADMIN_ID))
async def settings(event):
    await send_message_safe(
        bot, event.chat_id,
        "Configure the bot using the buttons below:",
        buttons=settings_buttons
    )

@bot.on(events.NewMessage(pattern="/setimage", from_users=ADMIN_ID))
async def set_image(event):
    if not event.message.photo:
        await send_message_safe(bot, event.chat_id, "Please attach an image with the /setimage command.")
        return
    try:
        await event.message.download_media(file=IMAGE_PATH)
        await send_message_safe(bot, event.chat_id, f"Image set to {IMAGE_PATH}.")
    except Exception as e:
        logging.error(f"Error setting image: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to set image: {str(e)}")

@bot.on(events.CallbackQuery(data=b"set_image"))
async def set_image_button(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    await send_message_safe(bot, event.chat_id, "Please send an image with the /setimage command.")
    await event.answer()

@bot.on(events.NewMessage(pattern="/setmessage", from_users=ADMIN_ID))
async def set_message(event):
    try:
        text = event.message.text.split(maxsplit=1)[1] if len(event.message.text.split()) > 1 else None
        if not text:
            await send_message_safe(bot, event.chat_id, "Please provide a message. Usage: /setmessage Your message here")
            return
        config["message"] = text
        save_config(config)
        await send_message_safe(bot, event.chat_id, f"Message set to: {text}")
    except Exception as e:
        logging.error(f"Error setting message: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to set message: {str(e)}")

@bot.on(events.CallbackQuery(data=b"set_message"))
async def set_message_button(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    await send_message_safe(bot, event.chat_id, "Please send the message text with the /setmessage command. Example: /setmessage Hello, this is a test!")
    await event.answer()

@bot.on(events.NewMessage(pattern="/setinterval", from_users=ADMIN_ID))
async def set_interval(event):
    try:
        interval = int(event.message.text.split(maxsplit=1)[1])
        if interval < 30:
            await send_message_safe(bot, event.chat_id, "Interval must be at least 30 seconds.")
            return
        config["interval"] = interval
        save_config(config)
        await send_message_safe(bot, event.chat_id, f"Send interval set to {interval} seconds.")
    except (IndexError, ValueError):
        await send_message_safe(bot, event.chat_id, "Please provide a valid number. Usage: /setinterval 120")
    except Exception as e:
        logging.error(f"Error setting interval: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to set interval: {str(e)}")

@bot.on(events.CallbackQuery(data=b"set_interval"))
async def set_interval_button(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    await send_message_safe(bot, event.chat_id, "Please send the interval in seconds with the /setinterval command. Example: /setinterval 120")
    await event.answer()

@bot.on(events.NewMessage(pattern="/addtarget", from_users=ADMIN_ID))
async def add_target(event):
    try:
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await send_message_safe(bot, event.chat_id, "Please provide a username or link. Usage: /addtarget @username or /addtarget https://t.me/link")
            return

        target_input = args[1].strip()
        logging.info(f"Attempting to add target: {target_input}")

        if target_input.startswith("https://t.me/"):
            target_input = target_input.replace("https://t.me/", "@")
        if not target_input.startswith("@"):
            target_input = f"@{target_input}"

        async with user_client:
            entity = await get_entity_safe(user_client, target_input)
            if not entity:
                await send_message_safe(bot, event.chat_id, f"Could not find entity: {target_input}")
                return

            if isinstance(entity, User):
                await send_message_safe(bot, event.chat_id, f"Cannot add {target_input}: Target must be a channel or group, not a user.")
                return
            elif not isinstance(entity, (Channel, Chat)):
                await send_message_safe(bot, event.chat_id, f"Cannot add {target_input}: Target must be a channel or group.")
                return

            target_id = entity.id
            if target_id in targets:
                await send_message_safe(bot, event.chat_id, f"Target {target_input} is already added.")
                return

            try:
                participants = await user_client.get_participants(entity, limit=1)
                is_member = any(p.id == (await user_client.get_me()).id for p in participants)
            except Exception:
                is_member = False

            if not is_member:
                joined = await join_chat_safe(user_client, target_input)
                if not joined:
                    await send_message_safe(bot, event.chat_id, f"Could not join {target_input}")
                    return

            targets.append(target_id)
            target_usernames[str(target_id)] = target_input
            message_counts[str(target_id)] = 0
            save_targets(targets)
            save_stats(message_counts)
            logging.info(f"Successfully added target: {target_input} (ID: {target_id})")
            await send_message_safe(bot, event.chat_id, f"Added target: {target_input} (ID: {target_id})")
            
    except Exception as e:
        logging.error(f"Error adding target {target_input}: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Error adding target: {str(e)}")

@bot.on(events.CallbackQuery(data=b"add_target"))
async def add_target_button(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    await send_message_safe(bot, event.chat_id, "Please send the target username or link with the /addtarget command. Example: /addtarget @username")
    await event.answer()

@bot.on(events.NewMessage(pattern="/removetarget", from_users=ADMIN_ID))
async def remove_target(event):
    try:
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await send_message_safe(bot, event.chat_id, "Please provide a target ID. Usage: /removetarget 123456789")
            return
        
        target_id = int(args[1])
        if target_id in targets:
            targets.remove(target_id)
            target_usernames.pop(str(target_id), None)
            message_counts.pop(str(target_id), None)
            save_targets(targets)
            save_stats(message_counts)
            await send_message_safe(bot, event.chat_id, f"Removed target ID: {target_id}")
        else:
            await send_message_safe(bot, event.chat_id, f"Target ID {target_id} not found.")
    except (ValueError, IndexError):
        await send_message_safe(bot, event.chat_id, "Please provide a valid target ID. Usage: /removetarget 123456789")
    except Exception as e:
        logging.error(f"Error removing target: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to remove target: {str(e)}")

@bot.on(events.CallbackQuery(data=b"remove_target"))
async def remove_target_button(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    await send_message_safe(bot, event.chat_id, "Please send the target ID to remove with the /removetarget command. Example: /removetarget 123456789")
    await event.answer()

@bot.on(events.NewMessage(pattern="/cleartargets", from_users=ADMIN_ID))
async def clear_targets(event):
    global targets, message_counts, target_usernames
    try:
        targets = []
        message_counts = {}
        target_usernames = {}
        save_targets(targets)
        save_stats(message_counts)
        await send_message_safe(bot, event.chat_id, "All targets and message counts cleared.")
    except Exception as e:
        logging.error(f"Error clearing targets: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to clear targets: {str(e)}")

@bot.on(events.NewMessage(pattern="/validate", from_users=ADMIN_ID))
async def validate_targets(event):
    if not targets:
        await send_message_safe(bot, event.chat_id, "No targets configured.")
        return
    
    target_details = []
    async with user_client:
        for tid in targets[:]:
            try:
                chat = await get_entity_safe(user_client, tid)
                if not chat:
                    target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')}): Inaccessible (Entity not found)")
                    continue
                    
                chat_title = getattr(chat, 'title', 'Unknown')
                try:
                    participants = await user_client.get_participants(chat, limit=1)
                    is_member = any(p.id == (await user_client.get_me()).id for p in participants)
                except Exception:
                    is_member = False
                    
                status = "Accessible, Joined" if is_member else "Accessible, Not Joined"
                target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')} - {chat_title}): {status}")
                
                if not is_member:
                    joined = await join_chat_safe(user_client, tid)
                    if joined:
                        target_details[-1] = f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')} - {chat_title}): Accessible, Joined"
                    else:
                        targets.remove(tid)
                        target_usernames.pop(str(tid), None)
                        message_counts.pop(str(tid), None)
                        save_targets(targets)
                        save_stats(message_counts)
                        await send_message_safe(bot, ADMIN_ID, f"Removed target {tid}: Failed to join")
                        
            except Exception as e:
                logging.error(f"Target {tid} inaccessible: {str(e)}")
                target_details.append(f"Target {tid} ({target_usernames.get(str(tid), 'Unknown')}): Inaccessible ({str(e)})")
                targets.remove(tid)
                target_usernames.pop(str(tid), None)
                message_counts.pop(str(tid), None)
                save_targets(targets)
                save_stats(message_counts)
                await send_message_safe(bot, ADMIN_ID, f"Removed target {tid}: Inaccessible ({str(e)})")
    
    await send_message_safe(bot, event.chat_id, "Target Validation:\n" + "\n".join(target_details) if target_details else "No valid targets.")

@bot.on(events.CallbackQuery(data=b"validate"))
async def validate_targets_button(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    await validate_targets(event)

@bot.on(events.NewMessage(pattern="/checksetup", from_users=ADMIN_ID))
async def check_setup(event):
    try:
        setup_status = []
        image_status = "Set" if os.path.exists(IMAGE_PATH) else "Not set (use /setimage to set an image)"
        setup_status.append(f"Image: {image_status}")
        target_status = f"{len(targets)} configured" if targets else "None configured (use /addtarget)"
        setup_status.append(f"Targets: {target_status}")
        
        client_status = "Disconnected"
        try:
            await user_client.connect()
            if await user_client.is_user_authorized():
                client_status = "Connected"
        except Exception as e:
            logging.error(f"Error checking client status: {str(e)}")
        
        setup_status.append(f"User Client: {client_status}")
        attack_status = f"Running, sending to {len(targets)} targets" if is_running and targets else "Stopped"
        setup_status.append(f"Target Attack Status: {attack_status}")
        await send_message_safe(bot, event.chat_id, "Setup Status:\n" + "\n".join(setup_status))
    except Exception as e:
        logging.error(f"Error checking setup: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to check setup: {str(e)}")

@bot.on(events.CallbackQuery(data=b"view_settings"))
async def view_settings(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    try:
        settings_text = await get_status_text()
        await send_message_safe(bot, event.chat_id, settings_text)
    except Exception as e:
        logging.error(f"Error viewing settings: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to view settings: {str(e)}")
    await event.answer()

@bot.on(events.NewMessage(pattern="/reset", from_users=ADMIN_ID))
async def reset(event):
    try:
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2 or args[1] not in ["stats", "session"]:
            await send_message_safe(bot, event.chat_id, "Usage: /reset stats (clear message counts) or /reset session (clear session file)")
            return
        
        if args[1] == "stats":
            global message_counts
            message_counts = {}
            save_stats(message_counts)
            await send_message_safe(bot, event.chat_id, "Message counts reset.")
        elif args[1] == "session":
            session_file = f"{SESSION_FILE}.session"
            if os.path.exists(session_file):
                os.remove(session_file)
                await send_message_safe(bot, event.chat_id, "Session file cleared. Restart the bot to re-authenticate.")
            else:
                await send_message_safe(bot, event.chat_id, "No session file found.")
    except Exception as e:
        logging.error(f"Error resetting: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to reset: {str(e)}")

@bot.on(events.NewMessage(pattern="/status", from_users=ADMIN_ID))
async def status(event):
    try:
        settings_text = await get_status_text()
        await send_message_safe(bot, event.chat_id, settings_text)
    except Exception as e:
        logging.error(f"Error getting status: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to get status: {str(e)}")

@bot.on(events.CallbackQuery(data=b"status"))
async def status_button(event):
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    try:
        settings_text = await get_status_text()
        await send_message_safe(bot, event.chat_id, settings_text)
    except Exception as e:
        logging.error(f"Error getting status: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to get status: {str(e)}")
    await event.answer()

async def get_status_text():
    target_details = []
    async with user_client:
        for tid in targets:
            try:
                chat = await get_entity_safe(user_client, tid)
                if not chat:
                    target_details.append(f"  Target {tid} ({target_usernames.get(str(tid), 'Unknown')}): {message_counts.get(str(tid), 0)} messages, Inaccessible")
                    continue
                    
                chat_title = getattr(chat, 'title', 'Unknown')
                try:
                    participants = await user_client.get_participants(chat, limit=1)
                    is_member = any(p.id == (await user_client.get_me()).id for p in participants)
                    status = "Joined" if is_member else "Not Joined"
                except Exception:
                    status = "Unknown"
                    
                count = message_counts.get(str(tid), 0)
                target_details.append(f"  Target {tid} ({target_usernames.get(str(tid), 'Unknown')} - {chat_title}): {count} messages, {status}")
            except Exception as e:
                target_details.append(f"  Target {tid} ({target_usernames.get(str(tid), 'Unknown')}): {message_counts.get(str(tid), 0)} messages, Error: {str(e)}")
    
    return (
        f"Target Attack Status: {'Running, sending to ' + str(len(targets)) + ' targets' if is_running and targets else 'Stopped'}\n"
        f"Message: {config['message']}\n"
        f"Interval: {config['interval']} seconds\n"
        f"Image: {'Set' if os.path.exists(IMAGE_PATH) else 'Not set'}\n"
        f"Targets: {', '.join(map(str, targets)) if targets else 'None'}\n"
        f"Messages Sent:\n" + "\n".join(target_details) if target_details else "Messages Sent: None"
    )

@bot.on(events.NewMessage(pattern="/startbot", from_users=ADMIN_ID))
async def start_bot(event):
    global is_running
    try:
        if is_running:
            await send_message_safe(bot, event.chat_id, "Target attack is already running.")
            return
        
        is_running = True
        await send_message_safe(bot, event.chat_id, "Target attack started.")
        
        if not os.path.exists(IMAGE_PATH):
            await send_message_safe(bot, event.chat_id, "Warning: No image set. Use /setimage to set an image.")
        if not targets:
            await send_message_safe(bot, event.chat_id, "Warning: No targets configured. Use /addtarget to add targets.")
    except Exception as e:
        logging.error(f"Error starting bot: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to start bot: {str(e)}")

@bot.on(events.CallbackQuery(data=b"start_bot"))
async def start_bot_button(event):
    global is_running
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    
    try:
        if is_running:
            await send_message_safe(bot, event.chat_id, "Target attack is already running.")
        else:
            is_running = True
            await send_message_safe(bot, event.chat_id, "Target attack started.")
            if not os.path.exists(IMAGE_PATH):
                await send_message_safe(bot, event.chat_id, "Warning: No image set. Use /setimage to set an image.")
            if not targets:
                await send_message_safe(bot, event.chat_id, "Warning: No targets configured. Use /addtarget to add targets.")
    except Exception as e:
        logging.error(f"Error starting bot: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to start bot: {str(e)}")
    await event.answer()

@bot.on(events.NewMessage(pattern="/stopbot", from_users=ADMIN_ID))
async def stop_bot(event):
    global is_running
    try:
        if not is_running:
            await send_message_safe(bot, event.chat_id, "Target attack is already stopped.")
            return
        
        is_running = False
        await send_message_safe(bot, event.chat_id, "Target attack stopped.")
    except Exception as e:
        logging.error(f"Error stopping bot: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to stop bot: {str(e)}")

@bot.on(events.CallbackQuery(data=b"stop_bot"))
async def stop_bot_button(event):
    global is_running
    if event.sender_id != ADMIN_ID:
        await event.answer("You are not authorized to use this button.")
        return
    
    try:
        if not is_running:
            await send_message_safe(bot, event.chat_id, "Target attack is already stopped.")
        else:
            is_running = False
            await send_message_safe(bot, event.chat_id, "Target attack stopped.")
    except Exception as e:
        logging.error(f"Error stopping bot: {str(e)}")
        await send_message_safe(bot, event.chat_id, f"Failed to stop bot: {str(e)}")
    await event.answer()

async def send_messages():
    WARNING_INTERVAL = 300
    while True:
        try:
            if not is_running:
                await asyncio.sleep(5)
                continue
            
            if not targets:
                current_time = datetime.now().timestamp()
                if current_time - last_warning_time.get('no_targets', 0) > WARNING_INTERVAL:
                    last_warning_time['no_targets'] = current_time
                    await send_message_safe(bot, ADMIN_ID, "No targets configured. Use /addtarget to add targets.")
                await asyncio.sleep(config["interval"])
                continue
            
            if not os.path.exists(IMAGE_PATH):
                current_time = datetime.now().timestamp()
                if current_time - last_warning_time.get('no_image', 0) > WARNING_INTERVAL:
                    last_warning_time['no_image'] = current_time
                    await send_message_safe(bot, ADMIN_ID, "Image not set. Use /setimage to set an image.")
                await asyncio.sleep(config["interval"])
                continue

            try:
                await user_client.connect()
                if not await user_client.is_user_authorized():
                    logging.error("User client not authorized")
                    await send_message_safe(bot, ADMIN_ID, "User client not authorized. Please reset session with /reset session")
                    await asyncio.sleep(config["interval"])
                    continue
            except Exception as e:
                logging.error(f"Error connecting user client: {str(e)}")
                await asyncio.sleep(10)
                continue

            for target_id in targets[:]:
                try:
                    chat = await get_entity_safe(user_client, target_id)
                    if not chat:
                        targets.remove(target_id)
                        target_usernames.pop(str(target_id), None)
                        message_counts.pop(str(target_id), None)
                        save_targets(targets)
                        save_stats(message_counts)
                        await send_message_safe(bot, ADMIN_ID, f"Removed target {target_id}: Inaccessible")
                        continue
                        
                    try:
                        participants = await user_client.get_participants(chat, limit=1)
                        is_member = any(p.id == (await user_client.get_me()).id for p in participants)
                    except Exception:
                        is_member = False
                        
                    if not is_member:
                        joined = await join_chat_safe(user_client, target_id)
                        if not joined:
                            targets.remove(target_id)
                            target_usernames.pop(str(target_id), None)
                            message_counts.pop(str(target_id), None)
                            save_targets(targets)
                            save_stats(message_counts)
                            await send_message_safe(bot, ADMIN_ID, f"Removed target {target_id}: Failed to join")
                            continue

                    message_number = message_counts.get(str(target_id), 0) + 1
                    unique_id = f"{target_id}-{message_number}"

                    for attempt in range(2):
                        try:
                            await user_client.send_file(
                                target_id,
                                file=IMAGE_PATH,
                                caption=config['message']
                            )
                            message_counts[str(target_id)] = message_number
                            save_stats(message_counts)
                            logging.info(f"Sent message #{message_number} (ID: {unique_id}) to {target_id} ({target_usernames.get(str(target_id), 'Unknown')})")
                            break
                        except FloodWaitError as e:
                            logging.warning(f"Flood wait for {e.seconds} seconds for target {target_id}")
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            if attempt == 1:
                                logging.error(f"Error sending to {target_id} ({target_usernames.get(str(target_id), 'Unknown')}): {str(e)}")
                                break
                            await asyncio.sleep(5)
                                
                except FloodWaitError as e:
                    logging.warning(f"Flood wait for {e.seconds} seconds for target {target_id}")
                    await asyncio.sleep(e.seconds)
                except (ChatAdminRequiredError, InviteHashExpiredError) as e:
                    targets.remove(target_id)
                    target_usernames.pop(str(target_id), None)
                    message_counts.pop(str(target_id), None)
                    save_targets(targets)
                    save_stats(message_counts)
                    await send_message_safe(bot, ADMIN_ID, f"Removed target {target_id}: {str(e)}")
                except Exception as e:
                    logging.error(f"Error processing target {target_id} ({target_usernames.get(str(target_id), 'Unknown')}): {str(e)}")
            
            await asyncio.sleep(config["interval"])
            
        except Exception as e:
            logging.error(f"Error in send_messages loop: {str(e)}")
            await asyncio.sleep(10)

async def authenticate_client():
    session_file = f"{SESSION_FILE}.session"
    is_interactive = sys.stdin.isatty()

    if os.path.exists(session_file):
        try:
            await user_client.connect()
            if await user_client.is_user_authorized():
                logging.info("User client started with existing session.")
                return True
        except Exception as e:
            logging.error(f"Failed to start user client with existing session: {str(e)}")
            os.remove(session_file)

    if not is_interactive:
        if credentials.get("phone"):
            try:
                phone = credentials["phone"]
                code = credentials.get("code")
                password = credentials.get("password")

                async def phone_callback():
                    return phone

                async def code_callback():
                    if code:
                        return code
                    raise Exception("No code provided in credentials for non-interactive environment")

                async def password_callback():
                    return password if password else None

                await user_client.start(
                    phone=phone_callback,
                    code=code_callback,
                    password=password_callback
                )
                logging.info("User client authenticated using stored credentials.")
                return True
            except Exception as e:
                logging.error(f"Authentication failed using stored credentials: {str(e)}")
                return False
        else:
            logging.error("No credentials provided for non-interactive environment. Please run interactively first or set credentials in config/credentials.json.")
            return False

    # Interactive authentication
    try:
        print("Please enter your phone (or bot token): ", end="")
        phone = input().strip()
        if not phone:
            raise Exception("No phone number provided")

        async def phone_callback():
            return phone

        async def code_callback():
            print("Please enter the code you received: ", end="")
            return input().strip()

        async def password_callback():
            if await user_client.is_user_authorized():
                print("Please enter your password (if 2FA is enabled): ", end="")
                return input().strip()
            return None

        await user_client.start(
            phone=phone_callback,
            code=code_callback,
            password=password_callback
        )
        logging.info("User client authenticated successfully.")

        # Save credentials
        credentials["phone"] = phone
        save_credentials(credentials)
        return True
    except PhoneCodeInvalidError:
        logging.error("Invalid code provided. Please try again.")
        if os.path.exists(session_file):
            os.remove(session_file)
        return False
    except SessionPasswordNeededError:
        logging.error("Two-factor authentication required. Please provide the password.")
        return False
    except Exception as e:
        logging.error(f"Authentication failed: {str(e)}")
        return False

async def heartbeat():
    while True:
        try:
            logging.info("Bot is running...")
            await asyncio.sleep(300)
        except Exception as e:
            logging.error(f"Error in heartbeat: {str(e)}")
            await asyncio.sleep(60)

async def main():
    global is_running
    init_dirs()
    
    try:
        await bot.start(bot_token=BOT_TOKEN)
        logging.info("Bot started with session file: %s.session", BOT_SESSION_FILE)
    except Exception as e:
        logging.error(f"Failed to start bot: {str(e)}")
        raise

    authenticated = await authenticate_client()
    if not authenticated:
        logging.error("Authentication failed. Exiting.")
        raise Exception("Authentication failed")

    is_running = True
    logging.info("Target attack started by default.")
    
    try:
        if not os.path.exists(IMAGE_PATH):
            last_warning_time['no_image'] = datetime.now().timestamp()
            await send_message_safe(bot, ADMIN_ID, "Warning: No image set. Use /setimage to set an image.")
        if not targets:
            last_warning_time['no_targets'] = datetime.now().timestamp()
            await send_message_safe(bot, ADMIN_ID, "Warning: No targets configured. Use /addtarget to add targets.")
    except Exception as e:
        logging.error(f"Error sending initial warnings: {str(e)}")

    try:
        tasks = [
            send_messages(),
            heartbeat(),
            bot.run_until_disconnected()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logging.error(f"Error in main loop: {str(e)}")
        raise

async def shutdown():
    global is_running
    is_running = False
    try:
        if bot.is_connected():
            await bot.disconnect()
            logging.info("Bot disconnected")
    except Exception as e:
        logging.error(f"Error disconnecting bot: {str(e)}")
    try:
        if user_client.is_connected():
            await user_client.disconnect()
            logging.info("User client disconnected")
    except Exception as e:
        logging.error(f"Error disconnecting user client: {str(e)}")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Shutting down due to manual interruption (Ctrl+C)...")
        loop.run_until_complete(shutdown())
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        loop.run_until_complete(shutdown())
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        logging.info("Event loop closed.")
