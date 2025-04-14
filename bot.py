import os
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import csv
import random
import time

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from telethon import TelegramClient, functions, types as telethon_types
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty, InputPeerChannel, InputPeerUser
from telethon.errors.rpcerrorlist import (
    PeerFloodError,
    UserPrivacyRestrictedError,
    FloodWaitError,
    ChannelPrivateError,
    UserNotMutualContactError
)

# --- Configuration ---
API_ID = 25781839  # Replace with your actual API ID
API_HASH = "20a3f2f168739259a180dcdd642e196c"  # Replace with your actual API HASH
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"  # Replace with your bot token
ADMIN_IDS = [7584086775]  # Replace with your admin IDs
SESSION_DIR = "sessions"
DATA_DIR = "data"
MAX_ADD_PER_SESSION = 80  # Max users to add per session before switching
DELAY_BETWEEN_ADDS = (30, 60)  # Random delay range between adds in seconds
SCRAPE_LIMIT = 10000  # Max members to scrape at once

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- State Machine ---
class BotStates(StatesGroup):
    waiting_for_session = State()
    waiting_for_group_link = State()
    waiting_for_target_group = State()
    waiting_for_member_file = State()
    waiting_for_scrape_limit = State()

# --- Aiogram Setup ---
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- Data Models ---
class Member:
    def __init__(self, user_id: int, username: str = None, access_hash: int = None, name: str = None):
        self.user_id = user_id
        self.username = username
        self.access_hash = access_hash
        self.name = name

    def to_dict(self) -> Dict:
        return {
            'user_id': self.user_id,
            'username': self.username,
            'access_hash': self.access_hash,
            'name': self.name
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Member':
        return cls(
            user_id=data.get('user_id'),
            username=data.get('username'),
            access_hash=data.get('access_hash'),
            name=data.get('name')
        )

# --- Utilities ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def save_session_file(file: types.Document) -> str:
    """Save uploaded session file and return its path"""
    file_path = os.path.join(SESSION_DIR, file.file_name)
    await bot.download(file, destination=file_path)
    return file_path

async def validate_telethon_session(session_path: str) -> bool:
    """Validate if a Telethon session file is authorized"""
    try:
        session_name = os.path.splitext(os.path.basename(session_path))[0]
        async with TelegramClient(session_path, API_ID, API_HASH) as client:
            return await client.is_user_authorized()
    except Exception as e:
        logger.error(f"Session validation failed for {session_path}: {str(e)}")
        return False

def list_sessions() -> List[str]:
    """List all available session files"""
    return [f for f in os.listdir(SESSION_DIR) if f.endswith(".session")]

async def get_entity_by_input(client: TelegramClient, input_str: str):
    """Get entity by username, link or ID"""
    try:
        return await client.get_entity(input_str)
    except ValueError as e:
        if "Cannot find any entity corresponding to" in str(e):
            # Try to extract username from link
            if "t.me/" in input_str:
                username = input_str.split("t.me/")[-1].replace("/", "")
                return await client.get_entity(username)
        raise e

async def scrape_members(client: TelegramClient, group_entity, limit: int = SCRAPE_LIMIT) -> List[Member]:
    """Scrape members from a group/channel"""
    members = []
    try:
        async for user in client.iter_participants(group_entity, aggressive=True, limit=limit):
            member = Member(
                user_id=user.id,
                username=user.username,
                access_hash=user.access_hash if hasattr(user, 'access_hash') else None,
                name=getattr(user, 'first_name', '') + ' ' + getattr(user, 'last_name', '')
            )
            members.append(member)
            if len(members) % 100 == 0:
                logger.info(f"Scraped {len(members)} members so far...")
    except ChannelPrivateError:
        logger.error("The channel is private and you don't have access")
        raise
    except Exception as e:
        logger.error(f"Error scraping members: {str(e)}")
        raise
    
    return members

def save_members_to_csv(members: List[Member], filename: str) -> str:
    """Save members list to CSV file"""
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['user_id', 'username', 'access_hash', 'name'])
        writer.writeheader()
        for member in members:
            writer.writerow(member.to_dict())
    return filepath

def load_members_from_csv(filename: str) -> List[Member]:
    """Load members list from CSV file"""
    filepath = os.path.join(DATA_DIR, filename)
    members = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            members.append(Member.from_dict(row))
    return members

async def add_members_to_group(
    client: TelegramClient,
    target_group_entity,
    members: List[Member],
    added_callback=None,
    skip_callback=None,
    error_callback=None
) -> Tuple[int, int, int]:
    """
    Add members to target group
    Returns: (added_count, skipped_count, error_count)
    """
    added = 0
    skipped = 0
    errors = 0
    
    target_peer = await client.get_input_entity(target_group_entity)
    
    for i, member in enumerate(members, 1):
        try:
            if member.username:
                user_entity = await client.get_input_entity(member.username)
            elif member.access_hash:
                user_entity = InputPeerUser(member.user_id, member.access_hash)
            else:
                logger.warning(f"Skipping member {member.user_id} - no username or access hash")
                skipped += 1
                if skip_callback:
                    await skip_callback(member, "No username or access hash")
                continue
            
            await client(functions.channels.InviteToChannelRequest(
                channel=target_peer,
                users=[user_entity]
            ))
            
            added += 1
            logger.info(f"Added {member.username or member.user_id} to group")
            
            if added_callback:
                await added_callback(member)
            
            # Random delay to avoid flood
            if i < len(members):
                delay = random.randint(*DELAY_BETWEEN_ADDS)
                logger.info(f"Waiting {delay} seconds before next add...")
                await asyncio.sleep(delay)
                
        except PeerFloodError:
            logger.error("Flood error detected. Stopping.")
            errors += len(members) - i  # Count remaining as errors
            if error_callback:
                await error_callback(member, "Flood error")
            break
        except UserPrivacyRestrictedError:
            logger.warning(f"Privacy restricted for {member.username or member.user_id}")
            skipped += 1
            if skip_callback:
                await skip_callback(member, "Privacy restricted")
        except UserNotMutualContactError:
            logger.warning(f"User {member.username or member.user_id} is not mutual contact")
            skipped += 1
            if skip_callback:
                await skip_callback(member, "Not mutual contact")
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"Flood wait for {wait_time} seconds")
            if error_callback:
                await error_callback(member, f"Flood wait {wait_time}s")
            await asyncio.sleep(wait_time)
            # Try again after waiting
            continue
        except Exception as e:
            logger.error(f"Error adding {member.username or member.user_id}: {str(e)}")
            errors += 1
            if error_callback:
                await error_callback(member, str(e))
    
    return added, skipped, errors

# --- Keyboards ---
def get_main_keyboard() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="📤 Upload Sessions", callback_data="upload_sessions")
    kb.button(text="📋 List Sessions", callback_data="list_sessions")
    kb.button(text="🔍 Scrape Members", callback_data="scrape_members")
    kb.button(text="➕ Add Members", callback_data="add_members")
    kb.button(text="🔄 Status", callback_data="bot_status")
    kb.adjust(2, 2, 1)
    return kb

def get_cancel_keyboard() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Cancel", callback_data="cancel_operation")
    return kb

# --- Handlers ---
@router.message(Command("start"))
async def cmd_start(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("❌ You are not authorized to use this bot.")
    
    await message.answer(
        "👨‍💻 <b>Telegram Member Management Bot</b>\n\n"
        "Choose an action:",
        reply_markup=get_main_keyboard().as_markup()
    )

@router.callback_query(F.data == "upload_sessions")
async def handle_upload_sessions(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("Unauthorized", show_alert=True)
    
    await callback.message.answer(
        "📤 <b>Upload Session Files</b>\n\n"
        "Please upload your <code>.session</code> files one by one.\n"
        "Each file will be validated automatically.",
        reply_markup=get_cancel_keyboard().as_markup()
    )
    await callback.answer()

@router.callback_query(F.data == "list_sessions")
async def handle_list_sessions(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("Unauthorized", show_alert=True)
    
    sessions = list_sessions()
    if not sessions:
        msg = "No session files uploaded yet."
    else:
        msg = "<b>Available Sessions:</b>\n\n" + "\n".join(
            f"• <code>{s}</code>" for s in sessions
        )
    
    await callback.message.answer(msg, reply_markup=get_main_keyboard().as_markup())
    await callback.answer()

@router.callback_query(F.data == "scrape_members")
async def handle_scrape_members(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return await callback.answer("Unauthorized", show_alert=True)
    
    sessions = list_sessions()
    if not sessions:
        await callback.message.answer(
            "No session files available. Please upload sessions first.",
            reply_markup=get_main_keyboard().as_markup()
        )
        return await callback.answer()
    
    await callback.message.answer(
        "🔍 <b>Scrape Members</b>\n\n"
        "Please send the group/channel username or invite link.\n"
        "Example:\n"
        "<code>@group_username</code>\n"
        "or\n"
        "<code>https://t.me/group_username</code>",
        reply_markup=get_cancel_keyboard().as_markup()
    )
    await state.set_state(BotStates.waiting_for_group_link)
    await callback.answer()

@router.callback_query(F.data == "add_members")
async def handle_add_members(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return await callback.answer("Unauthorized", show_alert=True)
    
    sessions = list_sessions()
    if not sessions:
        await callback.message.answer(
            "No session files available. Please upload sessions first.",
            reply_markup=get_main_keyboard().as_markup()
        )
        return await callback.answer()
    
    await callback.message.answer(
        "➕ <b>Add Members</b>\n\n"
        "Please send the target group/channel username or invite link "
        "where members should be added.\n\n"
        "Example:\n"
        "<code>@target_group</code>\n"
        "or\n"
        "<code>https://t.me/target_group</code>",
        reply_markup=get_cancel_keyboard().as_markup()
    )
    await state.set_state(BotStates.waiting_for_target_group)
    await callback.answer()

@router.callback_query(F.data == "bot_status")
async def handle_bot_status(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("Unauthorized", show_alert=True)
    
    sessions = list_sessions()
    session_status = []
    
    for session in sessions:
        session_path = os.path.join(SESSION_DIR, session)
        is_valid = await validate_telethon_session(session_path)
        status = "✅ Valid" if is_valid else "❌ Invalid"
        session_status.append(f"• <code>{session}</code> - {status}")
    
    status_msg = (
        "<b>🤖 Bot Status</b>\n\n"
        f"<b>Sessions ({len(sessions)}):</b>\n"
        + "\n".join(session_status) + "\n\n"
        f"<b>Data Directory:</b> {len(os.listdir(DATA_DIR))} files\n"
        f"<b>Last Activity:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await callback.message.answer(
        status_msg,
        reply_markup=get_main_keyboard().as_markup()
    )
    await callback.answer()

@router.callback_query(F.data == "cancel_operation")
async def handle_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "Operation cancelled.",
        reply_markup=get_main_keyboard().as_markup()
    )
    await callback.answer()

@router.message(F.document)
async def handle_session_upload(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("❌ Unauthorized.")
    
    doc = message.document
    if not doc.file_name.endswith('.session'):
        return await message.answer(
            "Please upload a valid <code>.session</code> file.",
            reply_markup=get_main_keyboard().as_markup()
        )
    
    try:
        # Save the session file
        saved_path = await save_session_file(doc)
        
        # Validate the session
        is_valid = await validate_telethon_session(saved_path)
        
        if is_valid:
            await message.answer(
                f"✅ <b>{doc.file_name}</b> is valid and saved.",
                reply_markup=get_main_keyboard().as_markup()
            )
        else:
            os.remove(saved_path)
            await message.answer(
                f"❌ <b>{doc.file_name}</b> is invalid or not authorized.",
                reply_markup=get_main_keyboard().as_markup()
            )
    except Exception as e:
        logger.error(f"Error processing session file: {str(e)}")
        await message.answer(
            "❌ An error occurred while processing the session file.",
            reply_markup=get_main_keyboard().as_markup()
        )

@router.message(BotStates.waiting_for_group_link)
async def handle_group_link(message: Message, state: FSMContext):
    group_input = message.text.strip()
    
    try:
        sessions = list_sessions()
        if not sessions:
            await message.answer(
                "No session files available. Please upload sessions first.",
                reply_markup=get_main_keyboard().as_markup()
            )
            await state.clear()
            return
        
        # Use the first available session
        session_file = sessions[0]
        session_path = os.path.join(SESSION_DIR, session_file)
        
        async with TelegramClient(session_path, API_ID, API_HASH) as client:
            # Get the group entity
            group_entity = await get_entity_by_input(client, group_input)
            
            # Start scraping
            await message.answer(
                f"🔍 Scraping members from <code>{group_entity.title}</code>...\n"
                "This may take a while depending on group size."
            )
            
            members = await scrape_members(client, group_entity)
            
            if not members:
                await message.answer(
                    "No members found or couldn't access the group.",
                    reply_markup=get_main_keyboard().as_markup()
                )
                await state.clear()
                return
            
            # Save to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"members_{group_entity.id}_{timestamp}.csv"
            csv_path = save_members_to_csv(members, filename)
            
            # Send the file
            await message.answer_document(
                FSInputFile(csv_path),
                caption=f"✅ Successfully scraped {len(members)} members from <code>{group_entity.title}</code>",
                reply_markup=get_main_keyboard().as_markup()
            )
            
    except ChannelPrivateError:
        await message.answer(
            "❌ The channel/group is private and you don't have access.",
            reply_markup=get_main_keyboard().as_markup()
        )
    except Exception as e:
        logger.error(f"Error scraping members: {str(e)}")
        await message.answer(
            f"❌ Error scraping members: {str(e)}",
            reply_markup=get_main_keyboard().as_markup()
        )
    
    await state.clear()

@router.message(BotStates.waiting_for_target_group)
async def handle_target_group(message: Message, state: FSMContext):
    target_group_input = message.text.strip()
    
    try:
        await state.update_data(target_group=target_group_input)
        await message.answer(
            "📁 Now please upload the CSV file containing members to add.",
            reply_markup=get_cancel_keyboard().as_markup()
        )
        await state.set_state(BotStates.waiting_for_member_file)
    except Exception as e:
        logger.error(f"Error setting target group: {str(e)}")
        await message.answer(
            "❌ Error processing target group.",
            reply_markup=get_main_keyboard().as_markup()
        )
        await state.clear()

@router.message(BotStates.waiting_for_member_file)
async def handle_member_file(message: Message, state: FSMContext):
    if not message.document:
        await message.answer(
            "Please upload a CSV file containing members data.",
            reply_markup=get_cancel_keyboard().as_markup()
        )
        return
    
    doc = message.document
    if not doc.file_name.endswith('.csv'):
        await message.answer(
            "Please upload a valid CSV file.",
            reply_markup=get_cancel_keyboard().as_markup()
        )
        return
    
    try:
        # Save the uploaded file
        file_path = os.path.join(DATA_DIR, doc.file_name)
        await bot.download(doc, destination=file_path)
        
        # Load members from CSV
        members = load_members_from_csv(doc.file_name)
        
        if not members:
            await message.answer(
                "No valid members found in the CSV file.",
                reply_markup=get_main_keyboard().as_markup()
            )
            await state.clear()
            return
        
        # Get target group from state
        data = await state.get_data()
        target_group_input = data.get('target_group')
        
        if not target_group_input:
            await message.answer(
                "Target group not found. Please start over.",
                reply_markup=get_main_keyboard().as_markup()
            )
            await state.clear()
            return
        
        sessions = list_sessions()
        if not sessions:
            await message.answer(
                "No session files available. Please upload sessions first.",
                reply_markup=get_main_keyboard().as_markup()
            )
            await state.clear()
            return
        
        # Use the first available session
        session_file = sessions[0]
        session_path = os.path.join(SESSION_DIR, session_file)
        
        async with TelegramClient(session_path, API_ID, API_HASH) as client:
            # Get the target group entity
            target_group_entity = await get_entity_by_input(client, target_group_input)
            
            # Start adding members
            progress_msg = await message.answer(
                f"➕ Starting to add {len(members)} members to <code>{target_group_entity.title}</code>...\n"
                "This may take a while."
            )
            
            # Callbacks for progress updates
            async def added_callback(member: Member):
                await progress_msg.edit_text(
                    f"✅ Added {member.username or member.user_id}\n"
                    f"Progress: {added_count}/{len(members)}"
                )
            
            async def skip_callback(member: Member, reason: str):
                await progress_msg.edit_text(
                    f"⚠️ Skipped {member.username or member.user_id} ({reason})\n"
                    f"Progress: {added_count}/{len(members)}"
                )
            
            async def error_callback(member: Member, error: str):
                await progress_msg.edit_text(
                    f"❌ Error adding {member.username or member.user_id}: {error}\n"
                    f"Progress: {added_count}/{len(members)}"
                )
            
            # Add members
            added_count, skipped_count, error_count = await add_members_to_group(
                client,
                target_group_entity,
                members,
                added_callback=added_callback,
                skip_callback=skip_callback,
                error_callback=error_callback
            )
            
            # Final report
            await message.answer(
                f"🏁 <b>Add Members Complete</b>\n\n"
                f"<b>Target Group:</b> <code>{target_group_entity.title}</code>\n"
                f"<b>Total Members:</b> {len(members)}\n"
                f"<b>Successfully Added:</b> {added_count}\n"
                f"<b>Skipped:</b> {skipped_count}\n"
                f"<b>Errors:</b> {error_count}",
                reply_markup=get_main_keyboard().as_markup()
            )
            
    except Exception as e:
        logger.error(f"Error adding members: {str(e)}")
        await message.answer(
            f"❌ Error adding members: {str(e)}",
            reply_markup=get_main_keyboard().as_markup()
        )
    
    await state.clear()

# --- Error Handler ---
@router.errors()
async def error_handler(event, **kwargs):
    logger.error(f"Error occurred: {str(event.exception)}")
    
    # Try to notify the user if possible
    try:
        if 'update' in kwargs and hasattr(kwargs['update'], 'message'):
            await kwargs['update'].message.answer(
                "❌ An error occurred. Please check logs.",
                reply_markup=get_main_keyboard().as_markup()
            )
    except Exception as e:
        logger.error(f"Error in error handler: {str(e)}")

# --- Bot Startup ---
async def main():
    logger.info("Starting bot...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())