#!/usr/bin/env python3
"""
Advanced Telegram Member Management Bot (Complete Implementation)
Version: 4.0
Features:
- Session management with validation
- Member scraping with progress tracking
- Member adding with flood control
- CSV import/export
- Admin controls
- Rate limiting
- Error handling
"""

import os
import sys
import asyncio
import logging
import random
import time
import csv
import signal
import atexit
import socket
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any, Union
from enum import Enum, auto

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    FSInputFile,
    Document,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.rate_limiter import RateLimiter, DefaultRateLimiter
from aiogram.utils.chat_action import ChatActionSender

from telethon import TelegramClient, functions, errors
from telethon.tl import types as tl_types
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.types import (
    InputPeerUser,
    InputPeerChannel,
    InputPeerChat,
    User,
    Channel,
    Chat
)
from telethon.errors import (
    PeerFloodError,
    UserPrivacyRestrictedError,
    FloodWaitError,
    ChannelPrivateError,
    UserNotMutualContactError,
    SessionPasswordNeededError
)

# ==================== CONFIGURATION ====================
class Config:
    # Telegram API credentials
    API_ID = 25781839
    API_HASH = "20a3f2f168739259a180dcdd642e196c"
    BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"
    
    # Admin controls
    ADMIN_IDS = [7584086775]
    RESTRICT_MODE = True
    
    # Filesystem
    SESSION_DIR = "sessions"
    DATA_DIR = "data"
    LOG_FILE = "bot.log"
    PID_FILE = "/tmp/telegram_bot.pid"
    
    # Operation limits
    MAX_ADD_PER_SESSION = 80
    DELAY_BETWEEN_ADDS = (30, 60)  # seconds
    SCRAPE_LIMIT = 10000
    MAX_RETRIES = 5
    REQUEST_TIMEOUT = 30
    RATE_LIMIT = 5  # messages per second
    
    # UI Settings
    PROGRESS_UPDATE_INTERVAL = 10  # seconds
    BATCH_SIZE = 50  # members per progress update

# ==================== SETUP ====================
os.makedirs(Config.SESSION_DIR, exist_ok=True)
os.makedirs(Config.DATA_DIR, exist_ok=True)

# ==================== LOGGING ====================
class CustomFormatter(logging.Formatter):
    """Custom log formatter with colors"""
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: grey + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger("TelegramBot")
logger.setLevel(logging.INFO)

# Console handler with colors
console_handler = logging.StreamHandler()
console_handler.setFormatter(CustomFormatter())
logger.addHandler(console_handler)

# File handler
file_handler = logging.FileHandler(os.path.join(Config.DATA_DIR, Config.LOG_FILE))
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
logger.addHandler(file_handler)

# ==================== PID MANAGEMENT ====================
class PidManager:
    """Prevent multiple bot instances"""
    def __init__(self, pidfile):
        self.pidfile = pidfile
        
    def __enter__(self):
        if os.path.exists(self.pidfile):
            with open(self.pidfile, 'r') as f:
                pid = f.read().strip()
            raise RuntimeError(f"Bot already running with PID {pid}")
            
        with open(self.pidfile, 'w') as f:
            f.write(str(os.getpid()))
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            os.remove(self.pidfile)
        except OSError:
            pass

# ==================== STATE MANAGEMENT ====================
class BotState(StatesGroup):
    """FSM states for bot operations"""
    MAIN_MENU = State()
    UPLOAD_SESSION = State()
    LIST_SESSIONS = State()
    SCRAPE_MEMBERS_INPUT = State()
    SCRAPE_MEMBERS_PROGRESS = State()
    ADD_MEMBERS_INPUT_GROUP = State()
    ADD_MEMBERS_INPUT_FILE = State()
    ADD_MEMBERS_PROGRESS = State()
    CONFIRM_ACTION = State()

# ==================== DATA MODELS ====================
class Member:
    """Represents a Telegram member/user"""
    def __init__(
        self,
        user_id: int,
        username: Optional[str] = None,
        access_hash: Optional[int] = None,
        name: Optional[str] = None,
        phone: Optional[str] = None
    ):
        self.user_id = user_id
        self.username = username
        self.access_hash = access_hash
        self.name = name
        self.phone = phone
        
    def to_dict(self) -> Dict:
        """Convert to dictionary for CSV storage"""
        return {
            'user_id': self.user_id,
            'username': self.username,
            'access_hash': self.access_hash,
            'name': self.name,
            'phone': self.phone
        }
        
    @classmethod
    def from_dict(cls, data: Dict) -> 'Member':
        """Create from dictionary"""
        return cls(
            user_id=data.get('user_id'),
            username=data.get('username'),
            access_hash=data.get('access_hash'),
            name=data.get('name'),
            phone=data.get('phone')
        )

class OperationStatus(Enum):
    """Status of ongoing operations"""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()

class OngoingOperation:
    """Tracks ongoing operations"""
    def __init__(self, op_type: str, user_id: int):
        self.op_type = op_type
        self.user_id = user_id
        self.status = OperationStatus.PENDING
        self.start_time = datetime.now()
        self.end_time = None
        self.progress = 0
        self.total = 0
        self.message_id = None
        
    def update_progress(self, current: int, total: int):
        """Update operation progress"""
        self.progress = current
        self.total = total
        if current >= total:
            self.status = OperationStatus.COMPLETED
            self.end_time = datetime.now()
            
    def mark_failed(self):
        """Mark operation as failed"""
        self.status = OperationStatus.FAILED
        self.end_time = datetime.now()
        
    def mark_cancelled(self):
        """Mark operation as cancelled"""
        self.status = OperationStatus.CANCELLED
        self.end_time = datetime.now()

# ==================== BOT SETUP ====================
storage = MemoryStorage()
bot = Bot(
    token=Config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# Rate limiting
rate_limiter = DefaultRateLimiter(
    limit=Config.RATE_LIMIT,
    interval=1.0,
    retry_after=0.5
)
dp.message.middleware(rate_limiter)

# ==================== UTILITIES ====================
def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return not Config.RESTRICT_MODE or user_id in Config.ADMIN_IDS

async def validate_session(session_path: str) -> bool:
    """Validate Telethon session"""
    try:
        async with TelegramClient(
            session_path,
            Config.API_ID,
            Config.API_HASH,
            timeout=Config.REQUEST_TIMEOUT
        ) as client:
            return await client.is_user_authorized()
    except SessionPasswordNeededError:
        logger.warning(f"Session {session_path} requires 2FA password")
        return False
    except Exception as e:
        logger.error(f"Session validation failed: {str(e)}")
        return False

async def get_entity_safe(client: TelegramClient, identifier: str) -> Any:
    """Safely get entity with retries"""
    for attempt in range(Config.MAX_RETRIES):
        try:
            return await client.get_entity(identifier)
        except ValueError as e:
            if "Cannot find any entity" in str(e):
                raise
            logger.warning(f"Retry {attempt + 1} for get_entity")
            await asyncio.sleep(1)
    raise ValueError(f"Failed to resolve entity: {identifier}")

async def save_members_csv(members: List[Member], filename: str) -> str:
    """Save members to CSV with error handling"""
    filepath = os.path.join(Config.DATA_DIR, filename)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=Member(0).to_dict().keys())
            writer.writeheader()
            for member in members:
                writer.writerow(member.to_dict())
        return filepath
    except Exception as e:
        logger.error(f"CSV save failed: {str(e)}")
        raise

async def load_members_csv(filename: str) -> List[Member]:
    """Load members from CSV"""
    filepath = os.path.join(Config.DATA_DIR, filename)
    members = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                members.append(Member.from_dict(row))
        return members
    except Exception as e:
        logger.error(f"CSV load failed: {str(e)}")
        raise

async def get_active_sessions() -> List[str]:
    """Get list of valid session files"""
    sessions = []
    for filename in os.listdir(Config.SESSION_DIR):
        if filename.endswith('.session'):
            session_path = os.path.join(Config.SESSION_DIR, filename)
            if await validate_session(session_path):
                sessions.append(filename)
    return sessions

# ==================== KEYBOARDS ====================
def main_menu_kb() -> InlineKeyboardMarkup:
    """Main menu keyboard"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📤 Upload Session", callback_data="upload_session")
    builder.button(text="📋 List Sessions", callback_data="list_sessions")
    builder.button(text="🔍 Scrape Members", callback_data="scrape_members")
    builder.button(text="➕ Add Members", callback_data="add_members")
    builder.button(text="📊 Stats", callback_data="bot_stats")
    builder.button(text="⚙️ Settings", callback_data="bot_settings")
    builder.adjust(2, 2, 2)
    return builder.as_markup()

def cancel_kb() -> InlineKeyboardMarkup:
    """Cancel operation keyboard"""
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Cancel", callback_data="cancel_op")
    return builder.as_markup()

def confirm_kb() -> InlineKeyboardMarkup:
    """Confirmation keyboard"""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Confirm", callback_data="confirm_yes")
    builder.button(text="❌ Cancel", callback_data="confirm_no")
    return builder.as_markup()

def session_selection_kb(sessions: List[str]) -> InlineKeyboardMarkup:
    """Session selection keyboard"""
    builder = InlineKeyboardBuilder()
    for session in sessions:
        builder.button(text=f"📁 {session}", callback_data=f"select_session:{session}")
    builder.button(text="🔙 Back", callback_data="back_to_main")
    builder.adjust(1)
    return builder.as_markup()

# ==================== HANDLERS ====================
@router.message(CommandStart())
async def cmd_start(message: Message):
    """Handle /start command"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Unauthorized access.")
        return
        
    await message.answer(
        "👨‍💻 <b>Telegram Member Manager</b>\n\n"
        "Choose an action:",
        reply_markup=main_menu_kb()
    )

@router.callback_query(F.data == "upload_session")
async def upload_session(callback: CallbackQuery, state: FSMContext):
    """Handle session upload request"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    await state.set_state(BotState.UPLOAD_SESSION)
    await callback.message.edit_text(
        "📤 <b>Upload Session File</b>\n\n"
        "Please send your <code>.session</code> file.",
        reply_markup=cancel_kb()
    )
    await callback.answer()

@router.message(BotState.UPLOAD_SESSION, F.document)
async def handle_session_upload(message: Message, state: FSMContext):
    """Process uploaded session file"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Unauthorized access.")
        return
        
    if not message.document.file_name.endswith('.session'):
        await message.answer(
            "❌ Invalid file type. Please upload a <code>.session</code> file.",
            reply_markup=cancel_kb()
        )
        return
        
    try:
        # Save the session file
        session_path = await save_session_file(message.document)
        
        # Validate the session
        async with ChatActionSender.upload_document(bot=bot, chat_id=message.chat.id):
            is_valid = await validate_session(session_path)
            
        if is_valid:
            await message.answer(
                f"✅ Session <code>{message.document.file_name}</code> is valid and saved.",
                reply_markup=main_menu_kb()
            )
        else:
            os.remove(session_path)
            await message.answer(
                "❌ Session is invalid or not authorized. File deleted.",
                reply_markup=main_menu_kb()
            )
            
    except Exception as e:
        logger.error(f"Session upload failed: {str(e)}")
        await message.answer(
            "❌ Failed to process session file. Please try again.",
            reply_markup=main_menu_kb()
        )
        
    await state.clear()

@router.callback_query(F.data == "list_sessions")
async def list_sessions_handler(callback: CallbackQuery, state: FSMContext):
    """List available sessions"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    try:
        sessions = await get_active_sessions()
        if not sessions:
            await callback.message.edit_text(
                "No active sessions found. Please upload sessions first.",
                reply_markup=main_menu_kb()
            )
            return
            
        await callback.message.edit_text(
            "📋 <b>Active Sessions</b>\n\n"
            "Select a session to view details:",
            reply_markup=session_selection_kb(sessions)
        )
        await state.set_state(BotState.LIST_SESSIONS)
        
    except Exception as e:
        logger.error(f"Failed to list sessions: {str(e)}")
        await callback.message.edit_text(
            "❌ Failed to list sessions. Please try again.",
            reply_markup=main_menu_kb()
        )
        
    await callback.answer()

@router.callback_query(F.data.startswith("select_session:"), BotState.LIST_SESSIONS)
async def select_session_handler(callback: CallbackQuery, state: FSMContext):
    """Handle session selection"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    session_file = callback.data.split(':')[1]
    session_path = os.path.join(Config.SESSION_DIR, session_file)
    
    try:
        async with TelegramClient(session_path, Config.API_ID, Config.API_HASH) as client:
            me = await client.get_me()
            info = (
                f"📌 <b>Session Info</b>\n\n"
                f"🔹 <b>File:</b> <code>{session_file}</code>\n"
                f"🔹 <b>User:</b> {me.first_name or ''} {me.last_name or ''}\n"
                f"🔹 <b>Username:</b> @{me.username}\n"
                f"🔹 <b>Phone:</b> {me.phone}\n"
                f"🔹 <b>ID:</b> <code>{me.id}</code>"
            )
            
            await callback.message.edit_text(
                info,
                reply_markup=main_menu_kb()
            )
            
    except Exception as e:
        logger.error(f"Failed to get session info: {str(e)}")
        await callback.message.edit_text(
            "❌ Failed to get session info. The session may be invalid.",
            reply_markup=main_menu_kb()
        )
        
    await state.clear()
    await callback.answer()

@router.callback_query(F.data == "scrape_members")
async def scrape_members_handler(callback: CallbackQuery, state: FSMContext):
    """Initiate member scraping"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    sessions = await get_active_sessions()
    if not sessions:
        await callback.message.edit_text(
            "No active sessions found. Please upload sessions first.",
            reply_markup=main_menu_kb()
        )
        return
        
    await callback.message.edit_text(
        "🔍 <b>Scrape Members</b>\n\n"
        "Please send the group/channel username or invite link:",
        reply_markup=cancel_kb()
    )
    await state.set_state(BotState.SCRAPE_MEMBERS_INPUT)
    await callback.answer()

@router.message(BotState.SCRAPE_MEMBERS_INPUT, F.text)
async def scrape_members_input_handler(message: Message, state: FSMContext):
    """Handle group input for scraping"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Unauthorized access.")
        return
        
    group_input = message.text.strip()
    sessions = await get_active_sessions()
    
    if not sessions:
        await message.answer(
            "No active sessions found. Please upload sessions first.",
            reply_markup=main_menu_kb()
        )
        await state.clear()
        return
        
    # Use the first available session
    session_file = sessions[0]
    session_path = os.path.join(Config.SESSION_DIR, session_file)
    
    try:
        async with TelegramClient(session_path, Config.API_ID, Config.API_HASH) as client:
            # Get the group entity
            try:
                group_entity = await get_entity_safe(client, group_input)
            except ValueError:
                await message.answer(
                    "❌ Could not find the group/channel. Please check the link/username.",
                    reply_markup=main_menu_kb()
                )
                await state.clear()
                return
                
            await state.update_data({
                'session_path': session_path,
                'group_entity': group_entity,
                'group_input': group_input
            })
            
            await message.answer(
                f"🔍 <b>Scraping Members</b>\n\n"
                f"Group: <code>{getattr(group_entity, 'title', group_input)}</code>\n"
                f"Session: <code>{session_file}</code>\n\n"
                f"Enter the maximum number of members to scrape (or press Enter for default {Config.SCRAPE_LIMIT}):",
                reply_markup=cancel_kb()
            )
            await state.set_state(BotState.SCRAPE_MEMBERS_PROGRESS)
            
    except Exception as e:
        logger.error(f"Scraping setup failed: {str(e)}")
        await message.answer(
            "❌ Failed to initialize scraping. Please try again.",
            reply_markup=main_menu_kb()
        )
        await state.clear()

@router.message(BotState.SCRAPE_MEMBERS_PROGRESS, F.text)
async def scrape_members_progress_handler(message: Message, state: FSMContext):
    """Handle scraping with progress updates"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Unauthorized access.")
        return
        
    data = await state.get_data()
    session_path = data.get('session_path')
    group_entity = data.get('group_entity')
    group_input = data.get('group_input')
    
    try:
        limit = int(message.text) if message.text.strip().isdigit() else Config.SCRAPE_LIMIT
        limit = min(limit, Config.SCRAPE_LIMIT)
    except ValueError:
        limit = Config.SCRAPE_LIMIT
        
    progress_msg = await message.answer(
        f"🔄 Starting to scrape up to {limit} members from "
        f"<code>{getattr(group_entity, 'title', group_input)}</code>..."
    )
    
    try:
        async with TelegramClient(session_path, Config.API_ID, Config.API_HASH) as client:
            members = []
            last_update = time.time()
            
            async for i, user in enumerate(client.iter_participants(group_entity, aggressive=True, limit=limit)):
                # Handle names safely
                first_name = getattr(user, 'first_name', '') or ''
                last_name = getattr(user, 'last_name', '') or ''
                full_name = f"{first_name} {last_name}".strip()
                
                member = Member(
                    user_id=user.id,
                    username=user.username,
                    access_hash=getattr(user, 'access_hash', None),
                    name=full_name,
                    phone=getattr(user, 'phone', None)
                )
                members.append(member)
                
                # Update progress periodically
                if (i % Config.BATCH_SIZE == 0) or (time.time() - last_update > Config.PROGRESS_UPDATE_INTERVAL):
                    await progress_msg.edit_text(
                        f"🔍 Scraping members...\n\n"
                        f"Progress: {len(members)}/{limit}\n"
                        f"Group: <code>{getattr(group_entity, 'title', group_input)}</code>"
                    )
                    last_update = time.time()
                    
            # Save results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"members_{getattr(group_entity, 'id', 'unknown')}_{timestamp}.csv"
            csv_path = await save_members_csv(members, filename)
            
            await message.answer_document(
                FSInputFile(csv_path),
                caption=f"✅ Successfully scraped {len(members)} members from "
                       f"<code>{getattr(group_entity, 'title', group_input)}</code>",
                reply_markup=main_menu_kb()
            )
            
    except ChannelPrivateError:
        await message.answer(
            "❌ The channel/group is private and you don't have access.",
            reply_markup=main_menu_kb()
        )
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        await message.answer(
            f"❌ Scraping failed: {str(e)}",
            reply_markup=main_menu_kb()
        )
    finally:
        await state.clear()

@router.callback_query(F.data == "add_members")
async def add_members_handler(callback: CallbackQuery, state: FSMContext):
    """Initiate member adding process"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    sessions = await get_active_sessions()
    if not sessions:
        await callback.message.edit_text(
            "No active sessions found. Please upload sessions first.",
            reply_markup=main_menu_kb()
        )
        return
        
    await callback.message.edit_text(
        "➕ <b>Add Members</b>\n\n"
        "Please send the target group/channel username or invite link "
        "where members should be added:",
        reply_markup=cancel_kb()
    )
    await state.set_state(BotState.ADD_MEMBERS_INPUT_GROUP)
    await callback.answer()

@router.message(BotState.ADD_MEMBERS_INPUT_GROUP, F.text)
async def add_members_group_handler(message: Message, state: FSMContext):
    """Handle target group input for member adding"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Unauthorized access.")
        return
        
    target_group = message.text.strip()
    sessions = await get_active_sessions()
    
    if not sessions:
        await message.answer(
            "No active sessions found. Please upload sessions first.",
            reply_markup=main_menu_kb()
        )
        await state.clear()
        return
        
    await state.update_data({'target_group': target_group})
    
    await message.answer(
        "📁 Please upload the CSV file containing members to add:",
        reply_markup=cancel_kb()
    )
    await state.set_state(BotState.ADD_MEMBERS_INPUT_FILE)

@router.message(BotState.ADD_MEMBERS_INPUT_FILE, F.document)
async def add_members_file_handler(message: Message, state: FSMContext):
    """Handle member file upload for adding"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Unauthorized access.")
        return
        
    if not message.document.file_name.endswith('.csv'):
        await message.answer(
            "❌ Invalid file type. Please upload a CSV file.",
            reply_markup=cancel_kb()
        )
        return
        
    data = await state.get_data()
    target_group = data.get('target_group')
    
    try:
        # Save the uploaded file
        file_path = os.path.join(Config.DATA_DIR, message.document.file_name)
        await bot.download(message.document, destination=file_path)
        
        # Load members
        members = await load_members_csv(message.document.file_name)
        
        if not members:
            await message.answer(
                "❌ No valid members found in the CSV file.",
                reply_markup=main_menu_kb()
            )
            await state.clear()
            return
            
        await state.update_data({
            'members_file': file_path,
            'members_count': len(members)
        })
        
        sessions = await get_active_sessions()
        await message.answer(
            f"➕ <b>Add Members</b>\n\n"
            f"🔹 Target: <code>{target_group}</code>\n"
            f"🔹 Members: {len(members)}\n\n"
            f"Select a session to use:",
            reply_markup=session_selection_kb(sessions)
        )
        await state.set_state(BotState.ADD_MEMBERS_PROGRESS)
        
    except Exception as e:
        logger.error(f"Member file processing failed: {str(e)}")
        await message.answer(
            "❌ Failed to process member file. Please try again.",
            reply_markup=main_menu_kb()
        )
        await state.clear()

@router.callback_query(F.data.startswith("select_session:"), BotState.ADD_MEMBERS_PROGRESS)
async def add_members_session_handler(callback: CallbackQuery, state: FSMContext):
    """Handle session selection for member adding"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    session_file = callback.data.split(':')[1]
    session_path = os.path.join(Config.SESSION_DIR, session_file)
    data = await state.get_data()
    
    target_group = data.get('target_group')
    members_file = data.get('members_file')
    members_count = data.get('members_count')
    
    progress_msg = await callback.message.edit_text(
        f"🔄 Preparing to add {members_count} members to "
        f"<code>{target_group}</code> using session <code>{session_file}</code>..."
    )
    
    try:
        members = await load_members_csv(os.path.basename(members_file))
        
        async with TelegramClient(session_path, Config.API_ID, Config.API_HASH) as client:
            # Get target group entity
            try:
                target_entity = await get_entity_safe(client, target_group)
            except ValueError:
                await callback.message.answer(
                    "❌ Could not find the target group. Please check the link/username.",
                    reply_markup=main_menu_kb()
                )
                await state.clear()
                return
                
            # Start adding members
            added = 0
            skipped = 0
            errors = 0
            last_update = time.time()
            
            for i, member in enumerate(members, 1):
                try:
                    if member.username:
                        user_entity = await client.get_input_entity(member.username)
                    elif member.access_hash:
                        user_entity = InputPeerUser(member.user_id, member.access_hash)
                    else:
                        skipped += 1
                        continue
                        
                    await client(InviteToChannelRequest(
                        channel=await client.get_input_entity(target_entity),
                        users=[user_entity]
                    ))
                    
                    added += 1
                    
                    # Update progress periodically
                    if (i % 10 == 0) or (time.time() - last_update > Config.PROGRESS_UPDATE_INTERVAL):
                        await progress_msg.edit_text(
                            f"➕ Adding members...\n\n"
                            f"🔹 Target: <code>{getattr(target_entity, 'title', target_group)}</code>\n"
                            f"🔹 Progress: {i}/{len(members)}\n"
                            f"✅ Added: {added}\n"
                            f"⚠️ Skipped: {skipped}\n"
                            f"❌ Errors: {errors}"
                        )
                        last_update = time.time()
                        
                    # Random delay to avoid flood
                    if i < len(members):
                        delay = random.randint(*Config.DELAY_BETWEEN_ADDS)
                        await asyncio.sleep(delay)
                        
                except PeerFloodError:
                    errors += len(members) - i
                    await progress_msg.edit_text(
                        f"⚠️ Flood error detected!\n\n"
                        f"✅ Added: {added}\n"
                        f"⚠️ Skipped: {skipped}\n"
                        f"❌ Errors: {errors}\n\n"
                        f"Stopping to prevent account restrictions."
                    )
                    break
                except (UserPrivacyRestrictedError, UserNotMutualContactError):
                    skipped += 1
                except FloodWaitError as e:
                    errors += 1
                    wait_time = e.seconds
                    await progress_msg.edit_text(
                        f"⏳ Waiting {wait_time} seconds due to flood limit..."
                    )
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    errors += 1
                    logger.error(f"Error adding member: {str(e)}")
                    
            # Final report
            await callback.message.answer(
                f"🏁 <b>Operation Complete</b>\n\n"
                f"🔹 Target: <code>{getattr(target_entity, 'title', target_group)}</code>\n"
                f"🔹 Total: {len(members)}\n"
                f"✅ Added: {added}\n"
                f"⚠️ Skipped: {skipped}\n"
                f"❌ Errors: {errors}",
                reply_markup=main_menu_kb()
            )
            
    except Exception as e:
        logger.error(f"Member adding failed: {str(e)}")
        await callback.message.answer(
            f"❌ Failed to add members: {str(e)}",
            reply_markup=main_menu_kb()
        )
    finally:
        await state.clear()
        await callback.answer()

@router.callback_query(F.data == "cancel_op")
async def cancel_operation_handler(callback: CallbackQuery, state: FSMContext):
    """Cancel current operation"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    await callback.message.edit_text(
        "❌ Operation cancelled.",
        reply_markup=main_menu_kb()
    )
    await state.clear()
    await callback.answer()

@router.callback_query(F.data == "bot_stats")
async def bot_stats_handler(callback: CallbackQuery):
    """Show bot statistics"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    sessions = await get_active_sessions()
    data_files = [f for f in os.listdir(Config.DATA_DIR) if f.endswith('.csv')]
    
    stats = (
        f"📊 <b>Bot Statistics</b>\n\n"
        f"🔹 Active sessions: {len(sessions)}\n"
        f"🔹 Data files: {len(data_files)}\n"
        f"🔹 Last activity: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await callback.message.edit_text(
        stats,
        reply_markup=main_menu_kb()
    )
    await callback.answer()

@router.callback_query(F.data == "bot_settings")
async def bot_settings_handler(callback: CallbackQuery):
    """Show bot settings"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
        
    settings = (
        f"⚙️ <b>Bot Settings</b>\n\n"
        f"🔹 Max adds per session: {Config.MAX_ADD_PER_SESSION}\n"
        f"🔹 Delay between adds: {Config.DELAY_BETWEEN_ADDS[0]}-{Config.DELAY_BETWEEN_ADDS[1]}s\n"
        f"🔹 Scrape limit: {Config.SCRAPE_LIMIT}\n"
        f"🔹 Rate limit: {Config.RATE_LIMIT} msg/s\n"
        f"🔹 Admin mode: {'🔒 Restricted' if Config.RESTRICT_MODE else '🔓 Open'}"
    )
    
    await callback.message.edit_text(
        settings,
        reply_markup=main_menu_kb()
    )
    await callback.answer()

@router.callback_query(F.data == "back_to_main")
async def back_to_main_handler(callback: CallbackQuery, state: FSMContext):
    """Return to main menu"""
    await callback.message.edit_text(
        "👨‍💻 <b>Telegram Member Manager</b>\n\n"
        "Choose an action:",
        reply_markup=main_menu_kb()
    )
    await state.clear()
    await callback.answer()

# ==================== ERROR HANDLING ====================
@router.errors()
async def error_handler(event, **kwargs):
    """Global error handler"""
    logger.error(f"Error occurred: {str(event.exception)}", exc_info=True)
    
    # Try to notify the user
    try:
        if 'update' in kwargs and hasattr(kwargs['update'], 'message'):
            await kwargs['update'].message.answer(
                "❌ An error occurred. Please check logs.",
                reply_markup=main_menu_kb()
            )
    except Exception as e:
        logger.error(f"Error in error handler: {str(e)}")

# ==================== SHUTDOWN HANDLING ====================
async def shutdown(signal, loop):
    """Cleanup tasks on shutdown"""
    logger.info(f"Received {signal.name}, shutting down...")
    await bot.close()
    await dp.storage.close()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

def cleanup():
    """Cleanup resources"""
    try:
        os.remove(Config.PID_FILE)
    except OSError:
        pass
    logger.info("Cleanup complete.")

# ==================== MAIN ====================
async def main():
    """Main application entry point"""
    try:
        with PidManager(Config.PID_FILE):
            # Register cleanup handlers
            atexit.register(cleanup)
            loop = asyncio.get_running_loop()
            
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(shutdown(s, loop))
            
            logger.info("Starting bot...")
            await dp.start_polling(bot)
            
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Bot stopped")

if __name__ == "__main__":
    asyncio.run(main())