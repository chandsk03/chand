import os
import asyncio
import logging
import aiosqlite
from datetime import datetime
from typing import Callable, Awaitable, Dict, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram import BaseMiddleware

from telethon import TelegramClient
from telethon.errors import (
    PhoneNumberBannedError, UserDeactivatedBanError,
    FloodWaitError, UserBannedInChannelError
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

# ================== CONFIG ==================
API_ID = 25781839
API_HASH = "20a3f2f168739259a180dcdd642e196c"
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"
ADMIN_IDS = [7584086775]

SESSION_FOLDER = 'sessions'
DB_FILE = 'scraped_users.db'
# ============================================

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

SOURCE_GROUPS = set()
TARGET_GROUPS = set()
VALID_SESSIONS = []
DEAD_SESSIONS = []

# ================== MIDDLEWARE ==================
class AdminOnlyMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[types.Update, Dict[str, Any]], Awaitable[Any]],
        event: types.Update,
        data: Dict[str, Any]
    ) -> Any:
        user = event.message.from_user if event.message else event.callback_query.from_user
        if user.id not in ADMIN_IDS:
            text = "This is a private bot."
            if event.message:
                await event.message.answer(text)
            elif event.callback_query:
                await event.callback_query.message.edit_text(text)
            return
        return await handler(event, data)

dp.update.outer_middleware(AdminOnlyMiddleware())

# ================== DATABASE ==================
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER,
            username TEXT,
            group_id INTEGER,
            group_name TEXT,
            date_scraped TEXT
        )''')
        await db.commit()

# ================== SESSION MANAGEMENT ==================
async def load_sessions():
    global VALID_SESSIONS, DEAD_SESSIONS
    VALID_SESSIONS.clear()
    DEAD_SESSIONS.clear()

    for file in os.listdir(SESSION_FOLDER):
        if file.endswith(".session"):
            path = os.path.join(SESSION_FOLDER, file)
            client = TelegramClient(path, API_ID, API_HASH)
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    raise Exception("Not logged in")
                await client.get_me()
                VALID_SESSIONS.append((file, client))
            except (PhoneNumberBannedError, UserDeactivatedBanError, Exception):
                DEAD_SESSIONS.append(file)

def remove_dead_sessions():
    removed = 0
    for f in DEAD_SESSIONS:
        try:
            os.remove(os.path.join(SESSION_FOLDER, f))
            os.remove(os.path.join(SESSION_FOLDER, f + ".session-journal"))
            removed += 1
        except:
            continue
    return removed

# ================== SCRAPER ==================
async def scrape_users():
    await init_db()
    for name, client in VALID_SESSIONS:
        try:
            dialogs = await client(GetDialogsRequest(
                offset_date=None, offset_id=0, offset_peer=InputPeerEmpty(),
                limit=100, hash=0
            ))
            for group in dialogs.chats:
                if group.id not in SOURCE_GROUPS:
                    continue
                async for user in client.iter_participants(group):
                    if user.username:
                        async with aiosqlite.connect(DB_FILE) as db:
                            await db.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)",
                                (user.id, user.username, group.id, group.title, datetime.utcnow().isoformat()))
                            await db.commit()
        except Exception as e:
            print(f"[SCRAPER ERROR] {name}: {e}")

# ================== AUTO JOIN ==================
async def auto_join():
    for name, client in VALID_SESSIONS:
        try:
            for group in TARGET_GROUPS:
                try:
                    await client(JoinChannelRequest(group))
                    await asyncio.sleep(2)
                except (FloodWaitError, UserBannedInChannelError):
                    continue
        except Exception as e:
            print(f"[JOIN ERROR] {name}: {e}")

# ================== INLINE KEYBOARD ==================
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Start Scraping", callback_data="start_scraping"),
         InlineKeyboardButton(text="Start Auto-Join", callback_data="start_join")],
        [InlineKeyboardButton(text="Session Stats", callback_data="stats"),
         InlineKeyboardButton(text="Clean Dead Sessions", callback_data="clean_sessions")],
        [InlineKeyboardButton(text="How to Add Sessions", callback_data="add_sessions_help")]
    ])

# ================== HANDLERS ==================
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    await msg.answer("Welcome to your private Telegram Member Scraper Bot!", reply_markup=main_menu())

@dp.callback_query(F.data == "start_scraping")
async def start_scraping(call: types.CallbackQuery):
    await call.message.edit_text("Starting to scrape users...")
    await load_sessions()
    await scrape_users()
    await call.message.edit_text("Scraping finished.", reply_markup=main_menu())

@dp.callback_query(F.data == "start_join")
async def start_join(call: types.CallbackQuery):
    await call.message.edit_text("Joining groups...")
    await load_sessions()
    await auto_join()
    await call.message.edit_text("All sessions finished joining.", reply_markup=main_menu())

@dp.callback_query(F.data == "stats")
async def show_stats(call: types.CallbackQuery):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            count = (await cur.fetchone())[0]
    await call.message.edit_text(
        f"✅ Valid Sessions: {len(VALID_SESSIONS)}\n"
        f"❌ Dead Sessions: {len(DEAD_SESSIONS)}\n"
        f"📦 Scraped Users: {count}",
        reply_markup=main_menu()
    )

@dp.callback_query(F.data == "clean_sessions")
async def clean_sessions(call: types.CallbackQuery):
    removed = remove_dead_sessions()
    await call.message.edit_text(f"Removed {removed} dead sessions.", reply_markup=main_menu())

@dp.callback_query(F.data == "add_sessions_help")
async def add_sessions_help(call: types.CallbackQuery):
    await call.message.edit_text(
        "To add sessions:\n"
        "1. Use a Telethon login script to log in accounts.\n"
        "2. Place `.session` files in the `sessions/` folder.\n"
        "3. Use inline buttons to start scraping or auto-join.",
        reply_markup=main_menu()
    )

@dp.message(Command("add_source"))
async def add_source_group(msg: types.Message):
    try:
        gid = int(msg.text.split(maxsplit=1)[1])
        SOURCE_GROUPS.add(gid)
        await msg.reply(f"Added source group ID: {gid}")
    except:
        await msg.reply("Usage: /add_source <group_id>")

@dp.message(Command("add_target"))
async def add_target_group(msg: types.Message):
    try:
        username = msg.text.split(maxsplit=1)[1]
        TARGET_GROUPS.add(username)
        await msg.reply(f"Added target group: {username}")
    except:
        await msg.reply("Usage: /add_target <@group_username>")

# ================== MAIN ==================
async def main():
    logging.basicConfig(level=logging.INFO)
    os.makedirs(SESSION_FOLDER, exist_ok=True)
    await init_db()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
