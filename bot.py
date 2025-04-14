import os
import asyncio
import logging
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError, UserBannedInChannelError
from telethon.errors.rpcerrorlist import PhoneNumberBannedError, UserDeactivatedBanError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

# ======================== CONFIG ========================
API_ID = 123456  # Replace with your API ID
API_HASH = 'your_api_hash'  # Replace with your API HASH
BOT_TOKEN = 'your_bot_token_here'
SESSION_FOLDER = 'sessions'
DB_FILE = 'scraped_users.db'
# ========================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

SOURCE_GROUPS = set()
TARGET_GROUPS = set()
VALID_SESSIONS = []
DEAD_SESSIONS = []

# ============ DATABASE SETUP ============
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

# ============ SESSION LOADER ============
async def load_sessions():
    global VALID_SESSIONS, DEAD_SESSIONS
    VALID_SESSIONS = []
    DEAD_SESSIONS = []

    files = os.listdir(SESSION_FOLDER)
    for file in files:
        if file.endswith(".session"):
            session_path = os.path.join(SESSION_FOLDER, file)
            client = TelegramClient(session_path, API_ID, API_HASH)
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    raise Exception("Not authorized")
                await client.get_me()
                VALID_SESSIONS.append((file, client))
            except (PhoneNumberBannedError, UserDeactivatedBanError, Exception):
                DEAD_SESSIONS.append(file)
    print(f"[+] Loaded {len(VALID_SESSIONS)} valid sessions. Found {len(DEAD_SESSIONS)} dead.")

# ============ DEAD SESSION REMOVER ============
def remove_dead_sessions():
    for dead_file in DEAD_SESSIONS:
        base = os.path.join(SESSION_FOLDER, dead_file)
        for ext in ["", ".session-journal"]:
            try:
                os.remove(base + ext)
            except FileNotFoundError:
                pass
    return len(DEAD_SESSIONS)

# ============ SCRAPER ============
async def scrape_users():
    await init_db()
    for session_name, client in VALID_SESSIONS:
        try:
            dialogs = await client(GetDialogsRequest(
                offset_date=None,
                offset_id=0,
                offset_peer=InputPeerEmpty(),
                limit=100,
                hash=0
            ))
            for group in dialogs.chats:
                if group.id not in SOURCE_GROUPS:
                    continue

                async for user in client.iter_participants(group):
                    if user.username:
                        async with aiosqlite.connect(DB_FILE) as db:
                            await db.execute("INSERT INTO users VALUES (?, ?, ?, ?, datetime('now'))",
                                             (user.id, user.username, group.id, group.title))
                            await db.commit()
        except Exception as e:
            print(f"[!] Error in {session_name}: {e}")

# ============ AUTO JOIN ============
async def auto_join():
    for session_name, client in VALID_SESSIONS:
        try:
            for group in TARGET_GROUPS:
                try:
                    await client(JoinChannelRequest(group))
                    await asyncio.sleep(2)
                except UserBannedInChannelError:
                    print(f"[!] Banned from {group}")
                except FloodWaitError as e:
                    print(f"[!] Flood wait {e.seconds} sec")
                    await asyncio.sleep(e.seconds)
        except Exception as e:
            print(f"[!] Join failed for {session_name}: {e}")

# ============ INLINE BUTTONS ============
def main_menu():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("Start Scraping", callback_data="start_scraping"),
        InlineKeyboardButton("Start Auto-Join", callback_data="start_join")
    )
    keyboard.add(
        InlineKeyboardButton("Session Stats", callback_data="stats"),
        InlineKeyboardButton("Clean Dead Sessions", callback_data="clean_sessions")
    )
    keyboard.add(
        InlineKeyboardButton("How to Add Sessions", callback_data="add_sessions_help")
    )
    return keyboard

# ============ BOT HANDLERS ============
@dp.message_handler(commands=["start"])
async def start_cmd(msg: types.Message):
    await msg.answer("Telegram Scraper Bot", reply_markup=main_menu())

@dp.callback_query_handler(lambda c: c.data == "start_scraping")
async def cb_scrape(call: types.CallbackQuery):
    await call.message.edit_text("Scraping started...")
    await load_sessions()
    await scrape_users()
    await call.message.edit_text("Scraping complete.", reply_markup=main_menu())

@dp.callback_query_handler(lambda c: c.data == "start_join")
async def cb_join(call: types.CallbackQuery):
    await call.message.edit_text("Joining groups...")
    await load_sessions()
    await auto_join()
    await call.message.edit_text("All sessions attempted to join target groups.", reply_markup=main_menu())

@dp.callback_query_handler(lambda c: c.data == "stats")
async def cb_stats(call: types.CallbackQuery):
    count = len(VALID_SESSIONS)
    dead = len(DEAD_SESSIONS)
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            total = (await cursor.fetchone())[0]
    await call.message.edit_text(
        f"Valid Sessions: {count}\nDead Sessions: {dead}\nScraped Users: {total}",
        reply_markup=main_menu()
    )

@dp.callback_query_handler(lambda c: c.data == "clean_sessions")
async def cb_clean(call: types.CallbackQuery):
    removed = remove_dead_sessions()
    await call.message.edit_text(f"Removed {removed} dead sessions.", reply_markup=main_menu())

@dp.callback_query_handler(lambda c: c.data == "add_sessions_help")
async def cb_add_sessions(call: types.CallbackQuery):
    await call.message.edit_text(
        "To add sessions:\n1. Use Telethon login script to generate user .session files.\n"
        "2. Place all .session files in the `sessions/` folder.\n"
        "3. Restart the bot or run a command.",
        reply_markup=main_menu()
    )

@dp.message_handler(commands=["add_source"])
async def add_source(msg: types.Message):
    try:
        gid = int(msg.get_args())
        SOURCE_GROUPS.add(gid)
        await msg.answer(f"Added source group: {gid}")
    except:
        await msg.answer("Usage: /add_source <group_id>")

@dp.message_handler(commands=["add_target"])
async def add_target(msg: types.Message):
    try:
        group = msg.get_args()
        TARGET_GROUPS.add(group)
        await msg.answer(f"Added target group: {group}")
    except:
        await msg.answer("Usage: /add_target <group_username>")

# ============ ENTRY POINT ============
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    os.makedirs(SESSION_FOLDER, exist_ok=True)
    asyncio.run(init_db())
    executor.start_polling(dp, skip_updates=True)
