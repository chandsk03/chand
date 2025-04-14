import os
import asyncio
import logging
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.strategy import FSMStrategy
from aiogram.utils.callback_answer import CallbackAnswerMiddleware
from telethon import TelegramClient
from telethon.errors import PhoneNumberBannedError, UserDeactivatedBanError, FloodWaitError, UserBannedInChannelError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

# ============== CONFIG ==============
API_ID = 25781839
API_HASH = "20a3f2f168739259a180dcdd642e196c"
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"
ADMIN_IDS = [7584086775]
SESSION_FOLDER = 'sessions'
DB_FILE = 'scraped_users.db'
# =====================================

bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

SOURCE_GROUPS = set()
TARGET_GROUPS = set()
VALID_SESSIONS = []
DEAD_SESSIONS = []

# ============ MIDDLEWARE: ADMIN ONLY ============
@dp.update.outer_middleware()
async def admin_only(update: types.Update, call_next):
    user_id = update.message.from_user.id if update.message else update.callback_query.from_user.id
    if user_id not in ADMIN_IDS:
        if update.message:
            await update.message.answer("This is a private bot.")
        elif update.callback_query:
            await update.callback_query.message.edit_text("This is a private bot.")
        return
    return await call_next(update)

# ============ DATABASE ============
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
    for f in DEAD_SESSIONS:
        try:
            os.remove(os.path.join(SESSION_FOLDER, f))
        except:
            pass
        try:
            os.remove(os.path.join(SESSION_FOLDER, f + ".session-journal"))
        except:
            pass
    return len(DEAD_SESSIONS)

# ============ SCRAPER ============
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
                            await db.execute("INSERT INTO users VALUES (?, ?, ?, ?, datetime('now'))",
                                (user.id, user.username, group.id, group.title))
                            await db.commit()
        except Exception as e:
            print(f"Scraper error in {name}: {e}")

# ============ AUTO JOIN ============
async def auto_join():
    for name, client in VALID_SESSIONS:
        try:
            for group in TARGET_GROUPS:
                try:
                    await client(JoinChannelRequest(group))
                    await asyncio.sleep(2)
                except (FloodWaitError, UserBannedInChannelError):
                    pass
        except Exception as e:
            print(f"Join error in {name}: {e}")

# ============ UI ============
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Start Scraping", callback_data="start_scraping"),
         InlineKeyboardButton(text="Start Auto-Join", callback_data="start_join")],
        [InlineKeyboardButton(text="Session Stats", callback_data="stats"),
         InlineKeyboardButton(text="Clean Dead Sessions", callback_data="clean_sessions")],
        [InlineKeyboardButton(text="How to Add Sessions", callback_data="add_sessions_help")]
    ])

# ============ HANDLERS ============
@dp.message(CommandStart())
async def cmd_start(msg: types.Message):
    await msg.answer("Welcome to your private scraper bot!", reply_markup=main_menu())

@dp.callback_query(F.data == "start_scraping")
async def handle_scrape(call: types.CallbackQuery):
    await call.message.edit_text("Scraping started...")
    await load_sessions()
    await scrape_users()
    await call.message.edit_text("Scraping finished.", reply_markup=main_menu())

@dp.callback_query(F.data == "start_join")
async def handle_join(call: types.CallbackQuery):
    await call.message.edit_text("Joining groups...")
    await load_sessions()
    await auto_join()
    await call.message.edit_text("All done joining.", reply_markup=main_menu())

@dp.callback_query(F.data == "stats")
async def handle_stats(call: types.CallbackQuery):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            total = (await cur.fetchone())[0]
    await call.message.edit_text(
        f"✅ Valid Sessions: {len(VALID_SESSIONS)}\n"
        f"❌ Dead Sessions: {len(DEAD_SESSIONS)}\n"
        f"👥 Scraped Users: {total}", reply_markup=main_menu()
    )

@dp.callback_query(F.data == "clean_sessions")
async def handle_clean(call: types.CallbackQuery):
    removed = remove_dead_sessions()
    await call.message.edit_text(f"Removed {removed} dead sessions.", reply_markup=main_menu())

@dp.callback_query(F.data == "add_sessions_help")
async def handle_add_sessions(call: types.CallbackQuery):
    await call.message.edit_text(
        "To add sessions:\n"
        "1. Use a Telethon login script to log in accounts.\n"
        "2. Place all `.session` files into the `sessions/` folder.\n"
        "3. Restart the bot or use the buttons.",
        reply_markup=main_menu()
    )

@dp.message(Command("add_source"))
async def cmd_add_source(msg: types.Message):
    try:
        gid = int(msg.text.split(maxsplit=1)[1])
        SOURCE_GROUPS.add(gid)
        await msg.reply(f"Added source group ID: {gid}")
    except:
        await msg.reply("Usage: /add_source <group_id>")

@dp.message(Command("add_target"))
async def cmd_add_target(msg: types.Message):
    try:
        username = msg.text.split(maxsplit=1)[1]
        TARGET_GROUPS.add(username)
        await msg.reply(f"Added target group: {username}")
    except:
        await msg.reply("Usage: /add_target <@group_username>")

# ============ STARTUP ============
async def main():
    logging.basicConfig(level=logging.INFO)
    os.makedirs(SESSION_FOLDER, exist_ok=True)
    await init_db()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())