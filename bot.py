import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from telethon import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty, InputPeerChannel, InputPeerUser
from telethon.errors.rpcerrorlist import PeerFloodError, UserPrivacyRestrictedError
import csv
import random
import time

# --- Configuration ---
API_ID = 25781839
API_HASH = "20a3f2f168739259a180dcdd642e196c"
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"
ADMIN_IDS = [7584086775]
SESSION_DIR = "sessions"

# --- Logging Setup ---
os.makedirs(SESSION_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Aiogram Setup ---
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- Utilities ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def save_session_file(file: types.Document) -> str:
    file_path = os.path.join(SESSION_DIR, file.file_name)
    await file.download(destination=file_path)
    return file_path

async def validate_telethon_session(session_path: str) -> bool:
    try:
        session_name = os.path.splitext(os.path.basename(session_path))[0]
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()
        authorized = await client.is_user_authorized()
        await client.disconnect()
        return authorized
    except Exception as e:
        logging.error(f"Validation failed for {session_path}: {e}")
        return False

def list_sessions() -> list[str]:
    return [f for f in os.listdir(SESSION_DIR) if f.endswith(".session")]

async def scrape_members(client, group) -> list:
    members = []
    async for user in client.get_participants(group):
        members.append(user)
    return members

async def add_members_to_group(client, target_group, users, mode=1):
    target_group_entity = InputPeerChannel(target_group.id, target_group.access_hash)
    n = 0
    for user in users:
        try:
            if mode == 1 and user.username:
                user_to_add = await client.get_input_entity(user.username)
            elif mode == 2:
                user_to_add = InputPeerUser(user.id, user.access_hash)
            else:
                continue
            await client(InviteToChannelRequest(target_group_entity, [user_to_add]))
            n += 1
            logging.info(f"Added {user.username if user.username else user.id}")
            if n % 80 == 0:  # Wait to avoid flood
                time.sleep(random.randint(60, 180))
        except PeerFloodError:
            logging.warning(f"Flood error while adding {user.username if user.username else user.id}. Stopping.")
            break
        except UserPrivacyRestrictedError:
            logging.warning(f"Privacy restrictions for {user.username if user.username else user.id}. Skipping.")
            continue
        except Exception as e:
            logging.error(f"Error adding {user.username if user.username else user.id}: {e}")
            continue

# --- Commands ---
@router.message(F.text == "/start")
async def cmd_start(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("❌ You are not authorized to use this bot.")
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="Upload Sessions", callback_data="upload_sessions")
    keyboard.button(text="List Sessions", callback_data="list_sessions")
    keyboard.button(text="Scrape Members", callback_data="scrape_members")
    keyboard.button(text="Add Members", callback_data="add_members")
    await message.answer("Welcome, Admin! Choose an action:", reply_markup=keyboard.as_markup())

@router.callback_query(F.data == "upload_sessions")
async def prompt_upload_sessions(callback: CallbackQuery):
    await callback.message.answer("Please upload `.session` files (one by one).")
    await callback.answer()

@router.callback_query(F.data == "list_sessions")
async def list_uploaded_sessions(callback: CallbackQuery):
    sessions = list_sessions()
    if not sessions:
        await callback.message.answer("No sessions uploaded yet.")
    else:
        msg = "<b>Uploaded Sessions:</b>\n" + "\n".join([f"• <code>{s}</code>" for s in sessions])
        await callback.message.answer(msg)
    await callback.answer()

@router.callback_query(F.data == "scrape_members")
async def scrape_members_callback(callback: CallbackQuery):
    await callback.message.answer("Please upload a session file to start scraping members.")
    await callback.answer()

@router.callback_query(F.data == "add_members")
async def add_members_callback(callback: CallbackQuery):
    await callback.message.answer("Please upload a session file and CSV of users to add.")
    await callback.answer()

@router.message(F.document)
async def handle_document_upload(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("❌ Unauthorized.")
    
    doc = message.document
    if not doc.file_name.endswith(".session"):
        return await message.answer("Please upload a valid <b>.session</b> file.")
    
    try:
        saved_path = await save_session_file(doc)
        is_valid = await validate_telethon_session(saved_path)
        if is_valid:
            await message.answer(f"✅ <b>{doc.file_name}</b> is valid and saved.")
        else:
            os.remove(saved_path)
            await message.answer(f"❌ <b>{doc.file_name}</b> is invalid or unauthorized.")
    except Exception as e:
        logging.exception("Failed to handle document upload:")
        await message.answer("❌ An error occurred while processing your file.")

@router.message(F.text == "/scrape_members")
async def cmd_scrape_members(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("❌ Unauthorized.")
    
    # You can replace this part with a more complex group selection if needed
    await message.answer("Please provide a group link or username to scrape members.")
    
@router.message(F.text)
async def scrape_members_handler(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("❌ Unauthorized.")
    
    group = message.text
    try:
        # Get group
        client = TelegramClient('temp', API_ID, API_HASH)
        await client.start()
        group_entity = await client.get_entity(group)
        
        # Scrape members
        members = await scrape_members(client, group_entity)
        await client.disconnect()
        
        # Save to CSV
        with open("scraped_members.csv", "w", newline="", encoding="UTF-8") as file:
            writer = csv.writer(file)
            writer.writerow(['username', 'user_id', 'access_hash', 'name'])
            for user in members:
                writer.writerow([user.username, user.id, user.access_hash, user.full_name])

        await message.answer("Members scraped successfully and saved to 'scraped_members.csv'.")
    except Exception as e:
        logging.error(f"Failed to scrape members: {e}")
        await message.answer("❌ Failed to scrape members. Check the logs.")

# --- Bot Entry ---
async def main():
    logging.info("Bot is starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
