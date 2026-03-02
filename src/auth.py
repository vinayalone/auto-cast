import logging
from pyrogram import Client, errors
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from src.config import API_ID, API_HASH
from src.db import save_session, delete_all_user_data
from src.utils import check_rate_limit, reset_rate_limit

logger = logging.getLogger(__name__)

# In‑memory login states
login_state = {}

async def start_login(uid: int, message):
    login_state[uid] = {"step": "waiting_phone"}
    await message.reply_text(
        "📱 **Step 1: Phone Number**\n\n"
        "Please enter your Telegram phone number with country code.\n"
        "Example: `+919876543210`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]
        ])
    )

async def process_phone(uid: int, text: str, message):
    if not check_rate_limit(f"login_{uid}"):
        await message.reply_text("❌ Too many attempts. Please try again later.")
        return
    try:
        temp_client = Client(
            ":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            device_model="AutoCast Client",
            system_version="PC",
            app_version="AutoCast Version"
        )
        await temp_client.connect()
        sent = await temp_client.send_code(text)
        login_state[uid].update({
            "client": temp_client,
            "phone": text,
            "hash": sent.phone_code_hash,
            "step": "waiting_code"
        })
        await message.reply_text(
            "📩 **Step 2: Enter Verification Code**\n\n"
            "Telegram has sent a verification code to your account. "
            "Please enter it below.\n\n"
            "⚠️ **Important:** To ensure the code is processed correctly and prevent expiry, "
            "please prefix the code with `aa`. For example, if your code is `12345`, send `aa12345`.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]
            ])
        )
    except Exception as e:
        logger.error(f"Phone login error: {e}")
        await message.reply_text(f"❌ Login failed: {e}")

async def process_code(uid: int, text: str, message):
    if not check_rate_limit(f"login_{uid}"):
        await message.reply_text("❌ Too many attempts. Please try again later.")
        return
    state = login_state.get(uid)
    if not state:
        return
    real_code = text.lower().replace("aa", "").strip()
    try:
        await state["client"].sign_in(state["phone"], state["hash"], real_code)
        # Success
        session_string = await state["client"].export_session_string()
        await save_session(uid, session_string)
        await state["client"].disconnect()
        reset_rate_limit(f"login_{uid}")
        del login_state[uid]
        await message.reply_text(
            "✅ **Login Successful!**\n\nYou can now manage your channels. Click /manage to start.",
            reply_markup=ReplyKeyboardRemove()
        )
    except errors.PhoneCodeInvalid:
        await message.reply_text("❌ Invalid verification code. Please try again, ensuring you prefix it with `aa`.")
    except errors.SessionPasswordNeeded:
        state["step"] = "waiting_password"
        await message.reply_text(
            "🔒 **Two-Step Verification Required**\n\n"
            "Your Telegram account has Two-Step Verification enabled. Please enter your cloud password.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]
            ])
        )
    except Exception as e:
        logger.error(f"Code verification error: {e}")
        await message.reply_text(f"❌ Login error: {e}")

async def process_password(uid: int, text: str, message):
    if not check_rate_limit(f"login_{uid}"):
        await message.reply_text("❌ Too many attempts. Please try again later.")
        return
    state = login_state.get(uid)
    if not state:
        return
    try:
        await state["client"].check_password(text)
        session_string = await state["client"].export_session_string()
        await save_session(uid, session_string)
        await state["client"].disconnect()
        reset_rate_limit(f"login_{uid}")
        del login_state[uid]
        await message.reply_text(
            "✅ **Login Successful!**\n\nYou can now manage your channels. Click /manage to start.",
            reply_markup=ReplyKeyboardRemove()
        )
    except errors.PasswordHashInvalid:
        await message.reply_text("❌ Invalid password. Please try again.")
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        await message.reply_text(f"❌ Login error: {e}")
