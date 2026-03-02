import pytz
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from src.db import set_user_timezone
from src.handlers.menu import update_menu

async def show_timezone_menu(app, uid, m):
    kb = [
        [InlineKeyboardButton("Asia/Kolkata (IST)", callback_data="tz_Asia/Kolkata")],
        [InlineKeyboardButton("America/New_York (EST/EDT)", callback_data="tz_America/New_York")],
        [InlineKeyboardButton("Europe/London (GMT/BST)", callback_data="tz_Europe/London")],
        [InlineKeyboardButton("Asia/Dubai (GST)", callback_data="tz_Asia/Dubai")],
        [InlineKeyboardButton("Custom Timezone", callback_data="tz_custom")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu_home")]
    ]
    await update_menu(app, m, "🌍 **Set Your Timezone**\n\nPlease select your preferred timezone or enter a custom one.", kb, uid)

async def process_custom_timezone(app, uid, m, text):
    try:
        pytz.timezone(text)
        await set_user_timezone(uid, text)
        import src.globals as globals
        globals.user_state[uid]["step"] = None
        await m.reply(f"✅ Timezone set to {text}!")
        from src.handlers.menu import show_main_menu
        await show_main_menu(app, m, uid)
    except pytz.exceptions.UnknownTimeZoneError:
        await m.reply("❌ Invalid timezone name. Please try again with a valid TZ database name (e.g., `America/Los_Angeles`).")
