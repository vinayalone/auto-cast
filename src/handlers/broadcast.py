import logging
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from src.db import get_channels
from src.handlers.menu import update_menu
import src.globals as globals  # Import global state

logger = logging.getLogger(__name__)

async def show_broadcast_selection(app, uid, m):
    chs = await get_channels(uid)
    if not chs:
        await update_menu(app, m, "❌ No channels found.", [[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]], uid)
        return
    # Use globals.user_state instead of importing from src.main
    targets = globals.user_state[uid].get("broadcast_targets", [])
    kb = []
    for c in chs:
        is_selected = c["channel_id"] in targets
        icon = "✅" if is_selected else "⬜"
        kb.append([InlineKeyboardButton(f"{icon} {c['title']}", callback_data=f"toggle_bc_{c['channel_id']}")])
    kb.append([InlineKeyboardButton(f"➡️ Done ({len(targets)} Selected)", callback_data="broadcast_confirm")])
    kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")])
    await update_menu(app, m, "📢 **Broadcast Mode**\n\nSelect channels to post to:", kb, uid)

async def start_broadcast_content(app, uid, m):
    # Use globals.user_state instead of importing from src.main
    targets = globals.user_state[uid].get("broadcast_targets", [])
    if not targets:
        await m.reply("❌ No channels selected. Operation cancelled.")
        return
    globals.user_state[uid]["broadcast_queue"] = []
    globals.user_state[uid]["step"] = "waiting_broadcast_content"
    markup = ReplyKeyboardMarkup(
        [[KeyboardButton("✅ Done Adding Posts")], [KeyboardButton("❌ Cancel")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    guide_text = (
        f"📢 **Multi-Post Mode Active**\n"
        f"Selected: **{len(targets)} Channels**\n\n"
        f"👇 **How to Use:**\n"
        f"1️⃣ **Send Posts:** Send text, photos, or videos one by one.\n"
        f"2️⃣ **Create Threads:** If you want Post B to reply to Post A, simply **reply to Post A** right here!\n"
        f"3️⃣ **Finish:** Click **✅ Done** when finished.\n\n"
        f"⚙️ *You can configure Pin/Delete settings for each post individually after adding them.*"
    )
    await app.send_message(m.chat.id, guide_text, reply_markup=markup)
