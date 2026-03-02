import logging
import datetime
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from src.db import get_channels, get_user_tasks, get_single_task, delete_task, del_channel
from src.utils import safe_get_user_timezone
import src.globals as globals  # <-- ADDED

logger = logging.getLogger(__name__)

# ------------------- Menu Helpers -------------------
async def show_main_menu(app, m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
        [InlineKeyboardButton("📢 My Channels", callback_data="list_channels")],
        [InlineKeyboardButton("➕ Add Channel (Forward Msg)", callback_data="add_channel_forward")],
        [InlineKeyboardButton("➕ Add Channel (By ID)", callback_data="add_channel_id")],
        [InlineKeyboardButton("⏰ Set Timezone", callback_data="set_timezone")],
        [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
    ]
    await update_menu(app, m, "👋 **Welcome to AutoCast | Channel Manager!**\n\nSelect an option below.", kb, uid, force_new)

async def show_channels(app, uid, m, force_new=False):
    chs = await get_channels(uid)
    if not chs:
        kb = [
            [InlineKeyboardButton("➕ Add One", callback_data="add_channel_forward")],
            [InlineKeyboardButton("➕ Add By ID", callback_data="add_channel_id")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_home")]
        ]
        await update_menu(app, m, "❌ **No Channels Linked Yet.**", kb, uid, force_new)
        return
    kb = []
    for c in chs:
        kb.append([InlineKeyboardButton(f"{c['title']}", callback_data=f"ch_{c['channel_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await update_menu(app, m, "**📢 Your Linked Channels**", kb, uid, force_new)

async def show_channel_options(app, uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    kb = [
        [InlineKeyboardButton("✍️ Schedule Post", callback_data=f"new_{cid}")],
        [InlineKeyboardButton(f"📅 Scheduled ({len(tasks)})", callback_data=f"tasks_{cid}")],
        [InlineKeyboardButton("🗑 Unlink", callback_data=f"rem_{cid}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ]
    await update_menu(app, m, f"⚙️ **Managing Channel**", kb, uid, force_new)

async def list_active_tasks(app, uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    if not tasks:
        await update_menu(app, m, "✅ No active tasks.", [[InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")]], uid, force_new)
        return
    tasks.sort(key=lambda x: x["start_time"])
    txt = "**📅 Scheduled Tasks:**\nSelect one to manage:"
    kb = []
    type_icons = {"text": "📝", "photo": "📷", "video": "📹", "audio": "🎵", "poll": "📊"}
    user_tz = await safe_get_user_timezone(uid)

    for t in tasks:
        snippet = (t["content_text"] or "Media")[:15] + "..."
        icon = type_icons.get(t["content_type"], "📁")
        try:
            dt = datetime.datetime.fromisoformat(t["start_time"])
            if dt.tzinfo is None:
                dt = user_tz.localize(dt)
            time_str = dt.astimezone(user_tz).strftime("%I:%M %p")
        except:
            time_str = "?"
        btn_text = f"{icon} {snippet} | ⏰ {time_str}"
        kb.append([InlineKeyboardButton(btn_text, callback_data=f"view_{t['task_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")])
    await update_menu(app, m, txt, kb, uid, force_new)

async def show_task_details(app, uid, m, tid):
    t = await get_single_task(tid)
    if not t:
        await update_menu(app, m, "❌ Task not found.", [[InlineKeyboardButton("🏠 Home", callback_data="menu_home")]], uid)
        return
    user_tz = await safe_get_user_timezone(uid)
    dt = datetime.datetime.fromisoformat(t["start_time"])
    if dt.tzinfo is None:
        dt = user_tz.localize(dt)
    time_str = dt.astimezone(user_tz).strftime("%d-%b %I:%M %p %Z%z")
    type_map = {"text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video", "audio": "🎵 Audio", "poll": "📊 Poll"}
    type_str = type_map.get(t["content_type"], "📁 File")
    txt = (f"⚙️ **Managing Task**\n\n"
           f"📝 **Snippet:** `{t['content_text'][:50]}...`\n"
           f"📂 **Type:** {type_str}\n"
           f"📅 **Time:** `{time_str}`\n"
           f"🔁 **Repeat:** `{t['repeat_interval'] or 'No'}`\n\n"
           f"👇 **Select Action:**")
    kb = [
        [InlineKeyboardButton("🗑 Delete Task", callback_data=f"del_task_{tid}")],
        [InlineKeyboardButton("🔙 Back to List", callback_data=f"back_list_{t['chat_id']}")]
    ]
    await update_menu(app, m, txt, kb, uid)

async def update_menu(app, m, text, kb, uid, force_new=False):
    markup = InlineKeyboardMarkup(kb) if kb else None
    # Use globals.user_state instead of importing from src.main
    if force_new:
        sent = await app.send_message(m.chat.id, text, reply_markup=markup)
        if uid in globals.user_state:
            globals.user_state[uid]["menu_msg_id"] = sent.id
        return
    st = globals.user_state.get(uid, {})
    menu_id = st.get("menu_msg_id")
    if menu_id:
        try:
            await app.edit_message_text(m.chat.id, menu_id, text, reply_markup=markup)
            return
        except Exception as e:
            logger.warning(f"Failed to edit message {menu_id}: {e}. Sending new.")
    sent = await app.send_message(m.chat.id, text, reply_markup=markup)
    if uid in globals.user_state:
        globals.user_state[uid]["menu_msg_id"] = sent.id
