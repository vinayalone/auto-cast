import logging
import datetime
import pytz
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from src.db import (
    get_user_tasks, get_single_task, delete_task, del_channel,
    get_user_timezone, set_user_timezone
)
from src.utils import safe_get_user_timezone
from src.handlers.menu import (
    show_main_menu, show_channels, show_channel_options,
    list_active_tasks, show_task_details, update_menu
)
from src.handlers.broadcast import show_broadcast_selection, start_broadcast_content
from src.handlers.timezone import show_timezone_menu
from src.auth import start_login
from src.scheduler import scheduler
import src.globals as globals  # <-- import globals

logger = logging.getLogger(__name__)

# Remove the old import from src.main

async def callback_router(c, q):
    uid = q.from_user.id
    data = q.data

    if uid not in globals.user_state:
        globals.user_state[uid] = {}
    globals.user_state[uid]["menu_msg_id"] = q.message.id

    # Home
    if data == "menu_home":
        globals.user_state[uid]["step"] = None
        await show_main_menu(globals.app, q.message, uid)

    # Login
    elif data == "login_start":
        await start_login(uid, q.message)

    # Logout (handled in separate function to keep this file manageable)
    elif data == "logout":
        await handle_logout(uid, q)

    elif data == "logout_step_2":
        await handle_logout_step2(uid, q)

    elif data == "logout_final":
        await handle_logout_final(uid, q)

    # Channels
    elif data == "list_channels":
        await show_channels(globals.app, uid, q.message)

    elif data == "add_channel_forward":
        globals.user_state[uid]["step"] = "waiting_forward"
        await update_menu(globals.app, q.message,
                          "📝 **Add Channel**\n\nForward a message from your channel to this chat now.",
                          None, uid)

    elif data == "add_channel_id":
        globals.user_state[uid]["step"] = "waiting_channel_id"
        await update_menu(globals.app, q.message,
                          "📝 **Add Channel by ID**\n\nPlease send the **Channel ID** now.\n\n"
                          "For public channels, use `@username`.\n"
                          "For private, forward to @RawDataBot to get the chat.id (negative number).",
                          [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)

    elif data.startswith("ch_"):
        cid = data.split("ch_")[1]
        await show_channel_options(globals.app, uid, q.message, cid)

    elif data.startswith("rem_"):
        cid = data.split("rem_")[1]
        await del_channel(uid, cid)
        await q.answer("Channel Unlinked!")
        await show_channels(globals.app, uid, q.message)

    # Task scheduling
    elif data.startswith("new_"):
        cid = data.split("new_")[1]
        globals.user_state[uid].update({"step": "waiting_content", "target": cid})
        await update_menu(globals.app, q.message,
                          "1️⃣ **Create Post**\n\nSend me the content you want to schedule.",
                          [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)

    elif data == "step_time":
        await show_time_menu(uid, q.message)

    elif data == "step_rep":
        await ask_repetition(uid, q.message)

    elif data == "step_settings":
        await ask_settings(uid, q.message)

    elif data.startswith("time_"):
        await handle_time_selection(uid, data, q)

    elif data.startswith("rep_"):
        await handle_repetition(uid, data, q)

    elif data.startswith("cfg_q_"):
        await handle_configure_post(uid, data, q)

    elif data.startswith("t_q_"):
        await handle_toggle_post(uid, data, q)

    elif data == "toggle_pin" or data == "toggle_del":
        await handle_toggle_setting(uid, data, q)

    elif data == "goto_confirm":
        await confirm_task(uid, q)

    elif data == "save_task":
        from src.handlers.inputs import create_task_logic
        await create_task_logic(uid, q)

    # Task management
    elif data.startswith("tasks_"):
        cid = data.split("tasks_")[1]
        await list_active_tasks(globals.app, uid, q.message, cid)

    elif data.startswith("view_"):
        tid = data.split("view_")[1]
        await show_task_details(globals.app, uid, q.message, tid)

    elif data.startswith("del_task_"):
        tid = data.split("del_task_")[1]
        try:
            if scheduler:
                scheduler.remove_job(tid)
            cid = await delete_task(tid)
            await q.answer("✅ Task deleted!")
            if cid:
                await list_active_tasks(globals.app, uid, q.message, cid)
            else:
                await show_main_menu(globals.app, q.message, uid)
        except Exception as e:
            logger.error(f"Error deleting task {tid}: {e}")
            await q.answer("❌ Failed to delete task.")

    # Broadcast
    elif data == "broadcast_start":
        globals.user_state[uid]["step"] = "broadcast_select_channels"
        globals.user_state[uid]["broadcast_targets"] = []
        await show_broadcast_selection(globals.app, uid, q.message)

    elif data.startswith("toggle_bc_"):
        cid = data.split("toggle_bc_")[1]
        targets = globals.user_state[uid].get("broadcast_targets", [])
        if cid in targets:
            targets.remove(cid)
        else:
            targets.append(cid)
        globals.user_state[uid]["broadcast_targets"] = targets
        await show_broadcast_selection(globals.app, uid, q.message)

    elif data == "broadcast_confirm":
        await start_broadcast_content(globals.app, uid, q.message)

    # Timezone
    elif data == "set_timezone":
        globals.user_state[uid]["step"] = "waiting_timezone_input"
        await show_timezone_menu(globals.app, uid, q.message)

    elif data.startswith("tz_"):
        tz_name = data.split("tz_")[1]
        if tz_name == "custom":
            globals.user_state[uid]["step"] = "waiting_custom_timezone"
            await update_menu(globals.app, q.message,
                              "📝 **Enter Custom Timezone**\n\nPlease send the exact timezone name (e.g., `America/Los_Angeles`).",
                              [[InlineKeyboardButton("🔙 Back", callback_data="set_timezone")]], uid)
        else:
            try:
                pytz.timezone(tz_name)
                await set_user_timezone(uid, tz_name)
                globals.user_state[uid]["step"] = None
                await q.answer(f"✅ Timezone set to {tz_name}!")
                await show_main_menu(globals.app, q.message, uid)
            except pytz.exceptions.UnknownTimeZoneError:
                await q.answer("❌ Invalid timezone name.", show_alert=True)

    # Wizard for auto-delete offset
    elif data.startswith("wizard_ask_offset"):
        await handle_ask_offset(uid, data, q)

    elif data.startswith("set_del_off_WIZARD"):
        await handle_set_offset(uid, data, q)

    else:
        logger.warning(f"Unhandled callback: {data}")

# ------------------- Helper functions for callbacks -------------------
async def show_time_menu(uid, m):
    user_tz = await get_user_timezone(uid)
    current_time = datetime.datetime.now(user_tz).strftime("%d-%b %I:%M %p %Z%z")
    kb = [
        [InlineKeyboardButton("⚡️ Now (5s delay)", callback_data="time_0")],
        [InlineKeyboardButton("5 Minutes", callback_data="time_5"), InlineKeyboardButton("15 Minutes", callback_data="time_15")],
        [InlineKeyboardButton("30 Minutes", callback_data="time_30"), InlineKeyboardButton("1 Hour", callback_data="time_60")],
        [InlineKeyboardButton("Custom Date/Time", callback_data="time_custom")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu_home")]
    ]
    await update_menu(globals.app, m,
                      f"2️⃣ **Schedule Time**\n\nYour timezone: `{user_tz.tzname(datetime.datetime.now())}` (`{current_time}`)",
                      kb, uid)

async def ask_repetition(uid, m):
    kb = [
        [InlineKeyboardButton("Once (No Repeat)", callback_data="rep_0")],
        [InlineKeyboardButton("Every 1 Hour", callback_data="rep_60"), InlineKeyboardButton("Every 3 Hours", callback_data="rep_180")],
        [InlineKeyboardButton("Every 6 Hours", callback_data="rep_360"), InlineKeyboardButton("Every 12 Hours", callback_data="rep_720")],
        [InlineKeyboardButton("Every 24 Hours", callback_data="rep_1440")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_time")]
    ]
    await update_menu(globals.app, m, "3️⃣ **Repetition**\n\nHow often should this post be repeated?", kb, uid)

async def ask_settings(uid, m):
    st = globals.user_state[uid]
    queue = st.get("broadcast_queue")
    if queue:
        # Batch mode
        txt = "4️⃣ **Batch Post Settings**\n\nConfigure individual post settings:"
        kb = []
        for i, post in enumerate(queue):
            p_stat = "ON" if post.get("pin", False) else "OFF"
            d_stat = "ON" if post.get("delete_old", False) else "OFF"
            offset = post.get("auto_delete_offset", 0)
            off_stat = f"{offset}m" if offset > 0 else "OFF"
            btn_txt = f"✅ Post #{i+1} | P: {p_stat} | D: {d_stat} | ⏰ {off_stat}"
            kb.append([InlineKeyboardButton(btn_txt, callback_data=f"cfg_q_{i}")])
        kb.append([InlineKeyboardButton("➡️ Confirm All", callback_data="goto_confirm")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="step_rep")])
        await update_menu(globals.app, m, txt, kb, uid)
        return

    # Single post
    st.setdefault("pin", True)
    st.setdefault("del", True)
    offset = st.get("auto_delete_offset", 0)
    pin_icon = "✅" if st["pin"] else "❌"
    del_icon = "✅" if st["del"] else "❌"
    off_text = f"⏰ Delete: {offset}m After Post" if offset > 0 else "⏰ Auto-Delete: OFF"
    kb = [
        [InlineKeyboardButton(f"📌 Pin Msg: {pin_icon}", callback_data="toggle_pin")],
        [InlineKeyboardButton(f"🗑 Del Old: {del_icon}", callback_data="toggle_del")],
        [InlineKeyboardButton(off_text, callback_data="wizard_ask_offset_single")],
        [InlineKeyboardButton("➡️ Confirm", callback_data="goto_confirm")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_rep")]
    ]
    await update_menu(globals.app, m,
                      f"4️⃣ **Settings**\n\nConfigure how your post behaves.\nAuto-delete: **{offset} minutes** after posting.",
                      kb, uid)

async def confirm_task(uid, m):
    st = globals.user_state[uid]
    user_tz = await get_user_timezone(uid)
    t_str = st["start_time"].astimezone(user_tz).strftime("%d-%b %I:%M %p %Z%z")
    r_str = st["interval"] if st["interval"] else "Once"
    queue = st.get("broadcast_queue")
    if queue:
        type_str = f"📦 Batch ({len(queue)} Posts)"
        pin_count = sum(1 for p in queue if p.get("pin", True))
        settings_str = f"📌 Pinning: {pin_count}/{len(queue)} Posts"
    else:
        type_map = {"text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
                    "audio": "🎵 Audio", "voice": "🎙 Voice", "document": "📁 File",
                    "poll": "📊 Poll", "animation": "🎞 GIF", "sticker": "✨ Sticker"}
        c_type = st.get("content_type", "unknown")
        type_str = type_map.get(c_type, c_type.upper())
        settings_str = f"📌 Pin: {'✅' if st.get('pin',True) else '❌'} | 🗑 Del: {'✅' if st.get('del',True) else '❌'}"
    txt = (f"✅ **Summary**\n\n"
           f"📢 Content: {type_str}\n"
           f"📅 Time: `{t_str}`\n"
           f"🔁 Repeat: `{r_str}`\n"
           f"{settings_str}")
    kb = [[InlineKeyboardButton("✅ Schedule It", callback_data="save_task")],
          [InlineKeyboardButton("🔙 Back", callback_data="step_settings")]]
    await update_menu(globals.app, m, txt, kb, uid)

async def handle_time_selection(uid, data, q):
    offset = data.split("time_")[1]
    user_tz = await get_user_timezone(uid)
    now = datetime.datetime.now(user_tz)
    if offset == "custom":
        globals.user_state[uid]["step"] = "waiting_custom_date"
        cur_time = now.strftime("%d-%b %I:%M %p")
        await update_menu(globals.app, q.message,
                          f"📅 **Select Custom Date**\n\nCurrent time: `{cur_time}`\n"
                          "Please type the date and time in format:\n"
                          "`DD-Mon YYYY HH:MM AM/PM` or `DD-Mon HH:MM AM/PM`",
                          [[InlineKeyboardButton("🔙 Back", callback_data="step_time")]], uid)
        return
    if offset == "0":
        run_time = now + datetime.timedelta(seconds=5)
    else:
        run_time = now + datetime.timedelta(minutes=int(offset))
        run_time = run_time.replace(second=0, microsecond=0)
    globals.user_state[uid]["start_time"] = run_time
    if globals.user_state[uid].get("step") == "waiting_time_for_broadcast":
        globals.user_state[uid]["step"] = "waiting_broadcast_settings"
        await ask_settings(uid, q.message)
    else:
        await ask_repetition(uid, q.message)

async def handle_repetition(uid, data, q):
    val = data.split("rep_")[1]
    interval = None if val == "0" else f"minutes={val}"
    globals.user_state[uid]["interval"] = interval
    await ask_settings(uid, q.message)

async def handle_configure_post(uid, data, q):
    idx = int(data.split("cfg_q_")[1])
    post = globals.user_state[uid]["broadcast_queue"][idx]
    p_stat = "Enabled ✅" if post.get("pin", False) else "Disabled ❌"
    d_stat = "Enabled ✅" if post.get("delete_old", False) else "Disabled ❌"
    offset = post.get("auto_delete_offset", 0)
    off_text = f"⏰ Delete: {offset}m After Post" if offset > 0 else "⏰ Auto-Delete: OFF"
    txt = (f"⚙️ **Configuring Post #{idx+1}**\n\n"
           f"📂 Type: **{post['content_type']}**\n"
           f"📌 Pin this post? **{p_stat}**\n"
           f"🗑 Delete previous? **{d_stat}**\n"
           f"{off_text}")
    kb = [
        [InlineKeyboardButton(f"📌 Toggle Pin", callback_data=f"t_q_pin_{idx}")],
        [InlineKeyboardButton(f"🗑 Toggle Delete", callback_data=f"t_q_del_{idx}")],
        [InlineKeyboardButton("⏰ Set Delete After", callback_data=f"wizard_ask_offset_{idx}")],
        [InlineKeyboardButton("🔙 Back to List", callback_data="step_settings")]
    ]
    await update_menu(globals.app, q.message, txt, kb, uid)

async def handle_toggle_post(uid, data, q):
    parts = data.split("_")
    action = parts[2]  # pin or del
    idx = int(parts[3])
    post = globals.user_state[uid]["broadcast_queue"][idx]
    if action == "pin":
        post["pin"] = not post.get("pin", False)
    elif action == "del":
        post["delete_old"] = not post.get("delete_old", False)
    # Re‑open config
    await handle_configure_post(uid, f"cfg_q_{idx}", q)

async def handle_toggle_setting(uid, data, q):
    st = globals.user_state[uid]
    st.setdefault("pin", True)
    st.setdefault("del", True)
    if data == "toggle_pin":
        st["pin"] = not st["pin"]
    else:
        st["del"] = not st["del"]
    await ask_settings(uid, q.message)

async def handle_ask_offset(uid, data, q):
    parts = data.split("_")
    # format: wizard_ask_offset_{idx} or wizard_ask_offset_single
    temp_id = parts[3]  # either a number or "single"
    kb = [
        [InlineKeyboardButton("Disable Auto-Delete", callback_data=f"set_del_off_WIZARD_{temp_id}_0")],
        [InlineKeyboardButton("Delete after 5m", callback_data=f"set_del_off_WIZARD_{temp_id}_5")],
        [InlineKeyboardButton("Delete after 15m", callback_data=f"set_del_off_WIZARD_{temp_id}_15")],
        [InlineKeyboardButton("Delete after 30m", callback_data=f"set_del_off_WIZARD_{temp_id}_30")],
        [InlineKeyboardButton("Delete after 1h", callback_data=f"set_del_off_WIZARD_{temp_id}_60")],
        [InlineKeyboardButton("Delete after 6h", callback_data=f"set_del_off_WIZARD_{temp_id}_360")],
        [InlineKeyboardButton("Delete after 24h", callback_data=f"set_del_off_WIZARD_{temp_id}_1440")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_settings")]
    ]
    await update_menu(globals.app, q.message,
                      "⏳ **Select Auto-Delete Time**\n\n"
                      "How long after posting should the message be automatically deleted?",
                      kb, uid)

async def handle_set_offset(uid, data, q):
    parts = data.split("_")
    # set_del_off_WIZARD_{idx}_{offset}
    # parts[3] is idx or 'single', parts[4] is offset
    temp_id = parts[3]
    offset = int(parts[4])
    if temp_id == "single":
        globals.user_state[uid]["auto_delete_offset"] = offset
    else:
        idx = int(temp_id)
        if "broadcast_queue" in globals.user_state[uid]:
            globals.user_state[uid]["broadcast_queue"][idx]["auto_delete_offset"] = offset
    time_str = f"{offset}m" if offset > 0 else "Disabled"
    await q.answer(f"✅ Auto-Delete set to {time_str}!")
    await ask_settings(uid, q.message)

# ------------------- Logout handlers -------------------
async def handle_logout(uid, q):
    from src.db import get_all_tasks
    tasks = await get_all_tasks()
    user_tasks = [t for t in tasks if t["owner_id"] == uid]
    txt = (f"⚠️ **Wait! Are you sure?**\n\n"
           f"You have **{len(user_tasks)} active tasks** scheduled.\n"
           f"If you logout, the bot will stop working.")
    kb = [
        [InlineKeyboardButton("⚠️ Yes, I want to Logout", callback_data="logout_step_2")],
        [InlineKeyboardButton("🔙 No, Cancel", callback_data="menu_home")]
    ]
    await update_menu(globals.app, q.message, txt, kb, uid)

async def handle_logout_step2(uid, q):
    txt = ("🛑 **FINAL WARNING** 🛑\n\n"
           "This will **PERMANENTLY DELETE** all your scheduled posts and settings.\n"
           "This action cannot be undone.\n\n"
           "Are you absolutely sure?")
    kb = [
        [InlineKeyboardButton("🗑️ Delete Everything & Logout", callback_data="logout_final")],
        [InlineKeyboardButton("🔙 No! Go Back", callback_data="menu_home")]
    ]
    await update_menu(globals.app, q.message, txt, kb, uid)

async def handle_logout_final(uid, q):
    from src.db import delete_all_user_data
    from src.scheduler import scheduler
    try:
        await globals.app.edit_message_text(uid, q.message.id, "⏳ **Logging out...**\nTerminating session and wiping data.")
    except:
        await q.answer("⏳ Processing...", show_alert=False)

    # Stop user's jobs
    if scheduler:
        jobs = scheduler.get_jobs()
        for job in jobs:
            if job.id.startswith(f"task_{uid}_") or job.id.startswith(f"del_{uid}_"):
                scheduler.remove_job(job.id)

    await delete_all_user_data(uid)

    try:
        await globals.app.edit_message_text(
            chat_id=uid,
            message_id=q.message.id,
            text="👋 **Logged out successfully.**\n\nAll data has been wiped."
        )
    except:
        await globals.app.send_message(uid, "👋 **Logged out successfully.**")
