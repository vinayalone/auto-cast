import logging
import datetime
import json
import pytz
import dateparser
from pyrogram import Client, errors, enums
from pyrogram.types import ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from src.config import API_ID, API_HASH
from src.db import (
    get_session, save_task, add_channel, get_single_task,
    serialize_entities, get_user_timezone
)
from src.auth import process_phone, process_code, process_password, login_state
# Fixed imports: show_time_menu and ask_repetition come from callback, not menu
from src.handlers.menu import show_main_menu, show_channel_options, update_menu
from src.handlers.callback import show_time_menu, ask_repetition, ask_settings
from src.handlers.timezone import process_custom_timezone
from src.scheduler import add_scheduler_job, scheduler
import src.globals as globals

logger = logging.getLogger(__name__)

async def handle_input(c, m):
    uid = m.from_user.id
    text = m.text.strip() if m.text else ""

    # Login flow
    if uid in login_state:
        st = login_state[uid]
        if st["step"] == "waiting_phone":
            await process_phone(uid, text, m)
        elif st["step"] == "waiting_code":
            await process_code(uid, text, m)
        elif st["step"] == "waiting_password":
            await process_password(uid, text, m)
        return

    # Regular user states
    st = globals.user_state.get(uid, {})
    step = st.get("step")

    # Handle broadcast finish/cancel buttons
    if text == "✅ Done Adding Posts":
        if globals.user_state[uid].get("broadcast_queue"):
            globals.user_state[uid]["step"] = "waiting_time_for_broadcast"
            await m.reply("✅ Done adding posts. Now, let's schedule them!", reply_markup=ReplyKeyboardRemove())
            await show_time_menu(uid, m)
        else:
            await m.reply("❌ No posts added. Operation cancelled.", reply_markup=ReplyKeyboardRemove())
            if uid in globals.user_state:
                del globals.user_state[uid]
            await show_main_menu(globals.app, m, uid)
        return

    if text == "❌ Cancel":
        if uid in globals.user_state:
            del globals.user_state[uid]
        await m.reply("Operation cancelled.", reply_markup=ReplyKeyboardRemove())
        await show_main_menu(globals.app, m, uid)
        return

    # Channel addition
    if step == "waiting_channel_id":
        await process_add_channel_by_id(uid, text, m)
        return

    if step == "waiting_forward" and m.forward_from_chat:
        await process_add_channel_forward(uid, m)
        return

    # Post creation
    if step == "waiting_content":
        await process_content_message(uid, m)
        return

    if step == "waiting_broadcast_content":
        await process_broadcast_content(uid, m)
        return

    # Custom date/time
    if step == "waiting_custom_date":
        await process_custom_date(uid, text, m)
        return

    # Custom timezone
    if step == "waiting_custom_timezone":
        await process_custom_timezone(globals.app, uid, m, text)
        return

    # Fallback
    if not m.text.startswith("/"):
        await m.reply("I'm not sure how to handle that. Please use the menu or /manage to begin.")

# ------------------- Channel Addition -------------------
async def process_add_channel_by_id(uid, text, m):
    channel_input = text.strip()
    from src.db import get_session
    session_str = await get_session(uid)
    if not session_str:
        await m.reply("❌ User session not found. Please login again.")
        globals.user_state[uid]["step"] = "menu_home"
        return

    async with Client(
        ":memory:",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_str
    ) as user:
        try:
            # Resolve identifier
            if channel_input.startswith("@"):
                chat = await user.get_chat(channel_input)
            else:
                chat = await user.get_chat(int(channel_input))

            if chat.type not in [enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP]:
                await m.reply("❌ Not a channel or supergroup.")
                return

            # Check if user (owner) is admin
            member = await user.get_chat_member(chat.id, uid)
            if member.status not in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR]:
                await m.reply("❌ You are not an admin in this channel.")
                return

            if not member.can_post_messages:
                await m.reply("⚠️ Warning: You lack 'Post Messages' permission.")

            await add_channel(uid, str(chat.id), chat.title)
            await m.reply(f"✅ Channel **{chat.title}** (`{chat.id}`) added successfully!")
            globals.user_state[uid]["step"] = "menu_home"
            await show_channel_options(globals.app, uid, m, str(chat.id))

        except errors.ChatIdInvalid:
            await m.reply("❌ Invalid channel ID/username.")
        except errors.UsernameNotOccupied:
            await m.reply("❌ Username does not exist.")
        except errors.ChatAdminRequired:
            await m.reply("❌ Userbot cannot access channel. Ensure it's public or you are admin.")
        except Exception as e:
            logger.error(f"Add channel error: {e}")
            await m.reply(f"❌ Unexpected error: {e}")

async def process_add_channel_forward(uid, m):
    cid = m.forward_from_chat.id
    title = m.forward_from_chat.title or "Private Channel"
    from src.db import get_session
    session_str = await get_session(uid)
    if not session_str:
        await m.reply("❌ User session not found.")
        globals.user_state[uid]["step"] = "menu_home"
        return

    async with Client(
        ":memory:",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_str
    ) as user:
        try:
            member = await user.get_chat_member(cid, uid)
            if member.status not in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR]:
                await m.reply("❌ You are not an admin in this channel.")
                return
        except errors.ChatAdminRequired:
            # Userbot itself not admin – that's okay if user is admin
            pass
        except Exception as e:
            logger.warning(f"Could not check permissions: {e}")

    await add_channel(uid, str(cid), title)
    await m.reply(f"✅ Channel **{title}** (`{cid}`) added successfully!")
    globals.user_state[uid]["step"] = "menu_home"
    await show_channel_options(globals.app, uid, m, str(cid))

# ------------------- Post Creation -------------------
async def process_content_message(uid, m):
    st = globals.user_state[uid]
    st["content_type"] = m.media.value if m.media else "text"
    st["content_text"] = m.caption or m.text
    st["file_id"] = (
        m.photo.file_id if m.photo else
        m.video.file_id if m.video else
        m.audio.file_id if m.audio else
        m.voice.file_id if m.voice else
        m.document.file_id if m.document else
        m.animation.file_id if m.animation else
        m.sticker.file_id if m.sticker else None
    )
    st["entities"] = serialize_entities(m.caption_entities or m.entities)
    st["input_msg_id"] = m.id
    if m.reply_to_message:
        st["reply_ref_id"] = m.reply_to_message.id

    await show_time_menu(uid, m)

async def process_broadcast_content(uid, m):
    st = globals.user_state[uid]
    queue = st.get("broadcast_queue", [])
    post = {
        "content_type": m.media.value if m.media else "text",
        "content_text": m.caption or m.text,
        "file_id": (
            m.photo.file_id if m.photo else
            m.video.file_id if m.video else
            m.audio.file_id if m.audio else
            m.voice.file_id if m.voice else
            m.document.file_id if m.document else
            m.animation.file_id if m.animation else
            m.sticker.file_id if m.sticker else None
        ),
        "entities": serialize_entities(m.caption_entities or m.entities),
        "input_msg_id": m.id,
        "reply_ref_id": m.reply_to_message.id if m.reply_to_message else None
    }
    queue.append(post)
    st["broadcast_queue"] = queue
    await m.reply(f"✅ Post #{len(queue)} added to queue. Send next post or click '✅ Done'.")

async def process_custom_date(uid, text, m):
    st = globals.user_state[uid]
    user_tz = await get_user_timezone(uid)
    dt = dateparser.parse(
        text,
        settings={'TIMEZONE': user_tz.zone, 'RETURN_AS_TIMEZONE_AWARE': True}
    )
    if dt is None:
        await m.reply("❌ Couldn't understand that date. Try something like 'tomorrow 3pm' or '25 Dec 2025 10:30'.")
        return
    if dt < datetime.datetime.now(user_tz):
        await m.reply("❌ That date is in the past.")
        return
    st["start_time"] = dt
    if st.get("step") == "waiting_time_for_broadcast":
        globals.user_state[uid]["step"] = "waiting_broadcast_settings"
        from src.handlers.callback import ask_settings
        await ask_settings(uid, m)
    else:
        await ask_repetition(uid, m)

# ------------------- Task Saving -------------------
async def create_task_logic(uid, q):
    st = globals.user_state[uid]
    targets = st.get("broadcast_targets", [st.get("target")])
    queue = st.get("broadcast_queue")

    if not queue:
        queue = [{
            "content_type": st["content_type"],
            "content_text": st["content_text"],
            "file_id": st["file_id"],
            "entities": st.get("entities"),
            "input_msg_id": 0,
            "reply_ref_id": None,
            "auto_delete_offset": st.get("auto_delete_offset", 0)
        }]

    base_tid = int(datetime.datetime.now().timestamp())
    user_tz = await get_user_timezone(uid)
    t_str = st["start_time"].astimezone(user_tz).strftime("%d-%b %I:%M %p %Z%z")
    total_tasks = 0

    for ch_idx, cid in enumerate(targets):
        batch_map = {}
        for post_idx, post in enumerate(queue):
            tid = f"task_{base_tid}_{ch_idx}_{post_idx}"
            if "input_msg_id" in post:
                batch_map[post["input_msg_id"]] = tid

        for post_idx, post in enumerate(queue):
            tid = f"task_{base_tid}_{ch_idx}_{post_idx}"
            run_time = st["start_time"] + datetime.timedelta(seconds=post_idx * 10)
            reply_target_task_id = None
            if post.get("reply_ref_id"):
                for prev_post_idx in range(post_idx):
                    if queue[prev_post_idx]["input_msg_id"] == post["reply_ref_id"]:
                        reply_target_task_id = f"task_{base_tid}_{ch_idx}_{prev_post_idx}"
                        break
                if not reply_target_task_id:
                    logger.warning(f"Reply target not found for {tid}")

            task_data = {
                "task_id": tid,
                "owner_id": uid,
                "chat_id": cid,
                "content_type": post["content_type"],
                "content_text": post["content_text"],
                "file_id": post["file_id"],
                "entities": post["entities"],
                "pin": post.get("pin", st.get("pin", True)),
                "delete_old": post.get("delete_old", st.get("del", True)),
                "auto_delete_offset": post.get("auto_delete_offset", st.get("auto_delete_offset", 0)),
                "repeat_interval": st["interval"],
                "start_time": run_time.isoformat(),
                "last_msg_id": None,
                "reply_target": reply_target_task_id,
                "fail_count": 0
            }
            await save_task(task_data)
            add_scheduler_job(task_data)
            total_tasks += 1

    # Clean up
    st.pop("broadcast_targets", None)
    st.pop("broadcast_queue", None)
    st.pop("auto_delete_offset", None)

    final_txt = (f"🎉 **Broadcast Scheduled!**\n\n"
                 f"📢 **Channels:** `{len(targets)}`\n"
                 f"📬 **Posts per Channel:** `{len(queue)}`\n"
                 f"⏱️ **Post Gap:** `10 seconds`\n"
                 f"📅 **Start Time:** `{t_str}`\n\n"
                 f"👉 Click /manage to schedule more.")
    await update_menu(globals.app, q.message, final_txt, None, uid, force_new=False)
