import os
import sys
import logging
import asyncio
import datetime
import pytz
import asyncpg
import json
from pyrogram import Client, filters, idle, errors, enums
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    Message, MessageEntity,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)

# ── Logging setup first so check_env_vars can log ───────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("AutoCast")

# ── FIX B1: Safe env loading — int(None) no longer crashes at import ─────────
def _require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        logger.error(f"❌ Missing required env var: {key}")
        sys.exit(1)
    return val

def check_env_vars():
    required = ["API_ID", "API_HASH", "BOT_TOKEN", "DATABASE_URL"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        logger.error(f"❌ Missing env vars: {', '.join(missing)}")
        sys.exit(1)

API_ID    = int(os.environ.get("API_ID", "0") or "0")
API_HASH  = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ── Default fallback timezone (UTC for global users) ─────────────────────────
DEFAULT_TZ = pytz.utc

# ── Init ─────────────────────────────────────────────────────────────────────
app = Client(
    "autocast_v2",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)
scheduler  = None
db_pool    = None
queue_lock = None

login_state = {}   # uid → login flow state
user_state  = {}   # uid → wizard state
tz_cache    = {}   # uid → pytz timezone object (in-memory cache)

# ─────────────────────────────────────────────────────────────────────────────
#  TIMEZONE HELPERS
# ─────────────────────────────────────────────────────────────────────────────
POPULAR_TIMEZONES = [
    ("🌍 UTC",              "UTC"),
    ("🇮🇳 India (IST)",     "Asia/Kolkata"),
    ("🇺🇸 New York (EST)",  "America/New_York"),
    ("🇺🇸 Los Angeles",     "America/Los_Angeles"),
    ("🇬🇧 London (GMT)",    "Europe/London"),
    ("🇩🇪 Berlin (CET)",    "Europe/Berlin"),
    ("🇦🇪 Dubai (GST)",     "Asia/Dubai"),
    ("🇸🇬 Singapore",       "Asia/Singapore"),
    ("🇯🇵 Tokyo (JST)",     "Asia/Tokyo"),
    ("🇦🇺 Sydney (AEST)",   "Australia/Sydney"),
    ("🇧🇷 São Paulo",       "America/Sao_Paulo"),
    ("🇷🇺 Moscow (MSK)",    "Europe/Moscow"),
]

async def get_user_tz(uid: int) -> pytz.BaseTzInfo:
    if uid in tz_cache:
        return tz_cache[uid]
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT timezone FROM userbot_settings WHERE user_id = $1", uid
    )
    tz_str = row["timezone"] if row else "UTC"
    try:
        tz = pytz.timezone(tz_str)
    except Exception:
        tz = DEFAULT_TZ
    tz_cache[uid] = tz
    return tz

async def set_user_tz(uid: int, tz_str: str):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_settings (user_id, timezone)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET timezone = $2
    """, uid, tz_str)
    try:
        tz_cache[uid] = pytz.timezone(tz_str)
    except Exception:
        tz_cache[uid] = DEFAULT_TZ

def now_in(tz: pytz.BaseTzInfo) -> datetime.datetime:
    return datetime.datetime.now(tz)

# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────────────────────────────────────
async def get_db():
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        # ── FIX B2: Create ALL tables, not just tasks ─────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_sessions (
                user_id       BIGINT PRIMARY KEY,
                session_string TEXT NOT NULL
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_channels (
                user_id     BIGINT,
                channel_id  TEXT,
                title       TEXT,
                access_hash BIGINT DEFAULT 0,
                PRIMARY KEY (user_id, channel_id)
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_settings (
                user_id  BIGINT PRIMARY KEY,
                timezone TEXT DEFAULT 'UTC'
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_tasks_v11 (
                task_id           TEXT PRIMARY KEY,
                owner_id          BIGINT,
                chat_id           TEXT,
                content_type      TEXT,
                content_text      TEXT,
                file_id           TEXT,
                entities          TEXT,
                pin               BOOLEAN DEFAULT FALSE,
                delete_old        BOOLEAN DEFAULT FALSE,
                repeat_interval   TEXT,
                start_time        TEXT,
                last_msg_id       BIGINT,
                auto_delete_offset INTEGER DEFAULT 0,
                reply_target      TEXT,
                src_chat_id       BIGINT DEFAULT 0,
                src_msg_id        BIGINT DEFAULT 0
            );
        """)
        # Safe column migrations for existing deployments
        for sql in [
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS auto_delete_offset INTEGER DEFAULT 0",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS reply_target TEXT",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS src_chat_id BIGINT DEFAULT 0",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS src_msg_id  BIGINT DEFAULT 0",
            "ALTER TABLE userbot_settings  ADD COLUMN IF NOT EXISTS timezone TEXT DEFAULT 'UTC'",
            "ALTER TABLE userbot_channels  ADD COLUMN IF NOT EXISTS access_hash BIGINT DEFAULT 0",
        ]:
            try:
                await conn.execute(sql)
            except Exception:
                pass
    logger.info("✅ Database initialised.")

async def migrate_to_v11():
    pool = await get_db()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name='tasks');"
        )
        if exists:
            try:
                await conn.execute("""
                    INSERT INTO userbot_tasks_v11 (
                        task_id, owner_id, chat_id, content_type, content_text,
                        file_id, entities, pin, delete_old, repeat_interval,
                        start_time, last_msg_id
                    )
                    SELECT task_id, owner_id, chat_id, content_type, content_text,
                           file_id, entities, pin, delete_old, repeat_interval,
                           start_time, last_msg_id
                    FROM tasks
                    ON CONFLICT (task_id) DO NOTHING;
                """)
                logger.info("✅ Migration from legacy 'tasks' table complete.")
            except Exception as e:
                logger.warning(f"Migration skipped: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  DB HELPERS
# ─────────────────────────────────────────────────────────────────────────────
async def get_session(user_id):
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT session_string FROM userbot_sessions WHERE user_id=$1", user_id
    )
    return row["session_string"] if row else None

async def save_session(user_id, session):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_sessions (user_id, session_string)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET session_string=$2
    """, user_id, session)

async def add_channel(user_id, cid, title, access_hash: int = 0):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_channels (user_id, channel_id, title, access_hash)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id, channel_id) DO UPDATE SET title=$3, access_hash=$4
    """, user_id, cid, title, access_hash)

async def get_channel_access_hash(user_id, channel_id: str) -> int:
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT access_hash FROM userbot_channels WHERE user_id=$1 AND channel_id=$2",
        user_id, channel_id
    )
    return row["access_hash"] if row and row["access_hash"] else 0

async def warm_peer_and_get_hash(user_client, owner_id: int, channel_id: int) -> int:
    """
    Iterate the user's dialogs using Pyrogram's high-level get_dialogs().
    Pyrogram automatically caches every peer it sees in each page of results.
    Once the target channel appears, resolve_peer() will work and we extract
    the correct user-account-specific access_hash and save it to the DB.

    This is the only reliable approach for :memory: clients because:
    - The session_string restores auth credentials only — no peer cache.
    - access_hash is per-account: the bot's hash is useless for the userbot.
    - channels.GetChannels with hash=0 is rejected by Telegram.
    - get_dialogs() works without any pre-cached peer (uses InputPeerEmpty first).
    """
    try:
        # Pyrogram's get_dialogs paginates automatically and caches all peers
        async for dialog in user_client.get_dialogs():
            if dialog.chat.id == channel_id:
                try:
                    peer = await user_client.storage.get_peer_by_id(channel_id)
                    ah = getattr(peer, "access_hash", 0) or 0
                    if ah:
                        pool = await get_db()
                        await pool.execute(
                            "UPDATE userbot_channels SET access_hash=$1 "
                            "WHERE user_id=$2 AND channel_id=$3",
                            ah, owner_id, str(channel_id)
                        )
                        logger.info(f"✅ Cached access_hash for {channel_id} owner={owner_id}")
                    return ah
                except Exception as e:
                    logger.warning(f"warm_peer resolve failed for {channel_id}: {e}")
                    return 0
    except Exception as e:
        logger.warning(f"warm_peer get_dialogs failed for {channel_id}: {e}")
    return 0

def inject_peer(storage, peer_id: int, access_hash: int):
    """
    Write a channel peer directly into the client's in-memory SQLite cache.
    Pyrogram's :memory: clients start with an empty peer table, so resolve_peer
    always fails. Inserting the row ourselves makes every subsequent high-level
    API call work normally with the plain integer chat_id.
    """
    try:
        storage.conn.execute(
            "INSERT OR REPLACE INTO peers "
            "(id, access_hash, type, username, phone_number) "
            "VALUES (?, ?, 'channel', NULL, NULL)",
            (peer_id, access_hash)
        )
        storage.conn.commit()
    except Exception as e:
        logger.warning(f"inject_peer failed (peer={peer_id}): {e}")

async def get_channels(user_id):
    pool = await get_db()
    return await pool.fetch(
        "SELECT * FROM userbot_channels WHERE user_id=$1", user_id
    )

async def del_channel(user_id, cid):
    pool = await get_db()
    tasks = await pool.fetch(
        "SELECT task_id FROM userbot_tasks_v11 WHERE chat_id=$1", cid
    )
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t["task_id"])
            except Exception: pass
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE chat_id=$1", cid)
    await pool.execute(
        "DELETE FROM userbot_channels WHERE user_id=$1 AND channel_id=$2", user_id, cid
    )

async def save_task(t):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_tasks_v11 (
            task_id, owner_id, chat_id, content_type, content_text,
            file_id, entities, pin, delete_old, repeat_interval,
            start_time, last_msg_id, auto_delete_offset, reply_target,
            src_chat_id, src_msg_id
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        ON CONFLICT (task_id) DO UPDATE SET
            content_type       = $4,
            content_text       = $5,
            file_id            = $6,
            entities           = $7,
            pin                = $8,
            delete_old         = $9,
            repeat_interval    = $10,
            start_time         = $11,
            last_msg_id        = $12,
            auto_delete_offset = $13,
            src_chat_id        = $15,
            src_msg_id         = $16
    """,
        t["task_id"], t["owner_id"], t["chat_id"],
        t["content_type"], t["content_text"], t["file_id"],
        t["entities"], t["pin"], t["delete_old"], t["repeat_interval"],
        t["start_time"], t["last_msg_id"],
        t.get("auto_delete_offset", 0), t.get("reply_target"),
        t.get("src_chat_id", 0), t.get("src_msg_id", 0)
    )

async def get_all_tasks():
    pool = await get_db()
    return [dict(r) for r in await pool.fetch("SELECT * FROM userbot_tasks_v11")]

async def get_user_tasks(user_id, chat_id):
    pool = await get_db()
    return [dict(r) for r in await pool.fetch(
        "SELECT * FROM userbot_tasks_v11 WHERE owner_id=$1 AND chat_id=$2",
        user_id, chat_id
    )]

async def get_single_task(task_id):
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT * FROM userbot_tasks_v11 WHERE task_id=$1", task_id
    )
    return dict(row) if row else None

async def delete_task(task_id):
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT chat_id FROM userbot_tasks_v11 WHERE task_id=$1", task_id
    )
    await pool.execute(
        "DELETE FROM userbot_tasks_v11 WHERE task_id=$1", task_id
    )
    return row["chat_id"] if row else None

async def update_last_msg(task_id, msg_id):
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET last_msg_id=$1 WHERE task_id=$2",
        msg_id, task_id
    )

async def update_next_run(task_id, iso_str):
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET start_time=$1 WHERE task_id=$2",
        iso_str, task_id
    )

async def delete_all_user_data(user_id):
    pool = await get_db()
    session_str = await get_session(user_id)
    if session_str:
        try:
            async def _logout():
                async with Client(
                    ":memory:", api_id=API_ID, api_hash=API_HASH,
                    session_string=session_str,
                    device_model="AutoCast", system_version="Railway",
                    app_version="2.0"
                ) as u:
                    await u.log_out()
            await asyncio.wait_for(_logout(), timeout=6.0)
        except Exception as e:
            logger.warning(f"Session logout skipped: {e}")

    tasks = await pool.fetch(
        "SELECT task_id FROM userbot_tasks_v11 WHERE owner_id=$1", user_id
    )
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t["task_id"])
            except Exception: pass

    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE owner_id=$1",  user_id)
    await pool.execute("DELETE FROM userbot_channels   WHERE user_id=$1",   user_id)
    await pool.execute("DELETE FROM userbot_sessions   WHERE user_id=$1",   user_id)
    await pool.execute("DELETE FROM userbot_settings   WHERE user_id=$1",   user_id)

    tz_cache.pop(user_id, None)
    user_state.pop(user_id, None)
    login_state.pop(user_id, None)

# ─────────────────────────────────────────────────────────────────────────────
#  AUTO-DELETE HELPER
# ─────────────────────────────────────────────────────────────────────────────
async def delete_sent_message(owner_id, chat_id, message_id):
    try:
        session = await get_session(owner_id)
        if not session:
            return
        access_hash = await get_channel_access_hash(owner_id, str(chat_id))
        chat_id_int = int(chat_id)
        async with Client(
            ":memory:", api_id=API_ID, api_hash=API_HASH,
            session_string=session,
            device_model="AutoCast", system_version="Railway", app_version="2.0"
        ) as user:
            inject_peer(user.storage, chat_id_int, access_hash)
            await user.delete_messages(chat_id_int, message_id)
            logger.info(f"🗑 Auto-deleted msg {message_id} in {chat_id}")
    except errors.MessageDeleteForbidden:
        logger.warning(f"Auto-delete forbidden in {chat_id}")
    except errors.MessageIdInvalid:
        logger.info(f"Auto-delete: msg {message_id} already gone")
    except errors.PeerIdInvalid:
        logger.warning(f"Auto-delete: invalid chat {chat_id}")
    except Exception as e:
        logger.error(f"Auto-delete error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  SERIALIZATION
# ─────────────────────────────────────────────────────────────────────────────
def serialize_entities(elist):
    if not elist:
        return None
    return json.dumps([{
        "type": str(e.type), "offset": e.offset, "length": e.length,
        "url": e.url, "language": e.language,
        "custom_emoji_id": e.custom_emoji_id
    } for e in elist])

def deserialize_entities(json_str):
    if not json_str:
        return None
    try:
        items = json.loads(json_str)
        result = []
        for item in items:
            type_str = item["type"].split(".")[-1]
            e_type = getattr(enums.MessageEntityType, type_str)
            result.append(MessageEntity(
                type=e_type, offset=item["offset"], length=item["length"],
                url=item.get("url"), language=item.get("language"),
                custom_emoji_id=item.get("custom_emoji_id")
            ))
        return result
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────────────────────
#  UI HELPERS
# ─────────────────────────────────────────────────────────────────────────────
async def update_menu(m, text, kb, uid, force_new=False):
    markup = InlineKeyboardMarkup(kb) if kb else None
    if force_new:
        try:
            sent = await app.send_message(m.chat.id, text, reply_markup=markup)
            if uid in user_state:
                user_state[uid]["menu_msg_id"] = sent.id
        except Exception as e:
            logger.error(f"update_menu send error: {e}")
        return
    st = user_state.get(uid, {})
    menu_id = st.get("menu_msg_id")
    if menu_id:
        try:
            await app.edit_message_text(m.chat.id, menu_id, text, reply_markup=markup)
            return
        except Exception:
            pass
    try:
        sent = await app.send_message(m.chat.id, text, reply_markup=markup)
        if uid in user_state:
            user_state[uid]["menu_msg_id"] = sent.id
    except Exception as e:
        logger.error(f"update_menu fallback send error: {e}")

async def show_main_menu(m, uid, force_new=False):
    tz = await get_user_tz(uid)
    tz_label = str(tz).replace("_", " ")
    kb = [
        [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
        [InlineKeyboardButton("📢 My Channels",             callback_data="list_channels")],
        [InlineKeyboardButton("➕ Add Channel (Forward)",   callback_data="add_channel_forward"),
         InlineKeyboardButton("➕ Add by ID",               callback_data="add_channel_id")],
        [InlineKeyboardButton(f"🌐 Timezone: {tz_label}",   callback_data="tz_select")],
        [InlineKeyboardButton("🚪 Logout",                  callback_data="logout")],
    ]
    await update_menu(
        m,
        "👋 **Welcome to AutoCast | Channel Manager!**\n\n"
        "Your central hub for managing scheduled posts across your Telegram channels.\n"
        "Select an option below to get started.",
        kb, uid, force_new
    )

async def show_channels(uid, m, force_new=False):
    chs = await get_channels(uid)
    if not chs:
        kb = [
            [InlineKeyboardButton("➕ Add Channel (Forward)", callback_data="add_channel_forward")],
            [InlineKeyboardButton("➕ Add by ID",             callback_data="add_channel_id")],
            [InlineKeyboardButton("🔙 Back",                  callback_data="menu_home")],
        ]
        await update_menu(
            m,
            "❌ **No Channels Linked Yet.**\n\nUse '➕ Add Channel' to link your first channel.",
            kb, uid, force_new
        )
        return
    kb = [[InlineKeyboardButton(c["title"], callback_data=f"ch_{c['channel_id']}")] for c in chs]
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await update_menu(m, "**📢 Your Linked Channels**\n\nSelect a channel to manage:", kb, uid, force_new)

async def show_channel_options(uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    kb = [
        [InlineKeyboardButton("✍️ Schedule Post",               callback_data=f"new_{cid}")],
        [InlineKeyboardButton(f"📅 Scheduled ({len(tasks)})",   callback_data=f"tasks_{cid}")],
        [InlineKeyboardButton("🗑 Unlink Channel",              callback_data=f"rem_{cid}"),
         InlineKeyboardButton("🔙 Back",                        callback_data="list_channels")],
    ]
    await update_menu(m, "⚙️ **Managing Channel**", kb, uid, force_new)

async def show_time_menu(m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("⚡️ Now (5s delay)",   callback_data="time_0")],
        [InlineKeyboardButton("5 Minutes",            callback_data="time_5"),
         InlineKeyboardButton("15 Minutes",           callback_data="time_15")],
        [InlineKeyboardButton("30 Minutes",           callback_data="time_30"),
         InlineKeyboardButton("1 Hour",               callback_data="time_60")],
        [InlineKeyboardButton("📅 Custom Date/Time",  callback_data="time_custom")],
        [InlineKeyboardButton("🔙 Back",              callback_data="menu_home")],
    ]
    tz = await get_user_tz(uid)
    await update_menu(
        m,
        f"2️⃣ **Schedule Time** _(your timezone: {tz})_\n\nWhen would you like this post to be sent?",
        kb, uid, force_new
    )

async def ask_repetition(m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("🔂 Once (No Repeat)",   callback_data="rep_0")],
        [InlineKeyboardButton("Every 1 Hour",          callback_data="rep_60"),
         InlineKeyboardButton("Every 3 Hours",         callback_data="rep_180")],
        [InlineKeyboardButton("Every 6 Hours",         callback_data="rep_360"),
         InlineKeyboardButton("Every 12 Hours",        callback_data="rep_720")],
        [InlineKeyboardButton("Every 24 Hours",        callback_data="rep_1440")],
        [InlineKeyboardButton("🔙 Back",               callback_data="step_time")],
    ]
    await update_menu(m, "3️⃣ **Repetition**\n\nHow often should this post repeat?", kb, uid, force_new)

async def ask_settings(m, uid, force_new=False):
    st = user_state[uid]
    queue = st.get("broadcast_queue")

    if queue:
        txt = (
            "4️⃣ **Batch Post Settings**\n\n"
            "📌 **P** = Pin | 🗑 **D** = Delete Previous | ⏰ **Off** = Auto-Delete After\n\n"
            "👇 Configure each post:"
        )
        kb = []
        for i, post in enumerate(queue):
            p  = "✅" if post.get("pin")        else "❌"
            d  = "✅" if post.get("delete_old") else "❌"
            off = post.get("auto_delete_offset", 0)
            off_s = f"{off}m" if off > 0 else "OFF"
            kb.append([InlineKeyboardButton(
                f"Post #{i+1} | P:{p} D:{d} ⏰:{off_s}",
                callback_data=f"cfg_q_{i}"
            )])
        kb.append([InlineKeyboardButton("➡️ Confirm All", callback_data="goto_confirm")])
        kb.append([InlineKeyboardButton("🔙 Back",        callback_data="step_rep")])
        await update_menu(m, txt, kb, uid, force_new)
        return

    st.setdefault("pin", True)
    st.setdefault("del", True)
    offset   = st.get("auto_delete_offset", 0)
    pin_icon = "✅" if st["pin"] else "❌"
    del_icon = "✅" if st["del"] else "❌"
    off_text = f"⏰ Auto-Delete: {offset}m after posting" if offset > 0 else "⏰ Auto-Delete: OFF"
    kb = [
        [InlineKeyboardButton(f"📌 Pin Message: {pin_icon}",    callback_data="toggle_pin")],
        [InlineKeyboardButton(f"🗑 Delete Previous: {del_icon}", callback_data="toggle_del")],
        [InlineKeyboardButton(off_text,                          callback_data="wizard_ask_offset")],
        [InlineKeyboardButton("➡️ Confirm",                     callback_data="goto_confirm")],
        [InlineKeyboardButton("🔙 Back",  callback_data=st.get("editing_task_id") and f'view_{st.get("editing_task_id")}' or "step_rep")],
    ]
    await update_menu(
        m,
        f"4️⃣ **Post Settings**\n\nConfigure how your post behaves.\n"
        f"Auto-delete: **{'OFF' if not offset else str(offset)+' minutes after posting'}**",
        kb, uid, force_new=False
    )

async def confirm_task(m, uid, force_new=False):
    st = user_state[uid]
    tz = await get_user_tz(uid)
    t_str = st["start_time"].astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
    # FIX B10: use .get() to avoid KeyError
    interval = st.get("interval")
    r_str = interval if interval else "Once"
    queue = st.get("broadcast_queue")

    if queue:
        type_str   = f"📦 Batch ({len(queue)} Posts)"
        pin_count  = sum(1 for p in queue if p.get("pin"))
        settings_str = f"📌 Pinning: {pin_count}/{len(queue)} Posts"
    else:
        type_map = {
            "text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
            "audio": "🎵 Audio", "voice": "🎙 Voice", "document": "📁 File",
            "poll": "📊 Poll", "animation": "🎞 GIF", "sticker": "✨ Sticker"
        }
        c_type     = st.get("content_type", "unknown")
        type_str   = type_map.get(c_type, c_type.upper())
        settings_str = (
            f"📌 Pin: {'✅' if st.get('pin', True) else '❌'} | "
            f"🗑 Del Old: {'✅' if st.get('del', True) else '❌'}"
        )

    txt = (
        f"✅ **Confirm Schedule**\n\n"
        f"📢 **Content:** {type_str}\n"
        f"📅 **Time:** `{t_str}`\n"
        f"🔁 **Repeat:** `{r_str}`\n"
        f"{settings_str}"
    )
    kb = [
        [InlineKeyboardButton("✅ Schedule It!", callback_data="save_task")],
        [InlineKeyboardButton("🔙 Back",         callback_data="step_settings")],
    ]
    await update_menu(m, txt, kb, uid, force_new)

async def list_active_tasks(uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    if not tasks:
        await update_menu(
            m, "✅ No scheduled tasks for this channel.",
            [[InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")]],
            uid, force_new
        )
        return
    tasks.sort(key=lambda x: x["start_time"])
    tz       = await get_user_tz(uid)
    icons    = {"text": "📝", "photo": "📷", "video": "📹", "audio": "🎵", "poll": "📊"}
    kb = []
    for t in tasks:
        snippet = (t["content_text"] or "Media")[:15] + "…"
        icon    = icons.get(t["content_type"], "📁")
        try:
            dt = datetime.datetime.fromisoformat(t["start_time"])
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            time_str = dt.astimezone(tz).strftime("%I:%M %p")
        except Exception:
            time_str = "?"
        kb.append([InlineKeyboardButton(
            f"{icon} {snippet} | ⏰ {time_str}",
            callback_data=f"view_{t['task_id']}"
        )])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")])
    await update_menu(m, "**📅 Scheduled Tasks** — select one to manage:", kb, uid, force_new)

async def show_task_details(uid, m, tid):
    t = await get_single_task(tid)
    if not t:
        await update_menu(
            m, "❌ Task not found.",
            [[InlineKeyboardButton("🏠 Home", callback_data="menu_home")]],
            uid
        )
        return
    tz = await get_user_tz(uid)
    try:
        dt = datetime.datetime.fromisoformat(t["start_time"])
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        time_str = dt.astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
    except Exception:
        time_str = t["start_time"]
    type_map = {
        "text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
        "audio": "🎵 Audio", "poll": "📊 Poll", "sticker": "✨ Sticker",
        "voice": "🎙 Voice", "document": "📁 File", "animation": "🎞 GIF"
    }
    type_str = type_map.get(t["content_type"], "📁 File")
    txt = (
        f"⚙️ **Task Details**\n\n"
        f"📂 **Type:** {type_str}\n"
        f"📝 **Content:** `{(t['content_text'] or 'Media')[:60]}…`\n"
        f"📅 **Scheduled:** `{time_str}`\n"
        f"🔁 **Repeat:** `{t['repeat_interval'] or 'Once'}`\n"
        f"📌 **Pin:** {'✅' if t['pin'] else '❌'} | "
        f"🗑 **Del Old:** {'✅' if t['delete_old'] else '❌'} | "
        f"⏰ **Auto-Delete:** {str(t.get('auto_delete_offset') or 0)+'m' if t.get('auto_delete_offset') else 'OFF'}"
    )
    kb = [
        [InlineKeyboardButton("👁 View Post",      callback_data=f"prev_{tid}"),
         InlineKeyboardButton("✏️ Change Post",   callback_data=f"edit_content_{tid}")],
        [InlineKeyboardButton("🗓 Reschedule",     callback_data=f"reschedule_{tid}"),
         InlineKeyboardButton("🔁 Repetition",    callback_data=f"edit_repeat_{tid}")],
        [InlineKeyboardButton("⚙️ Settings",      callback_data=f"edit_settings_{tid}")],
        [InlineKeyboardButton("🗑 Delete Task",    callback_data=f"del_task_{tid}")],
        [InlineKeyboardButton("🔙 Back to List",   callback_data=f"back_list_{t['chat_id']}")],
    ]
    await update_menu(m, txt, kb, uid)


async def show_broadcast_selection(uid, m):
    chs = await get_channels(uid)
    if not chs:
        await update_menu(
            m, "❌ No channels found. Add a channel first.",
            [[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]],
            uid
        )
        return
    targets = user_state[uid].get("broadcast_targets", [])
    kb = []
    for c in chs:
        icon = "✅" if c["channel_id"] in targets else "⬜"
        kb.append([InlineKeyboardButton(
            f"{icon} {c['title']}", callback_data=f"toggle_bc_{c['channel_id']}"
        )])
    kb.append([InlineKeyboardButton(
        f"➡️ Done ({len(targets)} selected)", callback_data="broadcast_confirm"
    )])
    kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")])
    await update_menu(m, "📢 **Broadcast Mode**\n\nSelect channels to post to:", kb, uid)

# ─────────────────────────────────────────────────────────────────────────────
#  TIMEZONE SELECTOR UI
# ─────────────────────────────────────────────────────────────────────────────
async def show_tz_selector(uid, m, force_new=False):
    current_tz = await get_user_tz(uid)
    kb = []
    for label, tz_str in POPULAR_TIMEZONES:
        tick = "✅ " if str(current_tz) == tz_str else ""
        kb.append([InlineKeyboardButton(
            f"{tick}{label}", callback_data=f"set_tz_{tz_str}"
        )])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await update_menu(
        m,
        f"🌐 **Select Your Timezone**\n\nCurrent: `{current_tz}`\n\n"
        "This affects how scheduled times are displayed and entered.",
        kb, uid, force_new
    )

# ─────────────────────────────────────────────────────────────────────────────
#  AUTO-DELETE KEYBOARD
# ─────────────────────────────────────────────────────────────────────────────
async def get_delete_before_kb(temp_task_id):
    options = [
        (0, "🚫 Disable"), (5, "5 min"), (10, "10 min"), (15, "15 min"),
        (30, "30 min"), (60, "1 hr"), (120, "2 hrs"), (180, "3 hrs"),
        (360, "6 hrs"), (720, "12 hrs"), (1440, "24 hrs"),
    ]
    kb, row = [], []
    for mins, label in options:
        row.append(InlineKeyboardButton(
            label, callback_data=f"set_del_off_{temp_task_id}_{mins}"
        ))
        if len(row) == 3:
            kb.append(row); row = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="step_settings")])
    return InlineKeyboardMarkup(kb)

# ─────────────────────────────────────────────────────────────────────────────
#  BOT COMMANDS
# ─────────────────────────────────────────────────────────────────────────────
@app.on_message(filters.command(["start", "manage"]))
async def start_cmd(c, m):
    uid = m.from_user.id
    if uid not in user_state:
        user_state[uid] = {}
    if await get_session(uid):
        sent = await m.reply(
            "👋 **Welcome to AutoCast | Channel Manager!**\n\n"
            "Your central hub for managing scheduled posts across your Telegram channels.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
                [InlineKeyboardButton("📢 My Channels",             callback_data="list_channels")],
                [InlineKeyboardButton("➕ Add Channel (Forward)",   callback_data="add_channel_forward"),
                 InlineKeyboardButton("➕ Add by ID",               callback_data="add_channel_id")],
                [InlineKeyboardButton("🌐 Set Timezone",            callback_data="tz_select")],
                [InlineKeyboardButton("🚪 Logout",                  callback_data="logout")],
            ])
        )
        user_state[uid]["menu_msg_id"] = sent.id
    else:
        await m.reply(
            "👋 **Welcome to AutoCast | Channel Manager!**\n\n"
            "Schedule and manage posts across your Telegram channels with ease.\n\n"
            "**Getting Started:**\n"
            "1️⃣ Login with your Telegram account\n"
            "2️⃣ Add your channels\n"
            "3️⃣ Schedule posts!\n\n"
            "👇 Click **Login** to begin:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔐 Login Account", callback_data="login_start")
            ]])
        )

# ─────────────────────────────────────────────────────────────────────────────
#  CALLBACK ROUTER
# ─────────────────────────────────────────────────────────────────────────────
@app.on_callback_query()
async def callback_router(c, q):
    uid = q.from_user.id
    d   = q.data

    if uid not in user_state:
        user_state[uid] = {}
    user_state[uid]["menu_msg_id"] = q.message.id

    try:
        await _handle_callback(c, q, uid, d)
    except Exception as e:
        logger.error(f"Callback error [{d}] uid={uid}: {e}", exc_info=True)
        try:
            await q.answer("⚠️ Something went wrong. Please try again.", show_alert=True)
        except Exception:
            pass

async def _handle_callback(c, q, uid, d):
    # ── HOME ──────────────────────────────────────────────────────────────────
    if d == "menu_home":
        user_state[uid]["step"] = None
        await show_main_menu(q.message, uid)

    # ── TIMEZONE ─────────────────────────────────────────────────────────────
    elif d == "tz_select":
        await show_tz_selector(uid, q.message)

    elif d.startswith("set_tz_"):
        tz_str = d[len("set_tz_"):]
        try:
            pytz.timezone(tz_str)   # validate
            await set_user_tz(uid, tz_str)
            await q.answer(f"✅ Timezone set to {tz_str}!", show_alert=False)
        except Exception:
            await q.answer("❌ Invalid timezone.", show_alert=True)
        await show_main_menu(q.message, uid)

    # ── LOGIN ─────────────────────────────────────────────────────────────────
    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await update_menu(
            q.message,
            "📱 **Step 1: Phone Number**\n\n"
            "Send your Telegram phone number with country code.\n"
            "Example: `+919876543210`",
            [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]],
            uid
        )

    # ── LOGOUT ────────────────────────────────────────────────────────────────
    elif d == "logout":
        pool = await get_db()
        count = await pool.fetchval(
            "SELECT COUNT(*) FROM userbot_tasks_v11 WHERE owner_id=$1", uid
        )
        kb = [
            [InlineKeyboardButton("⚠️ Yes, Logout", callback_data="logout_step_2")],
            [InlineKeyboardButton("🔙 Cancel",       callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            f"⚠️ **Logout Confirmation**\n\n"
            f"You have **{count} active task(s)** scheduled.\n"
            f"Logging out will stop all scheduled posts.",
            kb, uid
        )

    elif d == "logout_step_2":
        kb = [
            [InlineKeyboardButton("🗑 Delete Everything & Logout", callback_data="logout_final")],
            [InlineKeyboardButton("🔙 Go Back",                    callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            "🛑 **FINAL WARNING**\n\n"
            "This will permanently delete all your scheduled posts, "
            "channels, and session. This cannot be undone.\n\n"
            "Are you absolutely sure?",
            kb, uid
        )

    elif d == "logout_final":
        try:
            await app.edit_message_text(
                uid, q.message.id,
                "⏳ **Logging out…**\nTerminating session and wiping data."
            )
        except Exception:
            pass
        await delete_all_user_data(uid)
        try:
            await app.send_message(
                uid,
                "👋 **Logged out successfully.**\n\n"
                "All data has been wiped. Use /start to begin again."
            )
        except Exception:
            pass

    # ── TASK VIEW / DELETE ────────────────────────────────────────────────────
    elif d.startswith("view_"):
        await show_task_details(uid, q.message, d[5:])

    elif d.startswith("back_list_"):
        await list_active_tasks(uid, q.message, d[10:])

    elif d.startswith("del_task_"):
        tid = d[9:]
        # FIX B8: wrap remove_job separately so JobLookupError doesn't show error to user
        if scheduler:
            try: scheduler.remove_job(tid)
            except Exception: pass
        cid = await delete_task(tid)
        await q.answer("✅ Task deleted!")
        if cid:
            await list_active_tasks(uid, q.message, cid)
        else:
            await show_main_menu(q.message, uid)

    elif d.startswith("prev_"):
        tid  = d[5:]
        task = await get_single_task(tid)
        if not task:
            await q.answer("Task not found.", show_alert=True)
            return
        type_map = {
            "text": "Text", "photo": "Photo", "video": "Video",
            "audio": "Audio", "poll": "Poll", "sticker": "Sticker",
            "voice": "Voice", "document": "File", "animation": "GIF"
        }
        ct  = task["content_type"]
        tz  = await get_user_tz(uid)
        try:
            dt = datetime.datetime.fromisoformat(task["start_time"])
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            time_str = dt.astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
        except Exception:
            time_str = "?"
        back_kb = [[InlineKeyboardButton("Back to Task", callback_data=f"view_{tid}")]]
        if ct == "text":
            body    = task["content_text"] or "(empty)"
            rep_str = task["repeat_interval"] or "Once"
            preview = "Post Content\n" + ("-"*20) + "\n" + body + "\n" + ("-"*20) + "\nScheduled: " + time_str + "  |  Repeat: " + rep_str
            await update_menu(q.message, preview, back_kb, uid)
        else:
            type_label = type_map.get(ct, ct.upper())
            rep_str2   = task["repeat_interval"] or "Once"
            summary    = "Post Preview - " + type_label + "\n\nScheduled: " + time_str + "\nRepeat: " + rep_str2 + "\n\nMedia sent below (you can copy it)"
            await update_menu(q.message, summary, back_kb, uid)
            sent_media = False
            for src_chat, src_msg in [
                (task.get("src_chat_id"), task.get("src_msg_id")),
                (int(task["chat_id"]) if task.get("last_msg_id") else None, task.get("last_msg_id")),
            ]:
                if src_chat and src_msg:
                    try:
                        await app.copy_message(uid, from_chat_id=src_chat, message_id=src_msg)
                        sent_media = True
                        break
                    except Exception:
                        pass
            if not sent_media:
                cap = task.get("content_text") or ""
                await app.send_message(uid, "Cannot retrieve media." + ("\n\nCaption: " + cap[:300] if cap else ""))

    elif d.startswith("edit_content_"):
        tid = d[13:]
        task = await get_single_task(tid)
        if not task:
            await q.answer("❌ Task not found.", show_alert=True)
            return
        user_state[uid]["editing_task_id"] = tid
        user_state[uid]["step"] = "waiting_content_edit"
        await update_menu(
            q.message,
            "✏️ **Change Post Content**\n\n"
            "Send me the new content for this task:\n"
            "• Text  • Photo  • Video  • Audio\n"
            "• Voice Note  • Document  • Poll  • Sticker",
            [[InlineKeyboardButton("🔙 Cancel", callback_data=f"view_{tid}")]],
            uid
        )

    elif d.startswith("reschedule_"):
        tid = d[11:]
        task = await get_single_task(tid)
        if not task:
            await q.answer("❌ Task not found.", show_alert=True)
            return
        # Preload existing task into state for re-use
        user_state[uid]["editing_task_id"] = tid
        user_state[uid]["content_type"]    = task["content_type"]
        user_state[uid]["content_text"]    = task["content_text"]
        user_state[uid]["file_id"]         = task["file_id"]
        user_state[uid]["entities"]        = task["entities"]
        user_state[uid]["pin"]             = task["pin"]
        user_state[uid]["del"]             = task["delete_old"]
        user_state[uid]["auto_delete_offset"] = task.get("auto_delete_offset", 0)
        user_state[uid]["interval"]        = task["repeat_interval"]
        user_state[uid]["step"]            = "rescheduling"
        await show_time_menu(q.message, uid, force_new=False)

    elif d.startswith("edit_settings_"):
        tid = d[14:]
        task = await get_single_task(tid)
        if not task:
            await q.answer("❌ Task not found.", show_alert=True)
            return
        user_state[uid]["editing_task_id"] = tid
        user_state[uid]["content_type"]    = task["content_type"]
        user_state[uid]["content_text"]    = task["content_text"]
        user_state[uid]["file_id"]         = task["file_id"]
        user_state[uid]["entities"]        = task["entities"]
        user_state[uid]["pin"]             = task["pin"]
        user_state[uid]["del"]             = task["delete_old"]
        user_state[uid]["auto_delete_offset"] = task.get("auto_delete_offset", 0)
        user_state[uid]["interval"]        = task["repeat_interval"]
        try:
            dt = datetime.datetime.fromisoformat(task["start_time"])
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            user_state[uid]["start_time"] = dt
        except Exception:
            pass
        await ask_settings(q.message, uid)

    elif d.startswith("edit_repeat_"):
        tid = d[12:]
        task = await get_single_task(tid)
        if not task:
            await q.answer("❌ Task not found.", show_alert=True)
            return
        # Load task into state so rep_ handler can save it
        user_state[uid]["editing_task_id"] = tid
        user_state[uid]["content_type"]    = task["content_type"]
        user_state[uid]["content_text"]    = task["content_text"]
        user_state[uid]["file_id"]         = task["file_id"]
        user_state[uid]["entities"]        = task["entities"]
        user_state[uid]["pin"]             = task["pin"]
        user_state[uid]["del"]             = task["delete_old"]
        user_state[uid]["auto_delete_offset"] = task.get("auto_delete_offset", 0)
        user_state[uid]["interval"]        = task["repeat_interval"]
        try:
            dt = datetime.datetime.fromisoformat(task["start_time"])
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            user_state[uid]["start_time"] = dt
        except Exception:
            pass
        # Show repetition menu — back goes to task details
        kb = [
            [InlineKeyboardButton("🔂 Once (No Repeat)",   callback_data="rep_0")],
            [InlineKeyboardButton("Every 1 Hour",          callback_data="rep_60"),
             InlineKeyboardButton("Every 3 Hours",         callback_data="rep_180")],
            [InlineKeyboardButton("Every 6 Hours",         callback_data="rep_360"),
             InlineKeyboardButton("Every 12 Hours",        callback_data="rep_720")],
            [InlineKeyboardButton("Every 24 Hours",        callback_data="rep_1440")],
            [InlineKeyboardButton("🔙 Back",               callback_data=f"view_{tid}")],
        ]
        await update_menu(q.message, "🔁 **Change Repetition**\n\nHow often should this task repeat?", kb, uid)

    # ── BROADCAST ─────────────────────────────────────────────────────────────
    elif d == "broadcast_start":
        user_state[uid]["step"] = "broadcast_select_channels"
        user_state[uid]["broadcast_targets"] = []
        await show_broadcast_selection(uid, q.message)

    elif d.startswith("toggle_bc_"):
        cid     = d[10:]
        targets = user_state[uid].get("broadcast_targets", [])
        if cid in targets: targets.remove(cid)
        else: targets.append(cid)
        user_state[uid]["broadcast_targets"] = targets
        await show_broadcast_selection(uid, q.message)

    elif d == "broadcast_confirm":
        targets = user_state[uid].get("broadcast_targets", [])
        if not targets:
            await q.answer("❌ Select at least one channel!", show_alert=True)
            return
        user_state[uid]["broadcast_queue"] = []
        user_state[uid]["step"] = "waiting_broadcast_content"
        markup = ReplyKeyboardMarkup(
            [[KeyboardButton("✅ Done Adding Posts")],
             [KeyboardButton("❌ Cancel")]],
            resize_keyboard=True, one_time_keyboard=True
        )
        await app.send_message(
            q.message.chat.id,
            f"📢 **Multi-Post Mode Active** — {len(targets)} channel(s) selected\n\n"
            "**How to use:**\n"
            "1️⃣ Send posts one by one (text, photo, video…)\n"
            "2️⃣ Reply to a post here to make a thread reply\n"
            "3️⃣ Tap **✅ Done** when finished\n\n"
            "_Configure Pin/Delete settings per post after adding._",
            reply_markup=markup
        )

    # ── CHANNEL MANAGEMENT ────────────────────────────────────────────────────
    elif d == "list_channels":
        await show_channels(uid, q.message)

    elif d == "add_channel_forward":
        user_state[uid]["step"] = "waiting_forward"
        await update_menu(
            q.message,
            "📩 **Add Channel — Forward Method**\n\n"
            "Forward any message from your channel here.\n"
            "I will detect the channel automatically.",
            [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]],
            uid
        )

    elif d == "add_channel_id":
        user_state[uid]["step"] = "waiting_channel_id"
        await update_menu(
            q.message,
            "🔢 **Add Channel — ID Method**\n\n"
            "Send the **Channel ID** (a negative number starting with `-100`).\n\n"
            "**How to find it:** Forward a message from your channel to @JsonDumpBot "
            "and copy `forward_from_chat.id`.",
            [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]],
            uid
        )

    elif d.startswith("ch_"):
        await show_channel_options(uid, q.message, d[3:])

    elif d.startswith("rem_"):
        cid = d[4:]
        await del_channel(uid, cid)
        await q.answer("✅ Channel unlinked!")
        await show_channels(uid, q.message)

    elif d.startswith("new_"):
        cid = d[4:]
        user_state[uid].update({"step": "waiting_content", "target": cid})
        await update_menu(
            q.message,
            "1️⃣ **Create Post**\n\nSend me the content to schedule:\n"
            "• Text  • Photo  • Video  • Audio\n"
            "• Voice Note  • Document  • Poll  • Sticker",
            [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]],
            uid
        )

    elif d.startswith("tasks_"):
        await list_active_tasks(uid, q.message, d[6:])

    # ── WIZARD NAVIGATION ─────────────────────────────────────────────────────
    elif d == "step_time":     await show_time_menu(q.message, uid)
    elif d == "step_rep":      await ask_repetition(q.message, uid)
    elif d == "step_settings": await ask_settings(q.message, uid)

    # ── TIME SELECTION ────────────────────────────────────────────────────────
    elif d.startswith("time_"):
        offset_str = d[5:]
        tz = await get_user_tz(uid)
        if offset_str == "custom":
            user_state[uid]["step"] = "waiting_custom_date"
            cur = now_in(tz).strftime("%d-%b-%Y %I:%M %p")
            await update_menu(
                q.message,
                f"📅 **Custom Date & Time**\n\n"
                f"Your timezone: `{tz}`\n"
                f"Current time: `{cur}`\n\n"
                f"Send date/time in this exact format:\n"
                f"`DD-Mon-YYYY HH:MM AM/PM`\n\n"
                f"Example: `{cur}`",
                [[InlineKeyboardButton("🔙 Back", callback_data="step_time")]],
                uid
            )
            return
        now = now_in(tz)
        if offset_str == "0":
            run_time = now + datetime.timedelta(seconds=5)
        else:
            run_time = (now + datetime.timedelta(minutes=int(offset_str))).replace(
                second=0, microsecond=0
            )
        user_state[uid]["start_time"] = run_time
        # If rescheduling an existing task, skip to confirm directly
        if user_state[uid].get("step") == "rescheduling":
            await confirm_task(q.message, uid)
        else:
            await ask_repetition(q.message, uid)

    # ── REPETITION ────────────────────────────────────────────────────────────
    elif d.startswith("rep_"):
        val = d[4:]
        user_state[uid]["interval"] = f"minutes={val}" if val != "0" else None
        # If editing an existing task, save immediately and go back to task details
        editing_tid = user_state[uid].get("editing_task_id")
        if editing_tid:
            await update_task_logic(uid, q)
        else:
            await ask_settings(q.message, uid)

    # ── SETTINGS TOGGLES ─────────────────────────────────────────────────────
    elif d == "toggle_pin":
        st = user_state[uid]; st.setdefault("pin", True)
        st["pin"] = not st["pin"]
        await ask_settings(q.message, uid)

    elif d == "toggle_del":
        st = user_state[uid]; st.setdefault("del", True)
        st["del"] = not st["del"]
        await ask_settings(q.message, uid)

    # ── AUTO-DELETE OFFSET ────────────────────────────────────────────────────
    elif d.startswith("wizard_ask_offset"):
        parts = d.split("_")
        temp_id = f"WIZARD_{parts[3]}" if len(parts) > 3 else "WIZARD"
        markup = await get_delete_before_kb(temp_id)
        await update_menu(
            q.message,
            "⏳ **Auto-Delete Timing**\n\n"
            "How long after posting should the message be automatically deleted?",
            markup.inline_keyboard, uid
        )

    elif d.startswith("set_del_off_WIZARD"):
        parts = d.split("_")
        if len(parts) == 5:                      # single post
            user_state[uid]["auto_delete_offset"] = int(parts[4])
            await q.answer(f"✅ Set to {parts[4]}m" if int(parts[4]) > 0 else "✅ Disabled")
        elif len(parts) == 6:                    # batch post
            idx = int(parts[4]); offset = int(parts[5])
            user_state[uid]["broadcast_queue"][idx]["auto_delete_offset"] = offset
            await q.answer(f"✅ Post #{idx+1}: {offset}m" if offset > 0 else "✅ Disabled")
        await ask_settings(q.message, uid)

    # ── BATCH POST INDIVIDUAL CONFIG ──────────────────────────────────────────
    elif d.startswith("cfg_q_"):
        idx  = int(d[6:])
        post = user_state[uid]["broadcast_queue"][idx]
        p_s  = "✅ Enabled" if post.get("pin")        else "❌ Disabled"
        d_s  = "✅ Enabled" if post.get("delete_old") else "❌ Disabled"
        off  = post.get("auto_delete_offset", 0)
        kb   = [
            [InlineKeyboardButton(f"📌 Pin: {p_s}",         callback_data=f"t_q_pin_{idx}")],
            [InlineKeyboardButton(f"🗑 Delete Prev: {d_s}", callback_data=f"t_q_del_{idx}")],
            [InlineKeyboardButton(
                f"⏰ Auto-Delete: {off}m" if off > 0 else "⏰ Auto-Delete: OFF",
                callback_data=f"wizard_ask_offset_{idx}"
            )],
            [InlineKeyboardButton("🔙 Back", callback_data="step_settings")],
        ]
        await update_menu(
            q.message,
            f"⚙️ **Post #{idx+1} Settings**\n\n"
            f"Type: **{post['content_type']}**",
            kb, uid
        )

    elif d.startswith("t_q_"):
        parts  = d.split("_")
        action = parts[2]; idx = int(parts[3])
        post   = user_state[uid]["broadcast_queue"][idx]
        if action == "pin": post["pin"]        = not post.get("pin", True)
        if action == "del": post["delete_old"] = not post.get("delete_old", True)
        q.data = f"cfg_q_{idx}"
        await _handle_callback(c, q, uid, f"cfg_q_{idx}")

    # ── CONFIRM & SAVE ────────────────────────────────────────────────────────
    elif d == "goto_confirm":
        await confirm_task(q.message, uid)

    elif d == "save_task":
        if user_state[uid].get("editing_task_id"):
            await update_task_logic(uid, q)
        else:
            await create_task_logic(uid, q)

    else:
        await q.answer("Unknown action.", show_alert=False)

# ─────────────────────────────────────────────────────────────────────────────
#  MESSAGE HANDLER
# ─────────────────────────────────────────────────────────────────────────────
@app.on_message(filters.private & ~filters.command(["start", "manage"]))
async def handle_inputs(c, m):
    uid  = m.from_user.id
    # Safe text — never None
    text = (m.text or m.caption or "").strip()

    # ── LOGIN FLOW ────────────────────────────────────────────────────────────
    if uid in login_state:
        await _handle_login(c, m, uid, text)
        return

    # ── FIX B5: Check keyboard buttons BEFORE checking step ──────────────────
    if text == "✅ Done Adding Posts":
        # FIX B6: go to time menu, not confirm_task (start_time not set yet)
        if uid not in user_state:
            user_state[uid] = {}
        # Remove reply keyboard and ask for schedule time
        await m.reply("✅ All posts added!", reply_markup=ReplyKeyboardRemove())
        await show_time_menu(m, uid, force_new=True)
        return

    if text == "❌ Cancel":
        user_state.pop(uid, None)
        await m.reply("Operation cancelled.", reply_markup=ReplyKeyboardRemove())
        await start_cmd(c, m)
        return

    # ── STEP-BASED HANDLERS ───────────────────────────────────────────────────
    st   = user_state.get(uid, {})
    step = st.get("step")

    if step == "waiting_content":
        await process_content_message(c, m, uid)

    elif step == "waiting_content_edit":
        await process_content_edit_message(c, m, uid)

    elif step == "waiting_broadcast_content":
        await process_broadcast_content_message(c, m, uid)

    elif step == "waiting_custom_date":
        await process_custom_date(c, m, uid)

    elif step == "waiting_forward":
        if m.forward_from_chat:
            await handle_forward_add(c, m, uid)
        else:
            await m.reply(
                "⚠️ Please forward a message **from your channel**, not send a new one.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")
                ]])
            )

    elif step == "waiting_channel_id":
        await handle_channel_id_input(c, m, uid, text)

    else:
        # FIX B9: use `text` (safe), not `m.text` (may be None)
        if text and not text.startswith("/"):
            await m.reply(
                "Use the menu to interact with the bot. Type /start to open it."
            )

# ─────────────────────────────────────────────────────────────────────────────
#  LOGIN FLOW
# ─────────────────────────────────────────────────────────────────────────────
async def _handle_login(c, m, uid, text):
    st = login_state[uid]

    if st["step"] == "waiting_phone":
        wait = await m.reply("⏳ Connecting to Telegram… please wait.")
        try:
            tmp = Client(
                ":memory:", api_id=API_ID, api_hash=API_HASH,
                device_model="AutoCast", system_version="Railway", app_version="2.0"
            )
            await tmp.connect()
            sent_code = await tmp.send_code(text)
            st.update({
                "client": tmp, "phone": text,
                "hash": sent_code.phone_code_hash,
                "step": "waiting_code"
            })
            await wait.delete()
            await update_menu(
                m,
                "📩 **Step 2: Verification Code**\n\n"
                "Telegram sent a code to your account.\n\n"
                "⚠️ **Prefix your code with `aa`** to prevent expiry.\n"
                "Example: if code is `12345`, send `aa12345`",
                None, uid, force_new=True
            )
        except Exception as e:
            await wait.delete()
            await m.reply(f"❌ **Connection failed:** {e}\n\nCheck your phone number and try /start again.")

    elif st["step"] == "waiting_code":
        try:
            code = text.lower().replace("aa", "").strip()
            await st["client"].sign_in(st["phone"], st["hash"], code)
            session = await st["client"].export_session_string()
            await save_session(uid, session)
            await st["client"].disconnect()
            login_state.pop(uid, None)
            await m.reply(
                "✅ **Login Successful!**\n\nClick /manage to start scheduling.",
                reply_markup=ReplyKeyboardRemove()
            )
        except errors.PhoneCodeInvalid:
            await m.reply("❌ Wrong code. Try again (remember to prefix with `aa`).")
        except errors.SessionPasswordNeeded:
            st["step"] = "waiting_password"
            await update_menu(
                m,
                "🔒 **Two-Step Verification**\n\nEnter your cloud password:",
                None, uid, force_new=True
            )
        except Exception as e:
            logger.error(f"Login code error uid={uid}: {e}")
            await m.reply(f"❌ Login error: {e}\n\nTry /start again.")

    elif st["step"] == "waiting_password":
        try:
            await st["client"].check_password(text)
            session = await st["client"].export_session_string()
            await save_session(uid, session)
            await st["client"].disconnect()
            login_state.pop(uid, None)
            await m.reply(
                "✅ **Login Successful!**\n\nClick /manage to start scheduling.",
                reply_markup=ReplyKeyboardRemove()
            )
        except errors.PasswordHashInvalid:
            await m.reply("❌ Wrong password. Try again.")
        except Exception as e:
            logger.error(f"Login password error uid={uid}: {e}")
            await m.reply(f"❌ Error: {e}\n\nTry /start again.")

# ─────────────────────────────────────────────────────────────────────────────
#  CHANNEL ADD HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
async def handle_forward_add(c, m, uid):
    cid   = m.forward_from_chat.id
    title = m.forward_from_chat.title or "Private Channel"
    session = await get_session(uid)
    if not session:
        await m.reply("❌ Session expired. Please /start and login again.")
        return

    access_hash = 0
    try:
        async with Client(
            ":memory:", api_id=API_ID, api_hash=API_HASH,
            session_string=session,
            device_model="AutoCast", system_version="Railway", app_version="2.0"
        ) as user_client:
            # warm_peer iterates the user's own dialogs — this gives us the
            # user-account-specific access_hash, which is the only one that works
            access_hash = await warm_peer_and_get_hash(user_client, uid, cid)
            # Also try to get a better title if dialogs returned the chat
            try:
                peer = await user_client.storage.get_peer_by_id(cid)
                # peer found — get_chat will work now (peer is cached)
                chat = await user_client.get_chat(cid)
                title = chat.title or title
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"handle_forward_add session error for {cid}: {e}")

    await add_channel(uid, str(cid), title, access_hash)
    user_state[uid]["step"] = None
    await m.reply(
        f"✅ **Channel Added!**\n\n**{title}** (`{cid}`)\n\n"
        + ("" if access_hash else "⚠️ Could not cache peer — re-add if posting fails."),
        reply_markup=ReplyKeyboardRemove()
    )
    await start_cmd(c, m)

async def handle_channel_id_input(c, m, uid, text):
    try:
        channel_id = int(text)
    except ValueError:
        await m.reply("❌ Please send a numeric channel ID (e.g. `-1001234567890`).")
        return

    if not str(channel_id).startswith("-100"):
        await m.reply("❌ Channel IDs start with `-100`. Please check and try again.")
        return

    session = await get_session(uid)
    if not session:
        await m.reply("❌ Session expired. Please /start and login again.")
        return

    title       = str(channel_id)
    access_hash = 0
    try:
        async with Client(
            ":memory:", api_id=API_ID, api_hash=API_HASH,
            session_string=session,
            device_model="AutoCast", system_version="Railway", app_version="2.0"
        ) as user_client:
            # Warm peer cache by iterating dialogs — gives user-specific access_hash
            access_hash = await warm_peer_and_get_hash(user_client, uid, channel_id)
            if not access_hash:
                await m.reply(
                    "❌ Channel not found in your dialogs.\n\n"
                    "Make sure you are a **member or admin** of this channel and try again."
                )
                return
            # Now peer is cached — get_chat works
            try:
                chat = await user_client.get_chat(channel_id)
                if chat.type not in (enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
                    await m.reply("❌ That ID doesn't belong to a channel.")
                    return
                title = chat.title or title
            except Exception as e:
                logger.warning(f"get_chat after warm failed for {channel_id}: {e}")
    except Exception as e:
        await m.reply(f"❌ Error connecting your account: {e}")
        return

    await add_channel(uid, str(channel_id), title, access_hash)
    user_state[uid]["step"] = None
    await m.reply(
        f"✅ **Channel Added!**\n\n**{title}** (`{channel_id}`)",
        reply_markup=ReplyKeyboardRemove()
    )
    await start_cmd(c, m)

# ─────────────────────────────────────────────────────────────────────────────
#  CONTENT CAPTURE
# ─────────────────────────────────────────────────────────────────────────────
def _extract_file_id(m):
    if m.photo:      return m.photo.file_id
    if m.video:      return m.video.file_id
    if m.audio:      return m.audio.file_id
    if m.voice:      return m.voice.file_id
    if m.document:   return m.document.file_id
    if m.animation:  return m.animation.file_id
    if m.sticker:    return m.sticker.file_id
    return None

async def process_content_message(c, m, uid):
    st = user_state[uid]
    st["content_type"] = m.media.value if m.media else "text"
    st["content_text"] = m.caption or m.text
    st["file_id"]      = _extract_file_id(m)
    st["entities"]     = serialize_entities(m.caption_entities or m.entities)
    st["input_msg_id"] = m.id
    st["src_chat_id"]  = m.chat.id
    st["src_msg_id"]   = m.id
    if m.reply_to_message:
        st["reply_ref_id"] = m.reply_to_message.id
    await show_time_menu(m, uid)


async def process_broadcast_content_message(c, m, uid):
    st    = user_state[uid]
    queue = st.get("broadcast_queue", [])
    post  = {
        "content_type":       m.media.value if m.media else "text",
        "content_text":       m.caption or m.text,
        "file_id":            _extract_file_id(m),
        "entities":           serialize_entities(m.caption_entities or m.entities),
        "input_msg_id":       m.id,
        "src_chat_id":        m.chat.id,
        "src_msg_id":         m.id,
        "pin":                True,
        "delete_old":         True,
        "auto_delete_offset": 0,
    }
    if m.reply_to_message:
        post["reply_ref_id"] = m.reply_to_message.id
    queue.append(post)
    # Sort by original message_id — keeps forwarded batches in the correct
    # order even when Pyrogram dispatches them out of sequence
    queue.sort(key=lambda p: p["input_msg_id"])
    st["broadcast_queue"] = queue
    pos = next((i+1 for i, p in enumerate(queue) if p["input_msg_id"] == m.id), len(queue))
    await m.reply(
        f"✅ Post #{pos} added. ({len(queue)} total in queue)\n"
        f"Send the next post, or tap **✅ Done Adding Posts** when finished."
    )


async def process_custom_date(c, m, uid):
    st  = user_state[uid]
    tz  = await get_user_tz(uid)
    txt = (m.text or "").strip()
    # FIX B4: include year in format string so date is not 1900
    for fmt in ("%d-%b-%Y %I:%M %p", "%d-%b-%Y %H:%M", "%d/%m/%Y %I:%M %p", "%d/%m/%Y %H:%M"):
        try:
            dt = datetime.datetime.strptime(txt, fmt)
            dt = tz.localize(dt)
            if dt < now_in(tz):
                await m.reply(
                    "❌ That date/time is in the past. "
                    "Please enter a **future** date and time."
                )
                return
            st["start_time"] = dt
            st["step"] = None
            await ask_repetition(m, uid, force_new=True)
            return
        except ValueError:
            continue
    await m.reply(
        "❌ Invalid format. Use:\n`DD-Mon-YYYY HH:MM AM/PM`\n\n"
        "Example: `25-Dec-2025 03:30 PM`"
    )

# ─────────────────────────────────────────────────────────────────────────────
#  TASK CREATION
# ─────────────────────────────────────────────────────────────────────────────
async def process_content_edit_message(c, m, uid):
    """Handle new content sent to replace an existing task's content."""
    st  = user_state[uid]
    tid = st.get("editing_task_id")
    if not tid:
        await m.reply("❌ No task selected for editing.")
        return
    st["content_type"] = m.media.value if m.media else "text"
    st["content_text"] = m.caption or m.text
    st["file_id"]      = _extract_file_id(m)
    st["entities"]     = serialize_entities(m.caption_entities or m.entities)
    st["src_chat_id"]  = m.chat.id
    st["src_msg_id"]   = m.id
    st["step"] = None

    task = await get_single_task(tid)
    if not task:
        await m.reply("❌ Task no longer exists.")
        return
    # Preserve schedule and settings from existing task
    try:
        dt = datetime.datetime.fromisoformat(task["start_time"])
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        st["start_time"] = dt
    except Exception:
        pass
    st["interval"]           = task["repeat_interval"]
    st["pin"]                = task["pin"]
    st["del"]                = task["delete_old"]
    st["auto_delete_offset"] = task.get("auto_delete_offset", 0)

    await update_menu(
        m,
        "✅ **Content updated!**\n\nWould you like to also change the schedule?",
        [
            [InlineKeyboardButton("🗓 Change Schedule",  callback_data=f"reschedule_{tid}")],
            [InlineKeyboardButton("✅ Save & Keep Time", callback_data="save_task")],
            [InlineKeyboardButton("🔙 Cancel",           callback_data=f"view_{tid}")],
        ],
        uid, force_new=True
    )

async def update_task_logic(uid, q):
    """Update an existing task's content and/or schedule."""
    st  = user_state[uid]
    tid = st.get("editing_task_id")
    if not tid:
        await create_task_logic(uid, q)
        return

    task = await get_single_task(tid)
    if not task:
        await q.answer("❌ Task not found.", show_alert=True)
        return

    # Cancel existing scheduler job
    if scheduler:
        try: scheduler.remove_job(tid)
        except Exception: pass

    tz = await get_user_tz(uid)
    start_time = st.get("start_time")
    if not start_time:
        try:
            start_time = datetime.datetime.fromisoformat(task["start_time"])
            if start_time.tzinfo is None:
                start_time = pytz.utc.localize(start_time)
        except Exception:
            start_time = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)

    updated = {
        "task_id":            tid,
        "owner_id":           task["owner_id"],
        "chat_id":            task["chat_id"],
        "content_type":       st.get("content_type", task["content_type"]),
        "content_text":       st.get("content_text", task["content_text"]),
        "file_id":            st.get("file_id",      task["file_id"]),
        "entities":           st.get("entities",     task["entities"]),
        "pin":                st.get("pin",          task["pin"]),
        "delete_old":         st.get("del",          task["delete_old"]),
        "auto_delete_offset": st.get("auto_delete_offset", task.get("auto_delete_offset", 0)),
        "repeat_interval":    st.get("interval",     task["repeat_interval"]),
        "start_time":         start_time.isoformat(),
        "last_msg_id":        task["last_msg_id"],
        "reply_target":       task.get("reply_target"),
        "src_chat_id":        st.get("src_chat_id",  task.get("src_chat_id", 0)),
        "src_msg_id":         st.get("src_msg_id",   task.get("src_msg_id", 0)),
    }

    await save_task(updated)
    add_scheduler_job(updated)

    # Clear editing state
    for key in ("editing_task_id", "content_type", "content_text", "file_id",
                "entities", "pin", "del", "auto_delete_offset", "interval",
                "start_time", "src_chat_id", "src_msg_id", "step"):
        user_state[uid].pop(key, None)

    t_str = start_time.astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
    await q.answer("✅ Task updated!")
    # Go back to the task details screen
    await show_task_details(uid, q.message, tid)

async def create_task_logic(uid, q):
    st      = user_state[uid]
    targets = st.get("broadcast_targets", [st.get("target")])
    queue   = st.get("broadcast_queue")
    tz      = await get_user_tz(uid)

    if not queue:
        queue = [{
            "content_type":       st["content_type"],
            "content_text":       st["content_text"],
            "file_id":            st["file_id"],
            "entities":           st.get("entities"),
            "input_msg_id":       0,
            "reply_ref_id":       None,
            "pin":                st.get("pin", True),
            "delete_old":         st.get("del", True),
            "auto_delete_offset": st.get("auto_delete_offset", 0),
            "src_chat_id":        st.get("src_chat_id", 0),
            "src_msg_id":         st.get("src_msg_id", 0),
        }]

    base_tid   = int(datetime.datetime.now().timestamp())
    t_str      = st["start_time"].astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
    # FIX B10: use .get() to avoid KeyError
    interval   = st.get("interval")
    total      = 0

    for ch_idx, cid in enumerate(targets):
        batch_map = {
            post["input_msg_id"]: f"task_{base_tid}_{ch_idx}_{pi}"
            for pi, post in enumerate(queue)
            if "input_msg_id" in post
        }
        for pi, post in enumerate(queue):
            tid      = f"task_{base_tid}_{ch_idx}_{pi}"
            run_time = st["start_time"] + datetime.timedelta(seconds=pi * 10)

            reply_target = None
            if post.get("reply_ref_id") and post["reply_ref_id"] in batch_map:
                reply_target = batch_map[post["reply_ref_id"]]

            task_data = {
                "task_id":            tid,
                "owner_id":           uid,
                "chat_id":            cid,
                "content_type":       post["content_type"],
                "content_text":       post["content_text"],
                "file_id":            post["file_id"],
                "entities":           post.get("entities"),
                "pin":                post.get("pin", st.get("pin", True)),
                "delete_old":         post.get("delete_old", st.get("del", True)),
                "auto_delete_offset": post.get("auto_delete_offset", st.get("auto_delete_offset", 0)),
                "repeat_interval":    interval,
                "start_time":         run_time.isoformat(),
                "last_msg_id":        None,
                "reply_target":       reply_target,
                "src_chat_id":        post.get("src_chat_id", 0),
                "src_msg_id":         post.get("src_msg_id", 0),
            }
            try:
                await save_task(task_data)
                add_scheduler_job(task_data)
                total += 1
            except Exception as e:
                logger.error(f"Failed to create task {tid}: {e}")

    for key in ("broadcast_targets", "broadcast_queue", "auto_delete_offset"):
        user_state[uid].pop(key, None)

    await update_menu(
        q.message,
        f"🎉 **Scheduled Successfully!**\n\n"
        f"📢 **Channels:** {len(targets)}\n"
        f"📬 **Posts/Channel:** {len(queue)}\n"
        f"📅 **First Send:** `{t_str}`\n"
        f"🔁 **Repeat:** `{interval or 'Once'}`\n\n"
        f"Use /manage to schedule more.",
        None, uid, force_new=False
    )

# ─────────────────────────────────────────────────────────────────────────────
#  SCHEDULER JOB
# ─────────────────────────────────────────────────────────────────────────────
def add_scheduler_job(t):
    if scheduler is None:
        return
    tid = t["task_id"]
    dt  = datetime.datetime.fromisoformat(t["start_time"])
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)

    async def job_func():
        async with queue_lock:
            logger.info(f"🚀 Job {tid} triggered")
            fresh = await get_single_task(tid)
            if not fresh:
                logger.warning(f"Job {tid}: task gone from DB, skipping")
                return

            # Calculate next run time
            next_iso = None
            if fresh["repeat_interval"]:
                try:
                    last = datetime.datetime.fromisoformat(fresh["start_time"])
                    if last.tzinfo is None:
                        last = pytz.utc.localize(last)
                    mins = int(fresh["repeat_interval"].split("=")[1])
                    next_iso = (last + datetime.timedelta(minutes=mins)).isoformat()
                except Exception as e:
                    logger.error(f"Next-run calc error for {tid}: {e}")

            session = await get_session(fresh["owner_id"])
            if not session:
                logger.warning(f"Job {tid}: no session, removing task")
                if fresh["repeat_interval"] and scheduler:
                    try: scheduler.remove_job(tid)
                    except Exception: pass
                await delete_task(tid)
                return

            sent = None
            try:
                async with Client(
                    ":memory:", api_id=API_ID, api_hash=API_HASH,
                    session_string=session,
                    device_model="AutoCast", system_version="Railway", app_version="2.0"
                ) as user:

                    target_int  = int(fresh["chat_id"])

                    # Get access_hash — fetch from dialogs if not in DB (handles
                    # channels added before access_hash was stored, and refreshes
                    # stale values).
                    access_hash = await get_channel_access_hash(
                        fresh["owner_id"], fresh["chat_id"]
                    )
                    if not access_hash:
                        logger.info(f"Job {tid}: access_hash missing, scanning dialogs…")
                        access_hash = await warm_peer_and_get_hash(
                            user, fresh["owner_id"], target_int
                        )
                        if not access_hash:
                            logger.error(
                                f"Job {tid}: cannot resolve channel {fresh['chat_id']} — "
                                "user must re-add it so access_hash can be cached"
                            )
                            return

                    inject_peer(user.storage, target_int, access_hash)

                    caption  = fresh["content_text"]
                    ents     = deserialize_entities(fresh["entities"])
                    reply_id = None

                    # Delete previous message if requested
                    if fresh["delete_old"] and fresh.get("last_msg_id"):
                        try:
                            await user.delete_messages(target_int, fresh["last_msg_id"])
                            logger.info(f"Job {tid}: deleted old msg {fresh['last_msg_id']}")
                        except (errors.MessageDeleteForbidden, errors.MessageIdInvalid):
                            pass
                        except Exception as e:
                            logger.warning(f"Job {tid}: delete old failed: {e}")

                    if fresh.get("reply_target"):
                        ref = await get_single_task(fresh["reply_target"])
                        if ref and ref.get("last_msg_id"):
                            reply_id = ref["last_msg_id"]

                    ct  = fresh["content_type"]
                    fid = fresh["file_id"]

                    async def _send():
                        nonlocal sent
                        if ct == "text":
                            sent = await user.send_message(
                                target_int, caption or " ",
                                entities=ents,
                                parse_mode=None,
                                reply_to_message_id=reply_id,
                                disable_web_page_preview=False
                            )
                        elif ct == "poll":
                            pd = json.loads(caption)
                            sent = await user.send_poll(
                                target_int, pd["question"], pd["options"],
                                reply_to_message_id=reply_id
                            )
                        elif ct == "photo":
                            sent = await user.send_photo(
                                target_int, fid, caption=caption,
                                caption_entities=ents, reply_to_message_id=reply_id
                            )
                        elif ct == "video":
                            sent = await user.send_video(
                                target_int, fid, caption=caption,
                                caption_entities=ents, reply_to_message_id=reply_id
                            )
                        elif ct == "animation":
                            sent = await user.send_animation(
                                target_int, fid, caption=caption,
                                caption_entities=ents, reply_to_message_id=reply_id
                            )
                        elif ct == "document":
                            sent = await user.send_document(
                                target_int, fid, caption=caption,
                                caption_entities=ents, reply_to_message_id=reply_id
                            )
                        elif ct == "sticker":
                            sent = await user.send_sticker(
                                target_int, fid, reply_to_message_id=reply_id
                            )
                        elif ct == "audio":
                            sent = await user.send_audio(
                                target_int, fid, caption=caption,
                                caption_entities=ents, reply_to_message_id=reply_id
                            )
                        elif ct == "voice":
                            sent = await user.send_voice(
                                target_int, fid, caption=caption,
                                reply_to_message_id=reply_id
                            )

                    try:
                        await _send()
                    except (errors.FileIdInvalid, errors.MediaEmpty):
                        logger.warning(f"Job {tid}: {ct} file invalid/empty — re-downloading for re-upload")
                        # Try to re-download via bot first; fall back to src message copy
                        media = None
                        if fid:
                            try:
                                media = await app.download_media(fid, in_memory=True)
                            except Exception as dl_e:
                                logger.warning(f"Job {tid}: bot download failed: {dl_e}")
                        if media is None and fresh.get("src_chat_id") and fresh.get("src_msg_id"):
                            try:
                                # Copy the original message to the user's saved messages
                                # to get a fresh file reference from a known-good source
                                media_msg = await app.forward_messages(
                                    fresh["owner_id"],
                                    from_chat_id=fresh["src_chat_id"],
                                    message_ids=fresh["src_msg_id"]
                                )
                                media = await app.download_media(media_msg, in_memory=True)
                                await media_msg.delete()
                            except Exception as fwd_e:
                                logger.warning(f"Job {tid}: src forward failed: {fwd_e}")
                        if media is None:
                            logger.error(f"Job {tid}: cannot recover media for {ct}, skipping")
                            sent = None
                        else:
                            if ct == "photo":
                                sent = await user.send_photo(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id)
                            elif ct == "video":
                                sent = await user.send_video(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id)
                            elif ct == "animation":
                                sent = await user.send_animation(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id)
                            elif ct == "document":
                                sent = await user.send_document(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id)
                            elif ct == "audio":
                                media.name = "audio.mp3"
                                sent = await user.send_audio(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id)
                            elif ct == "voice":
                                media.name = "voice.ogg"
                                sent = await user.send_voice(target_int, media, caption=caption, reply_to_message_id=reply_id)
                            elif ct == "sticker":
                                logger.error(f"Job {tid}: sticker re-upload not supported")
                                sent = None

                    if sent:
                        logger.info(f"Job {tid}: sent msg {sent.id}")
                        if fresh["pin"]:
                            try:
                                pin_notif = await sent.pin()
                                if isinstance(pin_notif, Message):
                                    await pin_notif.delete()
                            except Exception as e:
                                logger.warning(f"Job {tid}: pin failed: {e}")

                        await update_last_msg(tid, sent.id)

                        off = fresh.get("auto_delete_offset", 0)
                        if off and off > 0:
                            try:
                                run_at = datetime.datetime.now(pytz.utc) + datetime.timedelta(minutes=off)
                                scheduler.add_job(
                                    delete_sent_message, "date", run_date=run_at,
                                    args=[fresh["owner_id"], fresh["chat_id"], sent.id],
                                    id=f"del_{tid}_{sent.id}",
                                    misfire_grace_time=120
                                )
                            except Exception as e:
                                logger.error(f"Job {tid}: auto-delete schedule failed: {e}")

                        if not fresh["repeat_interval"]:
                            await delete_task(tid)
                            logger.info(f"Job {tid}: one-time task removed")

            except errors.FloodWait as e:
                logger.warning(f"Job {tid}: FloodWait {e.value}s")
                await asyncio.sleep(e.value)
            except errors.MessageTooLong:
                logger.error(f"Job {tid}: message too long")
            except errors.MessageEmpty:
                logger.error(f"Job {tid}: message empty")
            except errors.ChatWriteForbidden:
                logger.error(f"Job {tid}: write forbidden in {fresh['chat_id']}")
            except errors.ChatAdminRequired:
                logger.error(f"Job {tid}: admin required in {fresh['chat_id']}")
            except errors.PeerIdInvalid:
                logger.error(f"Job {tid}: invalid chat {fresh['chat_id']}")
            except Exception as e:
                logger.error(f"Job {tid}: unexpected error: {e}", exc_info=True)
            finally:
                if next_iso and fresh.get("repeat_interval"):
                    try: await update_next_run(tid, next_iso)
                    except Exception: pass

    if t["repeat_interval"]:
        mins = int(t["repeat_interval"].split("=")[1])
        scheduler.add_job(
            job_func,
            IntervalTrigger(start_date=dt, timezone=pytz.utc, minutes=mins),
            id=tid, replace_existing=True,
            misfire_grace_time=3600, coalesce=True, max_instances=1
        )
    else:
        scheduler.add_job(
            job_func,
            DateTrigger(run_date=dt, timezone=pytz.utc),
            id=tid, replace_existing=True, misfire_grace_time=3600
        )

# ─────────────────────────────────────────────────────────────────────────────
#  STARTUP
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    check_env_vars()
    global queue_lock, scheduler

    queue_lock = asyncio.Lock()

    await init_db()
    await migrate_to_v11()

    scheduler = AsyncIOScheduler(
        timezone=pytz.utc,
        event_loop=asyncio.get_running_loop(),
        executors={"default": AsyncIOExecutor()},
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600}
    )
    scheduler.start()

    try:
        tasks = await get_all_tasks()
        logger.info(f"📂 Reloading {len(tasks)} tasks from DB")
        for t in tasks:
            try: add_scheduler_job(t)
            except Exception as e: logger.error(f"Failed to reload task {t['task_id']}: {e}")
    except Exception as e:
        logger.error(f"Startup task reload failed: {e}")

    await app.start()
    logger.info("🤖 AutoCast bot started.")
    await idle()
    await app.stop()
    logger.info("🛑 AutoCast bot stopped.")

if __name__ == "__main__":
    app.run(main())
