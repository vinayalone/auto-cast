import os
import sys
import io
import json
import base64
import hashlib
import logging
import asyncio
import datetime
import time
import pytz
import asyncpg
from cryptography.fernet import Fernet, InvalidToken
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("AutoCast")

# ─────────────────────────────────────────────────────────────────────────────
#  ENV / STARTUP
# ─────────────────────────────────────────────────────────────────────────────
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

API_ID       = int(os.environ.get("API_ID", "0") or "0")
API_HASH     = os.environ.get("API_HASH", "")
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

DEFAULT_TZ = pytz.utc

app = Client(
    "autocast_v2",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)
scheduler  = None
db_pool    = None

login_state    = {}
user_state     = {}
tz_cache       = {}
_user_last_seen: dict = {}
_db_init_lock: asyncio.Lock | None = None
_STATE_TTL     = 7200
_last_evict    = 0.0

def _touch(uid: int):
    _user_last_seen[uid] = time.monotonic()

def _evict_stale():
    global _last_evict
    now = time.monotonic()
    if now - _last_evict < 60:
        return
    _last_evict = now
    stale = [u for u, t in list(_user_last_seen.items()) if now - t > _STATE_TTL]
    for u in stale:
        user_state.pop(u, None)
        tz_cache.pop(u, None)
        _user_last_seen.pop(u, None)
    if stale:
        logger.debug(f"Evicted in-memory state for {len(stale)} inactive user(s)")

# ─────────────────────────────────────────────────────────────────────────────
#  FIX 12 — SESSION ENCRYPTION
#  Added InvalidToken catch in decrypt_session so key-rotation doesn't silently
#  corrupt sessions — it now logs a clear warning instead of returning garbage.
# ─────────────────────────────────────────────────────────────────────────────
_fernet: Fernet | None = None

def _init_encryption():
    global _fernet
    raw = os.environ.get("ENCRYPTION_KEY", "").strip()
    if not raw:
        sample = Fernet.generate_key().decode()
        logger.warning(
            "⚠️  ENCRYPTION_KEY not set — sessions stored in PLAINTEXT. "
            f"Add this to your env to enable encryption:\n  ENCRYPTION_KEY={sample}"
        )
        _fernet = None
        return
    derived = base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest())
    _fernet = Fernet(derived)
    logger.info("🔐 Session encryption enabled.")

def encrypt_session(s: str) -> str:
    if _fernet:
        return _fernet.encrypt(s.encode()).decode()
    return s

def decrypt_session(s: str) -> str | None:
    """
    FIX 12 (updated FIX 20): Catches InvalidToken and distinguishes two cases:

    • s starts with 'gAAA'  → it IS a Fernet token but the key has been rotated.
      Returning the raw ciphertext to Pyrogram causes silent auth failures.
      Return None so callers treat this as "no session" and prompt re-login.

    • s does NOT start with 'gAAA' → legacy plaintext row saved before encryption
      was enabled.  Return s as-is so the session still works (backward compat).
    """
    if _fernet is None:
        return s
    try:
        return _fernet.decrypt(s.encode()).decode()
    except InvalidToken:
        if s.startswith("gAAA"):
            # Confirmed Fernet ciphertext — key has been rotated, cannot recover.
            logger.warning(
                "decrypt_session: InvalidToken on Fernet ciphertext — "
                "ENCRYPTION_KEY was rotated. Session is unrecoverable; "
                "returning None to force re-login."
            )
            return None
        # Doesn't look like Fernet — treat as legacy plaintext session.
        logger.info(
            "decrypt_session: InvalidToken but value is not Fernet ciphertext — "
            "treating as legacy plaintext session."
        )
        return s
    except Exception:
        return s

# ─────────────────────────────────────────────────────────────────────────────
#  UTC NORMALISATION HELPER
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_utc(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return pytz.utc.localize(dt)
    return dt.astimezone(pytz.utc)

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
    global db_pool, _db_init_lock
    if db_pool:
        return db_pool
    if _db_init_lock is None:
        _db_init_lock = asyncio.Lock()
    async with _db_init_lock:
        if not db_pool:
            db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_sessions (
                user_id        BIGINT PRIMARY KEY,
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
                task_id            TEXT PRIMARY KEY,
                owner_id           BIGINT,
                chat_id            TEXT,
                content_type       TEXT,
                content_text       TEXT,
                file_id            TEXT,
                entities           TEXT,
                pin                BOOLEAN DEFAULT FALSE,
                delete_old         BOOLEAN DEFAULT FALSE,
                repeat_interval    TEXT,
                start_time         TEXT,
                last_msg_id        BIGINT,
                auto_delete_offset INTEGER DEFAULT 0,
                reply_target       TEXT,
                src_chat_id        BIGINT DEFAULT 0,
                src_msg_id         BIGINT DEFAULT 0,
                is_paused          BOOLEAN DEFAULT FALSE
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_wizard_state (
                user_id    BIGINT PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        for sql in [
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS auto_delete_offset INTEGER DEFAULT 0",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS reply_target TEXT",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS src_chat_id BIGINT DEFAULT 0",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS src_msg_id  BIGINT DEFAULT 0",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS is_paused   BOOLEAN DEFAULT FALSE",
            "ALTER TABLE userbot_settings  ADD COLUMN IF NOT EXISTS timezone      TEXT    DEFAULT 'UTC'",
            "ALTER TABLE userbot_settings  ADD COLUMN IF NOT EXISTS engine_paused BOOLEAN DEFAULT FALSE",
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
#  WIZARD STATE PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────
_WIZARD_PERSIST_KEYS = frozenset({
    "step", "target", "content_type", "content_text", "file_id", "entities",
    "pin", "del", "auto_delete_offset", "interval", "editing_task_id",
    "src_chat_id", "src_msg_id", "reply_to_channel_msg_id",
    "broadcast_targets", "broadcast_queue", "menu_msg_id", "input_msg_id",
    "start_time",
})

def _serialize_wizard(st: dict) -> str:
    out: dict = {}
    for k, v in st.items():
        if k not in _WIZARD_PERSIST_KEYS:
            continue
        if k == "start_time" and isinstance(v, datetime.datetime):
            out[k] = {"__dt__": _ensure_utc(v).isoformat()}
        elif k == "broadcast_queue" and isinstance(v, list):
            safe_queue = []
            for post in v:
                safe_post = {}
                for pk, pv in post.items():
                    # FIX 7: explicitly skip BytesIO and other non-JSON types;
                    # file_id is always a string so it serialises fine
                    if isinstance(pv, io.IOBase):
                        continue
                    if isinstance(pv, (str, int, float, bool, type(None))):
                        safe_post[pk] = pv
                    else:
                        try:
                            json.dumps(pv)
                            safe_post[pk] = pv
                        except (TypeError, ValueError):
                            pass
                safe_queue.append(safe_post)
            out[k] = safe_queue
        else:
            try:
                json.dumps(v)
                out[k] = v
            except (TypeError, ValueError):
                pass
    return json.dumps(out)

def _deserialize_wizard(s: str) -> dict:
    raw = json.loads(s)
    out: dict = {}
    for k, v in raw.items():
        if isinstance(v, dict) and "__dt__" in v:
            try:
                out[k] = datetime.datetime.fromisoformat(v["__dt__"])
            except Exception:
                pass
        else:
            out[k] = v
    # FIX 7: guarantee expected keys exist in broadcast_queue entries
    if "broadcast_queue" in out and isinstance(out["broadcast_queue"], list):
        for post in out["broadcast_queue"]:
            post.setdefault("content_type", "text")
            post.setdefault("content_text", None)
            post.setdefault("file_id", None)
            post.setdefault("entities", None)
            post.setdefault("src_chat_id", 0)
            post.setdefault("src_msg_id", 0)
            post.setdefault("pin", True)
            post.setdefault("delete_old", True)
            post.setdefault("auto_delete_offset", 0)
            post.setdefault("input_msg_id", 0)
    return out

async def persist_wizard_state(uid: int):
    st = user_state.get(uid)
    if st is None:
        return
    try:
        serialized = _serialize_wizard(st)
        pool = await get_db()
        await pool.execute("""
            INSERT INTO userbot_wizard_state (user_id, state_json, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE SET state_json=$2, updated_at=NOW()
        """, uid, serialized)
    except Exception as e:
        logger.warning(f"persist_wizard_state uid={uid}: {e}")

async def restore_wizard_state(uid: int):
    try:
        pool = await get_db()
        row = await pool.fetchrow(
            "SELECT state_json FROM userbot_wizard_state WHERE user_id=$1", uid
        )
        if row:
            st = _deserialize_wizard(row["state_json"])
            user_state.setdefault(uid, {}).update(st)
    except Exception as e:
        logger.warning(f"restore_wizard_state uid={uid}: {e}")

async def clear_wizard_state(uid: int):
    user_state.pop(uid, None)
    try:
        pool = await get_db()
        await pool.execute("DELETE FROM userbot_wizard_state WHERE user_id=$1", uid)
    except Exception as e:
        logger.warning(f"clear_wizard_state uid={uid}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  DB HELPERS
# ─────────────────────────────────────────────────────────────────────────────
async def get_session(user_id):
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT session_string FROM userbot_sessions WHERE user_id=$1", user_id
    )
    if not row:
        return None
    return decrypt_session(row["session_string"])

async def save_session(user_id, session):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_sessions (user_id, session_string)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET session_string=$2
    """, user_id, encrypt_session(session))

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

# ─────────────────────────────────────────────────────────────────────────────
#  FIX 6 — warm_peer_and_get_hash: try get_chat() first, fall back to dialogs
#  The original always scanned all dialogs — this reduces API usage dramatically
#  for common cases where the channel is resolvable directly.
# ─────────────────────────────────────────────────────────────────────────────
async def warm_peer_and_get_hash(user_client, owner_id: int, channel_id: int,
                                  timeout: float = 25.0) -> int:
    # Fast path: try get_chat directly (works if peer is already in Pyrogram's cache
    # or resolvable from the session)
    try:
        chat = await asyncio.wait_for(
            user_client.get_chat(channel_id), timeout=8.0
        )
        ah = getattr(chat, "access_hash", 0) or 0
        if ah:
            pool = await get_db()
            await pool.execute(
                "UPDATE userbot_channels SET access_hash=$1 "
                "WHERE user_id=$2 AND channel_id=$3",
                ah, owner_id, str(channel_id)
            )
            logger.info(f"✅ Cached access_hash for {channel_id} (fast path) owner={owner_id}")
            return ah
    except (asyncio.TimeoutError, errors.PeerIdInvalid):
        pass  # fall through to dialog scan
    except Exception as e:
        logger.debug(f"warm_peer fast path failed for {channel_id}: {e}")

    # Slow path: scan dialogs
    async def _scan():
        async for dialog in user_client.get_dialogs():
            if dialog.chat.id == channel_id:
                ah = getattr(dialog.chat, "access_hash", 0) or 0
                if ah:
                    pool = await get_db()
                    await pool.execute(
                        "UPDATE userbot_channels SET access_hash=$1 "
                        "WHERE user_id=$2 AND channel_id=$3",
                        ah, owner_id, str(channel_id)
                    )
                    logger.info(f"✅ Cached access_hash for {channel_id} (dialog scan) owner={owner_id}")
                return ah
        return 0

    try:
        return await asyncio.wait_for(_scan(), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"warm_peer timed out after {timeout}s for channel {channel_id}")
        return 0
    except Exception as e:
        logger.warning(f"warm_peer get_dialogs failed for {channel_id}: {e}")
        return 0

# ─────────────────────────────────────────────────────────────────────────────
#  FIX 1 — REMOVED inject_peer
#  The original used SQLite calls on an in-memory Pyrogram client whose storage
#  is a dict, not a SQLite file — this always silently crashed.
#  Replaced everywhere it was called with user.get_chat() which correctly warms
#  the in-memory peer cache via Pyrogram's own mechanism.
# ─────────────────────────────────────────────────────────────────────────────
async def _warm_peer_in_client(user_client, target_int: int, access_hash: int):
    """
    Warm the peer inside an already-started in-memory user client.
    Tries get_chat first; falls back to passing raw InputChannel if we have
    the access_hash so no dialog scan is needed.
    """
    try:
        await user_client.get_chat(target_int)
        return
    except Exception:
        pass
    if access_hash:
        try:
            from pyrogram.raw import types as raw_types, functions as raw_funcs
            peer = raw_types.InputChannel(
                channel_id=abs(target_int) - 1_000_000_000_000,
                access_hash=access_hash
            )
            await user_client.invoke(
                raw_funcs.channels.GetChannels(id=[peer])
            )
            logger.debug(f"Peer {target_int} warmed via raw InputChannel")
        except Exception as e:
            logger.warning(f"_warm_peer_in_client raw invoke failed for {target_int}: {e}")

async def get_channels(user_id):
    pool = await get_db()
    return await pool.fetch(
        "SELECT * FROM userbot_channels WHERE user_id=$1", user_id
    )

async def del_channel(user_id, cid):
    pool = await get_db()
    tasks = await pool.fetch(
        "SELECT task_id FROM userbot_tasks_v11 WHERE owner_id=$1 AND chat_id=$2",
        user_id, cid
    )
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t["task_id"])
            except Exception: pass
    await pool.execute(
        "DELETE FROM userbot_tasks_v11 WHERE owner_id=$1 AND chat_id=$2", user_id, cid
    )
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
            reply_target       = $14,
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

async def set_task_paused(task_id: str, paused: bool):
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET is_paused=$1 WHERE task_id=$2", paused, task_id
    )

async def set_all_tasks_paused(owner_id: int, paused: bool):
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET is_paused=$1 WHERE owner_id=$2", paused, owner_id
    )

async def get_engine_paused(owner_id: int) -> bool:
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT engine_paused FROM userbot_settings WHERE user_id=$1", owner_id
    )
    return bool(row["engine_paused"]) if row and row["engine_paused"] is not None else False

async def set_engine_paused(owner_id: int, paused: bool):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_settings (user_id, engine_paused)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET engine_paused = $2
    """, owner_id, paused)

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

            # FIX 5: Don't raise on second timeout — just warn and continue.
            # The important thing is that DB data is always cleaned up.
            for _attempt, _t in enumerate([8.0, 15.0], 1):
                try:
                    await asyncio.wait_for(_logout(), timeout=_t)
                    break
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Session logout timeout on attempt {_attempt} "
                        f"(waited {_t}s) — proceeding with data deletion."
                    )
                    # On second timeout just give up on the remote logout
                    # but continue to wipe local data
        except Exception as e:
            logger.warning(f"Session logout skipped: {e}")

    tasks = await pool.fetch(
        "SELECT task_id FROM userbot_tasks_v11 WHERE owner_id=$1", user_id
    )
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t["task_id"])
            except Exception: pass

    await pool.execute("DELETE FROM userbot_tasks_v11   WHERE owner_id=$1",  user_id)
    await pool.execute("DELETE FROM userbot_channels     WHERE user_id=$1",   user_id)
    await pool.execute("DELETE FROM userbot_sessions     WHERE user_id=$1",   user_id)
    await pool.execute("DELETE FROM userbot_settings     WHERE user_id=$1",   user_id)
    await pool.execute("DELETE FROM userbot_wizard_state WHERE user_id=$1",   user_id)

    tz_cache.pop(user_id, None)
    user_state.pop(user_id, None)
    login_state.pop(user_id, None)

async def clear_db_data(user_id):
    """
    Delete all tasks, channels, settings and wizard state for user_id,
    but keep the Telegram session so the user stays logged in.
    Scheduler jobs are removed so nothing fires after the wipe.
    """
    pool = await get_db()
    tasks = await pool.fetch(
        "SELECT task_id FROM userbot_tasks_v11 WHERE owner_id=$1", user_id
    )
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t["task_id"])
            except Exception: pass

    await pool.execute("DELETE FROM userbot_tasks_v11   WHERE owner_id=$1", user_id)
    await pool.execute("DELETE FROM userbot_channels     WHERE user_id=$1",  user_id)
    await pool.execute("DELETE FROM userbot_settings     WHERE user_id=$1",  user_id)
    await pool.execute("DELETE FROM userbot_wizard_state WHERE user_id=$1",  user_id)

    tz_cache.pop(user_id, None)
    user_state.pop(user_id, None)
    _user_last_seen.pop(user_id, None)

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
            # FIX 1: use _warm_peer_in_client instead of the broken inject_peer
            await _warm_peer_in_client(user, chat_id_int, access_hash)
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
        "custom_emoji_id": getattr(e, "custom_emoji_id", None)
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
#  EXPORT / IMPORT
#
#  FIX 13 — REPLY_TARGET PORTABILITY
#  reply_target stores a task_id like "task_123_0_1" which is process-specific
#  and breaks on import. The fix:
#  • Export:  if reply_target is a task_id, store reply_task_index (stable int
#             index into the tasks list) instead. Channel message IDs are kept
#             as-is in reply_target.
#  • Import:  first pass creates all tasks and records their new IDs.
#             second pass patches reply_targets that were task-references using
#             the index → new_task_id mapping.
# ─────────────────────────────────────────────────────────────────────────────
EXPORT_VERSION = 4   # bumped from 3 to reflect reply_task_index addition

_MAX_MEDIA_EMBED_BYTES = 25 * 1024 * 1024

def _next_future_run(original_iso: str, interval_str: str | None,
                     now_utc: datetime.datetime) -> datetime.datetime:
    try:
        dt = _ensure_utc(datetime.datetime.fromisoformat(original_iso))
    except Exception:
        return now_utc + datetime.timedelta(minutes=5)

    if dt > now_utc:
        return dt

    if not interval_str:
        return now_utc + datetime.timedelta(minutes=5)

    try:
        mins = int(interval_str.split("=")[1])
        delta = datetime.timedelta(minutes=mins)
        elapsed = now_utc - dt
        periods_missed = int(elapsed.total_seconds() / delta.total_seconds()) + 1
        return dt + delta * periods_missed
    except Exception:
        return now_utc + datetime.timedelta(minutes=5)


def _extract_media_file_id(msg, content_type: str) -> str | None:
    """
    Given a fetched Pyrogram Message and a content_type string, return the
    file_id of the media attachment (which is always fresh / never expired).
    Returns None if the message is empty or the media type doesn't match.
    """
    if not msg:
        return None
    attr_map = {
        "photo":     "photo",
        "video":     "video",
        "animation": "animation",
        "document":  "document",
        "audio":     "audio",
        "voice":     "voice",
        "sticker":   "sticker",
    }
    attr = attr_map.get(content_type)
    if attr:
        media = getattr(msg, attr, None)
        if media:
            return getattr(media, "file_id", None)
    return None


async def _download_media_bytes(file_id: str, user_client=None) -> bytes | None:
    """
    FIX 19 / FIX 24: Download media for export without ever silently skipping.

    Strategy:
      1. Try the user's own session client first — it runs on an isolated TCP
         connection that is NOT shared with job clients, so upload.GetFile
         congestion on the bot's main connection doesn't affect it.
      2. Fall back to the bot client (app) with 3 attempts + exponential backoff
         so transient congestion is retried rather than silently dropped.
    """
    fid_short = file_id[:20] + "…"

    # ── Tier 1: user session client (isolated connection) ──────────────────
    if user_client is not None:
        try:
            buf = await asyncio.wait_for(
                user_client.download_media(file_id, in_memory=True), timeout=60.0
            )
            if buf:
                raw = bytes(buf.getbuffer())
                if len(raw) <= _MAX_MEDIA_EMBED_BYTES:
                    return raw
                logger.warning(f"Media {fid_short} too large to embed ({len(raw)} bytes) — omitting bytes")
                return None
        except errors.FileReferenceExpired:
            # Stale file reference — caller must refresh via source message
            logger.warning(f"Media {fid_short}: FILE_REFERENCE_EXPIRED on user client — caller should prefetch source msg")
            return None
        except asyncio.TimeoutError:
            logger.warning(f"Media {fid_short}: user-client download timed out — falling back to bot client")
        except Exception as e:
            logger.warning(f"Media {fid_short}: user-client download failed ({e}) — falling back to bot client")

    # ── Tier 2: bot client with retries ────────────────────────────────────
    for attempt in range(1, 4):          # attempts 1, 2, 3
        delay = 0 if attempt == 1 else 10 * (attempt - 1)   # 0s, 10s, 20s
        if delay:
            logger.info(f"Media {fid_short}: retrying bot-client download in {delay}s (attempt {attempt}/3)…")
            await asyncio.sleep(delay)
        try:
            buf = await asyncio.wait_for(
                app.download_media(file_id, in_memory=True), timeout=45.0
            )
            if buf:
                raw = bytes(buf.getbuffer())
                if len(raw) <= _MAX_MEDIA_EMBED_BYTES:
                    return raw
                logger.warning(f"Media {fid_short} too large to embed ({len(raw)} bytes) — omitting bytes")
                return None
        except errors.FileReferenceExpired:
            # No point retrying — the reference is expired regardless of connection
            logger.warning(f"Media {fid_short}: FILE_REFERENCE_EXPIRED on bot client — no further retries")
            return None
        except asyncio.TimeoutError:
            logger.warning(f"Media {fid_short}: bot-client download timed out (attempt {attempt}/3)")
        except Exception as e:
            logger.warning(f"Media {fid_short}: bot-client download error (attempt {attempt}/3): {e}")

    logger.error(f"Media {fid_short}: all download attempts exhausted — task will be exported WITHOUT media bytes")
    return None


async def _upload_media_bytes(user_client, ct: str,
                               raw: bytes, caption: str | None,
                               ents) -> str | None:
    """
    FIX 3: Use "me" (Saved Messages) as the staging target instead of target_dummy
    (which was the user's Telegram ID — often unresolvable in a fresh in-memory session).
    Saved Messages is always accessible and is the correct staging area.
    """
    buf = io.BytesIO(raw)
    sent = None
    try:
        name_map = {
            "photo": "photo.jpg", "video": "video.mp4",
            "animation": "anim.mp4", "document": "file.bin",
            "audio": "audio.mp3", "voice": "voice.ogg",
            "sticker": "sticker.webp",
        }
        buf.name = name_map.get(ct, "file.bin")

        # "me" always resolves to Saved Messages — no peer resolution needed
        target = "me"

        if ct == "photo":
            sent = await user_client.send_photo(target, buf, caption=caption or "")
            fid  = sent.photo.file_id
        elif ct == "video":
            sent = await user_client.send_video(target, buf, caption=caption or "")
            fid  = sent.video.file_id
        elif ct == "animation":
            sent = await user_client.send_animation(target, buf, caption=caption or "")
            fid  = sent.animation.file_id
        elif ct == "audio":
            sent = await user_client.send_audio(target, buf, caption=caption or "")
            fid  = sent.audio.file_id
        elif ct == "voice":
            sent = await user_client.send_voice(target, buf)
            fid  = sent.voice.file_id
        elif ct == "sticker":
            sent = await user_client.send_sticker(target, buf)
            fid  = sent.sticker.file_id
        else:
            sent = await user_client.send_document(target, buf, caption=caption or "")
            fid  = sent.document.file_id
        # ── FIX 27: Do NOT delete the staging message ─────────────────────────
        # Previously we deleted it immediately after getting file_id.  That made
        # src_msg_id point to a deleted (empty) message, so all future attempts to
        # refresh the file reference via get_messages(src_chat_id, src_msg_id)
        # returned a ghost record with no media — causing FILE_REFERENCE_EXPIRED
        # on every job run and every export.
        # Keeping the message in Saved Messages means the file reference can always
        # be refreshed cheaply with a single get_messages() call.
        logger.debug(f"_upload_media_bytes ct={ct}: staging msg {sent.id} kept in Saved Messages for reference refresh")
        return fid
    except Exception as e:
        logger.warning(f"_upload_media_bytes ct={ct}: {e}")
        return None


async def export_user_config(uid: int) -> dict:
    """
    FIX 13 (export side): reply_target that references another task is stored as
    reply_task_index (a stable int index into the tasks list) so it survives import
    onto a different account with different task_ids.
    Channel message IDs (numeric strings) are kept as-is in reply_target.

    FIX 24 (export side): media downloads now use the user's own session client
    (isolated TCP connection) as the primary path, with bot-client fallback + 3
    retries.  Each task is individually guarded so one bad download never prevents
    the rest of the tasks from being exported.
    """
    pool = await get_db()
    tasks    = [dict(r) for r in await pool.fetch(
        "SELECT * FROM userbot_tasks_v11 WHERE owner_id=$1", uid
    )]
    channels = [dict(r) for r in await pool.fetch(
        "SELECT channel_id, title, access_hash FROM userbot_channels WHERE user_id=$1", uid
    )]
    settings = await pool.fetchrow(
        "SELECT timezone FROM userbot_settings WHERE user_id=$1", uid
    )

    # Build task_id → index map for reply_target remapping
    task_id_to_index = {t["task_id"]: i for i, t in enumerate(tasks)}

    # ── Spin up user-session client for media downloads ───────────────────
    # This gives us an isolated TCP connection that isn't starved by concurrent
    # job clients hammering the bot's main connection with upload.GetFile calls.
    export_user_client = None
    try:
        session = await get_session(uid)
        if session:
            export_user_client = Client(
                ":memory:", api_id=API_ID, api_hash=API_HASH,
                session_string=session, device_model="AutoCast",
                system_version="Railway", app_version="2.0",
            )
            await export_user_client.start()
            logger.info(f"Export uid={uid}: user-session client started for media downloads")
    except Exception as e:
        logger.warning(f"Export uid={uid}: could not start user-session client ({e}) — using bot client only")
        export_user_client = None

    export_tasks = []
    try:
        for t in tasks:
            # ── Per-task isolation: a crash here never drops the remaining tasks ──
            entry = None          # ensure the name is always bound before the except
            try:
                rt = t.get("reply_target")
                reply_target_export  = None
                reply_task_index     = None

                if rt:
                    rt_str = str(rt).strip()
                    if rt_str.startswith("task_") and rt_str in task_id_to_index:
                        # Cross-reference to a sibling task — store as stable index
                        reply_task_index = task_id_to_index[rt_str]
                    elif rt_str:
                        # Channel message ID or other stable reference — keep as-is
                        reply_target_export = rt_str

                entry = {
                    "chat_id":            t["chat_id"],
                    "content_type":       t["content_type"],
                    "content_text":       t["content_text"],
                    "file_id":            t["file_id"],
                    "entities":           t["entities"],
                    "pin":                t["pin"],
                    "delete_old":         t["delete_old"],
                    "repeat_interval":    t["repeat_interval"],
                    "start_time":         t["start_time"],
                    "auto_delete_offset": t.get("auto_delete_offset", 0),
                    "reply_target":       reply_target_export,
                    "reply_task_index":   reply_task_index,
                    "src_chat_id":        t.get("src_chat_id", 0),
                    "src_msg_id":         t.get("src_msg_id", 0),
                    "media_bytes":        None,
                }
                if t["content_type"] not in ("text", "poll") and t.get("file_id"):
                    download_fid = t["file_id"]

                    # ── FIX 25: Refresh expired file reference via source message ──
                    # Telegram file references expire.  Stored file_id values can be
                    # months old, making them stale on export.  Prefetching the source
                    # message gives us a guaranteed-fresh file_id and avoids the storm
                    # of Pyrogram reconnects that FILE_REFERENCE_EXPIRED triggers.
                    src_cid = t.get("src_chat_id") or 0
                    src_mid = t.get("src_msg_id") or 0
                    last_mid = int(t.get("last_msg_id") or 0)
                    logger.info(f"Export uid={uid}: task {t.get('task_id','?')} ct={t['content_type']} src={src_cid}/{src_mid} last_msg={last_mid}")

                    # ── Tier A: staging message in Saved Messages ─────────────────────
                    if src_cid and src_mid and export_user_client:
                        try:
                            src_msg = await asyncio.wait_for(
                                export_user_client.get_messages(int(src_cid), int(src_mid)),
                                timeout=15.0,
                            )
                            fresh_fid = _extract_media_file_id(src_msg, t["content_type"])
                            if fresh_fid:
                                download_fid = fresh_fid
                                logger.info(f"Export uid={uid}: task {t.get('task_id','?')}: file_id refreshed OK from Saved Messages msg {src_mid}")
                            else:
                                logger.warning(f"Export uid={uid}: task {t.get('task_id','?')}: Saved Messages msg {src_mid} has no {t['content_type']} (deleted before FIX 27) — trying last_msg fallback")
                        except Exception as ref_err:
                            logger.warning(f"Export uid={uid}: task {t.get('task_id','?')}: Saved Messages fetch FAILED ({ref_err}) — trying last_msg fallback")

                    # ── Tier B: last sent message in target channel ───────────────────
                    # For tasks created before FIX 27 (staging msg was deleted), fetch
                    # from the last successfully sent message in the channel instead.
                    if download_fid == t["file_id"] and last_mid and export_user_client:
                        try:
                            last_msg = await asyncio.wait_for(
                                export_user_client.get_messages(int(t["chat_id"]), last_mid),
                                timeout=15.0,
                            )
                            last_fid = _extract_media_file_id(last_msg, t["content_type"])
                            if last_fid:
                                download_fid = last_fid
                                logger.info(f"Export uid={uid}: task {t.get('task_id','?')}: file_id refreshed from last sent msg {last_mid} in channel")
                            else:
                                logger.warning(f"Export uid={uid}: task {t.get('task_id','?')}: last sent msg {last_mid} has no {t['content_type']} — falling back to stored fid")
                        except Exception as lme:
                            logger.warning(f"Export uid={uid}: task {t.get('task_id','?')}: last_msg fetch FAILED ({lme}) — using stored fid")

                    raw = await _download_media_bytes(
                        download_fid, user_client=export_user_client
                    )

                    # Last-resort: if refreshed fid still failed, try raw stored fid
                    if raw is None and download_fid != t["file_id"]:
                        logger.warning(f"Export uid={uid}: task {t.get('task_id','?')}: refreshed fid download failed, retrying with stored fid")
                        raw = await _download_media_bytes(
                            t["file_id"], user_client=export_user_client
                        )

                    if raw:
                        entry["media_bytes"] = base64.b64encode(raw).decode()
            except Exception as task_err:
                logger.error(f"Export uid={uid}: task {t.get('task_id','?')} — unexpected error ({task_err}); including without media")
                if entry is None:
                    # build a minimal safe shell so the task still shows up in the backup
                    entry = {
                        "chat_id":       t.get("chat_id"),
                        "content_type":  t.get("content_type"),
                        "content_text":  t.get("content_text"),
                        "file_id":       t.get("file_id"),
                        "repeat_interval": t.get("repeat_interval"),
                        "start_time":    t.get("start_time"),
                        "media_bytes":   None,
                        "_export_error": str(task_err),
                    }

            export_tasks.append(entry)
    finally:
        if export_user_client:
            try:
                await export_user_client.stop()
            except Exception:
                pass

    return {
        "version":     EXPORT_VERSION,
        "exported_at": datetime.datetime.now(pytz.utc).isoformat(),
        "settings":    {"timezone": settings["timezone"] if settings else "UTC"},
        "channels":    channels,
        "tasks":       export_tasks,
    }


async def import_user_config(uid: int, data: dict,
                              progress_cb=None) -> tuple[int, list[str]]:
    """
    FIX 13 (import side): two-pass approach.
    Pass 1 — create all tasks with reply_target=None for task cross-references.
    Pass 2 — patch reply_target using the reply_task_index → new_task_id mapping.

    Also supports v3 exports (reply_task_index absent — reply_target kept as-is,
    which may not work cross-account but degrades gracefully).
    """
    if data.get("version") not in (1, 2, 3, 4):
        return 0, [f"Unsupported export version '{data.get('version')}'. Expected 3 or 4."]

    async def _progress(msg: str):
        if progress_cb:
            try:
                await progress_cb(msg)
            except Exception:
                pass

    errs: list[str] = []
    imported = 0
    now_utc  = datetime.datetime.now(pytz.utc)
    base_tid = int(now_utc.timestamp())

    if "settings" in data:
        tz_str = data["settings"].get("timezone", "UTC")
        try:
            pytz.timezone(tz_str)
            await set_user_tz(uid, tz_str)
        except Exception:
            errs.append(f"Invalid timezone '{tz_str}' — kept UTC.")

    session = await get_session(uid)
    if not session:
        errs.append(
            "⚠️ You are not logged in. Channels were imported without access_hash "
            "and media file IDs were not re-uploaded. Log in and re-import for full functionality."
        )
    user_client_ctx = None
    if session:
        user_client_ctx = Client(
            ":memory:", api_id=API_ID, api_hash=API_HASH,
            session_string=session,
            device_model="AutoCast", system_version="Railway", app_version="2.0"
        )
        await user_client_ctx.start()

    try:
        channels = data.get("channels", [])

        # ── FIX 14 / FIX 23: Three-tier peer resolution ─────────────────────────
        # Tier 0 — access_hash from the backup itself (valid for same account;
        #           skips all network calls for channels already cached in export).
        # Tier 1 — single dialog scan across BOTH the main folder (0) AND the
        #           archived folder (1).  Per-dialog errors are isolated so a bad
        #           entry never aborts the whole scan.
        # Tier 2 — get_chat() fallback for any remaining unresolved channels
        #           (works when the peer was cached during the scan, or can be
        #           looked up directly from Telegram).
        ah_map: dict[int, int] = {}
        if user_client_ctx and channels:
            await _progress(f"⏳ Scanning your dialogs to resolve {len(channels)} channel(s)…")
            for folder_id in (0, 1):           # 0 = main inbox, 1 = archive
                try:
                    async for dialog in user_client_ctx.get_dialogs(folder_id=folder_id):
                        try:
                            cid_d = dialog.chat.id
                            ah_d  = getattr(dialog.chat, "access_hash", 0) or 0
                            if ah_d:
                                ah_map[cid_d] = ah_d
                        except Exception:
                            continue          # bad dialog entry — skip, don't abort
                except Exception as e:
                    logger.warning(f"Import dialog scan folder={folder_id} uid={uid}: {e}")
            logger.info(f"Import uid={uid}: dialog scan found {len(ah_map)} peer(s) (main+archive)")

        for ch in channels:
            cid_str = str(ch["channel_id"])
            cid_int = int(cid_str)
            title   = ch.get("title", "Imported Channel")

            # Tier 0: use access_hash stored in the backup (same-account import
            #         — access_hash is per-account, so it stays valid across sessions)
            ah = int(ch.get("access_hash") or 0)

            # Tier 1: use the pre-built dialog map (covers different-account imports)
            if not ah:
                ah = ah_map.get(cid_int, 0)

            # Tier 2: single get_chat() call (works if peer was cached during scan)
            if not ah and user_client_ctx:
                try:
                    chat = await asyncio.wait_for(
                        user_client_ctx.get_chat(cid_int), timeout=8.0
                    )
                    ah = getattr(chat, "access_hash", 0) or 0
                    if getattr(chat, "title", None):
                        title = chat.title
                except Exception as e:
                    logger.debug(f"import get_chat {cid_str}: {e}")

            # ── FIX 15: per-channel warning when access_hash still missing ──
            # Previously this was silent — channels got stored with ah=0 and
            # every job then failed with "user must re-add it" in the logs,
            # with no visible feedback to the user at all.
            if not ah:
                errs.append(
                    f"⚠️ Channel **{title}** (`{cid_str}`): could not resolve peer. "
                    "Make sure your account is an admin of this channel, then use "
                    "➕ Add Channel to re-link it so scheduled posts can reach it."
                )
            try:
                await add_channel(uid, cid_str, title, ah)
            except Exception as e:
                errs.append(f"Channel {cid_str}: {e}")

        tasks = data.get("tasks", [])
        media_tasks = [t for t in tasks if t.get("content_type") not in ("text", "poll")
                       and (t.get("media_bytes") or t.get("file_id"))]
        if media_tasks:
            await _progress(
                f"⏳ Re-uploading {len(media_tasks)} media file(s) — this may take a moment…"
            )

        # ── Pass 1: create all tasks, record new task_ids ─────────────────────
        new_task_ids: list[str | None] = []  # indexed by original position; None on failure

        for i, t in enumerate(tasks):
            try:
                dt = _next_future_run(
                    t.get("start_time", ""),
                    t.get("repeat_interval"),
                    now_utc + datetime.timedelta(seconds=i * 10)
                )

                ct  = t.get("content_type", "text")
                fid = t.get("file_id")

                if ct not in ("text", "poll") and user_client_ctx:
                    raw_b64 = t.get("media_bytes")
                    if raw_b64:
                        try:
                            raw = base64.b64decode(raw_b64)
                            # FIX 3: pass no target_dummy — _upload_media_bytes uses "me"
                            new_fid = await _upload_media_bytes(
                                user_client_ctx, ct,
                                raw, t.get("content_text"), None
                            )
                            if new_fid:
                                fid = new_fid
                            else:
                                errs.append(
                                    f"Task #{i+1}: media re-upload failed — "
                                    "original file_id kept (may not work)."
                                )
                        except Exception as ue:
                            errs.append(f"Task #{i+1}: media decode/upload error: {ue}")

                tid = f"task_{base_tid}_imp_{i}"

                # reply_target: for task-references set None here; patched in pass 2.
                # For channel message IDs (from reply_target field), keep as-is.
                reply_target_val = t.get("reply_target")  # channel msg id or None
                if t.get("reply_task_index") is not None:
                    reply_target_val = None   # will be set in pass 2

                task_data = {
                    "task_id":            tid,
                    "owner_id":           uid,
                    "chat_id":            str(t["chat_id"]),
                    "content_type":       ct,
                    "content_text":       t.get("content_text"),
                    "file_id":            fid,
                    "entities":           t.get("entities"),
                    "pin":                bool(t.get("pin", True)),
                    "delete_old":         bool(t.get("delete_old", True)),
                    "repeat_interval":    t.get("repeat_interval"),
                    "start_time":         dt.isoformat(),
                    "last_msg_id":        None,
                    "auto_delete_offset": int(t.get("auto_delete_offset", 0)),
                    "reply_target":       reply_target_val,
                    "src_chat_id":        int(t.get("src_chat_id", 0) or 0),
                    "src_msg_id":         int(t.get("src_msg_id", 0) or 0),
                }
                await save_task(task_data)
                add_scheduler_job(task_data)
                new_task_ids.append(tid)
                imported += 1

            except Exception as e:
                errs.append(f"Task #{i + 1}: {e}")
                new_task_ids.append(None)

        # ── Pass 2: patch reply_targets that were task cross-references ────────
        pool = await get_db()
        for i, t in enumerate(tasks):
            reply_idx = t.get("reply_task_index")
            if reply_idx is None:
                continue
            try:
                reply_idx = int(reply_idx)
            except (TypeError, ValueError):
                continue
            if reply_idx < 0 or reply_idx >= len(new_task_ids):
                errs.append(f"Task #{i+1}: reply_task_index {reply_idx} out of range — reply skipped.")
                continue
            referenced_tid = new_task_ids[reply_idx]
            if referenced_tid is None:
                errs.append(f"Task #{i+1}: referenced task #{reply_idx+1} failed to import — reply skipped.")
                continue
            my_tid = new_task_ids[i]
            if my_tid is None:
                continue
            try:
                await pool.execute(
                    "UPDATE userbot_tasks_v11 SET reply_target=$1 WHERE task_id=$2",
                    referenced_tid, my_tid
                )
                # Re-register the scheduler job with the updated reply_target
                updated = await get_single_task(my_tid)
                if updated and scheduler:
                    add_scheduler_job(updated)
                logger.info(f"Import: patched reply_target for {my_tid} → {referenced_tid}")
            except Exception as e:
                errs.append(f"Task #{i+1}: failed to patch reply_target: {e}")

    finally:
        if user_client_ctx:
            try:
                await user_client_ctx.stop()
            except Exception:
                pass

    return imported, errs

# ─────────────────────────────────────────────────────────────────────────────
#  UI HELPERS
# ─────────────────────────────────────────────────────────────────────────────
async def update_menu(m, text, kb, uid, force_new=False):
    markup = InlineKeyboardMarkup(kb) if kb else None
    if force_new:
        try:
            sent = await app.send_message(m.chat.id, text, reply_markup=markup)
            # FIX 9: always update menu_msg_id regardless of whether uid is in user_state
            user_state.setdefault(uid, {})["menu_msg_id"] = sent.id
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
        # FIX 9: use setdefault so evicted users also get their menu_msg_id tracked
        user_state.setdefault(uid, {})["menu_msg_id"] = sent.id
    except Exception as e:
        logger.error(f"update_menu fallback send error: {e}")

async def show_main_menu(m, uid, force_new=False):
    tz = await get_user_tz(uid)
    tz_label = str(tz).replace("_", " ")
    engine_off = await get_engine_paused(uid)
    engine_btn = (
        InlineKeyboardButton("▶️ Start Engine", callback_data="engine_start")
        if engine_off else
        InlineKeyboardButton("🛑 Stop Engine",  callback_data="engine_stop")
    )
    status_line = "⚠️ **Engine is STOPPED** — all scheduled posts are paused." if engine_off else ""
    kb = [
        [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
        [InlineKeyboardButton("📢 My Channels",             callback_data="list_channels")],
        [InlineKeyboardButton("➕ Add Channel (Forward)",   callback_data="add_channel_forward"),
         InlineKeyboardButton("➕ Add by ID",               callback_data="add_channel_id")],
        [InlineKeyboardButton(f"🌐 Timezone: {tz_label}",   callback_data="tz_select")],
        [InlineKeyboardButton("⬆️ Export Config",           callback_data="export_config"),
         InlineKeyboardButton("⬇️ Import Config",           callback_data="import_config")],
        [engine_btn],
        [InlineKeyboardButton("🗑 Clear Database",          callback_data="clear_db")],
        [InlineKeyboardButton("🚪 Logout",                  callback_data="logout")],
    ]
    body = (
        "👋 **Welcome to AutoCast | Channel Manager!**\n\n"
        "Your central hub for managing scheduled posts across your Telegram channels."
    )
    if status_line:
        body += "\n\n" + status_line
    await update_menu(m, body, kb, uid, force_new)

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
    st = user_state.get(uid, {})
    editing_tid = st.get("editing_task_id")
    back_target = f"view_{editing_tid}" if editing_tid else "menu_home"
    kb = [
        [InlineKeyboardButton("⚡️ Now (5s delay)",  callback_data="time_0")],
        [InlineKeyboardButton("5 Minutes",           callback_data="time_5"),
         InlineKeyboardButton("15 Minutes",          callback_data="time_15")],
        [InlineKeyboardButton("30 Minutes",          callback_data="time_30"),
         InlineKeyboardButton("1 Hour",              callback_data="time_60")],
        [InlineKeyboardButton("📅 Custom Date/Time", callback_data="time_custom")],
        [InlineKeyboardButton("🔙 Back",             callback_data=back_target)],
    ]
    tz = await get_user_tz(uid)
    await update_menu(
        m,
        f"2️⃣ **Schedule Time** _(your timezone: {tz})_\n\nWhen would you like this post to be sent?",
        kb, uid, force_new
    )

async def ask_repetition(m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("🔂 Once (No Repeat)",  callback_data="rep_0")],
        [InlineKeyboardButton("Every 1 Hour",         callback_data="rep_60"),
         InlineKeyboardButton("Every 3 Hours",        callback_data="rep_180")],
        [InlineKeyboardButton("Every 6 Hours",        callback_data="rep_360"),
         InlineKeyboardButton("Every 12 Hours",       callback_data="rep_720")],
        [InlineKeyboardButton("Every 24 Hours",       callback_data="rep_1440")],
        [InlineKeyboardButton("Every 2 Days",         callback_data="rep_2880"),
         InlineKeyboardButton("Every 1 Week",         callback_data="rep_10080")],
        [InlineKeyboardButton("Every 2 Weeks",        callback_data="rep_20160")],
        [InlineKeyboardButton("🔙 Back",              callback_data="step_time")],
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
            p   = "✅" if post.get("pin")        else "❌"
            d   = "✅" if post.get("delete_old") else "❌"
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
        [InlineKeyboardButton(f"📌 Pin Message: {pin_icon}",     callback_data="toggle_pin")],
        [InlineKeyboardButton(f"🗑 Delete Previous: {del_icon}", callback_data="toggle_del")],
        [InlineKeyboardButton(off_text,                           callback_data="wizard_ask_offset")],
        [InlineKeyboardButton("➡️ Confirm",                      callback_data="goto_confirm")],
        [InlineKeyboardButton("🔙 Back",
            callback_data=st.get("editing_task_id") and f'view_{st.get("editing_task_id")}' or "step_rep")],
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
    t_str    = _ensure_utc(st["start_time"]).astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
    interval = st.get("interval")
    r_str    = interval if interval else "Once"
    queue    = st.get("broadcast_queue")

    if queue:
        type_str     = f"📦 Batch ({len(queue)} Posts)"
        pin_count    = sum(1 for p in queue if p.get("pin"))
        settings_str = f"📌 Pinning: {pin_count}/{len(queue)} Posts"
    else:
        type_map = {
            "text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
            "audio": "🎵 Audio", "voice": "🎙 Voice", "document": "📁 File",
            "poll": "📊 Poll", "animation": "🎞 GIF", "sticker": "✨ Sticker"
        }
        c_type       = st.get("content_type", "unknown")
        type_str     = type_map.get(c_type, c_type.upper())
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
    def _sort_key(t):
        try:
            return _ensure_utc(datetime.datetime.fromisoformat(t["start_time"]))
        except Exception:
            return datetime.datetime.min.replace(tzinfo=pytz.utc)
    tasks.sort(key=_sort_key)
    tz    = await get_user_tz(uid)
    icons = {"text": "📝", "photo": "📷", "video": "📹", "audio": "🎵", "poll": "📊"}
    kb = []
    for t in tasks:
        snippet  = (t["content_text"] or "Media")[:15] + "…"
        icon     = icons.get(t["content_type"], "📁")
        if t.get("is_paused"):
            icon = "⏸"
        try:
            dt = _ensure_utc(datetime.datetime.fromisoformat(t["start_time"]))
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
        dt = _ensure_utc(datetime.datetime.fromisoformat(t["start_time"]))
        time_str = dt.astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
    except Exception:
        time_str = t["start_time"]
    type_map = {
        "text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
        "audio": "🎵 Audio", "poll": "📊 Poll", "sticker": "✨ Sticker",
        "voice": "🎙 Voice", "document": "📁 File", "animation": "🎞 GIF"
    }
    type_str = type_map.get(t["content_type"], "📁 File")
    is_paused = bool(t.get("is_paused", False))
    pause_btn = (
        InlineKeyboardButton("▶️ Resume Task", callback_data=f"task_resume_{tid}")
        if is_paused else
        InlineKeyboardButton("⏸ Pause Task",  callback_data=f"task_pause_{tid}")
    )
    paused_line = "\n⏸ **Status: PAUSED** — this task will not fire until resumed." if is_paused else ""
    txt = (
        f"⚙️ **Task Details**\n\n"
        f"📂 **Type:** {type_str}\n"
        f"📝 **Content:** `{(t['content_text'] or 'Media')[:60]}…`\n"
        f"📅 **Scheduled:** `{time_str}`\n"
        f"🔁 **Repeat:** `{t['repeat_interval'] or 'Once'}`\n"
        f"📌 **Pin:** {'✅' if t['pin'] else '❌'} | "
        f"🗑 **Del Old:** {'✅' if t['delete_old'] else '❌'} | "
        f"⏰ **Auto-Delete:** "
        f"{str(t.get('auto_delete_offset') or 0)+'m' if t.get('auto_delete_offset') else 'OFF'}"
        f"{paused_line}"
    )
    kb = [
        [InlineKeyboardButton("👁 View Post",     callback_data=f"prev_{tid}"),
         InlineKeyboardButton("✏️ Change Post",  callback_data=f"edit_content_{tid}")],
        [InlineKeyboardButton("🗓 Reschedule",    callback_data=f"reschedule_{tid}"),
         InlineKeyboardButton("🔁 Repetition",   callback_data=f"edit_repeat_{tid}")],
        [InlineKeyboardButton("⚙️ Settings",     callback_data=f"edit_settings_{tid}")],
        [pause_btn,
         InlineKeyboardButton("🗑 Delete Task",  callback_data=f"del_task_{tid}")],
        [InlineKeyboardButton("🔙 Back to List",  callback_data=f"back_list_{t['chat_id']}")],
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
        await restore_wizard_state(uid)
    if await get_session(uid):
        await show_main_menu(m, uid, force_new=True)
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

    _touch(uid)
    _evict_stale()

    if uid not in user_state:
        user_state[uid] = {}
        await restore_wizard_state(uid)
    user_state[uid]["menu_msg_id"] = q.message.id

    try:
        await q.answer()
    except Exception:
        pass

    try:
        await _handle_callback(c, q, uid, d)
    except Exception as e:
        logger.error(f"Callback error [{d}] uid={uid}: {e}", exc_info=True)
        try:
            await q.answer("⚠️ Something went wrong. Please try again.", show_alert=True)
        except Exception:
            pass
    finally:
        await persist_wizard_state(uid)

async def _handle_callback(c, q, uid, d):
    if d == "menu_home":
        user_state[uid]["step"] = None
        await show_main_menu(q.message, uid)

    elif d == "tz_select":
        await show_tz_selector(uid, q.message)

    elif d.startswith("set_tz_"):
        tz_str = d[len("set_tz_"):]
        try:
            pytz.timezone(tz_str)
            await set_user_tz(uid, tz_str)
            await q.answer(f"✅ Timezone set to {tz_str}!", show_alert=False)
        except Exception:
            await app.send_message(uid, "❌ Invalid timezone selected.")
        await show_main_menu(q.message, uid)

    elif d == "login_start":
        old = login_state.get(uid)
        if old and old.get("client"):
            try:
                await old["client"].disconnect()
            except Exception:
                pass
        login_state[uid] = {"step": "waiting_phone"}
        await update_menu(
            q.message,
            "📱 **Step 1: Phone Number**\n\n"
            "Send your Telegram phone number with country code.\n"
            "Example: `+919876543210`",
            [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]],
            uid
        )

    elif d == "logout":
        pool = await get_db()
        count = await pool.fetchval(
            "SELECT COUNT(*) FROM userbot_tasks_v11 WHERE owner_id=$1", uid
        )
        kb = [
            [InlineKeyboardButton("⚠️ Yes, Continue", callback_data="logout_step_2")],
            [InlineKeyboardButton("🔙 Cancel",          callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            f"⚠️ **Logout — Step 1 of 3**\n\n"
            f"You have **{count} active task(s)** scheduled.\n\n"
            f"Logging out will **terminate your Telegram session** and "
            f"stop all scheduled posts permanently.",
            kb, uid
        )

    elif d == "logout_step_2":
        kb = [
            [InlineKeyboardButton("⚠️ Yes, I'm Sure", callback_data="logout_step_3")],
            [InlineKeyboardButton("🔙 Cancel",          callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            "🛑 **Logout — Step 2 of 3**\n\n"
            "This will **permanently delete** all your scheduled posts, "
            "channels and your saved session.\n\n"
            "Your Telegram account itself is **not** affected — only "
            "the data stored in this bot.\n\n"
            "Are you sure you want to continue?",
            kb, uid
        )

    elif d == "logout_step_3":
        kb = [
            [InlineKeyboardButton("🗑 Delete Everything & Logout", callback_data="logout_final")],
            [InlineKeyboardButton("🔙 Cancel",                      callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            "🚨 **Logout — Step 3 of 3 — FINAL WARNING**\n\n"
            "This is your **last chance** to go back.\n\n"
            "Pressing the button below will:\n"
            "• Remove all scheduled tasks\n"
            "• Unlink all channels\n"
            "• Terminate your Telegram session in this bot\n\n"
            "**This cannot be undone.**",
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

    elif d == "clear_db":
        pool = await get_db()
        task_count = await pool.fetchval(
            "SELECT COUNT(*) FROM userbot_tasks_v11 WHERE owner_id=$1", uid
        )
        ch_count = await pool.fetchval(
            "SELECT COUNT(*) FROM userbot_channels WHERE user_id=$1", uid
        )
        kb = [
            [InlineKeyboardButton("⚠️ Yes, Continue", callback_data="clear_db_step_2")],
            [InlineKeyboardButton("🔙 Cancel",          callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            f"🗑 **Clear Database — Step 1 of 3**\n\n"
            f"You have **{task_count} task(s)** and **{ch_count} channel(s)**.\n\n"
            f"This will delete all tasks, channels and settings.\n"
            f"**Your Telegram session is kept** — you will stay logged in.",
            kb, uid
        )

    elif d == "clear_db_step_2":
        kb = [
            [InlineKeyboardButton("⚠️ Yes, I'm Sure", callback_data="clear_db_step_3")],
            [InlineKeyboardButton("🔙 Cancel",          callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            "🛑 **Clear Database — Step 2 of 3**\n\n"
            "All scheduled posts will stop immediately.\n"
            "All linked channels will be unlinked.\n"
            "All settings (timezone, engine state) will be reset.\n\n"
            "Your login session will **not** be touched.\n\n"
            "Are you sure you want to continue?",
            kb, uid
        )

    elif d == "clear_db_step_3":
        kb = [
            [InlineKeyboardButton("🗑 Clear Everything", callback_data="clear_db_final")],
            [InlineKeyboardButton("🔙 Cancel",            callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            "🚨 **Clear Database — Step 3 of 3 — FINAL WARNING**\n\n"
            "This is your **last chance** to go back.\n\n"
            "Pressing the button below will permanently erase:\n"
            "• All scheduled tasks\n"
            "• All linked channels\n"
            "• All bot settings\n\n"
            "**This cannot be undone.**",
            kb, uid
        )

    elif d == "clear_db_final":
        try:
            await app.edit_message_text(
                uid, q.message.id,
                "⏳ **Clearing database…** removing all tasks and channels."
            )
        except Exception:
            pass
        await clear_db_data(uid)
        user_state[uid] = {}
        try:
            await app.send_message(
                uid,
                "✅ **Database cleared.**\n\n"
                "All tasks, channels and settings have been deleted.\n"
                "You are still logged in — use /manage to start fresh."
            )
        except Exception:
            pass

    elif d == "export_config":
        await q.answer("⏳ Generating export…")
        prog_msg = await app.send_message(uid, "⏳ **Exporting…** downloading media files, please wait.")
        try:
            data = await export_user_config(uid)
            media_count = sum(1 for t in data["tasks"] if t.get("media_bytes"))
            buf = io.BytesIO(json.dumps(data, indent=2, ensure_ascii=False).encode())
            buf.name = f"autocast_backup_{uid}.json"
            await prog_msg.delete()
            await app.send_document(
                uid, buf,
                caption=(
                    "📦 **AutoCast Configuration Export**\n\n"
                    f"✅ Tasks: **{len(data['tasks'])}**  |  "
                    f"📢 Channels: **{len(data['channels'])}**  |  "
                    f"🎞 Media embedded: **{media_count}**\n"
                    f"🕐 Exported: `{data['exported_at'][:19]} UTC`\n\n"
                    "This backup is **fully portable** — send it to any AutoCast "
                    "instance and use **⬇️ Import Config** to restore everything, "
                    "including media, channels, reply chains, and schedules."
                )
            )
        except Exception as e:
            logger.error(f"Export error uid={uid}: {e}", exc_info=True)
            try:
                await prog_msg.edit_text(
                    "❌ **Export failed.**\n\nPlease try again. "
                    f"If the problem persists, check bot logs.\n\n`{type(e).__name__}: {e}`"
                )
            except Exception:
                pass

    elif d == "import_config":
        user_state[uid]["step"] = "waiting_import"
        await update_menu(
            q.message,
            "⬇️ **Import Configuration**\n\n"
            "Send me a JSON backup file previously exported from AutoCast.\n\n"
            "✅ The import will automatically:\n"
            "• Re-upload all media to your account\n"
            "• Resolve channel access so posting works immediately\n"
            "• Reschedule tasks to the correct next future time\n"
            "• Restore reply chains between posts\n\n"
            "Large exports with many media files may take a minute.",
            [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]],
            uid
        )

    elif d.startswith("task_pause_"):
        tid = d[11:]
        await set_task_paused(tid, True)
        if scheduler:
            try: scheduler.pause_job(tid)
            except Exception: pass
        await show_task_details(uid, q.message, tid)

    elif d.startswith("task_resume_"):
        tid  = d[12:]
        task = await get_single_task(tid)
        if task:
            # FIX 18: if start_time is in the past when resuming, roll it forward
            # to the next correct future interval so the task doesn't fire
            # immediately as a misfire or show a stale past date to the user.
            try:
                st_dt = _ensure_utc(datetime.datetime.fromisoformat(task["start_time"]))
                now_u = datetime.datetime.now(pytz.utc)
                if st_dt < now_u and task.get("repeat_interval"):
                    mins  = int(task["repeat_interval"].split("=")[1])
                    delta = datetime.timedelta(minutes=mins)
                    # Advance by however many full intervals have elapsed + 1
                    elapsed  = now_u - st_dt
                    periods  = int(elapsed.total_seconds() / delta.total_seconds()) + 1
                    new_dt   = st_dt + delta * periods
                    new_iso  = new_dt.isoformat()
                    await update_next_run(tid, new_iso)
                    task["start_time"] = new_iso
                    logger.info(
                        f"Resume {tid}: advanced stale start_time "
                        f"from {st_dt.isoformat()} → {new_iso}"
                    )
            except Exception as e:
                logger.warning(f"Resume {tid}: could not advance start_time: {e}")
            # Re-register the scheduler job with the (possibly updated) start_time
            # so APScheduler picks up the correct next fire time.
            add_scheduler_job(task)
        await set_task_paused(tid, False)
        if scheduler:
            try: scheduler.resume_job(tid)
            except Exception: pass
        await show_task_details(uid, q.message, tid)

    elif d == "engine_stop":
        kb = [
            [InlineKeyboardButton("🛑 Yes, Stop All Posts", callback_data="engine_stop_confirm")],
            [InlineKeyboardButton("🔙 Cancel",               callback_data="menu_home")],
        ]
        await update_menu(
            q.message,
            "🛑 **Stop Engine?**\n\n"
            "This will pause **all** your scheduled tasks. "
            "No posts will be sent until you start the engine again.\n\n"
            "Your tasks and channels are kept — nothing is deleted.",
            kb, uid
        )

    elif d == "engine_stop_confirm":
        await set_engine_paused(uid, True)
        await set_all_tasks_paused(uid, True)
        if scheduler:
            pool = await get_db()
            tids = await pool.fetch(
                "SELECT task_id FROM userbot_tasks_v11 WHERE owner_id=$1", uid
            )
            for row in tids:
                try: scheduler.pause_job(row["task_id"])
                except Exception: pass
        await show_main_menu(q.message, uid)

    elif d == "engine_start":
        await set_engine_paused(uid, False)
        await set_all_tasks_paused(uid, False)
        if scheduler:
            pool = await get_db()
            all_tasks = await pool.fetch(
                "SELECT * FROM userbot_tasks_v11 WHERE owner_id=$1", uid
            )
            now_u = datetime.datetime.now(pytz.utc)
            for row in all_tasks:
                task_d = dict(row)
                # FIX 18 (engine): same stale-date fix applied to every task
                # when the whole engine is restarted after a long pause.
                try:
                    st_dt = _ensure_utc(
                        datetime.datetime.fromisoformat(task_d["start_time"])
                    )
                    if st_dt < now_u and task_d.get("repeat_interval"):
                        mins  = int(task_d["repeat_interval"].split("=")[1])
                        delta = datetime.timedelta(minutes=mins)
                        elapsed = now_u - st_dt
                        periods = int(elapsed.total_seconds() / delta.total_seconds()) + 1
                        new_dt  = st_dt + delta * periods
                        new_iso = new_dt.isoformat()
                        await update_next_run(task_d["task_id"], new_iso)
                        task_d["start_time"] = new_iso
                        logger.info(
                            f"Engine start: advanced {task_d['task_id']} "
                            f"from {st_dt.isoformat()} → {new_iso}"
                        )
                except Exception as e:
                    logger.warning(
                        f"Engine start: could not advance {task_d['task_id']}: {e}"
                    )
                # Re-register with updated start_time so APScheduler
                # computes the correct next fire time.
                try: add_scheduler_job(task_d)
                except Exception: pass
                try: scheduler.resume_job(task_d["task_id"])
                except Exception: pass
        await show_main_menu(q.message, uid)

    elif d.startswith("view_"):
        await show_task_details(uid, q.message, d[5:])

    elif d.startswith("back_list_"):
        await list_active_tasks(uid, q.message, d[10:])

    elif d.startswith("del_task_"):
        tid = d[9:]
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
            await app.send_message(uid, "❌ Task not found.")
            return
        type_map = {
            "text": "Text", "photo": "Photo", "video": "Video",
            "audio": "Audio", "poll": "Poll", "sticker": "Sticker",
            "voice": "Voice", "document": "File", "animation": "GIF"
        }
        ct  = task["content_type"]
        tz  = await get_user_tz(uid)
        try:
            dt = _ensure_utc(datetime.datetime.fromisoformat(task["start_time"]))
            time_str = dt.astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
        except Exception:
            time_str = "?"
        back_kb = [[InlineKeyboardButton("Back to Task", callback_data=f"view_{tid}")]]
        if ct == "text":
            body    = task["content_text"] or "(empty)"
            rep_str = task["repeat_interval"] or "Once"
            preview = (
                "Post Content\n" + ("-"*20) + "\n" + body + "\n" + ("-"*20) +
                "\nScheduled: " + time_str + "  |  Repeat: " + rep_str
            )
            await update_menu(q.message, preview, back_kb, uid)
        else:
            type_label = type_map.get(ct, ct.upper())
            rep_str2   = task["repeat_interval"] or "Once"
            summary    = (
                "Post Preview - " + type_label +
                "\n\nScheduled: " + time_str +
                "\nRepeat: " + rep_str2 +
                "\n\nMedia sent below (you can copy it)"
            )
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
                await app.send_message(
                    uid,
                    "Cannot retrieve media." + ("\n\nCaption: " + cap[:300] if cap else "")
                )

    elif d.startswith("edit_content_"):
        tid = d[13:]
        task = await get_single_task(tid)
        if not task:
            await app.send_message(uid, "❌ Task not found.")
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
            await app.send_message(uid, "❌ Task not found.")
            return
        user_state[uid].update({
            "editing_task_id":    tid,
            "content_type":       task["content_type"],
            "content_text":       task["content_text"],
            "file_id":            task["file_id"],
            "entities":           task["entities"],
            "pin":                task["pin"],
            "del":                task["delete_old"],
            "auto_delete_offset": task.get("auto_delete_offset", 0),
            "interval":           task["repeat_interval"],
            "step":               "rescheduling",
        })
        await show_time_menu(q.message, uid, force_new=False)

    elif d.startswith("edit_settings_"):
        tid = d[14:]
        task = await get_single_task(tid)
        if not task:
            await app.send_message(uid, "❌ Task not found.")
            return
        try:
            dt = _ensure_utc(datetime.datetime.fromisoformat(task["start_time"]))
        except Exception:
            dt = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=30)
        user_state[uid].update({
            "editing_task_id":    tid,
            "content_type":       task["content_type"],
            "content_text":       task["content_text"],
            "file_id":            task["file_id"],
            "entities":           task["entities"],
            "pin":                task["pin"],
            "del":                task["delete_old"],
            "auto_delete_offset": task.get("auto_delete_offset", 0),
            "interval":           task["repeat_interval"],
            "start_time":         dt,
        })
        await ask_settings(q.message, uid)

    elif d.startswith("edit_repeat_"):
        tid = d[12:]
        task = await get_single_task(tid)
        if not task:
            await app.send_message(uid, "❌ Task not found.")
            return
        try:
            dt = _ensure_utc(datetime.datetime.fromisoformat(task["start_time"]))
        except Exception:
            dt = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=30)
        user_state[uid].update({
            "editing_task_id":    tid,
            "content_type":       task["content_type"],
            "content_text":       task["content_text"],
            "file_id":            task["file_id"],
            "entities":           task["entities"],
            "pin":                task["pin"],
            "del":                task["delete_old"],
            "auto_delete_offset": task.get("auto_delete_offset", 0),
            "interval":           task["repeat_interval"],
            "start_time":         dt,
        })
        kb = [
            [InlineKeyboardButton("🔂 Once (No Repeat)",  callback_data="rep_0")],
            [InlineKeyboardButton("Every 1 Hour",         callback_data="rep_60"),
             InlineKeyboardButton("Every 3 Hours",        callback_data="rep_180")],
            [InlineKeyboardButton("Every 6 Hours",        callback_data="rep_360"),
             InlineKeyboardButton("Every 12 Hours",       callback_data="rep_720")],
            [InlineKeyboardButton("Every 24 Hours",       callback_data="rep_1440")],
            [InlineKeyboardButton("Every 2 Days",         callback_data="rep_2880"),
             InlineKeyboardButton("Every 1 Week",         callback_data="rep_10080")],
            [InlineKeyboardButton("Every 2 Weeks",        callback_data="rep_20160")],
            [InlineKeyboardButton("🔙 Back",              callback_data=f"view_{tid}")],
        ]
        await update_menu(
            q.message, "🔁 **Change Repetition**\n\nHow often should this task repeat?", kb, uid
        )

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
            await app.send_message(uid, "❌ Select at least one channel first.")
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

    elif d == "step_time":     await show_time_menu(q.message, uid)
    elif d == "step_rep":      await ask_repetition(q.message, uid)
    elif d == "step_settings": await ask_settings(q.message, uid)

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
        if user_state[uid].get("step") == "rescheduling":
            await confirm_task(q.message, uid)
        else:
            await ask_repetition(q.message, uid)

    elif d.startswith("rep_"):
        val = d[4:]
        user_state[uid]["interval"] = f"minutes={val}" if val != "0" else None
        editing_tid = user_state[uid].get("editing_task_id")
        if editing_tid:
            await update_task_logic(uid, q)
        else:
            await ask_settings(q.message, uid)

    elif d == "toggle_pin":
        st = user_state[uid]; st.setdefault("pin", True)
        st["pin"] = not st["pin"]
        await ask_settings(q.message, uid)

    elif d == "toggle_del":
        st = user_state[uid]; st.setdefault("del", True)
        st["del"] = not st["del"]
        await ask_settings(q.message, uid)

    elif d.startswith("wizard_ask_offset"):
        # FIX 11: parse index from suffix more robustly using rsplit
        suffix = d[len("wizard_ask_offset"):]
        idx_str = suffix.lstrip("_")
        temp_id = f"solo" if not idx_str else f"q_{idx_str}"
        markup  = await get_delete_before_kb(temp_id)
        await update_menu(
            q.message,
            "⏳ **Auto-Delete Timing**\n\n"
            "How long after posting should the message be automatically deleted?",
            markup.inline_keyboard, uid
        )

    elif d.startswith("set_del_off_"):
        # FIX 11: parse from the right so adding underscores to the prefix never breaks parsing
        # Patterns:
        #   set_del_off_solo_<offset>          → single post
        #   set_del_off_q_<idx>_<offset>       → broadcast queue item
        rest = d[len("set_del_off_"):]  # e.g. "solo_60" or "q_2_60"
        if rest.startswith("solo_"):
            offset = int(rest[len("solo_"):])
            user_state[uid]["auto_delete_offset"] = offset
            await q.answer(f"✅ Set to {offset}m" if offset > 0 else "✅ Disabled")
        elif rest.startswith("q_"):
            parts = rest[len("q_"):].split("_", 1)  # ["2", "60"]
            idx, offset = int(parts[0]), int(parts[1])
            queue = user_state[uid].get("broadcast_queue", [])
            if 0 <= idx < len(queue):
                queue[idx]["auto_delete_offset"] = offset
            await q.answer(f"✅ Post #{idx+1}: {offset}m" if offset > 0 else "✅ Disabled")
        await ask_settings(q.message, uid)

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
            f"⚙️ **Post #{idx+1} Settings**\n\nType: **{post['content_type']}**",
            kb, uid
        )

    elif d.startswith("t_q_"):
        parts  = d.split("_")
        action = parts[2]; idx = int(parts[3])
        post   = user_state[uid]["broadcast_queue"][idx]
        if action == "pin": post["pin"]        = not post.get("pin", True)
        if action == "del": post["delete_old"] = not post.get("delete_old", True)
        await _handle_callback(c, q, uid, f"cfg_q_{idx}")

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
    text = (m.text or m.caption or "").strip()

    _touch(uid)
    _evict_stale()

    if uid not in user_state:
        user_state[uid] = {}
        await restore_wizard_state(uid)

    if uid in login_state:
        await _handle_login(c, m, uid, text)
        return

    if text == "✅ Done Adding Posts":
        await m.reply("✅ All posts added!", reply_markup=ReplyKeyboardRemove())
        await show_time_menu(m, uid, force_new=True)
        await persist_wizard_state(uid)
        return

    if text == "❌ Cancel":
        await clear_wizard_state(uid)
        user_state[uid] = {}
        await m.reply("Operation cancelled.", reply_markup=ReplyKeyboardRemove())
        await start_cmd(c, m)
        return

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
    elif step == "waiting_import":
        await handle_import_file(c, m, uid)
    else:
        if text and not text.startswith("/"):
            await m.reply("Use the menu to interact with the bot. Type /start to open it.")

    await persist_wizard_state(uid)

# ─────────────────────────────────────────────────────────────────────────────
#  IMPORT FILE HANDLER
# ─────────────────────────────────────────────────────────────────────────────
async def handle_import_file(c, m, uid):
    if not m.document:
        await m.reply(
            "⚠️ Please send a **JSON file** (.json) exported from AutoCast.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")
            ]])
        )
        return

    if not (m.document.file_name or "").lower().endswith(".json"):
        await m.reply(
            "❌ Only `.json` files are supported. Please export from AutoCast first.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")
            ]])
        )
        return

    if m.document.file_size and m.document.file_size > 50 * 1024 * 1024:
        await m.reply("❌ File too large (max 50 MB).")
        return

    wait = await m.reply("⏳ Reading backup file…")

    # FIX 22: use the user's own Telegram session to download the backup file
    # instead of the bot's main connection.
    #
    # Root cause of the repeated timeouts: the bot's main `app` connection shares
    # a single TCP pipe with all the concurrent userbot job clients that fire on
    # startup.  Those clients flood the connection with upload.GetFile retries,
    # starving the bot's own download and causing consistent 60s timeouts.
    #
    # The user's session runs on its own independent connection (often a different
    # DC) and is not affected by the job client congestion.  We pass the file_id
    # directly so no peer resolution is needed.
    #
    # Strategy:
    #   1. If user has a session → download via user client (fast, isolated).
    #   2. Fall back to bot client with up to 5 attempts and exponential backoff.
    file_id  = m.document.file_id
    data     = None
    last_err = None

    session = await get_session(uid)
    if session:
        try:
            async with Client(
                ":memory:", api_id=API_ID, api_hash=API_HASH,
                session_string=session,
                device_model="AutoCast", system_version="Railway", app_version="2.0"
            ) as _uc:
                buf = await asyncio.wait_for(
                    _uc.download_media(file_id, in_memory=True), timeout=90.0
                )
                if buf:
                    data = json.loads(bytes(buf.getbuffer()))
                    logger.info(f"Import file downloaded via user session uid={uid}")
        except asyncio.TimeoutError:
            last_err = "user-session download timed out after 90s"
            logger.warning(f"Import: user-session download timed out uid={uid}")
        except Exception as e:
            last_err = str(e)
            logger.warning(f"Import: user-session download failed uid={uid}: {e}")

    # Fallback: bot client with exponential backoff (5 attempts: 0s, 5s, 10s, 20s, 40s)
    if data is None:
        for _attempt in range(1, 6):
            delay = 0 if _attempt == 1 else 5 * (2 ** (_attempt - 2))  # 0,5,10,20,40
            if delay:
                try:
                    await wait.edit_text(
                        f"⚠️ Download attempt {_attempt}/5 failed — retrying in {delay}s…\n"
                        f"`{last_err}`"
                    )
                except Exception:
                    pass
                await asyncio.sleep(delay)
            try:
                buf = await asyncio.wait_for(
                    app.download_media(file_id, in_memory=True), timeout=90.0
                )
                if buf:
                    data = json.loads(bytes(buf.getbuffer()))
                    logger.info(f"Import file downloaded via bot client attempt {_attempt} uid={uid}")
                    break
            except asyncio.TimeoutError:
                last_err = f"bot download timed out after 90s (attempt {_attempt})"
                logger.warning(f"Import: bot download attempt {_attempt}/5 timed out uid={uid}")
            except Exception as e:
                last_err = str(e)
                logger.warning(f"Import: bot download attempt {_attempt}/5 failed uid={uid}: {e}")

    if data is None:
        await wait.edit_text(
            f"❌ **Could not download the backup file.**\n\n"
            f"Last error: `{last_err}`\n\n"
            "The server is under heavy load. Please wait 1–2 minutes and send the file again."
        )
        return

    tasks_count    = len(data.get("tasks", []))
    channels_count = len(data.get("channels", []))
    await wait.edit_text(
        f"⏳ **Importing…**\n\n"
        f"📋 Tasks: **{tasks_count}** | 📢 Channels: **{channels_count}**\n\n"
        f"Re-uploading media and resolving channels — please wait."
    )

    async def _update_progress(msg: str):
        try:
            await wait.edit_text(msg)
        except Exception:
            pass

    imported, errs = await import_user_config(uid, data, progress_cb=_update_progress)
    user_state[uid]["step"] = None

    lines = [f"✅ **Import Complete** — {imported} task(s) scheduled."]
    if errs:
        lines.append(f"\n⚠️ **{len(errs)} warning(s):**")
        for err in errs[:10]:
            lines.append(f"  • {err}")
        if len(errs) > 10:
            lines.append(f"  …and {len(errs)-10} more.")

    await wait.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Home", callback_data="menu_home")
        ]])
    )

# ─────────────────────────────────────────────────────────────────────────────
#  FIX 8 — LOGIN FLOW
#  login_state.pop() moved to finally so it always runs even if an exception
#  fires before the explicit pop in the try block.
# ─────────────────────────────────────────────────────────────────────────────
class _NullClient:
    async def disconnect(self): pass

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
            await m.reply(
                f"❌ **Connection failed:** {e}\n\nCheck your phone number and try /start again."
            )

    elif st["step"] == "waiting_code":
        advance_to_password = False
        try:
            code = text.lower().replace("aa", "").strip()
            await st["client"].sign_in(st["phone"], st["hash"], code)
            session = await st["client"].export_session_string()
            await save_session(uid, session)
            await m.reply(
                "✅ **Login Successful!**\n\nClick /manage to start scheduling.",
                reply_markup=ReplyKeyboardRemove()
            )
        except errors.PhoneCodeInvalid:
            await m.reply("❌ Wrong code. Try again (remember to prefix with `aa`).")
        except errors.SessionPasswordNeeded:
            advance_to_password = True
            st["step"] = "waiting_password"
            await update_menu(
                m,
                "🔒 **Two-Step Verification**\n\nEnter your cloud password:",
                None, uid, force_new=True
            )
        except Exception as e:
            logger.error(f"Login code error uid={uid}: {e}")
            await m.reply(f"❌ Login error: {e}\n\nTry /start again.")
        finally:
            # FIX 8: always clean up unless transitioning to password step
            if not advance_to_password:
                login_state.pop(uid, None)
                try:
                    await st.get("client", _NullClient()).disconnect()
                except Exception:
                    pass

    elif st["step"] == "waiting_password":
        success = False
        try:
            await st["client"].check_password(text)
            session = await st["client"].export_session_string()
            await save_session(uid, session)
            success = True
            await m.reply(
                "✅ **Login Successful!**\n\nClick /manage to start scheduling.",
                reply_markup=ReplyKeyboardRemove()
            )
        except errors.PasswordHashInvalid:
            await m.reply("❌ Wrong password. Try again.")
        except Exception as e:
            logger.error(f"Login password error uid={uid}: {e}")
            await m.reply(f"❌ Error: {e}\n\nTry /start again.")
        finally:
            # FIX 8: clean up on success or unrecoverable error; keep alive only for retry
            if success or (st.get("step") == "waiting_password" and
                           not isinstance(sys.exc_info()[1], errors.PasswordHashInvalid)):
                login_state.pop(uid, None)
                try:
                    await st.get("client", _NullClient()).disconnect()
                except Exception:
                    pass

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
            access_hash = await warm_peer_and_get_hash(user_client, uid, cid)
            try:
                chat  = await user_client.get_chat(cid)
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
            access_hash = await warm_peer_and_get_hash(user_client, uid, channel_id)
            if not access_hash:
                await m.reply(
                    "❌ Channel not found in your dialogs.\n\n"
                    "Make sure you are a **member or admin** of this channel and try again."
                )
                return
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
    if m.photo:     return m.photo.file_id
    if m.video:     return m.video.file_id
    if m.audio:     return m.audio.file_id
    if m.voice:     return m.voice.file_id
    if m.document:  return m.document.file_id
    if m.animation: return m.animation.file_id
    if m.sticker:   return m.sticker.file_id
    return None

async def process_content_message(c, m, uid):
    st = user_state[uid]
    st["content_type"] = m.media.value if m.media else "text"
    st["content_text"] = m.caption or m.text
    st["file_id"]      = _extract_file_id(m)
    st["entities"]     = serialize_entities(m.caption_entities or m.entities)
    st["input_msg_id"] = m.id
    if m.forward_from_chat and m.forward_from_message_id:
        st["src_chat_id"] = m.forward_from_chat.id
        st["src_msg_id"]  = m.forward_from_message_id
    else:
        st["src_chat_id"] = m.chat.id
        st["src_msg_id"]  = m.id
    st["reply_to_channel_msg_id"] = None
    if m.reply_to_message:
        fwd_msg_id = getattr(m.reply_to_message, "forward_from_message_id", None)
        if fwd_msg_id:
            st["reply_to_channel_msg_id"] = fwd_msg_id
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
        "src_chat_id":        (m.forward_from_chat.id
                               if m.forward_from_chat and m.forward_from_message_id
                               else m.chat.id),
        "src_msg_id":         (m.forward_from_message_id
                               if m.forward_from_chat and m.forward_from_message_id
                               else m.id),
        "pin":                True,
        "delete_old":         True,
        "auto_delete_offset": 0,
    }
    if m.reply_to_message:
        post["reply_ref_id"] = m.reply_to_message.id
    queue.append(post)
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
    for fmt in ("%d-%b-%Y %I:%M %p", "%d-%b-%Y %H:%M", "%d/%m/%Y %I:%M %p", "%d/%m/%Y %H:%M"):
        try:
            dt = datetime.datetime.strptime(txt, fmt)
            # pytz.localize is correct here — it handles DST transitions properly
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
#  TASK CREATION / EDITING
# ─────────────────────────────────────────────────────────────────────────────
async def process_content_edit_message(c, m, uid):
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
    try:
        st["start_time"] = _ensure_utc(datetime.datetime.fromisoformat(task["start_time"]))
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
    st  = user_state[uid]
    tid = st.get("editing_task_id")
    if not tid:
        await create_task_logic(uid, q)
        return

    task = await get_single_task(tid)
    if not task:
        await app.send_message(uid, "❌ Task not found.")
        return

    if scheduler:
        try: scheduler.remove_job(tid)
        except Exception: pass

    start_time = st.get("start_time")
    if not start_time:
        try:
            start_time = _ensure_utc(datetime.datetime.fromisoformat(task["start_time"]))
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
        "start_time":         _ensure_utc(start_time).isoformat(),
        "last_msg_id":        task["last_msg_id"],
        "reply_target":       task.get("reply_target"),
        "src_chat_id":        st.get("src_chat_id",  task.get("src_chat_id", 0)),
        "src_msg_id":         st.get("src_msg_id",   task.get("src_msg_id", 0)),
    }

    await save_task(updated)
    add_scheduler_job(updated)

    for key in ("editing_task_id", "content_type", "content_text", "file_id",
                "entities", "pin", "del", "auto_delete_offset", "interval",
                "start_time", "src_chat_id", "src_msg_id", "step"):
        user_state[uid].pop(key, None)

    await q.answer("✅ Task updated!")
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
    start_time = _ensure_utc(st["start_time"])
    t_str      = start_time.astimezone(tz).strftime("%d-%b-%Y %I:%M %p %Z")
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
            run_time = start_time + datetime.timedelta(seconds=pi * 10)

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
                "reply_target":       reply_target or str(st.get("reply_to_channel_msg_id") or ""),
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
#  FIX 2 — SCHEDULER JOB
#  Removed the global queue_lock that serialised ALL users' jobs behind a single
#  asyncio.Lock. The PostgreSQL advisory lock already prevents multi-instance
#  duplicate execution. The queue_lock caused a FloodWait in one user's job to
#  block every other user's posts for the entire wait duration.
# ─────────────────────────────────────────────────────────────────────────────
def add_scheduler_job(t):
    if scheduler is None:
        return
    tid = t["task_id"]
    dt  = _ensure_utc(datetime.datetime.fromisoformat(t["start_time"]))

    async def job_func():
        pool = await get_db()
        lock_key = int.from_bytes(hashlib.sha256(tid.encode()).digest()[:4], 'big') & 0x7FFFFFFF
        async with pool.acquire() as lock_conn:
            got_lock = await lock_conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", lock_key
            )
            if not got_lock:
                logger.info(f"Job {tid}: skipped — advisory lock held by another instance")
                return
            try:
                # FIX 2: _run_job called directly — no queue_lock wrapper
                await _run_job(tid)
            finally:
                try:
                    await lock_conn.execute("SELECT pg_advisory_unlock($1)", lock_key)
                except Exception:
                    pass

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

    if t.get("is_paused"):
        try:
            scheduler.pause_job(tid)
        except Exception:
            pass

async def _run_job(tid: str):
    """Core job logic. Called inside the PostgreSQL advisory lock."""
    logger.info(f"🚀 Job {tid} triggered")
    fresh = await get_single_task(tid)
    if not fresh:
        logger.warning(f"Job {tid}: task gone from DB, skipping")
        return

    # Compute next_iso BEFORE the paused check so the schedule always advances,
    # even when the task is paused and the message is not sent (FIX 17).
    next_iso = None
    if fresh["repeat_interval"]:
        try:
            mins = int(fresh["repeat_interval"].split("=")[1])
            next_iso = (
                datetime.datetime.now(pytz.utc) + datetime.timedelta(minutes=mins)
            ).isoformat()
        except Exception as e:
            logger.error(f"Next-run calc error for {tid}: {e}")

    if fresh.get("is_paused") or await get_engine_paused(fresh["owner_id"]):
        logger.info(f"Job {tid}: skipped (paused) — advancing schedule to {next_iso}")
        # FIX 17: still update start_time in DB so the next scheduled time stays
        # current while the task is paused. Without this, pausing a task freezes
        # its start_time in the past and the first post after resume appears
        # immediately (or is treated as a misfire) rather than at the correct interval.
        if next_iso:
            try:
                await update_next_run(tid, next_iso)
            except Exception as e:
                logger.warning(f"Job {tid}: failed to advance paused schedule: {e}")
        return

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
            access_hash = await get_channel_access_hash(fresh["owner_id"], fresh["chat_id"])

            # FIX 1: use _warm_peer_in_client instead of broken inject_peer
            if access_hash:
                await _warm_peer_in_client(user, target_int, access_hash)
            else:
                logger.info(f"Job {tid}: access_hash missing, scanning dialogs…")
                access_hash = await warm_peer_and_get_hash(
                    user, fresh["owner_id"], target_int
                )
                if not access_hash:
                    logger.error(
                        f"Job {tid}: cannot resolve channel {fresh['chat_id']} — "
                        "user must re-add it"
                    )
                    # ── FIX 16: notify user + auto-pause on unresolvable channel ──
                    # Previously this just returned silently. The task would retry
                    # on every interval, log the same error each time, and the user
                    # had no idea anything was wrong until they checked Railway logs.
                    # Fix: pause the task immediately so it stops firing, then send
                    # the user a clear Telegram notification with remediation steps.
                    try:
                        await set_task_paused(tid, True)
                        if scheduler:
                            try:
                                scheduler.pause_job(tid)
                            except Exception:
                                pass
                        pool_n = await get_db()
                        ch_row = await pool_n.fetchrow(
                            "SELECT title FROM userbot_channels "
                            "WHERE user_id=$1 AND channel_id=$2",
                            fresh["owner_id"], fresh["chat_id"]
                        )
                        ch_label = (ch_row["title"] if ch_row else None) or fresh["chat_id"]
                        await app.send_message(
                            fresh["owner_id"],
                            f"⚠️ **Scheduled post paused**\n\n"
                            f"Task `{tid}` could not reach channel **{ch_label}** "
                            f"(`{fresh['chat_id']}`) — your account is no longer "
                            f"recognised as an admin there.\n\n"
                            f"This task has been **paused automatically** to stop "
                            f"repeated failures.\n\n"
                            f"**To fix:**\n"
                            f"1. Confirm your account is still an admin of that channel.\n"
                            f"2. Re-add it via ➕ **Add Channel**.\n"
                            f"3. Open the task and tap ▶️ **Resume Task**."
                        )
                    except Exception as notify_err:
                        logger.warning(
                            f"Job {tid}: failed to send unresolvable-channel "
                            f"notification: {notify_err}"
                        )
                    return
                await _warm_peer_in_client(user, target_int, access_hash)

            caption  = fresh["content_text"]
            ents     = deserialize_entities(fresh["entities"])
            reply_id = None

            if fresh["delete_old"] and fresh.get("last_msg_id"):
                try:
                    await user.delete_messages(target_int, fresh["last_msg_id"])
                    logger.info(f"Job {tid}: deleted old msg {fresh['last_msg_id']}")
                except (errors.MessageDeleteForbidden, errors.MessageIdInvalid):
                    pass
                except Exception as e:
                    logger.warning(f"Job {tid}: delete old failed: {e}")

            if fresh.get("reply_target"):
                ref_val = str(fresh["reply_target"]).strip()
                if ref_val.startswith("task_"):
                    ref = await get_single_task(ref_val)
                    if ref and ref.get("last_msg_id"):
                        reply_id = ref["last_msg_id"]
                    else:
                        logger.info(
                            f"Job {tid}: reply_target '{ref_val}' has no last_msg_id yet "
                            "— posting without thread reply"
                        )
                elif ref_val:
                    try:
                        reply_id = int(ref_val)
                    except (ValueError, TypeError):
                        logger.warning(f"Job {tid}: invalid reply_target '{ref_val}' — ignored")

            ct  = fresh["content_type"]
            fid = fresh["file_id"]

            # ── FIX 26: Pre-refresh file_id from source message ───────────────────
            # Stored file_ids contain a "file reference" that Telegram expires over
            # time.  Using a stale fid triggers FILE_REFERENCE_EXPIRED inside
            # upload.GetFile, which Pyrogram retries 4× (each time reconnecting)
            # before raising — producing the reconnect storm visible in the logs.
            # Fix: fetch the source message via the user's own session to get a
            # guaranteed-fresh file_id before every send attempt.
            if ct not in ("text", "poll") and fid:
                _src_cid = int(fresh.get("src_chat_id") or 0)
                _src_mid = int(fresh.get("src_msg_id") or 0)
                logger.info(f"Job {tid}: media send — ct={ct} src={_src_cid}/{_src_mid} fid={fid[:30] if fid else None}")

                # ── Tier A: staging message in Saved Messages (FIX 27 keeps it alive) ──
                if _src_cid and _src_mid:
                    try:
                        _src_msg = await asyncio.wait_for(
                            user.get_messages(_src_cid, _src_mid), timeout=15.0
                        )
                        _fresh_fid = _extract_media_file_id(_src_msg, ct)
                        if _fresh_fid:
                            fid = _fresh_fid
                            logger.info(f"Job {tid}: file_id pre-refreshed OK from Saved Messages msg {_src_mid}")
                        else:
                            logger.warning(f"Job {tid}: Saved Messages msg {_src_mid} has no {ct} (was deleted before FIX 27) — trying last_msg fallback")
                    except Exception as _fre:
                        logger.warning(f"Job {tid}: Saved Messages fetch failed ({_fre}) — trying last_msg fallback")

                # ── Tier B: last sent message in target channel (fallback for old tasks) ──
                # For tasks created before FIX 27, the staging msg was deleted.
                # The last successfully sent message in the channel still has the media
                # with a fresh file reference — use it to refresh.
                if fid == fresh["file_id"]:    # still on stale fid after tier A
                    _last_mid = int(fresh.get("last_msg_id") or 0)
                    if _last_mid:
                        try:
                            _last_msg = await asyncio.wait_for(
                                user.get_messages(target_int, _last_mid), timeout=15.0
                            )
                            _last_fid = _extract_media_file_id(_last_msg, ct)
                            if _last_fid:
                                fid = _last_fid
                                logger.info(f"Job {tid}: file_id refreshed from last sent msg {_last_mid} in channel")
                            else:
                                logger.warning(f"Job {tid}: last sent msg {_last_mid} has no {ct} — stored fid will be used (may expire)")
                        except Exception as _lfe:
                            logger.warning(f"Job {tid}: last_msg fetch failed ({_lfe}) — stored fid will be used")
                    else:
                        logger.warning(f"Job {tid}: no last_msg_id and no valid src msg — stored fid may be stale")

            async def _send():
                nonlocal sent
                if ct == "text":
                    sent = await user.send_message(
                        target_int, caption or " ",
                        entities=ents, parse_mode=None,
                        reply_to_message_id=reply_id,
                        disable_web_page_preview=True
                    )
                elif ct == "poll":
                    pd = json.loads(caption)
                    sent = await user.send_poll(
                        target_int, pd["question"], pd["options"],
                        reply_to_message_id=reply_id
                    )
                elif ct == "photo":
                    sent = await user.send_photo(target_int, fid, caption=caption,
                        caption_entities=ents, reply_to_message_id=reply_id)
                elif ct == "video":
                    sent = await user.send_video(target_int, fid, caption=caption,
                        caption_entities=ents, reply_to_message_id=reply_id)
                elif ct == "animation":
                    sent = await user.send_animation(target_int, fid, caption=caption,
                        caption_entities=ents, reply_to_message_id=reply_id)
                elif ct == "document":
                    sent = await user.send_document(target_int, fid, caption=caption,
                        caption_entities=ents, reply_to_message_id=reply_id)
                elif ct == "sticker":
                    sent = await user.send_sticker(target_int, fid,
                        reply_to_message_id=reply_id)
                elif ct == "audio":
                    sent = await user.send_audio(target_int, fid, caption=caption,
                        caption_entities=ents, reply_to_message_id=reply_id)
                elif ct == "voice":
                    sent = await user.send_voice(target_int, fid, caption=caption,
                        reply_to_message_id=reply_id)

            try:
                await _send()
            except (errors.FileIdInvalid, errors.MediaEmpty, errors.FileReferenceExpired) as _send_err:
                logger.warning(f"Job {tid}: {ct} send failed ({type(_send_err).__name__}) — attempting recovery")
                media = None

                # ── Recovery path 1: refresh file_id via user session + retry ──────
                # Most reliable: the user can always read their own Saved Messages
                # (where media was staged on import) to get a fresh file reference.
                _rec_cid = int(fresh.get("src_chat_id") or 0)
                _rec_mid = int(fresh.get("src_msg_id") or 0)
                if _rec_cid and _rec_mid:
                    try:
                        _rec_msg = await asyncio.wait_for(
                            user.get_messages(_rec_cid, _rec_mid), timeout=15.0
                        )
                        _rec_fid = _extract_media_file_id(_rec_msg, ct)
                        if _rec_fid:
                            fid = _rec_fid          # update closure for _send()
                            try:
                                await _send()       # retry with fresh fid
                                media = "sent"      # signal that send succeeded
                                logger.info(f"Job {tid}: recovered via refreshed file_id from src {_rec_cid}/{_rec_mid}")
                            except Exception as _retry_err:
                                logger.warning(f"Job {tid}: retry with refreshed fid failed: {_retry_err}")
                    except Exception as _rec_err:
                        logger.warning(f"Job {tid}: could not fetch src msg for recovery: {_rec_err}")

                if media != "sent":
                    # ── Recovery path 2: download raw bytes via user session + re-upload ──
                    if fid:
                        try:
                            media = await asyncio.wait_for(
                                user.download_media(fid, in_memory=True), timeout=45.0
                            )
                        except errors.FileReferenceExpired:
                            logger.warning(f"Job {tid}: user download also expired — trying bot client")
                        except asyncio.TimeoutError:
                            logger.warning(f"Job {tid}: user download timed out after 45s")
                        except Exception as dl_e:
                            logger.warning(f"Job {tid}: user download failed: {dl_e}")
                    # ── Recovery path 3: bot client forward (legacy fallback) ─────────
                    if media is None and fresh.get("src_chat_id") and fresh.get("src_msg_id"):
                        try:
                            media_msg = await app.forward_messages(
                                fresh["owner_id"],
                                from_chat_id=fresh["src_chat_id"],
                                message_ids=fresh["src_msg_id"]
                            )
                            media = await asyncio.wait_for(
                                app.download_media(media_msg, in_memory=True), timeout=45.0
                            )
                            await media_msg.delete()
                        except asyncio.TimeoutError:
                            logger.warning(f"Job {tid}: src forward download timed out after 45s")
                        except Exception as fwd_e:
                            logger.warning(f"Job {tid}: src forward failed: {fwd_e}")
                if media == "sent":
                    pass  # already sent via recovery path 1 — sent is set by _send()
                elif media is None:
                    logger.error(f"Job {tid}: cannot recover media for {ct}, skipping")
                    sent = None
                else:
                    send_map = {
                        "photo":     lambda: user.send_photo(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id),
                        "video":     lambda: user.send_video(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id),
                        "animation": lambda: user.send_animation(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id),
                        "document":  lambda: user.send_document(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id),
                        "sticker":   lambda: user.send_sticker(target_int, media, reply_to_message_id=reply_id),
                    }
                    if ct == "audio":
                        media.name = "audio.mp3"
                        sent = await user.send_audio(target_int, media, caption=caption, caption_entities=ents, reply_to_message_id=reply_id)
                    elif ct == "voice":
                        media.name = "voice.ogg"
                        sent = await user.send_voice(target_int, media, caption=caption, reply_to_message_id=reply_id)
                    elif ct in send_map:
                        sent = await send_map[ct]()
                    else:
                        logger.error(f"Job {tid}: no re-upload handler for type '{ct}'")
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
        wait_secs = e.value + 5
        logger.warning(f"Job {tid}: FloodWait {e.value}s — rescheduling in {wait_secs}s")
        retry_at = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=wait_secs)
        async def _fw_retry(_tid=tid):
            await _run_job(_tid)
        try:
            scheduler.add_job(
                _fw_retry,
                DateTrigger(run_date=retry_at, timezone=pytz.utc),
                id=f"{tid}_fw_{int(retry_at.timestamp())}",
                replace_existing=True,
                misfire_grace_time=3600
            )
        except Exception as sched_err:
            logger.error(f"Job {tid}: failed to reschedule after FloodWait: {sched_err}")

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

# ─────────────────────────────────────────────────────────────────────────────
#  STARTUP
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    check_env_vars()
    _init_encryption()

    global scheduler
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
