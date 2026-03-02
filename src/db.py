import asyncpg
import json
import logging
import pytz  # <-- ADDED
from typing import Optional, List, Dict, Any  # <-- ADDED
from cryptography.fernet import Fernet
from pyrogram.types import MessageEntity
from pyrogram import enums
from src.config import DATABASE_URL, FERNET_KEY, DEFAULT_TZ

logger = logging.getLogger(__name__)
fernet = Fernet(FERNET_KEY)

_db_pool = None

async def get_db() -> asyncpg.Pool:
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(DATABASE_URL)
    return _db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_tasks_v11 (
                task_id TEXT PRIMARY KEY,
                owner_id BIGINT,
                chat_id TEXT,
                content_type TEXT,
                content_text TEXT,
                file_id TEXT,
                entities TEXT,
                pin BOOLEAN DEFAULT FALSE,
                delete_old BOOLEAN DEFAULT FALSE,
                repeat_interval TEXT,
                start_time TEXT,
                last_msg_id BIGINT,
                auto_delete_offset INTEGER DEFAULT 0,
                reply_target TEXT,
                fail_count INTEGER DEFAULT 0
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id BIGINT PRIMARY KEY,
                timezone TEXT DEFAULT 'Asia/Kolkata'
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_sessions (
                user_id BIGINT PRIMARY KEY,
                session_string TEXT NOT NULL
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS userbot_channels (
                user_id BIGINT,
                channel_id TEXT,
                title TEXT,
                PRIMARY KEY (user_id, channel_id)
            );
        """)
    logger.info("✅ Database initialized")

# ------------------- Encryption -------------------
def encrypt_session(session_str: str) -> str:
    return fernet.encrypt(session_str.encode()).decode()

def decrypt_session(encrypted: str) -> str:
    return fernet.decrypt(encrypted.encode()).decode()

# ------------------- Sessions -------------------
async def get_session(user_id: int) -> Optional[str]:
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT session_string FROM userbot_sessions WHERE user_id = $1",
        user_id
    )
    if row:
        try:
            return decrypt_session(row["session_string"])
        except Exception as e:
            logger.error(f"Failed to decrypt session for user {user_id}: {e}")
    return None

async def save_session(user_id: int, session_str: str):
    encrypted = encrypt_session(session_str)
    pool = await get_db()
    await pool.execute(
        "INSERT INTO userbot_sessions (user_id, session_string) VALUES ($1, $2) "
        "ON CONFLICT (user_id) DO UPDATE SET session_string = $2",
        user_id, encrypted
    )

async def del_session(user_id: int):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_sessions WHERE user_id = $1", user_id)

# ------------------- User Settings -------------------
async def get_user_timezone(user_id: int):
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT timezone FROM user_settings WHERE user_id = $1",
        user_id
    )
    if row:
        try:
            return pytz.timezone(row["timezone"])
        except Exception:
            pass
    return DEFAULT_TZ

async def set_user_timezone(user_id: int, tz_name: str):
    pool = await get_db()
    await pool.execute(
        "INSERT INTO user_settings (user_id, timezone) VALUES ($1, $2) "
        "ON CONFLICT (user_id) DO UPDATE SET timezone = $2",
        user_id, tz_name
    )

# ------------------- Channels -------------------
async def add_channel(user_id: int, channel_id: str, title: str):
    pool = await get_db()
    await pool.execute(
        "INSERT INTO userbot_channels (user_id, channel_id, title) VALUES ($1, $2, $3) "
        "ON CONFLICT (user_id, channel_id) DO UPDATE SET title = $3",
        user_id, channel_id, title
    )

async def get_channels(user_id: int) -> List[Dict]:
    pool = await get_db()
    rows = await pool.fetch(
        "SELECT * FROM userbot_channels WHERE user_id = $1",
        user_id
    )
    return [dict(r) for r in rows]

async def del_channel(user_id: int, channel_id: str):
    pool = await get_db()
    # Delete associated tasks first
    await pool.execute(
        "DELETE FROM userbot_tasks_v11 WHERE chat_id = $1",
        channel_id
    )
    await pool.execute(
        "DELETE FROM userbot_channels WHERE user_id = $1 AND channel_id = $2",
        user_id, channel_id
    )

# ------------------- Tasks -------------------
async def save_task(task: Dict[str, Any]):
    pool = await get_db()
    await pool.execute(
        """
        INSERT INTO userbot_tasks_v11 (
            task_id, owner_id, chat_id, content_type, content_text, file_id,
            entities, pin, delete_old, repeat_interval, start_time, last_msg_id,
            auto_delete_offset, reply_target, fail_count
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (task_id) DO UPDATE SET
            last_msg_id = $12,
            start_time = $11,
            fail_count = $15
        """,
        task["task_id"],
        task["owner_id"],
        task["chat_id"],
        task["content_type"],
        task.get("content_text"),
        task.get("file_id"),
        task.get("entities"),
        task.get("pin", False),
        task.get("delete_old", False),
        task.get("repeat_interval"),
        task["start_time"],
        task.get("last_msg_id"),
        task.get("auto_delete_offset", 0),
        task.get("reply_target"),
        task.get("fail_count", 0)
    )

async def get_all_tasks() -> List[Dict]:
    pool = await get_db()
    rows = await pool.fetch("SELECT * FROM userbot_tasks_v11")
    return [dict(r) for r in rows]

async def get_user_tasks(user_id: int, chat_id: str) -> List[Dict]:
    pool = await get_db()
    rows = await pool.fetch(
        "SELECT * FROM userbot_tasks_v11 WHERE owner_id = $1 AND chat_id = $2",
        user_id, chat_id
    )
    return [dict(r) for r in rows]

async def get_single_task(task_id: str) -> Optional[Dict]:
    pool = await get_db()
    row = await pool.fetchrow(
        "SELECT * FROM userbot_tasks_v11 WHERE task_id = $1",
        task_id
    )
    return dict(row) if row else None

async def delete_task(task_id: str) -> Optional[str]:
    pool = await get_db()
    row = await pool.fetchrow(
        "DELETE FROM userbot_tasks_v11 WHERE task_id = $1 RETURNING chat_id",
        task_id
    )
    return row["chat_id"] if row else None

async def update_last_msg(task_id: str, msg_id: int):
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET last_msg_id = $1 WHERE task_id = $2",
        msg_id, task_id
    )

async def update_next_run(task_id: str, next_run_iso: str):
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET start_time = $1 WHERE task_id = $2",
        next_run_iso, task_id
    )

async def increment_fail_count(task_id: str) -> int:
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET fail_count = fail_count + 1 WHERE task_id = $1",
        task_id
    )
    row = await pool.fetchrow(
        "SELECT fail_count FROM userbot_tasks_v11 WHERE task_id = $1",
        task_id
    )
    return row["fail_count"] if row else 0

async def reset_fail_count(task_id: str):
    pool = await get_db()
    await pool.execute(
        "UPDATE userbot_tasks_v11 SET fail_count = 0 WHERE task_id = $1",
        task_id
    )

# ------------------- Serialization Helpers -------------------
def serialize_entities(entities_list):
    if not entities_list:
        return None
    data = []
    for e in entities_list:
        data.append({
            "type": str(e.type).split(".")[-1],
            "offset": e.offset,
            "length": e.length,
            "url": e.url,
            "language": e.language,
            "custom_emoji_id": e.custom_emoji_id
        })
    return json.dumps(data)

def deserialize_entities(json_str):
    if not json_str:
        return None
    try:
        data = json.loads(json_str)
        entities = []
        for item in data:
            type_str = item["type"]
            e_type = getattr(enums.MessageEntityType, type_str, None)
            if e_type is None:
                continue
            entity = MessageEntity(
                type=e_type,
                offset=item["offset"],
                length=item["length"],
                url=item["url"],
                language=item["language"],
                custom_emoji_id=item["custom_emoji_id"]
            )
            entities.append(entity)
        return entities
    except Exception:
        return None

# ------------------- Cleanup -------------------
async def delete_all_user_data(user_id: int):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE owner_id = $1", user_id)
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1", user_id)
    await pool.execute("DELETE FROM userbot_sessions WHERE user_id = $1", user_id)
    await pool.execute("DELETE FROM user_settings WHERE user_id = $1", user_id)
