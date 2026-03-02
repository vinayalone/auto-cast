import time
import asyncio
import logging
from collections import defaultdict
from typing import Dict, List
from pyrogram import Client
from src.config import API_ID, API_HASH

logger = logging.getLogger(__name__)

# ------------------- Rate Limiting -------------------
login_attempts: Dict[str, List[float]] = defaultdict(list)
MAX_ATTEMPTS = 5
LOCKOUT_TIME = 300  # seconds

def check_rate_limit(identifier: str) -> bool:
    now = time.time()
    attempts = login_attempts[identifier]
    # Remove old attempts
    attempts = [t for t in attempts if now - t < LOCKOUT_TIME]
    login_attempts[identifier] = attempts
    if len(attempts) >= MAX_ATTEMPTS:
        return False
    attempts.append(now)
    return True

def reset_rate_limit(identifier: str):
    login_attempts.pop(identifier, None)

# ------------------- Client Pool -------------------
# Optional optimization: reuse Pyrogram clients per user
_client_pool: Dict[int, Client] = {}
_client_locks = defaultdict(asyncio.Lock)

async def get_user_client(user_id: int, session_string: str) -> Client:
    async with _client_locks[user_id]:
        client = _client_pool.get(user_id)
        if client and client.is_connected:
            client.last_used = time.time()
            return client
        if client:
            await client.disconnect()
        client = Client(
            f"user_{user_id}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True
        )
        await client.start()
        client.last_used = time.time()
        _client_pool[user_id] = client
        return client

async def close_idle_clients():
    """Run periodically to disconnect clients unused for 5 minutes."""
    now = time.time()
    for uid, client in list(_client_pool.items()):
        if hasattr(client, 'last_used') and now - client.last_used > 300:
            await client.stop()
            del _client_pool[uid]
            logger.info(f"Closed idle client for user {uid}")

# ------------------- Safe Timezone Fetch -------------------
async def safe_get_user_timezone(user_id: int):
    from src.db import get_user_timezone
    try:
        return await get_user_timezone(user_id)
    except Exception as e:
        logger.exception(f"Failed to fetch timezone for user {user_id}: {e}")
        from src.config import DEFAULT_TZ
        return DEFAULT_TZ
