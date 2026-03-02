import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from src.config import API_ID, API_HASH, BOT_TOKEN, validate_env
from src.db import init_db, get_all_tasks
from src.scheduler import setup_scheduler, add_scheduler_job, scheduler
from src.handlers.callback import callback_router
from src.handlers.inputs import handle_input
from src.handlers.menu import show_main_menu
from src.auth import login_state
import src.globals as globals

logger = logging.getLogger(__name__)

app = Client(
    "autocast_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)
globals.app = app


def _run_migrations_sync():
    """Run Alembic migrations in a thread (sync, uses psycopg2)."""
    from alembic.config import Config
    from alembic import command

    url = os.environ.get("DATABASE_URL", "")
    for prefix in ("postgresql+asyncpg://", "postgres+asyncpg://"):
        if url.startswith(prefix):
            url = "postgresql://" + url[len(prefix):]
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]

    # Resolve alembic.ini relative to project root (one level up from src/)
    ini_path = os.path.join(os.path.dirname(__file__), "..", "alembic.ini")
    ini_path = os.path.abspath(ini_path)

    cfg = Config(ini_path)
    cfg.set_main_option("sqlalchemy.url", url)
    # Also tell Alembic where the project root is so migrations/ is found
    cfg.set_main_option("prepend_sys_path", os.path.dirname(ini_path))
    command.upgrade(cfg, "head")
    logger.info("✅ Alembic migrations complete")


@app.on_message(filters.command(["start", "manage"]))
async def start_command(client, message):
    uid = message.from_user.id
    if uid not in globals.user_state:
        globals.user_state[uid] = {}
    from src.db import get_session
    if await get_session(uid):
        await show_main_menu(client, message, uid, force_new=True)
    else:
        await message.reply_text(
            "👋 **Welcome to AutoCast | Channel Manager!**\n\n"
            "Your ultimate tool for scheduling and managing content across your Telegram channels.\n\n"
            "👇 **Ready? Click 'Login' below!**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 Login Account", callback_data="login_start")]
            ])
        )


@app.on_message(filters.private & ~filters.command(["start", "manage"]))
async def private_message_handler(client, message):
    await handle_input(client, message)


@app.on_callback_query()
async def callback_query_handler(client, callback_query):
    await callback_router(client, callback_query)


async def main():
    validate_env()

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1) as pool:
        await loop.run_in_executor(pool, _run_migrations_sync)

    await init_db()

    global scheduler
    scheduler = setup_scheduler()
    scheduler.start()

    tasks = await get_all_tasks()
    logger.info(f"📂 Loaded {len(tasks)} tasks from DB")
    for t in tasks:
        add_scheduler_job(t)

    await app.start()
    logger.info("✅ Bot started!")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
