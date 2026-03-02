import asyncio
import logging
import datetime
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from pyrogram import Client, errors
from pyrogram.types import Message
from src.config import DATABASE_URL, DEFAULT_TZ, API_ID, API_HASH
from src.db import (
    get_session, get_single_task, delete_task, update_last_msg,
    update_next_run, increment_fail_count, reset_fail_count,
    deserialize_entities
)
from src.utils import safe_get_user_timezone
import src.globals as globals
import pytz

logger = logging.getLogger(__name__)

scheduler = None


async def delete_sent_message(owner_id: int, chat_id: str, message_id: int):
    """Auto-delete a message after a delay."""
    try:
        session = await get_session(owner_id)
        if not session:
            return
        async with Client(
            ":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session,
            device_model="AutoCast Client",
            system_version="PC",
            app_version="AutoCast Version"
        ) as user:
            await user.delete_messages(int(chat_id), message_id)
            logger.info(f"🗑️ Auto-delete success: Msg {message_id} in {chat_id}")
    except errors.MessageDeleteForbidden:
        logger.warning(f"⚠️ Auto-delete failed: Userbot lacks delete permissions in {chat_id}.")
    except errors.ChatAdminRequired:
        logger.warning(f"⚠️ Auto-delete failed: Userbot is not an admin in {chat_id}.")
    except errors.MessageNotModified:
        logger.info(f"ℹ️ Auto-delete: Message {message_id} already deleted or not found.")
    except Exception as e:
        logger.error(f"❌ Auto-delete failed: {e}")


async def run_task(task_id: str):
    """Top-level job entry point — picklable by APScheduler.

    Receives only the task_id string so APScheduler can serialize it.
    All task data is re-fetched fresh from the DB on every execution.
    """
    logger.info(f"🚀 JOB {task_id} TRIGGERED")
    try:
        t = await get_single_task(task_id)
        if not t:
            logger.warning(f"Task {task_id} not found in DB, removing job.")
            scheduler.remove_job(task_id)
            return

        session = await get_session(t["owner_id"])
        if not session:
            logger.warning(f"⚠️ Job {task_id}: No session for user {t['owner_id']}. Skipping.")
            fc = await increment_fail_count(task_id)
            if fc >= 3:
                await notify_user_and_disable(t)
            return

        user_tz = await safe_get_user_timezone(t["owner_id"])

        async with Client(
            ":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session,
            device_model="AutoCast Client",
            system_version="PC",
            app_version="AutoCast Version"
        ) as user:
            target = int(t["chat_id"])
            caption = t["content_text"]
            entities_objs = deserialize_entities(t["entities"])
            reply_id = None

            if t.get("reply_target"):
                target_task = await get_single_task(t["reply_target"])
                if target_task and target_task.get("last_msg_id"):
                    reply_id = target_task["last_msg_id"]
                    logger.info(f"🔗 Job {task_id}: Replying to msg {reply_id} from task {t['reply_target']}")

            sent = None
            try:
                if t["content_type"] == "text":
                    sent = await user.send_message(target, caption, entities=entities_objs, reply_to_message_id=reply_id)
                elif t["content_type"] == "poll":
                    poll_data = json.loads(caption)
                    sent = await user.send_poll(target, poll_data["question"], poll_data["options"], reply_to_message_id=reply_id)
                else:
                    file_id = t["file_id"]
                    try:
                        if t["content_type"] == "photo":
                            sent = await user.send_photo(target, file_id, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "video":
                            sent = await user.send_video(target, file_id, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "animation":
                            sent = await user.send_animation(target, file_id, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "document":
                            sent = await user.send_document(target, file_id, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "sticker":
                            sent = await user.send_sticker(target, file_id, reply_to_message_id=reply_id)
                        elif t["content_type"] == "audio":
                            sent = await user.send_audio(target, file_id, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "voice":
                            sent = await user.send_voice(target, file_id, caption=caption, reply_to_message_id=reply_id)
                    except errors.FileIdInvalid:
                        logger.warning(f"⚠️ Job {task_id}: File ID invalid, attempting re-upload")
                        media_file = await globals.app.download_media(file_id, in_memory=True)
                        if t["content_type"] == "sticker":
                            media_file.name = "sticker.webp"
                            sent = await user.send_sticker(target, media_file, reply_to_message_id=reply_id)
                        elif t["content_type"] == "photo":
                            sent = await user.send_photo(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "video":
                            sent = await user.send_video(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "animation":
                            sent = await user.send_animation(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "document":
                            sent = await user.send_document(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "audio":
                            media_file.name = "audio.mp3"
                            sent = await user.send_audio(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        elif t["content_type"] == "voice":
                            media_file.name = "voice.ogg"
                            sent = await user.send_voice(target, media_file, caption=caption, reply_to_message_id=reply_id)

                if sent:
                    logger.info(f"✅ Job {task_id}: Message sent! ID: {sent.id}")
                    await reset_fail_count(task_id)
                    await update_last_msg(task_id, sent.id)

                    if t.get("pin"):
                        try:
                            pinned = await sent.pin()
                            if isinstance(pinned, Message):
                                await pinned.delete()
                        except Exception as pin_e:
                            logger.warning(f"⚠️ Job {task_id}: Failed to pin: {pin_e}")

                    offset_mins = t.get("auto_delete_offset", 0)
                    if offset_mins > 0:
                        run_at = datetime.datetime.now(user_tz) + datetime.timedelta(minutes=offset_mins)
                        scheduler.add_job(
                            delete_sent_message,
                            'date',
                            run_date=run_at,
                            args=[t["owner_id"], t["chat_id"], sent.id],
                            id=f"del_{task_id}_{sent.id}",
                            misfire_grace_time=60
                        )
                        logger.info(f"⏳ Scheduled delete for {task_id} at {run_at}")

                    if not t["repeat_interval"]:
                        await delete_task(task_id)
                        scheduler.remove_job(task_id)
                        logger.info(f"🗑️ One-time task {task_id} deleted.")

            except errors.FloodWait as e:
                logger.warning(f"⚠️ Job {task_id} FloodWait: sleeping {e.value}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(f"🔥 Job {task_id} error: {e}")
                fc = await increment_fail_count(task_id)
                if fc >= 3:
                    await notify_user_and_disable(t)

    except Exception as e:
        logger.exception(f"Unhandled exception in job {task_id}: {e}")


async def notify_user_and_disable(task):
    """Send alert and disable task after 3 consecutive failures."""
    try:
        await globals.app.send_message(
            task["owner_id"],
            f"❌ Your task `{task['task_id']}` has failed repeatedly and has been disabled."
        )
        scheduler.remove_job(task["task_id"])
        await delete_task(task["task_id"])
    except Exception as e:
        logger.error(f"Failed to notify user: {e}")


def setup_scheduler():
    global scheduler
    jobstores = {
        # APScheduler's SQLAlchemyJobStore needs a sync psycopg2 URL
        'default': SQLAlchemyJobStore(url=DATABASE_URL)
    }
    executors = {
        'default': AsyncIOExecutor()
    }
    scheduler = AsyncIOScheduler(
        jobstores=jobstores,
        executors=executors,
        timezone=DEFAULT_TZ,
        job_defaults={
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 3600
        }
    )
    return scheduler


def add_scheduler_job(task_data: dict):
    """Add a job to the scheduler from task data.

    Passes only task_id as an arg so APScheduler can pickle it.
    The job function re-fetches full task data from DB at run time.
    """
    if scheduler is None:
        return

    tid = task_data["task_id"]
    dt = datetime.datetime.fromisoformat(task_data["start_time"])
    if dt.tzinfo is None:
        dt = DEFAULT_TZ.localize(dt)

    if task_data["repeat_interval"]:
        mins = int(task_data["repeat_interval"].split("=")[1])
        trigger = IntervalTrigger(
            start_date=dt,
            minutes=mins,
            timezone=DEFAULT_TZ
        )
    else:
        trigger = DateTrigger(run_date=dt, timezone=DEFAULT_TZ)

    scheduler.add_job(
        run_task,          # top-level function — fully picklable
        trigger=trigger,
        id=tid,
        args=[tid],        # pass task_id as a plain string arg
        replace_existing=True
    )
