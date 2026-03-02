"""initial

Revision ID: 001
Revises:
Create Date: 2025-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS userbot_tasks_v11 (
            task_id            TEXT PRIMARY KEY,
            owner_id           BIGINT NOT NULL,
            chat_id            TEXT NOT NULL,
            content_type       TEXT NOT NULL,
            content_text       TEXT,
            file_id            TEXT,
            entities           TEXT,
            pin                BOOLEAN DEFAULT FALSE,
            delete_old         BOOLEAN DEFAULT FALSE,
            repeat_interval    TEXT,
            start_time         TEXT NOT NULL,
            last_msg_id        BIGINT,
            auto_delete_offset INTEGER DEFAULT 0,
            reply_target       TEXT,
            fail_count         INTEGER DEFAULT 0
        )
    """))
    conn.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS ix_userbot_tasks_owner
        ON userbot_tasks_v11 (owner_id)
    """))
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id  BIGINT PRIMARY KEY,
            timezone TEXT DEFAULT 'Asia/Kolkata'
        )
    """))
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS userbot_sessions (
            user_id        BIGINT PRIMARY KEY,
            session_string TEXT NOT NULL
        )
    """))
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS userbot_channels (
            user_id    BIGINT,
            channel_id TEXT,
            title      TEXT,
            PRIMARY KEY (user_id, channel_id)
        )
    """))
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS apscheduler_jobs (
            id            VARCHAR(191) PRIMARY KEY,
            next_run_time DOUBLE PRECISION,
            job_state     BYTEA NOT NULL
        )
    """))
    conn.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS ix_apscheduler_jobs_next_run_time
        ON apscheduler_jobs (next_run_time)
    """))


def downgrade():
    conn = op.get_bind()
    conn.execute(sa.text("DROP TABLE IF EXISTS apscheduler_jobs"))
    conn.execute(sa.text("DROP TABLE IF EXISTS userbot_channels"))
    conn.execute(sa.text("DROP TABLE IF EXISTS userbot_sessions"))
    conn.execute(sa.text("DROP TABLE IF EXISTS user_settings"))
    conn.execute(sa.text("DROP TABLE IF EXISTS userbot_tasks_v11"))
