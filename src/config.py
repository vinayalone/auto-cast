import os
import sys
import logging
import pytz

logger = logging.getLogger("config")


def _normalise_db_url(url: str) -> str:
    """Ensure DATABASE_URL uses the postgresql:// scheme (not postgres://)."""
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def validate_env():
    required = ["API_ID", "API_HASH", "BOT_TOKEN", "DATABASE_URL", "FERNET_KEY"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        sys.exit(1)

    try:
        int(os.environ["API_ID"])
    except ValueError:
        logger.error("API_ID must be an integer")
        sys.exit(1)

    if len(os.environ["BOT_TOKEN"]) < 10:
        logger.error("BOT_TOKEN seems invalid")
        sys.exit(1)

    db_url = _normalise_db_url(os.environ["DATABASE_URL"])
    if not db_url.startswith("postgresql://"):
        logger.error("DATABASE_URL must start with postgresql:// or postgres://")
        sys.exit(1)


# Constants
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
# Normalise at import time so all consumers get a consistent URL
DATABASE_URL = _normalise_db_url(os.environ["DATABASE_URL"])
FERNET_KEY = os.environ["FERNET_KEY"].encode()
DEFAULT_TZ = pytz.timezone("Asia/Kolkata")

# Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
