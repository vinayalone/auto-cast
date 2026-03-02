FROM python:3.12-slim

# gcc + linux-libc-dev + python3-dev: required to compile TgCrypto (C extension needs stdint.h)
# libpq-dev: PostgreSQL client headers for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    linux-libc-dev \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Secrets (BOT_TOKEN, API_ID, API_HASH, DATABASE_URL, FERNET_KEY) are injected
# at runtime via Railway environment variables — never hardcoded here.

CMD ["python", "-m", "src.main"]
