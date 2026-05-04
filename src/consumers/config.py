import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────── Kafka ───────────────────────────────────────────
KAFKA_BROKER     = os.environ.get("KAFKA_BROKER")    or "kafka://localhost:19092"
KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITION", 3))
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC")     or "trades"
SQL_TOPIC        = os.environ.get("SQL_TOPIC")       or "trade_sql_queries"
DLQ_TOPIC        = os.environ.get("DLQ_TOPIC")       or "trade_sql_queries_dlq"

# ─────────────────────────── Storage ─────────────────────────────────────────
DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or "postgresql://postgres@password@localhost:5432/postgres"
)
REDIS_URL = os.environ.get("REDIS_URL") or "redis://localhost:6379"

# ─────────────────────────── Redis keys / channels ───────────────────────────
KEYEVENT_CHANNEL  = "__keyevent@0__:expired"
READER_MARKER_KEY = "reader_read_marker"
READER_LOCK_KEY   = "reader_scheduler_lock"

# ─────────────────────────── TTLs ────────────────────────────────────────────
VOLUME_TTL = 300   # 5 min  — triggers eviction event
SHADOW_TTL = 600   # 10 min — still readable when eviction fires

# ─────────────────────────── SQL operation tags ───────────────────────────────
OP_TRADE     = "trade"
OP_ANALYTICS = "analytics"

# ─────────────────────────── DB writer ───────────────────────────────────────
DB_WRITER_MAX_RETRIES = int(os.environ.get("DB_WRITER_MAX_RETRIES", 3))
DB_WRITER_RETRY_DELAY = float(os.environ.get("DB_WRITER_RETRY_DELAY", 2.0))  # seconds; doubles each attempt

# ─────────────────────────── Trades buffer ───────────────────────────────────
TRADES_FLUSH_SIZE = int(os.environ.get("TRADES_FLUSH_SIZE", 10_000))

KNOWN_SYMBOLS_KEY="known_symbols"