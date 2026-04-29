import faust
import json
import asyncio
from datetime import datetime, timezone, timedelta
import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row


# ENV Variables
KAFKA_BROKER= os.environ.get("KAFKA_BROKER", None ) or 'kafka://localhost:19092'
KAFKA_PARTITIONS= os.environ.get("KAFKA_PARTITION", None) or 3
KAFKA_TOPIC=os.environ.get("KAFKA_TOPIC", None) or "trades"
DATABASE_URL = (
  os.environ.get("DATABASE_URL", None ) or  "postgresql://postgres@password@localhost:5432/postgres"
)
REDIS_URL = os.environ.get("REDIS_URL", None) or "redis://localhost:6379"

# Constants
READER_MARKER_KEY = "reader_read_marker"
READER_LOCK_KEY   = "reader_scheduler_lock"

app = faust.App(
    'trade-windowed',
    broker=KAFKA_BROKER,
    value_serializer='json',
    topic_partitions=KAFKA_PARTITIONS
)

trades_topic = app.topic(KAFKA_TOPIC, value_type=bytes)


_redis: aioredis.Redis            = None
_pool:  AsyncConnectionPool       = None
known_symbols: set[str]           = set()



# ─────────────────────────── connectors ─────────────────────────────────────

async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = await aioredis.from_url(
            REDIS_URL, decode_responses=True
        )
    return _redis


async def get_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=DATABASE_URL,
            min_size=2,
            max_size=10,            # tune based on Supabase plan limits
            kwargs={"row_factory": dict_row}
        )
    return _pool


# ─────────────────────────── helpers ────────────────────────────────────────

def next_minute(ts: str) -> str:
    dt = datetime.strptime(ts, "%Y-%m-%d:%H-%M")
    dt += timedelta(minutes=1)
    return dt.strftime("%Y-%m-%d:%H-%M")


def current_minute() -> str:
    return datetime.now(timezone.utc) \
                   .replace(second=0, microsecond=0) \
                   .strftime("%Y-%m-%d:%H-%M")


def seconds_until_next_minute() -> float:
    now = datetime.now(timezone.utc)
    next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return (next_min - now).total_seconds()


async def init_marker(key: str):
    r = await get_redis()
    if not await r.exists(key):
        now = datetime.utcnow().replace(second=0, microsecond=0)
        await r.set(key, now.strftime("%Y-%m-%d:%H-%M"))


# ─────────────────────────── stream agent ───────────────────────────────────

# Buffer trades and flush in batches to avoid per-message DB writes
_trades_buffer: list[tuple] = []
_FLUSH_SIZE = 10000                               # flush every 10000 messages


async def flush_trades():
    if not _trades_buffer:
        return
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # executemany with psycopg3 is efficient — single round trip
            await cur.executemany(
                """
                INSERT INTO trades (symbol, qty, price, ts)
                VALUES (%s, %s, %s, %s)
                """,
                _trades_buffer
            )
    _trades_buffer.clear()


@app.agent(trades_topic)
async def process(stream):
    async for msg in stream.events():
        data = msg.value
        if not isinstance(data, dict):
            data = json.loads(data)

        symbol = data['symbol']
        qty    = float(data['qty'])
        price  = float(data.get('price', 0))
        ts     = data['time'] / 1000.0

        known_symbols.add(symbol)

        # ── Redis bucket ─────────────────────────────────────────────────
        r             = await get_redis()
        dt            = datetime.fromtimestamp(ts, tz=timezone.utc)
        minute_bucket = dt.strftime("%Y-%m-%d:%H-%M")
        key           = f"volume:{symbol}:{minute_bucket}"

        await r.incrbyfloat(key, qty)
        if await r.ttl(key) == -1:
            await r.expire(key, 600)

        # ── Buffer trade for DB ──────────────────────────────────────────
        _trades_buffer.append((symbol, qty, price, dt))

        if len(_trades_buffer) >= _FLUSH_SIZE:
            await flush_trades()                # batch insert every 100 trades


# ─────────────────────────── reader ─────────────────────────────────────────

async def read_minute_window(minute_bucket: str):
    if not known_symbols:
        print(f"[READER] No symbols tracked yet for window: {minute_bucket}")
        return

    r          = await get_redis()
    dt_display = datetime.strptime(minute_bucket, "%Y-%m-%d:%H-%M") \
                         .strftime("%Y-%m-%d %H:%M UTC")

    # collect rows first, then bulk insert in one query
    rows: list[tuple] = []

    for symbol in sorted(known_symbols):
        key = f"volume:{symbol}:{minute_bucket}"
        raw = await r.get(key)
        if raw is None:
            continue

        volume = float(raw)
        bucket_dt = datetime.strptime(minute_bucket, "%Y-%m-%d:%H-%M") \
                            .replace(tzinfo=timezone.utc)

        print(f"[{dt_display}] Symbol: {symbol:<10} CHANGE: {volume:.4f} USD")
        rows.append((symbol, bucket_dt, volume))

    if not rows:
        return

    # ── Bulk insert analytics ────────────────────────────────────────────
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO trades_analytics (symbol, minute_bucket, volume)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol, minute_bucket) DO UPDATE
                    SET volume = EXCLUDED.volume   -- idempotent re-runs
                """,
                rows
            )


@app.task
async def reader_scheduler():
    await init_marker(READER_MARKER_KEY)
    await asyncio.sleep(seconds_until_next_minute() + 2)

    try:
        while True:
            r        = await get_redis()
            acquired = await r.set(READER_LOCK_KEY, "1", nx=True, ex=55)

            if acquired:
                try:
                    marker = await r.get(READER_MARKER_KEY)
                    now    = current_minute()

                    while marker < now:
                        await asyncio.sleep(2)

                        print(f"\n{'─' * 50}")
                        print(f"[READER] Reading window: {marker}")
                        print(f"{'─' * 50}")

                        await read_minute_window(marker)

                        marker = next_minute(marker)
                        await r.set(READER_MARKER_KEY, marker)
                finally:
                    await r.delete(READER_LOCK_KEY)

            await asyncio.sleep(seconds_until_next_minute())

    finally:
        # cleanup on shutdown
        await flush_trades()
        if _pool:
            await _pool.close()
