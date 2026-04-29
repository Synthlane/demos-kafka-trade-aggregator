import faust
import json
import asyncio
from datetime import datetime, timezone, timedelta
import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
import os
from dotenv import load_dotenv
load_dotenv()
# ENV Variables
KAFKA_BROKER= os.environ.get("KAFKA_BROKER", None ) or 'kafka://localhost:19092'
KAFKA_PARTITIONS= os.environ.get("KAFKA_PARTITION", None) or 3
KAFKA_TOPIC=os.environ.get("KAFKA_TOPIC", None) or "trades"
DATABASE_URL = (
  os.environ.get("DATABASE_URL", None ) or  "postgresql://postgres@password@localhost:5432/postgres"
)
REDIS_URL = os.environ.get("REDIS_URL", None) or "redis://localhost:6379"
KEYEVENT_CHANNEL = "__keyevent@0__:expired" 

# Constants
READER_MARKER_KEY = "reader_read_marker"
READER_LOCK_KEY   = "reader_scheduler_lock"
VOLUME_TTL = 300   # 5 min — triggers eviction event
SHADOW_TTL = 600   # 10 min — still readable when eviction fires

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

async def get_redis_connection():
    return await aioredis.from_url(
            REDIS_URL, decode_responses=True
        )
async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = await get_redis_connection()
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
        shadow_key = get_shadow_key(key)


        await set_with_keepttl_or_default_ttl(r, key, 0)
        await r.incrbyfloat(shadow_key, qty)


        if await r.ttl(shadow_key) == -1:
            await r.expire(shadow_key, SHADOW_TTL)   
        if await r.ttl(key) == -1:
            await r.expire(key, VOLUME_TTL)    

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
        shadow_key = get_shadow_key(key)
        raw = await r.get(shadow_key)
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

# --------------------------- eviction handler --------------------------------
async def handle_evicted_key(key: str):
    # key = "volume:BTC:2026-04-29:11-20"
    parts = key.split(":")
    if len(parts) != 4 or parts[0] != "volume":
        return

    _, symbol, date_part, time_part = parts      # unpack directly from key

    bucket_dt = datetime.strptime(
        f"{date_part}:{time_part}", "%Y-%m-%d:%H-%M"
    ).replace(tzinfo=timezone.utc)               # parsed from key — no guessing
    r = await get_redis()
    
    volume = await r.get(get_shadow_key(key))
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
          await cur.execute(
            """
            INSERT INTO trades_analytics (symbol, minute_bucket, volume)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol, minute_bucket)
            DO UPDATE SET volume = EXCLUDED.volume
            """,
            (symbol, bucket_dt, volume)
        )




@app.task
async def redis_eviction_listener():

    """
    Subscribes to Redis keyspace expiry events.
    Runs as a separate task alongside reader_scheduler.
    """
    
    eviction_redis = await get_redis_connection()
    pubsub = eviction_redis.pubsub()
    await pubsub.subscribe(KEYEVENT_CHANNEL)

    print("-"*50)
    print(f"[EVICTION LISTENER] Subscribed to {KEYEVENT_CHANNEL}")
    print("-"*50)
    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue                          # skip subscribe confirmation messages

            key = message["data"]                 # the expired key name
            if not key.startswith("volume"):
                continue                          # only care about our keys

            await handle_evicted_key(key)

    finally:
        await pubsub.unsubscribe(KEYEVENT_CHANNEL)
        await eviction_redis.aclose()



# ─────────────────────────── utils ─────────────────────────────────────
def get_shadow_key(key:str): 
    return f"shadow_key_{key}"

async def set_with_keepttl_or_default_ttl(r, key, value, ttl=300) -> None:
    script = """
local t = redis.call("TTL", KEYS[1])
if t > 0 then
    return redis.call("SET", KEYS[1], ARGV[1], "KEEPTTL")
else
    return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
end
"""
    await r.eval(script, 1, key, str(value), int(ttl))