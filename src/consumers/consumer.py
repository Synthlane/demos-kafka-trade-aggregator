import faust
import json
import asyncio
from datetime import datetime, timezone, timedelta
import redis.asyncio as aioredis

app = faust.App(
    'trade-windowed',
    broker='kafka://localhost:19092',
    value_serializer='json',
    topic_partitions=3
)

trades_topic = app.topic('trades', value_type=bytes)

READER_MARKER_KEY = "reader_read_marker"

_redis: aioredis.Redis = None
known_symbols: set[str] = set()            # in-memory, no Redis bloat


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = await aioredis.from_url("redis://localhost:6379", decode_responses=True)
    return _redis


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

@app.agent(trades_topic)
async def process(stream):
    async for msg in stream.events():
        data = msg.value
        if not isinstance(data, dict):
            data = json.loads(data)

        symbol = data['symbol']
        qty    = float(data['qty'])
        ts     = data['time'] / 1000.0

        known_symbols.add(symbol)              # register symbol in-memory

        r             = await get_redis()
        dt            = datetime.fromtimestamp(ts, tz=timezone.utc)
        minute_bucket = dt.strftime("%Y-%m-%d:%H-%M")
        key           = f"volume:{symbol}:{minute_bucket}"

        await r.incrbyfloat(key, qty)

        if await r.ttl(key) == -1:
            await r.expire(key, 600)


# ─────────────────────────── reader ─────────────────────────────────────────

async def read_minute_window(minute_bucket: str):
    if not known_symbols:
        print(f"[READER] No symbols tracked yet for window: {minute_bucket}")
        return

    r          = await get_redis()
    dt_display = datetime.strptime(minute_bucket, "%Y-%m-%d:%H-%M") \
                         .strftime("%Y-%m-%d %H:%M UTC")

    for symbol in sorted(known_symbols):
        key = f"volume:{symbol}:{minute_bucket}"  # direct GET — no scan
        raw = await r.get(key)
        if raw is None:
            continue                              # no trades for this symbol this minute
        print(f"[{dt_display}] Symbol: {symbol:<10} CHANGE: {float(raw):.4f} USD")


@app.task
async def reader_scheduler():
    await init_marker(READER_MARKER_KEY)
    await asyncio.sleep(seconds_until_next_minute() + 2)  # let writer get a head-start

    while True:
        r      = await get_redis()
        marker = await r.get(READER_MARKER_KEY)
        now    = current_minute()

        # string comparison works because format is YYYY-MM-DD:HH-MM (lexicographic order)
        # e.g. "2026-04-29:11-20" < "2026-04-29:11-40" → True
        while marker < now:
            await asyncio.sleep(10) # buffer inbetween to ensure that updates are completed. 
            print(f"\n{'─' * 50}")
            print(f"[READER] Reading window: {marker}")
            print(f"{'─' * 50}")

            await read_minute_window(marker)

            marker = next_minute(marker)
            await r.set(READER_MARKER_KEY, marker)   # advance per bucket — crash-safe

        await asyncio.sleep(seconds_until_next_minute())