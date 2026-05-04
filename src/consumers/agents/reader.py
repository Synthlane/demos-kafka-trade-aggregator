import asyncio
from datetime import datetime, timezone

from app import app
from connectors import get_redis, close_pool
from publishers import enqueue_analytics
from agents.process import known_symbols
from utils.redis_utils import get_shadow_key, init_marker
from utils.time import next_minute, current_minute, seconds_until_next_minute, parse_minute_bucket
from config import READER_MARKER_KEY, READER_LOCK_KEY


async def read_minute_window(minute_bucket: str) -> None:
    if not known_symbols:
        print(f"[READER] No symbols tracked yet for window: {minute_bucket}")
        return

    r          = await get_redis()
    dt_display = parse_minute_bucket(minute_bucket).strftime("%Y-%m-%d %H:%M UTC")
    rows: list[tuple] = []

    for symbol in sorted(known_symbols):
        key        = f"volume:{symbol}:{minute_bucket}"
        shadow_key = get_shadow_key(key)
        raw        = await r.get(shadow_key)
        if raw is None:
            continue

        volume    = float(raw)
        bucket_dt = parse_minute_bucket(minute_bucket)

        print(f"[{dt_display}] Symbol: {symbol:<10} CHANGE: {volume:.4f} USD")
        rows.append((symbol, bucket_dt, volume))

    await enqueue_analytics(rows)


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
               await close_pool()