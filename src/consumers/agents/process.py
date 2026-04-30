import asyncio
import json
from datetime import datetime, timezone

from app import app, trades_topic
from connectors import get_redis
from publishers import enqueue_trades
from utils.redis_utils import get_shadow_key, set_with_keepttl_or_default_ttl
from config import VOLUME_TTL, SHADOW_TTL, TRADES_FLUSH_SIZE, KNOWN_SYMBOLS_KEY

# ─────────────────────────── Shared state ────────────────────────────────────
known_symbols: set[str]     = set()
_trades_buffer: list[tuple] = []
_flush_lock: asyncio.Lock | None = None


def _get_flush_lock() -> asyncio.Lock:
    """Lazily create the flush lock inside the running event loop."""
    global _flush_lock
    if _flush_lock is None:
        _flush_lock = asyncio.Lock()
    return _flush_lock


async def flush_trades() -> None:
    """Drain the in-memory buffer to the SQL topic (concurrency-safe)."""
    async with _get_flush_lock():
        if not _trades_buffer:
            return
        await enqueue_trades(list(_trades_buffer))
        _trades_buffer.clear()


# ─────────────────────────── Startup hydration ───────────────────────────────

@app.task
async def hydrate_known_symbols():
    """
    On startup, reload known_symbols from Redis so the reader
    doesn't skip windows because the in-memory set is empty after a restart.
    """
    r       = await get_redis()
    members = await r.smembers(KNOWN_SYMBOLS_KEY)
    if members:
        known_symbols.update(members)
        print(f"[PROCESS] Hydrated {len(members)} symbols from Redis")


# ─────────────────────────── Periodic flush ──────────────────────────────────

@app.task
async def periodic_flush():
    """
    Fix #2 — Flush the buffer every 2 seconds so a crash loses at most
    ~2s of trades rather than up to TRADES_FLUSH_SIZE messages.
    """
    while True:
        await asyncio.sleep(2)
        await flush_trades()


# ─────────────────────────── Stream agent ────────────────────────────────────

@app.agent(trades_topic)
async def process(stream):
    async for msg in stream.events():
        data = msg.value
        if not isinstance(data, dict):
            data = json.loads(data)

        symbol = data["symbol"]
        qty    = float(data["qty"])
        price  = float(data.get("price", 0))
        ts     = data["time"] / 1000.0

        # ── Persist symbol to Redis set so it survives restarts ───────────
        if symbol not in known_symbols:
            known_symbols.add(symbol)
            r_sym = await get_redis()
            await r_sym.sadd(KNOWN_SYMBOLS_KEY, symbol)

        # ── Accumulate volume in Redis ────────────────────────────────────
        r             = await get_redis()
        dt            = datetime.fromtimestamp(ts, tz=timezone.utc)
        minute_bucket = dt.strftime("%Y-%m-%d:%H-%M")
        key           = f"volume:{symbol}:{minute_bucket}"
        shadow_key    = get_shadow_key(key)

        await set_with_keepttl_or_default_ttl(r, key, 0)
        await r.incrbyfloat(shadow_key, qty)

        if await r.ttl(shadow_key) == -1:
            await r.expire(shadow_key, SHADOW_TTL)
        if await r.ttl(key) == -1:
            await r.expire(key, VOLUME_TTL)

        # ── Buffer trade row; flush when full ─────────────────────────────
        _trades_buffer.append((symbol, qty, price, dt))

        if len(_trades_buffer) >= TRADES_FLUSH_SIZE:
            await flush_trades()