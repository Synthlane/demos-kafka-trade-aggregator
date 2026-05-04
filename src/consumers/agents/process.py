import json
from datetime import datetime, timezone

from app import app, trades_topic
from connectors import get_redis
from publishers import enqueue_trades
from utils.redis_utils import get_shadow_key, set_with_keepttl_or_default_ttl
from config import VOLUME_TTL, SHADOW_TTL, TRADES_FLUSH_SIZE, KNOWN_SYMBOLS_KEY

# ─────────────────────────── Shared state ────────────────────────────────────
known_symbols: set[str] = set()


# ─────────────────────────── Startup hydration ───────────────────────────────

@app.task
async def hydrate_known_symbols():
    r       = await get_redis()
    members = await r.smembers(KNOWN_SYMBOLS_KEY)
    if members:
        known_symbols.update(members)
        print(f"[PROCESS] Hydrated {len(members)} symbols from Redis")


# ─────────────────────────── Stream agent ────────────────────────────────────

@app.agent(trades_topic)
async def process(stream):
    async for batch in stream.take(TRADES_FLUSH_SIZE, within=2.0):
        trades = []

        for data in batch:
            if not isinstance(data, dict):
                data = json.loads(data)

            symbol = data["symbol"]
            qty    = float(data["qty"])
            price  = float(data.get("price", 0))
            ts     = data["time"] / 1000.0
            dt     = datetime.fromtimestamp(ts, tz=timezone.utc)

            # ── Persist symbol to Redis set so it survives restarts ───────
            if symbol not in known_symbols:
                known_symbols.add(symbol)
                r_sym = await get_redis()
                await r_sym.sadd(KNOWN_SYMBOLS_KEY, symbol)

            # ── Accumulate volume in Redis ────────────────────────────────
            r             = await get_redis()
            minute_bucket = dt.strftime("%Y-%m-%d:%H-%M")
            key           = f"volume:{symbol}:{minute_bucket}"
            shadow_key    = get_shadow_key(key)

            await set_with_keepttl_or_default_ttl(r, key, 0)
            await r.incrbyfloat(shadow_key, qty)

            if await r.ttl(shadow_key) == -1:
                await r.expire(shadow_key, SHADOW_TTL)
            if await r.ttl(key) == -1:
                await r.expire(key, VOLUME_TTL)

            trades.append((symbol, qty, price, dt))

        # ── Flush batch to SQL topic (offsets commit only after this) ─────
        await enqueue_trades(trades)