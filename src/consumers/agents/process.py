import json
from datetime import datetime, timezone

from app import app, trades_topic, volume_window
from connectors import get_redis
from publishers import enqueue_trades, enqueue_analytics
from config import TRADES_FLUSH_SIZE, KNOWN_SYMBOLS_KEY

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
        deltas_by_bucket: dict[tuple[str, datetime], float] = {}

        for data in batch:
            if not isinstance(data, dict):
                data = json.loads(data)

            symbol = data["symbol"]
            qty    = float(data["qty"])
            price  = float(data.get("price", 0))
            ts     = data["time"] / 1000.0
            dt     = datetime.fromtimestamp(ts, tz=timezone.utc)
            bucket_dt = dt.replace(second=0, microsecond=0)

            # ── Persist symbol to Redis set so it survives restarts ───────
            if symbol not in known_symbols:
                known_symbols.add(symbol)
                r_sym = await get_redis()
                await r_sym.sadd(KNOWN_SYMBOLS_KEY, symbol)

            # ── Accumulate volume in a Faust tumbling window (1 minute) ───
            window = volume_window[symbol].relative_to(ts)
            window.value = float(window.value or 0.0) + qty

            trades.append((symbol, qty, price, dt))
            deltas_by_bucket[(symbol, bucket_dt)] = deltas_by_bucket.get((symbol, bucket_dt), 0.0) + qty

        # ── Flush batch to SQL topic (offsets commit only after this) ─────
        await enqueue_trades(trades)
        await enqueue_analytics(
            [(symbol, bucket_dt, delta) for (symbol, bucket_dt), delta in deltas_by_bucket.items()]
        )