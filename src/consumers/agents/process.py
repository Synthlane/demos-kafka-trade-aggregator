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


# ─────────────────────────── Agent 1: windowed volume ────────────────────────

@app.agent(trades_topic)
async def update_volume(stream):
    async for trade in stream:
        volume_window[trade.symbol] += trade.qty


# ─────────────────────────── Agent 2: batched DB writes ──────────────────────

@app.agent(trades_topic)
async def flush_trades(stream):
    async for batch in stream.take(TRADES_FLUSH_SIZE, within=2.0):
        trades = []
        deltas_by_bucket: dict[tuple[str, datetime], float] = {}
        new_symbols: set[str] = set()

        for trade in batch:
            dt        = datetime.fromtimestamp(trade.event_ts, tz=timezone.utc)
            bucket_dt = dt.replace(second=0, microsecond=0)

            if trade.symbol not in known_symbols:
                known_symbols.add(trade.symbol)
                new_symbols.add(trade.symbol)

            trades.append((trade.symbol, trade.qty, trade.price, dt))
            key = (trade.symbol, bucket_dt)
            deltas_by_bucket[key] = deltas_by_bucket.get(key, 0.0) + trade.qty

        if new_symbols:
            r = await get_redis()
            async with r.pipeline() as pipe:
                for symbol in new_symbols:
                    pipe.sadd(KNOWN_SYMBOLS_KEY, symbol)
                await pipe.execute()

        await enqueue_trades(trades)
        await enqueue_analytics(
            [(symbol, bucket_dt, delta) for (symbol, bucket_dt), delta in deltas_by_bucket.items()]
        )
