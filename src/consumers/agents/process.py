import json

from ..app import app, trades_topic, volume_window
from ..connectors import get_redis
from ..publishers import enqueue_trades
from ..config import TRADES_FLUSH_SIZE


@app.agent(trades_topic)
async def update_volume(stream):
    """Update the windowed volume table and track known symbols in Redis."""
    async for event in stream.events():
        data = json.loads(event.value)
        symbol = data["symbol"]
        qty = float(data["qty"])

        volume_window[symbol] += qty
        current = volume_window[symbol].now()
        print(f"{symbol} | qty: {qty:.4f} | 1-min volume: {current:.4f}")

        r = await get_redis()
        await r.sadd("known_symbols", symbol)


@app.agent(trades_topic)
async def flush_trades(stream):
    """Batch incoming trades and enqueue them for DB persistence."""
    batch: list[dict] = []
    async for msg in stream:
        data = json.loads(msg)
        batch.append(data)
        if len(batch) >= TRADES_FLUSH_SIZE:
            await enqueue_trades(batch)
            batch = []
