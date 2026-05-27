import json

from ..app import app, trades_topic, volume_window
from ..connectors import get_redis
from ..publishers import enqueue_trades
from ..config import TRADES_FLUSH_SIZE


def _parse(raw):
    """Handle both pre-deserialized dicts and raw JSON bytes/str."""
    if isinstance(raw, dict):
        return raw
    return json.loads(raw)


@app.agent(trades_topic)
async def update_volume(stream):
    """Update the windowed volume table and track known symbols in Redis."""
    async for event in stream.events():
        data = _parse(event.value)
        symbol = data["symbol"]
        qty = float(data["qty"])

        volume_window[symbol] += qty
        current = volume_window[symbol].now()
        print(f"{symbol} | qty: {qty:.4f} | 1-min volume: {current:.4f}")

        try:
            r = await get_redis()
            await r.sadd("known_symbols", symbol)
        except Exception:
            pass


@app.agent(trades_topic)
async def flush_trades(stream):
    """Batch incoming trades and enqueue them for DB persistence."""
    batch: list[dict] = []
    async for msg in stream:
        data = _parse(msg)
        batch.append(data)
        if len(batch) >= TRADES_FLUSH_SIZE:
            try:
                await enqueue_trades(batch)
            except Exception:
                pass
            batch = []
