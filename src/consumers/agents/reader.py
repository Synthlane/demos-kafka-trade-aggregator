from ..app import app, volume_window
from ..connectors import get_redis
from ..publishers import enqueue_analytics


@app.timer(interval=60.0)
async def emit_window_analytics():
    """Periodically read closed window volumes and publish analytics to DB."""
    try:
        r = await get_redis()
        symbols = await r.smembers("known_symbols")
    except Exception:
        return

    if not symbols:
        return

    rows = []
    for symbol in symbols:
        try:
            vol = volume_window[symbol].now()
            if vol and vol > 0:
                rows.append({"symbol": symbol, "volume": vol})
                print(f"[reader] {symbol} window volume: {vol:.4f}")
        except Exception as e:
            print(f"[reader] error reading {symbol}: {e}")

    if rows:
        try:
            await enqueue_analytics(rows)
            print(f"[reader] enqueued {len(rows)} analytics rows")
        except Exception:
            pass
