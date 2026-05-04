import json
from datetime import datetime
from app import sql_topic
from config import OP_TRADE, OP_ANALYTICS


async def enqueue_trades(rows: list[tuple]) -> None:
    """
    Publish a batch of trade rows to the SQL topic.

    Row tuple: (symbol: str, qty: float, price: float, dt: datetime)

    Kafka payload:
        { "op": "trade", "rows": [[symbol, qty, price, iso_ts], ...] }
    """
    if not rows:
        return

    payload = json.dumps({
        "op":   OP_TRADE,
        "rows": [
            [symbol, qty, price, dt.isoformat()]
            for symbol, qty, price, dt in rows
        ],
    }).encode()

    await sql_topic.send(value=payload)


async def enqueue_analytics(rows: list[tuple]) -> None:
    """
    Publish a batch of analytics rows to the SQL topic.

    Row tuple: (symbol: str, bucket_dt: datetime, volume: float)

    Kafka payload:
        { "op": "analytics", "rows": [[symbol, iso_bucket, volume], ...] }
    """
    if not rows:
        return

    payload = json.dumps({
        "op":   OP_ANALYTICS,
        "rows": [
            [symbol, bucket_dt.isoformat(), volume]
            for symbol, bucket_dt, volume in rows
        ],
    }).encode()

    await sql_topic.send(value=payload)