import json
from .app import sql_topic


async def enqueue_trades(batch: list[dict]):
    payload = json.dumps({"type": "trades", "rows": batch}).encode()
    await sql_topic.send(value=payload)


async def enqueue_analytics(rows: list[dict]):
    payload = json.dumps({"type": "analytics", "rows": rows}).encode()
    await sql_topic.send(value=payload)
