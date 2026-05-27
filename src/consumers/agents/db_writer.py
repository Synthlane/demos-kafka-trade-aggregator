import json
import asyncio
import traceback

from ..app import app, sql_topic, dlq_topic
from ..connectors import get_pool
from ..config import DB_WRITER_MAX_RETRIES

TRADES_INSERT = """
    INSERT INTO trades (symbol, price, qty, trade_time, buyer_maker)
    VALUES ($1, $2, $3, $4, $5)
"""

ANALYTICS_INSERT = """
    INSERT INTO trades_analytics (symbol, window_start, volume)
    VALUES ($1, now(), $2)
"""


@app.agent(sql_topic)
async def write_to_db(stream):
    """Consume internal SQL topic, persist to PostgreSQL with retry + DLQ."""
    async for msg in stream:
        payload = msg if isinstance(msg, dict) else json.loads(msg)
        msg_type = payload.get("type")
        rows = payload.get("rows", [])

        for attempt in range(1, DB_WRITER_MAX_RETRIES + 1):
            try:
                pool = await get_pool()
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        if msg_type == "trades":
                            for row in rows:
                                await conn.execute(
                                    TRADES_INSERT,
                                    row["symbol"],
                                    float(row["price"]),
                                    float(row["qty"]),
                                    int(row["time"]),
                                    bool(row["buyer_maker"]),
                                )
                        elif msg_type == "analytics":
                            for row in rows:
                                await conn.execute(
                                    ANALYTICS_INSERT,
                                    row["symbol"],
                                    float(row["volume"]),
                                )
                break  # success
            except Exception as e:
                if attempt == DB_WRITER_MAX_RETRIES:
                    print(f"[db_writer] all retries failed, skipping: {e}")
                    try:
                        await dlq_topic.send(value=json.dumps(payload).encode())
                    except Exception:
                        pass
                else:
                    await asyncio.sleep(min(2 ** attempt, 5))
