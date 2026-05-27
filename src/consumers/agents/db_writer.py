import json
import asyncio
import traceback

from ..app import app, sql_topic, dlq_topic
from ..connectors import get_pool
from ..config import DB_WRITER_MAX_RETRIES

TRADES_INSERT = """
    INSERT INTO trades (symbol, price, qty, trade_time, buyer_maker)
    VALUES ($1, $2, $3, to_timestamp($4 / 1000.0), $5)
"""

ANALYTICS_INSERT = """
    INSERT INTO trades_analytics (symbol, window_start, volume)
    VALUES ($1, now(), $2)
"""


@app.agent(sql_topic)
async def write_to_db(stream):
    """Consume internal SQL topic, persist to PostgreSQL with retry + DLQ."""
    async for msg in stream:
        payload = json.loads(msg)
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
                print(f"[db_writer] attempt {attempt} failed: {e}")
                if attempt == DB_WRITER_MAX_RETRIES:
                    print(f"[db_writer] sending to DLQ: {traceback.format_exc()}")
                    await dlq_topic.send(value=json.dumps(payload).encode())
                else:
                    await asyncio.sleep(2 ** attempt)
