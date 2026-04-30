import json
import asyncio
from datetime import datetime, timezone

from app import app, sql_topic, dlq_topic
from connectors import get_pool
from config import OP_TRADE, OP_ANALYTICS, DB_WRITER_MAX_RETRIES, DB_WRITER_RETRY_DELAY

# Errors that will never self-heal — bypass retries and go straight to DLQ
_PERMANENT_EXC = (json.JSONDecodeError, KeyError, ValueError, TypeError)


# ─────────────────────────── DB execution ────────────────────────────────────

async def _execute_db_op(op: str, rows: list) -> None:
    """Run the appropriate INSERT inside a single atomic Postgres transaction."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:

                if op == OP_TRADE:
                    parsed = [
                        (r[0], float(r[1]), float(r[2]), datetime.fromisoformat(r[3]))
                        for r in rows
                    ]
                    await cur.executemany(
                        """
                        INSERT INTO trades (symbol, qty, price, ts)
                        VALUES (%s, %s, %s, %s)
                        """,
                        parsed,
                    )

                elif op == OP_ANALYTICS:
                    parsed = [
                        (r[0], datetime.fromisoformat(r[1]), float(r[2]))
                        for r in rows
                    ]
                    await cur.executemany(
                        """
                        INSERT INTO trades_analytics (symbol, minute_bucket, volume)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol, minute_bucket) DO UPDATE
                            SET volume = EXCLUDED.volume
                        """,
                        parsed,
                    )

                else:
                    raise ValueError(f"Unknown op '{op}'")


# ─────────────────────────── DLQ ─────────────────────────────────────────────

async def _send_to_dlq(raw: bytes, reason: str, attempt: int) -> None:
    """Forward a failed message to the DLQ with error metadata attached."""
    envelope = json.dumps({
        "original":  raw.decode() if isinstance(raw, bytes) else raw,
        "reason":    reason,
        "attempts":  attempt,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }).encode()
    await dlq_topic.send(value=envelope)
    print(f"[DB WRITER] → DLQ after {attempt} attempt(s): {reason}")


# ─────────────────────────── Agent ───────────────────────────────────────────

@app.agent(sql_topic)
async def db_writer(stream):
    """
    Consumes SQL-write events from `trade_sql_queries`.

    Delivery guarantees:
      - Kafka offset is committed ONLY after the Postgres transaction commits.
      - Transient errors are retried up to DB_WRITER_MAX_RETRIES times
        with exponential back-off.
      - Permanent errors (bad payload, unknown op, parse failure) skip
        retries and go straight to the DLQ immediately.
      - After retries are exhausted the message is forwarded to the DLQ
        and the offset is acked so the partition is never stalled.
    """
    async for event in stream.events():
        raw     = event.value
        attempt = 0

        # ── Decode once; a bad payload is always a permanent failure ──────
        try:
            data = json.loads(raw) if isinstance(raw, (bytes, str)) else raw
            op   = data["op"]
            rows = data["rows"]
        except _PERMANENT_EXC as exc:
            await _send_to_dlq(raw, f"decode error: {exc!r}", attempt=0)
            event.ack()
            continue

        # ── Retry loop ────────────────────────────────────────────────────
        last_exc: Exception | None = None

        while attempt < DB_WRITER_MAX_RETRIES:
            attempt += 1
            try:
                await _execute_db_op(op, rows)

                event.ack()  # Postgres committed → safe to advance offset
                print(f"[DB WRITER] op={op} rows={len(rows)} committed & acked (attempt {attempt})")
                last_exc = None
                break

            except _PERMANENT_EXC as exc:
                last_exc = exc
                attempt  = DB_WRITER_MAX_RETRIES  # skip remaining retries
                break

            except Exception as exc:
                last_exc = exc
                delay    = DB_WRITER_RETRY_DELAY * (2 ** (attempt - 1))
                print(
                    f"[DB WRITER] op={op} attempt {attempt}/{DB_WRITER_MAX_RETRIES} "
                    f"failed: {exc!r} — retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)

        # ── Exhausted retries → DLQ ───────────────────────────────────────
        if last_exc is not None:
            await _send_to_dlq(
                raw,
                reason=f"{type(last_exc).__name__}: {last_exc}",
                attempt=attempt,
            )
            event.ack()  # ack so the partition keeps moving