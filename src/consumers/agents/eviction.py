from datetime import datetime, timezone

from app import app
from connectors import get_redis_connection
from publishers import enqueue_analytics
from utils.redis_utils import get_shadow_key
from config import KEYEVENT_CHANNEL


async def handle_evicted_key(key: str) -> None:
    parts = key.split(":")
    if len(parts) != 4 or parts[0] != "volume":
        return

    _, symbol, date_part, time_part = parts

    bucket_dt = datetime.strptime(
        f"{date_part}:{time_part}", "%Y-%m-%d:%H-%M"
    ).replace(tzinfo=timezone.utc)

    r   = await get_redis_connection()
    raw = await r.get(get_shadow_key(key))
    await r.aclose()

    if raw is None:
        return

    await enqueue_analytics([(symbol, bucket_dt, float(raw))])


@app.task
async def redis_eviction_listener():
    """Subscribe to Redis keyspace expiry events and enqueue analytics on eviction."""
    eviction_redis = await get_redis_connection()
    pubsub         = eviction_redis.pubsub()
    await pubsub.subscribe(KEYEVENT_CHANNEL)

    print("-" * 50)
    print(f"[EVICTION LISTENER] Subscribed to {KEYEVENT_CHANNEL}")
    print("-" * 50)

    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue

            key = message["data"]
            if not key.startswith("volume"):
                continue

            await handle_evicted_key(key)

    finally:
        await pubsub.unsubscribe(KEYEVENT_CHANNEL)
        await eviction_redis.aclose()