import asyncio

import asyncpg
import redis.asyncio as aioredis

from .config import DATABASE_URL, REDIS_URL

_redis = None
_pool = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            REDIS_URL, decode_responses=True, socket_connect_timeout=3
        )
    await asyncio.wait_for(_redis.ping(), timeout=3)
    return _redis


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncio.wait_for(
            asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5),
            timeout=5,
        )
    return _pool
