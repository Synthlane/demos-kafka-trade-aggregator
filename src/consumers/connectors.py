import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from config import REDIS_URL, DATABASE_URL

_redis: aioredis.Redis      = None
_pool:  AsyncConnectionPool = None


async def get_redis_connection() -> aioredis.Redis:
    """Open a fresh Redis connection (caller is responsible for lifecycle)."""
    return await aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        retry_on_timeout=True,
        socket_keepalive=True,
        health_check_interval=30,   # background ping every 30s detects silent drops
    )


async def get_redis() -> aioredis.Redis:
    """Return the shared Redis singleton, creating it on first call."""
    global _redis
    if _redis is None:
        _redis = await get_redis_connection()
    return _redis


async def get_pool() -> AsyncConnectionPool:
    """Return the shared Postgres connection pool, creating it on first call."""
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=DATABASE_URL,
            min_size=2,
            max_size=10,
            open=False,                      # don't open in constructor
            kwargs={"row_factory": dict_row},
        )
        await _pool.open()                   # explicit async open
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None