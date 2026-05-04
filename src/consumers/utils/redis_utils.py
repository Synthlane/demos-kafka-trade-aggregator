from datetime import datetime
from connectors import get_redis
from utils.time import MINUTE_FMT

_KEEPTTL_SCRIPT = """
local t = redis.call("TTL", KEYS[1])
if t > 0 then
    return redis.call("SET", KEYS[1], ARGV[1], "KEEPTTL")
else
    return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
end
"""


def get_shadow_key(key: str) -> str:
    return f"shadow_key_{key}"


async def set_with_keepttl_or_default_ttl(r, key: str, value, ttl: int = 300) -> None:
    """SET key preserving its TTL if one exists, or applying `ttl` if not."""
    await r.eval(_KEEPTTL_SCRIPT, 1, key, str(value), int(ttl))


async def init_marker(key: str) -> None:
    """Seed the reader marker to the current UTC minute if it doesn't exist yet."""
    r = await get_redis()
    if not await r.exists(key):
        now = datetime.utcnow().replace(second=0, microsecond=0)
        await r.set(key, now.strftime(MINUTE_FMT))