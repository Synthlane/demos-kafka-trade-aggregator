import time
from datetime import datetime, timezone


def current_minute() -> int:
    """Return the current minute as epoch seconds (floored)."""
    now = int(time.time())
    return now - (now % 60)


def next_minute() -> int:
    """Return the start of the next minute as epoch seconds."""
    return current_minute() + 60


def ms_to_datetime(ms: int) -> datetime:
    """Convert millisecond timestamp to UTC datetime."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
