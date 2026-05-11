from datetime import datetime, timezone, timedelta

MINUTE_FMT = "%Y-%m-%d:%H-%M"


def next_minute(ts: str) -> str:
    dt = datetime.strptime(ts, MINUTE_FMT) + timedelta(minutes=1)
    return dt.strftime(MINUTE_FMT)


def current_minute() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(second=0, microsecond=0)
        .strftime(MINUTE_FMT)
    )


def seconds_until_next_minute() -> float:
    now      = datetime.now(timezone.utc)
    next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return (next_min - now).total_seconds()


def parse_minute_bucket(ts: str) -> datetime:
    return datetime.strptime(ts, MINUTE_FMT).replace(tzinfo=timezone.utc)