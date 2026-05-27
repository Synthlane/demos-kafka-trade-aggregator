import faust
from datetime import timedelta

from .config import (
    KAFKA_BROKER,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_TOPIC,
    SQL_TOPIC,
    DLQ_TOPIC,
    WINDOW_TYPE,
    WINDOW_SIZE_SECONDS,
    WINDOW_EXPIRES_SECONDS,
    WINDOW_STEP_SECONDS,
)

broker_credentials = faust.SASLCredentials(
    username=KAFKA_SASL_USERNAME,
    password=KAFKA_SASL_PASSWORD,
    mechanism="SCRAM-SHA-512",
)

broker_url = f"kafka://{KAFKA_BROKER}"

app = faust.App(
    "trade-processor",
    broker=broker_url,
    broker_credentials=broker_credentials,
    value_serializer="json",
    topic_partitions=3,
    store="memory://",
    web_enabled=False,
)

trades_topic = app.topic(KAFKA_TOPIC, value_type=bytes)
sql_topic = app.topic(SQL_TOPIC, value_type=bytes, internal=True)
dlq_topic = app.topic(DLQ_TOPIC, value_type=bytes, internal=True)

# Windowed aggregation table
_base_table = app.Table("volume_window", default=float)

if WINDOW_TYPE == "hopping":
    volume_window = (
        _base_table.hopping(
            size=WINDOW_SIZE_SECONDS,
            step=WINDOW_STEP_SECONDS,
            expires=WINDOW_EXPIRES_SECONDS,
        ).relative_to_stream()
    )
else:
    volume_window = (
        _base_table.tumbling(
            size=WINDOW_SIZE_SECONDS,
            expires=WINDOW_EXPIRES_SECONDS,
        ).relative_to_stream()
    )

# Auto-discover agents
from . import agents  # noqa: E402, F401
