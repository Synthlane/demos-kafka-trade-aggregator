import faust
from config import KAFKA_BROKER, KAFKA_PARTITIONS, KAFKA_TOPIC, SQL_TOPIC, DLQ_TOPIC
from models import Trade

app = faust.App(
    "trade-windowed",
    broker=KAFKA_BROKER,
    value_serializer="json",
    topic_partitions=KAFKA_PARTITIONS,
)

trades_topic = app.topic(KAFKA_TOPIC, value_type=Trade)  # inbound raw trades
sql_topic    = app.topic(SQL_TOPIC,   value_type=bytes)   # DB-write queue
dlq_topic    = app.topic(DLQ_TOPIC,   value_type=bytes)   # dead-letter queue

# 1-minute tumbling window, wall-clock time. expires=360 keeps past buckets readable.
volume_window = (
    app.Table("volume_window", default=float)
    .tumbling(60.0, expires=360.0)
    .relative_to_now()
)