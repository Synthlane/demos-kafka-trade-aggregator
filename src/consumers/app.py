import faust
from config import KAFKA_BROKER, KAFKA_PARTITIONS, KAFKA_TOPIC, SQL_TOPIC, DLQ_TOPIC

app = faust.App(
    "trade-windowed",
    broker=KAFKA_BROKER,
    value_serializer="json",
    topic_partitions=KAFKA_PARTITIONS,
)

trades_topic = app.topic(KAFKA_TOPIC, value_type=bytes)  # inbound raw trades
sql_topic    = app.topic(SQL_TOPIC,   value_type=bytes)  # DB-write queue
dlq_topic    = app.topic(DLQ_TOPIC,   value_type=bytes)  # dead-letter queue

# 1-minute tumbling windows. Keep state slightly beyond the maximum expected
# lateness (5 minutes) so out-of-order events still update their window.
volume_window = app.Table("volume_window", default=float).tumbling(60.0, expires=360.0)