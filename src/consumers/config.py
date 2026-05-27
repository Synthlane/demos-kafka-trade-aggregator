import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trades")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/trades")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")

# Internal Kafka topics
SQL_TOPIC = "trade-processor-sql-writes"
DLQ_TOPIC = "trade-processor-dlq"

# DB writer config
TRADES_FLUSH_SIZE = int(os.getenv("TRADES_FLUSH_SIZE", "50"))
DB_WRITER_MAX_RETRIES = int(os.getenv("DB_WRITER_MAX_RETRIES", "3"))

# Window configuration
WINDOW_TYPE = os.getenv("WINDOW_TYPE", "tumbling")  # "tumbling" or "hopping"
WINDOW_SIZE_SECONDS = int(os.getenv("WINDOW_SIZE_SECONDS", "60"))
WINDOW_EXPIRES_SECONDS = int(os.getenv("WINDOW_EXPIRES_SECONDS", "600"))
WINDOW_STEP_SECONDS = int(os.getenv("WINDOW_STEP_SECONDS", "30"))
