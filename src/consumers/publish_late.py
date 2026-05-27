"""Publish a trade with a timestamp in the past to test late arrival handling.

Usage:
    uv run src/consumers/publish_late.py
    LATE_MINUTES=5 uv run src/consumers/publish_late.py
"""

import os
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
TOPIC = os.getenv("KAFKA_TOPIC", "trades")
LATE_MINUTES = int(os.getenv("LATE_MINUTES", "3"))


def main():
    conf = {"bootstrap.servers": BROKER}
    if PROTOCOL == "SASL_PLAINTEXT":
        conf.update({
            "security.protocol": PROTOCOL,
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": USERNAME,
            "sasl.password": PASSWORD,
        })

    p = Producer(conf)

    late_ts_ms = int((time.time() - LATE_MINUTES * 60) * 1000)

    late_trade = {
        "symbol": "BTCUSDT",
        "price": 50000.0,
        "qty": 1.0,
        "time": late_ts_ms,
        "buyer_maker": False,
    }

    p.produce(
        TOPIC,
        key="BTCUSDT",
        value=json.dumps(late_trade),
        timestamp=late_ts_ms,
    )
    p.flush()

    print(f"Late trade published with timestamp {LATE_MINUTES} minutes in the past")
    print(f"  Timestamp (ms): {late_ts_ms}")
    print(f"  Payload: {late_trade}")


if __name__ == "__main__":
    main()
