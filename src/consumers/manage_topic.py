"""Utility to manage Kafka topics on the remote Redpanda cluster.

Usage:
    uv run src/consumers/manage_topic.py list
    uv run src/consumers/manage_topic.py describe [topic_name]
    uv run src/consumers/manage_topic.py delete <topic_name>
    uv run src/consumers/manage_topic.py create <topic_name> --partitions 3
"""

import os
import sys
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")


def _admin() -> AdminClient:
    conf = {"bootstrap.servers": BROKER}
    if PROTOCOL == "SASL_PLAINTEXT":
        conf.update({
            "security.protocol": PROTOCOL,
            "sasl.mechanism": "PLAIN",
            "sasl.username": USERNAME,
            "sasl.password": PASSWORD,
        })
    return AdminClient(conf)


def list_topics():
    admin = _admin()
    metadata = admin.list_topics(timeout=10)
    for topic in sorted(metadata.topics.keys()):
        partitions = len(metadata.topics[topic].partitions)
        print(f"  {topic} ({partitions} partitions)")


def describe_topic(name: str):
    admin = _admin()
    metadata = admin.list_topics(timeout=10)
    topic_meta = metadata.topics.get(name)
    if not topic_meta:
        print(f"Topic '{name}' not found")
        return
    print(f"Topic: {name}")
    print(f"  Partitions: {len(topic_meta.partitions)}")
    for pid, part in topic_meta.partitions.items():
        print(f"    [{pid}] leader={part.leader} replicas={part.replicas}")


def delete_topic(name: str):
    admin = _admin()
    futures = admin.delete_topics([name], operation_timeout=30)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Deleted: {topic}")
        except Exception as e:
            print(f"Failed to delete {topic}: {e}")


def create_topic(name: str, partitions: int = 3):
    admin = _admin()
    topic = NewTopic(name, num_partitions=partitions, replication_factor=1)
    futures = admin.create_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"Created: {topic_name} ({partitions} partitions)")
        except Exception as e:
            print(f"Failed to create {topic_name}: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: manage_topic.py <list|describe|delete|create> [args]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "list":
        list_topics()
    elif cmd == "describe":
        name = sys.argv[2] if len(sys.argv) > 2 else "trades"
        describe_topic(name)
    elif cmd == "delete":
        if len(sys.argv) < 3:
            print("Usage: manage_topic.py delete <topic_name>")
            sys.exit(1)
        delete_topic(sys.argv[2])
    elif cmd == "create":
        if len(sys.argv) < 3:
            print("Usage: manage_topic.py create <topic_name> [--partitions N]")
            sys.exit(1)
        name = sys.argv[2]
        parts = 3
        if "--partitions" in sys.argv:
            idx = sys.argv.index("--partitions")
            parts = int(sys.argv[idx + 1])
        create_topic(name, parts)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
