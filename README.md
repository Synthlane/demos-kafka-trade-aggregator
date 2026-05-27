# Kafka Trade Aggregator

A real-time trade aggregation pipeline using Kafka (Redpanda), Faust, Redis, and PostgreSQL.

## Architecture

```
Binance WebSocket → Producer → Kafka (trades topic)
                                    ↓
                              Faust Worker
                         ┌─────────┼─────────┐
                         ↓         ↓         ↓
                  update_volume  flush_trades  reader (timer)
                    (window)    (batch→SQL)   (analytics)
                         ↓         ↓         ↓
                       Redis    sql_topic   sql_topic
                                    ↓         ↓
                              db_writer agent
                                    ↓
                              PostgreSQL
                              (trades + analytics)
```

## Prerequisites

- Python 3.11–3.13
- Docker (for Redis + PostgreSQL)
- `uv` package manager

## Quick Start

1. **Start local services (Redis + PostgreSQL):**
```bash
docker compose up -d
```

2. **Install dependencies:**
```bash
uv sync
```

3. **Run the Faust worker (tumbling window, default):**
```bash
./run.sh faust -A src.consumers.app worker -l info
```

4. **Run with hopping window:**
```bash
WINDOW_TYPE=hopping ./run.sh faust -A src.consumers.app worker -l info
```

## Topic Management

```bash
./run.sh src/consumers/manage_topic.py list
./run.sh src/consumers/manage_topic.py describe trades
./run.sh src/consumers/manage_topic.py delete <topic_name>
./run.sh src/consumers/manage_topic.py create <topic_name> --partitions 3
```

## Late Arrival Testing

```bash
./run.sh src/consumers/publish_late.py
LATE_MINUTES=5 ./run.sh src/consumers/publish_late.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka/Redpanda broker address |
| `KAFKA_SASL_USERNAME` | — | SASL username |
| `KAFKA_SASL_PASSWORD` | — | SASL password |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol |
| `KAFKA_TOPIC` | `trades` | Inbound trades topic |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/trades` | PostgreSQL DSN |
| `REDIS_URL` | `redis://localhost:6380` | Redis URL |
| `WINDOW_TYPE` | `tumbling` | Window type: `tumbling` or `hopping` |
| `WINDOW_SIZE_SECONDS` | `60` | Window size in seconds |
| `WINDOW_EXPIRES_SECONDS` | `600` | Window expiry (grace period) |
| `WINDOW_STEP_SECONDS` | `30` | Hopping window step size |
| `TRADES_FLUSH_SIZE` | `50` | Batch size before DB flush |

## Concepts Covered

- Topics, partitions, ordering guarantees
- Consumer groups, rebalancing, partition ownership
- Offsets, at-least-once delivery
- Stateful streaming with Faust Tables
- Tumbling vs hopping windows
- Late data handling via `expires` (grace period)
- Consumer lag and horizontal scaling
- Dead Letter Queue (DLQ) for failed writes
