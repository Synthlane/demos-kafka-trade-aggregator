# Producer Env-Var Config Design

**Date:** 2026-04-27  
**Scope:** `src/producers/producer.py` only

## Problem

Broker URL and auth credentials are hardcoded as `localhost:9092` with no authentication. The project connects to a remote Redpanda cluster (`redpanda.ext.synthlane.com:9093`) with SASL SCRAM-SHA-512 auth managed via Infisical.

## Decision

No docker-compose. Local Kafka is unnecessary — the remote cluster is always available and dedicated to this demo project.

## Environment Variables

| Var | Required | Default | Purpose |
|-----|----------|---------|---------|
| `KAFKA_BROKER` | yes | — | Broker address, e.g. `redpanda.ext.synthlane.com:9093` |
| `KAFKA_SASL_USERNAME` | no | — | SASL username; if absent, connects plaintext |
| `KAFKA_SASL_PASSWORD` | no* | — | SASL password; required when username is set |
| `KAFKA_SECURITY_PROTOCOL` | no | `SASL_PLAINTEXT` | Override to `SASL_SSL` if Traefik terminates TLS |
| `KAFKA_TOPIC` | no | `trades` | Topic to produce into |

## Producer Config Logic

Build a base config dict with `bootstrap.servers`. If `KAFKA_SASL_USERNAME` is present, merge in `security.protocol`, `sasl.mechanism` (`SCRAM-SHA-512`), `sasl.username`, `sasl.password`. Pass the merged dict to `confluent_kafka.Producer`.

## What Does Not Change

- `on_message` handler
- `start_stream` / threading logic
- `SYMBOLS` list (remains hardcoded — no operational reason to externalise it)

## Run Command

```bash
infisical run -- uv run src/producers/producer.py
```
