# Trade Aggregator

### Build a Live Trade Feed with Kafka

**What you're building:** You're going to tap into Binance's live market data and watch real Bitcoin, Ethereum, and Solana trades happen in real time — then build a pipeline that processes them. By the end of the week you'll have a system that tracks how much of each coin is being traded every minute, recovers from crashes on its own, and speeds up just by running one extra command.

**Why Kafka:** Every time someone buys or sells Bitcoin on Binance, an event fires. During busy market hours, thousands of these fire every second. A regular program reading them one by one would fall behind, lose data on a crash, and have no way to scale. Kafka solves all three problems — and this week you'll see exactly how, on real data, in a way that just reading docs never shows you.

**The cool part:** Binance makes their trade stream completely public. No account needed, no API key, nothing to sign up for. You'll connect to a live WebSocket URL and real trades will start pouring in immediately. The producer script that does this is already written for you — your job starts the moment data is inside Kafka.

**What the week looks like:** You start simple — just getting messages in and out. By the middle of the week you'll be intentionally crashing your consumer to watch Kafka recover it. By the end you'll be adding workers to your pipeline and watching it drain a backlog in real time. Each day builds on the last and every concept you learn shows up visibly in your terminal.

**What you won't need to worry about:** Setting up servers, writing the data ingestion code, mocking fake data, or any cloud accounts. One `docker compose up` and one `python producer.py` and you have live data flowing. Everything else is yours to build.

**By Friday you'll understand** why companies like Uber, Netflix and yes — trading firms — put Kafka at the center of their architecture. Not because you read about it, but because you'll have broken it and fixed it yourself on data that was real.


\
## TASK 

# Kafka Trade Aggregator — Week Project

**Stack:** Python · Faust · Redpanda · Docker

**Data source:** Binance public WebSocket (live trades, no API key needed)


---

## Setup (do this on Day 0 or before Day 1)

**1. Install dependencies**

```bash

pip install confluent-kafka faust websocket-client
```

**2. docker-compose.yml — copy this exactly**

```yaml

version: '3'
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 1
      - --memory 512M
      - --reserve-memory 0M
      - --node-id 0
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://localhost:9092
    ports:
      - "9092:9092"
      - "9644:9644"
```

```bash

docker compose up -d
```

Redpanda is now running on `localhost:9092`. No Zookeeper needed.

**3. [producer.py](http://producer.py) — this is pre-written, do not modify it**

```python

import json

import threading

import websocket

from confluent_kafka import Producer

BROKER = 'localhost:9092'
TOPIC  = 'trades'
SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt']

producer = Producer({'bootstrap.servers': BROKER})

def on_message(ws, message):
    trade = json.loads(message)
    symbol = trade['s']   # e.g. BTCUSDT
    payload = json.dumps({
        'symbol': symbol,
        'price':  float(trade['p']),
        'qty':    float(trade['q']),
        'time':   trade['T'],        # exchange timestamp in ms
        'buyer_maker': trade['m']
    })
    producer.produce(TOPIC, key=symbol, value=payload)
    producer.poll(0)

def start_stream(symbol):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    ws = websocket.WebSocketApp(url, on_message=on_message)
    ws.run_forever()

if __name__ == '__main__':
    threads = [threading.Thread(target=start_stream, args=(s,)) for s in SYMBOLS]
    for t in threads:
        t.daemon = True
        t.start()
    print("Producer running. Streaming BTCUSDT, ETHUSDT, SOLUSDT into 'trades' topic.")
    for t in threads:
        t.join()
```

Run this in a terminal and leave it running for the entire week:

```bash

python producer.py
```

You will see no output. That is correct. Data is flowing into Kafka silently.


---

## Day 1 — Topics and Partitioning

**Goal:** Understand what a topic is, what a partition is, and why partitioning by key gives you ordering guarantees per symbol.

**Task 1.1 — Inspect the topic**

The producer already created the `trades` topic automatically. Check it:

```bash

docker exec -it <container_id> rpk topic list

docker exec -it <container_id> rpk topic describe trades
```

You will see the topic has 1 partition by default. Note that down.

**Task 1.2 — Recreate with 3 partitions**

Delete the topic and recreate it with 3 partitions — one per symbol:

```bash

docker exec -it <container_id> rpk topic delete trades

docker exec -it <container_id> rpk topic create trades --partitions 3
```

Restart the producer. Now messages are routed by key hash. BTCUSDT always lands on the same partition. ETH on another. SOL on another.

**Task 1.3 — Write a simple consumer and print which partition each message lands on**

```python
# day1_consumer.py

from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id':          'day1-group',
    'auto.offset.reset': 'latest'
})
c.subscribe(['trades'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    print(f"Partition: {msg.partition()} | Key: {msg.key().decode()} | Offset: {msg.offset()}")
```

Run it. Observe that BTCUSDT always comes from the same partition number. This is the core guarantee: **same key → same partition → ordered delivery**.

**Questions to answer before moving to Day 2:**

* What happens to ordering if you publish without a key?
* Why does having 3 partitions and 1 consumer still work?
* What is an offset?


---

## Day 2 — Consumer Groups and Rebalancing

**Goal:** Understand how Kafka distributes partitions across consumers in a group and what happens when one dies.

**Task 2.1 — Run two consumers in the same group**

Open two terminals. Run the same script in both:

```python
# day2_consumer.py

import socket

from confluent_kafka import Consumer

CONSUMER_ID = socket.gethostname()  # just to tell them apart

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id':          'day2-group',
    'auto.offset.reset': 'latest'
})
c.subscribe(['trades'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    print(f"[{CONSUMER_ID}] Partition {msg.partition()} | {msg.key().decode()}")
```

You have 3 partitions and 2 consumers. Kafka assigns roughly: Consumer A gets partitions 0,1 — Consumer B gets partition 2. Each symbol is owned by exactly one consumer at a time.

**Task 2.2 — Kill one consumer**

Kill Consumer B with Ctrl+C. Within a few seconds, Consumer A prints messages from all 3 partitions. This is a **rebalance** — Kafka detected a consumer left the group and redistributed ownership.

Restart Consumer B. Another rebalance happens and partitions split again.

**Task 2.3 — Add a third consumer**

Start a third terminal running the same script. Now you have 3 consumers and 3 partitions — perfect 1:1 assignment. Each consumer owns exactly one symbol.

**Task 2.4 — Add a fourth consumer**

Start a fourth. One consumer sits idle and receives nothing. This is a critical insight: **you cannot parallelize beyond the number of partitions**. The fourth consumer is wasted until you increase partition count.

**Questions to answer:**

* What triggers a rebalance?
* Why is rebalancing briefly disruptive to throughput?
* What is the relationship between partition count and max parallelism?


---

## Day 3 — Offsets and Delivery Guarantees

**Goal:** Understand the difference between at-least-once and exactly-once, and what manual offset control gives you.

**Task 3.1 — Disable auto-commit**

```python
# day3_consumer.py

import json

import time

from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers':  'localhost:9092',
    'group.id':           'day3-group',
    'auto.offset.reset':  'earliest',
    'enable.auto.commit': False         # <-- key change
})
c.subscribe(['trades'])

count = 0

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue

    data = json.loads(msg.value())
    count += 1
    print(f"Processed #{count} | {data['symbol']} @ {data['price']}")

    # Only commit after processing
    c.commit(msg)
```

**Task 3.2 — Simulate a crash before commit**

Change the code so it crashes after processing 10 messages but before committing:

```python
    count += 1
    if count == 10:
        raise Exception("Simulated crash!")
    c.commit(msg)
```

Run it. It crashes at 10. Restart it. It replays from the last committed offset — you will see messages you already processed come through again. This is **at-least-once delivery**: messages may be reprocessed but never skipped.

**Task 3.3 — Understand earliest vs latest**

Change `auto.offset.reset` to `latest` and start a fresh group. It skips all historical messages and starts from now. Change back to `earliest` and a fresh group. It replays everything from the beginning.

**Questions to answer:**

* What is the difference between at-least-once and exactly-once?
* When would you choose to commit before vs after processing?
* What does `auto.offset.reset` control and when does it apply?


---

## Day 4 — Faust and Stateful Stream Processing (Table)

**Goal:** Learn what stateful streaming means. Instead of processing one message at a time and forgetting it, maintain running state — a live count of trade volume per symbol.

**Install Faust:**

```bash

pip install faust
```

**Task 4.1 — Basic Faust agent (stateless first)**

```python
# day4_app.py

import faust

app = faust.App(
    'trade-processor',
    broker='kafka://localhost:9092',
    value_serializer='json'
)

trades_topic = app.topic('trades', value_type=bytes)

@app.agent(trades_topic)
async def process(stream):
    async for msg in stream:
        import json
        data = json.loads(msg)
        print(f"{data['symbol']} traded {data['qty']} @ {data['price']}")
```

```bash

faust -A day4_app worker -l info
```

This is just a stateless consumer in Faust. Every message processed, nothing remembered.

**Task 4.2 — Add a Table (this is KTable equivalent)**

```python
# day4_app.py

import faust

import json

app = faust.App(
    'trade-processor',
    broker='kafka://localhost:9092',
    value_serializer='json'
)

trades_topic = app.topic('trades', value_type=bytes)

# This is the Table — distributed, fault-tolerant key-value state

volume_table = app.Table('volume_per_symbol', default=float)

@app.agent(trades_topic)
async def process(stream):
    async for msg in stream:
        data = json.loads(msg)
        symbol = data['symbol']
        qty    = data['qty']

        volume_table[symbol] += qty

        print(
            f"{symbol} | "
            f"this trade: {qty:.4f} | "
            f"total volume: {volume_table[symbol]:.4f}"
        )
```

Run it. You will see total volume per symbol climbing in real time. The Table persists state in a Kafka changelog topic — if you kill the worker and restart it, the state is recovered from the changelog. **The state lives in Kafka, not in your process.**

**Task 4.3 — Verify state recovery**

Let volume_table accumulate for a minute. Kill the worker. Note the total volume for BTCUSDT. Restart the worker. Observe it resumes from exactly where it stopped — not from zero.

**Questions to answer:**

* Where does the Table store its state?
* What is a changelog topic?
* What is the difference between a Faust Table and a regular Python dict?


---

## Day 5 — Windowed Aggregation

**Goal:** Instead of a total running count, compute volume per symbol per 1-minute window. This is the hardest and most important concept.

**Task 5.1 — Tumbling Window**

A tumbling window is a fixed, non-overlapping time bucket. Every minute is a separate bucket. A trade either falls in the 12:00–12:01 window or the 12:01–12:02 window — never both.

```python
# day5_app.py

import faust

import json

from datetime import timedelta

app = faust.App(
    'trade-windowed',
    broker='kafka://localhost:9092',
    value_serializer='json'
)

trades_topic = app.topic('trades', value_type=bytes)

# Tumbling window: 1-minute buckets, results expire after 10 minutes

volume_window = app.Table(
    'volume_window',
    default=float
).tumbling(
    timedelta(minutes=1),
    expires=timedelta(minutes=10)
)

@app.agent(trades_topic)
async def process(stream):
    async for msg in stream.events():
        data  = json.loads(msg.value)
        symbol = data['symbol']
        qty    = data['qty']
        ts     = data['time'] / 1000.0  # convert ms to seconds

        volume_window[symbol].set_relative_to_field(msg.message.timestamp)
        volume_window[symbol] += qty

        current = volume_window[symbol].current()
        print(f"{symbol} | 1-min volume: {current:.4f}")
```

Watch the volume for each symbol reset every minute. The window closes and a new one opens.

**Task 5.2 — Hopping Window**

A hopping window overlaps. A 1-minute window that hops every 30 seconds means each trade appears in two windows. Replace `.tumbling()` with:

```python
.hopping(
    size=timedelta(minutes=1),
    step=timedelta(seconds=30),
    expires=timedelta(minutes=10)
)
```

Observe the difference. The same trade now contributes to two overlapping windows. Think about when you would want this — e.g. a moving average rather than a discrete bucket.

**Questions to answer:**

* What is the difference between a tumbling and hopping window?
* What does `expires` do and why does it matter for memory?
* If a trade arrives with a timestamp from 5 minutes ago, which window does it go into?


---

## Day 6 — Late Arrivals and Grace Periods

**Goal:** Understand why late data is a fundamental distributed systems problem and how to handle it.

**The problem:** Binance DEX trades can arrive late because they wait for block confirmation. A trade that happened at 12:00:30 might not arrive in your consumer until 12:02:15. By then, the 12:00–12:01 window has already closed. Without handling this, that trade is silently dropped.

**Task 6.1 — Manually publish a late event**

While your Day 5 app is running, open a new terminal and publish a trade with a timestamp from 3 minutes ago:

```python
# publish_late.py

import json

import time

from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

late_trade = {
    'symbol': 'BTCUSDT',
    'price':  50000.0,
    'qty':    1.0,
    'time':   int((time.time() - 180) * 1000),  # 3 minutes ago in ms
    'buyer_maker': False
}

p.produce('trades', key='BTCUSDT', value=json.dumps(late_trade))
p.flush()
print("Late trade published with timestamp 3 minutes in the past")
```

Run this. The trade gets silently ignored — it falls outside any open window.

**Task 6.2 — Add a grace period**

A grace period tells the window: stay open a little longer to accept late arrivals. Update the window definition:

```python

volume_window = app.Table(
    'volume_window',
    default=float
).tumbling(
    timedelta(minutes=1),
    grace_period=timedelta(minutes=5),   # <-- accept events up to 5 min late
    expires=timedelta(minutes=10)
)
```

Publish the late trade again. Now it is accepted into the window it belongs to.

**Task 6.3 — Think about the tradeoff**

A grace period has a cost: you cannot finalize (emit) a window's result until the grace period expires. A 5-minute grace period means your aggregates are always 5 minutes stale. This is the fundamental tension in all streaming systems: **latency vs correctness**. There is no free lunch.

**Questions to answer:**

* Why do late events happen in a distributed system?
* What does a grace period cost you?
* How would you decide the right grace period in production?


---

## Day 7 — Consumer Lag and Scaling

**Goal:** Understand backpressure and how to diagnose and fix a consumer that cannot keep up with the producer.

**Task 7.1 — Create artificial lag**

Take your Day 4 app and add a deliberate slow processing step:

```python
@app.agent(trades_topic)
async def process(stream):
    async for msg in stream:
        data = json.loads(msg)
        await asyncio.sleep(0.5)   # simulate slow processing
        volume_table[data['symbol']] += data['qty']
```

Run it. The consumer processes 2 messages per second. Binance sends roughly 10–20 trades per second across 3 symbols. Lag is growing.

**Task 7.2 — Observe the lag**

In a separate terminal, watch consumer lag grow in real time:

```bash

docker exec -it <container_id> rpk group describe trade-processor
```

You will see the lag number climbing. This is how you diagnose a slow consumer in production.

**Task 7.3 — Scale by adding workers**

Faust workers can run concurrently. Open a second terminal and run:

```bash

faust -A day4_app worker -l info
```

Faust detects a second worker, rebalances partitions between them, and lag starts draining. You now have two workers processing in parallel — same principle as Day 2 consumer groups, but at the Faust layer.

**Task 7.4 — Find the ceiling**

Add a third worker. Lag drains faster. Add a fourth worker. It sits idle — you only have 3 partitions. To go beyond 3 workers you must increase partition count on the topic.

```bash

docker exec -it <container_id> rpk topic alter-config trades --set num_partitions=6
```

Now add more workers up to 6 and observe all of them receiving messages.

**Questions to answer:**

* What is the relationship between partition count and max consumer parallelism?
* What does consumer lag tell you about your system health?
* What happens to lag if processing time per message doubles?


---

## What you know by the end of this week

```
Day 1  →  Topics, partitions, ordering guarantees

Day 2  →  Consumer groups, rebalancing, partition ownership

Day 3  →  Offsets, at-least-once delivery, manual commit

Day 4  →  Stateful streaming, Faust Tables, changelog topics

Day 5  →  Tumbling vs hopping windows, time-based aggregation

Day 6  →  Late data, grace periods, latency vs correctness tradeoff

Day 7  →  Consumer lag, backpressure, horizontal scaling
```

These are the exact concepts behind the CEX/DEX trading infrastructure. The only difference between this project and that one is the producer is Binance's public WebSocket instead of a live exchange connector, and the sink is your terminal instead of a trading execution engine.


Repo:  <https://github.com/Synthlane/demos-kafka-trade-aggregator> 

<https://claude.ai/share/b8afadd2-c6e0-4b00-8212-42365cf67a32>

Kafka console URL: <https://redpanda.ext.synthlane.com/overview>
