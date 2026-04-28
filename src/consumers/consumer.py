import faust

import json

app = faust.App(
    'trade-processor',
    broker='kafka://localhost:19092',
    value_serializer='json',
    topic_partitions=3
)

trades_topic = app.topic('trades', value_type=bytes)

# This is the Table — distributed, fault-tolerant key-value state

volume_table = app.Table('volume_per_symbol', default=float)

@app.agent(trades_topic)
async def process(stream):
    async for msg in stream:
        data = json.loads(msg) if not isinstance(msg, dict) else msg 
        symbol = data['symbol']
        qty    = data['qty']

        volume_table[symbol] += qty

        print(
            f"{symbol} | "
            f"this trade: {qty:.4f} | "
            f"total volume: {volume_table[symbol]:.4f}"
        )


# * Where does the Table store its state?
# - Faust write the data in the local first and then push to the kafka changelog.
# * What is a changelog topic?
# - changelog topic is a place where the faust store it's state. so that if any failure occurs it'll resume from the point of failure. 
# * What is the difference between a Faust Table and a regular Python dict?
# - Faust table first writes the data into the local then push it into the kafka. Where python has no point of recovery. even if we store the stats still it's not that durable.
