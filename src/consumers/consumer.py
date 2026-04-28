
from confluent_kafka import Consumer
import socket 
CONSUMER_ID = socket.gethostname() 
c = Consumer({
    'bootstrap.servers': 'localhost:19092',
    'group.id':          'day1-group',
    'auto.offset.reset': 'latest'
})
c.subscribe(['trades'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    print(f"[{CONSUMER_ID}] Partition: {msg.partition()} | Key: {msg.key().decode()} | Offset: {msg.offset()}")


# * What triggers a rebalance?
# - kafka sends heartbeats to the consumers if the heartbeat response is failed then kafka mark it as dead and reimbalance the responsibility to other
# * Why is rebalancing briefly disruptive to throughput?
# - when the rebalancing occurs the entire consumption process stops and again the consumers are reassigns with partitions
# * What is the relationship between partition count and max parallelism?
# - the relationship is that one consumer can only consume one partition. so to achieve max parallelism follow 1:1 for partition and consumers . 
# - this will help us achieve full resource usage. 