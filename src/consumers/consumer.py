
from confluent_kafka import Consumer
import socket 
CONSUMER_ID = socket.gethostname() 
c = Consumer({
    'bootstrap.servers': 'localhost:19092',
    'group.id':          'day1-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False 
})
c.subscribe(['trades'])
count = 0 
while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    print(f"[{CONSUMER_ID}] Partition: {msg.partition()} | Key: {msg.key().decode()} | Offset: {msg.offset()}")

    count += 1
    if count == 10:
        raise Exception("Simulated crash!")
    c.commit(msg)
 

