
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
 


# What is the difference between at-least-once and exactly-once?
# - at least once ensure that the message delivers equal or more than 1 time whereas exactly once ensure that the message delivers exactly 1 time. 
# When would you choose to commit before vs after processing?
# - Because I want to keep my processing logic stateless the state should be present in the kafka. 
# - Even if it fails then the processing will start from where it left.  
# What does auto.offset.reset control and when does it apply?
# - if the commit offset is currupted or invalid. then it's used to reset the offset.
# - there are two options 'earliest' and 'latest' when assign with 1st it'll start from 0 if assign 2nd it'll start from the latest offset/current.  