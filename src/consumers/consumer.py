
from confluent_kafka import Consumer

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
    print(f"Partition: {msg.partition()} | Key: {msg.key().decode()} | Offset: {msg.offset()}")


# * What happens to ordering if you publish without a key?
# - kafka partition the messages based on the key provided. if no key is provided then I'll perform round robin or sticky partitions to send the message to different partitions. 
# * Why does having 3 partitions and 1 consumer still work?
# - because the consumer is subscribed to the topic not the partition. 
# * What is an offset?
# - offset is a pointer set in the kafka partition log which help consumer to tracks the progress. 