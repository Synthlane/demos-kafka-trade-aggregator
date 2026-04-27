import json
import os
import threading
import websocket
from confluent_kafka import Producer

BROKER   = os.environ['KAFKA_BROKER']
TOPIC    = os.environ.get('KAFKA_TOPIC', 'trades')
SYMBOLS  = ['btcusdt', 'ethusdt', 'solusdt']

_config = {'bootstrap.servers': BROKER}
if os.environ.get('KAFKA_SASL_USERNAME'):
    _config.update({
        'security.protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
        'sasl.mechanism':    'SCRAM-SHA-512',
        'sasl.username':     os.environ['KAFKA_SASL_USERNAME'],
        'sasl.password':     os.environ['KAFKA_SASL_PASSWORD'],
    })

producer = Producer(_config)

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