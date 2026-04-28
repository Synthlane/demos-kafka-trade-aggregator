import json
import os
import threading
import time
import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv()
BROKER  = os.environ['KAFKA_BROKER']
TOPIC   = os.environ.get('KAFKA_TOPIC', 'trades')
SYMBOLS = [
    'btcusdt',  'ethusdt',  'solusdt'
]
_config = {'bootstrap.servers': BROKER}
if os.environ.get('KAFKA_SASL_USERNAME'):
    _config.update({
        'security.protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
        'sasl.mechanism':    'SCRAM-SHA-512',
        'sasl.username':     os.environ['KAFKA_SASL_USERNAME'],
        'sasl.password':     os.environ['KAFKA_SASL_PASSWORD'],
    })

producer = Producer(_config)

_lock  = threading.Lock()
_count = 0

def on_message(ws, message):
    global _count
    trade   = json.loads(message)
    symbol  = trade['s']
    payload = json.dumps({
        'symbol':      symbol,
        'price':       float(trade['p']),
        'qty':         float(trade['q']),
        'time':        trade['T'],
        'buyer_maker': trade['m'],
    })
    producer.produce(TOPIC, key=symbol, value=payload)
    producer.poll(0)
    with _lock:
        _count += 1

def _log_stats():
    global _count
    while True:
        time.sleep(10)
        with _lock:
            n, _count = _count, 0
        print(f"[stats] {n} messages produced in last 10s ({n / 10:.1f}/s)")

def _on_error(ws, error):
    print(f"[error] {ws.url}: {error}")

def _on_close(ws, code, msg):
    print(f"[close] {ws.url} code={code} — reconnecting")

def start_stream(symbol):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    ws  = websocket.WebSocketApp(url, on_message=on_message, on_error=_on_error, on_close=_on_close)
    ws.run_forever(reconnect=5)

if __name__ == '__main__':
    threading.Thread(target=_log_stats, daemon=True).start()

    threads = [threading.Thread(target=start_stream, args=(s,), daemon=True) for s in SYMBOLS]
    for t in threads:
        t.start()
    print(f"Producer running. Streaming {len(SYMBOLS)} symbols into '{TOPIC}' topic.")
    for t in threads:
        t.join()
