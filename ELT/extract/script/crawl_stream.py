import asyncio
import json
import signal
from datetime import datetime, UTC
import websockets
from confluent_kafka import Producer
import logging,os
from dotenv import load_dotenv

KAFKA_BOOTSTRAP = "localhost:9092"

logger=logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
OKX_WS_PUB = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_BUSINESS = "wss://ws.okx.com:8443/ws/v5/business"

TOPIC_TRADES = "okx_trades"
TOPIC_ORDERBOOK = "okx_orderbook"
TOPIC_FUNDING = "okx_funding"
TOPIC_OHLC = "okx_ohlc"
TOPIC_MARK_PRICE = "okx_mark_price"

SUB_ARG_PUB = [
    {"channel": "trades", "instId": "BTC-USDT-SWAP"},
    {"channel": "books5", "instId": "BTC-USDT"},
    {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
    {"channel": "mark-price", "instId": "BTC-USDT-SWAP"}
]
SUB_ARG_BUSINESS = [
    {"channel": "candle1s", "instId": "BTC-USDT-SWAP"},
]
config={
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    #'sasl.username': kafka_api_key,
    #'sasl.password': kafka_secret_key,
    #'security.protocol': 'SASL_SSL',
    #'sasl.mechanisms': 'PLAIN', # credential username/password
    'linger.ms': 50,
    'batch.size': 16384
    #"queue.buffering.max.messages": 100000,
    #"buffer.memory": 102400
}
p = Producer(config)
running = True

# if remain any data shutdown() will push all of this before shutdown
def delivery_callback(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def shutdown():
    global running
    running = False
    try:
        p.flush(timeout=5)
    except Exception as e:
        logger.error(f"Error flushing producer: {e}")
    print("Producer shutdown complete.")

signal.signal(signal.SIGINT, lambda s, f: shutdown())
signal.signal(signal.SIGTERM, lambda s, f: shutdown()) # stop program and call shutdown() to push remain data

# send ping each 20s ->websocket don't close connect
async def heartbeat(ws):
    while running:
        try:
            await ws.send("ping")
            await asyncio.sleep(20)
        except Exception:
            break
        #await asyncio.sleep(20)


async def route_message(raw_msg):
    """Route OKX WS message to the correct Kafka topic."""
    if raw_msg == "pong":
        return

    try:
        msg = json.loads(raw_msg)
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON")
        return

    # Bỏ qua các tin nhắn event (login, subscribe, error)
    if 'event' in msg:
        if msg['event'] == 'error':
            logger.error(f"OKX Error: {msg}")
        else:
            logger.info(f"Event received: {msg}")
        return
    arg = msg.get("arg", {})
    chan = arg.get("channel", "")

    if chan == "trades":
        topic = TOPIC_TRADES
    elif "book" in chan:
        topic = TOPIC_ORDERBOOK
    elif chan == "funding-rate":
        topic = TOPIC_FUNDING
    elif "candle" in chan: # Handle candle từ Business URL
        topic = TOPIC_OHLC
    elif "mark-price" in chan:
        topic = TOPIC_MARK_PRICE
    else:
        topic = "okx_others" # Fallback topic

    record = {
        "received_at": datetime.now().isoformat(),
        "payload": msg
    }
    # khi send msg, nếu ko có poll() bộ nhớ sẽ đầy, khi đầy buffer nó sẽ poll và callback lại để xử lí msg
    try:
        p.produce(topic, json.dumps(record).encode("utf-8"),on_delivery=delivery_callback)
    except BufferError:
        p.poll(1) # return 1 message or callback
        p.produce(topic, json.dumps(record).encode("utf-8"),on_delivery=delivery_callback)

    p.poll(0)   # serve delivery callbacks

async def run_collector(url,channels,socket_name='WS'):
    global running

    while running:
        try:
            async with websockets.connect(url, ping_interval=None, max_size=None) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": channels}))
                #print("Subscribed:", SUBSCRIBE_ARGS)
                logger.info(f"Subscribed: {socket_name}")

                hb = asyncio.create_task(heartbeat(ws))

                while running:
                    try:
                        msg = await ws.recv()
                        await route_message(msg)

                    except websockets.ConnectionClosed:
                        logger.warning(f"[{socket_name}] close connection.")
                        break

                hb.cancel()

        except Exception as e:
            if running:
                logger.error(f"[{socket_name}] Error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)
            else:
                logger.info(f"[{socket_name}] Stopped.")


async def main():
    # Tạo 2 task chạy song song: một cho Public, một cho Business
    task_public = asyncio.create_task(
        run_collector(OKX_WS_PUB, SUB_ARG_PUB,'PUB_WS')
    )
    task_business = asyncio.create_task(
        run_collector(OKX_WS_BUSINESS, SUB_ARG_BUSINESS,'BUSINESS_WS')
    )

    # Chờ cả 2 task (hoặc chờ shutdown)
    await asyncio.gather(task_public, task_business)

if __name__ == "__main__":
    print("Collect from websockets.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        shutdown()