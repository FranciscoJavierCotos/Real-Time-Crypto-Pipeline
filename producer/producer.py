import json
import logging
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "btc-trades"
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"


def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


def on_message(ws, raw_message, producer):
    raw = json.loads(raw_message)
    trade = {
        "symbol": raw["s"],
        "price": float(raw["p"]),
        "volume": float(raw["q"]),
        "event_time": datetime.fromtimestamp(
            raw["T"] / 1000, tz=timezone.utc
        ).isoformat(),
    }
    producer.send(TOPIC, trade)
    logger.info("Published: %s", trade)


def on_error(ws, error):
    logger.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket closed (code=%s msg=%s)", close_status_code, close_msg)


def on_open(ws):
    logger.info("WebSocket connected to %s", WS_URL)


def run():
    producer = make_producer()
    ws = websocket.WebSocketApp(
        WS_URL,
        on_message=lambda ws, msg: on_message(ws, msg, producer),
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws.run_forever()


if __name__ == "__main__":
    run()
