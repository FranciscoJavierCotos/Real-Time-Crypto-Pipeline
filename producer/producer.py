import json
import logging
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:29092"

# Combined stream: trades + 1-min klines + 24hr ticker + order book top-of-book
WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@trade/btcusdt@kline_1m/btcusdt@ticker/btcusdt@bookTicker"
)

TOPIC_TRADES    = "btc-trades"
TOPIC_KLINES    = "btc-klines"
TOPIC_TICKER    = "btc-ticker"
TOPIC_ORDERBOOK = "btc-orderbook"

# Ticker fires on every trade; sample to ~1 msg every 15 s to keep volume manageable
TICKER_SAMPLE_SEC    = 15
# bookTicker fires on every order book update; sample to ~1 msg every 5 s
ORDERBOOK_SAMPLE_SEC = 5

_last_ticker_ts    = 0.0
_last_orderbook_ts = 0.0


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


# ---------------------------------------------------------------------------
# Per-stream handlers
# ---------------------------------------------------------------------------

def _handle_trade(data: dict, producer: KafkaProducer) -> None:
    msg = {
        "symbol":     data["s"],
        "price":      float(data["p"]),
        "volume":     float(data["q"]),
        "event_time": datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc).isoformat(),
    }
    producer.send(TOPIC_TRADES, msg)


def _handle_kline(data: dict, producer: KafkaProducer) -> None:
    k = data["k"]
    if not k["x"]:
        # kline is still forming — only publish completed (closed) candles
        return
    msg = {
        "symbol":                 data["s"],
        "open_time":              datetime.fromtimestamp(k["t"] / 1000, tz=timezone.utc).isoformat(),
        "close_time":             datetime.fromtimestamp(k["T"] / 1000, tz=timezone.utc).isoformat(),
        "open_price":             float(k["o"]),
        "high_price":             float(k["h"]),
        "low_price":              float(k["l"]),
        "close_price":            float(k["c"]),
        "volume":                 float(k["v"]),
        "quote_volume":           float(k["q"]),
        "trade_count":            int(k["n"]),
        "taker_buy_volume":       float(k["V"]),
        "taker_buy_quote_volume": float(k["Q"]),
    }
    producer.send(TOPIC_KLINES, msg)
    logger.info(
        "kline closed: %s  open=%.2f  close=%.2f  volume=%.4f  taker_buy_ratio=%.2f",
        msg["symbol"], msg["open_price"], msg["close_price"],
        msg["volume"],
        msg["taker_buy_volume"] / msg["volume"] if msg["volume"] else 0,
    )


def _handle_ticker(data: dict, producer: KafkaProducer) -> None:
    global _last_ticker_ts
    now = time.monotonic()
    if now - _last_ticker_ts < TICKER_SAMPLE_SEC:
        return
    _last_ticker_ts = now
    msg = {
        "symbol":             data["s"],
        "event_time":         datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc).isoformat(),
        "price_change":       float(data["p"]),
        "price_change_pct":   float(data["P"]),
        "weighted_avg_price": float(data["w"]),
        "open_price":         float(data["o"]),
        "high_price":         float(data["h"]),
        "low_price":          float(data["l"]),
        "volume_24h":         float(data["v"]),
        "quote_volume_24h":   float(data["q"]),
        "trade_count_24h":    int(data["n"]),
    }
    producer.send(TOPIC_TICKER, msg)
    logger.info("ticker: %s  24h_chg=%.2f%%  high=%.2f  low=%.2f",
                msg["symbol"], msg["price_change_pct"], msg["high_price"], msg["low_price"])


def _handle_orderbook(data: dict, producer: KafkaProducer) -> None:
    global _last_orderbook_ts
    now = time.monotonic()
    if now - _last_orderbook_ts < ORDERBOOK_SAMPLE_SEC:
        return
    _last_orderbook_ts = now
    msg = {
        "symbol":         data["s"],
        "event_time":     datetime.now(timezone.utc).isoformat(),
        "best_bid_price": float(data["b"]),
        "best_bid_qty":   float(data["B"]),
        "best_ask_price": float(data["a"]),
        "best_ask_qty":   float(data["A"]),
    }
    producer.send(TOPIC_ORDERBOOK, msg)


# ---------------------------------------------------------------------------
# WebSocket plumbing
# ---------------------------------------------------------------------------

_HANDLERS = {
    "btcusdt@trade":      _handle_trade,
    "btcusdt@kline_1m":   _handle_kline,
    "btcusdt@ticker":     _handle_ticker,
    "btcusdt@bookTicker": _handle_orderbook,
}


def on_message(ws, raw_message, producer: KafkaProducer) -> None:
    envelope = json.loads(raw_message)
    stream   = envelope.get("stream", "")
    data     = envelope.get("data", {})
    handler  = _HANDLERS.get(stream)
    if handler:
        handler(data, producer)
    else:
        logger.warning("Unknown stream: %s", stream)


def on_error(ws, error) -> None:
    logger.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg) -> None:
    logger.info("WebSocket closed (code=%s msg=%s)", close_status_code, close_msg)


def on_open(ws) -> None:
    logger.info("WebSocket connected — streams: trade, kline_1m, ticker, bookTicker")


def run() -> None:
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
