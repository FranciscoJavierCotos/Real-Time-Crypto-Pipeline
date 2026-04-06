"""
processor/consumer.py

Kafka consumer that implements the Bronze/Silver architecture without Spark.

Four parallel consumer threads:
  1. TradeConsumer   — btc-trades   → Bronze (raw trades) + Silver (1-min aggregations)
  2. KlineConsumer   — btc-klines   → Bronze (closed OHLC candles, already 1-min aggregated)
  3. TickerConsumer  — btc-ticker   → Bronze (24hr rolling ticker snapshots)
  4. OrderbookConsumer — btc-orderbook → Bronze (bid/ask spread snapshots)

Bronze: raw records flushed to Parquet every BRONZE_FLUSH_SEC seconds.
Silver: 1-minute windowed aggregations flushed every SILVER_FLUSH_SEC seconds.
        A window is written once it ended more than LATE_THRESHOLD_SEC seconds ago.
"""

import json
import os
import signal
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DATA_BASE          = Path(os.environ.get("DATA_BASE_PATH", "./data"))

BRONZE_TRADES_DIR   = DATA_BASE / "bronze" / "btc_trades"
SILVER_DIR          = DATA_BASE / "silver" / "btc_aggregates"
BRONZE_KLINES_DIR   = DATA_BASE / "bronze" / "btc_klines"
BRONZE_TICKER_DIR   = DATA_BASE / "bronze" / "btc_ticker"
BRONZE_ORDERBOOK_DIR = DATA_BASE / "bronze" / "btc_orderbook"

BRONZE_FLUSH_SEC   = int(os.environ.get("BRONZE_FLUSH_SEC", "10"))
SILVER_FLUSH_SEC   = int(os.environ.get("SILVER_FLUSH_SEC", "30"))
WINDOW_SIZE_SEC    = 60
LATE_THRESHOLD_SEC = 60  # close a window this many seconds after it ends

# ---------------------------------------------------------------------------
# Parquet schemas
# ---------------------------------------------------------------------------
BRONZE_SCHEMA = pa.schema([
    pa.field("symbol",      pa.string()),
    pa.field("price",       pa.float64()),
    pa.field("volume",      pa.float64()),
    pa.field("event_time",  pa.timestamp("us", tz="UTC")),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])

SILVER_SCHEMA = pa.schema([
    pa.field("window_start",  pa.timestamp("us", tz="UTC")),
    pa.field("window_end",    pa.timestamp("us", tz="UTC")),
    pa.field("symbol",        pa.string()),
    pa.field("avg_price",     pa.float64()),
    pa.field("total_volume",  pa.float64()),
])

KLINE_SCHEMA = pa.schema([
    pa.field("symbol",                 pa.string()),
    pa.field("open_time",              pa.timestamp("us", tz="UTC")),
    pa.field("close_time",             pa.timestamp("us", tz="UTC")),
    pa.field("open_price",             pa.float64()),
    pa.field("high_price",             pa.float64()),
    pa.field("low_price",              pa.float64()),
    pa.field("close_price",            pa.float64()),
    pa.field("volume",                 pa.float64()),
    pa.field("quote_volume",           pa.float64()),
    pa.field("trade_count",            pa.int64()),
    pa.field("taker_buy_volume",       pa.float64()),
    pa.field("taker_buy_quote_volume", pa.float64()),
    pa.field("ingested_at",            pa.timestamp("us", tz="UTC")),
])

TICKER_SCHEMA = pa.schema([
    pa.field("symbol",             pa.string()),
    pa.field("event_time",         pa.timestamp("us", tz="UTC")),
    pa.field("price_change",       pa.float64()),
    pa.field("price_change_pct",   pa.float64()),
    pa.field("weighted_avg_price", pa.float64()),
    pa.field("open_price",         pa.float64()),
    pa.field("high_price",         pa.float64()),
    pa.field("low_price",          pa.float64()),
    pa.field("volume_24h",         pa.float64()),
    pa.field("quote_volume_24h",   pa.float64()),
    pa.field("trade_count_24h",    pa.int64()),
    pa.field("ingested_at",        pa.timestamp("us", tz="UTC")),
])

ORDERBOOK_SCHEMA = pa.schema([
    pa.field("symbol",         pa.string()),
    pa.field("event_time",     pa.timestamp("us", tz="UTC")),
    pa.field("best_bid_price", pa.float64()),
    pa.field("best_bid_qty",   pa.float64()),
    pa.field("best_ask_price", pa.float64()),
    pa.field("best_ask_qty",   pa.float64()),
    pa.field("spread",         pa.float64()),
    pa.field("spread_pct",     pa.float64()),
    pa.field("ingested_at",    pa.timestamp("us", tz="UTC")),
])

# ---------------------------------------------------------------------------
# Shared shutdown event
# ---------------------------------------------------------------------------
_stop = threading.Event()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _floor_minute(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.replace(second=0, microsecond=0, nanosecond=0)


def _write_parquet(records: list, schema: pa.Schema, directory: Path, label: str) -> None:
    if not records:
        return
    ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    path = directory / f"{ts_str}_{uuid.uuid4().hex[:8]}.parquet"
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, str(path), compression="snappy")
    print(f"[{label}] Wrote {len(records)} rows → {path.name}", flush=True)


# ---------------------------------------------------------------------------
# Trade consumer (Bronze raw + Silver 1-min aggregations)
# ---------------------------------------------------------------------------

def _run_trade_consumer() -> None:
    BRONZE_TRADES_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        "btc-trades",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="btc-processor",
        consumer_timeout_ms=1000,
    )

    bronze_buffer: list = []
    silver_windows: dict = defaultdict(lambda: {"sum_price": 0.0, "sum_volume": 0.0, "count": 0})
    last_bronze_flush = time.monotonic()
    last_silver_flush = time.monotonic()

    def flush_bronze() -> None:
        nonlocal bronze_buffer, last_bronze_flush
        _write_parquet(bronze_buffer, BRONZE_SCHEMA, BRONZE_TRADES_DIR, "bronze/trades")
        bronze_buffer = []
        last_bronze_flush = time.monotonic()

    def flush_silver(now_utc: datetime, force: bool = False) -> None:
        nonlocal last_silver_flush
        cutoff = now_utc.timestamp() - LATE_THRESHOLD_SEC
        completed = []
        still_open = {}
        for (symbol, win_start), acc in silver_windows.items():
            win_end_ts = win_start + pd.Timedelta(seconds=WINDOW_SIZE_SEC)
            if win_end_ts.timestamp() < cutoff or force:
                completed.append({
                    "window_start": win_start.to_pydatetime(),
                    "window_end":   win_end_ts.to_pydatetime(),
                    "symbol":       symbol,
                    "avg_price":    acc["sum_price"] / acc["count"],
                    "total_volume": acc["sum_volume"],
                })
            else:
                still_open[(symbol, win_start)] = acc
        silver_windows.clear()
        silver_windows.update(still_open)
        _write_parquet(completed, SILVER_SCHEMA, SILVER_DIR, "silver")
        last_silver_flush = time.monotonic()

    try:
        while not _stop.is_set():
            try:
                for msg in consumer:
                    if _stop.is_set():
                        break
                    record = msg.value
                    try:
                        event_time = pd.Timestamp(record["event_time"], tz="UTC")
                        price      = float(record["price"])
                        volume     = float(record["volume"])
                        symbol     = str(record["symbol"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    if price <= 0 or volume <= 0:
                        continue

                    ingested_at = datetime.now(timezone.utc)
                    bronze_buffer.append({
                        "symbol":      symbol,
                        "price":       price,
                        "volume":      volume,
                        "event_time":  event_time.to_pydatetime(),
                        "ingested_at": ingested_at,
                    })
                    win_start = _floor_minute(event_time)
                    silver_windows[(symbol, win_start)]["sum_price"]  += price
                    silver_windows[(symbol, win_start)]["sum_volume"] += volume
                    silver_windows[(symbol, win_start)]["count"]      += 1
            except StopIteration:
                pass

            now_mono = time.monotonic()
            if now_mono - last_bronze_flush >= BRONZE_FLUSH_SEC:
                flush_bronze()
            if now_mono - last_silver_flush >= SILVER_FLUSH_SEC:
                flush_silver(datetime.now(timezone.utc))
    finally:
        flush_bronze()
        flush_silver(datetime.now(timezone.utc), force=True)
        consumer.close()
        print("[trade consumer] shut down cleanly.", flush=True)


# ---------------------------------------------------------------------------
# Generic Bronze consumer (klines / ticker / orderbook)
# ---------------------------------------------------------------------------

def _run_bronze_consumer(
    topic: str,
    group_id: str,
    directory: Path,
    schema: pa.Schema,
    transform_fn,
    flush_sec: int,
    label: str,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=group_id,
        consumer_timeout_ms=1000,
    )

    buffer: list = []
    last_flush = time.monotonic()

    try:
        while not _stop.is_set():
            try:
                for msg in consumer:
                    if _stop.is_set():
                        break
                    record = transform_fn(msg.value)
                    if record is not None:
                        buffer.append(record)
            except StopIteration:
                pass

            if time.monotonic() - last_flush >= flush_sec:
                _write_parquet(buffer, schema, directory, label)
                buffer = []
                last_flush = time.monotonic()
    finally:
        _write_parquet(buffer, schema, directory, label)
        consumer.close()
        print(f"[{label}] shut down cleanly.", flush=True)


# ---------------------------------------------------------------------------
# Transform functions
# ---------------------------------------------------------------------------

def _transform_kline(record: dict):
    try:
        return {
            "symbol":                 str(record["symbol"]),
            "open_time":              pd.Timestamp(record["open_time"], tz="UTC").to_pydatetime(),
            "close_time":             pd.Timestamp(record["close_time"], tz="UTC").to_pydatetime(),
            "open_price":             float(record["open_price"]),
            "high_price":             float(record["high_price"]),
            "low_price":              float(record["low_price"]),
            "close_price":            float(record["close_price"]),
            "volume":                 float(record["volume"]),
            "quote_volume":           float(record["quote_volume"]),
            "trade_count":            int(record["trade_count"]),
            "taker_buy_volume":       float(record["taker_buy_volume"]),
            "taker_buy_quote_volume": float(record["taker_buy_quote_volume"]),
            "ingested_at":            datetime.now(timezone.utc),
        }
    except (KeyError, ValueError, TypeError):
        return None


def _transform_ticker(record: dict):
    try:
        return {
            "symbol":             str(record["symbol"]),
            "event_time":         pd.Timestamp(record["event_time"], tz="UTC").to_pydatetime(),
            "price_change":       float(record["price_change"]),
            "price_change_pct":   float(record["price_change_pct"]),
            "weighted_avg_price": float(record["weighted_avg_price"]),
            "open_price":         float(record["open_price"]),
            "high_price":         float(record["high_price"]),
            "low_price":          float(record["low_price"]),
            "volume_24h":         float(record["volume_24h"]),
            "quote_volume_24h":   float(record["quote_volume_24h"]),
            "trade_count_24h":    int(record["trade_count_24h"]),
            "ingested_at":        datetime.now(timezone.utc),
        }
    except (KeyError, ValueError, TypeError):
        return None


def _transform_orderbook(record: dict):
    try:
        bid = float(record["best_bid_price"])
        ask = float(record["best_ask_price"])
        spread = ask - bid
        mid = (bid + ask) / 2
        spread_pct = (spread / mid * 100) if mid > 0 else 0.0
        return {
            "symbol":         str(record["symbol"]),
            "event_time":     pd.Timestamp(record["event_time"], tz="UTC").to_pydatetime(),
            "best_bid_price": bid,
            "best_bid_qty":   float(record["best_bid_qty"]),
            "best_ask_price": ask,
            "best_ask_qty":   float(record["best_ask_qty"]),
            "spread":         spread,
            "spread_pct":     spread_pct,
            "ingested_at":    datetime.now(timezone.utc),
        }
    except (KeyError, ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP} ...", flush=True)

    threads = [
        threading.Thread(target=_run_trade_consumer, name="trade", daemon=True),
        threading.Thread(
            target=_run_bronze_consumer,
            name="klines",
            daemon=True,
            kwargs=dict(
                topic="btc-klines",
                group_id="btc-processor-klines",
                directory=BRONZE_KLINES_DIR,
                schema=KLINE_SCHEMA,
                transform_fn=_transform_kline,
                flush_sec=BRONZE_FLUSH_SEC,
                label="bronze/klines",
            ),
        ),
        threading.Thread(
            target=_run_bronze_consumer,
            name="ticker",
            daemon=True,
            kwargs=dict(
                topic="btc-ticker",
                group_id="btc-processor-ticker",
                directory=BRONZE_TICKER_DIR,
                schema=TICKER_SCHEMA,
                transform_fn=_transform_ticker,
                flush_sec=SILVER_FLUSH_SEC,  # lower frequency stream → match silver interval
                label="bronze/ticker",
            ),
        ),
        threading.Thread(
            target=_run_bronze_consumer,
            name="orderbook",
            daemon=True,
            kwargs=dict(
                topic="btc-orderbook",
                group_id="btc-processor-orderbook",
                directory=BRONZE_ORDERBOOK_DIR,
                schema=ORDERBOOK_SCHEMA,
                transform_fn=_transform_orderbook,
                flush_sec=BRONZE_FLUSH_SEC,
                label="bronze/orderbook",
            ),
        ),
    ]

    for t in threads:
        t.start()
    print(f"Started {len(threads)} consumer threads.", flush=True)

    def _shutdown(signum, frame):
        print("Shutdown signal received — stopping consumers ...", flush=True)
        _stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    try:
        while not _stop.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        _stop.set()

    for t in threads:
        t.join()
    print("All consumers stopped.", flush=True)


if __name__ == "__main__":
    main()
