# Real-Time Crypto Pipeline

A production-style data engineering project that ingests live Bitcoin market data from Binance, processes it through a medallion architecture, and delivers analyst-ready Gold tables in Databricks — entirely orchestrated with Airflow and modelled with dbt.

---

## The Problem

Financial markets generate continuous, high-velocity data across multiple signals: individual trades, OHLC candles, order-book depth, and 24-hour rolling statistics. The challenge is not just capturing that data — it is turning four independent, noisy streams into a single coherent view that can answer a question like:

> _"Is the current BTC minute bullish or bearish, is volume spiking, and is the order book supporting the move?"_

That requires streaming ingestion, windowed aggregation, cloud storage, and multi-layer SQL modelling working together reliably every five minutes.

---

## Architecture

```
Binance WebSocket (4 combined streams)
  │
  │  wss://stream.binance.com  ──  one connection, four sub-streams
  │
  ▼
Python Producer  (Docker)
  │  ∙ deserialises each stream message
  │  ∙ samples ticker (~1 msg/15 s) and orderbook (~1 msg/5 s) to control volume
  │  ∙ publishes only closed kline candles  (k["x"] == True)
  │
  ▼
Apache Kafka  (KRaft, no ZooKeeper)
  ├── btc-trades     (3 partitions)   — every individual trade
  ├── btc-klines     (3 partitions)   — closed 1-min OHLC candles
  ├── btc-ticker     (1 partition)    — 24-hour rolling stats
  └── btc-orderbook  (3 partitions)   — best bid/ask snapshots
  │
  ▼
Python Processor  (Docker, 4 daemon threads)
  ├── Bronze layer  — raw records flushed to Parquet (Snappy) every 10 s
  └── Silver layer  — 1-min windowed trade aggregations, flushed every 30 s
                      (late-arrival window closed 60 s after window end)
  │
  │  ./data/  (volume-mounted, shared with Airflow)
  │
  ▼
Apache Airflow DAG  (every 5 minutes, max_active_runs=1)
  ├── push_*     — reads new Bronze Parquet since last cursor, uploads to UC Volume
  ├── copy_*     — idempotent COPY INTO landing Delta tables (Databricks skips duplicates)
  │
  ├── run_dbt_silver  ──  silver_btc_aggregates  (incremental merge)
  └── run_dbt_gold    ──  gold_ohlcv_enriched
                          gold_liquidity_1min
                          gold_market_pulse      ← master join: all four signals
```

**Task dependency graph (DAG)**

```
push_trades    → copy_trades    → run_dbt_silver
push_klines    → copy_klines    ↘
push_ticker    → copy_ticker    → run_dbt_gold
push_orderbook → copy_orderbook ↗
```

Trades trigger the Silver dbt model independently. Klines, ticker, and orderbook run in parallel and converge on the Gold models — only when at least one of the three has new data.

---

## Tech Stack

| Layer            | Tool                                         | Why                                                                                     |
| ---------------- | -------------------------------------------- | --------------------------------------------------------------------------------------- |
| Ingestion        | Python + `websocket-client`                  | Lightweight; Binance combined-stream API avoids N separate connections                  |
| Message bus      | Apache Kafka (KRaft 7.6)                     | Durable, partitioned, replay-capable; KRaft removes ZooKeeper operational overhead      |
| Processing       | Python + `kafka-python`, `pandas`, `pyarrow` | No Spark needed at this scale; pure-Python threading keeps the processor container tiny |
| Storage format   | Parquet + Snappy                             | Columnar, compressed, schema-enforced; compatible with Databricks COPY INTO             |
| Orchestration    | Apache Airflow 2.9 (TaskFlow API)            | Declarative task graph, cursor-based incremental loads, skip-if-no-data logic           |
| Cloud lakehouse  | Databricks Unity Catalog + Delta Lake        | ACID tables, COPY INTO idempotency, Unity Catalog governance                            |
| Transformation   | dbt-databricks 1.8                           | SQL-first, incremental merge strategy, testable, version-controlled                     |
| Containerisation | Docker Compose                               | One command spins up the full local stack                                               |

---

## Data Model: From Raw to Insight

### Bronze — Raw landing zone

Four directories of time-stamped Parquet files, one per stream, with a strict PyArrow schema enforced at write time. Each file carries an `ingested_at` column — the cursor Airflow uses to detect new records without scanning file names or tracking offsets.

### Silver — Cleaned aggregations

`silver_btc_aggregates`: 1-minute windowed average price and total volume derived from the raw trades stream. The processor accumulates running sums in memory and only writes a window to disk once it is at least 60 seconds past its end, giving late-arriving trades time to land.

### Gold — Analyst-ready tables

| Model                 | Description                                                                                                                                                                                                       |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gold_ohlcv_enriched` | Kline candles enriched with `candle_direction` (bullish/bearish), `price_range_pct` (intra-minute volatility), and `taker_buy_ratio` (fraction of volume that was aggressive buying)                              |
| `gold_liquidity_1min` | Order-book snapshots aggregated to 1-minute windows: avg/max spread (USD and %), and `bid_ask_imbalance` — a [-1, +1] metric where +1 is pure buy pressure                                                        |
| `gold_market_pulse`   | Master join of all four signals per 1-minute window. Adds `volume_spike` (True when trade volume exceeds 2× the 30-candle rolling average) and a composite `momentum_signal` (strong_buy → neutral → strong_sell) |

**`momentum_signal` logic:**

```sql
CASE
    WHEN taker_buy_ratio >= 0.65 AND candle_direction = 'bullish' THEN 'strong_buy'
    WHEN taker_buy_ratio >= 0.55 AND candle_direction = 'bullish' THEN 'moderate_buy'
    WHEN taker_buy_ratio <= 0.35 AND candle_direction = 'bearish' THEN 'strong_sell'
    WHEN taker_buy_ratio <= 0.45 AND candle_direction = 'bearish' THEN 'moderate_sell'
    ELSE 'neutral'
END
```

A candle alone can be misleading — price can close up on low volume while sellers are dominant at the order book. Combining candle direction with taker buy ratio and liquidity gives a more complete picture.

---

## Engineering Decisions Worth Noting

**KRaft instead of ZooKeeper.**
Confluent Platform 7.6 ships KRaft stable. Removing ZooKeeper cuts the compose file from six to five services and eliminates a whole class of coordination failure modes. For a single-broker development cluster this is the right call.

**No Spark in the processor.**
Spark would be the default answer for streaming ETL, but it introduces a multi-GB JVM, a cluster manager, and significant operational overhead. At one-symbol, sub-100 msg/s throughput, four Python daemon threads with `pandas` and `pyarrow` accomplish the same Bronze/Silver logic with a fraction of the resource cost. The architecture is explicit about this trade-off — Spark would be the upgrade path when scaling to hundreds of symbols.

**Cursor-based incremental state, not Kafka offset management.**
The Airflow DAG owns the Bronze-to-cloud transfer. Rather than committing Kafka consumer group offsets and reconstructing history, each table tracks the maximum `ingested_at` it has successfully pushed. This survives container restarts, DAG reruns, and Airflow metadata resets — the cursor is just a JSON file on a shared volume.

**COPY INTO idempotency.**
Databricks COPY INTO tracks loaded file paths in the Delta log. If Airflow retries a task, the same Parquet file will be uploaded to the UC volume and COPY INTO will silently skip it. No deduplication logic needed in the DAG.

**Incremental dbt with late-arrival lookback.**
All Gold models use `incremental_strategy='merge'`. The incremental filter re-processes the last 5 minutes relative to the current table max, so a late-arriving kline does not silently corrupt an already-written window.

---

## Running It Locally

**Prerequisites:** Docker Desktop, a Databricks workspace with a SQL warehouse, and a `.env` file:

```
DATABRICKS_HOST=<your-workspace-url>
DATABRICKS_TOKEN=<personal-access-token>
DATABRICKS_WAREHOUSE_ID=<sql-warehouse-id>
```

```bash
# Start all services
docker compose up -d

# Watch the processor writing Parquet
docker logs -f btc-processor

# Kafka UI
open http://localhost:8081

# Airflow UI  (admin / admin on first run)
open http://localhost:8080
```

The pipeline auto-starts. Within 10 seconds the processor writes the first Bronze files. Within 5 minutes Airflow pushes them to Databricks and dbt runs the full model graph.

---

## Repository Structure

```
├── producer/
│   ├── producer.py          # Binance WS → 4 Kafka topics
│   ├── Dockerfile
│   └── requirements.txt
├── processor/
│   ├── consumer.py          # 4 threaded consumers → Bronze/Silver Parquet
│   ├── Dockerfile
│   └── requirements.txt
├── airflow/
│   └── dags/
│       └── silver_to_databricks.py   # Airflow DAG: Bronze → UC → dbt
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── silver/
│       │   └── silver_btc_aggregates.sql
│       └── gold/
│           ├── gold_ohlcv_enriched.sql
│           ├── gold_liquidity_1min.sql
│           └── gold_market_pulse.sql
└── docker-compose.yml
```

---

## Status

| Phase | Description                              | Status   |
| ----- | ---------------------------------------- | -------- |
| 1     | Docker / Kafka (KRaft)                   | Complete |
| 2     | Binance Producer                         | Complete |
| 3     | Python Processor — Bronze / Silver       | Complete |
| 4     | Airflow + Databricks Unity Catalog + dbt | Complete |
| 5     | Data Quality / Observability             | Planned  |

**Phase 5 scope:** Formal validation assertions (`price > 0`, `volume > 0`, no null `event_time`) on Bronze files before upload, with failures written to a dedicated log stream. Likely implemented as lightweight Python checks in the Airflow tasks, not a full Great Expectations suite.
