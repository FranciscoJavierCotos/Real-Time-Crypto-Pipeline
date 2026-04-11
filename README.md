# Real-Time Crypto Market Intelligence Pipeline

A production-style data engineering project that turns four live Binance WebSocket streams into a single, trustworthy, analyst-ready view of the Bitcoin market — updated every two minutes, validated at every stage, and fully orchestrated in the cloud.

---

## The Business Problem

Crypto markets move in seconds. Traders, risk managers, and quant researchers who rely on stale or unvalidated data face two risks: **missing a move** because their pipeline is too slow, or **acting on bad data** because there was no quality gate.

Most dashboards give you one signal — price. But price alone is misleading. A bullish candle on thin volume while order-book sellers dominate tells a completely different story than the same candle backed by aggressive buyers.

This pipeline answers a question that actually matters:

> _"Right now, is BTC's move real — or is it noise? Is volume supporting the direction? Is the order book aligned?"_

It does this by fusing four independent market signals, applying a three-tier data quality framework, and delivering a single `gold_market_pulse` table per minute that a dashboard, alerting system, or ML model can consume directly.

---

## What Gets Delivered

| Gold Table | Business Question Answered |
|---|---|
| `gold_ohlcv_enriched` | Is BTC trending bullish or bearish this minute? How volatile? Are buyers or sellers aggressive? |
| `gold_liquidity_1min` | Is the market liquid? Is spread widening (stress indicator)? Is order-book pressure directional? |
| `gold_market_pulse` | Master signal: is the move real? Volume spike + momentum signal + liquidity context in one row per minute |

**`momentum_signal`** — the composite output driving downstream decisions:

```sql
CASE
    WHEN taker_buy_ratio >= 0.65 AND candle_direction = 'bullish' THEN 'strong_buy'
    WHEN taker_buy_ratio >= 0.55 AND candle_direction = 'bullish' THEN 'moderate_buy'
    WHEN taker_buy_ratio <= 0.35 AND candle_direction = 'bearish' THEN 'strong_sell'
    WHEN taker_buy_ratio <= 0.45 AND candle_direction = 'bearish' THEN 'moderate_sell'
    ELSE 'neutral'
END
```

`volume_spike` flags any minute where trade volume exceeds 2× the 30-minute rolling average — the earliest quantitative signal that something significant is happening.

---

## Architecture

```
Binance WebSocket (4 combined streams)
  │
  │  One connection  ──  four sub-streams, sampled to control volume
  │
  ▼
Python Producer  (Docker)
  │  · Deserialises each stream type with a dedicated handler
  │  · Publishes only closed kline candles  (k["x"] == True)
  │
  ▼
Apache Kafka  (KRaft — no ZooKeeper)
  ├── btc-trades     (3 partitions)   — every individual trade
  ├── btc-klines     (3 partitions)   — closed 1-min OHLC candles
  ├── btc-ticker     (1 partition)    — 24-hour rolling statistics
  └── btc-orderbook  (3 partitions)   — best bid/ask snapshots
  │
  ▼
Python Processor  (Docker, 4 daemon threads)
  ├── Bronze — raw records flushed to Parquet every 10 s
  └── Silver — 1-min windowed trade aggregations, flushed every 30 s
               (late-arrival tolerance: 60 s past window end)
  │
  │  ./data/  (Docker volume, shared with Airflow)
  │
  ▼
Apache Airflow DAG  (every 2 minutes, max_active_runs=1)
  │
  ├── Tier-2: Great Expectations validation on Bronze before upload
  │     · Critical failures  →  blocks upload (data never reaches cloud)
  │     · Soft warnings      →  logs alert, pipeline continues
  │
  ├── push_*     — incremental upload to Databricks Unity Catalog Volume
  ├── copy_*     — idempotent COPY INTO landing Delta tables
  │
  ├── run_dbt_silver  →  test_dbt_silver
  └── run_dbt_gold    →  test_dbt_gold   (Tier-3: dbt tests after every run)
```

**Full task dependency graph**

```
validate_trades    → push_trades    → copy_trades    → run_dbt_silver → test_dbt_silver
validate_klines    → push_klines    → copy_klines    ↘
validate_ticker    → push_ticker    → copy_ticker    → run_dbt_gold → test_dbt_gold
validate_orderbook → push_orderbook → copy_orderbook ↗
```

Trades trigger the Silver model independently. The three enrichment branches run in parallel and converge on Gold — which only runs when at least one source has new data.

---

## Data Quality: Three Tiers of Trust

Data quality is not a checkbox — it determines whether downstream consumers can trust the output. This pipeline enforces quality at three independent layers:

| Tier | Where | What | Failure Behaviour |
|---|---|---|---|
| **1 — Schema enforcement** | Python Processor | PyArrow schema enforced at Parquet write time | Hard error; bad records never hit disk |
| **2 — Business rule validation** | Airflow (pre-upload) | Great Expectations on Bronze files: null PKs, schema drift, price/volume ranges | Critical → blocks upload; Warning → logs, continues |
| **3 — Model integrity tests** | dbt (post-run) | Schema tests + singular SQL assertions on Silver and Gold | Fails Airflow task; stored in `dbt_test_failures` schema for inspection |

**dbt tests cover:**
- `not_null` on every primary key column
- `accepted_values` on `candle_direction` and `momentum_signal`
- `column_positive` on prices, volumes, and trade counts (custom macro)
- `column_between` on derived ratios, spreads, and volatility metrics (custom macro)
- `assert_ohlcv_high_gte_low` — physically impossible candles caught immediately
- `assert_ohlcv_close_within_hl` — close price must sit between high and low
- `assert_silver_aggregates_unique_key` — no duplicate `(window_start, symbol)` rows

Test failures are persisted to the `dbt_test_failures` Unity Catalog schema so analysts can inspect which rows failed, not just that something failed.

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python + `websocket-client` | Lightweight; Binance combined-stream API → one connection for four streams |
| Message bus | Apache Kafka (KRaft 7.6) | Durable, partitioned, replay-capable; KRaft removes ZooKeeper operational overhead |
| Processing | Python + `kafka-python`, `pandas`, `pyarrow` | Right-sized for one symbol; pure-Python threading keeps the container tiny |
| Storage format | Parquet + Snappy | Columnar, compressed, schema-enforced; compatible with Databricks COPY INTO |
| Data quality | Great Expectations + custom dbt macros | Two-tier quality gate: Bronze validation before upload, model tests after run |
| Orchestration | Apache Airflow 2.9 (TaskFlow API) | Declarative task graph; cursor-based incremental state; skip-if-no-data logic |
| Cloud lakehouse | Databricks Unity Catalog + Delta Lake | ACID tables, COPY INTO idempotency, centralised governance |
| Transformation | dbt-databricks 1.8 | SQL-first, incremental merge, testable, version-controlled |
| Containerisation | Docker Compose | One command spins up the full local stack |

---

## Medallion Architecture

### Bronze — Raw landing zone

Four directories of time-stamped Parquet files, one per stream. Each record carries an `ingested_at` timestamp — the cursor Airflow uses to detect new data without file-name parsing or Kafka offset management. PyArrow enforces the schema at write time; malformed records never reach disk.

### Silver — Cleaned aggregations

`silver_btc_aggregates`: 1-minute windowed average price and total volume derived from individual trades. The processor accumulates running sums in memory and only closes a window 60 seconds after it ends — giving late-arriving trades time to land before the window is written to disk.

### Gold — Analyst-ready, business-question-driven tables

Gold tables are not just views — they are engineered answers to specific business questions, merged incrementally on Databricks with a 5-minute lookback to handle late arrivals. All derived signals (`candle_direction`, `taker_buy_ratio`, `bid_ask_imbalance`, `momentum_signal`, `volume_spike`) are computed in SQL and tested on every run.

---

## Key Engineering Decisions

**No ZooKeeper (KRaft).**
Confluent Platform 7.6 ships KRaft stable. Removing ZooKeeper cuts service count and eliminates a whole class of coordination failure modes — the right call for a single-broker cluster.

**No Spark in the processor.**
Spark is the default for streaming ETL but introduces a multi-GB JVM and significant operational overhead. At one-symbol, sub-100 msg/s throughput, four Python daemon threads with `pandas` and `pyarrow` accomplish the same Bronze/Silver logic at a fraction of the resource cost. The architecture is explicit about this trade-off — Spark is the natural upgrade path when scaling to hundreds of symbols.

**Cursor-based incremental state, not Kafka offset management.**
Each table tracks its own `ingested_at` cursor in a shared JSON file. This survives container restarts, DAG reruns, and Airflow metadata resets — without coupling the pipeline's recovery logic to Kafka consumer group semantics.

**COPY INTO idempotency.**
Databricks COPY INTO tracks loaded file paths in the Delta log. If Airflow retries a task, the same Parquet file is re-uploaded and silently skipped. No deduplication logic is needed in the DAG.

**Quality gates block the pipeline.**
The validate tasks run *before* upload, not after. A null primary key or schema drift is caught on the local machine and blocks data from reaching Databricks — rather than poisoning Unity Catalog tables that downstream consumers are already querying.

---

## Running It Locally

**Prerequisites:** Docker Desktop, a Databricks workspace with a SQL warehouse, and a `.env` file:

```
DATABRICKS_HOST=<your-workspace-url>
DATABRICKS_TOKEN=<personal-access-token>
DATABRICKS_WAREHOUSE_ID=<sql-warehouse-id>
```

```bash
# Start the full stack
docker compose up -d

# Watch the processor writing Bronze/Silver Parquet
docker logs -f btc-processor

# Kafka topic browser
open http://localhost:8081

# Airflow UI  (admin / admin)
open http://localhost:8080
```

Within 10 seconds the processor writes its first Bronze files. Within 2 minutes Airflow validates, uploads, and runs the full dbt model graph. `gold_market_pulse` is queryable in Databricks from the first successful DAG run.

---

## Repository Structure

```
├── producer/
│   ├── producer.py               # Binance WS → 4 Kafka topics
│   ├── Dockerfile
│   └── requirements.txt
├── processor/
│   ├── consumer.py               # 4 threaded consumers → Bronze/Silver Parquet
│   ├── Dockerfile
│   └── requirements.txt
├── airflow/
│   └── dags/
│       ├── silver_to_databricks.py   # DAG: validate → upload → COPY INTO → dbt
│       └── validation_helpers.py     # Great Expectations Bronze validator
├── dbt/
│   ├── dbt_project.yml               # store_failures=true → dbt_test_failures schema
│   ├── profiles.yml
│   ├── macros/
│   │   ├── test_column_positive.sql  # custom: value > 0
│   │   └── test_column_between.sql   # custom: value in [min, max]
│   ├── tests/
│   │   ├── assert_ohlcv_high_gte_low.sql
│   │   ├── assert_ohlcv_close_within_hl.sql
│   │   └── assert_silver_aggregates_unique_key.sql
│   └── models/
│       ├── sources.yml
│       ├── silver/
│       │   ├── silver_btc_aggregates.sql
│       │   └── schema.yml
│       └── gold/
│           ├── gold_ohlcv_enriched.sql
│           ├── gold_liquidity_1min.sql
│           ├── gold_market_pulse.sql
│           └── schema.yml
└── docker-compose.yml
```

---

## Project Status

| Phase | Description | Status |
|---|---|---|
| 1 | Docker / Kafka (KRaft, no ZooKeeper) | Complete |
| 2 | Binance WebSocket Producer | Complete |
| 3 | Python Processor — Bronze / Silver Parquet | Complete |
| 4 | Airflow + Databricks Unity Catalog + dbt Gold | Complete |
| 5 | Three-tier data quality framework | Complete |
