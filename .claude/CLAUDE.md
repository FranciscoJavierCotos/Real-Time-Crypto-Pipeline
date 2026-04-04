# Real-Time Crypto Pipeline

## Project Overview

End-to-end streaming data pipeline that ingests live Bitcoin trade data from Binance, processes it with Apache Spark, and stores it in Databricks Delta Lake with a Bronze/Silver lakehouse architecture.

**Goal:** Portfolio project demonstrating real-time data engineering skills.

---

## Architecture

```
Binance WebSocket
      |
      v
Python Producer (Docker)
      |
      v
Apache Kafka - topic: btc-trades (KRaft mode, Docker)
      |
      v
PySpark Structured Streaming (local)
      |
      v
Delta Lake (Bronze -> Silver)
      |
      v
Databricks (storage + querying)
      |
      v
[Optional] Data Quality + Dashboard
```

---

## Tech Stack

| Layer      | Technology                         |
| ---------- | ---------------------------------- |
| Source     | Binance WebSocket API              |
| Messaging  | Apache Kafka (KRaft, no Zookeeper) |
| Processing | PySpark Structured Streaming       |
| Storage    | Databricks Delta Lake              |
| Infra      | Docker Compose                     |

---

## Current Status

**Active phase: Phase 3 — Spark Structured Streaming**

| Phase | Name                       | Status      |
| ----- | -------------------------- | ----------- |
| 1     | Local Infrastructure       | Complete    |
| 2     | WebSocket → Kafka Producer | Complete    |
| 3     | Spark Structured Streaming | Not started |
| 4     | Databricks Integration     | Not started |
| 5     | Data Quality               | Not started |

---

## Implementation Phases

### Phase 1 — Local Infrastructure (Docker)

**Goal:** Kafka running locally and accessible.

**Services in `docker-compose.yml`:**

- Kafka (KRaft mode — no Zookeeper, 1 broker is enough)
- Kafka UI (optional, recommended for visibility)

**Deliverable:**

- [ ] Kafka running via Docker Compose
- [ ] Topic created: `kafka-topics --create --topic btc-trades ...`
- [ ] Message visible in Kafka UI

---

### Phase 2 — WebSocket → Kafka Producer

**Goal:** Stream real-time Binance trades into Kafka.

**Step 2.1 — Connect to Binance WebSocket**

Endpoint: `wss://stream.binance.com:9443/ws/btcusdt@trade`

Raw message example:

```json
{
  "e": "trade",
  "s": "BTCUSDT",
  "p": "43250.12",
  "q": "0.001",
  "T": 1710000000000
}
```

**Step 2.2 — Transform to clean schema**

```json
{
  "symbol": "BTCUSDT",
  "price": 43250.12,
  "volume": 0.001,
  "event_time": "<ISO timestamp>"
}
```

**Step 2.3 — Publish to Kafka**

- Topic: `btc-trades`
- Format: JSON (no Avro)

**Deliverable:**

- [ ] Producer script running in Docker
- [ ] Real-time data flowing into `btc-trades` topic
- [ ] Trades visible in Kafka UI

---

## Phase 3 — Spark Structured Streaming

**Goal:** Read from Kafka, transform, and write Bronze + Silver Delta tables locally.

**Design decision:** Spark runs entirely locally. Delta files are written to a local folder structure. A local monitoring notebook gives live visibility into the stream. Databricks CE is used separately for ad-hoc analysis by uploading snapshots of the Delta files to DBFS.

---

### Step 3.1 — Session setup

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("btc-streaming-pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()
```

---

### Step 3.2 — Read from Kafka

```python
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "btc-trades") \
    .option("startingOffsets", "latest") \
    .load()
```

---

### Step 3.3 — Parse JSON

Extract: `symbol`, `price`, `volume`, `event_time`

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("event_time", TimestampType())

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
```

---

### Step 3.4 — Bronze layer

Raw ingestion, no transformation, append-only.

```python
bronze_query = parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "./delta/checkpoints/bronze") \
    .trigger(processingTime="10 seconds") \
    .start("./delta/bronze/btc_trades")
```

---

### Step 3.5 — Silver layer

Type validation, filtering, ingestion timestamp, 1-minute windowed aggregations.

```python
from pyspark.sql.functions import window, avg, sum, current_timestamp

silver_input = spark.readStream \
    .format("delta") \
    .load("./delta/bronze/btc_trades")

silver = silver_input \
    .filter(col("price") > 0) \
    .withColumn("ingested_at", current_timestamp()) \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        sum("volume").alias("total_volume")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("avg_price"),
        col("total_volume")
    )

silver_query = silver.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "./delta/checkpoints/silver") \
    .trigger(processingTime="30 seconds") \
    .start("./delta/silver/btc_aggregates")
```

---

### Step 3.6 — Keep both streams running

```python
bronze_query.awaitTermination()
silver_query.awaitTermination()
```

---

### Step 3.7 — Local monitoring notebook

Run this in a **separate local Jupyter notebook** alongside the streaming job to observe data landing in real time. Re-run each cell to refresh.

```python
# Cell 1 — Spark session for monitoring (read-only)
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("monitor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

```python
# Cell 2 — Latest bronze rows
spark.read.format("delta").load("./delta/bronze/btc_trades") \
    .orderBy(col("event_time").desc()) \
    .limit(20) \
    .show(truncate=False)
```

```python
# Cell 3 — Latest silver aggregations
spark.read.format("delta").load("./delta/silver/btc_aggregates") \
    .orderBy(col("window_start").desc()) \
    .limit(10) \
    .show(truncate=False)
```

```python
# Cell 4 — Delta table history (audit log)
DeltaTable.forPath(spark, "./delta/bronze/btc_trades") \
    .history() \
    .select("version", "timestamp", "operation", "operationMetrics") \
    .show(truncate=False)
```

```python
# Cell 5 — Row count over time (how fast is data landing?)
spark.read.format("delta").load("./delta/bronze/btc_trades") \
    .groupBy("symbol") \
    .count() \
    .show()
```

---

**Deliverables:**

- [ ] Spark streaming job running continuously (`streaming_job.py`)
- [ ] Bronze Delta table written to `./delta/bronze/btc_trades`
- [ ] Silver Delta table with aggregations written to `./delta/silver/btc_aggregates`
- [ ] Local monitoring notebook working and refreshing live data

---

## Phase 4 — Databricks CE Integration (Ad-hoc Analysis)

**Goal:** Upload local Delta snapshots to Databricks Community Edition DBFS and query them via notebook for ad-hoc exploration.

**Design decision:** Databricks CE has no always-on jobs or Unity Catalog. It is used purely as an interactive query layer. Data is uploaded manually or via a script when you want to analyse a snapshot. This is intentionally decoupled from the live stream.

---

### Step 4.1 — Upload Delta snapshot to DBFS

Run this locally whenever you want to push a fresh snapshot to Databricks.

```python
# upload_to_dbfs.py
import requests
import base64
import os

DATABRICKS_HOST = "https://community.cloud.databricks.com"
TOKEN = "your-pat-token"  # Settings > Developer > Access Tokens in Databricks CE

def upload_file(local_path: str, dbfs_path: str):
    with open(local_path, "rb") as f:
        contents = base64.b64encode(f.read()).decode("utf-8")
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/dbfs/put",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={"path": dbfs_path, "contents": contents, "overwrite": True}
    )
    resp.raise_for_status()
    print(f"Uploaded {local_path} → {dbfs_path}")

def sync_delta_table(local_dir: str, dbfs_dir: str):
    for root, dirs, files in os.walk(local_dir):
        # include Delta log and parquet files
        for fname in files:
            if fname.endswith(".parquet") or "_delta_log" in root:
                local_path = os.path.join(root, fname)
                relative = os.path.relpath(local_path, local_dir)
                dbfs_path = f"{dbfs_dir}/{relative}".replace("\\", "/")
                upload_file(local_path, dbfs_path)

# Upload both tables
sync_delta_table("./delta/bronze/btc_trades",      "/FileStore/delta/bronze/btc_trades")
sync_delta_table("./delta/silver/btc_aggregates",  "/FileStore/delta/silver/btc_aggregates")
```

Run it:

```bash
python upload_to_dbfs.py
```

---

### Step 4.2 — Query in Databricks CE notebook

Create a new notebook in Databricks CE and paste the cells below. Attach it to your cluster before running.

```python
# Cell 1 — Register tables so you can use SQL
spark.read.format("delta") \
    .load("dbfs:/FileStore/delta/bronze/btc_trades") \
    .createOrReplaceTempView("bronze_btc_trades")

spark.read.format("delta") \
    .load("dbfs:/FileStore/delta/silver/btc_aggregates") \
    .createOrReplaceTempView("silver_btc_aggregates")

print("Tables registered.")
```

```sql
-- Cell 2 — Browse raw trades
SELECT * FROM bronze_btc_trades
ORDER BY event_time DESC
LIMIT 50
```

```sql
-- Cell 3 — Browse 1-minute aggregations
SELECT * FROM silver_btc_aggregates
ORDER BY window_start DESC
LIMIT 20
```

```sql
-- Cell 4 — Price trend over time
SELECT
    window_start,
    symbol,
    ROUND(avg_price, 2) AS avg_price,
    ROUND(total_volume, 6) AS total_volume
FROM silver_btc_aggregates
ORDER BY window_start
```

```sql
-- Cell 5 — Summary stats
SELECT
    symbol,
    COUNT(*) AS windows,
    ROUND(MIN(avg_price), 2) AS min_price,
    ROUND(MAX(avg_price), 2) AS max_price,
    ROUND(AVG(avg_price), 2) AS overall_avg,
    ROUND(SUM(total_volume), 4) AS total_volume
FROM silver_btc_aggregates
GROUP BY symbol
```

---

### Step 4.3 — Re-uploading for a fresher snapshot

There is no live sync — each time you want updated data in Databricks CE, re-run the upload script:

```bash
python upload_to_dbfs.py
```

Then re-run the notebook cells. The Delta transaction log ensures Databricks sees the latest version of the table correctly.

---

**Deliverables:**

- [ ] `upload_to_dbfs.py` script working
- [ ] Bronze and Silver tables visible in DBFS at `/FileStore/delta/`
- [ ] Databricks CE notebook querying both tables successfully
- [ ] Query working: `SELECT * FROM silver_btc_aggregates`

### Phase 5 — Data Quality

**Goal:** Basic validation layer to catch bad data.

**Checks (Great Expectations or simple assertions):**

- `price > 0`
- `volume > 0`
- No null `event_time`

**Deliverable:**

- [ ] Validation runs as part of the pipeline
- [ ] Failures are logged (log file or print is fine)

---

## Project File Structure (target)

```
Real-Time Crypto Pipeline/
├── docker-compose.yml          # Kafka + Kafka UI
├── producer/
│   ├── Dockerfile
│   ├── producer.py             # Binance WS -> Kafka
│   └── requirements.txt
├── spark/
│   ├── streaming_job.py        # Kafka -> Bronze -> Silver
│   └── requirements.txt
├── databricks/
│   └── query_notebook.py       # Exploration queries
├── quality/
│   └── checks.py               # Data quality rules
└── .claude
│   ├── CLAUDE.md
│   └── skills
```
