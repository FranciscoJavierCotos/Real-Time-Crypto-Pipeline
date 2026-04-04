create a kafka skill.md file for a real-time data pipeline project. not too big, add non negotiables good practices

18:02

Viewed a file, created a file, read a file
Viewed a file, created a file, read a file
The skill covers the non-negotiables that actually bite teams in production:

The 5 hard rules baked in:

No auto-topic creation — prevents silent misconfiguration in prod
Schema Registry + Avro/Protobuf — kills schema drift before it kills your pipeline
acks=all + idempotence — exactly-once delivery, no silent data loss
Commit after processing, not before — preserves at-least-once guarantees
DLQ on every consumer — poison pill messages don't infinite-loop or silently drop
Also includes topic naming conventions, partition math, consumer group isolation rules, the exact metrics to alert on, and a deploy checklist. Kept it focused — no fluff about Kafka history or features you'd never use in a pipeline context.

Kafka skill
Document · MD
now one spark.md for general good practices with apache spark.

**Goal:** Read from Kafka and write to Delta Lake.
18:10

Viewed a file, created a file, read a file
Viewed a file, created a file, read a file
Scoped entirely to the Kafka → Delta Lake goal. The 5 non-negotiables are:

readStream not read — with maxOffsetsPerTrigger for backpressure and failOnDataLoss=true
Deserialize Kafka bytes immediately — keep partition + offset for lineage
foreachBatch over raw writeStream — gives you upserts, dedup, and multi-table control
One unique checkpoint per query, always versioned — shared or missing checkpoints are silent data corruption
Never collect()/count() inside a stream — driver OOM waiting to happen
Also included Delta table hygiene (OPTIMIZE + VACUUM are mandatory, not optional), quarantine routing for bad records instead of dropping them, and the exact streaming metrics worth alerting on. Matches the style and density of the Kafka skill so both work together as a set.

Spark skill
Document · MD

Claude is AI and can make mistakes. Please double-check responses.
Spark skill · MD
Copy

---

name: spark
description: "Use this skill for any Apache Spark work — batch or streaming — especially pipelines reading from Kafka and writing to Delta Lake. Triggers: PySpark jobs, Spark Structured Streaming, DataFrame transformations, partitioning strategy, Delta Lake writes, shuffle tuning, or any mention of 'Spark', 'SparkSession', 'structured streaming', or 'Delta'. Do NOT use for non-Spark compute engines (Flink, Beam, dbt) or pure SQL warehouse queries."
goal: "Read from Kafka, transform, write to Delta Lake with production-grade reliability."

---

# Spark — Real-Time Pipeline (Kafka → Delta Lake)

## Non-Negotiables (Always Apply These)

### 1. Never Use `spark.read` for Streaming — Use `readStream`

```python
# BAD — reads a snapshot, misses new Kafka messages
df = spark.read.format("kafka").load()

# GOOD — continuous micro-batch stream
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events.user-actions")
    .option("startingOffsets", "earliest")   # never 'latest' in prod
    .option("maxOffsetsPerTrigger", 50000)   # backpressure — always set this
    .option("failOnDataLoss", "true")        # fail loud, don't silently skip
    .load()
)
```

### 2. Always Deserialize Kafka Bytes Immediately — Never Pass Them Down

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType

event_schema = StructType() \
    .add("user_id", StringType()) \
    .add("action", StringType()) \
    .add("ts", LongType())

parsed = (
    df.select(
        col("key").cast("string").alias("kafka_key"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
        from_json(col("value").cast("string"), event_schema).alias("data")
    )
    .select("kafka_key", "partition", "offset", "kafka_ts", "data.*")
)
# Always keep partition + offset for lineage and replay debugging
```

### 3. Write to Delta Lake with `foreachBatch` — Not `writeStream` Directly

`writeStream` to Delta is fine for simple appends, but `foreachBatch` gives you upserts, multi-table writes, and per-batch control.

```python
def process_batch(batch_df, batch_id):
    # Deduplicate within the batch before writing
    deduped = batch_df.dropDuplicates(["user_id", "ts"])

    (
        deduped.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "false")   # never allow silent schema evolution
        .partitionBy("event_date")
        .save("/delta/user-actions")
    )

query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/checkpoints/user-actions")  # NEVER omit
    .trigger(processingTime="30 seconds")
    .start()
)
```

### 4. Checkpoint Location Is Mandatory — One Per Query, Never Shared

```python
# BAD — no checkpoint = restart replays everything from startingOffsets
query = df.writeStream.format("delta").start("/delta/output")

# BAD — shared checkpoint corrupts offset tracking across queries
.option("checkpointLocation", "/checkpoints/shared")

# GOOD — isolated, persistent, unique per query
.option("checkpointLocation", "/checkpoints/user-actions-v3")
# Version the path when you change schema or query logic
```

### 5. Never Call `collect()`, `toPandas()`, or `count()` Inside a Streaming Query

These actions pull data to the driver and will OOM or block indefinitely in a stream.

```python
# BAD — kills the driver in high-volume streams
row_count = batch_df.count()
print(batch_df.collect())

# GOOD — use accumulators or write metrics to a side table
from pyspark.sql.functions import count, lit
batch_df.select(count(lit(1)).alias("rows")).write \
    .format("delta").mode("append").save("/delta/pipeline-metrics")
```

---

## SparkSession — Standard Production Config

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("kafka-to-delta-user-actions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Shuffle
    .config("spark.sql.shuffle.partitions", "200")       # tune to cluster size
    .config("spark.default.parallelism", "200")
    # Memory
    .config("spark.executor.memory", "8g")
    .config("spark.executor.memoryOverhead", "2g")       # always set overhead
    .config("spark.memory.fraction", "0.8")
    # Serialization — Kryo is faster than Java default
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # Adaptive Query Execution — always on in Spark 3+
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # never leave on INFO in prod
```

---

## Partitioning Rules

| Scenario                     | Strategy                                                                     |
| ---------------------------- | ---------------------------------------------------------------------------- |
| Delta table (time-series)    | Partition by `event_date` (yyyy-MM-dd), never by hour (too many small files) |
| Skewed key (e.g. viral user) | Salt the key: `user_id + '_' + (rand * N).cast(int)`                         |
| Small data < 1GB total       | Don't partition — file listing overhead exceeds gain                         |
| Repartition before write     | `df.repartition(N, "event_date")` — avoids small file explosion              |

```python
# Repartition to control output file size (~128MB per file target)
batch_df \
    .repartition(col("event_date")) \
    .write.format("delta") \
    .partitionBy("event_date") \
    .save("/delta/user-actions")
```

---

## Delta Lake — Table Hygiene (Non-Optional)

```python
from delta.tables import DeltaTable

# Run OPTIMIZE + ZORDER regularly — set up as a scheduled job
dt = DeltaTable.forPath(spark, "/delta/user-actions")
dt.optimize().executeZOrderBy("user_id", "ts")

# Vacuum old files — default 7 days retention
dt.vacuum(retentionHours=168)

# Check table health after major writes
spark.sql("DESCRIBE HISTORY delta.`/delta/user-actions`").show(5, truncate=False)
```

**Never skip OPTIMIZE.** Small file accumulation from streaming writes kills read performance within days.

---

## Error Handling in `foreachBatch`

```python
def process_batch(batch_df, batch_id):
    try:
        # Validate — reject records missing critical fields
        valid = batch_df.filter(col("user_id").isNotNull() & col("ts").isNotNull())
        invalid = batch_df.filter(col("user_id").isNull() | col("ts").isNull())

        # Route bad records to quarantine table, not /dev/null
        if invalid.count() > 0:
            (
                invalid
                .withColumn("batch_id", lit(batch_id))
                .withColumn("failed_at", current_timestamp())
                .write.format("delta").mode("append")
                .save("/delta/user-actions-quarantine")
            )

        valid.write.format("delta").mode("append").save("/delta/user-actions")

    except Exception as e:
        # Log with batch_id for replay
        print(f"[FATAL] batch_id={batch_id} failed: {e}")
        raise  # re-raise — let Spark retry, don't swallow
```

---

## Monitoring — Metrics to Track

| Metric                     | Alert Threshold         | Meaning                               |
| -------------------------- | ----------------------- | ------------------------------------- |
| `inputRowsPerSecond`       | Drops > 50% vs baseline | Kafka consumer lag building           |
| `processedRowsPerSecond`   | Sustained low           | Executor bottleneck or GC pressure    |
| Batch duration             | > 2× trigger interval   | Query can't keep up with data rate    |
| Quarantine table row count | Any growth              | Schema or data quality issue upstream |
| Delta table file count     | > 10k small files       | OPTIMIZE not running                  |

```python
# Log streaming progress to stdout (wire to your observability stack)
query.lastProgress   # dict with inputRows, processedRows, batchDuration
query.status         # current query state
```

---

## Common Pitfalls

| Mistake                                | Fix                                                      |
| -------------------------------------- | -------------------------------------------------------- |
| `mergeSchema=true` on Delta writes     | Explicit schema enforcement; break on unexpected columns |
| No `maxOffsetsPerTrigger`              | Unbounded batches OOM executors on lag spikes            |
| Shared or missing checkpoint           | Each query gets its own versioned checkpoint path        |
| Partitioning by high-cardinality col   | Use date-level, not timestamp-level partitioning         |
| Forgetting `vacuum`                    | Delta log and old file versions accumulate indefinitely  |
| Catching exceptions without re-raising | Hides failures; Spark won't retry the batch              |

---

## Quick Deploy Checklist

- [ ] `startingOffsets=earliest`, `failOnDataLoss=true` on Kafka source
- [ ] `maxOffsetsPerTrigger` set (tune to executor memory)
- [ ] Schema parsed explicitly with `from_json` + defined `StructType`
- [ ] Unique, versioned `checkpointLocation` per query
- [ ] `foreachBatch` used for all non-trivial writes
- [ ] Bad records routed to quarantine Delta table
- [ ] OPTIMIZE + VACUUM scheduled as recurring jobs
- [ ] `spark.sql.adaptive.enabled=true` confirmed in session config
- [ ] Log level set to WARN
- [ ] Streaming progress wired to monitoring (Grafana / Datadog)
