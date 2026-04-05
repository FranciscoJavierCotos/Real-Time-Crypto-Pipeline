import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType


def configure_windows_runtime():
    if os.name != "nt":
        return None

    project_root = Path(__file__).resolve().parents[1]

    if not os.environ.get("JAVA_HOME"):
        adoptium_root = Path("C:/Program Files/Eclipse Adoptium")
        if adoptium_root.exists():
            jdks = sorted(adoptium_root.glob("jdk-17*"), reverse=True)
            if jdks:
                os.environ["JAVA_HOME"] = str(jdks[0])

    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        os.environ["PATH"] = f"{Path(java_home) / 'bin'};" + os.environ.get("PATH", "")

    hadoop_home = project_root / ".hadoop"
    hadoop_bin = hadoop_home / "bin"
    if (hadoop_bin / "winutils.exe").exists() and (hadoop_bin / "hadoop.dll").exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
        os.environ["PATH"] = f"{hadoop_bin};" + os.environ.get("PATH", "")
        return str(hadoop_bin)

    return None


windows_hadoop_bin = configure_windows_runtime()

builder = SparkSession.builder \
    .appName("btc-streaming-pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(
        "spark.jars.packages",
        "io.delta:delta-core_2.12:2.4.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
    ) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "2g")

if windows_hadoop_bin:
    builder = builder \
        .config("spark.driver.extraLibraryPath", windows_hadoop_bin) \
        .config("spark.executor.extraLibraryPath", windows_hadoop_bin)

spark = builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Schema ---
schema = (
    StructType()
    .add("symbol", StringType())
    .add("price", DoubleType())
    .add("volume", DoubleType())
    .add("event_time", TimestampType())
)

# --- Step 3.2: Read from Kafka ---
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "btc-trades")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)

# --- Step 3.3: Parse JSON ---
parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# --- Step 3.4: Bronze layer (raw append) ---
bronze_path = "./delta/bronze/btc_trades"

bronze_query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "./delta/checkpoints/bronze")
    .trigger(processingTime="1 minute")
    .start(bronze_path)
)

print("Bronze stream started.")

bronze_delta_log = Path(bronze_path) / "_delta_log"
deadline = time.time() + 90
while time.time() < deadline:
    if bronze_delta_log.exists() and list(bronze_delta_log.glob("*.json")):
        break
    time.sleep(2)
else:
    raise TimeoutError("Bronze table did not initialize within 90 seconds.")

# --- Step 3.5: Silver layer (1-min windowed aggregations) ---
silver_input = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

silver = (
    silver_input
    .filter(col("price") > 0)
    .filter(col("volume") > 0)
    .filter(col("event_time").isNotNull())
    .withWatermark("event_time", "2 minutes")
    .withColumn("ingested_at", current_timestamp())
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol"),
    )
    .agg(
        avg("price").alias("avg_price"),
        sum("volume").alias("total_volume"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("avg_price"),
        col("total_volume"),
    )
)

silver_query = (
    silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "./delta/checkpoints/silver")
    .trigger(processingTime="2 minutes")
    .start("./delta/silver/btc_aggregates")
)

print("Silver stream started.")

# --- Step 3.6: Keep both streams running ---
try:
    bronze_query.awaitTermination()
    silver_query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streams...")
    bronze_query.stop()
    silver_query.stop()
    spark.stop()
