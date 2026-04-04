from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("btc-streaming-pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(
        "spark.jars.packages",
        "io.delta:delta-core_2.12:2.4.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
    ) \
    .getOrCreate()

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
    .load()
)

# --- Step 3.3: Parse JSON ---
parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# --- Step 3.4: Bronze layer (raw append) ---
bronze_query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "./delta/checkpoints/bronze")
    .trigger(processingTime="10 seconds")
    .start("./delta/bronze/btc_trades")
)

print("Bronze stream started.")

# --- Step 3.5: Silver layer (1-min windowed aggregations) ---
silver_input = (
    spark.readStream
    .format("delta")
    .load("./delta/bronze/btc_trades")
)

silver = (
    silver_input
    .filter(col("price") > 0)
    .filter(col("volume") > 0)
    .filter(col("event_time").isNotNull())
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
    .outputMode("complete")
    .option("checkpointLocation", "./delta/checkpoints/silver")
    .trigger(processingTime="30 seconds")
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
