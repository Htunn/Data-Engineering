# Databricks notebook source
# DBTITLE 1,Structured Streaming — Overview
# MAGIC %md
# MAGIC # Structured Streaming: Real-Time Data Processing
# MAGIC
# MAGIC **Use Case**: Process real-time event streams with watermarks, windowed aggregations, and streaming joins.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **`rate` source** | Built-in streaming source for testing (generates rows at a configurable rate) |
# MAGIC | **Watermarks** | Handle late-arriving data — define how long to wait before finalising a window |
# MAGIC | **Windowed aggregations** | Tumbling and sliding window aggregations |
# MAGIC | **`foreachBatch`** | Write each micro-batch to any sink (Delta, Kafka, REST API) |
# MAGIC | **Streaming deduplication** | Drop duplicates within a watermark window |
# MAGIC | **Stream-static joins** | Join a stream with a static DataFrame |
# MAGIC | **Stream-stream joins** | Join two streams with watermarks (e.g., impressions + clicks) |
# MAGIC | **Output modes** | `append`, `update`, `complete` |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schema, Sink Tables

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.streaming")

# Drop existing tables
for t in ["rate_output", "windowed_agg", "dedup_output", "joined_output"]:
    spark.sql(f"DROP TABLE IF EXISTS demo.streaming.{t}")

# Clean checkpoints
for p in ["rate", "window", "dedup", "join"]:
    dbutils.fs.rm(f"/Volumes/demo/streaming/_checkpoints/{p}", True)

print("✅ Catalog, schema, and checkpoint locations ready")

# COMMAND ----------

# DBTITLE 1,Cell 2: Basic Streaming with Rate Source
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Basic Streaming with `rate` Source
# MAGIC
# MAGIC The `rate` source generates rows at a configurable rate — perfect for demos.

# COMMAND ----------

from pyspark.sql.functions import col

# Create a rate stream: 10 rows/second with 3 partitions
rate_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 10)
    .option("numPartitions", 3)
    .option("rampUpTime", "1s")
    .load()
    .withColumnRenamed("timestamp", "event_time")
    .withColumnRenamed("value", "event_id")
)

# Write to Delta (trigger once — process 10 seconds of data)
query = (
    rate_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/demo/streaming/_checkpoints/rate")
    .trigger(processingTime="5 seconds")
    .toTable("demo.streaming.rate_output")
)

# Let it run for 10 seconds, then stop
import time
time.sleep(10)
query.stop()

print(f"✅ Rate stream produced: {spark.table('demo.streaming.rate_output').count()} rows")
spark.table("demo.streaming.rate_output").display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Windowed Aggregations
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Windowed Aggregations with Watermark
# MAGIC
# MAGIC Aggregate events into **tumbling windows** (fixed-size, non-overlapping).
# MAGIC Watermark handles late data — drops events older than the threshold.

# COMMAND ----------

from pyspark.sql.functions import window, col, count, sum as spark_sum

# Generate a rate stream with simulated event values
events_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 20)
    .option("numPartitions", 2)
    .load()
    .withColumnRenamed("timestamp", "event_time")
    .withColumn("event_value", (col("value") % 100).cast("double"))
    .withColumn("category", col("value") % 4)
)

# Tumbling window: 10-second windows with 20-second watermark for late data
windowed = (
    events_stream
    .withWatermark("event_time", "20 seconds")  # tolerate 20s late data
    .groupBy(
        window(col("event_time"), "10 seconds"),  # 10s tumbling window
        col("category"),
    )
    .agg(
        count("*").alias("event_count"),
        spark_sum("event_value").alias("total_value"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "category",
        "event_count",
        "total_value",
    )
)

# Write with update mode (output new aggregates as they update)
query2 = (
    windowed.writeStream
    .format("delta")
    .outputMode("append")  # append finalised windows only
    .option("checkpointLocation", "/Volumes/demo/streaming/_checkpoints/window")
    .trigger(processingTime="5 seconds")
    .toTable("demo.streaming.windowed_agg")
)

time.sleep(15)
query2.stop()

print(f"✅ Windowed aggregation: {spark.table('demo.streaming.windowed_agg').count()} windows")
spark.table("demo.streaming.windowed_agg").orderBy("window_start").display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Streaming Deduplication
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Streaming Deduplication
# MAGIC
# MAGIC Use `dropDuplicates` with a watermark to remove duplicate events within a time window.

# COMMAND ----------

from pyspark.sql.functions import col, rand, when

# Generate a stream with occasional duplicates
dup_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 15)
    .load()
    .withColumnRenamed("timestamp", "event_time")
    .withColumn("event_id", col("value") % 10)  # event_id 0-9 → creates duplicates
    .withColumn("payload", (col("value") * 7).cast("double"))
)

# Deduplicate by event_id within a 15-second watermark
dedup_stream = (
    dup_stream
    .withWatermark("event_time", "15 seconds")
    .dropDuplicates(["event_id"])
)

query3 = (
    dedup_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/demo/streaming/_checkpoints/dedup")
    .trigger(processingTime="5 seconds")
    .toTable("demo.streaming.dedup_output")
)

time.sleep(10)
query3.stop()

total = spark.table("demo.streaming.dedup_output").count()
distinct = spark.table("demo.streaming.dedup_output").select("event_id").distinct().count()
print(f"✅ Dedup stream: {total} rows after dedup, {distinct} distinct event_ids")
spark.table("demo.streaming.dedup_output").orderBy("event_id").display()

# COMMAND ----------

# DBTITLE 1,Cell 5: foreachBatch with MERGE
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: `foreachBatch` — Custom Sink with MERGE
# MAGIC
# MAGIC Write each micro-batch to a Delta table with upsert logic using MERGE.

# COMMAND ----------

from pyspark.sql.functions import col

spark.sql("DROP TABLE IF EXISTS demo.streaming.foreach_output")
dbutils.fs.rm("/Volumes/demo/streaming/_checkpoints/foreach", True)

target_table = "demo.streaming.foreach_output"

foreach_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumnRenamed("timestamp", "event_time")
    .withColumnRenamed("value", "event_id")
    .withColumn("batch_id_col", col("event_id") % 3)
)

def upsert_batch(batch_df, batch_id):
    """Upsert each micro-batch into the target table using MERGE."""
    if batch_df.isEmpty():
        return

    if not spark.catalog.tableExists(target_table):
        batch_df.write.format("delta").saveAsTable(target_table)
        return

    batch_df.createOrReplaceTempView("_batch_updates")
    spark.sql(f"""
        MERGE INTO {target_table} AS t
        USING _batch_updates AS s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

query4 = (
    foreach_stream.writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", "/Volumes/demo/streaming/_checkpoints/foreach")
    .trigger(processingTime="5 seconds")
    .start()
)

time.sleep(10)
query4.stop()

print(f"✅ foreachBatch + MERGE: {spark.table(target_table).count()} rows (deduplicated by event_id)")
spark.table(target_table).orderBy("event_id").display()

# COMMAND ----------

# DBTITLE 1,Cell 6: Stream-Static Join
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Stream-Static Join
# MAGIC
# MAGIC Join a real-time stream with a static lookup table (e.g., enrich events with user profiles).

# COMMAND ----------

from pyspark.sql.functions import col

# Create a static lookup table
spark.sql("DROP TABLE IF EXISTS demo.streaming.user_lookup")

lookup_data = [
    (0, "Free",     "US"),
    (1, "Basic",    "US"),
    (2, "Premium",  "EU"),
    (3, "Enterprise","APAC"),
]
spark.createDataFrame(lookup_data, ["category_id", "tier", "region"]) \
    .write.format("delta").saveAsTable("demo.streaming.user_lookup")

# Stream of events
enrich_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumnRenamed("timestamp", "event_time")
    .withColumn("category_id", col("value") % 4)
)

# Join stream with static table
static_lookup = spark.table("demo.streaming.user_lookup")
enriched = enrich_stream.join(static_lookup, "category_id", "left")

spark.sql("DROP TABLE IF EXISTS demo.streaming.enriched_events")

query5 = (
    enriched.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/demo/streaming/_checkpoints/static_join")
    .trigger(processingTime="5 seconds")
    .toTable("demo.streaming.enriched_events")
)

time.sleep(10)
query5.stop()

print("✅ Stream-Static Join — enriched events:")
spark.sql("""
    SELECT event_time, category_id, tier, region 
    FROM demo.streaming.enriched_events 
    ORDER BY event_time DESC 
    LIMIT 10
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 7: Stream-Stream Join
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Stream-Stream Join (Watermarked)
# MAGIC
# MAGIC Join two streams: **impressions** and **clicks**, matched by `ad_id` within a time window.

# COMMAND ----------

from pyspark.sql.functions import col, expr, interval

# Simulated impressions stream
impressions = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumnRenamed("timestamp", "impression_time")
    .withColumn("ad_id", (col("value") % 5).cast("long"))
    .withColumn("impression_id", col("value"))
)

# Simulated clicks stream (same ad_ids, delayed)
clicks = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumnRenamed("timestamp", "click_time")
    .withColumn("ad_id", (col("value") % 5).cast("long"))
    .withColumn("click_id", col("value"))
)

# Join: match clicks to impressions within 30 seconds
joined = (
    impressions.withWatermark("impression_time", "30 seconds")
    .join(
        clicks.withWatermark("click_time", "30 seconds"),
        expr("""
            impressions.ad_id = clicks.ad_id AND
            click_time >= impression_time AND
            click_time <= impression_time + interval 30 seconds
        """),
        "inner"
    )
)

spark.sql("DROP TABLE IF EXISTS demo.streaming.impression_clicks")

query6 = (
    joined.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/demo/streaming/_checkpoints/stream_join")
    .trigger(processingTime="5 seconds")
    .toTable("demo.streaming.impression_clicks")
)

time.sleep(15)
query6.stop()

joined_count = spark.table("demo.streaming.impression_clicks").count()
print(f"✅ Stream-Stream Join: {joined_count} matched impression-click pairs")
if joined_count > 0:
    spark.table("demo.streaming.impression_clicks").select(
        "impression_time", "click_time", "ad_id", "impression_id", "click_id"
    ).display()
else:
    print("   (No matches — timing dependent. Re-run to see matches.)")

# COMMAND ----------

# DBTITLE 1,Cell 8: Monitoring Streaming Queries
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Monitoring Streaming Queries
# MAGIC
# MAGIC Track streaming query metrics: input rate, processing rate, latency, batches.

# COMMAND ----------

print("📊 Streaming Query Summary")
print("=" * 60)

tables = [
    ("demo.streaming.rate_output", "Basic rate stream"),
    ("demo.streaming.windowed_agg", "Windowed aggregation"),
    ("demo.streaming.dedup_output", "Deduplication"),
    ("demo.streaming.foreach_output", "foreachBatch + MERGE"),
    ("demo.streaming.enriched_events", "Stream-static join"),
    ("demo.streaming.impression_clicks", "Stream-stream join"),
]

print(f"\n{'Table':<35} {'Pattern':<25} {'Rows':>8}")
print("-" * 70)
for table, pattern in tables:
    try:
        count = spark.table(table).count()
    except:
        count = 0
    print(f"  {table:<35} {pattern:<25} {count:>8}")

print("""
💡 Monitoring Best Practices:
  1. Use StreamingQuery.lastProgress() for input/processing rates
  2. Set up alerts on backlog (inputRate > processingRate)
  3. Monitor checkpointLocation for disk usage
  4. Use Spark UI → Structured Streaming tab for detailed metrics
  5. Log batch IDs to detect stalled streams
""")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Pattern | When to Use |
# MAGIC |---------|------------|
# MAGIC | **`rate` source** | Testing/prototyping streaming logic without external dependencies |
# MAGIC | **Tumbling windows** | Fixed-interval aggregations (e.g., per-minute metrics) |
# MAGIC | **Sliding windows** | Overlapping time windows (e.g., 10-min rolling avg, updated every 1 min) |
# MAGIC | **Watermarks** | Anytime late data is possible — define how late is acceptable |
# MAGIC | **`dropDuplicates` + watermark** | Idempotent ingestion, exactly-once processing |
# MAGIC | **`foreachBatch`** | Custom sinks, MERGE upserts, multi-sink fan-out |
# MAGIC | **Stream-static join** | Enrich streams with dimension/lookup tables |
# MAGIC | **Stream-stream join** | Real-time correlation (impressions→clicks, orders→shipments) |
# MAGIC
# MAGIC ## Output Modes
# MAGIC | Mode | Behaviour | Use Case |
# MAGIC |------|-----------|----------|
# MAGIC | **`append`** | Only new rows (after window closes) | Finalised aggregates, append-only sinks |
# MAGIC | **`update`** | Only changed rows (anytime) | Dashboards, real-time updates |
# MAGIC | **`complete`** | Full result every trigger | Small result sets (e.g., top-N leaderboards) |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Always set watermarks** for stateful operations (windows, joins, dedup)
# MAGIC 2. **Use `append` mode** for Delta sinks — `complete` can cause performance issues
# MAGIC 3. **Set `processingTime` trigger** to control micro-batch frequency and cost
# MAGIC 4. **Monitor input vs processing rate** — backlog indicates scaling needed
# MAGIC 5. **Use `foreachBatch`** for complex sinks that don't support streaming writes

# COMMAND ----------

