# Databricks notebook source
# DBTITLE 1,Auto Loader — Overview
# MAGIC %md
# MAGIC # Auto Loader: Incremental Data Ingestion
# MAGIC
# MAGIC **Use Case**: Incrementally and idempotently ingest new files from cloud storage (S3, ADLS, GCS) into Delta Lake tables.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **`cloudFiles` format** | Automatically detects new files as they arrive |
# MAGIC | **Schema inference** | Auto-detects schema from source files |
# MAGIC | **Schema evolution** | Automatically evolves schema when new columns appear |
# MAGIC | **Checkpointing** | Tracks processed files — exactly-once processing |
# MAGIC | **Trigger modes** | `once` (batch), `availableNow` (micro-batch), `processingTime` (streaming) |
# MAGIC | **File notification** | Directory listing or file notification (event-based) modes |
# MAGIC
# MAGIC ## Pipeline
# MAGIC
# MAGIC ```
# MAGIC Cloud Storage (CSV/JSON/Parquet) → Auto Loader → Bronze Delta Table → MERGE → Silver Delta Table
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup — Catalog, Schema, Volume
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schema, Volume
# MAGIC
# MAGIC Create the Unity Catalog infrastructure for the Auto Loader demo.  
# MAGIC We use a **Volume** as a simulated landing zone for source files.

# COMMAND ----------

# Create catalog and schemas for Auto Loader demo
spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.silver")

# Create a volume to simulate a cloud storage landing zone
spark.sql("""
    CREATE VOLUME IF NOT EXISTS demo.bronze.landing_zone
""")

print("✅ Catalog, schemas, and volume created successfully")
print("   - Catalog: demo")
print("   - Schemas: demo.bronze, demo.silver")
print("   - Volume:  demo.bronze.landing_zone (simulated landing zone)")

# COMMAND ----------

# DBTITLE 1,Cell 2: Generate Sample Source Files
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Generate Sample Source Files
# MAGIC
# MAGIC Simulate files arriving in a landing zone by writing CSV batches into the Volume.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
import random, string

landing_path = "/Volumes/demo/bronze/landing_zone/raw_files"

# --- Batch 1: Initial load (5 files, 100 rows each) ---
for i in range(5):
    rows = [
        (
            j,
            f"customer_{random.randint(1, 50)}",
            f"{random.choice(['electronics', 'clothing', 'groceries', 'home'])}",
            round(random.uniform(10.0, 500.0), 2),
            f"2025-09-{random.randint(1, 15):02d}",
        )
        for j in range(i * 100, (i + 1) * 100)
    ]
    df = spark.createDataFrame(rows, ["txn_id", "customer", "category", "amount", "txn_date"])
    df.write.mode("overwrite").option("header", True).csv(f"{landing_path}/batch_1/file_{i}.csv")

print(f"✅ Batch 1 written: 5 CSV files, 500 total rows")
print(f"   Landing zone: {landing_path}")

# COMMAND ----------

# DBTITLE 1,Cell 3: Auto Loader — Schema Inference (Trigger Once)
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Auto Loader — Schema Inference (Trigger Once)
# MAGIC
# MAGIC Use `cloudFiles` format with `schemaLocation` for automatic schema inference.  
# MAGIC `trigger(once=True)` processes all available files and stops.

# COMMAND ----------

from pyspark.sql.functions import col, input_file_name, current_timestamp

bronze_table = "demo.bronze.transactions_raw"
checkpoint_path = "/Volumes/demo/bronze/landing_zone/_checkpoints/bronze_raw"
schema_location = "/Volumes/demo/bronze/landing_zone/_schemas/bronze_raw"

# Clean up for re-run
spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(schema_location, True)

# Auto Loader with schema inference
raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", True)
    .option("cloudFiles.useStrictGlobbing", True)
    .load(landing_path + "/batch_1/*")
    .withColumn("source_file", input_file_name())
    .withColumn("ingest_timestamp", current_timestamp())
)

# Write to Delta with trigger once (batch mode)
query = (
    raw_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", True)
    .trigger(once=True)  # process all available files, then stop
    .toTable(bronze_table)
)

query.awaitTermination()

# Verify
row_count = spark.table(bronze_table).count()
print(f"✅ Bronze table '{bronze_table}' loaded: {row_count} rows")
spark.table(bronze_table).limit(5).display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Auto Loader — Schema Evolution (addNewColumns)
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Auto Loader — Schema Evolution
# MAGIC
# MAGIC Simulate a **new column** appearing in source files.  
# MAGIC `cloudFiles.schemaEvolutionMode = addNewColumns` automatically evolves the schema.

# COMMAND ----------

import time

# --- Batch 2: New files with an extra column 'region' ---
for i in range(3):
    rows = [
        (
            j,
            f"customer_{random.randint(1, 50)}",
            f"{random.choice(['electronics', 'clothing', 'groceries', 'home'])}",
            round(random.uniform(10.0, 500.0), 2),
            f"2025-09-{random.randint(16, 28):02d}",
            random.choice(["US", "EU", "APAC", "LATAM"]),  # NEW column
        )
        for j in range(500 + i * 100, 500 + (i + 1) * 100)
    ]
    df = spark.createDataFrame(rows, ["txn_id", "customer", "category", "amount", "txn_date", "region"])
    df.write.mode("overwrite").option("header", True).csv(f"{landing_path}/batch_2/file_{i}.csv")

print("✅ Batch 2 written: 3 CSV files with new 'region' column")

# Re-run Auto Loader with schema evolution — now scanning batch_2
raw_stream_evolved = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # auto-evolve schema
    .option("header", True)
    .load(landing_path + "/batch_2/*")
    .withColumn("source_file", input_file_name())
    .withColumn("ingest_timestamp", current_timestamp())
)

query2 = (
    raw_stream_evolved.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", True)
    .trigger(once=True)  # process all available files, then stop
    .toTable(bronze_table)
)

query2.awaitTermination()

print(f"\n📋 Schema after evolution:")
spark.table(bronze_table).printSchema()
print(f"\n✅ Total rows in bronze: {spark.table(bronze_table).count()}")
spark.table(bronze_table).filter(col("region").isNotNull()).limit(5).display()

# COMMAND ----------

# DBTITLE 1,Cell 5: Auto Loader — Incremental MERGE to Silver
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Incremental MERGE to Silver Layer
# MAGIC
# MAGIC Use `foreachBatch` + Delta `MERGE` to upsert new bronze records into silver.

# COMMAND ----------

silver_table = "demo.silver.transactions_cleaned"
checkpoint_silver = "/Volumes/demo/bronze/landing_zone/_checkpoints/silver"

spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
dbutils.fs.rm(checkpoint_silver, True)

# Read bronze as a stream (process new inserts incrementally)
bronze_stream = (
    spark.readStream
    .format("delta")
    .load(f"{bronze_table}")
    .filter(col("amount").isNotNull() & col("customer").isNotNull())
)

def upsert_to_silver(batch_df, batch_id):
    """Upsert micro-batch into silver table using Delta MERGE."""
    if batch_df.isEmpty():
        return

    # Ensure silver table exists on first batch
    if not spark.catalog.tableExists(silver_table):
        batch_df.limit(0).write.format("delta").saveAsTable(silver_table)

    # Build MERGE: match on txn_id, update existing, insert new
    batch_df.createOrReplaceTempView("_silver_updates")

    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING _silver_updates AS src
        ON target.txn_id = src.txn_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# Trigger once for batch processing
query3 = (
    bronze_stream.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint_silver)
    .trigger(once=True)
    .start()
)

query3.awaitTermination()

silver_count = spark.table(silver_table).count()
print(f"✅ Silver table loaded: {silver_count} rows (null amounts filtered)")
spark.table(silver_table).limit(5).display()

# COMMAND ----------

# DBTITLE 1,Cell 6: Auto Loader — AvailableNow Trigger (Cost-Optimised)
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: AvailableNow Trigger (Cost-Optimised)
# MAGIC
# MAGIC `trigger(availableNow=True)` processes all available data in finite micro-batches,
# MAGIC then stops the stream. Ideal for scheduled jobs — pay only for compute you use.

# COMMAND ----------

# Simulate a third batch of files
for i in range(2):
    rows = [
        (
            j,
            f"customer_{random.randint(1, 50)}",
            f"{random.choice(['electronics', 'clothing', 'groceries', 'home'])}",
            round(random.uniform(10.0, 500.0), 2),
            f"2025-09-{random.randint(25, 30):02d}",
            random.choice(["US", "EU", "APAC", "LATAM"]),
        )
        for j in range(800 + i * 100, 800 + (i + 1) * 100)
    ]
    df = spark.createDataFrame(rows, ["txn_id", "customer", "category", "amount", "txn_date", "region"])
    df.write.mode("overwrite").option("header", True).csv(f"{landing_path}/batch_3/file_{i}.csv")

print("✅ Batch 3 written: 2 new CSV files")

# AvailableNow trigger — process all pending files in finite batches, then stop
raw_stream_an = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", True)
    .load(landing_path + "/batch_3/*")
    .withColumn("source_file", input_file_name())
    .withColumn("ingest_timestamp", current_timestamp())
)

query4 = (
    raw_stream_an.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", True)
    .trigger(availableNow=True)  # finite micro-batches
    .toTable(bronze_table)
)

query4.awaitTermination()

final_count = spark.table(bronze_table).count()
print(f"✅ Bronze table total: {final_count} rows after AvailableNow trigger")
print(f"   New files processed: {spark.table(bronze_table).filter(col('txn_id') >= 800).count()}")

# COMMAND ----------

# DBTITLE 1,Cell 7: Monitoring — Streaming Query Metrics
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Monitoring — Streaming Query Metrics
# MAGIC
# MAGIC Inspect Auto Loader metrics: files discovered, processed, and throughput.

# COMMAND ----------

# Show recent streaming query metrics from the Spark UI
print("📊 Auto Loader Summary")
print("=" * 60)

bronze_df = spark.table(bronze_table)
print(f"\n  Bronze table:        {bronze_table}")
print(f"  Total rows:           {bronze_df.count()}")
print(f"  Distinct files:       {bronze_df.select('source_file').distinct().count()}")
print(f"  Schema columns:       {len(bronze_df.columns)}")
print(f"  Date range:           {bronze_df.agg({'txn_date': 'min'}).collect()[0][0]} → {bronze_df.agg({'txn_date': 'max'}).collect()[0][0]}")

print(f"\n  Category distribution:")
bronze_df.groupBy("category").count().orderBy("count", ascending=False).display()

print(f"\n  Region distribution (evolved column):")
bronze_df.groupBy("region").count().orderBy("count", ascending=False).display()

# Delta table history (shows Auto Loader writes)
print(f"\n📋 Delta table history (last 5 operations):")
spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 5").display()

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Pattern | When to Use |
# MAGIC |---------|------------|
# MAGIC | **`trigger(once=True)`** | One-time backfill or ad-hoc ingestion |
# MAGIC | **`trigger(availableNow=True)`** | Scheduled jobs — process pending files, then stop (cost-efficient) |
# MAGIC | **`trigger(processingTime='30s')`** | Continuous near-real-time streaming |
# MAGIC | **Schema inference** | Unknown/expanding source schemas |
# MAGIC | **`addNewColumns`** | Source schema evolves over time (new columns added) |
# MAGIC | **`foreachBatch` + MERGE** | Idempotent upserts into silver/gold tables |
# MAGIC | **Checkpointing** | Exactly-once guarantee — never re-process files |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Always use checkpoints** — without them, reprocessing causes duplicates
# MAGIC 2. **Use `availableNow` for batch jobs** — saves compute cost vs always-on streaming
# MAGIC 3. **Monitor `numFilesSeen`** metric — detects file backlog or stuck streams
# MAGIC 4. **Set `cloudFiles.maxFilesPerTrigger`** — control batch size for large backfills
# MAGIC 5. **Use `schemaLocation`** — persists inferred schema across restarts

# COMMAND ----------

