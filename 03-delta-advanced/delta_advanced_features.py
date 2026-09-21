# Databricks notebook source
# DBTITLE 1,Delta Lake Advanced — Overview
# MAGIC %md
# MAGIC # Delta Lake: Advanced Features
# MAGIC
# MAGIC **Use Case**: Master advanced Delta Lake operations for production-grade data engineering.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **MERGE / Upsert** | Atomic insert-or-update with SCD Type 2 support |
# MAGIC | **Change Data Feed (CDF)** | Row-level change tracking for CDC pipelines |
# MAGIC | **Time Travel** | Query historical versions by version ID or timestamp |
# MAGIC | **OPTIMIZE / ZORDER** | Compact small files and co-locate data by key |
# MAGIC | **VACUUM** | Remove orphaned files past retention threshold |
# MAGIC | **Table Properties** | Tune Delta behaviour (CDF, log retention, deletion vectors) |
# MAGIC | **DESCRIBE HISTORY** | Audit trail of all table operations |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schema, Sample Tables

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.delta")

# --- Source table: customers ---
spark.sql("DROP TABLE IF EXISTS demo.delta.customers")

customers_data = [
    (1, "John Doe",   "john@example.com",   "Bronze", 100.0,  "2025-01-15"),
    (2, "Jane Smith", "jane@example.com",   "Gold",   5000.0, "2025-01-20"),
    (3, "Sam Brown",  "sam@example.com",    "Silver", 1500.0, "2025-02-01"),
    (4, "Lisa Wang",  "lisa@example.com",   "Gold",   7500.0, "2025-02-10"),
    (5, "Bob Lee",    "bob@example.com",     "Bronze", 50.0,   "2025-03-01"),
]

spark.createDataFrame(customers_data, ["customer_id", "name", "email", "tier", "total_spend", "signup_date"]) \
    .write.format("delta").saveAsTable("demo.delta.customers")

print("✅ Source table demo.delta.customers created with 5 rows")
spark.table("demo.delta.customers").display()

# COMMAND ----------

# DBTITLE 1,Cell 2: Delta MERGE — Upsert
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Delta MERGE — Upsert (Insert + Update)
# MAGIC
# MAGIC Atomically insert new rows and update existing ones in a single operation.

# COMMAND ----------

from pyspark.sql.functions import col

# Simulate incoming updates/inserts
updates_data = [
    (1, "John Doe",   "john.doe@example.com", "Silver", 250.0,  "2025-01-15"),  # UPDATE: tier + spend change
    (3, "Sam Brown",  "sam.brown@example.com","Gold",   1800.0, "2025-02-01"),  # UPDATE
    (6, "New Customer","new@example.com",    "Bronze", 10.0,   "2025-04-01"),  # INSERT
]

updates_df = spark.createDataFrame(updates_data, ["customer_id", "name", "email", "tier", "total_spend", "signup_date"])
updates_df.createOrReplaceTempView("customer_updates")

# MERGE: match on customer_id
spark.sql("""
    MERGE INTO demo.delta.customers AS target
    USING customer_updates AS src
    ON target.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ MERGE completed: 2 updates + 1 insert")
print(f"   Total rows: {spark.table('demo.delta.customers').count()}")
spark.sql("""
    SELECT customer_id, name, tier, total_spend 
    FROM demo.delta.customers 
    WHERE customer_id IN (1, 3, 6) 
    ORDER BY customer_id
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Delta MERGE — SCD Type 2
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Delta MERGE — SCD Type 2 (Slowly Changing Dimensions)
# MAGIC
# MAGIC Track historical changes by preserving old versions and marking them inactive.

# COMMAND ----------

# Create an SCD2 target table with validity tracking
spark.sql("DROP TABLE IF EXISTS demo.delta.customers_scd2")

spark.sql("""
    CREATE TABLE demo.delta.customers_scd2 (
        customer_id LONG,
        name STRING,
        tier STRING,
        total_spend DOUBLE,
        is_current BOOLEAN,
        effective_from STRING,
        effective_to STRING
    ) USING DELTA
""")

# Initial load
spark.sql("""
    INSERT INTO demo.delta.customers_scd2
    SELECT customer_id, name, tier, total_spend, 
           true AS is_current, signup_date AS effective_from, NULL AS effective_to
    FROM demo.delta.customers
""")

# Simulate a tier change for customer 1 (Bronze → Silver → Gold)
new_changes = [
    (1, "John Doe", "Gold", 500.0, "2025-06-01"),
]
new_df = spark.createDataFrame(new_changes, ["customer_id", "name", "tier", "total_spend", "effective_from"])
new_df.createOrReplaceTempView("scd2_updates")

# SCD2 MERGE: expire old record + insert new
spark.sql("""
    MERGE INTO demo.delta.customers_scd2 AS t
    USING scd2_updates AS s
    ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN MATCHED THEN UPDATE SET 
        t.is_current = false, 
        t.effective_to = s.effective_from
""")

spark.sql("""
    INSERT INTO demo.delta.customers_scd2
    SELECT customer_id, name, tier, total_spend, 
           true AS is_current, effective_from, NULL AS effective_to
    FROM scd2_updates
""")

print("✅ SCD Type 2 MERGE completed")
print("   Customer 1 history:")
spark.sql("""
    SELECT customer_id, name, tier, total_spend, is_current, effective_from, effective_to
    FROM demo.delta.customers_scd2 
    WHERE customer_id = 1
    ORDER BY effective_from
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Change Data Feed (CDF)
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Change Data Feed (CDF)
# MAGIC
# MAGIC Enable CDF to track row-level inserts, updates, and deletes — the foundation for CDC pipelines.

# COMMAND ----------

# Enable CDF on the customers table
spark.sql("ALTER TABLE demo.delta.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Make some changes to generate CDF events
spark.sql("INSERT INTO demo.delta.customers VALUES (7, 'Alice Chen', 'alice@example.com', 'Silver', 800.0, '2025-04-15')")
spark.sql("UPDATE demo.delta.customers SET tier = 'Platinum', total_spend = 10000.0 WHERE customer_id = 2")
spark.sql("DELETE FROM demo.delta.customers WHERE customer_id = 5")

# Read the Change Data Feed
print("📊 Change Data Feed for demo.delta.customers:")
spark.sql("""
    SELECT _change_type, customer_id, name, tier, total_spend, _commit_version, _commit_timestamp
    FROM table_changes('demo.delta.customers', 'earliest')
    ORDER BY _commit_version, customer_id
""").display()

print("\n📈 Change type summary:")
spark.sql("""
    SELECT _change_type, COUNT(*) as count
    FROM table_changes('demo.delta.customers', 'earliest')
    GROUP BY _change_type
    ORDER BY count DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 5: Time Travel
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Time Travel — Query Historical Versions
# MAGIC
# MAGIC Delta Lake maintains a versioned history — query data as it existed at any point.

# COMMAND ----------

# Show table history
print("📋 Table History:")
history_df = spark.sql("DESCRIBE HISTORY demo.delta.customers")
history_df.select("version", "timestamp", "operation", "operationParameters").display()

# Get the latest version number
latest_version = history_df.agg({"version": "max"}).collect()[0][0]

# Query at version 0 (original state)
print(f"\n🕐 Time Travel — Version 0 (original data):")
spark.sql("SELECT * FROM demo.delta.customers VERSION AS OF 0 ORDER BY customer_id").display()

# Query at latest version
print(f"\n🕐 Time Travel — Version {latest_version} (current):")
spark.sql(f"SELECT * FROM demo.delta.customers VERSION AS OF {latest_version} ORDER BY customer_id").display()

# Restore to previous version (if needed)
# spark.sql("RESTORE TABLE demo.delta.customers TO VERSION AS OF 0")
print("\n💡 Uncomment RESTORE above to roll back to any version")

# COMMAND ----------

# DBTITLE 1,Cell 6: OPTIMIZE & ZORDER
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: OPTIMIZE & ZORDER
# MAGIC
# MAGIC Compact small files and co-locate data by key for faster query performance.

# COMMAND ----------

# Create a table with many small files
spark.sql("DROP TABLE IF EXISTS demo.delta.events_partitioned")
spark.sql("CREATE TABLE demo.delta.events_partitioned (event_id LONG, user_id LONG, event_type STRING, event_time TIMESTAMP, value DOUBLE) USING DELTA PARTITIONED BY (event_type)")

# Write many small batches
for batch in range(20):
    events = [
        (i + batch * 50, random.randint(1, 100), random.choice(['click', 'view', 'purchase', 'signup']),
         f"2025-09-{random.randint(1, 28):02d} {random.randint(0,23):02d}:00:00", round(random.uniform(1.0, 100.0), 2))
        for i in range(50)
    ]
    spark.createDataFrame(events, ["event_id", "user_id", "event_type", "event_time", "value"]) \
        .write.format("delta").mode("append").saveAsTable("demo.delta.events_partitioned")

# Check files before optimization
print("📁 Before OPTIMIZE:")
spark.sql("SELECT count(*) as file_count FROM (SELECT distinct _metadata.file_path FROM demo.delta.events_partitioned)").display()

# OPTIMIZE with ZORDER on user_id (co-locates data by user_id for faster filtering)
print("⏳ Running OPTIMIZE with ZORDER on user_id...")
spark.sql("OPTIMIZE demo.delta.events_partitioned ZORDER BY (user_id)")

# Check files after optimization
print("\n📁 After OPTIMIZE + ZORDER:")
spark.sql("SELECT count(*) as file_count FROM (SELECT distinct _metadata.file_path FROM demo.delta.events_partitioned)").display()

print("\n✅ Files compacted — queries filtering on user_id will benefit from data co-location")

# COMMAND ----------

# DBTITLE 1,Cell 7: VACUUM
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: VACUUM — Remove Orphaned Files
# MAGIC
# MAGIC Clean up old data files no longer referenced by the Delta transaction log.

# COMMAND ----------

# VACUUM with retention check (default 7 days)
# For demo, we use 0 hours — DRY RUN first
print("🔍 VACUUM DRY RUN (what would be deleted):")
spark.sql("VACUUM demo.delta.events_partitioned RETAIN 0 HOURS DRY RUN").display()

# Actual VACUUM (override retention check for demo purposes)
# spark.conf.set("spark.databricks.delta.vacuum.parquetStatsIgnoreProtected", "true")
print("\n⏳ Running VACUUM (0 hours retention for demo)...")
spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
spark.sql("VACUUM demo.delta.events_partitioned RETAIN 0 HOURS")
spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = true")

print("✅ VACUUM completed — orphaned files removed")
print(f"   Table still has {spark.table('demo.delta.events_partitioned').count()} rows")

# ⚠️ Production best practices:
print("""
⚠️ Production VACUUM Best Practices:
  1. Use default 7-day retention (never VACUUM with 0 hours in prod)
  2. Schedule VACUUM during low-traffic periods
  3. Monitor storage savings via Delta table size before/after
  4. Do NOT VACUUM if concurrent long-running queries may still reference old files
""")

# COMMAND ----------

# DBTITLE 1,Cell 8: Table Properties & Deletion Vectors
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Delta Table Properties & Deletion Vectors
# MAGIC
# MAGIC Configure Delta table behaviour with table properties.

# COMMAND ----------

# Show current properties
print("📋 Current table properties:")
spark.sql("SHOW TBLPROPERTIES demo.delta.customers").display()

# Enable deletion vectors (improves UPDATE/DELETE/PMERGE performance)
spark.sql("ALTER TABLE demo.delta.customers SET TBLPROPERTIES (delta.enableDeletionVectors = true)")

# Set log retention to 30 days (default is 30 days)
spark.sql("ALTER TABLE demo.delta.customers SET TBLPROPERTIES (delta.logRetentionDuration = 'interval 30 days')")

# Set deleted file retention to 14 days
spark.sql("ALTER TABLE demo.delta.customers SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 14 days')")

print("✅ Table properties updated:")
print("   - delta.enableDeletionVectors = true")
print("   - delta.logRetentionDuration = 30 days")
print("   - delta.deletedFileRetentionDuration = 14 days")

print("\n📋 Updated properties:")
spark.sql("SHOW TBLPROPERTIES demo.delta.customers").display()

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|--------|
# MAGIC | **MERGE** | Atomic upserts — no separate INSERT + UPDATE |
# MAGIC | **SCD2** | Full historical audit trail for dimensional data |
# MAGIC | **CDF** | Row-level CDC for downstream sync (no full table scans) |
# MAGIC | **Time Travel** | Point-in-time queries, rollback, audit |
# MAGIC | **OPTIMIZE + ZORDER** | 10-100x faster queries on filtered columns |
# MAGIC | **VACUUM** | Reclaim storage from orphaned files |
# MAGIC | **Deletion Vectors** | Faster UPDATE/DELETE without rewriting whole files |
# MAGIC | **Table Properties** | Tune retention, CDF, deletion vectors per table |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Enable CDF before you need it** — cannot retroactively enable for past changes
# MAGIC 2. **ZORDER on your most-filtered columns** — typically join keys or date partitions
# MAGIC 3. **VACUUM with caution** — respect retention periods to avoid breaking time travel
# MAGIC 4. **Use deletion vectors** for workloads with frequent UPDATE/DELETE
# MAGIC 5. **Monitor `DESCRIBE HISTORY`** — detect unexpected operations or failed writes

# COMMAND ----------

