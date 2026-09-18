# Databricks notebook source
# DBTITLE 1,SDP Pipelines — Overview
# MAGIC %md
# MAGIC # Spark Declarative Pipelines (SDP): Medallion with Data Quality
# MAGIC
# MAGIC **Use Case**: Build a production-grade medallion pipeline with built-in data quality expectations using Databricks SDP.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **`@dlt.table`** | Define a materialized view or streaming table |
# MAGIC | **`@dlt.expect`** | Drop records that violate a quality rule (warn only) |
# MAGIC | **`@dlt.expect_or_drop`** | Drop records that fail (enforce) |
# MAGIC | **`@dlt.expect_or_fail`** | Fail the pipeline if any record violates |
# MAGIC | **Streaming tables** | Auto Loader-backed incremental ingestion |
# MAGIC | **Materialized views** | Batch-processed tables with automatic refresh |
# MAGIC | **Pipeline settings** | Configure scheduling, notifications, autoscaling |
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC   Cloud Files (CSV/JSON)
# MAGIC         │
# MAGIC         ▼
# MAGIC   ┌─────────────┐    @dlt.expect_or_drop
# MAGIC   │   Bronze     │    (drop bad records)
# MAGIC   │  raw_events  │
# MAGIC   └──────┬──────┘
# MAGIC          │ @dlt.expect
# MAGIC          ▼
# MAGIC   ┌─────────────┐    (warn on anomalies)
# MAGIC   │   Silver     │    Dedup + enrich
# MAGIC   │ cleaned_evt  │
# MAGIC   └──────┬──────┘
# MAGIC          │ @dlt.expect_or_fail
# MAGIC          ▼
# MAGIC   ┌─────────────┐    (enforce business rules)
# MAGIC   │   Gold       │    Aggregations
# MAGIC   │ daily_metrics│
# MAGIC   └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Pipeline Configuration
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Pipeline Configuration
# MAGIC
# MAGIC Define pipeline-level settings. These are typically set in the pipeline UI or via the API.
# MAGIC For reference, here is the JSON configuration:

# COMMAND ----------

# MAGIC %md
# MAGIC ```json
# MAGIC {
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "autoscale": {
# MAGIC         "min_workers": 1,
# MAGIC         "max_workers": 4,
# MAGIC         "mode": "ENHANCED"
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "development": true,
# MAGIC   "continuous": false,
# MAGIC   "channel": "CURRENT",
# MAGIC   "photon": true,
# MAGIC   "edition": "ADVANCED",
# MAGIC   "target": "demo.sdp",
# MAGIC   "configuration": {
# MAGIC     "pipelines.reset.allowed": false,
# MAGIC     "pipelines.streamVacuuming.enabled": true
# MAGIC   },
# MAGIC   "notifications": [
# MAGIC     {
# MAGIC       "email_recipients": ["data-team@company.com"],
# MAGIC       "alerts": ["on-update-failure", "on-flow-failure"]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 2: Bronze Layer — Streaming Table
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Bronze Layer — Streaming Table with Auto Loader
# MAGIC
# MAGIC Ingest raw events from cloud storage with Auto Loader.  
# MAGIC Apply `@dlt.expect_or_drop` to filter out malformed records.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# Define schema for source files
EVENT_SCHEMA = StructType([
    StructField("event_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_value", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("source_ip", StringType(), True),
])

# Bronze: streaming table with Auto Loader
@dlt.table(
    name="bronze_events_raw",
    comment="Raw events ingested from cloud storage via Auto Loader",
    table_properties={"pipelines.reset.allowed": "false"},
    partition_cols=["event_type"],
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def bronze_events_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/demo/sdp/_schemas/bronze")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .schema(EVENT_SCHEMA)
        .load("/Volumes/demo/sdp/events/")
        .withColumn("source_file", input_file_name())
        .withColumn("ingest_time", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Cell 3: Bronze DQ Summary View
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Bronze Layer — Data Quality Dashboard View
# MAGIC
# MAGIC A view that summarises data quality metrics across all expectations.

# COMMAND ----------

@dlt.view(
    name="bronze_dq_summary",
    comment="Data quality summary for bronze layer expectations",
)
def bronze_dq_summary():
    """
    Aggregates pass/fail counts per expectation rule.
    In production, these metrics flow to the pipeline event log automatically.
    """
    return (
        dlt.read("bronze_events_raw")
        .groupBy("event_type")
        .agg(
            spark.functions.count("*").alias("total_records"),
            spark.functions.sum(spark.functions.when(col("event_value") > 0, 1).otherwise(0)).alias("positive_values"),
            spark.functions.sum(spark.functions.when(col("event_value") <= 0, 1).otherwise(0)).alias("non_positive_values"),
            spark.functions.countDistinct("user_id").alias("distinct_users"),
        )
        .orderBy("event_type")
    )

# COMMAND ----------

# DBTITLE 1,Cell 4: Silver Layer — Cleaned & Enriched
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Silver Layer — Cleaned & Enriched Events
# MAGIC
# MAGIC Deduplicate, enrich with derived columns, and apply quality warnings.

# COMMAND ----------

from pyspark.sql.functions import col, when, to_date, hour, dayofweek

@dlt.table(
    name="silver_events_cleaned",
    comment="Cleaned and enriched events with deduplication and derived columns",
    table_properties={"pipelines.reset.allowed": "false"},
    partition_cols=["event_date"],
)
@dlt.expect("positive_value", "event_value > 0")
@dlt.expect("valid_ip_format", "source_ip RLIKE '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
def silver_events_cleaned():
    return (
        dlt.read_stream("bronze_events_raw")
        # Deduplicate by event_id (keep latest)
        .dropDuplicates(["event_id"])
        # Enrich with derived columns
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("day_of_week", dayofweek(col("event_timestamp")))
        .withColumn("is_weekend", when(dayofweek(col("event_timestamp")).isin([1, 7]), True).otherwise(False))
        .withColumn("value_category", 
            when(col("event_value") < 50, "low")
            .when(col("event_value") < 200, "medium")
            .otherwise("high")
        )
    )

# COMMAND ----------

# DBTITLE 1,Cell 5: Silver Layer — Reference Join
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Silver Layer — Reference Data Join
# MAGIC
# MAGIC Enrich events with a static reference table (user dimension).

# COMMAND ----------

# Static reference table (batch-processed materialized view)
@dlt.table(
    name="silver_user_profiles",
    comment="User profile reference data enriched into silver layer",
)
def silver_user_profiles():
    # In production, this would read from a dimension table
    return (
        dlt.read("bronze_events_raw")
        .select("user_id")
        .distinct()
        .withColumn("profile_status", lit("active"))
        .withColumn("enriched_at", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Cell 6: Gold Layer — Daily Metrics
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Gold Layer — Daily Metrics Aggregation
# MAGIC
# MAGIC Business-ready aggregations with strict quality enforcement (`expect_or_fail`).

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg, countDistinct, expr

@dlt.table(
    name="gold_daily_event_metrics",
    comment="Daily event metrics per event type — business-ready aggregation",
    table_properties={
        "pipelines.reset.allowed": "false",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_fail("non_null_event_date", "event_date IS NOT NULL")
@dlt.expect_or_fail("non_null_event_type", "event_type IS NOT NULL")
def gold_daily_event_metrics():
    return (
        dlt.read_stream("silver_events_cleaned")
        .groupBy("event_date", "event_type", "is_weekend")
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            sum("event_value").alias("total_value"),
            avg("event_value").alias("avg_value"),
            expr("percentile_approx(event_value, 0.95)").alias("p95_value"),
        )
        .withColumn("updated_at", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Cell 7: Gold Layer — User Activity
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Gold Layer — User Activity Summary
# MAGIC
# MAGIC Per-user daily activity counts — useful for churn analysis and engagement scoring.

# COMMAND ----------

@dlt.table(
    name="gold_user_activity",
    comment="Per-user daily activity summary for engagement analysis",
    table_properties={"pipelines.reset.allowed": "false"},
)
@dlt.expect_or_fail("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_fail("valid_date", "event_date IS NOT NULL")
def gold_user_activity():
    return (
        dlt.read_stream("silver_events_cleaned")
        .groupBy("event_date", "user_id")
        .agg(
            count("*").alias("daily_event_count"),
            countDistinct("event_type").alias("event_type_variety"),
            sum("event_value").alias("daily_total_value"),
            max("event_timestamp").alias("last_activity"),
        )
        .withColumn("activity_tier",
            when(col("daily_event_count") >= 10, "power_user")
            .when(col("daily_event_count") >= 3, "regular_user")
            .otherwise("casual_user")
        )
    )

# COMMAND ----------

# DBTITLE 1,Cell 8: Gold Layer — Quality KPIs
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Gold Layer — Quality Metrics Report
# MAGIC
# MAGIC A view that tracks data quality KPIs across all layers.

# COMMAND ----------

@dlt.view(
    name="gold_quality_kpis",
    comment="Data quality KPIs across pipeline layers",
)
def gold_quality_kpis():
    """Calculate quality KPIs: completeness, validity, uniqueness."""
    bronze = dlt.read("bronze_events_raw")
    silver = dlt.read("silver_events_cleaned")
    
    bronze_count = bronze.count()
    silver_count = silver.count()
    
    return spark.createDataFrame([
        ("bronze_to_silver_retention", round(silver_count / max(bronze_count, 1) * 100, 2)),
        ("silver_dedup_rate", round((bronze_count - silver_count) / max(bronze_count, 1) * 100, 2)),
    ], ["metric_name", "metric_value"])

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | **Streaming tables** | `readStream` + Auto Loader for incremental ingestion |
# MAGIC | **Materialized views** | `read` (batch) for dimension/reference tables |
# MAGIC | **`@dlt.expect`** | Log warning, keep the row |
# MAGIC | **`@dlt.expect_or_drop`** | Drop the row, log as metric |
# MAGIC | **`@dlt.expect_or_fail`** | Fail the pipeline (strict enforcement) |
# MAGIC | **Auto Loader in SDP** | Idempotent file ingestion with schema evolution |
# MAGIC | **`pipelines.reset.allowed`** | Prevent full refresh (protect historical data) |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Bronze → `expect_or_drop`** — tolerate bad source data, don't fail the pipeline
# MAGIC 2. **Silver → `expect`** — warn on anomalies for investigation, keep the data
# MAGIC 3. **Gold → `expect_or_fail`** — enforce business rules (downstream analytics depend on correctness)
# MAGIC 4. **Use `dropDuplicates`** for deduplication in streaming tables
# MAGIC 5. **Partition by date** — enables efficient incremental processing and pruning
# MAGIC 6. **Enable CDF on gold tables** — powers downstream CDC and incremental loads
# MAGIC 7. **Set `pipelines.reset.allowed = false`** — prevents accidental full refreshes in production
# MAGIC
# MAGIC > **Note**: This notebook is designed to be attached to a Spark Declarative Pipeline.
# MAGIC > Create a pipeline in the Databricks UI, select this notebook as the source, and configure
# MAGIC > the target schema as `demo.sdp`.

# COMMAND ----------

