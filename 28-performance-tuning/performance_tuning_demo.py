# Databricks notebook source
# DBTITLE 1,Liquid Clustering
# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Tuning - Query Optimization
# MAGIC
# MAGIC Liquid clustering, ZORDER, Photon, AQE, broadcast joins.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS demo.perf")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.perf.events
    USING DELTA
    CLUSTER BY (event_date, user_id)
    AS SELECT
        id,
        (id % 1000) AS user_id,
        rand() * 100 AS value,
        date_add(date('2025-01-01'), cast(id % 365 as int)) AS event_date
    FROM range(100000)
""")
print("✅ Liquid clustered table created")
spark.sql("DESCRIBE DETAIL demo.perf.events").select("clusteringColumns").display()

# COMMAND ----------

# DBTITLE 1,AQE & Broadcast
# Databricks notebook source
# MAGIC %md
# MAGIC ## Query Profiling & Adaptive Query Execution

# COMMAND ----------

# Enable AQE (enabled by default on Databricks)
spark.conf.set("spark.databricks.adaptive.enabled", "true")
print("✅ Adaptive Query Execution (AQE) enabled")

# Broadcast join optimization
from pyspark.sql.functions import broadcast, col

users = spark.range(1000).selectExpr("id AS user_id", "concat('user_', id) AS name")
events = spark.table("demo.perf.events")

# Small table broadcast
joined = events.join(broadcast(users), "user_id")
print(f"\nBroadcast join executed: {joined.count()} rows")

# Explain plan shows BroadcastHashJoin
print("\nQuery plan (shows BroadcastHashJoin):")
joined.explain(True)

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Technique | Benefit |
# MAGIC |-----------|----------|
# MAGIC | **Liquid clustering** | Auto-optimized data layout, no manual tuning |
# MAGIC | **ZORDER** | Co-locate related data (legacy, use liquid clustering) |
# MAGIC | **Photon** | 2-5x faster execution (enabled by default) |
# MAGIC | **AQE** | Runtime query optimization, skew handling |
# MAGIC | **Broadcast joins** | Efficient small-table joins |
# MAGIC | **Caching** | `CACHE TABLE` for repeated queries |

# COMMAND ----------

