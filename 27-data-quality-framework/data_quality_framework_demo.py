# Databricks notebook source
# DBTITLE 1,Setup
# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework
# MAGIC
# MAGIC Comprehensive data quality patterns on Databricks.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS demo.dq")
print("Schema created")

# COMMAND ----------

# DBTITLE 1,Delta Constraints
# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Constraints & Data Contracts

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.dq.orders AS
    SELECT id AS order_id, rand() * 1000 AS amount, 'pending' AS status FROM range(1000)
""")

# Add CHECK constraint
spark.sql("ALTER TABLE demo.dq.orders ADD CONSTRAINT amount_positive CHECK (amount >= 0)")
spark.sql("ALTER TABLE demo.dq.orders ADD CONSTRAINT valid_status CHECK (status IN ('pending', 'confirmed', 'shipped'))")
print("✅ Constraints added")
spark.sql("DESCRIBE DETAIL demo.dq.orders").select("constraints").display()

# COMMAND ----------

# DBTITLE 1,Profiling & Anomalies
# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Profiling & Anomaly Detection

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg, min, max, stddev

df = spark.table("demo.dq.orders")
profile = df.select(
    count("*").alias("total_rows"),
    count("amount").alias("non_null_amount"),
    avg("amount").alias("avg_amount"),
    stddev("amount").alias("stddev_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
)
print("Data Profile:")
profile.display()

# Anomaly detection - z-score
from pyspark.sql.functions import abs as spark_abs
stats = profile.collect()[0]
mean_val = stats.avg_amount
std_val = stats.stddev_amount

anomalies = df.withColumn(
    "z_score",
    spark_abs((col("amount") - mean_val) / std_val)
).filter(col("z_score") > 3)
print(f"\nAnomalies (z-score > 3): {anomalies.count()} rows")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Use |
# MAGIC |---------|-----|
# MAGIC | **Delta constraints** | CHECK, NOT NULL, PK/FK enforcement |
# MAGIC | **Data profiling** | Summary stats, null analysis |
# MAGIC | **Anomaly detection** | Statistical outlier detection |
# MAGIC | **DQ monitoring** | Unity Catalog DQ Monitoring (Module 13) |
# MAGIC | **SDP expectations** | Pipeline-level DQ checks (Module 04) |

# COMMAND ----------

