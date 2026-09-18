# Databricks notebook source
# DBTITLE 1,Cell 1: Setup & Overview
# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Medallion Architecture
# MAGIC
# MAGIC A demonstration of the medallion architecture pattern in Databricks, implementing Bronze, Silver, and Gold data layers using Delta Lake.
# MAGIC
# MAGIC ## Overview
# MAGIC - **Bronze Layer**: Raw data ingestion with minimal transformation
# MAGIC - **Silver Layer**: Cleaned and validated data
# MAGIC - **Gold Layer**: Business-level aggregations ready for analytics
# MAGIC
# MAGIC ## Unity Catalog Structure
# MAGIC ```
# MAGIC ops (catalog)
# MAGIC ├── bronze (schema) → customer_transactions
# MAGIC ├── silver (schema) → customer_transactions_cleaned
# MAGIC └── gold (schema)   → customer_spending
# MAGIC ```

# COMMAND ----------

# Cell 1: Import necessary libraries
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Cell 2: Bronze Layer — Raw Data
# Databricks notebook source
# Cell 2: Create sample data for the Bronze layer
# Create the catalog and schema if they don't exist
spark.sql("CREATE CATALOG IF NOT EXISTS ops")
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.bronze")

data = [
    (1, "John Doe", "2025-09-09", 100.0),
    (2, "Jane Smith", "2025-09-08", 150.0),
    (3, "Sam Brown", "2025-09-07", None)
]

columns = ["id", "name", "date", "amount"]
bronze_df = spark.createDataFrame(data, columns)
bronze_df.write.format("delta").mode("overwrite").saveAsTable("ops.bronze.customer_transactions")

print("✅ Bronze layer created: ops.bronze.customer_transactions")
bronze_df.display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Silver Layer — Data Cleaning
# Databricks notebook source
# Cell 3: Read data from the Bronze layer and perform data cleaning for the Silver layer
# Create the silver schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.silver")

bronze_df = spark.table("ops.bronze.customer_transactions")
silver_df = bronze_df.filter(col("amount").isNotNull())
silver_df.write.format("delta").mode("overwrite").saveAsTable("ops.silver.customer_transactions_cleaned")

print("✅ Silver layer created: ops.silver.customer_transactions_cleaned")
print(f"   Rows in bronze: {bronze_df.count()}, Rows in silver: {silver_df.count()} (null amounts filtered)")
silver_df.display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Gold Layer — Aggregation
# Databricks notebook source
# Cell 4: Read data from the Silver layer and perform aggregation for the Gold layer
# Create the gold schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.gold")

silver_df = spark.table("ops.silver.customer_transactions_cleaned")
gold_df = silver_df.groupBy("name").agg({"amount": "sum", "amount": "avg"}).withColumnRenamed("sum(amount)", "total_spent").withColumnRenamed("avg(amount)", "average_spent")
gold_df.write.format("delta").mode("overwrite").saveAsTable("ops.gold.customer_spending")

print("✅ Gold layer created: ops.gold.customer_spending")
print(f"   Aggregated by customer: total_spent and average_spent")

# COMMAND ----------

# DBTITLE 1,Cell 5: Display Results
# Databricks notebook source
# Cell 5: Display the Gold layer data
gold_df = spark.table("ops.gold.customer_spending")
print("📊 Gold Layer — Customer Spending Summary:")
gold_df.display()

print("\n📋 Data Flow Summary:")
print("   Bronze (3 rows, incl. null) → Silver (2 rows, filtered) → Gold (2 rows, aggregated)")
print("   Sam Brown filtered out in Silver due to null amount")

# COMMAND ----------

