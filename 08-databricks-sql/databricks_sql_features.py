# Databricks notebook source
# DBTITLE 1,Databricks SQL — Overview
# MAGIC %md
# MAGIC # Databricks SQL: Warehouses, Views & Query Optimization
# MAGIC
# MAGIC **Use Case**: Master Databricks SQL features for analytics, reporting, and query performance.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **Views** | Standard, temporary, and global temporary views |
# MAGIC | **Materialized Views** | Pre-computed results with scheduled refresh |
# MAGIC | **Streaming Tables** | DBSQL streaming ingestion with `STREAMING TABLE` |
# MAGIC | **Window Functions** | RANK, DENSE_RANK, LAG, LEAD, running totals |
# MAGIC | **CTEs** | Common Table Expressions for readable queries |
# MAGIC | **PIVOT / UNPIVOT** | Reshape data between wide and long formats |
# MAGIC | **Delta Lake SQL** | MERGE, CDF, Time Travel via SQL syntax |
# MAGIC | **AI Functions** | `ai_query`, `ai_forecast`, `ai_parse_document` |
# MAGIC | **Query Optimization** | EXPLAIN, partition pruning, liquid clustering |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schema, Sample Tables

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.sql_demo")

# Drop existing tables for clean re-run
for t in ["sales", "customers", "products", "sales_mv"]:
    spark.sql(f"DROP TABLE IF EXISTS demo.sql_demo.{t}")

# --- Customers table ---
customers = [
    (1, "Acme Corp", "Enterprise", "US", "2023-01-15"),
    (2, "TechStart",  "Startup",   "US", "2023-06-20"),
    (3, "EuroShop",   "SMB",       "EU", "2023-03-10"),
    (4, "AsiaTech",   "Enterprise", "APAC", "2023-09-01"),
    (5, "LocalBiz",   "SMB",       "US", "2024-01-05"),
]
spark.createDataFrame(customers, ["customer_id", "company", "tier", "region", "signup_date"]) \
    .write.format("delta").saveAsTable("demo.sql_demo.customers")

# --- Products table ---
products = [
    (101, "Cloud Storage",   "Infrastructure", 50.0),
    (102, "Analytics Pro",    "Analytics",    200.0),
    (103, "Security Suite",  "Security",    150.0),
    (104, "ML Platform",     "AI/ML",       500.0),
    (105, "API Gateway",     "Infrastructure", 25.0),
]
spark.createDataFrame(products, ["product_id", "product_name", "category", "unit_price"]) \
    .write.format("delta").saveAsTable("demo.sql_demo.products")

# --- Sales table ---
import random
sales_rows = []
for i in range(1, 201):
    cust = random.randint(1, 5)
    prod = random.choice([101, 102, 103, 104, 105])
    qty = random.randint(1, 10)
    date = f"2025-{random.randint(1,9):02d}-{random.randint(1,28):02d}"
    sales_rows.append((i, cust, prod, qty, date, random.choice(["completed", "pending", "cancelled"])))

spark.createDataFrame(sales_rows, ["order_id", "customer_id", "product_id", "quantity", "order_date", "status"]) \
    .write.format("delta").saveAsTable("demo.sql_demo.sales")

print("✅ Tables created: customers (5), products (5), sales (200)")

# COMMAND ----------

# DBTITLE 1,Cell 2: Views
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Views — Standard, Temporary, Global Temp

# COMMAND ----------

# --- Standard View (persisted in catalog) ---
spark.sql("""
    CREATE OR REPLACE VIEW demo.sql_demo.v_active_sales AS
    SELECT 
        s.order_id, s.customer_id, c.company, c.tier, c.region,
        p.product_name, p.category, s.quantity, s.order_date, s.status,
        (s.quantity * p.unit_price) AS revenue
    FROM demo.sql_demo.sales s
    JOIN demo.sql_demo.customers c ON s.customer_id = c.customer_id
    JOIN demo.sql_demo.products p ON s.product_id = p.product_id
    WHERE s.status = 'completed'
""")
print("✅ Standard view: demo.sql_demo.v_active_sales")
spark.sql("SELECT * FROM demo.sql_demo.v_active_sales LIMIT 5").display()

# --- Temporary View (session-scoped) ---
spark.sql("""
    CREATE TEMP VIEW tv_revenue_by_tier AS
    SELECT tier, ROUND(SUM(revenue), 2) as total_revenue, COUNT(*) as order_count
    FROM demo.sql_demo.v_active_sales
    GROUP BY tier
    ORDER BY total_revenue DESC
""")
print("\n✅ Temp view: tv_revenue_by_tier (session-scoped)")
spark.sql("SELECT * FROM tv_revenue_by_tier").display()

# --- Global Temp View (cross-session) ---
spark.sql("""
    CREATE GLOBAL TEMP VIEW gv_category_sales AS
    SELECT category, COUNT(*) as order_count, ROUND(AVG(revenue), 2) as avg_revenue
    FROM demo.sql_demo.v_active_sales
    GROUP BY category
    ORDER BY avg_revenue DESC
""")
print("\n✅ Global temp view: gv_category_sales (cross-session)")
spark.sql("SELECT * FROM global_temp.gv_category_sales").display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Materialized Views
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Materialized Views — Pre-computed & Scheduled
# MAGIC
# MAGIC Materialized views store results and refresh on a schedule or on demand.
# MAGIC Requires a SQL warehouse.

# COMMAND ----------

# Create a materialized view
spark.sql("""
    CREATE MATERIALIZED VIEW demo.sql_demo.sales_mv
    AS
    SELECT 
        c.region,
        c.tier,
        p.category,
        DATE_TRUNC('month', s.order_date) AS order_month,
        COUNT(*) AS order_count,
        SUM(s.quantity) AS total_quantity,
        ROUND(SUM(s.quantity * p.unit_price), 2) AS total_revenue
    FROM demo.sql_demo.sales s
    JOIN demo.sql_demo.customers c ON s.customer_id = c.customer_id
    JOIN demo.sql_demo.products p ON s.product_id = p.product_id
    WHERE s.status = 'completed'
    GROUP BY c.region, c.tier, p.category, DATE_TRUNC('month', s.order_date)
""")

print("✅ Materialized view: demo.sql_demo.sales_mv")
print("   Refresh with: ALTER MATERIALIZED VIEW demo.sql_demo.sales_mv REFRESH")
spark.sql("SELECT * FROM demo.sql_demo.sales_mv ORDER BY total_revenue DESC LIMIT 10").display()

# Schedule refresh (would run on a SQL warehouse)
# spark.sql("ALTER MATERIALIZED VIEW demo.sql_demo.sales_mv SCHEDULE EVERY 1 HOUR")

# COMMAND ----------

# DBTITLE 1,Cell 4: Window Functions
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Window Functions — Ranking, Lag/Lead, Running Totals

# COMMAND ----------

# Window functions for analytics
result = spark.sql("""
    SELECT 
        c.company,
        c.tier,
        p.product_name,
        s.order_date,
        s.quantity * p.unit_price AS revenue,
        
        -- Ranking within each customer's orders
        RANK() OVER (PARTITION BY s.customer_id ORDER BY s.quantity * p.unit_price DESC) AS revenue_rank,
        
        -- Dense rank (no gaps)
        DENSE_RANK() OVER (PARTITION BY s.customer_id ORDER BY s.quantity * p.unit_price DESC) AS revenue_dense_rank,
        
        -- Previous order revenue
        LAG(s.quantity * p.unit_price, 1) OVER (PARTITION BY s.customer_id ORDER BY s.order_date) AS prev_revenue,
        
        -- Next order revenue
        LEAD(s.quantity * p.unit_price, 1) OVER (PARTITION BY s.customer_id ORDER BY s.order_date) AS next_revenue,
        
        -- Running total per customer
        SUM(s.quantity * p.unit_price) OVER (PARTITION BY s.customer_id ORDER BY s.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
        
        -- 3-order moving average
        AVG(s.quantity * p.unit_price) OVER (PARTITION BY s.customer_id ORDER BY s.order_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3,
        
        -- Percent of customer total
        ROUND(s.quantity * p.unit_price * 100.0 / 
            SUM(s.quantity * p.unit_price) OVER (PARTITION BY s.customer_id), 2) AS pct_of_total
    FROM demo.sql_demo.sales s
    JOIN demo.sql_demo.customers c ON s.customer_id = c.customer_id
    JOIN demo.sql_demo.products p ON s.product_id = p.product_id
    WHERE s.status = 'completed'
    ORDER BY c.company, s.order_date
""")

print("✅ Window Functions — RANK, DENSE_RANK, LAG, LEAD, Running Total, Moving Average")
result.display()

# COMMAND ----------

# DBTITLE 1,Cell 5: CTEs & PIVOT
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: CTEs & PIVOT

# COMMAND ----------

# CTE for readable multi-step queries
spark.sql("""
    WITH monthly_revenue AS (
        SELECT 
            c.region,
            DATE_TRUNC('month', s.order_date) AS month,
            SUM(s.quantity * p.unit_price) AS revenue
        FROM demo.sql_demo.sales s
        JOIN demo.sql_demo.customers c ON s.customer_id = c.customer_id
        JOIN demo.sql_demo.products p ON s.product_id = p.product_id
        WHERE s.status = 'completed'
        GROUP BY c.region, DATE_TRUNC('month', s.order_date)
    ),
    region_totals AS (
        SELECT region, SUM(revenue) AS total_revenue
        FROM monthly_revenue
        GROUP BY region
    ),
    ranked AS (
        SELECT 
            r.region,
            r.month,
            r.revenue,
            rt.total_revenue,
            ROUND(r.revenue * 100.0 / rt.total_revenue, 2) AS pct_of_region_total
        FROM monthly_revenue r
        JOIN region_totals rt ON r.region = rt.region
    )
    SELECT * FROM ranked
    ORDER BY region, month
""").display()

# PIVOT — reshape rows to columns
print("\n✅ PIVOT — Revenue by Region per Category:")
spark.sql("""
    SELECT *
    FROM (
        SELECT c.region, p.category, s.quantity * p.unit_price AS revenue
        FROM demo.sql_demo.sales s
        JOIN demo.sql_demo.customers c ON s.customer_id = c.customer_id
        JOIN demo.sql_demo.products p ON s.product_id = p.product_id
        WHERE s.status = 'completed'
    )
    PIVOT (
        SUM(revenue) AS total_revenue
        FOR region IN ('US', 'EU', 'APAC')
    )
    ORDER BY category
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 6: Delta Lake SQL
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Delta Lake SQL — MERGE, CDF, Time Travel

# COMMAND ----------

# Delta MERGE in SQL — upsert new sales
spark.sql("""
    MERGE INTO demo.sql_demo.sales AS target
    USING (
        SELECT 999 AS order_id, 1 AS customer_id, 104 AS product_id, 
               5 AS quantity, '2025-09-30' AS order_date, 'completed' AS status
    ) AS src
    ON target.order_id = src.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("✅ Delta MERGE: upserted order_id=999")

# Enable CDF and view changes
spark.sql("ALTER TABLE demo.sql_demo.sales SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql("INSERT INTO demo.sql_demo.sales VALUES (1000, 2, 102, 3, '2025-09-29', 'pending')")

print("\n📊 Change Data Feed:")
spark.sql("""
    SELECT _change_type, order_id, customer_id, product_id, status, _commit_version
    FROM table_changes('demo.sql_demo.sales', 'earliest')
    ORDER BY _commit_version DESC
    LIMIT 10
""").display()

# Time Travel
print("\n🕐 Time Travel — Original sales data (before MERGE):")
spark.sql("SELECT COUNT(*) AS original_count FROM demo.sql_demo.sales VERSION AS OF 0").display()
print(f"   Current count: {spark.table('demo.sql_demo.sales').count()}")

# COMMAND ----------

# DBTITLE 1,Cell 7: AI Functions in SQL
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: AI Functions in SQL
# MAGIC
# MAGIC Databricks SQL provides built-in AI functions for LLM-powered analytics.

# COMMAND ----------

# ai_query — query an LLM endpoint directly from SQL
# Requires a serving endpoint (commented out for demo)
# spark.sql("""
#     SELECT 
#         company,
#         tier,
#         ai_query('databricks-dbrx-instruct', 
#                  CONCAT('Classify this customer tier: ', tier)) AS ai_classification
#     FROM demo.sql_demo.customers
# """).display()

# ai_forecast — time series forecasting
print("📊 ai_forecast — Predict next month's revenue:")
spark.sql("""
    WITH daily_revenue AS (
        SELECT 
            order_date AS ds,
            CAST(SUM(quantity * 50) AS DOUBLE) AS y  -- simplified revenue
        FROM demo.sql_demo.sales
        WHERE status = 'completed'
        GROUP BY order_date
    )
    SELECT * FROM AI_FORECAST(
        daily_revenue,
        horizon => 7,
        frequency => 'D',
        time_col => 'ds',
        value_col => 'y'
    )
""").display()

print("\n💡 AI Functions available in Databricks SQL:")
print("   - ai_query(endpoint, prompt)     — query LLM endpoints")
print("   - ai_forecast(table, horizon)     — time series forecasting")
print("   - ai_analyze_sentiment(text)     — sentiment analysis")
print("   - ai_classify(text, labels)       — text classification")
print("   - ai_extract_answer(text, quest)  — Q&A extraction")
print("   - ai_summarize(text)              — text summarization")
print("   - ai_translate(text, language)    — translation")

# COMMAND ----------

# DBTITLE 1,Cell 8: Query Optimization
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Query Optimization — EXPLAIN, Liquid Clustering

# COMMAND ----------

# EXPLAIN — understand query execution plan
print("📋 EXPLAIN — Query Execution Plan:")
explain_result = spark.sql("""
    EXPLAIN EXTENDED
    SELECT c.region, p.category, SUM(s.quantity * p.unit_price) AS revenue
    FROM demo.sql_demo.sales s
    JOIN demo.sql_demo.customers c ON s.customer_id = c.customer_id
    JOIN demo.sql_demo.products p ON s.product_id = p.product_id
    WHERE s.status = 'completed' AND c.region = 'US'
    GROUP BY c.region, p.category
""")
print(explain_result.collect()[0][0][:2000])

# Liquid Clustering (replaces partitioning + ZORDER)
spark.sql("""
    CREATE OR REPLACE TABLE demo.sql_demo.sales_clustered
    USING DELTA
    CLUSTER BY (customer_id, order_date)
    AS SELECT * FROM demo.sql_demo.sales
""")
print("\n✅ Liquid Clustering table created: demo.sql_demo.sales_clustered")
print("   CLUSTER BY (customer_id, order_date) — adaptive, self-optimizing")

# Compare: traditional partitioning vs liquid clustering
print("""
📋 Optimization Strategies:

  | Strategy              | When to Use                          | Benefit                          |
  |-----------------------|--------------------------------------|----------------------------------|
  | Partitioning          | Known, low-cardinality columns       | Partition pruning                |
  | ZORDER (OPTIMIZE)     | High-cardinality filter columns      | Data co-location, skip scanning  |
  | Liquid Clustering     | Multiple filter columns, evolving    | Adaptive, no manual OPTIMIZE     |
  | Deletion Vectors      | Frequent UPDATE/DELETE              | Faster writes, lazy deletion      |
  | Predictive Optimizer  | All Delta tables (serverless)       | Auto-tune OPTIMIZE + VACUUM       |
""")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Best For |
# MAGIC |---------|----------|
# MAGIC | **Views** | Reusable query logic, access control layer |
# MAGIC | **Materialized Views** | Expensive aggregations refreshed on schedule |
# MAGIC | **Streaming Tables** | DBSQL-native streaming ingestion |
# MAGIC | **Window Functions** | Rankings, running totals, time-series analysis |
# MAGIC | **CTEs** | Readable multi-step queries (no temp tables) |
# MAGIC | **PIVOT** | Converting rows to columns for reporting |
# MAGIC | **Delta SQL** | MERGE, CDF, Time Travel — all via SQL syntax |
# MAGIC | **AI Functions** | LLM-powered analytics without Python |
# MAGIC | **Liquid Clustering** | Multi-column filtering without manual OPTIMIZE |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Use materialized views** for dashboards that query large aggregations — refresh on schedule
# MAGIC 2. **Prefer CTEs** over nested subqueries for readability and maintainability
# MAGIC 3. **Use window functions** instead of self-joins for ranking and running totals
# MAGIC 4. **Enable CDF** before you need it — cannot retroactively enable
# MAGIC 5. **Use Liquid Clustering** over partitioning + ZORDER for new tables with multiple filter columns
# MAGIC 6. **Run `EXPLAIN`** to identify full table scans and missing optimizations

# COMMAND ----------

