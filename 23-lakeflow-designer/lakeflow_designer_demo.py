# Databricks notebook source
# DBTITLE 1,Overview
# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Designer — Visual Data Preparation
# MAGIC
# MAGIC **Use Case**: Learn Lakeflow Designer — Databricks' visual, no-code, AI-native data preparation tool.
# MAGIC
# MAGIC ## What is Lakeflow Designer?
# MAGIC
# MAGIC Lakeflow Designer is a visual canvas for analysts to perform data analytics, preparation, and basic automation — without writing code. You create **visual data preps** (designer files) made of operators arranged as a graph.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | **Visual data prep** | A file containing a sequence of operators arranged as a graph |
# MAGIC | **Source operator** | Brings data into the prep — tables, files, volumes, uploaded data |
# MAGIC | **Built-in operators** | Filter, Join, Aggregate, Reshape, Sort, Distinct, Union, Select |
# MAGIC | **User-defined operators** | Custom Python/SQL operators for complex transformations |
# MAGIC | **Genie Code** | Natural language prompts to generate/refine transformations |
# MAGIC | **Production-ready** | All transformations backed by code — version in Git, schedule as jobs |
# MAGIC
# MAGIC ## Designer vs Code-Based Pipeline
# MAGIC
# MAGIC | Aspect | Lakeflow Designer | Spark/SDP Pipeline |
# MAGIC |--------|-----------------|-------------------|
# MAGIC | Who | Analysts, citizen DE | Data engineers |
# MAGIC | How | Drag-and-drop canvas | Python/SQL code |
# MAGIC | AI assist | Genie Code (NL prompts) | Genie Code (chat) |
# MAGIC | Output | Production-ready code | Native code |
# MAGIC | Governance | Unity Catalog | Unity Catalog |
# MAGIC | Production | Git + Jobs + DAB | Jobs + DAB |

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.designer")
print("✅ Schema demo.designer created for Lakeflow Designer demos")

# COMMAND ----------

# DBTITLE 1,Cell 2: Designer Operators
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Understanding Designer Operators
# MAGIC
# MAGIC Lakeflow Designer provides built-in operators for common transformations. Here's how they map to Spark SQL equivalents.
# MAGIC
# MAGIC | Designer Operator | Spark SQL Equivalent | Description |
# MAGIC |-------------------|---------------------|-------------|
# MAGIC | **Source** | `SELECT * FROM table` | Read data from any UC source |
# MAGIC | **Filter** | `WHERE condition` | Row-level filtering |
# MAGIC | **Select** | `SELECT col1, col2` | Column selection |
# MAGIC | **Join** | `JOIN ON key` | Combine two sources |
# MAGIC | **Aggregate** | `GROUP BY + agg()` | Summarize data |
# MAGIC | **Sort** | `ORDER BY` | Sort rows |
# MAGIC | **Distinct** | `SELECT DISTINCT` | Remove duplicates |
# MAGIC | **Union** | `UNION ALL` | Stack datasets |
# MAGIC | **Reshape** | `PIVOT / UNPIVOT` | Reshape wide/long |
# MAGIC | **Custom** | Python/SQL UDF | User-defined operator |

# COMMAND ----------

from pyspark.sql.functions import col, count, avg, sum as spark_sum, desc

# Create sample data to demonstrate operator equivalents
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.designer.events AS
    SELECT
        id AS event_id,
        (id % 100) AS user_id,
        CASE WHEN id % 3 = 0 THEN 'click' WHEN id % 3 = 1 THEN 'view' ELSE 'purchase' END AS event_type,
        (rand() * 1000) AS value,
        date_add(date('2025-01-01'), cast(id % 365 as int)) AS event_date
    FROM range(10000)
""")

# Source operator equivalent
print("📋 Source: Read from demo.designer.events")
df = spark.table("demo.designer.events")
print(f"   Rows: {df.count()}")

# Filter operator equivalent
print("\n🔍 Filter: event_type = 'click'")
filtered = df.filter(col("event_type") == "click")
print(f"   Filtered rows: {filtered.count()}")

# Aggregate operator equivalent
print("\n📊 Aggregate: events per type")
agg = df.groupBy("event_type").agg(
    count("*").alias("event_count"),
    avg("value").alias("avg_value"),
    spark_sum("value").alias("total_value")
).orderBy(desc("event_count"))
agg.display()

# Join operator equivalent
print("\n🔗 Join: events + users")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.designer.users AS
    SELECT
        id AS user_id,
        CASE WHEN id % 2 = 0 THEN 'premium' ELSE 'standard' END AS tier,
        concat('user_', cast(id as string)) AS username
    FROM range(100)
""")
joined = df.join(spark.table("demo.designer.users"), "user_id", "left")
print(f"   Joined rows: {joined.count()}")
joined.groupBy("tier").agg(count("*").alias("events")).display()

# COMMAND ----------

# DBTITLE 1,Cell 3: SDK Designer File
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Creating a Visual Data Prep via SDK
# MAGIC
# MAGIC While Designer is primarily a visual tool, you can create designer files programmatically using the Databricks SDK. This enables CI/CD and automation.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
import json

w = WorkspaceClient()

# A visual data prep (designer file) is a JSON spec defining operators and connections
# This is a simplified example — in practice, use the Designer UI to create and refine

designer_spec = {
    "version": "1.0",
    "operators": [
        {
            "id": "source_1",
            "type": "source",
            "config": {
                "table": "demo.designer.events",
                "catalog": "demo",
                "schema": "designer"
            }
        },
        {
            "id": "filter_1",
            "type": "filter",
            "config": {
                "condition": "event_type = 'click' AND value > 100"
            },
            "inputs": ["source_1"]
        },
        {
            "id": "aggregate_1",
            "type": "aggregate",
            "config": {
                "groupBy": ["user_id"],
                "aggregations": [
                    {"column": "value", "function": "sum", "alias": "total_click_value"},
                                       {"column": "*", "function": "count", "alias": "click_count"}
                ]
            },
            "inputs": ["filter_1"]
        },
        {
            "id": "sink_1",
            "type": "sink",
            "config": {
                "table": "demo.designer.click_summary",
                "mode": "overwrite"
            },
            "inputs": ["aggregate_1"]
        }
    ]
}

print("📋 Visual data prep spec (JSON):")
print(json.dumps(designer_spec, indent=2))
print("\n💡 In practice: Create designer files in the Lakeflow Designer UI.")
print("   Use Git versioning and Jobs scheduling for production deployment.")
print("   Use DAB (Declarative Automation Bundles) for CI/CD pipeline promotion.")

# COMMAND ----------

# DBTITLE 1,Cell 4: Genie Code
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Genie Code — Natural Language Transforms
# MAGIC
# MAGIC Genie Code is integrated into Lakeflow Designer. You can type natural language prompts to generate or refine transformations.
# MAGIC
# MAGIC **Examples of Genie Code prompts:**
# MAGIC - "Filter events where value is above 500 and event_type is purchase"
# MAGIC - "Group by user_id and calculate total value per user"
# MAGIC - "Join with users table and show only premium users"
# MAGIC - "Create a new column 'revenue_category' based on value ranges"
# MAGIC
# MAGIC Genie Code generates the equivalent Spark SQL/Python code, which you can review and modify.

# COMMAND ----------

# Example: What Genie Code would generate for a natural language prompt
# Prompt: "Create revenue_category based on value: <100=low, 100-500=medium, >500=high"

from pyspark.sql.functions import when, col

events = spark.table("demo.designer.events")
events_categorized = events.withColumn(
    "revenue_category",
    when(col("value") < 100, "low")
    .when(col("value") < 500, "medium")
    .otherwise("high")
)

print("✅ Genie Code equivalent: Created revenue_category column")
events_categorized.groupBy("revenue_category").count().display()

# Another example: "Show top 5 users by total event value"
print("\n✅ Genie Code equivalent: Top 5 users by total value")
top_users = (events.groupBy("user_id")
    .agg(spark_sum("value").alias("total_value"))
    .orderBy(desc("total_value"))
    .limit(5))
top_users.display()

# COMMAND ----------

# DBTITLE 1,Cell 5: Production Deployment
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Production Deployment of Visual Data Preps
# MAGIC
# MAGIC Visual data preps are production-ready — they generate code that can be versioned, scheduled, and deployed.
# MAGIC
# MAGIC **Production workflow:**
# MAGIC 1. **Create** visual data prep in Designer UI
# MAGIC 2. **Review** generated code (Spark SQL/Python)
# MAGIC 3. **Version** in Git folder (commits the designer file)
# MAGIC 4. **Schedule** as a Lakeflow Job task
# MAGIC 5. **Deploy** via DAB for dev → staging → prod promotion
# MAGIC 6. **Monitor** via system tables and data quality monitoring

# COMMAND ----------

# Demonstrate the code that a visual data prep generates
print("🔧 Generated code from visual data prep (equivalent Spark SQL):\n")

generated_sql = """
-- Generated by Lakeflow Designer from visual data prep
-- Source: demo.designer.events
-- Filter: event_type = 'click' AND value > 100
-- Aggregate: GROUP BY user_id
-- Sink: demo.designer.click_summary

CREATE OR REPLACE TABLE demo.designer.click_summary AS
SELECT
    user_id,
    SUM(value) AS total_click_value,
    COUNT(*) AS click_count
FROM demo.designer.events
WHERE event_type = 'click' AND value > 100
GROUP BY user_id
ORDER BY total_click_value DESC;
"""
print(generated_sql)

# Execute the generated code
spark.sql(generated_sql.strip())
result = spark.table("demo.designer.click_summary")
print(f"✅ Generated table demo.designer.click_summary: {result.count()} rows")
result.display()

# DAB snippet for scheduling the visual data prep as a job
print("\n📦 DAB snippet (databricks.yml) for scheduling:")
dab_snippet = """
resources:
  jobs:
    designer_data_prep_job:
      name: "Designer Data Prep — Click Summary"
      tasks:
        - task_key: "run_designer_prep"
          job_cluster_key: "serverless"
          notebook_task:
            notebook_path: "./designer_prep.ipynb"
      job_clusters:
        - job_cluster_key: "serverless"
          new_cluster:
            spark_version: "auto"
            """
print(dab_snippet)

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|----------|
# MAGIC | **Visual canvas** | Drag-and-drop data prep — no code needed |
# MAGIC | **Built-in operators** | Filter, Join, Aggregate, Sort, Reshape — common transforms |
# MAGIC | **Genie Code** | Natural language transforms — AI generates code |
# MAGIC | **Production-ready** | All transforms backed by code — Git, Jobs, DAB |
# MAGIC | **Unity Catalog** | Full governance — lineage, access control, auditing |
# MAGIC
# MAGIC ## When to use Lakeflow Designer
# MAGIC 1. **Analyst self-service** — let analysts prep data without engineering support
# MAGIC 2. **Rapid prototyping** — visually explore transforms before writing production code
# MAGIC 3. **Hybrid teams** — analysts use Designer, engineers use SDP/Spark — both produce governed code
# MAGIC 4. **No-code pipelines** — visual data preps scheduled as production jobs

# COMMAND ----------

