# Databricks notebook source
# DBTITLE 1,AI Platform — Overview
# MAGIC %md
# MAGIC # Databricks AI Platform: Raw Data to LLM Inference
# MAGIC
# MAGIC **Use Case**: Build a complete AI platform on Databricks — from raw data ingestion through data processing, feature engineering, traditional ML, and all the way to LLM inference with RAG.
# MAGIC
# MAGIC ## What Makes Databricks an AI Platform?
# MAGIC
# MAGIC Databricks is not just a data warehouse or a notebook environment — it is a **unified data and AI platform** that covers the entire lifecycle:
# MAGIC
# MAGIC | Layer | Capability | Databricks Feature |
# MAGIC |------|-----------|-------------------|
# MAGIC | **Data ingestion** | Structured + unstructured data from any source | Auto Loader, Lakeflow Connect, Delta Lake |
# MAGIC | **Data processing** | ETL/ELT, streaming, batch, medallion architecture | Spark, Delta Lake, SDP |
# MAGIC | **Feature engineering** | Feature Store, point-in-time joins, online serving | Feature Store, Feature Engineering Client |
# MAGIC | **Traditional ML** | Training, tracking, tuning, registry, serving | MLflow, Model Registry, Model Serving |
# MAGIC | **Generative AI** | Embeddings, Vector Search, RAG, LLM serving | Vector Search, Foundation Model APIs, AI Gateway |
# MAGIC | **Governance** | Unified governance across data + AI assets | Unity Catalog, tags, lineage, audit |
# MAGIC | **Orchestration** | Multi-task workflows, CI/CD, scheduling | Lakeflow Jobs, DAB |
# MAGIC | **Monitoring** | Data quality, model quality, cost tracking | Lakehouse Monitoring, System Tables |
# MAGIC
# MAGIC ## AI Platform Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TD
# MAGIC     subgraph Ingestion [1. Data Ingestion]
# MAGIC         RAW_S[Structured Sources<br/>Transactions, Events] --> BRONZE
# MAGIC         RAW_U[Unstructured Sources<br/>Tickets, Reviews, Docs] --> BRONZE
# MAGIC     end
# MAGIC
# MAGIC     subgraph Processing [2. Data Processing — Medallion]
# MAGIC         BRONZE[Bronze Layer<br/>Raw Delta Tables] --> SILVER[Silver Layer<br/>Cleaned, Deduplicated]
# MAGIC         SILVER --> GOLD[Gold Layer<br/>Features, Aggregates, Chunks]
# MAGIC     end
# MAGIC
# MAGIC     subgraph ML [3. Traditional ML]
# MAGIC         GOLD --> FE[Feature Engineering<br/>User/Item Features]
# MAGIC         FE --> TRAIN[Model Training<br/>MLflow Tracking]
# MAGIC         TRAIN --> REG[Model Registry<br/>& Serving]
# MAGIC     end
# MAGIC
# MAGIC     subgraph GenAI [4. Generative AI]
# MAGIC         GOLD --> EMB[Embeddings<br/>ai_query / SDK]
# MAGIC         EMB --> VS[Vector Search<br/>Endpoint + Index]
# MAGIC         VS --> RAG[RAG Pipeline<br/>Retrieve + Augment + Generate]
# MAGIC         GOLD --> FT[LLM Data Prep<br/>Chunks, Prompts, Datasets]
# MAGIC     end
# MAGIC
# MAGIC     subgraph Serving [5. LLM Inference]
# MAGIC         RAG --> LLM[LLM Serving<br/>Foundation Model APIs]
# MAGIC         LLM --> GW[AI Gateway<br/>Rate Limit, Fallback, Log]
# MAGIC         REG --> SERVE[Model Serving<br/>Real-time Endpoint]
# MAGIC     end
# MAGIC
# MAGIC     subgraph Gov [6. Governance & Orchestration]
# MAGIC         UC[Unity Catalog<br/>Governs All Assets]
# MAGIC         JOB[Lakeflow Jobs<br/>Orchestrates Pipeline]
# MAGIC         MON[Monitoring<br/>Quality + Cost]
# MAGIC     end
# MAGIC
# MAGIC     Gov -.-> Ingestion
# MAGIC     Gov -.-> Processing
# MAGIC     Gov -.-> ML
# MAGIC     Gov -.-> GenAI
# MAGIC     Gov -.-> Serving
# MAGIC ```
# MAGIC
# MAGIC ## What This Module Covers
# MAGIC
# MAGIC | Stage | Feature | Description |
# MAGIC |-------|---------|-------------|
# MAGIC | **1. Raw Data** | Delta tables, UC volumes | Ingest structured + unstructured data into the lakehouse |
# MAGIC | **2. Processing** | Medallion (Bronze/Silver/Gold) | Clean, deduplicate, transform — both structured and text data |
# MAGIC | **3. Features** | Feature engineering, aggregation | Build ML features + prepare text chunks for GenAI |
# MAGIC | **4. Traditional ML** | MLflow, Model Registry | Train, log, register, and serve a churn prediction model |
# MAGIC | **5. LLM Data Prep** | Document chunking, prompt formatting | Prepare data for RAG and LLM fine-tuning |
# MAGIC | **6. Embeddings & VS** | Vector Search endpoints, indexes | Generate embeddings, create similarity search infrastructure |
# MAGIC | **7. RAG Pipeline** | Retrieve → Augment → Generate | Full RAG flow with Vector Search + LLM inference |
# MAGIC | **8. LLM Serving** | Foundation Model APIs, batch inference | Deploy and call LLM endpoints for production inference |
# MAGIC | **9. AI Gateway** | Rate limiting, fallback, usage tracking | Govern LLM endpoints with routing and safety controls |
# MAGIC | **10. Orchestration** | Lakeflow Jobs, DAB | Tie the full pipeline together with scheduling and CI/CD |
# MAGIC
# MAGIC ## How This Module Relates to Others
# MAGIC
# MAGIC | Module | Focus | This Module's Role |
# MAGIC |--------|-------|--------------------|
# MAGIC | 01 (Medallion) | Bronze/Silver/Gold basics | Uses medallion for AI data pipeline |
# MAGIC | 07 (MLflow) | ML tracking deep-dive | Lightweight ML training for platform demo |
# MAGIC | 09 (Feature Store) | Feature engineering deep-dive | Feature engineering as part of AI pipeline |
# MAGIC | 10 (GenAI/RAG) | Vector Search & RAG deep-dive | RAG as one stage in the full AI platform |
# MAGIC | 11 (E2E ML) | ML pipeline end-to-end | Extends to GenAI/LLM inference |
# MAGIC | 12 (Jobs/DAB) | Orchestration deep-dive | Orchestration for the AI platform |
# MAGIC
# MAGIC > This module is the **platform view** — it shows how all Databricks AI capabilities work together as a unified system. Individual modules go deep on each area.

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup — Catalog, Schema, Volume
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schema, Volume
# MAGIC
# MAGIC Create the Unity Catalog infrastructure for our AI platform demo.

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.ai_platform")
spark.sql("CREATE VOLUME IF NOT EXISTS demo.ai_platform.source_data")

print("✅ AI Platform setup complete:")
print("   Catalog:  demo")
print("   Schema:   demo.ai_platform")
print("   Volume:   demo.ai_platform.source_data (for raw files)")
print()
print("📋 This schema will hold:")
print("   • Bronze tables  — raw ingested data (structured + unstructured)")
print("   • Silver tables  — cleaned, deduplicated, standardized")
print("   • Gold tables    — ML features, text chunks, analytics")
print("   • Vector indexes — embeddings for similarity search")
print("   • ML artifacts   — models, experiments")

# COMMAND ----------

# DBTITLE 1,Cell 2: Raw Data Ingestion
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Raw Data Ingestion — Structured + Unstructured
# MAGIC
# MAGIC In a real AI platform, data comes from many sources:
# MAGIC - **Structured**: Transaction databases, event logs, CRM exports
# MAGIC - **Unstructured**: Support tickets, customer reviews, product documentation, emails
# MAGIC
# MAGIC We simulate both types as Delta tables in the Bronze layer.

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
import random

# --- 2a: Structured data — customer transactions ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.bronze_transactions")

transactions = []
for i in range(1, 5001):
    txn_type = random.choice(["purchase", "refund", "subscription", "upgrade"])
    amount = round(random.uniform(10, 500), 2) if txn_type != "refund" else round(random.uniform(5, 200), 2)
    channel = random.choice(["web", "mobile", "in-store", "partner"])
    status = random.choices(["completed", "pending", "failed"], weights=[85, 10, 5])[0]
    transactions.append((
        i,                              # transaction_id
        random.randint(1, 1000),        # customer_id
        txn_type,                        # transaction_type
        amount,                          # amount
        channel,                         # channel
        status,                          # status
        f"2026-{random.randint(1, 9):02d}-{random.randint(1, 28):02d}"  # txn_date
    ))

txn_df = spark.createDataFrame(
    transactions,
    ["transaction_id", "customer_id", "transaction_type", "amount", "channel", "status", "txn_date"]
)
txn_df.write.mode("overwrite").saveAsTable("demo.ai_platform.bronze_transactions")

# --- 2b: Structured data — customer profiles ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.bronze_customers")

customers = []
for i in range(1, 1001):
    plan = random.choices(["free", "starter", "pro", "enterprise"], weights=[40, 30, 20, 10])[0]
    region = random.choice(["NA", "EMEA", "APAC", "LATAM"])
    signup_days = random.randint(30, 1095)
    customers.append((
        i,                                    # customer_id
        f"customer_{i}",                       # customer_name
        f"user{i}@example.com",                # email
        plan,                                 # plan_tier
        region,                               # region
        signup_days,                          # tenure_days
        random.choice(["active", "inactive", "churned"])  # status
    ))

cust_df = spark.createDataFrame(
    customers,
    ["customer_id", "customer_name", "email", "plan_tier", "region", "tenure_days", "status"]
)
cust_df.write.mode("overwrite").saveAsTable("demo.ai_platform.bronze_customers")

# --- 2c: Unstructured data — support tickets ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.bronze_tickets")

ticket_subjects = [
    "Cannot log in after password reset",
    "Dashboard not loading — getting timeout errors",
    "Need help integrating API with our backend",
    "Billing discrepancy on latest invoice",
    "Feature request: dark mode for analytics",
    "Data export to CSV failing for large datasets",
    "Slow query performance on Delta tables",
    "How to set up Unity Catalog permissions?",
    "Stream job failing with checkpoint error",
    "MLflow model deployment stuck in pending",
]
ticket_descriptions = [
    "I reset my password twice but still cannot access the workspace. The login page shows a generic error. This is blocking our team's work.",
    "The analytics dashboard takes 30+ seconds to load. We have ~50 widgets. Other users are experiencing the same issue. Please investigate.",
    "We need to integrate the Databricks API with our Node.js backend. Looking for guidance on authentication and rate limits for the REST API.",
    "Our latest invoice shows charges for a premium cluster we never used. The cluster was auto-terminated but we were still billed. Need a refund.",
    "Dark mode would significantly improve our team's experience. Currently the bright UI causes eye strain during long sessions.",
    "When exporting more than 100K rows to CSV, the export fails silently. Smaller exports work fine. Using serverless compute.",
    "Queries on our Delta table with 500M rows are taking 5+ minutes even after OPTIMIZE and ZORDER. Looking for optimization guidance.",
    "We want to set up row-level security so regional teams only see their own data. How do we configure this in Unity Catalog?",
    "Our structured streaming job keeps failing with checkpoint corruption. We have tried clearing the checkpoint but it recurs every few days.",
    "Our MLflow model serving endpoint has been pending for 20 minutes. No error logs visible. Workload type is CPU, Small.",
]

tickets = []
for i in range(1, 501):
    idx = random.randint(0, len(ticket_subjects) - 1)
    priority = random.choices(["low", "medium", "high", "urgent"], weights=[30, 40, 20, 10])[0]
    tickets.append((
        i,                                    # ticket_id
        random.randint(1, 1000),              # customer_id
        ticket_subjects[idx],                 # subject
        ticket_descriptions[idx],             # description
        priority,                             # priority
        random.choice(["open", "resolved", "escalated"]),  # status
        f"2026-{random.randint(1, 9):02d}-{random.randint(1, 28):02d}"  # created_date
    ))

ticket_df = spark.createDataFrame(
    tickets,
    ["ticket_id", "customer_id", "subject", "description", "priority", "status", "created_date"]
)
ticket_df.write.mode("overwrite").saveAsTable("demo.ai_platform.bronze_tickets")

# --- 2d: Unstructured data — knowledge base articles ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.bronze_knowledge_base")

kb_articles = [
    (1, "Delta Lake Optimization Guide", "Delta Lake performance optimization starts with file compaction using OPTIMIZE. For query acceleration, use ZORDER BY on frequently filtered columns. Liquid Clustering automatically maintains data layout without manual OPTIMIZE. Deletion vectors improve DELETE and UPDATE performance by avoiding full rewrites. VACUUM removes old file versions but retains files within the retention period.", "data-engineering"),
    (2, "Unity Catalog Setup", "Unity Catalog provides centralized governance for all data and AI assets. Create catalogs for major domains, schemas within catalogs for sub-domains. Use GRANT statements for fine-grained access control: GRANT SELECT ON TABLE. Apply tags for classification and discovery. Row-level security filters rows based on user attributes. Column masks hide sensitive data from unauthorized users.", "governance"),
    (3, "MLflow Model Registry Workflow", "The Model Registry manages model lifecycle: log models with mlflow.sklearn.log_model(), register with mlflow.register_model(), transition stages (None → Staging → Production → Archived). Use model aliases for version pinning. Serve models via Databricks Model Serving endpoints. Batch scoring with mlflow models:/{name}/Production.", "mlops"),
    (4, "Vector Search for RAG", "Vector Search enables similarity-based retrieval for RAG applications. Create a Vector Search endpoint, then a Delta Sync Index or direct access index. The index auto-syncs from a Delta table containing embeddings. Query with a vector embedding to find the top-k most similar documents. Combine with an LLM for retrieval-augmented generation.", "genai"),
    (5, "Auto Loader Best Practices", "Auto Loader incrementally ingests files from cloud storage with exactly-once semantics. Use cloudFiles format with schema inference for unknown data. Set schema evolution mode to add new columns automatically. Use availableNow trigger for batch-style processing. Checkpointing to a UC volume ensures fault recovery. foreachBatch enables custom sinks with MERGE upserts.", "data-engineering"),
    (6, "Structured Streaming Watermarks", "Watermarks define how late data is accepted in streaming queries. Set watermark on event-time column to bound state. Use with window aggregations for tumbling or sliding windows. Stream-static joins enrich streams with reference data. Stream-stream joins require watermarks on both sides. foreachBatch with MERGE enables idempotent sinks.", "streaming"),
    (7, "Feature Store and Training Sets", "Feature Store manages ML features with point-in-time correctness. Create feature tables in Unity Catalog. Use FeatureLookup to join features into training data. create_training_set handles temporal joins to prevent leakage. Log models with feature specs for automatic feature lookup at inference. Online stores enable low-latency serving.", "mlops"),
    (8, "AI Gateway Configuration", "AI Gateway provides a unified interface for LLM serving with governance. Route requests to Foundation Model APIs or external providers. Configure rate limits per user or token. Set up fallback chains across providers. Log all requests to inference tables for audit. Enable PII detection and safety filters. Usage tracking for cost allocation.", "genai"),
    (9, "Lakeflow Jobs and DAB", "Lakeflow Jobs orchestrate multi-task workflows: notebooks, Python scripts, SQL, pipelines. Trigger types: scheduled, file arrival, continuous, run-once. Use task dependencies for DAG workflows. Declarative Automation Bundles (DAB) define infrastructure as code for CI/CD. Deploy via databricks bundle deploy. Parameterize with widgets and job parameters.", "orchestration"),
    (10, "Databricks Apps Deployment", "Databricks Apps run serverless data applications: Streamlit, Flask, Dash, Gradio. Define app.yaml with compute and resources. Deploy via CLI or UI. Apps can access Delta tables, MLflow models, and AI Gateway. Use Databricks SDK for data access. Authentication via OAuth or personal tokens. Environment variables and secrets for configuration.", "apps"),
]

kb_df = spark.createDataFrame(
    kb_articles,
    ["doc_id", "title", "content", "category"]
)
kb_df.write.mode("overwrite").saveAsTable("demo.ai_platform.bronze_knowledge_base")

print("✅ Bronze layer — raw data ingested:")
print(f"   bronze_transactions:    {spark.table('demo.ai_platform.bronze_transactions').count()} rows")
print(f"   bronze_customers:      {spark.table('demo.ai_platform.bronze_customers').count()} rows")
print(f"   bronze_tickets:        {spark.table('demo.ai_platform.bronze_tickets').count()} rows")
print(f"   bronze_knowledge_base: {spark.table('demo.ai_platform.bronze_knowledge_base').count()} rows")
print()
print("   📊 Structured: transactions, customers (tabular)")
print("   📝 Unstructured: tickets, knowledge base (text)")

# COMMAND ----------

# DBTITLE 1,Cell 3: Bronze → Silver Processing
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Bronze → Silver Processing
# MAGIC
# MAGIC Clean, deduplicate, and standardize both structured and unstructured data.
# MAGIC This is the medallion Silver layer — data is fit for analytics and feature engineering.

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, when, regexp_replace, length, to_date, row_number
from pyspark.sql.window import Window

# --- 3a: Silver transactions — clean and standardize ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.silver_transactions")

silver_txn = spark.table("demo.ai_platform.bronze_transactions") \
    .withColumn("txn_date", to_date(col("txn_date"))) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("channel", trim(lower(col("channel")))) \
    .filter(col("amount") > 0) \
    .filter(col("status").isin("completed", "pending", "failed"))

silver_txn.write.mode("overwrite").saveAsTable("demo.ai_platform.silver_transactions")

# --- 3b: Silver customers — deduplicate and standardize ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.silver_customers")

silver_cust = spark.table("demo.ai_platform.bronze_customers") \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("plan_tier", lower(trim(col("plan_tier")))) \
    .withColumn("region", upper(trim(col("region")))) \
    .withColumn("status", lower(trim(col("status")))) \
    .dropDuplicates(["customer_id"])

silver_cust.write.mode("overwrite").saveAsTable("demo.ai_platform.silver_customers")

# --- 3c: Silver tickets — text cleaning for GenAI ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.silver_tickets")

silver_tickets = spark.table("demo.ai_platform.bronze_tickets") \
    .withColumn("created_date", to_date(col("created_date"))) \
    .withColumn("subject", trim(col("subject"))) \
    .withColumn("description", trim(regexp_replace(col("description"), "\\s+", " "))) \
    .filter(length(col("description")) > 10) \
    .withColumn("priority", lower(trim(col("priority")))) \
    .withColumn("status", lower(trim(col("status"))))

silver_tickets.write.mode("overwrite").saveAsTable("demo.ai_platform.silver_tickets")

# --- 3d: Silver knowledge base — prepared for chunking ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.silver_knowledge_base")

silver_kb = spark.table("demo.ai_platform.bronze_knowledge_base") \
    .withColumn("content", trim(col("content"))) \
    .withColumn("category", lower(trim(col("category")))) \
    .filter(length(col("content")) > 50)

silver_kb.write.mode("overwrite").saveAsTable("demo.ai_platform.silver_knowledge_base")

print("✅ Silver layer — cleaned and standardized:")
print(f"   silver_transactions:    {spark.table('demo.ai_platform.silver_transactions').count()} rows")
print(f"   silver_customers:      {spark.table('demo.ai_platform.silver_customers').count()} rows")
print(f"   silver_tickets:        {spark.table('demo.ai_platform.silver_tickets').count()} rows")
print(f"   silver_knowledge_base: {spark.table('demo.ai_platform.silver_knowledge_base').count()} rows")
print()
print("   🔧 Applied: deduplication, type casting, text normalization, null filtering")

# COMMAND ----------

# DBTITLE 1,Cell 4: Gold Layer — Features & Analytics
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Gold Layer — ML Features & Text Chunks
# MAGIC
# MAGIC The Gold layer produces:
# MAGIC - **ML features**: Aggregated customer behavior for churn prediction
# MAGIC - **Text chunks**: Document chunks for Vector Search and RAG
# MAGIC - **Analytics**: Summary metrics for dashboards and monitoring

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, \
    min as spark_min, stddev, countDistinct, when, lit, datediff, current_date, \
    split, explode, posexplode, size as array_size, concat, substring

# --- 4a: Gold — customer features for ML ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.gold_customer_features")

txn_silver = spark.table("demo.ai_platform.silver_transactions")
cust_silver = spark.table("demo.ai_platform.silver_customers")

# Aggregate transaction behavior per customer
customer_txn_agg = txn_silver.groupBy("customer_id").agg(
    count("transaction_id").alias("total_transactions"),
    spark_sum(when(col("transaction_type") == "purchase", col("amount")).otherwise(0)).alias("total_spend"),
    avg("amount").alias("avg_transaction_amount"),
    spark_max("amount").alias("max_transaction_amount"),
    countDistinct("channel").alias("distinct_channels"),
    count(when(col("status") == "failed", 1)).alias("failed_transactions"),
    count(when(col("transaction_type") == "refund", 1)).alias("refund_count"),
    count(when(col("transaction_type") == "subscription", 1)).alias("subscription_count"),
)

# Join with customer profiles
gold_features = cust_silver.join(customer_txn_agg, "customer_id", "left") \
    .fillna(0, ["total_transactions", "total_spend", "avg_transaction_amount",
               "max_transaction_amount", "distinct_channels", "failed_transactions",
               "refund_count", "subscription_count"]) \
    .withColumn("failure_rate", when(col("total_transactions") > 0,
        col("failed_transactions") / col("total_transactions")).otherwise(0.0)) \
    .withColumn("is_enterprise", when(col("plan_tier") == "enterprise", 1).otherwise(0)) \
    .withColumn("is_churned", when(col("status") == "churned", 1).otherwise(0))

gold_features.write.mode("overwrite").saveAsTable("demo.ai_platform.gold_customer_features")

# --- 4b: Gold — document chunks for RAG ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.gold_doc_chunks")

# Chunk knowledge base articles into ~200-word segments for Vector Search
# In production, use a proper chunking library (e.g., LangChain text splitters)
kb_silver = spark.table("demo.ai_platform.silver_knowledge_base")

# Simple sentence-based chunking
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType

def chunk_text(text, chunk_size=300):
    """Split text into overlapping chunks by approximate character count."""
    sentences = text.split(". ")
    chunks = []
    current = ""
    chunk_idx = 0
    for sent in sentences:
        if len(current) + len(sent) > chunk_size and current:
            chunks.append((chunk_idx, current.strip() + "."))
            chunk_idx += 1
            current = sent + ". "
        else:
            current += sent + ". "
    if current.strip():
        chunks.append((chunk_idx, current.strip()))
    return chunks

chunk_schema = ArrayType(StructType([
    StructField("chunk_index", IntegerType(), False),
    StructField("chunk_text", StringType(), False)
]))

chunk_udf = udf(chunk_text, chunk_schema)

# Explode chunks into separate rows
doc_chunks = kb_silver \
    .withColumn("chunks", chunk_udf(col("content"))) \
    .withColumn("chunk", explode(col("chunks"))) \
    .select(
        col("doc_id"),
        col("title"),
        col("category"),
        col("chunk.chunk_index").alias("chunk_index"),
        col("chunk.chunk_text").alias("chunk_text"),
    ) \
    .withColumn("chunk_id", concat(col("doc_id"), lit("_"), col("chunk_index")))

doc_chunks.write.mode("overwrite").saveAsTable("demo.ai_platform.gold_doc_chunks")

# --- 4c: Gold — ticket analytics for dashboards ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.gold_ticket_analytics")

ticket_silver = spark.table("demo.ai_platform.silver_tickets")

ticket_analytics = ticket_silver.groupBy("priority", "status").agg(
    count("ticket_id").alias("ticket_count"),
    countDistinct("customer_id").alias("affected_customers"),
)

ticket_analytics.write.mode("overwrite").saveAsTable("demo.ai_platform.gold_ticket_analytics")

print("✅ Gold layer — ML features, text chunks, analytics:")
print(f"   gold_customer_features:  {spark.table('demo.ai_platform.gold_customer_features').count()} rows (ML-ready)")
print(f"   gold_doc_chunks:         {spark.table('demo.ai_platform.gold_doc_chunks').count()} rows (RAG-ready)")
print(f"   gold_ticket_analytics:   {spark.table('demo.ai_platform.gold_ticket_analytics').count()} rows (dashboard-ready)")
print()
print("   📊 Customer features: total_spend, failure_rate, is_enterprise, is_churned")
print("   📝 Doc chunks: chunked knowledge base for Vector Search")
print("   📈 Ticket analytics: priority × status summary")
print()
print("   Schema preview (gold_customer_features):")
spark.table("demo.ai_platform.gold_customer_features").printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 5: Traditional ML — Churn Prediction
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Traditional ML — Churn Prediction with MLflow
# MAGIC
# MAGIC Train a churn prediction model on the Gold features. This demonstrates the traditional ML path
# MAGIC within the AI platform. See modules 07 (MLflow) and 11 (E2E ML) for deeper dives.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler

# Load Gold features
features_df = spark.table("demo.ai_platform.gold_customer_features").toPandas()

feature_cols = [
    "total_transactions", "total_spend", "avg_transaction_amount", "max_transaction_amount",
    "distinct_channels", "failed_transactions", "refund_count", "subscription_count",
    "failure_rate", "is_enterprise", "tenure_days"
]
X = features_df[feature_cols].fillna(0).values
y = features_df["is_churned"].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

mlflow.set_experiment("/demo/ai_platform_churn")

models = {
    "logistic_regression": LogisticRegression(max_iter=1000, random_state=42),
    "random_forest": RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42),
}

best_model = None
best_auc = 0
best_name = ""

for name, model in models.items():
    with mlflow.start_run(run_name=name) as run:
        model.fit(X_train_scaled, y_train)
        preds = model.predict(X_test_scaled)
        proba = model.predict_proba(X_test_scaled)[:, 1] if len(model.classes_) > 1 else preds
        acc = accuracy_score(y_test, preds)
        auc = roc_auc_score(y_test, proba) if len(np.unique(y_test)) > 1 else 0.5

        mlflow.log_param("model", name)
        mlflow.log_param("features", len(feature_cols))
        mlflow.log_metrics({"accuracy": acc, "roc_auc": auc})
        mlflow.sklearn.log_model(model, "model")

        print(f"   {name}: accuracy={acc:.4f}, roc_auc={auc:.4f}")

        if auc > best_auc:
            best_auc = auc
            best_model = model
            best_name = name

print(f"\n✅ Best model: {best_name} (roc_auc={best_auc:.4f})")
print(f"   Feature count: {len(feature_cols)}")
print(f"   Training samples: {len(X_train)}, Test samples: {len(X_test)}")

# --- Register best model ---
model_name = "ai_platform_churn_model"
client = mlflow.tracking.MlflowClient()

# Get the best run ID
best_run = client.search_runs(
    experiment_ids=[mlflow.get_experiment_by_name("/demo/ai_platform_churn").experiment_id],
    order_by=["metrics.roc_auc DESC"],
    max_results=1
)[0]

result = mlflow.register_model(
    model_uri=f"runs:/{best_run.info.run_id}/model",
    name=model_name
)
print(f"\n📦 Model registered: {model_name} v{result.version}")
print(f"   → In production, transition to Staging/Production via MLflow UI")
print(f"   → Serve via Databricks Model Serving endpoint")

# COMMAND ----------

# DBTITLE 1,Cell 6: LLM Data Preparation
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: LLM Data Preparation — Chunks, Prompts, Fine-Tuning Data
# MAGIC
# MAGIC Prepare data specifically for GenAI use cases:
# MAGIC - **RAG chunks**: Already created in Gold layer (gold_doc_chunks)
# MAGIC - **Prompt templates**: System + user prompt structures for LLM inference
# MAGIC - **Fine-tuning dataset**: Instruction-response pairs for model fine-tuning
# MAGIC - **Inference batch**: Pre-formatted questions for batch LLM inference

# COMMAND ----------

# --- 6a: Prompt templates for RAG ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.gold_rag_prompts")

rag_prompts = [
    (1, "How do I optimize Delta Lake performance?", "data-engineering"),
    (2, "What is Unity Catalog and how do I set up permissions?", "governance"),
    (3, "How does the MLflow Model Registry work?", "mlops"),
    (4, "How do I set up Vector Search for RAG?", "genai"),
    (5, "What are Auto Loader best practices?", "data-engineering"),
    (6, "How do watermarks work in Structured Streaming?", "streaming"),
    (7, "What is the Feature Store and how do I create training sets?", "mlops"),
    (8, "How do I configure the AI Gateway?", "genai"),
    (9, "How do I orchestrate jobs with DAB?", "orchestration"),
    (10, "How do I deploy a Databricks App?", "apps"),
]

prompt_template = """You are a helpful Databricks assistant. Answer the user's question based on the provided context.

Context:
{context}

Question: {question}

Answer:"""

prompts_df = spark.createDataFrame(rag_prompts, ["prompt_id", "question", "expected_category"])
prompts_df = prompts_df.withColumn("prompt_template", lit(prompt_template))
prompts_df.write.mode("overwrite").saveAsTable("demo.ai_platform.gold_rag_prompts")

# --- 6b: Fine-tuning dataset (instruction-response pairs) ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.gold_finetune_dataset")

finetune_data = [
    (1, "system", "You are a Databricks expert. Provide concise, accurate answers."),
    (2, "user", "What is Delta Lake?"),
    (2, "assistant", "Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark. It provides serializable isolation, schema enforcement, time travel, and efficient upserts/deletes via MERGE."),
    (3, "user", "How do I create a Unity Catalog catalog?"),
    (3, "assistant", "Use CREATE CATALOG IF NOT EXISTS <catalog_name>; in SQL or spark.sql(). Catalogs contain schemas, which contain tables, volumes, and models. You need account admin permissions."),
    (4, "user", "What is Auto Loader?"),
    (4, "assistant", "Auto Loader incrementally processes new files in cloud storage with exactly-once semantics. It supports schema inference, evolution, and checkpointing for fault tolerance. Use cloudFiles format."),
    (5, "user", "How does Vector Search work?"),
    (5, "assistant", "Vector Search provides similarity-based retrieval. You create an endpoint, then an index from a Delta table with embeddings. Queries return the top-k most similar documents by cosine similarity."),
]

ft_df = spark.createDataFrame(finetune_data, ["conversation_id", "role", "content"])
ft_df.write.mode("overwrite").saveAsTable("demo.ai_platform.gold_finetune_dataset")

# --- 6c: Batch inference questions ---
spark.sql("DROP TABLE IF EXISTS demo.ai_platform.gold_batch_questions")

batch_questions = [
    (i, q) for i, q in enumerate([
        "Summarize the key benefits of Delta Lake for data engineering.",
        "Explain how Unity Catalog enables data governance.",
        "What are the best practices for MLflow model registration?",
        "How does Vector Search power RAG applications?",
        "What streaming patterns are available in Databricks?",
        "How do I optimize query performance on large Delta tables?",
        "What is the Feature Store used for?",
        "How does the AI Gateway manage LLM routing?",
    ], 1)
]

bq_df = spark.createDataFrame(batch_questions, ["question_id", "question"])
bq_df.write.mode("overwrite").saveAsTable("demo.ai_platform.gold_batch_questions")

print("✅ LLM data preparation complete:")
print(f"   gold_rag_prompts:        {spark.table('demo.ai_platform.gold_rag_prompts').count()} rows (RAG queries)")
print(f"   gold_finetune_dataset:   {spark.table('demo.ai_platform.gold_finetune_dataset').count()} rows (instruction pairs)")
print(f"   gold_batch_questions:    {spark.table('demo.ai_platform.gold_batch_questions').count()} rows (batch inference)")
print()
print("   🔗 RAG pipeline: gold_doc_chunks → embeddings → Vector Search → LLM")
print("   🔧 Fine-tuning: gold_finetune_dataset → instruction format → model training")
print("   📦 Batch inference: gold_batch_questions → ai_query / SDK → results table")

# COMMAND ----------

# DBTITLE 1,Cell 7: Embeddings & Vector Search
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Embeddings & Vector Search Index
# MAGIC
# MAGIC Generate text embeddings and create a Vector Search endpoint + index for similarity search.
# MAGIC This is the retrieval infrastructure for RAG.
# MAGIC
# MAGIC **Production approach**: Use `ai_query` SQL function or the Databricks SDK to generate embeddings
# MAGIC via a serving endpoint (e.g., `databricks-gte-large-en-v1`).

# COMMAND ----------

print("📋 Step 1 — Generate Embeddings (SQL with ai_query):")
print("""
-- Add embedding column to gold_doc_chunks
ALTER TABLE demo.ai_platform.gold_doc_chunks ADD COLUMN embedding ARRAY<DOUBLE>;

-- Generate embeddings using a foundation embedding model
-- (requires a serving endpoint for the embedding model)
UPDATE demo.ai_platform.gold_doc_chunks
SET embedding = ai_query(
    'databricks-gte-large-en-v1',
    chunk_text,
    '{
      "input_type": "document"
    }'
);
""")

# Simulate embeddings for demo (in production, use ai_query above)
import random

random.seed(42)
chunks = spark.table("demo.ai_platform.gold_doc_chunks").collect()

# Generate deterministic dummy embeddings (384-dim like GTE-small)
embedding_data = []
for row in chunks:
    random.seed(hash(row.chunk_id) % (2**32))
    emb = [random.gauss(0, 1) for _ in range(384)]
    embedding_data.append((row.chunk_id, row.doc_id, row.title, row.category,
                          row.chunk_index, row.chunk_text, emb))

from pyspark.sql.types import ArrayType, DoubleType
emb_schema = spark.table("demo.ai_platform.gold_doc_chunks").schema \
    .add("embedding", ArrayType(DoubleType()))

spark.sql("DROP TABLE IF EXISTS demo.ai_platform.gold_doc_chunks_embedded")
emb_df = spark.createDataFrame(embedding_data, emb_schema)
emb_df.write.mode("overwrite").saveAsTable("demo.ai_platform.gold_doc_chunks_embedded")

print(f"\n✅ Embeddings generated: {emb_df.count()} chunks × 384 dims (simulated)")
print("   In production, use: ai_query('databricks-gte-large-en-v1', chunk_text)")

print("\n📋 Step 2 — Create Vector Search Endpoint & Index (Python SDK):")
print("""
from databricks.vector_search.client import VectorSearchClient

# Create a Vector Search endpoint
vsc = VectorSearchClient(disable_warning=True)
endpoint = vsc.create_endpoint(
    name="ai-platform-vector-search",
    endpoint_type="STANDARD"
)

# Create a Delta Sync Index (auto-syncs from Delta table)
index = vsc.create_delta_sync_index(
    endpoint_name="ai-platform-vector-search",
    index_name="demo.ai_platform.knowledge_base_vs_index",
    source_table_name="demo.ai_platform.gold_doc_chunks_embedded",
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_column="chunk_text",
    embedding_model_endpoint_name="databricks-gte-large-en-v1"
)
print("✅ Vector Search index created")
""")

print("\n📋 Step 3 — Similarity Search (query the index):")
print("""
# Get the index
index = vsc.get_index(
    endpoint_name="ai-platform-vector-search",
    index_name="demo.ai_platform.knowledge_base_vs_index"
)

# Search for relevant documents
results = index.similarity_search(
    query_text="How to optimize Delta Lake performance?",
    columns=["chunk_text", "title", "category"],
    num_results=3
)
print(results)
""")

# COMMAND ----------

# DBTITLE 1,Cell 8: RAG Pipeline
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: RAG Pipeline — Retrieve → Augment → Generate
# MAGIC
# MAGIC Full RAG flow: embed question → Vector Search → retrieve documents → augment prompt → LLM generates answer.
# MAGIC This is the core GenAI application pattern on Databricks.

# COMMAND ----------

print("📋 Full RAG Pipeline (Python SDK):")
print("""
from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments

# --- 1. Initialize clients ---

vsc = VectorSearchClient(disable_warning=True)
index = vsc.get_index(
    endpoint_name="ai-platform-vector-search",
    index_name="demo.ai_platform.knowledge_base_vs_index"
)

# AI Gateway or Foundation Model API client
llm_client = mlflow.deployments.get_deploy_client("databricks")

# --- 2. RAG Function ---

def rag_pipeline(question: str, num_results: int = 3) -> dict:
    \"\"\"Retrieve relevant documents and generate an answer.\"\"\"

    # Step A: Vector Search — retrieve top-k chunks
    search_results = index.similarity_search(
        query_text=question,
        columns=["chunk_text", "title", "category"],
        num_results=num_results
    )

    # Step B: Build context from retrieved chunks
    chunks = search_results.get("data", {}).get("get", [])
    context = "\\n\\n".join([c["chunk_text"] for c in chunks])

    # Step C: Augment prompt with retrieved context
    prompt = f\"\"\"You are a helpful Databricks assistant.

Context:
{context}

Question: {question}

Answer based on the context above. If the context is insufficient, say so.\"\"\"

    # Step D: LLM inference — generate answer
    response = llm_client.predict(
        endpoint="databricks-dbrx-instruct",
        inputs={
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 500,
            "temperature": 0.1
        }
    )

    return {
        "question": question,
        "answer": response["choices"][0]["message"]["content"],
        "retrieved_chunks": len(chunks),
        "sources": [c["title"] for c in chunks]
    }

# --- 3. Run RAG queries ---

questions = spark.table("demo.ai_platform.gold_rag_prompts").collect()
for q in questions:
    result = rag_pipeline(q.question)
    print(f"Q: {result['question']}")
    print(f"A: {result['answer'][:200]}...")
    print(f"   Sources: {result['sources']}")
    print()
""")

print("\n📋 Batch RAG with SQL (ai_query + Vector Search):")
print("""
-- Run RAG at scale using SQL AI functions
CREATE TABLE demo.ai_platform.gold_rag_results AS
SELECT
    q.question_id,
    q.question,
    ai_query(
        'databricks-dbrx-instruct',
        CONCAT(
            'Answer this question about Databricks: ', q.question,
            '. Use this context: ', context_retrieved.chunk_text
        ),
        '{"max_tokens": 500, "temperature": 0.1}'
    ) AS llm_answer
FROM demo.ai_platform.gold_batch_questions q
CROSS JOIN LATERAL (
    SELECT chunk_text FROM vector_search(
        'demo.ai_platform.knowledge_base_vs_index',
        q.question,
        3
    )
) context_retrieved;
""")

print("\n✅ RAG pipeline demonstrated:")
print("   1. Question embedding → Vector Search")
print("   2. Retrieved chunks → Context")
print("   3. Context + Question → Prompt")
print("   4. Prompt → LLM → Answer")
print("   5. Batch RAG via SQL: ai_query + vector_search()")

# COMMAND ----------

# DBTITLE 1,Cell 9: LLM Serving & Inference
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 9: LLM Serving & Inference
# MAGIC
# MAGIC Deploy and call LLM endpoints for production inference.
# MAGIC Three patterns: Foundation Model APIs (pay-per-token), External Models, and Batch Inference.

# COMMAND ----------

print("📋 LLM Serving Options on Databricks:")
print("""
╔══════════════════════════════════════════════════════════════════════╗
║                  LLM Serving Options                               ║
╠══════════════════════════════════════════════════════════════════════╣
║ 1. Foundation Model APIs (pay-per-token)                          ║
║    - DBRX Instruct, Llama, Mistral, GTE embeddings                ║
║    - No infrastructure to manage                                  ║
║    - Endpoint: /serving-endpoints/<model>/invocations             ║
║                                                                    ║
║ 2. External Models (bring-your-own-key)                           ║
║    - OpenAI GPT-4, Anthropic Claude, Google Gemini                ║
║    - Unified API via Databricks SDK                               ║
║    - Routed through AI Gateway for governance                     ║
║                                                                    ║
║ 3. Fine-tuned Models (custom serving)                             ║
║    - Fine-tune foundation models on your data                    ║
║    - Deploy via Model Serving with provisioned throughput         ║
║    - LoRA adapters for cost-efficient customization               ║
║                                                                    ║
║ 4. Batch Inference (at scale)                                    ║
║    - Use ai_query in SQL for large-scale inference               ║
║    - Or spark.read + predict + write pattern                      ║
║    - No endpoint needed — runs on serverless compute              ║
╚══════════════════════════════════════════════════════════════════════╝
""")

print("📋 Pattern 1 — Real-time LLM Inference (Python SDK):")
print("""
import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")

response = client.predict(
    endpoint="databricks-dbrx-instruct",
    inputs={
        "messages": [
            {"role": "system", "content": "You are a data engineering expert."},
            {"role": "user", "content": "Explain Delta Lake in 3 sentences."}
        ],
        "max_tokens": 200,
        "temperature": 0.7
    }
)

answer = response["choices"][0]["message"]["content"]
print(answer)
""")

print("📋 Pattern 2 — Batch Inference with ai_query (SQL):")
print("""
-- Batch LLM inference on gold_batch_questions
CREATE TABLE demo.ai_platform.gold_batch_llm_results AS
SELECT
    question_id,
    question,
    ai_query(
        'databricks-dbrx-instruct',
        question,
        '{"max_tokens": 300, "temperature": 0.3}'
    ) AS llm_answer,
    ai_analyze_sentiment(
        ai_query('databricks-dbrx-instruct', question,
                 '{"max_tokens": 300, "temperature": 0.3}')
    ) AS answer_sentiment
FROM demo.ai_platform.gold_batch_questions;
""")

print("📋 Pattern 3 — Ticket Classification with LLM (SQL):")
print("""
-- Classify support tickets using LLM
CREATE TABLE demo.ai_platform.gold_ticket_classifications AS
SELECT
    ticket_id,
    subject,
    ai_query(
        'databricks-dbrx-instruct',
        CONCAT(
            'Classify this support ticket into one of: ',
            'billing, technical, feature_request, account, performance. ',
            'Ticket: ', subject, '. ', description
        ),
        '{"max_tokens": 50, "temperature": 0.0}'
    ) AS ticket_category,
    ai_analyze_sentiment(description) AS sentiment
FROM demo.ai_platform.silver_tickets;
""")

print("✅ LLM serving patterns:")
print("   1. Real-time: SDK client → Foundation Model API → response")
print("   2. Batch SQL: ai_query() on entire table → results table")
print("   3. Classification: ai_query + ai_analyze_sentiment for text analytics")
print("   4. Custom: Fine-tuned model → Model Serving endpoint")

# COMMAND ----------

# DBTITLE 1,Cell 10: AI Gateway — Governance
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 10: AI Gateway — Governance for LLMs
# MAGIC
# MAGIC The AI Gateway provides a unified interface for managing LLM endpoints with:
# MAGIC - **Rate limiting**: Per-user, per-token request caps
# MAGIC - **Fallback**: Automatic failover across providers
# MAGIC - **Usage tracking**: Token consumption, cost attribution
# MAGIC - **Inference logging**: All requests/responses logged to UC tables
# MAGIC - **Guardrails**: PII detection, safety filters, content moderation

# COMMAND ----------

print("📋 AI Gateway Configuration (Python SDK):")
print("""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving

w = WorkspaceClient()

# Create an AI Gateway endpoint with rate limiting + fallback
endpoint = w.serving_endpoints.create(
    name="ai-platform-llm-gateway",
    config=serving.EndpointConfig(
        served_entities=[
            serving.ServedEntity(
                entity_name="databricks-dbrx-instruct",
                entity_version="1",
                scale_to_zero_enabled=True,
                min_provisioned_throughput=0,
                max_provisioned_throughput=100
            )
        ],
        rate_limits=[
            serving.RateLimit(
                calls=100,
                renewal_period="minute",
                key="user"
            )
        ],
        fallback_route=serving.FallbackRoute(
            served_entity_name="databricks-llama-3-70b-instruct",
            enabled=True
        ),
        inference_table_config=serving.InferenceTableConfig(
            inference_table_name="demo.ai_platform.llm_inference_log",
            enabled=True
        )
    )
)
print(f"✅ AI Gateway endpoint created: {endpoint.name}")
""")

print("\n📋 Querying the AI Gateway (same SDK interface):")
print("""
import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")

# All requests go through the Gateway → rate limited, logged, fallback-enabled
response = client.predict(
    endpoint="ai-platform-llm-gateway",
    inputs={
        "messages": [{"role": "user", "content": "What are the 3 pillars of the Databricks AI Platform?"}],
        "max_tokens": 200
    }
)
print(response["choices"][0]["message"]["content"])
""")

print("\n📋 Monitoring LLM Usage (SQL):")
print("""
-- Query the inference log table for usage analytics
SELECT
    DATE(request_timestamp) AS request_date,
    COUNT(*) AS request_count,
    SUM(input_tokens) AS total_input_tokens,
    SUM(output_tokens) AS total_output_tokens,
    SUM(input_tokens + output_tokens) AS total_tokens,
    AVG(latency_ms) AS avg_latency_ms
FROM demo.ai_platform.llm_inference_log
GROUP BY DATE(request_timestamp)
ORDER BY request_date DESC;

-- PII detection on LLM inputs
SELECT
    request_id,
    request_text,
    ai_query(
        'databricks-dbrx-instruct',
        CONCAT('Extract any PII from this text and return as JSON: ', request_text)
    ) AS pii_detected
FROM demo.ai_platform.llm_inference_log
WHERE ai_query('databricks-dbrx-instruct',
    CONCAT('Does this text contain PII? Answer yes or no: ', request_text))
    LIKE '%yes%';
""")

print("\n✅ AI Gateway capabilities:")
print("   • Rate limiting: 100 req/min per user (configurable)")
print("   • Fallback: DBRX → Llama 3 70B (automatic)")
print("   • Logging: all requests → demo.ai_platform.llm_inference_log")
print("   • Usage tracking: tokens, latency, cost")
print("   • Guardrails: PII detection, content filtering")

# COMMAND ----------

# DBTITLE 1,Cell 11: End-to-End Orchestration
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 11: End-to-End AI Platform Orchestration
# MAGIC
# MAGIC Tie the entire AI platform together with Lakeflow Jobs and monitoring.
# MAGIC This shows the production architecture for running the full pipeline on a schedule.

# COMMAND ----------

print("📋 AI Platform Pipeline — Job Definition (Python SDK):")
print("""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Define the full AI platform pipeline as a multi-task job
job = w.jobs.create(
    name="ai-platform-pipeline",
    tasks=[
        # Task 1: Data Ingestion (Bronze)
        jobs.Task(
            task_key="ingest_raw_data",
            notebook_task=jobs.NotebookTask(
                notebook_path="/Repos/martin.mystery9@gmail.com/dataengineering/21-ai-platform/ai_platform_demo",
                notebook_params={"start_cell": "2", "end_cell": "2"}
            )
        ),
        # Task 2: Bronze → Silver Processing
        jobs.Task(
            task_key="silver_processing",
            depends_on=[jobs.TaskDependency(task_key="ingest_raw_data")],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Repos/martin.mystery9@gmail.com/dataengineering/21-ai-platform/ai_platform_demo",
                notebook_params={"start_cell": "3", "end_cell": "3"}
            )
        ),
        # Task 3: Gold Features + Chunks
        jobs.Task(
            task_key="gold_features",
            depends_on=[jobs.TaskDependency(task_key="silver_processing")],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Repos/martin.mystery9@gmail.com/dataengineering/21-ai-platform/ai_platform_demo",
                notebook_params={"start_cell": "4", "end_cell": "4"}
            )
        ),
        # Task 4: ML Training (parallel with GenAI prep)
        jobs.Task(
            task_key="ml_training",
            depends_on=[jobs.TaskDependency(task_key="gold_features")],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Repos/martin.mystery9@gmail.com/dataengineering/21-ai-platform/ai_platform_demo",
                notebook_params={"start_cell": "5", "end_cell": "5"}
            )
        ),
        # Task 5: LLM Data Prep + Embeddings + Vector Search Sync
        jobs.Task(
            task_key="genai_preparation",
            depends_on=[jobs.TaskDependency(task_key="gold_features")],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Repos/martin.mystery9@gmail.com/dataengineering/21-ai-platform/ai_platform_demo",
                notebook_params={"start_cell": "6", "end_cell": "7"}
            )
        ),
        # Task 6: Batch LLM Inference
        jobs.Task(
            task_key="batch_inference",
            depends_on=[
                jobs.TaskDependency(task_key="ml_training"),
                jobs.TaskDependency(task_key="genai_preparation")
            ],
            notebook_task=jobs.NotebookTask(
                notebook_path="/Repos/martin.mystery9@gmail.com/dataengineering/21-ai-platform/ai_platform_demo",
                notebook_params={"start_cell": "8", "end_cell": "10"}
            )
        ),
    ],
    schedule=jobs.CronSchedule(
        quartz_cron_expression="0 0 8 * * ?",  # Daily at 8 AM
        timezone_id="UTC"
    )
)
print(f"✅ AI Platform pipeline job created: {job.job_id}")
""")

print("\n📋 DAB (Declarative Automation Bundle) Structure:")
print("""
# databricks.yml
bundle:
  name: ai-platform

resources:
  jobs:
    ai_platform_pipeline:
      name: ai-platform-pipeline
      schedule:
        quartz_cron_expression: "0 0 8 * * ?"
        timezone_id: UTC
      tasks:
        - task_key: ingest_raw_data
          notebook_task:
            notebook_path: ./21-ai-platform/ai_platform_demo
        - task_key: silver_processing
          depends_on: [ingest_raw_data]
          notebook_task:
            notebook_path: ./21-ai-platform/ai_platform_demo
        - task_key: gold_features
          depends_on: [silver_processing]
          notebook_task:
            notebook_path: ./21-ai-platform/ai_platform_demo
        - task_key: ml_training
          depends_on: [gold_features]
          notebook_task:
            notebook_path: ./21-ai-platform/ai_platform_demo
        - task_key: genai_preparation
          depends_on: [gold_features]
          notebook_task:
            notebook_path: ./21-ai-platform/ai_platform_demo
        - task_key: batch_inference
          depends_on: [ml_training, genai_preparation]
          notebook_task:
            notebook_path: ./21-ai-platform/ai_platform_demo

# Deploy: databricks bundle deploy
""")

print("\n📋 Monitoring — Data Quality + Model Quality (SQL):")
print("""
-- Monitor Bronze data freshness
SELECT
    'bronze_transactions' AS table_name,
    COUNT(*) AS row_count,
    MAX(txn_date) AS latest_date,
    DATEDIFF(CURRENT_DATE(), MAX(txn_date)) AS days_behind
FROM demo.ai_platform.bronze_transactions;

-- Monitor LLM inference quality (from inference log)
SELECT
    endpoint_name,
    COUNT(*) AS total_requests,
    AVG(latency_ms) AS avg_latency,
    SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) / COUNT(*) AS success_rate,
    SUM(input_tokens + output_tokens) AS total_tokens_consumed
FROM demo.ai_platform.llm_inference_log
GROUP BY endpoint_name;

-- Monitor ML model drift (compare feature distributions)
SELECT
    'total_spend' AS feature,
    AVG(total_spend) AS current_avg,
    STDDEV(total_spend) AS current_stddev
FROM demo.ai_platform.gold_customer_features;
""")

print("\n✅ Full AI Platform Orchestration:")
print("   1. Daily schedule: ingest → process → features → ML + GenAI → inference")
print("   2. DAG dependencies: parallel ML training + GenAI preparation")
print("   3. DAB: infrastructure-as-code for CI/CD deployment")
print("   4. Monitoring: data freshness, LLM usage, model drift")
print("   5. Governance: Unity Catalog governs all tables, models, endpoints")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways — Databricks as an AI Platform
# MAGIC
# MAGIC ## The Full AI Platform Journey
# MAGIC
# MAGIC | Stage | Databricks Feature | Output |
# MAGIC |------|-------------------|--------|
# MAGIC | **1. Raw Data** | Delta Lake, UC volumes | Bronze tables (structured + unstructured) |
# MAGIC | **2. Processing** | Spark, Delta Lake | Silver tables (cleaned, deduplicated) |
# MAGIC | **3. Features** | Spark aggregations | Gold tables (ML features, text chunks) |
# MAGIC | **4. Traditional ML** | MLflow, Model Registry | Trained churn model, logged + registered |
# MAGIC | **5. LLM Data Prep** | Spark text processing | RAG prompts, fine-tune data, batch questions |
# MAGIC | **6. Embeddings & VS** | Vector Search, ai_query | Embedded chunks, VS endpoint + index |
# MAGIC | **7. RAG** | Vector Search + LLM | Question → retrieve → augment → generate |
# MAGIC | **8. LLM Serving** | Foundation Model APIs | Real-time + batch LLM inference |
# MAGIC | **9. AI Gateway** | AI Gateway | Rate limiting, fallback, logging, guardrails |
# MAGIC | **10. Orchestration** | Lakeflow Jobs, DAB | Scheduled pipeline, CI/CD, monitoring |
# MAGIC
# MAGIC ## Why Databricks as an AI Platform?
# MAGIC
# MAGIC 1. **Unified**: One platform for data engineering, ML, and GenAI — no data movement
# MAGIC 2. **Governed**: Unity Catalog governs data, models, and AI assets with the same framework
# MAGIC 3. **Scalable**: Serverless compute scales from batch processing to real-time LLM serving
# MAGIC 4. **Open**: Supports open-source models (DBRX, Llama, Mistral) and proprietary (GPT-4, Claude)
# MAGIC 5. **Cost-efficient**: Pay-per-token LLM inference, scale-to-zero endpoints, serverless compute
# MAGIC 6. **Production-ready**: Jobs orchestration, DAB CI/CD, monitoring, alerting
# MAGIC
# MAGIC ## AI Platform Architecture Summary
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph Data [Data Layer]
# MAGIC         B[Bronze] --> S[Silver] --> G[Gold]
# MAGIC     end
# MAGIC     subgraph ML [ML Layer]
# MAGIC         G --> FE[Features] --> TR[Train] --> MR[Registry] --> MS[Model Serving]
# MAGIC     end
# MAGIC     subgraph GenAI [GenAI Layer]
# MAGIC         G --> CK[Chunks] --> EM[Embeddings] --> VS[Vector Search] --> RAG[RAG]
# MAGIC     end
# MAGIC     subgraph Serving [Inference Layer]
# MAGIC         RAG --> LLM[LLM APIs] --> GW[AI Gateway]
# MAGIC         MS --> GW
# MAGIC     end
# MAGIC     subgraph Platform [Platform Layer]
# MAGIC         UC[Unity Catalog] -.-> Data
# MAGIC         UC -.-> ML
# MAGIC         UC -.-> GenAI
# MAGIC         JOB[Jobs + DAB] --> Data
# MAGIC         JOB --> ML
# MAGIC         JOB --> GenAI
# MAGIC         JOB --> Serving
# MAGIC     end
# MAGIC ```
# MAGIC
# MAGIC ## Related Modules
# MAGIC
# MAGIC - **Module 01**: Medallion fundamentals (Bronze/Silver/Gold)
# MAGIC - **Module 07**: MLflow tracking deep-dive
# MAGIC - **Module 09**: Feature Store and training sets
# MAGIC - **Module 10**: GenAI/RAG with Vector Search
# MAGIC - **Module 11**: End-to-end ML pipeline
# MAGIC - **Module 12**: Jobs orchestration and DAB
# MAGIC - **Module 13**: System tables and monitoring
# MAGIC
# MAGIC > This module is the **platform integration view** — it shows how all Databricks AI capabilities
# MAGIC > work together as a unified system from raw data to LLM inference.

# COMMAND ----------

