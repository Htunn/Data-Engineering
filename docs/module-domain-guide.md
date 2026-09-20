# Module Domain Guide — What Each Module Teaches

This document explains the domain concepts behind each of the 21 modules. For each module, we answer: **What is this domain? Why does it matter? What are the key concepts?**

Each section also includes **vendor-agnostic equivalents** — the same concepts apply across Snowflake, BigQuery, Synapse, Dremio, and other data platforms. This repo uses Databricks as a concrete implementation reference; the patterns transfer.

---

## Module 01 — Medallion Architecture Fundamentals

**What is Medallion Architecture?**

The medallion architecture is a data design pattern that organizes data in three layers of increasing quality and structure: Bronze (raw), Silver (cleaned), and Gold (curated). It is a **vendor-agnostic pattern** — the same concept applies on Snowflake (raw/staging/mart), BigQuery (landing/cleaned/curated), or any data platform.

**Why it matters**: Separates concerns — raw data is preserved for audit, cleaned data is reusable, and curated data is ready for consumption. Each layer has different SLAs, quality expectations, and consumers.

**Key concepts**: Bronze (append-only, schema-permissive), Silver (deduplicated, conformed dimensions), Gold (business-level aggregates, star schema).

**Vendor-agnostic equivalent**: Snowflake (raw/staging/mart), BigQuery (landing/cleaned/curated), dbt (staging/intermediate/marts).

---

## Module 02 — Auto Loader (File Ingestion)

**What is Auto Loader?**

Auto Loader is Databricks' managed file ingestion mechanism that incrementally processes new files from cloud storage. It is the **streaming equivalent of COPY INTO** — but with schema inference, evolution, and exactly-once semantics.

**Why it matters**: File ingestion is the #1 data engineering task. Auto Loader handles the hard parts: file discovery (millions of files), schema management (inference + evolution), and fault tolerance (checkpoints, idempotent processing).

**Key concepts**: cloudFiles source, schema inference and evolution modes, checkpointing, availableNow trigger (batch-like processing of streaming data), foreachBatch for custom sinks.

**Vendor-agnostic equivalent**: Snowpipe (Snowflake), Dataflow (GCP), AWS DMS, Fivetran/Airbyte.

---

## Module 03 — Delta Lake Advanced Features

**What is Delta Lake?**

Delta Lake is an open-source storage layer that brings ACID transactions, schema enforcement, and time travel to Parquet files. It is **not Databricks-specific** — Delta Lake works on any Spark platform.

**Why it matters**: Traditional Parquet is immutable and has no transaction log. Delta adds a transaction log enabling MERGE operations, CDC, time travel, schema evolution, and concurrent reads/writes.

**Key concepts**: ACID transactions, MERGE (upsert), SCD Type 2 (slowly changing dimensions), Change Data Feed (CDF), Time Travel (VERSION AS OF, TIMESTAMP AS OF), OPTIMIZE/ZORDER (file compaction and clustering), VACUUM (cleanup old files), Deletion Vectors (row-level deletes).

**Vendor-agnostic equivalent**: Apache Iceberg, Apache Hudi (all three are open table formats solving the same problem).

---

## Module 04 — Spark Declarative Pipelines (SDP)

**What are Declarative Pipelines?**

SDP (formerly DLT) is a declarative framework for building reliable data pipelines. You define **what** the data should look like (streaming tables, materialized views, expectations), and Databricks handles **how** to execute it (infrastructure, retries, scaling, checkpointing).

**Why it matters**: Reduces pipeline boilerplate by 70%+. No manual cluster management, no checkpoint configuration, no error handling code. Just define transformations and quality expectations.

**Key concepts**: @dlt.table (declare tables), @dlt.expect / expect_or_drop / expect_or_fail (data quality), streaming tables, materialized views, automatic dependency resolution, auto-scaling.

**Vendor-agnostic equivalent**: dbt (declarative transformations), Dagster (asset-based pipelines), Prefect (workflow orchestration).

---

## Module 05 — Unity Catalog Governance

**What is Data Governance?**

Data governance is the framework for ensuring data is **secure, compliant, discoverable, and auditable**. It covers who can access what data, how data is classified, and what operations were performed on it.

**Why it matters**: Without governance, organizations face regulatory fines (GDPR, CCPA), data breaches, and loss of trust. Governance is mandatory for production data platforms.

**Key concepts**: Three-level namespace (catalog.schema.table), GRANT/REVOKE permissions, row-level security (RLS), column masking, tags (for classification and cost attribution), audit logs, data lineage.

**Vendor-agnostic equivalent**: Apache Ranger, AWS Lake Formation, Immuta, Collibra, Snowflake Governance.

---

## Module 06 — Structured Streaming

**What is Structured Streaming?**

Structured Streaming is Spark's stream processing engine that treats streaming data as an unbounded table. You write the same SQL/DataFrame code whether the data is batch or streaming.

**Why it matters**: Real-time analytics, fraud detection, IoT ingestion, and event processing require sub-second latency. Structured Streaming provides exactly-once semantics with minimal code changes from batch.

**Key concepts**: Watermarks (late data handling), windowed aggregations (tumbling, sliding), deduplication, stream-stream joins, stream-static joins, foreachBatch (arbitrary sinks), output modes (append, update, complete).

**Vendor-agnostic equivalent**: Apache Flink, Kafka Streams, AWS Kinesis, Apache Pulsar.

---

## Module 07 — MLflow Tracking

**What is MLflow?**

MLflow is an open-source platform for managing the ML lifecycle — tracking experiments, packaging models, managing model versions, and serving models. It is **not Databricks-specific** — MLflow works standalone or on any cloud.

**Why it matters**: Without experiment tracking, ML is not reproducible. You cannot compare models, reproduce results, or deploy with confidence. MLflow is the industry standard for ML lifecycle management.

**Key concepts**: Experiments, runs, parameters, metrics, artifacts, autologging, Model Registry (staging/production/archived), model serving endpoints.

**Vendor-agnostic equivalent**: Weights and Biases, Neptune.ai, Comet ML, SageMaker Model Registry.

---

## Module 08 — Databricks SQL

**What is Databricks SQL (DBSQL)?**

DBSQL is the SQL analytics layer on Databricks — run SQL queries, create views, materialized views, and use AI functions directly in SQL. Powered by SQL Warehouses (separate from Spark clusters).

**Why it matters**: SQL is the universal language of data. DBSQL lets analysts work without learning PySpark, while still accessing the same Delta Lake data.

**Key concepts**: Views, materialized views (MV), window functions, CTEs, PIVOT, Delta SQL extensions, AI functions (ai_query, ai_forecast), Liquid Clustering (adaptive file pruning).

**Vendor-agnostic equivalent**: Snowflake SQL, BigQuery SQL, Dremio, Trino/Presto.

---

## Module 09 — Feature Engineering / Feature Store

**What is a Feature Store?**

A Feature Store is a centralized repository for ML features — it manages feature definitions, computes features at scale, stores them with point-in-time correctness, and serves them for training and inference.

**Why it matters**: Without a Feature Store, every ML team re-implements the same feature engineering code. Features become inconsistent between training and serving (training-serving skew). The Feature Store solves this by providing a single source of truth.

**Key concepts**: Feature tables, FeatureLookup, point-in-time joins (avoid data leakage), create_training_set, score_batch, model logging with feature spec, online/offline serving.

**Vendor-agnostic equivalent**: Feast (open source), Tecton, Hopsworks, SageMaker Feature Store.

---

## Module 10 — GenAI / RAG

**What is RAG (Retrieval-Augmented Generation)?**

RAG is an architecture that enhances LLM responses by retrieving relevant documents from a knowledge base before generating an answer. The LLM sees both the user question AND retrieved context, producing grounded, cited responses.

**Why it matters**: LLMs have a knowledge cutoff and hallucinate. RAG grounds responses in your organization's data, enabling accurate, up-to-date, domain-specific Q&A.

**Key concepts**: AI functions in SQL, embeddings (vector representations of text), Vector Search (similarity search over embeddings), RAG pipeline (chunk, embed, index, retrieve, augment, generate), LLM serving, AI Gateway (routing, rate limiting, logging), Agent Bricks.

**Vendor-agnostic equivalent**: LangChain + Pinecone, LlamaIndex, Haystack, Vertex AI Search.

---

## Module 11 — End-to-End ML Pipeline

**What is an End-to-End ML Pipeline?**

An end-to-end ML pipeline covers every stage from raw data to a deployed, monitored model: ingestion, exploration, feature engineering, training, evaluation, registry, serving.

**Why it matters**: Individual ML notebooks don't scale to production. You need a repeatable, automated pipeline that handles data drift, model retraining, and deployment. This module demonstrates the full lifecycle as a single runnable notebook.

**Key concepts**: Data ingestion (Delta tables), exploratory data analysis, feature engineering (PySpark aggregations), model training (scikit-learn + PyTorch), MLflow tracking, hyperparameter tuning (GridSearchCV), model evaluation (ROC, confusion matrix), Model Registry, PyTorch with Apple MPS (Mac M3 Pro GPU), Databricks Connect (local development).

**Vendor-agnostic equivalent**: SageMaker Pipeline, Vertex AI Pipeline, Kubeflow, ZenML.

---

## Module 12 — Jobs, Orchestration and DAB

**What is Job Orchestration?**

Job orchestration is the scheduling, chaining, and monitoring of data pipeline tasks. Instead of running notebooks manually, you define a **DAG** (Directed Acyclic Graph) of tasks with dependencies, schedules, and retry logic.

**Why it matters**: Production data pipelines must run automatically, recover from failures, and notify on errors. Orchestration transforms ad-hoc notebooks into reliable, scheduled workflows.

**Key concepts**: Lakeflow Jobs (multi-task workflows), task types (notebook, SQL, Python, pipeline), dependencies (ALL_OF, ANY_OF), triggers (schedule, file arrival, table update, manual), Declarative Automation Bundles (DAB — infrastructure-as-code with databricks.yml), CI/CD integration (GitHub Actions).

**Vendor-agnostic equivalent**: Apache Airflow, Dagster, Prefect, dbt + Airflow, Azure Data Factory.

---

## Module 13 — System Tables and Monitoring

**What are System Tables?**

System tables are Databricks' built-in operational data store — system.billing, system.compute, system.access, system.query, system.lakeflow — automatically collected and queryable via SQL. Think of them as the **data platform's own telemetry**.

**Why it matters**: You cannot manage what you cannot measure. System tables answer: How much are we spending? Who accessed what data? Which queries are slow? Which jobs are failing? Without this visibility, cost and reliability spiral out of control.

**Key concepts**: system.billing.usage (DBU consumption + cost), system.compute.clusters (SCD2 — dedup latest version), system.access.audit (who did what), system.query.history (query performance), system.lakeflow.jobs / job_runs (pipeline execution), Data Quality Monitoring (Lakehouse Monitoring — drift, null rates, freshness), SQL Alerts (proactive notifications).

**Vendor-agnostic equivalent**: Snowflake Account Usage, AWS CloudTrail + Cost Explorer, GCP Cloud Audit Logs, Datadog, OpenLineage.

---

## Module 14 — Notebook Utilities and Secrets

**What are Notebook Utilities?**

dbutils is Databricks' built-in utility API for file operations (dbutils.fs), parameter passing (dbutils.widgets), secrets (dbutils.secrets), inter-notebook communication (dbutils.notebook), and library management (dbutils.library).

**Why it matters**: These utilities are the glue that makes notebooks production-ready — parameterized, secure, and modular. Without them, notebooks are hardcoded, insecure, and non-reusable.

**Key concepts**: dbutils.fs (file system operations on DBFS/UC volumes), dbutils.widgets (parameters for notebook parameterization), dbutils.secrets (credential access from secret scopes), %run (shares notebook context), dbutils.notebook.run() (isolated execution with return values), dbutils.jobs.taskValues (cross-task communication in Jobs), %pip install (notebook-scoped libraries).

**Vendor-agnostic equivalent**: Papermill (notebook parameterization), environment variables + .env files, HashiCorp Vault / AWS Secrets Manager, DVC parameters.

---

## Module 15 — Delta Sharing

**What is Delta Sharing?**

Delta Sharing is an **open protocol** for sharing live data across organizations without copying or ETL. The provider (Databricks) exposes Delta tables via a REST API; the recipient (any platform) reads them using a client library — no Databricks account needed on the recipient side.

**Why it matters**: Traditional data sharing requires ETL pipelines, data copies, and format conversion. Delta Sharing eliminates all of that — recipients see live, up-to-date data via a standard protocol.

**Key concepts**: Shares (collection of tables), Recipients (OPEN token-based or Databricks-to-Databricks), provider/recipient model, activation URLs, cross-org access without data copy, open protocol (REST API).

**Vendor-agnostic equivalent**: Snowflake Secure Data Sharing, AWS Data Exchange, Delta Sharing open source (works with any provider).

---

## Module 16 — Dashboards, Alerts and Genie

**What is AI/BI?**

Databricks AI/BI is the analytics layer: **Lakeview Dashboards** (interactive visual analytics), **SQL Alerts** (proactive monitoring), and **Genie Spaces** (natural-language Q&A over data). It bridges the gap between data engineering and business users.

**Why it matters**: Data pipelines are useless if business users cannot access insights. Dashboards, alerts, and conversational AI make data accessible to non-technical stakeholders.

**Key concepts**: Lakeview dashboards (pages, widgets, datasets, parameters), SQL alerts (query + condition + notification channel), Genie spaces (natural language to SQL to answer, powered by LLMs, requires good table/column descriptions and example queries).

**Vendor-agnostic equivalent**: Tableau, Power BI, Looker, Metabase, ThoughtSpot (natural language BI).

---

## Module 17 — Databricks Apps

**What are Databricks Apps?**

Databricks Apps is a serverless framework for deploying data applications (Streamlit, Flask, Dash, Gradio) directly on the Databricks platform. Apps inherit workspace authentication, Unity Catalog governance, and SQL Warehouse access automatically.

**Why it matters**: Building a data app traditionally requires a separate deployment infrastructure (EC2, App Service, Vercel). Databricks Apps eliminates that — deploy from the same workspace where your data lives, with automatic SSO and data governance.

**Key concepts**: app.py (application code), app.yaml (configuration, dependencies, environment), databricks apps deploy (CLI deployment), serverless runtime, auto-authentication, supported frameworks (Streamlit, Flask, Dash, Gradio, FastAPI).

**Vendor-agnostic equivalent**: Streamlit Community Cloud, Vercel, Heroku, AWS App Runner, Azure Container Apps.

---

## Module 18 — Git Integration

**What is Git Integration on Databricks?**

Databricks Git folders (Repos) link workspace notebooks to external Git repositories (GitHub, GitLab, Bitbucket, Azure DevOps). You can branch, commit, push, and pull — all from within the Databricks workspace.

**Why it matters**: Without version control, notebook code is untracked, non-reviewable, and non-reproducible. Git integration enables code review, CI/CD, branching strategies, and collaboration — the same workflows software engineers use.

**Key concepts**: Git folders (clone repos into workspace), branch management (switch, create, delete), Git CLI (if enabled on cluster), Databricks SDK API (w.repos), CI/CD with DAB + GitHub Actions, commit from notebook UI.

**Vendor-agnostic equivalent**: Any Git-based workflow — the concept is universal. Databricks simply bridges Git and notebooks.

---

## Module 19 — Lakeflow Connect

**What is Lakeflow Connect?**

Lakeflow Connect provides **managed ingestion connectors** that automatically pull data from external SaaS applications and databases (Salesforce, Google Ads, MySQL, PostgreSQL, HubSpot, ServiceNow) into Delta Lake — no infrastructure, no connector code.

**Why it matters**: Building and maintaining custom ingestion connectors is the most time-consuming part of data engineering. Managed connectors eliminate 90% of this effort — just configure credentials and select tables.

**Key concepts**: Connector types (CRM, advertising, ITSM, HR, database), connection configuration (credentials in secret scopes), incremental vs full load, automatic schema inference, pipeline monitoring via system tables.

**Vendor-agnostic equivalent**: Fivetran, Airbyte, Stitch, HVR, AWS DMS, Debezium (CDC).

---

## Module 20 — Lakebase

**What is Lakebase?**

Lakebase is Databricks' managed PostgreSQL offering with autoscaling, database branching (like Git for databases), and reverse ETL (sync data between Delta Lake and Postgres in both directions).

**Why it matters**: Operational applications need low-latency, row-level data access (OLTP) — something Delta Lake (designed for OLAP) cannot provide. Lakebase fills this gap with a managed Postgres that integrates natively with the Databricks data stack.

**Key concepts**: Projects (managed Postgres instances), branches (instant database copies — no data duplication), endpoints (autoscaling compute, scale-to-zero), Data API (REST access), reverse ETL (sync Lakebase tables to Delta Lake for analytics), forward ETL (sync Delta tables to Lakebase for operational use).

**Vendor-agnostic equivalent**: Amazon RDS, Azure Database for PostgreSQL, Google Cloud SQL, Supabase, Neon (serverless Postgres with branching).

---

## Module 21 — AI Platform: Raw Data to LLM Inference

**What is an AI Platform?**

An AI Platform is a unified system that covers the entire AI lifecycle — from raw data ingestion through data processing, feature engineering, traditional ML, and all the way to LLM inference with RAG. It is not a single tool but the **integration of all AI capabilities** into one coherent pipeline with shared governance, compute, and orchestration.

**Why it matters**: Most organizations build ML and GenAI in silos — data engineers handle pipelines, ML engineers handle models, and GenAI engineers handle RAG and LLMs. This creates data inconsistency, duplicated effort, and governance gaps. A unified AI platform eliminates these silos: the same Delta tables feed both ML training and RAG retrieval; the same Unity Catalog governs models, tables, and LLM endpoints; the same Lakeflow Jobs orchestrate the full pipeline.

**Key concepts**: Medallion for AI (Bronze raw data → Silver cleaned → Gold ML features + text chunks), ML training with MLflow (traditional models), LLM data preparation (document chunking, prompt templates, fine-tuning datasets), embeddings & Vector Search (similarity retrieval infrastructure), RAG pipeline (retrieve → augment → generate), LLM serving (Foundation Model APIs, batch inference with ai_query), AI Gateway (rate limiting, fallback, usage logging, PII guardrails), end-to-end orchestration (Lakeflow Jobs DAG with parallel ML + GenAI paths, DAB for CI/CD).

**Vendor-agnostic equivalent**: A full AI platform stack — e.g., Snowflake + Cortex + Streamlit, Google Cloud (BigQuery + Vertex AI + Pinecone), AWS (Redshift + SageMaker + Bedrock + OpenSearch), Azure (Synapse + Azure ML + OpenAI + AI Search). Databricks provides all of these capabilities natively on one platform without data movement.

---

## Cross-Module Domain Summary

| Domain | Modules | Core Question Answered |
|-------|---------|----------------------|
| Ingestion | 02, 19 | How does data enter the platform? |
| Storage | 01, 03 | Where and how is data stored? |
| Transformation | 04, 08 | How is data cleaned and shaped? |
| Quality | 04, 13 | How do we ensure data is correct? |
| Features | 09, 11 | How do we prepare data for ML? |
| Training | 07, 11 | How do we build and track models? |
| Serving | 07, 10, 11 | How do we deploy models and apps? |
| GenAI | 10 | How do we build RAG and LLM apps? |
| Orchestration | 12 | How do we automate pipelines? |
| Monitoring | 13 | How do we observe cost and health? |
| Governance | 05, 14 | How do we secure and audit data? |
| Sharing | 15, 16 | How do we share data and insights? |
| DevTools | 14, 18 | How do we develop and version code? |
| Apps | 17 | How do we build data applications? |
| Operational DB | 20 | How do we serve data to applications? |
| AI Platform | 21 | How do all AI capabilities work as one system? |