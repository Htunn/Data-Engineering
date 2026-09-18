# Data Engineering Concepts — Core Principles

## What is Data Engineering?

**Data engineering** is the discipline of designing, building, and maintaining the infrastructure and pipelines that collect, transform, store, and deliver data for analytical and operational use. It sits at the intersection of software engineering, data architecture, and domain expertise.

```mermaid
flowchart LR
    subgraph "Data Engineering Scope"
        I[Ingest<br/>Collect from sources] --> S[Store<br/>Reliable storage]
        S --> T[Transform<br/>Clean, enrich, aggregate]
        T --> Q[Quality<br/>Validate, monitor]
        Q --> Srv[Serve<br/>BI, ML, APIs]
        O[Orchestrate<br/>Schedule, monitor] -.-> I
        O -.-> T
        G[Govern<br/>Security, lineage] -.-> S
    end
```

### Core Responsibilities

| Responsibility | Description | This Repo Module |
|---------------|-------------|------------------|
| **Data Ingestion** | Collect data from files, APIs, databases, streams | 02 (Auto Loader), 19 (Lakeflow Connect) |
| **Data Storage** | Reliable, query-optimized storage with ACID | 01 (Medallion), 03 (Delta Lake) |
| **Data Transformation** | Clean, enrich, aggregate, join | 03 (Delta), 08 (SQL), 04 (SDP) |
| **Data Quality** | Validate, enforce constraints, monitor drift | 04 (SDP), 13 (Monitoring) |
| **Data Pipeline Orchestration** | Schedule, chain, retry, monitor workflows | 12 (Jobs & DAB) |
| **Data Governance** | Access control, lineage, auditing, masking | 05 (UC Governance) |
| **Data Serving** | BI dashboards, ML features, APIs, GenAI | 16 (Dashboards), 09 (Features), 10 (GenAI) |

### Why Data Engineering Matters

1. **Foundation for analytics**: Without reliable pipelines, BI dashboards show stale or wrong data
2. **Foundation for ML**: Models are only as good as the training data — garbage in, garbage out
3. **Foundation for GenAI**: RAG pipelines need clean, chunked, embedded documents; vector indexes need fresh data
4. **Scale**: Manual data handling breaks at TB/PB scale — engineered pipelines scale horizontally
5. **Reliability**: Idempotent, monitored pipelines recover from failures automatically
6. **Compliance**: Governance ensures PII is masked, access is audited, lineage is traceable

---

## Data Engineering in the Machine Learning Landscape

ML is not just model training — it is a **data pipeline problem**. Data engineering underpins every stage of the ML lifecycle.

```mermaid
flowchart TB
    subgraph "Data Engineering for ML"
        D1[Raw Data<br/>Delta tables] --> D2[Feature Engineering<br/>Transformations]
        D2 --> D3[Training Set<br/>Point-in-time joins]
        D3 --> D4[Model Training<br/>MLflow tracking]
        D4 --> D5[Model Registry<br/>Versioning, staging]
        D5 --> D6[Serving<br/>Real-time / batch]
        D6 -.->|Predictions| D1
    end
    subgraph "Continuous Loop"
        D6 -->|Drift detection| D2
        D2 -->|Retraining trigger| D4
    end
```

### Where Data Engineering Powers ML

| ML Stage | Data Engineering Role | Module |
|---------|--------------------|---------|
| **Data collection** | Ingest from sources (files, CDC, APIs) | 02, 19 |
| **Data cleaning** | Remove nulls, deduplicate, standardize | 01, 03 |
| **Feature engineering** | Create features, encode, scale, aggregate | 09, 11 |
| **Training data prep** | Point-in-time joins to avoid leakage | 09 |
| **Model training** | Track experiments, hyperparameter tuning | 07, 11 |
| **Model deployment** | Register, version, serve models | 07, 11 |
| **Monitoring** | Detect data drift, model degradation | 13 |
| **Retraining** | Automated retraining pipelines | 12 |

**Key insight**: A production ML system spends ~80% of effort on data pipelines (ingestion, cleaning, features) and ~20% on model code. This repo reflects that ratio — 9 of 20 modules focus on data infrastructure.

---

## Data Engineering in the Generative AI Landscape

Generative AI (LLMs, RAG, agents) is fundamentally a **data pipeline challenge**. The quality of GenAI output depends entirely on the quality of data fed into it.

```mermaid
flowchart LR
    subgraph "Data Engineering for GenAI"
        S1[Source Documents<br/>PDFs, wikis, DBs] --> S2[Chunking<br/>Split into passages]
        S2 --> S3[Embedding<br/>Vector representation]
        S3 --> S4[Vector Index<br/>Vector Search]
        S4 --> S5[Retrieval<br/>Relevant context]
        S5 --> S6[LLM<br/>Generate response]
        S6 --> S7[Response<br/>With citations]
    end
    subgraph "Data Quality Layer"
        Q1[Freshness monitoring] -.-> S4
        Q2[Embedding drift detection] -.-> S3
        Q3[Source deduplication] -.-> S1
    end
```

### Where Data Engineering Powers GenAI

| GenAI Stage | Data Engineering Role | Module |
|------------|--------------------|---------|
| **Document ingestion** | Auto-load PDFs, HTML, wikis into Delta | 02, 19 |
| **Chunking & preprocessing** | Split, clean, normalize text | 10 |
| **Embedding generation** | Batch embedding pipelines with AI functions | 10 |
| **Vector index management** | Create, refresh Vector Search indexes | 10 |
| **RAG retrieval pipeline** | Query vector index, assemble context | 10 |
| **LLM serving** | Serve foundation models, manage latency | 10 |
| **Quality monitoring** | Track embedding drift, retrieval relevance | 13 |
| **Governance** | Track which documents were used for which answer | 05 |

**Key insight**: RAG is a data pipeline — ingest, chunk, embed, index, retrieve, augment. Every step is a data engineering problem. The LLM is only the final transformation.

### The Data-GenAI Feedback Loop

Data engineering does not stop after deployment. GenAI systems create new data engineering needs:

1. **Logging**: Every LLM call generates inputs, outputs, tokens, latency — all need pipelines to store and analyze
2. **Evaluation**: Automated quality scoring (faithfulness, relevance) requires data pipelines
3. **Fine-tuning data**: User feedback and corrections become training data for future models
4. **Cost monitoring**: Token usage and LLM costs need the same billing pipelines as any cloud resource

---

## Component Diagram — Data Engineering Lifecycle

```mermaid
graph TB
    subgraph Ingest [1. Ingestion]
        I1[Batch Files<br/>CSV/JSON/Parquet]
        I2[Streaming<br/>Kafka/Event Hubs]
        I3[APIs & CDC]
        I4[Cloud Storage<br/>S3/ADLS/GCS]
    end

    subgraph Store [2. Storage]
        ST1[Delta Lake<br/>ACID transactions]
        ST2[Unity Catalog<br/>Governance]
    end

    subgraph Transform [3. Transformation]
        T1[Spark / PySpark]
        T2[DBSQL]
        T3[SDP Pipelines]
    end

    subgraph Quality [4. Quality]
        Q1[Expectations]
        Q2[Constraints]
        Q3[Great Expectations]
        Q4[Data Monitoring]
    end

    subgraph Serve [5. Serving]
        SV1[BI Dashboards]
        SV2[ML Models]
        SV3[APIs / Delta Sharing]
        SV4[GenAI / RAG]
    end

    subgraph Orchestrate [6. Orchestration]
        O1[Lakeflow Jobs]
        O2[SDP Pipelines]
        O3[DAB Bundles]
    end

    I1 --> ST1
    I2 --> ST1
    I3 --> ST1
    I4 --> ST1
    ST1 --> T1
    ST1 --> T2
    ST1 --> T3
    T1 --> Q1
    T2 --> Q2
    T3 --> Q1
    Q1 --> SV1
    Q1 --> SV2
    Q1 --> SV3
    Q1 --> SV4
    O1 --> I1
    O1 --> T1
    O2 --> T3
    O3 --> O1
```

## Key Concepts

### 1. ETL vs ELT

| Approach | Description | Databricks Implementation |
|----------|-------------|-------------------------|
| **ETL** (Extract-Transform-Load) | Transform before loading to storage | Spark jobs that transform and write to Delta |
| **ELT** (Extract-Load-Transform) | Load raw, then transform in-place | Auto Loader → Bronze (raw) → Silver (transformed) |

**Databricks recommendation**: ELT with the Medallion Architecture — load raw data first, then transform progressively.

### 2. Idempotency

A pipeline is **idempotent** if running it multiple times produces the same result without side effects.

```python
# ❌ Not idempotent — duplicates on re-run
df.write.mode("append").saveAsTable("my_table")

# ✅ Idempotent — MERGE handles upserts
df.createOrReplaceTempView("updates")
spark.sql("""
    MERGE INTO my_table AS t
    USING updates AS s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# ✅ Idempotent — overwrite (for full refresh)
df.write.mode("overwrite").saveAsTable("my_table")
```

### 3. Data Quality

| Tool | When to Use | Severity |
|------|-------------|----------|
| **SDP `@dlt.expect`** | Log warning, keep row | Low severity anomalies |
| **SDP `@dlt.expect_or_drop`** | Drop bad rows, log metric | Source data issues |
| **SDP `@dlt.expect_or_fail`** | Fail the pipeline | Business-critical rules |
| **Delta constraints** | SQL-level CHECK / NOT NULL | Schema enforcement |
| **Great Expectations** | Python library, rich validation | Complex validation suites |
| **Data Quality Monitoring** | Automated drift detection | Production monitoring |

### 4. Lineage

```mermaid
flowchart LR
    A[Source Table] --> B[Transform Notebook] --> C[Silver Table] --> D[Gold View] --> E[Dashboard]
    A -.->|tracked by| L[Unity Catalog Lineage]
    B -.-> L
    C -.-> L
    D -.-> L
    E -.-> L
```

Databricks Unity Catalog automatically tracks lineage across:
- SQL queries, notebooks, and pipelines
- Upstream (source) and downstream (target) tables
- Column-level lineage for SELECT and JOIN operations
- Access via `system.access.table_lineage` and `system.access.column_lineage`

### 5. Incremental vs Full Refresh

| Strategy | Description | When to Use |
|----------|-------------|------------|
| **Full refresh** | Overwrite entire table | Small tables, dimension tables, initial loads |
| **Incremental (MERGE)** | Upsert only new/changed rows | Large fact tables, streaming data |
| **Incremental (CDF)** | Process only changes since last run | CDC pipelines, sync to downstream |
| **Incremental (Auto Loader)** | Process only new files | File-based ingestion |

### 6. Schema Evolution & Management

```mermaid
flowchart TD
    SC1[Source adds new column] --> SC2{Schema evolution mode?}
    SC2 -->|addNewColumns| SC3[Auto-add to Delta table]
    SC2 -->|failOnMissingColumns| SC4[Fail pipeline]
    SC2 -->|noEvolution| SC5[Ignore new column]
    SC3 --> SC6[MERGE_SCHEMA=true]
    SC6 --> SC7[Table now has new column]
```

### 7. Partitioning vs Liquid Clustering

| Strategy | Columns | When | Maintenance |
|----------|---------|------|-------------|
| **Partitioning** | Low cardinality (< 1000 values) | Date columns, regions | Manual OPTIMIZE |
| **ZORDER** | High cardinality filter columns | Join keys, IDs | Manual OPTIMIZE |
| **Liquid Clustering** | Multiple filter columns | New tables, evolving queries | Self-optimizing |

### 8. Stream Processing Patterns

| Pattern | Description |
|---------|-------------|
| **Tumbling windows** | Fixed, non-overlapping time windows (e.g., 1-min metrics) |
| **Sliding windows** | Overlapping windows (e.g., 10-min rolling avg, updated every 1 min) |
| **Session windows** | Dynamic windows based on activity gaps |
| **Watermarks** | Define how late data is accepted before dropping |
| **Deduplication** | Drop duplicate events within watermark window |
| **Stream-static join** | Enrich stream with dimension/lookup tables |
| **Stream-stream join** | Correlate two streams (impressions + clicks) |

## Orchestration Options

```mermaid
graph TB
    subgraph "Lakeflow Jobs"
        J1[Notebook Task] --> J2[Python Script Task]
        J2 --> J3[SQL Task]
        J3 --> J4[Pipeline Task]
        J4 --> J5[If/Else Condition]
        J5 --> J6[Email Notification]
    end

    subgraph "SDP Pipelines"
        P1[Continuous] --> P2[Triggered]
        P2 --> P3[Scheduled]
    end

    subgraph "Declarative Automation Bundles (DAB)"
        D1[Infrastructure as Code] --> D2[CI/CD Integration] --> D3[Environment Promotion]
    end
```

## Best Practices Checklist

- [ ] **Idempotent pipelines** — use MERGE or overwrite, never blind append
- [ ] **Checkpointing** — always use checkpoint locations for streaming
- [ ] **Data quality rules** — at least one expectation per layer
- [ ] **Schema management** — use schema inference with `addNewColumns` for evolving sources
- [ ] **Partitioning** — partition by date or low-cardinality columns only
- [ ] **VACUUM** — schedule regularly to reclaim storage (respect retention)
- [ ] **OPTIMIZE** — schedule file compaction for tables with frequent writes
- [ ] **Lineage** — use Unity Catalog for automatic lineage tracking
- [ ] **Monitoring** — set up alerts for pipeline failures and data quality drops
- [ ] **CI/CD** — use Declarative Automation Bundles for environment promotion