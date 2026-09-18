# Databricks Platform Overview

## Architecture — Component Diagram

```mermaid
graph TB
    subgraph Control [Control Plane]
        UC[Unity Catalog<br/>Governance & Lineage]
        WS[Workspace<br/>Notebooks, SQL, Dashboards]
        API[Databricks API<br/>REST & SDK]
        IAM[IAM & Access Control]
    end

    subgraph Compute [Compute Plane]
        SV[Serverless Compute<br/>Auto-scaling, no infra]
        CL[Classic Clusters<br/>Multi-purpose, all-purpose]
        WH[SQL Warehouses<br/>BI & SQL analytics]
        ML_C[ML Compute<br/>GPU, distributed training]
    end

    subgraph Storage [Storage — Delta Lake]
        DL[Delta Tables<br/>ACID, Time Travel, CDF]
        UC_Vol[Volumes<br/>File storage in UC]
        UC_Meta[Metastore<br/>Table & schema metadata]
    end

    subgraph Cloud [Cloud Provider — AWS]
        S3[S3 Buckets]
        EC2[EC2 Instances]
        VPC[VPC / Private Link]
        KMS[KMS Encryption]
    end

    subgraph AI [AI / ML Services]
        MLF[MLflow<br/>Tracking & Registry]
        FS[Feature Store<br/>Feature Engineering]
        MS[Model Serving<br/>Real-time inference]
        VS[Vector Search<br/>RAG & similarity]
        AG[Agent Bricks<br/>GenAI applications]
    end

    subgraph DataEng [Data Engineering]
        AL[Auto Loader<br/>File ingestion]
        SDP[Spark Declarative<br/>Pipelines]
        SS[Structured<br/>Streaming]
        JOBS[Lakeflow Jobs<br/>Orchestration]
    end

    UC --> Storage
    WS --> Compute
    API --> Compute
    Compute --> Storage
    Storage --> Cloud
    Compute --> Cloud
    AI --> Storage
    AI --> Compute
    DataEng --> Storage
    DataEng --> Compute
    UC --> AI
    UC --> DataEng
```

## Platform Components

### Compute

| Component | Purpose | Best For |
|-----------|---------|---------|
| **Serverless Compute** | Auto-provisioned, auto-scaled, pay-per-use | Notebooks, jobs, pipelines (no infrastructure management) |
| **Classic Clusters** | User-managed Spark clusters | Custom configurations, interactive development |
| **SQL Warehouses** | Optimized for SQL queries | BI tools, dashboards, ad-hoc SQL analytics |
| **ML Compute** | GPU-enabled clusters | Model training, distributed deep learning |

### Storage — Delta Lake

| Feature | Description |
|---------|-------------|
| **ACID transactions** | Serializable isolation level for concurrent reads/writes |
| **Time Travel** | Query data at any historical version or timestamp |
| **Change Data Feed (CDF)** | Row-level change tracking (inserts, updates, deletes) |
| **Schema evolution** | `MERGE_SCHEMA` and `addNewColumns` for evolving schemas |
| **Deletion Vectors** | Lazy deletion — faster UPDATE/DELETE without file rewrites |
| **Liquid Clustering** | Self-optimizing data layout for multi-column filtering |
| **OPTIMIZE / ZORDER** | File compaction and data co-location |
| **VACUUM** | Remove orphaned files past retention threshold |

### Unity Catalog — Governance

```mermaid
graph TB
    subgraph "Three-Level Namespace"
        C[Catalog] --> S1[Schema 1] --> T1[Table 1]
        S1 --> T2[Table 2]
        C --> S2[Schema 2] --> T3[Table 3]
        C --> S3[Schema 3] --> T4[Volume]
    end

    subgraph Governance Features
        G1[GRANT / REVOKE] --> G2[Tags & Classification]
        G2 --> G3[Row-Level Security]
        G3 --> G4[Column Masking]
        G4 --> G5[Audit Logs]
        G5 --> G6[Lineage]
    end

    C -.-> G1
```

### Data Engineering Services

| Service | Description |
|---------|-------------|
| **Auto Loader** | Incremental file ingestion from cloud storage with schema inference |
| **Structured Streaming** | Real-time stream processing with watermarks and windowed aggregations |
| **Spark Declarative Pipelines (SDP)** | Declarative pipeline framework with data quality expectations |
| **Lakeflow Jobs** | Workflow orchestration (notebook tasks, SQL tasks, pipeline tasks, conditions) |
| **Declarative Automation Bundles (DAB)** | Infrastructure-as-code for CI/CD and environment promotion |

### AI / ML Services

| Service | Description |
|---------|-------------|
| **MLflow** | Experiment tracking, model registry, model serving |
| **Feature Store** | Centralized feature management with point-in-time joins |
| **Model Serving** | Real-time inference via REST API endpoints |
| **Vector Search** | Similarity search over embeddings for RAG |
| **AI Gateway** | Rate limiting, fallback, logging for LLM endpoints |
| **Agent Bricks** | Managed GenAI apps (Knowledge Assistant, Supervisor Agent) |
| **Mosaic AI Agent Framework** | Build custom AI agents with tools |
| **AI Functions (SQL)** | `ai_query`, `ai_forecast`, `ai_analyze_sentiment` in SQL |

## UML Class Diagram — Key Platform Entities

```mermaid
classDiagram
    class Workspace {
        +String workspace_url
        +List notebooks
        +List dashboards
        +List queries
        +create_notebook() Notebook
        +create_dashboard() Dashboard
    }

    class UnityCatalog {
        +List catalogs
        +List schemas
        +grant_access() void
        +apply_tag() void
        +get_lineage() LineageGraph
    }

    class Catalog {
        +String name
        +String owner
        +List schemas
    }

    class Schema {
        +String name
        +List tables
        +List volumes
    }

    class DeltaTable {
        +String name
        +StructType schema
        +List properties
        +merge(updates) void
        +optimize() void
        +vacuum() void
        +time_travel(version) DataFrame
        +read_cdf() DataFrame
    }

    class Pipeline {
        +String name
        +String target_schema
        +Boolean continuous
        +run_update() void
        +get_events() DataFrame
    }

    class MLflowExperiment {
        +String name
        +List runs
        +log_param() void
        +log_metric() void
        +log_model() void
        +search_runs() DataFrame
    }

    class ModelRegistry {
        +String model_name
        +List versions
        +String current_stage
        +register_model() void
        +transition_stage() void
    }

    class VectorSearchIndex {
        +String endpoint_name
        +String index_name
        +Int embedding_dim
        +similarity_search() List
    }

    Workspace --> UnityCatalog : uses
    UnityCatalog --> Catalog : contains
    Catalog --> Schema : contains
    Schema --> DeltaTable : contains
    DeltaTable --> Pipeline : managed by
    DeltaTable --> MLflowExperiment : logs to
    MLflowExperiment --> ModelRegistry : registers to
    Schema --> VectorSearchIndex : indexes from
```

## Compute Selection Guide

| Workload | Recommended Compute | Why |
|----------|--------------------|-----|
| Interactive notebooks | Serverless | Auto-provisioned, no wait time |
| Scheduled ETL jobs | Serverless or Jobs cluster | Pay per use, auto-terminate |
| Streaming pipelines | Serverless | Continuous, auto-scaling |
| SQL analytics / BI | SQL Warehouse | Optimized for SQL, auto-start/stop |
| Model training | ML Compute (GPU) | Distributed training, GPU acceleration |
| Model serving | Serverless | Scale-to-zero, pay-per-request |
| RAG / GenAI | Serverless | Foundation Model APIs are serverless |

## Learning Path — This Repo's Modules

```mermaid
flowchart TD
    START([Start Here]) --> M01[01 — Medallion Fundamentals<br/>Beginner]
    M01 --> M02[02 — Auto Loader<br/>Intermediate]
    M01 --> M03[03 — Delta Lake Advanced<br/>Advanced]
    M02 --> M06[06 — Structured Streaming<br/>Advanced]
    M03 --> M04[04 — SDP Pipelines<br/>Advanced]
    M03 --> M08[08 — Databricks SQL<br/>Intermediate]
    M04 --> M05[05 — UC Governance<br/>Advanced]
    M01 --> M07[07 — MLflow Tracking<br/>Intermediate]
    M07 --> M09[09 — Feature Engineering<br/>Advanced]
    M07 --> M10[10 — GenAI / RAG<br/>Advanced]
    
    style START fill:#4CAF10,color:#fff
    style M01 fill:#2196F3,color:#fff
    style M02 fill:#FF9800,color:#fff
    style M03 fill:#f44336,color:#fff
    style M04 fill:#f44336,color:#fff
    style M05 fill:#f44336,color:#fff
    style M06 fill:#f44336,color:#fff
    style M07 fill:#FF9800,color:#fff
    style M08 fill:#FF9800,color:#fff
    style M09 fill:#f44336,color:#fff
    style M10 fill:#f44336,color:#fff
```

| Level | Color | Modules |
|-------|-------|---------|
| Beginner | 🟦 Blue | 01 — Medallion Fundamentals |
| Intermediate | 🟧 Orange | 02, 07, 08 |
| Advanced | 🟥 Red | 03, 04, 05, 06, 09, 10 |