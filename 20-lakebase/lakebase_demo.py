# Databricks notebook source
# DBTITLE 1,Lakebase Overview
# MAGIC %md
# MAGIC # Lakebase — Postgres Autoscaling on Databricks
# MAGIC
# MAGIC **Objective**: Learn Lakebase — managed PostgreSQL with autoscaling, branching, and reverse ETL to Delta Lake. This module covers projects, branches, endpoints, and data sync patterns.
# MAGIC
# MAGIC ## Lakebase Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC     subgraph "Lakebase"
# MAGIC         P[Project] --> B1[Main Branch]
# MAGIC         B1 --> B2[Feature Branch]
# MAGIC         B1 --> E[Endpoint<br/>Autoscaling Postgres]
# MAGIC         B2 --> E2[Dev Endpoint]
# MAGIC     end
# MAGIC     subgraph "Data Flow"
# MAGIC         E -->|Data API| APP[Applications]
# MAGIC         E -->|Reverse ETL| DL[Delta Lake<br/>Unity Catalog]
# MAGIC         UC[Unity Catalog Tables] -->|Sync| E
# MAGIC     end
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: Lakebase Concepts
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Lakebase Concepts and Project Setup

# COMMAND ----------

print('Lakebase Concepts:')
print()
print('1. **Project**: A Lakebase project is a managed PostgreSQL database')
print('   - Contains branches (like Git for databases)')
print('   - Has an autoscaling compute endpoint')
print('   - Can sync data to/from Unity Catalog')
print()
print('2. **Branches**: Database-level branching')
print('   - main: production data')
print('   - feature/*: isolated copies for development')
print('   - Instant branching (no data copy needed)')
print()
print('3. **Endpoint**: The compute layer')
print('   - Autoscales based on load')
print('   - Scale-to-zero when idle')
print('   - High availability option')
print('   - Accessible via Data API (REST) or Postgres protocol')
print()
print('4. **Reverse ETL**: Sync Lakebase data to Delta Lake')
print('   - Tables in Lakebase can sync to Unity Catalog')
print('   - Enables analytics on operational data')
print()
print('5. **Use Cases**:')
print('   - Operational database for apps running on Databricks Apps')
print('   - Low-latency lookups for ML serving')
print('   - Test data management with branching')
print('   - Stateful application backends')
print()
print('Prerequisites:')
print('   - databricks-sdk >= 0.118.0')
print('   - Lakebase enabled in workspace')
print('   - Unity Catalog enabled')

# COMMAND ----------

# DBTITLE 1,Cell 2: Create Project and Branch
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Create Project, Branch, and Endpoint

# COMMAND ----------

print('Lakebase SDK Operations:')
print()
print('from databricks.sdk import WorkspaceClient')
print('w = WorkspaceClient()')
print()
print('# 1. Create a project')
print('project = w.postgres.create_project(')
print('    name="learning-lakebase",')
print('    description="Learning demo - Lakebase Postgres",')
print('    region="us-west-2",')
print(')')
print('print(f"Project created: {project.name}")')
print()
print('# 2. Create the main branch')
print('main_branch = w.postgres.create_branch(')
print('    project_name=project.name,')
print('    branch_name="main",')
print(')')
print()
print('# 3. Create a feature branch (instant copy)')
print('feature_branch = w.postgres.create_branch(')
print('    project_name=project.name,')
print('    branch_name="feature-add-users",')
print('    source_branch="main",')
print(')')
print('print(f"Branch created: {feature_branch.branch_name}")')
print()
print('# 4. Create an endpoint (autoscaling compute)')
print('endpoint = w.postgres.create_endpoint(')
print('    project_name=project.name,')
print('    branch_name="main",')
print('    endpoint_name="prod-endpoint",')
print('    min_compute_units=1,')
print('    max_compute_units=5,')
print(')')
print('print(f"Endpoint created: {endpoint.endpoint_name}")')
print()
print('# 5. List projects')
print('for p in w.postgres.list_projects():')
print('    print(f"  Project: {p.name}")')
print()
print('# 6. List branches')
print('for b in w.postgres.list_branches(project_name=project.name):')
print('    print(f"  Branch: {b.branch_name} | Source: {b.source_branch}")')
print()
print('# 7. List endpoints')
print('for e in w.postgres.list_endpoints(project_name=project.name):')
print('    print(f"  Endpoint: {e.endpoint_name} | State: {e.state}")

# COMMAND ----------

# DBTITLE 1,Cell 3: Data Access
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Data Access — Data API and Postgres Protocol

# COMMAND ----------

print('Data Access Methods:')
print()
print('=== 1. Data API (REST) ===')
print('# Auto-authenticated via Databricks')
print('import requests')
print('url = f"https://{workspace_host}/api/2.0/postgres/projects/{project}/endpoints/{endpoint}/data"')
print()    
print('# Query')
print('response = requests.post(url, json={')
print('    "query": "SELECT * FROM users WHERE active = true",')
print('})')
print('rows = response.json()["rows"]')
print()
print('# Insert')
print('response = requests.post(url, json={')
print('    "query": "INSERT INTO users (name, email) VALUES ($1, $2)",')
print('    "params": ["Alice", "alice@example.com"],')
print('})')
print()
print('=== 2. Postgres Protocol ===')
print('import psycopg2')
print('conn = psycopg2.connect(')
print('    host=endpoint_host,')
print('    port=5432,')
print('    database=project_name,')
print('    user=dbutils.secrets.get("lb-scope", "user"),')
print('    password=dbutils.secrets.get("lb-scope", "token"),')
print('    sslmode=require,')
print(')')
print('cursor = conn.cursor()')
print('cursor.execute("SELECT * FROM users LIMIT 10")')
print('rows = cursor.fetchall()')
print()
print('=== 3. From Databricks Apps ===')
print('# In app.py:')
print('import os, requests')
print('token = os.environ["DATABRICKS_TOKEN"]')
print('headers = {"Authorization": f"Bearer {token}"}')
print('response = requests.post(data_api_url, json={"query": "..."}, headers=headers)')

# COMMAND ----------

# DBTITLE 1,Cell 4: Reverse ETL
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Reverse ETL and Sync to Delta Lake

# COMMAND ----------

print('Reverse ETL — Sync Lakebase to Unity Catalog:')
print()
print('# Create a synced table (Lakebase -> Delta)')
print('w.postgres.create_synced_table(')
print('    project_name=project.name,')
print('    branch_name="main",')
print('    table_name="users",')
print('    catalog_name="demo",')
print('    schema_name="lakebase",')
print('    sync_mode="incremental",')
print('    primary_key="id",')
print(')')
print()
print('# Query synced data in Delta Lake')
print('spark.sql("SELECT * FROM demo.lakebase.users").display()')
print()
print('# Sync from Unity Catalog to Lakebase (forward ETL)')
print('w.postgres.create_sync(')
print('    project_name=project.name,')
print('    branch_name="main",')
print('    source_catalog="demo",')
print('    source_schema="gold",')
print('    source_table="customer_spending",')
print('    destination_table="customer_spending",')
print(')')
print()
print('Key Concepts:')
print('   - Reverse ETL: Lakebase -> Delta Lake (operational to analytics)')
print('   - Forward ETL: Delta Lake -> Lakebase (analytics to operational)')
print('   - Synced tables use primary keys for incremental updates')
print('   - Sync runs on a configurable schedule')
print('   - Data is available in both systems simultaneously')
print()
print('Clean Up:')
print('   w.postgres.delete_endpoint(project_name=project.name, endpoint_name="prod-endpoint")')
print('   w.postgres.delete_branch(project_name=project.name, branch_name="feature-add-users")')
print('   w.postgres.delete_project(project_name=project.name)')
print('   print("Cleaned up Lakebase demo resources")')

# COMMAND ----------

