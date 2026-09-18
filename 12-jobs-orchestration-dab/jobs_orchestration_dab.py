# Databricks notebook source
# DBTITLE 1,Jobs & DAB Overview
# MAGIC %md
# MAGIC # Lakeflow Jobs, Orchestration & Declarative Automation Bundles (DAB)
# MAGIC
# MAGIC **Objective**: Learn how to orchestrate Databricks workloads using Lakeflow Jobs (multi-task workflows, scheduling, triggers) and Declarative Automation Bundles (DAB) for infrastructure-as-code and CI/CD.
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC     subgraph "Lakeflow Jobs"
# MAGIC         J[Job Definition] --> T1[Task 1: Notebook]
# MAGIC         T1 --> T2[Task 2: SQL Query]
# MAGIC         T2 --> T3[Task 3: Python Script]
# MAGIC         T1 --> T4[Task 4: Pipeline]
# MAGIC     end
# MAGIC     subgraph "Triggers"
# MAGIC         S[Schedule<br/>Cron] --> J
# MAGIC         F[File Arrival] --> J
# MAGIC         E[Event] --> J
# MAGIC         M[Manual] --> J
# MAGIC     end
# MAGIC     subgraph "DAB"
# MAGIC         Y[databricks.yml] --> |deploy| J
# MAGIC         Y --> P[Pipeline]
# MAGIC         Y --> EP[Serving Endpoint]
# MAGIC     end
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: Create Multi-Task Job
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Create a Multi-Task Job via Databricks SDK
# MAGIC
# MAGIC Use the Databricks SDK to programmatically create a job with task dependencies.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    JobTaskSettings, NotebookTask, TaskDependency, 
    CronSchedule, TriggerType
)
from databricks.sdk.service.workspace import ObjectType

w = WorkspaceClient()

# Define a multi-task job
job_name = "learning-orchestration-demo"

# Remove existing job if present
for j in w.jobs.list(name=job_name):
    w.jobs.delete(j.job_id)
    print(f"Removed existing job: {j.job_id}")

job = w.jobs.create(
    name=job_name,
    tasks=[
        {
            "task_key": "bronze_ingestion",
            "description": "Run bronze layer ingestion notebook",
            "notebook_task": {
                "notebook_path": "/Repos/martin.mystery9@gmail.com/dataengineering/01-medallion-fundamentals/simple_medallion_architecture",
            },
        },
        {
            "task_key": "data_validation",
            "description": "Validate data quality after ingestion",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": "/Repos/martin.mystery9@gmail.com/dataengineering/03-delta-advanced/delta_advanced_features",
            },
        },
        {
            "task_key": "ml_training",
            "description": "Train model on processed data",
            "depends_on": [{"task_key": "data_validation"}],
            "notebook_task": {
                "notebook_path": "/Repos/martin.mystery9@gmail.com/dataengineering/11-end-to-end-ml-pipeline/end_to_end_ml_pipeline",
            },
        },
    ],
    schedule={
        "quartz_cron_expression": "0 0 8 * * ?",  # Daily at 8 AM UTC
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED",
    },
    tags={"module": "12", "purpose": "learning", "env": "dev"},
)

print(f"✅ Job created: {job.job_id}")
print(f"   Name: {job_name}")
print(f"   Tasks: bronze_ingestion → data_validation → ml_training")
print(f"   Schedule: Daily at 8:00 AM UTC")

# COMMAND ----------

# DBTITLE 1,Cell 2: Trigger Types
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Trigger Types — Schedule, File Arrival, Manual

# COMMAND ----------

print("📋 Lakeflow Jobs Trigger Types:")
print()
print("1. **Schedule (Cron)** — Recurring on a cron expression")
print("   quartz_cron_expression: '0 0 8 * * ?'  # Daily at 8 AM")
print("   quartz_cron_expression: '0 0 0 ? * MON' # Weekly Monday midnight")
print()
print("2. **File Arrival** — Trigger when a file lands in a UC volume or cloud storage")
print("   file_arrival: {url: 's3://bucket/path/', min_size: 1024}")
print()
print("3. **Continuous / Run Continuous** — Always-on for streaming jobs")
print("   continuous: {type: 'PIPELINE'}")
print()
print("4. **Table Update** — Trigger when a table changes")
print("   table_update: {table_name: 'demo.bronze.events', condition: 'NEW_ROWS'}")
print()
print("5. **Manual / Run Now** — Triggered on-demand")
print()

# Run the job we created
run = w.jobs.run_now(job_id=job.job_id)
print(f"✅ Triggered manual run: {run.run_id}")
print(f"   Monitor at: #workspace/jobs/{job.job_id}/runs/{run.run_id}")

# COMMAND ----------

# DBTITLE 1,Cell 3: Task Types & Dependencies
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Task Types & Dependencies
# MAGIC
# MAGIC Lakeflow Jobs supports multiple task types that can be chained with dependencies.

# COMMAND ----------

print("📋 Lakeflow Jobs Task Types:")
print()
print("| Task Type | Key | Description |")
print("|-----------|-----|-------------|")
print("| Notebook | notebook_task | Run a Databricks notebook |")
print("| SQL Query | sql_task | Run a SQL warehouse query |")
print("| Python Script | spark_python_task | Run a .py file |")
print("| Pipeline | pipeline_task | Run an SDP pipeline |")
print("| DLT Pipeline | pipeline_task | Run Declarative Pipeline |")
print("| Python Wheel | python_wheel_task | Run a packaged wheel |")
print("| JAR | spark_jar_task | Run a Scala JAR |")
print("| Shell Script | dbt_task / custom | Run dbt or shell commands |")
print("| If/Else | condition_task | Conditional branching |")
print("| For Each | for_each_task | Loop over parameters |")
print()
print("🔗 Dependency Patterns:")
print("   depends_on: [{task_key: 'upstream_task'}]")
print("   ALL_OF (default) — runs when ALL dependencies succeed")
print("   ANY_OF — runs when ANY dependency succeeds")
print()
print("📊 Example DAG:")
print("   bronze_ingestion ──► data_validation ──► ml_training")
print("                                            ──► report_generation")
print("   Both ml_training and report_generation depend on data_validation")

# COMMAND ----------

# DBTITLE 1,Cell 4: DAB Structure
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Declarative Automation Bundles (DAB)
# MAGIC
# MAGIC DAB lets you define your entire Databricks project as code in a `databricks.yml` file and deploy it with `databricks bundle deploy`.

# COMMAND ----------

dab_template = """# databricks.yml — Declarative Automation Bundle
# This file defines all Databricks resources as code.
# Deploy with: databricks bundle deploy
# Run with: databricks bundle run <job_name>

bundle:
  name: dataengineering-learning

variables:
  catalog:
    description: "Unity Catalog for resources"
    default: demo
  env:
    description: "Environment (dev/staging/prod)"
    default: dev

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: demo
      env: dev
  staging:
    mode: staging
    variables:
      catalog: staging
      env: staging
  prod:
    mode: production
    variables:
      catalog: prod
      env: prod

resources:
  jobs:
    medallion_pipeline:
      name: medallion-pipeline-${var.env}
      tasks:
        - task_key: bronze
          notebook_task:
            notebook_path: /Workspace/Repos/dataengineering/01-medallion-fundamentals/simple_medallion_architecture
          job_clusters:
            - job_cluster_key: serverless

        - task_key: silver
          depends_on:
            - task_key: bronze
          notebook_task:
            notebook_path: /Workspace/Repos/dataengineering/03-delta-advanced/delta_advanced_features

        - task_key: gold
          depends_on:
            - task_key: silver
          notebook_task:
            notebook_path: /Workspace/Repos/dataengineering/08-databricks-sql/databricks_sql_features

      schedule:
        quartz_cron_expression: "0 0 8 * * ?"
        timezone_id: UTC

      job_clusters:
        - job_cluster_key: serverless
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2

  pipelines:
    sdp_pipeline:
      name: sdp-medallion-${var.env}
      development: true
      libraries:
        - notebook:
            path: /Workspace/Repos/dataengineering/04-sdp-pipelines/sdp_medallion_pipeline
      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 3

  models:
    churn_model:
      name: churn_prediction_model
      description: "Churn prediction model from Module 11"
"""

print("📄 Example databricks.yml:")
print(dab_template)

print("\n🔧 DAB CLI Commands:")
print("   databricks bundle validate          # Validate bundle config")
print("   databricks bundle deploy            # Deploy resources to workspace")
print("   databricks bundle deploy -t staging # Deploy to staging target")
print("   databricks bundle run medallion_pipeline  # Run a deployed job")
print("   databricks bundle destroy           # Remove deployed resources")

# COMMAND ----------

# DBTITLE 1,Cell 5: CI/CD with DAB
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: CI/CD with DAB — GitHub Actions Integration

# COMMAND ----------

github_workflow = """# .github/workflows/deploy.yml
name: Deploy Databricks Bundle

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Databricks CLI
        run: curl -fsSL https://databricks.com/install | sh
      
      - name: Configure Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks configure --host $DATABRICKS_HOST --token $DATABRICKS_TOKEN
      
      - name: Validate Bundle
        run: databricks bundle validate
      
      - name: Deploy Bundle (dev)
        if: github.ref == 'refs/heads/develop'
        run: databricks bundle deploy -t dev
      
      - name: Deploy Bundle (prod)
        if: github.ref == 'refs/heads/main'
        run: databricks bundle deploy -t prod
"""

print("📋 GitHub Actions Workflow for DAB CI/CD:")
print(github_workflow)

print("\n💡 CI/CD Best Practices:")
print("   1. Use separate targets (dev/staging/prod) in databricks.yml")
print("   2. Store Databricks tokens as GitHub Actions secrets")
print("   3. Validate bundle before deploying")
print("   4. Use environment-specific variables for catalog names")
print("   5. Auto-deploy on merge to main, manual deploy for prod")

# COMMAND ----------

# DBTITLE 1,Cell 6: Job Monitoring
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Job Monitoring & Run History

# COMMAND ----------

# List recent job runs
print("📋 Recent job runs:")
for run in w.jobs.list_runs(limit=5):
    print(f"   Run {run.run_id} | Job: {run.job_id} | State: {run.status.state if run.status else 'N/A'} | Start: {run.start_time}")

# Get job details
print(f"\n📋 Job details for {job_name}:")
job_details = w.jobs.get(job_id=job.job_id)
print(f"   Job ID: {job_details.job_id}")
print(f"   Tasks: {len(job_details.settings.tasks)}")
for t in job_details.settings.tasks:
    deps = [d.task_key for d in t.depends_on] if t.depends_on else []
    print(f"   - {t.task_key} (depends on: {deps or 'none'})")

# Clean up
print(f"\n🧹 Cleaning up: deleting job {job.job_id}")
w.jobs.delete(job_id=job.job_id)
print("✅ Job deleted (demo complete)")

# COMMAND ----------

