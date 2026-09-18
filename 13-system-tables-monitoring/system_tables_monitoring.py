# Databricks notebook source
# DBTITLE 1,System Tables Overview
# MAGIC %md
# MAGIC # System Tables & Monitoring
# MAGIC
# MAGIC **Objective**: Query Databricks operational data via `system.*` tables — billing, compute, audit logs, query history, job/pipeline runs — and set up data quality monitoring.
# MAGIC
# MAGIC ## System Tables Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph "system.* (read-only)"
# MAGIC         B[system.billing.usage] --> C[Cost attribution]
# MAGIC         CO[system.compute.clusters] --> W[Warehouse utilization]
# MAGIC         A[system.access.audit] --> AU[Audit trail]
# MAGIC         Q[system.query.history] --> QP[Query performance]
# MAGIC         L[system.lakeflow.jobs] --> JR[Job runs and reliability]
# MAGIC         LE[system.lakeflow.pipeline_events] --> PM[Pipeline monitoring]
# MAGIC     end
# MAGIC     subgraph "Monitoring"
# MAGIC         DQ[Data Quality<br/>Monitoring] --> AL[Alerts]
# MAGIC         PM --> AL
# MAGIC         JR --> AL
# MAGIC     end
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: Billing & Cost
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Billing and Cost Attribution
# MAGIC
# MAGIC `system.billing.usage` tracks DBU consumption. Join with `system.billing.list_prices` for dollar attribution.

# COMMAND ----------

# Daily DBU usage by sku
print('Daily DBU Usage (last 7 days):')
spark.sql("""
SELECT
    usage_date,
    sku_name,
    sum(usage_quantity) AS dbus,
    round(sum(usage_quantity) * avg(list_price), 2) AS estimated_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
WHERE usage_date >= dateadd(day, -7, current_date())
    AND p.price_start_time <= u.usage_date
    AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_date)
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC, dbus DESC
LIMIT 20
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 2: Compute Utilization
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Compute Utilization
# MAGIC
# MAGIC `system.compute.clusters` (SCD2) and `system.compute.warehouses` track compute resources.

# COMMAND ----------

# Active serverless compute (dedup SCD2 — take latest version per cluster)
print('Active Compute Clusters (latest state):')
spark.sql("""
SELECT 
    cluster_id,
    cluster_name,
    cluster_source,
    worker_node_type,
    worker_count,
    change_time
FROM system.compute.clusters
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY workspace_id, cluster_id 
    ORDER BY change_time DESC
) = 1
WHERE delete_time IS NULL
ORDER BY change_time DESC
LIMIT 15
""").display()

# SQL Warehouse utilization
print('\nSQL Warehouse Activity (recent):')
spark.sql("""
SELECT
    warehouse_id,
    warehouse_name,
    state,
    cluster_count,
    auto_stop_mins
FROM system.compute.warehouses
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY warehouse_id
    ORDER BY change_time DESC
) = 1
ORDER BY change_time DESC
LIMIT 10
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 3: Audit Logs
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Audit Logs — Access and Security
# MAGIC
# MAGIC `system.access.audit` records all workspace operations — who did what and when.

# COMMAND ----------

# Recent audit events by action
print('Recent Audit Events (last 24 hours):')
spark.sql("""
SELECT
    event_date,
    action_name,
    count(*) AS event_count,
    collect_set(useridentity.email) AS users
FROM system.access.audit
WHERE event_date >= dateadd(hour, -24, current_timestamp())
GROUP BY event_date, action_name
ORDER BY event_count DESC
LIMIT 15
""").display()

# Failed access attempts
print('\nFailed Access Attempts:')
spark.sql("""
SELECT
    event_time,
    action_name,
    useridentity.email AS user,
    request_params,
    response.error_message
FROM system.access.audit
WHERE event_date >= dateadd(day, -7, current_date())
    AND response.status_code >= 400
ORDER BY event_time DESC
LIMIT 10
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Query History
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Query History and Performance
# MAGIC
# MAGIC `system.query.history` captures all SQL statements with performance metrics.

# COMMAND ----------

# Slowest queries
print('Top 10 Slowest Queries (last 7 days):')
spark.sql("""
SELECT
    start_time,
    statement_type,
    executed_as_user_email,
    total_task_time_ms / 1000 AS total_seconds,
    rows_produced,
    LEFT(statement_text, 120) AS query_preview
FROM system.query.history
WHERE start_time >= dateadd(day, -7, current_date())
ORDER BY total_task_time_ms DESC
LIMIT 10
""").display()

# Query count by user
print('\nQuery Count by User (last 7 days):')
spark.sql("""
SELECT
    executed_as_user_email AS user,
    count(*) AS query_count,
    round(sum(total_task_time_ms) / 1000, 1) AS total_time_seconds
FROM system.query.history
WHERE start_time >= dateadd(day, -7, current_date())
GROUP BY executed_as_user_email
ORDER BY query_count DESC
LIMIT 10
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 5: Lakeflow Jobs Runs
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Lakeflow Jobs — Runs and Reliability
# MAGIC
# MAGIC `system.lakeflow.jobs` (SCD2) and `system.lakeflow.job_runs` track job definitions and execution history.

# COMMAND ----------

# Job run history
print('Recent Job Runs:')
spark.sql("""
SELECT
    workspace_id,
    job_id,
    job_name,
    result_state,
    start_time,
    end_time,
    task_run_count
FROM system.lakeflow.job_runs
WHERE start_time >= dateadd(day, -7, current_date())
ORDER BY start_time DESC
LIMIT 20
""").display()

# Job reliability — success rate
print('\nJob Success Rate (last 30 days):')
spark.sql("""
SELECT
    job_name,
    count(*) AS total_runs,
    sum(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful,
    round(
        sum(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / count(*),
        1
    ) AS success_rate_pct
FROM system.lakeflow.job_runs
WHERE start_time >= dateadd(day, -30, current_date())
GROUP BY job_name
ORDER BY total_runs DESC
LIMIT 15
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 6: Data Quality Monitoring
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Data Quality Monitoring
# MAGIC
# MAGIC Use Lakehouse Monitoring to track data quality metrics on your tables.

# COMMAND ----------

print('Data Quality Monitoring — Setup Example:')
print()
print('from databricks.sdk.service.catalog import CreateMonitor, IngestionLogType')
print()
print('# Create a monitor on a Delta table')
print('w.quality_monitors.create(')
print('    table_name="demo.bronze.customer_transactions",')
print('    assets_dir="/Workspace/monitoring/bronze_assets",')
print('    create_monitor=CreateMonitor(')
print('        schedule={"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"},')
print('        ingestion_log_type=IngestionLogType.CDF,')
print('        baseline_table_name="demo.silver.customer_transactions_cleaned",')
print('        sliced_metrics=True,')
print('    )')
print(')')
print()
print('Monitoring Metrics Tracked:')
print('   - Null counts and rates per column')
print('   - Distinct value counts')
print('   - Min/max/mean/median for numeric columns')
print('   - Data drift detection vs baseline')
print('   - Freshness and volume metrics')
print()
print('Monitor data is stored in *_profile and *_status tables')
print('   e.g., demo.bronze.customer_transactions_profile')
print('   e.g., demo.bronze.customer_transactions_status')

# COMMAND ----------

# DBTITLE 1,Cell 7: SQL Alerts
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: SQL Alerts on System Tables
# MAGIC
# MAGIC Set up SQL alerts to notify when costs spike, jobs fail, or data quality drops.

# COMMAND ----------

print('SQL Alert Examples:')
print()
print('1. Cost Spike Alert — Notify when daily DBU > 1000')
print('   SELECT sum(usage_quantity) AS dbus FROM system.billing.usage')
print('   WHERE usage_date = current_date()')
print('   Condition: dbus > 1000')
print()
print('2. Job Failure Alert — Notify when a job fails')
print('   SELECT count(*) AS failures FROM system.lakeflow.job_runs')
print('   WHERE result_state = FAILED AND start_time >= dateadd(hour, -1, now())')
print('   Condition: failures > 0')
print()
print('3. Data Quality Alert — Notify when null rate exceeds 5%')
print('   SELECT sum(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 100.0 / count(*) AS null_rate')
print('   FROM demo.bronze.customer_transactions')
print('   Condition: null_rate > 5')
print()
print('Create via SDK:')
print('   w.alerts.create(name="cost-spike",')
print('       query={"query": "SELECT sum(usage_quantity) AS dbus FROM system.billing.usage WHERE usage_date = current_date()"},')
print('       options={"op": ">", "value": "1000", "muted": False})')

# COMMAND ----------

