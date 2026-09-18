# Databricks notebook source
# DBTITLE 1,Dashboards & Alerts Overview
# MAGIC %md
# MAGIC # Dashboards, SQL Alerts & Genie Spaces
# MAGIC
# MAGIC **Objective**: Learn Databricks BI and AI capabilities — Lakeview dashboards, SQL alerts for proactive monitoring, and Genie spaces for natural-language data Q&A.
# MAGIC
# MAGIC ## AI/BI Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC     subgraph "Data Sources"
# MAGIC         D[Delta Tables<br/>Unity Catalog] --> DW[SQL Warehouse]
# MAGIC     end
# MAGIC     subgraph "AI/BI Layer"
# MAGIC         DW --> LV[Lakeview Dashboards<br/>Visual analytics]
# MAGIC         DW --> SA[SQL Alerts<br/>Proactive monitoring]
# MAGIC         DW --> GS[Genie Spaces<br/>Natural language Q&A]
# MAGIC     end
# MAGIC     subgraph "Consumption"
# MAGIC         LV --> U[Users<br/>Browser]
# MAGIC         SA --> N[Notifications<br/>Email/Slack/Webhook]
# MAGIC         GS --> C[Chat Interface<br/>Conversational analytics]
# MAGIC     end
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: Lakeview Dashboards
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Lakeview Dashboards
# MAGIC
# MAGIC Build interactive dashboards with the Databricks SDK or via the UI.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import *

w = WorkspaceClient()

print('Lakeview Dashboard Concepts:')
print()
print('1. **Dashboard Structure**:')
print('   - Pages (tabs) containing widgets')
print('   - Widgets: counter, table, chart, text, filter')
print('   - Datasets: SQL queries that power widgets')
print('   - Parameters: dynamic filters passed to datasets')
print()
print('2. **Create via SDK**:')
print('   dashboard = w.lakeview.create(')
print('       name="sales-analytics",')
print('       serialized_dashboard=json.dumps({...dashboard_spec...})')
print('   )')
print()
print('3. **Dashboard Spec (JSON)**:')
print('   {')
print('     "pages": [{')
print('       "name": "Overview",')
print('       "layout": [...widgets...],')
print('       "datasets": [{"name": "ds1", "query": "SELECT * FROM ops.gold.customer_spending"}]')
print('     }],')
print('     "datasets": [...],')
print('     "parameters": [...]')
print('   }')
print()
print('4. **Widget Types**:')
print('   - Counter: single KPI value')
print('   - Table: tabular data display')
print('   - Chart: bar, line, scatter, pie, heatmap, funnel')
print('   - Text: markdown annotations')
print('   - Filter: dashboard-level parameter widget')
print()
print('5. **Publish and Share**:')
print('   w.lakeview.publish(dashboard_id)')
print('   w.lakeview.update_permissions(dashboard_id, ...)')

# COMMAND ----------

# DBTITLE 1,Cell 2: Create Dashboard
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Create a Dashboard via SDK

# COMMAND ----------

import json

dashboard_spec = {
    'pages': [{
        'name': 'Customer Analytics',
        'layout': [
            {'widget': {'widget_type': 'counter', 'dataset': 'kpi_total', 'spec': {'field': 'total_customers'}}, 'position': {'x': 0, 'y': 0, 'w': 4, 'h': 3}},
            {'widget': {'widget_type': 'table', 'dataset': 'spending_data', 'spec': {}}, 'position': {'x': 4, 'y': 0, 'w': 8, 'h': 6}},
            {'widget': {'widget_type': 'chart', 'dataset': 'spending_data', 'spec': {'chart_type': 'bar', 'x': 'name', 'y': 'total_spent'}}, 'position': {'x': 0, 'y': 3, 'w': 4, 'h': 6}},
        ],
        'datasets': [
            {'name': 'kpi_total', 'query': 'SELECT count(*) AS total_customers FROM ops.gold.customer_spending'},
            {'name': 'spending_data', 'query': 'SELECT name, total_spent, average_spent FROM ops.gold.customer_spending ORDER BY total_spent DESC'},
        ],
    }],
}

print('Dashboard spec created (JSON):')
print(json.dumps(dashboard_spec, indent=2)[:500])
print('...')
print()
print('To create via SDK:')
print('  dashboard = w.lakeview.create(')
print('      name="learning-customer-analytics",')
print('      serialized_dashboard=json.dumps(dashboard_spec)')
print('  )')
print('  w.lakeview.publish(dashboard_id=dashboard.dashboard_id)')
print()
print('To update via SDK:')
print('  w.lakeview.update(dashboard_id, serialized_dashboard=json.dumps(updated_spec))')
print()
print('To list existing dashboards:')
try:
    dashboards = w.lakeview.list()
    for d in dashboards:
        print(f'  {d.dashboard_id} | {d.name}')
except Exception as e:
    print(f'  (API may differ: {e})')

# COMMAND ----------

# DBTITLE 1,Cell 3: SQL Alerts
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: SQL Alerts — Proactive Monitoring
# MAGIC
# MAGIC SQL alerts evaluate a query on a schedule and notify when a condition is met.

# COMMAND ----------

print('SQL Alert Concepts:')
print()
print('1. **Alert Components**:')
print('   - SQL query (returns a single row with a numeric value)')
print('   - Trigger condition (operator + threshold)')
print('   - Notification channel (email, webhook, Slack)')
print('   - Schedule (how often to evaluate)')
print()
print('2. **Create via SDK**:')
print('   w.alerts.create(')
print('       name="high-cost-alert",')
print('       query_text="SELECT sum(usage_quantity) AS dbus FROM system.billing.usage WHERE usage_date = current_date()",')
print('       options={')
print('           "op": ">",')
print('           "value": "500",')
print('           "muted": False,')
print('           "column": "dbus",')
print('       },')
print('       schedule={"quartz_cron": "0 0 9 * * ?", "timezone_id": "UTC"},')
print('       notify_email="user@example.com",')
print('   )')
print()
print('3. **Alert Operators**:')
print('   >  Greater than')
print('   <  Less than')
print('   >= Greater than or equal')
print('   <= Less than or equal')
print('   == Equal')
print('   != Not equal')
print()
print('4. **Notification Channels**:')
print('   Email: Direct email notification')
print('   Webhook: HTTP POST to external system')
print('   Slack: Message to Slack channel')
print('   Teams: Microsoft Teams notification')
print()
print('5. **Example Alert Queries**:')
print('   Cost: SELECT sum(usage_quantity) AS dbus FROM system.billing.usage WHERE usage_date = current_date()')
print('   Job failures: SELECT count(*) AS failures FROM system.lakeflow.job_runs WHERE result_state = FAILED')
print('   Data freshness: SELECT max(event_time) AS latest FROM demo.bronze.events')
print('   Null rate: SELECT sum(CASE WHEN col IS NULL THEN 1 ELSE 0 END) * 100.0 / count(*) AS pct_null FROM table')

# COMMAND ----------

# DBTITLE 1,Cell 4: Genie Spaces
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Genie Spaces — Natural Language Data Q&A
# MAGIC
# MAGIC Genie spaces let users ask questions in natural language and get SQL answers powered by AI.

# COMMAND ----------

print('Genie Space Concepts:')
print()
print('1. **What is a Genie Space?**')
print('   A conversational AI interface over your data')
print('   Users ask questions in English, Genie translates to SQL')
print('   Returns tables, charts, and explanations')
print()
print('2. **Setup Steps**:')
print('   a. Select tables/views in Unity Catalog')
print('   b. Add table descriptions and column comments')
print('   c. Provide example SQL queries (training data)')
print('   d. Add business glossary terms')
print('   e. Test with natural language questions')
print()
print('3. **Create via SDK**:')
print('   genie = w.genie.create_room(')
print('       name="Sales Analytics Genie",')
print('       tables=["ops.gold.customer_spending", "demo.bronze.customer_transactions"],')
print('       instructions="This data contains customer transaction and spending summaries",')
print('   )')
print()
print('4. **Best Practices for Genie Quality**:')
print('   - Add detailed column descriptions: COMMENT ON COLUMN ops.gold.customer_spending.total_spent IS Total amount spent by customer')
print('   - Provide example queries: teaches Genie the right SQL patterns')
print('   - Use table aliases in descriptions')
print('   - Define business terms: glossary entries for domain-specific vocabulary')
print('   - Test with diverse question types before publishing')
print()
print('5. **Example Questions Genie Can Answer**:')
print('   - Who are the top 5 customers by total spending?')
print('   - What is the average transaction value by region?')
print('   - How many customers have above-average spending?')
print('   - Show me monthly spending trends')
print()
print('6. **Limitations**:')
print('   - Works best with well-documented tables')
print('   - Complex joins may need additional training')
print('   - Results should be verified for critical decisions')

# COMMAND ----------

