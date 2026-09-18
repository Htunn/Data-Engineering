# Databricks notebook source
# DBTITLE 1,Databricks Apps Overview
# MAGIC %md
# MAGIC # Databricks Apps — Building Data Apps
# MAGIC
# MAGIC **Objective**: Learn how to build and deploy data applications on Databricks Apps — a serverless framework for running Streamlit, Flask, Dash, and Gradio apps.
# MAGIC
# MAGIC ## Databricks Apps Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph "Your App"
# MAGIC         CODE[app.py<br/>Streamlit/Flask/Dash] --> YAML[app.yaml<br/>Config & deps]
# MAGIC     end
# MAGIC     subgraph "Databricks Platform"
# MAGIC         DEPLOY[databricks apps deploy] --> RUN[Serverless<br/>App runtime]
# MAGIC         RUN --> AUTH[Databricks Auth<br/>SSO + UC]
# MAGIC         RUN --> SQL[SQL Warehouse<br/>Data access]
# MAGIC     end
# MAGIC     CODE --> DEPLOY
# MAGIC     AUTH --> SQL
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: App Structure
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: App Structure — app.py and app.yaml

# COMMAND ----------

app_py = '''# app.py — Streamlit data app on Databricks Apps
import streamlit as st
from databricks.sql.client import DatabricksSQLClient
import os

# Databricks authentication is automatic
server_hostname = os.environ.get("DATABRICKS_HOST")
http_path = os.environ.get("DATABRICKS_SQL_WAREHOUSE_HTTP_PATH")
access_token = os.environ.get("DATABRICKS_TOKEN")

st.title("Customer Spending Dashboard")

# Connect to Databricks SQL
client = DatabricksSQLClient(server_hostname, http_path, access_token)

cursor = client.cursor()
cursor.execute("SELECT name, total_spent, average_spent FROM ops.gold.customer_spending ORDER BY total_spent DESC")
rows = cursor.fetchall()

# Display as table
import pandas as pd
df = pd.DataFrame(rows, columns=[d["name"] for d in cursor.description])
st.dataframe(df)

# Chart
st.bar_chart(df.set_index("name")["total_spent"])

# Filter widget
selected = st.selectbox("Select customer", df["name"])
row = df[df["name"] == selected].iloc[0]
st.metric("Total Spent", f"${row["total_spent"]:.2f}")
st.metric("Average Spent", f"${row["average_spent"]:.2f}")
'''

app_yaml = '''# app.yaml — Databricks App configuration
app_name: customer-spending-app
description: Streamlit app showing customer spending data
runtime_version: 0.1.0

# Python dependencies
dependencies:
  - streamlit
  - databricks-sql-connector
  - pandas
  - plotly

# Environment variables (non-secret)
env:
  - DATABRICKS_SQL_WAREHOUSE_HTTP_PATH: /sql/1.0/warehouses/your-warehouse-id

# Access control
access_control:
  - user_email: martin.mystery9@gmail.com
    permission: CAN_VIEW
'''

print('=== app.py (Streamlit) ===')
print(app_py)
print('\n=== app.yaml (Configuration) ===')
print(app_yaml)

# COMMAND ----------

# DBTITLE 1,Cell 2: Deploy & Manage Apps
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Deploy and Manage Apps

# COMMAND ----------

print('Databricks Apps CLI Commands:')
print()
print('1. Create and deploy:')
print('   databricks apps create customer-spending-app')
print('   databricks apps deploy customer-spending-app --source /Workspace/apps/customer-spending')
print()
print('2. List and inspect:')
print('   databricks apps list')
print('   databricks apps get customer-spending-app')
print()
print('3. Start and stop:')
print('   databricks apps start customer-spending-app')
print('   databricks apps stop customer-spending-app')
print()
print('4. View logs:')
print('   databricks apps logs customer-spending-app')
print()
print('5. Permissions:')
print('   databricks apps update-permissions customer-spending-app --acl-can-view user@example.com')
print()

# Using the SDK
print('Using Databricks SDK:')
print()
print('from databricks.sdk import WorkspaceClient')
print('w = WorkspaceClient()')
print()
print('# List apps')
print('for app in w.apps.list():')
print('    print(f"{app.name} | {app.status}")')
print()
print('# Create app')
print('w.apps.create(name="customer-spending-app", description="Customer spending dashboard")')
print()
print('# Start/stop')
print('w.apps.start(name="customer-spending-app")')
print('w.apps.stop(name="customer-spending-app")')
print()

print('Supported Frameworks:')
print('   - Streamlit (recommended for data apps)')
print('   - Flask (REST APIs)')
print('   - Dash (interactive dashboards)')
print('   - Gradio (ML model demos)')
print('   - FastAPI (high-performance APIs)')
print('   - Custom WSGI/ASGI apps')

# COMMAND ----------

# DBTITLE 1,Cell 3: Flask API Example
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Flask REST API Example

# COMMAND ----------

flask_app = '''# app.py — Flask REST API on Databricks Apps
from flask import Flask, jsonify, request
from databricks.sql.client import DatabricksSQLClient
import os, pandas as pd

app = Flask(__name__)

# Auto-authenticated Databricks connection
def get_client():
    return DatabricksSQLClient(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_SQL_WAREHOUSE_HTTP_PATH"],
        os.environ["DATABRICKS_TOKEN"],
    )

@app.route("/api/customers")
def get_customers():
    client = get_client()
    cursor = client.cursor()
    cursor.execute("SELECT name, total_spent FROM ops.gold.customer_spending")
    df = pd.DataFrame(
        cursor.fetchall(),
        columns=[d["name"] for d in cursor.description]
    )
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/customer/<name>")
def get_customer(name):
    client = get_client()
    cursor = client.cursor()
    cursor.execute(f"SELECT * FROM ops.gold.customer_spending WHERE name = '{name}'")
    df = pd.DataFrame(
        cursor.fetchall(),
        columns=[d["name"] for d in cursor.description]
    )
    return jsonify(df.to_dict(orient="records"))

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
'''

print('=== Flask REST API (app.py) ===')
print(flask_app)
print()
print('Key Features:')
print('   - Automatic Databricks authentication (no manual token management)')
print('   - Direct SQL Warehouse access via Databricks SQL connector')
print('   - Unity Catalog governance applies to all data access')
print('   - Auto-scales based on traffic')
print('   - SSO authentication inherited from Databricks workspace')

# COMMAND ----------

