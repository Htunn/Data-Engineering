# Databricks notebook source
# DBTITLE 1,Notebook Utilities Overview
# MAGIC %md
# MAGIC # Notebook Utilities & Secrets Management
# MAGIC
# MAGIC **Objective**: Master `dbutils` utilities for file operations, widget-based parameterization, secrets management, and inter-notebook communication via `%run` and `dbutils.notebook.run()`.
# MAGIC
# MAGIC ## dbutils Overview
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC     DB[dbutils] --> FS[dbutils.fs<br/>File system ops]
# MAGIC     DB --> W[dbutils.widgets<br/>Parameters and widgets]
# MAGIC     DB --> S[dbutils.secrets<br/>Credential access]
# MAGIC     DB --> NB[dbutils.notebook<br/>Notebook execution]
# MAGIC     DB --> LIB[dbutils.library<br/>Library install]
# MAGIC     DB --> JOB[dbutils.jobs<br/>Task values]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: dbutils.fs
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: dbutils.fs — File System Operations
# MAGIC
# MAGIC Interact with files in DBFS, cloud storage, and UC volumes.

# COMMAND ----------

# List files in a directory
print('Files in root:')
for item in dbutils.fs.ls('/'):
    print(f'  {item.path} ({item.size} bytes)')

# Create a UC volume directory reference
print('\nUC Volume operations:')
spark.sql('CREATE CATALOG IF NOT EXISTS demo')
spark.sql('CREATE SCHEMA IF NOT EXISTS demo.utils')
spark.sql('CREATE VOLUME IF NOT EXISTS demo.utils.temp_files')

# Write a file to a UC volume
with open('/tmp/sample.txt', 'w') as f:
    f.write('Hello from Databricks!\n')
    f.write('Learning dbutils.fs operations.\n')

dbutils.fs.cp('file:/tmp/sample.txt', '/Volumes/demo/utils/temp_files/sample.txt')
print('Copied file to UC volume')

# Read the file back
content = dbutils.fs.head('/Volumes/demo/utils/temp_files/sample.txt', 100)
print(f'File content: {content}')

# List volume contents
print('\nFiles in volume:')
for item in dbutils.fs.ls('/Volumes/demo/utils/temp_files/'):
    print(f'  {item.path} ({item.size} bytes)')

# COMMAND ----------

# DBTITLE 1,Cell 2: dbutils.widgets
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: dbutils.widgets — Parameter Passing
# MAGIC
# MAGIC Widgets allow parameterizing notebooks. They appear in the UI and can be passed from jobs or other notebooks.

# COMMAND ----------

# Create widgets with defaults
dbutils.widgets.text('table_name', 'demo.bronze.customer_transactions', 'Table Name')
dbutils.widgets.dropdown('environment', 'dev', ['dev', 'staging', 'prod'], 'Environment')
dbutils.widgets.multiselect('columns', 'id,name', ['id', 'name', 'date', 'amount'], 'Columns')

# Read widget values
table_name = dbutils.widgets.get('table_name')
environment = dbutils.widgets.get('environment')
columns = dbutils.widgets.get('columns')

print(f'Table: {table_name}')
print(f'Environment: {environment}')
print(f'Columns: {columns}')

# Use widget values in SQL
selected_cols = ', '.join([c.strip() for c in columns.split(',')])
query = f'SELECT {selected_cols} FROM {table_name} LIMIT 5'
print(f'\nQuery: {query}')

try:
    spark.sql(query).display()
except Exception as e:
    print(f'Table not found (expected for first run): {e}')

# Remove widgets when done (optional)
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Cell 3: dbutils.secrets
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: dbutils.secrets — Credential Management
# MAGIC
# MAGIC Access secrets stored in Databricks secret scopes (backed by Databricks or Azure Key Vault / AWS Secrets Manager).

# COMMAND ----------

print('Secrets Management Patterns:')
print()
print('1. Create a secret scope (CLI):')
print('   databricks secrets create-scope my-scope')
print()
print('2. Put a secret (CLI):')
print('   databricks secrets put-secret my-scope api-key --string-value my-api-key-value')
print()
print('3. Access in notebook:')
print('   api_key = dbutils.secrets.get(scope=my-scope, key=api-key)')
print()
print('4. List scopes and keys:')
print('   dbutils.secrets.listScopes()')
print('   dbutils.secrets.list(my-scope)')
print()

# List existing secret scopes
print('Existing secret scopes:')
try:
    scopes = dbutils.secrets.listScopes()
    for s in scopes:
        print(f'  Scope: {s.name}')
        keys = dbutils.secrets.list(s.name)
        for k in keys:
            print(f'    Key: {k.key}')
except Exception as e:
    print(f'  (No scopes or access: {e})')

print('\nBest Practices:')
print('   - Never print secret values in notebooks')
print('   - Use secrets in Spark conf, not in string concatenation')
print('   - Scope names are visible to all users (not secret)')
print('   - Use ACLs to control who can read which scope')
print('   - For external APIs: store base URL + key separately')

# COMMAND ----------

# DBTITLE 1,Cell 4: Inter-Notebook Communication
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Inter-Notebook Communication
# MAGIC
# MAGIC %run shares variables; dbutils.notebook.run() runs isolated with return values.

# COMMAND ----------

print('Inter-Notebook Communication Patterns:')
print()
print('=== Method 1: %run (shares notebook context) ===')
print('   %run /Repos/.../simple_medallion_architecture')
print('   - Shares all variables from the called notebook')
print('   - No return value')
print('   - Runs synchronously in the same Spark session')
print('   - Good for importing helper functions and shared setup')
print()
print('=== Method 2: dbutils.notebook.run() (isolated) ===')
print('   result = dbutils.notebook.run(')
print('       path, timeout_seconds=300, arguments={param1: value1})')
print('   - Runs in isolated context (no shared variables)')
print('   - Returns a string value (via dbutils.notebook.exit())')
print('   - Can pass arguments as a dictionary')
print('   - Good for modular, reusable notebook calls')
print()
print('=== Method 3: dbutils.notebook.exit() (return value) ===')
print('   In the called notebook:')
print('   dbutils.notebook.exit(json.dumps({status: ok, rows: 1000}))')
print()
print('=== Method 4: dbutils.jobs.taskValues (for Jobs) ===')
print('   Set: dbutils.jobs.taskValues.set(key=row_count, value=1000)')
print('   Get (downstream): dbutils.jobs.taskValues.get(taskKey=upstream, key=row_count)')

# COMMAND ----------

# DBTITLE 1,Cell 5: Library Management
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Library Management and %pip

# COMMAND ----------

print('Library Management Patterns:')
print()
print('1. %pip install (preferred for notebook-scoped libraries):')
print('   %pip install scikit-learn pandas numpy')
print('   %pip install /path/to/custom.whl')
print()
print('2. Install from requirements.txt:')
print('   %pip install -r /Workspace/requirements.txt')
print()
print('3. Install from a UC volume:')
print('   %pip install /Volumes/demo/utils/packages/my-package-1.0-py3-none-any.whl')
print()
print('4. Restart Python after installing (if needed):')
print('   dbutils.library.restartPython()')
print()
print('Best Practices:')
print('   - Use %pip for notebook-scoped installs (recommended)')
print('   - Use cluster libraries for persistent installs across sessions')
print('   - Use DAB to define libraries in databricks.yml for reproducibility')
print('   - Pin versions in production: %pip install scikit-learn==1.5.0')

# COMMAND ----------

# DBTITLE 1,Cell 6: Notebook Context
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Notebook Context and Environment

# COMMAND ----------

import os
import json

# Databricks runtime context
print('Notebook Context:')
print(f'  Runtime version: {os.environ.get("DATABRICKS_RUNTIME_VERSION", "unknown")}')
print(f'  Cluster ID: {os.environ.get("DATABRICKS_CLUSTER_ID", "serverless")}')
print(f'  Notebook path: {os.environ.get("DATABRICKS_NOTEBOOK_PATH", "unknown")}')
print(f'  Workspace URL: {os.environ.get("DATABRICKS_HOST", "unknown")}')
print()

# Spark configuration
print('Key Spark Configurations:')
spark_confs = [
    'spark.app.name',
    'spark.databricks.workspace.host',
]
for conf in spark_confs:
    try:
        value = spark.conf.get(conf)
        print(f'  {conf}: {value}')
    except Exception:
        print(f'  {conf}: (not set)')
print()

# Programmatic context access
print('Programmatic Context Access:')
print('  ctx = dbutils.notebook.entry_point.getContext().toJson()')
print('  context = json.loads(ctx)')
print('  workspace_url = context["tags"]["DATABRICKS_WORKSPACE_URL"]')
print('  job_id = context.get("jobId", "N/A")')
print('  run_id = context.get("runId", "N/A")')
print()

# Common environment variables
print('Common Environment Variables:')
for key in ['HOME', 'TMPDIR', 'PYTHONPATH']:
    val = os.environ.get(key, '(not set)')
    print(f'  {key}: {val[:80]}')

# COMMAND ----------

