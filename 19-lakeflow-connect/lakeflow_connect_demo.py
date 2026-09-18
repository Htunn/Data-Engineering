# Databricks notebook source
# DBTITLE 1,Lakeflow Connect Overview
# MAGIC %md
# MAGIC # Lakeflow Connect — Managed Ingestion Connectors
# MAGIC
# MAGIC **Objective**: Learn how to use Lakeflow Connect to ingest data from external sources (SaaS apps, databases, APIs) into Databricks Delta Lake with managed connectors.
# MAGIC
# MAGIC ## Lakeflow Connect Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph "External Sources"
# MAGIC         SF[Salesforce] --> C[Lakeflow Connect]
# MAGIC         GA[Google Ads] --> C
# MAGIC         MYSQL[MySQL] --> C
# MAGIC         PG[PostgreSQL] --> C
# MAGIC         SA[ServiceNow] --> C
# MAGIC         HS[HubSpot] --> C
# MAGIC         WD[Workday] --> C
# MAGIC     end
# MAGIC     subgraph "Databricks"
# MAGIC         C -->|Managed Pipeline| BR[Bronze Layer<br/>Delta tables]
# MAGIC         BR --> SL[Silver Layer<br/>Cleaned + transformed]
# MAGIC     end
# MAGIC     C -.->|Auto-managed| M[Connection<br/>Credentials<br/>Scheduling<br/>Monitoring]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: Available Connectors
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Available Connectors and Setup

# COMMAND ----------

print('Lakeflow Connect Available Connectors:')
print()
print('| Category | Connector | Key Tables |")
print('|----------|-----------|------------|")
print('| CRM | Salesforce | Account, Contact, Opportunity, Lead |')
print('| CRM | HubSpot | Contacts, Deals, Companies |')
print('| Advertising | Google Ads | Campaigns, Ad Groups, Ads |')
print('| Advertising | Meta Ads | Campaigns, Ad Sets, Insights |')
print('| ITSM | ServiceNow | Incident, Change, Problem |')
print('| ITSM | Jira | Issues, Projects, Sprints |')
print('| HR | Workday | Workers, Positions, Payroll |')
print('| Database | MySQL | User-defined tables |')
print('| Database | PostgreSQL | User-defined tables |')
print('| Database | SQL Server | User-defined tables |')
print()
print('Setup Process (via UI wizard):')
print('   1. Navigate to: Lakeflow > Connect > Create Connection')
print('   2. Select connector type (e.g., Salesforce)')
print('   3. Provide credentials (stored in Databricks secrets)')
print('   4. Select tables to ingest')
print('   5. Configure schedule (incremental or full load)')
print('   6. Set destination catalog and schema')
print('   7. Start the pipeline')
print()
print('Via SDK/API:')
print('   w.connections.create(')
print('       name="salesforce-prod",')
print('       connection_type="SALESFORCE",')
print('       options={')
print('           "clientId": "<client-id>",')
print('           "clientSecret": dbutils.secrets.get("my-scope", "sf-client-secret"),')
print('           "loginUrl": "https://login.salesforce.com",')
print('       }')
print('   )')

# COMMAND ----------

# DBTITLE 1,Cell 2: Connection Config
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Connection Configuration Examples

# COMMAND ----------

print('Connection Configuration Examples:')
print()
print('=== Salesforce ===')
print('options = {')
print('    "clientId": "<consumer-key>",')
print('    "clientSecret": dbutils.secrets.get("sf-scope", "client-secret"),')
print('    "loginUrl": "https://login.salesforce.com",')
print('}')
print()
print('=== MySQL ===')
print('options = {')
print('    "host": "mysql.example.com",')
print('    "port": "3306",')
print('    "user": "readonly_user",')
print('    "password": dbutils.secrets.get("mysql-scope", "password"),')
print('    "database": "production",')
print('}')
print()
print('=== PostgreSQL ===')
print('options = {')
print('    "host": "pg.example.com",')
print('    "port": "5432",')
print('    "user": "readonly_user",')
print('    "password": dbutils.secrets.get("pg-scope", "password"),')
print('    "database": "production",')
print('}')
print()
print('=== Google Ads ===')
print('options = {')
print('    "clientId": "<oauth-client-id>",')
print('    "clientSecret": dbutils.secrets.get("ga-scope", "client-secret"),')
print('    "refreshToken": dbutils.secrets.get("ga-scope", "refresh-token"),')
print('    "developerToken": dbutils.secrets.get("ga-scope", "developer-token"),')
print('}')
print()
print('Best Practices:')
print('   - Always use dbutils.secrets for credentials (never hardcode)')
print('   - Use a dedicated read-only database user for source databases')
print('   - Set up incremental ingestion for large tables (CDC or watermark)')
print('   - Use separate connections for dev/staging/prod environments')
print('   - Monitor ingestion lag via system tables')

# COMMAND ----------

# DBTITLE 1,Cell 3: Pipeline Monitoring
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Pipeline Configuration and Monitoring

# COMMAND ----------

print('Pipeline Configuration:')
print()
print('# Create a managed ingestion pipeline')
print('w.pipelines.create(')
print('    name="salesforce-ingestion",')
print('    development=False,')
print('    catalog="demo",')
print('    target="bronze_sf",')
print('    configuration={')
print('        "source": "salesforce-prod",')
print('        "tables": "Account,Contact,Opportunity",')
print('        "ingestion_mode": "incremental",')
print('    }')
print(')')
print()
print('# Monitor via system tables')
print('spark.sql("""')
print('SELECT')
print('    pipeline_name,')
print('    state,')
print('    latest_update.state AS update_state,')
print('    latest_update.creation_time')
print('FROM system.lakeflow.pipelines')
print('ORDER BY latest_update.creation_time DESC')
print('LIMIT 10')
print('""").display()')
print()
print('# Check ingestion metrics')
print('spark.sql("""')
print('SELECT')
print('    event_type,')
print('    count(*) AS event_count,')
print('    max(timestamp) AS latest_event')
print('FROM system.lakeflow.pipeline_events')
print('WHERE pipeline_name LIKE "salesforce-%"')
print('GROUP BY event_type')
print('""").display()')
print()
print('Key Features of Lakeflow Connect:')
print('   - Fully managed (no infrastructure to maintain)')
print('   - Automatic schema inference and evolution')
print('   - Incremental ingestion with CDC support')
print('   - Built-in retry and error handling')
print('   - Automatic credential rotation awareness')
print('   - Data lands in Bronze layer Delta tables')
print('   - Ready for downstream Silver/Gold processing')

# COMMAND ----------

