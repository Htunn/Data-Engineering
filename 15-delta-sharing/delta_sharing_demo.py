# Databricks notebook source
# DBTITLE 1,Delta Sharing Overview
# MAGIC %md
# MAGIC # Delta Sharing — Cross-Organization Data Sharing
# MAGIC
# MAGIC **Objective**: Share Delta tables across organizations using open protocol Delta Sharing — create shares, add tables, create recipients, and grant access.
# MAGIC
# MAGIC ## Delta Sharing Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph "Provider (Databricks)"
# MAGIC         T[Delta Tables<br/>in Unity Catalog] --> S[Share]
# MAGIC         S --> R[Recipient]
# MAGIC     end
# MAGIC     subgraph "Recipient (External)"
# MAGIC         R -->|Open Protocol<br/>REST API| C[Client<br/>pandas/spark/powerbi]
# MAGIC     end
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 1: Create Share
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Create a Share and Add Tables
# MAGIC
# MAGIC A **share** is a collection of Delta tables that you want to share with recipients.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

share_name = 'learning_delta_share'

# Remove existing share if present
try:
    w.sharing.list_shares()
    for s in w.sharing.list_shares().shares:
        if s.name == share_name:
            w.sharing.delete_share(share_name)
            print(f'Removed existing share: {share_name}')
except Exception:
    pass

# Create a new share
w.sharing.create_share(name=share_name, comment='Learning demo — Delta Sharing')
print(f'Created share: {share_name}')

# Add tables from the ops catalog (created in Module 01)
tables_to_share = [
    {'catalog': 'ops', 'schema': 'bronze', 'table': 'customer_transactions'},
    {'catalog': 'ops', 'schema': 'silver', 'table': 'customer_transactions_cleaned'},
    {'catalog': 'ops', 'schema': 'gold', 'table': 'customer_spending'},
]

for t in tables_to_share:
    full_name = f'{t["catalog"]}.{t["schema"]}.{t["table"]}'
    try:
        w.sharing.update_share(
            name=share_name,
            add=[{'name': full_name, 'start_at': '2025-01-01T00:00:00Z'}],
        )
        print(f'  Added table: {full_name}')
    except Exception as e:
        print(f'  Could not add {full_name}: {e}')

# List tables in the share
print('\nTables in share:')
for t in w.sharing.list_share_permissions(share_name).tables_to_add:
    print(f'  {t.name}')


# COMMAND ----------

# DBTITLE 1,Cell 2: Create Recipient
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Create Recipients and Grant Access
# MAGIC
# MAGIC **Recipients** are the external organizations or users who receive the shared data. Two types: OPEN (token-based) and Databricks-to-Databricks.

# COMMAND ----------

recipient_name = 'learning_recipient'

# Remove existing recipient
try:
    for r in w.sharing.list_recipients().recipients:
        if r.name == recipient_name:
            w.sharing.delete_recipient(recipient_name)
            print(f'Removed existing recipient: {recipient_name}')
except Exception:
    pass

# Create an OPEN recipient (token-based, no Databricks account needed)
recipient = w.sharing.create_recipient(
    name=recipient_name,
    sharing_code='learning-demo-2025',
    comment='Learning demo recipient — open protocol',
    ip_access_list={'allowed_ip_addresses': ['0.0.0.0/0']},
)
print(f'Created recipient: {recipient_name}')
print(f'  Sharing code: learning-demo-2025')
print(f'  Activation URL will be generated for OPEN recipients')

# Grant the recipient access to the share
w.sharing.update_recipient_share_permissions(
    name=recipient_name,
    share=share_name,
)
print(f'\nGranted {recipient_name} access to {share_name}')

# List all recipients
print('\nAll recipients:')
for r in w.sharing.list_recipients().recipients:
    print(f'  {r.name} (owner: {r.owner})')


# COMMAND ----------

# DBTITLE 1,Cell 3: Recipient Access
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Recipient-Side Access Patterns
# MAGIC
# MAGIC How recipients access shared data using the open Delta Sharing client.

# COMMAND ----------

print('Recipient Access Patterns:')
print()
print('=== 1. Python (delta-sharing library) ===')
print('pip install delta-sharing')
print()
print('import delta_sharing')
print('# Load the profile file received from the provider')
print('provider = delta_sharing.SharingClient(profile_file="config.share"')
print('shares = provider.list_shares()')
print('tables = provider.list_all_tables()')
print()
print('# Read shared table as pandas DataFrame')
print('df = delta_sharing.load_as_pandas(profile_file, share, schema, table)')
print()
print('# Read as Spark DataFrame')
print('df = delta_sharing.load_as_spark(profile_file, share, schema, table)')
print()
print('=== 2. Spark (Scala/Java) ===')
print('df = spark.read.format("deltaSharing").load(profile_file, share, schema, table)')
print()
print('=== 3. Power BI / Tableau ===')
print('Use the Delta Sharing connector in Power BI Desktop')
print('Connect using the activation URL from the provider')
print()
print('=== 4. Databricks-to-Databricks ===')
print('# In the recipient workspace:')
print('CREATE CATALOG recipient_catalog USING deltaSharing;')
print('GRANT USE CATALOG ON CATALOG recipient_catalog TO `user@example.com`;')
print()
print('Key Concepts:')
print('   - OPEN recipients: token-based, works with any client supporting the protocol')
print('   - D2D recipients: automatic catalog creation in the recipient workspace')
print('   - Data is accessed via REST API, not copied')
print('   - Provider controls which tables, partitions, and columns are shared')
print('   - Recipients see real-time data (no ETL pipelines needed)')

# COMMAND ----------

# DBTITLE 1,Cell 4: Provider Management
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Provider Management — List, Update, Revoke

# COMMAND ----------

# List all shares
print('All Shares:')
for s in w.sharing.list_shares().shares:
    print(f'  {s.name} (owner: {s.owner}, created: {s.created_at})')

# Show share permissions
print(f'\nPermissions for {share_name}:')
try:
    perms = w.sharing.list_share_permissions(share_name)
    print(f'  Recipients: {perms.recipients}')
except Exception as e:
    print(f'  {e}')

# Revoke access (demo)
print(f'\nTo revoke access:')
print(f'  w.sharing.update_recipient_share_permissions(')
print(f'      name="{recipient_name}", share="{share_name}", action="UNGRANT"')
print(f'  )')

# Clean up (uncomment to remove demo resources)
# w.sharing.delete_recipient(recipient_name)
# w.sharing.delete_share(share_name)
# print('\nCleaned up demo resources')

# COMMAND ----------

