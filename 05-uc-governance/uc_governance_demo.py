# Databricks notebook source
# DBTITLE 1,UC Governance — Overview
# MAGIC %md
# MAGIC # Unity Catalog: Data Governance & Security
# MAGIC
# MAGIC **Use Case**: Implement enterprise-grade data governance with Unity Catalog.
# MAGIC
# MAGIC ## Features Covered
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **Catalog & Schema management** | Three-level namespace (catalog.schema.table) |
# MAGIC | **Grants & privileges** | Fine-grained access control (SELECT, MODIFY, CREATE, etc.) |
# MAGIC | **Tags** | Classify data for discovery and policy enforcement |
# MAGIC | **Row-level security** | Filter rows based on the querying user |
# MAGIC | **Column masking** | Mask sensitive columns for unauthorized users |
# MAGIC | **Data discovery** | `information_schema` queries for metadata catalog |
# MAGIC | **Lineage** | Track data flow across tables and notebooks |
# MAGIC | **Audit logs** | `system.access` tables for compliance |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Setup
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Setup — Catalog, Schemas, Tables

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS demo.governance")

# Drop existing tables for clean re-run
for table in ["employees", "employee_pii", "orders", "customer_profiles"]:
    spark.sql(f"DROP TABLE IF EXISTS demo.governance.{table}")

# --- Employees table ---
employees_data = [
    (1, "John Doe",   "Engineering",  95000, "john@example.com",   "408-555-0101", "US"),
    (2, "Jane Smith", "Sales",       75000, "jane@example.com",   "408-555-0102", "US"),
    (3, "Sam Brown",  "Engineering", 88000, "sam@example.com",    "408-555-0103", "EU"),
    (4, "Lisa Wang",  "Marketing",   65000, "lisa@example.com",  "408-555-0104", "EU"),
    (5, "Bob Lee",    "Sales",       72000, "bob@example.com",   "408-555-0105", "APAC"),
]
spark.createDataFrame(employees_data, ["emp_id", "name", "department", "salary", "email", "phone", "region"]) \
    .write.format("delta").saveAsTable("demo.governance.employees")

# --- Orders table ---
orders_data = [
    (1, 101, 1500.00, "2025-09-01", "US"),
    (2, 102, 2500.00, "2025-09-02", "EU"),
    (3, 103, 500.00,  "2025-09-03", "APAC"),
    (4, 101, 3200.00, "2025-09-04", "US"),
    (5, 102, 1800.00, "2025-09-05", "EU"),
]
spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount", "order_date", "region"]) \
    .write.format("delta").saveAsTable("demo.governance.orders")

print("✅ Tables created:")
print("   - demo.governance.employees (with PII columns)")
print("   - demo.governance.orders (with region for RLS)")

# COMMAND ----------

# DBTITLE 1,Cell 2: Grants & Privileges
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Granting & Revoking Privileges
# MAGIC
# MAGIC Unity Catalog uses standard SQL GRANT/REVOKE syntax.

# COMMAND ----------

# Get current user (for demo purposes)
current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")

# Grant schema-level privileges
spark.sql(f"GRANT USE SCHEMA ON SCHEMA demo.governance TO `{current_user}`")

# Grant table-level privileges
spark.sql(f"GRANT SELECT ON TABLE demo.governance.employees TO `{current_user}`")
spark.sql(f"GRANT SELECT, MODIFY ON TABLE demo.governance.orders TO `{current_user}`")

# Show current grants
print("\n📋 Current grants on demo.governance.employees:")
spark.sql(f"SHOW GRANTS ON TABLE demo.governance.employees").display()

print("\n📋 Current grants on demo.governance.orders:")
spark.sql(f"SHOW GRANTS ON TABLE demo.governance.orders").display()

# Privilege hierarchy:
print("""
🔧 Unity Catalog Privilege Hierarchy:

  Catalog level:  CREATE SCHEMA, USE CATALOG
  Schema level:   CREATE TABLE, USE SCHEMA, CREATE FUNCTION, CREATE MODEL
  Table level:    SELECT, MODIFY, READ_METADATA, APPLY TAG, ALL PRIVILEGES
  Function level: EXECUTE
  Volume level:   READ VOLUME, WRITE VOLUME, CREATE FILE
""")

# COMMAND ----------

# DBTITLE 1,Cell 3: Tags for Data Classification
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Tags for Data Classification
# MAGIC
# MAGIC Tag tables and columns to classify data for discovery, policy enforcement, and cost tracking.

# COMMAND ----------

# Apply table-level tags
spark.sql("""
    ALTER TABLE demo.governance.employees 
    SET TAGS ('data_domain' = 'hr', 'sensitivity' = 'confidential', 'owner_team' = 'people_ops')
""")

spark.sql("""
    ALTER TABLE demo.governance.orders 
    SET TAGS ('data_domain' = 'sales', 'sensitivity' = 'internal', 'owner_team' = 'revenue_ops')
""")

# Apply column-level tags (PII classification)
spark.sql("ALTER TABLE demo.governance.employees ALTER COLUMN email SET TAGS ('pii' = 'true', 'gdpr' = 'true')")
spark.sql("ALTER TABLE demo.governance.employees ALTER COLUMN phone SET TAGS ('pii' = 'true')")
spark.sql("ALTER TABLE demo.governance.employees ALTER COLUMN salary SET TAGS ('sensitivity' = 'highly_confidential')")

# Verify tags
print("🏷️ Table-level tags:")
spark.sql("""
    SELECT catalog_name, schema_name, table_name, tag_name, tag_value
    FROM system.information_schema.table_tags
    WHERE schema_name = 'governance' AND catalog_name = 'demo'
    ORDER BY table_name, tag_name
""").display()

print("\n🏷️ Column-level tags:")
spark.sql("""
    SELECT table_name, column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE table_schema = 'governance' AND table_catalog = 'demo'
    ORDER BY column_name, tag_name
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 4: Row-Level Security
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Row-Level Security (RLS)
# MAGIC
# MAGIC Filter rows dynamically based on the querying user — users only see data for their region.

# COMMAND ----------

# Create a function that returns the current user's region
# In production, this would look up a user-to-region mapping table
spark.sql("""
    CREATE OR REPLACE FUNCTION demo.governance.get_user_region()
    RETURNS STRING
    RETURN 'US'  -- Demo: always return 'US'. In production, query a user-mapping table.
""")

# Apply row filter: only show orders for the user's region
spark.sql("""
    CREATE FUNCTION demo.governance.orders_region_filter(region STRING)
    RETURNS BOOLEAN
    RETURN region = demo.governance.get_user_region()
""")

# Note: Row filters and column masks require SQL Warehouse or specific compute
# The following would be applied via the UI or API:
# spark.sql("ALTER TABLE demo.governance.orders SET ROW FILTER demo.governance.orders_region_filter(region)")

# Simulate the filter:
print("🔒 Row-Level Security Demo:")
print("   User sees only orders in their region (US):")
spark.sql("""
    SELECT * FROM demo.governance.orders 
    WHERE region = demo.governance.get_user_region()
    ORDER BY order_date
""").display()

print("\n   Without RLS, user would see all regions:")
spark.sql("SELECT * FROM demo.governance.orders ORDER BY order_date").display()

# COMMAND ----------

# DBTITLE 1,Cell 5: Column Masking
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Column Masking (Dynamic Data Masking)
# MAGIC
# MAGIC Mask sensitive columns for unauthorized users while keeping plain text for authorized ones.

# COMMAND ----------

# Create a masking function for email (show first 2 chars + masked domain)
spark.sql("""
    CREATE OR REPLACE FUNCTION demo.governance.mask_email(email STRING)
    RETURNS STRING
    RETURN CASE
        WHEN email IS NULL THEN NULL
        WHEN is_member('data_engineers') THEN email  -- authorized group sees full email
        ELSE CONCAT(SUBSTRING(email, 1, 2), '***@', SPLIT(email, '@')[1])  -- masked for others
    END
""")

# Create a masking function for phone (show last 4 digits only)
spark.sql("""
    CREATE OR REPLACE FUNCTION demo.governance.mask_phone(phone STRING)
    RETURNS STRING
    RETURN CASE
        WHEN phone IS NULL THEN NULL
        WHEN is_member('data_engineers') THEN phone
        ELSE CONCAT('***-***-', SUBSTRING(phone, -4))
    END
""")

# In production, apply masks via ALTER TABLE:
# spark.sql("ALTER TABLE demo.governance.employees ALTER COLUMN email SET MASK demo.governance.mask_email")
# spark.sql("ALTER TABLE demo.governance.employees ALTER COLUMN phone SET MASK demo.governance.mask_phone")

# Simulate masking:
print("🔒 Column Masking Demo:")
print("   Masked view (for non-data_engineers users):")
spark.sql("""
    SELECT 
        emp_id,
        name,
        department,
        CONCAT(SUBSTRING(email, 1, 2), '***@', SPLIT(email, '@')[1]) AS email_masked,
        CONCAT('***-***-', SUBSTRING(phone, -4)) AS phone_masked,
        '***' AS salary_masked,
        region
    FROM demo.governance.employees
""").display()

print("\n   Unmasked view (for authorized users):")
spark.sql("SELECT emp_id, name, email, phone, salary FROM demo.governance.employees").display()

# COMMAND ----------

# DBTITLE 1,Cell 6: Data Discovery
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Data Discovery via Information Schema
# MAGIC
# MAGIC Use `information_schema` to discover and catalog all data assets.

# COMMAND ----------

# List all tables in the demo catalog
print("📋 All tables in demo catalog:")
spark.sql("""
    SELECT table_schema, table_name, table_type, table_owner, created
    FROM system.information_schema.tables
    WHERE table_catalog = 'demo'
    ORDER BY table_schema, table_name
""").display()

# List all columns with types
print("\n📋 All columns in demo.governance:")
spark.sql("""
    SELECT table_name, column_name, data_type, is_nullable, column_default
    FROM system.information_schema.columns
    WHERE table_catalog = 'demo' AND table_schema = 'governance'
    ORDER BY table_name, ordinal_position
""").display()

# Find all PII columns using tags
print("\n🔍 PII columns (via tags):")
spark.sql("""
    SELECT table_name, column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE tag_name = 'pii' AND tag_value = 'true'
    ORDER BY table_name, column_name
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 7: Audit Logs & Compliance
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Audit Logs & Compliance
# MAGIC
# MAGIC Query `system.access` tables for audit trail of who accessed what.

# COMMAND ----------

# Note: system.access tables are available at the account level
# These queries show patterns for compliance auditing

print("📋 Recent table access audit log (sample pattern):")
spark.sql("""
    SELECT 
        event_time,
        action_name,
        event_type,
        user_identity.email AS user,
        request_params.full_name_arg AS table_name,
        source_ip_address,
        response.status_code
    FROM system.access.audit
    WHERE action_name IN ('searchedTables', 'getTable', 'generateTemporaryTableCredential')
      AND request_params.full_name_arg LIKE 'demo%'
    ORDER BY event_time DESC
    LIMIT 20
""").display()

print("\n📋 Data model lineage (sample pattern):")
spark.sql("""
    SELECT 
        source_table_full_name,
        target_table_full_name,
        source_column_name,
        target_column_name,
        entity_type
    FROM system.access.table_lineage
    WHERE source_table_catalog = 'demo'
    ORDER BY source_table_full_name
    LIMIT 20
""").display()

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|--------|
# MAGIC | **Three-level namespace** | Organized, scalable table management |
# MAGIC | **GRANT/REVOKE** | SQL-standard, fine-grained access control |
# MAGIC | **Tags** | Discoverability, policy enforcement, cost attribution |
# MAGIC | **Row filters** | Users see only data they're authorized for |
# MAGIC | **Column masks** | PII protection without creating separate tables |
# MAGIC | **information_schema** | Data catalog without external tools |
# MAGIC | **Audit logs** | Compliance-ready access trail |
# MAGIC | **Lineage** | Visual and programmatic data flow tracking |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Tag all PII columns** — enables automated policy enforcement and discovery
# MAGIC 2. **Use row filters for multi-tenant data** — avoid creating separate tables per tenant
# MAGIC 3. **Apply masks for PII columns** — protect data at query time without ETL changes
# MAGIC 4. **Grant least privilege** — start with SELECT only, add MODIFY when needed
# MAGIC 5. **Use `is_member()` in mask/filter functions** — group-based dynamic enforcement
# MAGIC 6. **Monitor `system.access.audit`** — detect anomalous access patterns

# COMMAND ----------

