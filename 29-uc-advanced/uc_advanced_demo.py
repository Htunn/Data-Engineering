# Databricks notebook source
# DBTITLE 1,Information Schema
# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Advanced - ABAC, Lineage, Marketplace
# MAGIC
# MAGIC Auto-classification, ABAC policies, information schema, Marketplace.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS demo.uc_advanced")
print("✅ Schema created")

# Query information schema
print("\nUnity Catalog information schema queries:")
print("  - system.information_schema.tables")
print("  - system.information_schema.columns")
print("  - system.information_schema.schemata")
print("  - system.information_schema.catalogs")

# List all tables in demo catalog
tables = spark.sql("""
    SELECT table_catalog, table_schema, table_name, table_type
    FROM system.information_schema.tables
    WHERE table_catalog = 'demo'
    ORDER BY table_schema, table_name
    LIMIT 20
""")
print("\nSample tables in demo catalog:")
tables.display()

# COMMAND ----------

# DBTITLE 1,ABAC Policies
# Databricks notebook source
# MAGIC %md
# MAGIC ## ABAC (Attribute-Based Access Control)

# COMMAND ----------

print("ABAC = Row Filters + Column Masks using governed tags")
print("-" * 60)
print("Example: Row filter based on region tag")
print("  CREATE FUNCTION demo.uc_advanced.region_filter(region STRING)")
print("  RETURN CASE")
print("    WHEN is_account_group_member('us_team') AND region = 'US' THEN true")
print("    WHEN is_account_group_member('eu_team') AND region = 'EU' THEN true")
print("    WHEN is_account_group_member('admin') THEN true")
print("    ELSE false")
print("  END")
print("\nApply row filter:")
print("  ALTER TABLE demo.uc_advanced.sales SET ROW FILTER demo.uc_advanced.region_filter ON (region)")

print("\n\nColumn masks - redact PII based on tags:")
print("  CREATE FUNCTION demo.uc_advanced.ssn_mask(ssn STRING)")
print("  RETURN CASE")
print("    WHEN is_account_group_member('pii_readers') THEN ssn")
print("    ELSE '***-**-****'")
print("  END")
print("\nApply column mask:")
print("  ALTER TABLE demo.uc_advanced.customers ALTER COLUMN ssn SET MASK demo.uc_advanced.ssn_mask")

# COMMAND ----------

# DBTITLE 1,Marketplace
# Databricks notebook source
# MAGIC %md
# MAGIC ## Databricks Marketplace

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("Databricks Marketplace:")
print("-" * 60)
print("  - Discover and consume data products from providers")
print("  - Publish your own data products to share with customers")
print("  - Data products can be: tables, Delta Shares, notebooks, ML models")
print("  - Built on Delta Sharing (Module 15)")

print("\nConsuming from Marketplace:")
print("  1. Browse Marketplace in Databricks UI")
print("  2. Request access to a data product")
print("  3. Provider grants access")
print("  4. Data product appears as a catalog in your metastore")
print("  5. Query like any other Unity Catalog table")

print("\nPublishing to Marketplace:")
print("  1. Create a share with tables/views")
print("  2. Submit to Marketplace")
print("  3. Set pricing (free or paid)")
print("  4. Approve/deny access requests")
print("  5. Track usage and consumption")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|----------|
# MAGIC | **Information schema** | Query catalog metadata programmatically |
# MAGIC | **ABAC** | Row filters + column masks based on tags/groups |
# MAGIC | **Marketplace** | Consume and publish data products |
# MAGIC | **Lineage API** | Programmatic access to table/column lineage |
# MAGIC | **Auto-classification** | AI tags PII columns automatically |

# COMMAND ----------

