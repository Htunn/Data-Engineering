# Databricks notebook source
# DBTITLE 1,Setup
# Databricks notebook source
# MAGIC %md
# MAGIC # Disaster Recovery and Migration
# MAGIC
# MAGIC Delta backup, DEEP CLONE, cross-region replication, workspace migration.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS demo.dr")
print("✅ Schema created")

# Create a production table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.dr.production_table AS
    SELECT id, rand() * 1000 AS value, current_timestamp() AS created_at
    FROM range(10000)
""")
print("Production table created")

# COMMAND ----------

# DBTITLE 1,DEEP CLONE
# Databricks notebook source
# MAGIC %md
# MAGIC ## DEEP CLONE for Backup

# COMMAND ----------

# DEEP CLONE copies all data files (vs SHALLOW CLONE which only copies metadata)
spark.sql("CREATE TABLE IF NOT EXISTS demo.dr.backup_table DEEP CLONE demo.dr.production_table")
print("✅ DEEP CLONE backup created")

# Verify
original_count = spark.table("demo.dr.production_table").count()
backup_count = spark.table("demo.dr.backup_table").count()
print(f"  Original: {original_count} rows")
print(f"  Backup: {backup_count} rows")

# DEEP CLONE preserves:
print("\nDEEP CLONE preserves:")
print("  - All data files (full copy)")
print("  - Schema and table properties")
print("  - Table history (time travel)")
print("  - Constraints and tags")

print("\nUse cases:")
print("  - Disaster recovery backup")
print("  - Test environment cloning")
print("  - Cross-region replication")
print("  - Data archival")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Time Travel Recovery
# Databricks notebook source
# MAGIC %md
# MAGIC ## Time Travel for Recovery

# COMMAND ----------

# Simulate data corruption
spark.sql("UPDATE demo.dr.production_table SET value = -999")
print("Data corrupted (all values set to -999)")

current = spark.table("demo.dr.production_table").select("value").limit(3).collect()
print(f"  Current values: {[r.value for r in current]}")

# Restore from version 0 (before corruption)
spark.sql("RESTORE TABLE demo.dr.production_table TO VERSION AS OF 0")
print("\nTable restored to version 0")

restored = spark.table("demo.dr.production_table").select("value").limit(3).collect()
print(f"  Restored values: {[round(r.value, 2) for r in restored]}")

print("\nTime Travel commands:")
print("  - SELECT * FROM table VERSION AS OF 5")
print("  - SELECT * FROM table TIMESTAMP AS OF '2025-01-01 10:00:00'")
print("  - RESTORE TABLE table TO VERSION AS OF 5")
print("  - RESTORE TABLE table TO TIMESTAMP AS OF '2025-01-01'")

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# Databricks notebook source
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Feature | Use |
# MAGIC |---------|-----|
# MAGIC | **DEEP CLONE** | Full table backup with all data files |
# MAGIC | **Time Travel** | Restore to previous versions (RESTORE TABLE) |
# MAGIC | **Delta Log** | Transaction log enables full recovery |
# MAGIC | **Cross-region replication** | DEEP CLONE to another region metastore |
# MAGIC | **Workspace migration** | Export/import via API or CLI |
# MAGIC
# MAGIC ## DR Workflow
# MAGIC 1. **Backup**: DEEP CLONE to backup catalog daily
# MAGIC 2. **Monitor**: Alert on failed jobs/pipelines
# MAGIC 3. **Recover**: Time Travel or RESTORE from backup
# MAGIC 4. **Test**: Regular DR drills on backups